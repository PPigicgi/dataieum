"""Durable, exact-metadata-deduplicated introductions. No model calls on reads."""
import hashlib
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import time

from .dataset_intro import VERSION, validate_ids, validate_rows
from .metadata_input import prepare_metadata

MODEL = 'gpt-5.6-luna'
READ_VERSIONS = (VERSION, 'dataset-intro-v2')


def missing(identifier):
    return {'id': identifier, 'available': False, 'ko': {'title': '', 'summary': ''},
            'en': {'title': '', 'summary': ''}}


def content_key(meta, *, version=None):
    return hashlib.sha256(json.dumps([version or VERSION, MODEL, meta['input_hash']]).encode()).hexdigest()


def metadata(row):
    record = json.loads(row['metadata'])
    record.update({k: row[k] for k in ('id', 'title', 'description', 'source_id')})
    return prepare_metadata(record)


def prompt_record(identifier, meta):
    limits = {'title': 700, 'description': 1500, 'classification_paths': 400, 'survey_name': 300, 'tags': 250}
    fields = {k: v.encode('utf-8')[:limits[k]].decode('utf-8', errors='ignore')
              for k, v in meta['fields'].items() if k in limits}
    return {'id': identifier, 'fields': fields,
            'truncated': bool(meta.get('truncated_fields')) or fields != meta['fields']}


def connect(path, *, readonly=False):
    db = sqlite3.connect(Path(path).resolve().as_uri() + ('?mode=ro' if readonly else '?mode=rwc'),
                         uri=True, timeout=.5)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA busy_timeout=500')
    db.execute('PRAGMA cache_size=-4096')
    if readonly:
        db.execute('PRAGMA query_only=ON')
    return db


def lookup(path, catalogue, ids):
    """Missing store, incomplete items, and changed metadata immediately use originals."""
    validate_ids(ids)
    result = {i: missing(i) for i in ids}
    if path is None or not Path(path).is_file():
        return {'items': list(result.values())}
    try:
        with closing(connect(path, readonly=True)) as db, closing(connect(catalogue, readonly=True)) as source:
            has_titles = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='dataset_titles'").fetchone()
            has_briefs = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='dataset_briefs'").fetchone()
            for identifier in ids:
                original = source.execute('SELECT id,title,description,source_id,metadata FROM datasets WHERE id=?',
                                          (identifier,)).fetchone()
                if not original:
                    continue
                if has_briefs:
                    from .brief_store import lookup_brief
                    brief = lookup_brief(db, original)
                    if brief:
                        result[identifier] = brief
                        continue
                if has_titles:
                    from .title_store import lookup_title
                    translated = lookup_title(db, identifier, original['title'])
                    if translated:
                        result[identifier] = translated
                        continue
                meta = metadata(original)
                keys = [content_key(meta, version=v) for v in READ_VERSIONS]
                mapping = db.execute('SELECT key FROM documents WHERE id=?', (identifier,)).fetchone()
                if not mapping or mapping['key'] not in keys:
                    continue
                for key in keys:
                    row = db.execute('SELECT status,result FROM contents WHERE key=?', (key,)).fetchone()
                    if not row:
                        continue
                    if row['status'] == 'unavailable':
                        break  # An explicit current rejection overrides older copy.
                    if row['status'] == 'ready':
                        try:
                            value = json.loads(row['result'])
                            value['id'] = identifier
                            result[identifier] = validate_rows({'items': [value]}, [identifier])['items'][0]
                        except (ValueError, TypeError, KeyError):
                            pass  # Invalid stored text is not a translated result.
                        break
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return {'items': [missing(i) for i in ids]}
    return {'items': [result[i] for i in ids]}


class IntroStore:
    def __init__(self, path):
        self.responses = Path(str(path)+'.responses')
        self.db = connect(path)
        # Offline writers may briefly overlap. Public reads keep their .5s cap.
        self.db.execute('PRAGMA busy_timeout=5000')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS state(name TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS contents(seq INTEGER PRIMARY KEY,key TEXT UNIQUE NOT NULL,
              representative TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',result TEXT,
              priority INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS intro_pending ON contents(status,priority DESC,seq);
            CREATE TABLE IF NOT EXISTS documents(id TEXT PRIMARY KEY,key TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS intro_document_content ON documents(key);
            CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY,started REAL NOT NULL,
              keys_json TEXT NOT NULL,status TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '');
        ''')

    def close(self):
        self.db.close()

    def get_state(self, name, default=None):
        row = self.db.execute('SELECT value FROM state WHERE name=?', (name,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_state(self, name, value):
        self.db.execute('INSERT OR REPLACE INTO state VALUES(?,?)', (name, json.dumps(value)))

    def ingest(self, rows, *, cursor=None, priority=0):
        # Parsing/hashing must not hold SQLite's single writer while CPU-limited.
        prepared = []
        for row in rows:
            meta = metadata(row)
            key = content_key(meta)
            # Preserve valid, exact-source v2 results; never regenerate all ready
            # records merely because the caption instructions changed.
            current = self.db.execute('SELECT status FROM contents WHERE key=?', (key,)).fetchone()
            if not current or current['status'] == 'pending':
                for version in READ_VERSIONS[1:]:
                    legacy_key = content_key(meta, version=version)
                    legacy = self.db.execute('SELECT status,result FROM contents WHERE key=?', (legacy_key,)).fetchone()
                    if legacy:
                        if legacy['status'] in {'reserved', 'uncertain', 'unavailable'}:
                            # A version change cannot authorize another billed
                            # attempt. Preserve its ledger/receipt for recovery.
                            key = legacy_key
                            break
                        if legacy['status'] != 'ready':
                            continue
                        try:
                            value = json.loads(legacy['result'])
                            validate_rows({'items': [value]}, [value['id']])
                            if value['available']:
                                key = legacy_key
                                break
                        except (ValueError, TypeError, KeyError):
                            pass
            prepared.append((row['id'], key, bool(meta['fields'].get('title'))))
        with self.db:
            for identifier, key, available in prepared:
                self.db.execute('INSERT OR IGNORE INTO contents(key,representative,priority,status) VALUES(?,?,?,?)',
                                (key, identifier, priority, 'pending' if available else 'unavailable'))
                self.db.execute('INSERT OR REPLACE INTO documents VALUES(?,?)', (identifier, key))
            if cursor is not None:
                self.set_state('cursor', cursor)

    def recover(self):
        attempts = self.db.execute("SELECT id,keys_json,detail FROM attempts WHERE status='reserved'").fetchall()
        for attempt in attempts:
            path = self.responses / (str(attempt['id'])+'.json')
            if path.is_file():
                try:
                    with path.open('rb') as stream: raw = stream.read(65537)
                    if len(raw) > 65536: raise ValueError('response limit')
                    saved = json.loads(raw)
                    rows = saved['rows']
                    if (set(saved) != {'attempt','rows','value'} or saved['attempt'] != attempt['id'] or
                            [r['key'] for r in rows] != json.loads(attempt['keys_json']) or
                            rows != json.loads(attempt['detail'])):
                        raise ValueError('unbound response')
                    self.finish(attempt['id'], rows, saved['value'], _write_receipt=False)
                except (ValueError, TypeError, KeyError):
                    pass  # An invalid receipt cannot authorize content or rebilling.
        # A crash might have happened after the upstream call. Never silently rebill.
        with self.db:
            self.db.execute("UPDATE contents SET status='uncertain' WHERE status='reserved'")
            self.db.execute("UPDATE attempts SET status='uncertain',detail='worker interrupted' WHERE status='reserved'")

    def reserve(self, max_attempts):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            if self.db.execute('SELECT count(*) FROM attempts').fetchone()[0] >= max_attempts:
                return None, []
            rows = self.db.execute("""SELECT c.seq,c.key,
                (SELECT d.id FROM documents d WHERE d.key=c.key ORDER BY d.id LIMIT 1) AS representative
                FROM contents c WHERE c.status='pending' AND EXISTS(SELECT 1 FROM documents d WHERE d.key=c.key)
                ORDER BY c.priority DESC,c.seq LIMIT 5""").fetchall()
            if not rows:
                return None, []
            keys = [r['key'] for r in rows]
            bindings = [{'key': r['key'], 'representative': r['representative']} for r in rows]
            attempt = self.db.execute("INSERT INTO attempts(started,keys_json,status,detail) VALUES(?,?,'reserved',?)",
                                      (time.time(), json.dumps(keys), json.dumps(bindings))).lastrowid
            self.db.executemany("UPDATE contents SET status='reserved' WHERE key=?", [(k,) for k in keys])
            return attempt, rows

    @staticmethod
    def _sync_directory(directory):
        if os.name != 'nt':  # Windows lacks directory fsync; production is Linux.
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)

    def _receipt(self, attempt, rows, value):
        self.responses.mkdir(mode=0o700, exist_ok=True)
        self._sync_directory(self.responses.parent)
        path = self.responses / (str(attempt)+'.json')
        raw = json.dumps({'attempt': attempt, 'rows': [{'key':r['key'],'representative':r['representative']} for r in rows],
                          'value':value}, ensure_ascii=False).encode()
        if len(raw) > 65536: raise ValueError('response limit')
        temp = path.with_suffix('.tmp')
        with temp.open('wb') as stream:
            os.chmod(temp, 0o600)
            stream.write(raw);stream.flush();os.fsync(stream.fileno())
        os.replace(temp, path)
        self._sync_directory(self.responses)

    def finish(self, attempt, rows, value, *, _write_receipt=True):
        validate_rows(value, [r['representative'] for r in rows])
        results = {r['id']: r for r in value['items']}
        saved = self.db.execute('SELECT status,keys_json,detail FROM attempts WHERE id=?', (attempt,)).fetchone()
        bindings = [{'key': r['key'], 'representative': r['representative']} for r in rows]
        if (not saved or saved['status'] != 'reserved' or json.loads(saved['keys_json']) != [r['key'] for r in rows] or
                not saved['detail'] or json.loads(saved['detail']) != bindings):
            raise ValueError('attempt is not owned')
        if _write_receipt:
            self._receipt(attempt, rows, value)
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            saved = self.db.execute('SELECT status,keys_json FROM attempts WHERE id=?', (attempt,)).fetchone()
            if not saved or saved['status'] != 'reserved' or json.loads(saved['keys_json']) != [r['key'] for r in rows]:
                raise ValueError('attempt is not owned')
            for row in rows:
                result = results[row['representative']]
                self.db.execute('UPDATE contents SET status=?,result=? WHERE key=?',
                                ('ready' if result['available'] else 'unavailable', json.dumps(result, ensure_ascii=False), row['key']))
            self.db.execute("UPDATE attempts SET status='complete' WHERE id=?", (attempt,))
        try:
            (self.responses / (str(attempt)+'.json')).unlink(missing_ok=True)
            if self.responses.exists(): self._sync_directory(self.responses)
        except OSError:
            pass  # DB is complete; a cleanup error cannot change the attempt.

    def fail(self, attempt, rows, detail, *, not_started=False):
        with self.db:
            status = 'pending' if not_started else 'uncertain'
            self.db.executemany('UPDATE contents SET status=? WHERE key=?', [(status, r['key']) for r in rows])
            self.db.execute('UPDATE attempts SET status=?,detail=? WHERE id=?',
                            ('deferred' if not_started else 'uncertain', detail[:200], attempt))

    def status(self):
        return {'scanned': self.db.execute('SELECT count(*) FROM documents').fetchone()[0],
                'contents': dict(self.db.execute('SELECT status,count(*) FROM contents GROUP BY status')),
                'attempts': dict(self.db.execute('SELECT status,count(*) FROM attempts GROUP BY status')),
                'cursor': self.get_state('cursor', 0), 'inventory_complete': self.get_state('inventory_complete', False),
                'generation': self.get_state('generation', {}),
                'ready_documents': self.db.execute("SELECT count(*) FROM documents d JOIN contents c ON c.key=d.key WHERE c.status='ready'").fetchone()[0]}
