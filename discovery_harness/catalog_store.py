"""Persistent, disposable read index for the existing catalog's query semantics.

The source DB stays read-only. Only a completed, source-matched index is published.
The existing catalog still renders records, graphs, ordering and year definitions.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from itertools import combinations
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from .errors import BudgetExceeded, DeadlineExceeded


VERSION = 3
# A dense reference table adds at most 32MiB + 4MiB on the supported 64-bit
# runtime. Sparse/large rowids retain the existing bounded-deadline SQL path.
MAX_LOOKUP_ROW = 4 * 1024**2
MAX_FAST_CANDIDATES = 250000


class _Triples(dict):
    def __missing__(self, codepoint):
        value = chr(codepoint) * 3
        self[codepoint] = value
        return value


TRIPLES = _Triples()


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


class PreparedCatalog:
    def __init__(self, catalog, database, directory, observer, expired, *, max_bytes=12 * 1024**3):
        self.catalog, self.database = catalog, Path(database)
        self.directory = Path(directory)
        self.path = self.directory / 'catalog-index.sqlite3'
        self.observer, self.expired, self.max_bytes = observer, expired, max_bytes
        self.preparing = False
        self.query_stats = {}
        self._row_records = self._row_original = None
        self._index = self.connection = self.signature = self.version = self.meta = None
        self.original_matching = catalog.matching_records
        code = Path(catalog.__file__).read_bytes()
        registry = Path(catalog.__file__).with_name('registry.py')
        if registry.exists():
            code += registry.read_bytes()
        self.code_hash = hashlib.sha256(code).hexdigest()

    def check(self):
        if self.expired():
            raise DeadlineExceeded('catalog index operation deadline exceeded')

    def _signature(self):
        stat = self.database.stat()
        wal = self.database.with_name(self.database.name + '-wal')
        try:
            ws = wal.stat()
            journal = [ws.st_ino, ws.st_size, ws.st_mtime_ns] if ws.st_size else None
        except FileNotFoundError:
            journal = None
        return {'format': VERSION, 'source': [stat.st_ino, stat.st_size, stat.st_mtime_ns],
                'wal': journal, 'code': self.code_hash}

    def _connect(self, path, *, readonly=True):
        db = sqlite3.connect(path.as_uri() + ('?mode=ro' if readonly else '?mode=rwc'), uri=True, timeout=.25)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA cache_size=-32768')
        db.set_progress_handler(lambda: int(self.expired()), 1000)
        return db

    def _metadata(self):
        if not self.path.is_file() or self.path.is_symlink() or self.path.stat().st_size > self.max_bytes:
            return None
        db = None
        try:
            db = self._connect(self.path)
            row = db.execute('SELECT value FROM metadata WHERE key=?', ('manifest',)).fetchone()
            if row is None or len(row[0]) > 1024**2:
                return None
            return json.loads(row[0])
        except (sqlite3.DatabaseError, ValueError):
            self.check()
            return None
        finally:
            if db is not None:
                db.close()

    def _long_search(self, target):
        # Positions prove exact substring order without fetching large descriptions.
        # Keep IDs compact so returning matches does not page through those texts.
        target.executescript('''
            DROP TABLE IF EXISTS vocabulary_long;
            DROP TABLE IF EXISTS search_long;
            CREATE TABLE search_ids(n INTEGER PRIMARY KEY,id TEXT);
            INSERT INTO search_ids SELECT n,id FROM search_text;
            CREATE VIRTUAL TABLE search_long USING fts5(text, content='', detail='full',
                columnsize=0, tokenize='trigram case_sensitive 1');
            CREATE VIRTUAL TABLE vocabulary_long USING fts5vocab(search_long,'row');
        ''')
        rows = target.execute('SELECT n,text FROM search_text WHERE safe=1')
        count, last = 0, time.monotonic()
        while batch := rows.fetchmany(1000):
            self.check()
            target.executemany('INSERT INTO search_long(rowid,text) VALUES(?,?)', batch)
            target.commit()
            count += len(batch)
            if time.monotonic() - last >= 10:
                print(_json({'catalog_index': 'long_search', 'records': count}), flush=True)
                last = time.monotonic()

    def _publish(self, temporary):
        self.check()
        with temporary.open('r+b') as stream:
            os.fsync(stream.fileno())
        temporary.replace(self.path)
        if os.name == 'posix':
            directory = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def _upgrade(self, meta, signature, version):
        # Older full-size indexes already have exact source projections.
        # Extend a bounded private copy, preserving the published file on failure.
        temporary = self.directory / '.catalog-index.building.sqlite3'
        if temporary.is_symlink():
            raise ValueError('derived index path must not be a symlink')
        temporary.unlink(missing_ok=True)
        target = None
        try:
            with self.path.open('rb') as source, temporary.open('xb') as output:
                copied = 0
                while chunk := source.read(1024**2):
                    self.check()
                    copied += len(chunk)
                    if copied > self.max_bytes:
                        raise BudgetExceeded('catalog_index_bytes', self.max_bytes)
                    output.write(chunk)
            target = self._connect(temporary, readonly=False)
            target.execute('PRAGMA journal_mode=OFF')
            target.execute('PRAGMA synchronous=OFF')
            target.execute(f'PRAGMA max_page_count={self.max_bytes // 4096}')
            self._long_search(target)
            if self._signature() != signature or self.observer.execute('PRAGMA data_version').fetchone()[0] != version:
                raise RuntimeError('catalog changed during index preparation')
            meta = {**meta, 'signature': signature, 'prepared_at': self.catalog.now()}
            target.execute('UPDATE metadata SET value=? WHERE key=?', (_json(meta), 'manifest'))
            target.commit()
            target.close()
            target = None
            self._publish(temporary)
            print(_json({'catalog_index': 'upgraded', 'records': meta['records'],
                         'bytes': self.path.stat().st_size}), flush=True)
        except sqlite3.DatabaseError as error:
            self.check()
            if getattr(error, 'sqlite_errorcode', None) == sqlite3.SQLITE_FULL:
                raise BudgetExceeded('catalog_index_bytes', self.max_bytes) from error
            raise
        finally:
            if target is not None:
                target.close()
            temporary.unlink(missing_ok=True)

    def _build(self, source, signature, version):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.directory / '.catalog-index.building.sqlite3'
        if temporary.is_symlink():
            raise ValueError('derived index path must not be a symlink')
        temporary.unlink(missing_ok=True)
        target = self._connect(temporary, readonly=False)
        started = last = time.monotonic()
        stored, unique, statuses = Counter(), Counter(), Counter()
        try:
            # Unpublished scratch DB only; a killed builder is discarded on retry.
            target.execute('PRAGMA journal_mode=OFF')
            target.execute('PRAGMA synchronous=OFF')
            target.execute('PRAGMA temp_store=MEMORY')
            target.execute(f'PRAGMA max_page_count={self.max_bytes // 4096}')
            target.executescript('''
                CREATE TABLE entries(n INTEGER PRIMARY KEY, id TEXT, source TEXT, title TEXT,
                    topics TEXT, years TEXT, proposed INTEGER, original INTEGER);
                CREATE TABLE groups(source TEXT, canonical TEXT, n INTEGER, id TEXT, title TEXT,
                    original INTEGER, PRIMARY KEY(source,canonical)) WITHOUT ROWID;
                CREATE TABLE search_text(n INTEGER PRIMARY KEY, id TEXT, text TEXT, safe INTEGER);
                CREATE VIRTUAL TABLE search_fts USING fts5(text, content='', detail='none',
                    columnsize=0, tokenize='trigram case_sensitive 1');
                CREATE VIRTUAL TABLE vocabulary USING fts5vocab(search_fts,'row');
                CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT);
            ''')
            rows = source.execute('''SELECT rowid AS n,id,source_id,title,description,mappings,
                json_extract(metadata,'$.duplicate_of') AS duplicate_of,
                json_extract(metadata,'$.period') AS period,
                json_extract(metadata,'$.source_modified') AS source_modified,
                lower(title||' '||description||' '||json_extract(metadata,'$.publisher')
                    ||' '||json_extract(metadata,'$.region')) AS search_text FROM datasets''')
            count = 0
            while batch := rows.fetchmany(1000):
                self.check()
                entries, groups, texts, terms = [], [], [], []
                for row in batch:
                    mappings = json.loads(row['mappings'])
                    topics = sorted({m['concept_id'] for m in mappings if m['status'] != 'rejected'})
                    years = self.catalog.year_fields({'period': row['period'], 'source_modified': row['source_modified']},
                                                    row['title'], row['description'])['reference_years']
                    original = row['duplicate_of'] is None
                    canonical = row['duplicate_of'] or row['id']
                    if not isinstance(canonical, str):
                        raise ValueError('catalog canonical identifier must be text')
                    entries.append((row['n'], row['id'], row['source_id'], row['title'], _json(topics),
                                    _json(years), any(m['status'] == 'proposed' for m in mappings), original))
                    groups.append((row['source_id'], canonical, row['n'], row['id'], row['title'], original))
                    text = row['search_text']
                    safe = text is not None and '\x00' not in text
                    texts.append((row['n'], row['id'], text, safe))
                    # Tripling makes even one character a trigram. MATCH only selects
                    # candidates; SQLite instr() below preserves exact substring semantics.
                    if safe:
                        terms.append((row['n'], text.translate(TRIPLES)))
                    stored[row['source_id']] += 1
                    if original:
                        unique[row['source_id']] += 1
                        statuses.update(m['status'] for m in mappings)
                target.executemany('INSERT INTO entries VALUES(?,?,?,?,?,?,?,?)', entries)
                target.executemany('''INSERT INTO groups VALUES(?,?,?,?,?,?)
                    ON CONFLICT(source,canonical) DO UPDATE SET n=excluded.n,id=excluded.id,
                    title=excluded.title,original=excluded.original WHERE excluded.original
                    OR (groups.id!=excluded.canonical AND
                        (excluded.title,excluded.id)<(groups.title,groups.id))''', groups)
                target.executemany('INSERT INTO search_text VALUES(?,?,?,?)', texts)
                target.executemany('INSERT INTO search_fts(rowid,text) VALUES(?,?)', terms)
                target.commit()
                count += len(batch)
                if time.monotonic() - last >= 10:
                    print(_json({'catalog_index': 'building', 'records': count,
                                 'bytes': temporary.stat().st_size,
                                 'seconds': round(time.monotonic() - started, 1)}), flush=True)
                    last = time.monotonic()
            target.executescript('''
                CREATE INDEX selected_records ON groups(n);
                CREATE TABLE records(ordinal INTEGER PRIMARY KEY,n INTEGER,id TEXT,source TEXT,title TEXT,
                    topics TEXT,years TEXT,proposed INTEGER,original INTEGER,selected INTEGER);
                INSERT INTO records(n,id,source,title,topics,years,proposed,original,selected)
                    SELECT e.*,EXISTS(SELECT 1 FROM groups g WHERE g.n=e.n) FROM entries e
                    WHERE e.original OR EXISTS(SELECT 1 FROM groups g WHERE g.n=e.n)
                    ORDER BY e.source,e.title,e.id;
                DROP TABLE entries;
                DROP TABLE groups;
                CREATE INDEX unsafe_text ON search_text(safe) WHERE safe=0;
            ''')
            self._long_search(target)
            self.check()
            if self._signature() != signature or self.observer.execute('PRAGMA data_version').fetchone()[0] != version:
                raise RuntimeError('catalog changed during index preparation')
            meta = {'signature': signature, 'stored': dict(stored), 'unique': dict(unique),
                    'statuses': dict(statuses), 'records': count, 'prepared_at': self.catalog.now()}
            target.execute('INSERT INTO metadata VALUES(?,?)', ('manifest', _json(meta)))
            target.commit()
            target.close()
            target = None
            self._publish(temporary)
            print(_json({'catalog_index': 'published', 'records': count, 'bytes': self.path.stat().st_size,
                         'seconds': round(time.monotonic() - started, 1)}), flush=True)
        except sqlite3.DatabaseError as error:
            self.check()
            if getattr(error, 'sqlite_errorcode', None) == sqlite3.SQLITE_FULL:
                raise BudgetExceeded('catalog_index_bytes', self.max_bytes) from error
            raise
        finally:
            if target is not None:
                target.close()
            temporary.unlink(missing_ok=True)

    def index(self, source):
        self.check()
        signature = self._signature()
        version = self.observer.execute('PRAGMA data_version').fetchone()[0]
        if self._index is not None and self.signature == signature and self.version == version:
            return self._index
        if not self.preparing:
            raise RuntimeError('catalog index refresh is required')
        self.close()
        meta = self._metadata()
        previous = meta.get('signature', {}) if meta else {}
        if previous.get('format') in (1, 2) and {**previous, 'format': VERSION} == signature:
            self._upgrade(meta, signature, version)
            meta = self._metadata()
        elif meta is None or meta.get('signature') != signature:
            self._build(source, signature, version)
            meta = self._metadata()
        if meta is None or meta.get('signature') != signature:
            raise RuntimeError('prepared catalog metadata is invalid')
        self.connection = self._connect(self.path)
        maximum = self.connection.execute('SELECT max(n) FROM search_text').fetchone()[0] or 0
        minimum = self.connection.execute('SELECT min(n) FROM search_text').fetchone()[0] or 0
        if minimum >= 0 and maximum <= MAX_LOOKUP_ROW and maximum <= 2 * sum(meta['stored'].values()) + 1:
            self._row_records = [None] * (maximum + 1)
            self._row_original = bytearray(maximum + 1)
        records, topics, sources, years = [], defaultdict(list), defaultdict(list), defaultdict(list)
        overlaps = Counter()
        source_names = {s: s for s in meta['stored']}
        topic_names = {c['id']: c['id'] for c in self.catalog.CONCEPTS}
        for number, row in enumerate(self.connection.execute('''SELECT n,id,source,title,topics,years,
                proposed,original,selected FROM records ORDER BY ordinal''')):
            if number % 4096 == 0:
                self.check()
            item = {'id': row['id'], 'source': source_names.get(row['source'], row['source']), 'title': row['title'],
                    'topics': tuple(topic_names.get(t, t) for t in json.loads(row['topics'])),
                    'proposed': bool(row['proposed'])}
            selected_years = tuple(json.loads(row['years']))
            if selected_years:
                item['years'] = selected_years
            if self._row_records is not None:
                self._row_records[row['n']] = item
                self._row_original[row['n']] = bool(row['original'])
            if row['selected']:
                sources[item['source']].append(item)
            if not row['original']:
                continue
            records.append(item)
            for topic in item['topics'] or ('__unlinked',):
                topics[topic].append(item)
            for year in selected_years:
                years[str(year)].append(item)
            overlaps.update(combinations(item['topics'], 2))
        self.meta, self.signature, self.version = meta, signature, version
        self._index = {'records': records, 'topics': topics, 'sources': dict(sources),
                       'statuses': Counter(meta['statuses']), 'overlaps': overlaps, 'years': years,
                       'year_counts': {y: len(r) for y, r in years.items()}, 'stored_counts': meta['stored'],
                       'searches': {}}
        return self._index

    def close(self):
        self._index = None
        self._row_records = self._row_original = None
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def matching_records(self, source, index, query):
        self.query_stats = {'phase': 'selection'}
        started = time.monotonic()
        self._phase_started = started
        try:
            result = self._matching_records(source, index, query)
            self.query_stats['matched'] = len(result)
            self._query_phase('complete')
            return result
        finally:
            self.query_stats[self.query_stats['phase']+'_seconds'] = round(time.monotonic()-self._phase_started, 4)
            self.query_stats['matching_seconds'] = round(time.monotonic()-started, 4)

    def _query_phase(self, name):
        now = time.monotonic()
        self.query_stats[self.query_stats['phase']+'_seconds'] = round(now-self._phase_started, 4)
        self.query_stats['phase'] = name
        self._phase_started = now

    def _matching_records(self, source, index, query):
        records = self.original_matching(source, index, {**query, 'q': ''})
        self.query_stats['candidates'] = len(records)
        q = query.get('q', '').strip().lower()[:200]
        if not q:
            return records
        self.check()
        self._query_phase('fts')
        connection = self.connection
        direct = self._row_records is not None
        column = 'n' if direct else 'id'
        if '\x00' in q:
            rows = connection.execute(f'SELECT {column} FROM search_text WHERE instr(text,?)>0', (q,))
        else:
            if len(q) >= 3:
                table, term, exact = 'search_long', q, True
            else:
                table = 'search_fts'
                term = q[0]*3 if len(q) == 1 or q[0] == q[1] else q[0]*2+q[1]
                # xxx means presence of x; xxy means adjacent x,y when x != y.
                # Repeated pairs still need the original substring check.
                exact = len(q) == 1 or q[0] != q[1]
            expression = '"' + term.replace('"', '""') + '"'
            if direct and exact:
                # FTS already knows the source rowids. Reuse the RAM record
                # references instead of random B-tree reads for every string ID.
                rows = connection.execute(f'''SELECT rowid FROM {table} WHERE {table} MATCH ?
                    UNION ALL SELECT n FROM search_text WHERE safe=0 AND instr(text,?)>0''', (expression,q))
            else:
                lookup = 'search_ids' if exact else 'search_text'
                condition = '' if exact else ' AND instr(s.text,?)>0'
                parameters = (expression, q) if exact else (expression, q, q)
                rows = connection.execute(f'''SELECT s.{column} FROM {table}
                    JOIN {lookup} s ON s.n={table}.rowid WHERE {table} MATCH ?{condition}
                    UNION ALL SELECT {column} FROM search_text WHERE safe=0 AND instr(text,?)>0''', parameters)
        ids = set()
        fast = direct and not any(query.get(k) for k in ('source','concept','year','status'))
        hits = []
        for n, row in enumerate(rows):
            if n % 4096 == 0:
                self.check()
            if direct:
                item = self._row_records[row[0]]
                # Unselected duplicates cannot occur in any catalog view.
                if item is None:
                    continue
                ids.add(item['id'])
                if fast and self._row_original[row[0]]:
                    hits.append(item)
                    if len(hits) > MAX_FAST_CANDIDATES:
                        fast = False
                        hits.clear()
            else:
                ids.add(row[0])
        self.query_stats['fts_ids'] = len(ids)
        self._query_phase('filter')
        topic_ids = {c['id'] for c in self.catalog.CONCEPTS
                     if q in c['name'].lower() or any(q == t.lower() for t in c['terms'])}
        estimate = len(hits) + sum(len(index['topics'].get(topic, ())) for topic in topic_ids)
        if fast and estimate <= MAX_FAST_CANDIDATES:
            selected = {record['id']: record for record in hits}
            for topic in topic_ids:
                for n, record in enumerate(index['topics'].get(topic, ())):
                    if n % 4096 == 0:
                        self.check()
                    selected[record['id']] = record
            # This is the native global ORDER BY source,title,id. Candidate count
            # is capped so sort scratch space stays within the request reservation.
            matched = sorted(selected.values(), key=lambda r:(r['source'],r['title'],r['id']))
            self.check()
            self.query_stats.update(strategy='indexed_candidates', examined=estimate)
            return matched
        self.query_stats.update(strategy='filtered_scan', examined=len(records))
        matched = []
        for n, record in enumerate(records):
            if n % 4096 == 0:
                self.check()
            if record['id'] in ids or not topic_ids.isdisjoint(record['topics']):
                matched.append(record)
        # No unbounded per-query ID-set cache; the persistent SQLite index is reused.
        return matched

    def acquisition(self, audit):
        with self.catalog.database() as source:
            self.index(source)  # Reject an index built from an older source generation.
            sources = [json.loads(row[0]) for row in source.execute('SELECT info FROM sources')]
        root = self.database.parent / 'collection-audit'

        def report(path):
            with path.open('rb') as stream:
                body = stream.read(1024**2 + 1)
            if len(body) > 1024**2:
                raise BudgetExceeded('acquisition_report_bytes', 1024**2)
            return json.loads(body)

        output = []
        implemented = audit.collectors()
        for source in sources:
            self.check()
            id = source['id']
            if not re.fullmatch(r'[A-Za-z0-9_-]+', id):
                raise ValueError('invalid source identifier')
            raw, unique = self.meta['stored'].get(id, 0), self.meta['unique'].get(id, 0)
            reports = {}
            for suffix in ('', '-independent', '-live', '-linked-repair', '-recovery'):
                path = root / (id + suffix + '.json')
                if path.exists():
                    reports[(suffix or 'collection').lstrip('-')] = report(path)
            supported = id in implemented or id == 'singapore'
            output.append({'id': id, 'name': source['name'], 'country': source['country'],
                           'registered_url': source['url'], 'stored_source_records': raw,
                           'searchable_records': unique, 'duplicate_records': raw-unique,
                           'collector_implemented': supported, 'reports': reports,
                           'status': 'not_implemented' if not supported else 'not_audited' if not reports else 'see_reports'})
        return {'checked_at': self.catalog.now(),
                'stored_source_records': sum(s['stored_source_records'] for s in output),
                'searchable_records': sum(s['searchable_records'] for s in output), 'sources': output,
                'duplicate_audit': report(root / 'duplicates.json') if (root / 'duplicates.json').exists() else None,
                'definitions': {
                    'stored_source_records': '출처별 원본 메타데이터 기록 수. 같은 자료의 재게시가 포함될 수 있음.',
                    'searchable_records': '근거가 확인된 제공 경로 중복을 제외한 탐색 기록 수. 전 세계 고유 자료 수라는 뜻은 아님.',
                    'count_reconciled': '명시된 범위의 제공처 표시 수와 수신 고유 식별자 수를 대조함.',
                    'unreconciled': '아직 차이가 남음. 완료로 취급하지 않음.',
                    'not_implemented': '사이트 정보만 등록되어 있고 직접 목록 수집은 구현되지 않음.'}}
