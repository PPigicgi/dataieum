"""Prepare locally; run OpenAI Batch only with the explicit run command and API key."""
import argparse
import array
import base64
import fcntl
import hashlib
import html
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
import uuid

MODEL = 'text-embedding-3-small'
DIMENSIONS = 1536
MAX_TOKENS = 8191
TAG = re.compile(r'</?[A-Za-z][^>]*>')
EMPTY = {'no notes provided', 'undescribed', 'untagged'}
VERSION = 'four-fields-v1'


class APIError(RuntimeError):
    def __init__(self, code, method, path):
        self.code = code
        super().__init__(f'OpenAI HTTP {code} on {method} {path}')


def clean(value):
    return re.sub(r'\s+', ' ', html.unescape(TAG.sub(' ', str(value or '')))).strip()


def unique(values):
    return list(dict.fromkeys(v for v in values if v))


def input_text(title, description, metadata, source):
    title, description = clean(title), clean(description)
    tags = unique(clean(s.get('label', '') if isinstance(s, dict) else str(s))
                  for s in (metadata.get('subjects') or []))
    tags = [t for t in tags if t.casefold() not in EMPTY]
    if description.casefold() in EMPTY or description == title:
        description = ''
    paths = metadata.get('native_catalog_paths') or []
    if not isinstance(paths, list):
        paths = [paths]
    paths = unique(clean(v) for v in [metadata.get('native_catalog_path', '')] + paths)
    surveys = unique(clean(metadata.get(k)) for k in (
        'survey_name', 'survey_title', 'native_survey_name', 'statistical_survey_name'))
    if source == 'kosis':
        surveys += re.findall('「([^」]+)」', str(metadata.get('publisher') or ''))
    context = unique(paths + [s for s in unique(surveys) if not any(s in p for p in paths)])
    text = 'Title: ' + title + '\nTags: ' + ', '.join(tags) + '\nDescription: ' + description
    if context:
        text += '\nContext: ' + ' | '.join(context)
    return title, text


def connect(root):
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / 'embeddings.sqlite3', timeout=60)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS embeddings (
        dataset_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, input_hash TEXT NOT NULL,
        input TEXT, tokens INTEGER NOT NULL, exclusion TEXT,
        batch_id INTEGER, request_id TEXT, position INTEGER, vector BLOB, error TEXT);
      CREATE INDEX IF NOT EXISTS embedding_pending ON embeddings(batch_id)
        WHERE exclusion IS NULL AND vector IS NULL;
      CREATE INDEX IF NOT EXISTS embedding_request ON embeddings(batch_id,request_id);
      CREATE TABLE IF NOT EXISTS batches (
        id INTEGER PRIMARY KEY, marker TEXT UNIQUE NOT NULL, status TEXT NOT NULL,
        tokens INTEGER NOT NULL, count INTEGER NOT NULL, file_id TEXT,
        remote_id TEXT, output_file_id TEXT, error_file_id TEXT, error TEXT);
    ''')
    config = json.dumps({'model': MODEL, 'dimensions': DIMENSIONS, 'version': VERSION})
    prior = get_state(db, 'config')
    if prior and prior != config:
        raise RuntimeError('Model/input configuration differs from existing store')
    db.execute('INSERT OR IGNORE INTO state VALUES (?,?)', ('config', config))
    db.commit()
    return db


def get_state(db, key, default=None):
    row = db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
    return row[0] if row else default


def set_state(db, key, value):
    db.execute('INSERT INTO state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
               (key, str(value)))


def prepare(db, catalog):
    import tiktoken
    if get_state(db, 'prepared') == '1':
        return status(db)
    if db.execute('SELECT count(*) FROM batches').fetchone()[0]:
        raise RuntimeError('Cannot change inputs after submission')
    enc = tiktoken.encoding_for_model(MODEL)
    source = sqlite3.connect(Path(catalog).resolve().as_uri() + '?mode=ro', uri=True)
    source.execute('BEGIN')
    signature = json.dumps(source.execute('SELECT count(*),max(rowid),max(checked_at) FROM datasets').fetchone())
    prior = get_state(db, 'source_signature')
    if prior and prior != signature:
        raise RuntimeError('Catalog changed since preparation began; retain snapshot or use a new run directory')
    set_state(db, 'source_signature', signature)
    db.commit()
    checkpoint = int(get_state(db, 'last_rowid', 0))
    count = 0
    for rid, ident, sid, title, desc, raw in source.execute(
            'SELECT rowid,id,source_id,title,description,metadata FROM datasets WHERE rowid>? ORDER BY rowid',
            (checkpoint,)):
        title, text = input_text(title, desc, json.loads(raw), sid)
        tokens = len(enc.encode_ordinary(text))
        reasons = []
        if not title or title.casefold() in {'test', '((name))'}:
            reasons.append('placeholder_title')
        if tokens > MAX_TOKENS:
            reasons.append('over8191')
        db.execute('INSERT INTO embeddings(dataset_id,source_id,input_hash,input,tokens,exclusion) VALUES (?,?,?,?,?,?)',
                   (ident, sid, hashlib.sha256(text.encode()).hexdigest(),
                    None if reasons else text, tokens, ','.join(reasons) or None))
        set_state(db, 'last_rowid', rid)
        count += 1
        if count % 2000 == 0:
            db.commit()
        if count % 250000 == 0:
            print(json.dumps({'prepared_this_run': count, 'last_rowid': rid}), flush=True)
    set_state(db, 'prepared', 1)
    db.commit()
    source.close()
    return status(db)


def status(db):
    totals = dict(db.execute('''SELECT count(*) AS records, sum(tokens) AS all_tokens,
        sum(exclusion IS NULL) AS eligible, sum(CASE WHEN exclusion IS NULL THEN tokens ELSE 0 END) AS eligible_tokens,
        sum(vector IS NOT NULL) AS completed FROM embeddings''').fetchone())
    totals['exclusions'] = {r[0]: r[1] for r in db.execute(
        'SELECT exclusion,count(*) FROM embeddings WHERE exclusion IS NOT NULL GROUP BY exclusion')}
    totals['prepared'] = get_state(db, 'prepared') == '1'
    totals['batch_estimate_usd'] = (totals['eligible_tokens'] or 0) * .01 / 1e6
    totals['batches'] = {r[0]: r[1] for r in db.execute('SELECT status,count(*) FROM batches GROUP BY status')}
    return totals


class API:
    def __init__(self):
        self.key = os.environ.get('OPENAI_API_KEY', '').strip()
        if not self.key:
            raise RuntimeError('OPENAI_API_KEY is not configured; no API request sent')

    def request(self, method, path, body=None, content_type='application/json', stream=False):
        headers = {'Authorization': 'Bearer ' + self.key, 'Content-Type': content_type}
        data = json.dumps(body).encode() if body is not None and not isinstance(body, bytes) else body
        req = urllib.request.Request('https://api.openai.com/v1' + path, data=data, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as exc:
            # Never print credentials or server-echoed input text.
            if method == 'DELETE' and exc.code == 404:
                return {'deleted': True}
            raise APIError(exc.code, method, path) from None
        if stream:
            return response
        with response:
            return json.load(response)

    def upload(self, path):
        boundary = 'wanted-' + uuid.uuid4().hex
        body = (f'--{boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n'
                f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{path.name}"\r\n'
                'Content-Type: application/jsonl\r\n\r\n').encode()
        body += path.read_bytes() + f'\r\n--{boundary}--\r\n'.encode()
        return self.request('POST', '/files', body, 'multipart/form-data; boundary=' + boundary)['id']


def make_batch(db, token_limit):
    rows, total = [], 0
    for row in db.execute('''SELECT rowid,dataset_id,tokens FROM embeddings
            WHERE exclusion IS NULL AND vector IS NULL AND batch_id IS NULL LIMIT 5000'''):
        if rows and total + row['tokens'] > token_limit:
            break
        rows.append(row)
        total += row['tokens']
    if not rows:
        return None
    with db:
        bid = db.execute('INSERT INTO batches(marker,status,tokens,count) VALUES (?,?,?,?)',
                         (uuid.uuid4().hex, 'prepared', total, len(rows))).lastrowid
        for index, row in enumerate(rows):
            db.execute('UPDATE embeddings SET batch_id=?,request_id=?,position=?,error=NULL WHERE dataset_id=?',
                       (bid, f'b{bid}-r{index//32}', index % 32, row['dataset_id']))
    return db.execute('SELECT * FROM batches WHERE id=?', (bid,)).fetchone()


def batch_file(db, batch, root):
    path = root / f'batch-{batch["id"]}.jsonl'
    groups = {}
    for row in db.execute('SELECT request_id,position,input FROM embeddings WHERE batch_id=? ORDER BY request_id,position',
                          (batch['id'],)):
        groups.setdefault(row['request_id'], []).append(row['input'])
    with path.open('w') as out:
        for ident, inputs in groups.items():
            # 32 * 8191 < the embedding endpoint's total request token limit.
            request = {'custom_id': ident, 'method': 'POST', 'url': '/v1/embeddings',
                       'body': {'model': MODEL, 'input': inputs, 'dimensions': DIMENSIONS, 'encoding_format': 'base64'}}
            out.write(json.dumps(request, ensure_ascii=False) + '\n')
    if path.stat().st_size >= 200_000_000:
        raise RuntimeError('Batch file exceeds upload size limit')
    return path


def submit(db, batch, root, api):
    bid = batch['id']
    if not batch['file_id']:
        fid = api.upload(batch_file(db, batch, root))
        with db:
            db.execute('UPDATE batches SET file_id=? WHERE id=?', (fid, bid))
    batch = db.execute('SELECT * FROM batches WHERE id=?', (bid,)).fetchone()
    # Save before POST: an interrupted/uncertain submission must not be blindly repeated.
    with db:
        db.execute('UPDATE batches SET status=? WHERE id=?', ('creating', bid))
    try:
        remote = api.request('POST', '/batches', {'input_file_id': batch['file_id'], 'endpoint': '/v1/embeddings',
                           'completion_window': '24h', 'metadata': {'wanted_run': batch['marker']}})
    except APIError as exc:
        if exc.code in {400, 401, 403, 404, 413, 422, 429}:
            with db:
                db.execute('UPDATE batches SET status=? WHERE id=?', ('prepared', bid))
        raise
    with db:
        db.execute('UPDATE batches SET status=?,remote_id=? WHERE id=?', ('submitted', remote['id'], bid))
    (root / f'batch-{bid}.jsonl').unlink(missing_ok=True)


def recover_submission(db, batch, api):
    after = ''
    while True:
        page = api.request('GET', '/batches?limit=100' + after)
        matches = [b for b in page['data'] if b.get('metadata', {}).get('wanted_run') == batch['marker']]
        if matches:
            if len(matches) != 1:
                raise RuntimeError('Multiple remote batches match the same local submission')
            with db:
                db.execute('UPDATE batches SET remote_id=?,status=? WHERE id=?',
                           (matches[0]['id'], 'submitted', batch['id']))
            return
        if not page.get('has_more'):
            raise RuntimeError('Uncertain submission: no matching remote batch found. Stopped to avoid duplicate billing; reconcile before retry.')
        after = '&after=' + page['last_id']


def vector_blob(value):
    if isinstance(value, str):
        blob = base64.b64decode(value, validate=True)
        values = array.array('f')
        values.frombytes(blob)
        if sys.byteorder != 'little':
            values.byteswap()
    else:
        values = array.array('f', value)
        output = array.array('f', values)
        if sys.byteorder != 'little':
            output.byteswap()
        blob = output.tobytes()
    if len(values) != DIMENSIONS or not all(math.isfinite(x) for x in values) or not any(values):
        raise ValueError('Invalid embedding length or non-finite/zero vector')
    return blob


def ingest_line(db, bid, line):
    result = json.loads(line)
    ident = result['custom_id']
    rows = db.execute('SELECT dataset_id,position FROM embeddings WHERE batch_id=? AND request_id=? ORDER BY position',
                      (bid, ident)).fetchall()
    if not rows:
        raise ValueError('Unknown custom_id in output')
    response = result.get('response') or {}
    if result.get('error') or response.get('status_code') != 200:
        with db:
            db.execute('UPDATE embeddings SET error=? WHERE batch_id=? AND request_id=? AND vector IS NULL',
                       ('remote_request_failed', bid, ident))
        return
    body = response['body']
    if body.get('model') != MODEL:
        raise ValueError('Unexpected embedding model')
    data = body['data']
    if len(data) != len(rows) or sorted(x['index'] for x in data) != list(range(len(rows))):
        raise ValueError('Missing/duplicate embedding output indexes')
    mapped = {x['index']: vector_blob(x['embedding']) for x in data}
    with db:
        for row in rows:
            db.execute('UPDATE embeddings SET vector=?,error=NULL WHERE dataset_id=? AND vector IS NULL',
                       (mapped[row['position']], row['dataset_id']))


def quota_backoff(db):
    # Multiple rejections from the same wave count as one congestion event.
    if get_state(db, 'admission_mode', 'running') in {'draining', 'waiting'}:
        return
    failures = int(get_state(db, 'adaptive_failures', 0)) + 1
    limit = max(1, int(get_state(db, 'adaptive_parallel', 1)) // 2)
    delay = min(1800, 60 * 2 ** min(failures - 1, 5))
    set_state(db, 'adaptive_failures', failures)
    set_state(db, 'adaptive_parallel', limit)
    set_state(db, 'adaptive_successes', 0)
    set_state(db, 'cooldown_seconds', delay)
    set_state(db, 'admission_mode', 'draining')
    print(json.dumps({'admission': 'draining', 'parallel_after_wait': limit,
                      'cooldown_after_drain_seconds': delay}), flush=True)


def admission_limit(db, maximum, active, now=None):
    now = time.time() if now is None else now
    mode = get_state(db, 'admission_mode', 'running')
    if mode == 'draining':
        if active == 0:
            until = now + float(get_state(db, 'cooldown_seconds', 600))
            with db:
                set_state(db, 'admission_mode', 'waiting')
                set_state(db, 'submit_after', until)
            print(json.dumps({'admission': 'waiting', 'resume_after_unix': until}), flush=True)
        return 0
    if mode == 'waiting':
        if now < float(get_state(db, 'submit_after', 0)):
            return 0
        with db:
            set_state(db, 'admission_mode', 'running')
            set_state(db, 'adaptive_successes', 0)
            set_state(db, 'last_increase', now)
        print(json.dumps({'admission': 'resumed', 'parallel': int(get_state(db, 'adaptive_parallel', 1))}), flush=True)
    limit = min(maximum, int(get_state(db, 'adaptive_parallel', 1)))
    if (int(get_state(db, 'adaptive_successes', 0)) >= 5
            and now - float(get_state(db, 'last_increase', now)) >= 120):
        with db:
            limit = min(maximum, limit + 1)
            set_state(db, 'adaptive_parallel', limit)
            set_state(db, 'adaptive_successes', 0)
            set_state(db, 'adaptive_failures', 0)
            set_state(db, 'last_increase', now)
        print(json.dumps({'admission': 'increased', 'parallel': limit}), flush=True)
    return limit


def collect(db, batch, api):
    info = api.request('GET', '/batches/' + batch['remote_id'])
    if info['status'] not in {'completed', 'expired', 'cancelled', 'failed'}:
        return
    bid = batch['id']
    errors = (info.get('errors') or {}).get('data') or []
    counts = info.get('request_counts') or {}
    if (info['status'] == 'failed' and errors
            and all(x.get('code') == 'token_limit_exceeded' for x in errors)
            and all(counts.get(k) == 0 for k in ('total','completed','failed'))
            and not info.get('output_file_id') and not info.get('error_file_id')
            and db.execute('SELECT count(*) FROM embeddings WHERE batch_id=? AND vector IS NOT NULL', (bid,)).fetchone()[0] == 0):
        # Definitively rejected before any inference: safe to requeue, retaining remote history.
        with db:
            db.execute('UPDATE embeddings SET batch_id=NULL,request_id=NULL,position=NULL,error=NULL WHERE batch_id=? AND vector IS NULL', (bid,))
            db.execute('UPDATE batches SET status=?,error=? WHERE id=?', ('retried', json.dumps(info['errors']), bid))
            quota_backoff(db)
        print(json.dumps({'quota_wait_batch': bid, 'processed_requests': 0}), flush=True)
        return
    with db:
        db.execute('UPDATE batches SET status=?,output_file_id=?,error_file_id=?,error=? WHERE id=?',
                   ('collecting', info.get('output_file_id'), info.get('error_file_id'),
                    json.dumps(info.get('errors')) if info.get('errors') else None, bid))
    for field in ['output_file_id', 'error_file_id']:
        if info.get(field):
            with api.request('GET', '/files/' + info[field] + '/content', stream=True) as stream:
                for line in stream:
                    if line.strip():
                        ingest_line(db, bid, line)
    with db:
        db.execute('UPDATE embeddings SET error=COALESCE(error,?) WHERE batch_id=? AND vector IS NULL',
                   ('missing_output_' + info['status'], bid))
        errors = db.execute('SELECT count(*) FROM embeddings WHERE batch_id=? AND vector IS NULL', (bid,)).fetchone()[0]
        db.execute('UPDATE batches SET status=? WHERE id=?', ('errors' if errors else 'done', bid))
        if not errors and get_state(db, 'admission_mode', 'running') == 'running':
            set_state(db, 'adaptive_successes', int(get_state(db, 'adaptive_successes', 0)) + 1)
    if errors:
        raise RuntimeError(f'Batch {bid} has {errors} unresolved inputs; successes saved, stopped for inspection')


def cleanup_remote(db, batch, api):
    for column in ['file_id', 'output_file_id', 'error_file_id']:
        fid = batch[column]
        if fid:
            api.request('DELETE', '/files/' + fid)
            with db:
                db.execute(f'UPDATE batches SET {column}=NULL WHERE id=?', (batch['id'],))


def progress(db, root):
    # Fixed prepared totals plus small batch metadata; do not rescan all vector blobs.
    result = json.loads((root / "prepare-summary.json").read_text())
    result["completed"] = db.execute("SELECT COALESCE(sum(count),0) FROM batches WHERE status='done'").fetchone()[0]
    for row in db.execute("SELECT id FROM batches WHERE status!='done'"):
        result["completed"] += db.execute("SELECT count(*) FROM embeddings WHERE batch_id=? AND vector IS NOT NULL", (row[0],)).fetchone()[0]
    result["batches"] = {r[0]:r[1] for r in db.execute("SELECT status,count(*) FROM batches GROUP BY status")}
    result.pop("validation", None)
    result["admission"] = {k:get_state(db,k) for k in
        ("admission_mode","adaptive_parallel","adaptive_successes","adaptive_failures","submit_after","cooldown_seconds")}
    return result


def run(db, root, args):
    if get_state(db, 'prepared') != '1':
        raise RuntimeError('Run prepare first')
    api = API()
    while True:
        if db.execute("SELECT count(*) FROM batches WHERE status='errors'").fetchone()[0]:
            raise RuntimeError('Unresolved batch errors: inspect and retry only failed inputs')
        for batch in db.execute("SELECT * FROM batches WHERE status IN ('creating','submitted','collecting','done','retried')").fetchall():
            if batch['status'] == 'creating':
                recover_submission(db, batch, api)
            elif batch['status'] in {'submitted', 'collecting'}:
                collect(db, batch, api)
            elif batch['status'] in {'done', 'retried'} and any(batch[x] for x in ['file_id','output_file_id','error_file_id']):
                cleanup_remote(db, batch, api)
        active, queued = db.execute("SELECT count(*),COALESCE(sum(tokens),0) FROM batches WHERE status IN ('creating','submitted','collecting')").fetchone()
        # Ramp up only after sustained successful batches; drain and back off on quota rejection.
        limit = admission_limit(db, args.parallel, active)
        while (active < limit and queued + args.batch_tokens <= args.queue_tokens):
            batch = db.execute("SELECT * FROM batches WHERE status='prepared' ORDER BY id LIMIT 1").fetchone()
            if batch is None:
                batch = make_batch(db, args.batch_tokens)
            if batch is None:
                break
            if queued + batch['tokens'] > args.queue_tokens:
                break
            try:
                submit(db, batch, root, api)
            except APIError as exc:
                if exc.code != 429:
                    raise
                with db:
                    quota_backoff(db)
                print(json.dumps({'http_429': True, 'admission': 'draining'}), flush=True)
                break
            active += 1
            queued += batch['tokens']
        snapshot = progress(db, root)
        print(json.dumps(snapshot), flush=True)
        if snapshot['completed'] == snapshot['eligible']:
            for batch in db.execute("SELECT * FROM batches WHERE status='done'").fetchall():
                cleanup_remote(db, batch, api)
            return snapshot
        time.sleep(15)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'status', 'run', 'retry-errors'])
    parser.add_argument('--catalog', default='/data/catalog.sqlite3')
    parser.add_argument('--root', default='/data/embeddings-small-v1')
    parser.add_argument('--batch-tokens', type=int, default=1_000_000)
    parser.add_argument('--queue-tokens', type=int, default=2_000_000)
    parser.add_argument('--parallel', type=int, default=2)
    args = parser.parse_args()
    if not (MAX_TOKENS <= args.batch_tokens <= args.queue_tokens and 1 <= args.parallel <= 20):
        parser.error('Invalid queue/batch limits')
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'worker.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = connect(root)
        try:
            if args.command == 'prepare':
                result = prepare(db, args.catalog)
            elif args.command == 'run':
                result = run(db, root, args)
            elif args.command == 'retry-errors':
                # Explicit only: completed vectors are never requeued.
                with db:
                    db.execute("UPDATE embeddings SET batch_id=NULL,request_id=NULL,position=NULL,error=NULL WHERE vector IS NULL AND batch_id IN (SELECT id FROM batches WHERE status='errors')")
                    db.execute("UPDATE batches SET status='retried' WHERE status='errors'")
                result = status(db)
            else:
                result = status(db)
            print(json.dumps(result, ensure_ascii=False), flush=True)
        finally:
            db.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        sys.exit(1)
