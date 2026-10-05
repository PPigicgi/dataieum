"""Offline, disposable substring index. Never writes to the source catalogue.

One FTS token per Unicode code point preserves exact substring order, including
one/two-character Korean queries. Contentless FTS stores positions, not a second
copy of the descriptions. Encoded tokens make punctuation literal, not syntax.
"""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import time
import unicodedata

from .display_text import display_text

VERSION = 2


def normalize(value):
    return ' '.join(unicodedata.normalize('NFKC', display_text(value or '')).casefold().split())


class _Tokens(dict):
    def __missing__(self, code):
        result = f'u{code:x} '
        self[code] = result
        return result


TOKENS = _Tokens()


def expression(value):
    normalized = normalize(value)
    if not normalized:
        raise ValueError('empty search text')
    return '"' + normalized.translate(TOKENS).strip() + '"'


def lookup(value):
    value = normalize(value)
    if len(value) >= 3:
        return 'phrases', '"'+value.replace('"','""')+'"'
    return 'terms', expression(value)


def signature(source):
    source = Path(source).resolve(strict=True)
    wal = source.with_name(source.name + '-wal')
    if wal.exists() and wal.stat().st_size:
        raise ValueError('source has an active WAL')
    stat = source.stat()
    return {'version': VERSION, 'size': stat.st_size, 'mtime_ns': stat.st_mtime_ns, 'inode': stat.st_ino}


def validate(source, index):
    with_index = sqlite3.connect(Path(index).resolve().as_uri()+'?mode=ro&immutable=1', uri=True)
    try:
        manifest = json.loads(with_index.execute('SELECT value FROM manifest').fetchone()[0])
        if manifest['source'] != signature(source):
            raise ValueError('search index source mismatch')
        return manifest
    finally:
        with_index.close()


def search_text(row):
    metadata = json.loads(row['metadata'] or '{}')
    values = [row['title'], row['description'], metadata.get('publisher'), metadata.get('region'),
              metadata.get('survey_name')]
    for key in ('native_catalog_path', 'native_catalog_paths', 'tags'):
        value = metadata.get(key)
        values.extend(value if isinstance(value, list) else [value])
    values.extend(s.get('label') for s in metadata.get('subjects', []) if isinstance(s, dict))
    return normalize(' '.join(v for v in values if isinstance(v, str)))


def build(source, index, *, max_bytes=24*1024**3):
    if Path(index).is_symlink():
        raise ValueError('search index must not be a symlink')
    source, index = Path(source).resolve(strict=True), Path(index).resolve()
    if source == index or (index.exists() and source.samefile(index)):
        raise ValueError('search index must be separate from the source')
    before = signature(source)
    index.parent.mkdir(parents=True, exist_ok=True)
    scratch = index.with_suffix('.building.sqlite3')
    # Fail closed on concurrent builders; never discard another process's work.
    with scratch.open('xb'):
        pass
    source_db = target = None
    started = time.monotonic()
    try:
        source_db = sqlite3.connect(source.as_uri()+'?mode=ro&immutable=1', uri=True)
        source_db.row_factory = sqlite3.Row
        target = sqlite3.connect(scratch)
        target.execute('PRAGMA journal_mode=OFF')
        target.execute('PRAGMA synchronous=OFF')
        target.execute('PRAGMA cache_size=-32768')
        target.execute(f'PRAGMA max_page_count={max_bytes//4096}')
        target.executescript("""
            CREATE TABLE documents(n INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE);
            CREATE VIRTUAL TABLE terms USING fts5(text, content='', columnsize=0,
                tokenize='unicode61 remove_diacritics 0');
            CREATE TABLE manifest(value TEXT NOT NULL);
            CREATE VIRTUAL TABLE phrases USING fts5(text, content='', columnsize=0,
                tokenize='trigram case_sensitive 1');
        """)
        cursor = source_db.execute('SELECT rowid AS n,id,title,description,metadata FROM datasets')
        count = 0
        while rows := cursor.fetchmany(500):
            target.executemany('INSERT INTO documents VALUES(?,?)', ((r['n'],r['id']) for r in rows))
            target.executemany('INSERT INTO terms(rowid,text) VALUES(?,?)',
                               ((r['n'],search_text(r).translate(TOKENS)) for r in rows))
            target.executemany('INSERT INTO phrases(rowid,text) VALUES(?,?)',
                               ((r['n'],search_text(r).replace('\x00','\x01')) for r in rows))
            target.commit()
            count += len(rows)
            if count % 10000 == 0:
                print(json.dumps({'records':count,'seconds':round(time.monotonic()-started,1),
                                  'mib':round(scratch.stat().st_size/1024**2,1)}),flush=True)
        if signature(source) != before:
            raise ValueError('source changed during search indexing')
        manifest = {'source':before,'records':count}
        target.execute('INSERT INTO manifest VALUES(?)',(json.dumps(manifest),))
        target.commit()
        target.close(); target = None
        with scratch.open('r+b') as stream: os.fsync(stream.fileno())
        scratch.replace(index)
        return manifest
    finally:
        if target: target.close()
        if source_db: source_db.close()
        scratch.unlink(missing_ok=True)


def add_phrases(source, index, *, max_bytes=24*1024**3):
    """Upgrade a completed v1 index atomically; preserve its short-query work."""
    source,index=Path(source).resolve(strict=True),Path(index).resolve(strict=True)
    if source.samefile(index):raise ValueError('search index must be separate from the source')
    before=signature(source)
    db=sqlite3.connect(index.as_uri()+'?mode=ro&immutable=1',uri=True)
    try:old=json.loads(db.execute('SELECT value FROM manifest').fetchone()[0])
    finally:db.close()
    if old['source'] != {**before,'version':1}:raise ValueError('expected matching v1 search index')
    scratch=index.with_suffix('.phrases-building.sqlite3')
    # Only remove scratch we created; preserve an existing builder's file.
    with scratch.open('xb'):pass
    target=source_db=None
    started=time.monotonic()
    try:
        if index.stat().st_size>max_bytes:raise ValueError('index exceeds size budget')
        with scratch.open('wb') as output,index.open('rb') as input:
            while chunk:=input.read(8*1024**2):output.write(chunk)
        target=sqlite3.connect(scratch)
        target.execute('PRAGMA journal_mode=OFF');target.execute('PRAGMA synchronous=OFF')
        target.execute('PRAGMA cache_size=-131072');target.execute(f'PRAGMA max_page_count={max_bytes//4096}')
        target.execute("CREATE VIRTUAL TABLE phrases USING fts5(text, content='', columnsize=0, tokenize='trigram case_sensitive 1')")
        source_db=sqlite3.connect(source.as_uri()+'?mode=ro&immutable=1',uri=True)
        source_db.row_factory=sqlite3.Row
        cursor=source_db.execute('SELECT rowid AS n,title,description,metadata FROM datasets')
        count=0
        while rows:=cursor.fetchmany(1000):
            # SQLite's trigram tokenizer drops NUL; retain a boundary so a
            # corrupt source cannot turn ab<NUL>c into a false hit for abc.
            target.executemany('INSERT INTO phrases(rowid,text) VALUES(?,?)',((r['n'],search_text(r).replace('\x00','\x01')) for r in rows))
            count+=len(rows)
            if count%10000==0:target.commit()
            if count%50000==0:print(json.dumps({'phrases':count,'seconds':round(time.monotonic()-started,1),'mib':round(scratch.stat().st_size/1024**2,1)}),flush=True)
        if count!=old['records'] or signature(source)!=before:raise ValueError('catalogue changed during index upgrade')
        manifest={'source':before,'records':count}
        target.execute('UPDATE manifest SET value=?',(json.dumps(manifest),));target.commit();target.close();target=None
        with scratch.open('r+b') as stream:os.fsync(stream.fileno())
        scratch.replace(index)
        return manifest
    finally:
        if source_db:source_db.close()
        if target:target.close()
        scratch.unlink(missing_ok=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=Path)
    parser.add_argument('index', type=Path)
    parser.add_argument('--upgrade-phrases',action='store_true')
    args = parser.parse_args()
    print(json.dumps((add_phrases if args.upgrade_phrases else build)(args.source,args.index)),flush=True)
