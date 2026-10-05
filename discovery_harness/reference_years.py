"""Exact catalogue reference-year postings, stored with the keyword index."""
import json
from contextlib import closing
import os
from pathlib import Path
import shutil
import sqlite3
import time

from catalog import year_fields
from .keyword_index import signature, validate


def replace(db, n, row):
    db.execute('DELETE FROM reference_years WHERE n=?', (n,))
    if row is not None:
        years = year_fields(json.loads(row['metadata'] or '{}'), row['title'], row['description'])['reference_years']
        db.executemany('INSERT INTO reference_years VALUES(?,?)', ((year, n) for year in years))


def build(source, index, target):
    """Copy an immutable keyword snapshot; never modify the serving file."""
    source, index, target = map(Path, (source, index, target))
    manifest = validate(source, index)
    if target.exists():
        raise FileExistsError(target)
    with target.open('xb') as output, index.open('rb') as original:
        shutil.copyfileobj(original, output, 8*1024**2)
    started = time.monotonic()
    with closing(sqlite3.connect(source.resolve().as_uri()+'?mode=ro&immutable=1', uri=True)) as cat, closing(sqlite3.connect(target)) as db:
        cat.row_factory = sqlite3.Row
        db.executescript('''CREATE TABLE reference_years(year INTEGER NOT NULL,n INTEGER NOT NULL,
            PRIMARY KEY(year,n)) WITHOUT ROWID;
            CREATE INDEX reference_years_document ON reference_years(n);
            CREATE TABLE reference_year_state(records INTEGER NOT NULL);''')
        count = 0
        for row in cat.execute('SELECT rowid AS n,title,description,metadata FROM datasets'):
            replace(db, row['n'], row)
            count += 1
            if count % 50000 == 0:
                db.commit()
                print(json.dumps({'records':count,'seconds':round(time.monotonic()-started,1)}), flush=True)
        if count != manifest['records'] or signature(source) != manifest['source']:
            raise ValueError('Catalogue changed while building reference years')
        db.execute('INSERT INTO reference_year_state VALUES(?)', (count,))
        db.commit()
        postings = db.execute('SELECT count(*) FROM reference_years').fetchone()[0]
    with target.open('rb') as stream:
        os.fsync(stream.fileno())
    return {'records':count,'postings':postings,'seconds':round(time.monotonic()-started,1)}
