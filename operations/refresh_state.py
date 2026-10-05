"""Persistent rotation, changed metadata and conservative Batch budget accounting."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

SEOUL = ZoneInfo('Asia/Seoul')
DAILY_NANODOLLARS = 100_000_000  # $0.10. Integer arithmetic, no rounding below cost.
NANODOLLARS_PER_TOKEN = 10      # $0.01 / 1M tokens: text-embedding-3-small Batch.


def day(now=None):
    return (now or datetime.now(timezone.utc)).astimezone(SEOUL).date().isoformat()


def connect(root):
    import embedding_batch
    db = embedding_batch.connect(Path(root))
    db.executescript('''
      CREATE TABLE IF NOT EXISTS rotations (
        id INTEGER PRIMARY KEY, started_day TEXT NOT NULL, last_active_day TEXT NOT NULL,
        start_index INTEGER NOT NULL, sites TEXT NOT NULL, status TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS source_runs (
        rotation INTEGER, source TEXT, status TEXT NOT NULL, report TEXT NOT NULL DEFAULT '{}',
        PRIMARY KEY(rotation,source));
      CREATE TABLE IF NOT EXISTS changes (
        dataset_id TEXT PRIMARY KEY, source TEXT NOT NULL, rotation INTEGER NOT NULL,
        metadata TEXT NOT NULL, content_hash TEXT NOT NULL, input_hash TEXT NOT NULL,
        embedding_key TEXT, exclusion TEXT, state TEXT NOT NULL DEFAULT 'pending');
      CREATE TABLE IF NOT EXISTS excluded (
        dataset_id TEXT PRIMARY KEY, reason TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS charges (
        batch_id INTEGER PRIMARY KEY, submitted_day TEXT NOT NULL, settled_day TEXT,
        nanodollars INTEGER NOT NULL, status TEXT NOT NULL);
    ''')
    from refresh_quality import setup
    setup(db)
    return db


@contextmanager
def transaction(db):
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise


def configure(db, sources):
    if len(sources) != 40 or len(set(sources)) != 40:
        raise ValueError('The rotation requires exactly 40 distinct registered sources')
    saved = db.execute("SELECT value FROM state WHERE key='rotation_sources'").fetchone()
    encoded = json.dumps(sources)
    if saved and saved[0] != encoded:
        raise ValueError('Source order changed; an explicit rotation migration is required')
    with db:
        db.execute("INSERT OR IGNORE INTO state VALUES('rotation_sources',?)", (encoded,))
        db.execute("INSERT OR IGNORE INTO state VALUES('next_source_index','0')")
        db.execute("INSERT OR IGNORE INTO state VALUES('adaptive_parallel','15')")


def claim_rotation(db, now=None):
    today = day(now)
    with transaction(db):
        current = db.execute("SELECT * FROM rotations WHERE status='running' ORDER BY id LIMIT 1").fetchone()
        if current:
            # Resumption consumes this collection day; don't catch up several days at once.
            if today > current['last_active_day']:
                db.execute('UPDATE rotations SET last_active_day=? WHERE id=?', (today,current['id']))
            return current['id'],json.loads(current['sites'])
        last = db.execute('SELECT max(last_active_day) FROM rotations').fetchone()[0]
        if last and today <= last:
            return None
        sources = json.loads(db.execute("SELECT value FROM state WHERE key='rotation_sources'").fetchone()[0])
        position = int(db.execute("SELECT value FROM state WHERE key='next_source_index'").fetchone()[0])
        selected = [sources[(position+i)%len(sources)] for i in range(8)]
        ident = db.execute('INSERT INTO rotations(started_day,last_active_day,start_index,sites,status) VALUES(?,?,?,?,?)',
                           (today,today,position,json.dumps(selected),'running')).lastrowid
        db.executemany('INSERT INTO source_runs(rotation,source,status) VALUES(?,?,?)',
                       [(ident,s,'pending') for s in selected])
        return ident,selected


def finish_rotation(db, ident, now=None):
    with transaction(db):
        row = db.execute('SELECT * FROM rotations WHERE id=?',(ident,)).fetchone()
        counts = dict(db.execute('SELECT status,count(*) FROM source_runs WHERE rotation=? GROUP BY status',(ident,)))
        if sum(counts.values()) != 8 or set(counts)-{'complete','failed','unverified'}:
            return False
        if row['status'] in {'complete','attempted'}:
            return True
        status='complete' if set(counts)=={'complete'} else 'attempted'
        db.execute("UPDATE rotations SET status=?,last_active_day=max(last_active_day,?) WHERE id=?",(status,day(now),ident))
        db.execute("UPDATE state SET value=? WHERE key='next_source_index'",(str((row['start_index']+8)%40),))
        return True


def budget_used(db, today):
    # Older in-flight requests carry their reservation forward. On resolution,
    # retain it for that day too; a delayed Batch must not free a fresh daily budget.
    return db.execute('''SELECT COALESCE(sum(nanodollars),0) FROM charges
       WHERE status!='released' AND
       (submitted_day=? OR settled_day=? OR status='reserved')''',(today,today)).fetchone()[0]


class DailyBudgetReached(Exception):
    pass


def reserve_batch(db, batch_id, now=None):
    today = day(now)
    with transaction(db):
        if db.execute('SELECT 1 FROM charges WHERE batch_id=?',(batch_id,)).fetchone():
            raise RuntimeError('Batch already has a submission reservation; reconcile instead of resubmitting')
        tokens = db.execute('SELECT tokens FROM batches WHERE id=?',(batch_id,)).fetchone()[0]
        cost = tokens*NANODOLLARS_PER_TOKEN
        if budget_used(db,today)+cost > DAILY_NANODOLLARS:
            raise DailyBudgetReached()
        db.execute("INSERT INTO charges VALUES(?,?,NULL,?,'reserved')",(batch_id,today,cost))


def settle_batch(db,batch_id,*,uncharged=False,now=None):
    with db:
        db.execute("UPDATE charges SET status=?,settled_day=COALESCE(settled_day,?) WHERE batch_id=?",
                   ('released' if uncharged else 'settled',day(now),batch_id))


def input_key(ident, input_hash):
    return hashlib.sha256((ident+'\0'+input_hash).encode()).hexdigest()


def native_identity(record):
    if record.get('source_id')!='us' or not record.get('native_id') or not record.get('native_organization_id'):
        return None
    parts=[record.get(k) for k in ('native_organization_id','native_id','native_parent_identifier','native_type')]
    return 'native:us:'+hashlib.sha256(json.dumps(parts,sort_keys=True).encode()).hexdigest()


def content_hash(record):
    # Provider tags MUST be included. The legacy fingerprint intentionally omitted them.
    ignored={'checked_at','fingerprint','live_verified_at','source_export_date','catalog_edition',
             'collection_method','description_languages_requested'}
    value={k:v for k,v in record.items() if k not in ignored}
    for key in ['subjects','access_paths','native_catalog_paths']:
        if isinstance(value.get(key),list):
            value[key]=sorted(value[key],key=lambda x:json.dumps(x,ensure_ascii=False,sort_keys=True))
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True).encode()).hexdigest()


def summary(db):
    today=day()
    last=db.execute('SELECT * FROM rotations ORDER BY id DESC LIMIT 1').fetchone()
    row=dict(last) if last else None
    if row:
        row['sites']=json.loads(row['sites'])
        row['sources']=[dict(x) for x in db.execute('SELECT source,status,report FROM source_runs WHERE rotation=?',(row['id'],))]
        for x in row['sources']:x['report']=json.loads(x['report'])
    return {'rotation':row,'next_source_index':int(db.execute("SELECT value FROM state WHERE key='next_source_index'").fetchone()[0]),
            'changes':dict(db.execute('SELECT state,count(*) FROM changes GROUP BY state')),
            'embedding_batches':dict(db.execute('SELECT status,count(*) FROM batches GROUP BY status')),
            'budget_day':today,'budget_usd':DAILY_NANODOLLARS/1e9,'reserved_or_spent_usd':budget_used(db,today)/1e9,
            'budget_basis':'Asia/Seoul submission day plus unresolved and same-day-settled reservations',
            'batch_only':True,
            'quality':dict(db.execute('SELECT status,count(*) FROM quality GROUP BY status'))}
