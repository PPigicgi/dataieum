"""Checkpointed full migration and all-row comparison of the paused publication."""
import argparse
from contextlib import contextmanager
import hashlib
import itertools
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time

import psycopg
from psycopg.types.json import Jsonb

FIELDS=('ordinal','id','source_id','title','description','metadata','original_mappings','fingerprint','checked_at','reference_years','classification')
JSON_FIELDS={5,6,10}


def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def identity(paths):
    result={}
    for key,path in paths.items():
        p=Path(path);s=p.stat();wal=Path(str(p)+'-wal')
        if wal.exists() and wal.stat().st_size:raise RuntimeError('Input has a live WAL: '+key)
        result[key]={'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}
    return result


@contextmanager
def source(paths):
    db=sqlite3.connect(Path(paths['catalog']).resolve().as_uri()+'?mode=ro&immutable=1',uri=True)
    db.row_factory=sqlite3.Row
    for key in ('topics','keyword'):db.execute('ATTACH DATABASE ? AS '+key,(Path(paths[key]).resolve().as_uri()+'?mode=ro&immutable=1',))
    db.execute('PRAGMA query_only=ON');db.execute('PRAGMA cache_size=-32768')
    try:yield db
    finally:db.close()


def records(db,after=0,ids=None):
    condition=' WHERE d.rowid>?';args=[after]
    if ids is not None:
        if not ids:return
        condition=' WHERE d.id IN (SELECT value FROM json_each(?))';args=[json.dumps(ids)]
    cursor=db.execute('SELECT d.rowid AS ordinal,d.* FROM datasets d'+condition+' ORDER BY d.rowid',args)
    while batch:=cursor.fetchmany(1000):
        ids_json=json.dumps([r['id'] for r in batch]);clause=' IN (SELECT value FROM json_each(?))'
        found={r['id']:dict(r) for r in db.execute('SELECT * FROM topics.records WHERE id'+clause,(ids_json,))}
        links={};primaries={};holds={};corrections={}
        for r in db.execute('SELECT * FROM topics.links WHERE dataset_id'+clause+' ORDER BY dataset_id,score DESC,topic',(ids_json,)):
            links.setdefault(r['dataset_id'],[]).append({k:r[k] for k in ('topic','band','score','alpha')})
        for r in db.execute('SELECT * FROM topics.primary_links WHERE dataset_id'+clause,(ids_json,)):primaries[r['dataset_id']]=dict(r)
        for r in db.execute('SELECT * FROM topics.classification_holds WHERE dataset_id'+clause,(ids_json,)):holds[r['dataset_id']]=dict(r)
        for r in db.execute('SELECT * FROM topics.membership_corrections WHERE dataset_id'+clause+' ORDER BY dataset_id,topic',(ids_json,)):
            corrections.setdefault(r['dataset_id'],[]).append(dict(r))
        years={}
        ordinals=json.dumps([r['ordinal'] for r in batch])
        for r in db.execute('SELECT n,year FROM keyword.reference_years WHERE n IN (SELECT value FROM json_each(?)) ORDER BY n,year',(ordinals,)):
            years.setdefault(r['n'],[]).append(r['year'])
        for r in batch:
            g=found.get(r['id']);p=primaries.get(r['id'])
            classification={'band':g['band'] if g else None,'score':g['score'] if g else None,
                'primary_topic':p['topic'] if p else None,'graph_record':g,'primary_link':p,
                'links':links.get(r['id'],[]),'hold':holds.get(r['id']),
                'corrections':corrections.get(r['id'],[]),'basis':'embedding_similarity'}
            yield (r['ordinal'],r['id'],r['source_id'],r['title'],r['description'],json.loads(r['metadata']),
                json.loads(r['mappings']),r['fingerprint'],r['checked_at'],years.get(r['ordinal'],[]),classification)


def save(pg,publication,state,details):
    pg.execute('INSERT INTO publications(id,state,details) VALUES(%s,%s,%s) ON CONFLICT(id) DO UPDATE SET state=excluded.state,details=excluded.details,updated_at=now()',
        (publication,state,Jsonb(details)))


def emit(root,value):
    value={**value,'updated_at':time.time()};tmp=root/'status.tmp'
    tmp.write_text(canonical(value));tmp.replace(root/'status.json')
    print(canonical(value),flush=True)


def verify(pg,db,details):
    left=hashlib.sha256();right=hashlib.sha256();count=links=visible=0
    # Stream both entire stores in the same stable ordinal order. Recompute the
    # hash from returned PostgreSQL values, not from a copied checksum column.
    with pg.cursor(name='full_verification') as cursor:
        cursor.itersize=1000;cursor.execute('SELECT '+','.join(FIELDS)+' FROM datasets ORDER BY ordinal')
        for original,stored in itertools.zip_longest(records(db),cursor):
            if original is None or stored is None:raise RuntimeError('Migration count differs')
            a=canonical(original).encode();b=canonical(stored).encode()
            if a!=b:raise RuntimeError('Migration value differs for ID '+original[1])
            left.update(a+b'\n');right.update(b+b'\n');count+=1
            links+=len(original[10]['links']);visible+=original[10]['band'] is not None
    expected=db.execute('SELECT count(*) FROM datasets').fetchone()[0]
    expected_links=db.execute('SELECT count(*) FROM topics.links').fetchone()[0]
    expected_visible=db.execute('SELECT count(*) FROM topics.records').fetchone()[0]
    if (count,links,visible)!=(expected,expected_links,expected_visible):raise RuntimeError('Migration membership differs')
    for table,sql in [('sources','SELECT id,info,last_sync,last_error FROM sources ORDER BY id'),('topics','SELECT * FROM topics.topics ORDER BY id')]:
        original=[list(r) for r in db.execute(sql)]
        if table=='sources':
            for row in original:row[1]=json.loads(row[1])
        stored=[list(r) for r in pg.execute('SELECT * FROM '+table+' ORDER BY id')]
        if canonical(original)!=canonical(stored):raise RuntimeError('Migration dictionary differs: '+table)
    maximum=db.execute('SELECT max(score) FROM topics.records').fetchone()[0]
    return {'records':count,'relationships':links,'visible_records':visible,'maximum_similarity':maximum,'source_sha256':left.hexdigest(),'postgres_sha256':right.hexdigest(),'all_values_equal':True}


def main():
    p=argparse.ArgumentParser();p.add_argument('--inputs',required=True);p.add_argument('--credentials',required=True);p.add_argument('--output',required=True)
    p.add_argument('--activate',action='store_true');p.add_argument('--verify-only',action='store_true');args=p.parse_args()
    paths=json.loads(Path(args.inputs).read_text());root=Path(args.output);root.mkdir(parents=True,exist_ok=True)
    sig=identity(paths);publication=hashlib.sha256(canonical(sig).encode()).hexdigest()[:24]
    settings=json.loads(Path(args.credentials).read_text())
    with psycopg.connect(**settings,connect_timeout=10) as pg,source(paths) as db:
        if not pg.execute('SELECT pg_try_advisory_lock(83914927)').fetchone()[0]:raise RuntimeError('Migration already running')
        pg.execute(Path(__file__).with_name('postgres.sql').read_text());pg.commit()
        current=pg.execute('SELECT state,details FROM publications WHERE id=%s',(publication,)).fetchone();pg.commit()
        if current and current[0]=='active' and not args.verify_only:
            emit(root,{'stage':'active','publication':publication,**current[1]});return
        if current is None and pg.execute('SELECT 1 FROM datasets LIMIT 1').fetchone():raise RuntimeError('A different publication is already loaded')
        details=current[1] if current else {'artifacts':sig,'last_ordinal':0,'copied':0,'started_at':time.time()}
        if not args.verify_only and (not current or current[0]=='copying'):
            if details['copied']==0:
                with pg.cursor() as cur:
                    cur.executemany('INSERT INTO sources VALUES(%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING',[(r['id'],Jsonb(json.loads(r['info'])),r['last_sync'],r['last_error']) for r in db.execute('SELECT * FROM sources')])
                    cur.executemany('INSERT INTO topics VALUES(%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING',[tuple(r) for r in db.execute('SELECT * FROM topics.topics')])
                save(pg,publication,'copying',details);pg.commit()
            stream=records(db,details['last_ordinal'])
            while batch:=list(itertools.islice(stream,4000)):
                if shutil.disk_usage(root).free<16*1024**3:raise RuntimeError('Migration disk reserve reached')
                if identity(paths)!=sig:raise RuntimeError('Source changed during migration')
                with pg.cursor().copy('COPY datasets ('+','.join(FIELDS)+') FROM STDIN') as copy:
                    for row in batch:copy.write_row(tuple(Jsonb(value) if i in JSON_FIELDS else value for i,value in enumerate(row)))
                details.update(last_ordinal=batch[-1][0],copied=details['copied']+len(batch))
                save(pg,publication,'copying',details);pg.commit()
                if details['copied']%40000==0:emit(root,{'stage':'copying','publication':publication,**details})
            save(pg,publication,'verifying',details);pg.commit()
        emit(root,{'stage':'verifying','publication':publication,**details})
        verification=verify(pg,db,details);pg.commit()
        if identity(paths)!=sig:raise RuntimeError('Source changed during verification')
        details['verification']=verification
        pg.execute('ANALYZE datasets');save(pg,publication,'verified',details);pg.commit()
        if args.activate:
            pg.execute("UPDATE publications SET state='retired' WHERE state='active'")
            save(pg,publication,'active',details);pg.commit()
        emit(root,{'stage':'active' if args.activate else 'verified','publication':publication,**details})


if __name__=='__main__':main()
