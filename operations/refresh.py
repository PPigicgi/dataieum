"""One resumable daily group of eight sites; hourly Batch settlement uses the same ledger."""
import argparse
import asyncio
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time

from refresh_state import (claim_rotation,configure,connect,finish_rotation,native_identity,summary)
from refresh_collect import write_json


def load_exclusions(db,paths):
    from refresh_quality import record_exclusion,parse_record
    loaded=0
    for path in paths:
        p=Path(path);stat=p.stat()
        marker='exclusion_inventory:'+str(p)
        signature=json.dumps([2,stat.st_size,stat.st_mtime_ns])
        old=db.execute('SELECT value FROM state WHERE key=?',(marker,)).fetchone()
        if old and old[0]==signature:continue
        with db:
            with p.open() as stream:
                for line in stream:
                    entry=json.loads(line)
                    row=entry.get('row') if entry.get('type')=='catalog' else entry if not entry.get('type') else None
                    if not row or not row.get('id'):continue
                    m=parse_record(row.get('metadata')) or {}
                    record_exclusion(db,row['id'],m,'previously_approved_deletion');loaded+=1
            db.execute('INSERT OR REPLACE INTO state VALUES(?,?)',(marker,signature))
    with db:db.execute("INSERT OR REPLACE INTO state VALUES('approved_exclusions_loaded',?)",(json.dumps({'updated_at':time.time(),'imported_records':loaded}),))


async def collect_group(args,db,rotation,sites,parallel):
    semaphore=asyncio.Semaphore(parallel)
    finished=asyncio.Event()
    async def batches():
        # A slow provider must not delay completed providers' embeddings/results.
        while not finished.is_set():
            with (args.root/'batch-worker.log').open('a') as log:
                proc=await asyncio.create_subprocess_exec(sys.executable,str(Path(__file__)),
                    'batch','--root',str(args.root),'--vectors',str(args.vectors),
                    '--profiles',str(args.profiles),stdout=log,stderr=log)
                code=await proc.wait()
            if code:
                write_json(args.root/'batch-status.json',{'state':'worker_error','exit_code':code})
            try:await asyncio.wait_for(finished.wait(),timeout=60)
            except asyncio.TimeoutError:pass
    async def one(source):
        prior=db.execute('SELECT status FROM source_runs WHERE rotation=? AND source=?',(rotation,source)).fetchone()[0]
        if prior in {'complete','failed','unverified'}:return
        async with semaphore:
            folder=args.root/'runs'/str(rotation)/source;folder.mkdir(parents=True,exist_ok=True)
            with (folder/'worker.log').open('a') as log:
                proc=await asyncio.create_subprocess_exec(sys.executable,str(Path(__file__).with_name('refresh_collect.py')),
                    '--root',str(args.root),'--baseline',str(args.catalog),'--rotation',str(rotation),
                    '--source',source,'--profiles',str(args.profiles),stdout=log,stderr=log)
                code=await proc.wait()
            if code:
                # Process death may leave a reusable checkpoint. Keep it pending,
                # not complete/failed, and resume it on the next tick/reboot.
                with db:db.execute("UPDATE source_runs SET status='pending',report=? WHERE rotation=? AND source=?",
                                   (json.dumps({'worker_exit_code':code}),rotation,source))
            write_json(args.root/'status.json',summary(db))
    batch_task=asyncio.create_task(batches())
    try:await asyncio.gather(*(one(s) for s in sites))
    finally:
        finished.set()
        await batch_task


def collect_tick(args,db,profiles):
    active=claim_rotation(db)
    if active is None:return
    rotation,sites=active
    write_json(args.root/'status.json',summary(db))
    asyncio.run(collect_group(args,db,rotation,sites,profiles['parallel_sources']))
    finish_rotation(db,rotation)


def trim_completed_evidence(root,db):
    # Retain each site's latest attempt, plus every unfinished checkpoint.
    latest=dict(db.execute('SELECT source,max(rotation) FROM source_runs GROUP BY source'))
    for rotation,source,status in db.execute('SELECT rotation,source,status FROM source_runs'):
        if status in {'complete','failed','unverified'} and rotation<latest[source]:
            path=root/'runs'/str(rotation)/source
            if path.exists():shutil.rmtree(path)


def batch_step(args,db):
    with (args.root/'batch.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return
        from refresh_batch import prepare_changes,tick
        from refresh_quality import check_pending,Fetcher
        profiles=json.loads(args.profiles.read_text())
        fetch=Fetcher(profiles=profiles)
        while check_pending(db,profiles,fetch):
            prepare_changes(db,args.vectors)
            tick(db,args.root)
        write_json(args.root/'batch-status.json',{'state':'preparing','updated_at':time.time()})
        prepare_changes(db,args.vectors)
        write_json(args.root/'batch-status.json',tick(db,args.root))


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['tick','batch','status'])
    p.add_argument('--root',type=Path,default=Path('/refresh'));p.add_argument('--catalog',type=Path,default=Path('/baseline/catalog.sqlite3'))
    p.add_argument('--vectors',type=Path,default=Path('/baseline/embeddings-small-v1/embeddings.sqlite3'))
    p.add_argument('--profiles',type=Path,default=Path('/app/operations/refresh-sources.json'))
    p.add_argument('--exclusions',type=Path,nargs='*',default=[])
    a=p.parse_args();a.root.mkdir(parents=True,exist_ok=True)
    os.umask(0o077)
    profiles=json.loads(a.profiles.read_text())
    with (a.root/'worker.lock').open('a') as lock:
        if a.action=='tick':
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                print(json.dumps({'state':'already_running'}));return
        with closing(connect(a.root)) as db:
            configure(db,[x['id'] for x in profiles['sources']])
            if a.action in {'tick','batch'}:
                if a.action=='tick':
                    load_exclusions(db,a.exclusions)
                    collect_tick(a,db,profiles)
                elif not db.execute("SELECT 1 FROM state WHERE key='approved_exclusions_loaded'").fetchone():
                    raise RuntimeError('Approved exclusions must be loaded before paid work')
                batch_step(a,db)
                if a.action=='tick':trim_completed_evidence(a.root,db)
            result=summary(db);write_json(a.root/'status.json',result)
            print(json.dumps(result,ensure_ascii=False),flush=True)


if __name__=='__main__':main()
