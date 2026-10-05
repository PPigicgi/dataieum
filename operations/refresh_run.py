"""Run the persistent daily collector; publication has a separate maintenance window."""
from contextlib import closing
from datetime import datetime
import fcntl
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from zoneinfo import ZoneInfo


ROOT=Path('/opt/wanted/refresh')
REGISTRY=Path('/opt/dataieum-releases/20260919-ontology-10/compose.active.json')
IMAGES=Path('/opt/wanted/operations/refresh-images.json')


def run(args,**kw):
    return subprocess.run(args,check=True,**kw)


def mounted(config,service,target):
    candidates=[]
    for m in config['services'][service]['volumes']:
        dest=Path(m['target'])
        if Path(target)==dest or dest in Path(target).parents:
            if m['type']=='bind':source=Path(m['source'])
            else:
                volume=config['volumes'][m['source']]['name']
                source=Path(subprocess.check_output(['docker','volume','inspect','--format','{{.Mountpoint}}',volume],text=True).strip())
            candidates.append((len(str(dest)),source/Path(target).relative_to(dest)))
    if not candidates:raise ValueError('Missing mounted source: '+target)
    return max(candidates,key=lambda x:x[0])[1]


def inputs(config):
    app=config['services']['atlas']['environment'];vector=config['services']['vector']['environment']
    return {'catalog':mounted(config,'atlas',app['DB_PATH']),
            'keyword':mounted(config,'atlas',app['DATAIEUM_KEYWORD_INDEX']),
            'topics':mounted(config,'atlas',app['DATAIEUM_TOPIC_GRAPH']).parent,
            'index':mounted(config,'vector','/vector-index'),
            'coverage':mounted(config,'vector',vector['DATAIEUM_COVERAGE_INDEX']),
            'embeddings':mounted(config,'vector','/data/embeddings-small-v1/embeddings.sqlite3'),
            'topic_vectors':mounted(config,'vector',vector['DATAIEUM_TOPIC_VECTORS'])}


def freeze(db,path):
    from refresh_quality import setup,allowed
    setup(db)
    db.row_factory=sqlite3.Row
    db.execute('SAVEPOINT publication_freeze')
    try:
        with closing(sqlite3.connect(path)) as out:
            out.execute('CREATE TABLE changes(dataset_id TEXT PRIMARY KEY,source TEXT,metadata TEXT,content_hash TEXT,input_hash TEXT,vector BLOB)')
            cursor=db.execute("""SELECT c.*,e.vector,e.input_hash AS stored_hash,e.exclusion AS vector_exclusion
                FROM changes c LEFT JOIN embeddings e ON e.dataset_id=c.embedding_key JOIN source_runs s
                ON s.rotation=c.rotation AND s.source=c.source
                WHERE c.state='ready' AND s.status='complete' AND c.exclusion IS NULL ORDER BY c.dataset_id""")
            count=0
            while rows:=cursor.fetchmany(200):
                for row in rows:
                    if not allowed(db,row):continue
                    if row['vector'] is None or row['vector_exclusion'] is not None or row['stored_hash']!=row['input_hash']:
                        raise RuntimeError('Ready metadata and stored embedding identities differ')
                    out.execute('INSERT INTO changes VALUES(?,?,?,?,?,?)',tuple(row[k] for k in ('dataset_id','source','metadata','content_hash','input_hash','vector')))
                    count+=1
                out.commit()
        return count
    finally:db.execute('RELEASE publication_freeze')


def check_frozen_quality(path):
    from refresh_quality import check_delta
    with closing(sqlite3.connect(ROOT/'embeddings.sqlite3')) as db,closing(sqlite3.connect(path)) as delta:
        check_delta(db,delta)


def collector_action(now=None):
    now=(now or datetime.now(ZoneInfo('Asia/Seoul'))).astimezone(ZoneInfo('Asia/Seoul'))
    return 'tick' if now.hour>=5 else 'batch'


def main():
    ROOT.mkdir(exist_ok=True)
    with (ROOT/'driver.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return
        configuration=json.loads(IMAGES.read_text())
        if not configuration.get('collection_enabled',True):
            print(json.dumps({'stage':'collection_paused'}),flush=True);return
        images={k:v['image'] for k,v in configuration['services'].items()}
        maintenance=ROOT/'maintenance-current.json'
        if maintenance.exists() and json.loads(maintenance.read_text()).get('stage') not in {'published','rolled_back','aborted'}:
            print(json.dumps({'stage':'waiting_for_maintenance_recovery'}),flush=True);return
        config=json.loads(REGISTRY.read_text());base=inputs(config)
        for key in ['catalog','embeddings']:
            wal=Path(str(base[key])+'-wal')
            if wal.exists() and wal.stat().st_size:raise RuntimeError('Collector requires an immutable baseline with no live WAL')
        run(['docker','run','--rm','--init','--name','dataieum-refresh','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges',
             '--cpus','0.4','--cpu-shares','128','--blkio-weight','100','--memory','2g','--memory-swap','2g','--tmpfs','/tmp:rw,nosuid,nodev,size=128m',
             '-v',str(ROOT)+':/refresh','-v',str(base['catalog'])+':/baseline/catalog.sqlite3:ro',
             '-v',str(base['embeddings'])+':/baseline/embeddings-small-v1/embeddings.sqlite3:ro',
             '-v','/opt/wanted/nonjs-review-20260924/deleted-records-backup.jsonl:/exclusions/first.jsonl:ro',
             '-v','/opt/wanted/catalog-cleanup-20260926/deleted-records-backup.jsonl:/exclusions/second.jsonl:ro',
             '-v','/opt/dataieum-releases/20260919-ontology-10/luna-secrets/embedding.key:/run/embedding.key:ro',
             images['collector'],collector_action(),'--exclusions','/exclusions/first.jsonl','/exclusions/second.jsonl'])


def stop():
    # ExecStopPost also runs if a oneshot is stopped while still activating.
    for name in ['dataieum-refresh']:
        found=subprocess.run(['docker','inspect','--format','{{.State.Running}}',name],capture_output=True,text=True)
        if found.returncode==0 and found.stdout.strip()=='true':
            subprocess.run(['docker','stop','-t','30',name],check=True,timeout=40)


if __name__=='__main__':stop() if sys.argv[1:]==['stop'] else main()
