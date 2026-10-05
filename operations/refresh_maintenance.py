"""Publish ready changes at 04:00 KST, with a persistent maintenance gate and undo."""
from contextlib import closing,ExitStack
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

import deploy
import refresh_run as refresh
import postgres_publish

ROOT=refresh.ROOT
CURRENT=ROOT/'maintenance-current.json'
TERMINAL={'published','rolled_back','aborted'}


def write(path,value):
    deploy.write_json(path,value)
    with Path(path).open('rb') as f:os.fsync(f.fileno())
    fd=os.open(Path(path).parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def save(state,stage,**extra):
    state.update(stage=stage,updated_at=time.time(),**extra)
    write(Path(state['root'])/'maintenance.json',state);write(CURRENT,state)
    write(ROOT/'publication-status.json',{k:v for k,v in state.items() if k not in {'consumers','base'}})


def gate_content(text):
    pattern=r'(?m)^(\s*)reverse_proxy\s+[^\s]+\s*\{'
    matches=list(re.finditer(pattern,text))
    if len(matches)!=1:raise ValueError('Expected exactly one public upstream')
    m=matches[0];indent=m.group(1)
    return text[:m.start()]+indent+'header Retry-After "600"\n'+indent+'respond "데이터를 업데이트하고 있습니다. 잠시 후 다시 접속해 주세요." 503\n'+text[m.start():]


def within_window(now=None):
    now=(now or datetime.now(ZoneInfo('Asia/Seoul'))).astimezone(ZoneInfo('Asia/Seoul'))
    return now.hour==4 and now.minute<10


def consumers(base):
    paths=[Path(base[k]).resolve() for k in ['catalog','keyword','topics','index','coverage']]
    ids=deploy.run(['docker','ps','-q']).stdout.split()
    inspected=json.loads(deploy.run(['docker','inspect',*ids]).stdout) if ids else []
    found=[]
    for c in inspected:
        if c['Name'] in ['/dataieum-refresh','/dataieum-maintenance']:raise RuntimeError('A worker already uses these inputs')
        if any((p==Path(m['Source']).resolve() or Path(m['Source']).resolve() in p.parents or p in Path(m['Source']).resolve().parents) for p in paths for m in c['Mounts']):
            found.append({'id':c['Id'],'name':c['Name'].lstrip('/'),'service':c['Config'].get('Labels',{}).get('com.docker.compose.service'),'project':c['Config'].get('Labels',{}).get('com.docker.compose.project')})
    return found


def builder(state,action):
    root=Path(state['root']);base=state['base']
    # Parent directories, rather than individual DB files, permit SQLite rollback journals.
    command=['docker','run','--rm','--init','--name','dataieum-maintenance','--user','0:0','--read-only',
             '--cap-drop','ALL','--cap-add','DAC_OVERRIDE','--cap-add','FOWNER','--security-opt','no-new-privileges',
             '--network','none','--cpus','1.2','--memory','3g','--memory-swap','3g','--tmpfs','/tmp:rw,nosuid,nodev,size=512m',
             '-v',str(root)+':/publication','-v',str(base['topic_vectors'])+':/topic-vectors.json:ro']
    internal={}
    access=':ro' if action=='plan' else ''
    for key,path in base.items():
        if key=='topic_vectors':continue
        path=Path(path)
        if key in ['topics','index']:
            command+=['-v',str(path)+':/live/'+key+access];internal[key]='/live/'+key
        elif key=='embeddings':
            command+=['-v',str(path)+':/original-embeddings.sqlite3:ro'];internal[key]='/original-embeddings.sqlite3'
        else:
            command+=['-v',str(path.parent)+':/live/'+key+access];internal[key]='/live/'+key+'/'+path.name
    write(root/'inputs.json',internal)
    command+=['--entrypoint','python',state['image'],'/app/operations/refresh_publish_data.py',action]
    with (root/'run.log').open('a') as out:
        subprocess.run(command,stdout=out,stderr=subprocess.STDOUT,check=True)


def close_site(state):
    root=Path(state['root']);before=(root/'Caddyfile.before').read_text()
    current=deploy.sha('/opt/wanted/Caddyfile')
    if current==state['caddy_before_sha']:deploy.caddy_reload(gate_content(before),current)
    elif current!=state['caddy_gate_sha']:raise RuntimeError('Proxy configuration changed; refusing to overwrite it')
    shared=Path(state['shared']);deploy.pause_dispatch(shared,False)
    deploy.wait_drained(shared,time.monotonic()+180)
    deploy.stop_stack(root/'compose.before.json')
    for c in state['consumers']:
        if c['project']==state['project'] and c['service'] in {'atlas','vector','luna'}:continue
        check=deploy.run(['docker','inspect','--format','{{.State.Running}}',c['id']],check=False)
        if check.returncode==0 and check.stdout.strip()=='true':deploy.run(['docker','stop','-t','90',c['id']],timeout=120)
    if consumers(state['base']):raise RuntimeError('A database consumer is still running')


def start_and_validate(state,updated):
    root=Path(state['root']);config=json.loads((root/('compose.updated.json' if updated else 'compose.before.json')).read_text())
    write(refresh.REGISTRY,config)
    deploy.run(['docker','compose','-f',str(refresh.REGISTRY),'up','-d','--wait','--wait-timeout','240'],timeout=360)
    # Reopen every prior reader before checking identities; startup must not invalidate them.
    for c in state['consumers']:
        if c['project']==state['project']:continue
        deploy.run(['docker','start',c['id']],timeout=120)
    port=int(config['services']['atlas']['ports'][0]['published'])
    results=[deploy.request(port,p) for p in ['/health/ready','/api/chat/status','/api/ontology/topics','/api/catalog?q=population']]
    if any(r['status']!=200 for r in results) or not all(r.get('ready') for r in results[:2]):raise RuntimeError('Maintenance service verification failed')
    if not results[-1].get('returned_records'):raise RuntimeError('Existing keyword search returned no records')
    write(root/('live-check.json' if updated else 'recovery-check.json'),{'passed':True,'results':results})
    return results


def reopen(state):
    deploy.resume_dispatch(Path(state['shared']))
    current=deploy.sha('/opt/wanted/Caddyfile')
    if current==state['caddy_gate_sha']:
        deploy.caddy_reload((Path(state['root'])/'Caddyfile.before').read_text(),current)
    elif current!=state['caddy_before_sha']:raise RuntimeError('Proxy configuration changed during maintenance')


def acknowledge(state):
    root=Path(state['root'])
    with closing(sqlite3.connect(ROOT/'embeddings.sqlite3',timeout=60)) as db,closing(sqlite3.connect(root/'delta.sqlite3')) as delta,db:
        for ident,h in delta.execute('SELECT dataset_id,content_hash FROM changes'):
            db.execute("UPDATE changes SET state='published' WHERE dataset_id=? AND content_hash=? AND state='ready'",(ident,h))
        db.execute("INSERT OR REPLACE INTO state VALUES('last_publication_day',?)",(json.dumps(state['day']),))
    save(state,'published',finished_at=time.time(),downtime_seconds=round(time.time()-state['closed_at'],1),result=json.loads((root/'build-result.json').read_text()))
    for p in (ROOT/'maintenance').iterdir():
        if p!=root and not p.is_symlink() and (p/'maintenance.json').exists() and json.loads((p/'maintenance.json').read_text()).get('stage') in TERMINAL:
            try:shutil.rmtree(p)
            except OSError:save(state,'published',cleanup_pending=str(p))


def unacknowledge(state):
    with closing(sqlite3.connect(ROOT/'embeddings.sqlite3',timeout=60)) as db,closing(sqlite3.connect(Path(state['root'])/'delta.sqlite3')) as delta,db:
        for ident,h in delta.execute('SELECT dataset_id,content_hash FROM changes'):
            db.execute("UPDATE changes SET state='ready' WHERE dataset_id=? AND content_hash=? AND state='published'",(ident,h))


def recover(state):
    root=Path(state['root'])
    if 'closed_at' not in state:
        save(state,'aborted',finished_at=time.time());return
    # A completed, verified publication can finish reopening after a reboot.
    if state['stage']=='validated':
        try:
            close_site(state);start_and_validate(state,True);reopen(state);acknowledge(state);return
        except Exception:pass
    close_site(state)
    if (root/'apply-started').exists():
        builder(state,'rollback')
        if (root/'postgres-before.json').is_file():postgres_publish.apply(root,state['base'],rollback=True)
    start_and_validate(state,False);unacknowledge(state);reopen(state)
    save(state,'rolled_back',finished_at=time.time())


def prepare(db,configuration,previous):
    root=ROOT/'maintenance'/str(time.time_ns());root.mkdir(parents=True)
    for folder in [root.parent,ROOT]:
        fd=os.open(folder,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    count=refresh.freeze(db,root/'delta.sqlite3')
    if not count:shutil.rmtree(root);return None
    config=json.loads(refresh.REGISTRY.read_text());base=refresh.inputs(config)
    gen=base['index']/(base['index']/'CURRENT').read_text().strip()
    files=[base['catalog'],base['keyword'],base['coverage'],base['topics']/'confidence.sqlite3',gen/'mapping.sqlite3']
    if (gen/'updates.sqlite3').exists():files.append(gen/'updates.sqlite3')
    # Worst single-DB recovery journal, changed metadata/vectors and two ANN copies.
    delta_bytes=(root/'delta.sqlite3').stat().st_size
    required=int(shutil.disk_usage(ROOT).total*.17)+max(p.stat().st_size for p in files)+delta_bytes*5+2*(gen/'vectors.faiss').stat().st_size+1024**3
    free=shutil.disk_usage(ROOT).free
    if free<required:
        shutil.rmtree(root);write(ROOT/'publication-status.json',{'stage':'insufficient_maintenance_headroom','free_bytes':free,'required_free_bytes':required});return None
    for p in files:
        wal=Path(str(p)+'-wal')
        if wal.exists() and wal.stat().st_size:raise RuntimeError('Publication inputs contain live WAL; maintenance not started')
    write(root/'compose.before.json',config)
    updated=json.loads(json.dumps(config));updated['services']['vector']['image']=configuration['services']['publisher']['image']
    write(root/'compose.updated.json',updated)
    proxy=Path('/opt/wanted/Caddyfile');(root/'Caddyfile.before').write_bytes(proxy.read_bytes());(root/'Caddyfile.before').chmod(0o600)
    gated=gate_content(proxy.read_text())
    # Validate the maintenance response before disabling any live service.
    result=deploy.run(['docker','exec','-i','wanted-atlas-caddy-1','caddy','validate','--config','/dev/stdin','--adapter','caddyfile'],data=gated,check=False)
    if result.returncode:raise ValueError('Maintenance proxy configuration is invalid')
    import hashlib
    state={'root':str(root),'day':datetime.now(ZoneInfo('Asia/Seoul')).date().isoformat(),'records':count,
           'base':{k:str(v) for k,v in base.items()},'project':config['name'],'image':configuration['services']['publisher']['image'],
           'shared':str(deploy.bind_source(config,'atlas','/harness-shared')),'consumers':consumers(base),
           'caddy_before_sha':deploy.sha(proxy),'caddy_gate_sha':hashlib.sha256(gated.encode()).hexdigest(),
           'previous_root':previous.get('root') if previous else None,'free_bytes':free,'required_free_bytes':required}
    for name in ['compose.before.json','compose.updated.json','Caddyfile.before','delta.sqlite3']:
        with (root/name).open('rb') as f:os.fsync(f.fileno())
    save(state,'planning')
    postgres_publish.capture(root)
    return state


def main():
    configuration=json.loads(refresh.IMAGES.read_text())
    old=json.loads(CURRENT.read_text()) if CURRENT.exists() else None
    pending=old and old.get('stage') not in TERMINAL
    if not pending and (not configuration.get('publication_enabled') or not within_window()):return
    with ExitStack() as stack:
        for p in [ROOT/'driver.lock',Path('/opt/wanted/operations/deploy.lock'),ROOT/'worker.lock']:
            lock=stack.enter_context(p.open('a'))
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                if pending:raise RuntimeError('Interrupted maintenance is waiting for the worker lock')
                write(ROOT/'publication-status.json',{'stage':'deferred_worker_busy','retry':'next 04:00 KST window','checked_at':time.time()});return
        if pending:
            try:recover(old)
            except Exception as e:save(old,'recovery_required',error_type=type(e).__name__);raise
            return
        today=datetime.now(ZoneInfo('Asia/Seoul')).date().isoformat()
        if old and old.get('day')==today:return
        with closing(sqlite3.connect(ROOT/'embeddings.sqlite3',timeout=60)) as db:
            state=prepare(db,configuration,old)
        if state is None:return
        try:
            builder(state,'plan');save(state,'closing',closed_at=time.time())
            close_site(state);save(state,'preparing_undo')
            builder(state,'prepare');save(state,'applying')
            refresh.check_frozen_quality(Path(state['root'])/'delta.sqlite3')
            builder(state,'apply')
            postgres_publish.apply(Path(state['root']),state['base'])
            save(state,'verifying_services')
            start_and_validate(state,True);save(state,'validated')
            reopen(state);acknowledge(state)
        except Exception as e:
            save(state,'recovering',error_type=type(e).__name__)
            try:recover(state)
            except Exception as recovery_error:save(state,'recovery_required',recovery_error_type=type(recovery_error).__name__);raise


def stop():
    out=deploy.run(['docker','inspect','--format','{{.State.Running}}','dataieum-maintenance'],check=False)
    if out.returncode==0 and out.stdout.strip()=='true':deploy.run(['docker','stop','-t','30','dataieum-maintenance'],timeout=45)


if __name__=='__main__':stop() if sys.argv[1:]==['stop'] else main()
