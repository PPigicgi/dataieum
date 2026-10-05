"""Prepare, validate and promote one isolated release; preserve durable chat jobs."""
import argparse
import copy
from contextlib import closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def run(args, *, data=None, check=True, timeout=240):
    result = subprocess.run(args, input=data, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        # Commands can carry mounted credential paths. Retain diagnostics locally.
        raise RuntimeError(f'{args[0]} {args[1] if len(args)>1 else ""} failed ({result.returncode}): {result.stderr[-1200:]}')
    return result


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    path = Path(path); tmp=path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value,indent=2));tmp.chmod(0o600);tmp.replace(path)


def service_containers(compose):
    return run(['docker','compose','-f',str(compose),'ps','-aq']).stdout.split()


def owned_directory(path, reference):
    path.mkdir(parents=True, exist_ok=True)
    s=reference.stat();os.chown(path,s.st_uid,s.st_gid);path.chmod(0o770)


def bind_source(config, service, target):
    matches=[Path(m['source']) for m in config['services'][service].get('volumes',[])
             if isinstance(m,dict) and m.get('type')=='bind' and m.get('target')==target]
    if len(matches)!=1:raise ValueError('Expected one source for '+target)
    return matches[0]


def maintenance_guard():
    marker=Path('/opt/wanted/refresh/maintenance-current.json')
    if marker.exists() and json.loads(marker.read_text()).get('stage') not in {'published','rolled_back','aborted'}:
        raise RuntimeError('Finish maintenance recovery before changing the serving release')


def prepare(root, old, port, images=None):
    maintenance_guard()
    if (root/'deployment.json').exists():raise RuntimeError('Release already prepared')
    images=images or {}
    if shutil.disk_usage('/').free < 8*1024**3:raise RuntimeError('Insufficient publication headroom')
    with socket.socket() as s:s.bind(('127.0.0.1',port))
    old=old.resolve();previous=json.loads(old.read_text())
    control=bind_source(previous,'atlas','/harness-control')
    workspace=bind_source(previous,'atlas','/harness-workspace')
    luna_state=bind_source(previous,'luna','/luna-state')
    shared_mounts=[m for m in previous['services']['atlas']['volumes'] if m.get('target')=='/harness-shared']
    name=root.name;shared=Path(shared_mounts[0]['source']) if shared_mounts else control
    ensure_dispatch_lock(shared)
    root.mkdir(parents=True,exist_ok=True)
    for folder,reference in [('control',control),('workspace',workspace),('luna-state',luna_state)]:
        owned_directory(root/folder,reference)
    # New controls are private. Mutable chat/intro databases retain one canonical path.
    shutil.copyfile(bind_source(previous,'atlas','/harness-config/policy.json'),root/'policy.json')
    shutil.copyfile(luna_state/'vector.json',root/'luna-state/vector.json')
    os.chown(root/'luna-state/vector.json',luna_state.stat().st_uid,luna_state.stat().st_gid)
    config=copy.deepcopy(previous);config['name']=name
    config['networks']['private']={'name':name+'-private'}
    for service,spec in config['services'].items():
        spec.pop('network_mode',None);spec.pop('depends_on',None)
        spec['networks']={'private':{'aliases':[service]}}
        if images.get(service):
            spec['image']=run(['docker','image','inspect','--format','{{.Id}}',
                images[service]]).stdout.strip()
        for mount in spec.get('volumes',[]):
            target=mount.get('target')
            if service=='atlas' and target in {'/harness-control','/harness-workspace'}:
                mount['source']=str(root/('control' if target=='/harness-control' else 'workspace'))
            if target=='/harness-config/policy.json':mount['source']=str(root/'policy.json')
            if service=='luna' and target=='/luna-state':mount['source']=str(root/'luna-state')
    app=config['services']['atlas'];app['networks']['web']={'aliases':[name]}
    app['ports']=[{'host_ip':'127.0.0.1','published':str(port),'target':8000,'protocol':'tcp'}]
    app['volumes']=[m for m in app['volumes'] if m.get('target')!='/harness-shared']
    app['volumes'].append({'type':'bind','source':str(shared),'target':'/harness-shared'})
    app['environment'].update(DATAIEUM_RELEASE=name,DATAIEUM_CHAT_GATEWAY='http://luna:8092',
        DATAIEUM_KEYWORD_INDEX='/harness-shared/catalog-search.sqlite3',
        DATAIEUM_INTRO_DB='/harness-shared/dataset-intros.sqlite3',
        DATAIEUM_JOBS_PATH='/harness-shared/chat-jobs.sqlite3',
        DATAIEUM_JOB_DISPATCH_LOCK='/harness-shared/dispatcher.lock',
        DATAIEUM_JOB_DISPATCH_PAUSE='/harness-shared/dispatcher.pause')
    luna=config['services']['luna'];args=luna['command']
    args[args.index('--catalog-url')+1]='http://atlas:8000'
    args[args.index('--instance')+1]=name
    if '--host' in args:args[args.index('--host')+1]='0.0.0.0'
    else:args.extend(['--host','0.0.0.0'])
    luna.setdefault('environment',{})['DATAIEUM_VECTOR_URL']='http://vector:8093'
    luna['depends_on']={'atlas':{'condition':'service_healthy'},'vector':{'condition':'service_healthy'}}
    vector=config['services']['vector'];args=vector['command'];args[args.index('--host')+1]='0.0.0.0'
    # One canonical metadata store, with immutable SQLite/FAISS read projections.
    # Credentials are runtime files, never copied into images or Compose values.
    pg=Path('/opt/wanted/postgres')
    if (pg/'publication.json').is_file():
        config['networks']['database']={'name':'dataieum-db','external':True}
        app['networks']['database']={}
        for target,filename in [('/run/dataieum-postgres.json','reader.json'),('/run/dataieum-publication.json','publication.json')]:
            app['volumes']=[m for m in app['volumes'] if m.get('target')!=target]
            app['volumes'].append({'type':'bind','source':str(pg/filename),'target':target,'read_only':True})
        app['environment'].update(DATAIEUM_POSTGRES_CONFIG='/run/dataieum-postgres.json',DATAIEUM_PUBLICATION_FILE='/run/dataieum-publication.json')
    vector['volumes']=[m for m in vector['volumes'] if m.get('target')!='/catalog-search.sqlite3']
    keyword_mounts=[m['source'] for m in previous['services']['atlas']['volumes'] if m.get('target')=='/harness-shared/catalog-search.sqlite3']
    vector['volumes'].append({'type':'bind','source':str(keyword_mounts[0] if keyword_mounts else shared/'catalog-search.sqlite3'),
        'target':'/catalog-search.sqlite3','read_only':True})
    vector.setdefault('environment',{})['DATAIEUM_KEYWORD_INDEX']='/catalog-search.sqlite3'
    legacy='DATAIEUM_JOB_DISPATCH_LOCK' not in previous['services']['atlas'].get('environment',{})
    if legacy:
        (shared/'dispatcher.pause').touch(mode=0o644,exist_ok=True)
    caddy=Path('/opt/wanted/Caddyfile')
    (root/'Caddyfile.previous').write_bytes(caddy.read_bytes());(root/'Caddyfile.previous').chmod(0o600)
    write_json(root/'compose.json',config)
    shutil.copyfile(old,root/'compose.previous.json');(root/'compose.previous.json').chmod(0o600)
    state={'stage':'prepared','name':name,'port':port,'previous_compose':str(root/'compose.previous.json'),
           'active_registry':str(old),
           'previous_sha256':sha(old),'caddy_previous_sha256':sha(caddy),'shared':str(shared),
           'legacy_dispatcher':legacy,'created_at':time.time()}
    write_json(root/'deployment.json',state)
    run(['docker','compose','-f',str(root/'compose.json'),'config','--quiet'])
    return state


def request(port,path,timeout=30):
    start=time.monotonic()
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}'+path,timeout=timeout) as r:
            raw=r.read(2*1024**2)
            result={'path':path,'status':r.status,'seconds':round(time.monotonic()-start,3),'bytes':len(raw)}
            if 'json' in r.headers.get('Content-Type',''):
                obj=json.loads(raw)
                if isinstance(obj,dict):
                    result.update({k:obj[k] for k in ['ready','total','page','pages','code','state'] if k in obj})
                    if isinstance(obj.get('datasets'),list):result['returned_records']=len(obj['datasets'])
            return result
    except urllib.error.HTTPError as e:
        return {'path':path,'status':e.code,'seconds':round(time.monotonic()-start,3),
                'body':e.read(2000).decode(errors='replace')}


def validate(root):
    state=json.loads((root/'deployment.json').read_text());port=state['port']
    queries=['population','인구','人口','Bevölkerung','this-query-should-match-no-dataset-734181']
    paths=['/health/ready','/api/bootstrap','/api/chat/status','/api/ontology/topics']
    paths += ['/api/catalog/page?'+urllib.parse.urlencode({'q':q}) for q in queries]
    paths += ['/api/catalog?q=population']
    results=[]
    for path in paths:
        # Mirror the bounded retry policy used by the UI. Keep every attempt
        # visible in the proof; persistent 503s still fail promotion.
        attempts=[]
        for attempt in range(4):
            result=request(port,path);attempts.append(result)
            if result['status']!=503 or attempt==3:break
            time.sleep(2**attempt)
        results.append({**result,'attempts':attempts})
        write_json(root/'validation.json',{'passed':False,'results':results})
        if results[-1]['status']!=200:
            raise RuntimeError('Candidate real request failed: '+path)
    # Health cannot substitute for a functioning catalogue or gateway.
    if not results[0].get('ready') or not results[2].get('ready'):
        raise RuntimeError('Candidate services are not ready')
    write_json(root/'validation.json',{'passed':True,'results':results,'validated_at':time.time(),
                                      'compose_sha256':sha(root/'compose.json'),
                                      'policy_sha256':sha(root/'policy.json')})
    return results


def replace_upstream(config, previous, target):
    pattern=r'(?m)^(\s*reverse_proxy\s+)'+re.escape(previous)+r'(?=\s*\{)'
    changed,n=re.subn(pattern,lambda m:m.group(1)+target,config)
    if n!=1:raise ValueError('Expected exactly one current upstream')
    return changed


def caddy_reload(content, expected):
    path=Path('/opt/wanted/Caddyfile')
    if sha(path)!=expected:raise RuntimeError('Proxy configuration changed concurrently')
    # Validate inside the existing proxy environment without exposing auth content.
    result=run(['docker','exec','-i','wanted-atlas-caddy-1','caddy','validate',
                '--config','/dev/stdin','--adapter','caddyfile'],data=content,check=False)
    if result.returncode:raise RuntimeError('Candidate proxy configuration is invalid')
    previous=path.read_bytes()
    with path.open('r+b') as f:
        f.seek(0);f.write(content.encode());f.truncate();f.flush();os.fsync(f.fileno())
    result=run(['docker','exec','wanted-atlas-caddy-1','caddy','reload','--config','/etc/caddy/Caddyfile',
                '--adapter','caddyfile'],check=False)
    if result.returncode:
        with path.open('r+b') as f:
            f.seek(0);f.write(previous);f.truncate();f.flush();os.fsync(f.fileno())
        run(['docker','exec','wanted-atlas-caddy-1','caddy','reload','--config','/etc/caddy/Caddyfile','--adapter','caddyfile'])
        raise RuntimeError('Proxy reload failed; previous configuration restored')
    return sha(path)


def job_count(path,status='running'):
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        return db.execute('SELECT count(*) FROM chat_jobs WHERE status=?',(status,)).fetchone()[0]


def ensure_dispatch_lock(shared):
    # The deploy command runs as root; the application uses the database owner.
    # Keep the same inode so existing flock ownership cannot be bypassed.
    owner=(shared/'chat-jobs.sqlite3').stat()
    with (shared/'dispatcher.lock').open('a') as lock:
        os.fchown(lock.fileno(),owner.st_uid,owner.st_gid)
        os.fchmod(lock.fileno(),0o600)


def pause_dispatch(shared,legacy):
    ensure_dispatch_lock(shared)
    (shared/'dispatcher.pause').touch(mode=0o644,exist_ok=True)
    if legacy:
        # Initial migration only: old code has no dispatcher gate. Freeze claims,
        # not submissions or finishes. The trigger is removed after the old worker exits.
        with closing(sqlite3.connect(shared/'chat-jobs.sqlite3',timeout=5)) as db, db:
            db.executescript("CREATE TRIGGER IF NOT EXISTS deployment_claim_barrier "
                "BEFORE UPDATE OF status ON chat_jobs WHEN OLD.status='queued' AND NEW.status='running' "
                "BEGIN SELECT RAISE(ABORT,'dispatcher_handover'); END;")


def drop_legacy_barrier(shared):
    with closing(sqlite3.connect(shared/'chat-jobs.sqlite3',timeout=5)) as db, db:
        db.execute('DROP TRIGGER IF EXISTS deployment_claim_barrier')


def resume_dispatch(shared):
    drop_legacy_barrier(shared)
    (shared/'dispatcher.pause').unlink(missing_ok=True)


def wait_drained(shared,deadline):
    ensure_dispatch_lock(shared)
    while time.monotonic()<deadline:
        if job_count(shared/'chat-jobs.sqlite3')==0:
            with (shared/'dispatcher.lock').open('a') as f:
                try:fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:pass
                else:return
        time.sleep(1)
    raise TimeoutError('Active jobs are still draining; both services remain running')


def promote(root):
    maintenance_guard()
    state=json.loads((root/'deployment.json').read_text());shared=Path(state['shared'])
    if state['stage']!='prepared':raise RuntimeError('Promotion requires a prepared release')
    proof=json.loads((root/'validation.json').read_text())
    if (not proof['passed'] or proof['compose_sha256']!=sha(root/'compose.json')
            or proof.get('policy_sha256')!=sha(root/'policy.json')
            or time.time()-proof['validated_at']>1800):
        raise RuntimeError('Candidate validation is absent or stale')
    previous=Path(state['previous_compose'])
    if sha(previous)!=state['previous_sha256'] or sha(state['active_registry'])!=state['previous_sha256']:
        raise RuntimeError('Serving release changed')
    old_config=json.loads(previous.read_text())
    old_alias=old_config['services']['atlas']['networks']['web']['aliases'][0]+':8000'
    candidate=replace_upstream((root/'Caddyfile.previous').read_text(),old_alias,state['name']+':8000')
    state['caddy_active_sha256']=caddy_reload(candidate,state['caddy_previous_sha256'])
    state['stage']='draining_previous';write_json(root/'deployment.json',state)
    pause_dispatch(shared,state['legacy_dispatcher'])
    try:wait_drained(shared,time.monotonic()+120)
    except TimeoutError:
        # No forced cancellation. The explicit finish command safely resumes this step.
        return state
    return finish(root)


def finish(root):
    state=json.loads((root/'deployment.json').read_text());shared=Path(state['shared'])
    if state['stage']!='draining_previous':raise RuntimeError('Release is not draining')
    if sha('/opt/wanted/Caddyfile')!=state['caddy_active_sha256']:raise RuntimeError('Proxy changed')
    if sha(state['active_registry'])!=state['previous_sha256']:raise RuntimeError('Active registry changed')
    pause_dispatch(shared,state['legacy_dispatcher'])
    wait_drained(shared,time.monotonic()+120)
    stop_stack(state['previous_compose'])
    write_json(state['active_registry'],json.loads((root/'compose.json').read_text()))
    resume_dispatch(shared)
    state['active_registry_sha256']=sha(state['active_registry'])
    state['stage']='active';state['activated_at']=time.time();write_json(root/'deployment.json',state)
    try:state['cleanup']=cleanup_retired(root,state)
    except Exception as error:state['cleanup']={'stage':'failed','error_type':type(error).__name__}
    write_json(root/'deployment.json',state)
    return state


def stop_stack(compose):
    # Drain frontend HTTP while its dependencies remain available. Compose's
    # default reverse dependency shutdown would terminate Luna/vector first.
    run(['docker','compose','-f',str(compose),'stop','--timeout','90','atlas'],timeout=150)
    run(['docker','compose','-f',str(compose),'stop','--timeout','90','luna','vector'],timeout=240)


def cleanup_retired(root,state):
    """Keep the previous images/configuration; remove its stopped containers."""
    run(['docker','compose','-f',state['previous_compose'],'rm','-f'])
    out=root/'cleanup'
    args=['python3','/opt/wanted/operations/cleanup.py','--output',str(out),
          '--active',state['active_registry'],'--rollback',state['previous_compose']]
    recovery=Path('/opt/wanted/operations/cleanup-20260927/running-image-recovery.json')
    if recovery.exists():args += ['--recovery-map',str(recovery)]
    result=run(args,check=False)
    if result.returncode:return {'stage':'failed','step':'plan','exit_code':result.returncode}
    result=run(['python3','/opt/wanted/operations/cleanup.py','--output',str(out),
                '--apply-plan',str(out/'plan.json')],check=False)
    if result.returncode:return {'stage':'failed','step':'apply','exit_code':result.returncode}
    report=json.loads((out/'report.json').read_text())
    return {'stage':report['stage'],'images_removed':report['images_removed'],'report':str(out/'report.json')}


def rollback(root):
    maintenance_guard()
    state=json.loads((root/'deployment.json').read_text());shared=Path(state['shared'])
    if state['stage'] not in {'active','draining_previous','rollback_draining_current'}:
        raise RuntimeError('No published release to roll back')
    if state['stage']!='rollback_draining_current':
        if sha('/opt/wanted/Caddyfile')!=state['caddy_active_sha256']:raise RuntimeError('Proxy changed')
        pause_dispatch(shared,state['legacy_dispatcher'])
        try:wait_drained(shared,time.monotonic()+120)
        except TimeoutError:
            if state['stage']=='active':resume_dispatch(shared)
            raise
        # The current dispatcher remains paused while the previous version takes
        # sole ownership. Legacy workers cannot run behind the migration trigger.
        drop_legacy_barrier(shared)
        try:
            run(['docker','compose','-f',state['previous_compose'],'up','-d','--force-recreate'],timeout=300)
            old=json.loads(Path(state['previous_compose']).read_text())
            port=int(old['services']['atlas']['ports'][0]['published'])
            deadline=time.monotonic()+180
            while time.monotonic()<deadline:
                try:
                    ready=request(port,'/health/ready')
                    if ready['status']==200 and ready.get('ready'):break
                except (OSError,TimeoutError):pass
                time.sleep(2)
            else:raise RuntimeError('Previous service is not ready; current proxy retained')
            state['caddy_rollback_sha256']=caddy_reload(
                (root/'Caddyfile.previous').read_text(),state['caddy_active_sha256'])
        except Exception:
            # A failed rollback must not strand the serving release's queue or
            # resume two dispatchers. If draining fails, leave the gate closed.
            pause_dispatch(shared,state['legacy_dispatcher'])
            wait_drained(shared,time.monotonic()+120)
            stop_stack(state['previous_compose'])
            resume_dispatch(shared)
            raise
        state['stage']='rollback_draining_current';write_json(root/'deployment.json',state)
    if sha('/opt/wanted/Caddyfile')!=state['caddy_rollback_sha256']:raise RuntimeError('Proxy changed')
    stop_stack(root/'compose.json')
    write_json(state['active_registry'],json.loads(Path(state['previous_compose']).read_text()))
    resume_dispatch(shared)
    state.update(stage='rolled_back',rolled_back_at=time.time());write_json(root/'deployment.json',state)
    return state


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','start','validate','promote','finish','rollback','status'])
    p.add_argument('--root',type=Path,required=True);p.add_argument('--previous',type=Path);p.add_argument('--port',type=int,default=18091)
    p.add_argument('--atlas-image');p.add_argument('--luna-image');p.add_argument('--vector-image')
    a=p.parse_args();lockpath=Path('/opt/wanted/operations/deploy.lock')
    with lockpath.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if a.action=='prepare':value=prepare(a.root,a.previous,a.port,
            {'atlas':a.atlas_image,'luna':a.luna_image,'vector':a.vector_image})
        elif a.action=='start':
            run(['docker','compose','-f',str(a.root/'compose.json'),'up','-d','--wait',
                 '--wait-timeout','180'],timeout=300);value={'started':True}
        elif a.action=='validate':value=validate(a.root)
        elif a.action=='promote':value=promote(a.root)
        elif a.action=='finish':value=finish(a.root)
        elif a.action=='rollback':value=rollback(a.root)
        else:value=json.loads((a.root/'deployment.json').read_text())
        print(json.dumps(value),flush=True)


if __name__=='__main__':main()
