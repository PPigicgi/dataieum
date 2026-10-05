"""Restricted CI ingress and durable, serialized automatic deployment."""
from contextlib import ExitStack
import fcntl
import importlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

import deploy

ROOT=Path('/opt/wanted/auto-deploy')
OPS=Path('/opt/wanted/operations')
REGISTRY=Path('/opt/dataieum-releases/20260919-ontology-10/compose.active.json')
REFRESH=OPS/'refresh-images.json'
RELEASES=Path('/opt/dataieum-releases')
ROLES=('atlas','vector','luna','refresh')
TERMINAL={'complete','rolled_back','failed','recovery_required'}
HOST_FILES=('deploy.py','cleanup.py','refresh_run.py','refresh_maintenance.py','refresh_state.py',
    'postgres_publish.py','postgres_migrate.py','postgres.sql','index_lookup.py','backup_export.py',
    'refresh_http.py','refresh_collect.py','refresh_batch.py','refresh_quality.py','refresh.py','ci_deploy.py')

def parse_command(value):
    parts=value.split()
    if len(parts)==2 and parts[0]=='status' and re.fullmatch(r'[1-9][0-9]{0,19}',parts[1]):return parts
    if len(parts)==3 and parts[0] in {'load','submit'} and re.fullmatch(r'[a-f0-9]{40}',parts[1]):
        if parts[0]=='load' and parts[2] in ROLES:return parts
        if parts[0]=='submit' and re.fullmatch(r'[1-9][0-9]{0,19}',parts[2]):return parts
    raise ValueError('Unsupported deployment command')

def inspect(image):
    d=json.loads(deploy.run(['docker','image','inspect',image]).stdout)[0]
    return {k:d[k] for k in ('Id','Architecture','Os','RootFS')}

def identities(revision,images):
    if set(images)!=set(ROLES):raise ValueError('Four tested images required')
    for role,image in images.items():
        if not re.fullmatch(r'sha256:[a-f0-9]{64}',image):raise ValueError('Invalid immutable image')
        found=inspect('ghcr.io/manzigit/wanted-'+role+':'+revision)
        if found['Id']!=image or found['Architecture']!='amd64' or found['Os']!='linux':raise ValueError('Revision image differs')

def load_image(revision,role,stream):
    if shutil.disk_usage(ROOT).free<8*1024**3:raise RuntimeError('Insufficient image headroom')
    with tempfile.TemporaryFile(dir=ROOT) as archive:
        total=0
        while chunk:=stream.read(1024**2):
            total+=len(chunk)
            if total>2*1024**3:raise ValueError('Image archive too large')
            archive.write(chunk)
        archive.seek(0)
        subprocess.run(['docker','load','--quiet'],stdin=archive,stdout=subprocess.DEVNULL,check=True,timeout=300)
    return inspect('ghcr.io/manzigit/wanted-'+role+':'+revision)

def read_state(run):
    path=ROOT/run/'status.json'
    return json.loads(path.read_text()) if path.is_file() else {'stage':'absent'}

def save(job,stage,**extra):
    path=job/'status.json'
    d=json.loads(path.read_text()) if path.exists() else {}
    d.update(stage=stage,updated_at=time.time(),**extra);deploy.write_json(path,d)
    return d

def unit(run):return 'dataieum-auto-deploy-'+run

def submit(revision,run,stream):
    raw=stream.read(8193)
    if len(raw)>8192:raise ValueError('Manifest too large')
    manifest=json.loads(raw)
    if manifest.get('revision')!=revision or str(manifest.get('run_id'))!=run:raise ValueError('Manifest identity differs')
    identities(revision,manifest['images'])
    with (ROOT/'ingress.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        job=ROOT/run;job.mkdir(mode=0o700,exist_ok=True)
        path=job/'manifest.json'
        if path.exists() and json.loads(path.read_text())!=manifest:raise ValueError('Run is immutable')
        deploy.write_json(path,manifest)
        state=read_state(run)
        if state['stage'] in TERMINAL:return state
        if state['stage']=='absent':save(job,'queued',revision=revision,run_id=int(run))
        active=subprocess.run(['systemctl','is-active','--quiet',unit(run)],capture_output=True)
        if active.returncode:
            subprocess.run(['systemctl','reset-failed',unit(run)],capture_output=True)
            subprocess.run(['systemd-run','--quiet','--collect','--unit',unit(run),
                '--property=Restart=on-failure','--property=RestartSec=60','--property=StartLimitBurst=3',
                '--property=StartLimitIntervalSec=600','--property=TimeoutStartSec=infinity',
                '/usr/bin/python3',str(OPS/'ci_deploy.py'),'execute',run],check=True)
        return read_state(run)

def locks(job,stack,timeout=43200):
    end=time.monotonic()+timeout
    handles=[stack.enter_context(p.open('a')) for p in (Path('/opt/wanted/refresh/driver.lock'),OPS/'deploy.lock')]
    while True:
        acquired=[]
        try:
            for handle in handles:
                fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB);acquired.append(handle)
            return
        except BlockingIOError:
            for handle in acquired:fcntl.flock(handle,fcntl.LOCK_UN)
            if time.monotonic()>end:raise TimeoutError('Deployment waits for active collection or maintenance')
            save(job,'waiting_for_worker');time.sleep(30)

def port():
    for number in range(18100,18130):
        with socket.socket() as sock:
            try:sock.bind(('127.0.0.1',number))
            except OSError:continue
            return number
    raise RuntimeError('No candidate port')

def install_host_code(job,image):
    # Never copy runtime credentials/configuration or replace them with image defaults.
    staged=job/'host-new';previous=job/'host-before'
    if not (job/'host-captured').exists():
        staged.mkdir(exist_ok=True);previous.mkdir(exist_ok=True)
        container=deploy.run(['docker','create','--entrypoint','/bin/true',image]).stdout.strip()
        try:
            for name in HOST_FILES:
                deploy.run(['docker','cp',container+':/app/operations/'+name,str(staged/name)])
                p=staged/name
                if p.is_symlink() or not p.is_file() or p.stat().st_size>1024**2:raise ValueError('Invalid host code')
                if name.endswith('.py'):compile(p.read_text(),name,'exec')
                if (OPS/name).exists():shutil.copyfile(OPS/name,previous/name)
            (job/'host-captured').touch()
        finally:deploy.run(['docker','rm',container])
    for name in HOST_FILES:
        temporary=OPS/(name+'.ci-tmp');shutil.copyfile(staged/name,temporary);temporary.chmod(0o644);temporary.replace(OPS/name)
    importlib.reload(deploy)

def restore_host_code(job):
    if not (job/'host-captured').exists():return
    for name in HOST_FILES:
        previous=job/'host-before'/name
        if previous.exists():
            temporary=OPS/(name+'.ci-tmp');shutil.copyfile(previous,temporary);temporary.chmod(0o644);temporary.replace(OPS/name)
        else:(OPS/name).unlink(missing_ok=True)
    importlib.reload(deploy)

def verify_live(root):
    state=json.loads((root/'deployment.json').read_text());proof={'passed':False,'attempts':[]}
    # Switching and Docker cleanup briefly compete for host CPU. Record bounded
    # attempts, as candidate validation does; persistent failures still roll back.
    for attempt in range(4):
        checks=[];ready={}
        try:
            ready=deploy.request(state['port'],'/health/ready')
            if ready.get('status')!=200 or not ready.get('ready'):raise RuntimeError('Serving readiness failed')
            for domain in ('dataieum.com','manzihub.com'):
                with urllib.request.urlopen('https://'+domain+'/api/catalog/page?q=population',timeout=30) as response:
                    body=json.load(response)
                    if response.status!=200 or not body.get('datasets'):raise RuntimeError('Public catalogue failed')
                    checks.append({'domain':domain,'status':response.status,'records':len(body['datasets'])})
        except (OSError,TimeoutError,ValueError,RuntimeError) as error:
            proof['attempts'].append({'ready':ready,'checks':checks,'error_type':type(error).__name__})
            deploy.write_json(root/'live-check.json',proof)
            if attempt==3:raise
            time.sleep(2**attempt)
        else:
            proof['attempts'].append({'ready':ready,'checks':checks})
            proof.update(passed=True,checks=checks);deploy.write_json(root/'live-check.json',proof)
            return checks


def execute(run):
    job=ROOT/run
    if read_state(run)['stage'] in TERMINAL:return read_state(run)
    manifest=json.loads((job/'manifest.json').read_text());revision=manifest['revision']
    root=RELEASES/('dataieum-'+revision[:12]+'-'+run)
    with ExitStack() as stack:
        locks(job,stack)
        try:
            # Do not replace an unfinished data publication or its recovery.
            deploy.maintenance_guard()
            identities(revision,manifest['images'])
            latest=ROOT/'last-success.json'
            if latest.exists() and int(json.loads(latest.read_text())['run_id'])>int(run):
                return save(job,'failed',reason='superseded')
            save(job,'preparing',release_root=str(root))
            install_host_code(job,manifest['images']['atlas'])
            if not (job/'refresh-before.json').exists():shutil.copyfile(REFRESH,job/'refresh-before.json')
            if not (root/'deployment.json').exists():
                deploy.prepare(root,REGISTRY,port(),manifest['images'])
            state=json.loads((root/'deployment.json').read_text())
            if state['stage']=='prepared':
                save(job,'validating')
                deploy.run(['docker','compose','-f',str(root/'compose.json'),'up','-d','--wait','--wait-timeout','180'],timeout=300)
                deploy.validate(root)
                images=json.loads(REFRESH.read_text())
                images['services']['collector']['image']=manifest['images']['refresh']
                images['services']['publisher']['image']=manifest['images']['vector']
                deploy.write_json(REFRESH,images)  # Preserve publication_enabled and scheduling policy.
                save(job,'promoting');state=deploy.promote(root)
            while state['stage']=='draining_previous':
                save(job,'draining_previous')
                try:state=deploy.finish(root)
                except TimeoutError:time.sleep(15)
            if state['stage']!='active':raise RuntimeError('Deployment requires recovery: '+state['stage'])
            save(job,'checking_live')
            checks=verify_live(root)
            state=save(job,'complete',checks=checks,completed_at=time.time())
            deploy.write_json(latest,state)
            return state
        except Exception as error:
            save(job,'recovering',error_type=type(error).__name__)
            try:
                path=root/'deployment.json'
                if path.exists():
                    state=json.loads(path.read_text())
                    if state['stage'] in {'active','draining_previous','rollback_draining_current'}:
                        deploy.rollback(root)
                    elif state['stage']=='prepared':
                        if deploy.sha('/opt/wanted/Caddyfile')!=state['caddy_previous_sha256']:
                            raise RuntimeError('Proxy changed before checkpoint; preserve both stacks')
                        deploy.run(['docker','compose','-f',str(root/'compose.json'),'down'],timeout=300)
                if (job/'refresh-before.json').exists():deploy.write_json(REFRESH,json.loads((job/'refresh-before.json').read_text()))
                restore_host_code(job)
                rolled=path.exists() and json.loads(path.read_text())['stage']=='rolled_back'
                return save(job,'rolled_back' if rolled else 'failed',error_type=type(error).__name__)
            except Exception as recovery:
                return save(job,'recovery_required',error_type=type(error).__name__,recovery_error=type(recovery).__name__)

def main():
    ROOT.mkdir(mode=0o700,exist_ok=True)
    if sys.argv[1:]==['recover']:
        for path in sorted(ROOT.glob('*/manifest.json')):
            manifest=json.loads(path.read_text());run=path.parent.name
            if read_state(run)['stage'] not in TERMINAL:
                submit(manifest['revision'],run,io.BytesIO(json.dumps(manifest).encode()))
        return
    if sys.argv[1:]:
        if len(sys.argv)!=3 or sys.argv[1]!='execute' or not re.fullmatch(r'[1-9][0-9]{0,19}',sys.argv[2]):raise ValueError('Invalid service arguments')
        result=execute(sys.argv[2])
    else:
        args=parse_command(os.environ.get('SSH_ORIGINAL_COMMAND',''))
        if args[0]=='status':result=read_state(args[1])
        elif args[0]=='load':result=load_image(args[1],args[2],sys.stdin.buffer)
        else:result=submit(args[1],args[2],sys.stdin.buffer)
    print(json.dumps(result),flush=True)

if __name__=='__main__':main()
