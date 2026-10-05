"""Ship verified CI images through a command-restricted deployment key."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time
from release import archive_identity,loaded_identity

ROLES=('atlas','vector','luna','refresh')

def main():
    p=argparse.ArgumentParser();p.add_argument('--artifacts',type=Path,required=True);p.add_argument('--revision',required=True);p.add_argument('--run',type=int,required=True);a=p.parse_args()
    if not re.fullmatch('[a-f0-9]{40}',a.revision) or a.run<1:raise ValueError('Invalid CI identity')
    ssh=['ssh','-T','-i',os.environ['DEPLOY_KEY_FILE'],'-o','IPQoS=none','-o','BatchMode=yes',
         '-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+os.environ['DEPLOY_KNOWN_HOSTS_FILE'],
         '-o','ServerAliveInterval=30','-o','ServerAliveCountMax=6','dataieum-deploy@'+os.environ['DEPLOY_HOST']]
    images={}
    for role in ROLES:
        folder=a.artifacts/('image-'+role)
        proofs=list(folder.rglob('image-'+role+'.json'));archives=list(folder.rglob('image.tar'))
        if len(proofs)!=1 or len(archives)!=1:raise RuntimeError('Missing CI artifact')
        proof=json.loads(proofs[0].read_text())
        if proof['target']!=role or not re.fullmatch('ghcr.io/manzigit/wanted-'+role+'@sha256:[a-f0-9]{64}',proof['image']):raise ValueError('Wrong image proof')
        expected=archive_identity(archives[0],proof['config_digest'])
        with archives[0].open('rb') as stream:
            result=subprocess.run([*ssh,'load '+a.revision+' '+role],stdin=stream,capture_output=True,text=True,check=True)
        images[role]=loaded_identity(json.loads(result.stdout),expected)
    body=json.dumps({'revision':a.revision,'run_id':a.run,'images':images})
    result=subprocess.run([*ssh,'submit '+a.revision+' '+str(a.run)],input=body,capture_output=True,text=True,check=True)
    state=json.loads(result.stdout)
    deadline=time.monotonic()+2400
    while state['stage'] not in {'complete','rolled_back','failed','recovery_required'}:
        print(json.dumps(state),flush=True)
        if time.monotonic()>deadline:
            raise TimeoutError('Server deployment continues asynchronously; inspect the durable run status')
        time.sleep(30)
        result=subprocess.run([*ssh,'status '+str(a.run)],capture_output=True,text=True,check=True)
        state=json.loads(result.stdout)
    print(json.dumps(state),flush=True)
    if state['stage']!='complete':raise RuntimeError('Deployment did not pass live verification')

if __name__=='__main__':main()
