"""Deploy successful GitHub Actions artifacts using the existing SSH identity.

No server key is uploaded to GitHub. Image configuration digests are checked
before the existing prepare/validate/promote gates run. Timers stay unchanged.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import tarfile

REPO='ManziGit/wanted'
def ssh_command():
    """Require operator-supplied SSH settings for the public code snapshot."""
    key = os.environ.get('DATAIEUM_SSH_KEY', '').strip()
    target = os.environ.get('DATAIEUM_SSH_TARGET', '').strip()
    if not key or not target or target.startswith('-'):
        raise RuntimeError('Set DATAIEUM_SSH_KEY and DATAIEUM_SSH_TARGET before remote operations')
    return ['ssh', '-i', str(Path(key).expanduser()), '-o', 'IPQoS=none',
            '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', target]


def run(args,**kwargs):return subprocess.run(args,check=True,capture_output=True,text=True,**kwargs)


def remote(args,**kwargs):return run([*ssh_command(),shlex.join(args)],**kwargs)


def archive_identity(path,config_digest):
    """Verify the OCI manifest/config chain before accepting Docker's image ID.

    Docker 29's containerd store identifies an image by its manifest; the classic
    store identifies the same image by its config. Both must resolve to the CI
    config and exact rootfs. No guessed or newly built image is accepted.
    """
    with tarfile.open(path) as archive:
        def read(name,digest=None):
            member=archive.getmember(name)
            if not member.isfile() or member.size>2*1024**2:raise RuntimeError('Oversized image metadata')
            raw=archive.extractfile(member).read()
            if digest and 'sha256:'+hashlib.sha256(raw).hexdigest()!=digest:raise RuntimeError('Image metadata hash differs')
            return json.loads(raw)
        config=read('blobs/sha256/'+config_digest.removeprefix('sha256:'),config_digest)
        if config.get('architecture')!='amd64' or config.get('os')!='linux':raise RuntimeError('Wrong image platform')
        index=read('index.json');matches=[]
        def visit(descriptor,depth=0):
            digest=descriptor.get('digest','')
            if depth>3 or not re.fullmatch('sha256:[a-f0-9]{64}',digest):raise RuntimeError('Invalid image descriptor')
            value=read('blobs/sha256/'+digest[7:],digest)
            if 'manifests' in value:
                if len(value['manifests'])>8:raise RuntimeError('Too many image manifests')
                for child in value['manifests']:visit(child,depth+1)
            elif value.get('config',{}).get('digest')==config_digest:matches.append(digest)
        for descriptor in index['manifests']:visit(descriptor)
        if len(set(matches))!=1:raise RuntimeError('CI config does not resolve to one image manifest')
        return {'manifest':matches[0],'config':config_digest,'layers':config['rootfs']['diff_ids']}


def loaded_identity(inspected,expected):
    if (inspected['Id'] not in {expected['config'],expected['manifest']} or
        inspected['RootFS']['Layers']!=expected['layers'] or
        inspected['Architecture']!='amd64' or inspected['Os']!='linux'):
        raise RuntimeError('Loaded image differs from the verified CI archive')
    return inspected['Id']


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',required=True,type=int);p.add_argument('--port',required=True,type=int);p.add_argument('--promote',action='store_true');a=p.parse_args()
    if not 18090<=a.port<=18199:raise ValueError('Use a loopback candidate port in 18090..18199')
    state=json.loads(run(['gh','run','view',str(a.run),'--repo',REPO,'--json','headSha,status,conclusion,workflowName,headBranch']).stdout)
    if state['status']!='completed' or state['conclusion']!='success' or state['workflowName']!='Verified images' or state['headBranch']!='main':raise RuntimeError('Successful main release workflow required')
    revision=state['headSha']
    if not re.fullmatch('[a-f0-9]{40}',revision):raise RuntimeError('Invalid source revision')
    with tempfile.TemporaryDirectory(prefix='wanted-release-') as tmp:
        run(['gh','run','download',str(a.run),'--repo',REPO,'--pattern','image-*','--dir',tmp])
        images={}
        for role in ['atlas','vector','luna','refresh']:
            folder=Path(tmp)/('image-'+role)
            proofs=list(folder.rglob('image-'+role+'.json'));archives=list(folder.rglob('image.tar'))
            if len(proofs)!=1 or len(archives)!=1:raise RuntimeError('Release artifact missing or ambiguous: '+role)
            proof=json.loads(proofs[0].read_text());digest=proof['config_digest']
            if proof['target']!=role or not re.fullmatch('sha256:[0-9a-f]{64}',digest) or not re.fullmatch('ghcr.io/manzigit/wanted-'+role+'@sha256:[0-9a-f]{64}',proof['image']):raise RuntimeError('Invalid image identity')
            expected=archive_identity(archives[0],digest)
            with archives[0].open('rb') as source:
                subprocess.run([*ssh_command(),'sudo docker load --quiet'],stdin=source,stdout=subprocess.DEVNULL,check=True)
            tag='ghcr.io/manzigit/wanted-'+role+':'+revision
            inspected=json.loads(remote(['sudo','docker','image','inspect',tag]).stdout)
            if len(inspected)!=1:raise RuntimeError('Loaded image is ambiguous')
            images[role]=loaded_identity(inspected[0],expected)
        root='/opt/dataieum-releases/dataieum-'+revision[:12]
        script='/opt/wanted/operations/deploy.py'
        prepare=['sudo','python3',script,'prepare','--root',root,'--previous','/opt/dataieum-releases/20260919-ontology-10/compose.active.json','--port',str(a.port)]
        for role in ['atlas','vector','luna']:prepare.extend(['--'+role+'-image',images[role]])
        remote(prepare);remote(['sudo','python3',script,'start','--root',root]);remote(['sudo','python3',script,'validate','--root',root])
        if a.promote:remote(['sudo','python3',script,'promote','--root',root])
        print(json.dumps({'revision':revision,'root':root,'stage':'promoted' if a.promote else 'validated','images':images,'timers_resumed':False}))


if __name__=='__main__':main()
