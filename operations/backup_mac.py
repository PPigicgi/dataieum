"""Create one age-encrypted Mac backup and fully verify every archived byte."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time

HOME=Path.home()
KEY=HOME/'.config/dataieum/backup.agekey'
RECIPIENT=HOME/'.config/dataieum/backup.recipient'
ROOT=HOME/'Library/Application Support/Dataieum/Backups'
def ssh_command():
    """Require operator-supplied SSH settings for the public code snapshot."""
    key = os.environ.get('DATAIEUM_SSH_KEY', '').strip()
    target = os.environ.get('DATAIEUM_SSH_TARGET', '').strip()
    if not key or not target or target.startswith('-'):
        raise RuntimeError('Set DATAIEUM_SSH_KEY and DATAIEUM_SSH_TARGET before remote operations')
    return ['ssh', '-i', str(Path(key).expanduser()), '-o', 'IPQoS=none',
            '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', target]
AGE='/opt/homebrew/bin/age'


def verify(path,extract_dump=None):
    process=subprocess.Popen([AGE,'--decrypt','--identity',str(KEY),str(path)],stdout=subprocess.PIPE)
    entries={};manifest=None
    try:
        with tarfile.open(fileobj=process.stdout,mode='r|gz') as archive:
            for member in archive:
                if not member.isfile() or member.name.startswith('/') or '..' in Path(member.name).parts or member.name in entries:raise RuntimeError('Unsafe backup member')
                stream=archive.extractfile(member)
                if member.name=='manifest.json':
                    if manifest is not None or member.size>1024**2:raise RuntimeError('Invalid backup manifest')
                    manifest=json.load(stream);continue
                digest=hashlib.sha256();size=0
                dest=open(extract_dump,'xb') if extract_dump and member.name=='postgres.dump' else None
                if dest:os.fchmod(dest.fileno(),0o600)
                try:
                    while chunk:=stream.read(1024**2):
                        digest.update(chunk);size+=len(chunk)
                        if dest:dest.write(chunk)
                finally:
                    if dest:dest.close()
                entries[member.name]={'bytes':size,'sha256':digest.hexdigest()}
        # Drain to authenticate age's final chunk, including trailing bytes.
        while process.stdout.read(1024**2):pass
        if process.wait()!=0:raise RuntimeError('Backup decryption failed')
        if not manifest or manifest['files']!=entries:raise RuntimeError('Backup all-file hash comparison failed')
        return manifest
    finally:
        if process.poll() is None:process.terminate();process.wait()


def main():
    p=argparse.ArgumentParser();p.add_argument('--verify',type=Path);p.add_argument('--extract-dump',type=Path);args=p.parse_args()
    if args.verify:
        result=verify(args.verify,args.extract_dump);print(json.dumps({'verified_files':len(result['files']),'publication':result['publication']}));return
    if not KEY.is_file() or not RECIPIENT.is_file():raise RuntimeError('Prepare a separate age key first')
    ROOT.mkdir(parents=True,exist_ok=True);ROOT.chmod(0o700)
    target=ROOT/'wanted-current.tar.gz.age';temporary=ROOT/'wanted-pending.tar.gz.age'
    if temporary.exists():raise RuntimeError('A pending backup exists; inspect it before replacement')
    started=time.time()
    with temporary.open('xb') as out:
        temporary.chmod(0o600)
        source=subprocess.Popen([*ssh_command(),'sudo systemd-run --quiet --wait --pipe --collect --unit=dataieum-backup-export '
            '--property=CPUQuota=40% --property=MemoryMax=1G --property=IOWeight=10 '
            '/opt/wanted/postgres/venv/bin/python /opt/wanted/operations/backup_export.py'],stdout=subprocess.PIPE)
        encrypt=subprocess.Popen([AGE,'--encrypt','--recipients-file',str(RECIPIENT)],stdin=source.stdout,stdout=out)
        source.stdout.close()
        code=encrypt.wait();source_code=source.wait();out.flush();os.fsync(out.fileno())
        if code or source_code:raise RuntimeError('Backup stream failed; current verified backup preserved')
    manifest=verify(temporary)
    temporary.replace(target)
    result={'path':str(target),'bytes':target.stat().st_size,'publication':manifest['publication'],
            'files':len(manifest['files']),'all_file_hashes_verified':True,'seconds':round(time.time()-started,1),'database_restore_verified':False}
    (ROOT/'verification.json').write_text(json.dumps(result,indent=2));(ROOT/'verification.json').chmod(0o600)
    print(json.dumps(result))


if __name__=='__main__':main()
