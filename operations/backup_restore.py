"""Restore the Mac encrypted backup into a disposable, network-isolated DB."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import time

from backup_mac import AGE,KEY,ROOT,verify

IMAGE='postgres@sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722'
NAME='dataieum-backup-restore'
COLUMNS='ordinal,id,source_id,title,description,metadata,original_mappings,fingerprint,checked_at,reference_years,classification'


def command(args,**kw):return subprocess.run(args,check=True,capture_output=True,text=True,**kw)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--backup',type=Path,default=ROOT/'wanted-current.tar.gz.age');args=parser.parse_args()
    before=args.backup.stat();manifest=verify(args.backup)
    if subprocess.run(['docker','container','inspect',NAME],capture_output=True).returncode==0:raise RuntimeError('A restore container already exists; do not overwrite it')
    started=time.time();decrypt=None
    command(['docker','run','-d','--rm','--name',NAME,'--network','none','--memory','2g','--cpus','2',
        '-e','POSTGRES_DB=wanted','-e','POSTGRES_HOST_AUTH_METHOD=trust',IMAGE,'-c','shared_buffers=256MB','-c','max_wal_size=1GB'])
    try:
        for _ in range(40):
            if subprocess.run(['docker','exec',NAME,'pg_isready','-h','127.0.0.1','-U','postgres','-d','wanted'],capture_output=True).returncode==0:break
            time.sleep(.5)
        else:raise RuntimeError('Restore DB did not start')
        decrypt=subprocess.Popen([AGE,'--decrypt','--identity',str(KEY),str(args.backup)],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
        with tarfile.open(fileobj=decrypt.stdout,mode='r|gz') as archive:
            member=next(iter(archive))
            if member.name!='postgres.dump':raise RuntimeError('Database dump must be the first backup entry')
            with subprocess.Popen(['docker','exec','-i',NAME,'pg_restore','-U','postgres','-d','wanted','--no-owner','--no-acl','--exit-on-error'],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE) as restore:
                data=archive.extractfile(member)
                while chunk:=data.read(1024**2):restore.stdin.write(chunk)
                restore.stdin.close()
                error=restore.stderr.read(8000)
                if restore.wait()!=0:raise RuntimeError('Database restore failed; '+error.decode(errors='replace')[:500])
        decrypt.stdout.close();decrypt.terminate();decrypt.wait();decrypt=None
        # Bound the database query itself as well as the client's result buffer.
        # A streaming client alone did not prevent the full JSON query from
        # exhausting the isolated server's memory. Keyset pages preserve the
        # exact global order without repeating rows or growing OFFSET scans.
        digest=hashlib.sha256();count=0;last=None
        psql=['docker','exec',NAME,'psql','--no-psqlrc','-v','ON_ERROR_STOP=1','-U','postgres','-d','wanted','-Atc']
        while True:
            condition='' if last is None else ' WHERE ordinal>'+str(last)
            sql='SELECT json_build_array('+COLUMNS+') FROM datasets'+condition+' ORDER BY ordinal LIMIT 1000'
            # splitlines() also splits valid JSON string characters such as
            # U+0085/U+2028/U+2029 present in multilingual source metadata.
            output=command([*psql,sql]).stdout
            lines=output.split('\n')
            if lines[-1]=='':lines.pop()
            if not lines:break
            for line in lines:
                record=json.loads(line)
                if not isinstance(record,list) or len(record)!=len(COLUMNS.split(',')) or type(record[0]) is not int:raise RuntimeError('Unexpected restored row shape')
                if last is not None and record[0]<=last:raise RuntimeError('Restored row order regressed')
                last=record[0]
                digest.update(json.dumps(record,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()+b'\n');count+=1
            if count%100000==0:print(json.dumps({'stage':'verify_restored_rows','records':count}),flush=True)
        query="SELECT details::text FROM publications WHERE id='"+manifest['publication']+"'"
        if len(manifest['publication'])!=24 or any(c not in '0123456789abcdef' for c in manifest['publication']):raise RuntimeError('Invalid publication identifier')
        details=json.loads(command(['docker','exec',NAME,'psql','-U','postgres','-d','wanted','-Atc',query]).stdout)
        expected=manifest['postgres_verification']
        if count!=expected['records'] or digest.hexdigest()!=expected['sha256'] or count!=details['verification']['records']:raise RuntimeError('Restored full-record hash differs from backup snapshot')
        after=args.backup.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise RuntimeError('Backup changed during restore')
        proof={'path':str(args.backup),'publication':manifest['publication'],'restored_records':count,'restored_sha256':digest.hexdigest(),
               'database_restore_verified':True,'all_file_hashes_verified':True,'network_exposure':False,'seconds':round(time.time()-started,1)}
        target=ROOT/'restore-verification.json';target.write_text(json.dumps(proof,indent=2));target.chmod(0o600)
        print(json.dumps(proof))
    except Exception:
        logs=subprocess.run(['docker','logs','--tail','60',NAME],capture_output=True,text=True)
        diagnostic=ROOT/'restore-diagnostic.log';diagnostic.write_text(logs.stdout+logs.stderr);diagnostic.chmod(0o600)
        raise
    finally:
        if decrypt and decrypt.poll() is None:decrypt.terminate();decrypt.wait()
        subprocess.run(['docker','stop','-t','10',NAME],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True)


if __name__=='__main__':main()
