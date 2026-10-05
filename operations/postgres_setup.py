"""Provision an isolated local PostgreSQL instance without exposing credentials."""
import json
import os
from pathlib import Path
import secrets
import subprocess

ROOT=Path('/opt/wanted/postgres')
IMAGE='postgres@sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722'


def run(args,**kwargs):
    return subprocess.run(args,check=True,capture_output=True,text=True,**kwargs)


def write(path,value,mode):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,mode)
    with os.fdopen(fd,'w') as f:f.write(value);f.flush();os.fsync(f.fileno())


def main():
    if os.geteuid()!=0:raise SystemExit('Run as root')
    ROOT.mkdir(mode=0o750,parents=True,exist_ok=True)
    os.chown(ROOT,0,10001)
    password=ROOT/'admin-password'
    if not password.exists():write(password,secrets.token_urlsafe(48),0o600)
    os.chown(password,0,999);password.chmod(0o640)
    if not (ROOT/'reader.json').exists():
        reader={'host':'dataieum-postgres','port':5432,'dbname':'wanted','user':'wanted_reader','password':secrets.token_urlsafe(48)}
        write(ROOT/'reader.json',json.dumps(reader),0o640);os.chown(ROOT/'reader.json',0,10001)
    if subprocess.run(['docker','network','inspect','dataieum-db'],capture_output=True).returncode:
        run(['docker','network','create','--internal','dataieum-db'])
    if subprocess.run(['docker','container','inspect','dataieum-postgres'],capture_output=True).returncode:
        run(['docker','volume','create','dataieum-postgres-data'])
        run(['docker','run','-d','--name','dataieum-postgres','--restart','unless-stopped',
             '--network','dataieum-db','--memory','1200m','--memory-swap','1200m','--cpus','1',
             '--security-opt','no-new-privileges','-e','POSTGRES_DB=wanted','-e','POSTGRES_PASSWORD_FILE=/run/admin-password',
             '-e','POSTGRES_INITDB_ARGS=--data-checksums',
             '-v',str(password)+':/run/admin-password:ro','-v','dataieum-postgres-data:/var/lib/postgresql',
             '--health-cmd','pg_isready -U postgres -d wanted','--health-interval','10s','--health-timeout','3s','--health-retries','10',
             IMAGE,'-c','shared_buffers=256MB','-c','work_mem=8MB','-c','maintenance_work_mem=96MB',
             '-c','max_connections=24','-c','max_wal_size=1GB','-c','log_min_error_statement=panic','-c','log_statement=none'])
    import time
    for _ in range(30):
        if subprocess.run(['docker','exec','dataieum-postgres','pg_isready','-h','127.0.0.1','-U','postgres','-d','wanted'],capture_output=True).returncode==0:break
        time.sleep(1)
    else:raise RuntimeError('PostgreSQL did not become ready')
    address=run(['docker','inspect','--format','{{(index .NetworkSettings.Networks "dataieum-db").IPAddress}}','dataieum-postgres']).stdout.strip()
    admin={'host':address,'port':5432,'dbname':'wanted','user':'postgres','password':password.read_text()}
    temporary=ROOT/'admin.tmp';temporary.unlink(missing_ok=True)
    write(temporary,json.dumps(admin),0o600);temporary.replace(ROOT/'admin.json')
    # Passwords travel only through stdin; never through process arguments or output.
    reader=json.loads((ROOT/'reader.json').read_text())
    exists=run(['docker','exec','dataieum-postgres','psql','-U','postgres','-d','wanted','-Atc',"SELECT 1 FROM pg_roles WHERE rolname='wanted_reader'"]).stdout.strip()
    if not exists:
        run(['docker','exec','-i','dataieum-postgres','psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','wanted'],
            input="CREATE ROLE wanted_reader LOGIN PASSWORD '"+reader['password']+"';\n")
    print(json.dumps({'postgres':'ready','public_port':False,'reader':'read_only','image':IMAGE}))


if __name__=='__main__':main()
