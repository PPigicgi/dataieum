"""Stream a complete encrypted-backup input; secrets are deliberately excluded."""
from contextlib import closing
import hashlib
import gzip
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import psycopg

REGISTRY=Path('/opt/dataieum-releases/20260919-ontology-10/compose.active.json')


def mount(config,service,target):
    entries=[m['source'] for m in config['services'][service]['volumes'] if m.get('target')==target]
    if len(entries)!=1:raise RuntimeError('Missing backup source: '+target)
    return Path(entries[0])


class HashingReader:
    def __init__(self,stream):self.stream=stream;self.digest=hashlib.sha256()
    def read(self,size=-1):
        value=self.stream.read(size);self.digest.update(value);return value


def add(archive,path,name,manifest):
    if not path.is_file():raise RuntimeError('Backup source missing: '+name)
    before=path.stat()
    wal=Path(str(path)+'-wal')
    if wal.exists() and wal.stat().st_size:raise RuntimeError('Immutable backup source has a live WAL: '+name)
    info=tarfile.TarInfo(name);info.size=before.st_size;info.mode=0o600;info.mtime=int(before.st_mtime)
    with path.open('rb') as stream:
        reader=HashingReader(stream);archive.addfile(info,reader)
    after=path.stat()
    if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):raise RuntimeError('Backup input changed: '+name)
    manifest['files'][name]={'bytes':info.size,'sha256':reader.digest.hexdigest()}


def main():
    config=json.loads(REGISTRY.read_text())
    import refresh_run
    base=refresh_run.inputs(config)
    if not Path('/opt/wanted/postgres/publication.json').is_file():raise RuntimeError('Verified PostgreSQL publication is required')
    for timer in ('dataieum-refresh.timer','dataieum-maintenance.timer'):
        if subprocess.run(['systemctl','is-active','--quiet',timer]).returncode==0:raise RuntimeError('Pause scheduled writes before a full backup')
    with tempfile.TemporaryDirectory(prefix='dataieum-backup-',dir='/opt/wanted/postgres') as tmp:
        scratch=Path(tmp)
        from postgres_migrate import FIELDS,canonical
        with psycopg.connect(**json.loads(Path('/opt/wanted/postgres/admin.json').read_text())) as pg:
            pg.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            snapshot=pg.execute('SELECT pg_export_snapshot()').fetchone()[0]
            digest=hashlib.sha256();count=0
            with pg.cursor(name='backup_records') as cursor:
                cursor.itersize=1000;cursor.execute('SELECT '+','.join(FIELDS)+' FROM datasets ORDER BY ordinal')
                for row in cursor:digest.update(canonical(row).encode()+b'\n');count+=1
            with (scratch/'postgres.dump').open('wb') as out:
                subprocess.run(['docker','exec','dataieum-postgres','pg_dump','-U','postgres','-d','wanted','--snapshot='+snapshot,'--format=custom','--compress=3','--no-owner','--no-acl'],stdout=out,check=True)
        manifest={'format':1,'created_at':time.time(),'publication':json.loads(Path('/opt/wanted/postgres/publication.json').read_text())['publication'],
                  'images':{k:v['image'] for k,v in config['services'].items()},'files':{},'secrets_included':False,
                  'postgres_verification':{'records':count,'sha256':digest.hexdigest()}}
        originals={'postgres.dump':scratch/'postgres.dump','catalog.sqlite3':base['catalog'],'topics/confidence.sqlite3':Path(base['topics'])/'confidence.sqlite3',
                   'topics/manifest.json':Path(base['topics'])/'manifest.json','catalog-search.sqlite3':base['keyword'],
                   'embeddings.sqlite3':base['embeddings'],'coverage.sqlite3':base['coverage'],'topic-vectors.json':base['topic_vectors']}
        # A restored collector must retain deletion exclusions and the paused
        # publication policy, otherwise it can reintroduce removed records.
        originals.update({
            'operations/refresh-images.json':Path('/opt/wanted/operations/refresh-images.json'),
            'operations/refresh-sources.json':Path('/opt/wanted/operations/refresh-sources.json'),
            'operations/publication.json':Path('/opt/wanted/postgres/publication.json'),
            'exclusions/first.jsonl':Path('/opt/wanted/nonjs-review-20260924/deleted-records-backup.jsonl'),
            'exclusions/second.jsonl':Path('/opt/wanted/catalog-cleanup-20260926/deleted-records-backup.jsonl'),
        })
        index=Path(base['index']);generation=(index/'CURRENT').read_text().strip()
        if '/' in generation or generation in ('.','..'):raise RuntimeError('Invalid vector generation')
        originals['vector/CURRENT']=index/'CURRENT'
        for path in sorted((index/generation).iterdir()):
            if path.is_file() and path.name in {'manifest.json','mapping.sqlite3','vectors.faiss','sources.npy','active.npy','updates.sqlite3'}:
                originals['vector/'+generation+'/'+path.name]=path
        source_identity={name:(Path(path).stat().st_size,Path(path).stat().st_mtime_ns) for name,path in originals.items()}
        # Online SQLite backup is used for the small, mutable job/intro ledgers.
        shared=mount(config,'atlas','/harness-shared')
        for name,path in {'refresh-state.sqlite3':Path('/opt/wanted/refresh/embeddings.sqlite3'),
                          'chat-jobs.sqlite3':shared/'chat-jobs.sqlite3','dataset-intros.sqlite3':shared/'dataset-intros.sqlite3'}.items():
            if not path.is_file():raise RuntimeError('Durable ledger is missing: '+name)
            target=scratch/name
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source,closing(sqlite3.connect(target)) as dest:source.backup(dest,pages=2048,sleep=.02)
            originals[name]=target
        # gzip level 1 bounds CPU work; age encryption is performed only on Mac.
        with gzip.GzipFile(fileobj=sys.stdout.buffer,mode='wb',compresslevel=1) as compressed,tarfile.open(fileobj=compressed,mode='w|') as archive:
            for name,path in originals.items():add(archive,Path(path),name,manifest)
            if any((Path(originals[name]).stat().st_size,Path(originals[name]).stat().st_mtime_ns)!=value for name,value in source_identity.items()):raise RuntimeError('Publication changed during backup')
            if json.loads(Path('/opt/wanted/postgres/publication.json').read_text())['publication']!=manifest['publication']:raise RuntimeError('PostgreSQL publication changed during backup')
            body=json.dumps(manifest,sort_keys=True).encode();info=tarfile.TarInfo('manifest.json');info.size=len(body);info.mode=0o600
            archive.addfile(info,io.BytesIO(body))


if __name__=='__main__':main()
