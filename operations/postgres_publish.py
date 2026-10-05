"""Commit PostgreSQL and its publication marker inside the existing closed-site gate."""
from contextlib import closing
import hashlib
import itertools
import json
import os
from pathlib import Path
import sqlite3

import psycopg
from psycopg.types.json import Jsonb

from postgres_migrate import FIELDS,JSON_FIELDS,canonical,identity,records,save,source

ROOT=Path('/opt/wanted/postgres')


def atomic(path,value):
    path=Path(path);temporary=path.with_suffix('.tmp')
    with temporary.open('w') as out:out.write(canonical(value));out.flush();os.fsync(out.fileno())
    temporary.chmod(0o640);os.chown(temporary,0,10001);temporary.replace(path)
    fd=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def configured():return (ROOT/'publication.json').is_file()


def capture(publication_root):
    if not configured():return
    with psycopg.connect(**json.loads((ROOT/'admin.json').read_text()),connect_timeout=5) as pg:
        expected=json.loads((ROOT/'publication.json').read_text())['publication']
        row=pg.execute('SELECT state,details FROM publications WHERE id=%s',(expected,)).fetchone()
        if not row or row[0]!='active':raise RuntimeError('PostgreSQL publication is not active')
        atomic(Path(publication_root)/'postgres-before.json',{'publication':expected,'details':row[1]})


def apply(publication_root,base,*,rollback=False):
    root=Path(publication_root)
    if not (root/'postgres-before.json').is_file():return
    gate=json.loads((root/'maintenance.json').read_text())
    if 'closed_at' not in gate:raise RuntimeError('PostgreSQL publication requires the existing maintenance gate')
    before=json.loads((root/'postgres-before.json').read_text())
    paths={'catalog':base['catalog'],'keyword':base['keyword'],'topics':str(Path(base['topics'])/'confidence.sqlite3')}
    sig=identity(paths);version=hashlib.sha256(canonical(sig).encode()).hexdigest()[:24]
    settings=json.loads((ROOT/'admin.json').read_text())
    with psycopg.connect(**settings,connect_timeout=5) as pg,source(paths) as src,closing(sqlite3.connect(root/'delta.sqlite3')) as delta:
        if not pg.execute('SELECT pg_try_advisory_xact_lock(83914927)').fetchone()[0]:raise RuntimeError('PostgreSQL writer is busy')
        ids=[r[0] for r in delta.execute('SELECT dataset_id FROM changes ORDER BY dataset_id')]
        if len(ids)!=len(set(ids)):raise RuntimeError('Duplicate publication ID')
        prior=pg.execute("SELECT id FROM publications WHERE state='active'").fetchone()
        if prior and prior[0] not in {before['publication'],version}:
            own=pg.execute('SELECT details FROM publications WHERE id=%s',(prior[0],)).fetchone()
            if not own or own[0].get('maintenance_root')!=str(root):raise RuntimeError('Another publication has advanced')
        columns=','.join(FIELDS);assignments=','.join(f'{c}=excluded.{c}' for c in FIELDS if c!='id')
        checked=0;digest=hashlib.sha256()
        for offset in range(0,len(ids),1000):
            batch=ids[offset:offset+1000];original=list(records(src,ids=batch));found={r[1] for r in original}
            missing=[ident for ident in batch if ident not in found]
            if missing:pg.execute('DELETE FROM datasets WHERE id=ANY(%s)',(missing,))
            with pg.cursor() as cursor:
                cursor.executemany(f'INSERT INTO datasets ({columns}) VALUES ('+','.join(['%s']*len(FIELDS))+') ON CONFLICT(id) DO UPDATE SET '+assignments,
                    [tuple(Jsonb(v) if i in JSON_FIELDS else v for i,v in enumerate(row)) for row in original])
            stored=pg.execute('SELECT '+columns+' FROM datasets WHERE id=ANY(%s) ORDER BY ordinal',(batch,)).fetchall()
            if canonical(original)!=canonical(stored):raise RuntimeError('PostgreSQL changed-record comparison failed')
            digest.update(canonical(original).encode());checked+=len(batch)
        with pg.cursor() as cursor:
            cursor.executemany('INSERT INTO sources VALUES(%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET info=excluded.info,last_sync=excluded.last_sync,last_error=excluded.last_error',
                [(r['id'],Jsonb(json.loads(r['info'])),r['last_sync'],r['last_error']) for r in src.execute('SELECT * FROM sources')])
        expected=src.execute('SELECT count(*) FROM datasets').fetchone()[0]
        if pg.execute('SELECT count(*) FROM datasets').fetchone()[0]!=expected:raise RuntimeError('PostgreSQL published record count differs')
        if identity(paths)!=sig:raise RuntimeError('Publication changed during PostgreSQL commit')
        details={'artifacts':sig,'maintenance_root':str(root),'parent':before['publication'],'rollback':rollback,
            'verification':{'all_values_equal':True,'scope':'all_changed_records_with_verified_parent','changed_records':checked,'changed_sha256':digest.hexdigest(),
                           'records':expected,'visible_records':src.execute('SELECT count(*) FROM topics.records').fetchone()[0],
                           'maximum_similarity':src.execute('SELECT max(score) FROM topics.records').fetchone()[0]}}
        pg.execute("UPDATE publications SET state='retired' WHERE state='active'")
        save(pg,version,'active',details)
    # Site remains closed between the transaction and marker; recovery replays
    # this exact delta if the host stops at either boundary.
    atomic(ROOT/'publication.json',{'publication':version})
    atomic(root/('postgres-rollback.json' if rollback else 'postgres-result.json'),{'publication':version,**details['verification']})
