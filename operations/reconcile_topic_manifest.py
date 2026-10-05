"""Reconcile an immutable graph's derived manifest without editing its data."""
from contextlib import closing
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3


def identity(path):
    s=path.stat()
    wal=path.with_name(path.name+'-wal')
    if wal.exists() and wal.stat().st_size:raise RuntimeError('Graph has a live WAL')
    return s.st_ino,s.st_size,s.st_mtime_ns


def reconcile(graph,manifest,report):
    before=identity(graph);original=manifest.read_bytes();value=json.loads(original)
    with closing(sqlite3.connect(graph.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        state=dict(db.execute("SELECT key,value FROM state WHERE key IN ('count','complete')"))
        if state.get('complete')!='1':raise RuntimeError('Graph is incomplete')
        actual={band:0 for band in ['high','low','unclassified']}
        actual.update(dict(db.execute('SELECT band,count(*) FROM records GROUP BY band')))
        actual['total']=sum(actual.values())
        actual['links']=db.execute('SELECT count(*) FROM links').fetchone()[0]
        aggregates=dict(db.execute("SELECT band,n FROM counts WHERE main_id='' AND topic=''"))
        if any(actual[b]!=aggregates.get(b) for b in ['high','low','unclassified']):
            raise RuntimeError('Band aggregates differ from full record counts')
        if db.execute("SELECT sum(n) FROM counts WHERE topic!=''").fetchone()[0]!=actual['links']:
            raise RuntimeError('Membership aggregates differ from full link count')
        if int(state['count'])!=actual['total']:raise RuntimeError('Graph state count differs')
    digest=hashlib.sha256()
    with graph.open('rb') as stream:
        while chunk:=stream.read(4*1024**2):digest.update(chunk)
    if identity(graph)!=before or manifest.read_bytes()!=original:
        raise RuntimeError('Graph or manifest changed during reconciliation')
    previous={k:value.get(k) for k in ['bytes','sha256','counts']}
    value.update(bytes=before[1],sha256=digest.hexdigest(),counts=actual)
    temporary=manifest.with_suffix('.reconciling')
    with temporary.open('x') as f:
        json.dump(value,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
    os.chmod(temporary,manifest.stat().st_mode & 0o777)
    os.chown(temporary,manifest.stat().st_uid,manifest.stat().st_gid)
    if identity(graph)!=before or manifest.read_bytes()!=original:
        temporary.unlink();raise RuntimeError('Inputs changed before publication')
    temporary.replace(manifest)
    result={'previous':previous,'current':{k:value[k] for k in ['bytes','sha256','counts']},
            'graph_unchanged':identity(graph)==before,'verification':'full record counts, full link count, full file hash'}
    report.write_text(json.dumps(result,indent=2));print(json.dumps(result))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--graph',type=Path,required=True)
    p.add_argument('--manifest',type=Path,required=True);p.add_argument('--report',type=Path,required=True)
    a=p.parse_args();reconcile(a.graph.resolve(),a.manifest.resolve(),a.report)
