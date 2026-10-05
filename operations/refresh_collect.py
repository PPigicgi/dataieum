"""Run one existing provider adapter into an isolated draft, then enqueue verified changes."""
import argparse
from contextlib import contextmanager,closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time
from refresh_quality import excluded_reason
from refresh_state import transaction

from refresh_http import ProviderHTTP
from refresh_state import connect,content_hash,native_identity


def write_json(path,value):
    path=Path(path)
    with tempfile.NamedTemporaryFile(mode='w',dir=path.parent,prefix=path.name+'.',delete=False) as out:
        tmp=Path(out.name);json.dump(value,out,ensure_ascii=False,indent=2)
    try:tmp.replace(path)
    finally:tmp.unlink(missing_ok=True)


class Collector:
    def __init__(self,root,baseline,rotation,profile):
        self.root=Path(root);self.baseline=Path(baseline).resolve();self.rotation=rotation
        self.profile=profile;self.source=profile['id']
        self.work=self.root/'runs'/str(rotation)/self.source;self.work.mkdir(parents=True,exist_ok=True)
        self.draft=self.work/'catalog.sqlite3'
        if self.draft.with_suffix('.sqlite3.gz').exists() and not self.draft.exists():
            raise RuntimeError('Archived checkpoint preserved; restore only with sufficient disk headroom')
        self.evidence=self.work/'evidence';self.evidence.mkdir(exist_ok=True)
        self.http=ProviderHTTP(self.work/'responses',profile,global_slots=self.root/'http-slots')
        self.reports={};self.last_status=0.;self.received=0
        self.native_aliases={}
        with closing(sqlite3.connect(self.draft)) as db,db:
            db.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS datasets(id TEXT PRIMARY KEY,source_id TEXT,title TEXT,
                  description TEXT,metadata TEXT,mappings TEXT,fingerprint TEXT,checked_at TEXT);
                CREATE INDEX IF NOT EXISTS source_ids ON datasets(source_id);''')
        if self.source=='us':
            with closing(sqlite3.connect(self.baseline.as_uri()+'?mode=ro&immutable=1',uri=True)) as base:
                for ident,raw in base.execute("SELECT id,metadata FROM datasets WHERE source_id='us'"):
                    key=native_identity(json.loads(raw))
                    if key:
                        self.native_aliases[key]=ident if key not in self.native_aliases else None

    @contextmanager
    def database(self):
        # Adapters can inspect historical aliases, but cannot mutate the live DB.
        with closing(sqlite3.connect(self.draft,timeout=60,uri=True)) as db:
            db.row_factory=sqlite3.Row
            db.execute('ATTACH DATABASE ? AS baseline',(self.baseline.as_uri()+'?mode=ro&immutable=1',))
            db.execute('''CREATE TEMP VIEW datasets AS SELECT * FROM main.datasets UNION ALL
                SELECT * FROM baseline.datasets b WHERE NOT EXISTS (SELECT 1 FROM main.datasets d WHERE d.id=b.id)''')
            db.execute('PRAGMA query_only=ON')
            yield db

    def store(self,source,records):
        if source!=self.source:raise ValueError('Adapter attempted another source')
        import catalog
        values=[]
        with closing(sqlite3.connect(self.draft,timeout=60,uri=True)) as db,db:
            db.row_factory=sqlite3.Row
            db.execute('ATTACH DATABASE ? AS baseline',(self.baseline.as_uri()+'?mode=ro&immutable=1',))
            for raw in records:
                raw={**raw,'source_id':source};ident=source+':'+str(raw.get('external_id',''))
                native=native_identity(raw)
                if native in self.native_aliases:
                    canonical=self.native_aliases[native]
                    if canonical:
                        ident=canonical;raw['external_id']=canonical.split(':',1)[1]
                    else:raw['_refresh_hold']='ambiguous_native_identity'
                current=db.execute('SELECT metadata,mappings FROM datasets WHERE id=?',(ident,)).fetchone()
                prior=current or db.execute('SELECT metadata,mappings FROM baseline.datasets WHERE id=?',(ident,)).fetchone()
                old=json.loads(prior['metadata']) if prior else {}
                merged={**old,**raw}
                if source=='kosis':
                    path=str(raw.get('native_catalog_path') or '')
                    paths=json.loads(current['metadata']).get('native_catalog_paths',[]) if current else []
                    merged['native_catalog_paths']=sorted(set(paths+([path] if path else [])))
                    if not raw.get('description'):merged['description']=old.get('description','')
                if source=='gyeonggi':
                    paths=json.loads(current['metadata']).get('access_paths',[]) if current else []
                    merged['access_paths']=sorted(set(paths+raw.get('access_paths',[])+[raw['url']]))
                for field in ['subjects','access_paths','native_catalog_paths']:
                    if isinstance(old.get(field),list) and isinstance(merged.get(field),list):
                        encode=lambda x:json.dumps(x,sort_keys=True,ensure_ascii=False)
                        if sorted(map(encode,old[field]))==sorted(map(encode,merged[field])):merged[field]=old[field]
                r=catalog.normalize(merged)
                db.execute('''INSERT INTO datasets VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                    title=excluded.title,description=excluded.description,metadata=excluded.metadata,
                    fingerprint=excluded.fingerprint,checked_at=excluded.checked_at''',
                    (r['id'],source,r['title'],r['description'],catalog.dump(r),prior['mappings'] if prior else '[]',r['fingerprint'],r['checked_at']))
                values.append(r['id'])
        self.received+=len(values)
        self.status('running')
        return set(values)

    def report(self,source,info):
        self.reports[source]=info
        write_json(self.evidence/(source+'.json'),info)
        self.status('running',force=True)

    def status(self,state,*,force=False,**extra):
        if not force and time.monotonic()-self.last_status<5:return
        self.last_status=time.monotonic()
        write_json(self.work/'status.json',{'source':self.source,'rotation':self.rotation,'status':state,
            'received_this_process':self.received,'http':self.http.snapshot(),'provider_reports':self.reports,
            'updated_at':time.time(),**extra})

    def install(self):
        # Importing the old module creates an evidence directory beside DB_PATH.
        os.environ['DB_PATH']=str(self.draft)
        import collection_audit as ca
        import collectors
        ca.ROOT=self.evidence;ca.database=self.database;ca.store_raw=self.store;ca.report=self.report;ca.read_url=self.http.fetch
        collectors.database=self.database
        collectors.fetch=lambda url,fields=None:json.loads(self.http.fetch(url,limit=64_000_000,fields=fields))
        if self.source in ca.CKAN_CATALOGS:
            # The old independent checker can delete aliases. This refresh uses
            # full paginated ID/count reconciliation without that mutation path.
            return lambda:ca.collect_ckan(self.source)
        return ca.collectors()[self.source]

    def completeness(self):
        accepted={'count_reconciled','export_reconciled','id_reconciled','tree_exhausted'}
        main=self.reports.get(self.source,{})
        if self.source=='seoul':return self.reports.get('seoul-live',{}).get('status') in accepted and main.get('status')=='snapshot_imported'
        return main.get('status') in accepted

    def enqueue(self,db):
        import embedding_batch
        counts={'changed':0,'unchanged':0,'previously_removed_suppressed':0}
        with closing(sqlite3.connect(self.draft)) as draft,closing(sqlite3.connect(self.baseline.as_uri()+'?mode=ro&immutable=1',uri=True)) as base:
            cursor=draft.execute('SELECT id,source_id,title,description,metadata FROM datasets ORDER BY id')
            while rows:=cursor.fetchmany(100):
                prepared=[]
                for ident,source,title,desc,raw in rows:
                    record=json.loads(raw);h=content_hash(record)
                    previous=base.execute('SELECT metadata FROM datasets WHERE id=?',(ident,)).fetchone()
                    same=bool(previous and content_hash(json.loads(previous[0]))==h)
                    _,text=embedding_batch.input_text(title,desc,record,source)
                    prepared.append((ident,source,raw,record,h,same,hashlib.sha256(text.encode()).hexdigest()))
                for attempt in range(4):
                    block={k:0 for k in counts}
                    try:
                        with transaction(db):
                            for ident,source,raw,record,h,same,text_hash in prepared:
                                if excluded_reason(db,ident,record):
                                    block['previously_removed_suppressed']+=1;continue
                                old=db.execute('SELECT content_hash FROM changes WHERE dataset_id=?',(ident,)).fetchone()
                                if same:
                                    if old:db.execute('DELETE FROM changes WHERE dataset_id=?',(ident,))
                                    block['unchanged']+=1;continue
                                if old and old[0]==h:block['unchanged']+=1;continue
                                db.execute("""INSERT INTO changes(dataset_id,source,rotation,metadata,content_hash,input_hash,state)
                                  VALUES(?,?,?,?,?,?,'pending') ON CONFLICT(dataset_id) DO UPDATE SET source=excluded.source,
                                  rotation=excluded.rotation,metadata=excluded.metadata,content_hash=excluded.content_hash,
                                  input_hash=excluded.input_hash,embedding_key=NULL,exclusion=NULL,state='pending'""",
                                  (ident,source,self.rotation,raw,h,text_hash))
                                block['changed']+=1
                        for key in counts:counts[key]+=block[key]
                        break
                    except sqlite3.OperationalError as error:
                        if not any(word in str(error).lower() for word in ('locked','busy')) or attempt==3:raise
                        db.rollback();time.sleep(2**attempt)
        return counts

    def run(self):
        db=connect(self.root)
        with db:db.execute("UPDATE source_runs SET status='running',report='{}' WHERE rotation=? AND source=?",(self.rotation,self.source))
        try:
            saved=json.loads((self.work/'status.json').read_text()) if (self.work/'status.json').exists() else {}
            self.reports=saved.get('provider_reports',{})
            if not (saved.get('status')=='failed' and self.completeness()):
                self.reports={};self.install()()
            self.http.close()
            result={'provider_reports':self.reports,'http':self.http.snapshot()}
            if self.completeness():
                result.update(self.enqueue(db));state='complete'
            else:state='unverified'
            with db:db.execute('UPDATE source_runs SET status=?,report=? WHERE rotation=? AND source=?',
                               (state,json.dumps(result,ensure_ascii=False),self.rotation,self.source))
            self.status(state,force=True,delta=result)
            # Restart cache is only needed while this attempt is unfinished.
            if state=='complete':
                shutil.rmtree(self.work/'responses',ignore_errors=True)
                for suffix in ['','-wal','-shm']:Path(str(self.draft)+suffix).unlink(missing_ok=True)
            return state
        except Exception as error:
            self.http.close()
            result={'error_type':type(error).__name__,'error':str(error)[:600],
                    'http':self.http.snapshot(),'provider_reports':self.reports}
            with db:db.execute("UPDATE source_runs SET status='failed',report=? WHERE rotation=? AND source=?",
                               (json.dumps(result,ensure_ascii=False),self.rotation,self.source))
            self.status('failed',force=True,error=result)
            return 'failed'
        finally:db.close()


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--baseline',type=Path,required=True)
    p.add_argument('--rotation',type=int,required=True);p.add_argument('--source',required=True);p.add_argument('--profiles',type=Path,required=True)
    a=p.parse_args();profiles=json.loads(a.profiles.read_text())['sources']
    profile=next(x for x in profiles if x['id']==a.source)
    result=Collector(a.root,a.baseline,a.rotation,profile).run()
    print(json.dumps({'source':a.source,'status':result}),flush=True)


if __name__=='__main__':main()
