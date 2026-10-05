"""Only changed inputs, only OpenAI Batch, with durable reservations before POST."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from refresh_quality import allowed,excluded_reason,save_result,result

import embedding_batch as batch
from refresh_state import (DailyBudgetReached, DAILY_NANODOLLARS, NANODOLLARS_PER_TOKEN,
                           budget_used, day, input_key, reserve_batch, settle_batch)


def prepare_changes(db, baseline_vectors):
    import tiktoken
    enc=tiktoken.encoding_for_model(batch.MODEL)
    old=sqlite3.connect(Path(baseline_vectors).resolve().as_uri()+'?mode=ro&immutable=1',uri=True)
    try:
        while True:
            rows=db.execute("""SELECT c.* FROM changes c JOIN source_runs s
                 ON s.rotation=c.rotation AND s.source=c.source WHERE c.state='pending' AND s.status='complete'
                 AND EXISTS(SELECT 1 FROM quality q WHERE q.dataset_id=c.dataset_id AND q.content_hash=c.content_hash AND q.status='verified' AND q.expires_at>?)
                 ORDER BY c.dataset_id LIMIT 200""",(time.time(),)).fetchall()
            if not rows:break
            for row in rows:
                m=json.loads(row['metadata'])
                if not allowed(db,row):
                    exact=excluded_reason(db,row['dataset_id'],m)
                    with db:save_result(db,row,result('excluded' if exact else 'hold','previously_deleted' if exact else 'quality_gate_blocked'))
                    continue
                title,text=batch.input_text(m['title'],m.get('description',''),m,row['source'])
                h=hashlib.sha256(text.encode()).hexdigest();key=input_key(row['dataset_id'],h)
                tokens=len(enc.encode_ordinary(text))
                exclusion=m.get('_refresh_hold') or ('placeholder_title' if not title or title.casefold() in {'test','((name))'} else 'over8191' if tokens>batch.MAX_TOKENS else None)
                cached=old.execute('SELECT vector FROM embeddings WHERE dataset_id=? AND input_hash=? AND exclusion IS NULL',
                                   (row['dataset_id'],h)).fetchone()
                vector=cached[0] if cached else None
                if vector is not None:batch.vector_blob(__import__('base64').b64encode(vector).decode())
                with db:
                    db.execute('''INSERT OR IGNORE INTO embeddings(dataset_id,source_id,input_hash,input,tokens,exclusion,vector)
                               VALUES(?,?,?,?,?,?,?)''',(key,row['source'],h,None if exclusion else text,tokens,exclusion,vector))
                    # An identity hold can clear without the embedding text changing.
                    # Preserve in-flight inputs, but release a cached/unsubmitted hold.
                    db.execute('UPDATE embeddings SET exclusion=?,input=? WHERE dataset_id=? AND (batch_id IS NULL OR vector IS NOT NULL)',
                               (exclusion,None if exclusion else text,key))
                    ready=db.execute('SELECT vector IS NOT NULL FROM embeddings WHERE dataset_id=?',(key,)).fetchone()[0]
                    db.execute('UPDATE changes SET embedding_key=?,input_hash=?,exclusion=?,state=? WHERE dataset_id=? AND content_hash=?',
                               (key,h,exclusion,'excluded' if exclusion else 'ready' if ready else 'embedding',row['dataset_id'],row['content_hash']))
    finally:old.close()


def discard_obsolete_unsubmitted(db):
    # No inference has started for prepared batches. If a newer provider version
    # replaced an input, rebuild that batch before any paid submission.
    eligible="""SELECT c.embedding_key FROM changes c JOIN source_runs s
       ON c.rotation=s.rotation AND c.source=s.source
       WHERE s.status='complete' AND c.state IN ('embedding','ready')
       AND EXISTS(SELECT 1 FROM quality q WHERE q.dataset_id=c.dataset_id AND q.content_hash=c.content_hash
                  AND q.status='verified' AND q.expires_at>CAST(strftime('%s','now') AS REAL))
       AND NOT EXISTS(SELECT 1 FROM excluded x WHERE x.dataset_id=c.dataset_id)"""
    with db:
        obsolete=db.execute(f"""SELECT DISTINCT b.id FROM batches b JOIN embeddings e ON e.batch_id=b.id
            WHERE b.status='prepared' AND NOT EXISTS (SELECT 1 FROM charges q WHERE q.batch_id=b.id)
            AND e.dataset_id NOT IN ({eligible})""").fetchall()
        for row in obsolete:
            db.execute('UPDATE embeddings SET batch_id=NULL,request_id=NULL,position=NULL WHERE batch_id=?',(row[0],))
            db.execute("UPDATE batches SET status='retried' WHERE id=?",(row[0],))
        # Expired/unverified inputs are rebuildable from their retained metadata.
        # Reset before dropping an unsubmitted vector input so it cannot strand
        # a change in 'embedding' without a corresponding embedding row.
        db.execute(f"""UPDATE changes SET state='pending',embedding_key=NULL
            WHERE state='embedding' AND embedding_key IN
            (SELECT dataset_id FROM embeddings WHERE batch_id IS NULL AND vector IS NULL
             AND dataset_id NOT IN ({eligible}))""")
        db.execute(f"""DELETE FROM embeddings WHERE batch_id IS NULL AND vector IS NULL
            AND dataset_id NOT IN ({eligible})""")


class QualityGateBlocked(ValueError):
    pass


class BudgetAPI:
    def __init__(self, db, api):
        self.db,self.api=db,api

    def check_quality(self,batch_id):
        inputs=self.db.execute('SELECT c.* FROM embeddings e LEFT JOIN changes c ON c.embedding_key=e.dataset_id WHERE e.batch_id=?',(batch_id,)).fetchall()
        if not inputs or any(x['dataset_id'] is None or not allowed(self.db,x) for x in inputs):
            raise QualityGateBlocked('Batch quality gate blocked')

    def upload(self,path):return self.api.upload(path)

    def request(self,method,path,body=None,**kwargs):
        if method=='POST' and path=='/batches':
            if body.get('endpoint')!='/v1/embeddings' or body.get('completion_window')!='24h':
                raise ValueError('Only embedding Batch requests are allowed')
            marker=body.get('metadata',{}).get('wanted_run')
            row=self.db.execute('SELECT id FROM batches WHERE marker=?',(marker,)).fetchone()
            if row is None:raise ValueError('Unregistered batch')
            self.check_quality(row[0])
            reserve_batch(self.db,row[0])
            try:return self.api.request(method,path,body,**kwargs)
            except batch.APIError as error:
                # These statuses are an explicit creation rejection, not an uncertain timeout.
                if error.code in {400,401,403,404,413,422,429}:
                    with self.db:self.db.execute('DELETE FROM charges WHERE batch_id=?',(row[0],))
                raise
        if method=='POST' and path=='/embeddings':
            raise ValueError('Synchronous embedding is disabled')
        return self.api.request(method,path,body,**kwargs)


def tick(db, root, *, api=None):
    root=Path(root)
    discard_obsolete_unsubmitted(db)
    needed=db.execute("SELECT 1 FROM batches WHERE status IN ('creating','submitted','collecting','prepared') OR (status IN ('done','retried') AND (file_id IS NOT NULL OR output_file_id IS NOT NULL OR error_file_id IS NOT NULL)) LIMIT 1").fetchone()
    needed=needed or db.execute('SELECT 1 FROM embeddings WHERE vector IS NULL AND exclusion IS NULL AND batch_id IS NULL LIMIT 1').fetchone()
    if not needed:return {'state':'idle'}
    if api is None:
        # The same protected runtime credential as the existing embedding service.
        # Never include it in command arguments, logs, state, or reports.
        keyfile=Path('/run/embedding.key')
        if not keyfile.exists():return {'state':'credential_unavailable'}
        os.environ['OPENAI_API_KEY']=keyfile.read_text().strip()
        api=batch.API()
        os.environ.pop('OPENAI_API_KEY',None)
    api=BudgetAPI(db,api)
    uncertain=[];errors=[]
    for row in db.execute("SELECT * FROM batches WHERE status IN ('creating','submitted','collecting','done','retried','errors')").fetchall():
        try:
            if row['status']=='creating':
                if not db.execute('SELECT 1 FROM charges WHERE batch_id=?',(row['id'],)).fetchone():
                    with db:db.execute("UPDATE batches SET status='prepared' WHERE id=?",(row['id'],))
                else:batch.recover_submission(db,row,api)
            elif row['status'] in {'submitted','collecting'}:batch.collect(db,row,api)
            updated=db.execute('SELECT * FROM batches WHERE id=?',(row['id'],)).fetchone()
            if updated['status'] in {'done','errors','retried'}:
                settle_batch(db,row['id'],uncharged=updated['status']=='retried')
                if updated['status'] in {'done','retried'}:batch.cleanup_remote(db,updated,api)
        except Exception as e:
            final=db.execute('SELECT status FROM batches WHERE id=?',(row['id'],)).fetchone()[0]
            if final=='errors':settle_batch(db,row['id'])
            if final=='creating':uncertain.append(row['id'])
            errors.append({'batch':row['id'],'type':type(e).__name__})
    with db:
        db.execute("UPDATE changes SET state='ready' WHERE state='embedding' AND embedding_key IN (SELECT dataset_id FROM embeddings WHERE vector IS NOT NULL)")
    discard_obsolete_unsubmitted(db)
    if uncertain:return {'state':'uncertain_submission','batches':uncertain,'errors':errors}
    active,tokens=db.execute("SELECT count(*),COALESCE(sum(tokens),0) FROM batches WHERE status IN ('creating','submitted','collecting')").fetchone()
    limit=batch.admission_limit(db,15,active)
    submitted=0
    while active<limit and tokens<1_500_000:
        available=(DAILY_NANODOLLARS-budget_used(db,day()))//NANODOLLARS_PER_TOKEN
        token_limit=min(100_000,1_500_000-tokens,available)
        if token_limit<batch.MAX_TOKENS:break
        row=db.execute("SELECT * FROM batches WHERE status='prepared' ORDER BY id LIMIT 1").fetchone()
        if row is None:row=batch.make_batch(db,token_limit)
        if row is None:break
        if row['tokens']>available:break
        try:
            api.check_quality(row['id'])
            batch.submit(db,row,root,api)
        except QualityGateBlocked:
            with db:db.execute("UPDATE batches SET status='prepared' WHERE id=?",(row['id'],))
            errors.append({'type':'quality_blocked'});break
        except DailyBudgetReached:
            # Budget rejected before the HTTP POST; it is safe to keep this unsubmitted.
            with db:db.execute("UPDATE batches SET status='prepared' WHERE id=?",(row['id'],))
            break
        except batch.APIError as e:
            if e.code==429:
                with db:batch.quota_backoff(db)
            errors.append({'type':'APIError','http_status':e.code});break
        except Exception as e:
            errors.append({'type':type(e).__name__});break
        active+=1;tokens+=row['tokens'];submitted+=1
    return {'state':'running' if active else 'waiting','submitted':submitted,'active':active,'errors':errors}
