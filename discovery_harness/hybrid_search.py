"""Bounded keyword candidates and reciprocal rank fusion; no model calls."""
import math
from pathlib import Path
import sqlite3
import time

RRF_K=60


def fuse(cosine_ranked,keyword_ids, *, vector_limit=100):
    lexical={identifier:rank for rank,identifier in enumerate(dict.fromkeys(keyword_ids),1)}
    result=[]
    for rank,row in enumerate(cosine_ranked,1):
        kr=lexical.get(row['dataset_id'])
        vr=rank if vector_limit is None or rank<=vector_limit else None
        if vr is None and kr is None:continue
        result.append({**row,'rrf_score':(1/(RRF_K+vr) if vr else 0)+(1/(RRF_K+kr) if kr else 0),
                       'retrieval_ranks':{'vector':vr,'keyword':kr}})
    return sorted(result,key=lambda r:(-r['rrf_score'],-r['cosine'],r['dataset_id']))


def rank(row):
    value=row.get('rrf_score')
    return value if type(value) in (int,float) and math.isfinite(value) and 0<value<=2/(RRF_K+1) else row['cosine']


def keywords(index,mapping,query,mask,deadline):
    from .keyword_index import lookup
    if not query:return [],[],'not_requested'
    if not isinstance(query,str) or not 0<len(query)<=200 or any(ord(c)<32 for c in query):raise ValueError('Invalid lexical query')
    if index is None:return [],[],'unavailable'
    table,expression=lookup(query)
    db=sqlite3.connect(Path(index).resolve().as_uri()+'?mode=ro&immutable=1',uri=True,timeout=.1)
    try:
        db.execute('ATTACH DATABASE ? AS vector',(Path(mapping).resolve().as_uri()+'?mode=ro&immutable=1',))
        db.execute('PRAGMA query_only=ON');db.execute('PRAGMA cache_size=-4096')
        db.set_progress_handler(lambda:int(time.monotonic()>=deadline),1000)
        db.create_function('eligible',1,lambda label:int(mask is None or 0<=label<len(mask) and bool(mask[label])))
        rows=db.execute(f'''SELECT m.label,m.dataset_id FROM {table}
            CROSS JOIN documents d ON d.n={table}.rowid
            JOIN vector.records m INDEXED BY records_dataset_id ON m.dataset_id=d.id
            WHERE {table} MATCH ? AND eligible(m.label)
            ORDER BY {table}.rank LIMIT 100''',(expression,)).fetchall()
        return [r[0] for r in rows],[r[1] for r in rows],'ready'
    except sqlite3.OperationalError as error:
        if getattr(error,'sqlite_errorcode',None)==sqlite3.SQLITE_INTERRUPT:return [],[],'deadline'
        raise
    finally:db.close()
