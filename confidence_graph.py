"""Bounded graph pages backed by current similarity memberships."""
import json
import math
import sqlite3
from pathlib import Path
from functools import lru_cache
from contextlib import contextmanager
from catalog import read_records, year_fields

PATH = '/data/topic-classification/confidence.sqlite3'
BANDS = {'high':'고신뢰','low':'저신뢰','unclassified':'미분류'}

@contextmanager
def connection():
    db = sqlite3.connect('file:'+PATH+'?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    db.execute("ATTACH DATABASE 'file:/data/catalog.sqlite3?mode=ro' AS cat")
    db.create_function('has_year', 4, lambda period,title,description,year: int(int(year) in year_fields({'period':period},title,description)['reference_years']))
    try: yield db
    finally: db.close()

def where(q):
    clauses=[];args=[]
    for key,col in [('band','r.band'),('source','r.source')]:
        if q.get(key):clauses.append(col+'=?');args.append(q[key])
    topic=q.get('concept','')
    if topic:
        clauses.append('EXISTS (SELECT 1 FROM primary_links l JOIN topics t ON t.id=l.topic WHERE l.dataset_id=r.id AND '+('t.id' if '-S' in topic else 't.main_id')+'=?)');args.append(topic)
    if q.get('q'):
        clauses.append("EXISTS (SELECT 1 FROM cat.datasets d WHERE d.id=r.id AND instr(lower(d.title || ' ' || d.description || ' ' || coalesce(json_extract(d.metadata,'$.publisher'),'') || ' ' || coalesce(json_extract(d.metadata,'$.region'),'')),lower(?))>0)");args.append(q['q'][:200])
    if q.get('year'):
        if not q['year'].isdigit() or len(q['year'])!=4:raise ValueError('자료 연도가 올바르지 않습니다.')
        clauses.append("EXISTS (SELECT 1 FROM cat.datasets d WHERE d.id=r.id AND has_year(json_extract(d.metadata,'$.period'),d.title,d.description,?))");args.append(q['year'])
    return (' WHERE '+' AND '.join(clauses) if clauses else ''),args

def total(db,q,w,args):
    if not any(q.get(k) for k in ('q','source','year')):
        band=q.get('band');topic=q.get('concept','')
        main=topic.split('-S')[0] if topic else ''
        sub=topic if '-S' in topic else ''
        sql='SELECT coalesce(sum(n),0) FROM primary_counts WHERE main_id=? AND topic=?'
        values=[main,sub]
        if band:sql+=' AND band=?';values.append(band)
        return db.execute(sql,values).fetchone()[0]
    if '-S' in q.get('concept',''):
        rest={k:v for k,v in q.items() if k!='concept'};rw,ra=where(rest)
        sql='SELECT count(*) FROM primary_links p CROSS JOIN records r ON r.id=p.dataset_id'+rw+(' AND ' if rw else ' WHERE ')+'p.topic=?'
        if q.get('band'):sql+=' AND p.band=?';ra=ra+[q['concept'],q['band']]
        else:ra=ra+[q['concept']]
        return db.execute(sql,ra).fetchone()[0]
    return db.execute('SELECT count(*) FROM records r'+w,args).fetchone()[0]

@lru_cache(maxsize=256)
def base_catalog():
    with connection() as db:
        sources=[{**json.loads(r['info']),'last_sync':r['last_sync'],'last_error':r['last_error']} for r in db.execute('SELECT * FROM cat.sources')]
        counts=json.loads(db.execute("SELECT value FROM state WHERE key='source_counts'").fetchone()[0])
        concepts=[{'id':r['id'],'name':r['name'],'definition':r['definition']} for r in db.execute('SELECT * FROM topics')]
        mains=[{'id':r[0],'name':r[1],'definition':r[1]} for r in db.execute('SELECT DISTINCT main_id,main_name FROM topics')]
        tc=dict(db.execute("SELECT topic,sum(n) FROM primary_counts WHERE topic!='' GROUP BY topic"))
        tc.update(dict(db.execute("SELECT main_id,sum(n) FROM primary_counts WHERE topic='' AND main_id!='' GROUP BY main_id")))
        total=db.execute("SELECT sum(n) FROM primary_counts WHERE main_id='' AND topic=''").fetchone()[0]
        # Preserve existing year choices; record matches use the same year parser.
        year_row=db.execute("SELECT value FROM state WHERE key='year_counts'").fetchone()
        years=json.loads(year_row[0]) if year_row else {}
    for s in sources:s['dataset_count']=counts.get(s['id'],0);s['stored_count']=s['dataset_count'];s.pop('scan',None)
    return {'sources':sources,'concepts':mains+concepts,'datasets':[], 'page':1,'pages':1,'total':total,
            'summary':{'datasets':total,'concept_counts':tc,'year_counts':years,'running':False}}

@lru_cache(maxsize=1)
def maximum_similarity(version):
    with connection() as db:
        return float(db.execute('SELECT coalesce(max(score),0) FROM records').fetchone()[0])

def confidence_score(score, band, maximum):
    if band == 'high':
        span = maximum - .25
        ratio = (score - .25) / span if span > 0 else 1.
        return round(75 + 25 * min(1., max(0., ratio)), 1)
    if band == 'low':
        return round(50 + 25 * min(1., max(0., (score - .15) / .10)), 1)
    return None

def records(db, ids):
    # Read metadata, replacing the obsolete concept associations on every response.
    result=read_records(db,[{'id':v} for v in ids])
    stat=Path(PATH).stat()
    maximum=maximum_similarity((stat.st_ino,stat.st_size,stat.st_mtime_ns)) if ids else 0.
    for item in result:
        row=db.execute('SELECT band,score FROM records WHERE id=?',(item['id'],)).fetchone()
        item['confidence_band']=row['band'];item['max_similarity']=row['score']
        item.pop('classification_note',None)
        item['mappings']=[{'concept_id':r[0],'confidence':r[1],'opacity':r[2],'status':'classified','confidence_score':confidence_score(r[1],row['band'],maximum),'evidence':f'신뢰도 {confidence_score(r[1],row["band"],maximum):g}점'}
                          for r in db.execute('SELECT topic,score,alpha FROM links WHERE dataset_id=? ORDER BY score DESC',(item['id'],))]
    return result

def page_ids(db,q,start,size):
    topic=q.get('concept','')
    if '-S' in topic and not any(q.get(k) for k in ('q','source','year')):
        sql='SELECT dataset_id FROM primary_links WHERE topic=?';args=[topic]
        if q.get('band'):sql+=' AND band=?';args.append(q['band'])
        return [r[0] for r in db.execute(sql+' ORDER BY dataset_id LIMIT ? OFFSET ?',args+[size,start])]
    if '-S' in topic:
        rest={k:v for k,v in q.items() if k!='concept'}
        w,args=where(rest)
        sql='SELECT r.id FROM primary_links p CROSS JOIN records r ON r.id=p.dataset_id'+w+(' AND ' if w else ' WHERE ')+'p.topic=?'
        if q.get('band'):sql+=' AND p.band=?';args=args+[topic,q['band']]
        else:args=args+[topic]
        return [r[0] for r in db.execute(sql+' ORDER BY r.id LIMIT ? OFFSET ?',args+[size,start])]
    w,args=where(q)
    return [r[0] for r in db.execute('SELECT r.id FROM records r'+w+' ORDER BY r.id LIMIT ? OFFSET ?',args+[size,start])]

def snapshot(query=None,include_datasets=True):
    q=query or {};w,args=where(q)
    result=dict(base_catalog())
    with connection() as db:
        n=total(db,q,w,args)
        page=min(max(1,int(q.get('page',1))),max(1,math.ceil(n/30)))
        ids=page_ids(db,q,(page-1)*30,30)
        result.update(total=n,page=page,pages=max(1,math.ceil(n/30)),datasets=records(db,ids) if include_datasets else [])
    return result

def graph_overview(query=None):
    q={k:v for k,v in (query or {}).items() if v and k in {'band','concept','source','year','q','branch'}}
    return _graph(tuple(sorted(q.items())))

@lru_cache(maxsize=256)
def _graph(key):
    q=dict(key);w,args=where(q);nodes=[];edges=[]
    with connection() as db:
        n=total(db,q,w,args)
        if not q.get('band'):
            for band,title in BANDS.items():
                bq={**q,'band':band};qw,qa=where(bq);count=total(db,bq,qw,qa)
                nodes.append({'id':'band:'+band,'kind':'band','title':title,'count':count,'query':{**q,'band':band}})
        elif q['band']!='unclassified' and '-S' not in q.get('concept',''):
            sub=bool(q.get('concept'));col='t.id' if sub else 't.main_id';label='t.name' if sub else 't.main_name'
            # counts is preaggregated for the common, unfiltered graph path.
            if not any(q.get(k) for k in ('q','source','year')):
                sql='SELECT DISTINCT '+col+' id,'+label+" title,c.n FROM primary_counts c JOIN topics t ON "+('t.id=c.topic' if sub else "t.main_id=c.main_id")+" WHERE c.band=? AND "+("c.main_id=? AND c.topic!=''" if sub else "c.main_id!='' AND c.topic=''")
                found=db.execute(sql,[q['band']]+([q['concept']] if sub else [])).fetchall()
            else:
                found=db.execute('SELECT '+col+' id,'+label+' title,count(DISTINCT r.id) n FROM records r JOIN primary_links l ON l.dataset_id=r.id JOIN topics t ON t.id=l.topic'+w+(' AND t.main_id=?' if sub else '')+' GROUP BY '+col,args+([q['concept']] if sub else [])).fetchall()
            nodes=[{'id':'topic:'+r['id'],'kind':'subtopic' if sub else 'main','title':r['title'],'count':r['n'],'query':{**q,'concept':r['id']}} for r in found]
        elif not q.get('source'):
            if '-S' in q.get('concept',''):
                rest={k:v for k,v in q.items() if k!='concept'};rw,ra=where(rest)
                sql='SELECT r.source,count(*) n FROM primary_links p CROSS JOIN records r ON r.id=p.dataset_id'+rw+(' AND ' if rw else ' WHERE ')+'p.topic=? AND p.band=? GROUP BY r.source'
                found=db.execute(sql,ra+[q['concept'],q['band']]).fetchall()
            else:found=db.execute('SELECT r.source,count(DISTINCT r.id) n FROM records r'+w+' GROUP BY r.source',args).fetchall()
            names={r['id']:json.loads(r['info'])['name'] for r in db.execute('SELECT id,info FROM cat.sources')}
            nodes=[{'id':'source:'+r['source'],'kind':'source','title':names.get(r['source'],r['source']),'count':r['n'],'query':{**q,'source':r['source']}} for r in found]
        else:
            start=0;size=n;branch=q.get('branch','')
            if len(branch)>64:raise ValueError('잘못된 자료 묶음')
            for part in branch.split('.') if branch else []:
                if not part.isdigit() or int(part)>=32 or size<=48:raise ValueError('잘못된 자료 묶음')
                width=math.ceil(size/32);off=int(part)*width
                if off>=size:raise ValueError('잘못된 자료 묶음')
                start+=off;size=min(width,size-off)
            if size>48:
                width=math.ceil(size/32)
                boundary_ids=page_ids(db,q,start,size)
                endpoints={boundary_ids[off] for off in range(0,len(boundary_ids),width)}|{boundary_ids[min(off+width,len(boundary_ids))-1] for off in range(0,len(boundary_ids),width)}
                titles_by_id=dict(db.execute('SELECT id,title FROM records WHERE id IN ('+','.join('?' for _ in endpoints)+')',list(endpoints))) if endpoints else {}
                for i,off in enumerate(range(0,size,width)):
                    path=(branch+'.' if branch else '')+str(i)
                    count=min(width,size-off)
                    titles=[titles_by_id.get(boundary_ids[index],'') for index in (off,min(off+count,len(boundary_ids))-1)]
                    nodes.append({'id':'range:'+path,'kind':'range','title':titles[0][:22]+' … '+titles[1][:22], 'count':count,'query':{**q,'branch':path}})
            else:
                ids=page_ids(db,q,start,size)
                for record in records(db,ids):
                    edge=next((m for m in record['mappings'] if m['concept_id']==q.get('concept')),None)
                    nodes.append({'id':record['id'],'kind':'dataset','title':record['title'],'count':1,'record':record,
                                  'confidenceBand':record['confidence_band'],'scoreAlpha':edge['opacity'] if edge else .5})
        for node in nodes:
            if node['kind']=='dataset':
                edges.extend({'a':node['id'],'b':'topic:'+m['concept_id'],'opacity':m['opacity'],'band':node['record']['confidence_band']} for m in node['record']['mappings'])
    return {'nodes':nodes,'edges':edges,'total':n,'represented':n,'query':q}

def initial_overview():
    result=dict(base_catalog());result['graph']=graph_overview();result['graph_children']=[graph_overview(n['query']) for n in result['graph']['nodes']]
    result['graph_children'] += [graph_overview(n['query']) for g in list(result['graph_children']) for n in g['nodes'] if n['kind']=='main']
    return result
