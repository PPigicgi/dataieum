"""Bounded catalogue pages, separate exact counts, and version-bound keysets."""
import base64
import hashlib
import json
import unicodedata


class StaleCursor(ValueError):
    pass


def publication(native):
    return hashlib.sha256(json.dumps(native.identity(),separators=(',',':')).encode()).hexdigest()[:24]


def filters(native, query):
    value={k:v for k,v in query.items() if k!='cursor' and v}
    if value.get('q'):value['q']=unicodedata.normalize('NFC',value['q'].strip())
    native.query(value)
    return value


def fingerprint(query):
    return hashlib.sha256(json.dumps(query,sort_keys=True,separators=(',',':')).encode()).hexdigest()[:24]


def decode_cursor(value, version, query):
    if not value:return 0
    if len(value)>512:raise ValueError('cursor too large')
    try:
        raw=json.loads(base64.b64decode(value+'='*(-len(value)%4),altchars=b'-_',validate=True))
        if not isinstance(raw,dict) or set(raw)!={'v','q','n'}:raise ValueError('invalid cursor')
        if type(raw['n']) is not int or not 0<raw['n']<2**63:raise ValueError('invalid cursor position')
        if raw['v']!=version:raise StaleCursor('publication changed')
        if raw['q']!=fingerprint(query):raise ValueError('cursor filters changed')
        return raw['n']
    except (TypeError,UnicodeError,json.JSONDecodeError) as exc:
        raise ValueError('invalid cursor') from exc


def encode_cursor(n,version,query):
    return base64.urlsafe_b64encode(json.dumps({'v':version,'q':fingerprint(query),'n':n},separators=(',',':')).encode()).decode().rstrip('=')


def candidates(native, query, after=None):
    # The FTS row number is stable within a publication and avoids a full ID sort
    # or deep OFFSET. Filtering is identical for pages and the separate count.
    rest={k:v for k,v in query.items() if k!='q'}
    clause,args=native.module.where(rest)
    predicate=clause.removeprefix(' WHERE ') or '1'
    if query.get('q'):
        from .keyword_index import lookup
        table,expression=lookup(query['q'])
        sql=f'FROM keyword.{table} CROSS JOIN keyword.documents d ON d.n={table}.rowid JOIN records r ON r.id=d.id WHERE {table} MATCH ? AND ({predicate})'
        args=[expression,*args]
        if after is not None:sql+=f' AND {table}.rowid>?';args.append(after)
    else:
        sql=f'FROM keyword.documents d JOIN records r ON r.id=d.id WHERE ({predicate})'
        if after is not None:sql+=' AND d.n>?';args.append(after)
    return sql,args


SUMMARY_FIELDS=('id','source_id','title','description','reference_year','reference_years','mappings','url')


def page(native, query):
    version=publication(native);q=filters(native,query)
    after=decode_cursor(query.get('cursor',''),version,q)
    sql,args=candidates(native,q,after)
    order='d.n'
    if q.get('q'):
        from .keyword_index import lookup
        order=lookup(q['q'])[0]+'.rowid'
    with native.connection() as db:
        if getattr(native,'postgres',None) and not q.get('q'):
            rows=native.postgres.page_ids(q,after)
        else:
            rows=db.execute('SELECT d.n,d.id '+sql+' ORDER BY '+order+' LIMIT 31',args).fetchall()
        records=native.module.records(db,[r[1] for r in rows[:30]])
    items=[]
    for record in records:
        value={key:record[key] for key in SUMMARY_FIELDS if key in record}
        value['description']=str(value.get('description',''))[:320]
        value['mappings']=[{k:m[k] for k in ('concept_id','status','confidence','opacity') if k in m}
                           for m in value.get('mappings',[])[:32]]
        items.append(value)
    if publication(native)!=version:raise StaleCursor('publication changed')
    return {'datasets':items,'has_more':len(rows)>30,
            'next_cursor':encode_cursor(rows[29][0],version,q) if len(rows)>30 else None,
            'publication':version}


def count(native,query):
    version=publication(native);q=filters(native,query)
    if not q:total=native.module.base_catalog()['total']
    else:
        sql,args=candidates(native,q)
        with native.connection() as db:total=db.execute('SELECT count(*) '+sql,args).fetchone()[0]
    if publication(native)!=version:raise StaleCursor('publication changed')
    return {'total':total,'publication':version}


def detail(native,query):
    if set(query)!={'id'} or not isinstance(query['id'],str) or not 0<len(query['id'])<=2048:raise ValueError('invalid ID')
    with native.connection() as db:
        if not db.execute('SELECT 1 FROM records WHERE id=?',(query['id'],)).fetchone():return {'dataset':None}
        records=native.module.records(db,[query['id']])
    return {'dataset':records[0] if records else None,'publication':publication(native)}
