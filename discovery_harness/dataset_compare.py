"""Compare a bounded selection of stored metadata, without collection or model calls."""
import json
from urllib.parse import urlsplit

MAX_RECORDS=4
FIELDS=('publisher','region','period','source_modified','format','license','checked_at','description')
UNKNOWN={'','미상','제공처 확인','정보 없음','unknown','n/a','not specified','null','-','—'}


def validate_query(query):
    if not isinstance(query,dict) or set(query)!={'ids'} or not isinstance(query['ids'],str):
        raise ValueError('Expected dataset IDs')
    if len(query['ids'].encode('utf-8'))>24576:raise ValueError('Selection too large')
    ids=json.loads(query['ids'])
    if not isinstance(ids,list) or not 2<=len(ids)<=MAX_RECORDS:raise ValueError('Select two to four records')
    for ident in ids:
        if not isinstance(ident,str) or not ident or len(ident)>2048 or any(ord(c)<32 or ord(c)==127 for c in ident):
            raise ValueError('Invalid record ID')
    if len(set(ids))!=len(ids):raise ValueError('Duplicate record ID')
    return ids


def text(value,limit=1200):
    if isinstance(value,list):value=' · '.join(v for v in value if isinstance(v,str))
    if not isinstance(value,str):return None
    value=' '.join(value.split())
    if value.casefold() in UNKNOWN:return None
    return value if len(value)<=limit else value[:limit]+'…'


def safe_url(value):
    if not isinstance(value,str) or len(value)>8192 or any(ord(c)<33 or ord(c)==127 for c in value):return None
    try:
        parts=urlsplit(value)
        if parts.scheme not in {'https','http'} or not parts.hostname or parts.username or parts.password:return None
        parts.port  # Reject malformed ports without rewriting a source URL.
    except ValueError:return None
    return value


def snapshot(db,query,postgres=None):
    ids=validate_query(query)
    placeholders=','.join('?' for _ in ids)
    # SQLite uses the existing ID index. A malformed/oversized metadata value cannot
    # expand the worker's response or trigger a full catalogue read.
    if postgres:
        postgres.check()
        rows=postgres.connect().execute('''SELECT id,source_id,left(title,2048) AS title,
            left(description,4001) AS description,checked_at,
            CASE WHEN octet_length(metadata::text)<=1048576 THEN metadata::text END AS metadata
            FROM datasets WHERE id=ANY(%s)''',(ids,)).fetchall()
    else:rows=db.execute(f'''SELECT id,source_id,substr(title,1,2048) AS title,
        substr(description,1,4001) AS description,checked_at,
        CASE WHEN length(CAST(metadata AS BLOB))<=1048576 THEN metadata END AS metadata
        FROM datasets WHERE id IN ({placeholders})''',ids).fetchall()
    by_id={row['id']:row for row in rows}
    sources={}
    for source in {row['source_id'] for row in rows}:
        if postgres:
            row=postgres.connect().execute('SELECT info FROM sources WHERE id=%s',(source,)).fetchone()
            info=row['info'] if row else {}
        else:
            row=db.execute('SELECT substr(info,1,65536) FROM sources WHERE id=?',(source,)).fetchone()
            try:info=json.loads(row[0]) if row else {}
            except (TypeError,ValueError):info={}
        sources[source]=text(info.get('name'),300) if isinstance(info,dict) else None
    items=[]
    for ident in ids:
        row=by_id.get(ident)
        if row is None:
            items.append({'id':ident,'found':False});continue
        try:meta=json.loads(row['metadata']) if row['metadata'] else None
        except (TypeError,ValueError):meta=None
        readable=isinstance(meta,dict);meta=meta if readable else {}
        values={key:text(meta.get(key)) for key in FIELDS}
        values['description']=text(row['description'],4000)
        values['checked_at']=text(row['checked_at'],100)
        items.append({'id':ident,'found':True,'title':text(row['title'],2048) or ident,
                      'source':sources.get(row['source_id']) or row['source_id'],
                      'url':safe_url(meta.get('url')) or safe_url(meta.get('metadata_url')),
                      'metadata_url':safe_url(meta.get('metadata_url')),
                      'license_url':safe_url(meta.get('license')),
                      'values':values,'metadata_available':readable})
    return {'items':items,'basis':'stored_metadata','selection_limit':MAX_RECORDS}
