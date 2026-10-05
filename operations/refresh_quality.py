"""Bounded, evidence-based metadata/link checks before embedding and publication."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import zlib
import hashlib
import html
from html.parser import HTMLParser
import http.client
import http.cookiejar
import ipaddress
import json
from pathlib import Path
import re
import socket
import sqlite3
import ssl
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

TTL=7*86400
MAX_BODY=196608
SCHEMA="""CREATE TABLE IF NOT EXISTS quality (
 dataset_id TEXT,content_hash TEXT,status TEXT NOT NULL,reason TEXT NOT NULL,
 evidence TEXT NOT NULL,checked_at REAL NOT NULL,expires_at REAL NOT NULL,
 PRIMARY KEY(dataset_id,content_hash));
CREATE INDEX IF NOT EXISTS quality_expiry ON quality(expires_at);"""

def setup(db):
    db.executescript(SCHEMA)

def norm(value):
    return ' '.join(unicodedata.normalize('NFKC',html.unescape(str(value or ''))).casefold().split())

def parse_record(raw):
    try:value=json.loads(raw) if isinstance(raw,str) else raw
    except (TypeError,ValueError):return None
    if not isinstance(value,dict):return None
    for key in ('source_id','title','url','metadata_url'):
        if value.get(key) is not None and not isinstance(value[key],str):return None
    return value

def identity_keys(ident,record,*,lookup=False):
    from refresh_state import native_identity
    keys={ident}
    native=native_identity(record)
    if native:keys.add(native)
    source=record.get('source_id') or ident.split(':',1)[0]
    native_id=record.get('native_id')
    if source!='us' and native_id and (source!='eu' or record.get('native_catalog')):
        payload=[source,record.get('native_catalog'),str(native_id),record.get('native_organization_id')]
        keys.add('identity:'+hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest())
    identifiers={str(record.get(k) or '') for k in ('external_id','native_id')}
    identifiers={v for v in identifiers if len(v)>=8}
    for key in ('url','metadata_url'):
        try:
            u=urllib.parse.urlsplit(record.get(key,''))
            pairs=urllib.parse.parse_qsl(u.query,keep_blank_values=True)
            components=set(urllib.parse.unquote(u.path).split('/'))|{v for k,v in pairs}
            # Shared portal landing pages are never an identity.
            if u.scheme not in ('http','https') or not u.hostname or (not lookup and not identifiers.intersection(components)):continue
            pairs=sorted((k,v) for k,v in pairs if not k.lower().startswith('utm_'))
            url=urllib.parse.urlunsplit((u.scheme,u.netloc.lower(),u.path,urllib.parse.urlencode(pairs),u.fragment))
            keys.add('url:'+hashlib.sha256((source+'\0'+url).encode()).hexdigest())
        except (TypeError,ValueError):pass
    return keys

def review_identity(record):
    title=norm(record.get('title'));publisher=norm(record.get('publisher'))
    if len(title)<8 or publisher in {'','미상','unknown','제공처 확인'}:return None
    fields=[record.get('source_id'),title,publisher,norm(record.get('period')),norm(record.get('region'))]
    return 'review:'+hashlib.sha256(json.dumps(fields,ensure_ascii=False).encode()).hexdigest()


def possible_deleted(db,record):
    key=review_identity(record)
    return bool(key and db.execute('SELECT 1 FROM excluded WHERE dataset_id=?',(key,)).fetchone())

def excluded_reason(db,ident,record):
    keys=sorted(identity_keys(ident,record,lookup=True))
    row=db.execute('SELECT reason FROM excluded WHERE dataset_id IN ('+','.join('?' for _ in keys)+') LIMIT 1',keys).fetchone()
    return row[0] if row else None

def record_exclusion(db,ident,record,reason):
    # Call before removing a catalogue record; a failed later removal remains safe.
    db.executemany('INSERT OR IGNORE INTO excluded(dataset_id,reason) VALUES(?,?)',
                   [(key,reason) for key in sorted(identity_keys(ident,record))])
    review=review_identity(record)
    if review:db.execute('INSERT OR IGNORE INTO excluded VALUES(?,?)',(review,'possible_deleted_identity'))

def register_deleted_rows(root,rows,reason):
    from refresh_state import connect,transaction
    with closing(connect(Path(root))) as db,transaction(db):
        for row in rows:
            record=parse_record(row.get('metadata')) or {}
            record_exclusion(db,row['id'],record,reason)


def allowed(db,row,now=None):
    now=time.time() if now is None else now
    record=parse_record(row['metadata'])
    if record is None:return False
    if excluded_reason(db,row['dataset_id'],record) or possible_deleted(db,record):return False
    return db.execute("""SELECT 1 FROM quality WHERE dataset_id=? AND content_hash=?
      AND status='verified' AND expires_at>?""",(row['dataset_id'],row['content_hash'],now)).fetchone() is not None

def save_result(db,row,result):
    now=time.time()
    db.execute("""INSERT INTO quality VALUES(?,?,?,?,?,?,?)
      ON CONFLICT(dataset_id,content_hash) DO UPDATE SET status=excluded.status,
      reason=excluded.reason,evidence=excluded.evidence,checked_at=excluded.checked_at,expires_at=excluded.expires_at""",
      (row['dataset_id'],row['content_hash'],result['status'],result['reason'],
       json.dumps(result.get('evidence',{}),ensure_ascii=False),now,now+TTL if result['status']=='verified' else now))

def validate_public_url(url):
    u=urllib.parse.urlsplit(url)
    if u.scheme not in ('http','https') or not u.hostname or u.username or u.password or u.port not in (None,80,443):
        raise ValueError('unsafe_url')
    if len(url)>8192 or any(ord(c)<33 for c in url):raise ValueError('unsafe_url')
    return u

def address(host,port):
    addresses=socket.getaddrinfo(host,port,type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(x[4][0]).is_global for x in addresses):
        raise ValueError('non_public_address')
    return sorted(addresses,key=lambda x:x[0]!=socket.AF_INET)[0][4][0]

class HTTP(http.client.HTTPConnection):
    def connect(self):
        self.sock=socket.create_connection((address(self.host,self.port),self.port),self.timeout)
class HTTPS(http.client.HTTPSConnection):
    def connect(self):
        sock=socket.create_connection((address(self.host,self.port),self.port),self.timeout)
        self.sock=self._context.wrap_socket(sock,server_hostname=self.host)
class HTTPHandler(urllib.request.HTTPHandler):
    def http_open(self,req):return self.do_open(HTTP,req)
class HTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self,req):return self.do_open(HTTPS,req,context=ssl.create_default_context())
class Redirect(urllib.request.HTTPRedirectHandler):
    max_redirections=5
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        validate_public_url(newurl)
        return super().redirect_request(req,fp,code,msg,headers,newurl)

def read_response(response,binary=False):
    if binary:return b''
    decoder=zlib.decompressobj(16+zlib.MAX_WBITS) if response.headers.get('Content-Encoding','').lower()=='gzip' else None
    data=bytearray();received=0;deadline=time.monotonic()+20
    while len(data)<=MAX_BODY:
        if time.monotonic()>deadline:raise TimeoutError('Response deadline exceeded')
        chunk=response.read1(min(8192,MAX_BODY+1-len(data)))
        if not chunk:break
        received+=len(chunk)
        if received>MAX_BODY*2+65536:raise ValueError('Compressed response limit exceeded')
        data.extend(decoder.decompress(chunk,MAX_BODY+1-len(data)) if decoder else chunk)
    return bytes(data)

class Fetcher:
    def __init__(self,interval=.5,profiles=None):
        self.interval=interval;self.lock=threading.Lock();self.next={}
        self.intervals={};self.limits={};self.semaphores={}
        for source in (profiles or {}).get('sources',[]):
            for host in source['hosts']:
                self.intervals[host]=max(.2,source.get('minimum_interval_seconds',interval))
                self.limits[host]=max(1,min(2,source.get('parallel_requests',1)))
    def __call__(self,url):
        host=validate_public_url(url).hostname
        with self.lock:
            semaphore=self.semaphores.setdefault(host,threading.BoundedSemaphore(self.limits.get(host,1)))
        with semaphore:return self._request(url)
    def _request(self,url):
        u=validate_public_url(url)
        with self.lock:
            now=time.monotonic();start=max(now,self.next.get(u.hostname,now))
            self.next[u.hostname]=start+self.intervals.get(u.hostname,self.interval)
        if start>now:time.sleep(start-now)
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),HTTPHandler(),HTTPSHandler(),Redirect(),
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        request=urllib.request.Request(url,headers={'User-Agent':'Dataieum metadata quality check/1.0',
            'Accept':'text/html,application/json,application/ld+json,text/plain;q=0.5','Accept-Encoding':'gzip'})
        try:
            with opener.open(request,timeout=12) as response:
                kind=response.headers.get('Content-Type','').lower()
                binary=(not any(x in kind for x in ('html','json','xml','text/plain')) and bool(kind)) or urllib.parse.urlsplit(response.url).path.lower().endswith(('.csv','.tsv','.zip','.gz','.xlsx','.xls','.parquet','.pdf'))
                raw=read_response(response,binary)
                encoding=response.headers.get_content_charset() or 'utf-8'
                if encoding.lower() not in {'utf-8','utf8','euc-kr','cp949','iso-8859-1','windows-1252','utf-16'}:encoding='utf-8'
                return {'code':response.status,'url':response.url,'type':kind,'binary':binary,
                    'truncated':len(raw)>MAX_BODY,'text':raw[:MAX_BODY].decode(encoding,'replace')}
        except urllib.error.HTTPError as error:
            if error.code==429:
                with self.lock:self.next[u.hostname]=max(self.next.get(u.hostname,0),time.monotonic()+60)
            return {'code':error.code,'url':error.url,'type':'','text':'','binary':False,'truncated':False}

def identities(record):
    return {str(record[k]) for k in ('external_id','native_id','id') if record.get(k)}

def json_identity(text,record):
    try:data=json.loads(text)
    except (ValueError,TypeError):return False
    expected=identities(record)
    candidates=[]
    if isinstance(data,dict):
        candidates=[data]
        for key in ('result','data','dataset'):
            value=data.get(key)
            if isinstance(value,dict):candidates.append(value)
    for item in candidates:
        if any(str(item.get(k,'')) in expected for k in ('id','identifier','dataset_id','dataSetId','name')):
            if item.get('success') is not False:
                variants=[]
                for key in ('title','title_translated'):
                    value=item.get(key)
                    if isinstance(value,str):variants.append(norm(value))
                    elif isinstance(value,dict):variants.extend(norm(x) for x in value.values() if isinstance(x,str))
                if not variants or norm(record.get('title')) in variants:return True
    return False

class PageFields(HTMLParser):
    def __init__(self):super().__init__(convert_charrefs=True);self.fields={};self.canonical=None;self.meta={}
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag.lower()=='input' and a.get('name',a.get('id')):
            key=a.get('name',a.get('id'));value=a.get('value','')
            if value or key not in self.fields:self.fields[key]=value
        if tag.lower()=='meta':self.meta[a.get('property',a.get('name',''))]=a.get('content','')
        if tag.lower()=='link' and 'canonical' in a.get('rel','').lower().split():self.canonical=a.get('href')


def page_identity(text,record):
    title=norm(record.get('title'));p=PageFields();p.feed(text)
    if len(title)<4:return False
    url=urllib.parse.urlsplit(record.get('url',''));query=dict(urllib.parse.parse_qsl(url.query))
    if url.hostname=='kosis.kr':
        if all(query.get(k) and query[k]==p.fields.get(k) for k in ('orgId','tblId')):
            return title in {norm(p.fields.get(k)) for k in ('tblNm','tblEngNm')}
    if url.hostname=='data.seoul.go.kr':
        parts=url.path.split('/')
        if len(parts)>3 and parts[1]=='dataList' and p.fields.get('infId')==parts[2]:
            if norm(p.fields.get('infNm'))==title:return True
    if url.hostname=='data.daegu.go.kr':
        # The portal embeds its official detail JSON in a Vue data property.
        for match in re.finditer(r'\bdataSetInfo\s*:\s*(?=\{)',text):
            try:item,_=json.JSONDecoder().raw_decode(text[match.end():])
            except ValueError:continue
            if str(item.get('dataSetId','')) in identities(record) and norm(item.get('dataName'))==title:return True
    if any(p.fields.get(k) in identities(record) for k in ('infId','dataSetId','datasetId')):
        if title in {norm(p.fields.get(k)) for k in ('infNm','dataSetNm','datasetNm')}:return True
    headings=[norm(re.sub('<[^>]+>',' ',x)) for x in re.findall(r'<(?:title|h1)\b[^>]*>(.*?)</(?:title|h1)>',text,re.I|re.S)]
    if title in headings and p.canonical:
        canonical=urllib.parse.urljoin(record.get('url',''),p.canonical)
        u=urllib.parse.urlsplit(canonical);components=set(urllib.parse.unquote(u.path).split('/'))|{v for k,v in urllib.parse.parse_qsl(u.query)}
        if canonical==record.get('url') and identities(record).intersection(components):return True
    for script in re.findall(r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',text,re.I|re.S):
        try:data=json.loads(html.unescape(script))
        except ValueError:continue
        for item in data if isinstance(data,list) else [data]:
            if isinstance(item,dict) and item.get('@type')=='Dataset' and str(item.get('identifier','')) in identities(record):
                if norm(item.get('name'))==title:return True
    return False


def soft_error(text):
    title=re.search(r'<title\b[^>]*>(.*?)</title>',text,re.I|re.S)
    value=norm(re.sub('<[^>]+>',' ',title.group(1))) if title else ''
    return value in {'404','404 not found','page not found','not found','error 404',
                     '페이지를 찾을 수 없습니다','요청하신 페이지를 찾을 수 없습니다'}

def result(status,reason,**evidence):
    return {'status':status,'reason':reason,'evidence':evidence}

def metadata_detail(url,record):
    # A whole catalogue/ZIP or a list form is not a per-record lookup.
    u=urllib.parse.urlsplit(url)
    parts=set(urllib.parse.unquote(u.path).split('/'))|{v for k,v in urllib.parse.parse_qsl(u.query)}
    return bool(identities(record).intersection(parts))


def inspect_record(record,fetch,hosts):
    if record.get('_refresh_hold'):return result('hold',str(record['_refresh_hold']))
    if norm(record.get('title')) in {'','test','((name))'}:return result('hold','placeholder_title')
    url=record.get('url')
    if not isinstance(url,str) or not url:return result('hold','missing_access_url')
    try:
        validate_public_url(url)
        page=fetch(url)
        evidence={'url':url,'final_url':page['url'],'http':page['code'],
                  'truncated':page.get('truncated',False),
                  'response_excerpt_sha256':hashlib.sha256(page['text'].encode()).hexdigest()}
        heading=re.search(r'<title\b[^>]*>(.*?)</title>',page['text'],re.I|re.S)
        if heading:evidence['page_title']=norm(re.sub('<[^>]+>',' ',heading.group(1)))[:300]
        if page['code'] in (404,410):return result('repeat','http_missing',**evidence)
        if page['code'] in (403,429):return result('hold','blocked_or_rate_limited',**evidence)
        if page['code']<200 or page['code']>=300:return result('hold','http_error',**evidence)
        text=page['text']
        if soft_error(text) or (urllib.parse.urlsplit(url).hostname=='kosis.kr' and '통계표 정보가 없습니다.' in text and '통계청::error' in text):return result('repeat','soft_error_page',**evidence)
        before=urllib.parse.urlsplit(url);after=urllib.parse.urlsplit(page['url'])
        if before.path not in ('','/') and after.path in ('','/'):
            return result('hold','redirected_to_homepage',**evidence)
        if page_identity(text,record) or ('json' in page.get('type','') and json_identity(text,record)):
            return result('verified','page_identity_matches',**evidence)
        meta=record.get('metadata_url')
        if meta and meta!=url and urllib.parse.urlsplit(meta).hostname in hosts and metadata_detail(meta,record):
            response=fetch(meta)
            evidence.update(metadata_url=meta,metadata_http=response['code'])
            if response['code']==200 and json_identity(response['text'],record):
                return result('verified','official_identity_and_access_response',**evidence)
        return result('hold','identity_not_confirmed',**evidence)
    except (OSError,ValueError,TypeError,TimeoutError,urllib.error.URLError,http.client.HTTPException,zlib.error) as error:
        return result('hold','request_unverifiable',error_type=type(error).__name__,url=str(url)[:8192])

def audit(root,folder,profiles,*,limit=None,expected=None):
    from refresh_state import connect
    from concurrent.futures import wait,FIRST_COMPLETED
    from collections import Counter,deque
    import fcntl
    folder.mkdir(parents=True,exist_ok=True)
    with (root/'quality-worker.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=connect(root);setup(db)
        tasks=sqlite3.connect(folder/'audit.sqlite3',timeout=30);tasks.row_factory=sqlite3.Row
        tasks.executescript("""CREATE TABLE IF NOT EXISTS targets(
          dataset_id TEXT PRIMARY KEY,content_hash TEXT,was_published INTEGER,source TEXT);
          CREATE TABLE IF NOT EXISTS results(dataset_id TEXT PRIMARY KEY,content_hash TEXT,
          status TEXT,reason TEXT,evidence TEXT,checked_at REAL);
          CREATE TABLE IF NOT EXISTS queue(dataset_id TEXT PRIMARY KEY,source TEXT,due REAL,reason TEXT);
          CREATE INDEX IF NOT EXISTS audit_queue_source ON queue(source,due,dataset_id);""")
        manifest=folder/'manifest.json'
        if not manifest.exists():
            if tasks.execute('SELECT count(*) FROM targets').fetchone()[0]:
                raise RuntimeError('Incomplete manifest; preserve targets for inspection')
            h=hashlib.sha256();count=0;published=0
            for row in db.execute("SELECT dataset_id,content_hash,state,source FROM changes WHERE state IN ('ready','published') ORDER BY dataset_id"):
                tasks.execute('INSERT INTO targets VALUES(?,?,?,?)',(row[0],row[1],row[2]=='published',row[3]))
                tasks.execute('INSERT INTO queue VALUES(?,?,0,NULL)',(row[0],row[3]))
                h.update(json.dumps([row[0],row[1]],ensure_ascii=False).encode()+b'\n');count+=1;published+=row[2]=='published'
            tasks.commit()
            if not count:raise RuntimeError('No audit targets')
            if expected is not None and count!=expected:raise RuntimeError('Audit target count differs')
            manifest.write_text(json.dumps({'target_records':count,'published_records':published,
                'target_sha256':h.hexdigest(),'created_at':time.time()},indent=2)+'\n')
        m=json.loads(manifest.read_text())
        if expected is not None and m['target_records']!=expected:raise RuntimeError('Frozen target differs')
        fetch=Fetcher(profiles=profiles)
        by_source={p['id']:p for p in profiles['sources']}
        sources=deque(r[0] for r in tasks.execute('SELECT DISTINCT source FROM targets ORDER BY source'))
        counts=Counter(dict(tasks.execute('SELECT status,count(*) FROM results GROUP BY status')))
        http_records=tasks.execute("SELECT count(*) FROM results WHERE json_extract(evidence,'$.http') IS NOT NULL").fetchone()[0]
        handled=0;last_report=0
        def report(stage):
            data={'stage':stage,'target_records':m['target_records'],'checked_records':sum(counts.values()),
                  'counts':dict(counts),'http_response_records':http_records,
                  'retry_waiting':tasks.execute('SELECT count(*) FROM queue WHERE due>0').fetchone()[0],
                  'updated_at':time.time()}
            p=folder/'audit-status.json';t=p.with_suffix('.tmp');t.write_text(json.dumps(data,indent=2)+'\n');t.replace(p)
            return data
        def finish(target,row,outcome):
            nonlocal handled,http_records
            ident=target['dataset_id']
            if outcome['status']=='repeat':
                if target['reason']==outcome['reason']:
                    outcome['status']='excluded';outcome['evidence']['repeat_confirmed']=True
                elif target['reason']:
                    outcome=result('hold','inconsistent_error_responses',**outcome['evidence'])
                else:
                    with tasks:tasks.execute('UPDATE queue SET due=?,reason=? WHERE dataset_id=?',(time.time()+60,outcome['reason'],ident))
                    return
            if row:
                with db:save_result(db,row,outcome)
            with tasks:
                tasks.execute('INSERT INTO results VALUES(?,?,?,?,?,?)',
                  (ident,target['content_hash'],outcome['status'],outcome['reason'],
                   json.dumps(outcome['evidence'],ensure_ascii=False),time.time()))
                tasks.execute('DELETE FROM queue WHERE dataset_id=?',(ident,))
            counts[outcome['status']]+=1;http_records+=outcome['evidence'].get('http') is not None;handled+=1
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                active={};inflight=set();per_source=Counter()
                while True:
                    # Fair scheduling prevents one slow host from occupying all workers.
                    for _ in range(len(sources)*2):
                        if len(active)>=8 or (limit and handled+len(active)>=limit):break
                        source=sources[0];sources.rotate(-1)
                        capacity=max(1,min(2,by_source.get(source,{}).get('parallel_requests',1)))
                        if per_source[source]>=capacity:continue
                        candidates=tasks.execute("""SELECT t.*,q.reason FROM queue q JOIN targets t USING(dataset_id)
                          WHERE q.source=? AND q.due<=? ORDER BY q.due,q.dataset_id LIMIT ?""",(source,time.time(),capacity+1)).fetchall()
                        target=next((x for x in candidates if x['dataset_id'] not in inflight),None)
                        if target is None:continue
                        row=db.execute('SELECT * FROM changes WHERE dataset_id=? AND content_hash=?',(target['dataset_id'],target['content_hash'])).fetchone()
                        if not row:finish(target,None,result('hold','input_changed'));continue
                        record=parse_record(row['metadata'])
                        if record is None:finish(target,row,result('hold','invalid_metadata'));continue
                        blocked=excluded_reason(db,row['dataset_id'],record)
                        if blocked:finish(target,row,result('excluded','previously_deleted',deletion_reason=blocked));continue
                        if possible_deleted(db,record):finish(target,row,result('hold','possible_deleted_identity'));continue
                        future=pool.submit(inspect_record,record,fetch,by_source.get(source,{}).get('hosts',[]))
                        active[future]=(target,row);inflight.add(target['dataset_id']);per_source[source]+=1
                    if active:
                        done,_=wait(active,timeout=5,return_when=FIRST_COMPLETED)
                        for future in done:
                            target,row=active.pop(future);inflight.remove(target['dataset_id']);per_source[target['source']]-=1
                            finish(target,row,future.result())
                    else:
                        if limit and handled>=limit:return report('running')
                        due=tasks.execute('SELECT min(due) FROM queue').fetchone()[0]
                        if due is None:break
                        report('retry_wait');time.sleep(min(15,max(.1,due-time.time())))
                    if time.monotonic()-last_report>=5:report('running');last_report=time.monotonic()
            missing=tasks.execute('SELECT count(*) FROM targets t LEFT JOIN results r USING(dataset_id) WHERE r.dataset_id IS NULL OR r.content_hash!=t.content_hash').fetchone()[0]
            extra=tasks.execute('SELECT count(*) FROM results r LEFT JOIN targets t USING(dataset_id) WHERE t.dataset_id IS NULL').fetchone()[0]
            digest=hashlib.sha256();target_count=0
            for row in tasks.execute('SELECT dataset_id,content_hash FROM targets ORDER BY dataset_id'):
                digest.update(json.dumps([row[0],row[1]],ensure_ascii=False).encode()+b'\n');target_count+=1
            if missing or extra or target_count!=m['target_records'] or digest.hexdigest()!=m['target_sha256']:
                raise RuntimeError('Audit target/result identities differ')
            return report('complete')
        except BaseException:
            report('interrupted');raise
        finally:tasks.close();db.close()


def check_pending(db,profiles,fetch=None,limit=64):
    fetch=fetch or Fetcher(profiles=profiles)
    setup(db)
    rows=db.execute("""SELECT c.* FROM changes c JOIN source_runs s ON s.rotation=c.rotation AND s.source=c.source
      LEFT JOIN quality q ON q.dataset_id=c.dataset_id AND q.content_hash=c.content_hash
      WHERE s.status='complete' AND c.state IN ('pending','ready','embedding') AND
      (q.dataset_id IS NULL OR (q.status='verified' AND q.expires_at<=?) OR
       (q.reason LIKE 'unconfirmed_%' AND q.checked_at<=?) OR
       (q.status='hold' AND q.checked_at<=?))
      ORDER BY c.dataset_id LIMIT ?""",(time.time(),time.time()-60,time.time()-86400,limit)).fetchall()
    by_source={p['id']:p for p in profiles['sources']}
    def check(row):
        record=parse_record(row['metadata'])
        if record is None:return result('hold','invalid_metadata')
        reason=excluded_reason(db,row['dataset_id'],record)
        if reason:return result('excluded','previously_deleted',deletion_reason=reason)
        if possible_deleted(db,record):return result('hold','possible_deleted_identity')
        return None
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs=[]
        for row in rows:
            fixed=check(row)
            jobs.append((row,fixed,None if fixed else pool.submit(inspect_record,json.loads(row['metadata']),fetch,by_source.get(row['source'],{}).get('hosts',[]))))
        for row,fixed,future in jobs:
            outcome=fixed if fixed else future.result()
            if outcome['status']=='repeat':
                prior=db.execute('SELECT reason,checked_at FROM quality WHERE dataset_id=? AND content_hash=?',(row['dataset_id'],row['content_hash'])).fetchone()
                if prior and prior[0]=='unconfirmed_'+outcome['reason'] and prior[1]<=time.time()-60:
                    outcome['status']='excluded';outcome['evidence']['repeat_confirmed']=True
                else:outcome['status']='hold';outcome['reason']='unconfirmed_'+outcome['reason']
            with db:save_result(db,row,outcome)
    return len(rows)


def check_delta(db,delta):
    db.row_factory=sqlite3.Row;delta.row_factory=sqlite3.Row
    for row in delta.execute('SELECT dataset_id,metadata,content_hash FROM changes'):
        if not allowed(db,row):raise RuntimeError('Publication quality gate blocked: '+row['dataset_id'])

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path('/refresh'))
    p.add_argument('--folder',type=Path,required=True);p.add_argument('--profiles',type=Path,default=Path('/app/operations/refresh-sources.json'))
    p.add_argument('--limit',type=int);p.add_argument('--expected',type=int);a=p.parse_args()
    print(json.dumps(audit(a.root,a.folder,json.loads(a.profiles.read_text()),limit=a.limit,expected=a.expected)),flush=True)
if __name__=='__main__':main()
