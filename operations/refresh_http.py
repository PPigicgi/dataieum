"""Bounded provider requests, retry pacing and per-run restart cache."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
import fcntl
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import shutil
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def next_pages(url, raw, depth):
    """Only independent numbered catalogue pages may be prefetched; never PIT cursors."""
    p=urllib.parse.urlsplit(url);q=dict(urllib.parse.parse_qsl(p.query))
    try:data=json.loads(raw)
    except (ValueError,UnicodeError):return []
    if p.path.endswith('/package_search') and {'rows','start'} <= q.keys() and data.get('success') is True:
        size=int(q['rows']);start=int(q['start']);total=int(data['result']['count'])
        values=[('start',start+size*i) for i in range(1,depth+1) if start+size*i<total]
    elif p.path.endswith('/datasets/') and {'page_size','page'} <= q.keys() and 'total' in data:
        size=int(q['page_size']);page=int(q['page']);total=int(data['total'])
        values=[('page',page+i) for i in range(1,depth+1) if (page+i-1)*size<total]
    else:return []
    return [urllib.parse.urlunsplit((p.scheme,p.netloc,p.path,urllib.parse.urlencode({**q,k:str(v)}),'')) for k,v in values]


class ProviderHTTP:
    def __init__(self, root, profile, *, reserve_bytes=None, global_slots=None):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
        self.profile=profile
        self.reserve_bytes=reserve_bytes if reserve_bytes is not None else max(12*1024**3,int(shutil.disk_usage(self.root).total*.28))
        self.global_slots=Path(global_slots) if global_slots else None
        if self.global_slots:self.global_slots.mkdir(parents=True,exist_ok=True)
        self.penalty=1.;self.stable_requests=0
        self.lock=threading.Lock();self.next_request=0.;self.occurrences={};self.futures={}
        self.stats={'requests':0,'cached':0,'bytes':0,'retries':0,'http_errors':{},'global_wait_seconds':0.,'latency_seconds':0.}
        self.pool=ThreadPoolExecutor(max_workers=profile['parallel_requests'])
        parent=self
        class Redirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self,req,fp,code,msg,headers,newurl):
                parent.validate(newurl)
                return super().redirect_request(req,fp,code,msg,headers,newurl)
        self.opener=urllib.request.build_opener(Redirect)

    def snapshot(self):
        with self.lock:return {**self.stats,'http_errors':dict(self.stats['http_errors']),'interval_multiplier':self.penalty}

    @contextmanager
    def slot(self):
        # Advisory locks are released on process exit. A reboot cannot strand a
        # lease or reset the separate, durable site rotation checkpoint.
        if self.global_slots is None:
            yield;return
        started=time.monotonic();handle=None
        while handle is None:
            try:
                fields=dict(line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())
                memory=int(fields['MemAvailable'].split()[0])*1024
                pressure=os.getloadavg()[0]/max(1,os.cpu_count() or 1)
                limit=1 if memory<1024**3 else 3 if pressure>.8 or memory<2*1024**3 else 6
            except (OSError,KeyError,ValueError):limit=3
            for n in range(limit):
                candidate=(self.global_slots/str(n)).open('a')
                try:fcntl.flock(candidate,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:candidate.close()
                else:handle=candidate;break
            if handle is None:
                if time.monotonic()-started>120:raise TimeoutError('Global collection capacity unavailable')
                time.sleep(.1)
        with self.lock:self.stats['global_wait_seconds']+=time.monotonic()-started
        try:yield
        finally:handle.close()

    def feedback(self,seconds,failed=False):
        with self.lock:
            self.stats['latency_seconds']+=seconds
            if failed or seconds>self.profile['timeout_seconds']*.6:
                self.penalty=min(8.,self.penalty*2);self.stable_requests=0
            else:
                self.stable_requests+=1
                if self.stable_requests>=8:self.penalty=max(1.,self.penalty*.8);self.stable_requests=0

    def validate(self,url):
        p=urllib.parse.urlsplit(url)
        if p.scheme not in {'https','http'} or p.hostname not in self.profile['hosts'] or p.username or p.password or p.port not in {None,80,443}:
            raise ValueError('Provider URL outside the registered metadata hosts')
        addresses=socket.getaddrinfo(p.hostname,p.port or (443 if p.scheme=='https' else 80),type=socket.SOCK_STREAM)
        if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
            raise ValueError('Provider URL resolved to a non-public address')

    def _load(self,url,key,limit,fields,payload,content_type,accept):
        path=self.root/(key+'.gz')
        if path.exists():
            with gzip.open(path,'rb') as f:raw=f.read(limit+1)
            if len(raw)>limit:raise ValueError('Catalog response exceeds size limit')
            with self.lock:self.stats['cached']+=1
            return raw
        self.validate(url)
        headers={'User-Agent':'Dataieum/1.0 public catalogue metadata refresh',
                 'Accept':accept or 'application/json,text/html,application/xml,*/*','Accept-Encoding':'gzip'}
        if fields:headers['X-Fields']=fields
        if content_type:headers['Content-Type']=content_type
        for attempt in range(self.profile['attempts']):
            with self.lock:
                pause=max(0,self.next_request-time.monotonic())
                self.next_request=max(time.monotonic(),self.next_request)+self.profile['minimum_interval_seconds']*self.penalty
            if pause:time.sleep(pause)
            started=time.monotonic()
            try:
                if shutil.disk_usage(self.root).free<self.reserve_bytes:raise RuntimeError('Refresh disk reserve reached')
                req=urllib.request.Request(url,headers=headers,data=payload)
                with self.lock:self.stats['requests']+=1
                with self.slot(), self.opener.open(req,timeout=self.profile['timeout_seconds']) as response:
                    stream=gzip.GzipFile(fileobj=response) if response.headers.get('Content-Encoding','').lower()=='gzip' else response
                    raw=stream.read(limit+1)
                if len(raw)>limit:raise ValueError('Catalog response exceeds size limit')
                self.feedback(time.monotonic()-started)
                with self.lock:self.stats['bytes']+=len(raw)
                tmp=path.with_suffix('.tmp')
                with gzip.open(tmp,'wb',compresslevel=1) as f:f.write(raw)
                tmp.replace(path)
                return raw
            except urllib.error.HTTPError as error:
                self.feedback(time.monotonic()-started,failed=error.code in {429,500,502,503,504})
                with self.lock:
                    codes=self.stats['http_errors'];codes[str(error.code)]=codes.get(str(error.code),0)+1
                if error.code not in {429,500,502,503,504} or attempt+1>=self.profile['attempts']:raise
                value=error.headers.get('Retry-After','');delay=2**(attempt+1)
                if value:
                    delay=max(delay,int(value) if value.isdigit() else parsedate_to_datetime(value).timestamp()-time.time())
                if delay>300:raise
                with self.lock:
                    self.next_request=max(self.next_request,time.monotonic()+delay);self.stats['retries']+=1
            except (TimeoutError,urllib.error.URLError):
                self.feedback(time.monotonic()-started,failed=True)
                if attempt+1>=self.profile['attempts']:raise
                with self.lock:
                    self.next_request=max(self.next_request,time.monotonic()+2**(attempt+1));self.stats['retries']+=1

    def fetch(self,url,limit=20_000_000,fields=None,payload=None,content_type=None,accept=None):
        if not 0<limit<=256_000_000:raise ValueError('Unbounded metadata response')
        args=(limit,fields,payload,content_type,accept)
        def key_for(u,ordinal):
            return hashlib.sha256(json.dumps([u,limit,fields,(payload or b'').hex(),content_type,accept,ordinal],sort_keys=True).encode()).hexdigest()
        with self.lock:
            base=key_for(url,0);ordinal=self.occurrences.get(base,0);self.occurrences[base]=ordinal+1
            key=key_for(url,ordinal)
            future=self.futures.get(key)
            if future is None:
                future=self.pool.submit(self._load,url,key,*args);self.futures[key]=future
        try:raw=future.result()
        finally:
            with self.lock:self.futures.pop(key,None)
        if not payload and ordinal==0:
            for following in next_pages(url,raw,self.profile['parallel_requests']-1):
                follow_key=key_for(following,0)
                with self.lock:
                    if (follow_key not in self.futures and len(self.futures)<self.profile['parallel_requests']*2
                            and not self.occurrences.get(follow_key)):
                        self.futures[follow_key]=self.pool.submit(self._load,following,follow_key,*args)
        return raw

    def close(self):
        self.pool.shutdown(wait=True,cancel_futures=True)
