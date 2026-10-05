"""Two bounded tools: same-model query embedding and local cosine retrieval.

Credentials stay on the host. No dataset re-embedding, shell, arbitrary URL,
provider fallback, response logging, or automatic retries are exposed to agents.
"""
import asyncio
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

from .errors import HarnessError, AdapterContractError, BudgetExceeded
from .dependency_stage import DependencyStage, DependencyBusy, DependencyTimeout
from .policy import CachePolicy
from .resources import SelectiveCache, bounded_json
from .site_search import INDICATORS, selected_concept

MODEL = 'text-embedding-3-small'
DIMENSIONS = 1536
INPUT_CONTRACT = 'luna-semantic-query-v2'


class VectorUnavailable(HarnessError):
    code = 'vector_unavailable'


def query_text(query, plan):
    """Embed normalized meaning only; `query` is retained for call compatibility.

    Raw questions, purpose/reason prose and exact filters never get appended.
    Older clients and graph-node selections use their structured subject/label.
    """
    value = plan.get('semantic_query', '')
    if (not isinstance(value, str) or len(value) > 320
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise AdapterContractError('Invalid normalized embedding text')
    value = ' '.join(value.split())
    if not value:
        phrases = []
        for need in plan['needs']:
            label = INDICATORS[need['indicator']] if need['indicator'] != 'other' else ''
            if plan.get('selected_concept'):
                label = selected_concept(plan['selected_concept'])['label']
            phrase = ' '.join(dict.fromkeys(part for part in (need.get('subject','').strip(), label) if part))
            if phrase:phrases.append(phrase)
        value = '; '.join(dict.fromkeys(phrases))
    if not value or len(value) > 320 or len(value.encode('utf-8')) > 1280:
        raise AdapterContractError('Normalized data meaning is missing or too large')
    return value


def vector_value(value):
    if (not isinstance(value, list) or len(value) != DIMENSIONS or
            any(type(x) not in (int, float) or not math.isfinite(x) for x in value)):
        raise AdapterContractError('Embedding shape or values are invalid')
    norm = math.sqrt(math.fsum(x*x for x in value))
    if not math.isfinite(norm) or norm < 1e-12:
        raise AdapterContractError('Embedding has zero or invalid norm')
    return [x/norm for x in value]


class VectorTools:
    supports_exploration_control = True
    supports_time_series_control = True
    def __init__(self, key_file, gateway_token, *, client=None):
        self.key_file = Path(key_file) if key_file else None
        self.gateway_token = gateway_token
        endpoint = os.environ.get('DATAIEUM_VECTOR_URL', 'http://127.0.0.1:8093')
        target = urlsplit(endpoint)
        if (target.scheme != 'http' or target.hostname not in {'127.0.0.1','localhost','::1','vector'}
                or target.username is not None or target.password is not None
                or target.path not in {'','/'} or target.query or target.fragment):
            raise ValueError('Vector endpoint must be the configured internal service')
        self.vector_url = endpoint.rstrip('/')
        self.client = client  # A test transport, not a configurable remote URL.
        self.calls = {'embed_query': 0, 'cosine_search': 0}
        self.embedding_stage = DependencyStage(workers=4, queued=1021,wait_seconds=7200,max_waiters=1025)
        # Match the server's two search workers; its health connections are separate.
        self.search_stage = DependencyStage(workers=2, queued=1023,wait_seconds=7200,operation_seconds=30,max_waiters=1025)
        self._client = None
        self.http_outcomes = Counter()
        self._cache_clock = time.monotonic
        self.embedding_cache=SelectiveCache(CachePolicy(max_entries=128,max_size_bytes=2*1024*1024,
            max_entry_bytes=65536,ttl_seconds=600.),clock=lambda:self._cache_clock())
        self.search_cache = SelectiveCache(CachePolicy(max_entries=8,
            max_size_bytes=2*1024*1024, max_entry_bytes=512*1024, ttl_seconds=10.),
            clock=lambda:self._cache_clock())
        self.search_cache_events = Counter()
        self._search_generation = None
        self._search_health_at = None
        self._search_cache_epoch = 0
        self._health_sequence = self._applied_health_sequence = 0

    def status(self):
        return {'embedding':self.embedding_stage.status(), 'search':self.search_stage.status(),
                'embedding_cache':{**self.embedding_cache.status(),'ttl_seconds':600},
                'http_outcomes':dict(self.http_outcomes),
                'search_cache':{**self.search_cache.status(), **dict(self.search_cache_events),
                    'ttl_seconds':10, 'health_max_age_seconds':3,
                    'max_entry_bytes':512*1024,
                    'health_fresh':self._fresh_search_generation() is not None}}

    async def aclose(self):
        await asyncio.gather(self.embedding_stage.aclose(),self.search_stage.aclose())
        self._invalidate_search_cache('closed')
        self.embedding_cache.clear()
        if self._client is not None:await self._client.aclose()

    def configured(self):
        return bool(self.key_file and self.key_file.is_file())

    async def _json(self, method, url, *, headers=None, payload=None, maximum=524288, deadline=None):
        import httpx
        service = 'embedding' if 'api.openai.com' in url else 'vector'
        async def read(client):
            try:
                async with client.stream(method, url, headers=headers, json=payload) as response:
                    self.http_outcomes[f'{service}_{response.status_code}'] += 1
                    if response.status_code != 200:
                        if service == 'vector' and response.status_code in {503,504}:
                            error_body=bytearray()
                            async for chunk in response.aiter_bytes():
                                if len(error_body)+len(chunk)>1024:break
                                error_body.extend(chunk)
                            else:
                                try:error=json.loads(error_body)
                                except (ValueError,UnicodeError):error=None
                                if isinstance(error,dict):
                                    if response.status_code==503 and error.get('error')=='vector_busy':
                                        raise DependencyBusy('Vector service is saturated')
                                    if response.status_code==504 and error.get('error')=='vector_search_timeout':
                                        raise DependencyTimeout('Vector execution deadline exceeded')
                        # Do not propagate vendor bodies, credentials or the user query.
                        raise VectorUnavailable('Embedding or vector search service is unavailable')
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        content.extend(chunk)
                        if len(content) > maximum:
                            raise AdapterContractError('Vector tool response exceeds its byte budget')
                    return json.loads(content)
            except (httpx.HTTPError, ValueError) as error:
                self.http_outcomes[service+'_transport_error'] += 1
                raise VectorUnavailable('Vector tool request failed') from None
        async with asyncio.timeout(min(7, max(0, deadline-time.monotonic())) if deadline else 7):
            if self.client is not None:
                return await read(self.client)
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=6.5, follow_redirects=False, trust_env=False,
                    limits=httpx.Limits(max_connections=8,max_keepalive_connections=4))
            return await read(self._client)

    async def embed_query(self, text, *, budget_seconds=None):
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > 8192:
            raise AdapterContractError('Invalid embedding input')
        if not self.configured():
            raise VectorUnavailable('Query embedding key is not configured')
        try:
            if self.key_file.stat().st_size > 4096:
                raise VectorUnavailable('Invalid embedding credential file')
            key = self.key_file.read_text(encoding='utf-8-sig').strip()
        except (OSError, UnicodeError):
            raise VectorUnavailable('Embedding credential file is unavailable') from None
        if not key or any(c.isspace() for c in key):
            raise VectorUnavailable('Invalid embedding credential file')
        self.calls['embed_query'] += 1
        # Credential rotation cannot join work sent with the preceding credential.
        identity = hashlib.sha256((MODEL+'\0'+key+'\0'+text).encode()).hexdigest()
        value = await self.embedding_stage.run(identity, lambda deadline:self._cached_embedding(identity,lambda:self._json('POST', 'https://api.openai.com/v1/embeddings',
            headers={'Authorization': 'Bearer '+key}, deadline=deadline,
            payload={'model': MODEL, 'dimensions': DIMENSIONS, 'encoding_format': 'float', 'input': text})),
            budget_seconds=budget_seconds,queue_seconds=3)
        if (not isinstance(value, dict) or value.get('model') != MODEL or
                not isinstance(value.get('data'), list) or len(value['data']) != 1 or
                not isinstance(value['data'][0], dict) or type(value['data'][0].get('index')) is not int or
                value['data'][0]['index'] != 0):
            raise AdapterContractError('Embedding provider contract changed')
        return {'model': MODEL, 'vector': vector_value(value['data'][0].get('embedding')),
                'input_contract': INPUT_CONTRACT}

    async def _cached_embedding(self,identity,fetch):
        # Identity includes model, credential and exact normalized text. Public
        # vectors are bounded in RAM; input text and credentials are not retained.
        cached=self.embedding_cache.get('metadata',identity)
        if cached is not None:return cached
        value=await fetch()
        if (not isinstance(value,dict) or value.get('model')!=MODEL or
            not isinstance(value.get('data'),list) or len(value['data'])!=1 or
            not isinstance(value['data'][0],dict) or type(value['data'][0].get('index')) is not int or
            value['data'][0]['index']!=0):raise AdapterContractError('Embedding provider contract changed')
        normalized=vector_value(value['data'][0].get('embedding'))
        result={'model':MODEL,'data':[{'index':0,'embedding':normalized}]}
        self.embedding_cache.put('metadata',identity,result)
        return result

    async def cosine_search(self, embedding, source_ids=(), *, budget_seconds=None, queued=False, include_related=True, filter_groups=None, lexical_query=None, collapse_time_series=False):
        from .coverage import validate_groups
        if type(collapse_time_series) is not bool:raise ValueError("collapse_time_series must be boolean")
        if embedding.get('model') != MODEL:
            raise AdapterContractError('Embedding model mismatch')
        self.calls['cosine_search'] += 1
        payload={'model': MODEL, 'vector': vector_value(embedding.get('vector')),
                 'limit': 40, 'explore': True, 'include_related': include_related,
                 **({'source_ids': list(source_ids)} if source_ids else {}),
                 **({'collapse_time_series':True} if collapse_time_series else {}),
                 **({'filter_groups':validate_groups(filter_groups)} if filter_groups else {}),
                 **({'lexical_query':lexical_query} if lexical_query else {})}
        identity=hashlib.sha256(json.dumps([queued,payload],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
        value = await self.search_stage.run(identity, lambda deadline:self._json('POST', self.vector_url+'/search',
            headers={'X-Dataieum-Gateway-Token': self.gateway_token},
            payload={**payload,'timeout_ms':max(1,min(7000,int((deadline-time.monotonic())*1000)))},
            deadline=min(deadline,time.monotonic()+7)),budget_seconds=min(30 if queued else 7,budget_seconds if budget_seconds is not None else 7),queue_seconds=20 if queued else 3)
        if (not isinstance(value, dict) or not isinstance(value.get('candidates'), list) or
                len(value['candidates']) > 40 or not isinstance(value.get('retrieval'), dict)):
            raise AdapterContractError('Invalid cosine search response')
        self._check_coverage_response(value,filter_groups)
        return value

    async def scheduled_embed_query(self,session,text):
        if not isinstance(text,str) or not text.strip() or len(text.encode())>8192:
            raise AdapterContractError('Invalid embedding input')
        if not self.configured():raise VectorUnavailable('Query embedding key is not configured')
        try:
            if self.key_file.stat().st_size>4096:raise VectorUnavailable('Invalid embedding credential file')
            key=self.key_file.read_text(encoding='utf-8-sig').strip()
        except (OSError,UnicodeError):raise VectorUnavailable('Embedding credential file is unavailable') from None
        if not key or any(c.isspace() for c in key):raise VectorUnavailable('Invalid embedding credential file')
        self.calls['embed_query']+=1
        identity=hashlib.sha256((MODEL+'\0'+key+'\0'+text).encode()).hexdigest()
        value=await session.scheduled_tool(self.embedding_stage,identity,
            lambda deadline:self._cached_embedding(identity,lambda:self._json('POST','https://api.openai.com/v1/embeddings',
                headers={'Authorization':'Bearer '+key},deadline=deadline,
                payload={'model':MODEL,'dimensions':DIMENSIONS,'encoding_format':'float','input':text})),operation_seconds=7)
        if (not isinstance(value,dict) or value.get('model')!=MODEL or
                not isinstance(value.get('data'),list) or len(value['data'])!=1 or
                not isinstance(value['data'][0],dict) or type(value['data'][0].get('index')) is not int or
                value['data'][0]['index']!=0):raise AdapterContractError('Embedding provider contract changed')
        return {'model':MODEL,'vector':vector_value(value['data'][0].get('embedding')),'input_contract':INPUT_CONTRACT}

    async def scheduled_cosine_search(self,session,embedding,source_ids=(),*,include_related=True,filter_groups=None,lexical_query=None,collapse_time_series=False):
        from .coverage import validate_groups
        if type(collapse_time_series) is not bool:raise ValueError("collapse_time_series must be boolean")
        if embedding.get('model')!=MODEL:raise AdapterContractError('Embedding model mismatch')
        self.calls['cosine_search']+=1
        payload={'model':MODEL,'vector':vector_value(embedding.get('vector')),'limit':40,'explore':True,'include_related':include_related,
                 **({'source_ids':list(source_ids)} if source_ids else {}),
                 **({'collapse_time_series':True} if collapse_time_series else {}),
                 **({'filter_groups':validate_groups(filter_groups)} if filter_groups else {}),
                 **({'lexical_query':lexical_query} if lexical_query else {})}
        identity=hashlib.sha256(json.dumps([payload,self._fresh_search_generation(),self._search_cache_epoch],
            sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

        async def operation(deadline):
            # The lookup still occupies the bounded search stage and charges
            # this session's tool call. Only identical public search metadata
            # may be reused; model interpretation/judgment are separate stages.
            generation = self._fresh_search_generation()
            epoch = self._search_cache_epoch
            key = (hashlib.sha256(json.dumps([payload,generation],sort_keys=True,
                separators=(',',':'),allow_nan=False).encode()).hexdigest() if generation else None)
            if key:
                cached = self.search_cache.get('metadata',key)
                if cached is not None:
                    value = cached['value']
                    value['retrieval']['search_cache'] = {'hit':True,'ttl_seconds':10,
                        'age_seconds':round(max(0.,self._cache_clock()-cached['stored_at']),3)}
                    return value
            else:
                self.search_cache_events['skipped_health'] += 1
            self.search_cache_events['http_searches'] += 1
            try:
                value = await self._json('POST',self.vector_url+'/search',
                    headers={'X-Dataieum-Gateway-Token':self.gateway_token},deadline=deadline,
                    payload={**payload,'timeout_ms':max(1,min(7000,int((deadline-time.monotonic())*1000)))})
                # Reject non-finite or malformed JSON metadata before retaining
                # it; parsing JSON alone does not reject NaN or lone surrogates.
                bounded_json(value,524288)
                self._validate_scheduled_search(value)
                self._check_coverage_response(value,filter_groups)
            except (HarnessError,TimeoutError,asyncio.CancelledError):
                self._invalidate_search_cache('search_error')
                raise
            response_generation = tuple(value['retrieval'].get(field) for field in
                ('model','dimensions','indexed_vectors','index_built_at','coverage_generation'))
            if generation and response_generation != (*generation[:4],generation[5]):
                self._invalidate_search_cache('response_generation')
            # A health failure/rotation during the HTTP request must not be
            # undone by its late completion, even if a later generation repeats.
            eligible = bool(key and epoch == self._search_cache_epoch and
                generation == self._fresh_search_generation())
            annotation = {'hit':False,'ttl_seconds':10,'age_seconds':0.}
            value['retrieval']['search_cache'] = annotation
            try:
                bounded_json(value,524288)
            except BudgetExceeded:
                # Diagnostics/cache overhead must not make an otherwise valid
                # response fail the pre-existing dependency result size limit.
                value['retrieval'].pop('search_cache')
                eligible = False
                self.search_cache_events['skipped_size'] += 1
            if eligible:
                try:
                    self.search_cache.put('metadata',key,{'value':value,'stored_at':self._cache_clock()})
                except BudgetExceeded:
                    self.search_cache_events['skipped_size'] += 1
            return value

        value=await session.scheduled_tool(self.search_stage,identity,operation,operation_seconds=7)
        self._validate_scheduled_search(value)
        return value

    @staticmethod
    def _check_coverage_response(value,groups):
        if not groups:return
        from .coverage import VERSION, validate_groups
        retrieval=value.get('retrieval',{})
        if (retrieval.get('coverage_policy')!=VERSION or not retrieval.get('coverage_generation') or
                retrieval.get('coverage_filter',{}).get('groups')!=validate_groups(groups)):
            raise AdapterContractError('Coverage prefilter was not applied')

    @staticmethod
    def _validate_scheduled_search(value):
        if (not isinstance(value,dict) or not isinstance(value.get('candidates'),list) or
                len(value['candidates'])>40 or not isinstance(value.get('retrieval'),dict)):
            raise AdapterContractError('Invalid cosine search response')
        for candidate in value['candidates']:
            if not isinstance(candidate,dict):raise AdapterContractError('Invalid cosine candidate')
            identifier,source,score = (candidate.get(field) for field in ('dataset_id','source_id','cosine'))
            if (not isinstance(identifier,str) or not identifier or len(identifier.encode('utf-8'))>1536 or
                    not isinstance(source,str) or not source or len(source.encode('utf-8'))>80 or
                    type(score) not in (int,float) or not math.isfinite(score) or not -1.000001<=score<=1.000001 or
                    not isinstance(candidate.get('metadata'),dict)):
                raise AdapterContractError('Invalid cosine candidate')

    def _invalidate_search_cache(self,reason):
        self.search_cache.clear()
        self._search_generation = self._search_health_at = None
        self._search_cache_epoch += 1
        self.search_cache_events['invalidated_'+reason] += 1

    def _fresh_search_generation(self):
        if self._search_generation is None or self._search_health_at is None:return None
        return self._search_generation if 0<=self._cache_clock()-self._search_health_at<=3 else None

    def _observe_vector_health(self,result,sequence):
        if sequence<self._applied_health_sequence:return
        self._applied_health_sequence=sequence
        if (not isinstance(result,dict) or result.get('ready') is not True or
                result.get('model')!=MODEL or type(result.get('dimensions')) is not int or
                result['dimensions']!=DIMENSIONS or type(result.get('indexed_vectors')) is not int or
                result['indexed_vectors']<0 or type(result.get('source_count')) is not int or
                result['source_count']<0 or not isinstance(result.get('built_at'),str) or
                not result['built_at'] or len(result['built_at'])>100):
            self._invalidate_search_cache('health')
            return
        generation=tuple(result.get(field) for field in ('model','dimensions','indexed_vectors','built_at','source_count','coverage_generation'))
        if generation!=self._search_generation:
            self._invalidate_search_cache('generation')
            self._search_generation=generation
        self._search_health_at=self._cache_clock()

    async def health(self):
        self._health_sequence+=1
        sequence=self._health_sequence
        if not self.configured():
            self._observe_vector_health(None,sequence)
            return {'ready': False, 'state': 'embedding_not_configured'}
        try:
            result = await self._json('GET', self.vector_url+'/health/ready', maximum=16384)
            self._observe_vector_health(result,sequence)
            ready=isinstance(result,dict) and result.get('ready') is True
            return {'ready': ready, 'state': 'ready' if ready else 'vector_preparing'}
        except (HarnessError, TimeoutError):
            self._observe_vector_health(None,sequence)
            return {'ready': False, 'state': 'vector_unavailable'}
        except asyncio.CancelledError:
            self._observe_vector_health(None,sequence)
            raise
