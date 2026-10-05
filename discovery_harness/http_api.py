"""FastAPI request contracts for read-only discovery; the harness owns execution."""
import asyncio
import hashlib
import json
from typing import Annotated
import unicodedata

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from .catalog_pages import StaleCursor
from .catalog_worker import CatalogUnavailable
from .capacity import ServerOverloaded
from .errors import BudgetExceeded, HarnessError
from .resources import bounded_json
from .response_cache import ResponseCache
from .observability import phase


class Filters(BaseModel):
    model_config=ConfigDict(extra='forbid')
    q:str=Field('',max_length=200,pattern=r'^[^\x00-\x1f\x7f]*$')
    source:str=Field('',max_length=200,pattern=r'^[^\x00-\x1f\x7f]*$')
    concept:str=Field('',pattern=r'^(?:M\d{2}(?:-S\d{2})?)?$')
    year:str=Field('',pattern=r'^(?:\d{4})?$')
    band:str=Field('',pattern=r'^(?:high|low|unclassified)?$')
    branch:str=Field('',max_length=64,pattern=r'^(?:\d+(?:\.\d+)*)?$')


class CursorFilters(Filters):
    cursor:str=Field('',max_length=512,pattern=r'^[A-Za-z0-9_-]*$')


class LegacyPage(Filters):
    page:int=Field(1,ge=1,le=99_999_999)


class BoundedResponse(Response):
    def __init__(self,backend,*args,**kwargs):
        self.backend=backend
        super().__init__(*args,**kwargs)

    async def __call__(self,scope,receive,send):
        if self.backend._cached_sends>=8:
            return await Response(b'{"code":"response_slots"}',status_code=503,media_type='application/json',headers={'Retry-After':'2'})(scope,receive,send)
        self.backend._cached_sends+=1
        try:
            async with asyncio.timeout(5):await super().__call__(scope,receive,send)
        except TimeoutError:pass
        finally:self.backend._cached_sends-=1


def create_read_api(backend):
    app=FastAPI(title='Dataieum',docs_url=None,redoc_url=None,openapi_url=None)
    cache=ResponseCache();backend.response_cache=cache

    def error(status,code):
        return Response(bounded_json({'code':code,'error':code},1024),status_code=status,media_type='application/json',headers={'Retry-After':'2'} if status==503 else {})

    @app.exception_handler(RequestValidationError)
    async def invalid(request,exc):return error(400,'invalid_query')

    async def execute(request,operation,query,ttl=120):
        if len(request.scope.get('query_string',b''))>32768:return error(400,'invalid_query')
        if any(len(request.query_params.getlist(k))!=1 for k in request.query_params):return error(400,'duplicate_query')
        query={k:str(v) for k,v in query.items() if v!=''}
        if query.get('q'):query['q']=unicodedata.normalize('NFC',query['q'].strip())
        generation=backend._cache_key('publication',{})
        access='admin' if operation=='acquisition' else 'public'
        key=hashlib.sha256(json.dumps([generation,'read-v2',access,request.scope.get('dataieum_brand'),operation,query],sort_keys=True).encode()).hexdigest()
        async def compute():
            async def work(session):
                if operation=='bootstrap':
                    if not backend.ready or not backend.worker.alive:raise CatalogUnavailable('index preparation pending')
                    from .dataieum import _file_chunks
                    value=json.loads(await session.tool_bytes(lambda:_file_chunks(backend.overview_file)))
                else:
                    if not backend.ready or not backend.worker.alive:raise CatalogUnavailable('index preparation pending')
                    with phase('database',operation):value=await backend._database_query(session,operation,query)
                if operation=='graph':backend._graph_budget(value)
                elif operation=='bootstrap':backend._graph_budget(value['graph'])
                elif operation in {'ontology_catalog','ontology_record'} and isinstance(value.get('graph'),dict):backend._graph_budget(value['graph'])
                from .display_text import clean_response
                value=clean_response(value)
                if operation=='dataset_detail' and value.get('dataset') is None:raise LookupError('not_found')
                with phase('serialize',operation):body=bounded_json(value,backend.harness.policy.server.max_response_bytes,'response_bytes')
                if generation!=backend._cache_key('publication',{}):raise StaleCursor('publication changed')
                return body
            return await backend.harness.run(work)
        try:
            from .dataieum import _until_disconnect
            if not backend.harness.healthy or not backend.ready or not backend.worker.alive:
                raise CatalogUnavailable('catalogue not ready')
            # Keep free-text searches and administrator audit responses out of
            # the shared result cache, as in the original API contract.
            work = compute() if query.get('q') or operation=='acquisition' else cache.get(key,compute,ttl)
            with phase('http',operation):body=await _until_disconnect(work,request.receive)
            backend.harness.capacity.check_response()
            return BoundedResponse(backend,body,media_type='application/json',headers={'X-Publication-Version':generation[:24]})
        except StaleCursor:return error(409,'publication_changed')
        except LookupError:return error(404,'not_found')
        except (ValueError,UnicodeError):return error(400,'invalid_query')
        except BudgetExceeded:return error(422,'request_limit')
        except ServerOverloaded as exc:
            backend.harness.capacity.record_rejection(exc.reason)
            return error(503,'catalog_unavailable')
        except CatalogUnavailable:return error(503,'catalog_unavailable')
        except HarnessError as exc:return error(504 if exc.code=='deadline_exceeded' else 503,exc.code)
        except asyncio.CancelledError:raise
        finally:
            if not backend.worker.alive:backend.ready=False

    @app.get('/api/bootstrap')
    async def bootstrap(request:Request):
        if request.query_params:return error(400,'invalid_query')
        return await execute(request,'bootstrap',{},600)

    @app.get('/api/catalog/page')
    async def page(request:Request,query:Annotated[CursorFilters,Query()]):
        return await execute(request,'catalog_page',query.model_dump())

    @app.get('/api/catalog/count')
    async def count(request:Request,query:Annotated[Filters,Query()]):
        return await execute(request,'catalog_count',query.model_dump(),300)

    @app.get('/api/catalog')
    async def catalog(request:Request,query:Annotated[LegacyPage,Query()]):
        return await execute(request,'catalog',query.model_dump())

    @app.get('/api/graph')
    async def graph(request:Request,query:Annotated[Filters,Query()]):
        return await execute(request,'graph',query.model_dump(),300)

    @app.get('/api/datasets/{dataset_id:path}')
    async def detail(request:Request,dataset_id:str):
        if request.query_params or not 0<len(dataset_id)<=2048 or any(ord(c)<32 for c in dataset_id):return error(400,'invalid_id')
        return await execute(request,'dataset_detail',{'id':dataset_id},600)

    @app.get('/api/compare')
    async def compare(request:Request,ids:Annotated[str,Query(max_length=24576)]):
        from .dataset_compare import validate_query
        if set(request.query_params)!={'ids'}:return error(400,'invalid_query')
        try:validate_query({'ids':ids})
        except ValueError:return error(400,'invalid_query')
        return await execute(request,'compare',{'ids':ids},600)

    @app.get('/api/acquisition')
    async def acquisition(request:Request):
        if request.query_params:return error(400,'invalid_query')
        # This route remains behind the existing Caddy administrator gate.
        return await execute(request,'acquisition',{},5)

    @app.get('/api/ontology/catalog')
    @app.get('/api/ontology/record')
    @app.get('/api/ontology/metadata')
    async def ontology(request:Request):
        operation=request.url.path.removeprefix('/api/').replace('/','_')
        if set(request.query_params)-{'source','concept','year','page','q','status','indicator','id','ids'}:return error(400,'invalid_query')
        return await execute(request,operation,dict(request.query_params))

    @app.get('/api/ontology/schema')
    async def schema(request:Request):
        if request.query_params:return error(400,'invalid_query')
        from .asset_ontology import scheme_snapshot
        response=await execute(request,'bootstrap',{},600)
        if response.status_code!=200:return response
        return Response(bounded_json(scheme_snapshot(json.loads(response.body)),backend.harness.policy.server.max_response_bytes),media_type='application/json')

    return app
