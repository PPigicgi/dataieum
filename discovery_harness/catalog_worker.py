"""One persistent, killable process for the existing synchronous catalog.

The large memory index lives in this process only. Confirmed cooperative query
stops preserve it; forced termination also discards its private IPC pipe.
The integrating application must include this process in its container limit.
"""
from __future__ import annotations

import asyncio
from collections import Counter, deque
import importlib
import json
import logging
import multiprocessing
import os
from pathlib import Path
import sqlite3
import sys
import time

from .errors import BudgetExceeded, CapacityExceeded, CleanupFailed, DeadlineExceeded, HarnessError
from .capacity import ServerOverloaded
from .catalog_pages import StaleCursor
from .resources import bounded_json


class CatalogUnavailable(HarnessError):
    code = 'catalog_unavailable'


class _ReadOnlySQLite:
    """Apply read-only mode to both catalog queries and its version observer."""
    def __init__(self, database, expired=lambda: False, *, immutable=False):
        self.database = database
        self.expired = expired
        self.immutable = immutable

    def __getattr__(self, name):
        return getattr(sqlite3, name)

    def connect(self, database, *, timeout=0.25, check_same_thread=True):
        if Path(database).resolve() != self.database:
            raise ValueError('catalog database path mismatch')
        if self.immutable:
            wal=self.database.with_name(self.database.name+'-wal')
            if wal.exists() and wal.stat().st_size:raise ValueError('snapshot has a live WAL')
        connection = sqlite3.connect(self.database.as_uri() + '?mode=ro' + ('&immutable=1' if self.immutable else ''), uri=True,
                                     timeout=min(timeout, 0.25), check_same_thread=check_same_thread)
        connection.execute('PRAGMA query_only=ON')
        connection.set_progress_handler(lambda: int(self.expired()), 1000)
        return connection


def _serve(connection, source, database, module_name, result_limit, index_directory, index_max_bytes, abort):
    """Trusted local catalog code; deliberately never initialize or collect data."""
    observer = prepared = None
    deadline = [None]
    def expired():
        return abort.is_set() or (deadline[0] is not None and time.monotonic() >= deadline[0])
    try:
        path = Path(database).resolve(strict=True)
        os.environ['DB_PATH'] = str(path)
        sys.path.insert(0, source)
        catalog = importlib.import_module(module_name)
        catalog.sqlite3 = _ReadOnlySQLite(path, expired, immutable=bool(os.environ.get('DATAIEUM_CONFIDENCE_GRAPH')))
        observer = catalog.sqlite3.connect(path)
        version = observer.execute('PRAGMA data_version').fetchone()[0]
        native = None
        if os.environ.get('DATAIEUM_CONFIDENCE_GRAPH'):
            from .native_catalog import NativeCatalog
            native = NativeCatalog(importlib.import_module('confidence_graph'), path,
                os.environ['DATAIEUM_CONFIDENCE_GRAPH'], os.environ['DATAIEUM_OVERVIEW_FILE'], expired,
                search_index=os.environ.get('DATAIEUM_KEYWORD_INDEX'),
                topic_view=os.environ.get('DATAIEUM_TOPIC_VIEW')=='1')
        if index_directory is not None and native is None:
            from .catalog_store import PreparedCatalog
            prepared = PreparedCatalog(catalog, path, index_directory, observer, expired,
                                       max_bytes=index_max_bytes)
            catalog.catalog_index = prepared.index
            catalog.matching_records = prepared.matching_records
        while True:
            request = json.loads(connection.recv_bytes(32768))
            operation, query = request['operation'], request['query']
            deadline[0] = request['deadline']
            started = time.monotonic()
            if prepared is not None:
                prepared.query_stats = {}
            details = {}
            try:
                if operation == 'prepare':
                    if prepared is not None:
                        prepared.preparing = True
                    prepared_version = observer.execute('PRAGMA data_version').fetchone()[0]
                    # Use the existing atomic summary writer and collection lock.
                    # An interrupted writer may leave its known .tmp file; never
                    # clean persistent catalog files as request workspaces.
                    refreshed = native.prepare() if native else catalog.refresh_overview_unless_updating()
                    if refreshed is None:
                        raise CatalogUnavailable('collection update is in progress')
                    version = prepared_version
                    value = {'ready': True}
                elif operation == 'changed':
                    value = observer.execute('PRAGMA data_version').fetchone()[0] != version or bool(native and native.changed())
                elif operation in {'catalog_page','catalog_count','dataset_detail'}:
                    from . import catalog_pages
                    if native is None:raise CatalogUnavailable('native index required')
                    fn={'catalog_page':catalog_pages.page,'catalog_count':catalog_pages.count,'dataset_detail':catalog_pages.detail}[operation]
                    value=fn(native,query)
                elif operation == 'catalog':
                    value = native.snapshot(query) if native else catalog.snapshot(query)
                elif operation == 'compare':
                    from .dataset_compare import snapshot
                    if native and native.changed():raise CatalogUnavailable('catalogue changed')
                    with catalog.database() as db:
                        value=snapshot(db,query,postgres=native.postgres if native else None)
                    if native and native.changed():raise CatalogUnavailable('catalogue changed')
                elif operation == 'cooperation_record':
                    identifier = query.get('id', '')
                    if set(query) != {'id'} or not isinstance(identifier, str) or not identifier or len(identifier) > 2048:
                        raise ValueError('invalid cooperation record id')
                    with catalog.database() as db:
                        rows = catalog.read_records(db, [{'id': identifier}])
                        sources = (native.postgres.sources() if getattr(native, 'postgres', None) else
                                   [json.loads(row[0]) for row in db.execute('SELECT info FROM sources')])
                    record = next((row for row in rows if row['id'] == identifier), None)
                    source = next((row for row in sources if record and row['id'] == record['source_id']), None)
                    value = ({'id': record['id'], 'source_id': record['source_id'], 'title': record.get('title'),
                              'url': record.get('url') or record.get('metadata_url'),
                              'source_name': source['name'], 'source_url': source.get('url'),
                              'format': record.get('format')} if record and source else None)
                elif operation == 'ontology_metadata':
                    identifier=query.get('id','')
                    if set(query)!={'id'} or not isinstance(identifier,str) or not identifier or len(identifier)>2048:
                        raise ValueError('invalid metadata record id')
                    with catalog.database() as db:
                        rows=catalog.read_records(db,[{'id':identifier}])
                    from .metadata_input import prepare_metadata
                    value={'id':identifier,'found':bool(rows),'metadata':prepare_metadata(rows[0]) if rows else None,
                           'retrieval':{'mode':'existing_catalog','vector_connected':False}}
                elif operation in {'ontology_catalog','ontology_record'}:
                    from .asset_ontology import record_projection,relationship_context_ids,asset_models,reviewed_records
                    if operation=='ontology_record':
                        identifier=query.get('id','')
                        if set(query)!={'id'} or not isinstance(identifier,str) or not identifier or len(identifier)>2048:raise ValueError('invalid record id')
                        with catalog.database() as db:
                            rows=catalog.read_records(db,[{'id':identifier}])
                            sources=[json.loads(row[0]) for row in db.execute('SELECT info FROM sources')]
                        snapshot={'datasets':rows,'sources':sources,'total':len(rows),'page':1,'pages':1}
                    else:
                        if set(query)-{'concept','source','year','page','q','status','indicator'}:raise ValueError('invalid ontology catalogue query')
                        if query.get('concept') and query['concept'] not in {c['id'] for c in catalog.CONCEPTS}:raise ValueError('unknown ontology domain')
                        if query.get('indicator'):
                            if set(query)-{'indicator','page','q'}:raise ValueError('mixed ontology selection')
                            indicator=query['indicator']
                            identifiers=list(dict.fromkeys(a['dataset_id'] for a in asset_models()['asset_assertions'] if indicator in a['indicators']))
                            with catalog.database() as db:
                                rows=catalog.read_records(db,[{'id':i} for i in identifiers]) if identifiers else []
                                sources=[json.loads(row[0]) for row in db.execute('SELECT info FROM sources')]
                            rows=reviewed_records(rows,indicator)
                            if query.get('q'):
                                term=query['q'].casefold();rows=[r for r in rows if term in (str(r.get('title',''))+' '+str(r.get('description',''))).casefold()]
                            pages=max(1,(len(rows)+29)//30);page=min(pages,max(1,int(query.get('page','1'))))
                            snapshot={'datasets':rows[(page-1)*30:page*30],'sources':sources,'page':page,'pages':pages,'total':len(rows)}
                        else:snapshot=native.snapshot(query) if native else catalog.snapshot(query)
                    related_ids=relationship_context_ids(r['id'] for r in snapshot['datasets'])
                    if related_ids:
                        with catalog.database() as db:snapshot['related_records']=catalog.read_records(db,[{'id':i} for i in related_ids])
                    value=record_projection(snapshot)
                elif operation == 'discovery':
                    if native:
                        raise ValueError('legacy full-catalog discovery is unavailable for the confidence catalogue; use vector chat')
                    from .catalog_discovery import discovery_snapshot
                    def check():
                        if expired():
                            raise DeadlineExceeded('discovery deadline exceeded')
                    value = discovery_snapshot(catalog, query, check)
                elif operation == 'graph':
                    value = native.graph(query) if native else catalog.graph_overview(query)
                elif operation == 'acquisition':
                    # Imported after the read-only catalog database has been bound.
                    audit = importlib.import_module('collection_audit')
                    value = prepared.acquisition(audit) if prepared is not None else audit.acquisition_status()
                else:
                    raise ValueError('unsupported catalog operation')
                encoding = time.monotonic()
                body = bounded_json(value, result_limit)
                details = {'encoding_seconds': round(time.monotonic()-encoding, 4), 'result_bytes': len(body)}
                packet = b'{"kind":"ok","value":'+body+b'}'
            except BudgetExceeded as error:
                packet = bounded_json({'kind': 'budget', 'resource': error.resource,
                                       'limit': error.limit}, 1024)
            except DeadlineExceeded:
                packet = b'{"kind":"deadline"}'
            except sqlite3.OperationalError:
                packet = b'{"kind":"deadline"}' if expired() else b'{"kind":"unavailable"}'
            except StaleCursor:
                packet = b'{"kind":"stale_cursor"}'
            except ValueError:
                packet = b'{"kind":"unavailable"}' if operation == 'prepare' else b'{"kind":"invalid_query"}'
            except Exception:
                logging.getLogger(__name__).exception('catalog %s failed', operation)
                packet = b'{"kind":"unavailable"}'
            finally:
                details.update(operation=operation, seconds=round(time.monotonic()-started, 4),
                               matching=dict(prepared.query_stats) if prepared is not None else {})
                packet = packet[:-1]+b',"diagnostics":'+bounded_json(details, 4096)+b'}'
                if prepared is not None:
                    prepared.preparing = False
                deadline[0] = None
            connection.send_bytes(packet)
    except (EOFError, BrokenPipeError, OSError):
        pass
    finally:
        if prepared is not None:
            prepared.close()
        if observer is not None:
            observer.close()
        connection.close()


class CatalogWorker:
    """No work queue: only one operation may own the process at a time.

    Cancellation drains the single IPC reader, or terminates unconfirmed work,
    before returning. A new process is not created while old work might still run.
    """
    def __init__(self, source, database, *, module_name='catalog', result_limit=1048576,
                 index_directory=None, index_max_bytes=12 * 1024**3):
        self.source = str(Path(source).resolve(strict=True))
        self.database = str(Path(database).resolve(strict=True))
        self.module_name, self.result_limit = module_name, result_limit
        self.index_directory = str(Path(index_directory).resolve()) if index_directory is not None else None
        self.index_max_bytes = index_max_bytes
        self._process = self._connection = None
        self._abort = None
        self._prepared = False
        self.busy = False
        self.closed = False
        self.failed = False
        self._recent = deque(maxlen=8)
        self._outcomes = Counter()

    def diagnostics(self):
        return {'outcomes':dict(self._outcomes), 'recent':list(self._recent)}

    @property
    def pid(self):
        return self._process.pid if self._process else None

    @property
    def alive(self):
        return self._process is not None and self._process.is_alive()

    def _start(self):
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        abort = context.Event()
        process = context.Process(target=_serve, args=(child, self.source, self.database,
                                  self.module_name, self.result_limit, self.index_directory,
                                  self.index_max_bytes, abort), daemon=True)
        try:
            process.start()
        except BaseException:
            parent.close()
            child.close()
            raise
        child.close()
        self._connection, self._process = parent, process
        self._abort = abort

    def _stop_sync(self):
        self._prepared = False
        process = self._process
        if process is not None:
            if process.is_alive():
                process.terminate()
            process.join(1)
            if process.is_alive():
                process.kill()
                process.join(2)
            if process.is_alive():
                self.failed = True
                raise CleanupFailed('catalog process termination is unconfirmed')
            process.close()
            self._process = None
            self._abort = None

    async def _discard(self, reader=None):
        # Cancellation can arrive repeatedly while we are stopping native work.
        async def cleanup():
            await asyncio.to_thread(self._stop_sync)
            if reader is not None:
                try:
                    await reader
                except Exception:
                    pass
            if self._connection is not None:
                self._connection.close()
                self._connection = None
        task = asyncio.create_task(cleanup())
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        return cancelled

    async def _cancel_read(self, reader, operation):
        """Keep a prepared process only after a bounded, complete read reply."""
        self._abort.set()
        async def stopped():
            try:
                await asyncio.wait_for(asyncio.shield(reader), .25)
                response = json.loads(reader.result())
                if not isinstance(response, dict) or not self.alive:
                    return False
                details = response.get('diagnostics')
                if not isinstance(details, dict) or details.get('operation') != operation:
                    return False
                kind = response.get('kind')
                return (kind in {'deadline', 'invalid_query'}
                        or kind == 'ok' and 'value' in response
                        or kind == 'budget' and isinstance(response.get('resource'), str)
                        and type(response.get('limit')) is int)
            except (asyncio.CancelledError, TimeoutError, ValueError, TypeError, RecursionError, EOFError, OSError):
                return False
        # Repeated cancellation must neither cancel recv_bytes nor extend the
        # acknowledgement deadline. Busy/DB ownership lasts through this drain.
        task = asyncio.create_task(stopped())
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                pass
        return task.result()

    async def call(self, operation, query=None, *, timeout=8.0, check=None):
        if self.closed or self.failed:
            raise CatalogUnavailable('catalog worker is closed')
        if self.busy:
            raise CapacityExceeded('catalog worker is occupied')
        # Reject an invalid local packet before disturbing a prepared worker.
        packet = bounded_json({'operation': operation, 'query': query or {},
                               'deadline': time.monotonic() + timeout - min(.25, timeout * .2)}, 32768 if operation=='compare' else 4096)
        self.busy = True
        reader = None
        reply_received = False
        try:
            if not self.alive:
                if await self._discard():
                    raise asyncio.CancelledError
                self._start()
            async with asyncio.timeout(timeout):
                if operation == 'prepare':
                    self._prepared = False
                self._abort.clear()
                self._connection.send_bytes(packet)
                reader = asyncio.create_task(asyncio.to_thread(
                    self._connection.recv_bytes, self.result_limit + 8192))
                while not reader.done():
                    if check is not None:
                        try:
                            check()
                        except ServerOverloaded as pressure:
                            if operation == 'prepare' or pressure.reason not in {
                                    'loop_lag', 'telemetry_stale', 'telemetry_unavailable'}:
                                raise
                            self._abort.set()
                            # Native SQL progress handlers and Python catalog
                            # loops share this stop flag. Keep the index only
                            # after a complete IPC reply confirms quiescence.
                            try:
                                await asyncio.wait_for(asyncio.shield(reader), 1.0)
                                stopped = json.loads(reader.result())
                                reply_received = stopped.get('kind') in {
                                    'ok', 'deadline', 'budget', 'invalid_query', 'stale_cursor', 'unavailable'}
                            except (TimeoutError, ValueError, EOFError, OSError):
                                reply_received = False
                            logging.getLogger(__name__).warning(
                                'catalog pressure stop: %s, confirmed=%s', pressure.reason, reply_received)
                            raise pressure
                    await asyncio.wait({reader}, timeout=0.05)
                response = json.loads(reader.result())
                reply_received = True
                name = operation if operation in {'catalog','discovery','graph','acquisition','changed','prepare'} else 'other'
                self._outcomes[name+'/'+response['kind']] += 1
                if operation not in {'changed','prepare'}:
                    self._recent.append({**response.get('diagnostics', {}), 'outcome':response['kind']})
            if response['kind'] == 'budget':
                raise BudgetExceeded(response['resource'], response['limit'])
            if response['kind'] == 'stale_cursor':
                raise StaleCursor('publication changed')
            if response['kind'] == 'invalid_query':
                raise ValueError('검색 조건이 올바르지 않습니다.')
            if response['kind'] == 'deadline':
                raise DeadlineExceeded('catalog query interrupted at its deadline')
            if response['kind'] != 'ok':
                raise CatalogUnavailable('catalog operation failed')
            if operation == 'prepare':
                self._prepared = response['value'] == {'ready': True}
            return response['value']
        except BaseException as error:
            if (isinstance(error, asyncio.CancelledError) and reader is not None
                    and self._prepared and operation in {
                        'catalog', 'discovery', 'graph', 'acquisition', 'changed',
                        'ontology_metadata', 'ontology_catalog', 'ontology_record', 'compare', 'catalog_page', 'catalog_count', 'dataset_detail', 'cooperation_record'}):
                confirmed = await self._cancel_read(reader, operation)
                logging.getLogger(__name__).warning(
                    'catalog cancellation stop: operation=%s error=CancelledError confirmed=%s',
                    operation, confirmed)
                if confirmed:
                    raise
            if reply_received and isinstance(error, (ValueError, BudgetExceeded, DeadlineExceeded, ServerOverloaded)):
                raise
            logging.getLogger(__name__).warning('catalog worker discarded: operation=%s error=%s reason=%s',
                operation, type(error).__name__, getattr(error, 'reason', None))
            cancelled = await self._discard(reader)
            if cancelled and not isinstance(error, asyncio.CancelledError):
                raise asyncio.CancelledError from error
            if isinstance(error, TimeoutError):
                raise DeadlineExceeded('catalog operation deadline exceeded') from error
            if isinstance(error, (EOFError, BrokenPipeError, OSError, KeyError, json.JSONDecodeError)):
                raise CatalogUnavailable('catalog worker connection failed') from error
            raise
        finally:
            self.busy = False

    async def aclose(self):
        self.closed = True
        if self.busy:
            raise CleanupFailed('cancel and drain the active catalog call before closing')
        if await self._discard():
            raise asyncio.CancelledError
