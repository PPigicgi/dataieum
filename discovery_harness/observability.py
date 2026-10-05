"""Bounded OpenTelemetry traces and phase histograms without queries or secrets."""
from collections import defaultdict
from contextlib import contextmanager
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import threading
import time

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor,SpanExporter,SpanExportResult

BUCKETS=(.01,.05,.1,.3,1.,3.,10.,30.)
_lock=threading.Lock();_stats=defaultdict(lambda:{'calls':0,'errors':0,'seconds':0.,'buckets':[0]*9})
_provider=None;_tracer=None


class Exporter(SpanExporter):
    def __init__(self,path):
        self.log=logging.getLogger('dataieum.spans');self.log.propagate=False;self.log.setLevel(logging.INFO)
        self.handler=RotatingFileHandler(path,maxBytes=2*1024**2,backupCount=1,encoding='utf-8')
        self.log.addHandler(self.handler)

    def export(self,spans):
        for span in spans:
            self.log.info(json.dumps({'trace':format(span.context.trace_id,'032x'),'span':span.name,
                'started_ns':span.start_time,'duration_ms':round((span.end_time-span.start_time)/1e6,3),
                'attributes':dict(span.attributes or {})},separators=(',',':')))
        return SpanExportResult.SUCCESS

    def shutdown(self):self.handler.close();self.log.removeHandler(self.handler)


def initialize(path):
    global _provider,_tracer
    if _provider is not None:return
    _provider=TracerProvider(resource=Resource.create({'service.name':'dataieum','service.version':os.environ.get('DATAIEUM_RELEASE','development')}))
    _provider.add_span_processor(BatchSpanProcessor(Exporter(path),max_queue_size=256,max_export_batch_size=32,schedule_delay_millis=5000))
    _tracer=_provider.get_tracer('dataieum.read', '1')


@contextmanager
def phase(name,operation):
    # All names are fixed call-site constants, never URL paths, IDs or search text.
    start=time.monotonic();failed=False
    span=_tracer.start_span(name,attributes={'operation':operation}) if _tracer else None
    try:yield span
    except BaseException as exc:
        failed=True
        if span:span.set_attribute('error.type',type(exc).__name__)
        raise
    finally:
        elapsed=time.monotonic()-start
        with _lock:
            item=_stats[name+':'+operation];item['calls']+=1;item['errors']+=failed;item['seconds']+=elapsed
            item['buckets'][next((i for i,b in enumerate(BUCKETS) if elapsed<=b),8)]+=1
        if span:span.end()


def status():
    with _lock:return {'bucket_upper_seconds':[*BUCKETS,None],'phases':{k:{**v,'seconds':round(v['seconds'],4),'buckets':list(v['buckets'])} for k,v in _stats.items()}}


def shutdown():
    global _provider,_tracer
    if _provider:_provider.shutdown()
    _provider=_tracer=None
