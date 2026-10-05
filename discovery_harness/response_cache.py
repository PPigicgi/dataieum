"""Byte-bounded results and bounded shared work; individual cancellation is isolated."""
import asyncio
from collections import OrderedDict
import random
import time

from .capacity import ServerOverloaded


class ResponseCache:
    def __init__(self, max_bytes=24*1024**2, max_entries=256, max_flights=2, max_waiters=32):
        self.max_bytes,self.max_entries,self.max_flights,self.max_waiters=max_bytes,max_entries,max_flights,max_waiters
        self.entries=OrderedDict();self.flights={};self.waiters={};self.closing=set();self.bytes=0
        self.hits=self.misses=self.shared=self.evictions=0

    def _drop(self,key):
        item=self.entries.pop(key,None)
        if item:self.bytes-=len(item[1]);self.evictions+=1

    async def get(self,key,compute,ttl=120):
        item=self.entries.get(key)
        if item and item[0]>time.monotonic():
            self.entries.move_to_end(key);self.hits+=1;return item[1]
        if item:self._drop(key)
        if key in self.closing:raise ServerOverloaded('cache_compute_draining')
        if sum(self.waiters.values())>=self.max_waiters:raise ServerOverloaded('cache_waiter_slots')
        task=self.flights.get(key)
        if task is None:
            if len(self.flights)>=self.max_flights:raise ServerOverloaded('cache_compute_slots')
            self.misses+=1
            async def work():
                body=await compute()
                if not isinstance(body,bytes):raise TypeError('cache requires immutable encoded bytes')
                if len(body)<=min(self.max_bytes,512*1024):
                    self._drop(key)
                    while self.entries and (self.bytes+len(body)>self.max_bytes or len(self.entries)>=self.max_entries):self._drop(next(iter(self.entries)))
                    self.entries[key]=(time.monotonic()+ttl*random.uniform(.9,1.1),body);self.bytes+=len(body)
                return body
            task=asyncio.create_task(work());self.flights[key]=task
        else:self.shared+=1
        self.waiters[key]=self.waiters.get(key,0)+1
        try:return await asyncio.shield(task)
        finally:
            self.waiters[key]=self.waiters.get(key,1)-1
            if self.waiters[key]<=0:
                self.waiters.pop(key,None)
                if not task.done():
                    self.closing.add(key)
                    task.cancel()
                    # Keep its slot and harness lease until cooperative cleanup
                    # finishes; never release capacity while DB work continues.
                    while not task.done():
                        try:await asyncio.shield(task)
                        except asyncio.CancelledError:continue
                        except Exception:break
                    if not task.cancelled():task.exception()
                self.closing.discard(key)
                self.flights.pop(key,None)

    async def close(self):
        tasks=list(self.flights.values())
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        self.entries.clear();self.flights.clear();self.waiters.clear();self.bytes=0

    def status(self):
        return {'bytes':self.bytes,'limit_bytes':self.max_bytes,'entries':len(self.entries),'inflight':len(self.flights),
                'waiters':sum(self.waiters.values()),'hits':self.hits,'misses':self.misses,'shared':self.shared,'evictions':self.evictions}
