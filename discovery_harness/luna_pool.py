"""Fair bounded leases for independent Luna processes, inside request deadlines."""
import asyncio
from collections import Counter
from contextlib import asynccontextmanager

from .errors import CapacityExceeded, CleanupFailed
from .process_memory import working_set_bytes


class QueueWaitExceeded(CapacityExceeded):
    code = 'queue_wait_timeout'


class LunaPool:
    def __init__(self, providers, *, queue_size, wait_seconds=2.):
        if (not 1 <= len(providers) <= 100 or type(queue_size) is not int or
                not 0 <= queue_size <= 25 or not 0 < wait_seconds <= 12):
            raise ValueError('Luna pool supports 1..100 workers and at most 25 bounded stage waiters')
        self.providers = tuple(providers)
        self.concepts = self.providers[0].concepts
        self.queue_size, self.wait_seconds = queue_size, wait_seconds
        self.permits = asyncio.Semaphore(len(providers))
        self.slots = asyncio.Queue()
        for provider in providers:
            self.slots.put_nowait(provider)
        self.waiters = self.peak_waiters = self.rejected = self.expired = 0
        self.peak_rss = 0
        self.peak_generation_waiters = 0

    @property
    def healthy(self):
        return all(provider.healthy and getattr(getattr(provider,'transport',None),'healthy',True)
                   for provider in self.providers)

    @asynccontextmanager
    async def lease(self, remaining_seconds):
        if not self.healthy:
            raise CleanupFailed('Luna pool cleanup was not confirmed')
        if self.permits.locked():
            if self.waiters >= self.queue_size:
                self.rejected += 1
                raise CapacityExceeded('Luna queue is full')
            self.waiters += 1
            self.peak_waiters = max(self.peak_waiters, self.waiters)
            try:
                async with asyncio.timeout(min(self.wait_seconds, remaining_seconds)):
                    await self.permits.acquire()
            except TimeoutError as error:
                self.expired += 1
                raise QueueWaitExceeded('Luna queue wait expired') from error
            finally:
                self.waiters -= 1
        else:
            await self.permits.acquire()
        provider = self.slots.get_nowait()
        try:
            if not self.healthy:
                raise CleanupFailed('Luna pool cleanup was not confirmed')
            yield provider
        finally:
            # Adapter cancellation reaps its owned CLI before returning here.
            self.slots.put_nowait(provider)
            self.permits.release()

    def status(self):
        states = [provider.status() for provider in self.providers]
        pids = sorted({state['pid'] for state in states if state['pid'] is not None})
        rss = sum(working_set_bytes(pid) or 0 for pid in pids)
        self.peak_rss = max(self.peak_rss, rss)
        totals = {key: sum(state[key] for state in states) for key in
                  ('started', 'completed', 'failed', 'input_tokens', 'cached_input_tokens', 'output_tokens')}
        shared = {id(provider.transport): state for provider, state in zip(self.providers, states)
                  if getattr(provider, 'transport', None) is not None}
        transports = list(shared.values())
        generation_waiters = sum(s.get('generation_waiters', 0) for s in transports)
        self.peak_generation_waiters = max(self.peak_generation_waiters, generation_waiters,
            max((s.get('peak_generation_waiters', 0) for s in transports), default=0))
        failures = Counter()
        for state in transports:failures.update(state.get('failure_reasons', {}))
        all_idle = bool(transports) and all(s.get('generation_idle', False) for s in transports)
        successes = [s['last_model_success_at'] for s in states if s.get('last_model_success_at') is not None]
        return {**totals, 'rejected': self.rejected, 'queue_expired': self.expired,
                'last_model_success_at': max(successes, default=None),
                'active': sum(state['active'] for state in states),
                'max_active': len(states), 'queue_length': self.waiters,
                'queue_capacity': self.queue_size, 'peak_queued': self.peak_waiters,
                'queue_wait_seconds': self.wait_seconds,
                'healthy': self.healthy, 'model': self.providers[0].model,
                'transport': states[0].get('transport','exec'),
                'shared_process_count':len(transports),
                'generations':sum(s.get('generations',0) for s in transports),
                'generation_thread_limit':states[0].get('generation_thread_limit'),
                'generation_idle_seconds':states[0].get('generation_idle_seconds',0),
                'generation_idle':all_idle,
                'generation_idle_remaining_seconds':min(s.get('generation_idle_remaining_seconds',0) for s in transports) if all_idle else 0,
                'generation_age_seconds':max((s.get('generation_age_seconds',0) for s in transports),default=0),
                'generation_max_age_seconds':states[0].get('generation_max_age_seconds',0),
                'generation_issued':sum(s.get('generation_issued',0) for s in transports),
                'generation_waiters':generation_waiters,
                'peak_generation_waiters':self.peak_generation_waiters,
                'generation_waiter_peak_basis':'sampled_lower_bound' if len(transports)>1 else 'per_process_counter',
                'failure_reasons':dict(failures),
                'pid': None, 'pids': pids, 'cli_rss_sum_bytes': rss,
                'cli_peak_rss_bytes': self.peak_rss,
                'upstream_output_token_cap': False, 'upstream_storage_control': False}
