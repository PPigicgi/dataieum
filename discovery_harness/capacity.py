"""Single-worker task admission. Reservations estimate demand; ECS enforces RAM/CPU."""
import asyncio
import math
import os
import time
from collections import Counter
from dataclasses import dataclass, fields
from pathlib import Path

from .errors import HarnessError


class ServerOverloaded(HarnessError):
    code = 'server_overloaded'
    retry_after = 1

    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class CapacitySnapshot:
    sampled_at: float
    cpu_fraction: float
    memory_used_mib: float
    memory_limit_mib: float
    disk_used_mib: float
    disk_limit_mib: float
    cpu_limit_vcpu: float


class _Lease:
    def __init__(self, release):
        self._release = release

    def release(self):
        if self._release is not None:
            release, self._release = self._release, None
            release()


class _TaskLock:
    """Nonblocking OS lock; all workers must share the same canonical volume/path."""
    def __init__(self, path):
        self.path, self.file = Path(path), None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open('a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                handle.seek(0, 2)
                if handle.tell() == 0:
                    handle.write(b'0')
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            handle.close()
            raise
        self.file = handle

    def close(self):
        if self.file:
            self.file.close()
            self.file = None


class CapacityController:
    def __init__(self, policy, telemetry):
        self.policy, self.telemetry = policy, telemetry
        self._lock = _TaskLock(policy.server.lock_path)
        self._sample_lock = asyncio.Lock()
        self._monitor = None
        self._started = False
        self._transition = None
        self._snapshot = None
        self._blocked = 'not_started'
        self._healthy_samples = 0
        self._recovering_pressures = set()
        self._last_recovery_sample = -math.inf
        self._loop_lag = 0.0
        self._fatal = None
        self._requests = 0
        self._operations = {'llm': 0, 'tool': 0, 'db': 0}
        self._rejections = Counter()

    def record_rejection(self, reason):
        # HTTP decisions only, not health-check invocations of check().
        if reason not in self._rejections and len(self._rejections) >= 24:
            reason = 'other'
        self._rejections[reason] += 1

    async def start(self):
        if self._transition:
            raise ServerOverloaded('lifecycle_transition')
        if self._started:
            return
        self._transition = 'starting'
        try:
            try:
                self._lock.acquire()
            except OSError as exc:
                raise ServerOverloaded('task_owner_unavailable') from exc
            self._started = True
            await self.refresh()
            self._monitor = asyncio.create_task(self._watch(), name='harness-capacity')
        except BaseException:
            self._started = False
            self._lock.close()
            raise
        finally:
            self._transition = None

    async def close(self):
        if self._transition:
            # Refuse overlapping lifecycle calls without releasing their lock.
            raise ServerOverloaded('lifecycle_transition')
        if self._requests or any(self._operations.values()):
            raise ServerOverloaded('work_still_active')
        self._transition = 'closing'
        try:
            self._started = False
            cancelled = False
            if self._monitor:
                task = self._monitor
                task.cancel()
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        cancelled = cancelled or asyncio.current_task().cancelling() > 0
                self._monitor = None
            # A public refresh may own the sampler instead of the monitor.
            # New/queued refreshes cannot collect once _started is withdrawn.
            acquire = asyncio.ensure_future(self._sample_lock.acquire())
            while not acquire.done():
                try:
                    await asyncio.shield(acquire)
                except asyncio.CancelledError:
                    cancelled = True
            acquire.result()
            self._sample_lock.release()
            # The sampler drains its bounded network thread before this unlock.
            self._lock.close()
            self._blocked = 'not_started'
            if cancelled:
                raise asyncio.CancelledError
        finally:
            self._transition = None

    async def _watch(self):
        while True:
            expected = time.monotonic() + self.policy.server.poll_interval_seconds
            await asyncio.sleep(self.policy.server.poll_interval_seconds)
            self._loop_lag = max(0.0, time.monotonic() - expected)
            await self.refresh()

    def _validate(self, sample):
        p = self.policy.server
        if not isinstance(sample, CapacitySnapshot):
            return 'telemetry_unavailable'
        try:
            invalid = any(type(getattr(sample, f.name)) not in (int, float) or
                          not math.isfinite(getattr(sample, f.name)) or getattr(sample, f.name) < 0
                          for f in fields(sample))
        except (OverflowError, TypeError, ValueError):
            invalid = True
        if invalid:
            return 'telemetry_invalid'
        if not 0 <= time.monotonic() - sample.sampled_at <= p.max_sample_age_seconds:
            return 'telemetry_stale'
        if p.telemetry == 'linux_host':
            wrong_envelope = (sample.memory_limit_mib < p.host_min_memory_mib or
                              sample.cpu_limit_vcpu < p.task_cpu_vcpu or
                              sample.disk_limit_mib < p.task_ephemeral_mib)
        else:
            wrong_envelope = (sample.memory_limit_mib != p.task_memory_mib or
                              sample.cpu_limit_vcpu != p.task_cpu_vcpu or
                              sample.disk_limit_mib < p.task_ephemeral_mib)
        if wrong_envelope:
            return 'task_envelope_mismatch'
        return None

    def _pressures(self, sample, *, resume=False):
        p = self.policy.server
        suffix = '_resume' if resume else '_high'
        checks = (
            ('cpu_pressure', sample.cpu_fraction, getattr(p, 'cpu' + suffix)),
            ('memory_pressure', sample.memory_used_mib / sample.memory_limit_mib,
             getattr(p, 'memory' + suffix)),
            ('disk_pressure', sample.disk_used_mib / p.task_ephemeral_mib,
             getattr(p, 'disk' + suffix)),
            ('loop_lag', self._loop_lag, p.loop_lag_high_seconds),
        )
        reasons = [reason for reason, observed, threshold in checks if observed >= threshold]
        if p.task_ephemeral_mib - sample.disk_used_mib < p.min_disk_free_mib:
            reasons.append('disk_headroom')
        return reasons

    def _block(self, reason):
        # Host CPU needs two readings at startup. No valid resource observation
        # means admission must stay closed, but no overload has been observed
        # that would justify applying recovery (low-water) thresholds yet.
        # Retain this distinction for readiness checks as well as the sampler.
        if reason == 'telemetry_unavailable' and self._blocked == 'not_started' and self._snapshot is None:
            self._healthy_samples = 0
            return
        self._blocked, self._healthy_samples = reason, 0

    async def refresh(self):
        # One sampler per task; callers never sample as part of admission.
        if not self._started:
            raise ServerOverloaded('not_started')
        async with self._sample_lock:
            if not self._started:
                raise ServerOverloaded('not_started')
            try:
                sample = await self.telemetry.sample()
            except Exception:
                self._block('telemetry_unavailable')
                return
            reason = self._validate(sample)
            if reason:
                self._block(reason)
                return
            self._snapshot = sample
            pressures = self._pressures(sample)
            if pressures:
                # Track every resource that actually crossed its high mark.
                # A later telemetry gap must not erase an overload's hysteresis.
                self._recovering_pressures.update(pressures)
                self._block(pressures[0])
                self._last_recovery_sample = sample.sampled_at
                return
            if self._blocked == 'not_started':
                self._blocked = None
            elif self._blocked:
                recovering = [reason for reason in self._pressures(sample, resume=True)
                              if reason in self._recovering_pressures]
                if recovering:
                    self._blocked = recovering[0]
                    self._healthy_samples = 0
                elif sample.sampled_at > self._last_recovery_sample:
                    self._healthy_samples += 1
                    self._last_recovery_sample = sample.sampled_at
                    if self._healthy_samples >= self.policy.server.recovery_samples:
                        self._blocked = None
                        self._recovering_pressures.clear()

    def check(self):
        if self._fatal:
            raise ServerOverloaded(self._fatal)
        if self._transition:
            raise ServerOverloaded('lifecycle_transition')
        if not self._started:
            raise ServerOverloaded('not_started')
        reason = self._validate(self._snapshot)
        if reason:
            self._block(reason)
        if self._blocked:
            raise ServerOverloaded(self._blocked)

    def check_response(self):
        """Drain an already bounded reply during CPU-only admission pressure.

        Do not clear admission hysteresis or mask a second resource pressure.
        The caller must retain its response-size and concurrent-buffer limits.
        """
        try:
            self.check()
        except ServerOverloaded as error:
            if error.reason != 'cpu_pressure' or self._fatal:
                raise
            remaining = (self._recovering_pressures | set(self._pressures(self._snapshot))) - {'cpu_pressure'}
            if remaining:
                raise ServerOverloaded(sorted(remaining)[0]) from None

    def acquire_request(self):
        self.check()
        p, sample = self.policy.server, self._snapshot
        count = self._requests + 1
        memory = count * p.request_memory_mib
        workspace = count * self.policy.workspace.max_size_mb
        if count > min(p.max_requests, self.policy.admission.max_concurrent_requests):
            raise ServerOverloaded('request_slots')
        if sample.memory_used_mib + memory > sample.memory_limit_mib * p.memory_high:
            raise ServerOverloaded('memory_reservations')
        if (workspace > p.max_workspace_mib or
                sample.disk_used_mib + workspace > p.task_ephemeral_mib * p.disk_high or
                p.task_ephemeral_mib - sample.disk_used_mib - workspace < p.min_disk_free_mib):
            raise ServerOverloaded('workspace_reservations')
        self._requests += 1
        return _Lease(self._release_request)

    def _release_request(self):
        self._requests -= 1

    def poison(self, reason):
        """Unknown adapter cleanup cannot recover merely because metrics look low."""
        self._fatal = reason

    def acquire_operation(self, kind):
        self.check()
        if kind not in self._operations:
            raise ValueError('unknown operation resource')
        if self._operations[kind] >= getattr(self.policy.server, f'max_{kind}_inflight'):
            raise ServerOverloaded(kind + '_slots')
        self._operations[kind] += 1
        def release():
            self._operations[kind] -= 1
        return _Lease(release)

    def status(self):
        try:
            self.check()
            ready, reason = True, None
        except ServerOverloaded as exc:
            ready, reason = False, exc.reason
        return {'ready': ready, 'reason': reason, 'active_requests': self._requests,
                'recovering_pressures': sorted(self._recovering_pressures),
                'sample_age_seconds': round(max(0, time.monotonic() - self._snapshot.sampled_at), 3)
                    if self._snapshot else None,
                'rejections': dict(self._rejections),
                'reserved_memory_mib': self._requests * self.policy.server.request_memory_mib,
                'reserved_workspace_mib': self._requests * self.policy.workspace.max_size_mb,
                'inflight': dict(self._operations)}
