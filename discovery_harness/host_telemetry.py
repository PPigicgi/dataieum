"""Host-wide Linux capacity for colocated application, PostgreSQL, and OS.

Sources: https://docs.kernel.org/filesystems/proc.html and
https://docs.python.org/3/library/os.html#os.statvfs
MemAvailable estimates capacity without swapping; it is not process RSS.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math
import os
from pathlib import Path
import re
import stat
import sys
import time

from .capacity import CapacitySnapshot
from .telemetry import TelemetryUnavailable


def _unsigned(value) -> int:
    if isinstance(value, str):
        if not value.isascii() or not value.isdecimal() or len(value) > 20:
            raise TelemetryUnavailable('invalid host resource counter')
        value = int(value)
    if type(value) is not int or not 0 <= value <= 2**64 - 1:
        raise TelemetryUnavailable('invalid host resource counter')
    return value


def _clock_value(value) -> float:
    if type(value) not in (int, float):
        raise TelemetryUnavailable('invalid host collection time')
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise TelemetryUnavailable('invalid host collection time') from exc
    if not math.isfinite(result) or result < 0:
        raise TelemetryUnavailable('invalid host collection time')
    return result


@dataclass(frozen=True)
class _HostReading:
    sampled_at: float
    identity: tuple
    cpu_ticks: tuple[int, ...]
    cpu_count: int
    memory_used_mib: float
    memory_limit_mib: float
    disk_used_mib: float
    disk_limit_mib: float


class LinuxHostTelemetry:
    """Sample the actual host envelope, not a selected process or container.

    Containers must mount the host procfs read-only and pass that proc_root.
    disk_path must resolve to the local filesystem shared by PostgreSQL data and
    the harness workspace. The parent integration must verify this deployment
    condition; this provider observes only the selected filesystem.

    CPU pressure excludes idle and iowait from the first eight /proc/stat counters.
    I/O waiting is bounded by operation deadlines, not treated as CPU execution.
    Steal remains unavailable CPU; guest time is already included
    in user/nice and is not added again. Linux can report decreasing iowait: a
    decrease invalidates the observation instead of producing false free CPU.

    At most one collecting thread can exist per instance. Each proc file is
    capped at one MiB with local read deadline checks. A stuck kernel syscall
    cannot be interrupted by Python; cancellation drains that worker and keeps
    the sampler occupied, rather than silently leaking threads. No network or
    Fargate environment variable is used.
    """

    PROC_READ_LIMIT = 1024**2
    READ_DEADLINE_SECONDS = 1.0

    def __init__(self, proc_root='/proc', *, disk_path='/', clock=time.monotonic,
                 max_sample_age_seconds=5.0):
        self.proc_root = Path(proc_root)
        self.disk_path = Path(disk_path)
        self._clock = clock
        self.max_sample_age_seconds = _clock_value(max_sample_age_seconds)
        if self.max_sample_age_seconds == 0:
            raise TelemetryUnavailable('sample age limit must be positive')
        self._baseline: _HostReading | None = None
        self._sampling = False

    async def sample(self) -> CapacitySnapshot:
        if self._sampling:
            raise TelemetryUnavailable('a host sample is already in progress')
        self._sampling = True
        cancelled = None
        try:
            worker = asyncio.create_task(asyncio.to_thread(self._collect))
            while not worker.done():
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError as exc:
                    cancelled = exc
                except Exception:
                    break
            if cancelled is not None:
                self._baseline = None
                try:
                    worker.result()
                except BaseException:
                    pass
                raise cancelled
            try:
                stat_text, meminfo_text, filesystem, sampled_at = worker.result()
            except Exception as exc:
                self._baseline = None
                if isinstance(exc, TelemetryUnavailable):
                    raise
                raise TelemetryUnavailable('Linux host collection failed') from exc
            return self.evaluate(stat_text, meminfo_text, filesystem, sampled_at=sampled_at)
        finally:
            self._sampling = False

    def _collect(self):
        if not sys.platform.startswith('linux') or not hasattr(os, 'statvfs'):
            raise TelemetryUnavailable('Linux host telemetry requires Linux procfs and statvfs')
        sampled_at = _clock_value(self._clock())
        stat_text = self._read_proc('stat')
        meminfo_text = self._read_proc('meminfo')
        filesystem = os.statvfs(self.disk_path)
        return stat_text, meminfo_text, filesystem, sampled_at

    def _read_proc(self, name: str) -> str:
        deadline = time.monotonic() + self.READ_DEADLINE_SECONDS
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
        flags |= getattr(os, 'O_BINARY', 0)
        descriptor = os.open(self.proc_root / name, flags)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise TelemetryUnavailable('host proc source must be a regular procfs file')
            body = bytearray()
            while True:
                if time.monotonic() >= deadline:
                    raise TelemetryUnavailable('host proc read deadline exceeded')
                chunk = os.read(descriptor, min(65536, self.PROC_READ_LIMIT + 1 - len(body)))
                if time.monotonic() >= deadline:
                    raise TelemetryUnavailable('host proc read deadline exceeded')
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > self.PROC_READ_LIMIT:
                    raise TelemetryUnavailable('host proc source exceeds one MiB')
            return body.decode('ascii')
        finally:
            os.close(descriptor)

    def evaluate(self, stat_text: str, meminfo_text: str, filesystem, *,
                 sampled_at: float | None = None) -> CapacitySnapshot:
        """Evaluate bounded host-source fixtures without accessing Linux APIs."""
        try:
            now = _clock_value(self._clock())
            start = now if sampled_at is None else _clock_value(sampled_at)
            if start > now or now - start > self.max_sample_age_seconds:
                raise TelemetryUnavailable('host observation is stale or in the future')
            current = self._parse(stat_text, meminfo_text, filesystem, start)
        except Exception as exc:
            self._baseline = None
            if isinstance(exc, TelemetryUnavailable):
                raise
            raise TelemetryUnavailable('incomplete or malformed Linux host sources') from exc
        previous = self._baseline
        self._baseline = current
        if previous is None or current.identity != previous.identity:
            raise TelemetryUnavailable('host CPU observation needs a second sample')
        elapsed = current.sampled_at - previous.sampled_at
        if elapsed <= 0 or elapsed > self.max_sample_age_seconds:
            raise TelemetryUnavailable('host observation time did not advance continuously')
        deltas = tuple(new - old for new, old in zip(current.cpu_ticks, previous.cpu_ticks))
        total = sum(deltas)
        if any(delta < 0 for delta in deltas) or total <= 0:
            raise TelemetryUnavailable('host CPU counters repeated or decreased')
        pressure = (total - deltas[3] - deltas[4]) / total
        return CapacitySnapshot(current.sampled_at, pressure,
                                current.memory_used_mib, current.memory_limit_mib,
                                current.disk_used_mib, current.disk_limit_mib,
                                float(current.cpu_count))

    def _parse(self, stat_text, meminfo_text, filesystem, sampled_at):
        for text in (stat_text, meminfo_text):
            if not isinstance(text, str) or len(text) > self.PROC_READ_LIMIT or not text.isascii():
                raise TelemetryUnavailable('host proc source is invalid or oversized')
        aggregate, core_ids, boot_time = None, set(), None
        for line in stat_text.splitlines():
            fields = line.split()
            if not fields:
                continue
            key = fields[0]
            if key == 'cpu' or re.fullmatch(r'cpu[0-9]+', key):
                if len(fields) < 9:
                    raise TelemetryUnavailable('incomplete host CPU counters')
                ticks = tuple(_unsigned(value) for value in fields[1:9])
                if key == 'cpu':
                    if aggregate is not None:
                        raise TelemetryUnavailable('duplicate host CPU total')
                    aggregate = ticks
                else:
                    if key in core_ids:
                        raise TelemetryUnavailable('duplicate host CPU core')
                    core_ids.add(key)
            elif key == 'btime':
                if len(fields) != 2 or boot_time is not None:
                    raise TelemetryUnavailable('invalid host boot identity')
                boot_time = _unsigned(fields[1])
        if aggregate is None or not core_ids or boot_time is None:
            raise TelemetryUnavailable('missing host CPU or boot counters')
        memory = {}
        for line in meminfo_text.splitlines():
            fields = line.split()
            if not fields or fields[0] not in ('MemTotal:', 'MemAvailable:'):
                continue
            key = fields[0]
            if key in memory or len(fields) != 3 or fields[2] != 'kB':
                raise TelemetryUnavailable('invalid host memory field')
            memory[key] = _unsigned(fields[1])
        total, available = memory['MemTotal:'], memory['MemAvailable:']
        if total == 0 or available > total:
            raise TelemetryUnavailable('invalid host available memory')
        fragment = _unsigned(filesystem.f_frsize)
        blocks = _unsigned(filesystem.f_blocks)
        available_blocks = _unsigned(filesystem.f_bavail)
        if fragment == 0 or blocks == 0 or available_blocks > blocks:
            raise TelemetryUnavailable('invalid host filesystem capacity')
        filesystem_id = _unsigned(getattr(filesystem, 'f_fsid', 0))
        identity = (boot_time, tuple(sorted(core_ids)), total, fragment, blocks, filesystem_id)
        return _HostReading(sampled_at, identity, aggregate, len(core_ids),
                            (total - available) / 1024, total / 1024,
                            (blocks - available_blocks) * fragment / 1024**2,
                            blocks * fragment / 1024**2)
