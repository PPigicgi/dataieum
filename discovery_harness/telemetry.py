"""Bounded Fargate Linux metadata v4 sampling; no credentials or AWS SDK.

Wire units and examples:
https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-fargate-examples.html
CPU counters are cumulative Linux nanoseconds, normalized by task vCPU.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
import json
import math
import os
import re
import time
from typing import Any, Callable
import urllib.request

from .capacity import CapacitySnapshot


class TelemetryUnavailable(Exception):
    """A complete, fresh task-wide capacity observation is unavailable."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise TelemetryUnavailable('metadata redirects are forbidden')


@dataclass(frozen=True)
class _Reading:
    identity: tuple[Any, ...]
    counters: dict[str, tuple[float, int]]
    memory_used_mib: float
    memory_limit_mib: float
    disk_used_mib: float
    disk_limit_mib: float
    cpu_limit_vcpu: float


def _number(value: Any, *, positive: bool = False) -> float:
    if type(value) not in (int, float):
        raise TelemetryUnavailable('invalid numeric metadata')
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise TelemetryUnavailable('invalid numeric metadata') from exc
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        raise TelemetryUnavailable('invalid numeric metadata')
    return number


def _counter(value: Any) -> int:
    if type(value) is not int or not 0 <= value <= 2**64 - 1:
        raise TelemetryUnavailable('invalid resource counter')
    return value


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise TelemetryUnavailable('missing metadata identity')
    return value


def _strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TelemetryUnavailable('duplicate metadata field')
        result[key] = value
    return result


def _invalid_constant(value):
    raise TelemetryUnavailable('nonfinite metadata JSON')


class FargateTelemetry:
    """One sampler per task; concurrent sample calls fail instead of queuing.

    ``evaluate`` is the deterministic, network-free parsing entry point. It
    maintains a CPU baseline and uses injected seconds-valued clocks for tests.
    The first observation, reset, incomplete data, or repeated source timestamp
    raises TelemetryUnavailable; callers must withdraw capacity readiness.

    HTTP socket operations time out after one second. A read1/deadline loop also
    stops continuously trickled bodies: one already-running socket operation may
    outlive that deadline by at most its socket timeout. Cancellation waits for
    this bounded worker to stop before releasing the single sampling slot.
    """

    RESPONSE_LIMIT = 1024**2
    HTTP_TIMEOUT_SECONDS = 1.0

    def __init__(self, base_uri: str | None = None, *,
                 monotonic_clock: Callable[[], float] = time.monotonic,
                 wall_clock: Callable[[], float] = time.time,
                 max_sample_age_seconds: float = 5.0,
                 future_tolerance_seconds: float = 0.5):
        uri = base_uri if base_uri is not None else os.environ.get('ECS_CONTAINER_METADATA_URI_V4', '')
        if not isinstance(uri, str):
            raise TelemetryUnavailable('ECS metadata v4 URI is unavailable')
        if re.fullmatch(r'http://169\.254\.170\.2/v4/[A-Za-z0-9][A-Za-z0-9_-]{0,255}/?', uri) is None:
            raise TelemetryUnavailable('only the ECS link-local metadata v4 URI is supported')
        self.base_uri = uri.rstrip('/')
        self._monotonic = monotonic_clock
        self._wall = wall_clock
        self.max_sample_age_seconds = _number(max_sample_age_seconds, positive=True)
        self.future_tolerance_seconds = _number(future_tolerance_seconds)
        self._baseline: _Reading | None = None
        self._sampling = False

    async def sample(self) -> CapacitySnapshot:
        if self._sampling:
            raise TelemetryUnavailable('a metadata sample is already in progress')
        self._sampling = True
        cancelled = None
        try:
            worker = asyncio.create_task(asyncio.to_thread(self._fetch_pair))
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
                task, stats = worker.result()
            except Exception as exc:
                self._baseline = None
                raise TelemetryUnavailable('metadata collection failed') from exc
            return self.evaluate(task, stats)
        except TelemetryUnavailable:
            raise
        except Exception as exc:
            self._baseline = None
            raise TelemetryUnavailable('metadata collection failed') from exc
        finally:
            self._sampling = False

    def _fetch_pair(self):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        return self._fetch_json(opener, '/task'), self._fetch_json(opener, '/task/stats')

    def _fetch_json(self, opener, suffix: str) -> dict:
        deadline = time.monotonic() + self.HTTP_TIMEOUT_SECONDS
        request = urllib.request.Request(self.base_uri + suffix, method='GET',
                                         headers={'Accept': 'application/json'})
        try:
            with opener.open(request, timeout=self.HTTP_TIMEOUT_SECONDS) as response:
                if response.status != 200 or response.headers.get('Content-Encoding', 'identity') != 'identity':
                    raise TelemetryUnavailable('unexpected metadata HTTP response')
                body = bytearray()
                while True:
                    if time.monotonic() >= deadline:
                        raise TelemetryUnavailable('metadata response deadline exceeded')
                    chunk = response.read1(min(65536, self.RESPONSE_LIMIT + 1 - len(body)))
                    if time.monotonic() >= deadline:
                        raise TelemetryUnavailable('metadata response deadline exceeded')
                    if not chunk:
                        break
                    body.extend(chunk)
                    if len(body) > self.RESPONSE_LIMIT:
                        raise TelemetryUnavailable('metadata response exceeds one MiB')
            value = json.loads(body, object_pairs_hook=_strict_object, parse_constant=_invalid_constant)
            if not isinstance(value, dict):
                raise TelemetryUnavailable('metadata response must be an object')
            return value
        except TelemetryUnavailable:
            raise
        except Exception as exc:
            raise TelemetryUnavailable('metadata response could not be read') from exc

    def evaluate(self, task: dict, stats: dict) -> CapacitySnapshot:
        """Parse source documents and form a fresh two-sample CPU observation."""
        try:
            now_wall = _number(self._wall(), positive=True)
            now_monotonic = _number(self._monotonic())
            current = self._parse(task, stats, now_wall)
        except Exception as exc:
            self._baseline = None
            if isinstance(exc, TelemetryUnavailable):
                raise
            raise TelemetryUnavailable('incomplete or malformed task metadata') from exc
        previous = self._baseline
        if previous is None or current.identity != previous.identity:
            self._baseline = current
            raise TelemetryUnavailable('CPU observation needs a second sample')
        total_cores = 0.0
        for container_id, (timestamp, usage) in current.counters.items():
            old_timestamp, old_usage = previous.counters[container_id]
            elapsed = timestamp - old_timestamp
            if elapsed <= 0 or usage < old_usage or elapsed > self.max_sample_age_seconds:
                if elapsed != 0 or usage != old_usage:
                    self._baseline = current
                raise TelemetryUnavailable('CPU observation is repeated, reset, or discontinuous')
            total_cores += (usage - old_usage) / (elapsed * 1e9)
        self._baseline = current
        oldest_read = min(timestamp for timestamp, _ in current.counters.values())
        source_age = max(0.0, now_wall - oldest_read)
        return CapacitySnapshot(
            now_monotonic - source_age, total_cores / current.cpu_limit_vcpu,
            current.memory_used_mib, current.memory_limit_mib,
            current.disk_used_mib, current.disk_limit_mib, current.cpu_limit_vcpu)

    def _parse(self, task: dict, stats: dict, now_wall: float) -> _Reading:
        if not isinstance(task, dict) or not isinstance(stats, dict):
            raise TelemetryUnavailable('metadata documents must be objects')
        if task['KnownStatus'] != 'RUNNING' or task['LaunchType'] != 'FARGATE':
            raise TelemetryUnavailable('running Fargate task required')
        task_id = _identifier(task['TaskARN'])
        cpu_limit = _number(task['Limits']['CPU'], positive=True)
        memory_limit = _number(task['Limits']['Memory'], positive=True)
        storage = task['EphemeralStorageMetrics']
        disk_used = _number(storage['Utilized'])
        disk_limit = _number(storage['Reserved'], positive=True)
        if not isinstance(task['Containers'], list):
            raise TelemetryUnavailable('missing task container list')
        identities, counters, all_ids = [], {}, set()
        memory_used = 0
        for container in task['Containers']:
            container_id = _identifier(container['DockerId'])
            if container_id in all_ids:
                raise TelemetryUnavailable('duplicate task container')
            all_ids.add(container_id)
            if not isinstance(container['KnownStatus'], str):
                raise TelemetryUnavailable('unknown container status')
            if container['KnownStatus'] != 'RUNNING':
                continue
            value = stats[container_id]
            if value.get('id', container_id) != container_id or value.get('os_type', 'linux') != 'linux':
                raise TelemetryUnavailable('container identity or operating system mismatch')
            raw_time = value['read']
            if not isinstance(raw_time, str) or len(raw_time) > 64:
                raise TelemetryUnavailable('invalid collection time')
            read_time = datetime.fromisoformat(raw_time)
            if read_time.tzinfo is None:
                raise TelemetryUnavailable('collection time requires a timezone')
            timestamp = read_time.timestamp()
            age = now_wall - timestamp
            if age < -self.future_tolerance_seconds or age > self.max_sample_age_seconds:
                raise TelemetryUnavailable('collection time is stale or in the future')
            usage = _counter(value['cpu_stats']['cpu_usage']['total_usage'])
            memory_used += _counter(value['memory_stats']['usage'])
            counters[container_id] = (timestamp, usage)
            restart = _counter(container.get('RestartCount', 0))
            started = container.get('StartedAt', '')
            if not isinstance(started, str):
                raise TelemetryUnavailable('invalid container start identity')
            identities.append((container_id, restart, started))
        if not counters or set(stats) - all_ids:
            raise TelemetryUnavailable('incomplete task container coverage')
        identity = (task_id, cpu_limit, memory_limit, disk_limit, tuple(sorted(identities)))
        return _Reading(identity, counters, memory_used / 1024**2,
                        memory_limit, disk_used, disk_limit, cpu_limit)
