"""Collect bounded async sources; source cleanup stays inside its operation lease."""
import asyncio
import inspect
import json
from dataclasses import asdict, dataclass

from .errors import AdapterContractError, BudgetExceeded, CleanupFailed, PolicyError
from .resources import bounded_json


@dataclass(frozen=True)
class BoundedRows:
    rows: tuple
    truncated: bool
    reason: str | None = None


def positive_limit(requested, ceiling):
    value = ceiling if requested is None else requested
    if type(value) is not int or value <= 0 or value > ceiling:
        raise PolicyError('stream limit must be positive and within policy')
    return value


async def _open(session, factory):
    source = factory()
    if not callable(getattr(source, '__anext__', None)) or not callable(getattr(source, 'aclose', None)):
        if callable(getattr(source, 'aclose', None)):
            await _finish(session, source)
        elif inspect.iscoroutine(source):
            source.close()
        elif callable(getattr(source, '__anext__', None)):
            session._mark_cleanup_failed()
        raise AdapterContractError('stream factory must return an async iterator with aclose')
    return source


async def _finish(session, source):
    cancelled = False
    try:
        closing = asyncio.ensure_future(source.aclose())
        while not closing.done():
            try:
                await asyncio.shield(closing)
            except asyncio.CancelledError:
                cancelled = True
        closing.result()
    except BaseException as exc:
        session._mark_cleanup_failed()
        raise CleanupFailed('stream cleanup unconfirmed; new work is blocked') from exc
    if cancelled:
        raise asyncio.CancelledError


async def _next(session, source):
    session._reserve(stream_items=1)  # empty items and the EOF probe also cost work
    if session._capacity:
        session._capacity.check()
    value = await anext(source)
    session._check()
    await asyncio.sleep(0)
    return value


async def collect_bytes(session, factory, limit):
    output = bytearray()
    source = await _open(session, factory)
    try:
        while True:
            try:
                chunk = await _next(session, source)
            except StopAsyncIteration:
                return bytes(output)
            if type(chunk) is not bytes:
                raise AdapterContractError('byte stream must yield bytes')
            if len(chunk) > limit - len(output):
                raise BudgetExceeded('result_bytes', limit)
            output.extend(chunk)
    finally:
        await _finish(session, source)


async def collect_rows(session, factory, limit, *, edges=False):
    if session.policy.agent.max_result_bytes < 2:
        raise BudgetExceeded('result_bytes', session.policy.agent.max_result_bytes)
    rows, size = [], 2  # JSON list brackets; bytes cap includes commas and UTF-8
    source = await _open(session, factory)
    try:
        while len(rows) < limit:
            try:
                row = await _next(session, source)
            except StopAsyncIteration:
                return BoundedRows(tuple(rows), False)
            if edges:
                from .graph import Edge
                if not isinstance(row, Edge):
                    raise AdapterContractError('graph stream must yield Edge')
                value = asdict(row)
            else:
                value = row
            available = session.policy.agent.max_result_bytes - size - bool(rows)
            encoded = bounded_json(value, max(0, available))
            size += len(encoded) + bool(rows)
            rows.append(row if edges else json.loads(encoded))
        # Reaching a row cap is conservatively partial; do not prefetch one extra.
        return BoundedRows(tuple(rows), True, 'result_rows')
    finally:
        await _finish(session, source)
