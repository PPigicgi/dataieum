"""Incremental breadth-first traversal with request-wide work budgets."""
import asyncio
from contextlib import contextmanager
from collections import deque
from dataclasses import dataclass
import inspect
from typing import TYPE_CHECKING

from .errors import AdapterContractError, BudgetExceeded, CleanupFailed, PolicyError

if TYPE_CHECKING:
    from .runtime import Session


@dataclass(frozen=True)
class Node:
    id: str
    kind: str = 'concept'

    def __post_init__(self):
        if not isinstance(self.id, str) or not self.id or len(self.id.encode('utf-8')) > 256:
            raise PolicyError('node id must be 1..256 UTF-8 bytes')
        if self.kind not in {'topic', 'concept', 'indicator', 'dataset', 'provider',
                            'request', 'purpose', 'analysis', 'assertion', 'evidence', 'area', 'format', 'access', 'organization', 'domain'}:
            raise PolicyError('unknown graph node kind')


@dataclass(frozen=True)
class Edge:
    source: str
    target: Node
    relation: str

    def __post_init__(self):
        if not isinstance(self.target, Node) or not isinstance(self.source, str):
            raise PolicyError('invalid edge endpoints')
        if not isinstance(self.relation, str) or not 0 < len(self.relation) <= 100:
            raise PolicyError('invalid edge relation')


@dataclass(frozen=True)
class GraphResult:
    nodes: tuple[Node, ...]
    edges: tuple[Edge, ...]
    truncated: bool
    reasons: tuple[str, ...]


def _close_iterator(session, source):
    try:
        close = getattr(source, 'close', None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise AdapterContractError('synchronous graph cursor requires synchronous close')
    except BaseException as exc:
        session._mark_cleanup_failed()
        raise CleanupFailed('graph cursor cleanup unconfirmed; new work is blocked') from exc


@contextmanager
def _managed_iterator(session, source):
    iterator = None
    try:
        iterator = iter(source)
        yield iterator
    finally:
        try:
            if iterator is not None:
                _close_iterator(session, iterator)
        finally:
            # Construction may fail before ownership transfers to an iterator.
            if source is not iterator:
                _close_iterator(session, source)


async def traverse(session: 'Session', roots, neighbors, *, streamed=False, in_memory=False) -> GraphResult:
    """neighbors(node, limit) returns a bounded iterable; backend calls count as tools.

    Depth counts every edge, including dataset/provider attachment. Roots count
    as nodes at depth zero. Repeated traversals consume the same request budget.
    """
    if streamed and in_memory:
        raise PolicyError('in-memory traversal cannot use a remote stream')
    policy = session.policy.ontology
    nodes, edges, seen_edges, queue = {}, [], set(), deque()
    reasons = set()

    def finish():
        return GraphResult(tuple(nodes.values()), tuple(edges), bool(reasons), tuple(sorted(reasons)))

    # Root observations consume the same scan budget: an infinite stream of
    # duplicate seeds must not bypass work limits either.
    try:
        with _managed_iterator(session, roots) as root_iterator:
            while True:
                session._check()
                if session.usage.neighbor_items >= policy.max_neighbor_items:
                    reasons.add('neighbor_items')
                    return finish()
                try:
                    node = next(root_iterator)
                except StopIteration:
                    break
                session._reserve(neighbor_items=1)
                if not isinstance(node, Node):
                    raise AdapterContractError('roots must contain Node objects')
                if node.id in nodes:
                    if nodes[node.id] != node:
                        raise AdapterContractError('one node id cannot have conflicting kinds')
                    continue
                session._reserve(nodes=1, datasets=int(node.kind == 'dataset'))
                nodes[node.id] = node
                queue.append((node, 0))
    except BudgetExceeded as exc:
        reasons.add(exc.resource)
        # The admitted roots can still have admissible edges between them.
        # A full node/dataset budget is not a global stop for edge discovery.

    async def consume(values):
        try:
            with _managed_iterator(session, values) as iterator:
                while True:
                    session._check()
                    if session._capacity:
                        session._capacity.check()
                    if session.usage.neighbor_items >= policy.max_neighbor_items:
                        reasons.add('neighbor_items')
                        return True
                    try:
                        edge = next(iterator)
                    except StopIteration:
                        break
                    session._reserve(neighbor_items=1)
                    if not isinstance(edge, Edge) or edge.source != node.id:
                        raise AdapterContractError('neighbor edge has an invalid source')
                    target = edge.target
                    if target.id in nodes and nodes[target.id] != target:
                        raise AdapterContractError('one node id cannot have conflicting kinds')
                    identity = (edge.source, target.id, edge.relation)
                    if target.id == node.id or identity in seen_edges:
                        await asyncio.sleep(0)
                        continue
                    is_new = target.id not in nodes
                    session._reserve(nodes=int(is_new), edges=1,
                                     datasets=int(is_new and target.kind == 'dataset'))
                    if is_new:
                        nodes[target.id] = target
                        queue.append((target, depth + 1))
                    edges.append(edge)
                    seen_edges.add(identity)
                    await asyncio.sleep(0)
        except BudgetExceeded as exc:
            reasons.add(exc.resource)
            return True
        return False

    while queue:
        session._check()
        node, depth = queue.popleft()
        if depth >= policy.max_depth:
            reasons.add('depth')
            continue
        if session.usage.neighbor_items >= policy.max_neighbor_items:
            reasons.add('neighbor_items')
            break
        if session.usage.edges >= policy.max_edges:
            reasons.add('edges')
            break
        remaining = policy.max_neighbor_items - session.usage.neighbor_items
        try:
            if in_memory:
                # One enclosing graph-query tool owns this in-process index.
                # Every node, edge and neighbor still consumes request budgets;
                # no database/tool call is fabricated for a local adjacency read.
                values = neighbors(node, remaining)
                if inspect.isawaitable(values):
                    if inspect.iscoroutine(values): values.close()
                    raise AdapterContractError('in-memory neighbors must be synchronous')
                stop = await consume(values)
            elif streamed:
                from .streams import collect_rows
                batch = await session._invoke(
                    lambda: collect_rows(session, lambda: neighbors(node, remaining), remaining, edges=True),
                    _resource='db', tool_calls=1)
                if batch.truncated:
                    reasons.add('neighbor_items')
                stop = await consume(batch.rows)
            else:
                stop = await session._invoke(lambda: neighbors(node, remaining),
                                             _resource='db', _consume=consume, tool_calls=1)
            if stop:
                return finish()
        except BudgetExceeded as exc:
            reasons.add(exc.resource)
            break
    return finish()
