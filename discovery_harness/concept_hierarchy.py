"""Definition-based containment, separate from topical and similarity links.

Only child -> parent is stored. Inverse navigation never creates another edge.
This small, immutable dictionary does not assert that any dataset is available.
"""
from functools import lru_cache
import hashlib
import json
from pathlib import Path

MAX_BYTES = 262144


def definition_hash(node):
    return hashlib.sha256(node['definition'].encode('utf-8')).hexdigest()


def validate(value):
    nodes, edges = value['nodes'], value['edges']
    if not 1 <= len(nodes) <= 256 or len(edges) > 256:
        raise ValueError('hierarchy size limit')
    by_id = {n['id']: n for n in nodes}
    if len(by_id) != len(nodes):
        raise ValueError('duplicate concept')
    for node in nodes:
        if not all(isinstance(node[k], str) and node[k] for k in ('id', 'label', 'definition', 'kind')):
            raise ValueError('missing concept definition')
    parents = {key: [] for key in by_id}
    seen = set()
    for edge in edges:
        child, parent = edge['source'], edge['target']
        if child not in by_id or parent not in by_id or child == parent or (child, parent) in seen:
            raise ValueError('invalid containment edge')
        seen.add((child, parent))
        if edge['relation'] != 'is_a' or not edge['reason'] or not edge['restriction']:
            raise ValueError('missing containment justification')
        kinds = (by_id[child]['kind'], by_id[parent]['kind'])
        if kinds not in {('entity', 'entity'), ('indicator', 'indicator'), ('indicator', 'indicator_family')}:
            raise ValueError('incompatible concept kinds')
        if edge['definition_versions'] != {child: definition_hash(by_id[child]), parent: definition_hash(by_id[parent])}:
            raise ValueError('stale definition review')
        parents[child].append(parent)
    visiting, done = set(), set()
    def visit(key):
        if key in visiting:
            raise ValueError('containment cycle')
        if key in done:
            return
        visiting.add(key)
        for parent in parents[key]:
            visit(parent)
        visiting.remove(key)
        done.add(key)
    for key in parents:
        visit(key)
    return value


@lru_cache(maxsize=1)
def snapshot_bytes():
    raw = Path(__file__).with_name('concept_hierarchy.json').read_bytes()
    if len(raw) > MAX_BYTES:
        raise ValueError('hierarchy byte limit')
    value = validate(json.loads(raw))
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
