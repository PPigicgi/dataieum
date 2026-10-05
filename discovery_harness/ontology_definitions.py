"""Load the versioned analyst-reviewed semantic vocabulary, without model calls."""
import json
import re
from functools import lru_cache
from pathlib import Path

CONCEPT_FIELDS = ('definition','entity_type','measure_kind','aliases','unit_dimension','canonical_unit',
                  'temporal_meaning','spatial_semantics','not_equivalent_to','comparability_requirements',
                  'definition_basis','retrieval_policy')

def validate_definitions(value):
    if not isinstance(value,dict) or value.get('version')!=1:
        raise ValueError('invalid ontology definition version')
    concepts=value.get('concepts');workflows=value.get('workflows')
    if not isinstance(concepts,list) or not 1<=len(concepts)<=80 or not isinstance(workflows,list) or not 1<=len(workflows)<=32:
        raise ValueError('unbounded ontology definitions')
    def identifier(v):
        if not isinstance(v,str) or not re.fullmatch('[a-z][a-z0-9_]{0,79}',v):raise ValueError('invalid ontology id')
    def text(v,limit):
        if not isinstance(v,str) or not v.strip() or len(v)>limit:raise ValueError('invalid ontology text')
    def texts(v,limit=20):
        if not isinstance(v,list) or len(v)>limit:raise ValueError('invalid ontology text array')
        for item in v:text(item,300)
    ids=set()
    for c in concepts:
        identifier(c['id']);text(c['label'],80)
        if c['id'] in ids:raise ValueError('duplicate semantic concept')
        ids.add(c['id'])
        for field in CONCEPT_FIELDS:
            if field in {'aliases','not_equivalent_to','comparability_requirements'}:texts(c[field])
            elif field=='canonical_unit':
                if c[field] is not None:text(c[field],80)
            else:text(c[field],600)
        if c['definition_basis']!='curated_domain_definition' or c['retrieval_policy'] not in {'exact_indicator_only','exact_claims_with_curated_relations'}:raise ValueError('unsafe concept basis')
        if c['measure_kind'] not in {'count','rate','amount','duration','position','category','index','event','composite'}:raise ValueError('invalid measure kind')
        if c['temporal_meaning'] not in {'stock','flow','event','static','mixed'}:raise ValueError('invalid temporal meaning')
    for c in concepts:
        if c['id'] in c['not_equivalent_to'] or not set(c['not_equivalent_to'])<=ids:raise ValueError('invalid non-equivalence')
    wids=set()
    for w in workflows:
        identifier(w['id']);text(w['label'],80);text(w['description'],160);texts(w['limitations'])
        if w['id'] in wids:raise ValueError('duplicate workflow')
        wids.add(w['id']);analyses=w['analyses'];seen=set();needed=set()
        if not isinstance(analyses,list) or not 1<=len(analyses)<=3:raise ValueError('invalid analyses')
        for a in analyses:
            identifier(a['id']);text(a['label'],80);text(a['reason'],140)
            if a['id'] in seen:raise ValueError('duplicate analysis')
            seen.add(a['id'])
            if not isinstance(a['indicators'],list) or not 1<=len(a['indicators'])<=5:raise ValueError('invalid analysis needs')
            for n in a['indicators']:
                if n['id'] not in ids:raise ValueError('unknown indicator in domain rule')
                text(n['reason'],100);needed.add(n['id'])
        if len(needed)>5:raise ValueError('workflow exceeds request needs budget')
    from .ontology_schema import RELATED_RELATIONS
    related=value.get('related_links',[]);seen_links=set()
    if not isinstance(related,list) or len(related)>256:raise ValueError('unbounded related links')
    for item in related:
        if not isinstance(item,dict):raise ValueError('invalid related link')
        key=(item.get('source'),item.get('target'),item.get('relation'))
        if key[0] not in ids or key[1] not in ids or key[0]==key[1] or key[2] not in RELATED_RELATIONS or key in seen_links:raise ValueError('invalid related link')
        seen_links.add(key);text(item.get('reason'),300)
        if type(item.get('priority',1)) is not int or not 1<=item.get('priority',1)<=100:raise ValueError('invalid related priority')
        refs=item.get('references',[])
        if not isinstance(refs,list) or len(refs)>4 or any(not isinstance(r,str) or not r.startswith('https://') or len(r)>4096 for r in refs):raise ValueError('invalid relation references')
    domains=value.get('catalog_domains',[]);domain_ids=set()
    if not isinstance(domains,list) or len(domains)>64:raise ValueError('unbounded catalog domains')
    for domain in domains:
        identifier(domain['id']);text(domain['label'],80);text(domain['definition'],600)
        if domain['id'] in domain_ids:raise ValueError('duplicate catalog domain')
        domain_ids.add(domain['id'])
    bindings=value.get('catalog_bindings',[]);bound=set()
    if not isinstance(bindings,list) or len(bindings)>80:raise ValueError('unbounded catalog bindings')
    for binding in bindings:
        iid=binding.get('indicator');targets=binding.get('domains')
        if iid not in ids or iid in bound or not isinstance(targets,list) or not 1<=len(targets)<=3 or len(set(targets))!=len(targets) or not set(targets)<=domain_ids:raise ValueError('invalid catalog binding')
        bound.add(iid);text(binding.get('reason'),300)
    if value.get('coverage_policy')=='all_supported_concepts':
        expected=ids-{'other'}
        if {r['source'] for r in related}!=expected or bound!=expected:raise ValueError('incomplete concept coverage')
        if not domain_ids-{'unclassified'}<={d for b in bindings for d in b['domains']}:raise ValueError('unmapped catalog domain')
    return value

@lru_cache(maxsize=1)
def definitions():
    path=Path(__file__).with_name('ontology_domains.json')
    with path.open(encoding='utf-8') as stream:value=json.load(stream)
    return validate_definitions(value)

def workflow_catalog():
    return {w['id']:{'label':w['label'],'description':w['description']} for w in definitions()['workflows']}
