"""Optional concept-diverse discovery using saved relations and unchanged filters."""
import copy
import hashlib
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from .ontology_definitions import definitions
from .site_search import (validate_plan, validate_site_result, related_search_plan,
                          effective_plan, SCOPE_FIELDS)

MAX_CONCEPTS = 3


def url_identity(url):
    parts=urlsplit(url)
    return urlunsplit((parts.scheme.lower(),parts.netloc.lower(),parts.path,
                      urlencode(sorted(parse_qsl(parts.query,keep_blank_values=True))),''))


def url_hash(url):return hashlib.sha256(url_identity(url).encode()).hexdigest()


def related_plans(plan):
    validate_plan(plan)
    if plan['question'] or plan.get('related_mode')=='exclude':return []
    domain=definitions(); concepts={c['id']:c for c in domain['concepts']}
    excluded={n['indicator'] for n in plan['needs']}|set(plan.get('related_exclusions',[]))
    # Round-robin across original needs. Never combine their country/year scopes.
    queues=[]
    for need in plan['needs']:
        queues.append([(link,need) for link in sorted(domain['related_links'],key=lambda r:r.get('priority',1))
                       if link['source']==need['indicator'] and link['target'] not in excluded])
    result=[]
    while any(queues) and len(result)<MAX_CONCEPTS:
        for queue in queues:
            while queue:
                link,source=queue.pop(0); identifier=link['target']
                if identifier in excluded:continue
                excluded.add(identifier); concept=concepts[identifier]
                target=related_search_plan(plan,source,identifier,link['reason'])
                # Narrow subgroups, explicit columns and units cannot silently vanish.
                criteria=effective_plan(plan,source)
                target['fields']=list(criteria['fields'])
                target['needs'][0].update(subject=source.get('subject',''),unit=source.get('unit',''))
                target.update(related_mode='exclude',semantic_query=' '.join(filter(None,[
                    source.get('subject',''),concept['label'],concept['definition']]))[:320])
                result.append({'concept_id':identifier,'label':concept['label'],
                               'source_indicator':source['indicator'],'plan':validate_plan(target)})
                break
            if len(result)==MAX_CONCEPTS:break
    return result


def facts(result):
    return [f for group in result.get('groups',[]) for site in group.get('sites',[]) for f in site.get('evidence',[])]


def parent_payload(result):
    validate_site_result(result)
    if result['state']=='clarify' or not related_plans(result['plan']):raise ValueError('No related concepts')
    rows=facts(result)
    return {'kind':'related','context':copy.deepcopy(result['plan']),
            'exclude_ids':list(dict.fromkeys(f['dataset_id'] for f in rows))[:10],
            'exclude_urls':list(dict.fromkeys(f['evidence_url'] for f in rows))[:10]}


def empty_result(status='empty'):
    return {'version':1,'kind':'related','status':status,'items':[]}


def validate_result(result):
    if (not isinstance(result,dict) or result.get('version')!=1 or result.get('kind')!='related'
            or result.get('status') not in {'results','empty','skipped'}
            or not isinstance(result.get('items'),list) or len(result['items'])>MAX_CONCEPTS):
        raise ValueError('Invalid related result')
    ids=set();urls=set();concepts=set()
    for item in result['items']:
        if not isinstance(item,dict) or set(item)!={'concept_id','label','source_indicator','result'}:
            raise ValueError('Invalid related item')
        known={c['id']:c for c in definitions()['concepts']}
        if item['concept_id'] not in known or item['label']!=known[item['concept_id']]['label'] or item['concept_id'] in concepts:
            raise ValueError('Invalid related concept')
        if not any(r['source']==item['source_indicator'] and r['target']==item['concept_id'] for r in definitions()['related_links']):
            raise ValueError('Unknown saved relation')
        value=validate_site_result(item['result']); rows=facts(value)
        if len(rows)!=1 or value['plan']['needs'][0]['indicator']!=item['concept_id'] or rows[0].get('relevance_tier')!='direct':
            raise ValueError('Related concept requires one directly matching record')
        row=rows[0];url=url_identity(row['evidence_url'])
        if row['dataset_id'] in ids or url in urls:raise ValueError('Duplicate related record')
        ids.add(row['dataset_id']);urls.add(url);concepts.add(item['concept_id'])
    if result['status']!='results' and result['items']:raise ValueError('Invalid related status')
    return result


async def retrieve_related(session,service,plan,exclude_ids,exclude_url_hashes=()):
    """One bounded judgment across three retrievals, no additional intent call."""
    from .coverage import groups_for_plan
    from .similarity_results import select_candidates, compose_similarity_result
    from .errors import CapacityExceeded
    options=related_plans(plan)
    if not options:return empty_result()
    def primary_first():
        if service.inflight_requests:raise CapacityExceeded('Primary search takes priority')
    candidate_map={};retrievals=[];excluded=set(exclude_ids)
    batch=copy.deepcopy(options[0]['plan']);batch['needs']=[]
    # Empty per-need values mean inherit. Neutralize defaults so another need's
    # format/frequency/columns cannot leak into an unrestricted request.
    batch.update(frequency='',formats=[],fields=[])
    for number,item in enumerate(options,1):
        primary_first();target=item['plan'];need=copy.deepcopy(target['needs'][0])
        need['scope']=[{'field':key,'values':[str(v) for v in target[key]] if isinstance(target[key],list) else [target[key]],
                        'mode':target['years_mode'] if key=='years' else 'list'}
                       for key in SCOPE_FIELDS]
        for entry in need['scope']:
            if entry['values']==['']:entry['values']=[]
        for key in ('frequency','formats','fields'):need[key]=copy.deepcopy(target[key])
        batch['needs'].append(need)
        embedding=await session.tool(lambda:service.vector_tools.embed_query(target['semantic_query'],budget_seconds=session.remaining_seconds))
        primary_first()
        result=await session.tool(lambda:service.vector_tools.cosine_search(embedding,target['source_ids'],
            filter_groups=groups_for_plan(target),include_related=False,budget_seconds=session.remaining_seconds))
        retrievals.append(result['retrieval'])
        accepted=select_candidates(target,result['candidates'],service.sites.sources,limit=40,filter_related=False)
        count=0
        for row in accepted:
            if row['dataset_id'] in excluded or url_hash(row['metadata']['url']) in exclude_url_hashes:continue
            item_row=candidate_map.setdefault(row['dataset_id'],{**row,'retrieval_scores':{}})
            item_row['retrieval_scores'][number]=row['cosine']
            item_row['cosine']=max(item_row['retrieval_scores'].values());count+=1
            if count==8:break
    candidates=list(candidate_map.values())
    if not candidates:return empty_result()
    primary_first()
    batch['semantic_query']='; '.join(item['label'] for item in options)
    judged=await service.assess_relevance(session,batch['semantic_query'],batch,candidates,per_need_limit=2)
    output=empty_result();ids=set(exclude_ids);urls=set()
    for number,item in enumerate(options,1):
        matches=sorted((r for r in judged['candidates'] if r['relevance_need']==number and number in r.get('retrieval_scores',{})),
                       key=lambda r:(-r['cosine'],r['dataset_id']))
        for row in matches:
            url=url_identity(row['metadata']['url'])
            if row['dataset_id'] in ids or url in urls:continue
            single={**row,'relevance_need':1}
            value=compose_similarity_result(item['plan'],[single],service.sites.sources,
                                             {**retrievals[number-1],'relevance':judged['assessment']})
            if len(facts(value))!=1:continue
            output['items'].append({k:v for k,v in item.items() if k!='plan'}|{'result':value})
            ids.add(row['dataset_id']);urls.add(url);break
    output['status']='results' if output['items'] else 'empty'
    return validate_result(output)
