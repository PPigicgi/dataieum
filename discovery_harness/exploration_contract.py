"""Small public projection of stored-vector exploration; never model-authored."""
import math
import re


def validate_exploration(value):
    if not isinstance(value,dict) or value.get('status') not in {'ready','unavailable','no_match'}:
        raise ValueError('invalid exploration status')
    def text(v,limit):
        if not isinstance(v,str) or not v.strip() or len(v)>limit or any(ord(c)<32 for c in v):
            raise ValueError('invalid exploration text')
        return v
    def score(v):
        if type(v) not in (int,float) or not math.isfinite(v) or not -1<=v<=1:
            raise ValueError('invalid exploration cosine')
        return v
    result={'status':value['status'],'version':value.get('version'),
            'query_topics':[],'related_topics':[],'relations':[]}
    if result['version'] is not None:text(result['version'],100)
    seen=set()
    for key,limit in (('query_topics',1),('related_topics',2)):
        items=value.get(key,[])
        if not isinstance(items,list) or len(items)>limit:raise ValueError('topic count')
        for item in items:
            if not isinstance(item,dict):raise ValueError('invalid topic')
            identifier=text(item.get('id'),7)
            if not re.fullmatch(r'M\d{2}-S\d{2}',identifier) or identifier in seen:raise ValueError('topic identity')
            parent=item.get('parent_id')
            if parent!=identifier[:3]:raise ValueError('topic parent')
            topic={'id':identifier,'label':text(item.get('label'),160),
                'definition':text(item.get('definition'),600),'parent_id':parent}
            if key=='query_topics':topic['query_cosine']=score(item.get('query_cosine'))
            else:
                topic['via_topic_id']=item.get('via_topic_id')
                if topic['via_topic_id'] not in {t['id'] for t in result['query_topics']}:raise ValueError('topic path')
                topic['topic_cosine']=score(item.get('topic_cosine'))
            seen.add(identifier);result[key].append(topic)
    relations=value.get('relations',[])
    if not isinstance(relations,list) or len(relations)>2:raise ValueError('relation count')
    targets=set()
    for item in relations:
        if not isinstance(item,dict):raise ValueError('invalid relation')
        related=next((t for t in result['related_topics'] if t['id']==item.get('target')),None)
        if (related is None or related['id'] in targets or item.get('source')!=related['via_topic_id'] or
            item.get('relation')!='semantic_similarity' or item.get('basis')!='stored_topic_vector_cosine' or
            score(item.get('cosine'))!=related['topic_cosine']):raise ValueError('relation evidence')
        targets.add(related['id'])
        result['relations'].append({k:item[k] for k in ('source','target','relation','basis','cosine')})
    if targets!={t['id'] for t in result['related_topics']}:raise ValueError('missing relation')
    if result['status']=='ready':
        if not result['query_topics'] or not result['version']:raise ValueError('missing query concept')
    elif seen or relations:raise ValueError('unavailable relations')
    return result
