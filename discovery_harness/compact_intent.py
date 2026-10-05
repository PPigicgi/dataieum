"""Sparse typed wire format; the application's full plan contract stays intact."""
import copy

from .site_search import plan_schema, planning_instructions, validate_plan


def _edits(properties):
    # Sparse output saves generation tokens, but field/value pairing must remain
    # strict. Group only identical schemas; never pool incompatible value types.
    groups=[]
    for field,spec in properties.items():
        for fields,existing in groups:
            if existing==spec:
                fields.append(field)
                break
        else:groups.append(([field],spec))
    choices=[{'type':'object','properties':{'field':{'type':'string','enum':fields},
                'value':spec},'required':['field','value'],'additionalProperties':False}
             for fields,spec in groups]
    return {'type':'array', 'maxItems':len(properties), 'items':{'anyOf':choices}}


def compact_schema():
    full = plan_schema()['properties']
    need = full['needs']['items']['properties']
    return {'type':'object', 'properties':{
        'followup':full['followup'],
        'countries':full['countries'],
        'semantic_query':full['semantic_query'],
        'needs':{'type':'array','maxItems':5,'items':{'type':'object','properties':{
            'indicator':need['indicator'], 'set':_edits({k:v for k,v in need.items() if k!='indicator'})},
            'required':['indicator','set'],'additionalProperties':False}},
        'set':_edits({k:v for k,v in full.items() if k not in {'followup','countries','needs','semantic_query'}})},
        'required':['followup','countries','semantic_query','needs','set'],'additionalProperties':False}


def compact_instructions(sources):
    fields=plan_schema()['properties']
    enums = '\nRoot clear values: ' + ', '.join(fields['clear']['items']['enum'])
    enums += '\nSupported formats: ' + ', '.join(fields['formats']['items']['enum'])
    # Keep the full semantic/security contract and dictionaries unchanged.
    # This suffix describes only the sparse wire format; preceding rules already
    # define filters, purpose, unsupported formats, clears and followup merging.
    return planning_instructions(sources) + enums + '''

ACTIVE WIRE FORMAT: Keep all rules above. Return {followup,countries,semantic_query,needs:[{indicator,set}],set}.
Explicit shared countries go in root countries ([] if absent/scoped), even with a business purpose.
Root semantic_query is complete concise data meaning, including followups; "" only for clarification.
Neither root field goes in set. Each set lists nondefault {"field":name,"value":typed_value}; no duplicates or encoded JSON.
Defaults: strings "", arrays [], flags false, need_selection="explicit", years_mode="list", related_mode="auto".
Examples:
한국 인구수 => {"followup":false,"countries":["대한민국"],"semantic_query":"인구수 총인구 주민 수 / population count total resident population","needs":[{"indicator":"population","set":[]}],"set":[]}
안녕하세요! 기후 데이터 좀 찾아줄래? => {"followup":false,"countries":[],"semantic_query":"기후 / climate","needs":[{"indicator":"weather","set":[]}],"set":[]}
2024년만 (population followup) => {"followup":true,"countries":[],"semantic_query":"인구수 총인구 주민 수 / population count total resident population","needs":[],"set":[{"field":"years","value":[2024]}]}
CSV 제한 해제 (population followup) => {"followup":true,"countries":[],"semantic_query":"인구수 총인구 주민 수 / population count total resident population","needs":[],"set":[{"field":"clear","value":["formats"]}]}
'''


def _default(field, spec):
    if field in {'need_selection','years_mode','related_mode'}:
        return {'need_selection':'explicit','years_mode':'list','related_mode':'auto'}[field]
    return [] if spec['type']=='array' else False if spec['type']=='boolean' else ''


def _apply(target, edits, allowed):
    if not isinstance(edits,list) or len(edits)>len(allowed):
        raise ValueError('invalid compact edit list')
    seen=set()
    for edit in edits:
        if not isinstance(edit,dict) or set(edit)!={'field','value'} or not isinstance(edit['field'],str):
            raise ValueError('invalid compact edit')
        field=edit['field']
        if field not in allowed or field in seen:
            raise ValueError('unknown or duplicate compact field')
        seen.add(field);target[field]=copy.deepcopy(edit['value'])


def decode_intent(wire):
    if not isinstance(wire,dict) or set(wire) not in ({'followup','countries','needs','set'}, {'followup','countries','semantic_query','needs','set'}) or type(wire['followup']) is not bool:
        raise ValueError('invalid compact intent')
    full=plan_schema()['properties'];need_schema=full['needs']['items']['properties']
    if not isinstance(wire['needs'],list) or len(wire['needs'])>5:
        raise ValueError('invalid compact needs')
    result={field:_default(field,spec) for field,spec in full.items()}
    result['followup']=wire['followup']
    result['countries']=copy.deepcopy(wire['countries'])
    _apply(result,wire['set'],set(full)-{'needs','followup','countries'}-({'semantic_query'} if 'semantic_query' in wire else set()))
    if 'semantic_query' in wire:result['semantic_query']=wire['semantic_query']
    for entry in wire['needs']:
        if not isinstance(entry,dict) or set(entry)!={'indicator','set'}:
            raise ValueError('invalid compact need')
        need={field:_default(field,spec) for field,spec in need_schema.items()}
        need['indicator']=entry['indicator']
        _apply(need,entry['set'],set(need_schema)-{'indicator'})
        result['needs'].append(need)
    return validate_plan(result)
