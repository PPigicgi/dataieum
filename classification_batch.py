"""Classify unlinked metadata in a bounded batch, retaining evidence and concurrent edits."""
import collections,json,os,time,fcntl
from contextlib import nullcontext
from pathlib import Path
from catalog import database,dump,suggestions,subject_mappings,now
ROOT=Path(os.environ.get('DB_PATH','catalog.sqlite3')).parent/'collection-audit'

def plan():
    counts=collections.Counter();started=time.time();cursor=0;last_ids=[]
    for name,key in [('classification-proposals.jsonl','classified'),('classification-unresolved.jsonl','unresolved')]:
        path=ROOT/name
        if path.exists():
            last=None
            with path.open() as journal:
                for line in journal:
                    if not line.endswith(chr(10)):raise ValueError('Incomplete classification journal')
                    counts[key]+=1;last=line
            if last:last_ids.append(json.loads(last)['id'])
    counts['examined']=counts['classified']+counts['unresolved']
    with database() as db:
        for id in last_ids:
            found=db.execute('SELECT rowid FROM datasets WHERE id=?',(id,)).fetchone()
            if not found:raise ValueError('Source record removed during classification')
            cursor=max(cursor,found[0])
    number=cursor
    with database() as db, (ROOT/'classification-proposals.jsonl').open('a') as output, (ROOT/'classification-unresolved.jsonl').open('a') as unresolved:
        for number,row in enumerate(db.execute('SELECT rowid,id,source_id,title,description,metadata,mappings,fingerprint FROM datasets WHERE rowid>? ORDER BY rowid',(cursor,)),cursor+1):
            previous=json.loads(row['mappings'])
            if any(m.get('status')!='rejected' and m.get('concept_id')!='unclassified' for m in previous):
                continue
            counts['examined']+=1
            meta=json.loads(row['metadata'])
            subjects=list(meta.get('subjects',[]))
            if meta.get('native_catalog_path'):
                subjects.append({'kind':'resource_title','label':meta['native_catalog_path']})
            rejected={m['concept_id'] for m in previous if m.get('status')=='rejected'}
            # EU catalogue themes are explicit provider classifications. Use them directly
            # instead of rescanning millions of translated municipal table titles.
            native=subject_mappings([s for s in subjects if s.get('kind')=='theme'],source_id=row['source_id'],publisher=meta.get('publisher',''),native_catalog=meta.get('native_catalog')) if row['source_id']=='eu' else []
            matches=[m for m in (native or suggestions(row['title'],row['description'],subjects=subjects,source_id=row['source_id'],publisher=meta.get('publisher',''),native_catalog=meta.get('native_catalog'),access_paths=meta.get('access_paths',[]),resource_format=meta.get('format',''))) if m['concept_id'] not in rejected]
            if matches:
                matches=[{**m,'status':'classified','classified_at':now()} for m in matches]
                matches += [m for m in previous if m.get('status')=='rejected']
                output.write(dump({'id':row['id'],'fingerprint':row['fingerprint'],'previous':row['mappings'],'mappings':matches})+'\n')
                counts['classified']+=1
            else:
                unresolved.write(dump({'id':row['id'],'title':row['title'],'description':row['description'][:4000],'source_id':row['source_id'],'publisher':meta.get('publisher'),'subjects':subjects,'native_catalog':meta.get('native_catalog'),'access_paths':meta.get('access_paths',[]),'format':meta.get('format',''),'fingerprint':row['fingerprint'],'previous':row['mappings']})+'\n')
                counts['unresolved']+=1
            if counts['examined']%10000==0:
                output.flush();unresolved.flush()
                report={'scanned':number,**counts,'seconds':round(time.time()-started)}
                (ROOT/'classification-progress.json').write_text(dump(report));print(dump(report),flush=True)
    report={'status':'planned','scanned':number,**counts,'seconds':round(time.time()-started)}
    (ROOT/'classification-progress.json').write_text(dump(report));print(dump(report),flush=True)


def journal_items(stream,path):
    # Apply sealed journals only; reject a cut record instead of losing its bytes.
    for line in stream:
        if not line.endswith(b'\n'):
            raise ValueError('Incomplete classification record: '+Path(path).name)
        yield line.decode('utf-8')


def apply(paths, update_guard=None):
    import re
    counts=collections.Counter()
    retired_terms={'representations','erp','신뢰','과실','법무','수입액'}
    extra_subjects=json.loads((ROOT/'eurostat-native-subjects.json').read_text()) if (ROOT/'eurostat-native-subjects.json').exists() else {}
    with (nullcontext(update_guard) if update_guard is not None else (ROOT.parent/'catalog-update.lock').open('a+')) as update_guard, database() as db, (ROOT/'classification-safety-unresolved.jsonl').open('a') as safety, (ROOT/'classification-apply-conflicts.jsonl').open('a') as conflicts:
        fcntl.flock(update_guard,fcntl.LOCK_EX)
        db.execute('PRAGMA cache_size=-524288')
        db.execute('PRAGMA synchronous=NORMAL')
        db.execute('PRAGMA wal_autocheckpoint=0')
        db.execute('PRAGMA busy_timeout=60000')
        for path_index,path in enumerate(paths):
            with Path(path).open('rb') as stream:
                skipped=int(os.environ.get('CLASSIFICATION_SKIP_COMMITTED','0')) if path_index==0 else 0
                for _ in range(skipped):
                    if not stream.readline():raise ValueError('Committed checkpoint exceeds journal')
                counts['previously_committed']+=skipped
                for line in journal_items(stream,path):
                    item=json.loads(line)
                    needs_recheck=any(m.get('method')=='keyword' and (match:=re.search('후 “([^”]+)”',m.get('evidence',''))) and match[1].casefold() in retired_terms for m in item['mappings'])
                    unresolved=None
                    if needs_recheck:
                        row=db.execute('SELECT metadata,mappings,fingerprint FROM datasets WHERE id=?',(item['id'],)).fetchone()
                        if row and row['fingerprint']==item['fingerprint']:
                            meta=json.loads(row['metadata']);subjects=list(meta.get('subjects',[]))
                            if meta.get('native_catalog_path'):subjects.append({'kind':'resource_title','label':meta['native_catalog_path']})
                            if meta['source_id']=='eurostat':subjects+=extra_subjects.get(meta['external_id'],[])
                            prior=json.loads(item['previous']);rejected={m['concept_id'] for m in prior if m.get('status')=='rejected'}
                            mappings=[{**m,'status':'classified','classified_at':now()} for m in suggestions(meta['title'],meta.get('description',''),subjects=subjects,source_id=meta['source_id'],publisher=meta.get('publisher',''),native_catalog=meta.get('native_catalog'),access_paths=meta.get('access_paths',[]),resource_format=meta.get('format','')) if m['concept_id'] not in rejected]
                            if not mappings:
                                unresolved={'id':item['id'],'title':meta['title'],'description':meta.get('description',''),'subjects':subjects,
                                            'reason':'중의적인 단어를 제외한 뒤 확인 가능한 분류 근거를 찾지 못함.'}
                                mappings=[{'concept_id':'unclassified','status':'unresolved','method':'insufficient_evidence','evidence':unresolved['reason']}]
                            item['mappings']=mappings+[m for m in prior if m.get('status')=='rejected']
                            counts['recomputed_ambiguous']+=1
                    for mapping in item['mappings']:
                        if mapping.get('method') in {'metadata_semantic_model','verified_native_context','verified_same_record'} and mapping.get('status')=='classified':
                            mapping.setdefault('evidence_fingerprint',item['fingerprint'])
                    updated=dump(item['mappings'])
                    changed=db.execute('UPDATE datasets SET mappings=? WHERE id=? AND fingerprint=? AND mappings=?',(updated,item['id'],item['fingerprint'],item['previous'])).rowcount
                    if changed:
                        if item.get('extra_subjects'):
                            metadata=json.loads(db.execute('SELECT metadata FROM datasets WHERE id=?',(item['id'],)).fetchone()[0])
                            subjects=metadata.get('subjects',[])
                            seen={dump(x) for x in subjects}
                            for extra in item['extra_subjects']:
                                key=dump(extra)
                                if key not in seen:subjects.append(extra);seen.add(key)
                            db.execute("UPDATE datasets SET metadata=json_set(metadata,'$.subjects',json(?)) WHERE id=?",(dump(subjects),item['id']))
                        counts['applied']+=1
                        if unresolved:safety.write(dump(unresolved)+'\n');counts['unresolved_after_safety']+=1
                    else:
                        current=db.execute('SELECT fingerprint,mappings FROM datasets WHERE id=?',(item['id'],)).fetchone()
                        equivalent=lambda value:[{k:v for k,v in m.items() if k!='classified_at'} for m in json.loads(value)]
                        already=current and current[0]==item['fingerprint'] and equivalent(current[1])==equivalent(updated)
                        counts['already_applied' if already else 'concurrent_conflicts']+=1
                        if not already:conflicts.write(dump({'id':item['id'],'expected_fingerprint':item['fingerprint'],'expected_mappings':item['mappings'],'current':dict(current) if current else None})+chr(10))
                    counts['processed']+=1
                    if counts['processed']%5000==0:
                        db.commit();safety.flush();conflicts.flush();time.sleep(0.02)
                    if counts['processed']%50000==0:
                        db.execute('PRAGMA wal_checkpoint(PASSIVE)')
                        report={'checked_at':now(),**counts};(ROOT/'classification-applied.json').write_text(dump(report));print(dump(report),flush=True)
        db.commit()
    (ROOT/'classification-applied.json').write_text(dump({'checked_at':now(),**counts}));print(dump(dict(counts)),flush=True)


def inherit_verified(update_guard=None):
    counts=collections.Counter()
    with (nullcontext(update_guard) if update_guard is not None else (ROOT.parent/'catalog-update.lock').open('a+')) as update_guard, database() as db, (ROOT/'classification-inherited.jsonl').open('a') as journal, (ROOT/'classification-inheritance-conflicts.jsonl').open('w') as unresolved:
        fcntl.flock(update_guard,fcntl.LOCK_EX)
        db.execute('PRAGMA busy_timeout=60000')
        db.execute('PRAGMA cache_size=-524288')
        db.execute('PRAGMA synchronous=NORMAL')
        with (ROOT/'duplicate-groups.jsonl').open() as groups:
            for line in groups:
                group=json.loads(line);members={}
                for id in group['ids']:
                    row=db.execute("SELECT mappings,fingerprint,json_extract(metadata,'$.duplicate_of') AS alias FROM datasets WHERE id=?",(id,)).fetchone()
                    if row:members[id]=row
                canonical=group['canonical']
                if canonical not in members:raise ValueError('Missing duplicate canonical')
                for id,row in members.items():
                    if (id==canonical and row['alias']) or (id!=canonical and row['alias']!=canonical):
                        raise ValueError('Duplicate group changed: '+id)
                known={};specific={}
                for id,row in members.items():
                    maps=[m for m in json.loads(row['mappings']) if m.get('status')!='rejected' and m.get('concept_id')!='unclassified']
                    if maps:known[id]=maps
                    precise=[m for m in maps if m['concept_id'] not in {'spatial_services','research_data'} and m.get('method')!='statistical_catalog_scope']
                    if precise:specific[id]=precise
                donors=specific or known
                if not donors:continue
                donor=canonical if canonical in donors else next(iter(donors))
                if canonical not in donors and len({tuple(sorted(m['concept_id'] for m in ms)) for ms in donors.values()})>1:
                    counts['conflicting_source_groups']+=1
                    unresolved.write(dump({'canonical':canonical,'known_topics':{id:[m['concept_id'] for m in ms] for id,ms in donors.items()}})+'\n')
                    continue
                for id,row in members.items():
                    if id in known and not (specific and id not in specific):continue
                    prior=json.loads(row['mappings'])
                    if any(m.get('status')=='approved' or m.get('method')=='manual' or m.get('reviewed_at') for m in prior if m.get('status')!='rejected' and m.get('concept_id')!='unclassified'):
                        counts['preserved_reviews']+=1;continue
                    rejected={m['concept_id'] for m in prior if m.get('status')=='rejected'}
                    mappings=[{'concept_id':m['concept_id'],'status':'classified','classified_at':now(),
                               'method':'verified_same_record','evidence_fingerprint':row['fingerprint'],
                               'evidence':'원 출처 ID·버전·접근 경로로 동일 자료임을 검증: '+donor+' — '+m.get('evidence',''),
                               'evidence_record_id':donor} for m in donors[donor] if m['concept_id'] not in rejected]
                    if not mappings:continue
                    mappings += [m for m in prior if m.get('status')=='rejected']
                    updated=db.execute('UPDATE datasets SET mappings=? WHERE id=? AND fingerprint=? AND mappings=?',
                                       (dump(mappings),id,row['fingerprint'],row['mappings'])).rowcount
                    if updated:
                        journal.write(dump({'id':id,'donor':donor,'mappings':mappings})+'\n')
                        if id in known:counts['upgraded_type_only']+=1
                    counts['applied']+=updated;counts['conflicts']+=1-updated
                counts['groups_checked']+=1
                if counts['groups_checked']%1000==0:db.commit();journal.flush()
        db.commit()
    (ROOT/'classification-inheritance.json').write_text(dump({'checked_at':now(),**counts}));print(dump(counts),flush=True)


if __name__=='__main__':
    import sys
    args=sys.argv[1:]
    if args==['plan']:plan()
    elif args==['inherit']:inherit_verified()
    elif args and args[0]=='apply' and len(args)>1:apply(args[1:])
    else:raise SystemExit('Usage: classification_batch.py plan | inherit | apply JOURNAL...')
