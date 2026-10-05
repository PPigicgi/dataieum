"""Apply a prepared delta during maintenance, with durable per-record undo and no paid calls."""
from collections import Counter
from contextlib import closing
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import shutil
import sys
import time

import numpy as np
import faiss
from discovery_harness import keyword_index as keyword
from discovery_harness import reference_years
from discovery_harness import coverage, coverage_index
from discovery_harness.vector_index import _identity, _sha, _json, normalized, project_metadata, VectorIndex


def checkpoint(phase):
    # Test hook; production failures are recovered using the same persisted undo.
    pass


def sync_file(path):
    with Path(path).open('rb') as f:os.fsync(f.fileno())


def write_json(path,value):
    path=Path(path);tmp=path.with_suffix('.tmp')
    tmp.write_text(_json(value));sync_file(tmp);tmp.replace(path)
    fd=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def database(path, *, readonly=False):
    path=Path(path).resolve()
    db=sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True) if readonly else sqlite3.connect(path)
    db.row_factory=sqlite3.Row
    if not readonly:
        db.execute('PRAGMA journal_mode=DELETE')
        db.execute('PRAGMA synchronous=EXTRA')
        db.execute('PRAGMA cache_size=-16384')
    return db


def copy(source,target):
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():raise FileExistsError(target)
    if _identity(source)['wal']:raise ValueError('Cannot copy a source with an active WAL')
    subprocess.run(['cp','--sparse=always','--preserve=timestamps',str(source),str(target)],check=True)


def coverage_entry(raw,title,description):
    meta=project_metadata(raw,title=title,description=description)
    if meta is None:return None
    facts=coverage.profile(meta)
    text=coverage.normal(facts['geo_text']+'\n'+'\n'.join(facts['regions']))
    return (hashlib.sha256(_json(meta).encode()).hexdigest(),
            _json(sorted(k for k in meta if k not in {'url','publisher'})),
            ' '.join(coverage_index._word(v) for v in facts['countries']),text.translate(keyword.TOKENS),
            ' '.join('y'+str(v) for v in facts['years']))


def change_counts(db,record,links,primary,sign):
    if record is None:return
    band=record['band']
    keys=Counter({(band,'',''):sign})
    for main in {r['main_id'] for r in links}:keys[(band,main,'')]+=sign
    for r in links:keys[(band,r['main_id'],r['topic'])]+=sign
    primary_keys=Counter({(band,'',''):sign})
    if primary:
        primary_keys[(band,primary['main_id'],'')]+=sign
        primary_keys[(band,primary['main_id'],primary['topic'])]+=sign
    for table,changes in [('counts',keys),('primary_counts',primary_keys)]:
        for key,delta in changes.items():
            n=db.execute(f'UPDATE {table} SET n=n+? WHERE band=? AND main_id=? AND topic=?',(delta,*key)).rowcount
            if n==0:db.execute(f'INSERT INTO {table} VALUES(?,?,?,?)',(*key,delta))


def classify(db,ident,title,source,scores,topics,thresholds):
    old=db.execute('SELECT * FROM records WHERE id=?',(ident,)).fetchone()
    prior=db.execute('SELECT l.*,t.main_id FROM links l JOIN topics t ON t.id=l.topic WHERE dataset_id=?',(ident,)).fetchall()
    first=db.execute('SELECT l.*,t.main_id FROM primary_links l JOIN topics t ON t.id=l.topic WHERE dataset_id=?',(ident,)).fetchone()
    change_counts(db,old,prior,first,-1)
    top=float(scores.max());band='high' if top>=thresholds['high'] else 'low' if top>=thresholds['low'] else 'unclassified'
    banned={r[0] for r in db.execute('SELECT topic FROM membership_corrections WHERE dataset_id=?',(ident,))}
    held=db.execute('SELECT 1 FROM classification_holds WHERE dataset_id=?',(ident,)).fetchone()
    selected=[]
    if band!='unclassified' and not held:
        for i in np.flatnonzero((scores>=thresholds[band]) & (scores>=top*thresholds['relative'])):
            if topics[i] not in banned:
                score=float(scores[i]);alpha=min(1.,.7+.3*(score-.25)/.25) if band=='high' else min(.65,max(.3,.3+.35*(score-.15)/.1))
                selected.append((ident,topics[i],band,score,alpha))
    if not selected:band='unclassified'
    db.execute('DELETE FROM links WHERE dataset_id=?',(ident,));db.execute('DELETE FROM primary_links WHERE dataset_id=?',(ident,))
    db.execute('INSERT OR REPLACE INTO records VALUES(?,?,?,?,?)',(ident,band,top,source,title))
    db.executemany('INSERT INTO links VALUES(?,?,?,?,?)',selected)
    if selected:db.execute('INSERT INTO primary_links VALUES(?,?,?,?,?)',min(selected,key=lambda r:(-r[3],r[1])))
    current=db.execute('SELECT * FROM records WHERE id=?',(ident,)).fetchone()
    links=db.execute('SELECT l.*,t.main_id FROM links l JOIN topics t ON t.id=l.topic WHERE dataset_id=?',(ident,)).fetchall()
    first=db.execute('SELECT l.*,t.main_id FROM primary_links l JOIN topics t ON t.id=l.topic WHERE dataset_id=?',(ident,)).fetchone()
    change_counts(db,current,links,first,1)


def apply_catalog_and_topics(base,root,delta,topic_vectors):
    manifest=json.loads((Path(base['topics'])/'manifest.json').read_text())
    plan=json.loads((root/'classification-plan.json').read_text())
    if plan['topic_vectors_sha256']!=_sha(topic_vectors):raise ValueError('Topic vectors changed after preparation')
    with closing(database(root/'catalog.sqlite3')) as cat, closing(database(root/'undo.sqlite3',readonly=True)) as old, \
         closing(database(root/'catalog-search.sqlite3')) as search,closing(database(root/'topics/confidence.sqlite3')) as graph:
        search.execute('CREATE TABLE IF NOT EXISTS refresh_applied(id TEXT PRIMARY KEY,label INTEGER)')
        search.commit()
        topics=[r[0] for r in graph.execute('SELECT id FROM topics ORDER BY id')]
        if plan['topics']!=topics:raise ValueError('Topic definitions changed after preparation')
        counts=Counter(dict(graph.execute('SELECT source,count(*) FROM records GROUP BY source')))
        new=0;processed=0
        for row in delta.execute('SELECT * FROM changes ORDER BY dataset_id'):
            m=json.loads(row['metadata']);ident=row['dataset_id']
            before=old.execute('SELECT original_rowid AS n,* FROM datasets WHERE id=?',(ident,)).fetchone()
            if before:
                doc=search.execute('SELECT n FROM documents WHERE id=?',(ident,)).fetchone()
                if not doc or doc[0]!=before['n']:raise ValueError('Old keyword identity differs')
                previous=keyword.search_text(before)
                search.execute("INSERT INTO terms(terms,rowid,text) VALUES('delete',?,?)",(doc[0],previous.translate(keyword.TOKENS)))
                search.execute("INSERT INTO phrases(phrases,rowid,text) VALUES('delete',?,?)",(doc[0],previous.replace('\x00','\x01')))
                search.execute('DELETE FROM documents WHERE n=?',(doc[0],))
                cat.execute('UPDATE datasets SET title=?,description=?,metadata=?,fingerprint=?,checked_at=? WHERE id=?',
                    (m['title'],m.get('description',''),row['metadata'],m['fingerprint'],m['checked_at'],ident))
            else:
                cat.execute('INSERT INTO datasets VALUES(?,?,?,?,?,?,?,?)',
                    (ident,row['source'],m['title'],m.get('description',''),row['metadata'],'[]',m['fingerprint'],m['checked_at']))
                new+=1
            after=cat.execute('SELECT rowid AS n,* FROM datasets WHERE id=?',(ident,)).fetchone()
            text=keyword.search_text(after)
            search.execute('INSERT INTO documents VALUES(?,?)',(after['n'],ident))
            search.execute('INSERT INTO terms(rowid,text) VALUES(?,?)',(after['n'],text.translate(keyword.TOKENS)))
            search.execute('INSERT INTO phrases(rowid,text) VALUES(?,?)',(after['n'],text.replace('\x00','\x01')))
            reference_years.replace(search,after['n'],after)
            if not graph.execute('SELECT 1 FROM records WHERE id=?',(ident,)).fetchone():counts[row['source']]+=1
            planned=delta.execute('SELECT scores FROM classifications WHERE dataset_id=?',(ident,)).fetchone()
            if not planned:raise ValueError('A changed record has no prepared classification')
            scores=np.frombuffer(planned[0],dtype=np.float32)
            if len(scores)!=len(topics) or not np.isfinite(scores).all():raise ValueError('Invalid classification scores')
            classify(graph,ident,m['title'],row['source'],scores,topics,manifest['thresholds'])
            search.execute('INSERT INTO refresh_applied(id) VALUES(?)',(ident,))
            processed+=1
            if processed%500==0:
                cat.commit();checkpoint('catalog');search.commit();checkpoint('keyword');graph.commit();checkpoint('graph')
                print(_json({'phase':'catalog_topics','records':processed}),flush=True)
        for source in delta.execute('SELECT DISTINCT source FROM changes'):
            cat.execute("UPDATE sources SET last_sync=?,last_error='' WHERE id=?",(time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),source[0]))
        graph.execute("UPDATE state SET value=? WHERE key='source_counts'",(_json(dict(counts)),))
        graph.execute("UPDATE state SET value=? WHERE key='count'",(str(sum(counts.values())),))
        cat.commit();checkpoint('catalog');graph.commit();checkpoint('graph')
        n=cat.execute('SELECT count(*) FROM datasets').fetchone()[0]
        search.execute('UPDATE reference_year_state SET records=?',(n,))
        search.execute('UPDATE manifest SET value=?',(_json({'source':keyword.signature(root/'catalog.sqlite3'),'records':n}),));search.commit();checkpoint('keyword')
        graph_counts={b:0 for b in ('high','low','unclassified')}
        graph_counts.update(dict(graph.execute('SELECT band,count(*) FROM records GROUP BY band')))
        graph_counts.update(total=sum(graph_counts.values()),links=graph.execute('SELECT count(*) FROM links').fetchone()[0])
        for table in ['counts','primary_counts']:
            if graph.execute(f'SELECT 1 FROM {table} WHERE n<0 LIMIT 1').fetchone():raise ValueError('Negative graph count')
            if dict(graph.execute(f"SELECT band,n FROM {table} WHERE main_id='' AND topic=''"))!={b:graph_counts[b] for b in ('high','low','unclassified')}:
                raise ValueError('Graph aggregates differ')
        if graph.execute("SELECT sum(n) FROM counts WHERE topic!=''").fetchone()[0]!=graph_counts['links']:raise ValueError('Link aggregates differ')
        manifest.update(counts=graph_counts,source='daily incremental publication',source_mtime_ns=(root/'catalog.sqlite3').stat().st_mtime_ns)
    manifest.update(bytes=(root/'topics/confidence.sqlite3').stat().st_size,sha256=_sha(root/'topics/confidence.sqlite3'))
    write_json(root/'topics/manifest.json',manifest)
    return {'records':processed,'new_records':new,'catalog_records':n,'graph':graph_counts}


def apply_vectors(base,root,delta):
    olddir=Path(base['index'])/Path(base['index'],'CURRENT').read_text().strip()
    target=olddir;gen=target.name
    oldmanifest=json.loads((olddir/'manifest.json').read_text())
    # The trained quantizer is reused; updated IDs replace their old vector slots.
    index=faiss.read_index(str(olddir/'vectors.faiss'));faiss.omp_set_num_threads(1)
    codes=np.load(olddir/'sources.npy').tolist();active=np.load(olddir/'active.npy').tolist() if (olddir/'active.npy').exists() else [True]*index.ntotal
    sources=list(oldmanifest['sources']);source_codes={s:i for i,s in enumerate(sources)}
    with closing(database(target/'mapping.sqlite3')) as mapping,closing(database(target/'updates.sqlite3')) as updates, \
         closing(database(root/'coverage.sqlite3')) as scope,closing(database(root/'undo.sqlite3',readonly=True)) as oldcat:
        updates.execute('CREATE TABLE IF NOT EXISTS embeddings(dataset_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,input_hash TEXT NOT NULL,vector BLOB NOT NULL)')
        mapping.execute('CREATE INDEX IF NOT EXISTS records_dataset_id ON records(dataset_id)')
        scope.execute('CREATE TABLE IF NOT EXISTS refresh_applied(id TEXT PRIMARY KEY,label INTEGER)')
        scope.commit()
        delta.execute('DROP TABLE IF EXISTS labels')
        delta.execute('CREATE TABLE labels(dataset_id TEXT PRIMARY KEY,label INTEGER,reindex INTEGER)')
        removed=[];next_label=index.ntotal;processed=0
        for row in delta.execute('SELECT * FROM changes ORDER BY dataset_id'):
            ident=row['dataset_id'];m=json.loads(row['metadata'])
            originals=mapping.execute('SELECT * FROM records WHERE dataset_id=?',(ident,)).fetchall()
            if len(originals)>1:raise ValueError('Duplicate active vector identity')
            prior=originals[0] if originals else None
            label=prior['label'] if prior else next_label
            if prior is None:next_label+=1
            if row['source'] not in source_codes:
                source_codes[row['source']]=len(sources);sources.append(row['source'])
            reindex=prior is None or prior['input_hash']!=row['input_hash']
            if reindex:
                if prior:removed.append(label)
                updates.execute('INSERT INTO embeddings VALUES(?,?,?,?) ON CONFLICT(dataset_id) DO UPDATE SET source_id=excluded.source_id,input_hash=excluded.input_hash,vector=excluded.vector',
                    (ident,row['source'],row['input_hash'],row['vector']))
                rid=updates.execute('SELECT rowid FROM embeddings WHERE dataset_id=?',(ident,)).fetchone()[0]
                mapping.execute('INSERT OR REPLACE INTO records VALUES(?,?,?,?,?)',(label,-rid,ident,row['source'],row['input_hash']))
            if prior is None:codes.append(source_codes[row['source']]);active.append(True)
            else:codes[label]=source_codes[row['source']];active[label]=True
            existing=scope.execute('SELECT * FROM records WHERE label=?',(label,)).fetchone()
            if existing:
                before=oldcat.execute('SELECT title,description,metadata FROM datasets WHERE id=?',(ident,)).fetchone()
                entry=coverage_entry(before['metadata'],before['title'],before['description']) if before else None
                if not entry or entry[0]!=existing['metadata_sha256']:raise ValueError('Old coverage metadata differs')
                scope.execute("INSERT INTO scope(scope,rowid,countries,regions,years) VALUES('delete',?,?,?,?)",(label,*entry[2:]))
                scope.execute('DELETE FROM records WHERE label=?',(label,))
            entry=coverage_entry(row['metadata'],m['title'],m.get('description',''))
            if entry:
                scope.execute('INSERT INTO records VALUES(?,?,?,?)',(label,ident,*entry[:2]))
                scope.execute('INSERT INTO scope(rowid,countries,regions,years) VALUES(?,?,?,?)',(label,*entry[2:]))
            delta.execute('INSERT INTO labels VALUES(?,?,?)',(ident,label,int(reindex)))
            scope.execute('INSERT INTO refresh_applied VALUES(?,?)',(ident,label))
            processed+=1
            if processed%500==0:
                updates.commit();checkpoint('updates');mapping.commit();checkpoint('mapping');scope.commit();checkpoint('coverage');delta.commit()
        updates.commit();checkpoint('updates');mapping.commit();checkpoint('mapping');scope.commit();checkpoint('coverage');delta.commit()
        if not oldmanifest['approximate'] and not isinstance(index,faiss.IndexIDMap2):
            old=index;index=faiss.IndexIDMap2(faiss.IndexFlatIP(old.d))
            index.add_with_ids(old.reconstruct_n(0,old.ntotal),np.arange(old.ntotal,dtype=np.int64))
        if removed and index.remove_ids(np.asarray(removed,dtype=np.int64))!=len(removed):raise ValueError('Not all old vector slots removed')
        cursor=delta.execute('SELECT c.vector,l.label FROM changes c JOIN labels l USING(dataset_id) WHERE l.reindex=1 ORDER BY l.label')
        while rows:=cursor.fetchmany(256):
            matrix=np.stack([normalized(r['vector'],from_blob=True) for r in rows]);labels=np.asarray([r['label'] for r in rows],dtype=np.int64)
            index.add_with_ids(matrix,labels)
        if index.ntotal!=len(codes):raise ValueError('Vector slots differ')
        faiss.write_index(index,str(target/'vectors.faiss'));sync_file(target/'vectors.faiss');checkpoint('faiss')
        np.save(target/'sources.npy',np.asarray(codes,dtype=np.uint16));np.save(target/'active.npy',np.asarray(active,dtype=bool))
        sync_file(target/'sources.npy');sync_file(target/'active.npy')
        active_count=mapping.execute('SELECT count(*) FROM records').fetchone()[0]
        if active_count!=sum(active):raise ValueError('Active vector mapping differs')
        counts=dict(mapping.execute('SELECT source_id,count(*) FROM records GROUP BY source_id'))
        coverage_manifest={'contract':coverage_index.CONTRACT,'source_identity':{'catalog':_identity(root/'catalog.sqlite3'),'mapping':_identity(target/'mapping.sqlite3')},
            'mapping_sha256':_sha(target/'mapping.sqlite3'),'records':active_count,'eligible_metadata':scope.execute('SELECT count(*) FROM records').fetchone()[0],'built_at':time.time_ns()}
        scope.execute('UPDATE manifest SET value=?',(_json(coverage_manifest),));scope.commit()
    manifest={**oldmanifest,'built_at':str(time.time_ns()),'indexed_vectors':index.ntotal,'active_vectors':active_count,'source_counts':counts,'sources':sources,
        'source_identity':{'catalog':_identity(root/'catalog.sqlite3'),'embeddings':_identity(base['embeddings']),'updates':_identity(target/'updates.sqlite3')},
        'refresh_parent':olddir.name,'files':{name:{'bytes':(target/name).stat().st_size,'sha256':_sha(target/name)} for name in ['vectors.faiss','mapping.sqlite3','sources.npy','active.npy','updates.sqlite3']}}
    for key in ['catalog_rebind','server_rebind','deletion_updates','reconciliation']:manifest.pop(key,None)
    with closing(database(root/'catalog.sqlite3',readonly=True)) as cat:
        manifest['catalog_signature']=list(cat.execute('SELECT count(*),max(rowid),max(checked_at) FROM datasets').fetchone())
    write_json(target/'manifest.json',manifest)
    return target


def verify(root,target,base,delta,topic_vectors):
    keyword.validate(root/'catalog.sqlite3',root/'catalog-search.sqlite3')
    with closing(database(root/'catalog.sqlite3',readonly=True)) as db:
        for alias,path in [('search',root/'catalog-search.sqlite3'),('graph',root/'topics/confidence.sqlite3'),('vectors',target/'mapping.sqlite3')]:
            db.execute(f'ATTACH DATABASE ? AS {alias}',(path.as_uri()+'?mode=ro&immutable=1',))
        if db.execute('SELECT id FROM datasets EXCEPT SELECT id FROM search.documents LIMIT 1').fetchone() or db.execute('SELECT id FROM search.documents EXCEPT SELECT id FROM datasets LIMIT 1').fetchone():raise ValueError('Full catalog/search ID sets differ')
        if db.execute('SELECT 1 FROM search.documents s JOIN datasets d ON d.id=s.id WHERE s.n!=d.rowid LIMIT 1').fetchone():raise ValueError('Search row identity differs')
        if db.execute('SELECT id FROM graph.records EXCEPT SELECT id FROM datasets LIMIT 1').fetchone():raise ValueError('Orphan graph records')
        if db.execute('SELECT dataset_id FROM vectors.records EXCEPT SELECT id FROM datasets LIMIT 1').fetchone():raise ValueError('Orphan vector records')
        for row in delta.execute('SELECT dataset_id,metadata,input_hash FROM changes'):
            if db.execute('SELECT metadata FROM datasets WHERE id=?',(row['dataset_id'],)).fetchone()[0]!=row['metadata']:raise ValueError('Published metadata differs')
            if not db.execute('SELECT 1 FROM graph.records WHERE id=?',(row['dataset_id'],)).fetchone():raise ValueError('Missing changed graph record')
            if db.execute('SELECT input_hash FROM vectors.records WHERE dataset_id=?',(row['dataset_id'],)).fetchone()[0]!=row['input_hash']:raise ValueError('Stale changed vector')
    service=VectorIndex(base['embeddings'],root/'catalog.sqlite3',root/'index',threads=1,topic_vectors=topic_vectors,topic_graph=root/'topics/confidence.sqlite3',coverage_index=root/'coverage.sqlite3')
    row=delta.execute('SELECT vector,source FROM changes ORDER BY dataset_id LIMIT 1').fetchone()
    # One existing real search flow, using a stored vector: no paid API call.
    result=service.search(normalized(row['vector'],from_blob=True),source_ids=[row['source']],timeout=10,limit=3)
    if not result['candidates']:raise ValueError('Published vector search returned no candidates')
    return {'all_changed_records_verified':delta.execute('SELECT count(*) FROM changes').fetchone()[0],
            'full_catalog_search_id_sets_equal':True,'orphan_graph_or_vector_ids':0,'vector_search_returned':len(result['candidates'])}


def live_links(base,root):
    for name,key in [('catalog.sqlite3','catalog'),('catalog-search.sqlite3','keyword'),('topics','topics'),('index','index'),('coverage.sqlite3','coverage')]:
        link=root/name
        if not link.exists():link.symlink_to(Path(base[key]).resolve())
        if link.resolve()!=Path(base[key]).resolve():raise ValueError('Publication target differs')


def prepare_undo(base,root):
    live_links(base,root)
    keyword.validate(base['catalog'],base['keyword'])
    gen=Path(base['index'])/Path(base['index'],'CURRENT').read_text().strip()
    original=json.loads((gen/'manifest.json').read_text())
    identities={k:_identity(base[k]) for k in ['catalog','embeddings']}
    if 'updates' in original['source_identity']:identities['updates']=_identity(gen/'updates.sqlite3')
    if original['source_identity']!=identities:raise ValueError('Base vector inputs changed')
    with closing(database(root/'delta.sqlite3',readonly=True)) as delta,closing(database(root/'undo.sqlite3')) as undo:
        undo.execute('ATTACH DATABASE ? AS delta',((root/'delta.sqlite3').resolve().as_uri()+'?mode=ro&immutable=1',))
        paths={'cat':base['catalog'],'search':base['keyword'],'graph':Path(base['topics'])/'confidence.sqlite3','mapping':gen/'mapping.sqlite3','coverage':base['coverage']}
        if (gen/'updates.sqlite3').exists():paths['updates']=gen/'updates.sqlite3'
        for alias,path in paths.items():
            if _identity(path)['wal']:raise ValueError('A stopped database still has an active WAL')
            undo.execute(f'ATTACH DATABASE ? AS {alias}',(Path(path).resolve().as_uri()+'?mode=ro&immutable=1',))
        undo.execute('CREATE TABLE datasets AS SELECT rowid AS original_rowid,* FROM cat.datasets WHERE 0')
        undo.execute('INSERT INTO datasets SELECT rowid,* FROM cat.datasets WHERE id IN (SELECT dataset_id FROM delta.changes)')
        undo.execute('CREATE UNIQUE INDEX undo_dataset_id ON datasets(id)')
        undo.execute('CREATE TABLE sources AS SELECT * FROM cat.sources WHERE id IN (SELECT DISTINCT source FROM delta.changes)')
        for table,key in [('records','id'),('links','dataset_id'),('primary_links','dataset_id')]:
            undo.execute(f'CREATE TABLE graph_{table} AS SELECT * FROM graph.{table} WHERE {key} IN (SELECT dataset_id FROM delta.changes)')
        for table in ['counts','primary_counts','state']:
            undo.execute(f'CREATE TABLE graph_{table} AS SELECT * FROM graph.{table}')
        undo.execute('CREATE TABLE mapping_records AS SELECT * FROM mapping.records WHERE dataset_id IN (SELECT dataset_id FROM delta.changes)')
        undo.execute('CREATE TABLE coverage_records AS SELECT * FROM coverage.records WHERE dataset_id IN (SELECT dataset_id FROM delta.changes)')
        undo.execute('CREATE UNIQUE INDEX undo_coverage_dataset_id ON coverage_records(dataset_id)')
        if 'updates' in paths:
            undo.execute('CREATE TABLE updates_embeddings AS SELECT rowid AS original_rowid,* FROM updates.embeddings WHERE dataset_id IN (SELECT dataset_id FROM delta.changes)')
        else:undo.execute('CREATE TABLE updates_embeddings(original_rowid INTEGER,dataset_id TEXT,source_id TEXT,input_hash TEXT,vector BLOB)')
        undo.commit()
        records=delta.execute('SELECT count(*) FROM changes').fetchone()[0]
    backup=root/'artifacts-before';backup.mkdir()
    for name in ['vectors.faiss','sources.npy','active.npy','manifest.json']:
        if (gen/name).exists():copy(gen/name,backup/name);sync_file(backup/name)
    copy(Path(base['topics'])/'manifest.json',backup/'topics.json');sync_file(backup/'topics.json')
    fd=os.open(backup,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)
    sync_file(root/'undo.sqlite3')
    write_json(root/'undo-ready.json',{'records':records,'generation':gen.name,'had_updates':'updates' in identities,
               'catalog_count':original.get('catalog_signature',[None])[0], 'base_identities':{k:_identity(v) for k,v in paths.items()},
               'undo_sha256':_sha(root/'undo.sqlite3'),'artifact_sha256':{p.name:_sha(p) for p in backup.iterdir() if p.is_file()},'created_at':time.time()})
    return {'records':records,'undo_bytes':(root/'undo.sqlite3').stat().st_size+sum(p.stat().st_size for p in backup.iterdir())}


def attach_undo(db,root):
    db.execute('ATTACH DATABASE ? AS undo',((root/'undo.sqlite3').resolve().as_uri()+'?mode=ro&immutable=1',))
    db.execute('ATTACH DATABASE ? AS delta',((root/'delta.sqlite3').resolve().as_uri()+'?mode=ro&immutable=1',))


def restore_rows(db,table,saved,key):
    db.execute(f'DELETE FROM {table} WHERE {key} IN (SELECT dataset_id FROM delta.changes)')
    db.execute(f'INSERT INTO {table} SELECT * FROM undo.{saved}')


def rebind(base,root):
    """Physical identities change on rollback too; bind every index to recovered data."""
    gen=Path(base['index'])/Path(base['index'],'CURRENT').read_text().strip()
    with closing(database(base['catalog'],readonly=True)) as cat,closing(database(base['keyword'])) as search:
        n=cat.execute('SELECT count(*) FROM datasets').fetchone()[0]
        search.execute('UPDATE manifest SET value=?',(_json({'source':keyword.signature(base['catalog']),'records':n}),));search.commit()
        signature=list(cat.execute('SELECT count(*),max(rowid),max(checked_at) FROM datasets').fetchone())
    with closing(database(base['coverage'])) as scope:
        m=json.loads(scope.execute('SELECT value FROM manifest').fetchone()[0])
        m.update(source_identity={'catalog':_identity(base['catalog']),'mapping':_identity(gen/'mapping.sqlite3')},mapping_sha256=_sha(gen/'mapping.sqlite3'))
        scope.execute('UPDATE manifest SET value=?',(_json(m),));scope.commit()
    m=json.loads((gen/'manifest.json').read_text());m['source_identity']={k:_identity(base[k]) for k in ['catalog','embeddings']}
    if (gen/'updates.sqlite3').exists():m['source_identity']['updates']=_identity(gen/'updates.sqlite3')
    m['catalog_signature']=signature
    for name in list(m['files']):
        if not (gen/name).exists():m['files'].pop(name);continue
        m['files'][name]={'bytes':(gen/name).stat().st_size,'sha256':_sha(gen/name)}
    write_json(gen/'manifest.json',m)
    graph=Path(base['topics'])/'confidence.sqlite3';m=json.loads((Path(base['topics'])/'manifest.json').read_text())
    m.update(bytes=graph.stat().st_size,sha256=_sha(graph),source_mtime_ns=Path(base['catalog']).stat().st_mtime_ns)
    write_json(Path(base['topics'])/'manifest.json',m)


def rollback_data(base,root):
    ready=json.loads((root/'undo-ready.json').read_text());backup=root/'artifacts-before'
    if _sha(root/'undo.sqlite3')!=ready['undo_sha256']:raise ValueError('Undo journal checksum differs')
    if any(_sha(backup/name)!=h for name,h in ready['artifact_sha256'].items()):raise ValueError('Undo artifact checksum differs')
    gen=Path(base['index'])/ready['generation']
    # FTS delete commands must use the text actually committed, never an inferred phase.
    with closing(database(base['keyword'])) as db,closing(database(root/'undo.sqlite3',readonly=True)) as old:
        attach_undo(db,root)
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='refresh_applied'").fetchone():
            rows=db.execute('SELECT c.dataset_id,c.metadata,a.label FROM delta.changes c JOIN refresh_applied a ON a.id=c.dataset_id').fetchall()
            for i,row in enumerate(rows,1):
                ident=row['dataset_id'];m=json.loads(row['metadata']);doc=db.execute('SELECT n FROM documents WHERE id=?',(ident,)).fetchone()
                if not doc:raise ValueError('Committed search entry absent during recovery')
                text=keyword.search_text({'title':m['title'],'description':m.get('description',''),'metadata':row['metadata']})
                db.execute("INSERT INTO terms(terms,rowid,text) VALUES('delete',?,?)",(doc[0],text.translate(keyword.TOKENS)))
                db.execute("INSERT INTO phrases(phrases,rowid,text) VALUES('delete',?,?)",(doc[0],text.replace('\x00','\x01')))
                db.execute('DELETE FROM documents WHERE id=?',(ident,))
                reference_years.replace(db,doc[0],None)
                before=old.execute('SELECT * FROM datasets WHERE id=?',(ident,)).fetchone()
                if before:
                    n=before['original_rowid'];text=keyword.search_text(before)
                    db.execute('INSERT INTO documents VALUES(?,?)',(n,ident))
                    db.execute('INSERT INTO terms(rowid,text) VALUES(?,?)',(n,text.translate(keyword.TOKENS)))
                    db.execute('INSERT INTO phrases(rowid,text) VALUES(?,?)',(n,text.replace('\x00','\x01')))
                    reference_years.replace(db,n,before)
                db.execute('DELETE FROM refresh_applied WHERE id=?',(ident,))
                if i%500==0:db.commit()
            db.execute('UPDATE reference_year_state SET records=(SELECT count(*) FROM documents)')
            db.commit()
    with closing(database(base['coverage'])) as db,closing(database(root/'undo.sqlite3',readonly=True)) as old:
        attach_undo(db,root)
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='refresh_applied'").fetchone():
            rows=db.execute('SELECT c.dataset_id,c.metadata,a.label FROM delta.changes c JOIN refresh_applied a ON a.id=c.dataset_id').fetchall()
            for i,row in enumerate(rows,1):
                ident=row['dataset_id'];m=json.loads(row['metadata'])
                current=db.execute('SELECT label FROM records WHERE label=?',(row['label'],)).fetchone()
                if current:
                    entry=coverage_entry(row['metadata'],m['title'],m.get('description',''))
                    if not entry:raise ValueError('Committed coverage text absent')
                    db.execute("INSERT INTO scope(scope,rowid,countries,regions,years) VALUES('delete',?,?,?,?)",(current[0],*entry[2:]))
                    db.execute('DELETE FROM records WHERE label=?',(current[0],))
                before=old.execute('SELECT * FROM coverage_records WHERE dataset_id=?',(ident,)).fetchone()
                if before:
                    m0=old.execute('SELECT * FROM datasets WHERE id=?',(ident,)).fetchone()
                    entry=coverage_entry(m0['metadata'],m0['title'],m0['description'])
                    db.execute('INSERT INTO records VALUES(?,?,?,?)',tuple(before))
                    db.execute('INSERT INTO scope(rowid,countries,regions,years) VALUES(?,?,?,?)',(before['label'],*entry[2:]))
                db.execute('DELETE FROM refresh_applied WHERE id=?',(ident,))
                if i%500==0:db.commit()
            db.commit()
    with closing(database(gen/'mapping.sqlite3')) as db:
        attach_undo(db,root);restore_rows(db,'records','mapping_records','dataset_id');db.commit()
    if ready['had_updates']:
        with closing(database(gen/'updates.sqlite3')) as db:
            attach_undo(db,root)
            db.execute('DELETE FROM embeddings WHERE dataset_id IN (SELECT dataset_id FROM delta.changes)')
            db.execute('INSERT INTO embeddings(rowid,dataset_id,source_id,input_hash,vector) SELECT * FROM undo.updates_embeddings');db.commit()
    else:
        for suffix in ['', '-journal', '-wal', '-shm']:(gen/('updates.sqlite3'+suffix)).unlink(missing_ok=True)
    with closing(database(Path(base['topics'])/'confidence.sqlite3')) as db:
        attach_undo(db,root)
        for table,key in [('records','id'),('links','dataset_id'),('primary_links','dataset_id')]:restore_rows(db,table,'graph_'+table,key)
        for table in ['counts','primary_counts','state']:
            db.execute(f'DELETE FROM {table}');db.execute(f'INSERT INTO {table} SELECT * FROM undo.graph_{table}')
        db.commit()
    with closing(database(base['catalog'])) as db:
        attach_undo(db,root)
        db.execute('DELETE FROM datasets WHERE id IN (SELECT dataset_id FROM delta.changes)')
        db.execute('INSERT INTO datasets(rowid,id,source_id,title,description,metadata,mappings,fingerprint,checked_at) SELECT * FROM undo.datasets')
        db.execute('DELETE FROM sources WHERE id IN (SELECT id FROM undo.sources)')
        db.execute('INSERT INTO sources SELECT * FROM undo.sources');db.commit()
    for name in ['vectors.faiss','sources.npy','active.npy','manifest.json']:
        if (backup/name).exists():shutil.copyfile(backup/name,gen/name);sync_file(gen/name)
    shutil.copyfile(backup/'topics.json',Path(base['topics'])/'manifest.json')
    rebind(base,root)
    keyword.validate(base['catalog'],base['keyword'])
    restored=VectorIndex(base['embeddings'],base['catalog'],base['index'],coverage_index=base['coverage']);restored.check_current()
    with closing(database(base['catalog'],readonly=True)) as db:
        attach_undo(db,root)
        if db.execute('SELECT * FROM undo.datasets EXCEPT SELECT rowid,* FROM datasets LIMIT 1').fetchone():raise ValueError('Catalog undo differs')
        if db.execute('SELECT id FROM datasets WHERE id IN (SELECT dataset_id FROM delta.changes) EXCEPT SELECT id FROM undo.datasets LIMIT 1').fetchone():raise ValueError('New catalog IDs remain after undo')
    checks=[(Path(base['topics'])/'confidence.sqlite3','records','graph_records','id'),
            (Path(base['topics'])/'confidence.sqlite3','links','graph_links','dataset_id'),
            (Path(base['topics'])/'confidence.sqlite3','primary_links','graph_primary_links','dataset_id'),
            (gen/'mapping.sqlite3','records','mapping_records','dataset_id'),
            (base['coverage'],'records','coverage_records','dataset_id')]
    for path,table,saved,key in checks:
        with closing(database(path,readonly=True)) as db:
            attach_undo(db,root)
            actual=f'SELECT * FROM {table} WHERE {key} IN (SELECT dataset_id FROM delta.changes)'
            before=f'SELECT * FROM undo.{saved}'
            if db.execute(before+' EXCEPT '+actual+' LIMIT 1').fetchone() or db.execute(actual+' EXCEPT '+before+' LIMIT 1').fetchone():raise ValueError('Recovered '+saved+' differs')
    write_json(root/'rollback-result.json',{'stage':'rolled_back','records':ready['records'],'finished_at':time.time()})



def begin_apply(base,root):
    if (root/'apply-started').exists():raise ValueError('An interrupted apply requires rollback')
    # Previous successful markers stay available until this next undo has been saved.
    for key in ['keyword','coverage']:
        with closing(database(base[key])) as db:
            db.execute('CREATE TABLE IF NOT EXISTS refresh_applied(id TEXT PRIMARY KEY,label INTEGER)')
            db.execute('DELETE FROM refresh_applied');db.commit()
    with (root/'apply-started').open('x') as f:f.write(str(time.time()));f.flush();os.fsync(f.fileno())
    fd=os.open(root,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def plan_classifications(base,root,topic_vectors):
    vectors=json.loads(Path(topic_vectors).read_text())['topics']
    by_id={t['id']:(base64.b64decode(t['vector']) if isinstance(t['vector'],str) else t['vector']) for t in vectors}
    with closing(database(Path(base['topics'])/'confidence.sqlite3',readonly=True)) as graph,closing(database(root/'delta.sqlite3')) as delta:
        topics=[r[0] for r in graph.execute('SELECT id FROM topics ORDER BY id')]
        matrix=np.stack([normalized(by_id[t],from_blob=isinstance(by_id[t],bytes)) for t in topics])
        delta.execute('DROP TABLE IF EXISTS classifications')
        delta.execute('CREATE TABLE classifications(dataset_id TEXT PRIMARY KEY,scores BLOB NOT NULL)')
        cursor=delta.execute('SELECT dataset_id,vector FROM changes ORDER BY dataset_id');count=0
        while rows:=cursor.fetchmany(256):
            scores=np.stack([normalized(r['vector'],from_blob=True) for r in rows]) @ matrix.T
            delta.executemany('INSERT INTO classifications VALUES(?,?)',[(r['dataset_id'],v.astype(np.float32).tobytes()) for r,v in zip(rows,scores)])
            delta.commit();count+=len(rows)
    result={'records':count,'topics':topics,'topic_vectors_sha256':_sha(topic_vectors)}
    write_json(root/'classification-plan.json',result)
    return result


def main():
    root=Path('/publication');base=json.loads((root/'inputs.json').read_text());started=time.time()
    faiss.omp_set_num_threads(1);action=sys.argv[1] if len(sys.argv)>1 else 'apply'
    if action=='plan':result=plan_classifications(base,root,'/topic-vectors.json')
    elif action=='prepare':result=prepare_undo(base,root)
    elif action=='rollback':rollback_data(base,root);result={'stage':'rolled_back'}
    elif action=='apply':
        ready=json.loads((root/'undo-ready.json').read_text())
        if _sha(root/'undo.sqlite3')!=ready['undo_sha256']:raise ValueError('Undo journal checksum differs')
        begin_apply(base,root)
        with closing(database(root/'delta.sqlite3')) as delta:
            result=apply_catalog_and_topics(base,root,delta,'/topic-vectors.json')
            target=apply_vectors(base,root,delta)
            result.update(verify(root,target,base,delta,'/topic-vectors.json'))
        result.update(stage='verified',seconds=round(time.time()-started,1));write_json(root/'build-result.json',result)
    else:raise ValueError('Unknown maintenance action')
    print(_json(result),flush=True)


if __name__=='__main__':main()
