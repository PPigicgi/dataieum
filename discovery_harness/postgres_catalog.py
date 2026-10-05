"""Authoritative metadata/relationships; SQLite and FAISS remain derived indexes."""
import json
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


class PostgresCatalog:
    def __init__(self,credentials,publication,native):
        self.settings=json.loads(Path(credentials).read_text())
        self.publication_path=Path(publication);self.native=native;self.db=None
        self.validate()

    def connect(self):
        if self.db is None or self.db.closed:
            self.db=psycopg.connect(**self.settings,connect_timeout=2,autocommit=True,row_factory=dict_row,
                options='-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=500')
        return self.db

    def validate(self):
        expected=json.loads(self.publication_path.read_text())['publication']
        row=self.connect().execute('SELECT id,state,details FROM publications WHERE id=%s',(expected,)).fetchone()
        if not row or row['state'] not in {'verified','active'} or not row['details'].get('verification',{}).get('all_values_equal'):
            raise RuntimeError('PostgreSQL publication is not verified')
        for key,path in {'catalog':self.native.catalog,'topics':self.native.topics,'keyword':self.native.search_index}.items():
            s=path.stat()
            if row['details']['artifacts'][key]!={'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}:
                raise RuntimeError('PostgreSQL and derived publication differ')
        self.version=row['id'];self.details=row['details']

    def check(self):
        if json.loads(self.publication_path.read_text())['publication']!=self.version:raise RuntimeError('PostgreSQL publication changed')
        if self.native.expired():raise TimeoutError('Catalogue deadline exceeded')

    def sources(self):
        self.check()
        return [{**r['info'],'last_sync':r['last_sync'],'last_error':r['last_error']} for r in self.connect().execute('SELECT * FROM sources ORDER BY id')]

    def raw(self,ids):
        if not ids:return []
        if len(ids)>2048:raise ValueError('Too many metadata IDs')
        self.check()
        rows={r['id']:r for r in self.connect().execute('SELECT * FROM datasets WHERE id=ANY(%s)',(ids,))}
        self.check()
        return [rows[i] for i in ids if i in rows]

    def page_ids(self,query,after):
        self.check()
        clauses=["classification->>'band' IS NOT NULL",'ordinal>%s'];args=[after]
        for key,column in [('source','source_id'),('band',"classification->>'band'")]:
            if query.get(key):clauses.append(column+'=%s');args.append(query[key])
        if query.get('year'):clauses.append('reference_years @> %s::integer[]');args.append([int(query['year'])])
        if query.get('concept'):
            topic=query['concept'];clauses.append("classification->>'primary_topic' IS NOT NULL")
            column="classification->>'primary_topic'" if '-S' in topic else "split_part(classification->>'primary_topic','-',1)"
            clauses.append(column+'=%s');args.append(topic)
        rows=self.connect().execute('SELECT ordinal,id FROM datasets WHERE '+' AND '.join(clauses)+' ORDER BY ordinal LIMIT 31',args).fetchall()
        self.check()
        return [(row['ordinal'],row['id']) for row in rows]

    def records(self,ids,*,classified=True):
        from catalog import year_fields
        from confidence_graph import confidence_score
        maximum=self.details['verification'].get('maximum_similarity')
        if maximum is None:maximum=self.native.module.maximum_similarity(self.native.identity())
        result=[]
        for r in self.raw(ids):
            meta=r['metadata'];c=r['classification']
            if classified and c['band'] is None:continue
            item={**meta,**year_fields(meta,meta.get('title',''),meta.get('description','')),
                'mappings':r['original_mappings'],'checked_at':r['checked_at']}
            if classified:
                item.pop('classification_note',None)
                item.update(confidence_band=c['band'],max_similarity=c['score'])
                item['mappings']=[{'concept_id':link['topic'],'confidence':link['score'],'opacity':link['alpha'],
                    'status':'classified','basis':c['basis'],'confidence_score':confidence_score(link['score'],c['band'],maximum),
                    'evidence':f'신뢰도 {confidence_score(link["score"],c["band"],maximum):g}점'} for link in c['links']]
            result.append(item)
        return result

    def close(self):
        if self.db is not None:self.db.close()
