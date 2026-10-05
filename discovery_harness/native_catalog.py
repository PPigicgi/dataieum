"""Run the imported confidence catalogue with bounded, read-only connections."""
from contextlib import contextmanager
from functools import lru_cache
import json
import hashlib
import math
import os
from pathlib import Path
import re
import sqlite3


class NativeCatalog:
    def __init__(self,module,catalog,topics,overview,expired,*,search_index=None,topic_view=False):
        self.module=module
        self.catalog=Path(catalog).resolve(strict=True)
        self.topics=Path(topics).resolve(strict=True)
        self.overview=Path(overview)
        self.expired=expired
        self.search_index=Path(search_index).resolve(strict=True) if search_index else None
        self._keyword_excluded=None
        if self.search_index:
            from .keyword_index import validate, lookup
            validate(self.catalog,self.search_index)
            original_where=self.module.where
            def indexed_where(query):
                clause,args=original_where({k:v for k,v in query.items() if k not in {'q','year','concept'}})
                filters=[]
                if query.get('concept'):
                    field='id' if '-S' in query['concept'] else 'main_id'
                    filters.append(f'r.id IN (SELECT p.dataset_id FROM primary_links p '
                        f'WHERE p.topic IN (SELECT id FROM topics WHERE {field}=?))')
                    args.append(query['concept'])
                if query.get('year'):
                    filters.append('r.id IN (SELECT d.id FROM keyword.reference_years y '
                        'JOIN keyword.documents d ON d.n=y.n WHERE y.year=?)')
                    args.append(int(query['year']))
                if query.get('q'):
                    table,expression=lookup(query['q'])
                    filters.append(f'r.id IN (SELECT d.id FROM keyword.{table} JOIN keyword.documents d '
                        f'ON d.n={table}.rowid WHERE {table} MATCH ?)')
                    args.append(expression)
                if filters:clause+=(' AND ' if clause else ' WHERE ')+' AND '.join(filters)
                return clause,args
            self.module.where=indexed_where
        self.identity()
        self.validate_complete()
        self.module.PATH=str(self.topics)
        self.module.connection=self.connection
        self.postgres=None
        if os.environ.get('DATAIEUM_POSTGRES_CONFIG'):
            from .postgres_catalog import PostgresCatalog
            self.postgres=PostgresCatalog(os.environ['DATAIEUM_POSTGRES_CONFIG'],os.environ['DATAIEUM_PUBLICATION_FILE'],self)
            import catalog
            catalog.read_records=lambda db,rows:self.postgres.records([r['id'] for r in rows],classified=False)
            self.module.records=lambda db,ids:self.postgres.records(ids)
        original_page_ids=self.module.page_ids
        def page_ids(db,query,start,size):
            if size<=48:return original_page_ids(db,query,start,size)
            return self._boundary_ids(db,query,start,size)
        self.module.page_ids=page_ids
        if hasattr(self.module,'records'):
            original_records=self.module.records
            def records(db,ids):
                result=original_records(db,ids)
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='classification_holds'").fetchone():
                    from .topic_graph import held_classification
                    for item in result:
                        row=db.execute('SELECT band,score FROM records WHERE id=?',(item['id'],)).fetchone()
                        classification=held_classification(db,item['id'],row) if row else None
                        if classification:
                            if item.get('mappings'):raise RuntimeError('Held record has active catalogue mappings')
                            item['classification_basis']=classification['basis']
                            item['original_classification']={'level':classification['original_level'],
                                'max_similarity':classification['max_similarity'],'excluded_links':classification['excluded_links']}
                return result
            self.module.records=records
        self.signature=self.identity()
        self._search_root=lru_cache(maxsize=256)(self._search_root)
        self.explorer=None
        if topic_view:
            from .topic_explorer import TopicExplorer
            self.explorer=TopicExplorer(self.module,self.connection)

    def identity(self):
        values=[]
        for path in (self.topics,self.catalog)+((self.search_index,) if self.search_index else ()):
            wal=path.with_name(path.name+'-wal')
            if wal.exists() and wal.stat().st_size:raise RuntimeError('snapshot has a live WAL')
            stat=path.stat();values.append((stat.st_size,stat.st_mtime_ns,stat.st_ino))
        publication=os.environ.get('DATAIEUM_PUBLICATION_FILE')
        if publication:
            stat=Path(publication).stat();values.append((stat.st_size,stat.st_mtime_ns,stat.st_ino))
        return tuple(values)

    def changed(self):return self.identity()!=self.signature

    def validate_complete(self):
        db=sqlite3.connect(self.topics.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=.25)
        try:
            db.set_progress_handler(lambda:int(self.expired()),1000)
            complete=db.execute("SELECT value FROM state WHERE key='complete'").fetchone()
            if complete is None or complete[0]!='1':raise RuntimeError('classification snapshot incomplete')
        finally:db.close()

    @contextmanager
    def connection(self):
        # The topic path is a verified immutable snapshot, mounted read-only.
        db=sqlite3.connect(self.topics.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=.25)
        try:
            db.row_factory=sqlite3.Row
            self.identity()
            db.execute('ATTACH DATABASE ? AS cat',(self.catalog.as_uri()+'?mode=ro&immutable=1',))
            if self.search_index:
                db.execute('ATTACH DATABASE ? AS keyword',(self.search_index.as_uri()+'?mode=ro&immutable=1',))
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA cache_size=-16384')
            db.set_progress_handler(lambda:int(self.expired()),1000)
            db.create_function('has_year',4,lambda period,title,description,year:
                int(int(year) in self.module.year_fields({'period':period},title,description)['reference_years']))
            yield db
        finally:db.close()

    def prepare(self):
        self._search_root.cache_clear()
        if self.explorer:self.explorer.clear()
        for name in ('base_catalog','maximum_similarity','_graph'):
            fn=getattr(self.module,name,None)
            if hasattr(fn,'cache_clear'):fn.cache_clear()
        before=self.identity()
        self.validate_complete()
        if self.postgres:self.postgres.validate()
        if self.search_index:
            from .keyword_index import validate
            manifest=validate(self.catalog,self.search_index)
            with self.connection() as db:
                indexed=db.execute('SELECT records FROM keyword.reference_year_state').fetchone()
                if indexed is None or indexed[0]!=manifest['records']:
                    raise RuntimeError('reference-year index incomplete')
            if not self._restore_preparation(before):
                self._prepare_keyword_membership()
                self._prepare_reference_year_counts()
                self._save_preparation(before)
        value=self.explorer.initial_overview() if self.explorer else self.module.initial_overview()
        if self.postgres:
            source_info={s['id']:s for s in self.postgres.sources()}
            for item in value['sources']:
                info=source_info.get(item['id'])
                if info is None:raise RuntimeError('Source absent in PostgreSQL')
                item.update({k:v for k,v in info.items() if k not in {'scan','dataset_count','stored_count'}})
        if before!=self.identity():raise RuntimeError('classification snapshot changed')
        self.overview.parent.mkdir(parents=True,exist_ok=True)
        temporary=self.overview.with_suffix('.tmp')
        temporary.write_text(json.dumps(value,ensure_ascii=False),encoding='utf-8')
        temporary.replace(self.overview)
        self.signature=before
        return value

    def _preparation_key(self,identity):
        return hashlib.sha256(json.dumps(['membership-years-v1',identity],separators=(',',':')).encode()).hexdigest()

    def _restore_preparation(self,identity):
        path=self.overview.with_suffix('.inputs.json')
        try:
            if path.stat().st_size>2*1024**2:return False
            value=json.loads(path.read_text())
            if value['key']!=self._preparation_key(identity):return False
            excluded=value['excluded'];years=value['years']
            if excluded is not None and (not isinstance(excluded,list) or len(excluded)>4096 or any(not isinstance(i,str) or not 0<len(i)<=2048 for i in excluded)):return False
            if not isinstance(years,dict) or len(years)>10000 or any(not re.fullmatch(r'\d{4}',y) or type(n) is not int or n<0 for y,n in years.items()):return False
            self._keyword_excluded=json.dumps(excluded) if excluded is not None else None
            self.module.base_catalog()['summary']['year_counts']=years
            return True
        except (OSError,ValueError,KeyError,TypeError):return False

    def _save_preparation(self,identity):
        if identity!=self.identity():raise RuntimeError('Preparation source changed')
        value={'key':self._preparation_key(identity),'excluded':json.loads(self._keyword_excluded) if self._keyword_excluded is not None else None,
               'years':self.module.base_catalog()['summary']['year_counts']}
        path=self.overview.with_suffix('.inputs.json');path.parent.mkdir(parents=True,exist_ok=True)
        temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(value));temporary.replace(path)

    def _prepare_keyword_membership(self):
        # Compare the immutable ID indexes once. A small exact exclusion list
        # avoids random graph lookups for every hit on unfiltered searches.
        # Large differences use the ordinary join, never a truncated list.
        with self.connection() as db:
            rows=db.execute('SELECT d.id FROM keyword.documents d '
                'INDEXED BY sqlite_autoindex_documents_1 LEFT JOIN records r ON r.id=d.id '
                'WHERE r.id IS NULL LIMIT 4097').fetchall()
        self._keyword_excluded=json.dumps([r[0] for r in rows]) if len(rows)<=4096 else None

    def _prepare_reference_year_counts(self):
        with self.connection() as db:
            if self._keyword_excluded is None:
                counts=dict(db.execute('SELECT y.year,count(*) FROM keyword.reference_years y '
                    'JOIN keyword.documents d ON d.n=y.n JOIN records r ON r.id=d.id GROUP BY y.year'))
            else:
                counts=dict(db.execute('SELECT year,count(*) FROM keyword.reference_years GROUP BY year'))
                hidden=db.execute('SELECT year,count(*) FROM keyword.reference_years WHERE n IN '
                    '(SELECT n FROM keyword.documents WHERE id IN (SELECT value FROM json_each(?))) GROUP BY year',
                    (self._keyword_excluded,))
                for year,n in hidden:counts[year]-=n
        # base_catalog is cached; every catalogue and bootstrap shares the same
        # counts, including after publication removes or changes a record's years.
        self.module.base_catalog()['summary']['year_counts']={str(y):n for y,n in counts.items() if n}

    def query(self,value,*,catalog=False):
        allowed={'band','concept','source','year','q','branch'}|({'page'} if catalog else set())
        if not isinstance(value,dict) or set(value)-allowed:raise ValueError('unsupported graph filter')
        for key,text in value.items():
            if not isinstance(text,str) or len(text)>200 or any(ord(c)<32 for c in text):raise ValueError('invalid graph filter')
            if not text:continue
            if key=='band' and text not in {'high','low','unclassified'}:raise ValueError('invalid band')
            if key=='concept' and not re.fullmatch(r'M\d{2}(?:-S\d{2})?',text):raise ValueError('invalid topic')
            if key=='year' and not re.fullmatch(r'\d{4}',text):raise ValueError('invalid year')
            if key=='branch' and (len(text)>64 or not re.fullmatch(r'\d+(?:\.\d+)*',text)):raise ValueError('invalid branch')
            if key=='page' and (not text.isascii() or not text.isdigit() or len(text)>8):raise ValueError('invalid page')
        if self.changed():raise RuntimeError('classification snapshot changed')
        return value

    def graph(self,query):
        query=self.query(query)
        if self.explorer and not query.get('band'):
            return self.explorer.graph(query)
        if self.search_index and query.get('q') and not query.get('band'):
            return self._search_root(tuple(sorted((k,v) for k,v in query.items() if v)))
        return self.module.graph_overview(query)

    def _search_root(self,key):
        # Keep the imported graph's 256-result cache and scan matching IDs once
        # for the total and all three bands. graph() still checks source identity.
        q=dict(key);where,args=self.module.where(q)
        with self.connection() as db:
            counts=dict(db.execute('SELECT r.band,count(*) FROM records r'+where+' GROUP BY r.band',args))
        total=sum(counts.values())
        nodes=[{'id':'band:'+band,'kind':'band','title':title,'count':counts.get(band,0),
                'query':{**q,'band':band}} for band,title in self.module.BANDS.items()]
        return {'nodes':nodes,'edges':[],'total':total,'represented':total,'query':q}
    def snapshot(self,query):
        query=self.query(query,catalog=True)
        if self.search_index and query.get('q'):
            return self._keyword_snapshot(query)
        if '-S' not in query.get('concept',''):
            return self.module.snapshot(query)
        # List relevance and graph range boundaries are different contracts.
        # Keep page_ids (and the complete map) in stable ID order; only the
        # selected concept's visible catalogue is sorted by its stored cosine.
        rest={k:v for k,v in query.items() if k!='concept'}
        clause,args=self.module.where(rest)
        clause+=(' AND ' if clause else ' WHERE ')+'p.topic=?'
        args=args+[query['concept']]
        result=dict(self.module.base_catalog())
        with self.connection() as db:
            original_clause,original_args=self.module.where(query)
            total=self.module.total(db,query,original_clause,original_args)
            pages=max(1,math.ceil(total/30));page=min(max(1,int(query.get('page',1))),pages)
            ids=[r[0] for r in db.execute('SELECT p.dataset_id FROM primary_links p '
                'JOIN records r ON r.id=p.dataset_id'+clause+
                ' ORDER BY p.score DESC,p.dataset_id LIMIT 30 OFFSET ?',args+[(page-1)*30])]
            result.update(total=total,page=page,pages=pages,datasets=self.module.records(db,ids))
        return result

    def _keyword_snapshot(self, query):
        """Evaluate every matching ID once for exact totals and a bounded page."""
        from .keyword_index import lookup
        table, expression = lookup(query['q'])
        rest = {k: v for k, v in query.items() if k not in {'q', 'page'}}
        clause, args = self.module.where(rest)
        predicate = clause.removeprefix(' WHERE ') or '1'
        ranked = '-S' in query.get('concept', '')
        rank = ('(SELECT score FROM primary_links p WHERE p.dataset_id=r.id)' if ranked else '0')
        order = 'score DESC,id' if ranked else 'id'
        # Read graph IDs in key order instead of random FTS row order. This keeps
        # exact membership checks from repeatedly evicting cold index pages.
        # The materialized result is shared by count and page.
        # The LEFT JOIN keeps total=0 visible even when no dataset matches.
        sql = f'''WITH candidates AS MATERIALIZED (
            SELECT d.id FROM keyword.{table}
            CROSS JOIN keyword.documents d ON d.n={table}.rowid
            WHERE {table} MATCH ? ORDER BY d.id
        ), matched AS MATERIALIZED (
            SELECT r.id,{rank} AS score FROM candidates c
            CROSS JOIN records r ON r.id=c.id WHERE {predicate}
        ), stats AS (SELECT count(*) AS n FROM matched), page AS (
            SELECT id,score FROM matched ORDER BY {order} LIMIT 30
            OFFSET (SELECT max(0,min(?,(n-1)/30))*30 FROM stats)
        ) SELECT stats.n,page.id,page.score FROM stats LEFT JOIN page ON 1
          ORDER BY {'page.score DESC,' if ranked else ''}page.id'''
        requested = max(1, int(query.get('page', 1)))
        parameters=[expression,*args,requested-1]
        if query.get('concept'):
            rest={k:v for k,v in rest.items() if k!='concept'}
            clause,args=self.module.where(rest)
            predicate=clause.removeprefix(' WHERE ') or '1'
            field='id' if ranked else 'main_id'
            # Start with the topic's indexed members, intersect with FTS once,
            # then read records only for that intersection.
            sql=f'''WITH candidates AS MATERIALIZED (
                SELECT d.id FROM keyword.{table}
                CROSS JOIN keyword.documents d ON d.n={table}.rowid WHERE {table} MATCH ?
            ), matched AS MATERIALIZED (
                SELECT r.id,p.score FROM primary_links p
                CROSS JOIN records r ON r.id=p.dataset_id
                WHERE p.topic IN (SELECT id FROM topics WHERE {field}=?)
                  AND p.dataset_id IN (SELECT id FROM candidates) AND {predicate}
            ), stats AS (SELECT count(*) AS n FROM matched), page AS (
                SELECT id,score FROM matched ORDER BY {order} LIMIT 30
                OFFSET (SELECT max(0,min(?,(n-1)/30))*30 FROM stats)
            ) SELECT stats.n,page.id,page.score FROM stats LEFT JOIN page ON 1
              ORDER BY {'page.score DESC,' if ranked else ''}page.id'''
            parameters=[expression,query['concept'],*args,requested-1]
        excluded=getattr(self,'_keyword_excluded',None)
        if excluded is not None and not any(query.get(k) for k in ('band','source','year','concept')):
            sql=f'''WITH matched AS MATERIALIZED (
                SELECT d.id FROM keyword.{table}
                CROSS JOIN keyword.documents d ON d.n={table}.rowid
                WHERE {table} MATCH ? AND d.id NOT IN (SELECT value FROM json_each(?))
            ), stats AS (SELECT count(*) AS n FROM matched), page AS (
                SELECT id FROM matched ORDER BY id LIMIT 30
                OFFSET (SELECT max(0,min(?,(n-1)/30))*30 FROM stats)
            ) SELECT stats.n,page.id FROM stats LEFT JOIN page ON 1 ORDER BY page.id'''
            parameters=[expression,excluded,requested-1]
        result = dict(self.module.base_catalog())
        with self.connection() as db:
            rows = db.execute(sql, parameters).fetchall()
            total = rows[0][0]
            pages = max(1, math.ceil(total / 30))
            ids = [r[1] for r in rows if r[1] is not None]
            result.update(total=total, page=min(requested, pages), pages=pages,
                          datasets=self.module.records(db, ids))
        return result

    def _boundary_ids(self,db,query,start,size):
        # Preserve the imported ID order and filters, but scan once rather than
        # sorting and skipping the same range separately for up to 64 labels.
        topic=query.get('concept','')
        if '-S' in topic:
            clause,args=self.module.where({k:v for k,v in query.items() if k!='concept'})
            sql='SELECT p.dataset_id FROM primary_links p JOIN records r ON r.id=p.dataset_id'+clause
            sql+=(' AND ' if clause else ' WHERE ')+'p.topic=?';args=args+[topic]
            if query.get('band'):
                sql+=' AND p.band=?';args.append(query['band'])
            # p.dataset_id == r.id; this lets the existing topic index stream
            # the same order for a band instead of building a temporary sort.
            sql+=' ORDER BY p.dataset_id'
        else:
            clause,args=self.module.where(query)
            sql='SELECT r.id FROM records r'+clause+' ORDER BY r.id'
        cursor=db.execute(sql+' LIMIT ? OFFSET ?',args+[size,start])
        try:return _BoundaryIDs(size,cursor)
        finally:cursor.close()


class _BoundaryIDs:
    """Range labels need at most 64 endpoints, never the whole topic corpus."""
    def __init__(self,size,rows):
        self.size,self.cache=size,{}
        width=(size+31)//32
        endpoints={off for off in range(0,size,width)}|{min(off+width,size)-1 for off in range(0,size,width)}
        count=0
        for index,row in enumerate(rows):
            count+=1
            if index in endpoints:self.cache[index]=row[0]
        if count!=size:raise RuntimeError('classification range changed')
    def __len__(self):return self.size
    def __getitem__(self,index):
        if type(index) is not int or not 0<=index<self.size:raise IndexError(index)
        if index not in self.cache:raise ValueError('not a graph range endpoint')
        return self.cache[index]
