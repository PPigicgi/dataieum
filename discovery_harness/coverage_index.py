"""Disposable coverage prefilter, bound to immutable catalogue/FAISS mapping.

The original database and vectors are read-only. Both this builder and final
selection parse the same bounded public metadata projection. FTS only narrows
candidates; the final coverage check remains authoritative.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from . import coverage
from .keyword_index import TOKENS, expression
from .vector_index import _connect, _identity, _json, _sha, project_metadata

CONTRACT = coverage.VERSION + '/projection-2000-v1'


def _word(value):
    return 'c'+coverage.normal(value).encode().hex()


def _expression(groups):
    clauses=[]
    for group in coverage.validate_groups(groups):
        terms=['countries:'+_word(c) for c in group['countries']]
        for region in group['regions']:
            aliases=coverage.REGIONS.get(region,('',(region,)))[1]
            terms.append('('+' OR '.join('regions:'+expression(a) for a in aliases)+')')
        years=group['years']
        if years and group['years_mode']=='range':years=range(min(years),max(years)+1)
        terms.extend('years:y'+str(y) for y in years)
        if not terms:raise ValueError('unrestricted group needs no coverage index')
        clauses.append('('+' AND '.join(terms)+')')
    if not clauses:raise ValueError('coverage filters required')
    return ' OR '.join(clauses)


def build(catalog, mapping, target, *, max_bytes=12*1024**3):
    catalog,mapping=Path(catalog).resolve(strict=True),Path(mapping).resolve(strict=True)
    target=Path(target).absolute()
    if target.is_symlink() or target.exists():raise FileExistsError('coverage output already exists')
    target.parent.mkdir(parents=True,exist_ok=True)
    scratch=target.with_name(target.name+'.building')
    with scratch.open('xb'):pass
    before={'catalog':_identity(catalog),'mapping':_identity(mapping)}
    if any(v['wal'] for v in before.values()):raise ValueError('immutable source snapshots required')
    started=time.monotonic();count=eligible=0
    try:
        with _connect(mapping) as source, closing(sqlite3.connect(scratch)) as db:
            source.execute('ATTACH DATABASE ? AS cat',(catalog.as_uri()+'?mode=ro&immutable=1',))
            db.executescript("""
                PRAGMA journal_mode=OFF;
                PRAGMA synchronous=OFF;
                PRAGMA cache_size=-32768;
                CREATE TABLE manifest(value TEXT NOT NULL);
                CREATE TABLE records(label INTEGER PRIMARY KEY,dataset_id TEXT NOT NULL,
                    metadata_sha256 TEXT NOT NULL,evidence_fields TEXT NOT NULL);
                CREATE VIRTUAL TABLE scope USING fts5(countries,regions,years,
                    content='',columnsize=0,tokenize='unicode61 remove_diacritics 0');
            """)
            db.execute('PRAGMA max_page_count='+str(max_bytes//4096))
            cursor=source.execute('SELECT m.label,m.dataset_id,d.title,d.description,d.metadata '
                'FROM records m LEFT JOIN cat.datasets d ON d.id=m.dataset_id ORDER BY m.label')
            while rows:=cursor.fetchmany(500):
                identities=[];entries=[]
                for row in rows:
                    count+=1
                    meta=project_metadata(row['metadata'],title=row['title'],description=row['description'])
                    if meta is None:continue
                    facts=coverage.profile(meta)
                    text=coverage.normal(facts['geo_text']+'\n'+'\n'.join(facts['regions']))
                    entries.append((row['label'],' '.join(_word(v) for v in facts['countries']),
                        text.translate(TOKENS),' '.join('y'+str(v) for v in facts['years'])))
                    identities.append((row['label'],row['dataset_id'],hashlib.sha256(_json(meta).encode()).hexdigest(),
                        _json(sorted(k for k in meta if k not in {'url','publisher'}))))
                db.executemany('INSERT INTO records VALUES(?,?,?,?)',identities)
                db.executemany('INSERT INTO scope(rowid,countries,regions,years) VALUES(?,?,?,?)',entries)
                eligible+=len(entries);db.commit()
                if count%10000==0:print(_json({'coverage_records':count,'seconds':round(time.monotonic()-started,1),
                    'mib':round(scratch.stat().st_size/1024**2,1)}),flush=True)
            if before!={'catalog':_identity(catalog),'mapping':_identity(mapping)}:
                raise ValueError('coverage sources changed during build')
            manifest={'contract':CONTRACT,'source_identity':before,'mapping_sha256':_sha(mapping),
                'records':count,'eligible_metadata':eligible,'built_at':time.time_ns()}
            db.execute('INSERT INTO manifest VALUES(?)',(_json(manifest),));db.commit()
        # rename on Windows refuses an existing target. Exclusive output claim
        # also prevents a concurrent completed builder being replaced on POSIX.
        with target.open('xb'):pass
        scratch.replace(target)
        return manifest
    finally:
        scratch.unlink(missing_ok=True)


class CoverageIndex:
    def __init__(self,catalog,mapping,path):
        self.catalog,self.mapping,self.path=map(lambda p:Path(p).resolve(strict=True),(catalog,mapping,path))
        with _connect(self.path) as db:self.manifest=json.loads(db.execute('SELECT value FROM manifest').fetchone()[0])
        if self.manifest['contract']!=CONTRACT:raise ValueError('coverage parser contract differs')
        self.identity=_identity(self.path)
        self.check_current()
        if _sha(self.mapping)!=self.manifest['mapping_sha256']:raise ValueError('coverage label mapping differs')

    def check_current(self):
        if self.manifest['source_identity']!={'catalog':_identity(self.catalog),'mapping':_identity(self.mapping)} or self.identity!=_identity(self.path):
            raise ValueError('coverage snapshot changed')

    def labels(self,groups,*,deadline):
        self.check_current()
        if time.monotonic()>=deadline:raise TimeoutError('coverage deadline exceeded')
        query=_expression(groups)
        with _connect(self.path,deadline=deadline) as db:
            labels=[]
            # Contentless FTS can retain terms for removed records. Only live
            # record labels may enter the ANN candidate filter.
            cursor=db.execute('SELECT scope.rowid FROM scope JOIN records r ON r.label=scope.rowid WHERE scope MATCH ?',(query,))
            while rows:=cursor.fetchmany(4096):
                if time.monotonic()>=deadline:raise TimeoutError('coverage deadline exceeded')
                labels.extend(row[0] for row in rows)
        self.check_current()
        return labels


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('catalog');parser.add_argument('mapping');parser.add_argument('output')
    args=parser.parse_args()
    print(_json(build(args.catalog,args.mapping,args.output)),flush=True)
