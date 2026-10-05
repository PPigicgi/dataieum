"""Add the missing ID lookup to an isolated copy of the published vector files."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import re
import shutil
import sqlite3

from discovery_harness.vector_index import _identity, _sha


def build(index,coverage,output):
    index,coverage,output=map(Path,(index,coverage,output))
    generation=(index/'CURRENT').read_text().strip()
    if not re.fullmatch(r'generation-[a-f0-9]{32}',generation):raise ValueError('Invalid vector generation')
    source=index/generation
    manifest=json.loads((source/'manifest.json').read_text())
    target=output/'index'/generation
    if target.exists() or (output/'verification.json').exists():raise FileExistsError('Inspect existing index copy before restarting')
    target.mkdir(parents=True)
    identities={name:_identity(source/name) for name in manifest['files']}
    coverage_identity=_identity(coverage)
    for name,proof in manifest['files'].items():
        if Path(name).name!=name or (source/name).is_symlink():raise ValueError('Unsafe index file')
        if (source/name).stat().st_size!=proof['bytes'] or _sha(source/name)!=proof['sha256']:raise ValueError('Source index checksum differs')
        shutil.copy2(source/name,target/name)
        if _sha(target/name)!=proof['sha256']:raise ValueError('Copied index checksum differs')
    copied=output/'coverage.sqlite3'
    before_coverage=_sha(coverage);shutil.copy2(coverage,copied)
    if _sha(copied)!=before_coverage:raise ValueError('Copied coverage differs')
    mapping=target/'mapping.sqlite3'
    with closing(sqlite3.connect(mapping,uri=True)) as db:
        db.execute('PRAGMA cache_size=-32768')
        db.execute('CREATE INDEX records_dataset_id ON records(dataset_id)');db.commit()
        db.execute('ATTACH DATABASE ? AS original',((source/'mapping.sqlite3').resolve().as_uri()+'?mode=ro&immutable=1',))
        expected=db.execute('SELECT count(*) FROM original.records').fetchone()[0]
        if db.execute('SELECT count(*) FROM records').fetchone()[0]!=expected:raise ValueError('Mapping count differs')
        mismatch=db.execute('''SELECT m.label FROM records m LEFT JOIN original.records o ON o.label=m.label
            WHERE o.label IS NULL OR m.embedding_rowid IS NOT o.embedding_rowid
            OR m.dataset_id IS NOT o.dataset_id OR m.source_id IS NOT o.source_id
            OR m.input_hash IS NOT o.input_hash LIMIT 1''').fetchone()
        if mismatch:raise ValueError('A vector mapping value changed')
        plan=db.execute('EXPLAIN QUERY PLAN SELECT label FROM records INDEXED BY records_dataset_id WHERE dataset_id=?',('probe',)).fetchall()
        if not any('SEARCH' in row[3] and 'records_dataset_id' in row[3] for row in plan):raise ValueError('Lookup remains a full scan')
    with closing(sqlite3.connect(copied)) as db:
        value=json.loads(db.execute('SELECT value FROM manifest').fetchone()[0])
        if value['source_identity']['mapping']!=identities['mapping.sqlite3'] or value['mapping_sha256']!=manifest['files']['mapping.sqlite3']['sha256']:raise ValueError('Coverage source mapping differs')
        value['source_identity']['mapping']=_identity(mapping);value['mapping_sha256']=_sha(mapping)
        db.execute('UPDATE manifest SET value=?',(json.dumps(value),));db.commit()
    if identities!={name:_identity(source/name) for name in manifest['files']} or _identity(coverage)!=coverage_identity:raise ValueError('Original files changed')
    manifest['files']['mapping.sqlite3']={'bytes':mapping.stat().st_size,'sha256':_sha(mapping)}
    (target/'manifest.json').write_text(json.dumps(manifest))
    (output/'index'/'CURRENT').write_text(generation+'\n')
    proof={'all_mapping_rows_equal':True,'rows':expected,'source_unchanged':True,'vector_bytes_unchanged':True,'index':'records_dataset_id','query_plan':plan}
    (output/'verification.json').write_text(json.dumps(proof,indent=2))
    return proof


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--index',required=True);parser.add_argument('--coverage',required=True);parser.add_argument('--output',required=True);a=parser.parse_args()
    print(json.dumps(build(a.index,a.coverage,a.output)),flush=True)
