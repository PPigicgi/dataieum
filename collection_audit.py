"""Collect source catalogs and record completeness evidence; never classify data.

Run on AWS with DB_PATH pointing at the existing catalog. Audit JSON and ID lists
are kept beside that database. Only metadata and source links are downloaded.
"""
import collections
import fcntl
import gzip
import concurrent.futures
import hashlib
import html
import io
import json
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog import database, dump, normalize, now, public_url

ROOT = Path(os.environ.get('DB_PATH', 'catalog.sqlite3')).parent / 'collection-audit'
ROOT.mkdir(parents=True, exist_ok=True)
os.umask(0o007)


def read_url(url, limit=20_000_000, fields=None, payload=None, content_type=None, accept=None):
    for attempt in range(3):
        try:
            headers={'User-Agent':'PublicAtlas/1.0 public catalog metadata audit', 'Accept':'application/json,text/html,application/xml,*/*','Accept-Encoding':'gzip'}
            if fields:headers['X-Fields']=fields
            if content_type:headers['Content-Type']=content_type
            if accept:headers['Accept']=accept
            req = urllib.request.Request(url, headers=headers, data=payload)
            with urllib.request.urlopen(req, timeout=40) as response:
                stream=gzip.GzipFile(fileobj=response) if response.headers.get('Content-Encoding','').lower()=='gzip' else response
                raw = stream.read(limit+1)
            if len(raw)>limit:
                raise ValueError('Catalog response exceeds size limit')
            return raw
        except urllib.error.HTTPError as error:
            if error.code not in (429,500,502,503,504) or attempt==2:
                raise
            retry=error.headers.get('Retry-After','')
            if retry and (not retry.isdigit() or int(retry)>60):
                raise
            time.sleep(max(2**attempt,int(retry or 0)))
        except (TimeoutError,urllib.error.URLError):
            if attempt==2:raise
            time.sleep(2**attempt)


def clean(value):
    return re.sub(r'\s+', ' ', html.unescape(re.sub('<[^>]+>', ' ', value or ''))).strip()


def store_raw(source_id, records):
    # No ontology changes: retain existing mappings and record new rows unclassified.
    values=[normalize({**r,'source_id':source_id}) for r in records]
    with database() as db:
        db.execute('PRAGMA busy_timeout=60000')
        db.execute('BEGIN IMMEDIATE')
        for r in values:
            previous=db.execute('SELECT metadata,mappings FROM datasets WHERE id=?',(r['id'],)).fetchone()
            mappings=previous['mappings'] if previous else '[]'
            if previous:
                old=json.loads(previous['metadata'])
                if source_id=='kosis':
                    r['native_catalog_paths']=sorted(set(old.get('native_catalog_paths',[])+[str(old.get('native_catalog_path') or ''),str(r.get('native_catalog_path') or '')]) - {''})
                    if not r.get('description'):r['description']=old.get('description','')
                if source_id=='gyeonggi':
                    r['access_paths']=sorted(set(old.get('access_paths',[])+r.get('access_paths',[])+[old['url'],r['url']]))
                for key in ('classification_note','duplicate_canonical'):
                    if key in old:r[key]=old[key]
                for key in ('duplicate_of','duplicate_evidence'):r.pop(key,None)
                if all(r.get(k)==old.get(k) for k in ('title','description','publisher','region','period','license','url','access_paths')):
                    for key in ('duplicate_of','duplicate_evidence'):
                        if key in old:r[key]=old[key]
                elif old.get('duplicate_canonical'):
                    db.execute("UPDATE datasets SET metadata=json_remove(metadata,'$.duplicate_of','$.duplicate_evidence') WHERE json_extract(metadata,'$.duplicate_of')=?",(r['id'],))
            r=normalize(r)
            db.execute('''INSERT INTO datasets VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,description=excluded.description,metadata=excluded.metadata,
                fingerprint=excluded.fingerprint,checked_at=excluded.checked_at''',
                (r['id'],source_id,r['title'],r['description'],dump(r),mappings,r['fingerprint'],r['checked_at']))
    return {r['id'] for r in values}


def report(source_id, info):
    info={**info,'checked_at':now()}
    target=ROOT/(source_id+'.json');tmp=target.with_suffix('.tmp')
    tmp.write_text(dump(info));tmp.replace(target)
    with database() as db:
        db.execute('PRAGMA busy_timeout=60000')
        state='running' if info.get('status')=='running' else 'complete' if info.get('status') in ('count_reconciled','export_reconciled','id_reconciled','tree_exhausted') else 'paused'
        db.execute("UPDATE sources SET info=json_set(info,'$.collection_audit',json(?),'$.scan.status',?,'$.scan.updated_at',?),last_error=? WHERE id=?",
                   (dump(info),state,info['checked_at'],info.get('error','') if state!='complete' else '',source_id.split('-')[0]))
    print(dump({'source':source_id,**info}),flush=True)


def korea_page(kind, page, size=1000, sort="date", category=None):
    url='https://www.data.go.kr/tcs/dss/selectDataSetList.do?'+urllib.parse.urlencode({'dType':kind,'currentPage':page,'perPage':size,'sort':sort,**({'brm':category} if category else {})})
    text=read_url(url).decode('utf-8')
    counter={'API':'apiCnt','FILE':'fileCnt','LINKED':'linkedCnt'}.get(kind)
    if counter:
        match=re.search(r'\$\("#'+counter+r'"\).text\("([\d,]+)"\)',text)
    else:
        match=re.search(r'표준데이터셋\s*(?:</?[^>]+>\s*)*([\d,]+)개',text)
    if not match and kind=='STD':
        match=re.search(r'표준데이터셋.*?([\d,]+)개',text,re.S)
    if not match:raise ValueError('Provider total could not be parsed: '+kind)
    total=int(match[1].replace(',',''))
    if total==0 and not re.search(r'href="/data/[^/]+/[^/]+\.do"',text):
        return [],total,url
    blocks=text.split('class="apply-result-item"')[1:]
    records=[]
    for block in blocks:
        link=re.search(r'<a[^>]+href="(/data/([^/]+)/([^/]+)\.do)"[^>]*>(.*?)</a>',block,re.S)
        if not link:raise ValueError('Catalog entry has no recognized landing link')
        path,id,mode,title=link.groups()
        title=clean(title)
        desc=re.search(r'<span class="apply-result-summary">(.*?)</span>',block,re.S)
        def field(name):
            m=re.search(r'<strong>'+name+r'</strong>(.*?)</li>',block,re.S)
            return clean(m[1]) if m else ''
        category=re.search(r'class="apply-result-category">(.*?)</div>',block,re.S)
        categories=re.findall(r'<span[^>]*>(.*?)</span>',category[1],re.S) if category else []
        ext=re.findall(r'data-ext="([^"]+)"',block)
        external=id if mode=='openapi' else mode+':'+id
        records.append({'external_id':external,'title':title,'description':clean(desc[1]) if desc else '',
                        'publisher':field('제공기관') or '제공처 확인','url':'https://www.data.go.kr'+path,
                        'metadata_url':url,'region':'미상','period':'미상','format':' / '.join(sorted(set(ext))) or mode,
                        'source_modified':field('수정일'),'native_id':id,'native_type':kind,
                        'subjects':[{'kind':'theme','label':clean(x)} for x in categories],
                        'collection_method':'official_catalog_html'})
    if len(records)!=len(blocks):raise ValueError('Unparsed catalog entries')
    return records,total,url


def collect_korea():
    all_ids=set();parts={}
    for kind in ('API','FILE','STD','LINKED'):
        seen=set();repeats=0;first_total=None;page=1
        log=ROOT/('korea-'+kind+'-ids.jsonl')
        # Full pass followed by a repair pass if the changing catalog shifted pages.
        for sweep in range(2):
            page=1;pass_ids=set()
            with log.open('a' if sweep else 'w') as journal:
                while True:
                    records,total,url=korea_page(kind,page)
                    if first_total is None:first_total=total
                    ids={r['external_id'] for r in records}
                    if not ids and (page-1)*1000<total:raise ValueError('Empty page before advertised end')
                    repeats+=len(records)-len(ids)+len(pass_ids&ids)
                    pass_ids.update(ids);seen.update(ids)
                    store_raw('korea',records)
                    journal.write(dump({'page':page,'sweep':sweep,'total':total,'url':url,'ids':sorted(ids)})+'\n');journal.flush()
                    parts[kind]={'advertised':total,'initial_total':first_total,'unique':len(seen),'page':page,'repeated':repeats,'sweep':sweep}
                    report('korea',{'status':'running','scope':'공공데이터포털 API·파일·표준목록·연계목록','parts':parts,'unique':len(all_ids|seen)})
                    if page*1000>=total:break
                    page+=1;time.sleep(.15)
            if len(pass_ids)==total:break
        all_ids.update(seen)
        parts[kind]['status']='count_reconciled' if len(pass_ids)==total else 'unreconciled'
    report('korea',{'status':'count_reconciled' if all(v['status']=='count_reconciled' for v in parts.values()) else 'unreconciled',
                    'scope':'API·파일·표준 템플릿·연계목록. 표준기관별 행은 별도 자료로 세지 않음. 변경 중 목록의 시점 차이 가능.',
                    'parts':parts,'unique':len(all_ids),'evidence':'collection-audit/korea-*-ids.jsonl'})



def repair_korea_linked():
    seen=set();journal_path=ROOT/'korea-LINKED-ids.jsonl'
    if journal_path.exists():
        for line in journal_path.read_text().splitlines():seen.update(json.loads(line)['ids'])
    for old in ROOT.glob('korea-LINKED-*.jsonl'):
        if old==journal_path:continue
        for line in old.read_text().splitlines():seen.update(json.loads(line)['ids'])
    initial=len(seen)
    for order in ('view','use'):
        with (ROOT/('korea-LINKED-'+order+'.jsonl')).open('a') as journal:
            page=1
            while True:
                records,total,url=korea_page('LINKED',page,size=2000,sort=order)
                if not records and (page-1)*2000<total:raise ValueError('Empty repair page')
                if len(records)<2000 and page*2000<total:raise ValueError('Provider changed requested repair page size')
                ids={r['external_id'] for r in records};new=ids-seen
                if new:store_raw('korea',[r for r in records if r['external_id'] in new])
                seen.update(ids)
                journal.write(dump({'page':page,'size':2000,'total':total,'url':url,'ids':sorted(ids)})+'\n');journal.flush()
                if page%25==0:report('korea-linked-repair',{'status':'running','advertised':total,'unique':len(seen),'remaining_count_gap':max(0,total-len(seen)),'sort':order,'page':page})
                if len(seen)>=total or page*2000>=total:break
                page+=1;time.sleep(.15)
        if len(seen)>=total:break
    report('korea-linked-repair',{'status':'count_reconciled' if len(seen)==total else 'unreconciled','advertised':total,'unique':len(seen),
                                 'newly_recovered':len(seen)-initial,'remaining_count_gap':max(0,total-len(seen)),
                                 'scope':'수정일·조회순·활용순 및 분류별 목록 합집합. 2000건 페이지로 경계 누락 교차 대조'})

    main=json.loads((ROOT/'korea.json').read_text())
    main['parts']['LINKED'].update({'status':'count_reconciled' if len(seen)==total else 'unreconciled','unique':len(seen),'advertised':total})
    main['unique']=sum(v['unique'] for v in main['parts'].values())
    main['status']='count_reconciled' if all(v['status']=='count_reconciled' for v in main['parts'].values()) else 'unreconciled'
    report('korea',main)

def repair_korea_categories():
    seen=set()
    for old in ROOT.glob('korea-LINKED-*.jsonl'):
        for line in old.read_text().splitlines():seen.update(json.loads(line)['ids'])
    _,total,_=korea_page('LINKED',1)
    initial=len(seen);parts={}
    categories=['공공행정','과학기술','교육','교통물류','국토관리','농축수산','문화관광','법률','보건의료','사회복지','산업고용','식품건강','재난안전','재정금융','통일외교안보','환경기상']
    with (ROOT/'korea-LINKED-categories.jsonl').open('a') as journal:
        for category in categories:
            page=1;category_ids=set()
            while True:
                records,count,url=korea_page('LINKED',page,category=category)
                ids={r['external_id'] for r in records};new=ids-seen
                if new:store_raw('korea',[r for r in records if r['external_id'] in new])
                category_ids.update(ids);seen.update(ids)
                journal.write(dump({'category':category,'page':page,'advertised':count,'url':url,'ids':sorted(ids)})+'\n');journal.flush()
                if page*1000>=count:break
                page+=1;time.sleep(.15)
            parts[category]={'advertised':count,'unique':len(category_ids)}
            report('korea-linked-repair',{'status':'running','advertised':total,'unique':len(seen),'remaining_count_gap':max(0,total-len(seen)),'category_parts':parts})
            if len(seen)>=total:break
    result={'status':'count_reconciled' if len(seen)==total else 'unreconciled','advertised':total,'unique':len(seen),
            'newly_recovered':len(seen)-initial,'remaining_count_gap':max(0,total-len(seen)),'category_parts':parts,
            'scope':'전체 목록 3개 정렬과 공식 분류별 목록의 고유 ID 합집합 대조'}
    report('korea-linked-repair',result)
    main=json.loads((ROOT/'korea.json').read_text())
    main['parts']['LINKED'].update({'status':result['status'],'unique':len(seen),'advertised':total})
    main['unique']=sum(v['unique'] for v in main['parts'].values())
    main['status']='count_reconciled' if all(v['status']=='count_reconciled' for v in main['parts'].values()) else 'unreconciled'
    report('korea',main)

def collect_singapore():
    from collectors import fetch
    endpoint='https://api-production.data.gov.sg/v2/public/api/datasets'
    first=fetch(endpoint+'?page=1')['data'];total=int(first['totalRowCount']);seen=set();statuses=collections.Counter();received=0
    previous=set()
    old=ROOT/'singapore-inventory.jsonl'
    if old.exists():
        for line in old.read_text().splitlines():previous.update(json.loads(line)['ids'])
    def page(n):
        d=first if n==1 else fetch(endpoint+'?page='+str(n))['data']
        time.sleep(.15)
        return n,d
    with (ROOT/'singapore-inventory.jsonl').open('a') as journal:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for n,d in pool.map(page,range(1,int(first['pages'])+1)):
                records=[]
                for r in d['datasets']:
                    id=r['datasetId'];statuses[r.get('status')]+=1
                    records.append({'external_id':id,'title':r['name'],'description':r.get('description') or '',
                        'publisher':r.get('managedByAgencyName') or '싱가포르 정부','url':'https://data.gov.sg/datasets/'+id+'/view',
                        'metadata_url':endpoint,'region':'싱가포르','source_availability':r.get('status'),
                        'period':' ~ '.join(filter(None,[r.get('coverageStart'),r.get('coverageEnd')])) or '미상',
                        'format':r.get('format') or '미상','source_modified':r.get('lastUpdatedAt') or ''})
                ids={r['external_id'] for r in records}
                if not ids or ids.issubset(seen):raise ValueError('Empty or repeated Singapore page')
                store_raw('singapore',records);seen.update(ids);received+=len(records)
                journal.write(dump({'page':n,'advertised':int(d['totalRowCount']),'ids':sorted(ids)})+'\n');journal.flush()
                if n%100==0:report('singapore',{'status':'running','unique':len(seen),'advertised':total,'page':n})
    report('singapore',{'combined_unique':len(seen|previous),'scope':'공식 페이지 목록 고유 ID 교차 순회. 수집 시점 사이 변경 가능.','status':'count_reconciled' if len(seen|previous)==total==int(d['totalRowCount']) else 'unreconciled',
                       'advertised':int(d['totalRowCount']),'initial_total':total,'unique':len(seen),'received':received,'status_counts':dict(statuses)})

def verify_singapore_missing():
    from collectors import fetch
    current=set()
    for line in (ROOT/'singapore-inventory.jsonl').read_text().splitlines():current.update(json.loads(line)['ids'])
    previous=json.loads((ROOT/'singapore.json').read_text());verified=[];failed=[]
    with database() as db:known={r[0].split(':',1)[1]:json.loads(r[1]) for r in db.execute("SELECT id,metadata FROM datasets WHERE source_id='singapore'")}
    for id in sorted(known.keys()-current):
        url='https://api-production.data.gov.sg/v2/public/api/datasets/'+id+'/metadata'
        try:
            r=fetch(url)['data']
            if r['datasetId']!=id:raise ValueError('Dataset ID mismatch')
            record={**known[id],'title':r['name'],'description':r.get('description') or '',
                    'publisher':r.get('managedBy') or known[id]['publisher'],'metadata_url':url,
                    'period':' ~ '.join(filter(None,[r.get('coverageStart'),r.get('coverageEnd')])) or '미상',
                    'source_modified':r.get('lastUpdatedAt') or '','source_availability':'metadata_accessible'}
            store_raw('singapore',[record]);verified.append({'id':id,'url':url})
        except Exception as error:failed.append({'id':id,'url':url,'error':str(error)})
    combined=current|{x['id'] for x in verified}
    (ROOT/'singapore-independent-evidence.json').write_text(dump({'verified':verified,'failed':failed}))
    report('singapore-independent',{'status':'provider_inventory_disagreement' if len(combined)==previous['advertised'] else 'unreconciled',
        'provider_count':previous['advertised'],'list_unique':len(current),'metadata_verified_extra':len(verified),'combined_unique':len(combined),
        'remaining_count_gap':max(0,previous['advertised']-len(combined)),'failures':failed,'scope':'페이지 목록에서 빠진 기존 ID를 공식 단일 메타데이터 API로 재확인'})

def collect_seoul():
    home=read_url('https://data.seoul.go.kr/index.do').decode()
    links=re.findall(r'https://data\.seoul\.go\.kr/together/notice/datasetNoticeView\.do\?[^\'"<>]+',home)
    if not links:raise ValueError('Monthly catalog notice not found')
    notice=links[0];text=read_url(notice).decode()
    files=re.findall(r'data-fileNm="([^"]+\.xlsx)"',text);codes=re.findall(r'data-bbsCd="([^"]+)"',text)
    if not files or not codes:raise ValueError('Monthly catalog attachment not found')
    url='https://data.seoul.go.kr/together/notice/fileDownload.do?'+urllib.parse.urlencode({'bbsCd':codes[0],'fileNm':files[0]})
    raw=read_url(url);z=zipfile.ZipFile(io.BytesIO(raw));ns={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    strings=[''.join(x.itertext()) for x in ET.fromstring(z.read('xl/sharedStrings.xml')).findall('s:si',ns)]
    sheet=ET.fromstring(z.read('xl/worksheets/sheet1.xml'));rows=sheet.findall('.//s:row',ns)
    records=[];statuses=collections.Counter();ids=set()
    def date(value):
        try:return (datetime(1899,12,30)+timedelta(days=float(value))).date().isoformat()
        except (ValueError,OverflowError):return value
    for row in rows[1:]:
        cells={}
        for c in row.findall('s:c',ns):
            v=c.find('s:v',ns);v=v.text if v is not None else ''
            cells[re.sub(r'\d','',c.get('r',''))]=strings[int(v)] if c.get('t')=='s' and v else v
        if not cells.get('A'):continue
        if not public_url(cells.get('I')):raise ValueError('Monthly catalog has invalid landing URL')
        id=cells['A'];ids.add(id);statuses[cells.get('K','')]+=1
        records.append({'external_id':id,'title':cells['C'],'description':cells.get('D','').replace('_x000D_',''),
                        'publisher':'서울특별시 / '+cells.get('F',''),'url':cells['I'],'metadata_url':url,'region':'서울특별시',
                        'source_modified':date(cells.get('H','')),'native_type':cells.get('B'),'source_availability':cells.get('K'),
                        'subjects':[{'kind':'theme','label':cells.get('E','')},{'kind':'tag','label':cells.get('J','')}],
                        'collection_method':'official_monthly_catalog','catalog_edition':files[0]})
    for start in range(0,len(records),200):store_raw('seoul',records[start:start+200])
    (ROOT/'seoul-ids.json').write_text(dump(sorted(ids)))
    live=read_url('https://data.seoul.go.kr/dataList/datasetList.do?datasetKind=&searchFlag=M').decode()
    total=re.search(r'검색결과[^<]*<strong>([\d,]+)',live)
    report('seoul',{'status':'snapshot_imported','rows':len(records),'unique':len(ids),'snapshot':files[0],
                     'snapshot_sha256':hashlib.sha256(raw).hexdigest(),'status_counts':dict(statuses),
                     'live_search_total':int(total[1].replace(',','')) if total else None,
                     'scope':'공식 월간 전체 목록. 실시간 목록과 시점·서비스 상태 차이가 있어 전체 최신 수집 완료로 처리하지 않음.',
                     'evidence_url':notice})


def guarded(source, fn):
    with (ROOT/(source+".lock")).open("a") as lock:
        try:fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:raise BlockingIOError("이 출처의 목록 수집이 이미 실행 중입니다.")
        try:fn()
        except Exception as error:
            previous=json.loads((ROOT/(source+'.json')).read_text()) if (ROOT/(source+'.json')).exists() else {}
            report(source,{**previous,'status':'failed','error':type(error).__name__+': '+str(error)})
            raise


CKAN_CATALOGS = {
    'uk':('https://ckan.publishing.service.gov.uk/api/3/action/package_search','https://www.data.gov.uk/dataset/'),
    'australia':('https://data.gov.au/data/api/3/action/package_search','https://data.gov.au/data/dataset/'),
    'switzerland':('https://ckan.opendata.swiss/api/3/action/package_search','https://opendata.swiss/en/dataset/'),
    'ireland':('https://data.gov.ie/api/3/action/package_search','https://data.gov.ie/dataset/'),
    'netherlands':('https://data.overheid.nl/data/api/3/action/package_search','https://data.overheid.nl/dataset/'),
    'japan':('https://data.e-gov.go.jp/data/ja/api/3/action/package_search','https://data.e-gov.go.jp/data/ja/dataset/'),
    'canada':('https://open.canada.ca/data/en/api/3/action/package_search','https://open.canada.ca/data/en/dataset/'),
    'finland':('https://avoindata.suomi.fi/data/fi/api/3/action/package_search','https://avoindata.suomi.fi/data/fi/dataset/'),
    'germany':('https://www.govdata.de/ckan/api/3/action/package_search','https://www.govdata.de/suche/daten/'),
    'newzealand':('https://catalogue.data.govt.nz/api/3/action/package_search','https://catalogue.data.govt.nz/dataset/'),
}


def canonical_url(url):
    # Keep query parameters, case-sensitive paths and edition identifiers.
    try:p=urllib.parse.urlsplit((url or '').strip())
    except (ValueError,AttributeError):return ''
    if p.scheme not in ('http','https') or not p.hostname or p.username:return ''
    return urllib.parse.urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path,p.query,''))


def ckan_record(d, endpoint, landing, source_id):
    from collectors import wording, source_subjects
    organization=d.get('organization') or {}
    publisher=wording(organization.get('title')) if isinstance(organization,dict) else str(organization)
    urls=d.get('res_url') or [r.get('url') for r in d.get('resources',[])]
    if isinstance(urls,str):urls=[urls]
    paths=sorted({canonical_url(url) for url in urls if isinstance(url,str) and canonical_url(url)})
    return {'external_id':d['id'],'title':wording(d.get('title')) or wording(d.get('title_translated')) or d.get('name') or d['id'],
            'description':wording(d.get('notes')) or wording(d.get('description')) or wording(d.get('notes_translated')),
            'publisher':publisher or '제공처 확인','url':landing+(d['id']+'/'+d['name'] if source_id=='uk' and d.get('name') else d['id'] if source_id=='canada' else d.get('name') or d['id']),
            'metadata_url':endpoint.replace('package_search','package_show')+'?id='+urllib.parse.quote(d['id']),
            'source_modified':d.get('metadata_modified') or '', 'license':wording(d.get('license_title')) or wording(d.get('license_id')) or '제공처 확인',
            'native_record_type':d.get('type'),'access_paths':paths,'upstream_url':d.get('url') or '', 'native_id':d['id'],'native_name':d.get('name'),
            'subjects':source_subjects(d),'collection_method':'official_catalog_api'}


def collect_ckan(source_id):
    endpoint,landing=CKAN_CATALOGS[source_id]
    offset=0;size=1000;seen=set();initial=None;repeats=0;added=0
    with database() as db:
        before={r[0] for r in db.execute('SELECT id FROM datasets WHERE source_id=?',(source_id,))}
    aliases={}
    with database() as db:
        for r in db.execute("SELECT id,metadata FROM datasets WHERE source_id=? AND json_type(metadata,'$.aliases')='array'",(source_id,)):
            for alias in json.loads(r['metadata']).get('aliases',[]):aliases[alias['id']]=r['id']
    fields='id,name,type,title,title_translated,notes,notes_translated,description,organization,metadata_modified,license_id,license_title,tags,groups,theme,res_url,url,identifier'
    inventory=ROOT/(source_id+'-inventory.jsonl')
    previous_report=json.loads((ROOT/(source_id+'.json')).read_text()) if (ROOT/(source_id+'.json')).exists() else {}
    if inventory.exists() and previous_report.get('status') in ('failed','running','unreconciled'):
        for line in inventory.read_text().splitlines():
            saved=json.loads(line)
            if 'items' not in saved:continue
            if initial is None:initial=saved['advertised']
            seen.update(d['id'] for d in saved['items'])
            offset=saved['offset']+len(saved['items'])
    with inventory.open('a' if offset else 'w') as journal:
        while True:
            params={'rows':size,'start':offset,'sort':'id asc'}
            # AU's full resource objects can exceed response limits; Solr exposes URLs.
            if source_id=='australia':params['fl']=fields
            url=endpoint+'?'+urllib.parse.urlencode(params)
            try:payload=json.loads(read_url(url))
            except ValueError as error:
                if 'size limit' not in str(error) or size==1:raise
                size=max(1,size//2);continue
            if payload.get('success') is not True:raise ValueError('Provider returned unsuccessful catalog response')
            result=payload['result'];items=result['results'];total=int(result['count'])
            if initial is None:initial=total
            ids={d['id'] for d in items}
            if not ids and offset<total:raise ValueError('Empty catalog page before advertised end')
            if ids and ids.issubset(seen):raise ValueError('Repeated page without new source identifiers')
            repeats+=len(items)-len(ids)+len(ids&seen);seen.update(ids)
            records=[]
            page_ids=[source_id+':'+d['id'] for d in items]
            with database() as db:
                previous={r['id']:json.loads(r['metadata']) for r in db.execute('SELECT id,metadata FROM datasets WHERE id IN ('+','.join('?' for _ in page_ids)+')',page_ids)} if page_ids else {}
            for d in items:
                full_id=source_id+':'+d['id']
                if full_id in aliases:continue
                record=ckan_record(d,endpoint,landing,source_id)
                if full_id in before:
                    # Retain fields omitted by source projections, including time coverage.
                    old=previous.get(full_id,{})
                    record={**old,**record}
                    if record['publisher']=='제공처 확인':record['publisher']=old.get('publisher','제공처 확인')
                else:added+=1
                records.append(record)
            for start in range(0,len(records),200):store_raw(source_id,records[start:start+200])
            journal.write(dump({'offset':offset,'advertised':total,'url':url,'items':[{'id':d['id'],'name':d.get('name'),'res_url':d.get('res_url') or [r.get('url') for r in d.get('resources',[])]} for d in items]})+'\n');journal.flush()
            offset+=len(items)
            if offset>=total:break
            if offset%5000<size:report(source_id,{'status':'running','advertised':total,'unique':len(seen),'offset':offset,'new_records':added})
            time.sleep(.15)
    with database() as db:
        stored={r[0] for r in db.execute('SELECT id FROM datasets WHERE source_id=?',(source_id,))}
    remote={source_id+':'+id for id in seen}
    covered=stored|{alias for alias,target in aliases.items() if target in stored}
    missing=sorted(remote-covered);stale=sorted(stored-remote)
    (ROOT/(source_id+'-difference.json')).write_text(dump({'missing':missing,'not_in_current_provider_list':stale}))
    status='count_reconciled' if len(seen)==total and initial==total and not missing else 'unreconciled'
    report(source_id,{'status':status,'advertised':total,'initial_total':initial,'received':offset,'unique':len(seen),
                      'repeated':repeats,'new_records':added,'missing':len(missing),'not_in_current_provider_list':len(stale),
                      'evidence':'collection-audit/'+source_id+'-inventory.jsonl','scope':'공식 공개 카탈로그의 ID 전체 순회. 원본 파일 내용은 수집하지 않음.'})


def udata_subjects(record):
    subjects=[{'kind':'tag','label':t} for t in record.get('tags',[])]
    for resource in record.get('resources',[]):
        for key,kind in [('title','resource_title'),('description','resource_description')]:
            if resource.get(key):subjects.append({'kind':kind,'label':clean(resource[key])[:4000]})
        schema=resource.get('schema') or {}
        if isinstance(schema,dict) and schema.get('name'):
            subjects.append({'kind':'theme','label':schema['name']})
    return list({dump(s):s for s in subjects}.values())


def collect_udata(source_id):
    endpoint={'france':'https://www.data.gouv.fr/api/1/datasets/','portugal':'https://dados.gov.pt/api/1/datasets/'}[source_id]
    seen=set();page=1;initial=None;received=0
    with (ROOT/(source_id+'-public-catalog.jsonl')).open('w') as journal:
        while True:
            url=endpoint+'?'+urllib.parse.urlencode({'page_size':1000,'page':page,'sort':'created'})
            fields='data{id,title,description,page,organization{name,badges},license,resources{format,url,title,description,schema},last_modified,tags},total,page,page_size,next_page'
            d=json.loads(read_url(url,fields=fields));total=int(d['total']);items=d['data']
            if int(d['page_size'])!=1000:raise ValueError('Provider page size changed; no cursor advancement')
            if initial is None:initial=total
            ids={x['id'] for x in items}
            if ids and ids.issubset(seen):raise ValueError('Repeated source page')
            if not ids and received<total:raise ValueError('Empty page before provider end')
            records=[]
            for x in items:
                org=x.get('organization') or {}
                records.append({'external_id':x['id'],'title':x['title'],'description':x.get('description') or '',
                                'publisher':org.get('name') or '제공처 확인','publisher_badges':[b.get('kind') for b in org.get('badges',[])],'url':x['page'],'metadata_url':endpoint+x['id']+'/',
                                'source_modified':x.get('last_modified') or '', 'license':x.get('license') or '제공처 확인',
                                'access_paths':sorted({canonical_url(r.get('url')) for r in x.get('resources',[]) if canonical_url(r.get('url'))}),
                                'subjects':udata_subjects(x),'collection_method':'official_catalog_api'})
            store_raw(source_id,records);seen.update(ids);received+=len(items)
            journal.write(dump({'page':page,'advertised':total,'url':url,'ids':sorted(ids)})+'\n');journal.flush()
            if page*int(d['page_size'])>=total:break
            if page%20==0:report(source_id,{'status':'running','advertised':total,'unique':len(seen),'page':page})
            page+=1;time.sleep(.15)
    with database() as db:stored={r[0] for r in db.execute('SELECT id FROM datasets WHERE source_id=?',(source_id,))}
    remote={source_id+':'+id for id in seen};missing=remote-stored;stale=stored-remote
    (ROOT/(source_id+'-difference.json')).write_text(dump({'missing':sorted(missing),'not_in_current_provider_list':sorted(stale)}))
    report(source_id,{'status':'count_reconciled' if len(seen)==total and initial==total and not missing else 'unreconciled',
                      'advertised':total,'initial_total':initial,'unique':len(seen),'received':received,'missing':len(missing),
                      'not_in_current_provider_list':len(stale),'scope':'공식 포털의 인증 없이 공개된 전체 자료 목록. 정부·민간 게시자 배지는 원문대로 보존.'})


def collect_seoul_live():
    def page(number):
        url='https://data.seoul.go.kr/dataList/datasetList.do?'+urllib.parse.urlencode({'datasetKind':'','searchFlag':'M','pageIndex':number,'sortColBy':'A'})
        text=read_url(url).decode()
        match=re.search(r'검색결과[^<]*<strong>([\d,]+)',text)
        if not match:raise ValueError('Live Seoul result count missing')
        total=int(match[1].replace(',',''));records=[]
        for block in re.findall(r'<dl class="type-[ab]">(.*?)</dl>',text,re.S):
            link=re.search(r'data-rel="([^\"]+/datasetView.do)" class="goView">(.*?)</a>',block,re.S)
            if not link:continue
            path,title=link.groups();id=path.split('/')[0]
            desc=re.search(r'<dd class="list-statistics-info1">(.*?)</dd>',block,re.S)
            publisher=re.search(r'<b>제공기관 :</b>(.*?)</span>',block,re.S)
            modified=re.search(r'<b>수정일자 :</b>(.*?)</span>',block,re.S)
            records.append({'external_id':id,'title':clean(title),'description':clean(desc[1]) if desc else '',
                            'publisher':clean(publisher[1]) if publisher else '서울특별시','source_modified':clean(modified[1]) if modified else '',
                            'url':'https://data.seoul.go.kr/dataList/'+path,'metadata_url':url,'source_availability':'서비스 중','region':'서울특별시'})
        if not records and (number-1)*10<total:raise ValueError('Empty Seoul page before end: '+str(number))
        time.sleep(.2)
        return records,total,url
    first,total,url=page(1);seen=set();received=0;missing=[]
    with database() as db:
        # Monthly statistics rows have OA IDs but DT landing IDs; match their exact URL.
        existing={json.loads(r['metadata'])['url']:json.loads(r['metadata']) for r in db.execute("SELECT metadata FROM datasets WHERE source_id='seoul'")}
    with (ROOT/'seoul-live-inventory.jsonl').open('w') as journal:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for number,(records,count,url) in enumerate(pool.map(page,range(1,(total+9)//10+1)),1):
                ids=[];updates=[]
                for r in records:
                    old=existing.get(r['url'])
                    if old:
                        id=old['external_id']
                        updated={**old,**r,'external_id':id,'source_availability':'서비스 중','live_verified_at':now()}
                        if not r.get('description'):updated['description']=old.get('description','')
                        if r.get('publisher')=='제공처 확인':updated['publisher']=old.get('publisher','제공처 확인')
                        updates.append(updated)
                    else:
                        id=r['external_id'];missing.append(id);updates.append(r)
                    ids.append(id)
                store_raw('seoul',updates);seen.update(ids);received+=len(ids)
                journal.write(dump({'page':number,'advertised':count,'url':url,'ids':ids})+'\n');journal.flush()
                if number%100==0:report('seoul-live',{'status':'running','advertised':total,'unique':len(seen),'page':number})
    with database() as db:stored={r[0].split(':',1)[1] for r in db.execute("SELECT id FROM datasets WHERE source_id='seoul'")}
    (ROOT/'seoul-live-difference.json').write_text(dump({'new_since_snapshot':missing,'not_in_current_list':sorted(stored-seen)}))
    report('seoul-live',{'status':'count_reconciled' if len(seen)==total==count else 'unreconciled',
                         'advertised':count,'initial_total':total,'unique':len(seen),'received':received,'new_since_snapshot':len(missing),
                         'not_in_current_list':len(stored-seen),'scope':'실시간 전체 검색 목록, 제목순 순회. 월간 목록의 종료·과거 자료는 별도 보존.'})


def eurostat_subjects(raw):
    root=ET.fromstring(raw);ns={'n':'urn:eu.europa.ec.eurostat.navtree'};result={}
    def title(node):
        return next((clean(x.text or '') for x in node.findall('n:title',ns) if x.get('language')=='en'),'')
    def visit(node,path):
        kind=node.tag.rsplit('}',1)[-1]
        if kind=='branch':path=path+[title(node)]
        if kind=='leaf':
            code=node.findtext('n:code',default='',namespaces=ns)
            if code:
                subjects=result.setdefault(code,[])
                # Keep source hierarchy order; the nearest subject is the most specific.
                for name in reversed(path[1:]):
                    entry={'kind':'tag','label':name,'metadata_url':'https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml'}
                    if name and entry not in subjects:subjects.append(entry)
                for description in node.findall('n:description',ns):
                    if description.get('language')=='en' and clean(description.text or ''):
                        subjects.append({'kind':'resource_description','label':clean(description.text)})
        for child in node:
            if child.tag.rsplit('}',1)[-1] in ('branch','leaf','children'):visit(child,path)
    visit(root,[]);return result


def collect_eurostat():
    import csv
    url='https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en'
    raw=read_url(url);rows=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig')),delimiter='\t'))
    records={};folders=0
    subjects=eurostat_subjects(read_url('https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml',limit=64_000_000))
    for d in rows:
        d={k.strip().strip('"'):v.strip().strip('"') for k,v in d.items() if k}
        if d.get('type')=='folder':folders+=1;continue
        id=d.get('code')
        if not id:raise ValueError('Eurostat TOC schema not recognized')
        records[id]={'external_id':id,'title':d['title'],'description':'','publisher':'Eurostat',
                     'url':'https://ec.europa.eu/eurostat/databrowser/view/'+urllib.parse.quote(id)+'/default/table?lang=en',
                     'metadata_url':url,'period':' / '.join([d.get('data start',''),d.get('data end','')]),
                     'native_type':d.get('type'),'subjects':subjects.get(id,[]),'source_modified':d.get('last update of data',''),'collection_method':'official_catalog_export'}
    values=list(records.values())
    for start in range(0,len(values),200):store_raw('eurostat',values[start:start+200])
    (ROOT/'eurostat-ids.json').write_text(dump(sorted(records)))
    report('eurostat',{'status':'export_reconciled','export_rows':len(rows),'unique':len(records),'folders_excluded':folders,
                       'repeated_navigation_entries':len(rows)-folders-len(records),'sha256':hashlib.sha256(raw).hexdigest(),'evidence_url':url})


def collect_gyeonggi():
    page=1;seen=set();entries=set();received=0;initial=None
    with (ROOT/'gyeonggi-inventory.jsonl').open('w') as journal:
        while True:
            url='https://data.gg.go.kr/portal/data/dataset/searchDataset.do?'+urllib.parse.urlencode({'page':page,'size':50,'sort':'name'})
            d=json.loads(read_url(url))['result'];meta=d['pageInfo'];items=d['contents'];total=int(meta['totalElements'])
            if initial is None:initial=total
            if int(meta['currentPage'])!=page:raise ValueError('Provider clamped page')
            if not items and received<total:raise ValueError('Empty page before end')
            records=[]
            for x in items:
                id=x['infId'];records.append({'external_id':id,'title':x['infNm'],'description':html.unescape(x.get('infExp') or ''),
                    'publisher':'경기도 / 상세 제공기관 확인','url':'https://data.gg.go.kr/portal/data/service/selectServicePage.do?'+urllib.parse.urlencode({'infId':id,'infSeq':x['infSeq']}),
                    'metadata_url':url,'region':'경기도','source_modified':x.get('updDttm') or '',
                    'format':' / '.join(str(x[k]) for k in ('scolInfNm','ccolInfNm','mcolInfNm','fileInfNm','acolInfNm','linkInfNm') if x.get(k)),
                    'subjects':[{'kind':'theme','label':x.get('topCateNm') or ''}],'native_id':id,'native_sequence':x['infSeq'],
                    'collection_method':'official_catalog_api'})
            ids={r['external_id'] for r in records}
            page_entries={str(x['infId'])+':'+str(x['infSeq']) for x in items}
            if page_entries and page_entries.issubset(entries):raise ValueError('Repeated catalog page')
            entries.update(page_entries)
            store_raw('gyeonggi',records);seen.update(ids);received+=len(items)
            journal.write(dump({'page':page,'advertised':total,'ids':sorted(ids),'entries':sorted(page_entries),'url':url})+'\n');journal.flush()
            if page>=int(meta['totalPages']):break
            page+=1;time.sleep(.2)
    report('gyeonggi',{'status':'count_reconciled' if len(entries)==total==initial else 'unreconciled',
                       'advertised':total,'initial_total':initial,'unique_datasets':len(seen),'unique_catalog_entries':len(entries),'received':received,'scope':'경기데이터드림 목록 항목 전체 대조. 동일 infId의 서비스 경로는 한 자료 안에 보존'})


def collect_who():
    url='https://ghoapi.azureedge.net/api/Indicator';next_url=url;records={};received=0;visited=set()
    while next_url:
        if next_url in visited or urllib.parse.urlsplit(next_url).hostname!='ghoapi.azureedge.net':raise ValueError('Invalid next catalog page')
        visited.add(next_url);d=json.loads(read_url(next_url));items=d['value'];received+=len(items)
        for x in items:
            id=x['IndicatorCode'];records[id]={'external_id':id,'title':x['IndicatorName'],'description':'','publisher':'World Health Organization',
                'url':url+"?$filter="+urllib.parse.quote("IndicatorCode eq '"+id+"'"),
                'metadata_url':url+"?$filter="+urllib.parse.quote("IndicatorCode eq '"+id+"'"),
                'access_paths':['https://ghoapi.azureedge.net/api/'+urllib.parse.quote(id)],'native_type':'indicator','collection_method':'official_indicator_catalog'}
        next_url=d.get('@odata.nextLink')
    values=list(records.values())
    for start in range(0,len(values),200):store_raw('who',values[start:start+200])
    (ROOT/'who-ids.json').write_text(dump(sorted(records)))
    report('who',{'status':'export_reconciled','received':received,'unique':len(records),'scope':'GHO 지표 정의 목록. 관측값·국가별 행은 제외.','evidence_url':url})


def collect_series(source_id):
    from collectors import metadata_page
    records,total,received,cursor,done=metadata_page(source_id,1)
    if not done:raise ValueError('Series collector requires further pages')
    if source_id=='census':
        items=json.loads(read_url('https://api.census.gov/data.json',64_000_000))['dataset']
        paths=collections.defaultdict(list)
        for d in items:
            key='/'.join(d.get('c_dataset') or [])
            if not key or d.get('accessLevel')!='public':continue
            paths['series-'+key].append({'year':d.get('c_vintage'),'url':d.get('c_examplesLink') or d.get('c_documentationLink'),
                                        'identifier':d.get('identifier'),'distribution':d.get('distribution',[])})
        for r in records:r['editions']=paths[r['external_id']]
    store_raw(source_id,records)
    ids={source_id+':'+r['external_id'] for r in records}
    with database() as db:stored={r[0] for r in db.execute('SELECT id FROM datasets WHERE source_id=?',(source_id,))}
    (ROOT/(source_id+'-ids.json')).write_text(dump(sorted(ids)))
    report(source_id,{'status':'count_reconciled' if not ids-stored else 'unreconciled','provider_entries':total,'received':received,
                      'unique_series':len(ids),'missing_series':len(ids-stored),'not_in_current_series':len(stored-ids),
                      'scope':'자료 주체인 시리즈·데이터베이스 단위. 연도판은 메타데이터 내부에 보존.'})





def collect_chungbuk():
    base='https://data.chungbuk.go.kr/portal/bigdata/'
    url=base+'bigdataListView.do?ctCode=CT64549453&sitemapCode=SM00000127&searchChkAll=checkAll&pageIndex=1&pageUnit=1000'
    text=read_url(url).decode();m=re.search(r'전체 <strong>([\d,]+)</strong>',text)
    if not m:raise ValueError('Chungbuk catalog total missing')
    total=int(m[1].replace(',',''));records=[];ids=set()
    for block in re.findall(r'<tr[^>]*>(.*?)</tr>',text,re.S):
        link=re.search(r"fncDetailView\('([^']+)', '([^']+)'\);\">(.*?)</a>",block,re.S)
        if not link:continue
        id,seq,title=link.groups();cells=[clean(v) for v in re.findall(r'<td[^>]*>(.*?)</td>',block,re.S)]
        dest=base+'bigdataDetailView.do?'+urllib.parse.urlencode({'ctCode':'CT64549453','sitemapCode':'SM00000127','dtst_dtl_cd':id}) if seq=='0' else base+'bigdataClctDetailJson.do?'+urllib.parse.urlencode({'ctCode':'CT64549453','sitemapCode':'SM00000127','clctLogSn':seq})
        records.append({'external_id':id+(':'+seq if seq!='0' else ''),'title':clean(title),'description':'',
                        'publisher':cells[3] if len(cells)>3 else '제공처 확인','native_origin':cells[4] if len(cells)>4 else '','url':dest,'metadata_url':url,
                        'subjects':([{'kind':'theme','label':cells[1]}] if len(cells)>1 else [])+([{'kind':'tag','label':cells[4]}] if len(cells)>4 else []),'native_catalog_cells':cells,'collection_method':'official_catalog_html'})
        ids.add(records[-1]['external_id'])
    if len(records)!=total:raise ValueError(f'Catalog has more pages or parser missed records: {len(records)}/{total}')
    for start in range(0,len(records),200):store_raw('chungbuk',records[start:start+200])
    (ROOT/'chungbuk-ids.json').write_text(dump(sorted(ids)))
    report('chungbuk',{'status':'count_reconciled' if len(ids)==total else 'unreconciled','advertised':total,'unique':len(ids),'received':len(records),'evidence_url':url})


def collect_busan(order='asc'):
    endpoint='https://data.busan.go.kr/bdip/srh/getPublicDataListSearch.do';offset=0;seen=set();initial=None
    with (ROOT/('busan-'+order+'-inventory.jsonl')).open('w') as journal:
        while True:
            params={'searchSort':'title','dataTy':'','brmnCd':'','insttCd':'','offset':offset,'pagelength':100,'listOrderCd':order}
            raw=read_url(endpoint,payload=json.dumps(params).encode(),content_type='application/json')
            result=json.loads(raw)['result'];items=result['rows'];total=int(result['total_count'])
            if initial is None:initial=total
            if not items and offset<total:raise ValueError('Empty Busan page before end')
            records=[]
            for item in items:
                d=item['fields'];id=d['PUBLICDATAPK']
                records.append({'external_id':id,'title':d['PUBLICDATASJ'],'description':d.get('PUBLICDATADC') or '',
                                'publisher':d.get('INSTTNM') or '제공처 확인','url':'https://data.busan.go.kr/bdip/opendata/detail.do?publicdatapk='+urllib.parse.quote(id),
                                'metadata_url':endpoint,'native_id':id,'native_type':d.get('DATATY'),'source_modified':d.get('UPDTDT') or '',
                                'subjects':[{'kind':'theme','label':d.get('BRMNNM') or ''},{'kind':'tag','label':d.get('KEYWORD') or ''}],
                                'collection_method':'official_catalog_search'})
            ids={r['external_id'] for r in records}
            if ids and ids.issubset(seen):raise ValueError('Repeated Busan catalog page')
            store_raw('busan',records);seen.update(ids)
            journal.write(dump({'offset':offset,'advertised':total,'ids':sorted(ids),'request':params})+'\n');journal.flush();offset+=len(items)
            if offset>=total:break
            if offset%2000==0:report('busan',{'status':'running','unique':len(seen),'advertised':total,'offset':offset})
            time.sleep(.2)
    before=set()
    old=ROOT/'busan-inventory.jsonl'
    if old.exists():
        for line in old.read_text().splitlines():before.update(json.loads(line)['ids'])
    union=seen|before
    report('busan',{'combined_unique':len(union),'remaining_count_gap':max(0,total-len(union)),'status':'count_reconciled' if len(union)==total==initial else 'unreconciled','advertised':total,'initial_total':initial,'received':offset,'unique':len(seen),'scope':'부산 공식 데이터 카탈로그 전체 공개 검색 목록'})

def collect_kosis():
    import xlrd
    url='https://kosis.kr/downXLS/ZTITLE.zip'
    raw=read_url(url,60_000_000);archive=zipfile.ZipFile(io.BytesIO(raw))
    members=[m for m in archive.infolist() if m.filename.endswith('.xls')]
    if len(members)!=1 or members[0].file_size>400_000_000:raise ValueError('Unexpected KOSIS metadata export')
    book=xlrd.open_workbook(file_contents=archive.read(members[0]),on_demand=True)
    ids=set();rows=0;folders=0;failures=[];snapshot=None
    with (ROOT/'kosis-export-ids.jsonl').open('w') as journal:
        for si in range(book.nsheets):
            sheet=book.sheet_by_index(si)
            stamp=str(sheet.cell_value(1,0)).strip()
            if snapshot is None:snapshot=stamp
            if stamp!=snapshot:raise ValueError('Mixed KOSIS snapshot dates')
            batch=[]
            for ri in range(3,sheet.nrows):
                values=sheet.row_values(ri)
                if len(values)<8:raise ValueError('KOSIS metadata columns changed')
                table=str(values[7]).strip()
                if table=='통계표 아이디(TBL_ID)':continue
                rows+=1
                if not table:folders+=1;continue
                link=sheet.hyperlink_map.get((ri,2))
                href=link.url_or_path if link else ''
                parts=urllib.parse.urlsplit(href)
                params=urllib.parse.parse_qs(parts.query)
                org=params.get('orgId',params.get('in_org_id',['']))[0];tid=params.get('tblId',params.get('in_tbl_id',['']))[0]
                if parts.hostname not in ('kosis.kr','stat.kosis.kr') or not org or tid!=table:
                    failures.append({'sheet':si+1,'row':ri+1,'table':table,'url':href,'reason':'통계표 링크와 식별자 불일치'});continue
                id=org+':'+table
                batch.append({'external_id':id,'title':clean(str(values[1])),'description':'',
                    'publisher':str(values[3]).strip(),'period':str(values[4]).strip() or '미상',
                    'url':href if parts.hostname=='stat.kosis.kr' else 'https://kosis.kr/statHtml/statHtml.do?'+urllib.parse.urlencode({'orgId':org,'tblId':table}),
                    'metadata_url':url,'native_id':table,'native_publisher_id':org,'native_catalog_path':str(values[5]).strip(),
                    'source_export_date':snapshot,'source_export_link':href,'collection_method':'official_catalog_export'})
                ids.add(id)
                journal.write(dump({'id':id,'sheet':si+1,'row':ri+1,'url':href})+'\n')
                if len(batch)>=200:store_raw('kosis',batch);batch=[]
            if batch:store_raw('kosis',batch)
            journal.flush();book.unload_sheet(si)
            report('kosis',{'status':'running','snapshot':snapshot,'sheets_processed':si+1,'sheets':book.nsheets,'unique':len(ids),'export_rows':rows})
    book.release_resources()
    with database() as db:stored={r[0].split(':',1)[1] for r in db.execute("SELECT id FROM datasets WHERE source_id='kosis'")}
    (ROOT/'kosis-export-difference.json').write_text(dump({'missing':sorted(ids-stored),'not_in_export':sorted(stored-ids),'unparsed':failures}))
    report('kosis',{'status':'export_reconciled' if not failures and not ids-stored else 'unreconciled','snapshot':snapshot,
                    'export_rows':rows,'folders_excluded':folders,'unique':len(ids),'repeated_navigation_entries':rows-folders-len(failures)-len(ids),
                    'missing':len(ids-stored),'not_in_export':len(stored-ids),'unparsed':len(failures),'sha256':hashlib.sha256(raw).hexdigest(),
                    'evidence_url':url,'scope':'공식 주제별통계 MT_ZTITLE 전체 목록 파일. 파일에 명시된 기준일의 통계표이며 관측값은 제외.'})

def verify_ckan_inventory(source_id):
    endpoint,landing=CKAN_CATALOGS[source_id]
    url=endpoint.replace('package_search','package_list')
    d=json.loads(read_url(url,40_000_000))
    if d.get('success') is not True or not isinstance(d.get('result'),list):raise ValueError('Independent source inventory unavailable')
    names=d['result']
    if not all(isinstance(n,str) for n in names):raise ValueError('Unexpected inventory identifiers')
    names=set(names)
    count=json.loads(read_url(endpoint+'?rows=0'))['result']['count']
    inventory_agrees=len(names)==count
    (ROOT/(source_id+'-independent-names.json')).write_text(dump(sorted(names)))
    with database() as db:
        existing={json.loads(r['metadata']).get('native_name') or urllib.parse.urlsplit(json.loads(r['metadata'])['url']).path.rstrip('/').split('/')[-1] for r in db.execute('SELECT metadata FROM datasets WHERE source_id=?',(source_id,))}
    search_names=set()
    search_inventory=ROOT/(source_id+'-inventory.jsonl')
    if search_inventory.exists():
        for line in search_inventory.read_text().splitlines():
            search_names.update(x.get('name') for x in json.loads(line).get('items',[]))
    inspect=sorted((names-search_names)|(names-existing));added=[];failed=[];excluded=[];verified=[]
    def fetch_one(name):
        show=endpoint.replace('package_search','package_show')+'?'+urllib.parse.urlencode({'id':name})
        try:
            payload=json.loads(read_url(show))
            if payload.get('success') is not True:raise ValueError('Source package_show failed')
            native=payload['result'];kind=native.get('type')
            if kind in ('harvest','showcase'):
                return {'name':name,'id':source_id+':'+native['id'],'excluded_type':kind,'url':show}
            if kind!='dataset':raise ValueError('Unrecognized source record type: '+str(kind))
            record=ckan_record(native,endpoint,landing,source_id)
            with database() as db:previous=db.execute('SELECT metadata FROM datasets WHERE id=?',(source_id+':'+record['external_id'],)).fetchone()
            if previous:record={**json.loads(previous[0]),**record}
            store_raw(source_id,[record]);time.sleep(.15)
            return {'name':name,'id':source_id+':'+record['external_id'],'new_record':not bool(previous),'url':show}
        except Exception as error:return {'name':name,'url':show,'error':type(error).__name__+': '+str(error)}
    with (ROOT/(source_id+'-non-dataset-records.jsonl')).open('a') as excluded_log:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for index,item in enumerate(pool.map(fetch_one,inspect),1):
                if item.get('error'):failed.append(item)
                elif item.get('excluded_type'):
                    with database() as db:
                        row=db.execute('SELECT metadata,mappings FROM datasets WHERE id=?',(item['id'],)).fetchone()
                        excluded_log.write(dump({**item,'removed_metadata':json.loads(row[0]) if row else None,'mappings':json.loads(row[1]) if row else []})+'\n');excluded_log.flush()
                        db.execute('DELETE FROM datasets WHERE id=?',(item['id'],))
                    excluded.append(item)
                else:
                    verified.append(item)
                    if item['new_record']:added.append(item['id'])
                if index%50==0:
                    report(source_id+'-independent',{'status':'running','provider_unique_names':len(names),'provider_count':count,'inventory_only_to_inspect':len(inspect),'processed':index,'added':len(added),'non_dataset_excluded':len(excluded),'failed':len(failed)})
    (ROOT/(source_id+'-independent-failures.json')).write_text(dump(failed))
    dataset_inventory_count=len(names)-len(excluded)-len(failed)
    report(source_id+'-independent',{'status':'unreconciled' if failed else 'id_reconciled' if dataset_inventory_count==count else 'provider_inventory_disagreement',
        'provider_unique_names':len(names),'provider_count':count,'dataset_inventory_count':dataset_inventory_count,
        'inventory_only_checked':len(inspect),'added':len(added),'verified_inventory_only_datasets':len(verified),'non_dataset_excluded':len(excluded),
        'unresolved_inventory_entries':len(failed),'missing_after':len(failed),'not_in_current_name_list':len(existing-names),'evidence_url':url,
        'failures':failed[:15],'scope':'전체 ID 목록과 검색 목록 교차 대조. harvest(수집 설정)·showcase(활용 사례)는 자료에서 제외. 실패 ID의 자료 여부는 미확정.'})


def duplicate_key(record, by_url=False, cross_source=False):
    identity=tuple(clean(str(record.get(k) or '')).casefold() for k in ('source_id','title','description','publisher','region','period','license','native_version'))
    if cross_source:identity=('',)+identity[1:]
    if not identity[1] or not identity[2] or identity[3] in ('','제공처 확인'):return None
    request=record.get('metadata_request')
    identity+=(dump(request) if isinstance(request,dict) else '',)
    if by_url:
        path=canonical_url(record.get('url'))
        parsed=urllib.parse.urlsplit(path or '')
        if not parsed.path.strip('/') and not parsed.query:return None
        return identity+(path,) if path else None
    paths=tuple(sorted({canonical_url(u) for u in record.get('access_paths',[]) if canonical_url(u)}))
    substantial=[u for u in paths if len(urllib.parse.urlsplit(u).path)>10 and not re.search(r'\.(png|jpg|svg|gif)$|creativecommons|license',u,re.I)]
    return identity+(paths,) if substantial else None



def original_record_keys(record):
    """Only namespaces whose record identity is explicit in the source metadata."""
    keys = set()
    source = record.get('source_id')
    native = str(record.get('native_id') or '')
    external = str(record.get('external_id') or '')
    catalog = record.get('native_catalog')
    uuid = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
    catalogs = {'data-gov-ie':'ireland', 'govdata':'germany', 'data-gv-at':'austria',
                'open-data-portal-austria':'austria', 'opendata-swiss':'switzerland',
                'plateforme-ouverte-des-donnees-publiques-francaises':'france',
                'dados-gov-pt':'portugal'}
    if source in set(catalogs.values()) and re.fullmatch(uuid if source not in ('france','portugal') else r'[0-9a-fA-F]{24}',native):
        keys.add(('catalogue-record', source, native.lower()))
    if source == 'eu' and catalog in catalogs:
        target = catalogs[catalog]
        match = re.fullmatch('('+(uuid if target not in ('france','portugal') else r'[0-9a-fA-F]{24}')+r')(?:~~\d+)?',external)
        if match:
            keys.add(('catalogue-record',target,match[1].lower()))
    if source == 'eu' and catalog == 'zenodo':
        match = re.fullmatch(r'(?:oai-zenodo-org-|oai:zenodo\.org:)(\d+)(?:~~\d+)?',external)
        if match:keys.add(('zenodo-record',match[1]))
    paths = [record.get(k) for k in ('url','upstream_url','metadata_url')] + record.get('access_paths',[])
    path_keys=set();arcgis_keys=set()
    for value in paths:
        if not isinstance(value,str) or not public_url(value):continue
        part=urllib.parse.urlsplit(value)
        host=(part.hostname or '').lower()
        path=urllib.parse.unquote(part.path)
        query=urllib.parse.parse_qs(part.query)
        if host == 'arcgis.com' or host.endswith('.arcgis.com'):
            match=re.fullmatch(r'/api/download/v1/items/([0-9a-fA-F]{32})/(?:csv|geojson|shapefile|kml|filegdb|excel|featurecollection)',path)
            layers=query.get('layers',[])
            if match and len(layers)==1 and re.fullmatch(r'\d+',layers[0]):
                # Same item and layer, preserving every selection parameter. Matching
                # titles additionally avoid merging differently scoped catalogue views.
                selectors=tuple(sorted((k,tuple(v)) for k,v in query.items() if k not in ('layers','format')))
                period=clean(str(record.get('period') or '')).casefold()
                if period in ('미상','unknown','-','제공처 확인'):period=''
                arcgis_keys.add(('arcgis-layer',match[1].lower(),int(layers[0]),selectors,clean(record.get('title')).casefold(),period))
        if host in ('zenodo.org','www.zenodo.org'):
            match=re.match(r'^/(?:api/)?records?/(\d+)(?:/|$)',path)
            if match:path_keys.add(('zenodo-record',match[1]))
        elif host in ('data.go.kr','www.data.go.kr'):
            match=re.fullmatch(r'/data/(\d+)/(fileData|openapi|linkedData)\.do',path)
            if match:path_keys.add(('korea-record',match[1],match[2]))
        elif host in ('kosis.kr','stat.kosis.kr','kosis.daegu.go.kr') and path in ('/statHtml/statHtml.do','/nsibsHtmlSvc/fileView/FileStbl/fileStblView.do'):
            orgs=query.get('orgId') or query.get('in_org_id') or []
            tables=query.get('tblId') or query.get('in_tbl_id') or []
            if len(orgs)==len(tables)==1 and orgs[0] and tables[0]:
                display={'orgId','tblId','in_org_id','in_tbl_id','conn_path','vw_cd','list_id','lang_mode','lang','language'}
                selectors=tuple(sorted((k,tuple(v)) for k,v in query.items() if k not in display))
                path_keys.add(('kosis-table',orgs[0],tables[0]) + ((selectors,) if selectors else ()))
    # A list pointing to several datasets is not the same record as each member.
    if len(path_keys)==1:keys.update(path_keys)
    if not path_keys and len(arcgis_keys)==1:keys.update(arcgis_keys)
    return keys


def deduplicate(source_id=None):
    # Bounded scratch index: avoid holding millions of full records in RAM.
    work=ROOT/'duplicate-work.sqlite3'
    if work.exists():work.unlink()
    index=sqlite3.connect(work)
    index.row_factory=sqlite3.Row
    index.execute('PRAGMA journal_mode=OFF')
    index.execute('PRAGMA synchronous=OFF')
    index.execute('PRAGMA cache_size=-65536')
    index.executescript('''
        CREATE TABLE records(id TEXT PRIMARY KEY,title TEXT,publisher TEXT,modified TEXT,fingerprint TEXT,oldalias TEXT,access BLOB,landing BLOB,titlekey BLOB);
        CREATE TABLE identities(key TEXT,id TEXT);
    ''')
    count=0;batch=[];identities=[]
    def digest(value):
        return hashlib.sha256(dump(value).encode()).digest() if value is not None else None
    with database() as db:
        for row in db.execute('SELECT id,metadata,fingerprint FROM datasets'+(' WHERE source_id=?' if source_id else ''),(source_id,) if source_id else ()):
            r=json.loads(row['metadata']);id=row['id'];count+=1
            title=clean(r.get('title')).casefold()
            batch.append((id,title,r.get('publisher'),str(r.get('source_modified') or ''),row['fingerprint'],r.get('duplicate_of'),
                          digest(duplicate_key(r,False,cross_source=source_id is None)),digest(duplicate_key(r,True,cross_source=source_id is None)),
                          digest((r.get('source_id'),title,r.get('publisher')))))
            # Explicit conflicting versions are never joined through a shared landing page.
            version=str(r.get('native_version') or '')
            identities.extend((dump((key,version)),id) for key in original_record_keys(r))
            if len(batch)>=1000:
                index.executemany('INSERT INTO records VALUES(?,?,?,?,?,?,?,?,?)',batch);batch=[]
                index.executemany('INSERT INTO identities VALUES(?,?)',identities);identities=[];index.commit()
            if count%100000==0:print(dump({'duplicate_scan':count}),flush=True)
    if batch:index.executemany('INSERT INTO records VALUES(?,?,?,?,?,?,?,?,?)',batch)
    if identities:index.executemany('INSERT INTO identities VALUES(?,?)',identities)
    index.commit()
    parent={}
    def root(id):
        parent.setdefault(id,id)
        while parent[id]!=id:parent[id]=parent[parent[id]];id=parent[id]
        return id
    def connect(members):
        first=root(members[0])
        for id in members[1:]:
            other=root(id)
            if other!=first:parent[other]=first
    for field in ('access','landing'):
        index.execute('CREATE INDEX records_'+field+' ON records('+field+')')
        for group in index.execute('SELECT '+field+' AS key FROM records WHERE '+field+' IS NOT NULL GROUP BY '+field+' HAVING count(*)>1'):
            connect([x[0] for x in index.execute('SELECT id FROM records WHERE '+field+'=?',(group['key'],))])
    index.execute('CREATE INDEX identity_keys ON identities(key)')
    evidence={}
    for group in index.execute('SELECT key FROM identities GROUP BY key HAVING count(DISTINCT id)>1'):
        members=[x[0] for x in index.execute('SELECT DISTINCT id FROM identities WHERE key=?',(group['key'],))]
        connect(members)
        for id in members:evidence.setdefault(id,set()).add(group['key'])
    groups=collections.defaultdict(list)
    for id in parent:groups[root(id)].append(id)
    expected={};canonical_ids=set();group_count=0
    with (ROOT/'duplicate-groups.jsonl').open('w') as journal:
        for members in groups.values():
            if len(members)<2:continue
            rows={id:index.execute('SELECT modified,fingerprint FROM records WHERE id=?',(id,)).fetchone() for id in members}
            members.sort(key=lambda id:(rows[id]['modified'],id),reverse=True);canonical=members[0]
            canonical_ids.add(canonical);group_count+=1
            expected.update({id:(canonical,rows[id]['fingerprint']) for id in members[1:]})
            journal.write(dump({'canonical':canonical,'ids':members,'sources':sorted({id.split(':',1)[0] for id in members}),
                'reason':'exact original catalogue identity, or identical descriptive metadata and full access/landing identity',
                'original_record_evidence':sorted({key for id in members for key in evidence.get(id,())}),
                'content_comparison':False,'checked_at':now()})+'\n')
    # Update only affected records; do not repeatedly rewrite/scan the whole catalogue.
    old={r['id']:r['oldalias'] for r in index.execute('SELECT id,oldalias FROM records WHERE oldalias IS NOT NULL')}
    with database() as db:
        oldcanonical={r[0] for r in db.execute("SELECT id FROM datasets WHERE json_extract(metadata,'$.duplicate_canonical')=1"+(' AND source_id=?' if source_id else ''),(source_id,) if source_id else ())}
        db.executemany("UPDATE datasets SET metadata=json_remove(metadata,'$.duplicate_canonical') WHERE id=?",[(id,) for id in oldcanonical-canonical_ids])
        db.executemany("UPDATE datasets SET metadata=json_set(metadata,'$.duplicate_canonical',json('true')) WHERE id=?",[(id,) for id in canonical_ids-oldcanonical])
    changes=[(id,None,None) for id in old.keys()-expected.keys()]+[(id,*value) for id,value in expected.items() if old.get(id)!=value[0]]
    conflicts=0
    for start in range(0,len(changes),500):
        with database() as db:
            for id,canonical,fingerprint in changes[start:start+500]:
                if canonical is None:
                    db.execute("UPDATE datasets SET metadata=json_remove(metadata,'$.duplicate_of','$.duplicate_evidence') WHERE id=?",(id,))
                else:
                    n=db.execute("UPDATE datasets SET metadata=json_set(metadata,'$.duplicate_of',?,'$.duplicate_evidence',?) WHERE id=? AND fingerprint=?",(canonical,'Verified source identity; see collection-audit/duplicate-groups.jsonl',id,fingerprint)).rowcount
                    conflicts+=1-n
    index.execute('CREATE INDEX records_title ON records(titlekey)')
    candidate_count=0
    with (ROOT/'duplicate-candidates.jsonl').open('w') as journal:
        for group in index.execute('SELECT titlekey FROM records GROUP BY titlekey HAVING count(*)>1'):
            rows=list(index.execute('SELECT id,title FROM records WHERE titlekey=?',(group[0],)))
            if len({root(r['id']) if r['id'] in parent else r['id'] for r in rows})<=1:continue
            journal.write(dump({'source':rows[0]['id'].split(':',1)[0],'title':rows[0]['title'],'ids':[r['id'] for r in rows]})+'\n');candidate_count+=1
    # Verify every planned alias, its target, and all cleared aliases by primary key.
    verified=0
    with database() as db:
        for id,(canonical,fingerprint) in expected.items():
            row=db.execute("SELECT json_extract(metadata,'$.duplicate_of'),fingerprint FROM datasets WHERE id=?",(id,)).fetchone()
            if row and row[0]==canonical and row[1]==fingerprint:verified+=1
        for id in canonical_ids | (old.keys()-expected.keys()):
            row=db.execute("SELECT json_extract(metadata,'$.duplicate_of') FROM datasets WHERE id=?",(id,)).fetchone()
            if not row or row[0] is not None:conflicts+=1
    report('duplicates',{'status':'audited' if verified==len(expected) and not conflicts else 'concurrent_change_requires_recheck',
        'audited_source':source_id or 'all_stored_sources','stored_source_records':count,'merged_access_target_groups':group_count,
        'duplicate_records':verified,'expected_duplicate_records':len(expected),'title_candidate_groups':candidate_count,
        'concurrent_conflicts':conflicts,'evidence':'duplicate-groups.jsonl',
        'scope':'모든 저장 기록의 원 출처 ID·버전·메타데이터·제공 경로 대조. 원본 기록은 보존하고 검증된 재게시만 탐색에서 통합. 동일 제목만으로 합치지 않음.'})
    index.close();work.unlink()


def as_list(value):
    return value if isinstance(value,list) else [] if value is None else [value]


def label(value):
    if isinstance(value,str):return clean(value)
    if isinstance(value,list):
        for lang in ('ko','en','de','es','fr'):
            for item in value:
                if isinstance(item,dict) and item.get('_lang',item.get('@language'))==lang:return label(item)
        return label(value[0]) if value else ''
    if isinstance(value,dict):
        for key in ('_value','@value','name','label','ko','en','de','es','fr','title','resource','@id','_about'):
            if value.get(key):return label(value[key])
        return label(next(iter(value.values()))) if value else ''
    return ''


def source_aliases(source):
    with database() as db:
        return {json.loads(r['metadata'])['url']:json.loads(r['metadata'])['external_id']
                for r in db.execute('SELECT metadata FROM datasets WHERE source_id=?',(source,))}


def native_language_subjects(title, description):
    """Keep original-language wording; machine translations are not source evidence."""
    result = []
    for kind, field in (("resource_title", title), ("resource_description", description)):
        if isinstance(field, dict) and "@value" not in field:
            values = [(language, value) for language, value in field.items()
                      if isinstance(value, str)]
        else:
            values = [(value.get("@language", ""), value.get("@value", ""))
                      for value in as_list(field) if isinstance(value, dict)]
        for language, value in values:
            if value and "-t-" not in language and language not in {"@id", "@type"}:
                entry = {"kind": kind, "label": value, "language": language}
                if entry not in result:
                    result.append(entry)
    return result


def piveau_record(d, source, base):
    if not d.get('id') or not label(d.get('title')):raise ValueError('Official record has no id or title')
    paths=[];formats=[];licenses=[]
    for part in as_list(d.get('distributions')):
        if not isinstance(part,dict):continue
        paths += as_list(part.get('access_url'))+as_list(part.get('download_url'))
        formats+=as_list(label(part.get('format')))
        licenses+=as_list(label(part.get('license')))
    return {'external_id':d['id'],'native_id':d['id'],'title':label(d['title']),
            'description':label(d.get('description')),'publisher':label(d.get('publisher')) or '제공처 확인',
            'url':('https://www.data.gv.at/datasets/' if source=='austria' else 'https://data.europa.eu/data/datasets/')+urllib.parse.quote(d['id'],safe=''),
            'metadata_url':base+'datasets/'+urllib.parse.quote(d['id'],safe=''),'access_paths':sorted({u for u in paths if isinstance(u,str) and public_url(u)}),
            'format':' / '.join(sorted(set(filter(None,formats)))),'license':' / '.join(sorted(set(filter(None,licenses)))),
            'region':label(d.get('country')) or '미상','period':label(d.get('temporal')) or '미상',
            'source_modified':d.get('modified',''),'native_catalog':(d.get('catalog') or {}).get('id'),
            'upstream_url':d.get('resource',''),'subjects':[{'kind':'theme','label':label(x.get('label')) or x.get('resource') or x.get('id','')} for x in as_list(d.get('categories'))]+[{'kind':'tag','label':label(x)} for x in as_list(d.get('keywords')) if label(x)]+native_language_subjects(d.get('title'),d.get('description')),
            'description_languages_requested':'all_available','collection_method':'official_piveau_search_point_in_time'}


def collect_piveau(source):
    base=('https://www.data.gv.at/' if source=='austria' else 'https://data.europa.eu/')+'api/hub/search/'
    fields='id,title,description,keywords,publisher.name,resource,country.id,country.label,temporal,modified,catalog.id,categories.id,categories.label.en,distributions.access_url,distributions.download_url,distributions.format.id,distributions.format.label,distributions.license.resource'
    params={'filters':'dataset','limit':1000,'searchAfter':'true','sort':'id+asc','aggregation':'false','includes':fields}
    seen=set();invalid={};pages=0;expected=None
    with (ROOT/(source+'-inventory.jsonl')).open('w') as journal, (ROOT/(source+'-invalid-records.jsonl')).open('w') as errors:
        while True:
            url=base+'search?'+urllib.parse.urlencode(params)
            try:data=json.loads(read_url(url))['result']
            except ValueError as e:
                if 'size limit' not in str(e) or params['limit']<=1:raise
                params['limit']=max(1,params['limit']//2)
                continue
            items=data.get('results',[])
            if expected is None:expected=data['count']
            ids={d['id'] for d in items if d.get('id')}
            if items and not ids-seen:raise ValueError('Provider repeated a point-in-time page')
            rows=[]
            for item in items:
                try:rows.append(piveau_record(item,source,base))
                except ValueError as e:
                    invalid[item.get('id','unknown')]=str(e)
                    errors.write(dump({'id':item.get('id'),'error':str(e),'metadata':item})+'\n')
            errors.flush();store_raw(source,rows);seen.update(ids);pages+=1
            journal.write(dump({'page':pages,'ids':sorted(ids),'expected':expected})+'\n');journal.flush()
            info={'status':'running','unique':len(seen),'stored_current':len(seen)-len(invalid),'advertised':expected,'pages':pages,
                  'invalid_metadata':len(invalid),'page_size':params['limit'],
                  'scope':'공식 검색 카탈로그의 동일 시점 목록을 끝까지 조회. 관측값·원본 파일은 수집하지 않음.'}
            if pages%20==1:report(source,info)
            if not items:break
            if not data.get('pitId') or not data.get('sort'):raise ValueError('Provider omitted point-in-time pagination cursor')
            params.pop('searchAfter',None)
            params['pitId']=data['pitId'];params['searchAfterSort']=','.join(str(x) for x in data['sort'])
    (ROOT/(source+'-invalid.json')).write_text(dump(invalid))
    report(source,{**info,'status':'count_reconciled' if len(seen)==expected and not invalid else 'unreconciled',
                   'evidence':source+'-inventory.jsonl','invalid_evidence':source+'-invalid-records.jsonl'})


def repair_piveau_catalogs(source):
    # A separate catalogue inventory detects partial full-index pagination.
    base=('https://www.data.gv.at/' if source=='austria' else 'https://data.europa.eu/')+'api/hub/search/'
    fields='id,title,description,keywords,publisher.name,resource,country.id,country.label,temporal,modified,catalog.id,categories.id,categories.label.en,distributions.access_url,distributions.download_url,distributions.format.id,distributions.format.label,distributions.license.resource'
    query={'filters':'dataset','limit':0,'aggregation':'true','aggregationAllFields':'false','aggregationFields':'catalog','includes':'id'}
    inventory=json.loads(read_url(base+'search?'+urllib.parse.urlencode(query)))['result']
    catalogs=next(x['items'] for x in inventory['facets'] if x['id']=='catalog')
    folder=ROOT/(source+'-catalogs');folder.mkdir(exist_ok=True)
    (folder/'provider-catalogs.json').write_text(dump(inventory))
    with database() as db:known={r[0].split(':',1)[1] for r in db.execute('SELECT id FROM datasets WHERE source_id=?',(source,))}
    results={}
    def collect(part):
        cat=part['id'];key=hashlib.sha256(cat.encode()).hexdigest()[:16];seen=set();invalid=set();added=0;pages=0
        params={'filters':'dataset','limit':1000,'searchAfter':'true','sort':'id+asc','aggregation':'false','includes':fields,'facets':dump({'catalog':[cat]})}
        checkpoint=folder/(key+'.json');journal_path=folder/(key+'.jsonl');error_path=folder/(key+'-invalid.jsonl')
        if checkpoint.exists():
            previous=json.loads(checkpoint.read_text())
            if previous.get('advertised')==part['count'] and previous.get('status')=='count_reconciled':return previous
        resume_sort=None;last_sort=None;boundary=False;renewals=0
        if journal_path.exists():
            for line in journal_path.read_text().splitlines():
                entry=json.loads(line);seen.update(entry['ids']);pages=entry['page']
                if entry.get('sort'):resume_sort=entry['sort']
            if error_path.exists():
                invalid={json.loads(line).get('id','') for line in error_path.read_text().splitlines()}
        with journal_path.open('a' if seen else 'w') as journal,error_path.open('a' if seen else 'w') as errors:
            while True:
                if resume_sort is not None:
                    fresh={k:v for k,v in params.items() if k not in ('pitId','searchAfterSort')}
                    fresh.update(searchAfter='true',limit=1)
                    snapshot=json.loads(read_url(base+'search?'+urllib.parse.urlencode(fresh)))['result']
                    params.pop('searchAfter',None);params['pitId']=snapshot['pitId']
                    # Revisit the boundary ID in a fresh snapshot; shard document IDs change.
                    params['searchAfterSort']=str(resume_sort[0])+',-1'
                    resume_sort=None;boundary=True
                try:data=json.loads(read_url(base+'search?'+urllib.parse.urlencode(params)))['result']
                except urllib.error.HTTPError as e:
                    detail=e.read(20000).decode('utf-8','replace')
                    if e.code==400 and last_sort and renewals<3 and any(x in detail for x in ('search_context_missing','No search context','point in time')):
                        renewals+=1
                        resume_sort=last_sort
                        continue
                    raise ValueError('Official catalogue HTTP '+str(e.code)+': '+detail[:800]) from e
                except ValueError as e:
                    if 'size limit' not in str(e) or params['limit']<=1:raise
                    params['limit']=max(1,params['limit']//2);continue
                items=data.get('results',[]);ids={x['id'] for x in items if x.get('id')}
                if items and not ids-seen and not boundary:raise ValueError('Repeated catalogue page: '+cat)
                boundary=False
                rows=[]
                for item in items:
                    try:
                        row=piveau_record(item,source,base)
                        if row['external_id'] not in known:rows.append(row)
                    except ValueError as e:
                        invalid.add(item.get('id',''))
                        errors.write(dump({'id':item.get('id'),'error':str(e),'metadata':item})+'\n')
                if rows:store_raw(source,rows);added+=len(rows)
                seen.update(ids);pages+=1;errors.flush()
                journal.write(dump({'page':pages,'ids':sorted(ids),'reported_count':data['count'],'sort':data.get('sort')})+'\n');journal.flush()
                info={'catalog':cat,'status':'running','advertised':part['count'],'reported_count':data['count'],'unique':len(seen),'invalid_metadata':len(invalid),'added':added,'pages':pages}
                checkpoint.write_text(dump(info))
                if pages%20==1:print(dump({'source':source,'catalog_progress':info}),flush=True)
                if not items:break
                last_sort=data.get('sort')
                if not data.get('pitId') or not data.get('sort'):raise ValueError('Missing catalogue pagination cursor: '+cat)
                params.pop('searchAfter',None);params['pitId']=data['pitId'];params['searchAfterSort']=','.join(str(x) for x in data['sort'])
        info.update(status='count_reconciled' if len(seen)==part['count'] else 'unreconciled',evidence=str(journal_path.relative_to(ROOT)),invalid_evidence=str(error_path.relative_to(ROOT)))
        checkpoint.write_text(dump(info));return info
    report(source,{'status':'running','advertised':inventory['count'],'scope':'공식 하위 카탈로그별 ID 정렬 목록을 전체 조회하고 각 제공처 건수와 대조. 기존 자료와 같은 공식 ID는 재삽입하지 않음.'})
    # Large catalogues first; at most two concurrent official requests.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures={pool.submit(collect,x):x for x in sorted(catalogs,key=lambda x:x['count'],reverse=True)}
        for future in concurrent.futures.as_completed(futures):
            part=futures[future]
            try:results[part['id']]=future.result()
            except Exception as e:
                partial=folder/(hashlib.sha256(part['id'].encode()).hexdigest()[:16]+'.json')
                info=json.loads(partial.read_text()) if partial.exists() else {}
                results[part['id']]={**info,'catalog':part['id'],'status':'failed','advertised':part['count'],'error':str(e)}
            summary={'status':'running','advertised':inventory['count'],'catalogs':len(catalogs),'catalogs_finished':len(results),
                     'unique_catalog_entries':sum(x.get('unique',0) for x in results.values()),'added':sum(x.get('added',0) for x in results.values()),
                     'invalid_metadata':sum(x.get('invalid_metadata',0) for x in results.values()),'parts':results}
            (folder/'summary.json').write_text(dump(summary))
            report(source,{k:v for k,v in summary.items() if k!='parts'})
    errors=[x for x in results.values() if x['status']!='count_reconciled']
    report(source,{**{k:v for k,v in summary.items() if k!='parts'},'status':'count_reconciled' if not errors and not summary['invalid_metadata'] and summary['unique_catalog_entries']==inventory['count'] else 'unreconciled',
                   'catalogs_unreconciled':len(errors),'evidence':str((folder/'summary.json').relative_to(ROOT)),
                   'scope':'공식 카탈로그별 전체 식별자 목록 대조. 제목 없는 원본 기록은 예외 증거에 남기고 검색 자료에 임의로 추가하지 않음.'})


def us_record(item):
    d=item.get('dcat') or {};org=item.get('organization') or {}
    uri=item.get('harvest_record') or ''
    if not re.fullmatch(r'https://catalog\.data\.gov/harvest_record/[a-zA-Z0-9-]+',uri):raise ValueError('Missing official catalog record identity')
    identity=uri.rsplit('/',1)[1]
    if not item.get('identifier') or not item.get('title') or not item.get('slug'):raise ValueError('Missing official US record identity/title/slug')
    distributions=as_list(d.get('distribution'));paths=[]
    for part in distributions:
        if isinstance(part,dict):paths += [part.get('accessURL'),part.get('downloadURL')]
    return {'external_id':identity,'native_id':item['identifier'],'native_catalog_record_uri':uri,'native_parent_identifier':item.get('parent_identifier'),'native_type':item.get('type'),
            'native_organization_id':org.get('id'),'title':clean(item['title']),'description':clean(item.get('description')),
            'publisher':label(item.get('publisher')) or label(d.get('publisher')) or org.get('name') or '제공처 확인',
            'url':'https://catalog.data.gov/dataset/'+item['slug'],'metadata_url':item.get('harvest_record'),
            'access_paths':sorted({u for u in paths if u and public_url(u)}),'license':label(d.get('license')),
            'period':label(d.get('temporal')) or '미상','region':label(d.get('spatial')) or '미상',
            'source_modified':d.get('modified',''),'format':' / '.join(sorted({label(x.get('mediaType') or x.get('format')) for x in distributions if isinstance(x,dict)})),
            'subjects':[{'kind':'keyword','label':label(x)} for x in as_list(item.get('keyword'))],
            'collection_method':'official_public_catalog_cursor'}



def collect_us():
    base='https://catalog.data.gov/'
    orgs=json.loads(read_url(base+'api/organizations'))['organizations']
    expected=sum(int(x.get('dataset_count') or 0) for x in orgs)
    seen=set();pages=0;after=None;failures=[]
    with (ROOT/'us-inventory.jsonl').open('w') as journal:
        while True:
            url=base+'search?'+urllib.parse.urlencode({'per_page':1000,'sort':'last_harvested_date',**({'after':after} if after else {})})
            data=json.loads(read_url(url));items=data['results'];rows=[]
            for item in items:
                try:
                    row=us_record(item)
                    rows.append(row)
                except ValueError as e:failures.append({'id':item.get('identifier'),'error':str(e)})
            ids={r['external_id'] for r in rows}
            if items and not ids-seen:raise ValueError('US catalog cursor repeated a page')
            store_raw('us',rows);seen.update(ids);pages+=1
            journal.write(dump({'page':pages,'ids':sorted(ids),'raw_rows':len(items)})+'\n');journal.flush()
            info={'status':'running','unique':len(seen),'advertised_organization_sum':expected,'pages':pages,'invalid_metadata':len(failures),
                  'scope':'Data.gov 공개 검색 API 전체 커서 목록. 제공기관별 dataset_count 합계와 대조.'}
            if pages%10==1:report('us',info)
            new_after=data.get('after')
            if not new_after:break
            if new_after==after:raise ValueError('US catalog returned unchanged cursor')
            after=new_after
    (ROOT/'us-invalid.json').write_text(dump(failures))
    latest=sum(int(x.get('dataset_count') or 0) for x in json.loads(read_url(base+'api/organizations'))['organizations'])
    report('us',{**info,'status':'count_reconciled' if len(seen)==latest and not failures else 'unreconciled',
                 'advertised_final':latest,'cursor_exhausted':True,'evidence':'us-inventory.jsonl'})


def collect_hongkong():
    base='https://data.gov.hk/en-data/api/3/action/'
    names=json.loads(read_url(base+'package_list'))['result'];expected=set(names)
    if len(expected)!=len(names):raise ValueError('Hong Kong official ID inventory repeats identifiers')
    (ROOT/'hongkong-provider-ids.json').write_text(dump(sorted(expected)))
    seen=set();failed=[];aliases=source_aliases('hongkong')
    def fetch(name):
        url=base+'package_show?'+urllib.parse.urlencode({'id':name})
        try:
            d=json.loads(read_url(url))['result']
            if d.get('type','dataset')!='dataset':raise ValueError('Not a dataset: '+str(d.get('type')))
            r=ckan_record(d,base+'package_search','https://data.gov.hk/en-data/dataset/','hongkong')
            if r['url'] in aliases:r['external_id']=aliases[r['url']]
            return name,r,None
        except Exception as e:return name,None,str(e)
    with (ROOT/'hongkong-inventory.jsonl').open('w') as journal, concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        for name,row,error in pool.map(fetch,names):
            if error:failed.append({'id':name,'error':error})
            else:store_raw('hongkong',[row]);seen.add(name)
            journal.write(dump({'id':name,'error':error})+'\n');journal.flush()
            if (len(seen)+len(failed))%100==1:report('hongkong',{'status':'running','unique':len(seen),'advertised':len(expected),'failed':len(failed)})
    (ROOT/'hongkong-failed.json').write_text(dump(failed))
    report('hongkong',{'status':'id_reconciled' if seen==expected else 'unreconciled','unique':len(seen),'advertised':len(expected),
                       'failed':len(failed),'evidence':'hongkong-inventory.jsonl','scope':'공식 package_list 전체 ID를 package_show 메타데이터와 대조.'})


def kma_links(text):
    text=re.sub(r'<!--.*?-->|<script\b.*?</script>','',text,flags=re.S|re.I)
    result={}
    # Only official service-navigation links: never follow observation pagination or downloads.
    for attrs,body in re.findall(r'<a\b([^>]*)>(.*?)</a>',text,re.S):
        m=re.search(r"goMenuPage\('([^']+)','([^']*)'",attrs)
        if m:
            path=m[1];path+=('&' if '?' in path else '?')+'pgmNo='+m[2]
        else:
            h=re.search(r'href=["\']([^"\']+)',attrs)
            if not h:continue
            path=html.unescape(h[1])
        u=urllib.parse.urlsplit(urllib.parse.urljoin('https://data.kma.go.kr/',path))
        if u.hostname!='data.kma.go.kr' or not u.path.startswith(('/data/','/climate/')) or not u.path.split(';')[0].endswith('.do'):continue
        if re.search(r'download|insert|delete|update|Popup|Layer|Ajax',u.path,re.I):continue
        query=urllib.parse.parse_qsl(u.query)
        # pgmNo is menu context; code/gubun select distinct service subjects.
        key=urllib.parse.urlunsplit(('https',u.netloc,u.path.split(';')[0],urllib.parse.urlencode(sorted((k,v) for k,v in query if k in ('code','gubun'))),''))
        path=urllib.parse.urlunsplit(('https',u.netloc,u.path.split(';')[0],urllib.parse.urlencode(query),''))
        title=clean(body)
        if title:result[key]={'url':path,'label':title}
    return result


def kma_record(key,url,text):
    t=re.search(r'<title>(.*?)</title>',text,re.S)
    if not t or '기상자료개방포털[' not in t[1]:
        if '컨텐츠 내용이 준비가 되지 않았습니다' in text:raise ValueError('Official service page says content is not prepared')
        raise ValueError('Not an official KMA service detail page')
    hierarchy=clean(t[1]).split('[',1)[1].rstrip(']').split(':')
    if hierarchy[-1] in ('자료','파일셋 조회','통계','조회'):hierarchy=hierarchy[:-1]
    if len(hierarchy)<2:raise ValueError('Missing KMA service subject')
    title=hierarchy[-1]
    body=re.sub(r'<script\b.*?</script>|<!--.*?-->','',text,flags=re.S|re.I)
    desc=re.search(r'<h[34][^>]*>\s*자료설명\s*</h[34]>(.*?)(?=<h[34]\b|<form\b|$)',body,re.S)
    description=clean(desc[1])[:8000] if desc else ''
    def field(name):
        m=re.search(r'<th[^>]*>\s*'+name+r'\s*</th>\s*<td[^>]*>(.*?)</td>',body,re.S)
        return clean(m[1]) if m else ''
    return {'external_id':hashlib.sha256(key.encode()).hexdigest(),'title':title,'description':description,
            'publisher':'기상청','url':url,'metadata_url':url,'access_paths':[url],'region':'미상',
            'period':field('제공기간') or '미상','format':field('자료형태') or '웹 서비스',
            'native_catalog_path':' > '.join(hierarchy),'subjects':[{'kind':'theme','label':x} for x in hierarchy[1:]],
            'collection_method':'official_service_navigation','native_service_key':key}


def collect_kma():
    start='https://data.kma.go.kr/data/grnd/selectAsosRltmList.do?pgmNo=36'
    pending=kma_links(read_url(start).decode('utf-8'));done=set();failed={};seen=set()
    aliases=source_aliases('kma')
    with (ROOT/'kma-inventory.jsonl').open('w') as journal:
        while pending:
            batch=list(pending.items())[:3]
            for key,item in batch:del pending[key]
            def fetch(pair):
                key,item=pair
                try:return key,item,read_url(item['url']).decode('utf-8'),None
                except Exception as e:return key,item,'',str(e)
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                for key,item,text,error in pool.map(fetch,batch):
                    done.add(key)
                    if not error:
                        for more,value in kma_links(text).items():
                            if more not in done and more not in {x[0] for x in batch}:pending.setdefault(more,value)
                        try:
                            row=kma_record(key,item['url'],text)
                            for oldurl,ext in aliases.items():
                                old=urllib.parse.urlsplit(oldurl)
                                if old.path.replace('selectAsosList','selectAsosRltmList')==urllib.parse.urlsplit(key).path and not old.query and 'code=' not in key:
                                    row['external_id']=ext
                            if 'selectAsosRltmList' in key:
                                for oldurl,ext in aliases.items():
                                    if 'selectAsos' in oldurl:row['external_id']=ext
                            store_raw('kma',[row]);seen.add(key)
                        except ValueError as e:error=str(e)
                    if error:failed[key]={'url':item['url'],'label':item['label'],'error':error}
                    journal.write(dump({'key':key,'url':item['url'],'error':error})+'\n');journal.flush()
            report('kma',{'status':'running','discovered_services':len(done|pending.keys()),'unique':len(seen),'failed':len(failed),'remaining':len(pending)})
    (ROOT/'kma-failed.json').write_text(dump(failed))
    report('kma',{'status':'tree_exhausted' if not failed else 'unreconciled','unique':len(seen),'discovered_services':len(done),
                  'failed':len(failed),'scope':'공식 메뉴에서 연결된 자료·기후통계·간행물 서비스 단위. 관측 지점·시간별 값이나 파일 개수를 세지 않음.',
                  'evidence':'kma-inventory.jsonl','failed_evidence':'kma-failed.json'})


def collect_jeju():
    base='https://jejudatahub.net';seen=set();page=1;aliases=source_aliases('jeju')
    with (ROOT/'jeju-inventory.jsonl').open('w') as journal:
        while True:
            params=dict(includedKeywords='',excludedKeywords='',categories='',userId='',dataType='',orderBy='createAt',authYn='false',autoUpdateYn='false',keyword='',keywordOrTagFlag='',isPaging='true',start=(page-1)*100,pageNumber=page,length=100)
            url=base+'/api/data?'+urllib.parse.urlencode(params)
            d=json.loads(read_url(url));items=d['data'];total=d['recordsTotal'];rows=[]
            for item in items:
                paths=[]
                for kind in ('dataApi','dataLink'):
                    part=item.get(kind) or {}
                    paths += [v for k,v in part.items() if isinstance(v,str) and public_url(v)]
                landing=base+'/data/view/data/'+str(item['id'])
                rows.append({'external_id':aliases.get(landing,str(item['id'])),'title':clean(item['title']),'description':clean(item.get('description')),
                             'url':landing,'metadata_url':url,'native_id':str(item['id']),'access_paths':sorted(set(paths)),
                             'publisher':item.get('owner') or item.get('createByName') or '제공처 확인',
                             'period':' ~ '.join(x for x in [item.get('startDate'),item.get('endDate')] if x) or '미상',
                             'source_modified':item.get('updateAt'),'region':'미상','subjects':[{'kind':'theme','label':item.get('categoryName','')}],
                             'collection_method':'official_catalog_api'})
            ids={r['native_id'] for r in rows}
            if items and not ids-seen:raise ValueError('Jeju pagination repeated a page')
            store_raw('jeju',rows);seen.update(ids)
            journal.write(dump({'page':page,'ids':sorted(ids),'advertised':total})+'\n');journal.flush()
            if page*100>=total:break
            page+=1
    report('jeju',{'status':'count_reconciled' if len(seen)==total else 'unreconciled','unique':len(seen),'advertised':total,
                   'scope':'제주데이터허브 공개 자료 목록 전체. 민간 제공 자료 포함, 원 제공기관 표시.','evidence':'jeju-inventory.jsonl'})


def collect_incheon():
    base='https://data.incheon.go.kr';seen=set();page=1;aliases=source_aliases('incheon')
    with (ROOT/'incheon-inventory.jsonl').open('w') as journal:
        while True:
            url=base+'/api/data/public?'+urllib.parse.urlencode({'numOfRows':100,'pageNo':page,'sortColNm':'REG_DT DESC','dataGrd':''})
            d=json.loads(read_url(url,payload=dump({'searchTitle':''}).encode(),content_type='application/json'))['result']
            rows=[];items=d['result'];total=d['totalCount']
            for item in items:
                id=str(item['dataId']);landing=base+'/findData/publicDataDetail?dataId='+urllib.parse.quote(id,safe='')
                paths=[v for k,v in item.items() if re.search('url|link',k,re.I) and isinstance(v,str) and public_url(v)]
                rows.append({'external_id':aliases.get(landing,id),'native_id':id,'title':clean(item['title']),'description':clean(item.get('descT')),
                             'url':landing,'metadata_url':landing,'metadata_request':{'url':url,'method':'POST','json':{'searchTitle':''}},'access_paths':sorted(set(paths)),'publisher':item.get('orgNm') or '제공처 확인',
                             'source_modified':item.get('modiDate') or item.get('updatedAt') or '',
                             'region':'미상','period':'미상','format':item.get('prvDiv') or '제공처 확인',
                             'subjects':[{'kind':'theme','label':item.get('lrgDataNm') or ''}],
                             'native_source':item.get('srcSeNm'),'collection_method':'official_catalog_api'})
            ids={r['native_id'] for r in rows}
            if items and not ids-seen:raise ValueError('Incheon pagination repeated a page')
            store_raw('incheon',rows);seen.update(ids)
            journal.write(dump({'page':page,'ids':sorted(ids),'advertised':total})+'\n');journal.flush()
            if page%10==1:report('incheon',{'status':'running','unique':len(seen),'advertised':total,'page':page})
            if page*100>=total:break
            page+=1
    report('incheon',{'status':'count_reconciled' if len(seen)==total else 'unreconciled','unique':len(seen),'advertised':total,
                      'scope':'인천데이터포털 공공데이터 목록 전체. 연계목록의 원 제공처 포함.','evidence':'incheon-inventory.jsonl'})


def collect_oecd():
    url='https://sdmx.oecd.org/public/rest/dataflow/all'
    root=ET.fromstring(read_url(url,accept='application/vnd.sdmx.structure+xml;version=2.1'));flows=root.findall('.//{*}Dataflow');rows=[];excluded=[]
    for flow in flows:
        annotations={x.findtext('{*}AnnotationType'):x.findtext('{*}AnnotationText') for x in flow.findall('.//{*}Annotation')}
        id=flow.attrib['id'];agency=flow.attrib['agencyID'];version=flow.attrib['version'];identity=agency+':'+id
        def textfield(tag):
            items=flow.findall('{*}'+tag)
            return label([{'_lang':x.attrib.get('{http://www.w3.org/XML/1998/namespace}lang'),'_value':x.text or ''} for x in items])
        endpoint='https://sdmx.oecd.org/public/rest/'
        meta=endpoint+'dataflow/'+agency+'/'+id+'/'+version
        landing='https://data-explorer.oecd.org/vis?'+urllib.parse.urlencode({'df[ds]':'dsDisseminateFinalDMZ','df[ag]':agency,'df[id]':id,'df[vs]':version})
        rows.append({'external_id':identity,'native_id':id,'title':textfield('Name'),'description':textfield('Description'),
                     'publisher':agency,'url':landing,'metadata_url':meta,'access_paths':[endpoint+'data/'+agency+','+id+','+version],
                     'native_version':version,'native_nonproduction_annotation':annotations.get('NonProductionDataflow'),'format':'SDMX','collection_method':'official_sdmx_dataflow_registry'})
    seen=store_raw('oecd',rows)
    (ROOT/'oecd-inventory.json').write_text(dump({'ids':sorted(seen),'excluded':excluded}))
    report('oecd',{'status':'export_reconciled','unique':len(seen),'official_dataflows':len(flows),'excluded_nonproduction':len(excluded),
                   'scope':'OECD가 전체 자료 목록으로 안내하는 공개 dataflow/all 메타데이터. 원 제공처의 NonProductionDataflow 표기를 보존하며 실제 관측값 응답 여부는 별도 검증하지 않음.','evidence':'oecd-inventory.json'})


def collect_spain():
    endpoint='https://datos.gob.es/virtuoso/sparql'
    count_query='SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s a <http://www.w3.org/ns/dcat#Dataset> }'
    def count():
        q=endpoint+'?'+urllib.parse.urlencode({'query':count_query,'format':'application/sparql-results+json'})
        return int(json.loads(read_url(q))['results']['bindings'][0]['count']['value'])
    expected=count();seen=set();offset=0;size=500
    inventory=ROOT/'spain-inventory.jsonl'
    previous=json.loads((ROOT/'spain.json').read_text()) if (ROOT/'spain.json').exists() else {}
    if inventory.exists() and previous.get('status') in ('failed','running') and previous.get('advertised')==expected:
        for line in inventory.read_text().splitlines():
            entry=json.loads(line)
            if 'offset' not in entry:raise ValueError('Cannot resume a different Spain collector inventory')
            seen.update(entry['ids']);offset=entry['offset']+entry.get('size',500)
    report('spain',{'status':'running','unique':len(seen),'advertised':expected,'offset':offset,'resumed':bool(offset)})
    successes=0;requests=0
    rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#';dct='http://purl.org/dc/terms/';dcat='http://www.w3.org/ns/dcat#'
    with inventory.open('a' if offset else 'w') as journal:
        while offset<expected:
            query=('PREFIX dcat: <'+dcat+'> PREFIX dct: <'+dct+'> '
                'CONSTRUCT { ?s ?p ?o . ?dist ?dp ?v . ?period ?tp ?tv . ?pub ?np ?name } WHERE { '
                '{ SELECT DISTINCT ?s WHERE { ?s a dcat:Dataset } ORDER BY ?s LIMIT '+str(size)+' OFFSET '+str(offset)+' } '
                '{ ?s ?p ?o } UNION { ?s dcat:distribution ?dist . ?dist ?dp ?v } '
                'UNION { ?s dct:temporal ?period . ?period ?tp ?tv } '
                'UNION { ?s dct:publisher ?pub . ?pub ?np ?name FILTER(?np IN (<http://xmlns.com/foaf/0.1/name>,<http://www.w3.org/2000/01/rdf-schema#label>)) } }')
            url=endpoint+'?'+urllib.parse.urlencode({'query':query,'format':'application/rdf+xml'})
            try:root=ET.fromstring(read_url(url,limit=30_000_000))
            except (ValueError,urllib.error.HTTPError) as error:
                if size<=1 or isinstance(error,urllib.error.HTTPError) and error.code not in (500,502,503,504):raise
                size=max(1,size//2);successes=0
                continue
            nodes={}
            for node in root.iter():
                id=node.attrib.get('{'+rdf+'}about') or ('_:'+node.attrib['{'+rdf+'}nodeID'] if '{'+rdf+'}nodeID' in node.attrib else None)
                if not id:continue
                props=nodes.setdefault(id,{})
                if node.tag!='{'+rdf+'}Description':props.setdefault(rdf+'type',[]).append({'ref':node.tag[1:].replace('}','')})
                for child in node:
                    prop=child.tag[1:].replace('}','');ref=child.attrib.get('{'+rdf+'}resource')
                    if '{'+rdf+'}nodeID' in child.attrib:ref='_:'+child.attrib['{'+rdf+'}nodeID']
                    val={'ref':ref} if ref else {'text':''.join(child.itertext()),'lang':child.attrib.get('{http://www.w3.org/XML/1998/namespace}lang','')}
                    props.setdefault(prop,[]).append(val)
            def value(values):
                if not values:return ''
                for language in ('ko','en','es','fr','de'):
                    for v in values:
                        if v.get('lang')==language:return clean(v.get('text',''))
                return clean(values[0].get('text') or values[0].get('ref',''))
            rows=[];ids=set()
            for uri,d in nodes.items():
                if not any(t.get('ref')==dcat+'Dataset' for t in d.get(rdf+'type',[])):continue
                landing=uri.replace('http://datos.gob.es/','https://datos.gob.es/');ids.add(landing)
                distributions=[nodes.get(x.get('ref'),{}) for x in d.get(dcat+'distribution',[])]
                paths={x.get('ref') or x.get('text') for part in distributions for field in ('accessURL','downloadURL') for x in part.get(dcat+field,[])}
                pub=value(d.get(dct+'publisher',[]));pubnode=nodes.get(pub,{})
                periodnode=nodes.get(value(d.get(dct+'temporal',[])),{})
                dates=[value(v) for k,v in periodnode.items() if k.endswith(('startDate','endDate'))]
                rows.append({'external_id':landing,'native_id':value(d.get(dct+'identifier',[])),'title':value(d.get(dct+'title',[])),
                    'description':value(d.get(dct+'description',[])),'url':landing,'metadata_url':endpoint,'native_graph_uri':uri,
                    'access_paths':sorted(u for u in paths if u and public_url(u)),
                    'publisher':value(pubnode.get('http://xmlns.com/foaf/0.1/name',[])) or value(pubnode.get('http://www.w3.org/2000/01/rdf-schema#label',[])) or pub or '제공처 확인',
                    'license':value(d.get(dct+'license',[])),'source_modified':value(d.get(dct+'modified',[])),
                    'region':value(d.get(dct+'spatial',[])) or '미상','period':' ~ '.join(dates) or '미상',
                    'format':' / '.join(sorted({value(part.get(dct+'format',[])) for part in distributions})),
                    'subjects':[{'kind':'theme','label':value([v])} for v in d.get(dcat+'theme',[])],
                    'collection_method':'official_sparql_catalog_metadata'})
            if not ids or ids<=seen:raise ValueError('Spain SPARQL inventory repeated an empty/old page')
            store_raw('spain',rows);seen.update(ids)
            journal.write(dump({'offset':offset,'size':size,'ids':sorted(ids),'advertised':expected,'url':url})+'\n');journal.flush()
            offset+=size;requests+=1;successes+=1
            if requests%20==1:report('spain',{'status':'running','unique':len(seen),'advertised':expected,'offset':offset,'page_size':size})
            if successes>=3 and size<500:size=min(500,size*2);successes=0
    final_count=count()
    report('spain',{'status':'count_reconciled' if len(seen)==final_count else 'unreconciled','unique':len(seen),
                    'advertised':final_count,'initial_total':expected,'scope':'공식 SPARQL 자료 카탈로그 전체 URI와 설명·제공 경로. 별도 COUNT(DISTINCT)와 대조.',
                    'evidence':'spain-inventory.jsonl'})


def collect_hira():
    base='https://opendata.hira.or.kr';seen=set();page=1
    with (ROOT/'hira-inventory.jsonl').open('w') as journal:
        while True:
            url=base+'/op/opc/selectOpenDataList.do?pageIndex='+str(page)
            text=read_url(url).decode();m=re.search(r'class="tot">전체\s*<span>([\d,]+)</span>',text)
            if not m:raise ValueError('HIRA provider count missing')
            total=int(m[1].replace(',',''))
            links=re.findall(r'<a href="(\d+)" class="select">(.*?)</a>',text,re.S)
            rows=[{'external_id':id,'title':clean(title),'url':base+'/op/opc/selectOpenData.do?sno='+id,'metadata_url':url,
                   'publisher':'건강보험심사평가원','collection_method':'official_catalog_html','subjects':[{'kind':'theme','label':'보건의료'}]} for id,title in links]
            ids={id for id,t in links}
            if not ids or ids<=seen:raise ValueError('HIRA empty/repeated catalog page')
            store_raw('hira',rows);seen.update(ids)
            journal.write(dump({'page':page,'ids':sorted(ids),'advertised':total})+'\n')
            if page*10>=total:break
            page+=1
    report('hira',{'status':'count_reconciled' if len(seen)==total else 'unreconciled','unique':len(seen),'advertised':total,
                   'scope':'공식 공공데이터 목록 전체. 별도 의료통계 분석 화면은 이 목록에 포함되지 않음.','evidence':'hira-inventory.jsonl'})



def daejeon_access(id,title):
    filters={k:'' for k in ('dataProductPart','dataProductType','dataProductName','dataProvideType','startDate','endDate','sortCondition')}
    filters['nowPage']='1'
    body={'currentMenuId':'0000000212','param':json.dumps({**filters,'dataProductInfoId':id},ensure_ascii=False)}
    url='https://openlab.daejeon.go.kr/dataset/dataProduct/list.do?'+urllib.parse.urlencode({'param':json.dumps({**filters,'dataProductName':title},ensure_ascii=False)})
    return url,{'url':'https://openlab.daejeon.go.kr/dataset/dataProduct/view.do','method':'POST','form':body}


def daejeon_detail_metadata(page):
    links=sorted({html.unescape(u) for u in re.findall(r"""href=["'](https?://[^"']+)["']""",page)
                  if re.fullmatch(r'https?://(?:www\.)?data\.go\.kr/data/\d+/(?:openapi|fileData|linkedData)\.do',html.unescape(u))})
    fields=[]
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>',page,re.S):
        cells=[clean(x) for x in re.findall(r'<td[^>]*>(.*?)</td>',tr,re.S)]
        if cells and any(x.startswith(('resultList','items','body.')) for x in cells):fields.append(cells)
    subjects=[{'kind':'resource_description','label':'공식 서비스 응답 필드: '+', '.join(row[-1] for row in fields if row)}] if fields else []
    return {'access_paths':links,'subjects':subjects,'native_response_fields':fields}


def collect_daejeon():
    base='https://openlab.daejeon.go.kr';seen=set();page=1
    with (ROOT/'daejeon-inventory.jsonl').open('w') as journal:
        while True:
            url=base+'/dataset/dataProduct/list.do?'+urllib.parse.urlencode({'param':dump({**{k:'' for k in ('dataProductName','dataProductPart','dataProductType','dataProvideType','startDate','endDate','sortCondition')},'nowPage':str(page)})})
            text=read_url(url).decode();m=re.search(r'조회 결과\s*:\s*<b>([\d,]+)</b>',text)
            if not m:raise ValueError('Daejeon provider count missing')
            total=int(m[1].replace(',',''));rows=[]
            for id,body in re.findall(r'<a href="javascript:dataProductAction.getDataProductView\(\'([^\']+)\'\)">(.*?)</a>',text,re.S):
                title=re.search(r'<div class="top">.*?<p>(.*?)</p>',body,re.S)
                desc=re.search(r'<article>(.*?)</article>',body,re.S);publisher=re.search(r'<div class="writer">\s*<p>(.*?)</p>',body,re.S)
                if not title:raise ValueError('Daejeon title missing')
                landing,access_request=daejeon_access(id,clean(title[1]))
                detail=read_url(access_request['url'],payload=urllib.parse.urlencode(access_request['form']).encode(),content_type='application/x-www-form-urlencoded').decode()
                rows.append({**daejeon_detail_metadata(detail),'external_id':id,'title':clean(title[1]),'description':clean(desc[1]) if desc else '',
                             'publisher':clean(publisher[1]) if publisher else '제공처 확인','url':landing,'metadata_url':landing,'metadata_request':access_request,
                             'collection_method':'official_catalog_html'})
            ids={r['external_id'] for r in rows}
            if not ids or ids<=seen:raise ValueError('Daejeon empty/repeated catalog page')
            store_raw('daejeon',rows);seen.update(ids);journal.write(dump({'page':page,'ids':sorted(ids),'advertised':total})+'\n')
            if len(seen)>=total:break
            page+=1
    report('daejeon',{'status':'count_reconciled' if len(seen)==total else 'unreconciled','unique':len(seen),'advertised':total,
                      'scope':'대전 데이터드림 공식 데이터 상품 검색 목록 전체.','evidence':'daejeon-inventory.jsonl'})


def collect_vworld():
    base='https://www.vworld.kr';seen=set();page=1
    with (ROOT/'vworld-inventory.jsonl').open('w') as journal:
        while True:
            url=base+'/dtmk/dtmk_ntads_s001.do?pageIndex='+str(page)
            text=read_url(url).decode();m=re.search(r'총\s*<b>([\d,]+)</b>건',text)
            if not m:raise ValueError('VWorld provider count missing')
            total=int(m[1].replace(',',''));rows=[]
            for kind,id,body in re.findall(r'<a href="javascript:listFnc.goDetail\(\'([^\']+)\',\s*\'([^\']+)\'\);"\s*class="item">(.*?)</a>',text,re.S):
                title=re.search(r'<div class="tit fix">(.*?)</div>',body,re.S);desc=re.search(r'<div class="con">(.*?)</div>',body,re.S)
                def field(name):
                    m=re.search(name+r'<em>(.*?)</em>',body,re.S);return clean(m[1]) if m else ''
                landing=base+'/dtmk/dtmk_ntads_s002.do?'+urllib.parse.urlencode({'svcCde':kind,'dsId':id})
                rows.append({'external_id':kind+':'+id,'title':clean(title[1]),'description':clean(desc[1]) if desc else '',
                             'publisher':field('기관') or '제공처 확인','url':landing,'metadata_url':url,
                             'period':field('기준일') or '미상','source_modified':field('갱신일'),'collection_method':'official_catalog_html'})
            ids={r['external_id'] for r in rows}
            if not ids or ids<=seen:raise ValueError('VWorld empty/repeated catalog page')
            store_raw('vworld',rows);seen.update(ids);journal.write(dump({'page':page,'ids':sorted(ids),'advertised':total})+'\n');journal.flush()
            if page%20==1:report('vworld',{'status':'running','unique':len(seen),'advertised':total,'page':page})
            if page*10>=total:break
            page+=1
    report('vworld',{'status':'count_reconciled' if len(seen)==total else 'unreconciled','unique':len(seen),'advertised':total,
                     'scope':'공식 공간정보 카탈로그 전체 서비스 유형 목록.','evidence':'vworld-inventory.jsonl'})



def daegu_page(kind,page,size):
    method,key=('getDataSetListInfo','dataSetListInfo') if kind=='FILE_API' else ('getLinkDataSetListInfo','linkDataSetListInfo')
    url='https://data.daegu.go.kr/data/rest/'+method+'.do'
    params={'currentPageNo':page,'recordCountPerPage':size,'provdMethod':'all','ctgryCode':'all','searchCnd':'all','searchWrd':'','consentType':'true','iog':'out'}
    text=read_url(url,payload=urllib.parse.urlencode(params).encode(),content_type='application/x-www-form-urlencoded',accept='application/json')
    data=json.loads(text)[key]
    if isinstance(data,str):data=json.loads(data)
    if not isinstance(data.get('dataSetList'),list):raise ValueError('Daegu catalog rows missing')
    if int(data['search']['currentPageNo'])!=page:raise ValueError('Daegu page index was ignored')
    return data['dataSetList'],int(data['search']['totalRecordCount']),int(data['search']['recordCountPerPage']),url


def collect_daegu():
    groups={};parts={};all_ids=set();failures=[]
    with (ROOT/'daegu-inventory.jsonl').open('w') as journal:
        for kind in ('FILE_API','LINK'):
            first,total,size,url=daegu_page(kind,1,1000);seen=set()
            def ingest(page,rows):
                ids=set()
                for d in rows:
                    child=str(d.get('dataSetDetailId') or d.get('dataMapId') or d.get('dataSetId') or '')
                    if not child:raise ValueError('Daegu catalog row identity missing')
                    ids.add(child);identity=str(d.get('dataSetId') or kind+':'+child)
                    parent_path=clean(d.get('lc'));parent_url=public_url(d.get('lcUrl') or '')
                    params={'dataSetId':str(d.get('dataSetId') or ''),'provdMethod':d.get('provdMethod') or kind}
                    if d.get('dataSetDetailId'):params['dataSetDetailId']=str(d['dataSetDetailId'])
                    landing=('https://data.daegu.go.kr/open/data/dataView.do?'+urllib.parse.urlencode(params)) if d.get('dataSetId') else (parent_url or 'https://data.daegu.go.kr/open/data/dataList.do')
                    r=groups.setdefault(identity,{'external_id':identity,'native_id':identity,'title':clean(d.get('dataName') or d.get('detailDataName')),
                        'description':clean(d.get('dataCn')),'publisher':d.get('detailInsttNm') or d.get('insttCodeName') or '제공처 확인',
                        'url':landing,'metadata_url':url,'metadata_request':{'method':'POST','accept':'application/json'},'access_paths':[],
                        'subjects':[{'kind':'theme','label':d.get('ctgryFullName') or d.get('ctgryName') or ''}],
                        'native_editions':{},'native_types':[],'collection_method':'official_catalog_api_service_groups'})
                    if parent_path:
                        r['native_catalog_path']=parent_path
                        extra={'kind':'resource_title','label':parent_path}
                        if extra not in r['subjects']:r['subjects'].append(extra)
                        years=re.findall(r'(?<!\d)(?:19|20)\d{2}(?!\d)',parent_path)
                        if len(set(years))==1:r['native_catalog_year']=years[0]
                    if parent_url:r['native_catalog_url']=parent_url
                    method=d.get('provdMethod') or kind
                    if method not in r['native_types']:r['native_types'].append(method)
                    paths=[d.get(k) for k in ('dataUrl','link','detailLink')]
                    paths=[u for u in paths if isinstance(u,str) and public_url(u)]
                    for path in paths:
                        if path not in r['access_paths']:r['access_paths'].append(path)
                    if kind=='LINK' and len(r['access_paths'])==1:
                        r['url']=r['access_paths'][0]
                    r['native_editions'][kind+':'+child]={'id':child,'title':clean(d.get('detailDataName') or d.get('dataName')),
                        'modified':d.get('detailUpdateDate') or d.get('updateDate'),'access_paths':paths}
                if rows and ids<=seen:raise ValueError('Daegu repeated catalog page')
                seen.update(ids);all_ids.update(kind+':'+id for id in ids)
                journal.write(dump({'part':kind,'page':page,'ids':sorted(ids),'advertised':total,'size':size})+'\n');journal.flush()
                parts[kind]={'unique_catalog_entries':len(seen),'advertised':total,'page':page}
                if page%100==1:report('daegu',{'status':'running','service_groups':len(groups),'unique_catalog_entries':len(all_ids),'parts':parts})
            ingest(1,first)
            def fetch(page):
                try:
                    rows,current,_,_=daegu_page(kind,page,size)
                    return page,rows,None
                except Exception as e:return page,None,str(e)
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                for page,rows,error in pool.map(fetch,range(2,(total+size-1)//size+1)):
                    if error:failures.append({'part':kind,'page':page,'error':error})
                    else:ingest(page,rows)
            parts[kind]['status']='count_reconciled' if len(seen)==total else 'unreconciled'
            records=[{**r,'native_editions':list(r['native_editions'].values())} for r in groups.values()]
            for offset in range(0,len(records),100):store_raw('daegu',records[offset:offset+100])
    (ROOT/'daegu-failed-pages.json').write_text(dump(failures))
    report('daegu',{'status':'count_reconciled' if not failures and all(p['status']=='count_reconciled' for p in parts.values()) else 'unreconciled',
                    'unique':len(groups),'unique_catalog_entries':len(all_ids),'parts':parts,'failed_pages':len(failures),
                    'catalog_without_access_url':sum(not r['access_paths'] for r in groups.values()),
                    'scope':'공식 FILE·API·LINK 목록 전체를 dataSetId 자료·서비스 단위로 묶고 개별 판본·경로 보존. LINK는 원 제공처의 연계 안내이며 실제 다운로드 가능성을 검증한 수는 아님.',
                    'evidence':'daegu-inventory.jsonl','failed_evidence':'daegu-failed-pages.json'})

def collect_sgis():
    url='https://sgis.mods.go.kr/view/pss/openDataIntrcn'
    text=re.sub(r'<!--.*?-->','',read_url(url).decode(),flags=re.S);rows=[]
    for table in re.findall(r'<table\b.*?</table>',text,re.S):
        if '대상자료명' not in table or '기준년도' not in table:continue
        for tr in re.findall(r'<tr\b.*?</tr>',table,re.S):
            cells=[clean(x) for x in re.findall(r'<td[^>]*>(.*?)</td>',tr,re.S)]
            if not cells:continue
            if len(cells)!=6:raise ValueError('SGIS catalog table structure changed')
            title,period,format,access,region,cost=cells
            rows.append({'external_id':hashlib.sha256(title.encode()).hexdigest(),'title':title,
                         'description':title+'; 공개여부: '+access+'; 비용: '+cost,'url':url,'metadata_url':url,
                         'period':period,'region':region,'format':format,'publisher':'국가데이터처',
                         'access_paths':['https://sgis.mods.go.kr/view/pss/requestData'],'collection_method':'official_service_catalog_table'})
    if not rows:raise ValueError('SGIS official catalog table is empty')
    seen=store_raw('sgis',rows);(ROOT/'sgis-inventory.json').write_text(dump(rows))
    report('sgis',{'status':'export_reconciled','unique':len(seen),'official_catalog_rows':len(rows),
                   'scope':'공식 자료제공 목록 표의 통계자료·통계지역경계 서비스 단위. 연도별 값·지점별 파일을 개별 자료로 세지 않음.','evidence':'sgis-inventory.json'})


def culture_page(kind,page):
    paths={'api':'/data/openapi/openapiList.do','file':'/data/filedat/filedatList.do',
           'linked':'/data/lnkdat/newPblcDatList.do','kosis':'/data/lnkdat/ntnStatsList.do'}
    url='https://www.culture.go.kr'+paths[kind]+'?'+urllib.parse.urlencode({'pageNo':page,**({'gubun':'A'} if kind=='api' else {})})
    text=re.sub(r'<!--.*?-->','',read_url(url).decode(),flags=re.S)
    m=re.search(r'class="count">([\d,]+)</span>',text)
    if not m:raise ValueError('Culture count missing: '+kind)
    total=int(m[1].replace(',',''));rows=[]
    for block in re.split(r'<div class="board__title[^"]*">',text)[1:]:
        head=block.split('class="board__desc"',1)[0]
        titles=re.findall(r'<p[^>]*>(.*?)</p>',head,re.S)
        title=next((clean(x) for x in titles if clean(x)),'')
        if not title:raise ValueError('Culture record title missing')
        desc=re.search(r'class="board__desc"[^>]*>(.*?)<div class="board__info',block,re.S)
        description=clean(desc[1]) if desc else ''
        if kind in ('api','file'):
            match=re.search(r'href="(/data/(?:openapi/openapiView|filedat/filedatDtl)\.do[^"]+)"',head)
            if not match:raise ValueError('Culture landing link missing')
            landing=urllib.parse.urljoin('https://www.culture.go.kr',html.unescape(match[1]))
            query=urllib.parse.parse_qs(urllib.parse.urlsplit(landing).query)
            id=(query.get('id') or query.get('fileDataNo'))[0]
            access=[html.unescape(x) for x in re.findall(r"fnFileDwld\('([^']+)'",block)]
        else:
            match=re.search(r"fnLinkDtlView\('([^']+)','([^']+)'\)",head)
            if not match:raise ValueError('Culture linked record identity missing')
            a,b=match.groups();id=a+':'+b
            if kind=='kosis':landing='https://kosis.kr/statHtml/statHtml.do?'+urllib.parse.urlencode({'orgId':a,'tblId':b})
            else:landing='https://www.data.go.kr/data/'+b+'/'+{'01':'openapi','02':'fileData','03':'standard'}[a]+'.do'
            access=[landing]
        rows.append({'external_id':kind+':'+id,'native_id':id,'title':title,'description':description,'url':landing,
                     'metadata_url':url,'access_paths':access,'publisher':'제공처 확인',
                     'native_type':kind,'collection_method':'official_catalog_html'})
    return rows,total,url


def collect_culture():
    parts={};all_ids=set()
    with (ROOT/'culture-inventory.jsonl').open('w') as journal:
        for kind in ('api','file','linked','kosis'):
            seen=set();page=1
            while True:
                rows,total,url=culture_page(kind,page);ids={r['external_id'] for r in rows}
                if total and (not ids or ids<=seen):raise ValueError('Culture empty/repeated page: '+kind)
                store_raw('culture',rows);seen.update(ids);all_ids.update(ids)
                journal.write(dump({'part':kind,'page':page,'ids':sorted(ids),'advertised':total})+'\n');journal.flush()
                parts[kind]={'unique':len(seen),'advertised':total,'page':page}
                if page%30==1:report('culture',{'status':'running','unique':len(all_ids),'parts':parts})
                if page*10>=total:break
                page+=1
            parts[kind]['status']='count_reconciled' if len(seen)==total else 'unreconciled'
    report('culture',{'status':'count_reconciled' if all(p['status']=='count_reconciled' for p in parts.values()) else 'unreconciled',
                      'unique':len(all_ids),'parts':parts,'scope':'공식 오픈API·파일·공공데이터 연계·국가통계 연계 목록 전체.','evidence':'culture-inventory.jsonl'})


def ecos_request(code,data=None):
    url='https://ecos.bok.or.kr/serviceEndpoint/httpService/request.json'
    header={'guidSeq':1,'trxCd':code,'scrId':'IECOSPC','sysCd':'03','fstChnCd':'WEB','langDvsnCd':'KO',
            'envDvsnCd':'D','sndRspnDvsnCd':'S','sndDtm':datetime.now(timezone(timedelta(hours=9))).strftime('%Y%m%d%H%M%S')+'000',
            'usrId':'IECOSPC','pageNum':1,'pageCnt':1000}
    response=json.loads(read_url(url,payload=json.dumps({'header':header,'data':data or {}},ensure_ascii=False).encode(),content_type='application/json'))
    if str(response.get('header',{}).get('rspnDvsnCd'))!='0':raise ValueError('ECOS metadata request failed')
    return response['data']


def ecos_labels(title,path):
    def without_number(value):return re.sub(r'^\d{1,2}(?:\.\d+)*\.?\s+','',clean(value))
    basis=next((without_number(x) for x in path if re.search(r'(?:19|20)\d{2}년.*기준',x)),None)
    display=without_number(path[-1]) if title.replace(' ','') in ('파일다운로드','자료다운로드') and path else title
    if basis and basis not in display:display+=' ('+basis+')'
    return {'title':display,'native_title':title,'native_reference_basis':basis}


def collect_ecos():
    data=ecos_request('OSUUA01R01');items=data['statClfList'];total=int(data['dataCcnt']);rows=[];folders=[]
    lookup={d['dsId']:d for d in items}
    for d in items:
        if d['typ']=='C':folders.append(d['dsId']);continue
        if d['typ']!='S':raise ValueError('Unknown ECOS catalog node type')
        path=[];parent=d.get('dsClfId');visited=set()
        while parent in lookup:
            if parent in visited:raise ValueError('Cycle in ECOS source catalog')
            visited.add(parent);node=lookup[parent];path.insert(0,node['dsNm']);parent=node.get('dsClfId')
        rows.append({'external_id':d['dsId'],'native_id':d['dsId'],**ecos_labels(d['dsNm'],path),'description':d.get('dsEngNm') or '',
                     'publisher':'한국은행 ECOS','url':'https://ecos.bok.or.kr/#/SearchStat','metadata_url':'https://ecos.bok.or.kr/#/SearchStat',
                     'metadata_request':{'url':'https://ecos.bok.or.kr/serviceEndpoint/httpService/request.json','method':'POST','transaction':'OSUUA01R01','dataset_id':d['dsId']},
                     'access_paths':['https://ecos.bok.or.kr/api/'],'native_catalog_path':' > '.join(path),
                     'native_frequency':d.get('freqList'),'native_description_id':d.get('statDescId'),
                     'subjects':[{'kind':'theme','label':x} for x in path],'collection_method':'official_public_metadata_service'})
    seen=store_raw('ecos',rows);(ROOT/'ecos-inventory.json').write_text(dump({'ids':sorted(seen),'classification_nodes':folders}))
    report('ecos',{'status':'export_reconciled' if len(items)==total and len(seen)+len(folders)==total else 'unreconciled',
                   'unique':len(seen),'official_catalog_nodes':total,'classification_nodes':len(folders),
                   'scope':'공식 통계조회 화면의 전체 통계표 목록. 분류 폴더 제외, 통계표 코드·분류경로·조회 주기 보존. 원문 링크는 ECOS 검색 진입점이며 통계표 코드는 별도 제공.',
                   'evidence':'ecos-inventory.json'})


def collect_taiwan():
    url='https://data.gov.tw/api/front/dataset/list';seen=set();page=1;size=1000;offset=0
    inventory=ROOT/'taiwan-inventory.jsonl'
    previous=json.loads((ROOT/'taiwan.json').read_text()) if (ROOT/'taiwan.json').exists() else {}
    if inventory.exists() and previous.get('status') in ('failed','running'):
        for line in inventory.read_text().splitlines():
            entry=json.loads(line);seen.update(entry['ids'])
            offset=entry.get('offset',(entry['page']-1)*entry.get('size',100))+entry.get('size',100)
        page=offset//size+1
    report('taiwan',{'status':'running','unique':len(seen),'advertised':previous.get('advertised'),'page':page,'resumed':bool(seen),'page_size':size})
    with inventory.open('a' if seen else 'w') as journal:
        while True:
            payload={'bool':[],'filter':[],'page_num':page,'page_limit':size,'tids':[],'sort':'nid_asc'}
            data=json.loads(read_url(url,payload=dump(payload).encode(),content_type='application/json'))
            if not data.get('success'):raise ValueError('Taiwan catalog API returned failure')
            data=data['payload'];total=data['search_count'];rows=[]
            for d in data['search_result']:
                id=str(d['nid']);paths=as_list(d.get('all_url'))+as_list(d.get('api_doc_url'))
                rows.append({'external_id':id,'native_id':id,'title':clean(d['title']),'description':clean(d.get('content')),
                             'publisher':d.get('agency_name') or '제공처 확인','url':'https://data.gov.tw/dataset/'+id,
                             'metadata_url':'https://data.gov.tw/api/front/dataset/detail?nid='+id,
                             'access_paths':sorted({u for u in paths if isinstance(u,str) and public_url(u)}),
                             'region':d.get('coverage_spatial') or '미상',
                             'period':' ~ '.join(x.get('date','') for x in [d.get('coverage_temporal_start'),d.get('coverage_temporal_end')] if isinstance(x,dict)) or '미상',
                             'source_modified':(d.get('metadata_changed') or {}).get('date',''),'license':d.get('license_name',''),
                             'format':' / '.join(as_list(d.get('all_file_format_name'))),
                             'subjects':[{'kind':'theme','label':d.get('category_name') or ''}],'collection_method':'official_public_search_api'})
            ids={r['external_id'] for r in rows}
            if not ids or ids<=seen:raise ValueError('Taiwan empty/repeated catalog page')
            store_raw('taiwan',rows);seen.update(ids);journal.write(dump({'page':page,'size':size,'offset':(page-1)*size,'ids':sorted(ids),'advertised':total,'sort':'nid_asc'})+'\n');journal.flush()
            if page%30==1:report('taiwan',{'status':'running','unique':len(seen),'advertised':total,'page':page})
            if page*size>=total:break
            page+=1
    report('taiwan',{'status':'count_reconciled' if len(seen)==total else 'unreconciled','unique':len(seen),'advertised':total,
                     'scope':'정부자료개방플랫폼 공개 검색 목록 전체. 연락처·내부 비고는 저장하지 않음.','evidence':'taiwan-inventory.jsonl'})


def piveau_repository_record(id,document,source='austria'):
    repo=('https://www.data.gv.at/' if source=='austria' else 'https://data.europa.eu/')+'api/hub/repo/'
    nodes=document.get('@graph',[document]);lookup={x.get('@id'):x for x in nodes}
    candidates=[x for x in nodes if x.get('@id','').endswith('/'+id)]
    d=next((x for x in candidates if x.get('dct:title')),candidates[0] if candidates else {})
    title=label(d.get('dct:title'));title_origin='dataset_title'
    def linked(node,key):
        return [lookup.get(v.get('@id'),v) if isinstance(v,dict) else v for v in as_list(node.get(key))]
    if not title:
        distributions=linked(d,'dcat:distribution')
        if len(distributions)==1 and isinstance(distributions[0],dict):
            title=label(distributions[0].get('dct:title'));title_origin='sole_distribution_title'
    if not title:raise ValueError('Repository metadata also has no title')
    paths=[];formats=[];licenses=[]
    for part in linked(d,'dcat:distribution'):
        if not isinstance(part,dict):continue
        paths += [label(v) for k in ('dcat:accessURL','dcat:downloadURL') for v in as_list(part.get(k))]
        formats.append(label(part.get('dct:format')));licenses.append(label(part.get('dct:license')))
    publisher=linked(d,'dct:publisher')
    publisher=label(publisher[0].get('foaf:name')) if publisher and isinstance(publisher[0],dict) else ''
    return {'external_id':id,'native_id':id,'title':title,'description':label(d.get('dct:description')),
            'publisher':publisher or '제공처 확인','url':('https://www.data.gv.at/datasets/' if source=='austria' else 'https://data.europa.eu/data/datasets/')+id,'metadata_url':repo+'datasets/'+id,
            'access_paths':sorted({u for u in paths if public_url(u)}),'source_modified':label(d.get('dct:modified')),
            'format':' / '.join(sorted(set(filter(None,formats)))),'license':label(d.get('dct:license')) or ' / '.join(sorted(set(filter(None,licenses)))),
            'subjects':[{'kind':'theme','label':label(x)} for x in as_list(d.get('dcat:theme'))]+native_language_subjects(d.get('dct:title'),d.get('dct:description')),
            'native_dataset_title':label(d.get('dct:title')),'title_origin':title_origin,
            'collection_method':'official_piveau_repository_metadata'}



def datacite_record(external_id, doi, document):
    """Recover a missing catalogue record from its exact DOI registration metadata."""
    data = document['data']
    value = data['attributes']
    if str(data['id']).casefold() != doi.casefold() or str(value.get('doi', '')).casefold() != doi.casefold():
        raise ValueError('DataCite returned a different DOI')
    title = label([x.get('title') for x in value.get('titles', []) if x.get('title')])
    if not title or not public_url(value.get('url', '')):
        raise ValueError('DOI metadata has no title or source landing page')
    return {'external_id': external_id, 'native_id': external_id, 'native_doi': doi,
            'title': title, 'description': '\n'.join(x['description'] for x in value.get('descriptions', []) if x.get('description')),
            'publisher': label(value.get('publisher')) or '제공처 확인', 'url': value['url'],
            'metadata_url': 'https://api.datacite.org/dois/' + doi,
            'catalog_url': 'https://www.data.gv.at/datasets/' + urllib.parse.quote(external_id, safe=''),
            'access_paths': [x for x in as_list(value.get('contentUrl')) if isinstance(x, str) and public_url(x)],
            'license': ' / '.join(x.get('rightsUri') or x.get('rights', '') for x in value.get('rightsList', [])),
            'native_version': value.get('version'), 'native_resource_type': value.get('types'),
            'native_dates': value.get('dates', []),
            'period': ' / '.join(x['date'] for x in value.get('dates', []) if x.get('dateType') == 'Collected' and x.get('date')) or '미상',
            'subjects': [{'kind': 'tag', 'label': x['subject']} for x in value.get('subjects', []) if x.get('subject')],
            'source_modified': value.get('updated'), 'collection_method': 'exact_registered_doi_metadata_recovery'}



def recover_austria_record(id):
    url='https://www.data.gv.at/api/hub/repo/datasets/'+urllib.parse.quote(id,safe='')
    try:
        return piveau_repository_record(id,json.loads(read_url(url,accept='application/ld+json')))
    except Exception:
        # This portal's DOI-form IDs encode a registered GeoSphere DOI exactly.
        match=re.fullmatch(r'https-doi-org-10-60669-(.+)',id)
        if not match:raise
        doi='10.60669/'+match[1]
        return datacite_record(id,doi,json.loads(read_url('https://api.datacite.org/dois/'+doi,accept='application/json')))


def verify_austria_repository():
    base='https://www.data.gv.at/api/hub/repo/';repo_ids=set();offset=0
    while True:
        url=base+'datasets?'+urllib.parse.urlencode({'valueType':'identifiers','limit':5000,'offset':offset})
        ids=json.loads(read_url(url))
        if not isinstance(ids,list):raise ValueError('Austria repository inventory is not an ID list')
        if ids and set(ids)<=repo_ids:raise ValueError('Austria repository repeated an ID page')
        repo_ids.update(ids)
        if not ids:break
        offset+=5000
    (ROOT/'austria-repository-ids.json').write_text(dump(sorted(repo_ids)))
    search_ids=set()
    for line in (ROOT/'austria-inventory.jsonl').read_text().splitlines():search_ids.update(json.loads(line)['ids'])
    with database() as db:stored={r[0] for r in db.execute("SELECT json_extract(metadata,'$.native_id') FROM datasets WHERE source_id='austria'")}
    targets=(search_ids|repo_ids)-stored;failed=[];recovered=[]
    for id in sorted(targets):
        url=base+'datasets/'+urllib.parse.quote(id,safe='')
        try:
            row=recover_austria_record(id);store_raw('austria',[row]);recovered.append(id)
        except Exception as e:failed.append({'id':id,'url':url,'error':str(e)})
    (ROOT/'austria-repository-failures.json').write_text(dump(failed))
    report('austria-independent',{'status':'id_reconciled' if not failed and repo_ids==search_ids else 'unreconciled',
        'search_ids':len(search_ids),'repository_ids':len(repo_ids),'search_only':len(search_ids-repo_ids),'repository_only':len(repo_ids-search_ids),
        'recovered':len(recovered),'failed':len(failed),'evidence':'austria-repository-ids.json','failed_evidence':'austria-repository-failures.json',
        'scope':'검색 목록과 별도 원본 메타데이터 저장소의 전체 ID 대조. 제목 없는 기록은 원본 저장소에서 재확인.'})



def zenodo_record(item,external_id=None):
    id=str(item['id']);external_id=external_id or 'oai-zenodo-org-'+id
    m=item.get('metadata') or {};title=clean(m.get('title') or item.get('title'))
    if not title:raise ValueError('Zenodo has no record title')
    paths=[];formats=[]
    for f in item.get('files',[]):
        paths.extend(v for k,v in (f.get('links') or {}).items() if k in ('self','content','download') and public_url(v))
        formats.append(f.get('type') or f.get('mimetype') or '')
    return {'external_id':external_id,'native_id':external_id,'native_catalog':'zenodo',
            'title':title,'description':clean(m.get('description')),'publisher':m.get('publisher') or 'Zenodo 원 제공자 확인',
            'url':item.get('doi_url') or 'https://zenodo.org/records/'+id,
            'catalog_url':'https://data.europa.eu/data/datasets/'+external_id,'metadata_url':'https://zenodo.org/api/records/'+id,
            'upstream_url':item.get('doi_url') or 'https://zenodo.org/records/'+id,'access_paths':sorted(set(paths)),
            'license':label(m.get('license')),'format':' / '.join(sorted(set(filter(None,formats)))),
            'native_publication_date':m.get('publication_date'),'native_access_right':m.get('access_right'),
            'native_resource_type':m.get('resource_type'),'native_series_id':str(item.get('conceptrecid') or ''),'native_version':m.get('version'),
            'source_modified':item.get('updated') or item.get('modified'),'native_repository_id':id,
            'subjects':[{'kind':'keyword','label':str(x)} for x in m.get('keywords',[])],
            'collection_method':'official_upstream_zenodo_metadata_recovery'}

def ngr_record(id,raw):
    root=ET.fromstring(raw)
    ns={'gmd':'http://www.isotc211.org/2005/gmd','gco':'http://www.isotc211.org/2005/gco','gml':'http://www.opengis.net/gml'}
    def values(path):return [clean(''.join(x.itertext())) for x in root.findall(path,ns) if clean(''.join(x.itertext()))]
    identifiers=values('./gmd:fileIdentifier')
    if identifiers!=[id]:raise ValueError('NGR record identifier does not match requested UUID')
    titles=values('.//gmd:identificationInfo//gmd:citation//gmd:title')
    if not titles:raise ValueError('NGR metadata contains no dataset/service title')
    metadata_url='https://www.nationaalgeoregister.nl/geonetwork/srv/api/records/'+id+'/formatters/xml'
    paths=values('.//gmd:distributionInfo//gmd:CI_OnlineResource/gmd:linkage/gmd:URL')
    dates=values('.//gmd:temporalElement//gml:beginPosition')+values('.//gmd:temporalElement//gml:endPosition')
    return {'external_id':id,'native_id':id,'native_catalog':'ngr-nl','title':titles[0],
            'description':'\n'.join(values('.//gmd:identificationInfo//gmd:abstract')),
            'publisher':' / '.join(values('.//gmd:pointOfContact//gmd:organisationName')) or '제공처 확인',
            'url':'https://www.nationaalgeoregister.nl/geonetwork/srv/dut/catalog.search#/metadata/'+id,
            'metadata_url':metadata_url,'access_paths':sorted({u for u in paths if public_url(u)}),
            'source_modified':' / '.join(values('./gmd:dateStamp')),'period':' ~ '.join(dates) or '미상',
            'license':' / '.join(values('.//gmd:resourceConstraints//gmd:otherConstraints')),
            'format':' / '.join(values('.//gmd:distributionFormat//gmd:name')),
            'subjects':[{'kind':'theme','label':x} for x in values('.//gmd:topicCategory')],
            'collection_method':'official_geonetwork_iso_metadata_recovery'}

def eu_upstream_record(id,original):
    cat=(original.get('catalog') or {}).get('id')
    if not id:
        paths={u for part in as_list(original.get('distributions')) for k in ('access_url','download_url') for u in as_list(part.get(k)) if isinstance(u,str) and public_url(u)}
        if label(original.get('title')) and len(paths)==1:
            upstream=next(iter(paths));derived='upstream-'+hashlib.sha256(upstream.encode()).hexdigest()
            row=piveau_record({**original,'id':derived},'eu','https://data.europa.eu/api/hub/search/')
            row.update(native_id='',url=upstream,upstream_url=upstream,
                       identifier_method='sha256_of_single_official_access_path',
                       metadata_url='https://data.europa.eu/api/hub/search/search?'+urllib.parse.urlencode({'filters':'dataset','facets':dump({'catalog':[cat]}),'q':label(original['title']),'fields':'title'}))
            return id,row,None
        return id,None,'Official catalogue record has no ID or unambiguous source path'
    if cat=='data-gov-ie':
        with database() as db:existing=db.execute('SELECT metadata FROM datasets WHERE id=?',('ireland:'+id,)).fetchone()
        if existing:
            row=json.loads(existing[0])
            for key in ('id','source_id','fingerprint','checked_at','duplicate_of','duplicate_evidence','duplicate_canonical','classification_note'):row.pop(key,None)
            row.update(external_id=id,native_catalog=cat,upstream_source_record='ireland:'+id,
                       collection_method='exact_official_provider_record_id_recovery')
            return id,row,None
    if cat=='ngr-nl':
        try:return id,ngr_record(id,read_url('https://www.nationaalgeoregister.nl/geonetwork/srv/api/records/'+id+'/formatters/xml')),None
        except Exception as e:return id,None,'Official NGR metadata: '+str(e)
    url='https://data.europa.eu/api/hub/repo/datasets/'+urllib.parse.quote(id,safe='')
    try:
        d=json.loads(read_url(url,accept='application/ld+json'));row=piveau_repository_record(id,d,'eu')
        row['native_catalog']=cat
        return id,row,None
    except Exception as e:return id,None,str(e)


def recover_eu_metadata():
    invalid={}
    for path in (ROOT/'eu-catalogs').glob('*-invalid.jsonl'):
        for line in path.read_text().splitlines():
            item=json.loads(line);invalid[item.get('id','')]=item
    recovered=set();failed={};pending=set(invalid)
    with database() as db:
        for start in range(0,len(pending),500):
            batch=list(pending)[start:start+500]
            found={r[0].split(':',1)[1] for r in db.execute('SELECT id FROM datasets WHERE id IN ('+','.join('?' for _ in batch)+')', ['eu:'+x for x in batch])}
            recovered.update(found)
    pending-=recovered
    zenodo={}
    for id in pending:
        match=re.fullmatch(r'(?:oai-zenodo-org-|oai:zenodo\.org:)(\d+)(?:~~\d+)?',id)
        if match:zenodo[id]=match[1]
    evidence=ROOT/'eu-metadata-recovery.jsonl'
    with evidence.open('a') as journal:
        ids=sorted(zenodo)
        # Public unauthenticated search explicitly limits pages to 25.
        for start in range(0,len(ids),25):
            batch=ids[start:start+25]
            query={'q':'recid:('+' OR '.join(zenodo[id] for id in batch)+')','all_versions':1,'size':25}
            url='https://zenodo.org/api/records?'+urllib.parse.urlencode(query)
            try:
                data=json.loads(read_url(url,accept='application/json'))
                items=data['hits']['hits'];returned=set()
                rows=[];received=[]
                for item in items:
                    matches=[id for id in batch if zenodo[id]==str(item.get('id'))]
                    if not matches:raise ValueError('Zenodo returned a record outside requested IDs')
                    returned.update(matches)
                    for id in matches:
                        try:
                            row=zenodo_record(item,id)
                            row['native_catalog']=(invalid[id].get('metadata',{}).get('catalog') or {}).get('id','zenodo')
                            rows.append(row);received.append(id)
                        except ValueError as e:failed[id]=str(e)
                if rows:store_raw('eu',rows);recovered.update(received)
                for id in set(batch)-returned:failed[id]='Official Zenodo search did not return this record ID'
            except Exception as e:
                for id in batch:
                    if id not in recovered:failed[id]=str(e)
            for id in batch:journal.write(dump({'id':id,'recovered':id in recovered,'error':failed.get(id),'metadata_url':'https://zenodo.org/api/records/'+zenodo[id]})+'\n')
            journal.flush()
            if start%250==0:print(dump({'source':'eu','metadata_recovery':{'recovered':len(recovered),'targeted':len(invalid),'failed':len(failed)}}),flush=True)
            time.sleep(2)
        remaining=sorted(pending-set(zenodo))
        def fetch(id):return eu_upstream_record(id,invalid[id].get('metadata',{}))
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for id,row,error in pool.map(fetch,remaining):
                if error:failed[id]=error
                else:store_raw('eu',[row]);recovered.add(id)
                journal.write(dump({'id':id,'recovered':id in recovered,'error':error})+'\n');journal.flush()
    (ROOT/'eu-metadata-unresolved.json').write_text(dump(failed))
    report('eu-recovery',{'status':'id_reconciled' if not failed else 'unreconciled','targeted':len(invalid),'recovered':len(recovered),
                         'unresolved':len(failed),'source_type_exceptions':sum('record type is not dataset' in x for x in failed.values()),'identity_from_access_path':int('' in recovered),'evidence':evidence.name,'unresolved_evidence':'eu-metadata-unresolved.json',
                         'scope':'유럽 검색 목록의 불완전한 기록을 공식 Zenodo 공개 API 및 원본 RDF 저장소에서 재확인. 자료 파일은 다운로드하지 않음.'})
    current=json.loads((ROOT/'eu.json').read_text())
    report('eu',{**current,'metadata_recovered':len(recovered),'remaining_invalid_records':len(failed),'recovery_evidence':'eu-recovery.json',
                 'records_without_official_id':int('' in invalid),'records_identified_by_access_path':int('' in recovered),
                 'observed_catalog_entries':current.get('unique_catalog_entries',0)+int('' in invalid),
                 'status':'count_reconciled' if not failed and current.get('unique_catalog_entries')==current.get('advertised') and not current.get('catalogs_unreconciled') else 'unreconciled'})


def acquisition_status():
    with database() as db:
        counts={r[0]:(r[1],r[2]) for r in db.execute("SELECT source_id,count(*),sum(CASE WHEN json_extract(metadata,'$.duplicate_of') IS NULL THEN 1 ELSE 0 END) FROM datasets GROUP BY source_id")}
        sources=[json.loads(r[0]) for r in db.execute('SELECT info FROM sources')]
    output=[]
    for source in sources:
        id=source['id'];raw,unique=counts.get(id,(0,0));reports={}
        for suffix in ('','-independent','-live','-linked-repair','-recovery'):
            path=ROOT/(id+suffix+'.json')
            if path.exists():reports[(suffix or 'collection').lstrip('-')]=json.loads(path.read_text())
        output.append({'id':id,'name':source['name'],'country':source['country'],'registered_url':source['url'],
                       'stored_source_records':raw,'searchable_records':unique,'duplicate_records':raw-unique,
                       'collector_implemented':id in collectors() or id=='singapore','reports':reports,
                       'status':'not_implemented' if id not in collectors() and id!='singapore' else 'not_audited' if not reports else 'see_reports'})
    return {'checked_at':now(),'stored_source_records':sum(x['stored_source_records'] for x in output),
            'searchable_records':sum(x['searchable_records'] for x in output),'sources':output,
            'duplicate_audit':json.loads((ROOT/'duplicates.json').read_text()) if (ROOT/'duplicates.json').exists() else None,
            'definitions':{'stored_source_records':'출처별 원본 메타데이터 기록 수. 같은 자료의 재게시가 포함될 수 있음.',
                           'searchable_records':'근거가 확인된 제공 경로 중복을 제외한 탐색 기록 수. 전 세계 고유 자료 수라는 뜻은 아님.',
                           'count_reconciled':'명시된 범위의 제공처 표시 수와 수신 고유 식별자 수를 대조함.',
                           'unreconciled':'아직 차이가 남음. 완료로 취급하지 않음.',
                           'not_implemented':'사이트 정보만 등록되어 있고 직접 목록 수집은 구현되지 않음.'}}


def collectors():
    def seoul():
        collect_seoul()
        collect_seoul_live()
    def singapore():
        collect_singapore()
        verify_singapore_missing()
    def korea():
        collect_korea()
        result=json.loads((ROOT/'korea.json').read_text())
        if result['parts']['LINKED']['status']!='count_reconciled':
            repair_korea_categories()
            if json.loads((ROOT/'korea.json').read_text())['parts']['LINKED']['status']!='count_reconciled':repair_korea_linked()
    def ckan(id):
        collect_ckan(id)
        verify_ckan_inventory(id)
    choices = {'korea':korea,'seoul':seoul,'gyeonggi':collect_gyeonggi,'kosis':collect_kosis,'busan':collect_busan,'chungbuk':collect_chungbuk,'singapore':singapore,'who':collect_who,'eurostat':collect_eurostat,'ecos':collect_ecos,'taiwan':collect_taiwan,'culture':collect_culture,'sgis':collect_sgis,'hira':collect_hira,'daejeon':collect_daejeon,'vworld':collect_vworld,'daegu':collect_daegu,'jeju':collect_jeju,'incheon':collect_incheon,'oecd':collect_oecd,'spain':collect_spain,'kma':collect_kma,'austria':lambda:collect_piveau('austria'),'eu':lambda:collect_piveau('eu'),'us':collect_us,'hongkong':collect_hongkong}
    choices.update({id:lambda id=id:ckan(id) for id in CKAN_CATALOGS})
    choices.update({id:lambda id=id:collect_udata(id) for id in ('france','portugal')})
    choices.update({id:lambda id=id:collect_series(id) for id in ('census','worldbank')})
    return choices


if __name__=='__main__':
    import sys
    choices={'eu-recovery':recover_eu_metadata,'eu-catalogs':lambda:repair_piveau_catalogs('eu'),'austria-independent':verify_austria_repository,'ecos':collect_ecos,'taiwan':collect_taiwan,'culture':collect_culture,'sgis':collect_sgis,'hira':collect_hira,'daejeon':collect_daejeon,'vworld':collect_vworld,'daegu':collect_daegu,'jeju':collect_jeju,'incheon':collect_incheon,'oecd':collect_oecd,'spain':collect_spain,'kma':collect_kma,'austria':lambda:collect_piveau('austria'),'eu':lambda:collect_piveau('eu'),'us':collect_us,'hongkong':collect_hongkong,'korea':collect_korea,'korea-linked-repair':repair_korea_linked,'korea-categories':repair_korea_categories,'singapore-independent':verify_singapore_missing,'seoul':collect_seoul,'seoul-live':collect_seoul_live,'eurostat':collect_eurostat,'gyeonggi':collect_gyeonggi,'kosis':collect_kosis,'busan':collect_busan,'chungbuk':collect_chungbuk,'singapore':collect_singapore,'who':collect_who,'duplicates':deduplicate,'australia-duplicates':lambda:deduplicate('australia')}
    choices.update({id+'-independent':lambda id=id:verify_ckan_inventory(id) for id in CKAN_CATALOGS})
    choices.update({id:lambda id=id:collect_series(id) for id in ('census','worldbank')})
    choices.update({id:lambda id=id:collect_ckan(id) for id in CKAN_CATALOGS})
    choices.update({id:lambda id=id:collect_udata(id) for id in ('france','portugal')})
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(guarded,s,choices[s]) for s in sys.argv[1:]]
        for f in futures:f.result()
