"""Conservative, shared coverage facts from dataset metadata, never its provider.

Facts are a retrieval constraint, not an independent validation of source data.
The same parser is used by the disposable filter index and final eligibility.
"""
import re
import unicodedata

VERSION = 'explicit-coverage-v4'
FIELDS = ('countries', 'regions', 'years', 'years_mode')
COUNTRIES = {
    '대한민국': ('대한민국', '한국', 'South Korea', 'Republic of Korea', 'KOR'),
    '미국': ('미국', 'United States', 'United States of America', 'USA'),
    '호주': ('호주', 'Australia', 'AUS'), '일본': ('일본', 'Japan', 'JPN'),
    '영국': ('영국', 'United Kingdom', 'GBR'), '중국': ('중국', 'China', 'CHN'),
    '캐나다': ('캐나다', 'Canada', 'CAN'), '프랑스': ('프랑스', 'France', 'FRA'),
    '독일': ('독일', 'Germany', 'DEU'), '뉴질랜드': ('뉴질랜드', 'New Zealand', 'NZL'),
    '아일랜드': ('아일랜드', 'Ireland', 'IRL'), '네덜란드': ('네덜란드', 'Netherlands', 'NLD'),
    '스페인': ('스페인', 'Spain', 'ESP'), '스위스': ('스위스', 'Switzerland', 'CHE'),
    '오스트리아': ('오스트리아', 'Austria', 'AUT'), '핀란드': ('핀란드', 'Finland', 'FIN'),
    '포르투갈': ('포르투갈', 'Portugal', 'PRT'), '싱가포르': ('싱가포르', 'Singapore', 'SGP'),
    '대만': ('대만', 'Taiwan', 'TWN'), '홍콩': ('홍콩', 'Hong Kong', 'HKG'),
}
REGIONS = {
    '서울특별시': ('대한민국', ('서울특별시', '서울', 'Seoul')),
    '부산광역시': ('대한민국', ('부산광역시', '부산', 'Busan')),
    '대구광역시': ('대한민국', ('대구광역시', '대구', 'Daegu')),
    '인천광역시': ('대한민국', ('인천광역시', '인천', 'Incheon')),
    '광주광역시': ('대한민국', ('광주광역시',)),
    '대전광역시': ('대한민국', ('대전광역시', '대전', 'Daejeon')),
    '울산광역시': ('대한민국', ('울산광역시', '울산', 'Ulsan')),
    '세종특별자치시': ('대한민국', ('세종특별자치시', '세종시')),
    '경기도': ('대한민국', ('경기도', 'Gyeonggi')),
    '강원특별자치도': ('대한민국', ('강원특별자치도', '강원도', 'Gangwon')),
    '충청북도': ('대한민국', ('충청북도', 'Chungcheongbuk')),
    '충청남도': ('대한민국', ('충청남도', 'Chungcheongnam')),
    '전북특별자치도': ('대한민국', ('전북특별자치도', '전라북도', 'Jeonbuk')),
    '전라남도': ('대한민국', ('전라남도', 'Jeollanam')),
    '경상북도': ('대한민국', ('경상북도', 'Gyeongsangbuk')),
    '경상남도': ('대한민국', ('경상남도', 'Gyeongsangnam')),
    '제주특별자치도': ('대한민국', ('제주특별자치도', '제주도', 'Jeju')),
    '캘리포니아': ('미국', ('캘리포니아', '캘리포니아주', 'California')),
    '뉴욕주': ('미국', ('뉴욕주', 'New York State')),
    '텍사스': ('미국', ('텍사스', 'Texas')),
    '플로리다': ('미국', ('플로리다', 'Florida')),
    '뉴사우스웨일스': ('호주', ('뉴사우스웨일스', 'New South Wales')),
    '퀸즐랜드': ('호주', ('퀸즐랜드', 'Queensland')),
}
UNKNOWN = {'', '미상', '미확인', '제공처 확인', 'unknown', 'n/a', 'none', 'null', '전국', '국내', 'global', 'worldwide'}
NEGATIVE = re.compile(r'제외|미포함|제공하지|\b(?:not|except|excluding|excluded|unavailable)\b', re.I)
SYNTHETIC = re.compile(r'상세\s*지역|설명\s*참고|원문\s*확인|see description|refer to description|https?://', re.I)
UPDATED = re.compile(r'갱신|공표|발행|수정|게시|발표|기후평년|updated?|published|released?|climate normal', re.I)
INCIDENTAL = re.compile(r'\b(?:published by|produced by|provided by|contact|university|institute|for example|such as|could|might|methodology|developed|prepared|author|publisher|provider|telephone)\b|대학교|예를\s*들|가령|발행기관|제공기관|문의|연락처|담당관|방법론|제작기관', re.I)
SCOPE_BEFORE = re.compile(r'\b(?:population|residents|statistics|observations|measurements|estimates|survey data|data|dataset)\s+(?:for|of|in|covers?|covering)\s+(?:the\s+)?$|\b(?:geographic(?:al)? coverage|spatial coverage|coverage|area covered|region covered)\s*(?::|=|is|includes|covers)\s*(?:the\s+)?$')
SCOPE_AFTER = re.compile(r'^\s+(?:resident\s+)?(?:population|residents|census|statistics|observations|measurements|data)\b|^(?:의|\s*지역|\s*내)\s*[^.!?;]{0,50}(?:자료|통계|인구|관측|측정|조사)')


def normal(value):
    return ' '.join(unicodedata.normalize('NFKC', value).casefold().split())


def values(value):
    if isinstance(value, str):return [value] if value.strip() else []
    if isinstance(value, list):return [v for v in value[:64] if isinstance(v, str) and v.strip()]
    return []


def usable(value):
    return normal(value) not in UNKNOWN and not SYNTHETIC.search(value) and not NEGATIVE.search(value)


def contains(text, term):
    text,term=normal(text),normal(term)
    if term.isascii():return bool(re.search(r'(?<![\w])'+re.escape(term)+r'(?![\w])',text))
    if term=='한국':return bool(re.search(r'한국(?!어|인|학|은행)',text))
    return bool(term and term in text)


_COUNTRY_NAMES={normal(alias):name for name,aliases in COUNTRIES.items() for alias in aliases}
_REGION_NAMES={normal(alias):name for name,(_,aliases) in REGIONS.items() for alias in aliases}


def _pattern(aliases):
    parts=[]
    for alias in aliases:
        alias=normal(alias)
        parts.append(r'(?<![\w])'+re.escape(alias)+r'(?![\w])' if alias.isascii()
                     else r'한국(?!어|인|학|은행)' if alias=='한국' else re.escape(alias))
    return re.compile('|'.join(parts))


_COUNTRY_PATTERNS={name:_pattern(aliases) for name,aliases in COUNTRIES.items()}
_REGION_PATTERNS={name:_pattern(aliases) for name,(_,aliases) in REGIONS.items()}
_ALL_ALIASES=set(_COUNTRY_NAMES)|set(_REGION_NAMES)
_ASCII_PLACES=re.compile(r'(?<![\w])(?:'+'|'.join(re.escape(a) for a in sorted(_ALL_ALIASES,key=len,reverse=True) if a.isascii())+r')(?![\w])')
_LOCAL_PLACES=[a for a in _ALL_ALIASES if not a.isascii()]


def _place_aliases(text):
    found={m[0] for m in _ASCII_PLACES.finditer(text)}
    found.update(a for a in _LOCAL_PLACES if a in text and (a!='한국' or re.search(r'한국(?!어|인|학|은행)',text)))
    return found


def _places(text):
    found=_place_aliases(text)
    regions={_REGION_NAMES[a] for a in found if a in _REGION_NAMES}
    countries={_COUNTRY_NAMES[a] for a in found if a in _COUNTRY_NAMES}
    countries.update(REGIONS[r][0] for r in regions)
    return countries,regions


def _description_places(sentence):
    """Require a local grammatical coverage link, not location co-occurrence."""
    if not usable(sentence) or INCIDENTAL.search(sentence):return []
    text=normal(sentence);accepted=[]
    for alias in _place_aliases(text):
        for match in re.finditer(re.escape(alias),text):
            if SCOPE_BEFORE.search(text[max(0,match.start()-90):match.start()]) or SCOPE_AFTER.search(text[match.end():match.end()+90]):
                accepted.append(alias);break
    return accepted


def country(value):return _COUNTRY_NAMES.get(normal(value),value.strip())
def region(value):return _REGION_NAMES.get(normal(value),value.strip())


def _years(text):
    if not isinstance(text,str) or not usable(text):return []
    hits=[int(y) for y in re.findall(r'(?<!\d)(?:1[6-9]\d{2}|2[01]\d{2}|2200)(?!\d)',text)]
    interval=re.search(r'(?<!\d)(\d{4})\s*(?:~|–|—|-|\.\.|부터|to)\s*(\d{4})(?!\d)',text,re.I)
    if interval:
        lo,hi=map(int,interval.groups())
        if 1600<=lo<=hi<=2200:
            frequency=re.match(r'\s*(\d+)년\s*\(',text)
            step=int(frequency[1]) if frequency else 1
            return list(range(lo,hi+1,step)) if 1<=step<=100 else []
    return sorted(set(hits))


# Only labelled data-reference years, never any year appearing in prose.
# Boundary vintage, publication dates and climate-normal baselines are different
# dimensions. Conflicting labels cannot establish a single dataset period.
REFERENCE_YEAR = re.compile(
    r'(?<![가-힣A-Za-z])(?:[가-힣]{0,12}통계|자료(?:기준|대상)?|관측|측정|조사)(?:년도?|연도)\s*[:：=]\s*'
    r'|\b(?:data reference|reference|observation|measurement|survey)\s+year\s*[:：=]\s*', re.I)
REFERENCE_AUXILIARY = re.compile(
    r'\b(?:boundary|boundaries|publication|published|updated?|climate\s+(?:normal|baseline)|baseline)\s*$',re.I)


def reference_years(description):
    if not isinstance(description,str):return [],False
    years=set();denied=False
    for segment in re.split(r'[\n\r;]|(?<=[.!?])\s+',description[:6000]):
        matches=[m for m in REFERENCE_YEAR.finditer(segment)
                 if not REFERENCE_AUXILIARY.search(segment[max(0,m.start()-80):m.start()])]
        if matches and (NEGATIVE.search(segment) or re.search(r'아님|아니[다며라]|미제공',segment)):
            denied=True;continue
        for match in matches:
            tail=segment[match.end():]
            if re.match(r'(?:unknown|unavailable|미상|미확인)\b',tail,re.I):
                denied=True;continue
            value=re.match(r'((?:1[6-9]\d{2}|2[01]\d{2}|2200))(?!\d)',tail)
            if value:years.add(int(value.group(1)))
    return sorted(years),denied


def profile(metadata):
    """Keep explicit scope authoritative; use actual dataset text for missing scope.

    Provider names, URL years and collector-generated country/description hints
    are deliberately absent. A place mentioned in prose is metadata evidence,
    not proof of download contents or of an administrative breakdown.
    """
    raw_countries=[v for k in ('coverage_countries','countries') for v in values(metadata.get(k))]
    raw_regions=[v for k in ('coverage_regions','regions','region') for v in values(metadata.get(k))]
    country_denied=any(NEGATIVE.search(v) for v in raw_countries)
    region_denied=any(NEGATIVE.search(v) for v in raw_regions)
    explicit_countries=[country(v) for v in raw_countries if usable(v)]
    explicit_regions=[region(v) for v in raw_regions if usable(v)]
    if country_denied:explicit_countries=[]
    if region_denied:explicit_regions=[]
    parts=[]
    for key in ('title','native_catalog_path','native_catalog_paths','survey_name','survey_title','native_survey_name','statistical_survey_name'):
        parts.extend(v[:2000] for v in values(metadata.get(key)) if usable(v) and not INCIDENTAL.search(v))
    # Descriptions supply place evidence only through bounded sentences, avoiding
    # negated statements. Years require separate explicit reference labels.
    description=metadata.get('description','')
    if isinstance(description,str):
        for sentence in re.split(r'[\n\r;]|(?<=[.!?])\s+',description[:6000]):
            parts.extend(_description_places(sentence))
    narrative='\n'.join(parts)[:12000]
    countries=set(explicit_countries)
    region_known=bool(explicit_regions) or region_denied
    inference_text=normal('\n'.join(explicit_regions) if region_known else narrative)
    inferred,found_regions=_places(inference_text)
    if not explicit_countries and not country_denied:
        countries.update(inferred)
    geo_text='\n'.join(explicit_regions) if region_known else narrative
    years=[];typed_years=False
    year_denied=any(NEGATIVE.search(v) for k in ('temporal_coverage','period') for v in values(metadata.get(k)))
    raw=metadata.get('coverage_years')
    if isinstance(raw,list) and raw and all(type(v) is int and 1600<=v<=2200 for v in raw):
        years=sorted(set(raw));typed_years=True
    else:
        for key in ('temporal_coverage','period'):
            candidates=_years(metadata.get(key))
            if candidates:years=candidates;typed_years=True;break
        title=metadata.get('title','')
        if not years and isinstance(title,str) and not UPDATED.search(title):
            found=_years(title)
            if len(found)==1 or re.search(r'\d{4}\s*(?:~|–|—|-|\.\.|to)\s*\d{4}',title):years=found
    labelled,reference_denied=reference_years(description)
    if labelled:
        # A title may date its administrative boundaries while the population
        # or other observations are older. Do not silently choose either year.
        if years and not set(labelled).issubset(years) or not years and len(labelled)>1:
            years=[];typed_years=True
        elif not years:
            years=labelled;typed_years=True
    if year_denied or reference_denied:years=[];typed_years=True
    return {'version':VERSION,'countries':sorted(countries),'regions':sorted(found_regions|set(explicit_regions)),
            'geo_text':geo_text,'years':years,'country_explicit':bool(explicit_countries) or country_denied or region_known,
            'region_explicit':region_known,'year_explicit':typed_years}


def validate_groups(groups):
    if groups is None:return []
    if not isinstance(groups,list) or len(groups)>5:raise ValueError('invalid coverage groups')
    result=[]
    for entry in groups:
        if not isinstance(entry,dict) or set(entry)-set(FIELDS):raise ValueError('invalid coverage filter')
        group={}
        for key,maximum in (('countries',5),('regions',4)):
            vals=entry.get(key,[])
            if (not isinstance(vals,list) or len(vals)>maximum or any(not isinstance(v,str) or not v.strip() or len(v)>60 or any(ord(c)<32 for c in v) for v in vals)):
                raise ValueError('invalid coverage place')
            group[key]=sorted(set((country if key=='countries' else region)(v) for v in vals))
        years=entry.get('years',[])
        if not isinstance(years,list) or len(years)>10 or any(type(v) is not int or not 1600<=v<=2200 for v in years):raise ValueError('invalid coverage year')
        mode=entry.get('years_mode','list')
        if mode not in ('range','list'):raise ValueError('invalid coverage period mode')
        group.update(years=sorted(set(years)),years_mode=mode)
        if group not in result:result.append(group)
    return result


def assess(facts,criteria):
    group=validate_groups([criteria])[0]
    missing=[];conflicts=[]
    for key,explicit in (('countries','country_explicit'),('regions','region_explicit')):
        for value in group[key]:
            aliases=REGIONS.get(value,('',(value,)))[1] if key=='regions' else (value,)
            matched=value in facts[key] or (key=='regions' and any(contains(facts['geo_text'],a) for a in aliases))
            if not matched:
                token=key+':'+value
                (conflicts if facts[explicit] else missing).append(token)
    years=group['years']
    requested=set(range(min(years),max(years)+1)) if years and group['years_mode']=='range' else set(years)
    if not requested<=set(facts['years']):
        (conflicts if facts['year_explicit'] or facts['years'] else missing).extend('years:'+str(y) for y in sorted(requested-set(facts['years'])))
    return {'status':'conflict' if conflicts else 'unknown' if missing else 'match','missing':missing,'conflicts':conflicts}


def any_match(facts,groups):
    return not groups or any(assess(facts,g)['status']=='match' for g in groups)


def groups_for_plan(plan):
    from .site_search import effective_plan
    groups=validate_groups([{k:effective_plan(plan,n).get(k,'list' if k=='years_mode' else []) for k in FIELDS} for n in plan['needs']])
    if any(not any(g[k] for k in ('countries','regions','years')) for g in groups):return []
    return groups
