"""Versioned semantic types; a link's domain/range and meaning are executable.

The ontology describes discovery metadata, not copies of source observations.
Analytical rules, user intent and documented availability have separate bases.
"""
SCHEMA_VERSION = '1.2.0'

# A nullable property means unverified/unknown, never false or unrestricted.
OBJECT_TYPES = {
    'domain': {'label':'카탈로그 분야','definition':'전체 카탈로그를 조직하는 주제 분류. 소속 자료가 세부 지표를 제공하거나 검색 조건을 만족한다는 뜻은 아니다.',
               'properties':{'catalog_id':'text','definition':'text','record_count':'nullable_integer','count_basis':'text','browse_path':'text'}},
    'request': {'label':'사용자 요청','definition':'이번 질문의 목적과 명시된 데이터 요구. 제공 사실의 근거로 사용할 수 없다.',
                'properties':{'purpose':'text'}},
    'purpose': {'label':'업무 목적','definition':'중소기업의 의사결정을 위한 업무 유형. 분석 관점의 필요성을 정의한다.',
                'properties':{'description':'text','limitations':'texts'}},
    'analysis': {'label':'분석 관점','definition':'업무 목적을 판단하기 위해 확인할 분석 항목. 통계적 인과관계나 결합 가능성을 보증하지 않는다.',
                 'properties':{'reason':'text'}},
    'indicator': {'label':'데이터 개념','definition':'관측 대상·측정 의미·단위 차원·시간과 공간 기준을 구분한 데이터 종류.',
                  'properties':{'definition':'text','entity_type':'text','measure_kind':'text','aliases':'texts',
                    'unit_dimension':'text','canonical_unit':'nullable_text','temporal_meaning':'text',
                    'spatial_semantics':'text','not_equivalent_to':'texts','comparability_requirements':'texts',
                    'definition_basis':'text','retrieval_policy':'text'}},
    'dataset': {'label':'공식 안내의 자료 단위','definition':'공식 문서에서 식별한 제공 자료. 개별 원본 파일 또는 API 응답을 수집한 객체가 아니다.',
                'properties':{'identity_basis':'text','resource_key':'text','reference_urls':'urls'}},
    'assertion': {'label':'제공 범위 주장','definition':'한 문서에서 확인한 자료·사이트·지표·범위·형식의 원자적 주장. 다른 주장과 속성을 섞어 조건 충족을 만들 수 없다.',
                  'properties':{'claim_id':'text','source_id':'text','resource_id':'text','countries':'texts','regions':'texts',
                    'years':'integers','dates':'texts','frequencies':'texts','geography_levels':'texts','formats':'texts',
                    'delivery':'texts','fields':'texts','subjects':'texts','free':'nullable_bool','commercial':'nullable_bool',
                    'observed_unit':'nullable_text','measurement_basis':'nullable_text','sample_verified':'bool'}},
    'provider': {'label':'접근 사이트','definition':'등록된 자료 접근 홈페이지. 자료 생산기관·소유자·자료 대상 국가와 동일시하지 않는다.',
                 'properties':{'url':'url','catalogue_country':'text','role':'text'}},
    'organization': {'label':'생산·발행 기관','definition':'자료의 생산 또는 발행 책임이 별도 근거로 확인된 기관. 포털 이름만으로 생성하지 않는다.',
                     'properties':{'name':'text'}},
    'evidence': {'label':'공식 문서 근거','definition':'제공 주장 확인에 사용한 공식 안내와 확인일. 문서 확인일은 데이터 기준일이 아니다.',
                 'properties':{'url':'url','checked_on':'text','level':'text','sample_verified':'bool',
                    'independent_evidence_key':'text'}},
    'area': {'label':'자료 대상 지역','definition':'자료가 다루는 국가 또는 지역. 기관 소재지·통계 경계·조인키와 동일하지 않다.',
             'properties':{'level':'text','boundary_version':'nullable_text'}},
    'format': {'label':'데이터 표현 형식','definition':'CSV, JSON, XML 같은 직렬화·파일 형식. API 접근 방식과 구분한다.',
               'properties':{'name':'text'}},
    'access': {'label':'접근 방식','definition':'API 호출 또는 파일 다운로드. 형식·무료 여부·상업적 권리·인증 여부를 대신하지 않는다.',
               'properties':{'name':'text'}},
}

def link(label, domain, range_, minimum, maximum, definition, basis, inverse=None):
    return {'label':label,'domain':domain,'range':range_,'min_targets':minimum,'max_targets':maximum,
            'definition':definition,'basis':basis,'inverse':inverse,'transitive':False}

LINK_TYPES = {
    'selects_workflow':link('업무로 해석','request','purpose',0,2,'사용자 의도를 등록된 업무 유형에 연결한다.','user_intent'),
    'requests_indicator':link('직접 요청','request','indicator',0,5,'사용자가 명시한 종류를 유지하며 업무의 기본 목록으로 덮어쓰지 않는다.','user_intent'),
    'requires_analysis':link('판단에 필요','purpose','analysis',1,3,'업무 판단에 사용할 분석 관점. 검토된 분석 설계이며 제공 사실은 아니다.','curated_domain_rule'),
    'uses_indicator':link('분석에 활용','analysis','indicator',1,5,'분석 관점을 확인하는 데이터 종류. 데이터 간 인과성·결합 가능성은 함의하지 않는다.','curated_domain_rule'),
    'documents_indicator':link('제공 지표 명시','assertion','indicator',1,8,'이 원자적 제공 주장이 다루는 데이터 개념. 상위 개념으로 확대하지 않는다.','official_documentation','has_documented_offer'),
    'has_documented_offer':link('제공 근거 탐색','indicator','assertion',0,256,'documents_indicator의 역방향 탐색. 동일한 주장의 범위만 검사한다.','official_documentation','documents_indicator'),
    'describes_resource':link('대상 자료','assertion','dataset',1,1,'주장이 설명하는 공식 자료 단위. 제목이 같다는 이유로 자료를 병합하지 않는다.','official_documentation'),
    'available_at':link('접근 사이트','assertion','provider',1,1,'이 제공 주장이 확인된 등록 사이트. 생산기관이라는 주장은 아니다.','official_documentation'),
    'evidenced_by':link('확인 근거','assertion','evidence',1,1,'원자적 주장의 공식 안내 근거. 같은 URL의 반복은 독립 검증 횟수를 늘리지 않는다.','official_documentation'),
    'published_by':link('발행 책임','dataset','organization',0,1,'문서가 명시한 발행 기관만 연결한다. 포털명으로 추정하지 않는다.','official_documentation'),
    'covers_area':link('자료 대상','assertion','area',0,16,'주장에 명시된 국가·지역. 하위 지역으로 자동 전파하지 않는다.','official_documentation'),
    'supports_format':link('제공 형식','assertion','format',0,12,'이 주장에서 확인한 표현 형식. 다른 자료의 형식을 가져오지 않는다.','official_documentation'),
    'accessed_via':link('접근 방식','assertion','access',0,2,'확인된 API/다운로드 방식. 인증·가격·이용권한은 별도 조건이다.','official_documentation'),
    'not_equivalent_to':link('동일 개념 아님','indicator','indicator',0,41,'개념 간 단위·모집단·의미 차이. 추천 확대나 조인 근거로 탐색하지 않는다.','curated_domain_definition','not_equivalent_to'),
    'demographic_change_context':link('인구 변화 맥락','indicator','indicator',0,8,'인구 규모의 변화를 해석할 때 함께 확인할 인구동태 지표. 비율을 인구 증감 인원으로 직접 환산하거나 인과 효과를 추정하지 않는다.','curated_domain_rule'),
    'age_composition_context':link('연령 구성 맥락','indicator','indicator',0,8,'전체 인구의 연령 구성을 이해하는 데 필요한 지표. 전체 규모와 구성 비율을 동일시하지 않는다.','curated_domain_rule'),
    'complementary_indicator':link('함께 살펴볼 지표','indicator','indicator',0,8,'사전 검토한 분석 맥락에서 함께 살펴볼 지표. 같은 개념이나 결합 가능한 자료라는 뜻은 아니다.','curated_domain_rule'),
    'spatial_context':link('공간 해석에 활용','indicator','indicator',0,8,'위치·경계·거리·공간 분포를 해석하기 위한 맥락. 좌표계·경계 버전·공간 결합 가능성을 따로 확인한다.','curated_domain_rule'),
    'operational_context':link('서비스 운영 맥락','indicator','indicator',0,8,'시설·서비스의 공급 또는 운영 조건을 함께 검토하는 관계. 동일 시설 식별자나 실시간 가용성을 보장하지 않는다.','curated_domain_rule'),
    'economic_context':link('경제 활동 맥락','indicator','indicator',0,8,'규모·소득·가격·생산·고용 등 경제 활동을 해석할 보완 지표. 단위·모집단·가격 기준과 인과성은 별도 검증한다.','curated_domain_rule'),
    'environmental_context':link('자연·환경 맥락','indicator','indicator',0,8,'자연 조건·자원·환경 상태를 함께 검토하는 관계. 시간·공간 해상도와 측정 방식의 일치를 보장하지 않는다.','curated_domain_rule'),
    'institutional_context':link('제도·정책 맥락','indicator','indicator',0,8,'행정·법률·정책 및 국제 관계의 해석 맥락. 적용 관할·시점·정책 효과를 추측하지 않는다.','curated_domain_rule'),
    'measurement_context':link('정의·측정 맥락','indicator','indicator',0,8,'통계·연구의 정의와 측정 체계를 이해하기 위한 관계. 실제 자료의 품질·대표성·동일성을 인증하지 않는다.','curated_domain_rule'),
    'in_catalog_domain':link('탐색할 분야','indicator','domain',0,3,'이 개념과 관련된 카탈로그 주제 분류. 주제 일치만으로 세부 지표 제공을 확정하지 않는다.','curated_domain_definition'),
}
RELATED_RELATIONS = frozenset({'demographic_change_context','age_composition_context','complementary_indicator',
                              'spatial_context','operational_context','economic_context','environmental_context','institutional_context','measurement_context'})

INFERENCE_RULES = [
 {'id':'workflow_requires_indicator','premises':['requires_analysis','uses_indicator'],
  'conclusion':'업무 목적에서 분석 관점을 거쳐 필요한 데이터 종류를 도출',
  'restriction':'분석가가 검토한 경로만 사용; 직접 요청과 업무에서 도출한 지표를 구분'},
 {'id':'curated_related_indicator','premises':sorted(RELATED_RELATIONS),
  'conclusion':'직접 요청한 개념에서 사전 정의된 관계 한 단계의 관련 지표와 제공 사이트를 추천',
  'restriction':'직접 요청을 덮어쓰지 않음; 최대4개, 관련 지표에서 다시 확장하지 않음; 관련성은 동일성·인과성·제공 사실의 증거가 아님'},
 {'id':'documented_site_candidate','premises':['has_documented_offer','describes_resource','available_at','evidenced_by'],
  'conclusion':'도달한 원자적 제공 주장에 모든 요청 조건이 맞으면 기존 사이트 추천',
  'restriction':'국가별 개별 범위의 합집합은 탐색용; 조인·비교 가능성 또는 단일 자료의 전 국가 제공을 추론하지 않음'},
]

COMPARABILITY_REQUIREMENTS = [
 '같은 개념 이름뿐 아니라 모집단·대상 집단·측정 정의가 일치하는지 확인',
 '실제 자료의 단위·배율·통화와 가격 기준을 확인',
 '관측 기준일·기간·집계 주기와 경계 버전을 확인',
 '지역 단위·좌표계·식별키 및 결측·중복 처리 기준을 확인',
 '이용조건을 확인하고 실제 표본으로 결합 결과를 검증',
]

DESIGN_REFERENCES = [
 'https://www.palantir.com/docs/foundry/object-link-types/type-reference',
 'https://www.palantir.com/docs/foundry/object-link-types/properties-overview',
]
