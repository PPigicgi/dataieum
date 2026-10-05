"""Paginated public metadata only; no data rows or resource downloads."""
import hashlib
import json
import re
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode, urlsplit

from catalog import database, dump, now, save_records

SYNC_LOCK = threading.Lock()
# Select catalog fields on the provider, before transfer; resource files are never fetched.
AU_FIELDS = "id,name,title,notes,organization,metadata_modified,license_id,license_title,res_format,tags,groups,theme"
FR_FIELDS = "data{id,title,description,page,organization{name,badges},license,resources{format},last_modified,tags},total,page,page_size,next_page"
CKAN = {
    "uk": ("https://ckan.publishing.service.gov.uk/api/3/action/package_search", "https://www.data.gov.uk/dataset/", "영국"),
    "australia": ("https://data.gov.au/data/api/3/action/package_search", "https://data.gov.au/data/dataset/", "호주"),
    "switzerland": ("https://ckan.opendata.swiss/api/3/action/package_search", "https://opendata.swiss/en/dataset/", "스위스"),
    "ireland": ("https://data.gov.ie/api/3/action/package_search", "https://data.gov.ie/dataset/", "아일랜드"),
    "netherlands": ("https://data.overheid.nl/data/api/3/action/package_search", "https://data.overheid.nl/dataset/", "네덜란드"),
    "japan": ("https://data.e-gov.go.jp/data/ja/api/3/action/package_search", "https://data.e-gov.go.jp/data/ja/dataset/", "일본"),
}
UDATA = {"france": ("https://www.data.gouv.fr/api/1/datasets/", "프랑스"),
         "portugal": ("https://dados.gov.pt/api/1/datasets/", "포르투갈")}
COLLECTORS = ["korea", "seoul", "gyeonggi", "kosis", "busan", "chungbuk", "worldbank", "census", *CKAN, *UDATA, "singapore", "canada", "finland", "germany", "newzealand", "eurostat", "who"]
ALLOWED_HOSTS = {"www.data.go.kr", "api-production.data.gov.sg", "api.worldbank.org", "api.census.gov", "data.gov.uk", "www.data.gov.uk",
                 *(urlsplit(v[0]).hostname for v in [*CKAN.values(), *UDATA.values()])}


class MetadataTooLarge(ValueError):
    pass


class RestrictedRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if urlsplit(newurl).scheme != "https" or urlsplit(newurl).hostname not in ALLOWED_HOSTS:
            raise ValueError("등록 범위를 벗어난 리디렉션")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def fetch(url, *, fields=None):
    if urlsplit(url).hostname not in ALLOWED_HOSTS or urlsplit(url).scheme != "https":
        raise ValueError("등록되지 않은 메타데이터 주소")
    for attempt in range(3):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (PublicAtlas metadata catalog)", "Accept": "application/json"}
            if fields:
                headers["X-Fields"] = fields
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.build_opener(RestrictedRedirect).open(request, timeout=45) as response:
                content = response.read(16_000_001)
            break
        except urllib.error.HTTPError as error:
            if attempt == 2 or error.code not in (429, 500, 502, 503, 504):
                raise
            delay = error.headers.get("Retry-After", "")
            # Long server-requested waits pause the source rather than retry early.
            if not delay.isdigit() and delay:
                raise
            if delay.isdigit() and int(delay) > 60:
                raise
            time.sleep(max(2 ** (attempt + 1), int(delay or 0)))
        except OSError:
            if attempt == 2:
                raise
            time.sleep(2 ** (attempt + 1))
    if len(content) > 16_000_000:
        raise MetadataTooLarge("메타데이터 응답 크기 제한 초과")
    try:
        return json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("JSON 대신 다른 응답 수신 — 접근 제한 또는 제공처 변경 확인 필요") from error


def wording(value):
    if isinstance(value, dict):
        return next((value[k] for k in ("en", "ko", "de", "fr", "it") if isinstance(value.get(k), str) and value[k]), "") or next((v for v in value.values() if isinstance(v, str) and v), "")
    return value if isinstance(value, str) else ""


def source_subjects(item):
    subjects = []

    def add(kind, value):
        if isinstance(value, str) and value.startswith(("[", "{")):
            try:
                value = json.loads(value)
            except ValueError:
                pass
        if isinstance(value, list):
            for part in value:
                add(kind, part)
        elif isinstance(value, dict):
            add(kind, value.get("display_name") or value.get("title") or value.get("name") or wording(value))
        elif isinstance(value, str) and value.strip():
            subject = {"kind": kind, "label": value.strip()}
            if subject not in subjects:
                subjects.append(subject)

    for field in ("tags", "groups", "theme", "theme_primary", "theme_secondary", "topic_category", "subject", "subjects", "themes"):
        add("tag" if field == "tags" else "theme", item.get(field))
    for extra in item.get("extras", []):
        if isinstance(extra, dict) and any(t in extra.get("key", "").lower() for t in ("theme", "topic", "subject", "keyword")):
            add("theme", extra.get("value"))
    for resource in item.get("resources", []):
        if isinstance(resource, dict):
            add("resource_title", resource.get("name") or resource.get("title"))
            add("resource_description", resource.get("description"))
    return sorted(subjects, key=lambda s: (s["kind"], s["label"]))


def ckan_page(source_id, cursor):
    endpoint, landing, region = CKAN[source_id]
    rows = 200
    while True:
        params = {"rows": rows, "start": cursor, "sort": "id asc"}
        if source_id == "australia":
            # This installation expects fl as a comma-separated string, but facet.field as JSON.
            params.update(fl=AU_FIELDS, **{"facet.field": '["organization"]', "facet.limit": -1})
        url = endpoint + "?" + urlencode(params)
        try:
            payload = fetch(url)
            break
        except MetadataTooLarge:
            if rows == 1:
                raise
            rows = max(1, rows // 2)
    if payload.get("success") is not True:
        raise ValueError("CKAN 목록 조회 실패: " + str(payload.get("error"))[:180])
    result = payload["result"]
    items, total = result["results"], int(result["count"])
    organizations = {o["name"]: o.get("display_name") or o["name"] for o in result.get("search_facets", {}).get("organization", {}).get("items", [])}
    records = []
    for d in items:
        organization = d.get("organization") or {}
        publisher = wording(organization.get("title")) if isinstance(organization, dict) else organizations.get(organization) or organization
        if source_id == "uk":
            publisher = next((wording(e.get("value")) for e in d.get("extras", []) if e.get("key") == "dcat_publisher_name" and wording(e.get("value"))), publisher)
        formats = d.get("res_format") if source_id == "australia" else [r.get("format") for r in d.get("resources", [])]
        if isinstance(formats, str):
            formats = [formats]
        records.append(dict(external_id=d["id"], title=wording(d.get("title")) or wording(d.get("title_translated")) or d["name"],
                            description=wording(d.get("notes")) or wording(d.get("description")) or wording(d.get("notes_translated")),
                            publisher=publisher or "제공처 확인",
                            url=landing + (d["id"] + "/" + d["name"] if source_id == "uk" and d.get("name") else d.get("name") or d["id"]), metadata_url=endpoint,
                            region=region + " / 상세 지역은 설명 참고", license=wording(d.get("license_title")) or wording(d.get("license_id")) or "제공처 확인",
                            format=" / ".join(sorted({wording(f) for f in (formats or []) if wording(f)})) or "제공처 확인",
                            source_modified=wording(d.get("metadata_modified")), subjects=source_subjects(d)))
    return records, total, len(items), cursor + len(items), cursor + len(items) >= total


def udata_page(source_id, cursor):
    endpoint, region = UDATA[source_id]
    result = fetch(endpoint + "?" + urlencode({"page_size": 100, "page": cursor}), fields=FR_FIELDS if source_id == "france" else None)
    items, total = result["data"], int(result["total"])
    records = []
    for d in items:
        organization = d.get("organization") or {}
        if not any(b.get("kind") == "public-service" for b in organization.get("badges", [])):
            continue
        records.append(dict(external_id=d["id"], title=d["title"], description=d.get("description") or "",
                            publisher=organization.get("name") or "제공처 확인", url=d["page"], metadata_url=endpoint + d["id"] + "/",
                            region=region + " / 상세 지역은 설명 참고", license=d.get("license") or "제공처 확인",
                            format=" / ".join(sorted({r.get("format") for r in d.get("resources", []) if r.get("format")})) or "제공처 확인",
                            source_modified=d.get("last_modified") or "", subjects=source_subjects(d)))
    return records, total, len(items), cursor + 1, cursor * int(result["page_size"]) >= total


def metadata_page(source_id, cursor):
    if source_id in CKAN:
        return ckan_page(source_id, cursor)
    if source_id in UDATA:
        return udata_page(source_id, cursor)
    if source_id == "singapore":
        endpoint = "https://api-production.data.gov.sg/v2/public/api/datasets"
        result = fetch(endpoint + f"?page={cursor}")["data"]
        records = []
        for d in result["datasets"]:
            if d.get("status") != "active":
                continue
            id = d["datasetId"]
            if not re.fullmatch(r"d_[a-zA-Z0-9]+", id):
                raise ValueError("제공처 데이터 ID 형식 변경")
            records.append(dict(external_id=id, title=d["name"], description=d.get("description") or "",
                                publisher=d.get("managedByAgencyName") or "싱가포르 정부", url=f"https://data.gov.sg/datasets/{id}/view",
                                metadata_url=endpoint, region="싱가포르", period=" ~ ".join(filter(None, [d.get("coverageStart"), d.get("coverageEnd")])) or "미상",
                                format=d.get("format") or "미상", source_modified=d.get("lastUpdatedAt") or ""))
        return records, int(result["totalRowCount"]), len(result["datasets"]), cursor + 1, cursor >= int(result["pages"])
    if source_id == "worldbank":
        endpoint = "https://api.worldbank.org/v2/sources"
        meta, items = fetch(endpoint + f"?format=json&per_page=1000&page={cursor}")
        records = [dict(external_id="source-" + d["id"], title=d["name"], description=d.get("description") or "",
                        publisher="World Bank", url=f'https://databank.worldbank.org/source/{d["id"]}',
                        metadata_url=endpoint + "/" + d["id"] + "?format=json", region="여러 국가·지역",
                        format="데이터베이스 / 이용 안내", source_modified=d.get("lastupdated") or "") for d in items]
        return records, int(meta["total"]), len(items), cursor + 1, cursor >= int(meta["pages"])
    if source_id == "census":
        endpoint = "https://api.census.gov/data.json"
        items = fetch(endpoint)["dataset"]
        series = {}
        for d in items:
            key = "/".join(d.get("c_dataset") or [])
            if key and d.get("accessLevel") == "public":
                series.setdefault(key, []).append(d)
        records = []
        for key, editions in series.items():
            d = max(editions, key=lambda x: (int(x.get("c_vintage") or 0), x.get("modified") or ""))
            years = sorted({str(x["c_vintage"]) for x in editions if x.get("c_vintage")})
            # Reuse the provider's series identifier; don't split variables, tables or annual editions.
            landing = d.get("c_examplesLink") or d.get("c_documentationLink") or "https://www.census.gov/data/developers.html"
            records.append(dict(external_id="series-" + key, title=re.sub(r"^\d{4}\s+", "", d["title"]),
                                description=d.get("description") or "", publisher=(d.get("publisher") or {}).get("name") or "U.S. Census Bureau",
                                region=d.get("spatial") or "미국", period=", ".join(years) or "제공처 확인",
                                url=landing, metadata_url=endpoint, format="통계 자료 / API 이용 안내",
                                license=d.get("license") or "제공처 확인", source_modified=d.get("modified") or ""))
        return records, len(items), len(items), 1, True
    if source_id == "korea":
        from collection_audit import korea_page
        kinds = ("API", "FILE", "STD", "LINKED")
        kind_index, page = divmod(int(cursor), 1_000_000)
        if kind_index >= len(kinds) or page < 1:
            raise ValueError("공공데이터포털 목록 커서가 올바르지 않습니다.")
        records, total, url = korea_page(kinds[kind_index], page)
        last = page * 1000 >= total
        following = (kind_index + 1) * 1_000_000 + 1 if last else cursor + 1
        return records, None, len(records), following, last and kind_index == len(kinds)-1
    raise ValueError("수집기가 구현되지 않은 사이트입니다.")


def set_scan(source_id, scan, error=""):
    with database() as db:
        db.execute("UPDATE sources SET info=json_set(info,'$.scan',json(?)),last_error=? WHERE id=?", (dump(scan), error, source_id))


def recover_interrupted():
    with database() as db:
        db.execute("UPDATE sources SET info=json_set(info,'$.scan.status','paused'),last_error='앱이 재시작되었습니다. 갱신 버튼으로 이어서 수집하세요.' WHERE json_extract(info,'$.scan.status') IN ('queued','running')")


def sync_one(source_id):
    from collection_audit import collectors as audited_collectors, guarded, ROOT, CKAN_CATALOGS
    audited = audited_collectors()
    if source_id in audited:
        try:
            guarded(source_id, audited[source_id])
            result_path = ROOT / (("seoul-live" if source_id == "seoul" else source_id + "-independent" if source_id in CKAN_CATALOGS else source_id) + ".json")
            result = json.loads(result_path.read_text())
            complete = result["status"] in ("count_reconciled", "export_reconciled", "id_reconciled", "tree_exhausted")
            with database() as db:
                db.execute("UPDATE sources SET info=json_set(info,'$.scan.status',?,'$.scan.updated_at',?),last_error=? WHERE id=?",
                           ("complete" if complete else "paused", now(), "" if complete else "목록 대조 미완료 — collection_audit 확인", source_id))
        except Exception as error:
            with database() as db:
                db.execute("UPDATE sources SET info=json_set(info,'$.scan.status','paused'),last_error=? WHERE id=?", (str(error)[:350], source_id))
        return
    with database() as db:
        scan = json.loads(db.execute("SELECT info FROM sources WHERE id=?", (source_id,)).fetchone()[0])["scan"]
    scan["status"] = "running"
    set_scan(source_id, scan)
    try:
        while True:
            records, total, seen, next_cursor, done = metadata_page(source_id, scan["cursor"])
            if seen == 0 and not done:
                raise ValueError("전체 건수 도달 전에 빈 페이지 수신 — 완료로 처리하지 않았습니다.")
            signature = hashlib.sha256(dump([r["external_id"] for r in records]).encode()).hexdigest()
            if records and signature == scan.get("last_page_signature"):
                raise ValueError("페이지가 반복되어 수집을 중단했습니다. 제공처의 페이지 처리를 확인하세요.")
            scan.update(cursor=next_cursor, remote_total=total, seen=scan["seen"] + seen,
                        accepted=scan["accepted"] + len(records), pages=scan["pages"] + 1,
                        last_page_signature=signature, updated_at=now(), status="complete" if done else "running")
            # The next cursor is committed together with this page, so a restart cannot skip it.
            save_records(source_id, records, scan)
            if done:
                with database() as db:
                    db.execute("UPDATE sources SET last_sync=? WHERE id=?", (now(), source_id))
                return
            time.sleep(.3)
    except Exception as error:
        # Reload the committed cursor: failed validation/writes must not advance it.
        with database() as db:
            saved = json.loads(db.execute("SELECT info FROM sources WHERE id=?", (source_id,)).fetchone()[0])["scan"]
        saved.update(status="paused", updated_at=now())
        set_scan(source_id, saved, f"{type(error).__name__}: {error}"[:350])


def sync(source_id=None, source_ids=None):
    if source_ids is not None and (source_id is not None or not isinstance(source_ids, list) or not 1 <= len(source_ids) <= len(COLLECTORS) or any(not isinstance(id, str) or id not in COLLECTORS for id in source_ids)):
        raise ValueError("등록된 사이트 ID 목록을 지정하세요. 단일 ID와 함께 지정할 수 없습니다.")
    if source_id is not None and source_id not in COLLECTORS:
        raise ValueError("아직 수집기가 연결되지 않은 사이트입니다.")
    if not SYNC_LOCK.acquire(blocking=False):
        raise BlockingIOError("수집이 이미 실행 중입니다. 진행 상황은 공식 사이트 화면에서 확인하세요.")
    try:
        ids = list(dict.fromkeys(source_ids)) if source_ids is not None else [source_id] if source_id else COLLECTORS
        for id in ids:
            with database() as db:
                old = json.loads(db.execute("SELECT info FROM sources WHERE id=?", (id,)).fetchone()[0]).get("scan", {})
            scan = old if old.get("status") == "paused" else {"cursor": 0 if id in CKAN else 1, "seen": 0, "accepted": 0, "pages": 0, "started_at": now()}
            scan["status"] = "queued"
            set_scan(id, scan)
        def run():
            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    list(pool.map(sync_one, ids))
            finally:
                SYNC_LOCK.release()
        threading.Thread(target=run, daemon=True, name="metadata-sync").start()
        return {"started": ids}
    except Exception:
        SYNC_LOCK.release()
        raise
