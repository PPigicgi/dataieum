import hashlib
import fcntl
import html
import json
import os
import re
import sqlite3
import threading
from collections import Counter, defaultdict
from itertools import combinations
from functools import lru_cache
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from registry import CONCEPTS, CURATED, SOURCES, SOURCE_TOPICS, SUBJECT_RULES, SOURCE_ARCHIVE_TOPICS


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@contextmanager
def database():
    path = Path(os.environ.get("DB_PATH", "catalog.sqlite3"))
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=60)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    try:
        with db:
            yield db
    finally:
        db.close()


def dump(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def public_url(value):
    try:
        parts = urlsplit(value)
        return value if parts.scheme in ("https", "http") and parts.hostname and not parts.username else ""
    except (TypeError, ValueError):
        return ""


YEAR_RE = re.compile(r"(?<!\d)(?:17|18|19|20)\d{2}(?!\d)")
UNKNOWN_PERIODS = {"", "미상", "제공처 확인", "unknown", "none", "n/a"}


def year_fields(metadata, title="", description=""):
    """Derive a transparent year range while preserving source metadata."""
    period = str(metadata.get("period") or "").strip()
    candidates, basis = [], None
    period_based = period.casefold() not in UNKNOWN_PERIODS
    if period_based:
        candidates = [int(value) for value in YEAR_RE.findall(period)]
        if candidates:
            basis = "제공처 기준기간"
    if not candidates:
        candidates = [int(value) for value in YEAR_RE.findall(str(title or ""))]
        if candidates:
            basis = "자료명"
    if not candidates:
        candidates = [int(value) for value in YEAR_RE.findall(str(description or ""))]
        if candidates:
            basis = "자료 설명"
    maximum = datetime.now(timezone.utc).year + 5
    candidates = sorted({year for year in candidates if 1700 <= year <= maximum})
    modified = [int(value) for value in YEAR_RE.findall(str(metadata.get("source_modified") or ""))]
    updated = next((year for year in modified if 1700 <= year <= maximum), None)
    if not candidates:
        return {"reference_year": None, "reference_years": [],
                "year_basis": None, "updated_year": updated}
    start, end = candidates[0], candidates[-1]
    years = list(range(start, end + 1)) if period_based and len(candidates) > 1 else candidates
    label = str(start) if len(candidates) == 1 else f"{start}–{end}" if period_based else ", ".join(map(str, candidates))
    return {"reference_year": label, "reference_years": years, "year_basis": basis,
            "updated_year": updated}


@lru_cache(maxsize=8192)
def lexical_text(value):
    value = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", value)
    value = "".join(c for c in unicodedata.normalize("NFKD", value) if not unicodedata.combining(c))
    value = unicodedata.normalize("NFC", value)
    return re.sub(r"\s+", " ", re.sub(r"[-_‐‑–—]", " ", value)).strip()


def topic_pattern(terms):
    latin, exact, cjk = [], [], []
    for term in sorted(terms, key=len, reverse=True):
        term = lexical_text(term)
        (cjk if re.search(r"[\u3040-\u9fff\uac00-\ud7a3]", term) else exact if len(term) <= 3 or term.isupper() else latin).append(re.escape(term))
    alternatives = []
    if latin:
        alternatives.append(r"(?<!\w)(?:" + "|".join(latin) + r")(?:s|es)?(?!\w)")
    if exact:
        alternatives.append(r"(?<!\w)(?:" + "|".join(exact) + r")(?!\w)")
    if cjk:
        alternatives.append("(?:" + "|".join(cjk) + ")")
    return re.compile("|".join(alternatives), re.I)


TOPIC_PATTERNS = [(c["id"], topic_pattern(c["terms"]), topic_pattern(c["description_terms"]) if c.get("description_terms") else None, [re.compile(p) for p in c.get("title_patterns", [])]) for c in CONCEPTS if c.get("terms")]


@lru_cache(maxsize=1)
def _topic_prefilter():
    # One shared prefix tree rejects unrelated topic regexes before expensive scans.
    # The original topic patterns still decide every match and its evidence.
    variants = set()
    for concept in CONCEPTS:
        for term in concept.get("terms", []) + concept.get("description_terms", []):
            normalized = lexical_text(term).casefold()
            if normalized:
                variants.add(normalized)
                if not re.search(r"[\u3040-\u9fff\uac00-\ud7a3]", term) and len(term) > 3 and not term.isupper():
                    variants.update((normalized + "s", normalized + "es"))
    latin, cjk = {}, {}
    lookup = {}
    for term in variants:
        node = cjk if re.search(r"[\u3040-\u9fff\uac00-\ud7a3]", term) else latin
        for char in term:
            node = node.setdefault(char, {})
        node[""] = {}
        lookup[term] = frozenset(cid for cid, pattern, specific, _ in TOPIC_PATTERNS
                                 if pattern.search(term) or (specific and specific.search(term)))
    def expression(node):
        parts = [re.escape(char) + expression(child) for char, child in sorted(node.items()) if char]
        branch = "(?:" + "|".join(parts) + ")" if len(parts) > 1 else "".join(parts)
        return "(?:" + branch + ")?" if "" in node and branch else branch
    scan = re.compile(r"(?=((?<!\w)" + expression(latin) + r"(?!\w)|" + expression(cjk) + "))", re.I)
    return scan, lookup




@lru_cache(maxsize=16384)
def topic_candidates(value):
    scan, lookup = _topic_prefilter()
    found = set()
    for match in scan.finditer(value):
        found.update(lookup.get(match.group(1).casefold(), ()))
    return frozenset(found)


def suggestions(title, description, *, subjects=(), source_id="", publisher="", native_catalog=None, access_paths=(), resource_format=""):
    origin = {"data-gov-uk":"uk","data-gov-ie":"ireland","govdata":"germany",
              "data-gv-at":"austria","open-data-portal-austria":"austria","opendata-swiss":"switzerland",
              "plateforme-ouverte-des-donnees-publiques-francaises":"france","dados-gov-pt":"portugal"}.get(native_catalog) if source_id=="eu" else None
    rule_sources = {source_id, origin}
    # Provider-supplied tags and themes are the primary topic evidence. This
    # also prevents a generic word in the title from hiding an official topic.
    mappings = exact_subject_mappings(subjects, source_id=source_id, publisher=publisher, native_catalog=native_catalog)
    if mappings:
        return mappings
    for rule in SUBJECT_RULES:
        if rule.get("source") and rule["source"] not in rule_sources:
            continue
        if rule.get("publisher") and not re.search(rule["publisher"], publisher, re.I):
            continue
        if re.search(rule["pattern"], re.sub(r"[-_]", " ", title), re.I):
            return [{"concept_id": c, "status": "proposed", "method": "semantic_rule",
                     "evidence": rule["evidence"] + " — 제목: " + title} for c in rule["concepts"]]
    # A concrete title wins over incidental subjects in a long description.
    for label, value in (("제목", title), ("설명", description)):
        value = value[:4000] if label == "설명" else value
        value = html.unescape(re.sub(r"<[^>]*>", " ", value))
        value = re.sub(r"https?://\S+", " ", value)
        value = lexical_text(value)
        mappings = []
        candidates = topic_candidates(value)
        for concept_id, pattern, specific, contextual in TOPIC_PATTERNS:
            if concept_id not in candidates and not (label == "제목" and contextual):
                continue
            if label == "제목":
                match = pattern.search(value)
            else:
                # Prefer a specific subject in the opening description; otherwise
                # require two distinct signals, stopping once enough evidence exists.
                match = specific.search(value[:1000]) if specific else None
                if not match:
                    distinct, first = set(), None
                    for candidate in pattern.finditer(value):
                        first = first or candidate
                        distinct.add(candidate.group().casefold().rstrip("s"))
                        if len(distinct) >= 2:
                            match = first
                            break
            if not match and label == "제목":
                match = next((m for p in contextual if (m := p.search(value))), None)
            if match:
                excerpt = value[max(0, match.start() - 45):match.end() + 150]
                mappings.append({"concept_id": concept_id, "status": "proposed", "method": "keyword",
                                 "evidence": f'{label} 표현 정규화 후 “{match.group()}” 확인: {excerpt}'})
        if mappings:
            return mappings
    if (source_id == "france" or origin == "france") and re.search(r"(?i)\bIRVE\b", description):
        return [{"concept_id": cid, "status": "proposed", "method": "source_schema",
                 "evidence": "프랑스 원문 설명에 명시된 IRVE 전기차 충전 인프라 스키마."} for cid in ("transport", "energy")]
    if source_id == "us" and publisher.casefold() in SOURCE_ARCHIVE_TOPICS and not re.search(r"(?i)^(?:test|untitled|unknown|toto)(?:\b|_)", title):
        topic, reference = SOURCE_ARCHIVE_TOPICS[publisher.casefold()]
        return [{"concept_id": topic, "status": "proposed", "method": "archive_scope",
                 "evidence": "해당 전문 자료 저장소가 명시한 수록 분야: " + publisher + " — " + reference}]
    if source_id == "who" and publisher == "World Health Organization" and not re.search(r"(?i)^(?:test|untitled|unknown)(?:\b|_)", title):
        return [{"concept_id": "health", "status": "proposed", "method": "indicator_catalog_scope",
                 "evidence": "WHO Global Health Observatory 공식 보건 지표 정의 목록에서 수집된 항목. 세부 질환은 추정하지 않음 — https://ghoapi.azureedge.net/api/Indicator"}]
    if source_id == "eu" and native_catalog == "geocatalogue-fr" and re.search(r"(?i)^(?:Simple download service|Service de téléchargement simple)\s*\(Atom\)", title):
        return [{"concept_id": "spatial_services", "status": "proposed", "method": "service_type",
                 "evidence": "프랑스 GeoCatalogue 원문에 명시된 Atom 공간자료 다운로드 서비스. 개별 자료의 세부 주제는 추정하지 않음."}]
    research = re.search(
        r"(?i)\b(?:data(?:set)?|code)\b.{0,100}\b(?:accompanying (?:the )?(?:paper|manuscript)|"
        r"(?:for|associated with|underlying) (?:the |our |a )?(?:journal article|research paper|paper|manuscript)|"
        r"associated with the following publication)\b|"
        r"\b(?:scientific publication|journal article|research manuscript)\b",
        title + " " + description[:4000])
    research = research or re.search(r"(?i)\b(?:data|dataset|material)\b.{0,120}\b(?:used|presented|described) in (?:the |this |an )?(?:article|paper|study)\b", title + " " + description[:4000])
    research = research or re.search(r"(?i)\b(?:research data|research dataset|research outputs|scientific data|replication data|reproducibility artifact|Horizon 2020|Horizon Europe|Marie Sk[łl]odowska.Curie)\b", title + " " + description[:4000])
    if research:
        return [{"concept_id":"research_data","status":"proposed","method":"research_artifact",
                 "evidence":"원문에 명시된 학술 연구·연구과제 관련성: "+research.group()[:240]+". 세부 연구 분야는 추정하지 않음."}]
    match = re.search(r"(?i)\b(?:WMS|WFS|WMTS|Web Map Service|Web Feature Service|GIS (?:download|view) service|INSPIRE (?:download|view) service)\b", title)
    if match:
        return [{"concept_id": "spatial_services", "status": "proposed", "method": "service_type",
                 "evidence": "제목에 명시된 공간정보 조회·배포 서비스 유형: " + match.group() + ". 개별 자료의 세부 주제는 추정하지 않음."}]
    if not re.search(r"(?i)^(?:test|untitled|unknown|toto|template title|테스트)(?:\b|_)", title):
        format_tokens = set(re.split(r"[^a-z0-9]+", str(resource_format).casefold().replace("geo+json", "geojson")))
        if "sdmx" in format_tokens:
            return [{"concept_id": "statistics", "status": "proposed", "method": "statistical_catalog_scope",
                     "evidence": "공식 메타데이터에 명시된 SDMX 통계 교환 자료. 세부 통계 주제는 추정하지 않음."}]
        service_formats = format_tokens & {"wms", "wfs", "wcs", "wmts"}
        spatial_formats = format_tokens & {"shp", "shapefile", "geojson", "gml", "kml", "geopackage", "gpkg", "geotiff"}
        if service_formats or spatial_formats:
            return [{"concept_id": "spatial_services", "status": "proposed",
                     "method": "service_type" if service_formats else "format_scope",
                     "evidence": "공식 메타데이터에 명시된 공간자료·서비스 형식: " + str(resource_format)[:180] + ". 확인된 자료 유형에만 연결하며 세부 주제는 추정하지 않음."}]
        if source_id in {"oecd", "eurostat", "kosis", "census"} or (source_id == "eu" and native_catalog == "estat"):
            return [{"concept_id": "statistics", "status": "proposed", "method": "statistical_catalog_scope",
                     "evidence": "공식 통계표·지표·데이터플로 목록에 등록된 통계 자료. 공개된 정보만으로 더 세부적인 통계 주제는 확정하지 않음: " + (native_catalog or source_id)}]
        for address in access_paths:
            if not public_url(address):
                continue
            parsed = urlsplit(address)
            params = {k.casefold(): v for k, v in parse_qs(parsed.query).items()}
            if parsed.hostname in {"kosis.kr", "stat.kosis.kr", "kosis.daegu.go.kr"} and parsed.path in {"/statHtml/statHtml.do", "/nsibsHtmlSvc/fileView/FileStbl/fileStblView.do"}:
                return [{"concept_id": "statistics", "status": "proposed", "method": "statistical_catalog_scope",
                         "evidence": "원문 접근 경로에서 KOSIS 통계표·통계 간행물임을 확인. 세부 분야는 추정하지 않음 — " + address}]
            service = (params.get("service") or [""])[0].upper()
            if service in {"WMS", "WFS", "WCS", "WMTS"} or re.search(r"/(?:FeatureServer|MapServer)(?:/\d+)?(?:/query)?/?$", parsed.path, re.I):
                return [{"concept_id": "spatial_services", "status": "proposed", "method": "service_type",
                         "evidence": "공식 메타데이터의 제공 경로에서 확인한 공간정보 조회 서비스 유형. 개별 데이터의 세부 주제는 추정하지 않음 — " + address}]
    return []


def subject_mappings(subjects, *, source_id="", publisher="", native_catalog=None):
    # Specific tags precede broad portal themes. A URL is an identifier, not prose.
    # KOSIS supplies a subject hierarchy, not merely the final table label.
    if source_id in {"kosis", "ecos", "kma"}:
        for subject in subjects:
            path = subject.get("label", "")
            if source_id == "kosis" and not path.startswith("주제별통계 >"):
                continue
            if source_id != "kosis" and ">" not in path:
                continue
            for branch in reversed(path.split(">")):
                branch = re.sub(r"^[\d.]+\s*", "", branch.strip())
                ids = SOURCE_TOPICS.get(branch.casefold())
                if ids is None:
                    ids = [m["concept_id"] for m in suggestions(branch, "")]
                if ids:
                    return [{"concept_id": cid, "status": "proposed", "method": "source_hierarchy",
                             "evidence": source_id.upper() + " 공식 분류 경로에서 가장 가까운 확인 가능한 분야 “" + branch + "”: " + path}
                            for cid in ids]
    for kind in ("tag", "theme", "resource_title", "resource_description"):
        matches = {}
        for subject in subjects:
            subject_kind = {"keyword": "tag", "category": "theme"}.get(subject.get("kind"), subject.get("kind"))
            if subject_kind != kind:
                continue
            label = subject.get("label", "")
            normalized = label.casefold().strip()
            if normalized.startswith(("http://", "https://")):
                parsed = urlsplit(normalized)
                if parsed.hostname == "sns.uba.de" and parsed.path == "/umthes/de/concepts/_00019329.html":
                    matches.setdefault("basemap", {"concept_id": "basemap", "status": "proposed", "method": "source_subject",
                                                   "evidence": "독일 Umweltbundesamt UMTHES 공식 개념 Photogrammetrie(사진측량): " + label})
                    continue
                if parsed.hostname == "www.eionet.europa.eu" and re.fullmatch(r"/gemet/concept/\d+/?", parsed.path):
                    # Official GEMET preferred labels retrieved through getConcept (English).
                    concepts = {"1200": ("basemap", "cartography"), "14891": ("digital", "internet search service"),
                                "14922": ("basemap", "raster"), "14926": ("basemap", "gridding"),
                                "1517": ("water", "coastal environment"), "208": ("agriculture", "agricultural management"),
                                "2599": ("energy", "electric line"), "2712": ("energy", "energy"),
                                "3007": ("environment", "eutrophication"), "3645": ("basemap", "geographic information system"),
                                "3917": ("nature", "hedge"), "3960": ("energy", "high voltage line"),
                                "4599": ("geography", "land"), "5605": ("science", "nitrogen"),
                                "5944": ("basemap", "orography"), "6204": ("basemap", "photogrammetry"),
                                "7272": ("transport", "road"), "7731": ("geology", "excavation side"),
                                "8068": ("environment", "data on the state of the environment"), "8076": ("statistics", "statistics"),
                                "8641": ("transport", "transportation"), "8815": ("nature", "urban green"),
                                "8820": ("geography", "urbanisation"), "8922": ("nature", "vegetation"), "9023": ("housing", "wall")}
                    entry = concepts.get(parsed.path.rstrip("/").rsplit("/", 1)[-1])
                    if entry:
                        cid, term = entry
                        matches.setdefault(cid, {"concept_id": cid, "status": "proposed", "method": "source_subject",
                                                 "evidence": "공식 GEMET 주제 식별자와 원어 정의 확인: " + term + " — " + label})
                    continue
                if parsed.hostname == "inspire.ec.europa.eu" and parsed.path.startswith("/metadata-codelist/spatialdataservicecategory/"):
                    code = parsed.path.rstrip("/").rsplit("/",1)[-1]
                    for cid in SOURCE_TOPICS.get(code, []):
                        matches.setdefault(cid, {"concept_id":cid,"status":"proposed","method":"service_type",
                                                "evidence":"공식 INSPIRE 공간정보 서비스 유형 식별자: "+label})
                    continue
                if parsed.hostname == "inspire.ec.europa.eu" and parsed.path.startswith("/theme/"):
                    codes = {"ac":["weather"],"ad":["geography"],"af":["agriculture"],"am":["geography"],"au":["geography"],
                             "br":["nature"],"bu":["housing"],"cp":["geography"],"ef":["environment"],"el":["basemap"],
                             "er":["energy"],"ge":["geology"],"gg":["basemap"],"gn":["geography"],"hb":["nature"],
                             "hh":["health","safety"],"hy":["water"],"lc":["geography"],"lu":["geography"],"mf":["weather"],
                             "mr":["geology"],"nz":["safety"],"of":["water"],"oi":["basemap"],"pd":["population"],
                             "pf":["commerce"],"ps":["nature"],"rs":["basemap"],"sd":["nature"],"so":["geology"],
                             "sr":["water"],"su":["statistics"],"tn":["transport"],"us":["facilities"]}
                    for cid in codes.get(parsed.path.rstrip("/").rsplit("/",1)[-1],[]):
                        matches.setdefault(cid, {"concept_id":cid,"status":"proposed","method":"source_subject",
                                                "evidence":"공식 INSPIRE 자료 주제 식별자: "+label})
                    continue
                if not (parsed.hostname == "standaarden.overheid.nl"
                        or (parsed.hostname == "publications.europa.eu" and parsed.path.startswith("/resource/authority/data-theme/"))
                        or (parsed.hostname == "datos.gob.es" and parsed.path.startswith("/kos/sector-publico/sector/"))):
                    continue
                normalized = unquote(parsed.path.rstrip("/").rsplit("/", 1)[-1])
            normalized = re.sub(r"[_-]+", " ", normalized)
            normalized = re.sub(r"\s+", " ", normalized).strip()
            # CSO records are administratively filed under Government even when
            # the actual statistical subject is unrelated to public administration.
            if kind == "theme" and normalized == "government" and (source_id == "ireland" or (source_id == "eu" and native_catalog == "data-gov-ie")) and publisher == "Central Statistics Office":
                continue
            ids = SOURCE_TOPICS.get(normalized)
            if ids is None:
                ids = SOURCE_TOPICS.get(re.sub(r" \(thema\)$", "", normalized))
            if ids is None and kind == "theme" and "," in normalized:
                parts = [p.strip() for p in normalized.split(",")]
                if all(part in SOURCE_TOPICS for part in parts):
                    ids = list(dict.fromkeys(c for part in parts for c in SOURCE_TOPICS[part]))
            if ids is None and kind in ("tag", "theme", "resource_title", "resource_description"):
                if ">" in normalized:
                    if normalized.startswith(("geographic region", "continent", "ocean >", "platform", "instrument")):
                        continue
                    branches = [p.strip() for p in normalized.split(">")]
                    if branches[0] == "earth science" and len(branches) > 1:
                        ids = {"biological classification": ["nature"], "biosphere": ["nature"],
                               "oceans": ["water"], "terrestrial hydrosphere": ["water"],
                               "solid earth": ["geology"], "cryosphere": ["weather"]}.get(branches[1])
                    normalized = branches[-1]
                if ids is None:
                    ids = [m["concept_id"] for m in suggestions(normalized, "", source_id=source_id, publisher=publisher, native_catalog=native_catalog)] if kind in ("tag", "theme", "resource_title") else [m["concept_id"] for m in suggestions("", label, source_id=source_id, publisher=publisher, native_catalog=native_catalog)]
            for concept_id in ids or []:
                matches.setdefault(concept_id, {"concept_id": concept_id, "status": "proposed", "method": "source_subject",
                                               "evidence": f'공식 사이트 {dict(tag="태그", theme="주제", resource_title="첨부 자료명", resource_description="첨부 자료 설명")[kind]}: {label[:1000]}'})
        if matches:
            return list(matches.values())
    return []


def exact_subject_mappings(subjects, *, source_id="", publisher="", native_catalog=None):
    """Map only exact provider taxonomy values; never infer from a word fragment."""
    accepted = []
    known_url_prefixes = (
        ("sns.uba.de", "/umthes/de/concepts/"),
        ("www.eionet.europa.eu", "/gemet/concept/"),
        ("inspire.ec.europa.eu", "/metadata-codelist/spatialdataservicecategory/"),
        ("inspire.ec.europa.eu", "/theme/"),
        ("standaarden.overheid.nl", "/"),
        ("publications.europa.eu", "/resource/authority/data-theme/"),
        ("datos.gob.es", "/kos/sector-publico/sector/"),
    )
    for subject in subjects:
        if not isinstance(subject, dict):
            continue
        label = str(subject.get("label") or "").strip()
        if not label:
            continue
        normalized = label.casefold()
        if normalized.startswith(("http://", "https://")):
            parsed = urlsplit(normalized)
            if any(parsed.hostname == host and parsed.path.startswith(prefix)
                   for host, prefix in known_url_prefixes):
                accepted.append(subject)
            continue
        normalized = re.sub(r"[_-]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        exact = normalized in SOURCE_TOPICS or re.sub(r" \(thema\)$", "", normalized) in SOURCE_TOPICS
        if not exact and subject.get("kind") in {"theme", "category"} and "," in normalized:
            exact = all(part.strip() in SOURCE_TOPICS for part in normalized.split(","))
        if not exact and source_id in {"kosis", "ecos", "kma"} and ">" in normalized:
            branches = [re.sub(r"^[\d.]+\s*", "", part.strip()) for part in normalized.split(">")]
            exact = any(branch in SOURCE_TOPICS for branch in branches)
        if not exact and normalized.startswith("earth science >"):
            branch = normalized.split(">", 2)[1].strip()
            exact = branch in {"biological classification", "biosphere", "oceans", "terrestrial hydrosphere", "solid earth", "cryosphere"}
        if exact:
            accepted.append(subject)
    return subject_mappings(accepted, source_id=source_id, publisher=publisher, native_catalog=native_catalog) if accepted else []


def refreshed_mappings(title, description, previous, **context):
    # Keep explicit reviews, including assistant reviews with recorded evidence.
    reviewed = {m["concept_id"]: m for m in previous if m["concept_id"] != "unclassified" and (m["status"] in ("approved", "rejected") or m.get("reviewed_at"))}
    note = context.pop("classification_note", None)
    fingerprint = context.pop("fingerprint", None)
    semantic = {m["concept_id"]: m for m in previous
                if m.get("method") in {"metadata_semantic_model", "verified_native_context", "verified_same_record"} and m.get("status") == "classified"
                and fingerprint and m.get("evidence_fingerprint") == fingerprint}
    fallback = [m for m in previous if m["concept_id"] == "unclassified"] or [{"concept_id": "unclassified", "status": "proposed", "method": "unclassified",
                 "evidence": (note or {}).get("reason") or "현재 제공된 제목·설명·주제 정보로 분류를 확인하지 못했습니다."}]
    if any(m.get("method") == "semantic_review" for m in reviewed.values()) or (note or {}).get("blocked"):
        return list(reviewed.values()) or fallback
    proposed = {m["concept_id"]: m for m in suggestions(title, description, **context)}
    return list((proposed | semantic | reviewed).values()) or fallback


def reclassify(only_unclassified=False):
    cursor, scanned, changed, conflicts = "", 0, 0, 0
    scope = " AND (mappings='[]' OR (json_array_length(mappings)=1 AND json_extract(mappings,'$[0].concept_id')='unclassified'))" if only_unclassified else ""
    while True:
        with database() as db:
            rows = list(db.execute("SELECT id,source_id,title,description,metadata,mappings,fingerprint FROM datasets WHERE id>?" + scope + " ORDER BY id LIMIT 500", (cursor,)))
        if not rows:
            return {"scanned": scanned, "changed": changed, "concurrent_changes_skipped": conflicts}
        updates = []
        for r in rows:
            metadata = json.loads(r["metadata"])
            updated = dump(refreshed_mappings(r["title"], r["description"], json.loads(r["mappings"]),
                                             subjects=metadata.get("subjects", []), source_id=r["source_id"], publisher=metadata.get("publisher", ""), classification_note=metadata.get("classification_note"), fingerprint=r["fingerprint"], native_catalog=metadata.get("native_catalog"), access_paths=metadata.get("access_paths", []), resource_format=metadata.get("format", "")))
            if updated != r["mappings"]:
                updates.append((updated, r["id"], r["fingerprint"], r["mappings"]))
        with database() as db:
            for args in updates:
                n = db.execute("UPDATE datasets SET mappings=? WHERE id=? AND fingerprint=? AND mappings=?", args).rowcount
                changed += n
                conflicts += 1 - n
        scanned += len(rows)
        cursor = rows[-1]["id"]


def normalize(record):
    r = dict(record)
    if not r.get("external_id") or not r.get("title") or not public_url(r.get("url", "")):
        raise ValueError("원본 ID, 제목, 유효한 원본 페이지가 필요합니다.")
    r["id"] = f'{r["source_id"]}:{r["external_id"]}'
    r.setdefault("description", "")
    r.setdefault("publisher", "제공처 확인")
    r.setdefault("region", "미상")
    r.setdefault("period", "미상")
    r.setdefault("license", "제공처 확인")
    r.setdefault("format", "제공처 확인")
    r.setdefault("metadata_url", r["url"])
    r.setdefault("method", "api")
    # Fingerprint the meaning and destination, not the time of the network request.
    # Supplemental classification evidence can be enriched without invalidating a review.
    core = {k: v for k, v in r.items() if k not in ("subjects", "classification_note", "fingerprint", "checked_at", "duplicate_of", "duplicate_evidence", "aliases", "duplicate_candidates", "duplicate_canonical")}
    r["fingerprint"] = hashlib.sha256(dump(core).encode()).hexdigest()
    r["checked_at"] = now()
    return r


def save_records(source_id, records, scan=None):
    normalized = [normalize({**r, "source_id": source_id}) for r in records]
    with database() as db:
        db.execute("BEGIN IMMEDIATE")
        for r in normalized:
            existing = db.execute("SELECT fingerprint,mappings,metadata FROM datasets WHERE id=?", (r["id"],)).fetchone()
            if existing:
                prior = json.loads(existing["metadata"])
                for field in ("access_paths", "editions", "native_id", "native_name"):
                    if field in prior and field not in r:
                        r[field] = prior[field]
                r = normalize(r)
                for key in ("duplicate_of", "duplicate_evidence"):
                    r.pop(key, None)
                if all(r.get(k) == prior.get(k) for k in ("title", "description", "publisher", "region", "period", "license", "url", "access_paths")):
                    for key in ("duplicate_of", "duplicate_evidence"):
                        if key in prior:r[key] = prior[key]
                elif prior.get("duplicate_canonical"):
                    db.execute("UPDATE datasets SET metadata=json_remove(metadata,'$.duplicate_of','$.duplicate_evidence') WHERE json_extract(metadata,'$.duplicate_of')=?", (r["id"],))
            if existing and existing["fingerprint"] == r["fingerprint"]:
                previous_metadata = json.loads(existing["metadata"])
                # Retain supplemental resource descriptions when a projected response omits them.
                incoming_kinds = {s["kind"] for s in r.get("subjects", [])}
                combined = {(s["kind"], s["label"]): s for s in previous_metadata.get("subjects", [])
                            if s["kind"] in ("resource_title", "resource_description") and s["kind"] not in incoming_kinds}
                combined.update({(s["kind"], s["label"]): s for s in r.get("subjects", [])})
                r["subjects"] = list(combined.values())
                if previous_metadata.get("classification_note"):
                    r["classification_note"] = previous_metadata["classification_note"]
                mappings = refreshed_mappings(r["title"], r["description"], json.loads(existing["mappings"]), subjects=r.get("subjects", []), source_id=source_id, publisher=r["publisher"], classification_note=r.get("classification_note"), fingerprint=r["fingerprint"], native_catalog=r.get("native_catalog"), access_paths=r.get("access_paths", []), resource_format=r.get("format", ""))
            else:
                mappings = refreshed_mappings(r["title"], r["description"], [], subjects=r.get("subjects", []), source_id=source_id, publisher=r["publisher"], native_catalog=r.get("native_catalog"), access_paths=r.get("access_paths", []), resource_format=r.get("format", ""))
            db.execute("""INSERT INTO datasets(id,source_id,title,description,metadata,mappings,fingerprint,checked_at)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,description=excluded.description,metadata=excluded.metadata,
                mappings=excluded.mappings,fingerprint=excluded.fingerprint,checked_at=excluded.checked_at""",
                (r["id"], source_id, r["title"], r["description"], dump(r), dump(mappings), r["fingerprint"], r["checked_at"]))
        if scan is not None:
            db.execute("UPDATE sources SET info=json_set(info,'$.scan',json(?)),last_error='' WHERE id=?", (dump(scan), source_id))
    return len(normalized)


def initialize():
    with database() as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript("""
        CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY, info TEXT NOT NULL,
            last_sync TEXT, last_error TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS datasets (
            id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(id),
            title TEXT NOT NULL, description TEXT NOT NULL,
            metadata TEXT NOT NULL, mappings TEXT NOT NULL,
            fingerprint TEXT NOT NULL, checked_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS datasets_source ON datasets(source_id);
        """)
        for s in SOURCES:
            db.execute("INSERT INTO sources(id,info) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET info=json_patch(sources.info,excluded.info)", (s["id"], dump(s)))
        for seed in CURATED:
            r = normalize({k: v for k, v in seed.items() if k != "concepts"})
            r["method"] = "curated"
            r["checked_at"] = "2026-09-14T00:00:00+09:00"
            mappings = [{"concept_id": c, "status": "approved", "method": "curated",
                         "evidence": "공식 페이지의 제목과 설명 직접 확인 (2026-09-14): " + seed["description"]} for c in seed["concepts"]]
            db.execute("INSERT OR IGNORE INTO datasets VALUES(?,?,?,?,?,?,?,?)", (r["id"], r["source_id"], r["title"], r["description"], dump(r), dump(mappings), r["fingerprint"], r["checked_at"]))


# One compact, read-only index per process. SQLite's data_version invalidates it
# after any committed writer, including collectors and manual reviews.
_index_lock = threading.Lock()
_index_connection = None
_index_identity = None
_index_version = None
_index_value = None


def catalog_index(db):
    global _index_connection, _index_identity, _index_version, _index_value
    path = Path(db.execute("PRAGMA database_list").fetchone()[2])
    identity = (str(path), path.stat().st_ino)
    with _index_lock:
        if identity != _index_identity:
            if _index_connection is not None:
                _index_connection.close()
            _index_connection = sqlite3.connect(path, check_same_thread=False)
            _index_identity, _index_version = identity, None
        version = _index_connection.execute("PRAGMA data_version").fetchone()[0]
        if version == _index_version:
            return _index_value
        # The lock protects replacement; ongoing requests retain their own references.
        # Release the stale index before building another multi-million-record index.
        _index_version, _index_value = None, None
        records, topics, source_groups = [], defaultdict(list), defaultdict(dict)
        statuses, overlaps, stored_counts = Counter(), Counter(), Counter()
        # Stream rows instead of sorting long provenance JSON in SQLite temp storage.
        for r in db.execute("""SELECT id,source_id,title,description,mappings,
                json_extract(metadata,'$.duplicate_of') AS duplicate_of,
                json_extract(metadata,'$.period') AS period,
                json_extract(metadata,'$.source_modified') AS source_modified
                FROM datasets"""):
            stored_counts[r["source_id"]] += 1
            mappings = json.loads(r["mappings"])
            active = tuple(sorted({m["concept_id"] for m in mappings if m["status"] != "rejected"}))
            years = year_fields({"period": r["period"], "source_modified": r["source_modified"]}, r["title"], r["description"])
            item = {"id": r["id"], "source": r["source_id"], "title": r["title"],
                    "topics": active, "proposed": any(m["status"] == "proposed" for m in mappings)}
            if years["reference_years"]:
                item["years"] = tuple(years["reference_years"])
            canonical = r["duplicate_of"] or r["id"]
            group = source_groups[item["source"]]
            existing = group.get(canonical)
            if (existing is None or r["duplicate_of"] is None
                    or (existing["id"] != canonical
                        and (item["title"], item["id"]) < (existing["title"], existing["id"]))):
                group[canonical] = item
            if r["duplicate_of"] is not None:
                continue
            records.append(item)
            statuses.update(m["status"] for m in mappings)
        records.sort(key=lambda r: (r["source"], r["title"], r["id"]))
        years = defaultdict(list)
        for item in records:
            for topic in item["topics"] or ("__unlinked",):
                topics[topic].append(item)
            overlaps.update(combinations(item["topics"], 2))
            for year in item.get("years", ()):
                years[str(year)].append(item)
        year_counts = {year: len(items) for year, items in years.items()}
        _index_value = {"records": records, "topics": topics, "sources": {source: sorted(groups.values(), key=lambda r: (r["title"], r["id"])) for source, groups in sorted(source_groups.items())},
                        "statuses": statuses, "overlaps": overlaps, "years": years,
                        "year_counts": year_counts, "stored_counts": stored_counts,
                        "searches": {}}
        _index_version = version
        return _index_value


def matching_records(db, index, query):
    year = query.get("year", "").strip()
    if year and not re.fullmatch(r"(?:17|18|19|20)\d{2}", year):
        raise ValueError("자료 연도가 올바르지 않습니다.")
    if query.get("source"):
        records = index["sources"].get(query["source"], [])
        if query.get("concept"):
            topic = query["concept"]
            records = [r for r in records if topic in (r["topics"] or ("__unlinked",))]
    else:
        records = index["years"].get(year, []) if year else index["topics"].get(query["concept"], []) if query.get("concept") else index["records"]
        if year and query.get("concept"):
            topic = query["concept"]
            records = [r for r in records if topic in (r["topics"] or ("__unlinked",))]
    if query.get("status") == "proposed":
        records = [r for r in records if r["proposed"]]
    if year and query.get("source"):
        selected = int(year)
        records = [r for r in records if selected in r.get("years", ())]
    q = query.get("q", "").strip().lower()[:200]
    if q:
        topic_ids = {c["id"] for c in CONCEPTS if q in c["name"].lower() or any(q == t.lower() for t in c["terms"])}
        ids = index["searches"].get(q)
        if ids is None:
            ids = {r[0] for r in db.execute("""SELECT id FROM datasets
                WHERE instr(lower(title || ' ' || description || ' ' || json_extract(metadata,'$.publisher')
                || ' ' || json_extract(metadata,'$.region')),?)>0""", (q,))}
            with _index_lock:
                if len(index["searches"]) >= 8:
                    index["searches"].pop(next(iter(index["searches"])))
                index["searches"][q] = ids
        records = [r for r in records if r["id"] in ids or topic_ids.intersection(r["topics"])]
    return records


def read_records(db, records):
    if not records:
        return []
    ids = [r["id"] for r in records]
    rows = db.execute("SELECT id,metadata,mappings,checked_at FROM datasets WHERE id IN (" + ",".join("?" for _ in ids) + ")", ids)
    found = {}
    for r in rows:
        metadata = json.loads(r["metadata"])
        found[r["id"]] = {**metadata, **year_fields(metadata, metadata.get("title", ""), metadata.get("description", "")),
                           "mappings": json.loads(r["mappings"]), "checked_at": r["checked_at"]}
    return [found[id] for id in ids if id in found]


def snapshot(query=None, *, include_datasets=True):
    query = query or {}
    page = max(1, int(query.get("page", "1")))
    limit = 30
    with database() as db:
        index = catalog_index(db)
        sources = [{**json.loads(r["info"]), "last_sync": r["last_sync"], "last_error": r["last_error"]} for r in db.execute("SELECT * FROM sources")]
        records = matching_records(db, index, query)
        total = len(records)
        page = min(page, max(1, (total + limit - 1) // limit))
        datasets = read_records(db, records[(page-1)*limit:page*limit]) if include_datasets else []
    for s in sources:
        s["dataset_count"] = len(index["sources"].get(s["id"], []))
        s["stored_count"] = index["stored_counts"].get(s["id"], 0)
    return {"sources": sources, "datasets": datasets,
            "concepts": [{k: v for k, v in c.items() if k not in ("terms", "description_terms", "title_patterns")} for c in CONCEPTS],
            "page": page, "pages": max(1, (total+limit-1)//limit), "total": total,
            "summary": {"datasets": len(index["records"]),
                        "concept_counts": {c["id"]: len(index["topics"].get(c["id"], [])) for c in CONCEPTS},
                        "year_counts": index["year_counts"],
                        "pending": index["statuses"]["proposed"], "approved": index["statuses"]["approved"],
                        "running": any(s.get("scan", {}).get("status") in ("queued", "running") for s in sources)}}


def graph_overview(query=None):
    """Every matching record is represented: topic, source, title ranges, then leaves.

    Ranges partition records for display; they are not additional ontology claims.
    At most 48 records (or 32 ranges) are sent at any one level.
    """
    query = query or {}
    branch = query.get("branch", "")
    if branch and (len(branch) > 64 or any(not p.isdigit() or int(p) >= 32 for p in branch.split("."))):
        raise ValueError("자료 묶음 경로가 올바르지 않습니다.")
    with database() as db:
        index = catalog_index(db)
        records = matching_records(db, index, query)
        total = len(records)
        nodes, edges = [], []
        names = {s["id"]: s["name"] for s in SOURCES}
        if not query.get("concept"):
            if not query:
                counts = {id: len(rows) for id, rows in index["topics"].items()}
                overlaps = index["overlaps"]
            else:
                counts, overlaps = Counter(), Counter()
                for r in records:
                    counts.update(r["topics"] or ("__unlinked",))
                    overlaps.update(combinations(r["topics"], 2))
            names = {c["id"]: c["name"] for c in CONCEPTS} | {"__unlinked": "연결 없는 자료"}
            nodes = [{"id": "topic:"+id, "kind": "topic", "title": names.get(id, id), "count": count,
                      "query": {**query, "concept": id}} for id, count in counts.items()]
            edges = [{"a": "topic:"+a, "b": "topic:"+b, "count": n} for (a,b), n in overlaps.items()]
        elif not query.get("source"):
            counts = Counter(r["source"] for r in records)
            nodes = [{"id": "source:"+id, "kind": "source", "title": names.get(id, id), "count": count,
                      "query": {**query, "source": id}} for id, count in counts.items()]
        else:
            for part in branch.split(".") if branch else []:
                if len(records) <= 48:
                    raise ValueError("자료 묶음 경로가 올바르지 않습니다.")
                size = (len(records)+31)//32
                start = int(part)*size
                if start >= len(records):
                    raise ValueError("자료 묶음 경로가 올바르지 않습니다.")
                records = records[start:start+size]
            if len(records) > 48:
                size = (len(records)+31)//32
                for i, start in enumerate(range(0, len(records), size)):
                    group = records[start:start+size]
                    path = (branch+"." if branch else "")+str(i)
                    nodes.append({"id": "range:"+path, "kind": "range",
                                  "title": group[0]["title"][:22]+" … "+group[-1]["title"][:22],
                                  "count": len(group), "query": {**query, "branch": path}})
            else:
                nodes = [{"id": d["id"], "kind": "dataset", "title": d["title"], "count": 1, "record": d}
                         for d in read_records(db, records)]
        return {"nodes": nodes, "edges": edges, "total": total, "represented": len(records), "query": query}


def initial_overview():
    # A small, atomically replaced summary; serving it never scans the dataset table.
    path = Path(os.environ.get("DB_PATH", "catalog.sqlite3")).parent / "graph-overview.json"
    return json.loads(path.read_text())


def refresh_overview():
    result = snapshot(include_datasets=False)
    result["graph"] = graph_overview()
    result["graph_children"] = [graph_overview(node["query"]) for node in result["graph"]["nodes"]]
    if any(child["represented"] != node["count"] for node, child in zip(result["graph"]["nodes"], result["graph_children"])):
        raise RuntimeError("Catalog changed while preparing topic summaries; retrying background refresh")
    if result["summary"]["datasets"] != result["graph"]["represented"]:
        raise RuntimeError("Catalog changed while preparing the overview; retrying in background")
    result["generated_at"] = now()
    path = Path(os.environ.get("DB_PATH", "catalog.sqlite3")).parent / "graph-overview.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(dump(result))
    temporary.replace(path)
    return result


def refresh_overview_unless_updating():
    # Bulk writers hold an advisory lock; avoid rebuilding millions of graph rows
    # repeatedly while their transactions are still being committed.
    path = Path(os.environ.get("DB_PATH", "catalog.sqlite3")).parent / "catalog-update.lock"
    with path.open("a+") as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return None
        return refresh_overview()


def start_overview_refresh():
    # The persisted overview makes restarts immediate. Rebuild only after a commit
    # observed by this process; the deployment flow refreshes once before restart.
    def run():
        with database() as watcher:
            version = watcher.execute("PRAGMA data_version").fetchone()[0]
            while True:
                threading.Event().wait(10)
                current = watcher.execute("PRAGMA data_version").fetchone()[0]
                if current != version:
                    try:
                        if refresh_overview_unless_updating() is not None:
                            version = current
                    except Exception as error:
                        print("Graph overview refresh failed:", error, flush=True)
    threading.Thread(target=run, daemon=True, name="graph-overview-refresh").start()


def review(dataset_id, concept_id, status, fingerprint):
    if status not in ("approved", "rejected", "proposed") or concept_id not in {c["id"] for c in CONCEPTS}:
        raise ValueError("올바른 개념과 승인 상태를 선택하세요.")
    with database() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT mappings,fingerprint,metadata FROM datasets WHERE id=?", (dataset_id,)).fetchone()
        if not row:
            raise KeyError("데이터를 찾을 수 없습니다.")
        if row["fingerprint"] != fingerprint:
            raise ValueError("설명이 갱신되었습니다. 새로고침 후 다시 확인하세요.")
        mappings = json.loads(row["mappings"])
        mapping = next((m for m in mappings if m["concept_id"] == concept_id), None)
        if not mapping:
            metadata = json.loads(row["metadata"])
            mapping = {"concept_id": concept_id, "method": "manual",
                       "evidence": f'직접 연결 — 제목: {metadata["title"]}. 설명: {metadata["description"][:300]}'}
            mappings.append(mapping)
        mapping.update(status=status, reviewed_at=now())
        if concept_id != "unclassified" and status != "rejected":
            mappings = [m for m in mappings if m["concept_id"] != "unclassified"]
        db.execute("UPDATE datasets SET mappings=? WHERE id=?", (dump(mappings), dataset_id))
