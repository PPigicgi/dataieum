"""Presentation identity selected only from the origin proxy's allowlisted host."""
from dataclasses import dataclass
from html import escape


@dataclass(frozen=True)
class Brand:
    host: str
    key: str
    ko: str
    en: str


BRANDS = {
    "dataieum.com": Brand("dataieum.com", "dataieum", "데이터이음", "Dataieum"),
    "manzihub.com": Brand("manzihub.com", "datahub", "데이터 허브", "Data Hub"),
}


def request_brand(scope):
    # Caddy overwrites this header. The app still requires its existing local
    # Host boundary; this value never grants access or changes a data query.
    hosts = [value for key, value in scope.get("headers", [])
             if key.lower() == b"x-dataieum-host"]
    if not hosts:
        return BRANDS["dataieum.com"]
    if len(hosts) != 1:
        raise ValueError("duplicate presentation host")
    try:
        return BRANDS[hosts[0].decode("ascii").lower()]
    except (UnicodeError, KeyError) as exc:
        raise ValueError("unknown presentation host") from exc


def brand_document(body, brand):
    # Only application-owned HTML is branded, never catalogue records or API
    # output. Dynamic UI labels use the same identity through i18n.js.
    text = body.decode("utf-8")
    text = text.replace("데이터이음", brand.ko).replace("Dataieum", brand.en)
    identity = (f'<meta name="dataieum-brand" content="{brand.key}">'
                f'<meta name="application-name" content="{escape(brand.ko)}">'
                f'<meta property="og:site_name" content="{escape(brand.ko)}">'
                f'<meta name="description" content="{escape(brand.ko)} · 공공데이터 검색과 연결 지도">')
    return text.replace("<head>", "<head>" + identity, 1).encode("utf-8")
