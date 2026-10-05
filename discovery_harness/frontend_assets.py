"""One hashed build; host-specific HTML is never shared by HTTP caches."""
from functools import lru_cache
import hashlib
import re

from starlette.responses import Response

from .branding import brand_document
from .chat import FAVICON_LINKS


@lru_cache(maxsize=12)
def _read(path):
    with path.open('rb') as f:body=f.read(2*1024**2+1)
    if len(body)>2*1024**2:raise ValueError('asset size limit')
    return body


async def serve(scope,receive,send,root,brand):
    path=scope['path']
    try:
        if path in {'/','/catalogue'}:
            body=brand_document(_read(root/'index.html').replace(b'</head>',FAVICON_LINKS+b'<script src="/cooperation-mail.js" defer></script><link rel="stylesheet" href="/cooperation-mail.css">'+b'</head>',1),brand)
            response=Response(body,media_type='text/html')
        else:
            name=path.removeprefix('/assets/')
            if not re.fullmatch(r'[A-Za-z0-9_-]+-[A-Za-z0-9_-]{8,}\.(?:js|css)',name):raise FileNotFoundError
            body=_read(root/'assets'/name)
            etag='"'+hashlib.sha256(body).hexdigest()+'"'
            headers={'Cache-Control':'public, max-age=31536000, immutable','ETag':etag}
            tags=[v for k,v in scope.get('headers',[]) if k.lower()==b'if-none-match']
            response=Response(status_code=304,headers=headers) if tags==[etag.encode()] else Response(body,media_type='text/javascript' if name.endswith('.js') else 'text/css',headers=headers)
    except (FileNotFoundError,ValueError):response=Response(b'{"code":"not_found"}',status_code=404,media_type='application/json')
    await response(scope,receive,send)
