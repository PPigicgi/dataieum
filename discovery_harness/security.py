"""Bounded abuse controls. Pattern detection supplements, never grants, authority."""
from collections import Counter, OrderedDict, deque
from dataclasses import dataclass, field
import hmac
import ipaddress
import math
import os
from pathlib import Path
import re
import time
import unicodedata
from urllib.parse import unquote, urlsplit


class UnsafePrompt(ValueError):
    pass


_PATTERNS = tuple(re.compile(p, re.I) for p in (
    r'(?:ignore|disregard|override|bypass)\b.{0,70}\b(?:instructions?|system\s*prompt|rules|safety|guardrails)',
    r'(?:system|developer)\s*(?:prompt|instructions?).{0,70}\b(?:reveal|print|show|output|leak)',
    r'(?:reveal|print|show|output|leak)\b.{0,70}(?:system\s*prompt|developer\s*instructions?|your\s+(?:api\s*key|secret|credentials))',
    r'(?:이전|시스템|개발자|보안|모든).{0,30}(?:지시|지침|명령|규칙|프롬프트).{0,30}(?:무시|우회|폐기|출력|공개|보여)',
    r'(?:너의|내부|저장된).{0,25}(?:인증키|비밀키|api\s*키|자격증명).{0,25}(?:보여|출력|보내|공개)',
    r'<\|(?:im_start|im_end|system|developer)\|>|\[\s*(?:system|developer)\s*\]|<\s*/?\s*(?:system|developer)\s*>',
    r'(?:execute|run)\s+(?:the\s+)?(?:shell\s+command|bash\s+command|powershell\s+command)',
    r'(?:셸|쉘|bash|powershell|터미널)\s*(?:명령|명령어).{0,25}(?:실행|호출)',
))


def check_prompt(value):
    """Reject explicit control-plane attacks, including simple Unicode obfuscation.

    Do not recursively decode arbitrary payloads or claim semantic detection of
    every injection. Schema, no-tool execution and curated evidence remain the
    authority boundary even for attacks these patterns do not recognize.
    """
    if not isinstance(value, str) or len(value.encode('utf-8')) > 24576:
        raise ValueError('invalid prompt input')
    normalized = unicodedata.normalize('NFKC', value)
    normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Cf')
    normalized = ' '.join(normalized.split())
    for candidate in (normalized, unquote(normalized)):
        if any(pattern.search(candidate) for pattern in _PATTERNS):
            raise UnsafePrompt('instructions outside search scope')


def load_gateway_token(path):
    token = Path(path).read_text(encoding='utf-8').strip()
    if not re.fullmatch(r'[a-f0-9]{64}', token):
        raise ValueError('gateway token file must contain a 256-bit hex token')
    return token


def gateway_authorized(headers, expected):
    values = [v for k, v in headers if k.lower() == 'x-dataieum-gateway-token']
    return bool(expected and len(values) == 1 and hmac.compare_digest(values[0], expected))


@dataclass(frozen=True)
class AbusePolicy:
    api_per_minute: int = 120
    chat_per_minute: int = 12
    chat_per_hour: int = 120
    chat_concurrent: int = 2
    max_clients: int = 4096
    trusted_proxies: tuple = ()

    def __post_init__(self):
        for name in ('api_per_minute', 'chat_per_minute', 'chat_per_hour', 'chat_concurrent', 'max_clients'):
            if type(getattr(self, name)) is not int or not 1 <= getattr(self, name) <= 10000:
                raise ValueError('invalid abuse limit: '+name)
        for cidr in self.trusted_proxies:
            if ipaddress.ip_network(cidr).prefixlen == 0:
                raise ValueError('trusting every proxy is forbidden')

    @classmethod
    def from_env(cls):
        names = {'api_per_minute':'API_PER_MINUTE', 'chat_per_minute':'CHAT_PER_MINUTE',
                 'chat_per_hour':'CHAT_PER_HOUR', 'chat_concurrent':'CHAT_CONCURRENT', 'max_clients':'MAX_CLIENTS'}
        values = {key:int(os.environ['DATAIEUM_IP_'+suffix]) for key,suffix in names.items() if 'DATAIEUM_IP_'+suffix in os.environ}
        values['trusted_proxies'] = tuple(v.strip() for v in os.environ.get('DATAIEUM_TRUSTED_PROXIES','').split(',') if v.strip())
        return cls(**values)


@dataclass
class _Client:
    api: deque = field(default_factory=deque)
    chat: deque = field(default_factory=deque)
    active: int = 0
    seen: float = 0


class RateLimited(Exception):
    def __init__(self, code, retry_after, status=429):
        self.code, self.retry_after, self.status = code, max(1, math.ceil(retry_after)), status


class ClientGuard:
    def __init__(self, policy=None, *, clock=time.monotonic):
        self.policy = policy or AbusePolicy()
        self.clock = clock
        self.clients = OrderedDict()
        self.rejected = Counter()
        self.trusted = tuple(ipaddress.ip_network(c) for c in self.policy.trusted_proxies)

    @staticmethod
    def _ip(value):
        if '%' in value: raise ValueError('scoped client address not accepted')
        address = ipaddress.ip_address(value)
        return address.ipv4_mapped or address if address.version == 6 else address

    def identity(self, scope):
        peer = scope.get('client')
        if not peer: return 'unknown'
        address = self._ip(peer[0])
        if any(address in network for network in self.trusted):
            values = [v for k,v in scope.get('headers',[]) if k.lower() == b'x-forwarded-for']
            if values:
                if len(values)!=1 or len(values[0])>1024:raise ValueError('invalid forwarded chain')
                chain = values[0].decode('ascii').split(',')
                if len(chain)>10:raise ValueError('forwarded chain too long')
                parsed = [self._ip(v.strip()) for v in chain]
                while parsed and any(address in network for network in self.trusted):
                    address = parsed.pop()
        # Rotate an IPv6 host identifier without obtaining a fresh quota.
        return str(ipaddress.ip_network(str(address)+'/64', strict=False)) if address.version==6 else str(address)

    def acquire(self, scope, *, chat=False):
        now = self.clock(); key = self.identity(scope)
        while self.clients:
            oldest, state = next(iter(self.clients.items()))
            if state.active or now-state.seen<3600: break
            self.clients.pop(oldest)
        state = self.clients.get(key)
        if state is None:
            if len(self.clients)>=self.policy.max_clients:
                self._deny('client_capacity', 60, 503)
            state = self.clients[key] = _Client(seen=now)
        state.seen = now; self.clients.move_to_end(key)
        while state.api and state.api[0] <= now-60:state.api.popleft()
        while state.chat and state.chat[0] <= now-3600:state.chat.popleft()
        if len(state.api)>=self.policy.api_per_minute:self._deny('ip_api_rate', state.api[0]+60-now)
        state.api.append(now)
        if chat:
            recent = [stamp for stamp in state.chat if stamp>now-60]
            if len(recent)>=self.policy.chat_per_minute:self._deny('ip_chat_rate', recent[0]+60-now)
            if len(state.chat)>=self.policy.chat_per_hour:self._deny('ip_chat_hour', state.chat[0]+3600-now)
            if state.active>=self.policy.chat_concurrent:self._deny('ip_chat_concurrency', 2)
            state.chat.append(now);state.active+=1
        return state if chat else None

    def _deny(self, code, delay, status=429):
        self.rejected[code]+=1
        raise RateLimited(code,delay,status)

    @staticmethod
    def release(state):
        if state is not None:state.active-=1

    def status(self):
        return {'tracked_clients':len(self.clients),'active_chat':sum(c.active for c in self.clients.values()),
                'rejected':dict(self.rejected),'limits':{k:getattr(self.policy,k) for k in
                ('api_per_minute','chat_per_minute','chat_per_hour','chat_concurrent','max_clients')}}


def same_origin(scope):
    headers=scope.get('headers',[])
    sites=[v for k,v in headers if k.lower()==b'sec-fetch-site']
    if sites and (len(sites)!=1 or sites[0] not in {b'same-origin',b'none'}):return False
    origins=[v for k,v in headers if k.lower()==b'origin']
    if not origins:return True
    hosts=[v for k,v in headers if k.lower()==b'host']
    if len(origins)!=1 or len(hosts)!=1:return False
    try:
        origin=urlsplit(origins[0].decode('ascii'));host=urlsplit(scope.get('scheme','http')+'://'+hosts[0].decode('ascii'))
        port=lambda u:u.port or (443 if u.scheme=='https' else 80)
        return not origin.username and not origin.password and not origin.path and not origin.query and not origin.fragment and (origin.scheme,origin.hostname,port(origin))==(host.scheme,host.hostname,port(host))
    except (ValueError,UnicodeError):return False
