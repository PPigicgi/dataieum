"""Bounded request memory, flat temporary workspace, and selective JSON cache."""
import json
import math
import os
import re
import shutil
import stat
import tempfile
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from .errors import BudgetExceeded, CleanupFailed, PolicyError, SessionClosed, WorkspaceError
from .policy import CachePolicy, MemoryPolicy, WorkspacePolicy

ALLOWED_KINDS = frozenset({'metadata', 'ontology_mapping'})


def bounded_json(value, limit: int, resource: str = 'result_bytes') -> bytes:
    """Encode JSON incrementally without coercing object keys or huge strings.

    Non-string keys are rejected: coercing 1 and '1' into the same JSON key
    silently loses metadata on decoding. Tuples retain JSON's list conversion.
    """
    output = bytearray()
    active = set()

    def append(chunk):
        if len(chunk) > limit - len(output):
            raise BudgetExceeded(resource, limit)
        encoded = chunk.encode('utf-8')
        if len(output) + len(encoded) > limit:
            raise BudgetExceeded(resource, limit)
        output.extend(encoded)

    def visit(item):
        kind = type(item)
        if item is None:
            append('null')
        elif kind is bool:
            append('true' if item else 'false')
        elif kind is int:
            append(str(item))
        elif kind is float:
            if not math.isfinite(item):
                raise ValueError('non-finite JSON number')
            append(repr(item))
        elif kind is str:
            if len(item) > limit - len(output):
                raise BudgetExceeded(resource, limit)
            append(json.encoder.encode_basestring(item))
        elif kind in (list, tuple, dict):
            identity = id(item)
            if identity in active:
                raise PolicyError('cyclic JSON value')
            active.add(identity)
            is_object = kind is dict
            append('{' if is_object else '[')
            for index, entry in enumerate(item):
                if index:
                    append(',')
                if is_object:
                    if type(entry) is not str:
                        raise PolicyError('JSON object keys must be strings')
                    visit(entry)
                    append(':')
                    visit(item[entry])
                else:
                    visit(entry)
            append('}' if is_object else ']')
            active.remove(identity)
        else:
            raise PolicyError('value must contain JSON primitives')

    try:
        visit(value)
    except (TypeError, ValueError, RecursionError, UnicodeError) as exc:
        raise PolicyError('value must be finite, serializable JSON') from exc
    return bytes(output)


def _text_size(value: str, limit: int, resource: str) -> int:
    if not isinstance(value, str):
        raise PolicyError('text must be a string')
    if len(value) > limit:
        raise BudgetExceeded(resource, limit)
    try:
        size = len(value.encode('utf-8'))
    except UnicodeError as exc:
        raise PolicyError('text must be valid UTF-8') from exc
    if size > limit:
        raise BudgetExceeded(resource, limit)
    return size


@dataclass(frozen=True)
class Message:
    role: str
    content: str


class Memory:
    def __init__(self, policy: MemoryPolicy):
        self._policy = policy
        self._entries = deque()
        self._bytes = 0
        self._turn = 0
        self._closed = False

    def append(self, role: str, content: str) -> None:
        if self._closed:
            raise SessionClosed('memory is closed')
        if role not in ('user', 'assistant'):
            raise PolicyError('memory role must be user or assistant')
        size = _text_size(content, self._policy.max_size_bytes, 'memory_bytes') + len(role) + 8
        if size > self._policy.max_size_bytes:
            raise BudgetExceeded('memory_bytes', self._policy.max_size_bytes)
        if not self._policy.keep_recent_turns:
            return
        if role == 'user' or self._turn == 0:
            self._turn += 1
        self._entries.append((self._turn, Message(role, content), size))
        self._bytes += size
        earliest = self._turn - self._policy.keep_recent_turns + 1
        while self._entries and (self._entries[0][0] < earliest or
                                 self._bytes > self._policy.max_size_bytes):
            self._bytes -= self._entries.popleft()[2]

    def snapshot(self) -> tuple[Message, ...]:
        return tuple(entry[1] for entry in self._entries)

    def close(self) -> None:
        self._closed = True
        self._entries.clear()
        self._bytes = 0
        self._turn = 0


def _is_link(path: Path) -> bool:
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0))


class Workspace:
    """Only metadata/mapping bytes written through this interface are budgeted.

    Root must be private to this process. This is not a hostile-code filesystem sandbox.
    """
    def __init__(self, root: Path, policy: WorkspacePolicy, *, clock=time.monotonic,
                 on_cleanup_failure=None):
        self._policy = policy
        self._clock = clock
        self._expires = clock() + policy.ttl_seconds
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self._lock = RLock()
        generated = tempfile.mkdtemp(prefix='request-', dir=self._root)
        try:
            self._path = Path(generated).resolve()
        except BaseException:
            try:
                # The root was resolved before creation. Roll back only the
                # generated direct child; never recursively remove its content.
                target = Path(generated)
                if (not target.is_absolute() or target.parent != self._root or
                        not target.name.startswith('request-') or _is_link(target)):
                    raise WorkspaceError('refusing rollback of changed workspace path')
                os.rmdir(target)
            except BaseException as cleanup_error:
                if on_cleanup_failure is not None:
                    on_cleanup_failure()
                raise CleanupFailed('workspace initialization cleanup unconfirmed') from cleanup_error
            raise

    @property
    def path(self) -> Path:
        return self._path

    def _check(self):
        if self._closed:
            raise SessionClosed('workspace is closed')
        if self._clock() >= self._expires:
            self.close()
            raise WorkspaceError('workspace TTL expired')
        if _is_link(self.path) or self.path.resolve() != self.path or self.path.parent != self._root:
            raise WorkspaceError('workspace path changed')

    def _target(self, name: str) -> Path:
        if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,119}', name):
            raise WorkspaceError('use a flat ASCII filename')
        stem = name.split('.')[0].upper()
        if name.endswith('.') or stem in {'CON', 'PRN', 'AUX', 'NUL',
                                         *(f'COM{i}' for i in range(10)),
                                         *(f'LPT{i}' for i in range(10))}:
            raise WorkspaceError('reserved filename')
        target = self.path / name
        if target.is_symlink() or (target.exists() and (_is_link(target) or
                                  not target.is_file() or target.stat().st_nlink != 1)):
            raise WorkspaceError('workspace links and special files are forbidden')
        return target

    def _inventory(self):
        total, count = 0, 0
        for entry in self.path.iterdir():
            if _is_link(entry) or not entry.is_file() or entry.stat().st_nlink != 1:
                raise WorkspaceError('unexpected workspace entry')
            total += entry.stat().st_size
            count += 1
        return total, count

    @property
    def size_bytes(self) -> int:
        with self._lock:
            self._check()
            return self._inventory()[0]

    def write(self, name: str, data: bytes, *, kind: str) -> Path:
        with self._lock:
            self._check()
            if kind not in ALLOWED_KINDS or type(data) is not bytes:
                raise WorkspaceError('only metadata/mapping bytes may be stored')
            target = self._target(name)
            total, count = self._inventory()
            old_size = target.stat().st_size if target.exists() else 0
            limit = self._policy.max_size_mb * 1024 * 1024
            if total - old_size + len(data) > limit:
                raise BudgetExceeded('workspace_bytes', limit)
            if count + int(not target.exists()) > self._policy.max_files:
                raise BudgetExceeded('workspace_files', self._policy.max_files)
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, 'O_NOFOLLOW', 0)
            try:
                descriptor = os.open(target, flags, 0o600)
                try:
                    stream = os.fdopen(descriptor, 'wb')
                except BaseException:
                    os.close(descriptor)
                    raise
                with stream:
                    if stream.write(data) != len(data):
                        raise OSError('incomplete workspace write')
            except BaseException as exc:
                # No staging copy that doubles the disk quota: an incomplete
                # overwrite instead poisons this workspace until final cleanup.
                self._closed = True
                if isinstance(exc, OSError):
                    raise WorkspaceError('workspace write failed; further I/O is blocked') from exc
                raise
            return target

    def read(self, name: str) -> bytes:
        with self._lock:
            self._check()
            target = self._target(name)
            limit = self._policy.max_size_mb * 1024 * 1024
            if self._inventory()[0] > limit:
                raise BudgetExceeded('workspace_bytes', limit)
            with target.open('rb') as source:
                size = os.fstat(source.fileno()).st_size
                value = source.read(min(size, limit) + 1)
            if len(value) > limit:
                raise BudgetExceeded('workspace_bytes', limit)
            if len(value) != size:
                raise WorkspaceError('workspace file changed during read')
            return value

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if not self.path.exists() and not self.path.is_symlink():
                return
            # Verify the absolute deletion target remains the generated child.
            if (self.path.parent != self._root or _is_link(self.path) or
                    self.path.resolve() != self.path or not self.path.name.startswith('request-')):
                raise WorkspaceError('refusing cleanup of a changed workspace path')
            shutil.rmtree(self.path)


class SelectiveCache:
    """Process-local LRU of detached public JSON, with TTL and byte/entry caps."""
    def __init__(self, policy: CachePolicy, *, clock=time.monotonic):
        self._policy = policy
        self._clock = clock
        self._items = OrderedDict()
        self._bytes = 0
        self._lock = RLock()
        self._stats = dict(hits=0, misses=0, puts=0, evictions=0, expirations=0)

    def _key(self, kind, key):
        if kind not in ALLOWED_KINDS:
            raise PolicyError('cache kind must be metadata or ontology_mapping')
        _text_size(key, 256, 'cache_key_bytes')
        if not key:
            raise PolicyError('cache key cannot be empty')
        return kind, key

    def _remove(self, key):
        self._bytes -= self._items.pop(key)[2]

    def expire(self) -> int:
        with self._lock:
            expired = [key for key, value in self._items.items() if value[0] <= self._clock()]
            for key in expired:
                self._remove(key)
            self._stats['expirations'] += len(expired)
            return len(expired)

    def status(self) -> dict:
        """Bounded numeric telemetry; never expose cache keys or cached content."""
        with self._lock:
            self.expire()
            return {**self._stats, 'entries': len(self._items), 'bytes': self._bytes,
                    'max_entries': self._policy.max_entries, 'max_bytes': self._policy.max_size_bytes}

    @property
    def size_bytes(self) -> int:
        with self._lock:
            self.expire()
            return self._bytes

    def put(self, kind: str, key: str, value) -> None:
        with self._lock:
            identity = self._key(kind, key)
            encoded = bounded_json(value, min(self._policy.max_size_bytes,
                                             self._policy.max_entry_bytes), 'cache_bytes')
            size = len(encoded) + len(kind.encode()) + len(key.encode('utf-8'))
            if size > self._policy.max_size_bytes:
                raise BudgetExceeded('cache_bytes', self._policy.max_size_bytes)
            if self._policy.max_entries == 0:
                raise BudgetExceeded('cache_entries', 0)
            self.expire()
            if identity in self._items:
                self._remove(identity)
            while self._items and (len(self._items) >= self._policy.max_entries or
                                    self._bytes + size > self._policy.max_size_bytes):
                self._remove(next(iter(self._items)))
                self._stats['evictions'] += 1
            self._items[identity] = (self._clock() + self._policy.ttl_seconds, encoded, size)
            self._bytes += size
            self._stats['puts'] += 1

    def get(self, kind: str, key: str):
        encoded = self.get_encoded(kind, key)
        return json.loads(encoded) if encoded is not None else None

    def get_encoded(self, kind: str, key: str):
        """Immutable serialized JSON for HTTP reuse without decode/encode copies."""
        with self._lock:
            identity = self._key(kind, key)
            self.expire()
            item = self._items.get(identity)
            if item is None:
                self._stats['misses'] += 1
                return None
            self._stats['hits'] += 1
            self._items.move_to_end(identity)
            return item[1]

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
            self._bytes = 0
