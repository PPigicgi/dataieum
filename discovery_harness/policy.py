"""Validated immutable policy. Unknown fields fail closed."""
import json
import math
import sys
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import get_type_hints

from .errors import PolicyError


class _Section:
    def __post_init__(self):
        for name, kind in get_type_hints(type(self)).items():
            value = getattr(self, name)
            if kind is int:
                valid = type(value) is int and 0 <= value <= sys.maxsize
            elif kind is float:
                try:
                    valid = (type(value) in (int, float) and math.isfinite(value)
                             and (value >= 0 if name == 'max_queue_seconds' else value > 0))
                except OverflowError:
                    valid = False
            elif kind is bool:
                valid = type(value) is bool
            elif kind is str:
                valid = isinstance(value, str) and 0 < len(value) <= 100 and value == value.strip()
            else:
                valid = type(value) is kind
            if not valid:
                raise PolicyError(f'invalid policy field: {type(self).__name__}.{name}')


@dataclass(frozen=True)
class ModelPolicy(_Section):
    default: str = 'gpt-5.6-luna'
    escalation: str = 'gpt-5.6-sol'


@dataclass(frozen=True)
class AgentPolicy(_Section):
    max_turns: int = 4
    max_llm_calls: int = 2
    max_tool_calls: int = 8
    max_retries: int = 1
    timeout_seconds: float = 15.0
    call_timeout_seconds: float = 8.0
    max_queue_seconds: float = 0.0
    max_input_tokens: int = 12000
    max_output_tokens: int = 2000
    max_prompt_bytes: int = 48000
    max_result_bytes: int = 262144
    max_result_rows: int = 200
    max_stream_items: int = 256

    def __post_init__(self):
        super().__post_init__()
        if self.max_queue_seconds > 7200:
            raise PolicyError('scheduled queue budget must not exceed 7200 seconds')
        if self.max_queue_seconds and self.timeout_seconds > 60:
            raise PolicyError('scheduled execution budget must not exceed 60 seconds')


@dataclass(frozen=True)
class OntologyPolicy(_Section):
    max_depth: int = 2
    max_nodes: int = 30
    max_edges: int = 60
    max_datasets: int = 10
    max_neighbor_items: int = 200


@dataclass(frozen=True)
class MemoryPolicy(_Section):
    keep_recent_turns: int = 3
    max_size_bytes: int = 24000
    persist_session: bool = False
    cleanup_on_finish: bool = True

    def __post_init__(self):
        super().__post_init__()
        if self.persist_session or not self.cleanup_on_finish:
            raise PolicyError('request memory must be ephemeral')


@dataclass(frozen=True)
class WorkspacePolicy(_Section):
    max_size_mb: int = 200
    max_files: int = 32
    ttl_seconds: float = 300.0
    cleanup_on_finish: bool = True

    def __post_init__(self):
        super().__post_init__()
        if not self.cleanup_on_finish:
            raise PolicyError('workspace cleanup is mandatory')
        if self.max_size_mb > sys.maxsize // (1024 * 1024):
            raise PolicyError('workspace byte limit exceeds the platform integer range')


@dataclass(frozen=True)
class CachePolicy(_Section):
    policy: str = 'selective'
    max_entries: int = 256
    max_size_bytes: int = 8 * 1024 * 1024
    max_entry_bytes: int = 8 * 1024 * 1024
    ttl_seconds: float = 900.0

    def __post_init__(self):
        super().__post_init__()
        if self.policy != 'selective':
            raise PolicyError('only selective caching is supported')


@dataclass(frozen=True)
class AdmissionPolicy(_Section):
    max_concurrent_requests: int = 8


@dataclass(frozen=True)
class ServerPolicy(_Section):
    enabled: bool = False
    telemetry: str = 'fargate'
    proc_root: str = '/proc'
    disk_path: str = '/'
    host_min_memory_mib: int = 7168
    task_cpu_vcpu: float = 2.0
    task_memory_mib: int = 8192
    task_ephemeral_mib: int = 20480
    max_requests: int = 4
    request_memory_mib: int = 256
    max_workspace_mib: int = 800
    max_llm_inflight: int = 2
    max_tool_inflight: int = 2
    max_db_inflight: int = 1
    cpu_high: float = .75
    cpu_resume: float = .60
    memory_high: float = .70
    memory_resume: float = .60
    disk_high: float = .80
    disk_resume: float = .70
    min_disk_free_mib: int = 2048
    max_sample_age_seconds: float = 3.0
    poll_interval_seconds: float = 1.0
    loop_lag_high_seconds: float = .10
    recovery_samples: int = 2
    db_timeout_seconds: float = 2.5
    catalog_prepare_timeout_seconds: float = 1800.0
    catalog_index_max_mib: int = 12288
    max_request_body_bytes: int = 8192
    max_response_bytes: int = 262144
    lock_path: str = '/harness-control/owner.lock'

    def __post_init__(self):
        super().__post_init__()
        if self.telemetry not in {'fargate', 'linux_host'}:
            raise PolicyError('unsupported telemetry source')
        if self.host_min_memory_mib < 5120:
            raise PolicyError('host must leave room for app, database and OS')
        for resource in ('cpu', 'memory', 'disk'):
            if not 0 < getattr(self, resource + '_resume') < getattr(self, resource + '_high') < 1:
                raise PolicyError('resume threshold must be below high threshold and 1')
        for name in ('task_memory_mib', 'task_ephemeral_mib', 'max_requests',
                     'request_memory_mib', 'max_workspace_mib', 'max_llm_inflight',
                     'max_tool_inflight', 'max_db_inflight', 'recovery_samples',
                     'max_request_body_bytes', 'max_response_bytes'):
            if not getattr(self, name):
                raise PolicyError('server capacities must be positive')
        if not self.catalog_index_max_mib:
            raise PolicyError('catalog index budget must be positive')
        if (self.request_memory_mib >= self.task_memory_mib * self.memory_high or
                self.max_workspace_mib >= self.task_ephemeral_mib * self.disk_high or
                self.min_disk_free_mib >= self.task_ephemeral_mib or
                self.poll_interval_seconds >= self.max_sample_age_seconds):
            raise PolicyError('server envelope has insufficient headroom')


@dataclass(frozen=True)
class Policy(_Section):
    model: ModelPolicy = field(default_factory=ModelPolicy)
    agent: AgentPolicy = field(default_factory=AgentPolicy)
    ontology: OntologyPolicy = field(default_factory=OntologyPolicy)
    memory: MemoryPolicy = field(default_factory=MemoryPolicy)
    workspace: WorkspacePolicy = field(default_factory=WorkspacePolicy)
    cache: CachePolicy = field(default_factory=CachePolicy)
    admission: AdmissionPolicy = field(default_factory=AdmissionPolicy)
    server: ServerPolicy = field(default_factory=ServerPolicy)

    def __post_init__(self):
        super().__post_init__()
        if self.server.enabled and self.workspace.max_size_mb > self.server.max_workspace_mib:
            raise PolicyError('one workspace exceeds task workspace capacity')
        if (self.agent.max_queue_seconds and self.workspace.ttl_seconds <=
                self.agent.timeout_seconds + self.agent.max_queue_seconds):
            raise PolicyError('workspace TTL must exceed scheduled request lifetime')

    @classmethod
    def from_dict(cls, value: dict) -> 'Policy':
        if not isinstance(value, dict):
            raise PolicyError('policy must be an object')
        types = get_type_hints(cls)
        if value.keys() - types.keys():
            raise PolicyError('unknown policy section')
        sections = {}
        for name, config in value.items():
            kind = types[name]
            if not isinstance(config, dict) or config.keys() - {f.name for f in fields(kind)}:
                raise PolicyError(f'invalid fields in section {name}')
            sections[name] = kind(**config)
        return cls(**sections)

    @classmethod
    def load(cls, path: str | Path) -> 'Policy':
        def unique_object(pairs):
            obj = {}
            for key, value in pairs:
                if key in obj:
                    raise PolicyError('duplicate JSON policy field')
                obj[key] = value
            return obj

        try:
            with Path(path).open('rb') as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                raise PolicyError('policy exceeds 64 KiB')
            return cls.from_dict(json.loads(raw.decode('utf-8'), object_pairs_hook=unique_object))
        except PolicyError:
            raise
        except (UnicodeError, ValueError, RecursionError, OverflowError) as exc:
            raise PolicyError('policy must contain valid UTF-8 JSON') from exc
