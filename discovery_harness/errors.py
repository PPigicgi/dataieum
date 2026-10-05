"""Stable error codes; errors never include prompts or adapter responses."""


class HarnessError(Exception):
    code = 'harness_error'

    def as_dict(self):
        result = {'code': self.code}
        if isinstance(self, BudgetExceeded):
            result.update(resource=self.resource, limit=self.limit)
        return result


class PolicyError(HarnessError, ValueError):
    code = 'invalid_policy'


class BudgetExceeded(HarnessError):
    code = 'budget_exceeded'

    def __init__(self, resource: str, limit: int):
        self.resource = resource
        self.limit = limit
        super().__init__(f'{resource} budget exhausted (limit={limit})')


class CapacityExceeded(HarnessError):
    code = 'capacity_exceeded'


class DeadlineExceeded(HarnessError):
    code = 'deadline_exceeded'


class SessionClosed(HarnessError):
    code = 'session_closed'


class AdapterContractError(HarnessError):
    code = 'adapter_contract_error'


class WorkspaceError(HarnessError):
    code = 'workspace_error'


class CleanupFailed(HarnessError):
    code = 'cleanup_failed'


class RetryableError(Exception):
    """A trusted adapter explicitly permits a retry of this operation."""
