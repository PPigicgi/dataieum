"""Bounded, coalesced local login checks; never capture authentication output."""
import asyncio
import time


class CodexLoginReadiness:
    def __init__(self, executable, *, failed=lambda: False):
        self.executable = str(executable)
        self.failed = failed
        self.lock = asyncio.Lock()
        self.checked_at = None
        self.available = False

    def status(self, *, last_model_success_at=None):
        # Observations, not an active upstream probe or a readiness TTL.
        return {'basis': 'local_session_and_observed_rejections',
                'local_session_available': self.available,
                'upstream_authentication_rejected': bool(self.failed()),
                'last_model_success_at': last_model_success_at}

    async def ready(self):
        # An observed upstream authentication rejection needs reauthentication
        # and gateway restart; a locally present stale cache cannot clear it.
        if self.failed():
            return False
        async with self.lock:
            if self.checked_at is None or time.monotonic() - self.checked_at >= 5:
                process = None
                self.available = False
                try:
                    async with asyncio.timeout(2):
                        process = await asyncio.create_subprocess_exec(
                            self.executable, 'login', 'status',
                            stdin=asyncio.subprocess.DEVNULL,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL)
                        self.available = await process.wait() == 0
                except (OSError, TimeoutError):
                    pass
                finally:
                    if process is not None and process.returncode is None:
                        try: process.kill()
                        except ProcessLookupError: pass
                        await process.wait()
                    self.checked_at = time.monotonic()
            return self.available and not self.failed()
