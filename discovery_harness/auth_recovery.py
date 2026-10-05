"""One-shot host-side Codex recovery. No network auth refresh or model calls.

Invoked over an existing administrator SSH connection; never expose this as HTTP.
Only the current Compose Luna container is restarted. Credentials stay on stdin
and in its existing private auth volume. All outward errors are fixed codes.
"""
import base64
import contextlib
import hashlib
import hmac
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import signal
import stat
import subprocess
import time
import uuid


class RecoveryError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def require(condition, code):
    if not condition:
        raise RecoveryError(code)


def auth_info(payload, *, fresh=False):
    """JWT expiry is a local freshness check, not proof of upstream acceptance."""
    try:
        require(isinstance(payload, bytes) and 0 < len(payload) <= 65536, 'invalid_auth_document')
        value = json.loads(payload)
        tokens = value['tokens']
        require(value.get('auth_mode') == 'chatgpt' and all(
            isinstance(tokens.get(k), str) and tokens[k]
            for k in ('access_token', 'refresh_token', 'account_id')), 'invalid_auth_document')
        account = hashlib.sha256(tokens['account_id'].encode()).hexdigest()
        credential = hashlib.sha256(json.dumps(
            [tokens[k] for k in ('account_id', 'access_token', 'refresh_token')],
            separators=(',', ':')).encode()).hexdigest()
        if fresh:
            middle = tokens['access_token'].split('.')[1]
            expiry = json.loads(base64.urlsafe_b64decode(middle + '='*(-len(middle)%4)))['exp']
            require(type(expiry) in (int, float) and math.isfinite(expiry) and
                    expiry > time.time()+600, 'local_login_required')
        return {'account_sha256': account, 'credential_sha256': credential}
    except RecoveryError:
        raise
    except (ValueError, KeyError, TypeError, IndexError, UnicodeError):
        raise RecoveryError('invalid_auth_document') from None


def private_read(path, limit=65536):
    """Bounded no-follow read; the caller owns a private containing directory."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError:
        raise RecoveryError('unsafe_private_file') from None
    with os.fdopen(fd, 'rb') as stream:
        mode = os.fstat(stream.fileno()).st_mode
        require(stat.S_ISREG(mode) and mode & 0o077 == 0, 'unsafe_private_file')
        data = stream.read(limit+1)
        require(len(data) <= limit, 'private_file_too_large')
        return data


def replace_private(path, data, uid, gid):
    require(not path.is_symlink(), 'unsafe_private_file')
    temporary = path.with_name('.recovery-'+uuid.uuid4().hex)
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            os.fchown(stream.fileno(), uid, gid)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def configuration_changes(before, after):
    """Compare configuration, excluding Docker's changing runtime IP/state."""
    def values(row):
        config = dict(row['Config'])
        environment = sorted(config.pop('Env', []) or [])
        return {'container': row['Id'], 'image': row['Image'], 'config': config,
                'environment': environment, 'host_config': row['HostConfig'],
                'mounts': sorted(row['Mounts'], key=lambda m:m['Destination']),
                'networks': sorted(row['NetworkSettings']['Networks'])}
    left, right = values(before), values(after)
    return [key for key in left if left[key] != right[key]]


def enroll(host):
    identity = auth_info(host.auth())['account_sha256']
    state = host.read_state()
    require(not state.get('account_sha256') or
            hmac.compare_digest(identity, state['account_sha256']), 'account_mismatch')
    if not state.get('account_sha256'):
        host.save_state(dict(state, account_sha256=identity))
    return {'status':'account_enrolled', 'credentials_transferred':False}


def recover(host, payload, *, apply=False, activate=False, validate_source=lambda: None):
    readiness = host.readiness()
    if readiness.get('ready') is True and not activate:
        require(not host.is_paused(), 'dispatcher_already_paused')
        return {'status':'already_ready', 'credentials_transferred':False}
    if not apply and not activate:
        return {'status':'authentication_required' if readiness.get('state') == 'authentication_required'
                else 'service_unavailable', 'credentials_transferred':False,
                'account_enrolled': bool(host.read_state().get('account_sha256'))}
    require(readiness.get('state') == 'authentication_required' or
            (activate and readiness.get('ready') is True), 'not_authentication_failure')
    incoming = auth_info(payload, fresh=True)
    state = host.read_state()
    existing = host.auth()
    identity = state.get('account_sha256')
    if existing:
        current = auth_info(existing)
        require(not identity or hmac.compare_digest(identity,current['account_sha256']), 'account_mismatch')
        identity = current['account_sha256']
    require(identity is not None, 'server_identity_unknown')
    require(hmac.compare_digest(identity,incoming['account_sha256']), 'account_mismatch')
    require(state.get('last_attempt_sha256') != incoming['credential_sha256'], 'credential_already_attempted')
    if existing:
        require(auth_info(existing)['credential_sha256'] != incoming['credential_sha256'], 'credential_unchanged')
        if activate:
            require(json.loads(existing)['tokens']['refresh_token'] !=
                    json.loads(payload)['tokens']['refresh_token'], 'server_session_not_new')
    # The identity originates only from existing server auth or its saved pin.
    state['account_sha256'] = identity
    result = {'status':'preparing', 'phase':'pausing', 'credentials_transferred':False, 'dispatcher_unpaused':False}
    paused = False
    stop_attempted = False
    failure = None
    try:
        host.pause()
        paused = True
        result['phase'] = 'draining'
        host.drain()
        host.unchanged()
        require(host.auth() == existing, 'server_auth_changed')
        validate_source()
        result['phase'] = 'stopping'
        stop_attempted = True
        host.stop()
        require(host.auth() == existing, 'server_auth_changed')
        validate_source()
        # Record intent before replacement; ambiguous/interrupted attempts must
        # inspect current health rather than blindly overwrite rotated tokens.
        state.update(last_attempt_sha256=incoming['credential_sha256'], outcome='installing')
        host.save_state(state)
        result['phase'] = 'installing'
        host.install(payload)
        result['credentials_transferred'] = True
        state['outcome'] = 'installed'
        host.save_state(state)
        result['phase'] = 'starting'
        host.start()
        stop_attempted = False
        result['phase'] = 'verifying'
        host.wait_ready()
        host.unchanged()
        state['outcome'] = 'ready'
        host.save_state(state)
        result.update(status='recovered', phase='complete')
    except BaseException as error:
        failure = error if isinstance(error, RecoveryError) else RecoveryError('host_operation_failed')
        if result['phase'] == 'installing':
            # os.replace may have succeeded before directory fsync failed.
            # Never claim no transfer when the actual outcome is unknown.
            result['credentials_transferred'] = None
            with contextlib.suppress(Exception):
                observed = host.auth()
                if observed == payload:
                    result['credentials_transferred'] = True
                elif observed == existing:
                    result['credentials_transferred'] = False
    finally:
        cleanup_errors = []
        if stop_attempted:
            try:
                host.start()
            except BaseException:
                cleanup_errors.append('luna_start_failed')
        if paused:
            try:
                host.unpause()
                result['dispatcher_unpaused'] = True
            except BaseException:
                cleanup_errors.append('dispatcher_unpause_failed')
        if cleanup_errors:
            result['cleanup_errors'] = cleanup_errors
            failure = failure or RecoveryError('cleanup_failed')
    if failure:
        if state.get('last_attempt_sha256') == incoming['credential_sha256']:
            state['outcome'] = 'verification_incomplete'
            with contextlib.suppress(Exception):
                host.save_state(state)
        result.update(status='incomplete', code=failure.code)
        failure.result = result
        raise failure from None
    return result


def activate_session(host, name):
    """An explicit cutover from a completed login inside the server auth mount."""
    payload = host.staged_auth(name)
    def unchanged():
        require(host.staged_auth(name) == payload, 'server_session_changed')
    result = recover(host, payload, activate=True, validate_source=unchanged)
    try:
        host.consume_staged_auth(name, payload)
    except Exception:
        error = RecoveryError('staged_auth_cleanup_failed')
        error.result = dict(result, phase='staged_cleanup')
        raise error from None
    return dict(result, session_source='server_login')


class DockerHost:
    def __init__(self, project):
        require(isinstance(project,str) and re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,100}',project), 'invalid_project')
        self.project = project
        self.names = {s:project+'-'+s+'-1' for s in ('atlas','luna','vector')}
        self.before = self.inspect()
        for service, row in self.before.items():
            labels = row['Config'].get('Labels') or {}
            require(labels.get('com.docker.compose.project') == project and
                    labels.get('com.docker.compose.service') == service, 'unexpected_container')
            require(row['State']['Running'] and not row['State']['Restarting'], 'container_not_running')
        luna = self.before['luna']
        env = dict(v.split('=',1) for v in luna['Config']['Env'])
        auth_root = env.get('CODEX_HOME')
        mounts = [m for m in luna['Mounts'] if m['Destination'] == auth_root]
        require(len(mounts) == 1 and mounts[0]['Type'] == 'bind' and mounts[0]['RW'], 'unsafe_auth_mount')
        self.folder = Path(mounts[0]['Source'])
        require(self.folder.is_absolute() and not self.folder.is_symlink() and
                self.folder.is_dir() and self.folder.stat().st_mode & 0o777 == 0o700, 'unsafe_auth_directory')
        self.uid,self.gid = self.folder.stat().st_uid,self.folder.stat().st_gid
        self.auth_path = self.folder/'auth.json'
        self.state_path = self.folder/'.dataieum-recovery.json'
        # Refuse a second running service with overlapping authentication storage.
        ids = self.command(['docker','ps','-q']).split()
        if ids:
            others=json.loads(self.command(['docker','inspect',*ids]))
            for row in others:
                if row['Id'] == luna['Id']:
                    continue
                for mount in row['Mounts']:
                    root=Path(mount['Source'])
                    require(not (self.folder.is_relative_to(root) or root.is_relative_to(self.folder)), 'shared_auth_owner')
        atlas=self.before['atlas']
        env=dict(v.split('=',1) for v in atlas['Config']['Env'])
        pause=PurePosixPath(env['DATAIEUM_JOB_DISPATCH_PAUSE'])
        self.pause_path=None
        for mount in sorted(atlas['Mounts'],key=lambda m:len(m['Destination']),reverse=True):
            try:
                relative=pause.relative_to(mount['Destination'])
            except ValueError:
                continue
            root=Path(mount['Source']).resolve()
            candidate=root/str(relative)
            require(mount['RW'] and candidate.resolve().is_relative_to(root) and
                    not candidate.is_symlink(), 'unsafe_pause_path')
            self.pause_path=candidate
            break
        require(self.pause_path is not None, 'pause_not_configured')
        self.marker=uuid.uuid4().hex.encode()

    @staticmethod
    def command(args, *, timeout=15):
        try:
            result=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=timeout,check=True)
            return result.stdout.decode()
        except (OSError,subprocess.SubprocessError,UnicodeError):
            raise RecoveryError('host_command_failed') from None

    def inspect(self):
        rows=json.loads(self.command(['docker','inspect',*self.names.values()]))
        by_name={row['Name'].lstrip('/'):row for row in rows}
        return {s:by_name[name] for s,name in self.names.items()}

    def get(self, service, port, path):
        code=("import json,urllib.request,urllib.error\n"
              "try:\n r=urllib.request.urlopen(URL,timeout=5); data=r.read(1048577)\n"
              "except urllib.error.HTTPError as e: data=e.read(1048577)\n"
              "assert len(data)<=1048576\nprint(data.decode())").replace('URL',repr(f'http://127.0.0.1:{port}{path}'))
        return json.loads(self.command(['docker','exec',self.names[service],'python','-c',code],timeout=10))

    def readiness(self):
        value=self.get('luna',8092,'/health/ready')
        require(value.get('service') == 'dataieum-luna', 'unexpected_service')
        return value

    def read_state(self):
        data=private_read(self.state_path)
        state=json.loads(data) if data else {}
        require(isinstance(state,dict), 'invalid_recovery_state')
        for key in ('account_sha256','last_attempt_sha256'):
            require(key not in state or isinstance(state[key],str) and re.fullmatch('[0-9a-f]{64}',state[key]), 'invalid_recovery_state')
        return state

    def save_state(self, state):
        replace_private(self.state_path,json.dumps(state).encode(),self.uid,self.gid)

    def auth(self):
        return private_read(self.auth_path)

    def staged_path(self, name):
        require(isinstance(name, str) and
                re.fullmatch(r'\.server-login-[a-z0-9_-]{1,80}', name), 'invalid_server_session')
        folder = self.folder / name
        require(not folder.is_symlink() and folder.is_dir() and
                folder.stat().st_mode & 0o777 == 0o700 and
                (folder.stat().st_uid, folder.stat().st_gid) == (self.uid, self.gid),
                'unsafe_server_session')
        return folder / 'auth.json'

    def staged_auth(self, name):
        payload = private_read(self.staged_path(name))
        require(payload is not None, 'server_login_required')
        auth_info(payload, fresh=True)
        return payload

    def consume_staged_auth(self, name, payload):
        path = self.staged_path(name)
        require(private_read(path) == payload, 'server_session_changed')
        path.unlink()

    def pause(self):
        try:
            fd=os.open(self.pause_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
        except FileExistsError:
            raise RecoveryError('dispatcher_already_paused') from None
        owned=os.fstat(fd)
        try:
            with os.fdopen(fd,'wb') as stream:
                stream.write(self.marker)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            current=self.pause_path.lstat()
            if (current.st_dev,current.st_ino)==(owned.st_dev,owned.st_ino):
                self.pause_path.unlink()
            raise

    def is_paused(self):
        return self.pause_path.exists() or self.pause_path.is_symlink()

    def unpause(self):
        require(private_read(self.pause_path) == self.marker, 'pause_ownership_changed')
        self.pause_path.unlink()

    def drain(self):
        deadline=time.monotonic()+60
        while True:
            jobs=self.get('atlas',8000,'/health/metrics')['jobs']
            luna=self.get('luna',8092,'/health/metrics')
            if jobs['running']==0 and not jobs['dispatcher_owner'] and luna['provider']['active']==0 and not luna['related_suggestions']['active']:
                return
            require(time.monotonic()<deadline,'work_did_not_drain')
            time.sleep(1)

    def unchanged(self):
        after=self.inspect()
        for service in self.names:
            changes=configuration_changes(self.before[service],after[service])
            require(not changes,'configuration_changed_'+service+('_'+changes[0] if changes else ''))

    def stop(self):
        self.unchanged()
        self.command(['docker','stop','--time','35',self.before['luna']['Id']],timeout=45)
        self.unchanged()
        require(not self.inspect()['luna']['State']['Running'],'luna_not_stopped')

    def install(self, payload):
        self.unchanged()
        require(not self.inspect()['luna']['State']['Running'],'luna_not_stopped')
        replace_private(self.auth_path,payload,self.uid,self.gid)
        require(hmac.compare_digest(self.auth(),payload),'credential_write_mismatch')

    def start(self):
        self.unchanged()
        self.command(['docker','start',self.before['luna']['Id']],timeout=30)

    def wait_ready(self):
        deadline=time.monotonic()+100
        while time.monotonic()<deadline:
            try:
                if self.readiness().get('ready') is True:
                    return
            except RecoveryError:
                pass
            time.sleep(2)
        raise RecoveryError('readiness_not_restored')


@contextlib.contextmanager
def recovery_lock(project, directory=Path('/run/lock')):
    import fcntl
    path=directory/('dataieum-auth-recovery-'+project+'.lock')
    fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode),'unsafe_recovery_lock')
        try:
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RecoveryError('recovery_already_running') from None
        yield
    finally:
        os.close(fd)


def run_request(request):
    """Fixed JSON result only; never print traceback, exception text or tokens."""
    try:
        require(request.get('action') in ('check','enroll','recover','activate'),'invalid_action')
        project=request['project']
        require(isinstance(project,str) and re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,100}',project),'invalid_project')
        def interrupted(signum, frame):
            raise RecoveryError('interrupted')
        signal.signal(signal.SIGTERM,interrupted)
        signal.signal(signal.SIGINT,interrupted)
        manager=recovery_lock(project) if request['action']!='check' else contextlib.nullcontext()
        with manager:
            host=DockerHost(project)
            if request['action']=='enroll':
                return enroll(host)
            if request['action']=='activate':
                return activate_session(host,request.get('server_session'))
            payload=request.get('auth')
            payload=payload.encode() if isinstance(payload,str) else None
            return recover(host,payload,apply=request['action']=='recover')
    except RecoveryError as error:
        return dict(getattr(error,'result',{}),status='incomplete',code=error.code)
    except Exception:
        return {'status':'incomplete','code':'unexpected_host_error'}
