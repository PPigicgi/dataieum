"""Plan and apply bounded cleanup; never prune volumes or running containers."""
import argparse
from collections import defaultdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def command(*args, check=True):
    return subprocess.run(args, capture_output=True, text=True, check=check, timeout=240)


def containers():
    ids = command('docker', 'ps', '-aq').stdout.split()
    return json.loads(command('docker', 'inspect', *ids).stdout) if ids else []


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def identity(path):
    s = Path(path).stat()
    return dict(device=s.st_dev, inode=s.st_ino, size=s.st_size,
                mtime_ns=s.st_mtime_ns, allocated=s.st_blocks * 512, links=s.st_nlink)


def related(a, b):
    a, b = Path(a).resolve(), Path(b).resolve()
    return a == b or a in b.parents or b in a.parents


def selected_files(paths, mounts):
    result = []
    for name in paths:
        p = Path(name)
        if p.is_symlink() or not p.is_file():
            raise ValueError(f'Expected a regular obsolete file: {p}')
        if any(related(p, mount) for mount in mounts):
            raise ValueError(f'File overlaps a protected mount: {p}')
        result.append({'path': str(p.resolve()), 'identity': identity(p)})
    return result


def fully_reclaimable(files):
    groups = defaultdict(list)
    for f in files:
        s = f['identity']
        groups[s['device'], s['inode']].append(s)
    return sum(v[0]['allocated'] for v in groups.values() if len(v) == v[0]['links'])


def check_file_readers(files):
    targets = {(f['identity']['device'], f['identity']['inode']): f['path'] for f in files}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            for fd in (proc / 'fd').iterdir():
                try:
                    s = fd.stat()
                    if (s.st_dev, s.st_ino) in targets:
                        raise RuntimeError(f'Cleanup file is open in PID {proc.name}')
                except FileNotFoundError:
                    pass
            # mmap readers can remain after closing their file descriptor.
            for line in (proc / 'maps').read_text().splitlines():
                bits = line.split(None, 5)
                if len(bits) < 5:
                    continue
                major, minor = (int(v, 16) for v in bits[3].split(':'))
                if (os.makedev(major, minor), int(bits[4])) in targets:
                    raise RuntimeError(f'Cleanup file is mapped in PID {proc.name}')
        except FileNotFoundError:
            pass


def image_rows():
    return [json.loads(x) for x in command('docker', 'image', 'ls', '--no-trunc',
            '--format', '{{json .}}').stdout.splitlines()]


def removable(c):
    prefixes = ('dataieum-keyword-', 'dataieum-catalog-', 'dataieum-audit-',
                'dataieum-coverage-', 'dataieum-vector-evaluation-', 'dataieum-topic-')
    return c['Name'].lstrip('/').startswith(prefixes) and c['State']['Status'] == 'exited'


def make_plan(active, rollback, obsolete, recovery_images=None):
    configs = {str(Path(p).resolve()): digest(p) for p in [active, rollback]}
    scheduled = Path('/opt/wanted/operations/refresh-images.json')
    if scheduled.exists():
        configs[str(scheduled)] = digest(scheduled)
    maintenance = Path('/opt/wanted/refresh/maintenance-current.json')
    if maintenance.exists():
        root = Path(json.loads(maintenance.read_text())['root'])
        for name in ['compose.before.json', 'compose.updated.json']:
            path = root/name
            if path.exists():configs[str(path)] = digest(path)
    cs = containers()
    recovery_images = recovery_images or {}
    protected_images = set()
    for c in cs:
        if removable(c):
            continue
        image = recovery_images.get(c['Image'], c['Image'])
        # A running container can outlive its original image. Fail before deletion
        # unless an explicitly captured recovery image has been supplied.
        protected_images.add(json.loads(command('docker', 'image', 'inspect', image).stdout)[0]['Id'])
    mounts = {m['Source'] for c in cs if not removable(c) for m in c.get('Mounts', [])}
    for path in configs:
        config = json.loads(Path(path).read_text())
        for svc in config['services'].values():
            protected_images.add(json.loads(command('docker', 'image', 'inspect', svc['image']).stdout)[0]['Id'])
            for m in svc.get('volumes', []):
                if isinstance(m, dict) and str(m.get('source', '')).startswith('/'):
                    mounts.add(m['source'])
    rows = image_rows()
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['ID']].append(row)
    candidates = []
    for ident, versions in grouped.items():
        repos = {v['Repository'] for v in versions}
        owned=lambda r:r.startswith('dataieum-') or r in {
            'ghcr.io/manzigit/wanted-'+role for role in ('atlas','vector','luna','refresh')}
        if ident in protected_images or not all(owned(r) for r in repos):
            continue
        candidates.append({'id': ident, 'tags': sorted({v['Repository'] + ':' + v['Tag'] for v in versions})})
    files = selected_files(obsolete, mounts)
    check_file_readers(files)
    return {'created_at': time.time(), 'configs': configs, 'recovery_images': recovery_images,
            'protected_images': sorted(protected_images),
            'running': {c['Id']: {'name': c['Name'], 'image': c['Image'],
                        'started_at': c['State']['StartedAt']} for c in cs if c['State']['Running']},
            'containers': [{'id': c['Id'], 'name': c['Name'], 'exit_code': c['State']['ExitCode']}
                           for c in cs if removable(c)],
            'images': candidates, 'files': files,
            'file_reclaimable_bytes': fully_reclaimable(files),
            'disk_before': shutil.disk_usage('/')._asdict(),
            'counts_before': {'images': len(grouped), 'containers': len(cs)}}


def unchanged(plan):
    for path, sha in plan['configs'].items():
        if digest(path) != sha:
            raise RuntimeError('Deployment configuration changed during cleanup')
    cs = containers()
    live = {c['Id']: {'name': c['Name'], 'image': c['Image'], 'started_at': c['State']['StartedAt']}
            for c in cs if c['State']['Running']}
    if live != plan['running']:
        raise RuntimeError('Running containers changed during cleanup')
    return cs


def verify(plan, report, report_path, recovery_images=None):
    cs = unchanged(plan)
    replacements = recovery_images or plan.get('recovery_images', {})
    protected = {replacements.get(i, i) for i in plan['protected_images']}
    # Inspect every protected image directly, including explicitly captured recovery images.
    for ident in protected:
        if command('docker', 'image', 'inspect', '--format', '{{.Id}}', ident).stdout.strip() != ident:
            raise RuntimeError('Protected image identity differs')
    for item in plan['files']:
        if Path(item['path']).exists():
            raise RuntimeError('Obsolete file still exists')
    rows = image_rows()
    remaining = {r['ID'] for r in rows}
    if not protected <= remaining:
        raise RuntimeError('Protected image missing after cleanup')
    if any(c['Id'] in {x['id'] for x in plan['containers']} for c in cs):
        raise RuntimeError('A selected stopped container still exists')
    if report.get('error'):
        report['repaired_verification_error'] = report.pop('error')
    report.update(stage='complete', completed_at=time.time(),
                  images_removed=len({v['id'] for v in plan['images']} - remaining),
                  counts_after={'images': len(remaining), 'containers': len(cs)},
                  running_unchanged=True, protected_images_present=True,
                  protected_images=sorted(protected), recovery_images=replacements,
                  disk_after=shutil.disk_usage('/')._asdict())
    tmp = report_path.with_suffix('.tmp')
    tmp.write_text(json.dumps(report, indent=2)); tmp.replace(report_path)
    return report


def apply(plan, report_path):
    report = {'stage': 'running', 'containers_removed': [], 'tags_removed': [],
              'files_removed': [], 'skipped': [], 'disk_before': plan['disk_before']}
    def save():
        tmp = report_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(report, indent=2))
        tmp.replace(report_path)
    save()
    try:
        unchanged(plan)
        check_file_readers(plan['files'])
        for item in plan['containers']:
            cs = unchanged(plan)
            current = next((c for c in cs if c['Id'] == item['id']), None)
            if current is None:
                continue
            if not removable(current):
                raise RuntimeError('Container is no longer eligible for cleanup')
            command('docker', 'container', 'rm', item['id'])  # Deliberately no force or volumes.
            report['containers_removed'].append(item['name']); save()
        for item in plan['images']:
            unchanged(plan)
            if item['id'] in plan['protected_images']:
                raise RuntimeError('Refusing to remove a protected image')
            for tag in item['tags']:
                current = command('docker', 'image', 'inspect', '--format', '{{.Id}}', tag, check=False)
                if current.returncode or current.stdout.strip() != item['id']:
                    report['skipped'].append({'tag': tag, 'reason': 'tag changed or missing'}); continue
                result = command('docker', 'image', 'rm', tag, check=False)
                if result.returncode:
                    report['skipped'].append({'tag': tag, 'reason': result.stderr[:250]})
                else:
                    report['tags_removed'].append(tag)
            save()
        unchanged(plan)
        check_file_readers(plan['files'])
        for item in plan['files']:
            p = Path(item['path'])
            current = identity(p)
            # Removing another hard link deliberately changes link count, but not contents.
            for field in ['device', 'inode', 'size', 'mtime_ns']:
                if current[field] != item['identity'][field]:
                    raise RuntimeError('Obsolete file changed during cleanup')
            p.unlink()
            report['files_removed'].append(str(p)); save()
        unchanged(plan)
        # Only unused old cache, keeping recent build acceleration. Never prune volumes.
        result = command('docker', 'builder', 'prune', '--force', '--filter', 'until=168h',
                         '--keep-storage', '512MB', check=False)
        report['build_cache'] = {'exit_code': result.returncode, 'result': result.stdout[-700:]}
        verify(plan, report, report_path)
    except BaseException as error:
        report.update(stage='failed', error=str(error)); save(); raise
    save()
    return report


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--active')
    p.add_argument('--rollback')
    p.add_argument('--obsolete-file', action='append', default=[])
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--apply-plan', type=Path)
    p.add_argument('--verify-plan', type=Path)
    p.add_argument('--recovery-map', type=Path)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'cleanup.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        recovery_images = json.loads(args.recovery_map.read_text()) if args.recovery_map else {}
        if args.verify_plan:
            report_path = args.output / 'report.json'
            report = verify(json.loads(args.verify_plan.read_text()), json.loads(report_path.read_text()),
                            report_path, recovery_images)
            print(json.dumps({k: report[k] for k in ['stage', 'images_removed', 'counts_after', 'disk_after']}), flush=True)
        elif args.apply_plan:
            report = apply(json.loads(args.apply_plan.read_text()), args.output / 'report.json')
            print(json.dumps(report), flush=True)
        else:
            if not args.active or not args.rollback:
                p.error('--active and --rollback are required for planning')
            plan = make_plan(args.active, args.rollback, args.obsolete_file, recovery_images)
            (args.output / 'plan.json').write_text(json.dumps(plan, indent=2))
            print(json.dumps({'containers': len(plan['containers']), 'images': len(plan['images']),
                              'files': len(plan['files']), 'file_reclaimable_bytes': plan['file_reclaimable_bytes'],
                              'protected_images': len(plan['protected_images'])}), flush=True)


if __name__ == '__main__':
    main()
