"""Private local Unix control client. Contains no browser or Hermes internals."""
import json
import os
import socket
import stat
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def socket_address(path):
    """Linux fd-relative address avoids AF_UNIX's short pathname limit."""
    path = Path(path)
    if len(os.fsencode(path)) < 104:
        yield str(path)
        return
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        yield f'/proc/self/fd/{fd}/{path.name}'
    finally:
        os.close(fd)


def request(state_dir, command, *, timeout=5, **fields):
    path = Path(state_dir) / 'control.sock'
    info = path.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError('untrusted tab-watchdog control socket')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        deadline = time.monotonic() + timeout
        conn.settimeout(timeout)
        with socket_address(path) as address:
            conn.connect(address)
        conn.sendall((json.dumps(dict(cmd=command, **fields))+'\n').encode())
        data = bytearray()
        while b'\n' not in data:
            conn.settimeout(max(.001, deadline - time.monotonic()))
            if time.monotonic() >= deadline:
                raise TimeoutError('watchdog_control_deadline')
            part = conn.recv(65536)
            if not part:
                break
            data.extend(part)
            if len(data) > 1024 * 1024:
                raise RuntimeError('oversize watchdog response')
    value = json.loads(bytes(data).split(b'\n',1)[0])
    if not isinstance(value,dict):
        raise RuntimeError('invalid watchdog response')
    if value.get('ok') is False or value.get('error') or value.get('errors'):
        raise RuntimeError('tab-watchdog command rejected')
    if command in {'lease_start','lease_end'} and value.get('ok') is not True:
        raise RuntimeError('tab-watchdog lease was not acknowledged')
    return value


def fence_stamp(state_dir):
    try:
        info = (Path(state_dir) / 'cleanup.gap').stat()
        return info.st_ino, info.st_mtime_ns, info.st_size
    except FileNotFoundError:
        return None


GAP_REASONS = frozenset({
    'untracked_activity_gap', 'lease_unavailable', 'route_unobserved',
    'settings_changed', 'profile_mismatch', 'validation_failed',
    'owner_settings_conflict', 'owner_invalid_settings', 'invalid_settings',
    'browser_route_mismatch', 'ledger_failure', 'outgoing_telemetry_failure',
    'incoming_telemetry_failure', 'completion_telemetry_failure',
    'observer_telemetry_failure', 'cleanup_failure', 'health_telemetry_failure',
    'control_failure', 'unknown_gap_reason', 'legacy_gap_without_cause'})


def safe_reason(value):
    return value if isinstance(value, str) and value in GAP_REASONS else 'unknown_gap_reason'


def gap_reason(state_dir):
    try:
        with (Path(state_dir) / 'cleanup.gap').open('rb') as source:
            data = source.read(2049)
        return safe_reason(json.loads(data).get('reason')) if len(data) <= 2048 else 'unknown_gap_reason'
    except FileNotFoundError:
        return None
    except Exception:
        return 'legacy_gap_without_cause'


def fence_cleanup(state_dir, *, reason='untracked_activity_gap'):
    """Publish a durable gap before dispatching an unleased browser call.

    The owner checks this at every health/candidate/final close decision. Only a
    new owner/load can accept the marker, starting fresh coverage grace.
    """
    state = Path(state_dir)
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    name = None
    try:
        with tempfile.NamedTemporaryFile(dir=state, prefix='.gap-', delete=False) as out:
            name = out.name
            record = {'reason': safe_reason(reason), 'at': time.time(), 'pid': os.getpid()}
            out.write((json.dumps(record) + '\n').encode())
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, state / 'cleanup.gap')
        name = None
        fd = os.open(state, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if name is not None:
            Path(name).unlink(missing_ok=True)
