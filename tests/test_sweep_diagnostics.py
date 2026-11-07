"""Failed sweeps retain sanitized evidence without retrying uncertain closes."""
import json
import asyncio
import sqlite3

import pytest

from tabwatch_plugin.service import Service
from tabwatch_plugin.control import socket_address, request, fence_cleanup

SECRET = 'https://private.invalid/?token=synthetic-secret title typed-input'


@pytest.fixture
def owner(tmp_path):
    service = Service(tmp_path / 'state', port=0)
    now = [10000.0]
    service.store.clock = lambda: now[0]
    service.store.coverage('fixture')
    targets = [{'targetId': tid, 'type': 'page', 'url': f'https://example.test/{tid}'}
               for tid in ('a', 'b', 'c')]
    service.store.observe(targets)
    now[0] += 21601
    service.store.touch('c', 'tool')
    service.store.heartbeat()
    calls = []

    class FakeRPC:
        async def call(self, method, params=None):
            calls.append(method)
            if method == 'Target.closeTarget':
                targets[:] = [t for t in targets if t['targetId'] != params['targetId']]
                return {'success': True}
            return {'targetInfos': list(targets)}

    service.rpc = FakeRPC()

    async def version():
        return {'webSocketDebuggerUrl': 'ws://127.0.0.1/devtools/browser/fixture'}

    service.version = version
    yield service, calls, now
    service.store.close()
    service.lock.close()


@pytest.mark.asyncio
async def test_discovery_failure_retains_private_safe_diagnostic_and_fence(owner):
    service, calls, now = owner

    async def fail():
        raise ConnectionError(SECRET)

    service.version = fail
    result = await service.sweep(True)
    diagnostic = service.health()['last_cleanup_error']
    assert diagnostic == {
        'stage': 'discovery', 'kind': 'ConnectionError', 'method': None,
        'progress': 'not_attempted', 'reason': 'close_failed', 'at': diagnostic['at']}
    assert diagnostic['at'] > 0
    assert result['errors'][0]['diagnostic'] == diagnostic
    assert result['closed'] == []
    assert calls == []
    assert service.cleanup_error == 'cleanup_failure'
    now[0] += 2000
    service.store.heartbeat()
    assert not service.recover_control()
    assert (await service.sweep(True))['errors'] == [{'reason': 'cleanup_disabled'}]
    assert calls == []
    assert SECRET not in json.dumps(result)
    assert SECRET not in (service.socket_path.parent / 'cleanup.gap').read_text()
    state = service.socket_path.parent
    service.store.close()
    service.lock.close()
    reloaded = Service(state, port=0)
    try:
        assert reloaded.health()['last_cleanup_error'] == diagnostic
    finally:
        reloaded.store.close()
        reloaded.lock.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault,stage,progress,kind,method,reason,closed', [
    ('planning', 'planning', 'not_attempted', 'DatabaseError', None, 'close_failed', []),
    ('inventory', 'inventory', 'not_attempted', 'ConnectionError', 'Target.getTargets', 'close_failed', []),
    ('timeout', 'close', 'outcome_unknown', 'TimeoutError', 'Target.closeTarget', 'close_failed', []),
    ('refused', 'close', 'acknowledged', 'RuntimeError', 'Target.closeTarget', 'close_refused', []),
    ('verification', 'verification', 'acknowledged', 'ConnectionError', 'Target.getTargets', 'close_failed', []),
    ('unverified', 'verification', 'acknowledged', 'RuntimeError', 'Target.getTargets', 'close_unverified', []),
    ('ledger', 'post_close_ledger', 'disappearance_verified', 'DatabaseError', None, 'close_failed', ['a']),
])
async def test_failure_stage_and_close_progress(owner, monkeypatch, fault, stage,
                                               progress, kind, method, reason, closed):
    service, calls, now = owner
    original = service.rpc.call
    close_attempts = []
    attempted = False

    async def call(command, params=None):
        nonlocal attempted
        if command == 'Target.closeTarget':
            attempted = True
            close_attempts.append(params['targetId'])
            if fault == 'timeout':
                exc = TimeoutError(SECRET)
                exc.cdp_method = SECRET  # Never expose arbitrary attributes.
                raise exc
            if fault == 'refused':
                return {'success': False}
            if fault == 'unverified':
                return {'success': True}
        elif fault == 'inventory' or (fault == 'verification' and attempted):
            raise ConnectionError(SECRET)
        return await original(command, params)

    service.rpc.call = call
    observe = service.store.observe

    def fail_ledger(targets):
        if fault == 'ledger' and attempted:
            raise sqlite3.OperationalError(SECRET)
        observe(targets)

    service.store.observe = fail_ledger
    if fault == 'planning':
        def fail():
            raise sqlite3.OperationalError(SECRET)
        monkeypatch.setattr(service.store, 'candidates', fail)
    if fault == 'unverified':
        async def no_wait(_):
            pass
        monkeypatch.setattr(asyncio, 'sleep', no_wait)
    result = await service.sweep(True)
    diagnostic = result['errors'][0]['diagnostic']
    assert diagnostic == dict(stage=stage, progress=progress, kind=kind, method=method,
                              reason=reason, at=diagnostic['at'])
    assert result['errors'][0]['reason'] == reason
    assert result['closed'] == closed
    assert service.health()['last_cleanup_error'] == diagnostic
    assert service.cleanup_error == 'cleanup_failure'
    assert len(close_attempts) <= 1
    before = list(close_attempts)
    now[0] += 2000
    service.store.heartbeat()
    assert not service.recover_control()
    assert (await service.sweep(True))['closed'] == []
    assert close_attempts == before
    assert SECRET not in json.dumps(result)
    assert SECRET not in (service.socket_path.parent / 'cleanup.gap').read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['discovery', 'ledger'])
async def test_partial_close_receipt_survives_real_control_client_and_cli(owner, capsys, fault):
    import tabwatch_plugin.cli as cli
    service, calls, _ = owner
    observe = service.store.observe

    def fresh(targets):
        if fault == 'ledger' and calls.count('Target.closeTarget') == 1:
            raise sqlite3.OperationalError(SECRET)
        observe(targets)
        service.store.heartbeat()  # Simulated independent observer completion.

    service.store.observe = fresh
    version = service.version
    attempts = 0

    async def fail_second():
        nonlocal attempts
        attempts += 1
        if fault == 'discovery' and attempts == 2:
            service.store.lease_start('inflight-browser-call', 1900)
            raise ConnectionError(SECRET)
        return await version()

    service.version = fail_second
    with socket_address(service.socket_path) as address:
        server = await asyncio.start_unix_server(service.control_client, path=address)
    try:
        state = service.socket_path.parent
        assert await asyncio.to_thread(request, state, 'lease_start', call_id='probe') == {'ok': True}
        assert service.health()['active_leases'] == 1
        assert await asyncio.to_thread(request, state, 'lease_end', call_id='probe') == {'ok': True}
        assert await asyncio.to_thread(cli.main, ['--apply', '--state-dir', str(service.socket_path.parent)]) == 1
        result = json.loads(capsys.readouterr().out)
        assert result['closed'] == ['a']
        assert result['errors'][0]['target_id'] == ('b' if fault == 'discovery' else 'a')
        assert result['errors'][0]['diagnostic']['progress'] == (
            'not_attempted' if fault == 'discovery' else 'disappearance_verified')
        assert calls.count('Target.closeTarget') == 1
        assert service.health()['active_leases'] == (1 if fault == 'discovery' else 0)
        assert service.last_control_error is None
        assert service.cleanup_error == 'cleanup_failure'
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_exception_name_and_attributes_cannot_leak_and_later_fence_retains_incident(owner):
    service, _, _ = owner
    private_exception = type(SECRET, (Exception,), {})

    async def fail():
        exc = private_exception(SECRET)
        exc.cdp_method = SECRET
        raise exc

    service.version = fail
    result = await service.sweep(True)
    diagnostic = service.health()['last_cleanup_error']
    assert diagnostic['kind'] == 'Exception'
    assert diagnostic['method'] is None
    fence_cleanup(service.socket_path.parent, reason='route_unobserved')
    from tabwatch_plugin.control import cleanup_diagnostic
    assert cleanup_diagnostic(service.socket_path.parent) == diagnostic
    assert SECRET not in json.dumps(result)
    assert SECRET not in (service.socket_path.parent / 'cleanup.gap').read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize('command,reply', [
    ('lease_start', {}), ('lease_end', {'ok': False}),
    ('lease_start', {'ok': True, 'errors': [{'reason': 'close_failed'}]}),
    ('health', {'error': 'invalid_request'}),
    ('sweep', {'error': 'invalid_request'}),
    ('sweep', {'candidates': [], 'closed': ['a'], 'errors': 'not-a-list'}),
    ('sweep', {'candidates': [], 'closed': ['a'], 'errors': [{'reason': SECRET}]}),
    ('sweep', {'candidates': [], 'closed': [False], 'errors': []}),
    ('sweep', {'candidates': [], 'closed': [], 'errors': [{'reason': 'close_failed', 'diagnostic': None}]}),
])
async def test_control_still_rejects_invalid_receipts_and_lease_acknowledgements(owner, command, reply):
    service, _, _ = owner

    async def respond(reader, writer):
        await reader.readline()
        writer.write(json.dumps(reply).encode() + b'\n')
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    with socket_address(service.socket_path) as address:
        server = await asyncio.start_unix_server(respond, path=address)
    try:
        with pytest.raises(RuntimeError):
            await asyncio.to_thread(request, service.socket_path.parent, command)
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.parametrize('field,value,retained', [
    ('method', [SECRET], True), ('method', SECRET, True),
    ('kind', SECRET, False), ('stage', [SECRET], False),
    ('progress', SECRET, False), ('at', float('nan'), False),
    ('at', True, False), ('extra', SECRET, True),
])
def test_persisted_diagnostic_is_bounded_and_revalidated(owner, field, value, retained):
    from tabwatch_plugin.control import cleanup_diagnostic
    service, _, _ = owner
    diagnostic = dict(stage='discovery', kind='ConnectionError', progress='not_attempted',
                      reason='close_failed', method=None, at=10000.0)
    raw = dict(diagnostic, **{field: value})
    marker = service.socket_path.parent / 'cleanup.gap'
    marker.write_text(json.dumps({'reason': 'cleanup_failure', 'last_cleanup_error': raw}))
    assert cleanup_diagnostic(marker.parent) == (diagnostic if retained else None)
    marker.write_text(' ' * 2049)
    assert cleanup_diagnostic(marker.parent) is None


@pytest.mark.asyncio
async def test_failed_diagnostic_persistence_keeps_memory_and_permanent_fence(owner, monkeypatch):
    import tabwatch_plugin.service as module
    service, calls, _ = owner

    async def fail():
        raise TimeoutError(SECRET)

    def cannot_persist(*args, **kwargs):
        raise OSError(SECRET)

    service.version = fail
    monkeypatch.setattr(module, 'fence_cleanup', cannot_persist)
    result = await service.sweep(True)
    assert result['errors'][0]['diagnostic'] == service.health()['last_cleanup_error']
    assert service.cleanup_error == 'cleanup_failure'
    assert service.store.disabled
    assert not service.recover_control()
    assert (await service.sweep(True))['closed'] == []
    assert calls == []
