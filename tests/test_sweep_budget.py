"""Sweep admission is time-bounded, not target-count bounded."""
import asyncio
import json
import time
from types import SimpleNamespace

import pytest

from tabwatch_plugin import cli
from tabwatch_plugin.control import request, socket_address
from tabwatch_plugin.service import Service


@pytest.fixture
def sweep_owner(tmp_path):
    service = Service(tmp_path / 'state', port=0)
    now = [10000.0]
    service.store.clock = lambda: now[0]
    service.store.coverage('fixture')
    targets = [{'targetId': str(i), 'type': 'page', 'url': f'https://example.test/{i}'}
               for i in range(9)]
    service.store.observe(targets)
    now[0] += 21601
    service.store.touch('8', 'tool')
    service.store.heartbeat()
    calls = []

    class FakeRPC:
        async def call(self, method, params=None):
            calls.append((method, params))
            if method == 'Target.closeTarget':
                targets[:] = [t for t in targets if t['targetId'] != params['targetId']]
                return {'success': True}
            return {'targetInfos': list(targets)}

    service.rpc = FakeRPC()

    async def version():
        return {'webSocketDebuggerUrl': 'ws://127.0.0.1/devtools/browser/fixture'}

    service.version = version
    yield service, calls, targets
    service.store.close()
    service.lock.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('expires_at,expected', [
    ('planning', []), ('discovery', []), ('inventory', []), ('revalidation', []),
    ('close', ['0']), ('verification', ['0']),
])
async def test_deadline_stops_admission_but_finishes_sent_close(
        sweep_owner, monkeypatch, expires_at, expected):
    import tabwatch_plugin.service as module
    service, calls, targets = sweep_owner
    monotonic = [100.0]
    # Never patch the shared time module: asyncio keeps its real clock.
    monkeypatch.setattr(module, 'time', SimpleNamespace(
        monotonic=lambda: monotonic[0], time=time.time))
    original_version, original_call = service.version, service.rpc.call
    original_candidates = service.store.candidates
    attempts = []
    candidate_calls = 0
    sent = False

    async def version():
        attempts.append('discovery')
        if expires_at == 'discovery':
            monotonic[0] += 60
        return await original_version()

    async def call(method, params=None):
        nonlocal sent
        if method == 'Target.closeTarget':
            sent = True
            if expires_at == 'close':
                monotonic[0] += 60
        elif expires_at == ('verification' if sent else 'inventory'):
            monotonic[0] += 60
        return await original_call(method, params)

    def candidates():
        nonlocal candidate_calls
        candidate_calls += 1
        result = original_candidates()
        if (expires_at == 'planning' and candidate_calls == 1 or
                expires_at == 'revalidation' and candidate_calls == 2):
            monotonic[0] += 60
        return result

    service.version, service.rpc.call = version, call
    service.store.candidates = candidates
    receipt = await service.sweep(True)
    assert receipt['closed'] == expected
    assert receipt['errors'] == []
    assert len(attempts) == (0 if expires_at == 'planning' else 1)
    assert [params['targetId'] for method, params in calls
            if method == 'Target.closeTarget'] == expected
    assert service.cleanup_error is service.last_cleanup_error is None
    assert not (service.socket_path.parent / 'cleanup.gap').exists()
    if expected:
        assert not any(t['targetId'] == '0' for t in targets)
        assert calls[-1][0] == 'Target.getTargets'  # Verified even beyond deadline.


@pytest.mark.asyncio
@pytest.mark.parametrize('when', ['discovery', 'after_first_close'])
@pytest.mark.parametrize('guard', ['lease', 'pending', 'fence', 'observation'])
async def test_new_global_work_or_lost_observation_stops_scanning(
        sweep_owner, monkeypatch, when, guard):
    import tabwatch_plugin.service as module
    from tabwatch_plugin.control import fence_cleanup
    service, calls, _ = sweep_owner
    version, call = service.version, service.rpc.call
    discoveries = []

    async def no_wait(_):
        pass

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(sleep=no_wait))

    def interrupt():
        if guard == 'lease':
            service.store.lease_start('new-browser-work', 1900)
        elif guard == 'pending':
            service.store.pending['*'] = 1
        elif guard == 'fence':
            fence_cleanup(service.socket_path.parent, reason='route_unobserved')
        else:
            service.store.observer_missing.add('8')

    async def discover():
        discoveries.append(True)
        if when == 'discovery':
            interrupt()
        return await version()

    async def invoke(method, params=None):
        result = await call(method, params)
        if method == 'Target.closeTarget' and when == 'after_first_close':
            interrupt()
        return result

    service.version, service.rpc.call = discover, invoke
    receipt = await service.sweep(True)
    assert receipt['closed'] == ([] if when == 'discovery' else ['0'])
    assert receipt['errors'] == []
    assert len(discoveries) == 1  # Stop, rather than re-scan every remaining candidate.
    assert service.cleanup_error is None


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['timeout', 'unverified'])
async def test_expiry_never_hides_uncertain_close(sweep_owner, monkeypatch, fault):
    import tabwatch_plugin.service as module
    service, calls, _ = sweep_owner
    monotonic = [0.0]
    monkeypatch.setattr(module, 'time', SimpleNamespace(
        monotonic=lambda: monotonic[0], time=time.time))
    original = service.rpc.call

    async def no_wait(_):
        pass

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(sleep=no_wait))

    async def call(method, params=None):
        if method == 'Target.closeTarget':
            calls.append((method, params))
            monotonic[0] = 60.0
            if fault == 'timeout':
                raise TimeoutError('synthetic uncertain close')
            return {'success': True}  # No disappearance; verification must still run.
        return await original(method, params)

    service.rpc.call = call
    receipt = await service.sweep(True)
    assert receipt['closed'] == []
    assert len(receipt['errors']) == 1
    assert receipt['errors'][0]['reason'] == ('close_failed' if fault == 'timeout' else 'close_unverified')
    assert service.cleanup_error == 'cleanup_failure'
    assert sum(method == 'Target.closeTarget' for method, _ in calls) == 1
    before = len(calls)
    assert (await service.sweep(True))['errors'] == [{'reason': 'cleanup_disabled'}]
    assert len(calls) == before


@pytest.mark.parametrize('field', ['candidates', 'closed', 'errors'])
def test_large_receipts_still_validate_every_entry(field):
    from tabwatch_plugin.control import valid_sweep_result
    receipt = dict(candidates=[{'target_id': str(i), 'reason': 'idle'} for i in range(8)],
                   closed=[str(i) for i in range(8)], errors=[])
    assert valid_sweep_result(receipt)
    if field == 'candidates':
        receipt[field][-1]['reason'] = 'invalid'
    elif field == 'closed':
        receipt[field][-1] = False
    else:
        receipt[field] = [{'reason': 'close_failed'}] * 6
    assert not valid_sweep_result(receipt)


@pytest.mark.asyncio
async def test_large_receipt_still_has_one_megabyte_control_frame_bound(sweep_owner):
    service, _, _ = sweep_owner

    async def respond(reader, writer):
        await reader.readline()
        writer.write(json.dumps(dict(candidates=[], closed=['t' * 256] * 5000,
                                     errors=[])).encode() + b'\n')
        try:
            await writer.drain()
        except ConnectionError:
            pass  # Client rejects the frame without consuming it all.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    with socket_address(service.socket_path) as address:
        server = await asyncio.start_unix_server(respond, path=address)
    try:
        with pytest.raises(RuntimeError, match='oversize watchdog response'):
            await asyncio.to_thread(request, service.socket_path.parent, 'sweep')
    finally:
        server.close()
        await server.wait_closed()


def test_cli_allows_budget_plus_bounded_close_verification(sweep_owner, monkeypatch):
    service, _, _ = sweep_owner
    timeouts = []

    def control(state, command, *, timeout=5, **fields):
        if command == 'health':
            return {'healthy': True}
        timeouts.append(timeout)
        return {'candidates': [], 'closed': [], 'errors': []}

    monkeypatch.setattr(cli, 'request', control)
    assert cli.main(['--apply', '--state-dir', str(service.socket_path.parent)]) == 0
    assert timeouts == [180]


@pytest.mark.asyncio
async def test_more_than_five_actual_closes_through_control_and_cli(sweep_owner, capsys):
    service, calls, targets = sweep_owner
    with socket_address(service.socket_path) as address:
        server = await asyncio.start_unix_server(service.control_client, path=address)
    try:
        state = service.socket_path.parent
        dry = await asyncio.to_thread(request, state, 'sweep', apply=False)
        assert len(dry['candidates']) == 8
        assert dry['closed'] == dry['errors'] == []
        assert await asyncio.to_thread(cli.main, ['--apply', '--state-dir', str(state)]) == 0
        receipt = json.loads(capsys.readouterr().out)
        assert receipt == dict(candidates=dry['candidates'], closed=list('01234567'), errors=[])
        assert [t['targetId'] for t in targets] == ['8']
        assert sum(method == 'Target.closeTarget' for method, _ in calls) == 8
        assert service.cleanup_error is None
    finally:
        server.close()
        await server.wait_closed()
