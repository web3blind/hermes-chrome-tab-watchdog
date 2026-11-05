import asyncio

import pytest

from tabwatch_plugin.control import fence_cleanup, request
from tabwatch_plugin.service import Service, CONTROL_RECOVERY_SECONDS
import tabwatch_plugin.service as module


@pytest.fixture
def owner(tmp_path, monkeypatch):
    now = [10000.0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: now[0])
    service = Service(tmp_path, port=0)
    service.store.clock = lambda: now[0]
    service.store.coverage('test-browser')
    service.store.observe([
        {'targetId': 'blank', 'type': 'page', 'url': 'about:blank'},
        {'targetId': 'site', 'type': 'page', 'url': 'https://example.test'},
    ])
    service.store.heartbeat()
    yield service, now
    service.store.close()
    service.lock.close()


def test_transient_control_recovers_only_after_bound_and_fresh_observation(owner):
    service, now = owner
    service.disable_cleanup('control_failure')
    assert service.recover_control() is False
    now[0] += CONTROL_RECOVERY_SECONDS
    # Stale observation must not unlock anything, even after the lease bound.
    assert service.recover_control() is False
    service.store.heartbeat()
    assert service.recover_control() is True
    assert service.store.candidates() == []
    service.store.heartbeat()
    assert service.store.candidates() == []  # Old blank age is not reused.
    now[0] += 1800
    service.store.heartbeat()
    assert service.store.candidates() == [('blank', 'blank')]
    assert service.cleanup_error is None
    now[0] += 6 * 3600 - 1800 - 1
    service.store.heartbeat()
    assert ('site', 'idle') not in service.store.candidates()
    now[0] += 1
    service.store.heartbeat()
    assert ('site', 'idle') in service.store.candidates()


def test_recovery_preserves_pending_activity_and_active_leases(owner):
    service, now = owner
    service.disable_cleanup('control_failure')
    now[0] += CONTROL_RECOVERY_SECONDS
    service.store.heartbeat()
    service.store.lease_start('active', 60)
    assert service.recover_control() is False
    service.store.lease_end('active')
    service.store.pending['site'] = 1
    assert service.recover_control() is False
    service.store.pending.clear()
    assert service.recover_control() is True


@pytest.mark.parametrize('cause', ['route_unobserved', 'settings_changed', 'ledger_failure'])
def test_recovery_never_accepts_a_later_external_fence(owner, cause):
    service, now = owner
    service.disable_cleanup('control_failure')
    fence_cleanup(service.socket_path.parent, reason=cause)
    now[0] += CONTROL_RECOVERY_SECONDS
    service.store.heartbeat()
    assert service.recover_control() is False
    assert service.store.disabled is True


def test_new_transient_fence_restarts_full_wait(owner):
    service, now = owner
    service.disable_cleanup('control_failure')
    now[0] += CONTROL_RECOVERY_SECONDS
    fence_cleanup(service.socket_path.parent, reason='lease_unavailable')
    service.store.heartbeat()
    assert service.recover_control() is False
    now[0] += CONTROL_RECOVERY_SECONDS - 1
    service.store.heartbeat()
    assert service.recover_control() is False
    now[0] += 1
    service.store.heartbeat()
    assert service.recover_control() is True


@pytest.mark.parametrize('cause', ['route_unobserved', 'settings_changed', 'ledger_failure'])
@pytest.mark.parametrize('external', [False, True])
def test_transient_failure_preserves_earlier_permanent_fence(owner, cause, external):
    service, now = owner
    if external:
        fence_cleanup(service.socket_path.parent, reason=cause)
    else:
        service.disable_cleanup('untracked_activity_gap', cause=cause)
    service.disable_cleanup('control_failure')
    now[0] += CONTROL_RECOVERY_SECONDS
    service.store.heartbeat()
    assert service.recover_control() is False
    assert service.store.disabled is True
    from tabwatch_plugin.control import gap_reason
    assert gap_reason(service.socket_path.parent) == cause


def test_other_faults_never_auto_recover(owner):
    service, now = owner
    for reason in ['ledger_failure', 'cleanup_failure', 'invalid_settings']:
        service.disable_cleanup(reason)
        now[0] += CONTROL_RECOVERY_SECONDS
        service.store.heartbeat()
        assert service.recover_control() is False
        assert service.store.disabled is True


def test_reload_with_gap_outwaits_old_owner_actions(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: now[0])
    fence_cleanup(tmp_path, reason='lease_unavailable')
    service = Service(tmp_path, port=0)
    try:
        service.store.clock = lambda: now[0] + 10000
        service.store.coverage('fixture')
        service.store.observe([{'targetId': 'blank', 'type': 'page', 'url': 'about:blank'}])
        # Old pages can be expired, but the old owner's lease bound still wins.
        service.store.db.execute('UPDATE pages SET first_seen=0')
        service.store.blank_grace_since = 0
        service.store.heartbeat()
        assert service.store.candidates() == []
        now[0] += CONTROL_RECOVERY_SECONDS - 1
        service.store.heartbeat()
        assert service.store.candidates() == []
        now[0] += 1
        service.store.heartbeat()
        assert service.store.candidates() == [('blank', 'blank')]
    finally:
        service.store.close()
        service.lock.close()


async def test_disconnected_dry_run_reader_does_not_disable_cleanup(tmp_path, monkeypatch):
    service = Service(tmp_path, port=0)
    async def quiet():
        await asyncio.Event().wait()
    async def slow_sweep(apply=False):
        await asyncio.sleep(.15)
        return {'candidates': [], 'closed': [], 'errors': []}
    monkeypatch.setattr(service, 'observe_forever', quiet)
    monkeypatch.setattr(service, 'sweep', slow_sweep)
    await service.start()
    try:
        service.store.coverage('fixture')
        service.store.heartbeat()
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(request, tmp_path, 'sweep', timeout=.02, apply=False)
        await asyncio.sleep(.25)
        assert service.cleanup_error is None
        assert not (tmp_path / 'cleanup.gap').exists()
    finally:
        await service.stop()
