import asyncio
import contextvars
import os
from pathlib import Path
import socket
import subprocess
import time

import pytest

import tabwatch_plugin as plugin
from tabwatch_plugin.control import request, fence_cleanup
from tabwatch_plugin.policy import DEFAULTS, Policy
from tabwatch_plugin.runtime import Runtime
from tabwatch_plugin.service import Service


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def settings():
    return dict(DEFAULTS, upstream=f'http://127.0.0.1:{free_port()}', proxy_port=free_port())


def until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.02)
    raise AssertionError('test deadline')


def test_lock_election_before_store_and_nonowner_unload(tmp_path, monkeypatch):
    import tabwatch_plugin.service as module
    async def quiet_observer(self):
        await asyncio.Event().wait()
    # Isolate ownership from legitimate reconnect gaps on the absent upstream.
    monkeypatch.setattr(module.Service, 'observe_forever', quiet_observer)
    one = Runtime(tmp_path, settings(), Policy(), None)
    one.start()
    two = None
    try:
        assert one.ready.wait(5) and one.status == 'owner'
        socket_path = one.state / 'control.sock'
        stamp = socket_path.stat().st_ino
        before = request(one.state, 'health')['coverage_since']
        original = module.Store
        monkeypatch.setattr(module, 'Store', lambda *a, **kw: pytest.fail('nonowner opened Store'))
        two = Runtime(tmp_path, one.settings, Policy(), None)
        two.start()
        assert two.ready.wait(5) and two.status == 'attached'
        two.stop()
        assert socket_path.stat().st_ino == stamp
        assert request(one.state, 'health')['coverage_since'] == before
        monkeypatch.setattr(module, 'Store', original)
    finally:
        if two:
            two.stop()
        one.stop()
    assert not socket_path.exists() and not one.thread.is_alive()
    replacement = Runtime(tmp_path, one.settings, Policy(), None)
    replacement.start()
    try:
        assert replacement.ready.wait(5) and replacement.status == 'owner'
    finally:
        replacement.stop()


def test_standby_takes_over_after_owner_unload(tmp_path):
    one = Runtime(tmp_path, settings(), Policy(), None)
    two = Runtime(tmp_path, one.settings, Policy(), None)
    one.start()
    try:
        assert one.ready.wait(5)
        two.start()
        assert two.ready.wait(5) and two.status == 'attached'
        one.stop()
        until(lambda: two.status == 'owner')
        assert request(two.state, 'health')['runtime_pid'] == os.getpid()
    finally:
        one.stop()
        two.stop()


@pytest.mark.parametrize('change', ['invalid_settings', 'browser_route_mismatch', 'valid_conflict'])
def test_attached_settings_conflict_fences_owner(tmp_path, change):
    first = Runtime(tmp_path, settings(), Policy(), None)
    first.start()
    second = None
    try:
        assert first.ready.wait(5) and first.status == 'owner'
        values = dict(first.settings)
        policy = Policy(idle_hours=7) if change == 'valid_conflict' else Policy()
        second = Runtime(tmp_path, values, policy, None if change == 'valid_conflict' else change)
        second.start()
        assert second.ready.wait(5) and second.status == 'attached'
        assert (first.state / 'cleanup.gap').exists()
        assert request(first.state, 'health')['healthy'] is False
        assert first.service.store.disabled
    finally:
        if second:
            second.stop()
        first.stop()


def test_conflicting_contender_fences_replacement_owner_same_pid(tmp_path):
    first = Runtime(tmp_path, settings(), Policy(), None)
    first.start()
    second = replacement = None
    try:
        assert first.ready.wait(5)
        original = request(first.state, 'health')
        second = Runtime(tmp_path, first.settings, Policy(idle_hours=7), None)
        second.start()
        assert second.ready.wait(5) and second.status == 'attached'
        assert first.service.store.disabled
        first.stop()
        replacement = Runtime(tmp_path, first.settings, Policy(), None)
        replacement.start()
        assert replacement.ready.wait(5) and replacement.status == 'owner'
        after = request(replacement.state, 'health')
        assert after['runtime_pid'] == original['runtime_pid']
        assert after['runtime_incarnation'] != original['runtime_incarnation']
        until(lambda: replacement.service.store.disabled, timeout=5)
        assert request(replacement.state, 'health')['healthy'] is False
    finally:
        if second:
            second.stop()
        if replacement:
            replacement.stop()
        first.stop()


def test_context_copied_and_start_never_spawns_process(tmp_path, monkeypatch):
    from hermes_constants import get_hermes_home
    marker = contextvars.ContextVar('proof')
    marker.set('captured')
    seen = []
    original = Service.start

    async def start(service):
        seen.append((marker.get(), get_hermes_home()))
        await original(service)

    monkeypatch.setattr(Service, 'start', start)
    monkeypatch.setattr(subprocess, 'Popen', lambda *a, **kw: pytest.fail('child process'))
    expected = get_hermes_home()
    runtime = Runtime(tmp_path, settings(), Policy(), None)
    runtime.start()
    marker.set('changed')
    try:
        assert runtime.ready.wait(5) and runtime.status == 'owner'
        assert seen == [('captured', expected)]
        assert runtime.thread.daemon
    finally:
        runtime.stop()


def test_partial_start_failure_releases_socket_lock_loop_and_store(tmp_path, monkeypatch):
    original = Service.start
    held = []

    async def fail(service):
        await original(service)
        held.append(service)
        raise RuntimeError('fixture after listeners')

    monkeypatch.setattr(Service, 'start', fail)
    runtime = Runtime(tmp_path, settings(), Policy(), None)
    runtime.start()
    assert runtime.ready.wait(5)
    runtime.stop()
    assert runtime.status == 'startup_failed'
    assert not runtime.thread.is_alive() and runtime.loop is None
    assert not (runtime.state / 'control.sock').exists()
    assert held[0].lock.closed
    with pytest.raises(Exception):
        held[0].store.db.execute('select 1')
    monkeypatch.setattr(Service, 'start', original)
    retry = Runtime(tmp_path, runtime.settings, Policy(), None)
    retry.start()
    try:
        assert retry.ready.wait(5) and retry.status == 'owner'
    finally:
        retry.stop()


def test_immediate_unload_and_reload_has_no_thread_leaks(tmp_path):
    for _ in range(10):
        runtime = Runtime(tmp_path, settings(), Policy(), None)
        runtime.start()
        runtime.stop()
        assert not runtime.thread.is_alive() and runtime.loop is None


def test_durable_gap_fences_existing_owner(tmp_path):
    service = Service(tmp_path, port=0)
    try:
        service.store.coverage('fixture')
        service.store.heartbeat()
        assert service.store.healthy()
        fence_cleanup(tmp_path)
        assert not service.store.healthy()
        assert service.health()['healthy'] is False
    finally:
        asyncio.run(service.stop())
