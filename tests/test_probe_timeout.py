import asyncio
import pytest

import tabwatch_plugin.runtime as runtime_module
from tabwatch_plugin.runtime import Runtime
from tabwatch_plugin.policy import Policy
from test_runtime import settings, until


def test_readonly_probe_timeout_does_not_poison_owner(tmp_path, monkeypatch):
    async def quiet(self):
        await asyncio.Event().wait()
    monkeypatch.setattr(runtime_module.Service, 'observe_forever', quiet)
    owner = Runtime(tmp_path, settings(), Policy(), None)
    owner.start()
    contender = None
    try:
        assert owner.ready.wait(5)
        original = runtime_module.request
        attempts = []
        def probe(*args, **kwargs):
            if args[1] == 'health' and not attempts:
                attempts.append('timeout')
                raise TimeoutError('synthetic slow read-only probe')
            reply = original(*args, **kwargs)
            if args[1] == 'health':
                attempts.append('success')
            return reply
        monkeypatch.setattr(runtime_module, 'request', probe)
        contender = Runtime(tmp_path, owner.settings, Policy(), None)
        contender.start()
        assert contender.ready.wait(5)
        until(lambda: attempts == ['timeout', 'success'])
        assert not (owner.state / 'cleanup.gap').exists()
        assert not owner.service.store.disabled
    finally:
        if contender:
            contender.stop()
        owner.stop()


@pytest.mark.parametrize('error', ['invalid_settings', 'browser_route_mismatch'])
def test_invalid_settings_still_fence_on_probe_timeout(tmp_path, monkeypatch, error):
    owner = Runtime(tmp_path, settings(), Policy(), None)
    owner.start()
    contender = None
    try:
        assert owner.ready.wait(5)
        def unavailable(*args, **kwargs):
            raise TimeoutError('synthetic probe timeout')
        monkeypatch.setattr(runtime_module, 'request', unavailable)
        contender = Runtime(tmp_path, owner.settings, Policy(), error)
        contender.start()
        assert contender.ready.wait(5)
        assert (owner.state / 'cleanup.gap').exists()
        from tabwatch_plugin.control import request
        assert request(owner.state, 'health')['healthy'] is False
    finally:
        if contender:
            contender.stop()
        owner.stop()
