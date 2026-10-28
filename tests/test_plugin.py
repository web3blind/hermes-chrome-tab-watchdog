import json
import time

import pytest


import tabwatch_plugin as plugin
from tabwatch_plugin.control import fence_stamp, request
from tabwatch_plugin.policy import Policy, read_settings
from test_runtime import settings


class Context:
    def __init__(self, values):
        self.values = values
        self.cleanups = []

    def get_config(self, key, default=None):
        return self.values.get(key, default)

    def on_unload(self, callback):
        self.cleanups.append(callback)

    def register_middleware(self, name, callback):
        assert name == 'tool_execution'
        self.wrap = callback

    def unload(self):
        for cleanup in reversed(self.cleanups):
            cleanup()


def configured(home, values, route=None):
    config = {'plugins': {'entries': {'chrome-tab-watchdog': {'settings': values}}},
              'browser': {'cdp_url': route or f"http://{values['proxy_host']}:{values['proxy_port']}"}}
    (home / 'config.yaml').write_text(json.dumps(config))
    return Context(values)


@pytest.fixture
def loaded(isolated_home):
    ctx = configured(isolated_home, settings())
    start = time.monotonic()
    runtime = plugin.register(ctx)
    assert time.monotonic() - start < 2
    assert runtime.ready.wait(5) and runtime.status == 'owner'
    yield ctx, runtime
    ctx.unload()


def test_browser_single_dispatch_namespaced_and_released(loaded):
    ctx, runtime = loaded
    calls = []
    args = {'session':'research', 'code':'print(42)'}
    result = ctx.wrap(tool_name='browser_exec', args=args,
                      next_call=lambda a: calls.append(a) or {'success':True})
    assert result == {'success':True} and len(calls) == 1
    assert calls[0]['code'] == args['code'] and calls[0]['session'].startswith('tw-')
    assert args['session'] == 'research'
    assert request(runtime.state, 'health')['active_leases'] == 0


def test_missing_lease_fences_before_fallback_and_calls_once(loaded, monkeypatch):
    ctx, runtime = loaded
    def fail(*args, **kw):
        raise OSError('private fixture payload')
    monkeypatch.setattr(plugin, 'request', fail)
    calls = []
    def action(args):
        assert fence_stamp(runtime.state) is not None
        assert request(runtime.state, 'health')['healthy'] is False
        calls.append(args)
        return {'success':True, 'receipt':'unchanged'}
    assert ctx.wrap(tool_name='browser_exec', args={'code':'synthetic'}, next_call=action)['receipt'] == 'unchanged'
    assert len(calls) == 1
    assert request(runtime.state, 'health')['cleanup_error'] == 'untracked_activity_gap'


def test_downstream_exception_not_retried(loaded):
    ctx, runtime = loaded
    calls = []
    def action(args):
        calls.append(args)
        raise ValueError('downstream')
    with pytest.raises(ValueError, match='downstream'):
        ctx.wrap(tool_name='browser_exec', args={}, next_call=action)
    assert len(calls) == 1
    assert request(runtime.state, 'health')['active_leases'] == 1


@pytest.mark.parametrize('name', ['secure_env_vault', 'secure_login_fill', 'secure_payment_fill'])
def test_protected_browser_tools_hold_lease_during_dispatch(loaded, name):
    ctx, runtime = loaded
    calls = []
    def action(args):
        assert request(runtime.state, 'health')['active_leases'] == 1
        calls.append(args)
        return {'success':True}
    assert ctx.wrap(tool_name=name, args={'handle':'fixture'}, next_call=action)['success']
    assert calls == [{'handle':'fixture'}]
    assert request(runtime.state, 'health')['active_leases'] == 0


def test_other_tools_and_vault_preserved(loaded):
    ctx, runtime = loaded
    for name in ['terminal', 'browser_vault_fill']:
        args = {'handle':'synthetic'}
        calls = []
        assert ctx.wrap(tool_name=name, args=args, next_call=lambda a: calls.append(a) or {'success':True})['success']
        assert calls == [args]


@pytest.mark.parametrize('value', [0, -1, True, '6', None, float('inf'), float('nan'), 1e308])
def test_invalid_policy_retains_transport_and_dispatch(isolated_home, value):
    values = settings()
    values['idle_hours'] = value
    ctx = configured(isolated_home, values)
    runtime = plugin.register(ctx)
    try:
        assert runtime.ready.wait(5) and runtime.status == 'owner'
        health = request(runtime.state, 'health')
        assert not health['healthy'] and health['cleanup_error'] == 'invalid_settings'
        assert health['policy'] is None
        calls = []
        ctx.wrap(tool_name='browser_exec', args={}, next_call=lambda a: calls.append(a) or {'success':True})
        assert len(calls) == 1
    finally:
        ctx.unload()


def test_settings_profile_scope_captured_at_register(loaded, monkeypatch, tmp_path):
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    ctx, runtime = loaded
    other = tmp_path / 'other'
    monkeypatch.setenv('HERMES_HOME', str(other))
    token = set_hermes_home_override(other)
    try:
        got = []
        ctx.wrap(tool_name='browser_exec', args={}, next_call=lambda a: got.append(a) or {'success':True})
        assert got[0]['session'] == plugin.namespace(runtime.home, '')
        assert not other.exists()
    finally:
        reset_hermes_home_override(token)


def test_unknown_settings_and_route_mismatch_disable_cleanup(isolated_home):
    for extra, route, reason in [({'typo':1}, None, 'invalid_settings'), ({}, 'http://127.0.0.1:1', 'browser_route_mismatch')]:
        values = dict(settings(), **extra)
        ctx = configured(isolated_home, values, route)
        _, _, error = read_settings(ctx)
        assert error == reason


@pytest.mark.parametrize('key', ['BROWSER_CDP_URL', 'BU_CDP_URL', 'BU_CDP_WS', 'config_changed'])
def test_bypassed_route_fences_before_single_dispatch(loaded, monkeypatch, key):
    ctx, runtime = loaded
    if key == 'config_changed':
        configured(runtime.home, ctx.values, route='http://127.0.0.1:1')
    else:
        monkeypatch.setenv(key, 'ws://127.0.0.1:1/devtools/browser/fixture' if key.endswith('WS') else 'http://127.0.0.1:1')
    calls = []
    def action(args):
        assert fence_stamp(runtime.state) is not None
        assert request(runtime.state, 'health')['healthy'] is False
        calls.append(args)
        return {'success': True}
    assert ctx.wrap(tool_name='browser_exec', args={}, next_call=action)['success']
    assert len(calls) == 1


@pytest.mark.parametrize('resolver', ['live_cdp_override', 'env_cdp_url'])
def test_profile_override_fences_before_single_dispatch(loaded, monkeypatch, resolver):
    import importlib
    import importlib.util
    name = ('tools.browser_session_cdp' if importlib.util.find_spec('tools.browser_session_cdp')
            else 'tools.browser_tool_cdp')
    browser_cdp = importlib.import_module(name)
    ctx, runtime = loaded
    monkeypatch.setattr(browser_cdp, resolver, lambda: 'http://127.0.0.1:1')
    calls = []
    def action(args):
        assert fence_stamp(runtime.state) is not None
        assert request(runtime.state, 'health')['healthy'] is False
        calls.append(args)
        return {'success': True}
    assert ctx.wrap(tool_name='browser_exec', args={}, next_call=action)['success']
    assert len(calls) == 1


def test_namespace_is_stable_and_profile_scoped(tmp_path):
    value = plugin.namespace(tmp_path, 'research')
    assert plugin.namespace(tmp_path, value) == value
    assert plugin.namespace(tmp_path / 'other', value) != value
