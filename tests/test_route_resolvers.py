"""Fail-closed route checks across the released and current Hermes CDP modules."""
import importlib
from types import SimpleNamespace

import pytest

from tabwatch_plugin.policy import route_is_observed

NAMES = ('tools.browser_session_cdp', 'tools.browser_tool_cdp')
SETTINGS = {'proxy_host': '127.0.0.1', 'proxy_port': 9223}
CONFIG = {'browser': {'cdp_url': 'http://127.0.0.1:9223'}}


@pytest.mark.parametrize('available', [(NAMES[0],), (NAMES[1],), NAMES])
def test_supported_resolvers_accept_observed_route_and_reject_any_bypass(monkeypatch, available):
    modules = {name: SimpleNamespace(live_cdp_override=lambda: '', env_cdp_url=lambda: '')
               for name in available}
    original = importlib.import_module
    def load(name, *args, **kwargs):
        if name not in NAMES:
            return original(name, *args, **kwargs)
        if name not in modules:
            raise ModuleNotFoundError(name=name)
        return modules[name]
    monkeypatch.setattr(importlib, 'import_module', load)
    assert route_is_observed(SETTINGS, CONFIG, {})
    modules[available[-1]].env_cdp_url = lambda: 'http://127.0.0.1:1'
    assert not route_is_observed(SETTINGS, CONFIG, {})


@pytest.mark.parametrize('failure', ['missing', 'dependency', 'resolver', 'incomplete'])
def test_unknown_or_broken_resolver_remains_fail_closed(monkeypatch, failure):
    original = importlib.import_module
    def broken():
        raise RuntimeError('fixture resolver unavailable')
    def load(name, *args, **kwargs):
        if name not in NAMES:
            return original(name, *args, **kwargs)
        if failure == 'dependency':
            raise ModuleNotFoundError(name='fixture_dependency')
        if failure == 'missing':
            raise ModuleNotFoundError(name=name)
        if failure == 'incomplete':
            return SimpleNamespace(live_cdp_override=lambda: '')
        return SimpleNamespace(live_cdp_override=broken, env_cdp_url=lambda: '')
    monkeypatch.setattr(importlib, 'import_module', load)
    assert not route_is_observed(SETTINGS, CONFIG, {})
