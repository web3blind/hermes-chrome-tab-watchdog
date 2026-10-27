"""Validated common Hermes plugin settings; no independent policy file."""
from dataclasses import asdict, dataclass
import math
import os
import json
import hashlib
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Policy:
    idle_hours: float = 6
    duplicate_hours: float = 2
    blank_minutes: float = 30

    def __post_init__(self):
        for value, multiplier in zip(asdict(self).values(), (3600, 3600, 60)):
            if (type(value) not in (int, float) or not math.isfinite(value)
                    or value <= 0 or not math.isfinite(value * multiplier)
                    or value * multiplier <= 0):
                raise ValueError('invalid_policy_duration')


DEFAULTS = dict(asdict(Policy()), upstream='http://127.0.0.1:9222',
                proxy_host='127.0.0.1', proxy_port=9223)


def read_settings(ctx):
    # Read the whole public subtree too: get_config alone cannot detect typos or
    # a scalar settings node. Do not silently use the SDK's legacy config fallback.
    from hermes_cli.config import load_config_readonly
    try:
        config = load_config_readonly()
        values = config.get('plugins', {}).get('entries', {}).get(
            'chrome-tab-watchdog', {}).get('settings', {})
        if not isinstance(values, dict) or set(values) - DEFAULTS.keys():
            raise ValueError('invalid_settings')
        settings = {key: ctx.get_config(key, default) if key in values else default
                    for key, default in DEFAULTS.items()}
        policy = Policy(**{key: settings[key] for key in asdict(Policy())})
        error = None
    except Exception:
        # Keep valid transport fields available even if durations are invalid.
        settings = dict(DEFAULTS)
        if 'values' in locals() and isinstance(values, dict):
            settings.update({key: values[key] for key in DEFAULTS if key in values})
        policy, error = Policy(), 'invalid_settings'
    validate_transport(settings)
    if error is None:
        expected = f"http://{settings['proxy_host']}:{settings['proxy_port']}"
        if config.get('browser', {}).get('cdp_url', '').rstrip('/') != expected:
            error = 'browser_route_mismatch'
    return settings, policy, error


def settings_signature(settings, policy, error):
    values = dict(asdict(policy), **{key: settings[key] for key in ('upstream', 'proxy_host', 'proxy_port')}, error=error)
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def route_is_observed(settings, config, args):
    if args.get('local'):
        return False
    expected = (settings['proxy_host'], settings['proxy_port'])
    routes = [config.get('browser', {}).get('cdp_url', '')]
    routes += [os.environ[key] for key in ('BROWSER_CDP_URL', 'BU_CDP_URL', 'BU_CDP_WS') if os.environ.get(key)]
    # New Hermes releases keep live and secret-scoped overrides outside os.environ.
    # These are nonmutating resolvers; older supported releases expose only env/config.
    try:
        from tools import browser_tool_cdp
        for name in ('live_cdp_override', 'env_cdp_url'):
            resolver = getattr(browser_tool_cdp, name, None)
            if resolver is not None:
                value = resolver()
                if value:
                    routes.append(value)
    except Exception:
        return False
    for route in routes:
        try:
            parts = urlsplit(route)
            if (parts.scheme not in {'http', 'ws'} or (parts.hostname, parts.port) != expected
                    or parts.username or parts.password):
                return False
        except (ValueError, TypeError):
            return False
    return True


def validate_transport(settings):
    parts = urlsplit(settings['upstream'])
    if (parts.scheme != 'http' or parts.hostname not in {'localhost', '127.0.0.1'}
            or parts.port is None or parts.path not in {'', '/'} or parts.query or parts.fragment):
        raise ValueError('invalid_upstream')
    if settings['proxy_host'] not in {'127.0.0.1', 'localhost'}:
        raise ValueError('invalid_proxy_host')
    port = settings['proxy_port']
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError('invalid_proxy_port')
    if parts.port == port:
        raise ValueError('proxy_upstream_loop')
