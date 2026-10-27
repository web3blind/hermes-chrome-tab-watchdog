"""Supported Hermes tool_execution middleware; no core or Harness monkeypatch."""
import hashlib
import json
import contextlib
import logging
import re
import uuid
from pathlib import Path
from .control import request, fence_cleanup
from .policy import read_settings, route_is_observed, settings_signature
from .runtime import Runtime


def namespace(home, session):
    # Stable across calls and upgrades; never collide with old upstream daemons.
    prefix = 'tw-'+hashlib.sha256(str(home).encode()).hexdigest()[:8]+'-'
    if isinstance(session,str) and re.fullmatch(re.escape(prefix)+'[0-9a-f]{20}',session):
        return session
    return prefix+hashlib.sha256((session or 'default').encode()).hexdigest()[:20]


def uncertain(value):
    if isinstance(value,str):
        try:
            value=json.loads(value)
        except (ValueError,TypeError):
            return True
    if not isinstance(value,dict):
        return True
    return value.get('success') is False or bool(value.get('error'))


def register(ctx):
    from hermes_constants import get_hermes_home
    home = Path(get_hermes_home()).resolve()
    state = home / 'state' / 'chrome-tab-watchdog'
    runtime = None
    try:
        settings, policy, error = read_settings(ctx)
        runtime = Runtime(home, settings, policy, error)
        # Register release before acquiring resources, including a failed start.
        ctx.on_unload(runtime.stop)
        runtime.start()
        if error:
            logging.getLogger(__name__).error('Chrome tab watchdog cleanup disabled: %s', error)
    except Exception:
        if runtime is not None:
            runtime.stop()
        logging.getLogger(__name__).error('Chrome tab watchdog startup failed; cleanup disabled.')

    def fence(reason):
        if runtime is not None and runtime.service is not None:
            runtime.service.cleanup_error = 'untracked_activity_gap'
            if runtime.service.store is not None:
                runtime.service.store.disabled = True
        try:
            fence_cleanup(state, reason=reason)
        except Exception:
            logging.getLogger(__name__).error('Chrome tab watchdog durable gap unavailable; cleanup cannot be guaranteed.')
        with contextlib.suppress(Exception):
            request(state, 'fence', timeout=2, cause=reason)

    def wrap(**kw):
        name=kw.get('tool_name','')
        args=kw['args']
        next_call=kw['next_call']
        protected = name in {'secure_env_vault', 'secure_login_fill', 'secure_payment_fill'}
        if name != 'browser_exec' and not name.startswith('browser_vault') and not protected:
            return next_call(args)
        call=uuid.uuid4().hex
        try:
            limit=float(args.get('timeout_s',300))
        except (TypeError,ValueError):
            limit=300
        ttl=max(60,min(1900,limit+60))
        effective=dict(args)
        if name=='browser_exec' and args.get('action','exec')=='exec':
            session=args.get('session','')
            if isinstance(session,str) and (not session or re.fullmatch('[A-Za-z0-9][A-Za-z0-9_-]{0,63}',session)):
                effective['session']=namespace(home,session)
        if runtime is not None:
            runtime.ready.wait(2)
        try:
            if Path(get_hermes_home()).resolve() != home:
                fence('profile_mismatch')
            else:
                from hermes_cli.config import load_config_readonly
                current, current_policy, error = read_settings(ctx)
                if runtime is None or error:
                    fence('validation_failed')
                elif not route_is_observed(current, load_config_readonly(), args):
                    fence('route_unobserved')
                elif settings_signature(current, current_policy, error) != runtime.signature:
                    fence('settings_changed')
        except Exception:
            fence('validation_failed')
        leased = False
        try:
            request(state,'lease_start',timeout=2,call_id=call,ttl=ttl)
            leased = True
        except Exception:
            # Fence BEFORE the fallback. Neither a failed lease nor a failed
            # cleanup subsystem may short-circuit or replay the browser action.
            fence('lease_unavailable')
        completed=False
        try:
            result=next_call(effective)
            completed=not uncertain(result)
            return result
        finally:
            # Timeouts can leave the daemon running. Keep the bounded lease then;
            # exact in-flight CDP requests have their own target-level fences.
            if completed and leased:
                try:
                    request(state,'lease_end',timeout=2,call_id=call)
                except Exception:
                    pass
    try:
        ctx.register_middleware('tool_execution',wrap)
    except BaseException:
        if runtime is not None:
            runtime.stop()
        raise
    return runtime
