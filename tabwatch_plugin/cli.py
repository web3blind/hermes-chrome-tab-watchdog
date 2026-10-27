#!/usr/bin/env python3
"""No-model scheduled client. Dry run unless --apply is explicit."""
import argparse
import json
import time
from pathlib import Path
from .control import request, safe_reason


def observation_health(state, wait=15):
    # A navigation is temporarily fenced; allow normal reinstrumentation to finish.
    deadline=time.monotonic()+wait
    while True:
        health=request(state,'health',timeout=5)
        if health.get('healthy') is True or time.monotonic()>=deadline:
            return health
        time.sleep(.5)


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--state-dir',type=Path)
    modes=parser.add_mutually_exclusive_group()
    modes.add_argument('--apply',action='store_true')
    modes.add_argument('--dry-run',action='store_true')
    modes.add_argument('--health',action='store_true')
    args=parser.parse_args(argv)
    from hermes_constants import get_hermes_home
    home=Path(get_hermes_home())
    state=args.state_dir or home/'state/chrome-tab-watchdog'
    try:
        if not args.health:
            health=observation_health(state)
            if health.get('healthy') is not True:
                if health.get('cleanup_error'):
                    error = safe_reason(health['cleanup_error'])
                    cause = safe_reason(health.get('cleanup_gap_reason'))
                    print(f'Chrome tab watchdog: cleanup blocked ({error}; cause={cause}); cleanup skipped.')
                else:
                    print('Chrome tab watchdog: observer is unhealthy; cleanup skipped.')
                return 1
        result=request(state,'health' if args.health else 'sweep',timeout=120,**({} if args.health else {'apply':args.apply}))
    except Exception as exc:
        print('Chrome tab watchdog: control request failed ('+type(exc).__name__+').')
        return 1
    # Service responses are metadata only; no DOM, cookies, URLs or titles.
    if not args.apply or result.get('closed') or result.get('errors'):
        print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 1 if result.get('errors') or (args.health and result.get('healthy') is not True) else 0


if __name__=='__main__':
    raise SystemExit(main())
