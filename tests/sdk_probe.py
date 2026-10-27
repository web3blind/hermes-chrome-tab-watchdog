"""Run with a chosen unmodified Hermes interpreter/PYTHONPATH; disposable home only."""
import json
import os
from pathlib import Path
import socket
import time

from hermes_cli.plugins import PluginManager

home = Path(os.environ['HERMES_HOME'])
assert (home / 'ISOLATED_TEST_HOME').exists()
manager = PluginManager(scope_key=str(home))
manager.discover_and_load()
loaded = [p for p in manager.list_plugins() if p.get('name') == 'chrome-tab-watchdog']
assert len(loaded) == 1, loaded
assert manager.has_middleware('tool_execution')
state = home / 'state' / 'chrome-tab-watchdog'
for _ in range(100):
    if (state / 'control.sock').exists():
        break
    time.sleep(.05)
assert (state / 'control.sock').exists(), loaded
# Short fixture directory; avoid depending on the installed plugin import path.
with socket.socket(socket.AF_UNIX) as conn:
    conn.connect(str(state / 'control.sock'))
    conn.sendall(b'{"cmd":"health"}\n')
    health = json.loads(conn.recv(65536))
assert health['runtime_pid'] == os.getpid(), health
assert health['policy'] == {'idle_hours': 6, 'duplicate_hours': 2, 'blank_minutes': 30}
assert manager.unload('chrome-tab-watchdog')
assert not (state / 'control.sock').exists()
assert not manager.has_middleware('tool_execution')
print(json.dumps({'actual_loader': True, 'pid_same_process': True, 'unload_cleanup': True,
                  'policy': health['policy']}))
