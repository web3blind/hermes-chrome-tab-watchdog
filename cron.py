"""Transient native Hermes no_agent cron entrypoint; never starts an observer."""
from tabwatch_plugin.cli import main

if __name__ == '__main__':
    raise SystemExit(main(['--apply']))
