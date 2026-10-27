"""All integration data lives in disposable homes; never load real plugins."""
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    home = tmp_path / 'hermes'
    home.mkdir()
    monkeypatch.setenv('HERMES_HOME', str(home))
    from hermes_constants import get_hermes_home, set_hermes_home_override, reset_hermes_home_override
    token = set_hermes_home_override(home)
    assert get_hermes_home() == home
    yield home
    reset_hermes_home_override(token)
