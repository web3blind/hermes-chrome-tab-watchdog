import json

import tabwatch_plugin.cli as cli
from tabwatch_plugin.control import fence_cleanup


def test_gap_record_keeps_safe_cause(tmp_path):
    fence_cleanup(tmp_path, reason='owner_settings_conflict')
    data = json.loads((tmp_path / 'cleanup.gap').read_text())
    assert data['reason'] == 'owner_settings_conflict'
    assert isinstance(data['pid'], int)
    assert data['at'] > 0


def test_unknown_gap_reason_is_redacted(tmp_path):
    from tabwatch_plugin.control import gap_reason
    secret = 'https://private.invalid/?secret=synthetic-token'
    fence_cleanup(tmp_path, reason=secret)
    assert secret not in (tmp_path / 'cleanup.gap').read_text()
    assert gap_reason(tmp_path) == 'unknown_gap_reason'
    (tmp_path / 'cleanup.gap').write_bytes(b'untracked_browser_activity\n')
    assert gap_reason(tmp_path) == 'legacy_gap_without_cause'


def test_cli_reports_cleanup_latch_not_dead_observer(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'observation_health', lambda state: {
        'healthy': False, 'cleanup_error': 'untracked_activity_gap',
        'cleanup_gap_reason': 'lease_unavailable', 'observer_error': None})
    monkeypatch.setattr(cli, 'request', lambda *a, **kw: (_ for _ in ()).throw(AssertionError('must not sweep')))
    assert cli.main(['--apply']) == 1
    text = capsys.readouterr().out
    assert 'cleanup blocked' in text and 'lease_unavailable' in text
    assert 'observer is unhealthy' not in text
