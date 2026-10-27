from tabwatch_plugin.store import Store


def test_gap_causes_are_bounded_and_do_not_leak_input(tmp_path):
    store = Store(tmp_path)
    try:
        store.coverage('fixture')
        for _ in range(20):
            store.gap('unattributed_command', 'Runtime.evaluate')
        assert len(store.gap_events) == 16
        assert store.gap_events[-1]['reason'] == 'unattributed_command'
        assert store.gap_events[-1]['method'] == 'Runtime.evaluate'
        store.gap('PRIVATE-INPUT', 'PRIVATE-INPUT')
        assert 'PRIVATE-INPUT' not in repr(store.gap_events)
    finally:
        store.close()
