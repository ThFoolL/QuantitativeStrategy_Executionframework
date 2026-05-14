from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCRIPT = Path('/root/.openclaw/workspace-mike/tools/compare_baseline_execfw_monthly.py')
spec = importlib.util.spec_from_file_location('compare_baseline_execfw_monthly', SCRIPT)
if spec is None or spec.loader is None:
    raise RuntimeError(f'failed to load {SCRIPT}')
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


class CompareWindowEventAttributionCase(unittest.TestCase):
    def test_execfw_open_event_uses_strategy_ts_for_window_attribution(self) -> None:
        tz_bj = timezone(timedelta(hours=8))
        windows = [
            (
                datetime(2026, 1, 5, 18, 0, 0, tzinfo=tz_bj),
                datetime(2026, 1, 6, 0, 0, 0, tzinfo=tz_bj),
            ),
            (
                datetime(2026, 1, 6, 0, 0, 0, tzinfo=tz_bj),
                datetime(2026, 1, 6, 6, 0, 0, tzinfo=tz_bj),
            ),
        ]
        records = [
            {
                'decision_ts': '2026-01-05T15:59:59.999000+00:00',
                'strategy_ts': '2026-01-05T16:00:00+00:00',
                'plan': {
                    'action_type': 'open',
                    'target_side': 'long',
                    'target_strategy': 'trend',
                    'reason': 'trend_long_entry',
                    'risk_fraction': 0.1,
                    'conflict_context': {},
                },
                'state_after': {'active_strategy': 'trend', 'active_side': 'long'},
            }
        ]
        aggregated = compare.aggregate_execfw_windows(records=records, windows=windows, tz_bj=tz_bj)
        self.assertEqual(aggregated[0]['open_count'], 0)
        self.assertEqual(aggregated[0]['end_state'], {'active_strategy': 'none', 'active_side': 'none'})
        self.assertEqual(aggregated[1]['open_count'], 1)
        self.assertEqual(aggregated[1]['open_events'][0]['ts'], '2026-01-05T16:00:00+00:00')
        self.assertEqual(aggregated[1]['end_state'], {'active_strategy': 'trend', 'active_side': 'long'})


if __name__ == '__main__':
    unittest.main()
