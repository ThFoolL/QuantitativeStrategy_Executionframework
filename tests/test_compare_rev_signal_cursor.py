from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path('/root/.openclaw/workspace-mike/tools/compare_baseline_execfw_monthly.py')


def _load_compare_module():
    spec = importlib.util.spec_from_file_location('compare_baseline_execfw_monthly_testmod_cursor', SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'failed to load {SCRIPT_PATH}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CompareRevSignalCursorCase(unittest.TestCase):
    def test_cursor_consumes_signal_once_in_order(self) -> None:
        mod = _load_compare_module()
        signals = pd.DataFrame(
            [
                {
                    'ts': pd.Timestamp('2025-06-20T02:15:00Z'),
                    'side': 'short',
                    'entry': 100.0,
                    'stop': 101.0,
                    'tp1': 99.0,
                    'risk': 1.0,
                    'value_window_15m': 32,
                    'score': 1.0,
                    'depth_in_band_frac': 1.0,
                    'retrace_side': 1.0,
                    'close_away_from_edge': 1.0,
                    'reversion_space': 1.0,
                    'body_frac': 0.1,
                    'fav_r_30m': 1.0,
                    'adv_r_30m': 0.1,
                    'fav_r_60m': 1.2,
                    'adv_r_60m': 0.2,
                    'close_r_30m': 0.5,
                    'core_mid': 100.0,
                    'total_width': 2.0,
                },
                {
                    'ts': pd.Timestamp('2025-06-20T02:40:00Z'),
                    'side': 'short',
                    'entry': 102.0,
                    'stop': 103.0,
                    'tp1': 101.0,
                    'risk': 1.0,
                    'value_window_15m': 48,
                    'score': 1.0,
                    'depth_in_band_frac': 1.0,
                    'retrace_side': 1.0,
                    'close_away_from_edge': 1.0,
                    'reversion_space': 1.0,
                    'body_frac': 0.1,
                    'fav_r_30m': 1.0,
                    'adv_r_30m': 0.1,
                    'fav_r_60m': 1.2,
                    'adv_r_60m': 0.2,
                    'close_r_30m': 0.5,
                    'core_mid': 102.0,
                    'total_width': 2.0,
                },
            ]
        )
        cursor = mod.RevSignalCursor(signals)

        first = cursor.candidate_for_slot(pd.Timestamp('2025-06-20T02:15:00Z'))
        self.assertIsNotNone(first)
        self.assertEqual(first['ts'], '2025-06-20T02:15:00+00:00')

        expired = cursor.candidate_for_slot(pd.Timestamp('2025-06-20T02:35:00Z'))
        self.assertIsNone(expired)

        second = cursor.candidate_for_slot(pd.Timestamp('2025-06-20T02:40:00Z'))
        self.assertIsNotNone(second)
        self.assertEqual(second['ts'], '2025-06-20T02:40:00+00:00')

        cursor.mark_consumed(second)
        self.assertIsNone(cursor.candidate_for_slot(pd.Timestamp('2025-06-20T02:45:00Z')))

    def test_cursor_advance_matches_baseline_consume_even_if_not_executed(self) -> None:
        mod = _load_compare_module()
        signals = pd.DataFrame(
            [
                {
                    'ts': pd.Timestamp('2025-06-20T02:15:00Z'),
                    'side': 'short',
                    'entry': 100.0,
                    'stop': 101.0,
                    'tp1': 99.0,
                    'risk': 1.0,
                    'value_window_15m': 32,
                    'score': 1.0,
                    'depth_in_band_frac': 1.0,
                    'retrace_side': 1.0,
                    'close_away_from_edge': 1.0,
                    'reversion_space': 1.0,
                    'body_frac': 0.1,
                    'fav_r_30m': 1.0,
                    'adv_r_30m': 0.1,
                    'fav_r_60m': 1.2,
                    'adv_r_60m': 0.2,
                    'close_r_30m': 0.5,
                    'core_mid': 100.0,
                    'total_width': 2.0,
                },
                {
                    'ts': pd.Timestamp('2025-06-20T02:25:00Z'),
                    'side': 'long',
                    'entry': 98.0,
                    'stop': 97.0,
                    'tp1': 99.0,
                    'risk': 1.0,
                    'value_window_15m': 24,
                    'score': 1.0,
                    'depth_in_band_frac': 1.0,
                    'retrace_side': 1.0,
                    'close_away_from_edge': 1.0,
                    'reversion_space': 1.0,
                    'body_frac': 0.1,
                    'fav_r_30m': 1.0,
                    'adv_r_30m': 0.1,
                    'fav_r_60m': 1.2,
                    'adv_r_60m': 0.2,
                    'close_r_30m': 0.5,
                    'core_mid': 98.0,
                    'total_width': 2.0,
                },
            ]
        )
        cursor = mod.RevSignalCursor(signals)
        seen = cursor.candidate_for_slot(pd.Timestamp('2025-06-20T02:15:00Z'))
        self.assertIsNotNone(seen)
        cursor.mark_consumed(seen)
        next_seen = cursor.candidate_for_slot(pd.Timestamp('2025-06-20T02:25:00Z'))
        self.assertIsNotNone(next_seen)
        self.assertEqual(next_seen['ts'], '2025-06-20T02:25:00+00:00')


if __name__ == '__main__':
    unittest.main()
