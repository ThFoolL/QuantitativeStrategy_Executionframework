from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT_PATH = Path('/root/.openclaw/workspace-mike/tools/compare_baseline_execfw_monthly.py')


def _load_compare_module():
    spec = importlib.util.spec_from_file_location('compare_signal_history_depth_mod', SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'failed to load {SCRIPT_PATH}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CompareSignalHistoryDepthCase(unittest.TestCase):
    def test_execfw_replay_uses_six_signal_bars_for_baseline_swing_lookback(self) -> None:
        mod = _load_compare_module()
        raw_5m = mod.load_5m_csv_as_klines(
            csv_path=Path('/root/.openclaw/workspace-mike/out/baseline_vs_execfw_2025_full_year_rerun_20260514T023508Z/202507/ethusdt_202507_5m_with_preload.csv'),
            symbol='ETHUSDT',
        )
        raw_15m = mod.derive_resampled_klines_from_5m(symbol='ETHUSDT', interval='15m', source_5m=raw_5m)
        signal_idx = 0
        found = None
        for decision_bar in raw_5m:
            signal_idx = mod._advance_visible_idx(raw_15m, signal_idx, decision_bar.close_time_ms)
            if signal_idx >= 6:
                signal_closed = mod._bars_slice(raw_15m, signal_idx)
                found = [
                    {'close': item.close_price, 'low': item.low_price, 'high': item.high_price}
                    for item in signal_closed[-6:]
                ]
                break
        self.assertIsNotNone(found)
        self.assertEqual(len(found), 6)


if __name__ == '__main__':
    unittest.main()
