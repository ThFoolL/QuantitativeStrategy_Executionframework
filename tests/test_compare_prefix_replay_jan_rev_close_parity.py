from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

SCRIPT_PATH = Path('/root/.openclaw/workspace-mike/tools/compare_baseline_execfw_monthly.py')
CSV_PATH = Path('/root/.openclaw/workspace-mike/out/baseline_vs_execfw_jan_apr_20260513/jan2026/ethusdt_202601_5m_with_preload.csv')
EXECFW_REPO = Path('/root/.openclaw/workspace-mike/repos/QuantitativeStrategy_Executionframework')


def _load_compare_module():
    spec = importlib.util.spec_from_file_location('compare_baseline_execfw_monthly_testmod_jan_rev', SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'failed to load {SCRIPT_PATH}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ComparePrefixReplayJanRevCloseParityCase(unittest.TestCase):
    def test_jan02_target_window_uses_strategy_bar_fast5m(self) -> None:
        if not CSV_PATH.exists():
            raise unittest.SkipTest(f'missing cached csv: {CSV_PATH}')
        mod = _load_compare_module()
        raw_5m = mod.load_5m_csv_as_klines(csv_path=CSV_PATH, symbol='ETHUSDT')
        raw_15m = mod.derive_resampled_klines_from_5m(symbol='ETHUSDT', interval='15m', source_5m=raw_5m)
        raw_1h = mod.derive_resampled_klines_from_5m(symbol='ETHUSDT', interval='1h', source_5m=raw_5m)

        records, _ = mod.run_execfw_full_month(
            execfw_repo_root=EXECFW_REPO,
            symbol='ETHUSDT',
            raw_5m=raw_5m,
            raw_15m=raw_15m,
            raw_1h=raw_1h,
        )

        by_ts = {row['strategy_ts']: row for row in records}
        self.assertEqual(by_ts['2026-01-01T17:00:00+00:00']['plan']['action_type'], 'open')
        self.assertEqual(by_ts['2026-01-01T17:05:00+00:00']['plan']['reason'], 'rev_position_hold_long')
        self.assertEqual(by_ts['2026-01-01T17:05:00+00:00']['state_before']['active_strategy'], 'rev')
        self.assertEqual(by_ts['2026-01-01T17:15:00+00:00']['plan']['reason'], 'tp1_hit')


if __name__ == '__main__':
    unittest.main()
