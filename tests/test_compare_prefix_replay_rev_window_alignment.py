from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

SCRIPT_PATH = Path('/root/.openclaw/workspace-mike/tools/compare_baseline_execfw_monthly.py')
CSV_PATH = Path('/root/.openclaw/workspace-mike/tmp/apr2026_rerun_latest_3d_compare/ethusdt_202604_5m_with_preload.csv')


def _load_compare_module():
    spec = importlib.util.spec_from_file_location('compare_baseline_execfw_monthly_testmod', SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'failed to load {SCRIPT_PATH}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ComparePrefixReplayRevWindowCase(unittest.TestCase):
    def test_target_window_has_rev_candidate_at_20260403_1230z(self) -> None:
        mod = _load_compare_module()
        self.assertTrue(CSV_PATH.exists(), f'missing cached csv: {CSV_PATH}')
        raw_5m = mod.load_5m_csv_as_klines(csv_path=CSV_PATH, symbol='ETHUSDT')
        feature_builder = mod.import_execfw_modules(Path('/root/.openclaw/workspace-mike/repos/QuantitativeStrategy_Executionframework'))['LiveFeatureBuilder']()
        signals = mod._build_full_rev_signals(feature_builder=feature_builder, symbol='ETHUSDT', raw_5m=raw_5m)
        candidate = mod._rev_candidate_at_or_before(signals, mod.pd.Timestamp('2026-04-03T12:30:00Z'))
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate['side'], 'short')
        self.assertEqual(candidate['value_window_15m'], 48)

    def test_target_window_rev_candidate_expires_after_15m(self) -> None:
        mod = _load_compare_module()
        raw_5m = mod.load_5m_csv_as_klines(csv_path=CSV_PATH, symbol='ETHUSDT')
        feature_builder = mod.import_execfw_modules(Path('/root/.openclaw/workspace-mike/repos/QuantitativeStrategy_Executionframework'))['LiveFeatureBuilder']()
        signals = mod._build_full_rev_signals(feature_builder=feature_builder, symbol='ETHUSDT', raw_5m=raw_5m)
        candidate = mod._rev_candidate_at_or_before(signals, mod.pd.Timestamp('2026-04-03T12:50:00Z'))
        self.assertIsNone(candidate)


if __name__ == '__main__':
    unittest.main()
