from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT_PATH = Path('/root/.openclaw/workspace-mike/tools/compare_baseline_execfw_monthly.py')


def _load_compare_module():
    spec = importlib.util.spec_from_file_location('compare_baseline_execfw_monthly_testmod', SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'failed to load {SCRIPT_PATH}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ComparePrefixReplayRevShortCircuitCase(unittest.TestCase):
    def test_full_rev_signal_cache_exposes_target_window_candidate(self) -> None:
        mod = _load_compare_module()
        raw_5m = mod.load_5m_csv_as_klines(
            csv_path=Path('/root/.openclaw/workspace-mike/tmp/apr2026_rerun_latest_3d_compare/ethusdt_202604_5m_with_preload.csv'),
            symbol='ETHUSDT',
        )
        feature_builder = mod.import_execfw_modules(Path('/root/.openclaw/workspace-mike/repos/QuantitativeStrategy_Executionframework'))['LiveFeatureBuilder']()
        signals = mod._build_full_rev_signals(feature_builder=feature_builder, symbol='ETHUSDT', raw_5m=raw_5m)
        candidate = mod._rev_candidate_at_or_before(signals, mod.pd.Timestamp('2026-04-03T12:30:00Z'))
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate['side'], 'short')
        self.assertEqual(candidate['value_window_15m'], 48)

    def test_rev_candidate_helper_enforces_15m_max_age(self) -> None:
        mod = _load_compare_module()
        raw_5m = mod.load_5m_csv_as_klines(
            csv_path=Path('/root/.openclaw/workspace-mike/tmp/apr2026_rerun_latest_3d_compare/ethusdt_202604_5m_with_preload.csv'),
            symbol='ETHUSDT',
        )
        feature_builder = mod.import_execfw_modules(Path('/root/.openclaw/workspace-mike/repos/QuantitativeStrategy_Executionframework'))['LiveFeatureBuilder']()
        signals = mod._build_full_rev_signals(feature_builder=feature_builder, symbol='ETHUSDT', raw_5m=raw_5m)
        self.assertIsNone(mod._rev_candidate_at_or_before(signals, mod.pd.Timestamp('2026-04-03T12:50:00Z')))


if __name__ == '__main__':
    unittest.main()
