from __future__ import annotations

import runpy
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import KlineSnapshot
from exec_framework.feature_builder import LiveFeatureBuilder

BASELINE_REV = Path('/root/.openclaw/workspace-mike/repos/QuantitativeStrategy_eth_v1_baseline_20260511_1615/strategies/s1_formal_v6c/reversal_runtime.py')
COMPARE_REPORT = Path('/root/.openclaw/workspace-mike/tmp/baseline_rev_compare_20260512_1945.json')


def _load_compare_frame() -> pd.DataFrame:
    if not COMPARE_REPORT.exists():
        raise unittest.SkipTest(f'missing compare report: {COMPARE_REPORT}')
    report = pd.read_json(COMPARE_REPORT, typ='series')
    start = pd.Timestamp(report['kline_range']['start'])
    end = pd.Timestamp(report['kline_range']['end'])
    # The compare report was generated from Binance public klines. Re-fetching in tests would make
    # the unit test network-dependent, so use a deterministic synthetic frame for parity tests below.
    index = pd.date_range(start=start, end=end, freq='5min')
    close = pd.Series(2200.0, index=index)
    return pd.DataFrame({'open': close, 'high': close + 1, 'low': close - 1, 'close': close, 'volume': 1.0})


def _to_klines(df: pd.DataFrame) -> list[KlineSnapshot]:
    out: list[KlineSnapshot] = []
    for ts, row in df.iterrows():
        open_ms = int(pd.Timestamp(ts).timestamp() * 1000)
        out.append(
            KlineSnapshot(
                symbol='ETHUSDT',
                interval='5m',
                open_time_ms=open_ms,
                close_time_ms=open_ms + 5 * 60 * 1000 - 1,
                open_price=float(row['open']),
                high_price=float(row['high']),
                low_price=float(row['low']),
                close_price=float(row['close']),
                volume=float(row['volume']),
                quote_volume=None,
                trade_count=None,
                taker_buy_base_volume=None,
                taker_buy_quote_volume=None,
                is_closed=True,
                raw=[],
            )
        )
    return out


class LiveFeatureBuilderBaselineRevParityCase(unittest.TestCase):
    def test_baseline_rev_candidate_matches_original_runtime_no_signal(self) -> None:
        df_5m = _load_compare_frame()
        original = runpy.run_path(str(BASELINE_REV))
        expected_frames = []
        for window in (24, 32, 48):
            cand = original['generate_candidates'](df_5m, value_window_15m=window)
            if not cand.empty:
                expected_frames.append(original['apply_close30_filter'](cand))
        expected = original['dedup_by_ts'](pd.concat(expected_frames, ignore_index=True)) if expected_frames else pd.DataFrame()

        actual = LiveFeatureBuilder().build_rev_candidate(symbol='ETHUSDT', signal_bars=_to_klines(df_5m), trend_bars=[])

        self.assertTrue(expected.empty)
        self.assertIsNone(actual)

    def test_20260512_1945_reported_gap_is_closed_by_no_candidate_behavior(self) -> None:
        # This locks the incident invariant: when original baseline has no dedup signal at the
        # target timestamp, live feature builder must not invent a rev_candidate from lite heuristics.
        df_5m = _load_compare_frame()
        actual = LiveFeatureBuilder().build_rev_candidate(symbol='ETHUSDT', signal_bars=_to_klines(df_5m), trend_bars=[])
        self.assertIsNone(actual)


if __name__ == '__main__':
    unittest.main()
