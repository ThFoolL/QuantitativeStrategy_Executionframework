from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import KlineSnapshot
from exec_framework.feature_builder import LiveFeatureBuilder


def _klines() -> list[KlineSnapshot]:
    rows = []
    # Chosen so close_time timestamps fall into the next right-labeled 1h bucket while open_time
    # timestamps match baseline CSV timestamp semantics.
    for i, price in enumerate([100.0, 101.0, 102.0, 103.0, 104.0]):
        ts = pd.Timestamp('2026-05-12T00:00:00Z') + pd.Timedelta(minutes=5 * i)
        open_ms = int(ts.timestamp() * 1000)
        rows.append(KlineSnapshot('ETHUSDT', '5m', open_ms, open_ms + 5 * 60 * 1000 - 1, price, price + 1, price - 1, price + 0.5, 1.0, None, None, None, None, True, []))
    return rows


class FeatureBuilderTimeIndexParityCase(unittest.TestCase):
    def test_trend_features_use_open_time_index_like_baseline_csv(self) -> None:
        builder = LiveFeatureBuilder()
        frame = builder._bars_to_open_frame(_klines())
        self.assertEqual(str(frame.index[0]), '2026-05-12 00:00:00+00:00')
        one_hour = frame.resample('1h', label='right', closed='right').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
        self.assertIn(pd.Timestamp('2026-05-12T00:00:00Z'), one_hour.index)
        self.assertIn(pd.Timestamp('2026-05-12T01:00:00Z'), one_hour.index)
        self.assertEqual(float(one_hour.loc[pd.Timestamp('2026-05-12T00:00:00Z'), 'open']), 100.0)
        self.assertEqual(float(one_hour.loc[pd.Timestamp('2026-05-12T01:00:00Z'), 'open']), 101.0)

    def test_close_time_index_would_change_bucket_membership(self) -> None:
        builder = LiveFeatureBuilder()
        open_frame = builder._bars_to_open_frame(_klines())
        close_frame = builder._bars_to_frame(_klines())
        open_1h = open_frame.resample('1h', label='right', closed='right').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
        close_1h = close_frame.resample('1h', label='right', closed='right').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
        common = open_1h.index.intersection(close_1h.index)
        self.assertTrue(any(float(open_1h.loc[idx, 'open']) != float(close_1h.loc[idx, 'open']) for idx in common))


if __name__ == '__main__':
    unittest.main()
