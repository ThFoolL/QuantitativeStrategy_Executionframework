from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import KlineSnapshot
from exec_framework.feature_builder import LiveFeatureBuilder
from exec_framework.market_data import BinanceReadOnlyMarketDataProvider


class _FakeReadOnlyClient:
    def __init__(self, bars_by_interval):
        self.bars_by_interval = bars_by_interval

    def get_klines(self, *, symbol, interval, limit, start_time_ms=None):
        del symbol, limit, start_time_ms
        return list(self.bars_by_interval[interval])


def _kline(interval: str, open_iso: str, minutes: int, o: float, h: float, l: float, c: float) -> KlineSnapshot:
    import pandas as pd

    ts = pd.Timestamp(open_iso)
    open_ms = int(ts.timestamp() * 1000)
    close_ms = open_ms + minutes * 60 * 1000 - 1
    return KlineSnapshot('ETHUSDT', interval, open_ms, close_ms, o, h, l, c, 1.0, None, None, None, None, True, [])


class TrendTimeSemanticsParityCase(unittest.TestCase):
    def test_trend_features_use_close_time_index_for_aggregated_trend_bars(self) -> None:
        builder = LiveFeatureBuilder()
        bars = [
            _kline('1h', '2026-05-12T00:00:00+00:00', 60, 100, 101, 99, 100.5),
            _kline('1h', '2026-05-12T01:00:00+00:00', 60, 101, 102, 100, 101.5),
        ]
        frame = builder._bars_to_frame(bars)
        self.assertEqual(frame.index[0].value // 1_000_000, 1778547599999)
        self.assertEqual(frame.index[1].value // 1_000_000, 1778551199999)

    def test_market_data_exposes_closed_bar_timestamps_for_signal_and_trend(self) -> None:
        bars_5m = [
            _kline('5m', '2026-05-12T00:00:00+00:00', 5, 100, 101, 99, 100.5),
            _kline('5m', '2026-05-12T00:05:00+00:00', 5, 100.5, 101.5, 100, 101),
            _kline('5m', '2026-05-12T00:10:00+00:00', 5, 101, 102, 100.5, 101.2),
            _kline('5m', '2026-05-12T00:15:00+00:00', 5, 101.2, 102.2, 101, 101.8),
            _kline('5m', '2026-05-12T00:20:00+00:00', 5, 101.8, 102.5, 101.5, 102.0),
            _kline('5m', '2026-05-12T00:25:00+00:00', 5, 102.0, 102.8, 101.8, 102.3),
            _kline('5m', '2026-05-12T00:30:00+00:00', 5, 102.3, 103.0, 102.0, 102.7),
            _kline('5m', '2026-05-12T00:35:00+00:00', 5, 102.7, 103.2, 102.4, 102.9),
            _kline('5m', '2026-05-12T00:40:00+00:00', 5, 102.9, 103.5, 102.6, 103.1),
            _kline('5m', '2026-05-12T00:45:00+00:00', 5, 103.1, 103.8, 102.9, 103.4),
            _kline('5m', '2026-05-12T00:50:00+00:00', 5, 103.4, 104.0, 103.0, 103.6),
            _kline('5m', '2026-05-12T00:55:00+00:00', 5, 103.6, 104.2, 103.3, 103.9),
            _kline('5m', '2026-05-12T01:00:00+00:00', 5, 103.9, 104.5, 103.6, 104.1),
        ]
        bars_15m = [
            _kline('15m', '2026-05-12T00:00:00+00:00', 15, 100, 102, 99, 101.2),
            _kline('15m', '2026-05-12T00:15:00+00:00', 15, 101.2, 103.0, 101.0, 102.3),
            _kline('15m', '2026-05-12T00:30:00+00:00', 15, 102.3, 103.8, 102.0, 103.4),
            _kline('15m', '2026-05-12T00:45:00+00:00', 15, 103.4, 104.5, 103.0, 104.1),
        ]
        bars_1h = [
            _kline('1h', '2026-05-12T00:00:00+00:00', 60, 100, 104.5, 99, 104.1),
        ]
        provider = BinanceReadOnlyMarketDataProvider(_FakeReadOnlyClient({'5m': bars_5m, '15m': bars_15m, '1h': bars_1h}), warmup_limit=32, refresh_window_limit=2)
        bundle = provider.load(symbol='ETHUSDT', decision_time=_ts('2026-05-12T01:00:00+00:00'))
        self.assertEqual(bundle.signal_15m_ts, '2026-05-12T00:59:59.999000+00:00')
        self.assertEqual(bundle.trend_1h_ts, '2026-05-12T00:59:59.999000+00:00')


def _ts(iso: str):
    import pandas as pd

    return pd.Timestamp(iso).to_pydatetime()


if __name__ == '__main__':
    unittest.main()
