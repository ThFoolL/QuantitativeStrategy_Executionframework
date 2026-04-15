from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class LiveFeatureConfig:
    ema_fast: int = 20
    ema_slow: int = 50
    atr_period: int = 14
    adx_period: int = 14
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    state_lookback: int = 120
    range_lookback: int = 12
    rev_windows_15m: tuple[int, ...] = (24, 32, 48)
    rev_dedup_tolerance_bars: int = 1


class LiveFeatureBuilder:
    """从只读 kline 构造 live 最小特征集。

    已尽量复用回测主脚本中的指标公式：
    - ema_fast / ema_slow
    - atr / adx / atr_rank
    - structure_tag

    rev_candidate 当前升级为 `shared_formal_lite_v1`：
    - 复用 formal 主线中的多窗口入口思想（24/32/48）
    - 保持 live 内部纯本地计算，不依赖外部回测脚本运行时
    - 明确仍不是 full formal parity；只是比 heuristic_v1 更接近正式口径
    """

    def __init__(self, config: LiveFeatureConfig | None = None):
        self.config = config or LiveFeatureConfig()

    def build(self, *, symbol: str, trend_bars: list[Any], signal_bars: list[Any]) -> dict[str, Any]:
        trend_df = self._bars_to_frame(trend_bars)
        signal_df = self._bars_to_frame(signal_bars)
        trend_features = self.build_trend_features(symbol=symbol, trend_bars=trend_bars)
        rev_candidate = self.build_rev_candidate(symbol=symbol, signal_bars=signal_bars, trend_bars=trend_bars)
        return {
            'trend_features': trend_features,
            'rev_candidate': rev_candidate,
            'metadata': {
                'trend_rows': len(trend_df),
                'signal_rows': len(signal_df),
                'rev_candidate_mode': rev_candidate.get('approximation') if rev_candidate else 'none',
            },
        }

    def build_trend_features(self, *, symbol: str, trend_bars: list[Any]) -> dict[str, Any]:
        del symbol
        trend_df = self._bars_to_frame(trend_bars)
        if trend_df.empty:
            return {
                'ema_fast': None,
                'ema_slow': None,
                'adx': None,
                'atr_rank': None,
                'structure_tag': 'UNAVAILABLE',
                'feature_status': 'insufficient_trend_bars',
            }

        featured = self._compute_trend_features(trend_df)
        current = featured.iloc[-1]
        structure_tag = self._classify_structure(featured)
        return {
            'ema_fast': self._clean_float(current.get('ema_fast')),
            'ema_slow': self._clean_float(current.get('ema_slow')),
            'adx': self._clean_float(current.get('adx')),
            'atr_rank': self._clean_float(current.get('atr_rank')),
            'structure_tag': structure_tag,
            'feature_status': 'ready' if self._trend_row_ready(current) else 'insufficient_trend_bars',
        }

    def build_rev_candidate(self, *, symbol: str, signal_bars: list[Any], trend_bars: list[Any]) -> dict[str, Any] | None:
        del symbol
        signal_df = self._bars_to_frame(signal_bars)
        trend_df = self._compute_trend_features(self._bars_to_frame(trend_bars))
        if len(signal_df) < 12 or trend_df.empty:
            return None

        current_trend = trend_df.iloc[-1]
        structure_tag = self._classify_structure(trend_df)
        atr_rank = self._clean_float(current_trend.get('atr_rank'))
        ema_fast = self._clean_float(current_trend.get('ema_fast'))
        adx = self._clean_float(current_trend.get('adx'))
        if atr_rank is None or ema_fast is None or adx is None:
            return None

        candidates = self._generate_formal_lite_candidates(signal_df)
        if not candidates:
            return None

        candidates = self._apply_formal_lite_filter(candidates, trend_df, structure_tag)
        if not candidates:
            return None

        chosen = self._dedup_candidates(candidates)
        current_close = float(signal_df.iloc[-1]['close'])
        risk = abs(float(chosen['entry']) - float(chosen['stop']))
        if risk <= 0:
            return None

        score = self._score_rev_candidate(
            current_close=current_close,
            risk=risk,
            atr_rank=atr_rank,
            adx=adx,
            window=int(chosen['value_window_15m']),
        )
        return {
            'ts': pd.Timestamp(chosen['ts']).isoformat(),
            'side': str(chosen['side']),
            'entry': float(chosen['entry']),
            'stop': float(chosen['stop']),
            'tp1': float(chosen['tp1']),
            'value_window_15m': int(chosen['value_window_15m']),
            'score': score,
            'approximation': 'shared_formal_lite_v1',
            'source': 'live_feature_builder',
            'structure_tag_basis': structure_tag,
            'formal_alignment': 'shared_formal_lite',
            'notes': [
                'multi_window_rev_candidate',
                'not_full_backtest_parity',
            ],
        }

    def _generate_formal_lite_candidates(self, signal_df: pd.DataFrame) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        current = signal_df.iloc[-1]
        current_close = float(current['close'])
        current_ts = signal_df.index[-1]

        for window in self.config.rev_windows_15m:
            if len(signal_df) < window + 2:
                continue
            recent = signal_df.tail(window)
            prior = recent.iloc[:-1]
            prev = recent.iloc[-2]

            prev_high = float(prev['high'])
            prev_low = float(prev['low'])
            prior_low = float(prior['low'].min())
            prior_high = float(prior['high'].max())
            recent_range = max(prior_high - prior_low, 1e-9)

            long_break = prev_low <= prior_low * 1.001 and current_close > prev_high
            short_break = prev_high >= prior_high * 0.999 and current_close < prev_low

            if long_break:
                stop = min(prior_low, prev_low)
                if current_close > stop:
                    risk = current_close - stop
                    candidates.append(
                        {
                            'ts': current_ts,
                            'side': 'long',
                            'entry': current_close,
                            'stop': stop,
                            'tp1': current_close + risk,
                            'value_window_15m': window,
                            'range_ratio': min(recent_range / max(current_close, 1e-9), 1.0),
                        }
                    )

            if short_break:
                stop = max(prior_high, prev_high)
                if stop > current_close:
                    risk = stop - current_close
                    candidates.append(
                        {
                            'ts': current_ts,
                            'side': 'short',
                            'entry': current_close,
                            'stop': stop,
                            'tp1': current_close - risk,
                            'value_window_15m': window,
                            'range_ratio': min(recent_range / max(current_close, 1e-9), 1.0),
                        }
                    )

        return candidates

    def _apply_formal_lite_filter(
        self,
        candidates: list[dict[str, Any]],
        trend_df: pd.DataFrame,
        structure_tag: str,
    ) -> list[dict[str, Any]]:
        current_trend = trend_df.iloc[-1]
        ema_fast = float(current_trend['ema_fast'])
        adx = self._clean_float(current_trend.get('adx')) or 0.0
        atr_rank = self._clean_float(current_trend.get('atr_rank')) or 0.0

        filtered: list[dict[str, Any]] = []
        for item in candidates:
            entry = float(item['entry'])
            side = str(item['side'])
            near_ema = abs(entry - ema_fast) / max(entry, 1e-9) <= 0.02
            if not near_ema:
                continue
            if structure_tag not in {'CHOP', 'COMPRESSION', 'TREND_CONT'}:
                continue
            if adx > 35 and atr_rank > 0.75:
                continue
            if side == 'long' and entry < ema_fast * 0.985:
                continue
            if side == 'short' and entry > ema_fast * 1.015:
                continue
            filtered.append(item)
        return filtered

    def _dedup_candidates(self, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        ranked = sorted(
            candidates,
            key=lambda item: (
                int(item['value_window_15m']) != 24,
                -float(item.get('range_ratio', 0.0)),
                int(item['value_window_15m']),
            ),
        )
        return ranked[0]

    @staticmethod
    def _score_rev_candidate(*, current_close: float, risk: float, atr_rank: float, adx: float, window: int) -> float:
        raw = 0.4 * min(risk / max(current_close, 1e-9) * 100, 1.0) + 0.4 * max(0.0, 1.0 - atr_rank) + 0.2 * max(0.0, 1.0 - min(adx / 40.0, 1.0))
        if window == 24:
            raw += 0.05
        return round(min(max(raw, 0.0), 1.0), 4)

    def _compute_trend_features(self, trend_df: pd.DataFrame) -> pd.DataFrame:
        df = trend_df.copy()
        cfg = self.config
        df['ema_fast'] = df['close'].ewm(span=cfg.ema_fast, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=cfg.ema_slow, adjust=False).mean()
        df['atr'] = self._atr(df, cfg.atr_period)
        df['atr_pct'] = df['atr'] / df['close']
        df['adx'] = self._adx(df, cfg.adx_period)
        df['range_pct'] = (df['high'].rolling(cfg.range_lookback).max() - df['low'].rolling(cfg.range_lookback).min()) / df['close']
        df['bb_mid'] = df['close'].rolling(cfg.bollinger_period).mean()
        df['bb_std'] = df['close'].rolling(cfg.bollinger_period).std(ddof=0)
        df['bbw'] = (4 * cfg.bollinger_std * df['bb_std']) / df['bb_mid']
        df['atr_rank'] = df['atr_pct'].rolling(cfg.state_lookback).rank(pct=True)
        df['bbw_rank'] = df['bbw'].rolling(cfg.state_lookback).rank(pct=True)
        df['range_rank'] = df['range_pct'].rolling(cfg.state_lookback).rank(pct=True)
        return df

    def _classify_structure(self, trend_df: pd.DataFrame) -> str:
        if trend_df.empty:
            return 'UNAVAILABLE'
        row = trend_df.iloc[-1]
        cfg = self.config
        if pd.isna(row.get('atr_rank')) or pd.isna(row.get('adx')):
            return 'CHOP'
        recent_high = trend_df['high'].shift(1).tail(cfg.range_lookback).max()
        recent_low = trend_df['low'].shift(1).tail(cfg.range_lookback).min()
        breakout_up = row['close'] >= recent_high
        breakout_down = row['close'] <= recent_low
        trend_up = row['close'] > row['ema_fast'] > row['ema_slow']
        trend_down = row['close'] < row['ema_fast'] < row['ema_slow']
        if (breakout_up or breakout_down) and row['atr_rank'] >= 0.6 and row['adx'] >= 20:
            return 'EXPANSION'
        if (trend_up or trend_down) and row['adx'] >= 20:
            return 'TREND_CONT'
        if row['atr_rank'] <= 0.3 and row['bbw_rank'] <= 0.3 and row['range_rank'] <= 0.3 and row['adx'] < 20:
            return 'COMPRESSION'
        return 'CHOP'

    @staticmethod
    def _trend_row_ready(row: pd.Series) -> bool:
        required = ('ema_fast', 'ema_slow', 'adx', 'atr_rank')
        return all(pd.notna(row.get(key)) for key in required)

    @staticmethod
    def _bars_to_frame(bars: list[Any]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for bar in bars:
            if not getattr(bar, 'is_closed', False):
                continue
            rows.append(
                {
                    'ts': pd.Timestamp(int(getattr(bar, 'close_time_ms')) / 1000.0, unit='s', tz='UTC'),
                    'open': float(getattr(bar, 'open_price')),
                    'high': float(getattr(bar, 'high_price')),
                    'low': float(getattr(bar, 'low_price')),
                    'close': float(getattr(bar, 'close_price')),
                    'volume': float(getattr(bar, 'volume')),
                }
            )
        if not rows:
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        frame = pd.DataFrame(rows).drop_duplicates(subset=['ts'], keep='last').set_index('ts').sort_index()
        return frame[['open', 'high', 'low', 'close', 'volume']]

    @staticmethod
    def _atr(df: pd.DataFrame, period: int) -> pd.Series:
        prev_close = df['close'].shift(1)
        true_range = pd.concat(
            [
                df['high'] - df['low'],
                (df['high'] - prev_close).abs(),
                (df['low'] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return true_range.rolling(period).mean()

    @staticmethod
    def _adx(df: pd.DataFrame, period: int) -> pd.Series:
        up_move = df['high'].diff()
        down_move = -df['low'].diff()
        plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
        minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)
        prev_close = df['close'].shift(1)
        true_range = pd.concat(
            [
                df['high'] - df['low'],
                (df['high'] - prev_close).abs(),
                (df['low'] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr_value = true_range.rolling(period).mean()
        plus_di = 100 * (plus_dm.rolling(period).mean() / atr_value)
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr_value)
        dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di)).replace([np.inf, -np.inf], np.nan)
        return dx.rolling(period).mean()

    @staticmethod
    def _clean_float(value: Any) -> float | None:
        if value is None or pd.isna(value):
            return None
        return float(value)
