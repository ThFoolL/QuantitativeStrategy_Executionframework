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
    rev_band_buffer_frac: float = 0.2


class LiveFeatureBuilder:
    """从只读 kline 构造 live 策略特征集。

    trend features 继续使用 live adapter 当前所需的最小实时特征。

    rev_candidate 必须复刻 baseline 策略层 `strategies/s1_formal_v6c/reversal_runtime.py`
    的候选生成、close30 过滤与同 timestamp 去重语义。这里不允许再使用
    `shared_formal_lite` 近似候选，否则会让 execution framework 执行出策略层
    不会产生的 rev_entry。
    """

    def __init__(self, config: LiveFeatureConfig | None = None):
        self.config = config or LiveFeatureConfig()

    def build(self, *, symbol: str, trend_bars: list[Any], signal_bars: list[Any]) -> dict[str, Any]:
        trend_df = self._bars_to_frame(trend_bars)
        signal_df = self._bars_to_open_frame(signal_bars)
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
        # Baseline trend runtime consumes right-labeled / closed-right 1h bars.
        # For already-aggregated 1h klines, feature indexing must therefore align to
        # bar close semantics rather than bar open semantics.
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
        # baseline reversal strategy is defined on 5m bars indexed by bar-open timestamp and
        # internally resamples those 5m bars to 15m/1h. Reusing 15m bars or close_time indices here
        # silently changes touch/depth/filter semantics, so rev parity must build from 5m open time.
        df_5m = self._bars_to_open_frame(signal_bars)
        if len(df_5m) < 12:
            return None

        signals: list[pd.DataFrame] = []
        for window in self.config.rev_windows_15m:
            cand = self._generate_baseline_rev_candidates(df_5m, value_window_15m=int(window))
            if cand.empty:
                continue
            sig = self._apply_baseline_close30_filter(cand)
            if not sig.empty:
                signals.append(sig)
        if not signals:
            return None

        merged = pd.concat(signals, ignore_index=True)
        deduped = self._baseline_dedup_by_ts(merged)
        if deduped.empty:
            return None

        current_ts = df_5m.index[-1]
        current_rows = deduped[pd.to_datetime(deduped['ts'], utc=True) == current_ts]
        if current_rows.empty:
            return None

        chosen = current_rows.iloc[0]
        risk = float(chosen['risk'])
        if risk <= 0:
            return None
        return {
            'ts': pd.Timestamp(chosen['ts']).isoformat(),
            'side': str(chosen['side']),
            'entry': float(chosen['entry']),
            'stop': float(chosen['stop']),
            'tp1': float(chosen['tp1']),
            'risk': risk,
            'value_window_15m': int(chosen['value_window_15m']),
            'score': float(chosen['score']),
            'depth_in_band_frac': float(chosen['depth_in_band_frac']),
            'retrace_side': float(chosen['retrace_side']),
            'close_away_from_edge': float(chosen['close_away_from_edge']),
            'reversion_space': float(chosen['reversion_space']),
            'body_frac': float(chosen['body_frac']),
            'fav_r_30m': float(chosen['fav_r_30m']),
            'adv_r_30m': float(chosen['adv_r_30m']),
            'fav_r_60m': float(chosen['fav_r_60m']),
            'adv_r_60m': float(chosen['adv_r_60m']),
            'close_r_30m': float(chosen['close_r_30m']),
            'core_mid': float(chosen['core_mid']),
            'total_width': float(chosen['total_width']),
            'source': 'live_feature_builder',
            'strategy_source': 'baseline_reversal_runtime',
            'formal_alignment': 'baseline_reversal_runtime_exact',
            'approximation': 'none',
            'notes': [
                'baseline_generate_candidates',
                'baseline_apply_close30_filter',
                'baseline_dedup_by_ts',
                'no_shared_formal_lite',
            ],
        }

    def _generate_baseline_rev_candidates(self, df_5m: pd.DataFrame, *, value_window_15m: int) -> pd.DataFrame:
        cfg = self.config
        df_15m = df_5m.resample('15min', label='right', closed='right').agg(
            {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
        ).dropna()
        regime = self._build_baseline_rev_regime(df_5m)

        rows: list[dict[str, Any]] = []
        for i in range(value_window_15m, len(df_15m)):
            ts = df_15m.index[i]
            row = df_15m.iloc[i]
            oneh = regime[regime.index <= ts]
            if oneh.empty or not bool(oneh.iloc[-1]['range_ok']):
                continue
            hist = df_15m.iloc[i - value_window_15m : i]
            vals = hist['close'].to_numpy()
            low, mid, high, core_width = self._baseline_compute_zone(vals)
            if not np.isfinite(core_width) or core_width <= 0:
                continue
            total_low = low - cfg.rev_band_buffer_frac * core_width
            total_high = high + cfg.rev_band_buffer_frac * core_width
            total_width = total_high - total_low
            if total_width <= 0:
                continue

            long_touch = row['low'] <= low
            short_touch = row['high'] >= high
            if not (long_touch or short_touch):
                continue

            fut = df_5m[df_5m.index > ts].iloc[:96]
            fut_30 = fut.iloc[:6]
            fut_60 = fut.iloc[:12]
            if len(fut_60) < 6:
                continue

            candle_range = max(float(row['high'] - row['low']), 1e-9)
            body_frac = abs(float(row['close'] - row['open'])) / total_width

            def append_side(side: str, entry: float, stop: float, depth: float, retrace: float, away: float, rev_space: float) -> None:
                risk = entry - stop if side == 'long' else stop - entry
                if risk <= 0:
                    return
                if side == 'long':
                    fav30 = (float(fut_30['high'].max()) - entry) / risk
                    adv30 = (entry - float(fut_30['low'].min())) / risk
                    fav60 = (float(fut_60['high'].max()) - entry) / risk
                    adv60 = (entry - float(fut_60['low'].min())) / risk
                    close30 = (float(fut_30.iloc[-1]['close']) - entry) / risk
                else:
                    fav30 = (entry - float(fut_30['low'].min())) / risk
                    adv30 = (float(fut_30['high'].max()) - entry) / risk
                    fav60 = (entry - float(fut_60['low'].min())) / risk
                    adv60 = (float(fut_60['high'].max()) - entry) / risk
                    close30 = (entry - float(fut_30.iloc[-1]['close'])) / risk
                rows.append(
                    {
                        'ts': ts,
                        'side': side,
                        'entry': entry,
                        'stop': stop,
                        'risk': risk,
                        'tp1': entry + risk if side == 'long' else entry - risk,
                        'depth_in_band_frac': depth,
                        'retrace_side': retrace,
                        'close_away_from_edge': away,
                        'reversion_space': rev_space,
                        'body_frac': body_frac,
                        'fav_r_30m': fav30,
                        'adv_r_30m': adv30,
                        'fav_r_60m': fav60,
                        'adv_r_60m': adv60,
                        'close_r_30m': close30,
                        'core_mid': mid,
                        'total_width': total_width,
                        'value_window_15m': value_window_15m,
                    }
                )

            if long_touch:
                depth = min(max((low - float(row['low'])) / max(low - total_low, 1e-9), 0.0), 3.0)
                retr = (float(row['close'] - row['low'])) / candle_range
                away = (float(row['close']) - total_low) / total_width
                rev_space = (mid - float(row['close'])) / total_width
                stop = float(total_low - 0.15 * total_width)
                append_side('long', float(row['close']), stop, depth, retr, away, rev_space)

            if short_touch:
                depth = min(max((float(row['high']) - high) / max(total_high - high, 1e-9), 0.0), 3.0)
                retr = (float(row['high'] - row['close'])) / candle_range
                away = (total_high - float(row['close'])) / total_width
                rev_space = (float(row['close']) - mid) / total_width
                stop = float(total_high + 0.15 * total_width)
                append_side('short', float(row['close']), stop, depth, retr, away, rev_space)

        out = pd.DataFrame(rows)
        if out.empty:
            return out
        return out.sort_values(['ts', 'side']).reset_index(drop=True)

    def _build_baseline_rev_regime(self, df_5m: pd.DataFrame) -> pd.DataFrame:
        df_1h = df_5m.resample('1h', label='right', closed='right').agg(
            {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
        ).dropna()
        ema_fast = df_1h['close'].ewm(span=20, adjust=False).mean()
        ema_slow = df_1h['close'].ewm(span=50, adjust=False).mean()
        prev_close = df_1h['close'].shift(1)
        tr = pd.concat(
            [
                df_1h['high'] - df_1h['low'],
                (df_1h['high'] - prev_close).abs(),
                (df_1h['low'] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(14).mean()
        atr_pct = atr / df_1h['close']
        atr_rank = atr_pct.rolling(120).rank(pct=True)
        ema_slow_gap_pct = (df_1h['close'] - ema_slow).abs() / df_1h['close'] * 100.0
        ema_stack_pct = (ema_fast - ema_slow).abs() / df_1h['close'] * 100.0
        return pd.DataFrame(
            {
                'range_ok': (atr_rank <= 0.5) & (ema_slow_gap_pct <= 1.5) & (ema_stack_pct <= 0.45),
            },
            index=df_1h.index,
        )

    @staticmethod
    def _baseline_compute_zone(hist_close: np.ndarray, q_low: float = 0.2, q_mid: float = 0.5, q_high: float = 0.8) -> tuple[float, float, float, float]:
        low, mid, high = np.quantile(hist_close, [q_low, q_mid, q_high])
        width = high - low
        return float(low), float(mid), float(high), float(width)

    @staticmethod
    def _apply_baseline_close30_filter(cand: pd.DataFrame) -> pd.DataFrame:
        mask = (
            (cand['depth_in_band_frac'] >= 0.25)
            & (cand['retrace_side'] >= 0.85)
            & (cand['close_away_from_edge'] >= 0.20)
            & (cand['body_frac'] <= 0.50)
            & (cand['adv_r_30m'] <= 0.35)
            & (cand['adv_r_60m'] <= 0.50)
            & (cand['fav_r_30m'] >= 0.35)
            & (cand['close_r_30m'] >= 0.00)
        )
        return cand[mask].copy()

    @staticmethod
    def _baseline_dedup_by_ts(sig: pd.DataFrame) -> pd.DataFrame:
        if sig.empty:
            return sig
        s = sig.copy()
        s['score'] = s['retrace_side'] * 2 + s['fav_r_30m'] - s['adv_r_30m']
        kept = []
        for _, g in s.groupby('ts', sort=True):
            kept.append(g.sort_values('score', ascending=False).iloc[0])
        return pd.DataFrame(kept).sort_values('ts').reset_index(drop=True)

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
    def _bars_to_open_frame(bars: list[Any]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for bar in bars:
            if not getattr(bar, 'is_closed', False):
                continue
            rows.append(
                {
                    'ts': pd.Timestamp(int(getattr(bar, 'open_time_ms')) / 1000.0, unit='s', tz='UTC'),
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
