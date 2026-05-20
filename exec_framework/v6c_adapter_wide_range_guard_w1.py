from __future__ import annotations

from typing import Any

import pandas as pd

from .models import FinalActionPlan, LiveStateSnapshot, MarketSnapshot
from .v6c_adapter_baseline import V6CBaselineLiveAdapter


class V6CWideRangeGuardW1LiveAdapter(V6CBaselineLiveAdapter):
    """Runtime adapter for `WIDE_RANGE_GUARD_W1`.

    W1 is a defensive overlay on top of v6c baseline:
    - keep all baseline trend/reversal management unchanged
    - when a baseline `TREND_OPEN` would be emitted, block it if the latest
      5m window is wide and directionally inefficient

    Parameters are aligned with the accepted research version:
    - lookback: 48 x 5m bars
    - ATR window: 14
    - box_width_atr >= 3.0
    - trend_efficiency <= 0.45
    """

    WR_LOOKBACK_BARS = 48
    WR_ATR_WINDOW = 14
    WR_BOX_WIDTH_ATR_THRESHOLD = 3.0
    WR_TREND_EFFICIENCY_THRESHOLD = 0.45

    def _float_or_none(self, value: Any) -> float | None:
        try:
            if value is None or pd.isna(value):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _precomputed_wide_range_features(self, market: MarketSnapshot) -> dict[str, Any] | None:
        for source_name, source in (
            ('fast_5m', market.fast_5m),
            ('signal_15m', market.signal_15m),
            ('trend_1h', market.trend_1h),
        ):
            if not source:
                continue
            box_width_atr = self._float_or_none(source.get('wr_box_width_atr'))
            trend_efficiency = self._float_or_none(source.get('wr_trend_efficiency', source.get('trend_efficiency')))
            if box_width_atr is None or trend_efficiency is None:
                continue
            return {
                'source': source_name,
                'box_width_atr': box_width_atr,
                'trend_efficiency': trend_efficiency,
                'box_width': self._float_or_none(source.get('wr_box_width')),
                'atr': self._float_or_none(source.get('wr_atr')),
                'rolling_high': self._float_or_none(source.get('wr_rolling_high')),
                'rolling_low': self._float_or_none(source.get('wr_rolling_low')),
            }
        return None

    def _computed_wide_range_features(self, market: MarketSnapshot) -> dict[str, Any] | None:
        hist = list(getattr(market, 'fast_5m_history', []) or [])
        if len(hist) < self.WR_LOOKBACK_BARS:
            return None
        rows = hist[-self.WR_LOOKBACK_BARS :]
        try:
            df = pd.DataFrame(rows)
            for col in ('high', 'low', 'close'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[['high', 'low', 'close']].isna().any().any():
                return None
            rolling_high = float(df['high'].max())
            rolling_low = float(df['low'].min())
            box_width = rolling_high - rolling_low
            if box_width <= 0:
                return None
            close_now = float(df['close'].iloc[-1])
            close_start = float(df['close'].iloc[0])
            trend_efficiency = abs(close_now - close_start) / box_width

            atr_rows = hist[-max(self.WR_LOOKBACK_BARS, self.WR_ATR_WINDOW + 1) :]
            atr_df = pd.DataFrame(atr_rows)
            for col in ('high', 'low', 'close'):
                atr_df[col] = pd.to_numeric(atr_df[col], errors='coerce')
            prev_close = atr_df['close'].shift(1)
            tr = pd.concat(
                [
                    atr_df['high'] - atr_df['low'],
                    (atr_df['high'] - prev_close).abs(),
                    (atr_df['low'] - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)
            atr = float(tr.tail(self.WR_ATR_WINDOW).mean())
            if atr <= 0:
                return None
            box_width_atr = box_width / atr
            return {
                'source': 'fast_5m_history',
                'box_width_atr': box_width_atr,
                'trend_efficiency': trend_efficiency,
                'box_width': box_width,
                'atr': atr,
                'rolling_high': rolling_high,
                'rolling_low': rolling_low,
            }
        except (KeyError, TypeError, ValueError):
            return None

    def _wide_range_guard_w1_features(self, market: MarketSnapshot) -> dict[str, Any] | None:
        return self._precomputed_wide_range_features(market) or self._computed_wide_range_features(market)

    def _wide_range_guard_w1_should_block(self, market: MarketSnapshot) -> tuple[bool, dict[str, Any]]:
        features = self._wide_range_guard_w1_features(market)
        if not features:
            return False, {'wide_range_guard_w1': 'missing_features'}
        should_block = (
            features['box_width_atr'] >= self.WR_BOX_WIDTH_ATR_THRESHOLD
            and features['trend_efficiency'] <= self.WR_TREND_EFFICIENCY_THRESHOLD
        )
        context = {
            'wide_range_guard_w1': bool(should_block),
            'wr_source': features['source'],
            'wr_lookback_bars': self.WR_LOOKBACK_BARS,
            'wr_atr_window': self.WR_ATR_WINDOW,
            'wr_box_width_atr_threshold': self.WR_BOX_WIDTH_ATR_THRESHOLD,
            'wr_trend_efficiency_threshold': self.WR_TREND_EFFICIENCY_THRESHOLD,
            'wr_box_width_atr': features['box_width_atr'],
            'wr_trend_efficiency': features['trend_efficiency'],
            'wr_box_width': features.get('box_width'),
            'wr_atr': features.get('atr'),
            'wr_rolling_high': features.get('rolling_high'),
            'wr_rolling_low': features.get('rolling_low'),
        }
        return should_block, context

    def _resolve_entry_conflict(self, trend_plan: FinalActionPlan, rev_plan: FinalActionPlan) -> FinalActionPlan:
        if trend_plan.reason == 'wide_range_guard_w1_block':
            return trend_plan
        return super()._resolve_entry_conflict(trend_plan, rev_plan)

    def _plan_trend_entry(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan:
        plan = super()._plan_trend_entry(market, state)
        if plan.action_type != 'open' or plan.target_strategy != 'trend':
            return plan

        should_block, context = self._wide_range_guard_w1_should_block(market)
        if not should_block:
            if plan.conflict_context is None:
                plan.conflict_context = {}
            plan.conflict_context = {**plan.conflict_context, **context}
            return plan

        return FinalActionPlan(
            market.decision_ts,
            market.bar_ts,
            'hold',
            None,
            None,
            'wide_range_guard_w1_block',
            conflict_context={**(plan.conflict_context or {}), **context, 'blocked_plan': plan.reason, 'blocked_side': plan.target_side},
            requires_execution=False,
        )
