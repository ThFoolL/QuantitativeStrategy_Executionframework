from __future__ import annotations

import pandas as pd

from .models import FinalActionPlan, LiveStateSnapshot, MarketSnapshot


class V6CBaselineLiveAdapter:
    """Runtime adapter aligned with backtest `v6c-baseline`."""

    def _allow_low_activity_trend_entry(self) -> bool:
        return False

    def _trend_entry_session_tags(self) -> set[str]:
        return {'US_CORE', 'NON_US_ACTIVE'}

    def _is_us_open_impulse(self, bar_ts: str) -> bool:
        ts = pd.Timestamp(bar_ts)
        return 13 <= ts.hour < 15

    def _trim_target_fraction(self, state: LiveStateSnapshot) -> float:
        rf = state.risk_fraction or 0.0
        if rf >= state.risk_fraction_extreme - 1e-12:
            return state.risk_fraction_high / max(rf, 1e-12)
        if rf >= state.risk_fraction_high - 1e-12:
            return state.risk_fraction_medium / max(rf, 1e-12)
        if rf >= state.risk_fraction_medium - 1e-12:
            return min(0.5, 0.05 / max(rf, 1e-12))
        return 1.0

    def _session_tag(self, bar_ts: str) -> str:
        hour = int(bar_ts[11:13])
        if 13 <= hour < 20:
            return 'US_CORE'
        if 7 <= hour < 13:
            return 'NON_US_ACTIVE'
        return 'LOW_ACTIVITY'

    def _trade_grade(self, market: MarketSnapshot) -> str:
        session_tag = self._session_tag(market.bar_ts)
        structure_tag = str(market.trend_1h.get('structure_tag', 'CHOP'))
        if session_tag == 'LOW_ACTIVITY' or structure_tag == 'CHOP':
            return 'C'
        if session_tag == 'US_CORE' and structure_tag == 'EXPANSION':
            return 'S'
        if structure_tag == 'TREND_CONT':
            return 'A'
        return 'B'

    def _manage_trend_position(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan | None:
        if state.active_strategy != 'trend' or not state.active_side or state.strategy_entry_price is None or state.stop_price is None:
            return None
        if not state.can_modify_position:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'modify_blocked_by_state', requires_execution=False)

        tr = market.trend_1h
        sig = market.signal_15m
        fast = market.fast_5m or market.signal_15m
        if not tr or not sig or not fast:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'insufficient_snapshot_for_management', requires_execution=False)

        close = float(fast.get('close', sig['close']))
        high = float(fast.get('high', close))
        low = float(fast.get('low', close))
        ema_fast = float(tr['ema_fast'])
        ema_slow = float(tr['ema_slow'])
        adx = float(tr['adx'])
        structure_tag = str(tr.get('structure_tag', 'CHOP'))
        grade = self._trade_grade(market)

        entry_price = float(state.strategy_entry_price)
        stop_price = float(state.stop_price)
        risk_per_unit = float(state.risk_per_unit or 0.0)
        if risk_per_unit <= 0:
            risk_per_unit = abs(entry_price - stop_price)
        if risk_per_unit <= 0:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'invalid_risk_per_unit', requires_execution=False)

        if state.active_side == 'long':
            if low <= stop_price:
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'long', 'stop_hit', qty_mode='full_close', price_hint=stop_price, requires_execution=True)
            if close < ema_slow or (grade == 'C' and adx < 18):
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'long', 'trend_fail', qty_mode='full_close', price_hint=close, requires_execution=True)

            current_r = (close - entry_price) / risk_per_unit
            high_water_r = max(state.high_water_r, current_r)
            elapsed_minutes = (pd.Timestamp(market.bar_ts) - pd.Timestamp(state.strategy_entry_time)).total_seconds() / 60.0 if state.strategy_entry_time else 0.0
            open_impulse_fail = (
                self._is_us_open_impulse(state.strategy_entry_time or market.bar_ts)
                and elapsed_minutes <= 30
                and (
                    current_r <= -0.35
                    or (elapsed_minutes <= 15 and current_r < 0 and close < ema_fast)
                )
            )
            trend_cont_a_early_fail = (
                state.quality_bucket == 'HIGH'
                and elapsed_minutes <= 30
                and structure_tag == 'TREND_CONT'
                and grade == 'A'
                and (
                    current_r <= -0.35
                    or (elapsed_minutes >= 15 and high_water_r < 0.30 and close < ema_fast)
                )
            )
            if open_impulse_fail or trend_cont_a_early_fail:
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'long', 'open_impulse_early_fail', qty_mode='full_close', price_hint=close, requires_execution=True)
            weaken = (structure_tag == 'TREND_CONT' and grade in {'A', 'B'}) or (grade == 'C' and adx >= 18)
            severe = structure_tag == 'CHOP' or close < ema_slow or (grade == 'C' and adx < 18)
            next_degrade_state = state.degrade_state
            next_stop_price = stop_price
            next_p1_armed = state.p1_armed
            next_p2_armed = state.p2_armed
            pending_reason = 'trend_position_hold_long'
            base_ctx = {'high_water_r': high_water_r}

            if next_degrade_state == 'ATTACK' and weaken:
                next_degrade_state = 'HOLD'
                pending_reason = 'degrade_to_hold'
            elif next_degrade_state in {'ATTACK', 'HOLD'} and severe:
                trim_fraction = min(max(1.0 - self._trim_target_fraction(state), 0.0), 1.0)
                if trim_fraction > 1e-12:
                    new_stop = max(next_stop_price, ema_fast, entry_price * (1 + state.break_even_buffer))
                    return FinalActionPlan(market.decision_ts, market.bar_ts, 'trim', 'trend', 'long', 'degrade_trim', qty_mode='fractional_reduce', qty=trim_fraction, price_hint=close, stop_price=new_stop, conflict_context={**base_ctx, 'degrade_state': 'DEFENSE'}, requires_execution=True)
                next_degrade_state = 'DEFENSE'
                pending_reason = 'degrade_to_defense'
            if (not next_p1_armed) and current_r >= state.p1_trigger_r:
                next_stop_price = max(next_stop_price, entry_price + risk_per_unit * 0.25)
                next_p1_armed = True
                pending_reason = 'update_stop_p1'
            elif next_p1_armed and (not next_p2_armed) and current_r >= state.p2_trigger_r:
                next_stop_price = max(next_stop_price, ema_fast)
                next_p2_armed = True
                pending_reason = 'update_stop_p2'
            elif next_p2_armed:
                new_stop = max(next_stop_price, ema_fast)
                if new_stop > next_stop_price:
                    next_stop_price = new_stop
                    pending_reason = 'trail_stop_p2'
            base_ctx = {**base_ctx, 'degrade_state': next_degrade_state, 'stop_price': next_stop_price, 'p1_armed': next_p1_armed, 'p2_armed': next_p2_armed}
            defense_base = (state.equity_at_entry or 0.0) * state.profit_defense_start_pct
            defense_runup = high_water_r * (state.risk_amount or 0.0)
            if defense_runup >= defense_base and current_r <= high_water_r * (1.0 - state.profit_defense_giveback_pct):
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'long', 'profit_defense_exit', qty_mode='full_close', price_hint=close, requires_execution=True)
            if state.add_on_count > 0 and high_water_r >= 3.0 and current_r <= high_water_r * 0.65:
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'long', 'add_on_pullback_exit', qty_mode='full_close', price_hint=close, requires_execution=True)
            add_trigger = state.add_trigger_r_first if state.add_on_count == 0 else state.add_trigger_r_second
            if next_p1_armed and state.quality_bucket == 'HIGH' and state.add_on_count < 2 and current_r >= add_trigger:
                new_stop = max(next_stop_price, entry_price * (1 + state.break_even_buffer), ema_fast)
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'add', 'trend', 'long', f'pyramid_add_{state.add_on_count + 1}', qty_mode='risk_based_add', price_hint=close, stop_price=new_stop, risk_fraction=state.risk_fraction, conflict_context=base_ctx, requires_execution=True)
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'state_update', 'trend', 'long', pending_reason, conflict_context=base_ctx, requires_execution=False)

        if high >= stop_price:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'short', 'stop_hit', qty_mode='full_close', price_hint=stop_price, requires_execution=True)
        if close > ema_slow or (grade == 'C' and adx < 18):
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'short', 'trend_fail', qty_mode='full_close', price_hint=close, requires_execution=True)

        current_r = (entry_price - close) / risk_per_unit
        high_water_r = max(state.high_water_r, current_r)
        elapsed_minutes = (pd.Timestamp(market.bar_ts) - pd.Timestamp(state.strategy_entry_time)).total_seconds() / 60.0 if state.strategy_entry_time else 0.0
        open_impulse_fail = (
            self._is_us_open_impulse(state.strategy_entry_time or market.bar_ts)
            and elapsed_minutes <= 30
            and (
                current_r <= -0.35
                or (elapsed_minutes <= 15 and current_r < 0 and close > ema_fast)
            )
        )
        trend_cont_a_early_fail = (
            state.quality_bucket == 'HIGH'
            and elapsed_minutes <= 30
            and structure_tag == 'TREND_CONT'
            and grade == 'A'
            and (
                current_r <= -0.35
                or (elapsed_minutes >= 15 and high_water_r < 0.30 and close > ema_fast)
            )
        )
        if open_impulse_fail or trend_cont_a_early_fail:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'short', 'open_impulse_early_fail', qty_mode='full_close', price_hint=close, requires_execution=True)
        weaken = (structure_tag == 'TREND_CONT' and grade in {'A', 'B'}) or (grade == 'C' and adx >= 18)
        severe = structure_tag == 'CHOP' or close > ema_slow or (grade == 'C' and adx < 18)
        next_degrade_state = state.degrade_state
        next_stop_price = stop_price
        next_p1_armed = state.p1_armed
        next_p2_armed = state.p2_armed
        pending_reason = 'trend_position_hold_short'
        base_ctx = {'high_water_r': high_water_r}

        if next_degrade_state == 'ATTACK' and weaken:
            next_degrade_state = 'HOLD'
            pending_reason = 'degrade_to_hold'
        elif next_degrade_state in {'ATTACK', 'HOLD'} and severe:
            trim_fraction = min(max(1.0 - self._trim_target_fraction(state), 0.0), 1.0)
            if trim_fraction > 1e-12:
                new_stop = min(next_stop_price, ema_fast, entry_price * (1 - state.break_even_buffer))
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'trim', 'trend', 'short', 'degrade_trim', qty_mode='fractional_reduce', qty=trim_fraction, price_hint=close, stop_price=new_stop, conflict_context={**base_ctx, 'degrade_state': 'DEFENSE'}, requires_execution=True)
            next_degrade_state = 'DEFENSE'
            pending_reason = 'degrade_to_defense'
        if (not next_p1_armed) and current_r >= state.p1_trigger_r:
            next_stop_price = min(next_stop_price, entry_price - risk_per_unit * 0.25)
            next_p1_armed = True
            pending_reason = 'update_stop_p1'
        elif next_p1_armed and (not next_p2_armed) and current_r >= state.p2_trigger_r:
            next_stop_price = min(next_stop_price, ema_fast)
            next_p2_armed = True
            pending_reason = 'update_stop_p2'
        elif next_p2_armed:
            new_stop = min(next_stop_price, ema_fast)
            if new_stop < next_stop_price - 1e-12:
                next_stop_price = new_stop
                pending_reason = 'trail_stop_p2'
        base_ctx = {**base_ctx, 'degrade_state': next_degrade_state, 'stop_price': next_stop_price, 'p1_armed': next_p1_armed, 'p2_armed': next_p2_armed}
        defense_base = (state.equity_at_entry or 0.0) * state.profit_defense_start_pct
        defense_runup = high_water_r * (state.risk_amount or 0.0)
        if defense_runup >= defense_base and current_r <= high_water_r * (1.0 - state.profit_defense_giveback_pct):
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'short', 'profit_defense_exit', qty_mode='full_close', price_hint=close, requires_execution=True)
        if state.add_on_count > 0 and high_water_r >= 3.0 and current_r <= high_water_r * 0.65:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'trend', 'short', 'add_on_pullback_exit', qty_mode='full_close', price_hint=close, requires_execution=True)
        add_trigger = state.add_trigger_r_first if state.add_on_count == 0 else state.add_trigger_r_second
        if next_p1_armed and state.quality_bucket == 'HIGH' and state.add_on_count < 2 and current_r >= add_trigger:
            new_stop = min(next_stop_price, entry_price * (1 - state.break_even_buffer), ema_fast)
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'add', 'trend', 'short', f'pyramid_add_{state.add_on_count + 1}', qty_mode='risk_based_add', price_hint=close, stop_price=new_stop, risk_fraction=state.risk_fraction, conflict_context=base_ctx, requires_execution=True)
        return FinalActionPlan(market.decision_ts, market.bar_ts, 'state_update', 'trend', 'short', pending_reason, conflict_context=base_ctx, requires_execution=False)

    def _manage_rev_position(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan | None:
        if state.active_strategy != 'rev' or not state.active_side or state.strategy_entry_price is None or state.stop_price is None or state.tp_price is None:
            return None
        if not state.can_modify_position:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'modify_blocked_by_state', requires_execution=False)

        bar = market.fast_5m or market.signal_15m
        if not bar:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'insufficient_snapshot_for_rev_management', requires_execution=False)

        high = float(bar.get('high', market.current_price))
        low = float(bar.get('low', market.current_price))
        close = float(bar.get('close', market.current_price))
        stop_price = float(state.stop_price)
        tp_price = float(state.tp_price)
        hold_bars = int(state.hold_bars) + 1

        if state.active_side == 'long':
            if low <= stop_price:
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'rev', 'long', 'stop_hit', qty_mode='full_close', price_hint=stop_price, requires_execution=True)
            if high >= tp_price:
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'rev', 'long', 'tp1_hit', qty_mode='full_close', price_hint=tp_price, requires_execution=True)
            if hold_bars >= 96:
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'rev', 'long', 'time_exit', qty_mode='full_close', price_hint=close, requires_execution=True)
            return FinalActionPlan(
                market.decision_ts,
                market.bar_ts,
                'hold',
                None,
                None,
                'rev_position_hold_long',
                conflict_context={'hold_bars': hold_bars},
                requires_execution=False,
            )

        if high >= stop_price:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'rev', 'short', 'stop_hit', qty_mode='full_close', price_hint=stop_price, requires_execution=True)
        if low <= tp_price:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'rev', 'short', 'tp1_hit', qty_mode='full_close', price_hint=tp_price, requires_execution=True)
        if hold_bars >= 96:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'close', 'rev', 'short', 'time_exit', qty_mode='full_close', price_hint=close, requires_execution=True)
        return FinalActionPlan(
            market.decision_ts,
            market.bar_ts,
            'hold',
            None,
            None,
            'rev_position_hold_short',
            conflict_context={'hold_bars': hold_bars},
            requires_execution=False,
        )

    def _plan_trend_entry(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan:
        tr = market.trend_1h
        sig = market.signal_15m
        hist = market.signal_15m_history
        if not tr or not sig or len(hist) < 4:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'insufficient_snapshot_data', requires_execution=False)

        signal_ts = market.signal_15m_ts or market.bar_ts
        session_tag = self._session_tag(market.bar_ts)
        if session_tag == 'LOW_ACTIVITY' and not self._allow_low_activity_trend_entry():
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'low_activity_block', requires_execution=False)
        if state.last_trend_signal_ts is not None and signal_ts == state.last_trend_signal_ts:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'duplicate_trend_signal_ts', requires_execution=False)

        prev2, prev, current = hist[-3], hist[-2], hist[-1]
        swing_low = min(x['low'] for x in hist[:-1])
        swing_high = max(x['high'] for x in hist[:-1])
        required_trend_fields = ('close', 'ema_fast', 'ema_slow', 'adx', 'atr_rank')
        if any(tr.get(field) in (None, '') for field in required_trend_fields):
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'trend_features_incomplete', requires_execution=False)

        close = float(sig['close'])
        trend_close = float(tr['close'])
        ema_fast = float(tr['ema_fast'])
        ema_slow = float(tr['ema_slow'])
        adx = float(tr['adx'])
        atr_rank = float(tr['atr_rank'])
        structure_tag = str(tr.get('structure_tag', 'CHOP'))

        entry_sessions = self._trend_entry_session_tags()
        long_expansion_bias = (trend_close > ema_fast > ema_slow and structure_tag == 'EXPANSION' and adx >= state.adx_long_threshold and atr_rank >= state.atr_rank_long_threshold and session_tag in entry_sessions)
        long_trend_cont_bias = (trend_close > ema_fast > ema_slow and structure_tag == 'TREND_CONT' and adx >= state.adx_trend_cont_long_threshold and atr_rank >= state.atr_rank_trend_cont_long_threshold and session_tag in entry_sessions)
        short_expansion_bias = (trend_close < ema_fast < ema_slow and structure_tag == 'EXPANSION' and adx >= state.adx_short_threshold and atr_rank >= state.atr_rank_short_threshold and session_tag in entry_sessions)
        short_trend_cont_bias = (trend_close < ema_fast < ema_slow and structure_tag == 'TREND_CONT' and adx >= state.adx_trend_cont_short_threshold and atr_rank >= state.atr_rank_trend_cont_short_threshold and session_tag in entry_sessions)

        if long_expansion_bias or long_trend_cont_bias:
            recent_push = hist[-4]['close'] < hist[-3]['close'] < hist[-2]['close']
            shallow_pullback = close > ema_fast and float(current['low']) > swing_low * 0.997 and close >= float(prev['close'])
            confirm = close > float(prev['high']) or (close > float(prev['close']) and float(prev['close']) > float(prev2['close']))
            breakout_follow = long_expansion_bias and close > float(prev['high']) and close > float(hist[-4]['high']) and float(prev['close']) > float(prev2['close'])
            pullback_follow = recent_push and shallow_pullback and confirm
            if breakout_follow or pullback_follow:
                risk_fraction = state.risk_fraction_extreme if (long_expansion_bias and session_tag == 'US_CORE' and atr_rank >= 0.7 and adx >= 26 and breakout_follow) else (state.risk_fraction_high if (long_trend_cont_bias and pullback_follow) else state.risk_fraction_medium)
                stop_anchor = min(float(swing_low), float(ema_fast * (1 - 0.007)))
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'open', 'trend', 'long', 'trend_long_entry', qty_mode='risk_based', price_hint=close, stop_price=stop_anchor, risk_fraction=risk_fraction, conflict_context={'signal_ts': signal_ts}, requires_execution=True)

        if short_expansion_bias or short_trend_cont_bias:
            recent_push = hist[-4]['close'] > hist[-3]['close'] > hist[-2]['close']
            shallow_pullback = close < ema_fast and float(current['high']) < swing_high * 1.003 and close <= float(prev['close'])
            confirm = close < float(prev['low']) or (close < float(prev['close']) and float(prev['close']) < float(prev2['close']))
            breakout_follow = short_expansion_bias and close < float(prev['low']) and close < float(hist[-4]['low']) and float(prev['close']) < float(prev2['close'])
            ema_slow_gap_short_pullback_pct = (ema_slow - close) / max(close, 1e-9)
            ema_stack_short_pullback_pct = (ema_slow - ema_fast) / max(close, 1e-9)
            short_pullback_expand_ok = (
                atr_rank >= 0.60
                and ema_slow_gap_short_pullback_pct >= 0.02
                and ema_stack_short_pullback_pct >= 0.008
            )
            pullback_follow = recent_push and shallow_pullback and confirm and ((not short_trend_cont_bias) or short_pullback_expand_ok)
            if breakout_follow or pullback_follow:
                risk_fraction = state.risk_fraction_extreme if (short_expansion_bias and atr_rank >= 0.65 and adx >= 24 and breakout_follow) else (state.risk_fraction_high if (short_trend_cont_bias and pullback_follow) else state.risk_fraction_medium)
                risk_fraction *= 0.8
                stop_anchor = max(float(swing_high), float(ema_fast * (1 + 0.007)))
                return FinalActionPlan(market.decision_ts, market.bar_ts, 'open', 'trend', 'short', 'trend_short_entry', qty_mode='risk_based', price_hint=close * (1 - 0.0002), stop_price=stop_anchor * (1 + 0.0002), risk_fraction=risk_fraction, conflict_context={'signal_ts': signal_ts}, requires_execution=True)

        return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'no_supported_trend_signal', requires_execution=False)

    def _plan_rev_entry(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan:
        cand = market.rev_candidate
        if not cand:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'no_rev_candidate', requires_execution=False)
        side = str(cand['side'])
        entry = float(cand['entry'])
        stop = float(cand['stop'])
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'invalid_rev_candidate', requires_execution=False)
        risk_fraction = 0.10 if int(cand.get('value_window_15m', 24)) == 24 else 0.05
        tp_price = entry + risk_per_unit if side == 'long' else entry - risk_per_unit
        return FinalActionPlan(
            market.decision_ts,
            market.bar_ts,
            'open',
            'rev',
            side,
            'rev_entry',
            qty_mode='risk_based',
            price_hint=entry,
            stop_price=stop,
            risk_fraction=risk_fraction,
            conflict_context={'tp_price': tp_price, 'rev_window': int(cand.get('value_window_15m', 24))},
            requires_execution=True,
        )

    def _resolve_entry_conflict(self, trend_plan: FinalActionPlan, rev_plan: FinalActionPlan) -> FinalActionPlan:
        if trend_plan.action_type == 'open' and rev_plan.action_type == 'open':
            return FinalActionPlan(
                trend_plan.plan_ts,
                trend_plan.bar_ts,
                'open',
                trend_plan.target_strategy,
                trend_plan.target_side,
                'same_bar_trend_priority',
                qty_mode=trend_plan.qty_mode,
                qty=trend_plan.qty,
                price_hint=trend_plan.price_hint,
                stop_price=trend_plan.stop_price,
                risk_fraction=trend_plan.risk_fraction,
                conflict_context={'blocked_plan': 'rev_open_same_bar'},
                requires_execution=trend_plan.requires_execution,
            )
        if trend_plan.action_type == 'open':
            return trend_plan
        if rev_plan.action_type == 'open':
            return rev_plan
        return trend_plan if trend_plan.action_type != 'hold' else rev_plan

    def build_plan_debug(self, *, market: MarketSnapshot, state: LiveStateSnapshot, final_plan: FinalActionPlan) -> dict[str, object]:
        trend_plan = self._plan_trend_entry(market, state)
        rev_plan = self._plan_rev_entry(market, state)
        resolved_plan = self._resolve_entry_conflict(trend_plan, rev_plan)
        if final_plan.reason == 'same_bar_trend_priority':
            conflict_resolution = {
                'winner': 'trend',
                'why_final_selected': 'same_bar_trend_priority',
                'blocked_plan': ((resolved_plan.conflict_context or {}).get('blocked_plan')),
            }
        elif final_plan.target_strategy in {'trend', 'rev'}:
            conflict_resolution = {
                'winner': final_plan.target_strategy,
                'why_final_selected': final_plan.reason,
                'blocked_plan': None,
            }
        else:
            conflict_resolution = {
                'winner': None,
                'why_final_selected': final_plan.reason,
                'blocked_plan': None,
            }
        return {
            'trend_plan': self._plan_to_debug_dict(trend_plan),
            'rev_plan': self._plan_to_debug_dict(rev_plan),
            'final_plan': self._plan_to_debug_dict(final_plan),
            'conflict_resolution': conflict_resolution,
        }

    @staticmethod
    def _plan_to_debug_dict(plan: FinalActionPlan) -> dict[str, object | None]:
        return {
            'action_type': plan.action_type,
            'target_strategy': plan.target_strategy,
            'target_side': plan.target_side,
            'reason': plan.reason,
            'qty_mode': plan.qty_mode,
            'qty': plan.qty,
            'price_hint': plan.price_hint,
            'stop_price': plan.stop_price,
            'risk_fraction': plan.risk_fraction,
            'requires_execution': plan.requires_execution,
            'close_reason': plan.close_reason,
            'conflict_context': plan.conflict_context,
        }

    def plan(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan:
        if state.consistency_status != 'OK':
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, f'frozen_or_inconsistent:{state.consistency_status}', requires_execution=False)

        trend_manage = self._manage_trend_position(market, state)
        rev_manage = self._manage_rev_position(market, state)

        if state.active_strategy == 'trend':
            if state.can_open_new_position:
                rev_entry = self._plan_rev_entry(market, state)
                if rev_entry.action_type == 'open' and rev_entry.target_side != state.active_side:
                    return FinalActionPlan(
                        rev_entry.plan_ts,
                        rev_entry.bar_ts,
                        'flip',
                        'rev',
                        rev_entry.target_side,
                        'reverse_signal_flip_to_rev',
                        qty_mode=rev_entry.qty_mode,
                        qty=rev_entry.qty,
                        price_hint=rev_entry.price_hint,
                        stop_price=rev_entry.stop_price,
                        risk_fraction=rev_entry.risk_fraction,
                        conflict_context=rev_entry.conflict_context,
                        requires_execution=True,
                        close_reason='conflict_flip_to_rev',
                    )
            if trend_manage:
                return trend_manage
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'trend_management_fallback', requires_execution=False)

        if state.active_strategy == 'rev':
            if state.can_open_new_position:
                trend_entry = self._plan_trend_entry(market, state)
                if trend_entry.action_type == 'open':
                    reason = 'reverse_signal_flip_to_trend' if trend_entry.target_side != state.active_side else 'same_side_trend_priority_takeover'
                    close_reason = 'conflict_flip_to_trend' if trend_entry.target_side != state.active_side else 'conflict_same_side_trend_priority'
                    return FinalActionPlan(
                        trend_entry.plan_ts,
                        trend_entry.bar_ts,
                        'flip',
                        'trend',
                        trend_entry.target_side,
                        reason,
                        qty_mode=trend_entry.qty_mode,
                        qty=trend_entry.qty,
                        price_hint=trend_entry.price_hint,
                        stop_price=trend_entry.stop_price,
                        risk_fraction=trend_entry.risk_fraction,
                        conflict_context=trend_entry.conflict_context,
                        requires_execution=True,
                        close_reason=close_reason,
                    )
            if rev_manage:
                return rev_manage
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'rev_management_fallback', requires_execution=False)

        if not state.can_open_new_position:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'open_blocked_by_state', requires_execution=False)
        if state.last_signal_bar == market.bar_ts:
            return FinalActionPlan(market.decision_ts, market.bar_ts, 'hold', None, None, 'duplicate_signal_bar', requires_execution=False)

        trend_plan = self._plan_trend_entry(market, state)
        rev_plan = self._plan_rev_entry(market, state)
        return self._resolve_entry_conflict(trend_plan, rev_plan)
