from __future__ import annotations

from .models import ExecutionResult, FinalActionPlan, LiveStateSnapshot, MarketSnapshot


DEFAULT_LEVERAGE = 20.0
# Only local strategy/control fields may flow from plan context into persisted state.
STATE_UPDATE_WHITELIST = {
    'signal_ts',
    'tp_price',
    'rev_window',
    'high_water_r',
    'degrade_state',
    'p1_armed',
    'p2_armed',
    'stop_price',
    'equity_at_entry',
    'risk_amount',
    'risk_per_unit',
    'last_conflict_resolution',
}


class InMemoryStateStore:
    def __init__(self, initial_state: LiveStateSnapshot):
        self.state = initial_state
        self.last_result = None

    def load_state(self) -> LiveStateSnapshot:
        return self.state

    def save_state(self, state: LiveStateSnapshot) -> None:
        self.state = state

    def save_result(self, state: LiveStateSnapshot, result: ExecutionResult) -> None:
        if result.state_updates:
            for key, value in result.state_updates.items():
                setattr(state, key, value)
        self.state = state
        self.last_result = result


class HoldStrategyModule:
    def plan(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan:
        return FinalActionPlan(
            plan_ts=market.decision_ts,
            bar_ts=market.bar_ts,
            action_type='hold',
            target_strategy=None,
            target_side=None,
            reason='placeholder',
            requires_execution=False,
        )


class MockExecutorModule:
    """Local-only executor used for replay/smoke/regression.

    Important boundary:
    - It materializes strategy intent into state immediately for local closed-loop tests.
    - Real executor must not assume `plan -> filled -> persisted` in one step.
    - Real executor should confirm fills / position / average price / fees from exchange
      callbacks or reconciliation before writing final position state.
    """

    def _filter_state_patch(self, payload: dict | None) -> dict:
        if not payload:
            return {}
        return {key: value for key, value in payload.items() if key in STATE_UPDATE_WHITELIST}

    def _open_state_updates(self, plan: FinalActionPlan, market: MarketSnapshot, state: LiveStateSnapshot) -> dict:
        updates = {
            'active_strategy': plan.target_strategy,
            'active_side': plan.target_side,
            'strategy_entry_time': market.bar_ts,
            'strategy_entry_price': plan.price_hint,
            'stop_price': plan.stop_price,
            'risk_fraction': plan.risk_fraction,
            'last_signal_bar': market.bar_ts,
            'freeze_reason': None,
        }
        if plan.target_strategy == 'trend':
            quality_bucket = 'HIGH' if (plan.risk_fraction or 0.0) >= 0.16 else 'MEDIUM'
            signal_ts = (plan.conflict_context or {}).get('signal_ts')
            equity_at_entry = float((plan.conflict_context or {}).get('equity_at_entry', state.account_equity))
            risk_per_unit = None
            if plan.price_hint is not None and plan.stop_price is not None:
                risk_per_unit = abs(float(plan.price_hint) - float(plan.stop_price))
            risk_fraction = float(plan.risk_fraction or 0.0)
            risk_budget = equity_at_entry * risk_fraction
            quantity = 0.0
            risk_amount = 0.0
            if risk_per_unit and risk_per_unit > 0 and plan.price_hint is not None:
                quantity = risk_budget / risk_per_unit
                notional = min(quantity * float(plan.price_hint), equity_at_entry * DEFAULT_LEVERAGE)
                quantity = notional / max(float(plan.price_hint), 1e-12)
                risk_amount = risk_per_unit * quantity
            updates.update({
                'tp_price': None,
                'hold_bars': 0,
                'rev_window': None,
                'add_on_count': 0,
                'degrade_state': 'ATTACK',
                'quality_bucket': quality_bucket,
                'base_quantity': quantity,
                'equity_at_entry': equity_at_entry,
                'risk_amount': risk_amount,
                'risk_per_unit': risk_per_unit,
                'p1_armed': False,
                'p2_armed': False,
                'high_water_r': 0.0,
                'last_trend_signal_ts': signal_ts,
            })
        elif plan.target_strategy == 'rev':
            ctx = plan.conflict_context or {}
            updates.update({
                'tp_price': ctx.get('tp_price'),
                'hold_bars': 0,
                'rev_window': ctx.get('rev_window'),
                'add_on_count': 0,
                'degrade_state': 'ATTACK',
                'quality_bucket': 'MEDIUM',
                'base_quantity': None,
                'p1_armed': False,
                'p2_armed': False,
                'high_water_r': 0.0,
            })
        return updates

    def _close_state_updates(self) -> dict:
        return {
            'active_strategy': 'none',
            'active_side': None,
            'strategy_entry_time': None,
            'strategy_entry_price': None,
            'stop_price': None,
            'risk_fraction': None,
            'tp_price': None,
            'hold_bars': 0,
            'rev_window': None,
            'add_on_count': 0,
            'degrade_state': 'ATTACK',
            'quality_bucket': 'MEDIUM',
            'base_quantity': None,
            'equity_at_entry': None,
            'risk_amount': None,
            'risk_per_unit': None,
            'p1_armed': False,
            'p2_armed': False,
            'high_water_r': 0.0,
        }

    def execute(self, plan: FinalActionPlan, market: MarketSnapshot, state: LiveStateSnapshot) -> ExecutionResult:
        state_updates = None
        status = 'SKIPPED' if not plan.requires_execution else 'FILLED'

        if plan.action_type == 'update_stop' and plan.stop_price is not None:
            status = 'STATE_UPDATED'
            state_updates = {'stop_price': plan.stop_price}
            state_updates.update(self._filter_state_patch(plan.conflict_context))
            if plan.reason == 'update_stop_p1':
                state_updates['p1_armed'] = True
            if plan.reason == 'update_stop_p2':
                state_updates['p2_armed'] = True
        elif plan.action_type == 'state_update':
            status = 'STATE_UPDATED'
            state_updates = self._filter_state_patch(plan.conflict_context)
        elif plan.action_type == 'open' and plan.target_strategy is not None and plan.target_side is not None:
            state_updates = self._open_state_updates(plan, market, state)
        elif plan.action_type == 'close':
            state_updates = self._close_state_updates()
        elif plan.action_type == 'flip' and plan.target_strategy is not None and plan.target_side is not None:
            # Mock semantics: treat flip as atomic "close old + open new" in one local step.
            # Real executor will likely see this as multiple exchange events/orders and must
            # confirm the intermediate flat / residual / partial-fill states before persistence.
            state_updates = self._close_state_updates()
            state_updates.update(self._open_state_updates(plan, market, state))
        elif plan.action_type == 'add':
            state_updates = {
                'add_on_count': int(state.add_on_count) + 1,
                'last_signal_bar': market.bar_ts,
            }
            if plan.stop_price is not None:
                state_updates['stop_price'] = plan.stop_price
            state_updates.update(self._filter_state_patch(plan.conflict_context))
            if state.active_strategy == 'trend' and state.active_side and state.strategy_entry_price is not None and state.stop_price is not None and state.base_quantity is not None:
                close = float(plan.price_hint if plan.price_hint is not None else market.current_price)
                prev_entry = float(state.strategy_entry_price)
                prev_qty = float(state.base_quantity)
                prev_stop = float(state.stop_price)
                add_risk_fraction = float(plan.risk_fraction if plan.risk_fraction is not None else (state.risk_fraction or 0.0))
                cash_like = float(state.account_equity)
                add_risk_budget = cash_like * add_risk_fraction
                if state.active_side == 'long':
                    add_risk = max(close - prev_stop, 1e-9)
                else:
                    add_risk = max(prev_stop - close, 1e-9)
                add_qty = add_risk_budget / add_risk
                max_notional = max(cash_like * DEFAULT_LEVERAGE - prev_qty * prev_entry, 0.0)
                add_notional = min(add_qty * close, max_notional)
                if add_notional > 0:
                    add_qty = add_notional / max(close, 1e-12)
                    total_qty = prev_qty + add_qty
                    weighted_entry = ((prev_entry * prev_qty) + (close * add_qty)) / max(total_qty, 1e-12)
                    updated_stop = state_updates.get('stop_price', prev_stop)
                    if state.active_side == 'long':
                        protective_stop = max(prev_stop, weighted_entry * (1 + state.break_even_buffer), close - add_risk * 0.8, float(updated_stop))
                    else:
                        protective_stop = min(prev_stop, weighted_entry * (1 - state.break_even_buffer), close + add_risk * 0.8, float(updated_stop))
                    state_updates.update({
                        'strategy_entry_price': weighted_entry,
                        'base_quantity': total_qty,
                        'risk_amount': float(state.risk_amount or 0.0) + add_risk_budget,
                        'stop_price': protective_stop,
                    })
        elif plan.action_type == 'trim':
            state_updates = {
                'last_signal_bar': market.bar_ts,
            }
            if plan.stop_price is not None:
                state_updates['stop_price'] = plan.stop_price
            if plan.qty is not None and state.base_quantity is not None:
                remaining_fraction = max(0.0, 1.0 - float(plan.qty))
                state_updates['base_quantity'] = float(state.base_quantity) * remaining_fraction
            state_updates.update(self._filter_state_patch(plan.conflict_context))

        return ExecutionResult(
            result_ts=market.decision_ts,
            bar_ts=market.bar_ts,
            status=status,
            action_type=plan.action_type,
            executed_side=plan.target_side,
            avg_fill_price=plan.price_hint,
            reconcile_status=state.consistency_status,
            should_freeze=False,
            state_updates=state_updates,
        )
