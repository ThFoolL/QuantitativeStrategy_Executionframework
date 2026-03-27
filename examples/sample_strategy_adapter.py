from __future__ import annotations

from exec_framework.models import FinalActionPlan, LiveStateSnapshot, MarketSnapshot


class SampleStrategyAdapter:
    """最小策略接入样例。

    这个文件只说明 strategy -> FinalActionPlan 的接入格式：
    - 不代表生产策略
    - 不包含真实 alpha / 特征 / 仓位逻辑
    - 仅用于 private 仓接线时参考输入输出边界
    """

    def plan(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan:
        if state.consistency_status != 'OK' or state.freeze_reason:
            return FinalActionPlan(
                plan_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                action_type='hold',
                target_strategy=None,
                target_side=None,
                reason='blocked_by_state_consistency',
                requires_execution=False,
            )

        if state.active_side is not None or state.exchange_position_qty != 0:
            return FinalActionPlan(
                plan_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                action_type='hold',
                target_strategy=None,
                target_side=None,
                reason='position_already_present',
                requires_execution=False,
            )

        if market.source_status != 'OK':
            return FinalActionPlan(
                plan_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                action_type='hold',
                target_strategy=None,
                target_side=None,
                reason='market_snapshot_not_ready',
                requires_execution=False,
            )

        return FinalActionPlan(
            plan_ts=market.decision_ts,
            bar_ts=market.bar_ts,
            action_type='open',
            target_strategy='sample_placeholder',
            target_side='long',
            reason='sample_open_format_only',
            qty_mode='fixed',
            qty=0.001,
            price_hint=market.current_price,
            stop_price=market.current_price * 0.99,
            risk_fraction=0.001,
            conflict_context={
                'note': 'placeholder_only',
                'integration_boundary': 'strategy_outputs_final_action_plan',
            },
            requires_execution=True,
        )
