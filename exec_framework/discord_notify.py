from __future__ import annotations

from dataclasses import dataclass

from .models import ExecutionResult, LiveStateSnapshot, MarketSnapshot

# 兼容旧接口；新代码优先使用 exec_framework.discord_publisher.DiscordPublisher。


@dataclass(frozen=True)
class DiscordExecutionNotification:
    channel_id: str
    message: str


class DiscordNotificationBuilder:
    """仅构造消息，不直接发送。

    强约束：
    - 只能基于真实执行确认结果构造执行通知
    - 不能用计划代替成交
    - 对账异常/冻结时只能构造风险或异常通知
    """

    def __init__(self, channel_id: str):
        self.channel_id = channel_id

    def build_execution_confirmation(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordExecutionNotification:
        self._assert_confirmed_execution(result)
        message = (
            f'【执行确认】\n'
            f'交易对: {market.symbol}\n'
            f'K线时间: {market.bar_ts}\n'
            f'动作: {result.action_type}\n'
            f'方向: {result.executed_side}\n'
            f'成交数量: {result.executed_qty}\n'
            f'成交均价: {result.avg_fill_price}\n'
            f'持仓方向: {result.post_position_side}\n'
            f'持仓数量: {result.post_position_qty}\n'
            f'持仓均价: {result.post_entry_price}\n'
            f'对账状态: {result.reconcile_status}\n'
            f'是否冻结: {result.should_freeze}'
        )
        return DiscordExecutionNotification(channel_id=self.channel_id, message=message)

    def build_risk_alert(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordExecutionNotification:
        message = (
            f'【风险/冻结告警】\n'
            f'交易对: {market.symbol}\n'
            f'K线时间: {market.bar_ts}\n'
            f'对账状态: {result.reconcile_status}\n'
            f'是否冻结: {result.should_freeze}\n'
            f'冻结原因: {result.freeze_reason or state.freeze_reason}\n'
            f'交易所持仓方向: {state.exchange_position_side}\n'
            f'交易所持仓数量: {state.exchange_position_qty}\n'
            f'交易所持仓均价: {state.exchange_entry_price}'
        )
        return DiscordExecutionNotification(channel_id=self.channel_id, message=message)

    def _assert_confirmed_execution(self, result: ExecutionResult) -> None:
        if result.status not in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}:
            raise ValueError(f'execution notification requires confirmed result status, got: {result.status}')
        if result.reconcile_status != 'OK':
            raise ValueError(f'execution notification requires reconcile_status=OK, got: {result.reconcile_status}')
        if result.should_freeze:
            raise ValueError('execution notification forbidden while should_freeze=True')
        if result.avg_fill_price is None:
            raise ValueError('execution notification requires avg_fill_price from real confirmation')
        if result.post_position_side is None and result.post_position_qty > 0:
            raise ValueError('post_position_side is required when post_position_qty > 0')
