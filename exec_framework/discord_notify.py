from __future__ import annotations

from dataclasses import dataclass

from .discord_publisher import DiscordPublisher
from .models import ExecutionResult, LiveStateSnapshot, MarketSnapshot

# 兼容旧接口；新代码优先使用 live.discord_publisher.DiscordPublisher。


@dataclass(frozen=True)
class DiscordExecutionNotification:
    channel_id: str
    message: str


class DiscordNotificationBuilder:
    """仅构造消息，不直接发送。

    兼容层不再维护独立文案模板，统一复用主链 DiscordPublisher 的展示口径。

    强约束：
    - 只能基于真实执行确认结果构造执行通知
    - 不能用计划代替成交
    - 对账异常/冻结时只能构造风险或异常通知
    """

    def __init__(self, channel_id: str):
        self.channel_id = channel_id
        self._publisher = DiscordPublisher(channel_id)

    def build_execution_confirmation(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordExecutionNotification:
        payload = self._publisher.build_execution_confirmation(
            market=market,
            state=state,
            result=result,
        )
        return DiscordExecutionNotification(channel_id=self.channel_id, message=payload.content)

    def build_risk_alert(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordExecutionNotification:
        payload = self._publisher.build_risk_alert(
            market=market,
            state=state,
            result=result,
        )
        return DiscordExecutionNotification(channel_id=self.channel_id, message=payload.content)
