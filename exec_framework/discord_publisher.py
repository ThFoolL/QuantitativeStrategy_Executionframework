from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol

try:
    from .models import ExecutionResult, LiveStateSnapshot, MarketSnapshot
except ImportError:  # pragma: no cover
    from models import ExecutionResult, LiveStateSnapshot, MarketSnapshot


@dataclass(frozen=True)
class DiscordMessagePayload:
    channel_id: str
    content: str
    metadata: dict[str, Any]


class DiscordSender(Protocol):
    def send(self, payload: DiscordMessagePayload) -> dict[str, Any]: ...


@dataclass(frozen=True)
class DiscordSendAttempt:
    eligible: bool
    blocked_reason: str | None
    payload: DiscordMessagePayload | None
    sender_result: dict[str, Any] | None


class DiscordPublisher:
    """Discord 真发送接线准备。

    当前阶段：
    - 默认只做 payload/export/preview
    - 执行确认与演练消息严格区分 kind 与文案
    - 演练消息只用于链路验证，不表达真实成交事实
    """

    def __init__(self, channel_id: str):
        self.channel_id = channel_id

    def build_execution_confirmation(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordMessagePayload:
        self._assert_sendable_execution(result, state)
        realized_pnl = self._derive_realized_pnl(result)
        content = (
            f'【执行确认】\n'
            f'交易对: {market.symbol}\n'
            f'K线时间: {market.bar_ts}\n'
            f'动作: {result.action_type}\n'
            f'执行阶段: {result.execution_phase}\n'
            f'订单状态: {result.confirmed_order_status}\n'
            f'成交方向: {result.executed_side}\n'
            f'成交数量: {result.executed_qty}\n'
            f'成交均价: {result.avg_fill_price}\n'
            f'手续费: {result.fees}\n'
            f'持仓方向: {result.post_position_side}\n'
            f'持仓数量: {result.post_position_qty}\n'
            f'持仓均价: {result.post_entry_price}\n'
            f'对账状态: {result.reconcile_status}'
            f'{self._render_realized_pnl_line(result, realized_pnl)}'
        )
        payload_kind = 'execution_confirmation'
        return DiscordMessagePayload(
            channel_id=self.channel_id,
            content=content,
            metadata={
                'kind': payload_kind,
                'sendable': True,
                'channel_id': self.channel_id,
                'exchange_order_ids': result.exchange_order_ids or [],
                'trade_summary': result.trade_summary or {},
                'export_version': 'v1',
                'idempotency_key': self._build_idempotency_key(market=market, result=result, payload_kind=payload_kind),
            },
        )

    def build_rehearsal_message(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordMessagePayload:
        payload_kind = 'rehearsal_notification'
        content = (
            f'【演练】【非真实发单】Discord 链路验证\n'
            f'交易对: {market.symbol}\n'
            f'K线时间: {market.bar_ts}\n'
            f'用途: 仅验证 Discord message tool 发送链路、权限、幂等与审计\n'
            f'当前运行模式: {state.runtime_mode}\n'
            f'当前执行阶段: {result.execution_phase}\n'
            f'当前确认状态: {result.confirmation_status}\n'
            f'当前对账状态: {result.reconcile_status}\n'
            f'说明: 这不是成交回报，不代表已开仓/已平仓/已成交\n'
            f'说明: Binance 真实发单仍保持关闭'
        )
        preview = {
            'title': '【演练】【非真实发单】Discord 链路验证',
            'symbol': market.symbol,
            'bar_ts': market.bar_ts,
            'runtime_mode': state.runtime_mode,
            'execution_phase': result.execution_phase,
            'confirmation_status': result.confirmation_status,
            'reconcile_status': result.reconcile_status,
            'intent': 'discord_transport_rehearsal_only',
        }
        return DiscordMessagePayload(
            channel_id=self.channel_id,
            content=content,
            metadata={
                'kind': payload_kind,
                'sendable': True,
                'channel_id': self.channel_id,
                'export_version': 'v1',
                'rehearsal': True,
                'preview': preview,
                'trade_summary': result.trade_summary or {},
                'exchange_order_ids': result.exchange_order_ids or [],
                'idempotency_key': self._build_idempotency_key(market=market, result=result, payload_kind=payload_kind),
            },
        )

    def build_risk_alert(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordMessagePayload:
        content = (
            f'【冻结/风险告警】\n'
            f'交易对: {market.symbol}\n'
            f'K线时间: {market.bar_ts}\n'
            f'运行模式: {state.runtime_mode}\n'
            f'冻结原因: {result.freeze_reason or state.freeze_reason}\n'
            f'对账状态: {state.consistency_status}\n'
            f'执行阶段: {result.execution_phase}\n'
            f'确认状态: {result.confirmation_status}\n'
            f'交易所持仓方向: {state.exchange_position_side}\n'
            f'交易所持仓数量: {state.exchange_position_qty}\n'
            f'交易所持仓均价: {state.exchange_entry_price}'
        )
        payload_kind = 'risk_alert'
        return DiscordMessagePayload(
            channel_id=self.channel_id,
            content=content,
            metadata={
                'kind': payload_kind,
                'sendable': True,
                'channel_id': self.channel_id,
                'exchange_order_ids': result.exchange_order_ids or [],
                'export_version': 'v1',
                'idempotency_key': self._build_idempotency_key(market=market, result=result, payload_kind=payload_kind),
            },
        )

    def export_if_sendable(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordMessagePayload | None:
        if self.is_sendable_execution(result=result, state=state):
            return self.build_execution_confirmation(market=market, state=state, result=result)
        if state.runtime_mode == 'FROZEN' or result.should_freeze:
            return self.build_risk_alert(market=market, state=state, result=result)
        return None

    def build_sender_input(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordMessagePayload:
        return self.build_execution_confirmation(market=market, state=state, result=result)

    def build_rehearsal_sender_input(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordMessagePayload:
        return self.build_rehearsal_message(market=market, state=state, result=result)

    def build_dispatch_audit(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> dict[str, Any]:
        payload = self.export_if_sendable(market=market, state=state, result=result)
        payload_kind = None if payload is None else payload.metadata.get('kind')
        eligible = payload is not None
        blocked_reasons = [] if eligible else self._collect_execution_blockers(result=result, state=state)
        dispatch = None if payload is None else {
            'channel': 'discord',
            'target': self.channel_id,
            'message': payload.content,
            'metadata': dict(payload.metadata or {}),
        }
        idempotency_key = self._build_idempotency_key(
            market=market,
            result=result,
            payload_kind=('risk_alert' if state.runtime_mode == 'FROZEN' or result.should_freeze else payload_kind),
        )
        return {
            'eligible': eligible,
            'channel': 'discord',
            'target': self.channel_id,
            'sent': False,
            'reason': ('preview_only_real_send_disabled' if eligible else 'not_sendable_under_current_state'),
            'kind': payload_kind or ('risk_alert' if state.runtime_mode == 'FROZEN' or result.should_freeze else 'not_sendable'),
            'idempotency_key': idempotency_key,
            'blocked_reasons': blocked_reasons,
            'dispatch': dispatch,
            'payload_preview': None if payload is None else {
                'channel_id': payload.channel_id,
                'content': payload.content,
                'metadata': dict(payload.metadata or {}),
            },
            'rehearsal_preview': self.build_rehearsal_preview(market=market, state=state, result=result),
            'runtime': {
                'runtime_mode': state.runtime_mode,
                'freeze_status': state.freeze_status,
                'freeze_reason': result.freeze_reason or state.freeze_reason,
                'consistency_status': result.reconcile_status or state.consistency_status,
            },
            'confirm': {
                'execution_phase': result.execution_phase,
                'confirmation_status': result.confirmation_status,
                'confirmed_order_status': result.confirmed_order_status,
                'avg_fill_price': result.avg_fill_price,
                'executed_qty': result.executed_qty,
                'executed_side': result.executed_side,
                'exchange_order_ids': result.exchange_order_ids or [],
            },
            'minimum_live_send_config': {
                'discord_channel': 'discord',
                'discord_execution_channel_id': self.channel_id,
                'message_tool_enabled': False,
                'discord_real_send_enabled': False,
                'binance_dry_run_must_remain': True,
                'binance_submit_enabled_must_remain_false': True,
                'idempotency_key_required': True,
                'discord_send_ledger_required': True,
                'discord_retry_strategy': 'retryable_failures_only',
                'allowed_first_real_message_kind': 'rehearsal_notification',
            },
        }

    def build_rehearsal_preview(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> dict[str, Any]:
        payload = self.build_rehearsal_message(market=market, state=state, result=result)
        return {
            'kind': payload.metadata.get('kind'),
            'idempotency_key': payload.metadata.get('idempotency_key'),
            'dispatch': {
                'channel': 'discord',
                'target': self.channel_id,
                'message': payload.content,
                'metadata': dict(payload.metadata or {}),
            },
            'payload_preview': {
                'channel_id': payload.channel_id,
                'content': payload.content,
                'metadata': dict(payload.metadata or {}),
            },
        }

    def send_via_bridge(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
        sender: DiscordSender,
    ) -> DiscordSendAttempt:
        try:
            payload = self.build_sender_input(market=market, state=state, result=result)
        except ValueError as exc:
            return DiscordSendAttempt(
                eligible=False,
                blocked_reason=str(exc),
                payload=None,
                sender_result=None,
            )
        return DiscordSendAttempt(
            eligible=True,
            blocked_reason=None,
            payload=payload,
            sender_result=sender.send(payload),
        )

    def send_rehearsal_via_bridge(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
        sender: DiscordSender,
    ) -> DiscordSendAttempt:
        payload = self.build_rehearsal_sender_input(market=market, state=state, result=result)
        return DiscordSendAttempt(
            eligible=True,
            blocked_reason=None,
            payload=payload,
            sender_result=sender.send(payload),
        )

    def is_sendable_execution(self, *, result: ExecutionResult, state: LiveStateSnapshot) -> bool:
        try:
            self._assert_sendable_execution(result, state)
            return True
        except ValueError:
            return False

    def _assert_sendable_execution(self, result: ExecutionResult, state: LiveStateSnapshot) -> None:
        if state.runtime_mode == 'FROZEN' or result.should_freeze:
            raise ValueError('frozen state cannot emit execution confirmation')
        if result.confirmation_status != 'CONFIRMED':
            raise ValueError(f'confirmation required before publish, got: {result.confirmation_status}')
        if result.execution_phase != 'confirmed':
            raise ValueError(f'execution phase must be confirmed, got: {result.execution_phase}')
        if result.confirmed_order_status not in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}:
            raise ValueError(f'confirmed order status invalid for publish: {result.confirmed_order_status}')
        if result.reconcile_status != 'OK':
            raise ValueError(f'reconcile_status must be OK, got: {result.reconcile_status}')
        if result.avg_fill_price is None:
            raise ValueError('avg_fill_price is required for execution confirmation')

    def _collect_execution_blockers(self, *, result: ExecutionResult, state: LiveStateSnapshot) -> list[str]:
        blockers: list[str] = []
        if state.runtime_mode == 'FROZEN' or result.should_freeze:
            blockers.append('frozen_runtime')
        if result.confirmation_status != 'CONFIRMED':
            blockers.append('confirmation_not_confirmed')
        if result.execution_phase != 'confirmed':
            blockers.append('execution_phase_not_confirmed')
        if result.confirmed_order_status not in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}:
            blockers.append('confirmed_order_status_invalid')
        if result.reconcile_status != 'OK':
            blockers.append('reconcile_not_ok')
        if result.avg_fill_price is None:
            blockers.append('avg_fill_price_missing')
        return blockers

    @staticmethod
    def _build_idempotency_key(
        *,
        market: MarketSnapshot,
        result: ExecutionResult,
        payload_kind: str | None,
    ) -> str:
        raw = {
            'kind': payload_kind,
            'symbol': market.symbol,
            'bar_ts': market.bar_ts,
            'action_type': result.action_type,
            'execution_phase': result.execution_phase,
            'confirmation_status': result.confirmation_status,
            'confirmed_order_status': result.confirmed_order_status,
            'exchange_order_ids': result.exchange_order_ids or [],
        }
        digest = sha256(str(raw).encode('utf-8')).hexdigest()[:16]
        return f"discord:{market.symbol}:{market.bar_ts}:{payload_kind or 'unknown'}:{digest}"

    @staticmethod
    def _derive_realized_pnl(result: ExecutionResult) -> float | None:
        trade_summary = result.trade_summary or {}
        fills = trade_summary.get('fills') or []
        pnl_values = [item.get('realized_pnl') for item in fills if item.get('realized_pnl') is not None]
        if not pnl_values:
            return None
        return float(sum(float(v) for v in pnl_values))

    @staticmethod
    def _render_realized_pnl_line(result: ExecutionResult, realized_pnl: float | None) -> str:
        if result.action_type != 'close':
            return ''
        return f'\n盈亏: {realized_pnl}'
