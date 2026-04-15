from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Protocol

OPEN_MESSAGE_DIVIDER = '-' * 60

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


def _format_beijing_ts(value: str | None) -> str:
    if not value:
        return ''
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    bj = dt.astimezone(timezone(timedelta(hours=8)))
    return bj.strftime('%Y-%m-%d %H:%M:%S')


def _resolve_display_ts(market: MarketSnapshot, result: ExecutionResult) -> str:
    trade_summary = result.trade_summary or {}
    execution_ref = trade_summary.get('execution_ref') or {}
    candidate_values = [
        market.bar_ts,
        market.execution_attributed_bar,
        market.strategy_ts,
        market.decision_ts,
        result.bar_ts,
        result.result_ts,
        execution_ref.get('bar_ts'),
        execution_ref.get('decision_ts'),
        execution_ref.get('plan_ts'),
    ]
    for value in candidate_values:
        if value:
            return _format_beijing_ts(str(value))
    return ''


def _render_direction(value: str | None, *, action_type: str | None = None) -> str:
    if value in {'long', 'short'}:
        return value
    if action_type == 'close':
        return 'close'
    if value is None:
        return ''
    return str(value)


def _render_value(value: Any) -> str:
    if value is None:
        return ''
    return str(value)


def _build_recover_audit_summary(*, state: LiveStateSnapshot, result: ExecutionResult) -> dict[str, Any]:
    trade_summary = result.trade_summary or {}
    confirm_context = dict(trade_summary.get('confirm_context') or {})
    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    recover_check = dict(getattr(state, 'recover_check', {}) or {})
    retry_budget = dict(confirm_context.get('retry_budget') or readonly_recheck.get('retry_budget') or recover_check.get('retry_budget') or {})
    return {
        'confirm_phase': confirm_context.get('confirm_phase'),
        'confirm_attempted': confirm_context.get('confirm_attempted'),
        'stop_reason': confirm_context.get('stop_reason') or readonly_recheck.get('stop_reason') or recover_check.get('stop_reason'),
        'stop_condition': confirm_context.get('stop_condition') or readonly_recheck.get('stop_condition') or recover_check.get('stop_condition'),
        'readonly_recheck_status': readonly_recheck.get('status'),
        'readonly_recheck_action': readonly_recheck.get('action'),
        'recover_policy': recover_check.get('recover_policy'),
        'recover_stage': recover_check.get('recover_stage'),
        'recover_result': recover_check.get('result'),
        'pending_execution_phase': getattr(state, 'pending_execution_phase', None),
        'retry_budget': retry_budget or None,
    }


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
        display_ts = _resolve_display_ts(market, result)
        content = (
            f'{OPEN_MESSAGE_DIVIDER}\n'
            f'【执行确认】\n'
            f'交易对: {market.symbol}\n'
            f'时间(北京时间): {display_ts}\n'
            f'动作: {result.action_type}\n'
            f'执行阶段: {result.execution_phase}\n'
            f'订单状态: {result.confirmed_order_status}\n'
            f'成交方向: {_render_direction(result.executed_side, action_type=result.action_type)}\n'
            f'成交数量: {_render_value(result.executed_qty)}\n'
            f'成交均价: {_render_value(result.avg_fill_price)}\n'
            f'手续费: {_render_value(result.fees)}\n'
            f'持仓方向: {_render_direction(result.post_position_side, action_type=result.action_type)}\n'
            f'持仓数量: {_render_value(result.post_position_qty)}\n'
            f'持仓均价: {_render_value(result.post_entry_price)}'
            f'{self._render_open_protection_lines(result=result, state=state)}\n'
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

    def build_async_protective_close_confirmation(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        result: ExecutionResult,
    ) -> DiscordMessagePayload:
        self._assert_sendable_async_protective_close(result, state)
        display_ts = _resolve_display_ts(market, result)
        async_summary = dict(((result.trade_summary or {}).get('protective_async_close')) or {})
        evidence_notes = ', '.join(str(item) for item in (async_summary.get('evidence_notes') or []))
        content = (
            f'{OPEN_MESSAGE_DIVIDER}\n'
            f'【异步保护单触发平仓补报】\n'
            f'交易对: {market.symbol}\n'
            f'时间(北京时间): {display_ts}\n'
            f'说明: 本条不是主动 close submit 成交回报，而是基于交易所持仓/订单事实确认的异步保护单触发补报\n'
            f'动作: protective_async_close\n'
            f'执行阶段: {result.execution_phase}\n'
            f'确认状态: {result.confirmation_status}\n'
            f'订单状态: {result.confirmed_order_status}\n'
            f'上一轮持仓方向: {_render_direction(async_summary.get("previous_position_side"), action_type="close")}\n'
            f'上一轮持仓数量: {_render_value(async_summary.get("previous_position_qty"))}\n'
            f'参考成交均价: {_render_value(result.avg_fill_price)}\n'
            f'当前持仓方向: {_render_direction(result.post_position_side, action_type="close")}\n'
            f'当前持仓数量: {_render_value(result.post_position_qty)}\n'
            f'保护单证据数: {_render_value(async_summary.get("protective_orders_count"))}\n'
            f'保护单校验: {_render_value(async_summary.get("protective_validation_status"))}\n'
            f'证据摘要: {_render_value(evidence_notes)}\n'
            f'对账状态: {result.reconcile_status}'
        )
        payload_kind = 'protective_async_close_confirmation'
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
        display_ts = _resolve_display_ts(market, result)
        content = (
            f'{OPEN_MESSAGE_DIVIDER}\n'
            f'【演练】【非真实发单】Discord 链路验证\n'
            f'交易对: {market.symbol}\n'
            f'时间(北京时间): {display_ts}\n'
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
            'display_ts': display_ts,
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
        display_ts = _resolve_display_ts(market, result)
        content = (
            f'{OPEN_MESSAGE_DIVIDER}\n'
            f'【冻结/风险告警】\n'
            f'交易对: {market.symbol}\n'
            f'时间(北京时间): {display_ts}\n'
            f'运行模式: {state.runtime_mode}\n'
            f'冻结原因: {_render_value(result.freeze_reason or state.freeze_reason)}\n'
            f'对账状态: {_render_value(state.consistency_status)}\n'
            f'执行阶段: {_render_value(result.execution_phase)}\n'
            f'确认状态: {_render_value(result.confirmation_status)}\n'
            f'交易所持仓方向: {_render_direction(state.exchange_position_side)}\n'
            f'交易所持仓数量: {_render_value(state.exchange_position_qty)}\n'
            f'交易所持仓均价: {_render_value(state.exchange_entry_price)}'
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
        if self.is_sendable_async_protective_close(result=result, state=state):
            return self.build_async_protective_close_confirmation(market=market, state=state, result=result)
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
        if self.is_sendable_async_protective_close(result=result, state=state):
            return self.build_async_protective_close_confirmation(market=market, state=state, result=result)
        if not self.is_sendable_execution(result=result, state=state):
            raise ValueError('execution confirmation not publishable under current state')
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
        preferred_async_protective_close = self.is_sendable_async_protective_close(result=result, state=state)
        preferred_execution = self.is_sendable_execution(result=result, state=state)
        effective_kind = payload_kind or ('risk_alert' if state.runtime_mode == 'FROZEN' or result.should_freeze else 'not_sendable')
        if preferred_async_protective_close:
            effective_kind = 'protective_async_close_confirmation'
        elif preferred_execution:
            effective_kind = 'execution_confirmation'
        idempotency_key = self._build_idempotency_key(
            market=market,
            result=result,
            payload_kind=effective_kind,
        )
        rehearsal_preview = self.build_rehearsal_preview(market=market, state=state, result=result)
        primary_kind = effective_kind
        recover_audit = _build_recover_audit_summary(state=state, result=result)
        return {
            'eligible': eligible,
            'channel': 'discord',
            'target': self.channel_id,
            'sent': False,
            'reason': ('preview_only_real_send_disabled' if eligible else 'not_sendable_under_current_state'),
            'kind': primary_kind,
            'primary_kind': primary_kind,
            'primary_preview': {
                'eligible': eligible,
                'kind': primary_kind,
                'idempotency_key': idempotency_key,
                'blocked_reasons': blocked_reasons,
                'dispatch': dispatch,
                'payload_preview': None if payload is None else {
                    'channel_id': payload.channel_id,
                    'content': payload.content,
                    'metadata': dict(payload.metadata or {}),
                },
            },
            'idempotency_key': idempotency_key,
            'blocked_reasons': blocked_reasons,
            'dispatch': dispatch,
            'payload_preview': None if payload is None else {
                'channel_id': payload.channel_id,
                'content': payload.content,
                'metadata': dict(payload.metadata or {}),
            },
            'rehearsal_preview': rehearsal_preview,
            'auxiliary_previews': {
                'rehearsal_preview': rehearsal_preview,
            },
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
                'confirm_phase': recover_audit.get('confirm_phase'),
                'stop_reason': recover_audit.get('stop_reason'),
                'stop_condition': recover_audit.get('stop_condition'),
            },
            'recover_audit': recover_audit,
            'minimum_live_send_config': {
                'discord_channel': 'discord',
                'discord_execution_channel_id': self.channel_id,
                'message_tool_enabled': False,
                'discord_real_send_enabled': False,
                'binance_dry_run_must_remain': True,
                'binance_submit_enabled_must_remain_false': True,
                'execution_confirmation_real_send_default': False,
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

    def is_sendable_async_protective_close(self, *, result: ExecutionResult, state: LiveStateSnapshot) -> bool:
        try:
            self._assert_sendable_async_protective_close(result, state)
            return True
        except ValueError:
            return False

    @staticmethod
    def _open_like_publishable_phases() -> set[str]:
        return {'confirmed', 'entry_confirmed_pending_protective'}

    @classmethod
    def _should_prefer_execution_confirmation(cls, *, result: ExecutionResult) -> bool:
        open_like_actions = {'open', 'flip', 'add', 'trim'}
        allowed_phase = (
            result.execution_phase in cls._open_like_publishable_phases()
            if result.action_type in open_like_actions
            else result.execution_phase == 'confirmed'
        )
        if not (
            result.action_type in {'open', 'close', 'flip', 'add', 'trim'}
            and result.confirmation_status == 'CONFIRMED'
            and allowed_phase
            and result.confirmed_order_status in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}
            and result.reconcile_status == 'OK'
            and (result.avg_fill_price is not None or result.post_entry_price is not None)
        ):
            return False
        if result.action_type in {'open', 'flip', 'add', 'trim'}:
            return result.post_position_side in {'long', 'short'} and float(result.post_position_qty or 0.0) > 0.0
        return True

    def _assert_sendable_async_protective_close(self, result: ExecutionResult, state: LiveStateSnapshot) -> None:
        trade_summary = result.trade_summary or {}
        async_summary = dict(trade_summary.get('protective_async_close') or {})
        if result.action_type != 'protective_async_close':
            raise ValueError(f'action type not async protective close: {result.action_type}')
        if result.confirmation_status != 'CONFIRMED':
            raise ValueError(f'confirmation required before async protective close publish, got: {result.confirmation_status}')
        if result.execution_phase != 'protective_async_close_confirmed':
            raise ValueError(f'execution phase invalid for async protective close publish: {result.execution_phase}')
        if result.reconcile_status != 'OK':
            raise ValueError(f'reconcile_status must be OK, got: {result.reconcile_status}')
        if result.should_freeze or state.runtime_mode == 'FROZEN' or state.freeze_status not in {None, '', 'NONE'}:
            raise ValueError('async protective close publish requires non-frozen runtime')
        if result.post_position_side not in {None, '', 'flat'}:
            raise ValueError(f'post position side must be flat, got: {result.post_position_side}')
        if float(result.post_position_qty or 0.0) > 0.0:
            raise ValueError(f'post position qty must be flat, got: {result.post_position_qty}')
        if async_summary.get('previous_position_side') not in {'long', 'short'}:
            raise ValueError('previous_position_side evidence missing for async protective close publish')
        if float(async_summary.get('previous_position_qty') or 0.0) <= 0.0:
            raise ValueError('previous_position_qty evidence missing for async protective close publish')
        if not list(async_summary.get('evidence_notes') or []):
            raise ValueError('protective evidence notes required for async protective close publish')

    def _assert_sendable_execution(self, result: ExecutionResult, state: LiveStateSnapshot) -> None:
        if not self._should_prefer_execution_confirmation(result=result):
            if result.action_type not in {'open', 'close', 'flip', 'add', 'trim'}:
                raise ValueError(f'action type not publishable as execution confirmation: {result.action_type}')
            if result.confirmation_status != 'CONFIRMED':
                raise ValueError(f'confirmation required before publish, got: {result.confirmation_status}')
            open_like_actions = {'open', 'flip', 'add', 'trim'}
            allowed_phases = self._open_like_publishable_phases() if result.action_type in open_like_actions else {'confirmed'}
            if result.execution_phase not in allowed_phases:
                raise ValueError(f'execution phase not publishable, got: {result.execution_phase}')
            if result.confirmed_order_status not in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}:
                raise ValueError(f'confirmed order status invalid for publish: {result.confirmed_order_status}')
            if result.reconcile_status != 'OK':
                raise ValueError(f'reconcile_status must be OK, got: {result.reconcile_status}')
            raise ValueError('avg_fill_price or post_entry_price is required for execution confirmation')
        if result.action_type not in {'open', 'close', 'flip', 'add', 'trim'}:
            raise ValueError(f'action type not publishable as execution confirmation: {result.action_type}')
        if result.reconcile_status != 'OK':
            raise ValueError(f'reconcile_status must be OK, got: {result.reconcile_status}')
        if result.avg_fill_price is None and result.post_entry_price is None:
            raise ValueError('avg_fill_price or post_entry_price is required for execution confirmation')

        if result.action_type in {'open', 'flip', 'add', 'trim'}:
            if result.post_position_side not in {'long', 'short'}:
                raise ValueError(f'post position side invalid for open-like publish: {result.post_position_side}')
            if float(result.post_position_qty or 0.0) <= 0.0:
                raise ValueError(f'post position qty invalid for open-like publish: {result.post_position_qty}')
            if state.stop_price is None:
                raise ValueError('stop_price is required for open-like execution confirmation')
            strategy = getattr(state, 'active_strategy', None)
            tp_price = getattr(state, 'tp_price', None)
            if strategy != 'trend' and tp_price is None:
                raise ValueError('tp_price is required for non-trend open-like execution confirmation')
            return

        if result.confirmation_status != 'CONFIRMED':
            raise ValueError(f'confirmation required before publish, got: {result.confirmation_status}')
        if result.execution_phase != 'confirmed':
            raise ValueError(f'execution phase must be confirmed, got: {result.execution_phase}')
        if result.confirmed_order_status not in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}:
            raise ValueError(f'confirmed order status invalid for publish: {result.confirmed_order_status}')

    def _collect_execution_blockers(self, *, result: ExecutionResult, state: LiveStateSnapshot) -> list[str]:
        blockers: list[str] = []
        if state.runtime_mode == 'FROZEN' or result.should_freeze:
            blockers.append('frozen_runtime')
        if self.is_sendable_async_protective_close(result=result, state=state):
            return blockers
        if self._should_prefer_execution_confirmation(result=result):
            strategy = getattr(state, 'active_strategy', None)
            if state.stop_price is not None and (strategy == 'trend' or getattr(state, 'tp_price', None) is not None):
                return blockers
        if result.action_type not in {'open', 'close', 'flip', 'add', 'trim'}:
            blockers.append('action_type_not_publishable')
        if result.reconcile_status != 'OK':
            blockers.append('reconcile_not_ok')
        if result.avg_fill_price is None and result.post_entry_price is None:
            blockers.append('avg_fill_price_missing')

        if result.action_type in {'open', 'flip', 'add', 'trim'}:
            if result.post_position_side not in {'long', 'short'}:
                blockers.append('post_position_side_invalid')
            if float(result.post_position_qty or 0.0) <= 0.0:
                blockers.append('post_position_qty_invalid')
            if state.stop_price is None:
                blockers.append('stop_price_missing')
            strategy = getattr(state, 'active_strategy', None)
            if strategy != 'trend' and getattr(state, 'tp_price', None) is None:
                blockers.append('tp_price_missing')
            return blockers

        if result.confirmation_status != 'CONFIRMED':
            blockers.append('confirmation_not_confirmed')
        open_like_actions = {'open', 'flip', 'add', 'trim'}
        allowed_phases = self._open_like_publishable_phases() if result.action_type in open_like_actions else {'confirmed'}
        if result.execution_phase not in allowed_phases:
            blockers.append('execution_phase_not_publishable')
        if result.confirmed_order_status not in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}:
            blockers.append('confirmed_order_status_invalid')
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

    @staticmethod
    def _render_open_protection_lines(*, result: ExecutionResult, state: LiveStateSnapshot) -> str:
        if result.action_type not in {'open', 'flip', 'add', 'trim'}:
            return ''
        lines = [
            f'\n止损价: {_render_value(state.stop_price)}',
            f'\n止盈价: {_render_value(getattr(state, "tp_price", None))}',
        ]
        protective_status = getattr(state, 'protective_order_status', None)
        if protective_status:
            lines.append(f'\n保护单状态: {_render_value(protective_status)}')
        return ''.join(lines)
