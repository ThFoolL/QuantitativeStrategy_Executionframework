from __future__ import annotations

import json
import time

from dataclasses import asdict, dataclass, replace
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any


REQUIRES_NON_EMPTY_QUANTITY_ACTIONS = {'open', 'add', 'trim', 'flip'}

from .binance_exception_helpers import build_guarded_exception_plan
from .binance_exception_policy import classify_submit_exception_detail
from .binance_posttrade import BinancePostTradeConfirmer, PostTradeConfirmation, SimulatedExecutionReceipt, build_confirm_context
from .binance_readonly import BinanceReadOnlyClient, ExchangeSymbolRules
from .binance_submit import (
    BinanceCancelReceipt,
    BinanceSignedSubmitClient,
    BinanceSubmitError,
    BinanceSubmitReceipt,
)
from .binance_readonly import OrderSnapshot
from .models import ExecutionResult, FinalActionPlan, LiveStateSnapshot, MarketSnapshot
from .protective_orders import (
    PROTECTIVE_PENDING_STATUSES,
    build_protective_order_intents,
    serialize_protective_order,
    snapshot_protective_orders,
    split_open_orders,
    validate_protective_orders,
)
from .runtime_env import BinanceEnvConfig, LIVE_SUBMIT_MANUAL_ACK_TOKEN, LIVE_SUBMIT_UNLOCK_TOKEN
from .strategy_protection_intent import build_strategy_protection_intent
from .async_operation import attach_execution_confirm_async_operation, attach_protection_followup_async_operation, attach_submit_auto_repair_async_operation
from .runtime_guard import (
    RECOVER_RESULT_ALLOWED,
    RECOVER_RESULT_BLOCKED,
    RuntimeFreezeController,
    append_recover_record,
    build_recover_record,
)
from .unified_risk_action import classify_reconcile_risk


_NEGATIVE_PROTECTIVE_VALIDATION_LEVELS = {
    'INVALID',
    'MISMATCH',
    'MISSING',
    'STRUCTURAL_MISMATCH',
    'SEMANTIC_MISMATCH',
}
_NEGATIVE_PROTECTIVE_RECOVER_RISKS = {
    'replace_invalid_protective_orders',
    'cannot_safely_cancel_existing_protective_orders',
    'await_submit_and_posttrade_confirmation',
    'will_replace_existing_protective_orders_during_submit',
    'position_open_without_protection',
    'unclassified_protective_order_state',
}


@dataclass(frozen=True)
class BinanceCancelOrderRequest:
    symbol: str
    order_id: str | None
    client_order_id: str | None
    metadata: dict[str, Any] = None


@dataclass(frozen=True)
class BinanceOrderRequest:
    symbol: str
    side: str
    order_type: str
    quantity: float | None
    reduce_only: bool
    position_side: str | None
    client_order_id: str
    stop_price: float | None = None
    close_position: bool = False
    working_type: str | None = None
    price_protect: bool | None = None
    metadata: dict[str, Any] = None


class BinanceRealExecutor:
    """真实执行器接口骨架。

    当前阶段默认 dry-run / no-submit：
    - 先构造稳定的订单请求对象
    - 通过 submit gate 严格拦截真实发单
    - 预留 post-trade confirm 链路，但 submit disabled 时绝不伪装为 submitted / confirmed
    - 明确写出 execution_ref / request_context / confirm_context 供后续实盘接线复用
    """


    PROTECTIVE_RECOVERABLE_REASONS = {
        'protective_orders_exchange_state_mismatch',
        'protective_orders_missing_on_exchange',
        'protective_cancel_identity_missing',
    }

    ENTRY_PHASE_ACTIONS = {'open', 'flip'}
    PROTECTIVE_REBUILD_ACTIONS = {'protective_rebuild'}
    FROZEN_BYPASS_CLOSE_REASONS = {
        'emergency_close_after_protective_missing',
    }
    FROZEN_BYPASS_QTY_MODES = {'full_close', 'exchange_position'}
    POSTTRADE_CONFIRM_RETRYABLE_CATEGORIES = {'pending', 'query_failed', 'position_confirmed'}
    REDUCE_ONLY_REPAIRABLE_ACTIONS = {'close'}
    TIMESTAMP_DRIFT_REPAIRABLE_ACTIONS = {'open', 'add', 'trim', 'close', 'flip', 'protective_rebuild'}

    def __init__(self, config: BinanceEnvConfig, readonly_client: BinanceReadOnlyClient | None = None):
        self.config = config
        self.readonly_client = readonly_client or BinanceReadOnlyClient(config)
        self.posttrade_confirmer = BinancePostTradeConfirmer(self.readonly_client)
        self.freeze_controller = RuntimeFreezeController()
        self.submit_client = BinanceSignedSubmitClient(
            config=config,
            recv_window_ms=config.recv_window_ms,
            allow_live_submit_call=bool(getattr(config, 'submit_http_post_enabled', False)),
        )

    @staticmethod
    def _build_client_order_id(bar_ts: str, suffix: str) -> str:
        compact_ts = ''.join(ch for ch in str(bar_ts) if ch.isdigit())
        if not compact_ts:
            compact_ts = '0'
        compact_ts = compact_ts[:24]
        clean_suffix = ''.join(ch for ch in str(suffix) if ch.isalnum() or ch in {'-', '_'})
        clean_suffix = clean_suffix[:11] or 'ord'
        candidate = f"{compact_ts}-{clean_suffix}"
        return candidate[:36]

    def _is_frozen_bypass_emergency_close(self, plan: FinalActionPlan) -> bool:
        if plan.action_type != 'close' or not plan.requires_execution:
            return False
        if plan.close_reason not in self.FROZEN_BYPASS_CLOSE_REASONS:
            return False
        if plan.reason not in self.FROZEN_BYPASS_CLOSE_REASONS:
            return False
        if plan.qty_mode not in self.FROZEN_BYPASS_QTY_MODES:
            return False
        conflict_context = plan.conflict_context or {}
        return bool(conflict_context.get('protective_missing_force_close'))

    def execute(self, plan: FinalActionPlan, market: MarketSnapshot, state: LiveStateSnapshot) -> ExecutionResult:
        if state.runtime_mode == 'FROZEN' and not self._is_frozen_bypass_emergency_close(plan):
            result = ExecutionResult(
                result_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                status='FROZEN',
                action_type=plan.action_type,
                executed_side=plan.target_side,
                reconcile_status=state.consistency_status,
                should_freeze=True,
                freeze_reason=state.freeze_reason or state.last_freeze_reason,
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'pending_execution_phase': 'frozen',
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
                execution_phase='frozen',
                confirmation_status='FROZEN',
                confirmed_order_status='FROZEN',
                trade_summary={
                    'confirmation_category': 'mismatch',
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'submit_gate': self._build_submit_gate_context(),
                },
            )
            return self._apply_freeze_if_needed(state, result)

        if not plan.requires_execution:
            state_updates = {'pending_execution_phase': 'none'}
            execution_phase = 'none'
            status = 'SKIPPED'
            conflict_context = dict(plan.conflict_context or {})
            management_pending_phase = conflict_context.get('pending_execution_phase')
            if (
                plan.action_type == 'state_update'
                and management_pending_phase == 'management_stop_update_pending_protective'
                and plan.stop_price is None
                and 'stop_price' in conflict_context
            ):
                # Keep the management stop-update phase visible so runtime orchestration can
                # hand off immediately into protective_rebuild instead of collapsing to none.
                state_updates = {
                    'pending_execution_phase': management_pending_phase,
                    'stop_price': conflict_context.get('stop_price'),
                }
                execution_phase = 'management_state_updated'
                status = 'STATE_UPDATED'
            return ExecutionResult(
                result_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                status=status,
                action_type=plan.action_type,
                executed_side=plan.target_side,
                reconcile_status=state.consistency_status,
                should_freeze=False,
                state_updates=state_updates,
                execution_phase=execution_phase,
                confirmation_status='NOT_REQUIRED',
                confirmed_order_status='NOT_REQUIRED',
                trade_summary={
                    'confirmation_category': None,
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'submit_gate': self._build_submit_gate_context(),
                },
            )

        symbol_rules = self.readonly_client.get_exchange_info(market.symbol)
        protective_exchange_snapshot = None
        protective_recover = None
        try:
            protective_exchange_snapshot = self._load_exchange_protective_snapshot(market.symbol, state=state)
            protective_recover = self._recover_protective_orders_before_submit(
                plan=plan,
                market=market,
                state=state,
                exchange_snapshot=protective_exchange_snapshot,
            )
            cancel_requests = self._build_protective_cancel_requests(
                plan,
                market,
                state,
                protective_recover['effective_exchange_snapshot'],
            )
        except ValueError as exc:
            recover_updates = self._build_protective_recover_state_updates(
                state=state,
                market=market,
                plan=plan,
                recover=protective_recover,
                result=RECOVER_RESULT_BLOCKED,
                reason=str(exc),
                allowed=False,
            )
            result = ExecutionResult(
                result_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                status='FROZEN',
                action_type=plan.action_type,
                executed_side=plan.target_side,
                reconcile_status=state.consistency_status,
                error_code='PROTECTIVE_CANCEL_PRECHECK_FAILED',
                error_message=str(exc),
                should_freeze=True,
                freeze_reason=str(exc),
                state_updates={
                    'pending_execution_phase': 'frozen',
                    'protective_order_last_sync_ts': market.decision_ts,
                    'protective_order_last_sync_action': plan.action_type,
                    'protective_order_freeze_reason': str(exc),
                    **recover_updates,
                },
                execution_phase='frozen',
                confirmation_status='UNCONFIRMED',
                confirmed_order_status='CANCEL_PRECHECK_FAILED',
                trade_summary={
                    'confirmation_category': 'mismatch',
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'protective_exchange_snapshot': protective_exchange_snapshot,
                    'protective_recover': protective_recover,
                    'protective_cancel_summary': {'ok': False, 'reason': str(exc), 'cancel_count': 0, 'receipts': []},
                },
            )
            return self._apply_freeze_if_needed(state, result)
        protective_recover_updates = self._build_protective_recover_state_updates(
            state=state,
            market=market,
            plan=plan,
            recover=protective_recover,
            result=RECOVER_RESULT_ALLOWED,
            reason=((protective_recover or {}).get('result') or 'protective_recover_ok'),
            allowed=True,
        )
        if protective_recover is not None:
            protective_recover = {
                **protective_recover,
                'state_updates': {
                    **dict((protective_recover or {}).get('state_updates') or {}),
                    **protective_recover_updates,
                },
            }
        order_requests = self._build_order_requests(plan, market, state, symbol_rules)
        draft_ids = [request.client_order_id for request in order_requests]
        request_context = self._build_request_context(
            plan=plan,
            market=market,
            state=state,
            order_requests=order_requests,
            cancel_requests=cancel_requests,
        )

        submit_gate = self._evaluate_submit_gate(
            market=market,
            state=state,
            order_requests=order_requests,
            blocked_reason='dry_run_or_submit_disabled',
            allow_frozen_emergency_close=self._is_frozen_bypass_emergency_close(plan),
        )
        if self.config.dry_run or not self.config.submit_enabled or not submit_gate['submit_allowed']:
            confirmation = PostTradeConfirmation(
                confirmation_status='UNCONFIRMED',
                confirmation_category='mismatch',
                order_status='NOT_SUBMITTED',
                exchange_order_ids=draft_ids,
                executed_qty=0.0,
                avg_fill_price=None,
                fees=0.0,
                fee_assets=[],
                fill_count=0,
                post_position_side=state.exchange_position_side,
                post_position_qty=state.exchange_position_qty,
                post_entry_price=state.exchange_entry_price,
                reconcile_status='DRY_RUN',
                should_freeze=False,
                freeze_reason=None,
                notes=['dry_run_no_submit'],
                trade_summary={
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'request_context': request_context,
                    'submit_gate': submit_gate,
                    'confirm_context': {
                        'confirm_attempted': False,
                        'confirm_path': 'skipped_because_not_submitted',
                    },
                    'draft_order_requests': [self._serialize_order_request(request) for request in order_requests],
                    'submitted': False,
                },
            )
            result = self._build_execution_result_from_confirmation(
                market=market,
                plan=plan,
                confirmation=confirmation,
                status='DRY_RUN',
                execution_phase='planned',
                error_code='NO_SUBMIT',
                error_message=f'draft_order_requests={draft_ids}',
                state=state,
            )
            return self._apply_freeze_if_needed(state, result)

        cancel_result = self._cancel_existing_protective_orders(cancel_requests)
        if not cancel_result['ok']:
            result = ExecutionResult(
                result_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                status='FROZEN',
                action_type=plan.action_type,
                executed_side=plan.target_side,
                reconcile_status=state.consistency_status,
                error_code='PROTECTIVE_CANCEL_FAILED',
                error_message=cancel_result['reason'],
                should_freeze=True,
                freeze_reason=cancel_result['reason'],
                state_updates={
                    'pending_execution_phase': 'frozen',
                    'protective_order_status': state.protective_order_status,
                    'exchange_protective_orders': state.exchange_protective_orders,
                    'protective_order_last_sync_ts': market.decision_ts,
                    'protective_order_last_sync_action': plan.action_type,
                    'protective_order_freeze_reason': cancel_result['reason'],
                    **self._build_protective_recover_state_updates(
                        state=state,
                        market=market,
                        plan=plan,
                        recover=protective_recover,
                        result=RECOVER_RESULT_BLOCKED,
                        reason=cancel_result['reason'],
                        allowed=False,
                    ),
                },
                execution_phase='frozen',
                confirmation_status='UNCONFIRMED',
                confirmed_order_status='CANCEL_FAILED',
                trade_summary={
                    'confirmation_category': 'mismatch',
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'request_context': request_context,
                    'submit_gate': submit_gate,
                    'protective_exchange_snapshot': protective_exchange_snapshot,
                    'protective_recover': protective_recover,
                    'protective_cancel_summary': cancel_result,
                },
            )
            return self._apply_freeze_if_needed(state, result)

        setattr(self, '_last_submit_exception_context', None)
        submit_receipts = self._submit_orders(order_requests)
        exception_context = getattr(self, '_last_submit_exception_context', None) or {}

        confirmation = self._confirm_with_short_retry_window(
            market=market,
            order_requests=order_requests,
            simulated_receipts=submit_receipts,
            allow_unacknowledged_lookup=bool(exception_context),
        )
        if exception_context:
            confirmation = PostTradeConfirmation(
                confirmation_status=confirmation.confirmation_status,
                confirmation_category=confirmation.confirmation_category,
                order_status=confirmation.order_status,
                exchange_order_ids=confirmation.exchange_order_ids,
                executed_qty=confirmation.executed_qty,
                avg_fill_price=confirmation.avg_fill_price,
                fees=confirmation.fees,
                fee_assets=confirmation.fee_assets,
                fill_count=confirmation.fill_count,
                post_position_side=confirmation.post_position_side,
                post_position_qty=confirmation.post_position_qty,
                post_entry_price=confirmation.post_entry_price,
                reconcile_status=confirmation.reconcile_status,
                should_freeze=confirmation.should_freeze,
                freeze_reason=confirmation.freeze_reason,
                notes=confirmation.notes,
                trade_summary={
                    **(confirmation.trade_summary or {}),
                    'submit_exception_policy': exception_context.get('exception_policy'),
                    'submit_exception_metadata': exception_context,
                    'exception_policy_view': exception_context.get('exception_policy_view'),
                    'exception_helper_plan': exception_context.get('exception_helper_plan'),
                    'protective_exchange_snapshot': protective_exchange_snapshot,
                    'protective_recover': protective_recover,
                    'protective_cancel_summary': cancel_result,
                },
            )
        else:
            confirmation = PostTradeConfirmation(
                confirmation_status=confirmation.confirmation_status,
                confirmation_category=confirmation.confirmation_category,
                order_status=confirmation.order_status,
                exchange_order_ids=confirmation.exchange_order_ids,
                executed_qty=confirmation.executed_qty,
                avg_fill_price=confirmation.avg_fill_price,
                fees=confirmation.fees,
                fee_assets=confirmation.fee_assets,
                fill_count=confirmation.fill_count,
                post_position_side=confirmation.post_position_side,
                post_position_qty=confirmation.post_position_qty,
                post_entry_price=confirmation.post_entry_price,
                reconcile_status=confirmation.reconcile_status,
                should_freeze=confirmation.should_freeze,
                freeze_reason=confirmation.freeze_reason,
                notes=confirmation.notes,
                trade_summary={
                    **(confirmation.trade_summary or {}),
                    'protective_exchange_snapshot': protective_exchange_snapshot,
                    'protective_recover': protective_recover,
                    'protective_cancel_summary': cancel_result,
                },
            )

        result = self._build_execution_result_from_confirmation(
            market=market,
            plan=plan,
            confirmation=confirmation,
            status=confirmation.order_status,
            execution_phase=self._map_execution_phase(confirmation),
            error_code=None,
            error_message=None,
            state=state,
        )
        return self._apply_freeze_if_needed(state, result)

    def _confirm_with_short_retry_window(
        self,
        *,
        market: MarketSnapshot,
        order_requests: list[BinanceOrderRequest],
        simulated_receipts: list[SimulatedExecutionReceipt],
        allow_unacknowledged_lookup: bool,
    ) -> PostTradeConfirmation:
        max_attempts = max(1, int(getattr(self.config, 'posttrade_confirm_retry_attempts', 5) or 5))
        retry_interval_seconds = max(0.0, float(getattr(self.config, 'posttrade_confirm_retry_interval_seconds', 3.0) or 0.0))
        attempt_trace: list[dict[str, Any]] = []

        confirmation = self.posttrade_confirmer.confirm(
            market=market,
            order_requests=order_requests,
            simulated_receipts=simulated_receipts,
            allow_unacknowledged_lookup=allow_unacknowledged_lookup,
        )
        attempts_used = 1
        attempt_trace.append(self._build_confirm_attempt_row(attempt=attempts_used, confirmation=confirmation))
        if not self._should_retry_posttrade_confirmation(confirmation=confirmation):
            stop_reason, stop_condition = self._derive_posttrade_retry_stop(confirmation=confirmation, attempts_used=attempts_used, max_attempts=1)
            return self._annotate_posttrade_retry_context(
                confirmation=confirmation,
                attempts_used=1,
                max_attempts=1,
                retry_interval_seconds=3.0,
                retried=False,
                attempt_trace=attempt_trace,
                stop_reason=stop_reason,
                stop_condition=stop_condition,
            )

        while attempts_used < max_attempts:
            time.sleep(retry_interval_seconds)
            attempts_used += 1
            confirmation = self.posttrade_confirmer.confirm(
                market=market,
                order_requests=order_requests,
                simulated_receipts=simulated_receipts,
                allow_unacknowledged_lookup=allow_unacknowledged_lookup,
            )
            attempt_trace.append(self._build_confirm_attempt_row(attempt=attempts_used, confirmation=confirmation))
            if not self._should_retry_posttrade_confirmation(confirmation=confirmation):
                break
        stop_reason, stop_condition = self._derive_posttrade_retry_stop(
            confirmation=confirmation,
            attempts_used=attempts_used,
            max_attempts=max_attempts,
        )
        return self._annotate_posttrade_retry_context(
            confirmation=confirmation,
            attempts_used=attempts_used,
            max_attempts=max_attempts,
            retry_interval_seconds=retry_interval_seconds,
            retried=attempts_used > 1,
            attempt_trace=attempt_trace,
            stop_reason=stop_reason,
            stop_condition=stop_condition,
        )

    def _should_retry_posttrade_confirmation(self, *, confirmation: PostTradeConfirmation) -> bool:
        if not bool(getattr(self.config, 'posttrade_confirm_retry_enabled', True)):
            return False
        if str(confirmation.confirmation_category or '') not in self.POSTTRADE_CONFIRM_RETRYABLE_CATEGORIES:
            return False
        if confirmation.confirmation_status == 'CONFIRMED':
            return False
        if confirmation.confirmation_status == 'POSITION_CONFIRMED':
            return self._position_confirmed_trade_reconcile_pending(confirmation=confirmation)
        if confirmation.confirmation_status not in {'PENDING', 'UNCONFIRMED'}:
            return False
        if confirmation.should_freeze and confirmation.freeze_reason not in {None, 'posttrade_pending_confirmation', 'posttrade_missing_fills'}:
            return False
        return True

    @staticmethod
    def _position_confirmed_trade_reconcile_pending(*, confirmation: PostTradeConfirmation) -> bool:
        trade_summary = confirmation.trade_summary or {}
        fills_count = int(trade_summary.get('fills_count') or confirmation.fill_count or 0)
        has_fill_evidence = bool(confirmation.executed_qty) or str(confirmation.order_status or '').upper() == 'FILLED'
        if confirmation.order_status == 'PARTIALLY_FILLED':
            return False
        if has_fill_evidence and fills_count <= 0:
            return True
        if confirmation.executed_qty and confirmation.avg_fill_price is None:
            return True
        if confirmation.executed_qty and not list(confirmation.fee_assets or []):
            return True
        return False

    @staticmethod
    def _build_confirm_attempt_row(*, attempt: int, confirmation: PostTradeConfirmation) -> dict[str, Any]:
        return {
            'attempt': attempt,
            'confirmation_status': confirmation.confirmation_status,
            'confirmation_category': confirmation.confirmation_category,
            'order_status': confirmation.order_status,
            'reconcile_status': confirmation.reconcile_status,
            'should_freeze': confirmation.should_freeze,
            'freeze_reason': confirmation.freeze_reason,
            'executed_qty': confirmation.executed_qty,
            'fill_count': confirmation.fill_count,
            'post_position_side': confirmation.post_position_side,
            'post_position_qty': confirmation.post_position_qty,
        }

    @staticmethod
    def _derive_posttrade_retry_stop(
        *,
        confirmation: PostTradeConfirmation,
        attempts_used: int,
        max_attempts: int,
    ) -> tuple[str, str]:
        if attempts_used >= max_attempts and confirmation.confirmation_category in {'pending', 'query_failed'}:
            return 'retry_budget_exhausted', 'max_attempts_reached'
        if confirmation.confirmation_status == 'CONFIRMED':
            return 'confirmed', 'terminal_confirmation_reached'
        if confirmation.confirmation_status == 'POSITION_CONFIRMED':
            trade_summary = confirmation.trade_summary or {}
            fills_count = int(trade_summary.get('fills_count') or confirmation.fill_count or 0)
            if str(confirmation.order_status or '').upper() == 'PARTIALLY_FILLED':
                return 'partial_position_working', 'partial_fill_position_working'
            if bool(trade_summary.get('protective_pending_confirm')):
                return 'protection_pending_confirm', 'position_confirmed_but_protection_pending'
            if confirmation.executed_qty and fills_count == 0:
                return 'position_confirmed_pending_trades', 'trade_rows_missing_after_fill'
            if confirmation.executed_qty and confirmation.avg_fill_price is None:
                return 'avg_fill_price_missing', 'avg_fill_price_missing_after_fills'
            if confirmation.executed_qty and not list(confirmation.fee_assets or []):
                return 'fee_reconciliation_pending', 'fee_reconciliation_pending'
            return 'confirmed', 'position_fact_confirmed_before_trade_rows'
        if confirmation.confirmation_category == 'mismatch':
            return 'mismatch', 'non_retryable_confirmation_category'
        if confirmation.confirmation_category == 'rejected':
            return 'rejected', 'terminal_exchange_rejection'
        return 'stopped', 'retry_not_required'

    @staticmethod
    def _annotate_posttrade_retry_context(
        *,
        confirmation: PostTradeConfirmation,
        attempts_used: int,
        max_attempts: int,
        retry_interval_seconds: float,
        retried: bool,
        attempt_trace: list[dict[str, Any]],
        stop_reason: str,
        stop_condition: str,
    ) -> PostTradeConfirmation:
        retry_budget = {
            'scope': 'executor_short_window',
            'attempts_used': attempts_used,
            'attempts_remaining': max(0, max_attempts - attempts_used),
            'max_attempts': max_attempts,
            'retry_interval_seconds': retry_interval_seconds,
        }
        trade_summary = {
            **(confirmation.trade_summary or {}),
            'posttrade_retry': {
                'enabled': max_attempts > 1,
                'retried': retried,
                'attempts_used': attempts_used,
                'max_attempts': max_attempts,
                'retry_interval_seconds': retry_interval_seconds,
                'exhausted': attempts_used >= max_attempts,
                'final_confirmation_category': confirmation.confirmation_category,
                'final_confirmation_status': confirmation.confirmation_status,
                'attempt_trace': attempt_trace,
                'retry_budget': retry_budget,
                'stop_reason': stop_reason,
                'stop_condition': stop_condition,
            },
            'confirm_context': build_confirm_context(
                phase='posttrade_confirm',
                confirmation=confirmation,
                attempts_used=attempts_used,
                max_attempts=max_attempts,
                retry_interval_seconds=retry_interval_seconds,
                retried=retried,
                attempt_trace=attempt_trace,
                retry_budget=retry_budget,
                stop_reason=stop_reason,
                stop_condition=stop_condition,
                extra={
                    'short_retry_window_enabled': max_attempts > 1,
                    'short_retry_window_retried': retried,
                    'short_retry_window_attempts_used': attempts_used,
                    'short_retry_window_max_attempts': max_attempts,
                    'short_retry_window_retry_interval_seconds': retry_interval_seconds,
                },
            ),
        }
        return PostTradeConfirmation(
            confirmation_status=confirmation.confirmation_status,
            confirmation_category=confirmation.confirmation_category,
            order_status=confirmation.order_status,
            exchange_order_ids=confirmation.exchange_order_ids,
            executed_qty=confirmation.executed_qty,
            avg_fill_price=confirmation.avg_fill_price,
            fees=confirmation.fees,
            fee_assets=confirmation.fee_assets,
            fill_count=confirmation.fill_count,
            post_position_side=confirmation.post_position_side,
            post_position_qty=confirmation.post_position_qty,
            post_entry_price=confirmation.post_entry_price,
            reconcile_status=confirmation.reconcile_status,
            should_freeze=confirmation.should_freeze,
            freeze_reason=confirmation.freeze_reason,
            notes=confirmation.notes,
            trade_summary=trade_summary,
        )

    def _load_exchange_protective_snapshot(self, symbol: str, *, state: LiveStateSnapshot | None = None) -> dict[str, Any]:
        state = state if state is not None else None
        protection_records = list((state.exchange_protective_orders if state is not None else []) or [])

        protection_client_ids = [
            str(item.get('client_order_id'))
            for item in protection_records
            if isinstance(item, dict) and item.get('client_order_id')
        ]
        protection_order_ids = [
            str(item.get('order_id'))
            for item in protection_records
            if isinstance(item, dict) and item.get('order_id') is not None and str(item.get('order_id'))
        ]
        open_orders = self.readonly_client.get_open_orders(
            symbol,
            order_ids=protection_order_ids,
            client_order_ids=protection_client_ids,
        )
        protective_open_orders, regular_open_orders = split_open_orders(open_orders)
        snapshot = snapshot_protective_orders(protective_open_orders)
        if snapshot.orders:
            return {
                'source': 'exchange_open_orders+algo_bridge',
                'protective_orders': snapshot.orders,
                'protective_order_count': len(snapshot.orders),
                'regular_open_order_count': len(regular_open_orders),
            }

        bootstrap_records = self._bootstrap_protective_lookup_records(symbol=symbol, state=state) if state is not None else []
        if bootstrap_records:
            return {
                'source': 'algo_order_precise_lookup',
                'protective_orders': bootstrap_records,
                'protective_order_count': len(bootstrap_records),
                'regular_open_order_count': len(regular_open_orders),
            }

        # During stop-replace the new closePosition algo order can be briefly invisible to both openOrders
        # and immediate order lookup, but the previous exchange protective snapshot is still the best bridge.
        if (
            state is not None
            and state.pending_execution_phase == 'protection_pending_confirm'
            and state.protective_order_status == 'PENDING_CONFIRM'
            and protection_records
        ):
            return {
                'source': 'state_exchange_protective_orders_bridge',
                'protective_orders': protection_records,
                'protective_order_count': len(protection_records),
                'regular_open_order_count': len(regular_open_orders),
            }

        return {
            'source': 'exchange_open_orders+algo_bridge',
            'protective_orders': [],
            'protective_order_count': 0,
            'regular_open_order_count': len(regular_open_orders),
        }

    def _bootstrap_protective_lookup_records(self, *, symbol: str, state: LiveStateSnapshot) -> list[dict[str, Any]]:
        if state.active_strategy not in {'trend', 'rev'}:
            return []
        if state.exchange_position_side not in {'long', 'short'}:
            return []
        if float(state.exchange_position_qty or 0.0) <= 0.0:
            return []
        if not state.strategy_entry_time or state.stop_price is None:
            return []

        kinds = ['hard_stop']
        if state.active_strategy == 'rev' and state.tp_price is not None:
            kinds.append('take_profit')

        recovered: list[dict[str, Any]] = []
        for kind in kinds:
            client_order_id = self._build_client_order_id(state.strategy_entry_time, f'protect-{kind}')
            try:
                snapshot = self.readonly_client.get_order(symbol=symbol, client_order_id=client_order_id)
            except Exception:
                continue
            recovered.append(serialize_protective_order(snapshot))
        return recovered

    def _recover_protective_orders_before_submit(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        exchange_snapshot: dict[str, Any] | None,
    ) -> dict[str, Any]:
        exchange_orders = list((exchange_snapshot or {}).get('protective_orders') or [])
        position_snapshot = self.readonly_client.get_position_snapshot(market.symbol)
        position_side = position_snapshot.side
        position_qty = float(position_snapshot.qty or 0.0)
        state_orders = list(state.exchange_protective_orders or [])

        recover = {
            'attempted': True,
            'action_type': plan.action_type,
            'fact_source': 'positionRisk+openOrders',
            'position_fact': {
                'side': position_side,
                'qty': position_qty,
                'entry_price': position_snapshot.entry_price,
            },
            'exchange_order_count': len(exchange_orders),
            'state_order_count': len(state_orders),
            'attempts': [],
            'result': 'NOOP',
            'remaining_risk': None,
            'effective_exchange_snapshot': exchange_snapshot or {
                'source': 'exchange_open_orders',
                'protective_orders': [],
                'protective_order_count': 0,
                'regular_open_order_count': 0,
            },
            'state_updates': {},
        }

        if plan.action_type == 'close' and position_qty <= 0.0:
            recover['result'] = 'FLAT_CONFIRMED'
            recover['remaining_risk'] = 'none'
            recover['attempts'].append({'step': 'position_fact', 'result': 'flat_confirmed_for_close'})
            return recover

        expected_strategy = plan.target_strategy
        expected_side = position_side if plan.action_type in {'close', 'trim', 'add'} else (plan.target_side or position_side)
        expected_stop_price = plan.stop_price if plan.action_type != 'close' else None
        expected_tp_price = (plan.conflict_context or {}).get('tp_price') if plan.action_type != 'close' else None
        expected_qty = None
        if plan.action_type == 'close':
            expected_qty = 0.0
        elif plan.action_type == 'trim':
            current_qty = float(state.exchange_position_qty or position_qty)
            trim_qty = self._resolve_order_quantity(plan.action_type, plan, state, market, self.readonly_client.get_exchange_info(market.symbol))
            expected_qty = max(current_qty - float(trim_qty or 0.0), 0.0)
        elif plan.action_type == 'add':
            current_qty = float(state.exchange_position_qty or position_qty)
            add_qty = self._resolve_order_quantity(plan.action_type, plan, state, market, self.readonly_client.get_exchange_info(market.symbol))
            expected_qty = current_qty + float(add_qty or 0.0)
        elif plan.action_type in {'open', 'flip'}:
            expected_qty = self._resolve_order_quantity(plan.action_type if plan.action_type != 'flip' else 'open', plan, state, market, self.readonly_client.get_exchange_info(market.symbol))

        if exchange_orders and state_orders:
            exchange_ids = {str(item.get('order_id')) for item in exchange_orders if item.get('order_id') is not None}
            state_ids = {str(item.get('order_id')) for item in state_orders if item.get('order_id') is not None}
            if exchange_ids and state_ids and exchange_ids != state_ids:
                recover['attempts'].append({'step': 'rebuild_state_mapping', 'result': 'rebuilt_from_exchange', 'exchange_ids': sorted(exchange_ids), 'state_ids': sorted(state_ids)})
                recover['state_updates'].update({
                    'exchange_protective_orders': exchange_orders,
                    'protective_order_status': 'ACTIVE' if exchange_orders else 'NONE',
                    'protective_order_freeze_reason': None,
                })
                recover['result'] = 'STATE_REBUILT'

        if not exchange_orders and state_orders:
            recover['attempts'].append({'step': 'stale_state_cleanup', 'result': 'state_snapshot_cleared'})
            recover['state_updates'].update({
                'exchange_protective_orders': [],
                'protective_order_status': 'NONE' if position_qty <= 0.0 else state.protective_order_status,
                'protective_order_freeze_reason': None,
            })
            recover['result'] = 'STATE_REBUILT'

        if plan.action_type == 'protective_rebuild':
            can_validate_current_position = position_side in {'long', 'short'} and position_qty > 0.0 and plan.stop_price is not None
            if can_validate_current_position:
                validation = self._validate_protective_snapshot_from_serialized(
                    strategy=plan.target_strategy,
                    position_side=position_side,
                    position_qty=position_qty,
                    stop_price=plan.stop_price,
                    tp_price=(plan.conflict_context or {}).get('tp_price'),
                    serialized_orders=exchange_orders,
                )
                recover['validation'] = validation
                if validation['ok']:
                    recover['attempts'].append({'step': 'protective_rebuild_validate', 'result': 'already_valid'})
                    recover['state_updates'].update({
                        'exchange_protective_orders': exchange_orders,
                        'protective_order_status': 'ACTIVE',
                        'protective_order_freeze_reason': None,
                    })
                    recover['result'] = 'VALID_ON_EXCHANGE'
                    recover['remaining_risk'] = 'none'
                    return recover
                recover['attempts'].append({'step': 'protective_rebuild_validate', 'result': 'invalid', 'freeze_reason': validation['freeze_reason'], 'notes': validation['notes']})

            if exchange_orders:
                missing_identity = [item for item in exchange_orders if item.get('order_id') is None and not item.get('client_order_id')]
                if missing_identity:
                    recover['result'] = 'BLOCKED'
                    recover['remaining_risk'] = 'cannot_safely_cancel_existing_protective_orders'
                    raise ValueError('protective_cancel_identity_missing')
                recover['effective_exchange_snapshot'] = {
                    **(exchange_snapshot or {}),
                    'protective_orders': exchange_orders,
                    'protective_order_count': len(exchange_orders),
                }
                recover['state_updates'].update({
                    'exchange_protective_orders': exchange_orders,
                    'protective_order_status': 'ACTIVE',
                    'protective_order_freeze_reason': None,
                })
                recover['result'] = 'CANCEL_USING_EXCHANGE_FACTS'
                recover['remaining_risk'] = 'replace_invalid_protective_orders'
                return recover

            recover['attempts'].append({'step': 'protective_rebuild_submit', 'result': 'submit_new_protective_orders'})
            recover['state_updates'].update({
                'exchange_protective_orders': [],
                'protective_order_status': 'MISSING' if position_qty > 0.0 else 'NONE',
                'protective_order_freeze_reason': None,
            })
            recover['result'] = 'RECREATE_ON_SUBMIT'
            recover['remaining_risk'] = 'await_submit_and_posttrade_confirmation'
            return recover

        can_validate = expected_side in {'long', 'short'} and expected_qty is not None and expected_qty > 0.0 and expected_stop_price is not None
        if can_validate:
            validation = self._validate_protective_snapshot_from_serialized(
                strategy=expected_strategy,
                position_side=expected_side,
                position_qty=float(expected_qty),
                stop_price=expected_stop_price,
                tp_price=expected_tp_price,
                serialized_orders=exchange_orders,
            )
            recover['validation'] = validation
            if validation['ok']:
                recover['attempts'].append({'step': 'validate_exchange_orders', 'result': 'acceptable'})
                recover['state_updates'].update({
                    'exchange_protective_orders': exchange_orders,
                    'protective_order_status': 'ACTIVE',
                    'protective_order_freeze_reason': None,
                })
                recover['result'] = 'VALID_ON_EXCHANGE'
                recover['remaining_risk'] = 'none'
                return recover
            recover['attempts'].append({'step': 'validate_exchange_orders', 'result': 'invalid', 'freeze_reason': validation['freeze_reason'], 'notes': validation['notes']})

        if plan.action_type in {'close', 'flip', 'add', 'trim'} and exchange_orders:
            missing_identity = [item for item in exchange_orders if item.get('order_id') is None and not item.get('client_order_id')]
            if missing_identity:
                recover['result'] = 'BLOCKED'
                recover['remaining_risk'] = 'cannot_safely_cancel_existing_protective_orders'
                raise ValueError('protective_cancel_identity_missing')
            recover['effective_exchange_snapshot'] = {
                **(exchange_snapshot or {}),
                'protective_orders': exchange_orders,
                'protective_order_count': len(exchange_orders),
            }
            recover['state_updates'].update({
                'exchange_protective_orders': exchange_orders,
                'protective_order_status': 'ACTIVE',
                'protective_order_freeze_reason': None,
            })
            if recover['result'] == 'NOOP':
                recover['result'] = 'CANCEL_USING_EXCHANGE_FACTS'
            recover['remaining_risk'] = 'will_replace_existing_protective_orders_during_submit'
            return recover

        if plan.action_type in {'open', 'flip', 'add', 'trim'} and not exchange_orders:
            recover['attempts'].append({'step': 'allow_recreate', 'result': 'submit_path_will_create_protective_orders'})
            recover['state_updates'].update({
                'exchange_protective_orders': [],
                'protective_order_status': 'MISSING' if position_qty > 0.0 else 'NONE',
                'protective_order_freeze_reason': None,
            })
            recover['result'] = 'RECREATE_ON_SUBMIT'
            recover['remaining_risk'] = 'await_submit_and_posttrade_confirmation'
            return recover

        if plan.action_type == 'close' and not exchange_orders:
            recover['result'] = 'NO_PROTECTIVE_ON_EXCHANGE'
            recover['remaining_risk'] = 'position_open_without_protection'
            recover['state_updates'].update({
                'exchange_protective_orders': [],
                'protective_order_status': 'MISSING' if position_qty > 0.0 else 'NONE',
                'protective_order_freeze_reason': 'protective_order_missing' if position_qty > 0.0 else None,
            })
            return recover

        recover['remaining_risk'] = 'unclassified_protective_order_state'
        return recover

    def _validate_protective_snapshot_from_serialized(
        self,
        *,
        strategy: str | None,
        position_side: str | None,
        position_qty: float,
        stop_price: float | None,
        tp_price: float | None,
        serialized_orders: list[dict[str, Any]],
    ) -> dict[str, Any]:
        open_orders = [
            self._serialized_to_order_snapshot(item)
            for item in serialized_orders
        ]
        validation = validate_protective_orders(
            strategy=strategy,
            position_side=position_side,
            position_qty=position_qty,
            stop_price=stop_price,
            tp_price=tp_price,
            open_orders=open_orders,
        )
        return {
            'ok': validation.ok,
            'freeze_reason': validation.freeze_reason,
            'status': validation.status,
            'notes': list(validation.notes),
            'summary': dict(validation.summary),
        }

    @staticmethod
    def _protective_recover_has_negative_fact(
        *,
        recover: dict[str, Any] | None,
        protective_validation: dict[str, Any] | None,
        submit_readback_empty: bool,
    ) -> bool:
        recover = dict(recover or {})
        protective_validation = dict(protective_validation or {})
        validation_level = str(protective_validation.get('validation_level') or protective_validation.get('status') or '').upper()
        if validation_level in _NEGATIVE_PROTECTIVE_VALIDATION_LEVELS:
            return True
        if submit_readback_empty:
            return True
        remaining_risk = str(recover.get('remaining_risk') or '').strip()
        if remaining_risk in _NEGATIVE_PROTECTIVE_RECOVER_RISKS:
            return True
        for attempt in list(recover.get('attempts') or []):
            if str((attempt or {}).get('step') or '') == 'protective_rebuild_validate' and str((attempt or {}).get('result') or '') == 'invalid':
                return True
        return False

    def _build_protective_recover_state_updates(
        self,
        *,
        state: LiveStateSnapshot,
        market: MarketSnapshot,
        plan: FinalActionPlan,
        recover: dict[str, Any] | None,
        result: str,
        reason: str,
        allowed: bool,
    ) -> dict[str, Any]:
        if recover is None:
            return {}
        checked_at = market.decision_ts
        state_updates = dict(recover.get('state_updates') or {})
        recover_check = build_recover_record(
            checked_at=checked_at,
            source='protective_order_recover',
            result=result,
            allowed=allowed,
            reason=reason,
            pending_execution_phase=state.pending_execution_phase,
            freeze_reason=None if allowed else reason,
            consistency_status=state.consistency_status,
            runtime_mode=state.runtime_mode,
            recover_ready=allowed,
            requires_manual_resume=False,
            guard_decision='protective_recover_first',
        )
        remaining_risk = recover.get('remaining_risk')
        position_fact = dict(recover.get('position_fact') or {})
        dangerous_missing_protection = bool(
            recover.get('result') == 'NO_PROTECTIVE_ON_EXCHANGE'
            and float(position_fact.get('qty') or 0.0) > 0.0
        )
        effective_allowed = bool(allowed and not dangerous_missing_protection)
        effective_reason = 'protective_order_missing' if dangerous_missing_protection else reason
        details = {
            **recover_check,
            'allowed': effective_allowed,
            'reason': effective_reason,
            'freeze_reason': None if effective_allowed else effective_reason,
            'recover_ready': effective_allowed,
            'requires_manual_resume': not effective_allowed,
            'action_type': plan.action_type,
            'remaining_risk': remaining_risk,
            'attempts': list(recover.get('attempts') or []),
            'result_detail': recover.get('result'),
            'position_fact': position_fact,
        }
        effective_snapshot = dict(recover.get('effective_exchange_snapshot') or {})
        protective_validation = dict(recover.get('validation') or {})
        submit_readback_empty = bool(dict(protective_validation.get('summary') or {}).get('submit_readback_empty'))
        negative_recover_fact = self._protective_recover_has_negative_fact(
            recover=recover,
            protective_validation=protective_validation,
            submit_readback_empty=submit_readback_empty,
        )
        positive_recover_fact = bool(
            effective_allowed
            and not negative_recover_fact
            and result in {'VALID_ON_EXCHANGE', 'STATE_REBUILT'}
            and len(list(state_updates.get('exchange_protective_orders') or effective_snapshot.get('protective_orders') or [])) >= 1
        )
        if positive_recover_fact:
            details.update({
                'recover_policy': 'recover_ready',
                'recover_policy_display': 'recover_ready',
                'legacy_recover_policy': 'recover_ready',
                'recover_stage': 'recover_ready',
                'risk_action': 'OBSERVE',
                'stop_reason': 'success_protective_visible',
                'stop_category': 'observe_only',
                'stop_condition': 'protective_order_visible_on_exchange',
                'remaining_risk': 'none',
            })
        elif dangerous_missing_protection:
            details.update({
                'recover_policy': 'observe_pending',
                'recover_policy_display': 'observe_pending',
                'legacy_recover_policy': 'observe_pending',
                'recover_stage': 'observe_pending',
                'risk_action': 'FORCE_CLOSE',
                'stop_reason': 'protective_order_missing',
                'stop_category': 'frozen',
                'stop_condition': 'position_open_without_protection',
            })
        updates = {
            'last_recover_result': RECOVER_RESULT_ALLOWED if effective_allowed else RECOVER_RESULT_BLOCKED,
            'last_recover_at': checked_at,
            'recover_attempt_count': int(state.recover_attempt_count or 0) + 1,
            'recover_check': details,
            'recover_timeline': append_recover_record(state.recover_timeline, details),
        }
        updates.update(state_updates)
        return updates

    @staticmethod
    def _serialized_to_order_snapshot(item: dict[str, Any]):
        from .binance_readonly import OrderSnapshot

        return OrderSnapshot(
            order_id=str(item.get('order_id')) if item.get('order_id') is not None else '',
            client_order_id=item.get('client_order_id'),
            status=str(item.get('status') or 'UNKNOWN'),
            type=item.get('type'),
            time_in_force=None,
            side=item.get('side'),
            position_side=item.get('position_side'),
            qty=item.get('qty'),
            executed_qty=item.get('executed_qty'),
            price=item.get('price'),
            avg_price=item.get('avg_price'),
            cum_quote=None,
            stop_price=item.get('stop_price'),
            working_type=item.get('working_type'),
            orig_type=item.get('orig_type'),
            activate_price=item.get('activate_price'),
            price_protect=item.get('price_protect'),
            reduce_only=item.get('reduce_only'),
            close_position=item.get('close_position'),
            update_time_ms=item.get('update_time_ms'),
            raw=dict(item),
        )

    def _build_protective_cancel_requests(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        exchange_snapshot: dict[str, Any] | None,
    ) -> list[BinanceCancelOrderRequest]:
        if plan.action_type not in {'close', 'flip', 'add', 'trim', 'protective_rebuild'}:
            return []

        exchange_orders = list((exchange_snapshot or {}).get('protective_orders') or [])
        state_orders = list(state.exchange_protective_orders or [])
        position_open = state.exchange_position_side in {'long', 'short'} and float(state.exchange_position_qty or 0.0) > 0.0

        if not exchange_orders:
            if state_orders and state.protective_order_status in {'ACTIVE', 'MISSING', 'UNEXPECTED_WHILE_FLAT'}:
                raise ValueError('protective_orders_exchange_state_mismatch')
            if plan.action_type == 'close':
                return []
            if plan.action_type == 'protective_rebuild':
                return []
            return []

        if any(order.get('kind') is None for order in exchange_orders):
            raise ValueError('protective_orders_exchange_dirty')
        if any((order.get('order_id') is None and not order.get('client_order_id')) for order in exchange_orders):
            raise ValueError('protective_cancel_identity_missing')

        exchange_order_ids = {str(order.get('order_id')) for order in exchange_orders if order.get('order_id') is not None}
        state_order_ids = {str(order.get('order_id')) for order in state_orders if order.get('order_id') is not None}
        if state_order_ids and exchange_order_ids and state_order_ids != exchange_order_ids:
            return [
                BinanceCancelOrderRequest(
                    symbol=market.symbol,
                    order_id=str(order.get('order_id')) if order.get('order_id') is not None else None,
                    client_order_id=order.get('client_order_id'),
                    metadata={
                        'phase': 'protective_cancel',
                        'replace_during': plan.action_type,
                        'protective_kind': order.get('kind'),
                        'protective_source': 'exchange_open_orders',
                        'algo_order': True,
                    },
                )
                for order in exchange_orders
            ]

        requests: list[BinanceCancelOrderRequest] = []
        for order in exchange_orders:
            requests.append(
                BinanceCancelOrderRequest(
                    symbol=market.symbol,
                    order_id=(str(order.get('order_id')) if order.get('order_id') is not None else None),
                    client_order_id=(str(order.get('client_order_id')) if order.get('client_order_id') else None),
                    metadata={
                        'phase': 'protective_cancel',
                        'source_action': plan.action_type,
                        'protective_kind': order.get('kind'),
                        'protective_source': 'exchange_open_orders',
                        'algo_order': True,
                    },
                )
            )
        return requests

    def _cancel_existing_protective_orders(self, cancel_requests: list[BinanceCancelOrderRequest]) -> dict[str, Any]:
        if not cancel_requests:
            return {'ok': True, 'reason': None, 'cancel_count': 0, 'receipts': []}

        receipts: list[dict[str, Any]] = []
        for request in cancel_requests:
            signed_request = self.submit_client.build_cancel_request(
                symbol=request.symbol,
                order_id=request.order_id,
                client_order_id=request.client_order_id,
                metadata=dict(request.metadata or {}),
            )
            prepared = self.submit_client.prepare_signed_cancel(signed_request)
            try:
                _, receipt = self.submit_client.cancel_order(signed_request)
                receipts.append(self._serialize_cancel_receipt(receipt))
            except BinanceSubmitError as exc:
                idempotent_receipt = self._build_idempotent_cancel_success_receipt(
                    request=request,
                    prepared=prepared,
                    exc=exc,
                )
                if idempotent_receipt is not None:
                    receipts.append(idempotent_receipt)
                    continue
                receipts.append({
                    'client_order_id': request.client_order_id,
                    'exchange_order_id': request.order_id,
                    'canceled': False,
                    'cancel_status': 'ERROR',
                    'request_payload': prepared.body_redacted,
                    'response_payload': getattr(exc, 'detail', None),
                    'metadata': {'future_endpoint': ('/fapi/v1/algoOrder' if (request.metadata or {}).get('algo_order') else '/fapi/v1/order'), **dict(request.metadata or {})},
                    'error_code': exc.category.upper(),
                    'error_message': str(exc),
                })
                return {
                    'ok': False,
                    'reason': 'protective_cancel_failed',
                    'cancel_count': len(cancel_requests),
                    'receipts': receipts,
                }
        return {'ok': True, 'reason': None, 'cancel_count': len(cancel_requests), 'receipts': receipts}

    def _build_idempotent_cancel_success_receipt(
        self,
        *,
        request: BinanceCancelOrderRequest,
        prepared: Any,
        exc: BinanceSubmitError,
    ) -> dict[str, Any] | None:
        detail = getattr(exc, 'detail', None)
        classification = self._classify_cancel_exception_as_idempotent_success(request=request, exc=exc)
        if not classification.get('success'):
            return None
        return {
            'client_order_id': request.client_order_id,
            'exchange_order_id': request.order_id,
            'canceled': True,
            'cancel_status': 'IDEMPOTENT_SUCCESS',
            'request_payload': prepared.body_redacted,
            'response_payload': detail,
            'metadata': {
                'future_endpoint': ('/fapi/v1/algoOrder' if (request.metadata or {}).get('algo_order') else '/fapi/v1/order'),
                **dict(request.metadata or {}),
                'idempotent_success': True,
                'idempotent_reason': classification.get('reason'),
                'idempotent_query_result': classification.get('query_result'),
                'idempotent_query_status': classification.get('query_status'),
            },
            'error_code': exc.category.upper(),
            'error_message': str(exc),
        }

    def _classify_cancel_exception_as_idempotent_success(
        self,
        *,
        request: BinanceCancelOrderRequest,
        exc: BinanceSubmitError,
    ) -> dict[str, Any]:
        detail = getattr(exc, 'detail', None)
        payload = detail if isinstance(detail, dict) else {}
        payload_text = json.dumps(payload, ensure_ascii=False, sort_keys=True) if isinstance(payload, dict) else str(detail or '')
        message_text = f"{str(exc)} {payload_text}".lower()

        if self._looks_like_cancel_terminal_response(payload, message_text=message_text):
            return {
                'success': True,
                'reason': 'cancel_terminal_http_response',
                'query_result': 'not_needed',
                'query_status': self._extract_terminal_status(payload, message_text=message_text),
            }

        if not self._looks_like_cancel_not_found_or_query_miss(payload, message_text=message_text):
            return {'success': False}

        query = self._query_cancel_terminal_state(request)
        if query.get('result') in {'terminal', 'query_miss'}:
            return {
                'success': True,
                'reason': ('cancel_query_terminal' if query.get('result') == 'terminal' else 'cancel_query_miss_after_not_found'),
                'query_result': query.get('result'),
                'query_status': query.get('status'),
            }
        return {'success': False}

    @staticmethod
    def _extract_terminal_status(payload: dict[str, Any] | None, *, message_text: str = '') -> str | None:
        payload = payload or {}
        for key in ('status', 'orderStatus', 'algoStatus'):
            value = payload.get(key)
            if value:
                return str(value).upper()
        if 'already cancel' in message_text or 'already canceled' in message_text or 'already cancelled' in message_text:
            return 'CANCELED'
        return None

    @staticmethod
    def _looks_like_cancel_terminal_response(payload: dict[str, Any] | None, *, message_text: str = '') -> bool:
        status = BinanceRealExecutor._extract_terminal_status(payload, message_text=message_text)
        return status in {'CANCELED', 'CANCELLED', 'EXPIRED', 'FILLED', 'REJECTED'}

    @staticmethod
    def _looks_like_cancel_not_found_or_query_miss(payload: dict[str, Any] | None, *, message_text: str = '') -> bool:
        payload = payload or {}
        code = payload.get('code')
        msg = str(payload.get('msg') or payload.get('message') or '')
        merged = f'{message_text} {msg}'.lower()
        if code in {-2011, '-2011'}:
            return True
        phrases = (
            'unknown order',
            'order does not exist',
            'not found',
            'no such order',
            'already canceled',
            'already cancelled',
            'already cancel',
            'query miss',
            'does not exist',
        )
        return any(token in merged for token in phrases)

    def _query_cancel_terminal_state(self, request: BinanceCancelOrderRequest) -> dict[str, Any]:
        try:
            snapshot = self.readonly_client.get_order(
                symbol=request.symbol,
                order_id=request.order_id,
                client_order_id=request.client_order_id,
            )
        except Exception:
            return {
                'result': 'query_miss',
                'status': None,
            }
        if not isinstance(snapshot, OrderSnapshot):
            return {
                'result': 'query_unknown',
                'status': None,
            }
        status = str(getattr(snapshot, 'status', None) or '').upper() or None
        if status in {'CANCELED', 'CANCELLED', 'EXPIRED', 'FILLED', 'REJECTED'}:
            return {
                'result': 'terminal',
                'status': status,
            }
        return {
            'result': 'still_active',
            'status': status,
        }

    def _submit_orders(self, order_requests: list[BinanceOrderRequest]) -> list[SimulatedExecutionReceipt]:
        receipts: list[SimulatedExecutionReceipt] = []
        exception_rows: list[dict[str, Any]] = []
        gate_blocked = False
        reduce_only_repair_attempted = False
        timestamp_repair_attempted = False
        setattr(self, '_last_reduce_only_repair_context', None)
        setattr(self, '_last_timestamp_repair_context', None)
        for request in order_requests:
            payload = self._serialize_submit_payload(request)
            signed_request = self.submit_client.build_submit_request(payload, metadata=dict(request.metadata or {}))
            prepared = self.submit_client.prepare_signed_post(signed_request)
            try:
                _, receipt = self.submit_client.submit_order(signed_request)
                receipts.append(self._receipt_from_submit_receipt(receipt))
            except BinanceSubmitError as exc:
                repaired = False
                if not reduce_only_repair_attempted and self._is_reduce_only_repair_candidate(request, exc):
                    reduce_only_repair_attempted = True
                    repair_outcome = self._attempt_reduce_only_conflict_repair(request)
                    repair_outcome.setdefault('action_type', str((request.metadata or {}).get('source_action') or 'close'))
                    repair_outcome.setdefault('source_action', str((request.metadata or {}).get('source_action') or 'close'))
                    repair_outcome.setdefault('repair_target', 'reduce_only_close_submit')
                    repair_outcome.setdefault('submit_exception_category', exc.category)
                    repair_outcome.setdefault('error_code', -2022)
                    repair_outcome.setdefault('stop_reason', 'submit_auto_repair_pending')
                    repair_outcome.setdefault('stop_condition', 'await_repair_retry')
                    if repair_outcome.get('attempted') and repair_outcome.get('retry_signed_request') is not None:
                        repaired = True
                        try:
                            _, retried_receipt = self.submit_client.submit_order(repair_outcome['retry_signed_request'])
                            base_receipt = self._receipt_from_submit_receipt(retried_receipt)
                            repair_outcome['retry_submitted'] = True
                            repair_outcome['retry_client_order_id'] = getattr(repair_outcome['retry_signed_request'], 'client_order_id', None)
                            repair_outcome['stop_reason'] = 'submit_auto_repair_retry_submitted'
                            repair_outcome['stop_condition'] = 'retry_submit_dispatched'
                            receipt_row = replace(
                                base_receipt,
                                metadata={
                                    **dict(base_receipt.metadata or {}),
                                    'auto_repair': repair_outcome,
                                },
                            )
                            receipts.append(receipt_row)
                            continue
                        except BinanceSubmitError as retry_exc:
                            exc = retry_exc
                            repair_outcome['retry_submitted'] = False
                            repair_outcome['retry_client_order_id'] = getattr(repair_outcome['retry_signed_request'], 'client_order_id', None)
                            repair_outcome['stop_reason'] = 'repair_retry_failed'
                            repair_outcome['stop_condition'] = 'repair_retry_failed'
                            prepared = self.submit_client.prepare_signed_post(repair_outcome['retry_signed_request'])
                            setattr(self, '_last_reduce_only_repair_context', repair_outcome)
                elif not timestamp_repair_attempted and self._is_timestamp_drift_repair_candidate(request, exc):
                    timestamp_repair_attempted = True
                    repair_outcome = self._attempt_timestamp_drift_repair(request)
                    repair_outcome.setdefault('action_type', str((request.metadata or {}).get('source_action') or 'submit'))
                    repair_outcome.setdefault('source_action', str((request.metadata or {}).get('source_action') or 'submit'))
                    repair_outcome.setdefault('repair_target', 'signed_submit_timestamp')
                    repair_outcome.setdefault('submit_exception_category', exc.category)
                    repair_outcome.setdefault('error_code', -1021)
                    repair_outcome.setdefault('stop_reason', 'submit_auto_repair_pending')
                    repair_outcome.setdefault('stop_condition', 'await_repair_retry')
                    if repair_outcome.get('attempted') and repair_outcome.get('retry_signed_request') is not None:
                        repaired = True
                        try:
                            _, retried_receipt = self.submit_client.submit_order(repair_outcome['retry_signed_request'])
                            base_receipt = self._receipt_from_submit_receipt(retried_receipt)
                            repair_outcome['retry_submitted'] = True
                            repair_outcome['retry_client_order_id'] = getattr(repair_outcome['retry_signed_request'], 'client_order_id', None)
                            repair_outcome['stop_reason'] = 'submit_auto_repair_retry_submitted'
                            repair_outcome['stop_condition'] = 'retry_submit_dispatched'
                            receipt_row = replace(
                                base_receipt,
                                metadata={
                                    **dict(base_receipt.metadata or {}),
                                    'auto_repair': repair_outcome,
                                },
                            )
                            receipts.append(receipt_row)
                            continue
                        except BinanceSubmitError as retry_exc:
                            exc = retry_exc
                            repair_outcome['retry_submitted'] = False
                            repair_outcome['retry_client_order_id'] = getattr(repair_outcome['retry_signed_request'], 'client_order_id', None)
                            repair_outcome['stop_reason'] = 'repair_retry_failed'
                            repair_outcome['stop_condition'] = 'repair_retry_failed'
                            prepared = self.submit_client.prepare_signed_post(repair_outcome['retry_signed_request'])
                            setattr(self, '_last_timestamp_repair_context', repair_outcome)
                policy = classify_submit_exception_detail(getattr(exc, 'detail', None))
                helper_plan = build_guarded_exception_plan(
                    policy,
                    runtime_mode='ACTIVE',
                    manual_ack_present=bool((self.config.submit_manual_ack_token or '').strip()),
                    automation_enabled=False,
                )
                submit_gate = self._evaluate_submit_gate(
                    market=MarketSnapshot(
                        decision_ts='',
                        bar_ts='',
                        strategy_ts=None,
                        execution_attributed_bar=None,
                        symbol=request.symbol,
                        preclose_offset_seconds=0,
                        current_price=0.0,
                        source_status='UNKNOWN',
                    ),
                    state=LiveStateSnapshot(
                        state_ts='',
                        consistency_status='OK',
                        freeze_reason=None,
                        account_equity=0.0,
                        available_margin=0.0,
                        exchange_position_side=None,
                        exchange_position_qty=0.0,
                        exchange_entry_price=None,
                        active_strategy='none',
                        active_side=None,
                        strategy_entry_time=None,
                        strategy_entry_price=None,
                        stop_price=None,
                        risk_fraction=None,
                        runtime_mode='ACTIVE',
                    ),
                    order_requests=[request],
                    blocked_reason=exc.category,
                )
                exception_metadata = self._build_submit_exception_metadata(
                    category=exc.category,
                    submit_gate=submit_gate,
                    exception_policy=policy.as_dict(),
                    exception_policy_view=helper_plan.policy,
                    exception_helper_plan=helper_plan.as_dict(),
                )
                repair_context = getattr(self, '_last_reduce_only_repair_context', None)
                if repair_context is None:
                    repair_context = getattr(self, '_last_timestamp_repair_context', None)
                if repair_context is not None:
                    exception_metadata['auto_repair'] = repair_context
                exception_rows.append(
                    {
                        'client_order_id': request.client_order_id,
                        'category': exc.category,
                        'message': str(exc),
                        'submit_gate': submit_gate,
                        'exception_policy': policy.as_dict(),
                    }
                )
                gate_blocked = gate_blocked or exc.category in {'submit_gate_blocked', 'submit_http_disabled'}
                setattr(self, '_last_submit_exception_context', {
                    **exception_metadata,
                    'submit_exception_rows': exception_rows,
                    'any_submit_succeeded': bool(receipts),
                    'all_submit_failed': not bool(receipts),
                    'gate_blocked': gate_blocked,
                })
                receipts.append(
                    SimulatedExecutionReceipt(
                        client_order_id=request.client_order_id,
                        exchange_order_id=None,
                        acknowledged=False,
                        submitted_qty=request.quantity,
                        submitted_side=request.side,
                        submit_status='BLOCKED' if exc.category == 'submit_gate_blocked' else ('DISABLED' if exc.category == 'submit_http_disabled' else 'ERROR'),
                        exchange_status=None,
                        transact_time_ms=None,
                        request_payload=prepared.body_redacted,
                        response_payload=getattr(exc, 'detail', None),
                        metadata={
                            'submit_attempted': False if exc.category in {'submit_gate_blocked', 'submit_http_disabled'} else True,
                            'future_endpoint': '/fapi/v1/order',
                            **getattr(self, '_last_submit_exception_context'),
                        },
                        error_code=exc.category.upper(),
                        error_message=str(exc),
                    )
                )
                if gate_blocked:
                    break
        return receipts

    def _is_reduce_only_repair_candidate(self, request: BinanceOrderRequest, exc: BinanceSubmitError) -> bool:
        if request.order_type.upper() != 'MARKET':
            return False
        if not request.reduce_only:
            return False
        source_action = str((request.metadata or {}).get('source_action') or '').lower()
        if source_action not in self.REDUCE_ONLY_REPAIRABLE_ACTIONS:
            return False
        payload = getattr(exc, 'detail', None) or {}
        payload = payload.get('payload') if isinstance(payload, dict) else None
        code = None if not isinstance(payload, dict) else payload.get('code')
        try:
            code = int(code)
        except (TypeError, ValueError):
            code = None
        return code == -2022

    def _is_timestamp_drift_repair_candidate(self, request: BinanceOrderRequest, exc: BinanceSubmitError) -> bool:
        if request.order_type.upper() != 'MARKET':
            return False
        source_action = str((request.metadata or {}).get('source_action') or request.metadata.get('action_type') or '').lower()
        if source_action not in self.TIMESTAMP_DRIFT_REPAIRABLE_ACTIONS:
            return False
        payload = getattr(exc, 'detail', None) or {}
        payload = payload.get('payload') if isinstance(payload, dict) else None
        code = None if not isinstance(payload, dict) else payload.get('code')
        try:
            code = int(code)
        except (TypeError, ValueError):
            code = None
        return code == -1021

    def _attempt_timestamp_drift_repair(self, request: BinanceOrderRequest) -> dict[str, Any]:
        outcome: dict[str, Any] = {
            'attempted': True,
            'repair_kind': 'timestamp_drift_retry_once',
            'step': 'sync_server_time_then_retry_once',
            'request_client_order_id': request.client_order_id,
            'request_side': str(request.side or '').upper(),
            'retry_signed_request': None,
            'attempt_no': 1,
            'max_attempts': 1,
            'window_seconds': 0,
            'window_started_at': None,
            'repair_once': True,
            'retry_submitted': False,
            'retry_client_order_id': None,
            'stop_reason': 'submit_auto_repair_pending',
            'stop_condition': 'await_repair_retry',
        }
        sync_fn = getattr(self.submit_client, 'sync_server_time_offset', None)
        if not callable(sync_fn):
            outcome['blocked_reason'] = 'submit_client_missing_server_time_sync'
            outcome['stop_reason'] = 'submit_client_missing_server_time_sync'
            outcome['stop_condition'] = 'submit_client_missing_server_time_sync'
            setattr(self, '_last_timestamp_repair_context', outcome)
            return outcome
        refresh_fn = getattr(self.submit_client, 'refresh_request_timestamp', None)
        if not callable(refresh_fn):
            outcome['blocked_reason'] = 'submit_client_missing_request_timestamp_refresh'
            outcome['stop_reason'] = 'submit_client_missing_request_timestamp_refresh'
            outcome['stop_condition'] = 'submit_client_missing_request_timestamp_refresh'
            setattr(self, '_last_timestamp_repair_context', outcome)
            return outcome
        sync_fn()
        outcome['server_time_sync_attempted'] = True
        outcome['retry_signed_request'] = refresh_fn(request)
        setattr(self, '_last_timestamp_repair_context', outcome)
        return outcome

    def _attempt_reduce_only_conflict_repair(self, request: BinanceOrderRequest) -> dict[str, Any]:
        open_orders = list(self.readonly_client.get_open_orders(symbol=request.symbol) or [])
        position_snapshot = self.readonly_client.get_position_snapshot(symbol=request.symbol)
        position_side = str(getattr(position_snapshot, 'side', None) or '').lower() or None
        position_qty = float(getattr(position_snapshot, 'qty', 0.0) or 0.0)
        expected_close_side = str(request.side or '').upper()
        conflicting_orders = [
            order for order in open_orders
            if bool(getattr(order, 'reduce_only', False))
            and str(getattr(order, 'side', '') or '').upper() == expected_close_side
            and str(getattr(order, 'status', '') or '').upper() in {'NEW', 'PARTIALLY_FILLED'}
        ]
        outcome: dict[str, Any] = {
            'attempted': True,
            'repair_kind': 'reduce_only_conflict_once',
            'request_client_order_id': request.client_order_id,
            'request_side': expected_close_side,
            'position_side': position_side,
            'position_qty': position_qty,
            'open_order_count': len(open_orders),
            'conflicting_order_ids': [str(getattr(order, 'order_id', None) or '') for order in conflicting_orders if getattr(order, 'order_id', None) is not None],
            'conflicting_client_order_ids': [str(getattr(order, 'client_order_id', None) or '') for order in conflicting_orders if getattr(order, 'client_order_id', None)],
            'canceled_receipts': [],
            'retry_signed_request': None,
            'attempt_no': 1,
            'max_attempts': 1,
            'window_seconds': 0,
            'window_started_at': None,
            'repair_once': True,
            'retry_submitted': False,
            'retry_client_order_id': None,
            'stop_reason': 'submit_auto_repair_pending',
            'stop_condition': 'await_repair_retry',
        }
        if position_side not in {'long', 'short'} or position_qty <= 0 or not conflicting_orders:
            outcome['blocked_reason'] = 'repair_precheck_not_satisfied'
            outcome['stop_reason'] = 'repair_precheck_not_satisfied'
            outcome['stop_condition'] = 'repair_precheck_not_satisfied'
            setattr(self, '_last_reduce_only_repair_context', outcome)
            return outcome
        cancel_requests = [
            self.submit_client.build_cancel_request(
                symbol=request.symbol,
                order_id=getattr(order, 'order_id', None),
                client_order_id=getattr(order, 'client_order_id', None),
                metadata={'reduce_only_conflict_repair': True},
            )
            for order in conflicting_orders
        ]
        for cancel_request in cancel_requests:
            _, cancel_receipt = self.submit_client.cancel_order(cancel_request)
            outcome['canceled_receipts'].append(self._serialize_cancel_receipt(cancel_receipt))
        retry_payload = self._serialize_submit_payload(request)
        outcome['retry_signed_request'] = self.submit_client.build_submit_request(retry_payload, metadata=dict(request.metadata or {}))
        setattr(self, '_last_reduce_only_repair_context', outcome)
        return outcome

    @staticmethod
    def _build_submit_exception_metadata(
        *,
        category: str,
        submit_gate: dict[str, Any],
        exception_policy: dict[str, Any],
        exception_policy_view: dict[str, Any],
        exception_helper_plan: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            'submit_exception_category': category,
            'submit_gate': submit_gate,
            'exception_policy': exception_policy,
            'exception_policy_view': exception_policy_view,
            'exception_helper_plan': exception_helper_plan,
        }

    def _build_execution_result_from_confirmation(
        self,
        *,
        market: MarketSnapshot,
        plan: FinalActionPlan,
        confirmation: PostTradeConfirmation,
        status: str,
        execution_phase: str,
        error_code: str | None,
        error_message: str | None,
        state: LiveStateSnapshot | None = None,
    ) -> ExecutionResult:
        position_confirmation_level = 'NONE'
        trade_confirmation_level = 'NONE'
        needs_trade_reconciliation = False
        fills_reconciled = False
        if confirmation.confirmation_status == 'CONFIRMED':
            position_confirmation_level = 'TRADES_CONFIRMED'
            trade_confirmation_level = 'TRADES_CONFIRMED'
            fills_reconciled = True
        elif confirmation.confirmation_status == 'POSITION_CONFIRMED':
            position_confirmation_level = 'POSITION_CONFIRMED'
            trade_confirmation_level = 'PENDING'
            needs_trade_reconciliation = True

        protective_phase_deferred = bool((confirmation.trade_summary or {}).get('protective_phase_deferred'))
        protective_orders = list(((confirmation.trade_summary or {}).get('protective_orders') or []))
        protective_validation = ((confirmation.trade_summary or {}).get('protective_validation') or {})
        protective_order_requested = bool((confirmation.trade_summary or {}).get('protective_order_requested'))
        protective_pending_confirm = bool((confirmation.trade_summary or {}).get('protective_pending_confirm'))
        pretrade_recover = (confirmation.trade_summary or {}).get('protective_recover') or {}
        pretrade_recover_updates = dict(pretrade_recover.get('state_updates') or {})
        pretrade_recover_result = str(pretrade_recover.get('result') or '')
        pretrade_positive_protective_fact = pretrade_recover_result in {'VALID_ON_EXCHANGE', 'STATE_REBUILT'}
        if pretrade_positive_protective_fact and not protective_orders:
            protective_orders = list(pretrade_recover_updates.get('exchange_protective_orders') or [])
        protective_visibility = dict(protective_validation.get('exchange_visibility') or {})
        if pretrade_positive_protective_fact:
            protective_validation = {
                **protective_validation,
                'ok': True,
                'freeze_reason': None,
                'status': str(protective_validation.get('status') or 'OK'),
                'validation_level': str(protective_validation.get('validation_level') or 'EXCHANGE_VISIBLE'),
                'exchange_visibility': {
                    **protective_visibility,
                    'exchange_visible': True,
                    'confirmed_via_exchange_visibility': True,
                },
            }
        protective_status = 'NONE'
        if confirmation.post_position_side in {'long', 'short'} and confirmation.post_position_qty > 0:
            if protective_phase_deferred:
                protective_status = 'PENDING_SUBMIT'
            elif protective_orders and protective_validation.get('ok', True):
                protective_status = 'ACTIVE'
            elif protective_order_requested and protective_pending_confirm:
                protective_status = 'PENDING_CONFIRM'
            elif pretrade_positive_protective_fact:
                protective_status = 'ACTIVE'
            elif protective_orders and protective_validation.get('ok', True):
                protective_status = 'ACTIVE'
            elif plan.action_type == 'protective_rebuild' and confirmation.confirmation_status in {'CONFIRMED', 'POSITION_CONFIRMED'}:
                protective_status = 'ACTIVE'
            else:
                protective_status = 'MISSING'
        elif protective_orders:
            protective_status = 'UNEXPECTED_WHILE_FLAT'

        state_updates = {}
        if protective_order_requested and protective_pending_confirm and not protective_orders:
            bridge_orders = list((pretrade_recover_updates.get('exchange_protective_orders') or []))
            if not bridge_orders and state is not None:
                bridge_orders = list((state.exchange_protective_orders or []))
            if bridge_orders:
                protective_orders = bridge_orders
        if pretrade_recover_updates:
            state_updates.update(pretrade_recover_updates)
        protective_phase_status = (
            'DEFERRED'
            if protective_phase_deferred
            else ('PENDING_CONFIRM' if protective_status == 'PENDING_CONFIRM' else ('ACTIVE' if protective_status == 'ACTIVE' else ('FROZEN' if confirmation.should_freeze else 'NONE')))
        )
        intent_pending_execution_phase = None if pretrade_positive_protective_fact else execution_phase
        strategy_protection_intent = build_strategy_protection_intent(
            runtime_mode='FROZEN' if confirmation.should_freeze else 'ACTIVE',
            position_side=confirmation.post_position_side,
            position_qty=confirmation.post_position_qty,
            active_strategy=plan.target_strategy,
            stop_price=plan.stop_price,
            tp_price=(plan.conflict_context or {}).get('tp_price'),
            pending_execution_phase=intent_pending_execution_phase,
            pending_execution_block_reason=protective_validation.get('freeze_reason'),
            protective_order_status=protective_status,
            protective_phase_status=protective_phase_status,
            protective_orders=protective_orders,
            protective_validation=protective_validation,
            confirmation_category=confirmation.confirmation_category,
            freeze_reason=confirmation.freeze_reason,
            last_eval_ts=market.decision_ts,
        )

        state_updates.update({
            'pending_execution_phase': intent_pending_execution_phase if pretrade_positive_protective_fact else execution_phase,
            'position_confirmation_level': position_confirmation_level,
            'trade_confirmation_level': trade_confirmation_level,
            'needs_trade_reconciliation': needs_trade_reconciliation,
            'fills_reconciled': fills_reconciled,
            'last_confirmed_order_ids': confirmation.exchange_order_ids,
            'protective_order_status': protective_status,
            'exchange_protective_orders': protective_orders,
            'protective_order_last_sync_ts': market.decision_ts,
            'protective_order_last_sync_action': plan.action_type,
            'protective_order_freeze_reason': protective_validation.get('freeze_reason'),
            'protective_phase_status': protective_phase_status,
            'strategy_protection_intent': strategy_protection_intent,
        })
        confirmed_state_updates = self._build_confirmed_state_updates(
            plan=plan,
            market=market,
            confirmation=confirmation,
            state=state,
        )
        state_updates.update(confirmed_state_updates)
        risk_action_view = classify_reconcile_risk(
            freeze_reason=confirmation.freeze_reason,
            pending_execution_phase=execution_phase,
            notes=list(confirmation.notes or []),
        )
        submit_exception_policy = (confirmation.trade_summary or {}).get('submit_exception_policy') or {}
        if submit_exception_policy.get('action') == 'readonly_recheck':
            risk_action_view = {
                **risk_action_view,
                'risk_action': 'OBSERVE',
                'recover_policy': 'observe_only',
                'recover_stage': risk_action_view.get('recover_stage') or 'readonly_query_failed',
                'stop_condition': risk_action_view.get('stop_condition') or 'await_more_exchange_facts',
            }
        trade_summary = {
            **(confirmation.trade_summary or {}),
            'confirmation_category': confirmation.confirmation_category,
            'submit_exception_policy': (confirmation.trade_summary or {}).get('submit_exception_policy'),
            'submit_exception_metadata': (confirmation.trade_summary or {}).get('submit_exception_metadata'),
            'exception_policy_view': (confirmation.trade_summary or {}).get('exception_policy_view'),
            'exception_helper_plan': (confirmation.trade_summary or {}).get('exception_helper_plan'),
            'risk_action': risk_action_view.get('risk_action'),
            'recover_policy': risk_action_view.get('recover_policy'),
            'recover_stage': risk_action_view.get('recover_stage'),
            'stop_condition': risk_action_view.get('stop_condition'),
            'protective_validation': {
                **dict((confirmation.trade_summary or {}).get('protective_validation') or {}),
                **protective_validation,
            },
            'protective_orders': protective_orders,
        }
        result = ExecutionResult(
            result_ts=market.decision_ts,
            bar_ts=market.bar_ts,
            status=status,
            action_type=plan.action_type,
            executed_side=plan.target_side,
            executed_qty=confirmation.executed_qty,
            avg_fill_price=confirmation.avg_fill_price,
            fees=confirmation.fees,
            exchange_order_ids=confirmation.exchange_order_ids,
            post_position_side=confirmation.post_position_side,
            post_position_qty=confirmation.post_position_qty,
            post_entry_price=confirmation.post_entry_price,
            reconcile_status=confirmation.reconcile_status,
            error_code=error_code,
            error_message=error_message,
            should_freeze=confirmation.should_freeze,
            freeze_reason=confirmation.freeze_reason,
            state_updates=state_updates,
            execution_phase=execution_phase,
            confirmation_status=confirmation.confirmation_status,
            confirmed_order_status=confirmation.order_status,
            trade_summary=trade_summary,
        )
        _, result_payload, _ = attach_execution_confirm_async_operation(
            market_decision_ts=market.decision_ts,
            symbol=market.symbol,
            strategy_ts=market.strategy_ts,
            state_payload=state_updates,
            result_payload=asdict(result),
        )
        _, result_payload, _ = attach_submit_auto_repair_async_operation(
            market_decision_ts=market.decision_ts,
            symbol=market.symbol,
            strategy_ts=market.strategy_ts,
            state_payload=state_updates,
            result_payload=result_payload,
        )
        _, result_payload, _ = attach_protection_followup_async_operation(
            market_decision_ts=market.decision_ts,
            symbol=market.symbol,
            strategy_ts=market.strategy_ts,
            state_payload=state_updates,
            result_payload=result_payload,
        )
        result.state_updates = result_payload.get('state_updates')
        result.trade_summary = result_payload.get('trade_summary')
        return result

    def _build_flat_terminal_state_updates(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        confirmation: PostTradeConfirmation,
    ) -> dict[str, Any]:
        close_cancel_ok = bool(((confirmation.trade_summary or {}).get('protective_cancel_summary') or {}).get('ok'))
        flat_confirmed = confirmation.post_position_side not in {'long', 'short'} and float(confirmation.post_position_qty or 0.0) <= 0.0
        should_force_terminal_cleanup = plan.action_type == 'close' and flat_confirmed and close_cancel_ok

        updates: dict[str, Any] = {
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
            'protective_order_status': 'NONE',
            'exchange_protective_orders': [],
            'protective_order_freeze_reason': None,
        }
        if should_force_terminal_cleanup:
            updates.update({
                'protective_phase_status': 'NONE',
                'strategy_protection_intent': build_strategy_protection_intent(
                    runtime_mode='ACTIVE',
                    position_side=None,
                    position_qty=0.0,
                    active_strategy='none',
                    stop_price=None,
                    tp_price=None,
                    pending_execution_phase=None,
                    pending_execution_block_reason=None,
                    protective_order_status='NONE',
                    protective_phase_status='NONE',
                    protective_orders=[],
                    protective_validation={'ok': True, 'freeze_reason': None},
                    confirmation_category=confirmation.confirmation_category,
                    freeze_reason=None,
                    last_eval_ts=market.decision_ts,
                ),
            })
        return updates

    def _build_confirmed_state_updates(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        confirmation: PostTradeConfirmation,
        state: LiveStateSnapshot | None = None,
    ) -> dict[str, Any]:
        if confirmation.confirmation_status not in {'CONFIRMED', 'POSITION_CONFIRMED'}:
            return {}

        post_side = confirmation.post_position_side
        post_qty = float(confirmation.post_position_qty or 0.0)
        post_entry_price = confirmation.post_entry_price
        avg_fill_price = confirmation.avg_fill_price
        position_open = post_side in {'long', 'short'} and post_qty > 0.0

        updates: dict[str, Any] = {
            'exchange_position_side': post_side if position_open else None,
            'exchange_position_qty': post_qty if position_open else 0.0,
            'exchange_entry_price': (post_entry_price if position_open else None),
            'freeze_reason': None,
        }

        if not position_open:
            updates.update(
                self._build_flat_terminal_state_updates(
                    plan=plan,
                    market=market,
                    confirmation=confirmation,
                )
            )
            return updates

        if plan.action_type in {'open', 'flip'}:
            updates.update(self._build_confirmed_open_like_state_updates(plan=plan, market=market, confirmation=confirmation))
            return updates

        if plan.action_type == 'add':
            if state is None:
                raise ValueError('state is required to build confirmed add state updates')
            updates.update(
                self._build_confirmed_add_state_updates(
                    plan=plan,
                    market=market,
                    confirmation=confirmation,
                    state=state,
                )
            )
            return updates

        if plan.action_type == 'trim':
            updates.update({
                'active_side': post_side,
                'strategy_entry_price': post_entry_price if post_entry_price is not None else avg_fill_price,
                'base_quantity': post_qty,
            })
            return updates

        return updates

    def _build_confirmed_open_like_state_updates(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        confirmation: PostTradeConfirmation,
    ) -> dict[str, Any]:
        entry_price = confirmation.post_entry_price
        if entry_price is None:
            entry_price = confirmation.avg_fill_price if confirmation.avg_fill_price is not None else plan.price_hint
        stop_price = plan.stop_price
        risk_per_unit = None
        risk_amount = None
        if entry_price is not None and stop_price is not None:
            risk_per_unit = abs(float(entry_price) - float(stop_price))
            if risk_per_unit <= 0:
                risk_per_unit = None
        if risk_per_unit is not None and confirmation.post_position_qty and confirmation.post_position_qty > 0:
            risk_amount = float(confirmation.post_position_qty) * risk_per_unit

        updates: dict[str, Any] = {
            'active_strategy': plan.target_strategy or 'none',
            'active_side': confirmation.post_position_side or plan.target_side,
            'strategy_entry_time': market.bar_ts,
            'strategy_entry_price': entry_price,
            'stop_price': stop_price,
            'risk_fraction': plan.risk_fraction,
            'last_signal_bar': market.bar_ts,
            'base_quantity': float(confirmation.post_position_qty or 0.0),
            'equity_at_entry': None,
            'risk_amount': risk_amount,
            'risk_per_unit': risk_per_unit,
            'p1_armed': False,
            'p2_armed': False,
            'high_water_r': 0.0,
        }
        if plan.target_strategy == 'trend':
            quality_bucket = 'HIGH' if (plan.risk_fraction or 0.0) >= 0.16 else 'MEDIUM'
            signal_ts = (plan.conflict_context or {}).get('signal_ts')
            updates.update({
                'tp_price': None,
                'hold_bars': 0,
                'rev_window': None,
                'add_on_count': 0,
                'degrade_state': 'ATTACK',
                'quality_bucket': quality_bucket,
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
            })
        return updates

    def _build_confirmed_add_state_updates(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        confirmation: PostTradeConfirmation,
        state: LiveStateSnapshot,
    ) -> dict[str, Any]:
        updates: dict[str, Any] = {
            'active_side': confirmation.post_position_side or plan.target_side,
            'base_quantity': float(confirmation.post_position_qty or 0.0),
            'strategy_entry_price': confirmation.post_entry_price if confirmation.post_entry_price is not None else confirmation.avg_fill_price,
            'last_signal_bar': market.bar_ts,
            'add_on_count': int(state.add_on_count or 0) + 1,
        }
        if plan.stop_price is not None:
            updates['stop_price'] = plan.stop_price
        return {k: v for k, v in updates.items() if v is not None}

    def _apply_freeze_if_needed(self, state: LiveStateSnapshot, result: ExecutionResult) -> ExecutionResult:
        decision = self.freeze_controller.freeze_from_result(state, result)
        if not decision.should_freeze:
            return result
        merged_updates = dict(result.state_updates or {})
        merged_updates.update(decision.state_updates)
        return ExecutionResult(
            result_ts=result.result_ts,
            bar_ts=result.bar_ts,
            status='FROZEN' if result.status not in {'FROZEN'} else result.status,
            action_type=result.action_type,
            executed_side=result.executed_side,
            executed_qty=result.executed_qty,
            avg_fill_price=result.avg_fill_price,
            fees=result.fees,
            exchange_order_ids=result.exchange_order_ids,
            post_position_side=result.post_position_side,
            post_position_qty=result.post_position_qty,
            post_entry_price=result.post_entry_price,
            reconcile_status=result.reconcile_status,
            error_code=result.error_code,
            error_message=result.error_message,
            should_freeze=True,
            freeze_reason=decision.freeze_reason,
            state_updates=merged_updates,
            execution_phase='frozen',
            confirmation_status=result.confirmation_status,
            confirmed_order_status=result.confirmed_order_status,
            trade_summary=result.trade_summary,
        )

    def _map_execution_phase(self, confirmation: PostTradeConfirmation) -> str | None:
        if confirmation.should_freeze:
            return 'frozen'
        trade_summary = confirmation.trade_summary or {}
        protective_phase_deferred = bool(trade_summary.get('protective_phase_deferred'))
        protective_order_requested = bool(trade_summary.get('protective_order_requested'))
        protective_pending_confirm = bool(trade_summary.get('protective_pending_confirm'))
        if confirmation.confirmation_status == 'CONFIRMED':
            if protective_phase_deferred:
                return 'entry_confirmed_pending_protective'
            if protective_order_requested and protective_pending_confirm:
                return 'protection_pending_confirm'
            return 'confirmed'
        if confirmation.confirmation_status == 'POSITION_CONFIRMED':
            if confirmation.order_status == 'PARTIALLY_FILLED':
                return 'position_working_partial_fill'
            if protective_phase_deferred:
                return 'entry_confirmed_pending_protective'
            if protective_order_requested and protective_pending_confirm:
                return 'protection_pending_confirm'
            flat_after_reduce_only_close = (
                confirmation.post_position_side is None
                and float(getattr(confirmation, 'post_position_qty', 0.0) or 0.0) <= 0.0
                and str(getattr(confirmation, 'order_status', '') or '').upper() == 'FILLED'
                and bool(trade_summary.get('requested_reduce_only'))
                and not bool(trade_summary.get('has_open_orders'))
                and int(trade_summary.get('open_orders_count') or 0) == 0
                and not list(trade_summary.get('protective_orders') or [])
            )
            if flat_after_reduce_only_close:
                return None
            return 'position_confirmed_pending_trades'
        if confirmation.confirmation_status == 'PENDING':
            if protective_order_requested:
                return 'protection_pending_confirm'
            return 'submitted'
        return 'submitted'

    def _build_order_requests(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
    ) -> list[BinanceOrderRequest]:
        if plan.action_type == 'flip':
            return self._build_flip_requests(plan, market, state, symbol_rules)

        if plan.action_type == 'protective_rebuild':
            return self._build_protective_rebuild_requests(plan, market, state, symbol_rules)

        primary_request = self._build_single_order_request(plan, market, state, symbol_rules)
        if plan.action_type in {'add', 'trim'}:
            return [primary_request, *self._build_protective_order_requests(plan, market, state, symbol_rules, primary_request)]
        return [primary_request]

    def _build_flip_requests(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
    ) -> list[BinanceOrderRequest]:
        requests: list[BinanceOrderRequest] = []
        if state.exchange_position_side and state.exchange_position_qty > 0:
            close_side = 'SELL' if state.exchange_position_side == 'long' else 'BUY'
            qty = self._normalize_quantity(state.exchange_position_qty, symbol_rules)
            requests.append(
                BinanceOrderRequest(
                    symbol=market.symbol,
                    side=close_side,
                    order_type='MARKET',
                    quantity=qty,
                    reduce_only=True,
                    position_side=None,
                    client_order_id=self._build_client_order_id(market.bar_ts, 'flip-close'),
                    metadata={'phase': 'close_existing', 'close_reason': plan.close_reason},
                )
            )
        open_plan = replace(plan, action_type='open')
        open_request = self._build_single_order_request(open_plan, market, state, symbol_rules, client_order_suffix='flip-open')
        requests.append(open_request)
        return requests

    def _build_single_order_request(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
        *,
        client_order_suffix: str | None = None,
    ) -> BinanceOrderRequest:
        action_type = plan.action_type
        if action_type not in {'open', 'close', 'add', 'trim', 'flip'}:
            raise ValueError(f'unsupported executable action_type: {action_type}')

        if plan.target_side not in {'long', 'short'} and action_type != 'close':
            raise ValueError(f'unsupported target_side: {plan.target_side}')

        reduce_only = action_type in {'close', 'trim'}
        if action_type == 'flip':
            reduce_only = False

        side = self._resolve_order_side(action_type, plan, state)
        raw_quantity = self._resolve_order_quantity(action_type, plan, state, market, symbol_rules)
        quantity = self._normalize_quantity(raw_quantity, symbol_rules)
        validation_context = self._build_quantity_validation_context(
            plan=plan,
            state=state,
            market=market,
            symbol_rules=symbol_rules,
            action_type=action_type,
            side=side,
            raw_quantity=raw_quantity,
            normalized_quantity=quantity,
        )
        self._ensure_quantity_ready(
            action_type=action_type,
            quantity=quantity,
            validation_context=validation_context,
        )
        if quantity is not None:
            self._validate_quantity(quantity, symbol_rules, market)

        suffix = client_order_suffix or action_type
        return BinanceOrderRequest(
            symbol=market.symbol,
            side=side,
            order_type='MARKET',
            quantity=quantity,
            reduce_only=reduce_only,
            position_side=None,
            client_order_id=self._build_client_order_id(market.bar_ts, suffix),
            metadata={
                'reason': plan.reason,
                'qty_mode': plan.qty_mode,
                'price_hint': plan.price_hint,
                'stop_price': plan.stop_price,
                'risk_fraction': plan.risk_fraction,
                'strategy': plan.target_strategy,
                'source_action': action_type,
                'protective_stop_price': plan.stop_price,
                'protective_tp_price': (plan.conflict_context or {}).get('tp_price'),
                'quantity_validation': validation_context,
            },
        )

    def _build_protective_order_requests(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
        primary_request: BinanceOrderRequest,
    ) -> list[BinanceOrderRequest]:
        protective_quantity = self._resolve_protective_recreate_quantity(plan, state, primary_request)
        if protective_quantity is None or protective_quantity <= 0:
            return []
        return self._build_protective_requests_from_quantity(
            plan=plan,
            market=market,
            symbol_rules=symbol_rules,
            quantity=float(protective_quantity),
            position_side=(plan.target_side or state.exchange_position_side),
        )

    def _build_protective_rebuild_requests(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
    ) -> list[BinanceOrderRequest]:
        if state.exchange_position_side not in {'long', 'short'}:
            return []
        quantity = self._normalize_quantity(float(state.exchange_position_qty or 0.0), symbol_rules)
        if quantity is None or quantity <= 0:
            return []
        return self._build_protective_requests_from_quantity(
            plan=plan,
            market=market,
            symbol_rules=symbol_rules,
            quantity=float(quantity),
            position_side=state.exchange_position_side,
        )

    def _build_protective_requests_from_quantity(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        symbol_rules: ExchangeSymbolRules,
        quantity: float,
        position_side: str | None,
    ) -> list[BinanceOrderRequest]:
        intents = build_protective_order_intents(
            strategy=plan.target_strategy,
            position_side=position_side,
            quantity=float(quantity),
            stop_price=self._normalize_price(plan.stop_price, symbol_rules),
            tp_price=self._normalize_price((plan.conflict_context or {}).get('tp_price'), symbol_rules),
        )
        requests: list[BinanceOrderRequest] = []
        for intent in intents:
            normalized_qty = self._normalize_quantity(intent.quantity, symbol_rules)
            if normalized_qty is None or normalized_qty <= 0:
                continue
            requests.append(
                BinanceOrderRequest(
                    symbol=market.symbol,
                    side=intent.side,
                    order_type=intent.order_type,
                    quantity=(None if intent.close_position else normalized_qty),
                    reduce_only=intent.reduce_only,
                    position_side=None,
                    client_order_id=self._build_client_order_id(market.bar_ts, f'protect-{intent.kind}'),
                    stop_price=float(intent.trigger_price),
                    close_position=intent.close_position,
                    working_type='MARK_PRICE',
                    price_protect=False,
                    metadata={
                        'phase': 'protective_order',
                        'protective_order': True,
                        'protective_kind': intent.kind,
                        'strategy': plan.target_strategy,
                        'source_action': plan.action_type,
                        'trigger_price': float(intent.trigger_price),
                        'protective_quantity_hint': normalized_qty,
                        'protective_stop_price': plan.stop_price,
                        'protective_tp_price': (plan.conflict_context or {}).get('tp_price'),
                    },
                )
            )
        return requests

    def _resolve_protective_recreate_quantity(
        self,
        plan: FinalActionPlan,
        state: LiveStateSnapshot,
        primary_request: BinanceOrderRequest,
    ) -> float | None:
        if primary_request.quantity is None or primary_request.quantity <= 0:
            return None
        if plan.action_type in {'open', 'flip', 'protective_rebuild'}:
            return float(primary_request.quantity)
        current_qty = float(state.exchange_position_qty or 0.0)
        if plan.action_type == 'add':
            return current_qty + float(primary_request.quantity)
        if plan.action_type == 'trim':
            remaining_qty = current_qty - float(primary_request.quantity)
            if remaining_qty <= 0:
                return None
            return remaining_qty
        return None

    def _resolve_order_side(self, action_type: str, plan: FinalActionPlan, state: LiveStateSnapshot) -> str:
        if action_type in {'open', 'add', 'flip'}:
            return 'BUY' if plan.target_side == 'long' else 'SELL'
        if action_type in {'close', 'trim'}:
            close_side = state.exchange_position_side or state.active_side
            if close_side == 'long':
                return 'SELL'
            if close_side == 'short':
                return 'BUY'
            raise ValueError('close/trim requires an existing position side')
        raise ValueError(f'unsupported action_type for side resolution: {action_type}')

    def _resolve_order_quantity(
        self,
        action_type: str,
        plan: FinalActionPlan,
        state: LiveStateSnapshot,
        market: MarketSnapshot,
        symbol_rules: ExchangeSymbolRules,
    ) -> float | None:
        if action_type in {'close', 'flip'}:
            return state.exchange_position_qty or state.base_quantity
        if action_type == 'trim':
            if state.exchange_position_qty > 0 and plan.qty is not None:
                return state.exchange_position_qty * float(plan.qty)
            if state.base_quantity is not None and plan.qty is not None:
                return float(state.base_quantity) * float(plan.qty)
            return None
        if plan.qty_mode in {'risk_based', 'risk_based_add'}:
            return self._resolve_risk_based_quantity(plan=plan, state=state, market=market, symbol_rules=symbol_rules)
        if plan.qty is not None:
            return float(plan.qty)
        return state.base_quantity

    def _resolve_risk_based_quantity(
        self,
        *,
        plan: FinalActionPlan,
        state: LiveStateSnapshot,
        market: MarketSnapshot,
        symbol_rules: ExchangeSymbolRules,
    ) -> float | None:
        account_equity = float(state.account_equity or 0.0)
        available_margin = float(state.available_margin or 0.0)
        risk_fraction = float(plan.risk_fraction or 0.0)
        entry_price = float(plan_price_or_market_price(plan.price_hint, market))
        if account_equity <= 0:
            raise ValueError('risk_based_quantity_blocked: readonly_account_equity_non_positive')
        if available_margin <= 0:
            raise ValueError('risk_based_quantity_blocked: readonly_available_margin_non_positive')
        if risk_fraction <= 0 or entry_price <= 0:
            return None
        if plan.stop_price is None:
            return None
        stop_price = float(plan.stop_price)
        risk_per_unit = abs(entry_price - stop_price)
        if risk_per_unit <= 0:
            return None
        risk_budget = account_equity * risk_fraction
        raw_quantity = risk_budget / risk_per_unit
        if raw_quantity <= 0:
            return None

        max_notional: float | None = None
        if symbol_rules.min_notional is not None:
            max_notional = None
        leverage_like_cap = None
        if available_margin > 0:
            leverage_like_cap = available_margin * 20.0
        elif account_equity > 0:
            leverage_like_cap = account_equity * 20.0
        if leverage_like_cap is not None and leverage_like_cap > 0:
            max_notional = leverage_like_cap if max_notional is None else min(max_notional, leverage_like_cap)

        if plan.qty_mode == 'risk_based_add' and state.active_strategy == 'trend' and state.active_side and state.strategy_entry_price is not None and state.stop_price is not None and state.base_quantity is not None:
            existing_notional = float(state.base_quantity) * float(state.strategy_entry_price)
            if max_notional is not None:
                remaining_notional = max(max_notional - existing_notional, 0.0)
                raw_quantity = min(raw_quantity, remaining_notional / entry_price)
        elif max_notional is not None:
            raw_quantity = min(raw_quantity, max_notional / entry_price)

        if raw_quantity <= 0:
            return None
        return float(raw_quantity)

    def _build_quantity_validation_context(
        self,
        *,
        plan: FinalActionPlan,
        state: LiveStateSnapshot,
        market: MarketSnapshot,
        symbol_rules: ExchangeSymbolRules,
        action_type: str,
        side: str,
        raw_quantity: float | None,
        normalized_quantity: float | None,
    ) -> dict[str, Any]:
        return {
            'action_type': action_type,
            'side': side,
            'qty_mode': plan.qty_mode,
            'plan_qty': plan.qty,
            'state_base_quantity': state.base_quantity,
            'state_exchange_position_qty': state.exchange_position_qty,
            'risk_fraction': plan.risk_fraction,
            'account_equity': state.account_equity,
            'available_margin': state.available_margin,
            'price_hint': plan.price_hint,
            'market_price': market.current_price,
            'stop_price': plan.stop_price,
            'raw_quantity': raw_quantity,
            'normalized_quantity': normalized_quantity,
            'min_qty': symbol_rules.min_qty,
            'min_notional': symbol_rules.min_notional,
            'qty_step': symbol_rules.qty_step,
        }

    def _ensure_quantity_ready(
        self,
        *,
        action_type: str,
        quantity: float | None,
        validation_context: dict[str, Any],
    ) -> None:
        if action_type not in REQUIRES_NON_EMPTY_QUANTITY_ACTIONS:
            return
        if quantity is not None and quantity > 0:
            return
        detail = ', '.join(f'{key}={validation_context.get(key)!r}' for key in (
            'action_type',
            'side',
            'qty_mode',
            'plan_qty',
            'state_base_quantity',
            'state_exchange_position_qty',
            'risk_fraction',
            'account_equity',
            'available_margin',
            'price_hint',
            'market_price',
            'stop_price',
            'raw_quantity',
            'normalized_quantity',
            'min_qty',
            'min_notional',
            'qty_step',
        ))
        raise ValueError(f'quantity missing_or_non_positive before submit: {detail}')

    def _normalize_quantity(self, quantity: float | None, symbol_rules: ExchangeSymbolRules) -> float | None:
        if quantity is None:
            return None
        if symbol_rules.qty_step is None or symbol_rules.qty_step <= 0:
            return float(quantity)
        step = Decimal(str(symbol_rules.qty_step))
        normalized = (Decimal(str(quantity)) / step).quantize(Decimal('1'), rounding=ROUND_DOWN) * step
        return float(normalized)

    def _normalize_price(self, price: float | None, symbol_rules: ExchangeSymbolRules) -> float | None:
        if price is None:
            return None
        if symbol_rules.price_tick is None or symbol_rules.price_tick <= 0:
            return float(price)
        tick = Decimal(str(symbol_rules.price_tick))
        normalized = (Decimal(str(price)) / tick).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick
        return float(normalized)

    def _validate_quantity(self, quantity: float, symbol_rules: ExchangeSymbolRules, market: MarketSnapshot) -> None:
        if symbol_rules.min_qty is not None and quantity < symbol_rules.min_qty:
            raise ValueError(f'quantity below min_qty: {quantity} < {symbol_rules.min_qty}')
        if symbol_rules.min_notional is not None:
            mark_price = float(plan_price_or_market_price(None, market))
            if quantity * mark_price < symbol_rules.min_notional:
                raise ValueError(
                    f'notional below min_notional: {quantity * mark_price} < {symbol_rules.min_notional}'
                )

    def _build_execution_ref(self, *, plan: FinalActionPlan, market: MarketSnapshot) -> dict[str, Any]:
        return {
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'bar_ts': market.bar_ts,
            'plan_ts': plan.plan_ts,
            'action_type': plan.action_type,
            'target_side': plan.target_side,
        }

    def _build_submit_gate_context(self, *, blocked_reason: str | None = None) -> dict[str, Any]:
        context = self.submit_client.gate_context()
        context['submit_allowed'] = bool(context.get('http_post_allowed'))
        context['blocked_reason'] = blocked_reason
        return context

    def _evaluate_submit_gate(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        order_requests: list[BinanceOrderRequest],
        blocked_reason: str | None = None,
        allow_frozen_emergency_close: bool = False,
    ) -> dict[str, Any]:
        gate = self._build_submit_gate_context(blocked_reason=blocked_reason)
        guardrail_blockers: list[str] = []

        allowlist = tuple(item.upper() for item in self.config.submit_symbol_allowlist)
        effective_symbol_allowlist = allowlist or (self.config.symbol.upper(),)
        if market.symbol.upper() != self.config.symbol.upper():
            guardrail_blockers.append(f'config_symbol_mismatch:{market.symbol}!={self.config.symbol}')
        if market.symbol.upper() not in effective_symbol_allowlist:
            guardrail_blockers.append(f'symbol_not_allowed:{market.symbol}')

        total_qty = sum(float(item.quantity or 0.0) for item in order_requests)
        total_notional = sum(float(item.quantity or 0.0) * float(market.current_price or 0.0) for item in order_requests)

        if self.config.submit_max_qty is not None and total_qty > float(self.config.submit_max_qty):
            guardrail_blockers.append(f'max_qty_exceeded:{total_qty}>{self.config.submit_max_qty}')
        if self.config.submit_max_notional is not None and total_notional > float(self.config.submit_max_notional):
            guardrail_blockers.append(f'max_notional_exceeded:{total_notional}>{self.config.submit_max_notional}')
        if self.config.submit_require_reconcile_ok and state.consistency_status != 'OK':
            guardrail_blockers.append(f'consistency_not_ok:{state.consistency_status}')
        if self.config.submit_require_active_runtime and state.runtime_mode != 'ACTIVE' and not allow_frozen_emergency_close:
            guardrail_blockers.append(f'runtime_not_active:{state.runtime_mode}')
        pending_phase = state.pending_execution_phase
        protective_followup_pending_phases = {
            'entry_confirmed_pending_protective',
            'management_stop_update_pending_protective',
        }
        protective_followup_phase = pending_phase in protective_followup_pending_phases and any(
            bool((item.metadata or {}).get('protective_order')) for item in order_requests
        )
        reduce_only_close_followup_phase = (
            pending_phase == 'position_confirmed_pending_trades'
            and all(bool(getattr(item, 'reduce_only', False)) for item in order_requests)
            and any(str(getattr(item, 'order_type', '') or '').upper() == 'MARKET' for item in order_requests)
        )
        if (
            self.config.submit_require_no_pending_execution
            and pending_phase not in {None, 'none', 'confirmed'}
            and not protective_followup_phase
            and not reduce_only_close_followup_phase
        ):
            guardrail_blockers.append(f'pending_execution_phase:{pending_phase}')
        if not self.config.discord_audit_enabled:
            guardrail_blockers.append('discord_audit_disabled')
        if (self.config.submit_manual_ack_token or '').strip() != LIVE_SUBMIT_MANUAL_ACK_TOKEN:
            guardrail_blockers.append('manual_ack_missing_or_invalid')

        gate['guardrail_checks'] = {
            'symbol_allowlist': list(effective_symbol_allowlist),
            'config_symbol': self.config.symbol.upper(),
            'symbol_allowed': market.symbol.upper() in effective_symbol_allowlist,
            'max_qty': self.config.submit_max_qty,
            'total_qty': total_qty,
            'max_notional': self.config.submit_max_notional,
            'total_notional': total_notional,
            'require_reconcile_ok': self.config.submit_require_reconcile_ok,
            'consistency_status': state.consistency_status,
            'require_active_runtime': self.config.submit_require_active_runtime,
            'runtime_mode': state.runtime_mode,
            'allow_frozen_emergency_close': allow_frozen_emergency_close,
            'require_no_pending_execution': self.config.submit_require_no_pending_execution,
            'pending_execution_phase': state.pending_execution_phase,
            'discord_audit_enabled': self.config.discord_audit_enabled,
            'manual_ack_present': bool((self.config.submit_manual_ack_token or '').strip()),
            'manual_ack_valid': (self.config.submit_manual_ack_token or '').strip() == LIVE_SUBMIT_MANUAL_ACK_TOKEN,
            'unlock_token_present': bool((self.config.submit_unlock_token or '').strip()),
            'unlock_token_valid': (self.config.submit_unlock_token or '').strip() == LIVE_SUBMIT_UNLOCK_TOKEN,
            'http_post_enabled': bool(self.config.submit_http_post_enabled),
        }
        gate['guardrail_blockers'] = guardrail_blockers
        gate['submit_allowed'] = (
            (not self.config.dry_run)
            and bool(self.config.submit_enabled)
            and bool(gate.get('http_post_allowed'))
            and not guardrail_blockers
        )
        if guardrail_blockers:
            gate['blocked_reason'] = guardrail_blockers[0]
        elif not gate.get('http_post_allowed'):
            gate['blocked_reason'] = 'submit_http_post_not_open'
        else:
            gate['blocked_reason'] = None
        return gate

    def _build_request_context(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        order_requests: list[BinanceOrderRequest],
        cancel_requests: list[BinanceCancelOrderRequest],
    ) -> dict[str, Any]:
        primary_order_requests = [item for item in order_requests if not bool((item.metadata or {}).get('protective_order'))]
        request_payloads = [self._serialize_submit_payload(item) for item in order_requests]
        return {
            'plan_action_type': plan.action_type,
            'plan_reason': plan.reason,
            'plan_qty_mode': plan.qty_mode,
            'plan_qty': plan.qty,
            'plan_price_hint': plan.price_hint,
            'plan_stop_price': plan.stop_price,
            'plan_risk_fraction': plan.risk_fraction,
            'market_symbol': market.symbol,
            'market_price_hint': market.current_price,
            'state_position_side': state.exchange_position_side,
            'state_position_qty': state.exchange_position_qty,
            'state_base_quantity': state.base_quantity,
            'state_account_equity': state.account_equity,
            'request_count': len(order_requests),
            'primary_request_count': len(primary_order_requests),
            'protective_cancel_count': len(cancel_requests),
            'protective_cancel_requests': [self._serialize_cancel_request(item) for item in cancel_requests],
            'client_order_ids': [item.client_order_id for item in order_requests],
            'primary_client_order_ids': [item.client_order_id for item in primary_order_requests],
            'request_payloads': request_payloads,
            'request_payload_audit': [
                {
                    'symbol': payload.get('symbol'),
                    'side': payload.get('side'),
                    'type': payload.get('type'),
                    'client_order_id': payload.get('newClientOrderId'),
                    'quantity': payload.get('quantity'),
                    'quantity_missing': payload.get('quantity') is None,
                    'reduce_only': payload.get('reduceOnly') == 'true',
                    'payload': payload,
                }
                for payload in request_payloads
            ],
            'final_quantities': [item.quantity for item in primary_order_requests],
            'protective_final_quantities': [item.quantity for item in order_requests if bool((item.metadata or {}).get('protective_order'))],
            'has_missing_quantity': any(item.quantity is None for item in primary_order_requests),
        }

    @staticmethod
    def _serialize_cancel_request(request: BinanceCancelOrderRequest) -> dict[str, Any]:
        return {
            'symbol': request.symbol,
            'order_id': request.order_id,
            'client_order_id': request.client_order_id,
            'metadata': dict(request.metadata or {}),
        }

    @staticmethod
    def _serialize_order_request(request: BinanceOrderRequest) -> dict[str, Any]:
        return {
            'symbol': request.symbol,
            'side': request.side,
            'order_type': request.order_type,
            'quantity': request.quantity,
            'reduce_only': request.reduce_only,
            'position_side': request.position_side,
            'client_order_id': request.client_order_id,
            'stop_price': request.stop_price,
            'close_position': request.close_position,
            'working_type': request.working_type,
            'price_protect': request.price_protect,
            'metadata': dict(request.metadata or {}),
        }

    @staticmethod
    def _serialize_submit_payload(request: BinanceOrderRequest) -> dict[str, Any]:
        is_algo_order = bool((request.metadata or {}).get('protective_order'))
        payload = {
            'symbol': request.symbol,
            'side': request.side,
            'type': request.order_type,
            'newClientOrderId': request.client_order_id,
        }
        if is_algo_order:
            payload['algoType'] = 'CONDITIONAL'
            payload['clientAlgoId'] = request.client_order_id
            payload.pop('newClientOrderId', None)
        if request.quantity is not None:
            payload['quantity'] = request.quantity
        if request.reduce_only and not request.close_position:
            payload['reduceOnly'] = 'true'
        if request.position_side is not None:
            payload['positionSide'] = request.position_side.upper()
        if request.stop_price is not None:
            if is_algo_order:
                payload['triggerPrice'] = request.stop_price
            else:
                payload['stopPrice'] = request.stop_price
        if request.close_position:
            payload['closePosition'] = 'true'
        if request.working_type is not None:
            payload['workingType'] = request.working_type
        if request.price_protect is not None:
            payload['priceProtect'] = 'TRUE' if request.price_protect else 'FALSE'
        return payload

    @staticmethod
    def _serialize_cancel_receipt(receipt: BinanceCancelReceipt) -> dict[str, Any]:
        return {
            'client_order_id': receipt.client_order_id,
            'exchange_order_id': receipt.exchange_order_id,
            'canceled': receipt.canceled,
            'cancel_status': receipt.cancel_status,
            'request_payload': receipt.request_payload,
            'response_payload': receipt.response_payload,
            'metadata': receipt.metadata,
            'error_code': receipt.error_code,
            'error_message': receipt.error_message,
        }

    @staticmethod
    def _receipt_from_submit_receipt(receipt: BinanceSubmitReceipt) -> SimulatedExecutionReceipt:
        return SimulatedExecutionReceipt(
            client_order_id=receipt.client_order_id,
            exchange_order_id=receipt.exchange_order_id,
            acknowledged=receipt.acknowledged,
            submitted_qty=receipt.submitted_qty,
            submitted_side=receipt.submitted_side,
            submit_status=receipt.submit_status,
            exchange_status=receipt.exchange_status,
            transact_time_ms=receipt.transact_time_ms,
            request_payload=receipt.request_payload,
            response_payload=receipt.response_payload,
            metadata=receipt.metadata,
            error_code=receipt.error_code,
            error_message=receipt.error_message,
        )



def plan_price_or_market_price(price_hint: float | None, market: MarketSnapshot) -> float:
    if price_hint is not None:
        return float(price_hint)
    return float(market.current_price)
