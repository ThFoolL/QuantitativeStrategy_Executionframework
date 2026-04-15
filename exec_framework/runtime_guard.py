from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from .models import ExecutionResult, LiveStateSnapshot
    from .unified_risk_action import (
        RECOVER_POLICY_KEEP_FROZEN,
        RECOVER_POLICY_MANUAL_REVIEW,
        RECOVER_POLICY_OBSERVE_ONLY,
        RECOVER_POLICY_READY_ONLY,
        RECOVER_STAGE_EXTERNAL_POSITION_OVERRIDE,
        RECOVER_STAGE_KEEP_FROZEN,
        RECOVER_STAGE_MANUAL_FLAT_CONFIRMED,
        RECOVER_STAGE_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT,
        RECOVER_STAGE_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT,
        RECOVER_STAGE_OBSERVE_PENDING,
        RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES,
        RECOVER_STAGE_POSITION_WORKING_PARTIAL_FILL,
        RECOVER_STAGE_PROTECTION_MISSING,
        RECOVER_STAGE_PROTECTION_PARTIAL_MISSING,
        RECOVER_STAGE_PROTECTION_PENDING_CONFIRM,
        RECOVER_STAGE_PROTECTION_SEMANTIC_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_TYPE_MISMATCH,
        RECOVER_STAGE_PROTECTION_SUBMIT_GATE_BLOCKED,
        RECOVER_STAGE_READONLY_QUERY_FAILED,
        RECOVER_STAGE_RECOVER_READY,
        RECOVER_STAGE_RELAPSE,
        RECOVER_STAGE_SHARED_BUDGET_EXHAUSTED,
        RISK_ACTION_FORCE_CLOSE,
        RISK_ACTION_MANUAL_REVIEW,
        RISK_ACTION_OBSERVE,
        RISK_ACTION_RECOVER_PROTECTION,
        STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS,
        STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS,
        STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE,
        STOP_CONDITION_FEE_RECONCILIATION_PENDING,
        STOP_CONDITION_GUARD_BLOCKED,
        STOP_CONDITION_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT,
        STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED,
        STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT,
        STOP_CONDITION_NON_RETRYABLE_CONFIRMATION_CATEGORY,
        STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING,
        STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING,
        STOP_CONDITION_POSITION_FACT_CONFIRMED_BEFORE_TRADE_ROWS,
        STOP_CONDITION_PROTECTION_ORDERS_MISSING,
        STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH,
        STOP_CONDITION_PROTECTION_STOP_MISSING,
        STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED,
        STOP_CONDITION_PROTECTION_TP_MISSING,
        STOP_CONDITION_READONLY_QUERY_FAILED,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED,
        STOP_CONDITION_SHARED_BUDGET_EXHAUSTED,
        STOP_CONDITION_TERMINAL_CONFIRMATION_REACHED,
        STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL,
        STOP_CONDITION_TRADES_CONFIRMED,
        STOP_CONDITION_UNRESOLVED_CONFIRMATION_STATE,
        derive_recover_policy as _shared_derive_recover_policy,
        derive_recover_stage as _shared_derive_recover_stage,
        derive_risk_action as _shared_derive_risk_action,
    )
except ImportError:  # pragma: no cover
    from models import ExecutionResult, LiveStateSnapshot
    from unified_risk_action import (
        RECOVER_POLICY_KEEP_FROZEN,
        RECOVER_POLICY_MANUAL_REVIEW,
        RECOVER_POLICY_OBSERVE_ONLY,
        RECOVER_POLICY_READY_ONLY,
        RECOVER_STAGE_EXTERNAL_POSITION_OVERRIDE,
        RECOVER_STAGE_KEEP_FROZEN,
        RECOVER_STAGE_MANUAL_FLAT_CONFIRMED,
        RECOVER_STAGE_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT,
        RECOVER_STAGE_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT,
        RECOVER_STAGE_OBSERVE_PENDING,
        RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES,
        RECOVER_STAGE_POSITION_WORKING_PARTIAL_FILL,
        RECOVER_STAGE_PROTECTION_MISSING,
        RECOVER_STAGE_PROTECTION_PARTIAL_MISSING,
        RECOVER_STAGE_PROTECTION_PENDING_CONFIRM,
        RECOVER_STAGE_PROTECTION_SEMANTIC_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_TYPE_MISMATCH,
        RECOVER_STAGE_PROTECTION_SUBMIT_GATE_BLOCKED,
        RECOVER_STAGE_READONLY_QUERY_FAILED,
        RECOVER_STAGE_RECOVER_READY,
        RECOVER_STAGE_RELAPSE,
        RECOVER_STAGE_SHARED_BUDGET_EXHAUSTED,
        RISK_ACTION_FORCE_CLOSE,
        RISK_ACTION_MANUAL_REVIEW,
        RISK_ACTION_OBSERVE,
        RISK_ACTION_RECOVER_PROTECTION,
        STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS,
        STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS,
        STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE,
        STOP_CONDITION_FEE_RECONCILIATION_PENDING,
        STOP_CONDITION_GUARD_BLOCKED,
        STOP_CONDITION_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT,
        STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED,
        STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT,
        STOP_CONDITION_NON_RETRYABLE_CONFIRMATION_CATEGORY,
        STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING,
        STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING,
        STOP_CONDITION_POSITION_FACT_CONFIRMED_BEFORE_TRADE_ROWS,
        STOP_CONDITION_PROTECTION_ORDERS_MISSING,
        STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH,
        STOP_CONDITION_PROTECTION_STOP_MISSING,
        STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED,
        STOP_CONDITION_PROTECTION_TP_MISSING,
        STOP_CONDITION_READONLY_QUERY_FAILED,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED,
        STOP_CONDITION_SHARED_BUDGET_EXHAUSTED,
        STOP_CONDITION_TERMINAL_CONFIRMATION_REACHED,
        STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL,
        STOP_CONDITION_TRADES_CONFIRMED,
        STOP_CONDITION_UNRESOLVED_CONFIRMATION_STATE,
        derive_recover_policy as _shared_derive_recover_policy,
        derive_recover_stage as _shared_derive_recover_stage,
        derive_risk_action as _shared_derive_risk_action,
    )

FREEZE_REASON_RECONCILE_MISMATCH = 'reconcile_mismatch'
FREEZE_REASON_POSTTRADE_PENDING = 'posttrade_pending_confirmation'
FREEZE_REASON_POSTTRADE_QUERY_FAILED = 'posttrade_query_failed'
FREEZE_REASON_POSTTRADE_REJECTED = 'posttrade_rejected_or_canceled'
FREEZE_REASON_MANUAL = 'manual_freeze'
FREEZE_REASON_UNKNOWN = 'unknown_risk_state'

RECOVER_RESULT_ALLOWED = 'RECOVERED'
RECOVER_RESULT_BLOCKED = 'BLOCKED'
RECOVER_RESULT_READY = 'READY'
RECOVER_RESULT_OBSERVE = 'OBSERVE'

RECOVER_STATE_READY = 'recover_ready'
RECOVER_STATE_OBSERVE = 'recover_observe'
RECOVER_STATE_BLOCKED = 'recover_blocked'
RECOVER_STATE_RELAPSE = 'recover_relapse'

RECOVER_POLICY_READY_ONLY = 'ready_only'
RECOVER_POLICY_OBSERVE_ONLY = 'observe_only'
RECOVER_POLICY_KEEP_FROZEN = 'keep_frozen'
RECOVER_POLICY_MANUAL_REVIEW = 'manual_review'

RISK_ACTION_OBSERVE = 'OBSERVE'
RISK_ACTION_RECOVER_PROTECTION = 'RECOVER_PROTECTION'
RISK_ACTION_FORCE_CLOSE = 'FORCE_CLOSE'
RISK_ACTION_MANUAL_REVIEW = 'MANUAL_REVIEW'

RECOVER_STAGE_RECOVER_READY = 'recover_ready'
RECOVER_STAGE_OBSERVE_PENDING = 'observe_pending'
RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES = 'position_confirmed_pending_trades'
RECOVER_STAGE_POSITION_WORKING_PARTIAL_FILL = 'position_working_partial_fill'
RECOVER_STAGE_PROTECTION_PENDING_CONFIRM = 'protection_pending_confirm'
RECOVER_STAGE_SHARED_BUDGET_EXHAUSTED = 'shared_budget_exhausted'
RECOVER_STAGE_READONLY_QUERY_FAILED = 'readonly_query_failed'
RECOVER_STAGE_KEEP_FROZEN = 'keep_frozen'
RECOVER_STAGE_RELAPSE = 'recover_relapse'
RECOVER_STAGE_MANUAL_FLAT_CONFIRMED = 'manual_flat_confirmed'
RECOVER_STAGE_EXTERNAL_POSITION_OVERRIDE = 'external_position_override'
RECOVER_STAGE_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT = 'manual_open_orders_side_or_qty_conflict'
RECOVER_STAGE_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT = 'manual_reduce_only_position_not_flat'
RECOVER_STAGE_PROTECTION_MISSING = 'protection_missing'
RECOVER_STAGE_PROTECTION_PARTIAL_MISSING = 'protection_partial_missing'
RECOVER_STAGE_PROTECTION_SUBMIT_GATE_BLOCKED = 'protection_submit_gate_blocked'
RECOVER_STAGE_PROTECTION_SEMANTIC_MISMATCH = 'protection_semantic_mismatch'
RECOVER_STAGE_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH = 'protection_semantic_position_side_mismatch'
RECOVER_STAGE_PROTECTION_SEMANTIC_TYPE_MISMATCH = 'protection_semantic_type_mismatch'
RECOVER_STAGE_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH = 'protection_semantic_stop_payload_mismatch'
RECOVER_STAGE_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH = 'protection_semantic_tp_payload_mismatch'

STOP_CONDITION_TERMINAL_CONFIRMATION_REACHED = 'terminal_confirmation_reached'
STOP_CONDITION_TRADES_CONFIRMED = 'trades_confirmed'
STOP_CONDITION_POSITION_FACT_CONFIRMED_BEFORE_TRADE_ROWS = 'position_fact_confirmed_before_trade_rows'
STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING = 'partial_fill_position_working'
STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS = 'avg_fill_price_missing_after_fills'
STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL = 'trade_rows_missing_after_fill'
STOP_CONDITION_FEE_RECONCILIATION_PENDING = 'fee_reconciliation_pending'
STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING = 'position_confirmed_but_protection_pending'
STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS = 'await_more_exchange_facts'
STOP_CONDITION_SHARED_BUDGET_EXHAUSTED = 'shared_budget_exhausted'
STOP_CONDITION_READONLY_QUERY_FAILED = 'readonly_query_failed'
STOP_CONDITION_NON_RETRYABLE_CONFIRMATION_CATEGORY = 'non_retryable_confirmation_category'
STOP_CONDITION_UNRESOLVED_CONFIRMATION_STATE = 'unresolved_confirmation_state'
STOP_CONDITION_GUARD_BLOCKED = 'guard_blocked'
STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED = 'manual_position_flat_confirmed'
STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE = 'external_position_override'
STOP_CONDITION_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT = 'manual_open_orders_side_or_qty_conflict'
STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT = 'manual_reduce_only_position_not_flat'
STOP_CONDITION_PROTECTION_ORDERS_MISSING = 'protection_orders_missing'
STOP_CONDITION_PROTECTION_STOP_MISSING = 'protection_stop_missing'
STOP_CONDITION_PROTECTION_TP_MISSING = 'protection_tp_missing'
STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED = 'protection_submit_gate_blocked'
STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH = 'protection_semantic_mismatch'
STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH = 'protection_semantic_position_side_mismatch'
STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH = 'protection_semantic_type_mismatch'
STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH = 'protection_semantic_stop_payload_mismatch'
STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH = 'protection_semantic_tp_payload_mismatch'
STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH = 'relapse_after_recover_ready_mismatch'
STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING = 'relapse_after_recover_ready_protection_missing'
STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED = 'relapse_after_recover_ready_query_failed'

RECOVER_AUDIT_MAX_ITEMS = 10

ASYNC_RUNNING_STATUS = 'running'
ASYNC_SUCCEEDED_STATUS = 'succeeded'
ASYNC_GUARD_FAMILY_PRIORITY = {
    'submit_auto_repair': 3,
    'protection_followup': 2,
    'execution_confirm': 1,
}
ASYNC_GUARD_FAMILIES = set(ASYNC_GUARD_FAMILY_PRIORITY)


def _clean_recover_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if value is not None}


def _current_async_stop_condition(operation: dict[str, Any]) -> str | None:
    stop_condition = operation.get('stop_condition')
    if isinstance(stop_condition, dict):
        current = stop_condition.get('current_condition')
        return str(current) if current else None
    if stop_condition is None:
        return None
    return str(stop_condition)


def _select_runtime_guard_async_operation(async_operations: dict[str, Any] | None) -> dict[str, Any]:
    try:
        from .async_operation import select_primary_async_operation
    except ImportError:  # pragma: no cover
        from async_operation import select_primary_async_operation

    candidate = dict(select_primary_async_operation(async_operations) or {})
    if not candidate:
        return {}
    if candidate.get('family') not in ASYNC_GUARD_FAMILIES:
        return {}
    if candidate.get('status') not in {ASYNC_RUNNING_STATUS, ASYNC_SUCCEEDED_STATUS}:
        return {}
    return candidate


def _build_async_recover_projection(*, state: LiveStateSnapshot, operation: dict[str, Any]) -> dict[str, Any]:
    family = str(operation.get('family') or '')
    status = str(operation.get('status') or '')
    stop_reason = str(operation.get('stop_reason') or '') or None
    stop_condition = _current_async_stop_condition(operation)
    pending_execution_phase = (
        operation.get('pending_execution_phase_view')
        or operation.get('trigger_phase')
        or state.pending_execution_phase
    )
    retry_budget = dict(operation.get('budget') or {})
    confirm_context = {
        'confirm_phase': family,
        'operation_id': operation.get('operation_id'),
        'stop_reason': stop_reason,
        'stop_condition': stop_condition,
        'retry_budget': retry_budget or None,
        'async_family': family,
        'async_status': status,
        'async_trigger_phase': operation.get('trigger_phase'),
    }
    recover_ready = status == ASYNC_SUCCEEDED_STATUS
    result = RECOVER_RESULT_READY if recover_ready else RECOVER_RESULT_OBSERVE
    allowed = recover_ready
    if family == 'protection_followup':
        result = RECOVER_RESULT_OBSERVE
        allowed = False
    reason = stop_reason or str(pending_execution_phase or family or 'runtime_guard_async_operation')
    recover_policy = _derive_recover_policy(
        result=result,
        allowed=allowed,
        recover_ready=recover_ready,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
    )
    recover_stage = _derive_recover_stage(
        result=result,
        stop_reason=stop_reason or reason,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
        freeze_reason=state.freeze_reason,
    )
    return build_recover_record(
        checked_at=state.state_ts,
        source=f'{family}_async_operation',
        result=result,
        allowed=allowed and state.consistency_status in {'OK', 'DRY_RUN'},
        reason=reason,
        pending_execution_phase=pending_execution_phase,
        freeze_reason=state.freeze_reason,
        consistency_status=state.consistency_status,
        runtime_mode=state.runtime_mode,
        recover_ready=recover_ready,
        requires_manual_resume=bool(recover_ready),
        guard_decision=(operation.get('guard_decision') or ('ready_only_no_resubmit' if recover_ready else f'keep_frozen_{recover_stage or family}')),
        recover_policy=recover_policy,
        recover_stage=recover_stage,
        stop_reason=stop_reason,
        stop_condition=stop_condition,
        stop_category=None,
        confirm_phase=family,
        confirm_context=confirm_context,
        retry_budget=retry_budget,
    )


def _derive_risk_action(*, recover_policy: str | None, recover_stage: str | None, stop_condition: str | None) -> str:
    return _shared_derive_risk_action(
        recover_policy=recover_policy,
        recover_stage=recover_stage,
        stop_condition=stop_condition,
    )


def build_recover_record(
    *,
    checked_at: str,
    source: str,
    result: str,
    allowed: bool,
    reason: str,
    pending_execution_phase: str | None,
    freeze_reason: str | None,
    consistency_status: str | None,
    runtime_mode: str | None,
    recover_ready: bool | None = None,
    requires_manual_resume: bool = True,
    guard_decision: str | None = None,
    recover_policy: str | None = None,
    recover_policy_display: str | None = None,
    recover_policy_legacy: str | None = None,
    recover_stage: str | None = None,
    stop_reason: str | None = None,
    stop_condition: str | None = None,
    stop_category: str | None = None,
    confirm_phase: str | None = None,
    confirm_context: dict[str, Any] | None = None,
    retry_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_confirm_context = dict(confirm_context or {})
    effective_retry_budget = dict(retry_budget or effective_confirm_context.get('retry_budget') or {})
    effective_stop_reason = stop_reason or effective_confirm_context.get('stop_reason') or reason
    effective_stop_condition = stop_condition or effective_confirm_context.get('stop_condition')
    effective_confirm_phase = confirm_phase or effective_confirm_context.get('confirm_phase')
    # Manual-review scenarios stay explicit even when the record itself is blocked.
    effective_recover_policy = recover_policy or _derive_recover_policy(
        result=result,
        allowed=allowed,
        recover_ready=recover_ready if recover_ready is not None else (result == RECOVER_RESULT_READY),
        stop_condition=effective_stop_condition,
        pending_execution_phase=pending_execution_phase,
    )
    effective_recover_stage = recover_stage or _derive_recover_stage(
        result=result,
        stop_reason=effective_stop_reason,
        stop_condition=effective_stop_condition,
        pending_execution_phase=pending_execution_phase,
        freeze_reason=freeze_reason,
    )
    effective_stop_category = stop_category or _derive_stop_category(
        result=result,
        allowed=allowed,
        stop_condition=effective_stop_condition,
        freeze_reason=freeze_reason,
    )
    effective_recover_policy_display = recover_policy_display or _derive_recover_policy_display(
        recover_policy=effective_recover_policy,
        stop_condition=effective_stop_condition,
    )
    effective_recover_policy_legacy = recover_policy_legacy or effective_recover_policy
    return _clean_recover_record(
        {
            'checked_at': checked_at,
            'source': source,
            'result': result,
            'allowed': allowed,
            'reason': reason,
            'pending_execution_phase': pending_execution_phase,
            'freeze_reason': freeze_reason,
            'consistency_status': consistency_status,
            'runtime_mode': runtime_mode,
            'recover_ready': recover_ready if recover_ready is not None else (result == RECOVER_RESULT_READY),
            'requires_manual_resume': requires_manual_resume,
            'guard_decision': guard_decision,
            'recover_policy': effective_recover_policy,
            'recover_policy_display': effective_recover_policy_display,
            'legacy_recover_policy': effective_recover_policy_legacy,
            'recover_stage': effective_recover_stage,
            'risk_action': _derive_risk_action(
                recover_policy=effective_recover_policy,
                recover_stage=effective_recover_stage,
                stop_condition=effective_stop_condition,
            ),
            'stop_reason': effective_stop_reason,
            'stop_condition': effective_stop_condition,
            'stop_category': effective_stop_category,
            'confirm_phase': effective_confirm_phase,
            'confirm_context': effective_confirm_context or None,
            'retry_budget': effective_retry_budget or None,
        }
    )


def _derive_recover_policy(
    *,
    result: str,
    allowed: bool,
    recover_ready: bool,
    stop_condition: str | None,
    pending_execution_phase: str | None,
) -> str:
    return _shared_derive_recover_policy(
        result=result,
        allowed=allowed,
        recover_ready=recover_ready,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
    )


def _derive_recover_stage(
    *,
    result: str,
    stop_reason: str | None,
    stop_condition: str | None,
    pending_execution_phase: str | None,
    freeze_reason: str | None,
) -> str:
    return _shared_derive_recover_stage(
        result=result,
        stop_reason=stop_reason,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
        freeze_reason=freeze_reason,
    )


def _derive_recover_policy_display(*, recover_policy: str | None, stop_condition: str | None) -> str | None:
    if stop_condition in {
        STOP_CONDITION_PROTECTION_ORDERS_MISSING,
        STOP_CONDITION_PROTECTION_STOP_MISSING,
        STOP_CONDITION_PROTECTION_TP_MISSING,
    }:
        return 'recover_protection'
    if stop_condition in {
        STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED,
        STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED,
    }:
        return 'manual_review'
    return recover_policy


def _derive_stop_category(
    *,
    result: str,
    allowed: bool,
    stop_condition: str | None,
    freeze_reason: str | None,
) -> str:
    if result == RECOVER_RESULT_READY:
        return 'ready'
    if stop_condition in {
        STOP_CONDITION_PROTECTION_ORDERS_MISSING,
        STOP_CONDITION_PROTECTION_STOP_MISSING,
        STOP_CONDITION_PROTECTION_TP_MISSING,
    }:
        return 'recover_protection'
    if stop_condition in {
        STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED,
        STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE,
        STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED,
        STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
        STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED,
    }:
        return 'manual_review'
    if stop_condition in {
        STOP_CONDITION_TRADES_CONFIRMED,
        STOP_CONDITION_TERMINAL_CONFIRMATION_REACHED,
    }:
        return 'confirmed'
    if stop_condition in {
        STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS,
        STOP_CONDITION_POSITION_FACT_CONFIRMED_BEFORE_TRADE_ROWS,
        STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING,
        STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS,
        STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL,
        STOP_CONDITION_FEE_RECONCILIATION_PENDING,
        STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING,
        STOP_CONDITION_SHARED_BUDGET_EXHAUSTED,
        STOP_CONDITION_READONLY_QUERY_FAILED,
    }:
        return 'observe'
    if not allowed or stop_condition in {
        STOP_CONDITION_NON_RETRYABLE_CONFIRMATION_CATEGORY,
        STOP_CONDITION_UNRESOLVED_CONFIRMATION_STATE,
        STOP_CONDITION_GUARD_BLOCKED,
    }:
        return 'blocked'
    if freeze_reason:
        return 'frozen'
    return 'unknown'


# recover guard 的真正放行/观察/阻断由 policy + stage + stop_condition 显式决定，避免只靠粗粒度 consistency/pending phase。
def _resolve_recover_guard_decision(
    *,
    consistency_status: str | None,
    recover_policy: str | None,
    recover_stage: str | None,
    stop_condition: str | None,
    pending_execution_phase: str | None,
) -> tuple[str, str, str]:
    effective_consistency = str(consistency_status or '')
    effective_stage = str(recover_stage or '')
    effective_policy = str(recover_policy or '')
    effective_stop_condition = str(stop_condition or '')
    effective_pending_phase = str(pending_execution_phase or '')

    if effective_consistency not in {'OK', 'DRY_RUN'}:
        return (
            'block',
            'keep_frozen_consistency_not_ok',
            f'consistency_not_ok:{effective_consistency or "unknown"}',
        )

    if effective_stage == RECOVER_STAGE_RECOVER_READY and effective_policy == RECOVER_POLICY_READY_ONLY:
        if effective_stop_condition in {
            STOP_CONDITION_TRADES_CONFIRMED,
            STOP_CONDITION_TERMINAL_CONFIRMATION_REACHED,
            'retry_submit_dispatched',
        }:
            return ('allow', 'ready_only_resume_allowed', 'recover_ready')
        return ('block', 'keep_frozen_ready_stage_without_terminal_confirmation', 'recover_ready_missing_terminal_confirmation')

    if effective_policy == RECOVER_POLICY_OBSERVE_ONLY:
        observe_cases = {
            (RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES, STOP_CONDITION_POSITION_FACT_CONFIRMED_BEFORE_TRADE_ROWS): (
                'keep_frozen_position_confirmed_pending_trades',
                'position_confirmed_pending_trades',
            ),
            (RECOVER_STAGE_POSITION_WORKING_PARTIAL_FILL, STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING): (
                'keep_frozen_partial_fill_position_working',
                'partial_position_working',
            ),
            (RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES, STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS): (
                'keep_frozen_avg_fill_price_missing_after_fills',
                'avg_fill_price_missing',
            ),
            (RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES, STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL): (
                'keep_frozen_trade_rows_missing_after_fill',
                'position_confirmed_pending_trades',
            ),
            (RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES, STOP_CONDITION_FEE_RECONCILIATION_PENDING): (
                'keep_frozen_fee_reconciliation_pending',
                'fee_reconciliation_pending',
            ),
            (RECOVER_STAGE_PROTECTION_PENDING_CONFIRM, STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING): (
                'keep_frozen_protection_pending_confirm',
                'protection_pending_confirm',
            ),
            (RECOVER_STAGE_READONLY_QUERY_FAILED, STOP_CONDITION_READONLY_QUERY_FAILED): (
                'keep_frozen_readonly_query_failed',
                'readonly_query_failed',
            ),
            (RECOVER_STAGE_SHARED_BUDGET_EXHAUSTED, STOP_CONDITION_SHARED_BUDGET_EXHAUSTED): (
                'keep_frozen_shared_budget_exhausted',
                'shared_budget_exhausted',
            ),
            (RECOVER_STAGE_OBSERVE_PENDING, STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS): (
                'keep_frozen_observe_pending',
                f'pending_execution_phase:{effective_pending_phase or "submitted"}',
            ),
        }
        if (effective_stage, effective_stop_condition) in observe_cases:
            guard_decision, reason = observe_cases[(effective_stage, effective_stop_condition)]
            return ('observe', guard_decision, reason)
        return ('block', 'keep_frozen_observe_policy_mismatch', f'observe_policy_mismatch:{effective_stage or "unknown"}:{effective_stop_condition or "unknown"}')

    if effective_policy == RECOVER_POLICY_KEEP_FROZEN:
        blocked_reason = effective_stop_condition or effective_stage or 'guard_blocked'
        return ('block', 'keep_frozen_blocked', blocked_reason)

    if effective_policy == RECOVER_POLICY_MANUAL_REVIEW:
        manual_review_cases = {
            (RECOVER_STAGE_MANUAL_FLAT_CONFIRMED, STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED): ('keep_frozen_manual_flat_confirmed', 'manual_position_flat_confirmed'),
            (RECOVER_STAGE_EXTERNAL_POSITION_OVERRIDE, STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE): ('keep_frozen_external_position_override', 'external_position_override'),
            (RECOVER_STAGE_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT, STOP_CONDITION_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT): ('keep_frozen_manual_open_orders_side_or_qty_conflict', 'manual_open_orders_side_or_qty_conflict'),
            (RECOVER_STAGE_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT, STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT): ('keep_frozen_manual_reduce_only_position_not_flat', 'manual_reduce_only_position_not_flat'),
            (RECOVER_STAGE_PROTECTION_MISSING, STOP_CONDITION_PROTECTION_ORDERS_MISSING): ('keep_frozen_protection_missing', 'protection_missing'),
            (RECOVER_STAGE_PROTECTION_PARTIAL_MISSING, STOP_CONDITION_PROTECTION_STOP_MISSING): ('keep_frozen_protection_stop_missing', 'protection_stop_missing'),
            (RECOVER_STAGE_PROTECTION_PARTIAL_MISSING, STOP_CONDITION_PROTECTION_TP_MISSING): ('keep_frozen_protection_tp_missing', 'protection_tp_missing'),
            (RECOVER_STAGE_PROTECTION_SUBMIT_GATE_BLOCKED, STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED): ('keep_frozen_protection_submit_gate_blocked', 'protection_submit_gate_blocked'),
            (RECOVER_STAGE_PROTECTION_SEMANTIC_MISMATCH, STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH): ('keep_frozen_protection_semantic_mismatch', 'protection_semantic_mismatch'),
            (RECOVER_STAGE_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH, STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH): ('keep_frozen_protection_semantic_position_side_mismatch', 'protection_semantic_mismatch'),
            (RECOVER_STAGE_PROTECTION_SEMANTIC_TYPE_MISMATCH, STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH): ('keep_frozen_protection_semantic_type_mismatch', 'protection_semantic_mismatch'),
            (RECOVER_STAGE_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH, STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH): ('keep_frozen_protection_semantic_stop_payload_mismatch', 'protection_semantic_mismatch'),
            (RECOVER_STAGE_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH, STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH): ('keep_frozen_protection_semantic_tp_payload_mismatch', 'protection_semantic_mismatch'),
            (RECOVER_STAGE_RELAPSE, STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH): ('keep_frozen_relapse_after_recover_ready_mismatch', 'relapse_after_recover_ready_mismatch'),
            (RECOVER_STAGE_RELAPSE, STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING): ('keep_frozen_relapse_after_recover_ready_protection_missing', 'relapse_after_recover_ready_protection_missing'),
            (RECOVER_STAGE_RELAPSE, STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED): ('keep_frozen_relapse_after_recover_ready_query_failed', 'relapse_after_recover_ready_query_failed'),
        }
        if (effective_stage, effective_stop_condition) in manual_review_cases:
            guard_decision, reason = manual_review_cases[(effective_stage, effective_stop_condition)]
            return ('block', guard_decision, reason)
        return ('block', 'keep_frozen_manual_review', effective_stop_condition or effective_stage or 'manual_review_required')

    return ('block', 'keep_frozen_unknown_recover_policy', effective_policy or 'recover_policy_missing')


def _build_runtime_recover_check(state: LiveStateSnapshot) -> dict[str, Any]:
    active_async_operation = _select_runtime_guard_async_operation(state.async_operations)
    if active_async_operation:
        return _build_async_recover_projection(state=state, operation=active_async_operation)

    recover_check = dict(state.recover_check or {})
    confirm_context = dict(recover_check.get('confirm_context') or {})
    stop_reason = recover_check.get('stop_reason') or confirm_context.get('stop_reason')
    stop_condition = recover_check.get('stop_condition') or confirm_context.get('stop_condition')
    recover_policy = recover_check.get('recover_policy') or _derive_recover_policy(
        result=RECOVER_RESULT_BLOCKED if state.consistency_status not in {'OK', 'DRY_RUN'} else RECOVER_RESULT_OBSERVE,
        allowed=state.consistency_status in {'OK', 'DRY_RUN'},
        recover_ready=False,
        stop_condition=stop_condition,
        pending_execution_phase=state.pending_execution_phase,
    )
    recover_stage = recover_check.get('recover_stage') or _derive_recover_stage(
        result=RECOVER_RESULT_BLOCKED if state.consistency_status not in {'OK', 'DRY_RUN'} else RECOVER_RESULT_OBSERVE,
        stop_reason=stop_reason or state.freeze_reason or state.pending_execution_phase,
        stop_condition=stop_condition,
        pending_execution_phase=state.pending_execution_phase,
        freeze_reason=state.freeze_reason,
    )
    return build_recover_record(
        checked_at=state.state_ts,
        source=recover_check.get('source') or 'runtime_guard',
        result=recover_check.get('result') or (RECOVER_RESULT_BLOCKED if state.consistency_status not in {'OK', 'DRY_RUN'} else RECOVER_RESULT_OBSERVE),
        allowed=bool(recover_check.get('allowed')) and state.consistency_status in {'OK', 'DRY_RUN'},
        reason=recover_check.get('reason') or stop_reason or state.freeze_reason or state.pending_execution_phase or 'runtime_guard_recover_check',
        pending_execution_phase=state.pending_execution_phase,
        freeze_reason=state.freeze_reason,
        consistency_status=state.consistency_status,
        runtime_mode=state.runtime_mode,
        recover_ready=bool(recover_check.get('recover_ready')),
        requires_manual_resume=bool(recover_check.get('requires_manual_resume', True)),
        guard_decision=recover_check.get('guard_decision'),
        recover_policy=recover_policy,
        recover_stage=recover_stage,
        stop_reason=stop_reason or state.freeze_reason or state.pending_execution_phase,
        stop_condition=stop_condition,
        stop_category=recover_check.get('stop_category'),
        confirm_phase=recover_check.get('confirm_phase') or confirm_context.get('confirm_phase'),
        confirm_context=confirm_context,
        retry_budget=recover_check.get('retry_budget') or confirm_context.get('retry_budget'),
    )


def append_recover_record(
    timeline: list[dict[str, Any]] | None,
    record: dict[str, Any],
    *,
    limit: int = RECOVER_AUDIT_MAX_ITEMS,
) -> list[dict[str, Any]]:
    items = list(timeline or [])
    items.append(_clean_recover_record(record))
    return items[-max(1, int(limit)):]


@dataclass(frozen=True)
class FreezeDecision:
    should_freeze: bool
    freeze_reason: str | None
    runtime_mode: str
    freeze_status: str
    state_updates: dict[str, object]


@dataclass(frozen=True)
class RecoverDecision:
    allowed: bool
    result: str
    reason: str
    state_updates: dict[str, object]


class RuntimeFreezeController:
    def freeze_from_result(self, state: LiveStateSnapshot, result: ExecutionResult) -> FreezeDecision:
        reason = result.freeze_reason or state.freeze_reason
        if result.should_freeze or state.consistency_status not in {'OK', 'DRY_RUN'}:
            resolved_reason = reason or self._reason_from_state(state)
            return FreezeDecision(
                should_freeze=True,
                freeze_reason=resolved_reason,
                runtime_mode='FROZEN',
                freeze_status='ACTIVE',
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': resolved_reason,
                    'last_freeze_reason': resolved_reason,
                    'last_freeze_at': result.result_ts,
                    'pending_execution_phase': result.execution_phase,
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
            )
        return FreezeDecision(
            should_freeze=False,
            freeze_reason=None,
            runtime_mode=state.runtime_mode,
            freeze_status=state.freeze_status,
            state_updates={},
        )

    def evaluate_recover(self, state: LiveStateSnapshot) -> RecoverDecision:
        runtime_recover_check = _build_runtime_recover_check(state)
        guard_action, guard_decision, guard_reason = _resolve_recover_guard_decision(
            consistency_status=state.consistency_status,
            recover_policy=runtime_recover_check.get('recover_policy'),
            recover_stage=runtime_recover_check.get('recover_stage'),
            stop_condition=runtime_recover_check.get('stop_condition'),
            pending_execution_phase=state.pending_execution_phase,
        )

        if guard_action == 'allow':
            recover_check = build_recover_record(
                checked_at=state.state_ts,
                source='runtime_guard',
                result=RECOVER_RESULT_ALLOWED,
                allowed=True,
                reason=guard_reason,
                pending_execution_phase=state.pending_execution_phase,
                freeze_reason=state.freeze_reason,
                consistency_status=state.consistency_status,
                runtime_mode='ACTIVE',
                recover_ready=True,
                guard_decision=guard_decision,
                recover_policy=runtime_recover_check.get('recover_policy'),
                recover_stage=runtime_recover_check.get('recover_stage'),
                stop_reason=runtime_recover_check.get('stop_reason'),
                stop_condition=runtime_recover_check.get('stop_condition'),
                stop_category=runtime_recover_check.get('stop_category'),
                confirm_phase=runtime_recover_check.get('confirm_phase'),
                confirm_context=runtime_recover_check.get('confirm_context'),
                retry_budget=runtime_recover_check.get('retry_budget'),
            )
            recover_check['source'] = runtime_recover_check.get('source') or 'runtime_guard'
            recover_check['decision'] = RECOVER_RESULT_ALLOWED
            return RecoverDecision(
                allowed=True,
                result=RECOVER_RESULT_ALLOWED,
                reason=guard_reason,
                state_updates={
                    'runtime_mode': 'ACTIVE',
                    'freeze_status': 'NONE',
                    'freeze_reason': None,
                    'last_recover_result': RECOVER_RESULT_ALLOWED,
                    'last_recover_at': state.state_ts,
                    'recover_attempt_count': int(state.recover_attempt_count) + 1,
                    'pending_execution_phase': None,
                    'can_open_new_position': True,
                    'can_modify_position': True,
                    'recover_check': recover_check,
                    'recover_timeline': append_recover_record(state.recover_timeline, recover_check),
                },
            )

        observe_result = RECOVER_RESULT_OBSERVE if guard_action == 'observe' else RECOVER_RESULT_BLOCKED
        observe_allowed = guard_action == 'observe'
        recover_check = build_recover_record(
            checked_at=state.state_ts,
            source='runtime_guard',
            result=observe_result,
            allowed=observe_allowed,
            reason=guard_reason,
            pending_execution_phase=state.pending_execution_phase,
            freeze_reason=state.freeze_reason,
            consistency_status=state.consistency_status,
            runtime_mode=state.runtime_mode,
            recover_ready=False,
            guard_decision=guard_decision,
            recover_policy=runtime_recover_check.get('recover_policy'),
            recover_stage=runtime_recover_check.get('recover_stage'),
            stop_reason=runtime_recover_check.get('stop_reason'),
            stop_condition=runtime_recover_check.get('stop_condition'),
            stop_category=runtime_recover_check.get('stop_category'),
            confirm_phase=runtime_recover_check.get('confirm_phase'),
            confirm_context=runtime_recover_check.get('confirm_context'),
            retry_budget=runtime_recover_check.get('retry_budget'),
        )
        recover_check['source'] = runtime_recover_check.get('source') or 'runtime_guard'
        recover_check['decision'] = observe_result
        return RecoverDecision(
            allowed=False,
            result=observe_result,
            reason=guard_reason,
            state_updates={
                'last_recover_result': observe_result,
                'recover_attempt_count': int(state.recover_attempt_count) + 1,
                'recover_check': recover_check,
                'recover_timeline': append_recover_record(state.recover_timeline, recover_check),
            },
        )

    def _recover_block_reason(self, state: LiveStateSnapshot) -> str | None:
        if state.consistency_status != 'OK':
            return f'consistency_not_ok:{state.consistency_status}'
        if state.pending_execution_phase not in {None, 'confirmed', 'position_confirmed_pending_trades', 'protection_pending_confirm', 'none'}:
            return f'pending_execution_phase:{state.pending_execution_phase}'
        if state.position_confirmation_level == 'POSITION_CONFIRMED' and state.exchange_position_side in {'long', 'short'} and state.exchange_position_qty > 0:
            return None
        if state.exchange_position_qty > 0 and state.exchange_position_side is None:
            return 'exchange_position_side_missing'
        return None

    def _reason_from_state(self, state: LiveStateSnapshot) -> str:
        if state.consistency_status == 'MISMATCH':
            return FREEZE_REASON_RECONCILE_MISMATCH
        return state.freeze_reason or FREEZE_REASON_UNKNOWN


def build_readonly_recheck_recover_check(*, decision: dict[str, Any], checked_at: str | None = None) -> dict[str, Any]:
    status = decision.get('status')
    freeze_reason = decision.get('freeze_reason')
    checked_at = checked_at or str(decision.get('checked_at') or '')
    confirm_context = dict(decision.get('confirm_context') or {})
    retry_budget = dict(decision.get('retry_budget') or confirm_context.get('retry_budget') or {})
    pending_execution_phase = str(decision.get('pending_execution_phase') or '') or None
    stop_reason = decision.get('stop_reason') or confirm_context.get('stop_reason')
    stop_condition = decision.get('stop_condition') or confirm_context.get('stop_condition')
    confirm_phase = decision.get('confirm_phase') or confirm_context.get('confirm_phase') or 'readonly_recheck'
    explicit_recover_stage = decision.get('recover_stage')
    explicit_recover_policy = decision.get('recover_policy')

    def _resolve_guard_decision(*, recover_policy: str | None, recover_stage: str | None, effective_stop_condition: str | None, effective_pending_phase: str | None, fallback: str) -> str:
        _, guard_decision, _ = _resolve_recover_guard_decision(
            consistency_status='OK',
            recover_policy=recover_policy,
            recover_stage=recover_stage,
            stop_condition=effective_stop_condition,
            pending_execution_phase=effective_pending_phase,
        )
        return guard_decision or fallback

    if status == 'readonly_recheck_recover_ready':
        recover_policy = RECOVER_POLICY_READY_ONLY
        recover_stage = RECOVER_STAGE_RECOVER_READY
        effective_pending_phase = pending_execution_phase or 'confirmed'
        effective_stop_condition = stop_condition or STOP_CONDITION_TRADES_CONFIRMED
        return build_recover_record(
            checked_at=checked_at,
            source='readonly_recheck',
            result=RECOVER_RESULT_READY,
            allowed=True,
            reason='readonly_recheck_recover_ready',
            pending_execution_phase=effective_pending_phase,
            freeze_reason=None,
            consistency_status='OK',
            runtime_mode='FROZEN',
            recover_ready=True,
            guard_decision=_resolve_guard_decision(
                recover_policy=recover_policy,
                recover_stage=recover_stage,
                effective_stop_condition=effective_stop_condition,
                effective_pending_phase=effective_pending_phase,
                fallback='ready_only_no_resubmit',
            ),
            recover_policy=recover_policy,
            recover_stage=recover_stage,
            stop_reason=stop_reason or 'recover_ready',
            stop_condition=effective_stop_condition,
            confirm_phase=confirm_phase,
            confirm_context=confirm_context,
            retry_budget=retry_budget,
        )
    if status == 'readonly_recheck_pending':
        pending_reason = freeze_reason or str(decision.get('reason') or 'pending_execution_phase:submitted')
        if pending_execution_phase is None:
            if pending_reason == 'position_confirmed_pending_trades':
                pending_execution_phase = 'position_confirmed_pending_trades'
            elif pending_reason == 'protection_pending_confirm':
                pending_execution_phase = 'protection_pending_confirm'
            else:
                pending_execution_phase = 'submitted'
        derived_stop_condition = stop_condition
        if derived_stop_condition is None:
            if pending_reason == 'partial_position_working':
                derived_stop_condition = STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING
            elif pending_reason == 'avg_fill_price_missing':
                derived_stop_condition = STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS
            elif pending_reason == 'fee_reconciliation_pending':
                derived_stop_condition = STOP_CONDITION_FEE_RECONCILIATION_PENDING
            elif pending_reason == 'position_confirmed_pending_trades':
                derived_stop_condition = STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL
            elif pending_reason == 'protection_pending_confirm':
                derived_stop_condition = STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING
            elif pending_reason in {'readonly_recheck_query_failed', 'readonly_recheck_query_exception', 'query_failed'}:
                derived_stop_condition = STOP_CONDITION_READONLY_QUERY_FAILED
            elif stop_reason == 'retry_budget_exhausted':
                derived_stop_condition = STOP_CONDITION_SHARED_BUDGET_EXHAUSTED
            else:
                derived_stop_condition = STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS
        recover_policy = explicit_recover_policy or _derive_recover_policy(
            result=RECOVER_RESULT_OBSERVE,
            allowed=False,
            recover_ready=False,
            stop_condition=derived_stop_condition,
            pending_execution_phase=pending_execution_phase,
        )
        recover_stage = explicit_recover_stage or _derive_recover_stage(
            result=RECOVER_RESULT_OBSERVE,
            stop_reason=stop_reason or pending_reason,
            stop_condition=derived_stop_condition,
            pending_execution_phase=pending_execution_phase,
            freeze_reason=freeze_reason,
        )
        return build_recover_record(
            checked_at=checked_at,
            source='readonly_recheck',
            result=RECOVER_RESULT_OBSERVE,
            allowed=False,
            reason=pending_reason,
            pending_execution_phase=pending_execution_phase,
            freeze_reason=freeze_reason,
            consistency_status='OK',
            runtime_mode='FROZEN',
            recover_ready=False,
            guard_decision=_resolve_guard_decision(
                recover_policy=recover_policy,
                recover_stage=recover_stage,
                effective_stop_condition=derived_stop_condition,
                effective_pending_phase=pending_execution_phase,
                fallback='keep_frozen_observe',
            ),
            recover_policy=recover_policy,
            recover_stage=recover_stage,
            stop_reason=stop_reason or pending_reason,
            stop_condition=derived_stop_condition,
            confirm_phase=confirm_phase,
            confirm_context=confirm_context,
            retry_budget=retry_budget,
        )
    recover_policy = explicit_recover_policy
    recover_stage = explicit_recover_stage
    effective_pending_phase = pending_execution_phase or 'frozen'
    effective_stop_condition = stop_condition or STOP_CONDITION_UNRESOLVED_CONFIRMATION_STATE
    return build_recover_record(
        checked_at=checked_at,
        source='readonly_recheck',
        result=RECOVER_RESULT_BLOCKED,
        allowed=False,
        reason=freeze_reason or 'readonly_recheck_blocked',
        pending_execution_phase=effective_pending_phase,
        freeze_reason=freeze_reason,
        consistency_status=decision.get('consistency_status'),
        runtime_mode='FROZEN',
        recover_ready=False,
        guard_decision=_resolve_guard_decision(
            recover_policy=recover_policy,
            recover_stage=recover_stage,
            effective_stop_condition=effective_stop_condition,
            effective_pending_phase=effective_pending_phase,
            fallback='keep_frozen_blocked',
        ),
        recover_policy=recover_policy,
        recover_stage=recover_stage,
        stop_reason=stop_reason or freeze_reason or 'readonly_recheck_blocked',
        stop_condition=effective_stop_condition,
        confirm_phase=confirm_phase,
        confirm_context=confirm_context,
        retry_budget=retry_budget,
    )


def derive_recover_state(
    *,
    freeze: dict[str, Any],
    recover_check: dict[str, Any] | None,
    recover_timeline: list[dict[str, Any]] | None,
    readonly_recheck: dict[str, Any] | None = None,
) -> str | None:
    timeline = list(recover_timeline or [])
    timeline_results = [str(item.get('result') or '').upper() for item in timeline]
    check = dict(recover_check or {})
    readonly = dict(readonly_recheck or {})
    check_result = str(check.get('result') or '').upper()
    readonly_status = readonly.get('status')
    recover_stage = str(check.get('recover_stage') or '')

    if 'RECOVERED' in timeline_results and timeline_results and timeline_results[-1] == 'BLOCKED':
        return RECOVER_STATE_RELAPSE
    if recover_stage == RECOVER_STAGE_RELAPSE:
        return RECOVER_STATE_RELAPSE
    if check.get('stop_condition') in {
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING,
        STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED,
    }:
        return RECOVER_STATE_RELAPSE
    if check_result == RECOVER_RESULT_READY or readonly_status == 'readonly_recheck_recover_ready':
        return RECOVER_STATE_READY
    if check_result == RECOVER_RESULT_OBSERVE or readonly_status == 'readonly_recheck_pending':
        return RECOVER_STATE_OBSERVE
    if freeze.get('freeze_status') == 'NONE' and freeze.get('last_recover_result') == RECOVER_RESULT_ALLOWED:
        return RECOVER_STATE_READY
    if (recover_check or {}).get('allowed') is False or freeze.get('last_recover_result') == RECOVER_RESULT_BLOCKED:
        return RECOVER_STATE_BLOCKED
    return None
