from __future__ import annotations

from typing import Any

RISK_ACTION_OBSERVE = 'OBSERVE'
RISK_ACTION_RECOVER_PROTECTION = 'RECOVER_PROTECTION'
RISK_ACTION_FORCE_CLOSE = 'FORCE_CLOSE'
RISK_ACTION_MANUAL_REVIEW = 'MANUAL_REVIEW'

RECOVER_POLICY_READY_ONLY = 'ready_only'
RECOVER_POLICY_OBSERVE_ONLY = 'observe_only'
RECOVER_POLICY_KEEP_FROZEN = 'keep_frozen'
RECOVER_POLICY_MANUAL_REVIEW = 'manual_review'

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

OBSERVE_STOP_CONDITIONS = {
    STOP_CONDITION_POSITION_FACT_CONFIRMED_BEFORE_TRADE_ROWS,
    STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING,
    STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS,
    STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL,
    STOP_CONDITION_FEE_RECONCILIATION_PENDING,
    STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING,
    STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS,
    STOP_CONDITION_SHARED_BUDGET_EXHAUSTED,
    STOP_CONDITION_READONLY_QUERY_FAILED,
}

MANUAL_REVIEW_STOP_CONDITIONS = {
    STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED,
    STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE,
    STOP_CONDITION_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT,
    STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT,
    STOP_CONDITION_PROTECTION_ORDERS_MISSING,
    STOP_CONDITION_PROTECTION_STOP_MISSING,
    STOP_CONDITION_PROTECTION_TP_MISSING,
    STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED,
    STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
    STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_MISMATCH,
    STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_PROTECTION_MISSING,
    STOP_CONDITION_RELAPSE_AFTER_RECOVER_READY_QUERY_FAILED,
}

FORCE_CLOSE_STOP_CONDITIONS = {
    STOP_CONDITION_NON_RETRYABLE_CONFIRMATION_CATEGORY,
    STOP_CONDITION_UNRESOLVED_CONFIRMATION_STATE,
    STOP_CONDITION_GUARD_BLOCKED,
}

OBSERVE_FREEZE_REASONS = {
    'execution_status_unknown',
    'posttrade_pending_confirmation',
    'posttrade_missing_fills',
    'readonly_recheck_pending_confirmation',
    'readonly_recheck_query_failed',
    'readonly_recheck_query_exception',
}

MANUAL_REVIEW_FREEZE_REASONS = {
    'protective_order_missing',
    'protection_stop_missing',
    'protection_tp_missing',
    'protection_submit_gate_blocked',
    'local_exchange_position_presence_mismatch',
    'reconcile_mismatch',
}

_PARTIAL_PROTECTION_NOTE_MAP = {
    'protection_orders_missing': ('protection_missing', STOP_CONDITION_PROTECTION_ORDERS_MISSING),
    'protection_stop_missing': ('protection_stop_missing', STOP_CONDITION_PROTECTION_STOP_MISSING),
    'protection_tp_missing': ('protection_tp_missing', STOP_CONDITION_PROTECTION_TP_MISSING),
}

_MANUAL_REVIEW_NOTE_MAP = {
    'manual_position_flat_confirmed': ('manual_position_flat_confirmed', STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED),
    'external_position_override': ('external_position_override', STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE),
    'open_orders_side_or_qty_conflict': ('manual_open_orders_side_or_qty_conflict', STOP_CONDITION_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT),
    'reduce_only_filled_but_position_not_flat': ('manual_reduce_only_position_not_flat', STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT),
    'protection_submit_gate_blocked': ('protection_submit_gate_blocked', STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED),
}


def derive_risk_action(*, recover_policy: str | None, recover_stage: str | None, stop_condition: str | None) -> str:
    if recover_policy == RECOVER_POLICY_READY_ONLY and recover_stage == RECOVER_STAGE_RECOVER_READY:
        return RISK_ACTION_RECOVER_PROTECTION
    if recover_policy == RECOVER_POLICY_READY_ONLY and recover_stage in {
        RECOVER_STAGE_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT,
        RECOVER_STAGE_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT,
        RECOVER_STAGE_MANUAL_FLAT_CONFIRMED,
        RECOVER_STAGE_EXTERNAL_POSITION_OVERRIDE,
        RECOVER_STAGE_PROTECTION_MISSING,
        RECOVER_STAGE_PROTECTION_PARTIAL_MISSING,
        RECOVER_STAGE_PROTECTION_SUBMIT_GATE_BLOCKED,
        RECOVER_STAGE_PROTECTION_SEMANTIC_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_TYPE_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
        RECOVER_STAGE_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
    }:
        return RISK_ACTION_MANUAL_REVIEW
    if recover_policy == RECOVER_POLICY_OBSERVE_ONLY:
        return RISK_ACTION_OBSERVE
    if stop_condition in MANUAL_REVIEW_STOP_CONDITIONS:
        return RISK_ACTION_MANUAL_REVIEW
    if stop_condition in FORCE_CLOSE_STOP_CONDITIONS:
        return RISK_ACTION_FORCE_CLOSE
    if recover_policy == RECOVER_POLICY_MANUAL_REVIEW:
        return RISK_ACTION_MANUAL_REVIEW
    if recover_policy == RECOVER_POLICY_KEEP_FROZEN:
        return RISK_ACTION_FORCE_CLOSE
    return RISK_ACTION_MANUAL_REVIEW


def derive_recover_policy(
    *,
    result: str,
    allowed: bool,
    recover_ready: bool,
    stop_condition: str | None,
    pending_execution_phase: str | None,
) -> str:
    if recover_ready or result == 'READY':
        return RECOVER_POLICY_READY_ONLY
    if stop_condition in OBSERVE_STOP_CONDITIONS:
        return RECOVER_POLICY_OBSERVE_ONLY
    if stop_condition in MANUAL_REVIEW_STOP_CONDITIONS:
        return RECOVER_POLICY_MANUAL_REVIEW
    if result == 'OBSERVE' or pending_execution_phase in {
        'submitted',
        'position_confirmed_pending_trades',
        'position_working_partial_fill',
        'protection_pending_confirm',
    }:
        return RECOVER_POLICY_OBSERVE_ONLY
    if stop_condition in FORCE_CLOSE_STOP_CONDITIONS:
        return RECOVER_POLICY_KEEP_FROZEN
    if not allowed:
        return RECOVER_POLICY_MANUAL_REVIEW
    return RECOVER_POLICY_MANUAL_REVIEW


def derive_recover_stage(
    *,
    result: str,
    stop_reason: str | None,
    stop_condition: str | None,
    pending_execution_phase: str | None,
    freeze_reason: str | None,
) -> str:
    if result == 'READY':
        return RECOVER_STAGE_RECOVER_READY
    if stop_reason == 'position_confirmed_pending_trades' or stop_condition == STOP_CONDITION_POSITION_FACT_CONFIRMED_BEFORE_TRADE_ROWS:
        return RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES
    if stop_reason == 'partial_position_working' or stop_condition == STOP_CONDITION_PARTIAL_FILL_POSITION_WORKING:
        return RECOVER_STAGE_POSITION_WORKING_PARTIAL_FILL
    if stop_reason == 'avg_fill_price_missing' or stop_condition == STOP_CONDITION_AVG_FILL_PRICE_MISSING_AFTER_FILLS:
        return RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES
    if stop_reason == 'position_confirmed_pending_trades' and stop_condition == STOP_CONDITION_TRADE_ROWS_MISSING_AFTER_FILL:
        return RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES
    if stop_reason == 'fee_reconciliation_pending' or stop_condition == STOP_CONDITION_FEE_RECONCILIATION_PENDING:
        return RECOVER_STAGE_POSITION_CONFIRMED_PENDING_TRADES
    if stop_reason in {'protection_pending_confirm', 'entry_confirmed_pending_protective', 'management_stop_update_pending_protective'} or stop_condition == STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING:
        return RECOVER_STAGE_PROTECTION_PENDING_CONFIRM
    if stop_reason == 'retry_budget_exhausted' or stop_condition == STOP_CONDITION_SHARED_BUDGET_EXHAUSTED:
        return RECOVER_STAGE_SHARED_BUDGET_EXHAUSTED
    if stop_reason == 'manual_position_flat_confirmed' or stop_condition == STOP_CONDITION_MANUAL_POSITION_FLAT_CONFIRMED:
        return RECOVER_STAGE_MANUAL_FLAT_CONFIRMED
    if stop_reason == 'external_position_override' or stop_condition == STOP_CONDITION_EXTERNAL_POSITION_OVERRIDE:
        return RECOVER_STAGE_EXTERNAL_POSITION_OVERRIDE
    if stop_reason == 'protection_missing' or stop_condition == STOP_CONDITION_PROTECTION_ORDERS_MISSING:
        return RECOVER_STAGE_PROTECTION_MISSING
    if stop_reason in {'protection_stop_missing', 'protection_tp_missing'} or stop_condition in {STOP_CONDITION_PROTECTION_STOP_MISSING, STOP_CONDITION_PROTECTION_TP_MISSING}:
        return RECOVER_STAGE_PROTECTION_PARTIAL_MISSING
    if stop_reason == 'protection_submit_gate_blocked' or stop_condition == STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED:
        return RECOVER_STAGE_PROTECTION_SUBMIT_GATE_BLOCKED
    if stop_condition == STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH:
        return RECOVER_STAGE_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH
    if stop_condition == STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH:
        return RECOVER_STAGE_PROTECTION_SEMANTIC_TYPE_MISMATCH
    if stop_condition == STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH:
        return RECOVER_STAGE_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH
    if stop_condition == STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH:
        return RECOVER_STAGE_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH
    if stop_reason == 'protection_semantic_mismatch' or stop_condition == STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH:
        return RECOVER_STAGE_PROTECTION_SEMANTIC_MISMATCH
    if stop_reason in {'query_failed', 'readonly_recheck_query_exception'} or stop_condition == STOP_CONDITION_READONLY_QUERY_FAILED or freeze_reason in {'readonly_recheck_query_failed', 'readonly_recheck_query_exception'}:
        return RECOVER_STAGE_READONLY_QUERY_FAILED
    if result == 'OBSERVE' or pending_execution_phase in {'submitted', 'position_working_partial_fill'}:
        return RECOVER_STAGE_OBSERVE_PENDING
    if result == 'BLOCKED':
        return RECOVER_STAGE_KEEP_FROZEN
    return pending_execution_phase or RECOVER_STAGE_KEEP_FROZEN


def classify_manual_review_from_notes(*, notes: list[str] | set[str], trade_summary: dict[str, Any] | None, semantic_stop: tuple[str, str] | None = None) -> tuple[str | None, str | None, str | None]:
    note_set = set(notes or [])
    for note, (reason, stop_condition) in _MANUAL_REVIEW_NOTE_MAP.items():
        if note in note_set:
            return reason, stop_condition, derive_recover_stage(
                result='BLOCKED',
                stop_reason=reason,
                stop_condition=stop_condition,
                pending_execution_phase='frozen',
                freeze_reason=reason,
            )
    for note, (reason, stop_condition) in _PARTIAL_PROTECTION_NOTE_MAP.items():
        if note in note_set:
            return reason, stop_condition, derive_recover_stage(
                result='BLOCKED',
                stop_reason=reason,
                stop_condition=stop_condition,
                pending_execution_phase='frozen',
                freeze_reason='protective_order_missing',
            )
    if semantic_stop is not None:
        reason, stop_condition = semantic_stop
        return reason, stop_condition, derive_recover_stage(
            result='BLOCKED',
            stop_reason=reason,
            stop_condition=stop_condition,
            pending_execution_phase='frozen',
            freeze_reason=reason,
        )
    return None, None, None


def classify_reconcile_risk(*, freeze_reason: str | None, pending_execution_phase: str | None, notes: list[str] | None = None) -> dict[str, str | None]:
    note_set = set(notes or [])
    if freeze_reason == 'management_stop_update_pending_protective' or pending_execution_phase == 'management_stop_update_pending_protective':
        stop_condition = STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING
        recover_policy = RECOVER_POLICY_OBSERVE_ONLY
        recover_stage = RECOVER_STAGE_PROTECTION_PENDING_CONFIRM
    elif note_set & {'protection_orders_missing', 'protection_stop_missing', 'protection_tp_missing'}:
        reason, stop_condition, recover_stage = classify_manual_review_from_notes(
            notes=list(note_set),
            trade_summary=None,
            semantic_stop=None,
        )
        freeze_reason = freeze_reason or reason
        recover_policy = RECOVER_POLICY_MANUAL_REVIEW
    elif freeze_reason in MANUAL_REVIEW_FREEZE_REASONS:
        reason, stop_condition, recover_stage = classify_manual_review_from_notes(
            notes=['protection_orders_missing'] if freeze_reason == 'protective_order_missing' else [freeze_reason],
            trade_summary=None,
            semantic_stop=None,
        )
        freeze_reason = freeze_reason or reason
        if stop_condition is None:
            if freeze_reason == 'protective_order_missing':
                stop_condition = STOP_CONDITION_PROTECTION_ORDERS_MISSING
            elif freeze_reason == 'local_exchange_position_presence_mismatch':
                stop_condition = STOP_CONDITION_MANUAL_OPEN_ORDERS_SIDE_OR_QTY_CONFLICT
            elif freeze_reason == 'reconcile_mismatch':
                stop_condition = STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT
            else:
                stop_condition = STOP_CONDITION_MANUAL_REDUCE_ONLY_POSITION_NOT_FLAT
            recover_stage = derive_recover_stage(
                result='BLOCKED',
                stop_reason=freeze_reason,
                stop_condition=stop_condition,
                pending_execution_phase='frozen',
                freeze_reason=freeze_reason,
            )
        recover_policy = RECOVER_POLICY_MANUAL_REVIEW
    elif freeze_reason in OBSERVE_FREEZE_REASONS:
        stop_condition = STOP_CONDITION_READONLY_QUERY_FAILED if 'query' in freeze_reason else STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS
        recover_policy = RECOVER_POLICY_OBSERVE_ONLY
        recover_stage = RECOVER_STAGE_READONLY_QUERY_FAILED if 'query' in freeze_reason else RECOVER_STAGE_OBSERVE_PENDING
    elif freeze_reason:
        stop_condition = STOP_CONDITION_UNRESOLVED_CONFIRMATION_STATE
        recover_policy = RECOVER_POLICY_KEEP_FROZEN
        recover_stage = RECOVER_STAGE_KEEP_FROZEN
    else:
        reason, derived_stop_condition, derived_stage = classify_manual_review_from_notes(
            notes=list(note_set),
            trade_summary=None,
            semantic_stop=None,
        )
        if derived_stop_condition is not None:
            freeze_reason = freeze_reason or reason
            stop_condition = derived_stop_condition
            recover_policy = RECOVER_POLICY_MANUAL_REVIEW
            recover_stage = derived_stage
        else:
            stop_condition = STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS if pending_execution_phase else STOP_CONDITION_TERMINAL_CONFIRMATION_REACHED
            recover_policy = RECOVER_POLICY_OBSERVE_ONLY if pending_execution_phase else RECOVER_POLICY_READY_ONLY
            recover_stage = RECOVER_STAGE_OBSERVE_PENDING if pending_execution_phase else RECOVER_STAGE_RECOVER_READY
    return {
        'stop_condition': stop_condition,
        'recover_policy': recover_policy,
        'recover_stage': recover_stage,
        'risk_action': derive_risk_action(
            recover_policy=recover_policy,
            recover_stage=recover_stage,
            stop_condition=stop_condition,
        ),
        'freeze_reason': freeze_reason,
    }
