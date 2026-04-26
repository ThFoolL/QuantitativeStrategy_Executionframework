from __future__ import annotations

from typing import Any

try:
    from .runtime_guard import build_recover_record
    from .unified_risk_action import (
        STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS,
        STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING,
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
        STOP_CONDITION_SHARED_BUDGET_EXHAUSTED,
        derive_recover_policy,
        derive_recover_stage,
        derive_risk_action,
    )
except ImportError:  # pragma: no cover
    from runtime_guard import build_recover_record
    from unified_risk_action import (
        STOP_CONDITION_AWAIT_MORE_EXCHANGE_FACTS,
        STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING,
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
        STOP_CONDITION_SHARED_BUDGET_EXHAUSTED,
        derive_recover_policy,
        derive_recover_stage,
        derive_risk_action,
    )


PROTECTION_FOLLOWUP_FAMILY = 'protection_followup'
EXECUTION_CONFIRM_FAMILY = 'execution_confirm'
SUBMIT_AUTO_REPAIR_FAMILY = 'submit_auto_repair'
ASYNC_STATUS_RUNNING = 'running'
ASYNC_STATUS_SUCCEEDED = 'succeeded'
ASYNC_STATUS_FAILED = 'failed'
ASYNC_STATUS_EXHAUSTED = 'exhausted'
ASYNC_STATUS_SUPERSEDED = 'superseded'
ASYNC_STATUS_CANCELLED = 'cancelled'
ASYNC_TERMINAL_TO_HISTORY_STATUSES = {
    ASYNC_STATUS_SUCCEEDED,
    ASYNC_STATUS_FAILED,
    ASYNC_STATUS_EXHAUSTED,
    ASYNC_STATUS_SUPERSEDED,
    ASYNC_STATUS_CANCELLED,
}
PROTECTION_FOLLOWUP_PHASES = {
    'entry_confirmed_pending_protective',
    'protection_pending_confirm',
    'management_stop_update_pending_protective',
}
EXECUTION_CONFIRM_PENDING_PHASES = {
    'submitted',
    'position_working_partial_fill',
    'position_confirmed_pending_trades',
    'confirmed',
}
SUBMIT_AUTO_REPAIR_PENDING_PHASES = {
    'submit_auto_repair_pending',
}
ASYNC_FAMILY_PRIORITY = {
    SUBMIT_AUTO_REPAIR_FAMILY: 3,
    PROTECTION_FOLLOWUP_FAMILY: 2,
    EXECUTION_CONFIRM_FAMILY: 1,
}
_PROTECTION_MISSING_STOP_CONDITIONS = {
    STOP_CONDITION_PROTECTION_ORDERS_MISSING,
    STOP_CONDITION_PROTECTION_STOP_MISSING,
    STOP_CONDITION_PROTECTION_TP_MISSING,
    STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED,
    STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH,
    STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH,
}
_CANCELLED_STOP_REASONS = {
    'manual_reset',
    'manual_cancel',
    'manual_cancelled',
    'cancelled',
}
_FAILED_STOP_REASONS = {
    'query_failed',
    'readonly_recheck_query_failed',
    'readonly_recheck_query_exception',
}
_FAILED_STOP_CONDITIONS = {
    STOP_CONDITION_READONLY_QUERY_FAILED,
}
_EXHAUSTED_STOP_REASONS = {
    'retry_budget_exhausted',
    'readonly_recheck_retry_budget_exhausted',
}
_EXHAUSTED_STOP_CONDITIONS = {
    STOP_CONDITION_SHARED_BUDGET_EXHAUSTED,
}
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


def build_protection_followup_async_operation(
    *,
    market_decision_ts: str | None,
    symbol: str | None,
    strategy_ts: str | None,
    state_payload: dict[str, Any] | None,
    result_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    state_payload = dict(state_payload or {})
    result_payload = dict(result_payload or {})
    state_updates = dict(result_payload.get('state_updates') or {})
    trade_summary = dict(result_payload.get('trade_summary') or {})
    intent = dict(state_updates.get('strategy_protection_intent') or state_payload.get('strategy_protection_intent') or {})

    pending_phase = state_updates.get('pending_execution_phase')
    if pending_phase is None:
        pending_phase = state_payload.get('pending_execution_phase')
    trigger_phase = pending_phase or result_payload.get('execution_phase')
    if trigger_phase not in PROTECTION_FOLLOWUP_PHASES:
        return None

    existing_operation = _find_async_operation(
        async_operations=state_payload.get('async_operations'),
        family=PROTECTION_FOLLOWUP_FAMILY,
        action_type=intent.get('pending_action') or 'protective_rebuild',
        trigger_phase=trigger_phase,
    )

    action_type = str(intent.get('pending_action') or 'protective_rebuild')
    operation_id = intent.get('operation_id') or (existing_operation or {}).get('operation_id') or _build_operation_id(
        decision_ts=market_decision_ts,
        symbol=symbol,
        action_type=action_type,
        trigger_phase=trigger_phase,
    )

    protective_order_status = state_updates.get('protective_order_status')
    if protective_order_status is None:
        protective_order_status = state_payload.get('protective_order_status')
    protective_phase_status = state_updates.get('protective_phase_status')
    if protective_phase_status is None:
        protective_phase_status = state_payload.get('protective_phase_status')

    result_execution_phase = result_payload.get('execution_phase')
    status = ASYNC_STATUS_RUNNING if trigger_phase in PROTECTION_FOLLOWUP_PHASES else ASYNC_STATUS_SUCCEEDED
    if result_execution_phase == 'confirmed' and trigger_phase != 'protection_pending_confirm':
        status = ASYNC_STATUS_SUCCEEDED

    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    budget = _build_budget(
        trade_summary=trade_summary,
        readonly_recheck=readonly_recheck,
        existing_operation=existing_operation,
        market_decision_ts=market_decision_ts,
    )
    derived_stop_reason, derived_stop_condition = _derive_operation_stop(
        trigger_phase=trigger_phase,
        result_payload=result_payload,
        trade_summary=trade_summary,
        protective_order_status=protective_order_status,
        budget=budget,
    )
    stop_reason = derived_stop_reason
    stop_condition = derived_stop_condition
    if stop_reason == 'success_protective_visible' or stop_condition == 'protective_order_visible_on_exchange':
        status = ASYNC_STATUS_SUCCEEDED
    if status == ASYNC_STATUS_SUCCEEDED:
        stop_reason = 'success_protective_visible'
        stop_condition = 'protective_order_visible_on_exchange'
    else:
        status = _derive_terminal_status(
            stop_reason=stop_reason,
            stop_condition=stop_condition,
            fallback_status=status,
        )

    latest_observation = {
        'execution_phase': result_execution_phase,
        'confirmation_status': result_payload.get('confirmation_status'),
        'confirmed_order_status': result_payload.get('confirmed_order_status'),
        'reconcile_status': result_payload.get('reconcile_status'),
        'protective_order_status': protective_order_status,
        'protective_phase_status': protective_phase_status,
        'protective_order_present': protective_order_status == 'ACTIVE',
        'pending_execution_phase': pending_phase,
        'readonly_recheck_status': readonly_recheck.get('status'),
        'stop_reason': stop_reason,
        'stop_condition': stop_condition,
    }

    if state_payload.get('exchange_position_side') in {'long', 'short'}:
        latest_observation['position_side'] = state_payload.get('exchange_position_side')
        latest_observation['position_qty'] = state_payload.get('exchange_position_qty')

    linked_refs = {
        'strategy_protection_intent_path': 'state.strategy_protection_intent',
        'confirm_context_path': 'result.trade_summary.confirm_context',
        'readonly_recheck_path': 'result.trade_summary.readonly_recheck',
        'protective_validation_path': 'result.trade_summary.protective_validation',
        'pending_execution_phase': pending_phase,
    }
    if result_payload.get('action_type'):
        linked_refs['result_action_type'] = result_payload.get('action_type')
    if action_type:
        linked_refs['intent_pending_action'] = action_type

    attempt_trace = list((existing_operation or {}).get('attempt_trace') or [])
    next_attempt = {
        'attempt_no': max(1, int(budget.get('attempts_used') or 1)),
        'attempt_ts': market_decision_ts,
        'step': _derive_attempt_step(result_payload=result_payload, trade_summary=trade_summary),
        'outcome': _derive_attempt_outcome(
            trigger_phase=trigger_phase,
            result_execution_phase=result_execution_phase,
            protective_order_status=protective_order_status,
            readonly_recheck_status=readonly_recheck.get('status'),
        ),
        'error_code': result_payload.get('error_code'),
        'note': trade_summary.get('confirmation_category') or trigger_phase,
    }
    if stop_reason is not None:
        next_attempt['stop_reason'] = stop_reason
    if stop_condition is not None:
        next_attempt['stop_condition'] = stop_condition
    if not attempt_trace or attempt_trace[-1] != next_attempt:
        attempt_trace.append(next_attempt)

    return {
        'operation_id': operation_id,
        'family': PROTECTION_FOLLOWUP_FAMILY,
        'action_type': action_type,
        'status': status,
        'symbol': symbol,
        'decision_ts': market_decision_ts,
        'strategy_ts': strategy_ts,
        'trigger_phase': trigger_phase,
        'pending_execution_phase_view': pending_phase,
        'linked_refs': linked_refs,
        'budget': budget,
        'stop_condition': {
            'success_when': 'protective_order_visible_on_exchange',
            'freeze_when': [
                'position_present_but_no_protection_after_budget_exhausted',
                'readonly_query_failed_repeatedly',
            ],
            'stop_when': ['superseded_by_flat_state', 'manual_reset'],
            'current_reason': stop_reason,
            'current_condition': stop_condition,
        },
        'latest_observation': latest_observation,
        'attempt_trace': attempt_trace[-20:],
        'stop_reason': stop_reason,
        'resolved_at': market_decision_ts if status in ASYNC_TERMINAL_TO_HISTORY_STATUSES else None,
    }


def attach_protection_followup_async_operation(
    *,
    market_decision_ts: str | None,
    symbol: str | None,
    strategy_ts: str | None,
    state_payload: dict[str, Any] | None,
    result_payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    next_state = dict(state_payload or {})
    next_result = dict(result_payload or {})
    state_updates = dict(next_result.get('state_updates') or {})
    merged_state = dict(next_state)
    merged_state.update(state_updates)

    protective_validation = dict((next_result.get('trade_summary') or {}).get('protective_validation') or next_result.get('protective_validation') or {})
    protective_visibility = dict(protective_validation.get('exchange_visibility') or {})
    exchange_visible_terminal = bool(
        protective_validation.get('ok')
        and (
            protective_visibility.get('confirmed_via_exchange_visibility')
            or protective_visibility.get('exchange_visible')
            or state_updates.get('protective_order_status') == 'ACTIVE'
            or merged_state.get('protective_order_status') == 'ACTIVE'
        )
    )
    if exchange_visible_terminal:
        state_updates.pop('pending_execution_block_reason', None)
        if state_updates.get('pending_execution_phase') in PROTECTION_FOLLOWUP_PHASES:
            state_updates['pending_execution_phase'] = None
        merged_state.update(state_updates)

    operation = build_protection_followup_async_operation(
        market_decision_ts=market_decision_ts,
        symbol=symbol,
        strategy_ts=strategy_ts,
        state_payload=merged_state,
        result_payload=next_result,
    )
    if operation is None:
        return next_state, next_result, None

    derived_pending_phase = derive_pending_execution_phase_from_async_operation(
        state_payload=merged_state,
        operation=operation,
    )
    if _is_blank_pending_phase(state_updates.get('pending_execution_phase')) and derived_pending_phase is not None:
        state_updates['pending_execution_phase'] = derived_pending_phase
        merged_state['pending_execution_phase'] = derived_pending_phase

    next_state['async_operations'] = _merge_async_operations(next_state.get('async_operations'), operation)
    state_updates['async_operations'] = next_state['async_operations']

    if _is_blank_pending_phase(next_state.get('pending_execution_phase')) and derived_pending_phase is not None:
        next_state['pending_execution_phase'] = derived_pending_phase

    if exchange_visible_terminal and operation.get('status') in ASYNC_TERMINAL_TO_HISTORY_STATUSES:
        state_updates['pending_execution_phase'] = None
        next_state['pending_execution_phase'] = None
        merged_state['pending_execution_phase'] = None
        state_updates.pop('pending_execution_block_reason', None)
        next_state.pop('pending_execution_block_reason', None)

    intent = dict(state_updates.get('strategy_protection_intent') or next_state.get('strategy_protection_intent') or {})
    intent['operation_id'] = operation['operation_id']
    if _is_blank_pending_phase(intent.get('pending_execution_phase')) and derived_pending_phase is not None:
        intent['pending_execution_phase'] = derived_pending_phase
    if exchange_visible_terminal and operation.get('status') in ASYNC_TERMINAL_TO_HISTORY_STATUSES:
        intent['pending_execution_phase'] = None
    state_updates['strategy_protection_intent'] = intent
    next_state['strategy_protection_intent'] = intent

    trade_summary = dict(next_result.get('trade_summary') or {})
    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    confirm_context = dict(trade_summary.get('confirm_context') or {})
    stop_reason = operation.get('stop_reason')
    stop_condition = _current_stop_condition(operation)
    budget = dict(operation.get('budget') or {})
    if stop_reason is not None:
        if readonly_recheck:
            readonly_recheck['stop_reason'] = stop_reason
        if confirm_context:
            confirm_context['stop_reason'] = stop_reason
    if stop_condition is not None:
        if readonly_recheck:
            readonly_recheck['stop_condition'] = stop_condition
        if confirm_context:
            confirm_context['stop_condition'] = stop_condition
    if budget:
        if readonly_recheck and not readonly_recheck.get('retry_budget'):
            readonly_recheck['retry_budget'] = budget
        if confirm_context and not confirm_context.get('retry_budget'):
            confirm_context['retry_budget'] = budget
    if readonly_recheck:
        readonly_recheck['operation_id'] = operation['operation_id']
        trade_summary['readonly_recheck'] = readonly_recheck
    if confirm_context:
        confirm_context['operation_id'] = operation['operation_id']
        trade_summary['confirm_context'] = confirm_context
    trade_summary['async_operation'] = operation
    if _is_blank_pending_phase(trade_summary.get('pending_execution_phase')) and derived_pending_phase is not None:
        trade_summary['pending_execution_phase'] = derived_pending_phase
    if exchange_visible_terminal and operation.get('status') in ASYNC_TERMINAL_TO_HISTORY_STATUSES:
        trade_summary['pending_execution_phase'] = None
    next_result['trade_summary'] = trade_summary

    recover_check = _build_protection_followup_recover_check(
        market_decision_ts=market_decision_ts,
        merged_state={**next_state, **state_updates},
        result_payload=next_result,
        operation=operation,
    )
    if recover_check is not None:
        state_updates['recover_check'] = recover_check
        next_state['recover_check'] = recover_check
        next_state['recover_timeline'] = _append_recover_timeline(next_state.get('recover_timeline'), recover_check)
        state_updates['recover_timeline'] = next_state['recover_timeline']

    next_result['state_updates'] = state_updates
    return next_state, next_result, operation


def derive_pending_execution_phase_from_async_operation(
    *,
    state_payload: dict[str, Any] | None,
    operation: dict[str, Any] | None = None,
) -> str | None:
    state_payload = dict(state_payload or {})
    candidate = dict(operation or {})
    if not candidate:
        candidate = select_primary_async_operation(state_payload.get('async_operations'))
    if not candidate:
        return None
    if candidate.get('status') != ASYNC_STATUS_RUNNING:
        return None

    pending_phase = candidate.get('pending_execution_phase_view') or candidate.get('trigger_phase')
    if candidate.get('family') == PROTECTION_FOLLOWUP_FAMILY and pending_phase in PROTECTION_FOLLOWUP_PHASES:
        return str(pending_phase)
    if candidate.get('family') == EXECUTION_CONFIRM_FAMILY and pending_phase in EXECUTION_CONFIRM_PENDING_PHASES:
        return str(pending_phase)
    if candidate.get('family') == SUBMIT_AUTO_REPAIR_FAMILY and pending_phase in SUBMIT_AUTO_REPAIR_PENDING_PHASES:
        return str(pending_phase)
    return None


def select_primary_async_operation(async_operations: dict[str, Any] | None) -> dict[str, Any]:
    payload = summarize_async_operations(async_operations)
    return dict(payload.get('primary') or {})


def summarize_async_operations(async_operations: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(async_operations or {})
    active = [dict(item) for item in list(payload.get('active') or []) if isinstance(item, dict)]
    history = [dict(item) for item in list(payload.get('history') or []) if isinstance(item, dict)]
    if not active:
        return {'active': [], 'history': history[-20:], 'primary': None, 'active_count': 0}

    normalized = []
    for item in active:
        entry = dict(item)
        drives_summary = _async_operation_can_drive_summary(entry)
        entry['drives_summary'] = drives_summary
        entry['superseded_by_operation_id'] = None
        normalized.append(entry)

    primary = _choose_primary_async_operation(normalized)
    primary_id = primary.get('operation_id') if primary else None
    normalized_active = []
    promoted_history = []
    for item in normalized:
        entry = dict(item)
        entry['is_primary'] = bool(primary_id and entry.get('operation_id') == primary_id)
        if primary_id and entry.get('operation_id') != primary_id and entry.get('drives_summary'):
            entry['superseded_by_operation_id'] = primary_id
            if entry.get('status') == ASYNC_STATUS_RUNNING:
                entry['status'] = ASYNC_STATUS_SUPERSEDED
                entry['resolved_at'] = entry.get('resolved_at') or primary.get('decision_ts') or entry.get('decision_ts')
                promoted_history.append(entry)
                continue
        normalized_active.append(entry)

    history.extend(promoted_history)
    history = _dedupe_async_operation_history(history)
    return {
        'active': normalized_active,
        'history': history[-20:],
        'primary': dict(primary) if primary else None,
        'active_count': len(normalized_active),
    }


def _choose_primary_async_operation(active_operations: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [dict(item) for item in active_operations if _async_operation_can_drive_summary(item)]
    if not candidates:
        return {}
    candidates.sort(
        key=lambda item: (
            int(ASYNC_FAMILY_PRIORITY.get(str(item.get('family') or ''), 0)),
            str(item.get('decision_ts') or ''),
            str(item.get('operation_id') or ''),
        )
    )
    return candidates[-1]


def _async_operation_can_drive_summary(operation: dict[str, Any] | None) -> bool:
    payload = dict(operation or {})
    family = str(payload.get('family') or '')
    status = str(payload.get('status') or '')
    if family not in ASYNC_FAMILY_PRIORITY:
        return False
    if status == ASYNC_STATUS_RUNNING:
        return True
    if family == SUBMIT_AUTO_REPAIR_FAMILY and status == ASYNC_STATUS_SUCCEEDED:
        return True
    if family == EXECUTION_CONFIRM_FAMILY and status == ASYNC_STATUS_SUCCEEDED:
        return True
    return False


def _find_async_operation(
    async_operations: dict[str, Any] | None,
    *,
    family: str,
    action_type: str,
    trigger_phase: str | None,
) -> dict[str, Any]:
    payload = dict(async_operations or {})
    candidates = []
    for bucket in ('active', 'history'):
        for item in list(payload.get(bucket) or []):
            if not isinstance(item, dict):
                continue
            if item.get('family') != family:
                continue
            if item.get('action_type') != action_type:
                continue
            if trigger_phase is not None and item.get('trigger_phase') != trigger_phase:
                continue
            candidates.append(dict(item))
    if not candidates:
        return {}
    candidates.sort(key=lambda item: (str(item.get('decision_ts') or ''), str(item.get('operation_id') or '')))
    return candidates[-1]


def _merge_async_operations(existing: dict[str, Any] | None, operation: dict[str, Any]) -> dict[str, Any]:
    payload = dict(existing or {})
    active = [dict(item) for item in list(payload.get('active') or []) if isinstance(item, dict)]
    history = [dict(item) for item in list(payload.get('history') or []) if isinstance(item, dict)]
    active = [item for item in active if item.get('operation_id') != operation.get('operation_id')]
    history = [item for item in history if item.get('operation_id') != operation.get('operation_id')]
    if _should_move_async_operation_to_history(operation):
        history.append(dict(operation))
    else:
        active.append(dict(operation))
    summarized = summarize_async_operations({'active': active, 'history': history})
    payload['active'] = summarized.get('active') or []
    payload['history'] = summarized.get('history') or []
    return payload


def _mark_execution_confirm_flat_superseded(
    item: dict[str, Any],
    *,
    market_decision_ts: str | None,
) -> dict[str, Any]:
    resolved = dict(item)
    resolved['status'] = ASYNC_STATUS_CANCELLED
    resolved['resolved_at'] = resolved.get('resolved_at') or market_decision_ts
    resolved['stop_reason'] = 'superseded_by_flat_state'
    stop_condition = dict(resolved.get('stop_condition') or {})
    stop_condition['current_reason'] = 'superseded_by_flat_state'
    stop_condition['current_condition'] = 'superseded_by_flat_state'
    resolved['stop_condition'] = stop_condition
    latest_observation = dict(resolved.get('latest_observation') or {})
    latest_observation['stop_reason'] = 'superseded_by_flat_state'
    latest_observation['stop_condition'] = 'superseded_by_flat_state'
    latest_observation['pending_execution_phase'] = None
    latest_observation['terminal_flat_cleanup'] = True
    resolved['latest_observation'] = latest_observation
    return resolved



def _sweep_execution_confirm_terminal_flat_cleanup(
    async_operations: dict[str, Any] | None,
    *,
    market_decision_ts: str | None,
    symbol: str | None,
) -> dict[str, Any]:
    payload = dict(async_operations or {})
    active = [dict(item) for item in list(payload.get('active') or []) if isinstance(item, dict)]
    history = [dict(item) for item in list(payload.get('history') or []) if isinstance(item, dict)]
    kept_active = []
    stale_history_ids: set[str] = set()

    for item in active:
        if (
            item.get('family') == EXECUTION_CONFIRM_FAMILY
            and (symbol is None or item.get('symbol') in {None, '', symbol})
        ):
            resolved = _mark_execution_confirm_flat_superseded(item, market_decision_ts=market_decision_ts)
            history.append(resolved)
            operation_id = str(resolved.get('operation_id') or '')
            if operation_id:
                stale_history_ids.add(operation_id)
            continue
        kept_active.append(item)

    kept_history = []
    for item in history:
        if item.get('family') != EXECUTION_CONFIRM_FAMILY:
            kept_history.append(item)
            continue
        if symbol is not None and item.get('symbol') not in {None, '', symbol}:
            kept_history.append(item)
            continue
        if item.get('stop_reason') == 'superseded_by_flat_state':
            kept_history.append(item)
            continue
        if item.get('trigger_phase') not in EXECUTION_CONFIRM_PENDING_PHASES:
            kept_history.append(item)
            continue
        operation_id = str(item.get('operation_id') or '')
        if operation_id and operation_id in stale_history_ids:
            continue
        kept_history.append(_mark_execution_confirm_flat_superseded(item, market_decision_ts=market_decision_ts))

    summarized = summarize_async_operations({'active': kept_active, 'history': kept_history})
    payload['active'] = summarized.get('active') or []
    payload['history'] = summarized.get('history') or []
    return payload


def _should_move_async_operation_to_history(operation: dict[str, Any] | None) -> bool:
    status = str(dict(operation or {}).get('status') or '')
    return status in ASYNC_TERMINAL_TO_HISTORY_STATUSES


def _dedupe_async_operation_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for item in history:
        operation_id = str(item.get('operation_id') or '')
        if not operation_id:
            continue
        deduped[operation_id] = dict(item)
    items = list(deduped.values())
    items.sort(key=lambda item: (str(item.get('resolved_at') or item.get('decision_ts') or ''), str(item.get('operation_id') or '')))
    return items


def _build_operation_id(*, decision_ts: str | None, symbol: str | None, action_type: str, trigger_phase: str | None) -> str:
    ts_token = str(decision_ts or 'unknown').replace(':', '').replace('-', '').replace('+00:00', 'Z')
    phase_token = str(trigger_phase or 'unknown').replace(' ', '_')
    return f'op_{ts_token}_{PROTECTION_FOLLOWUP_FAMILY}_{action_type}_{symbol or "UNKNOWN"}_{phase_token}'


def _derive_terminal_status(*, stop_reason: Any, stop_condition: Any, fallback_status: str) -> str:
    reason = str(stop_reason or '')
    condition = _normalize_stop_condition(stop_condition)
    if reason in _CANCELLED_STOP_REASONS or condition in _CANCELLED_STOP_REASONS:
        return ASYNC_STATUS_CANCELLED
    if reason in _EXHAUSTED_STOP_REASONS or condition in _EXHAUSTED_STOP_CONDITIONS:
        return ASYNC_STATUS_EXHAUSTED
    if reason in _FAILED_STOP_REASONS or condition in _FAILED_STOP_CONDITIONS:
        return ASYNC_STATUS_FAILED
    return fallback_status


def _build_budget(
    *,
    trade_summary: dict[str, Any],
    readonly_recheck: dict[str, Any],
    existing_operation: dict[str, Any] | None,
    market_decision_ts: str | None,
) -> dict[str, Any]:
    retry_budget = dict(readonly_recheck.get('retry_budget') or {})
    existing_budget = dict((existing_operation or {}).get('budget') or {})
    attempts_used = retry_budget.get('attempts_used')
    max_attempts = retry_budget.get('max_attempts')
    if attempts_used is None:
        attempts_used = existing_budget.get('attempts_used')
    if max_attempts is None:
        max_attempts = existing_budget.get('max_attempts')
    if attempts_used is None:
        attempts_used = 1
    if max_attempts is None:
        max_attempts = max(1, int(attempts_used or 1))
    window_seconds = retry_budget.get('retry_interval_seconds')
    if window_seconds is None:
        window_seconds = existing_budget.get('window_seconds')
    if window_seconds is None:
        window_seconds = 0
    window_started_at = (
        retry_budget.get('current_bar_ts')
        or retry_budget.get('window_started_at')
        or existing_budget.get('window_started_at')
        or market_decision_ts
    )
    budget = {
        'scope': 'operation',
        'shared_key': None,
        'max_attempts': int(max_attempts),
        'attempts_used': int(attempts_used),
        'attempts_remaining': max(0, int(max_attempts) - int(attempts_used)),
        'window_seconds': int(window_seconds),
        'window_started_at': window_started_at,
        'window_status': 'active',
        'window_last_observed_at': market_decision_ts,
        'next_earliest_retry_ts': retry_budget.get('next_earliest_retry_ts') or existing_budget.get('next_earliest_retry_ts'),
    }
    orchestration = dict(trade_summary.get('orchestration') or {})
    rebuild_result = dict(orchestration.get('rebuild_result') or {})
    if budget['attempts_used'] <= 0:
        budget['attempts_used'] = 1 if rebuild_result else 0
        budget['attempts_remaining'] = max(0, budget['max_attempts'] - budget['attempts_used'])
    if budget['max_attempts'] < budget['attempts_used']:
        budget['max_attempts'] = budget['attempts_used']
        budget['attempts_remaining'] = 0
    if budget['next_earliest_retry_ts']:
        budget['window_status'] = 'cooldown'
    return budget


def _derive_attempt_step(*, result_payload: dict[str, Any], trade_summary: dict[str, Any]) -> str:
    action_type = result_payload.get('action_type')
    orchestration = dict(trade_summary.get('orchestration') or {})
    if action_type == 'protective_rebuild' or orchestration.get('next_action') == 'protective_rebuild':
        return 'protective_rebuild_submit'
    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    if readonly_recheck:
        return 'readonly_recheck'
    return 'protective_followup_observe'


def _derive_attempt_outcome(
    *,
    trigger_phase: str | None,
    result_execution_phase: str | None,
    protective_order_status: str | None,
    readonly_recheck_status: str | None,
) -> str:
    if result_execution_phase == 'confirmed' or protective_order_status == 'ACTIVE':
        return 'success'
    if trigger_phase == 'protection_pending_confirm' or readonly_recheck_status == 'pending':
        return 'pending_confirm'
    if trigger_phase == 'management_stop_update_pending_protective':
        return 'management_rebuild_pending'
    return 'pending_submit'


def _has_negative_protective_recover_fact(
    *,
    trade_summary: dict[str, Any],
    protective_validation: dict[str, Any],
) -> bool:
    validation_level = str(protective_validation.get('validation_level') or protective_validation.get('status') or '').upper()
    if validation_level in _NEGATIVE_PROTECTIVE_VALIDATION_LEVELS:
        return True

    validation_summary = dict(protective_validation.get('summary') or {})
    if bool(validation_summary.get('submit_readback_empty')):
        return True

    protective_recover = dict(trade_summary.get('protective_recover') or {})
    remaining_risk = str(protective_recover.get('remaining_risk') or '').strip()
    if remaining_risk in _NEGATIVE_PROTECTIVE_RECOVER_RISKS:
        return True

    for attempt in list(protective_recover.get('attempts') or []):
        if str((attempt or {}).get('step') or '') == 'protective_rebuild_validate' and str((attempt or {}).get('result') or '') == 'invalid':
            return True
    return False


def _derive_operation_stop(
    *,
    trigger_phase: str | None,
    result_payload: dict[str, Any],
    trade_summary: dict[str, Any],
    protective_order_status: str | None,
    budget: dict[str, Any],
) -> tuple[str | None, str | None]:
    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    confirm_context = dict(trade_summary.get('confirm_context') or {})

    raw_stop_reason = (
        result_payload.get('stop_reason')
        or trade_summary.get('stop_reason')
        or readonly_recheck.get('stop_reason')
        or confirm_context.get('stop_reason')
    )
    raw_stop_condition = _normalize_stop_condition(
        result_payload.get('stop_condition')
        or trade_summary.get('stop_condition')
        or readonly_recheck.get('stop_condition')
        or confirm_context.get('stop_condition')
    )
    stop_reason = raw_stop_reason
    stop_condition = raw_stop_condition if raw_stop_condition in _PROTECTION_MISSING_STOP_CONDITIONS else None

    protective_validation = dict(trade_summary.get('protective_validation') or {})
    if not protective_validation:
        protective_validation = dict(result_payload.get('protective_validation') or {})
    if _has_negative_protective_recover_fact(
        trade_summary=trade_summary,
        protective_validation=protective_validation,
    ):
        return 'protective_rebuild_negative_projection', 'protective_rebuild_negative_projection'
    freeze_reason = (
        result_payload.get('freeze_reason')
        or protective_validation.get('freeze_reason')
        or readonly_recheck.get('freeze_reason')
    )
    readonly_status = readonly_recheck.get('status')
    pending_block_reason = (
        dict(result_payload.get('state_updates') or {}).get('pending_execution_block_reason')
        or trade_summary.get('pending_execution_block_reason')
    )

    if stop_reason == 'success_protective_visible':
        return stop_reason, 'protective_order_visible_on_exchange'

    if stop_condition is not None:
        if stop_reason is None:
            stop_reason = _default_stop_reason_from_condition(stop_condition)
        return stop_reason, stop_condition

    protective_visibility = dict(protective_validation.get('exchange_visibility') or {})
    if (
        protective_order_status == 'ACTIVE'
        or result_payload.get('execution_phase') == 'confirmed'
        or protective_visibility.get('confirmed_via_exchange_visibility')
        or protective_visibility.get('exchange_visible')
    ):
        return 'success_protective_visible', 'protective_order_visible_on_exchange'

    if _budget_exhausted(budget, readonly_recheck=readonly_recheck, trade_summary=trade_summary, existing_stop_reason=raw_stop_reason):
        if freeze_reason in {'readonly_recheck_query_failed', 'readonly_recheck_query_exception'} or readonly_status == 'readonly_recheck_pending':
            return 'retry_budget_exhausted', STOP_CONDITION_SHARED_BUDGET_EXHAUSTED
        mapped_reason, mapped_condition = _map_protection_freeze_reason(freeze_reason or pending_block_reason)
        if mapped_condition is not None:
            return mapped_reason or 'retry_budget_exhausted', mapped_condition
        return 'retry_budget_exhausted', STOP_CONDITION_SHARED_BUDGET_EXHAUSTED

    if freeze_reason in {'readonly_recheck_query_failed', 'readonly_recheck_query_exception'}:
        return 'query_failed', STOP_CONDITION_READONLY_QUERY_FAILED

    # While protection followup is still within budget, keep the original pending phase as
    # the primary stop reason so legacy orchestration semantics stay stable.
    if trigger_phase in PROTECTION_FOLLOWUP_PHASES:
        if trigger_phase == 'management_stop_update_pending_protective':
            return 'management_stop_update_pending_protective', STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING
        if trigger_phase == 'entry_confirmed_pending_protective':
            return 'entry_confirmed_pending_protective', STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING
        return 'protection_pending_confirm', STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING

    mapped_reason, mapped_condition = _map_protection_freeze_reason(freeze_reason or pending_block_reason)
    if mapped_condition is not None:
        return mapped_reason, mapped_condition

    return None, None


def _normalize_stop_condition(value: Any) -> str | None:
    if isinstance(value, dict):
        return str(value.get('current_condition') or '') or None
    if value in {None, '', 'none'}:
        return None
    return str(value)


def _current_stop_condition(operation: dict[str, Any]) -> str | None:
    stop_condition = operation.get('stop_condition')
    if isinstance(stop_condition, dict):
        value = stop_condition.get('current_condition')
        return None if value in {None, '', 'none'} else str(value)
    if stop_condition in {None, '', 'none'}:
        return None
    return str(stop_condition)


def _default_stop_reason_from_condition(stop_condition: str) -> str:
    mapping = {
        STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING: 'protection_pending_confirm',
        STOP_CONDITION_SHARED_BUDGET_EXHAUSTED: 'retry_budget_exhausted',
        STOP_CONDITION_READONLY_QUERY_FAILED: 'query_failed',
        STOP_CONDITION_PROTECTION_ORDERS_MISSING: 'protection_missing',
        STOP_CONDITION_PROTECTION_STOP_MISSING: 'protection_stop_missing',
        STOP_CONDITION_PROTECTION_TP_MISSING: 'protection_tp_missing',
        STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED: 'protection_submit_gate_blocked',
        STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH: 'protection_semantic_mismatch',
        STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH: 'protection_semantic_mismatch',
        STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH: 'protection_semantic_mismatch',
        STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH: 'protection_semantic_mismatch',
        STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH: 'protection_semantic_mismatch',
    }
    return mapping.get(stop_condition, stop_condition)


def _map_protection_freeze_reason(reason: Any) -> tuple[str | None, str | None]:
    if reason in {None, '', 'none'}:
        return None, None
    value = str(reason)
    mapping = {
        'protective_order_missing': ('protection_missing', STOP_CONDITION_PROTECTION_ORDERS_MISSING),
        'protection_stop_missing': ('protection_stop_missing', STOP_CONDITION_PROTECTION_STOP_MISSING),
        'protection_tp_missing': ('protection_tp_missing', STOP_CONDITION_PROTECTION_TP_MISSING),
        'protection_submit_gate_blocked': ('protection_submit_gate_blocked', STOP_CONDITION_PROTECTION_SUBMIT_GATE_BLOCKED),
        'protective_order_semantic_mismatch': ('protection_semantic_mismatch', STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH),
        'protection_semantic_mismatch': ('protection_semantic_mismatch', STOP_CONDITION_PROTECTION_SEMANTIC_MISMATCH),
        'protection_semantic_position_side_mismatch': ('protection_semantic_mismatch', STOP_CONDITION_PROTECTION_SEMANTIC_POSITION_SIDE_MISMATCH),
        'protection_semantic_type_mismatch': ('protection_semantic_mismatch', STOP_CONDITION_PROTECTION_SEMANTIC_TYPE_MISMATCH),
        'protection_semantic_stop_payload_mismatch': ('protection_semantic_mismatch', STOP_CONDITION_PROTECTION_SEMANTIC_STOP_PAYLOAD_MISMATCH),
        'protection_semantic_tp_payload_mismatch': ('protection_semantic_mismatch', STOP_CONDITION_PROTECTION_SEMANTIC_TP_PAYLOAD_MISMATCH),
    }
    return mapping.get(value, (value, None))


def _budget_exhausted(
    budget: dict[str, Any] | None,
    *,
    readonly_recheck: dict[str, Any] | None,
    trade_summary: dict[str, Any] | None,
    existing_stop_reason: Any,
) -> bool:
    payload = dict(budget or {})
    readonly_payload = dict(readonly_recheck or {})
    retry_budget = dict(readonly_payload.get('retry_budget') or {})
    explicit_budget = bool(retry_budget)
    if not explicit_budget and existing_stop_reason == 'retry_budget_exhausted':
        explicit_budget = True
    if not explicit_budget:
        orchestration = dict((trade_summary or {}).get('orchestration') or {})
        rebuild_result = dict(orchestration.get('rebuild_result') or {})
        explicit_budget = bool(rebuild_result.get('trade_summary')) and payload.get('max_attempts') is not None
    if not explicit_budget:
        return False
    attempts_remaining = payload.get('attempts_remaining')
    attempts_used = payload.get('attempts_used')
    max_attempts = payload.get('max_attempts')
    if attempts_remaining is not None and int(attempts_remaining) <= 0:
        return True
    if attempts_used is not None and max_attempts is not None and int(attempts_used) >= int(max_attempts):
        return True
    return False


def _build_protection_followup_recover_check(
    *,
    market_decision_ts: str | None,
    merged_state: dict[str, Any],
    result_payload: dict[str, Any],
    operation: dict[str, Any],
) -> dict[str, Any] | None:
    stop_reason = operation.get('stop_reason')
    stop_condition = _current_stop_condition(operation)
    pending_execution_phase = (
        dict(result_payload.get('state_updates') or {}).get('pending_execution_phase')
        or merged_state.get('pending_execution_phase')
        or operation.get('pending_execution_phase_view')
    )
    if stop_reason is None and stop_condition is None and pending_execution_phase not in PROTECTION_FOLLOWUP_PHASES:
        return None

    freeze_reason = result_payload.get('freeze_reason') or merged_state.get('freeze_reason')
    result_name = 'READY' if operation.get('status') == ASYNC_STATUS_SUCCEEDED else 'OBSERVE'
    recover_ready = result_name == 'READY'
    recover_policy = derive_recover_policy(
        result=result_name,
        allowed=recover_ready,
        recover_ready=recover_ready,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
    )
    recover_stage = derive_recover_stage(
        result=result_name,
        stop_reason=stop_reason,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
        freeze_reason=freeze_reason,
    )
    guard_decision = _initial_guard_decision(
        result=result_name,
        stop_condition=stop_condition,
        recover_policy=recover_policy,
        recover_stage=recover_stage,
    )

    trade_summary = dict(result_payload.get('trade_summary') or {})
    confirm_context = dict(trade_summary.get('confirm_context') or {})
    confirm_context.setdefault('confirm_phase', 'protection_followup')
    if stop_reason is not None:
        confirm_context['stop_reason'] = stop_reason
    if stop_condition is not None:
        confirm_context['stop_condition'] = stop_condition
    if operation.get('budget'):
        confirm_context.setdefault('retry_budget', dict(operation.get('budget') or {}))

    record = build_recover_record(
        checked_at=str(market_decision_ts or merged_state.get('state_ts') or ''),
        source='protection_followup_async_operation',
        result=result_name,
        allowed=recover_ready,
        reason=stop_reason or pending_execution_phase or 'protection_followup_async_operation',
        pending_execution_phase=pending_execution_phase,
        freeze_reason=freeze_reason,
        consistency_status=str(merged_state.get('consistency_status') or 'OK'),
        runtime_mode=str(merged_state.get('runtime_mode') or 'ACTIVE'),
        recover_ready=recover_ready,
        requires_manual_resume=True,
        guard_decision=guard_decision,
        recover_policy=recover_policy,
        recover_stage=recover_stage,
        stop_reason=stop_reason,
        stop_condition=stop_condition,
        confirm_phase='protection_followup',
        confirm_context=confirm_context,
        retry_budget=dict(operation.get('budget') or {}),
    )
    record['decision'] = result_name
    return record


def _initial_guard_decision(*, result: str, stop_condition: str | None, recover_policy: str, recover_stage: str) -> str:
    if result == 'READY':
        return 'ready_only_no_resubmit'
    if stop_condition == STOP_CONDITION_SHARED_BUDGET_EXHAUSTED:
        return 'keep_frozen_shared_budget_exhausted'
    if stop_condition == STOP_CONDITION_READONLY_QUERY_FAILED:
        return 'keep_frozen_readonly_query_failed'
    if stop_condition == STOP_CONDITION_POSITION_CONFIRMED_BUT_PROTECTION_PENDING:
        return 'keep_frozen_protection_pending_confirm'
    if stop_condition in _PROTECTION_MISSING_STOP_CONDITIONS:
        return f'keep_frozen_{stop_condition}'
    if recover_policy == 'observe_only':
        return f'keep_frozen_{recover_stage or "observe"}'
    return 'keep_frozen_manual_review'


def _append_recover_timeline(timeline: Any, record: dict[str, Any], *, limit: int = 10) -> list[dict[str, Any]]:
    items = [dict(item) for item in list(timeline or []) if isinstance(item, dict)]
    items.append(dict(record))
    return items[-max(1, int(limit)):]


def build_execution_confirm_async_operation(
    *,
    market_decision_ts: str | None,
    symbol: str | None,
    strategy_ts: str | None,
    state_payload: dict[str, Any] | None,
    result_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    state_payload = dict(state_payload or {})
    result_payload = dict(result_payload or {})
    state_updates = dict(result_payload.get('state_updates') or {})
    trade_summary = dict(result_payload.get('trade_summary') or {})
    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    confirm_context = dict(trade_summary.get('confirm_context') or {})
    posttrade_retry = dict(trade_summary.get('posttrade_retry') or {})

    pending_phase = state_updates.get('pending_execution_phase')
    result_execution_phase = result_payload.get('execution_phase')
    readonly_recheck_summary = dict(trade_summary.get('readonly_recheck') or {})
    terminal_flat_cleanup = bool(readonly_recheck_summary.get('confirmed_flat')) and result_execution_phase in {None, '', 'none'}
    if pending_phase is None and not terminal_flat_cleanup:
        pending_phase = state_payload.get('pending_execution_phase')
    trigger_phase = pending_phase or result_execution_phase
    # Terminal flat settle should keep audit in readonly_recheck/recover history and stay out of runtime async queues.
    if terminal_flat_cleanup:
        return None
    # Close-after-flat readonly recheck explicitly clears the phase; do not resurrect a stale pending phase
    # from the pre-merge state when the result has already projected a terminal flat settle.
    if pending_phase is None and result_execution_phase in {None, '', 'none'}:
        trigger_phase = None
    if trigger_phase not in EXECUTION_CONFIRM_PENDING_PHASES:
        return None

    action_type = str(confirm_context.get('confirm_phase') or readonly_recheck.get('action') or 'posttrade_confirm')
    existing_operation = _find_async_operation(
        async_operations=state_payload.get('async_operations'),
        family=EXECUTION_CONFIRM_FAMILY,
        action_type=action_type,
        trigger_phase=trigger_phase,
    )
    operation_id = (
        confirm_context.get('operation_id')
        or readonly_recheck.get('operation_id')
        or (existing_operation or {}).get('operation_id')
        or _build_operation_id(
            decision_ts=market_decision_ts,
            symbol=symbol,
            action_type=action_type,
            trigger_phase=trigger_phase,
        )
    )

    status = ASYNC_STATUS_RUNNING
    if trigger_phase == 'confirmed' and readonly_recheck.get('action') == 'recover_ready':
        status = ASYNC_STATUS_SUCCEEDED

    budget = _build_execution_confirm_budget(
        confirm_context=confirm_context,
        readonly_recheck=readonly_recheck,
        posttrade_retry=posttrade_retry,
        existing_operation=existing_operation,
        market_decision_ts=market_decision_ts,
    )
    stop_reason, stop_condition = _derive_execution_confirm_stop(
        pending_phase=trigger_phase,
        state_payload=state_payload,
        result_payload=result_payload,
        trade_summary=trade_summary,
        readonly_recheck=readonly_recheck,
        confirm_context=confirm_context,
        budget=budget,
    )
    if status == ASYNC_STATUS_SUCCEEDED:
        stop_reason = stop_reason or 'recover_ready'
        stop_condition = stop_condition or 'trades_confirmed'
    else:
        status = _derive_terminal_status(
            stop_reason=stop_reason,
            stop_condition=stop_condition,
            fallback_status=status,
        )

    latest_observation = {
        'execution_phase': result_payload.get('execution_phase'),
        'confirmation_status': result_payload.get('confirmation_status'),
        'confirmed_order_status': result_payload.get('confirmed_order_status'),
        'reconcile_status': result_payload.get('reconcile_status'),
        'pending_execution_phase': pending_phase,
        'confirmation_category': trade_summary.get('confirmation_category'),
        'readonly_recheck_status': readonly_recheck.get('status'),
        'readonly_recheck_action': readonly_recheck.get('action'),
        'stop_reason': stop_reason,
        'stop_condition': stop_condition,
        'recover_ready': bool(readonly_recheck.get('action') == 'recover_ready'),
        'query_failed': bool(trade_summary.get('query_failed')),
    }

    linked_refs = {
        'confirm_context_path': 'result.trade_summary.confirm_context',
        'readonly_recheck_path': 'result.trade_summary.readonly_recheck',
        'posttrade_retry_path': 'result.trade_summary.posttrade_retry',
        'pending_execution_phase': pending_phase,
    }

    attempt_trace = list((existing_operation or {}).get('attempt_trace') or [])
    next_attempt = {
        'attempt_no': max(1, int(budget.get('attempts_used') or 1)),
        'attempt_ts': market_decision_ts,
        'step': _derive_execution_confirm_attempt_step(readonly_recheck=readonly_recheck, confirm_context=confirm_context),
        'outcome': _derive_execution_confirm_attempt_outcome(
            pending_phase=trigger_phase,
            status=status,
            stop_reason=stop_reason,
            readonly_recheck=readonly_recheck,
        ),
        'note': trade_summary.get('confirmation_category') or trigger_phase,
    }
    if stop_reason is not None:
        next_attempt['stop_reason'] = stop_reason
    if stop_condition is not None:
        next_attempt['stop_condition'] = stop_condition
    if not attempt_trace or attempt_trace[-1] != next_attempt:
        attempt_trace.append(next_attempt)

    return {
        'operation_id': operation_id,
        'family': EXECUTION_CONFIRM_FAMILY,
        'action_type': action_type,
        'status': status,
        'symbol': symbol,
        'decision_ts': market_decision_ts,
        'strategy_ts': strategy_ts,
        'trigger_phase': trigger_phase,
        'pending_execution_phase_view': pending_phase,
        'linked_refs': linked_refs,
        'budget': budget,
        'stop_condition': {
            'success_when': 'readonly_recheck_recover_ready',
            'freeze_when': [
                'readonly_query_failed',
                'shared_budget_exhausted',
            ],
            'stop_when': ['terminal_confirmation_reached', 'manual_reset'],
            'current_reason': stop_reason,
            'current_condition': stop_condition,
        },
        'latest_observation': latest_observation,
        'attempt_trace': attempt_trace[-20:],
        'stop_reason': stop_reason,
        'resolved_at': market_decision_ts if status in ASYNC_TERMINAL_TO_HISTORY_STATUSES else None,
    }


def attach_execution_confirm_async_operation(
    *,
    market_decision_ts: str | None,
    symbol: str | None,
    strategy_ts: str | None,
    state_payload: dict[str, Any] | None,
    result_payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    next_state = dict(state_payload or {})
    next_result = dict(result_payload or {})
    state_updates = dict(next_result.get('state_updates') or {})
    merged_state = dict(next_state)
    merged_state.update(state_updates)

    operation = build_execution_confirm_async_operation(
        market_decision_ts=market_decision_ts,
        symbol=symbol,
        strategy_ts=strategy_ts,
        state_payload=merged_state,
        result_payload=next_result,
    )
    if operation is None:
        readonly_recheck = dict(dict(next_result.get('trade_summary') or {}).get('readonly_recheck') or {})
        terminal_flat_cleanup = bool(readonly_recheck.get('confirmed_flat')) and next_result.get('execution_phase') in {None, '', 'none'}
        if terminal_flat_cleanup:
            next_state['async_operations'] = _sweep_execution_confirm_terminal_flat_cleanup(
                next_state.get('async_operations'),
                market_decision_ts=market_decision_ts,
                symbol=symbol,
            )
            state_updates['async_operations'] = next_state['async_operations']
            next_result['state_updates'] = state_updates
        return next_state, next_result, None

    derived_pending_phase = derive_pending_execution_phase_from_async_operation(
        state_payload=merged_state,
        operation=operation,
    )
    if _is_blank_pending_phase(state_updates.get('pending_execution_phase')) and derived_pending_phase is not None:
        state_updates['pending_execution_phase'] = derived_pending_phase
        merged_state['pending_execution_phase'] = derived_pending_phase

    next_state['async_operations'] = _merge_async_operations(next_state.get('async_operations'), operation)
    state_updates['async_operations'] = next_state['async_operations']

    if _is_blank_pending_phase(next_state.get('pending_execution_phase')) and derived_pending_phase is not None:
        next_state['pending_execution_phase'] = derived_pending_phase

    trade_summary = dict(next_result.get('trade_summary') or {})
    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    confirm_context = dict(trade_summary.get('confirm_context') or {})
    stop_reason = operation.get('stop_reason')
    current_condition = _current_stop_condition(operation)
    budget = dict(operation.get('budget') or {})
    if readonly_recheck:
        readonly_recheck['operation_id'] = operation['operation_id']
        if budget and not readonly_recheck.get('retry_budget'):
            readonly_recheck['retry_budget'] = dict(budget)
        if stop_reason is not None:
            readonly_recheck.setdefault('stop_reason', stop_reason)
        if current_condition is not None:
            readonly_recheck.setdefault('stop_condition', current_condition)
        trade_summary['readonly_recheck'] = readonly_recheck
    if confirm_context:
        confirm_context['operation_id'] = operation['operation_id']
        if budget and not confirm_context.get('retry_budget'):
            confirm_context['retry_budget'] = dict(budget)
        if stop_reason is not None:
            confirm_context.setdefault('stop_reason', stop_reason)
        if current_condition is not None:
            confirm_context.setdefault('stop_condition', current_condition)
        trade_summary['confirm_context'] = confirm_context
    trade_summary['async_operation'] = operation
    if _is_blank_pending_phase(trade_summary.get('pending_execution_phase')) and derived_pending_phase is not None:
        trade_summary['pending_execution_phase'] = derived_pending_phase
    next_result['trade_summary'] = trade_summary

    recover_check = _build_execution_confirm_recover_check(
        market_decision_ts=market_decision_ts,
        merged_state={**next_state, **state_updates},
        result_payload=next_result,
        operation=operation,
    )
    if recover_check is not None:
        state_updates['recover_check'] = recover_check
        next_state['recover_check'] = recover_check
        next_state['recover_timeline'] = _append_recover_timeline(next_state.get('recover_timeline'), recover_check)
        state_updates['recover_timeline'] = next_state['recover_timeline']

    next_result['state_updates'] = state_updates
    return next_state, next_result, operation


def _build_execution_confirm_recover_check(
    *,
    market_decision_ts: str | None,
    merged_state: dict[str, Any],
    result_payload: dict[str, Any],
    operation: dict[str, Any],
) -> dict[str, Any] | None:
    from .runtime_guard import build_recover_record

    trade_summary = dict(result_payload.get('trade_summary') or {})
    readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
    confirm_context = dict(trade_summary.get('confirm_context') or {})
    current_condition = _current_stop_condition(operation)
    stop_reason = operation.get('stop_reason')
    budget = dict(operation.get('budget') or {})
    pending_execution_phase = (
        merged_state.get('pending_execution_phase')
        or trade_summary.get('pending_execution_phase')
        or operation.get('pending_execution_phase_view')
        or operation.get('trigger_phase')
    )
    if pending_execution_phase not in EXECUTION_CONFIRM_PENDING_PHASES:
        return None

    if readonly_recheck and not readonly_recheck.get('retry_budget') and budget:
        readonly_recheck['retry_budget'] = dict(budget)
    if confirm_context and not confirm_context.get('retry_budget') and budget:
        confirm_context['retry_budget'] = dict(budget)
    if stop_reason is not None:
        if readonly_recheck:
            readonly_recheck.setdefault('stop_reason', stop_reason)
        if confirm_context:
            confirm_context.setdefault('stop_reason', stop_reason)
    if current_condition is not None:
        if readonly_recheck:
            readonly_recheck.setdefault('stop_condition', current_condition)
        if confirm_context:
            confirm_context.setdefault('stop_condition', current_condition)
    effective_confirm_context = confirm_context or dict(readonly_recheck.get('confirm_context') or {})
    if not effective_confirm_context and readonly_recheck:
        effective_confirm_context = {
            'confirm_phase': readonly_recheck.get('confirm_phase') or 'readonly_recheck',
            'stop_reason': readonly_recheck.get('stop_reason'),
            'stop_condition': readonly_recheck.get('stop_condition'),
            'retry_budget': readonly_recheck.get('retry_budget'),
        }

    status = str(operation.get('status') or '')
    recover_ready = status == ASYNC_STATUS_SUCCEEDED or stop_reason == 'recover_ready' or readonly_recheck.get('action') == 'recover_ready'
    if recover_ready:
        result = 'READY'
        allowed = True
        reason = stop_reason or 'recover_ready'
    elif stop_reason in {'query_failed', 'retry_budget_exhausted', 'position_confirmed_pending_trades'} or pending_execution_phase in {'submitted', 'position_confirmed_pending_trades'}:
        result = 'OBSERVE'
        allowed = False
        reason = stop_reason or pending_execution_phase or 'execution_confirm_async_operation'
    else:
        return None

    return build_recover_record(
        checked_at=market_decision_ts or merged_state.get('state_ts') or result_payload.get('result_ts'),
        source='execution_confirm_async_operation',
        result=result,
        allowed=allowed,
        reason=reason,
        pending_execution_phase=pending_execution_phase,
        freeze_reason=(
            result_payload.get('freeze_reason')
            or merged_state.get('freeze_reason')
            or readonly_recheck.get('freeze_reason')
        ),
        consistency_status=merged_state.get('consistency_status') or 'OK',
        runtime_mode=merged_state.get('runtime_mode') or 'ACTIVE',
        recover_ready=recover_ready,
        requires_manual_resume=bool(recover_ready),
        stop_reason=stop_reason,
        stop_condition=current_condition,
        confirm_phase=(
            effective_confirm_context.get('confirm_phase')
            or readonly_recheck.get('confirm_phase')
            or ('readonly_recheck' if readonly_recheck else 'posttrade_confirm')
        ),
        confirm_context=effective_confirm_context,
        retry_budget=budget or effective_confirm_context.get('retry_budget'),
    )



def _build_execution_confirm_budget(
    *,
    confirm_context: dict[str, Any],
    readonly_recheck: dict[str, Any],
    posttrade_retry: dict[str, Any],
    existing_operation: dict[str, Any] | None,
    market_decision_ts: str | None,
) -> dict[str, Any]:
    retry_budget = dict(readonly_recheck.get('retry_budget') or confirm_context.get('retry_budget') or posttrade_retry.get('retry_budget') or {})
    existing_budget = dict((existing_operation or {}).get('budget') or {})
    attempts_used = retry_budget.get('attempts_used')
    max_attempts = retry_budget.get('max_attempts')
    if attempts_used is None:
        attempts_used = existing_budget.get('attempts_used')
    if max_attempts is None:
        max_attempts = existing_budget.get('max_attempts')
    if attempts_used is None:
        attempts_used = confirm_context.get('attempts_used') or 1
    if max_attempts is None:
        max_attempts = confirm_context.get('max_attempts') or max(1, int(attempts_used or 1))
    retry_interval_seconds = retry_budget.get('retry_interval_seconds')
    if retry_interval_seconds is None:
        retry_interval_seconds = confirm_context.get('retry_interval_seconds') or existing_budget.get('retry_interval_seconds') or 0
    window_started_at = (
        retry_budget.get('current_bar_ts')
        or retry_budget.get('window_started_at')
        or existing_budget.get('window_started_at')
        or market_decision_ts
    )
    budget = {
        'scope': 'execution_confirm',
        'shared_key': retry_budget.get('shared_key'),
        'max_attempts': int(max_attempts),
        'attempts_used': int(attempts_used),
        'attempts_remaining': max(0, int(max_attempts) - int(attempts_used)),
        'retry_interval_seconds': float(retry_interval_seconds),
        'window_started_at': window_started_at,
        'window_last_observed_at': market_decision_ts,
        'next_earliest_retry_ts': retry_budget.get('next_earliest_retry_ts') or existing_budget.get('next_earliest_retry_ts'),
    }
    if budget['next_earliest_retry_ts']:
        budget['window_status'] = 'cooldown'
    else:
        budget['window_status'] = 'active'
    return budget


def _derive_execution_confirm_stop(
    *,
    pending_phase: str | None,
    state_payload: dict[str, Any],
    result_payload: dict[str, Any],
    trade_summary: dict[str, Any],
    readonly_recheck: dict[str, Any],
    confirm_context: dict[str, Any],
    budget: dict[str, Any],
) -> tuple[str | None, str | None]:
    stop_reason = (
        result_payload.get('stop_reason')
        or trade_summary.get('stop_reason')
        or readonly_recheck.get('stop_reason')
        or confirm_context.get('stop_reason')
    )
    stop_condition = _normalize_stop_condition(
        result_payload.get('stop_condition')
        or trade_summary.get('stop_condition')
        or readonly_recheck.get('stop_condition')
        or confirm_context.get('stop_condition')
    )
    freeze_reason = result_payload.get('freeze_reason') or state_payload.get('freeze_reason') or readonly_recheck.get('freeze_reason')

    if stop_reason == 'recover_ready' or readonly_recheck.get('action') == 'recover_ready':
        return 'recover_ready', stop_condition or 'trades_confirmed'
    if pending_phase == 'confirmed':
        return 'recover_ready', stop_condition or 'trades_confirmed'
    if pending_phase == 'protection_pending_confirm' or stop_reason == 'protection_pending_confirm':
        return 'protection_pending_confirm', stop_condition or 'position_confirmed_but_protection_pending'
    if stop_reason == 'position_confirmed_pending_trades' or pending_phase == 'position_confirmed_pending_trades':
        return 'position_confirmed_pending_trades', stop_condition or 'position_fact_confirmed_before_trade_rows'
    if stop_reason == 'query_failed' or freeze_reason in {'readonly_recheck_query_failed', 'readonly_recheck_query_exception'}:
        return 'query_failed', stop_condition or STOP_CONDITION_READONLY_QUERY_FAILED
    if pending_phase == 'submitted':
        if _budget_exhausted(budget, readonly_recheck=readonly_recheck, trade_summary=trade_summary, existing_stop_reason=stop_reason):
            return 'retry_budget_exhausted', STOP_CONDITION_SHARED_BUDGET_EXHAUSTED
    if pending_phase in EXECUTION_CONFIRM_PENDING_PHASES:
        return str(pending_phase), stop_condition
    return stop_reason, stop_condition


def _derive_execution_confirm_attempt_step(*, readonly_recheck: dict[str, Any], confirm_context: dict[str, Any]) -> str:
    if readonly_recheck:
        return 'readonly_recheck'
    return str(confirm_context.get('confirm_phase') or 'posttrade_confirm')


def _derive_execution_confirm_attempt_outcome(
    *,
    pending_phase: str | None,
    status: str,
    stop_reason: str | None,
    readonly_recheck: dict[str, Any],
) -> str:
    if status == ASYNC_STATUS_SUCCEEDED:
        return 'recover_ready'
    if stop_reason == 'query_failed' or readonly_recheck.get('reason') == 'readonly_recheck_query_failed':
        return 'query_failed'
    if pending_phase == 'position_confirmed_pending_trades':
        return 'position_confirmed_pending_trades'
    return str(pending_phase or stop_reason or 'pending')


def _is_blank_pending_phase(value: Any) -> bool:
    return value in {None, '', 'none'}


def build_submit_auto_repair_async_operation(
    *,
    market_decision_ts: str | None,
    symbol: str | None,
    strategy_ts: str | None,
    state_payload: dict[str, Any] | None,
    result_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    state_payload = dict(state_payload or {})
    result_payload = dict(result_payload or {})
    state_updates = dict(result_payload.get('state_updates') or {})
    trade_summary = dict(result_payload.get('trade_summary') or {})
    auto_repair = dict(trade_summary.get('auto_repair') or result_payload.get('auto_repair') or {})
    if not auto_repair:
        return None

    pending_phase = state_updates.get('pending_execution_phase')
    if pending_phase is None:
        pending_phase = state_payload.get('pending_execution_phase')
    trigger_phase = pending_phase or 'submit_auto_repair_pending'
    if trigger_phase not in SUBMIT_AUTO_REPAIR_PENDING_PHASES:
        return None

    action_type = str(auto_repair.get('action_type') or auto_repair.get('source_action') or 'reduce_only_conflict_repair')
    existing_operation = _find_async_operation(
        async_operations=state_payload.get('async_operations'),
        family=SUBMIT_AUTO_REPAIR_FAMILY,
        action_type=action_type,
        trigger_phase=trigger_phase,
    )
    operation_id = (
        auto_repair.get('operation_id')
        or (existing_operation or {}).get('operation_id')
        or _build_operation_id(
            decision_ts=market_decision_ts,
            symbol=symbol,
            action_type=action_type,
            trigger_phase=trigger_phase,
        )
    )

    stop_reason = auto_repair.get('stop_reason')
    stop_condition = _normalize_stop_condition(auto_repair.get('stop_condition'))
    if stop_reason is None:
        if auto_repair.get('attempted') and auto_repair.get('retry_submitted'):
            stop_reason = 'submit_auto_repair_retry_submitted'
        elif auto_repair.get('blocked_reason'):
            stop_reason = str(auto_repair.get('blocked_reason'))
        else:
            stop_reason = 'submit_auto_repair_pending'
    if stop_condition is None:
        if auto_repair.get('attempted') and auto_repair.get('retry_submitted'):
            stop_condition = 'retry_submit_dispatched'
        elif auto_repair.get('blocked_reason'):
            stop_condition = str(auto_repair.get('blocked_reason'))
        else:
            stop_condition = 'await_repair_retry'

    status = ASYNC_STATUS_SUCCEEDED if auto_repair.get('attempted') and auto_repair.get('retry_submitted') else ASYNC_STATUS_RUNNING

    existing_budget = dict((existing_operation or {}).get('budget') or {})
    attempt_no = int(auto_repair.get('attempt_no') or existing_budget.get('attempts_used') or 1)
    max_attempts = int(auto_repair.get('max_attempts') or existing_budget.get('max_attempts') or 1)
    budget = {
        'scope': 'submit_auto_repair',
        'shared_key': auto_repair.get('shared_key'),
        'max_attempts': max_attempts,
        'attempts_used': attempt_no,
        'attempts_remaining': max(0, max_attempts - attempt_no),
        'window_seconds': int(auto_repair.get('window_seconds') or existing_budget.get('window_seconds') or 0),
        'window_started_at': auto_repair.get('window_started_at') or existing_budget.get('window_started_at') or market_decision_ts,
        'window_last_observed_at': market_decision_ts,
        'next_earliest_retry_ts': auto_repair.get('next_earliest_retry_ts') or existing_budget.get('next_earliest_retry_ts'),
        'repair_once': bool(auto_repair.get('repair_once', True)),
    }
    status = _derive_terminal_status(
        stop_reason=stop_reason,
        stop_condition=stop_condition,
        fallback_status=status,
    )

    latest_observation = {
        'execution_phase': result_payload.get('execution_phase'),
        'confirmation_status': result_payload.get('confirmation_status'),
        'submit_exception_category': auto_repair.get('submit_exception_category'),
        'error_code': auto_repair.get('error_code'),
        'repair_kind': auto_repair.get('repair_kind'),
        'repair_target': auto_repair.get('repair_target'),
        'blocked_reason': auto_repair.get('blocked_reason'),
        'retry_submitted': bool(auto_repair.get('retry_submitted')),
        'retry_client_order_id': auto_repair.get('retry_client_order_id'),
        'stop_reason': stop_reason,
        'stop_condition': stop_condition,
    }

    linked_refs = {
        'submit_exception_context_path': 'result.trade_summary.submit_exception_context',
        'auto_repair_path': 'result.trade_summary.auto_repair',
        'pending_execution_phase': trigger_phase,
    }
    if auto_repair.get('request_client_order_id'):
        linked_refs['request_client_order_id'] = auto_repair.get('request_client_order_id')
    if auto_repair.get('retry_client_order_id'):
        linked_refs['retry_client_order_id'] = auto_repair.get('retry_client_order_id')

    attempt_trace = list((existing_operation or {}).get('attempt_trace') or [])
    next_attempt = {
        'attempt_no': attempt_no,
        'attempt_ts': market_decision_ts,
        'step': str(auto_repair.get('step') or 'cancel_conflicting_reduce_only_orders_then_retry_once'),
        'outcome': 'retry_submitted' if auto_repair.get('retry_submitted') else ('blocked' if auto_repair.get('blocked_reason') else 'pending'),
        'error_code': auto_repair.get('error_code'),
        'note': auto_repair.get('repair_kind') or action_type,
        'stop_reason': stop_reason,
        'stop_condition': stop_condition,
    }
    if not attempt_trace or attempt_trace[-1] != next_attempt:
        attempt_trace.append(next_attempt)

    return {
        'operation_id': operation_id,
        'family': SUBMIT_AUTO_REPAIR_FAMILY,
        'action_type': action_type,
        'status': status,
        'symbol': symbol,
        'decision_ts': market_decision_ts,
        'strategy_ts': strategy_ts,
        'trigger_phase': trigger_phase,
        'pending_execution_phase_view': trigger_phase,
        'linked_refs': linked_refs,
        'budget': budget,
        'stop_condition': {
            'success_when': 'retry_submit_dispatched',
            'freeze_when': ['repair_precheck_not_satisfied', 'repair_retry_failed'],
            'stop_when': ['manual_reset', 'superseded_by_flat_state'],
            'current_reason': stop_reason,
            'current_condition': stop_condition,
        },
        'latest_observation': latest_observation,
        'attempt_trace': attempt_trace[-20:],
        'stop_reason': stop_reason,
        'resolved_at': market_decision_ts if status in ASYNC_TERMINAL_TO_HISTORY_STATUSES else None,
    }


def attach_submit_auto_repair_async_operation(
    *,
    market_decision_ts: str | None,
    symbol: str | None,
    strategy_ts: str | None,
    state_payload: dict[str, Any] | None,
    result_payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    next_state = dict(state_payload or {})
    next_result = dict(result_payload or {})
    state_updates = dict(next_result.get('state_updates') or {})
    merged_state = dict(next_state)
    merged_state.update(state_updates)

    operation = build_submit_auto_repair_async_operation(
        market_decision_ts=market_decision_ts,
        symbol=symbol,
        strategy_ts=strategy_ts,
        state_payload=merged_state,
        result_payload=next_result,
    )
    if operation is None:
        return next_state, next_result, None

    derived_pending_phase = derive_pending_execution_phase_from_async_operation(
        state_payload=merged_state,
        operation=operation,
    )
    if _is_blank_pending_phase(state_updates.get('pending_execution_phase')) and derived_pending_phase is not None:
        state_updates['pending_execution_phase'] = derived_pending_phase
        merged_state['pending_execution_phase'] = derived_pending_phase

    next_state['async_operations'] = _merge_async_operations(next_state.get('async_operations'), operation)
    state_updates['async_operations'] = next_state['async_operations']
    if _is_blank_pending_phase(next_state.get('pending_execution_phase')) and derived_pending_phase is not None:
        next_state['pending_execution_phase'] = derived_pending_phase

    trade_summary = dict(next_result.get('trade_summary') or {})
    auto_repair = dict(trade_summary.get('auto_repair') or next_result.get('auto_repair') or {})
    auto_repair['operation_id'] = operation['operation_id']
    auto_repair.setdefault('stop_reason', operation.get('stop_reason'))
    current_condition = _current_stop_condition(operation)
    if current_condition is not None:
        auto_repair.setdefault('stop_condition', current_condition)
    auto_repair.setdefault('retry_budget', dict(operation.get('budget') or {}))
    trade_summary['auto_repair'] = auto_repair
    trade_summary['async_operation'] = operation
    if _is_blank_pending_phase(trade_summary.get('pending_execution_phase')) and derived_pending_phase is not None:
        trade_summary['pending_execution_phase'] = derived_pending_phase

    recover_check = _build_submit_auto_repair_recover_check(
        market_decision_ts=market_decision_ts,
        merged_state={**next_state, **state_updates},
        result_payload={**next_result, 'trade_summary': trade_summary},
        operation=operation,
    )
    if recover_check is not None:
        state_updates['recover_check'] = recover_check
        next_state['recover_check'] = recover_check
        next_state['recover_timeline'] = _append_recover_timeline(next_state.get('recover_timeline'), recover_check)
        state_updates['recover_timeline'] = next_state['recover_timeline']

    next_result['trade_summary'] = trade_summary
    next_result['state_updates'] = state_updates
    return next_state, next_result, operation


def _build_submit_auto_repair_recover_check(
    *,
    market_decision_ts: str | None,
    merged_state: dict[str, Any],
    result_payload: dict[str, Any],
    operation: dict[str, Any],
) -> dict[str, Any] | None:
    stop_reason = operation.get('stop_reason')
    stop_condition = _current_stop_condition(operation)
    pending_execution_phase = (
        merged_state.get('pending_execution_phase')
        or dict(result_payload.get('trade_summary') or {}).get('pending_execution_phase')
        or operation.get('pending_execution_phase_view')
    )
    if pending_execution_phase not in SUBMIT_AUTO_REPAIR_PENDING_PHASES:
        return None

    result_name = 'READY' if operation.get('status') == ASYNC_STATUS_SUCCEEDED else 'OBSERVE'
    recover_ready = result_name == 'READY'
    recover_policy = derive_recover_policy(
        result=result_name,
        allowed=recover_ready,
        recover_ready=recover_ready,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
    )
    recover_stage = derive_recover_stage(
        result=result_name,
        stop_reason=stop_reason,
        stop_condition=stop_condition,
        pending_execution_phase=pending_execution_phase,
        freeze_reason=result_payload.get('freeze_reason') or merged_state.get('freeze_reason'),
    )
    guard_decision = 'ready_only_no_resubmit' if recover_ready else f'keep_frozen_{recover_stage or "submit_auto_repair"}'

    return build_recover_record(
        checked_at=str(market_decision_ts or merged_state.get('state_ts') or ''),
        source='submit_auto_repair_async_operation',
        result=result_name,
        allowed=recover_ready,
        reason=stop_reason or pending_execution_phase or 'submit_auto_repair_async_operation',
        pending_execution_phase=pending_execution_phase,
        freeze_reason=result_payload.get('freeze_reason') or merged_state.get('freeze_reason'),
        consistency_status=str(merged_state.get('consistency_status') or 'OK'),
        runtime_mode=str(merged_state.get('runtime_mode') or 'ACTIVE'),
        recover_ready=recover_ready,
        requires_manual_resume=True,
        guard_decision=guard_decision,
        recover_policy=recover_policy,
        recover_stage=recover_stage,
        stop_reason=stop_reason,
        stop_condition=stop_condition,
        confirm_phase='submit_auto_repair',
        confirm_context=dict(result_payload.get('trade_summary') or {}),
        retry_budget=dict(operation.get('budget') or {}),
    )
