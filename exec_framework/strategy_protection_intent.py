from __future__ import annotations

from typing import Any


PROTECTION_INTENT_IDLE = 'idle'
PROTECTION_INTENT_FROZEN = 'frozen'
PROTECTION_INTENT_EXPECTED = 'expected'
PROTECTION_INTENT_ACTIVE = 'active'
PROTECTION_INTENT_SUBMITTED = 'submitted'
PROTECTION_INTENT_MISMATCH = 'mismatch'
PROTECTION_INTENT_REBUILDING = 'rebuilding'
PROTECTION_INTENT_BLOCKED = 'blocked'


def derive_protective_rebuild_block_reason(state_payload: dict[str, Any]) -> str:
    position_side = state_payload.get('exchange_position_side')
    position_qty = float(state_payload.get('exchange_position_qty') or 0.0)
    active_strategy = state_payload.get('active_strategy')
    stop_price = state_payload.get('stop_price')
    if position_side not in {'long', 'short'}:
        return 'protective_rebuild_missing_exchange_position_side'
    if position_qty <= 0.0:
        return 'protective_rebuild_missing_exchange_position_qty'
    if active_strategy in {None, 'none'}:
        return 'protective_rebuild_missing_active_strategy'
    if stop_price is None:
        return 'protective_rebuild_missing_stop_price'
    return 'protective_rebuild_blocked_unknown'


def _expected_protection(*, position_side: str | None, position_qty: float, active_strategy: Any, stop_price: Any) -> bool:
    return bool(
        position_side in {'long', 'short'}
        and float(position_qty or 0.0) > 0.0
        and active_strategy not in {None, 'none'}
        and stop_price is not None
    )


def build_strategy_protection_intent(
    *,
    runtime_mode: str | None = None,
    position_side: str | None = None,
    position_qty: float | None = None,
    active_strategy: Any = None,
    stop_price: Any = None,
    tp_price: Any = None,
    pending_execution_phase: str | None = None,
    pending_execution_block_reason: str | None = None,
    protective_order_status: str | None = None,
    protective_phase_status: str | None = None,
    protective_orders: list[dict[str, Any]] | None = None,
    protective_validation: dict[str, Any] | None = None,
    confirmation_category: str | None = None,
    freeze_reason: str | None = None,
    last_eval_ts: str | None = None,
    orchestration_entry_pending_protective: str = 'entry_confirmed_pending_protective',
    orchestration_management_pending_protective: str = 'management_stop_update_pending_protective',
) -> dict[str, Any]:
    runtime_mode = runtime_mode or 'ACTIVE'
    position_qty = float(position_qty or 0.0)
    protective_order_status = protective_order_status or 'NONE'
    protective_phase_status = protective_phase_status or 'NONE'
    protective_orders = list(protective_orders or [])
    protective_validation = dict(protective_validation or {})
    block_reason = pending_execution_block_reason

    expected_protection = _expected_protection(
        position_side=position_side,
        position_qty=position_qty,
        active_strategy=active_strategy,
        stop_price=stop_price,
    )

    intent_state = PROTECTION_INTENT_IDLE
    lifecycle_status = PROTECTION_INTENT_IDLE
    pending_action = None

    validation_status = protective_validation.get('status')
    validation_level = protective_validation.get('validation_level')
    validation_ok = protective_validation.get('ok', True)
    exchange_visibility = dict(protective_validation.get('exchange_visibility') or {})
    exchange_visible_confirmed = bool(
        exchange_visibility.get('confirmed_via_exchange_visibility')
        or exchange_visibility.get('exchange_visible')
        or protective_orders
    )
    pending_confirm = bool(protective_validation.get('pending_confirm'))
    positive_protective_fact = bool(exchange_visible_confirmed and validation_ok)
    missing_detected = bool(
        not positive_protective_fact
        and not pending_confirm
        and (
            validation_level == 'MISSING'
            or validation_status == 'MISSING'
            or protective_order_status == 'MISSING'
        )
    )
    missing_rebuildable = missing_detected and confirmation_category not in {'pending', 'query_failed'}
    mismatch_detected = (
        confirmation_category == 'mismatch'
        or validation_level in {'STRUCTURAL_MISMATCH', 'SEMANTIC_MISMATCH'}
        or (
            validation_status == 'MISMATCH'
            and validation_level not in {'MISSING'}
        )
        or protective_order_status == 'UNEXPECTED_WHILE_FLAT'
    )

    if runtime_mode == 'FROZEN':
        if mismatch_detected:
            pending_action = 'manual_review'
            intent_state = PROTECTION_INTENT_MISMATCH
            lifecycle_status = PROTECTION_INTENT_MISMATCH
        elif expected_protection and missing_rebuildable:
            pending_action = 'protective_rebuild'
            intent_state = PROTECTION_INTENT_EXPECTED
            lifecycle_status = PROTECTION_INTENT_EXPECTED
        else:
            pending_action = 'manual_review'
            intent_state = PROTECTION_INTENT_FROZEN
            lifecycle_status = PROTECTION_INTENT_FROZEN
    elif protective_phase_status == 'DEFERRED' or protective_validation.get('phase_deferred'):
        pending_action = 'protective_rebuild'
        intent_state = PROTECTION_INTENT_EXPECTED
        lifecycle_status = PROTECTION_INTENT_EXPECTED
    elif pending_execution_phase in {
        orchestration_entry_pending_protective,
        orchestration_management_pending_protective,
    }:
        pending_action = 'protective_rebuild'
        if expected_protection:
            intent_state = PROTECTION_INTENT_EXPECTED
            lifecycle_status = PROTECTION_INTENT_EXPECTED
        else:
            intent_state = PROTECTION_INTENT_BLOCKED
            lifecycle_status = PROTECTION_INTENT_BLOCKED
            block_reason = block_reason or derive_protective_rebuild_block_reason(
                {
                    'exchange_position_side': position_side,
                    'exchange_position_qty': position_qty,
                    'active_strategy': active_strategy,
                    'stop_price': stop_price,
                }
            )
    elif protective_phase_status in {'REBUILDING', 'SUBMITTING'} or pending_execution_phase in {'protective_rebuilding', 'protection_submitting'}:
        pending_action = 'protective_rebuild'
        intent_state = PROTECTION_INTENT_REBUILDING
        lifecycle_status = PROTECTION_INTENT_REBUILDING
    elif expected_protection and exchange_visible_confirmed and validation_ok:
        intent_state = PROTECTION_INTENT_ACTIVE
        lifecycle_status = PROTECTION_INTENT_ACTIVE
    elif pending_execution_phase in {'submitted', 'position_confirmed_pending_trades', 'protection_pending_confirm'} or protective_phase_status == 'PENDING_CONFIRM' or (protective_order_status == 'PENDING_SUBMIT' and not positive_protective_fact):
        pending_action = 'protective_confirm'
        intent_state = PROTECTION_INTENT_SUBMITTED
        lifecycle_status = PROTECTION_INTENT_SUBMITTED
    elif mismatch_detected:
        pending_action = 'manual_review'
        intent_state = PROTECTION_INTENT_MISMATCH
        lifecycle_status = PROTECTION_INTENT_MISMATCH
    elif expected_protection and (protective_order_status == 'ACTIVE' or (protective_orders and validation_ok)):
        intent_state = PROTECTION_INTENT_ACTIVE
        lifecycle_status = PROTECTION_INTENT_ACTIVE
    elif expected_protection and protective_order_status == 'MISSING':
        pending_action = 'protective_rebuild'
        intent_state = PROTECTION_INTENT_EXPECTED
        lifecycle_status = PROTECTION_INTENT_EXPECTED
    elif expected_protection:
        pending_action = 'protective_rebuild'
        intent_state = PROTECTION_INTENT_EXPECTED
        lifecycle_status = PROTECTION_INTENT_EXPECTED

    return {
        'intent_status': intent_state.upper(),
        'intent_state': intent_state,
        'lifecycle_status': lifecycle_status,
        'pending_action': pending_action,
        'strategy': active_strategy,
        'position_side': position_side,
        'position_qty': position_qty,
        'stop_price': stop_price,
        'tp_price': tp_price,
        'pending_execution_phase': pending_execution_phase,
        'protective_order_status': protective_order_status,
        'protective_phase_status': protective_phase_status,
        'expected_protection': expected_protection,
        'exchange_order_count': len(protective_orders),
        'validation_status': validation_status,
        'validation_level': validation_level,
        'risk_class': protective_validation.get('risk_class'),
        'mismatch_class': protective_validation.get('mismatch_class'),
        'freeze_reason': freeze_reason,
        'block_reason': block_reason,
        'last_eval_ts': last_eval_ts,
    }
