from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .binance_readonly import AccountSnapshot, OrderSnapshot, PositionSnapshot
from .models import ExecutionResult, LiveStateSnapshot
from .state_store import FLAT_READY_PENDING_PHASE
from .protective_orders import split_open_orders, validate_protective_orders
from .unified_risk_action import (
    STOP_CONDITION_PROTECTION_STOP_MISSING,
    STOP_CONDITION_PROTECTION_TP_MISSING,
    classify_reconcile_risk,
)

RECONCILE_OK = 'OK'
RECONCILE_MISMATCH = 'MISMATCH'
RECONCILE_PENDING_ORDER = 'PENDING_ORDER'
RECONCILE_FREEZE = 'FREEZE'

PENDING_ORDER_STATUSES = {
    'NEW',
    'PARTIALLY_FILLED',
    'PENDING_CANCEL',
    'ACCEPTED',
    'CALCULATED',
}


@dataclass(frozen=True)
class ReconcileDecision:
    status: str
    freeze_reason: str | None
    can_open_new_position: bool
    can_modify_position: bool
    local_position_side: str | None
    local_position_qty: float
    exchange_position_side: str | None
    exchange_position_qty: float
    risk_action: str | None = None
    recover_policy: str | None = None
    recover_stage: str | None = None
    stop_condition: str | None = None
    open_order_ids: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExchangeSnapshot:
    account: AccountSnapshot
    position: PositionSnapshot
    open_orders: list[OrderSnapshot]


@dataclass(frozen=True)
class ReconcileInput:
    state: LiveStateSnapshot
    exchange: ExchangeSnapshot
    last_result: ExecutionResult | None = None
    qty_tolerance: float = 1e-9


def _local_position_semantics(state: LiveStateSnapshot) -> tuple[str | None, float]:
    confirmed_exchange_side = state.exchange_position_side if state.exchange_position_side in {'long', 'short'} else None
    confirmed_exchange_qty = float(state.exchange_position_qty or 0.0)

    in_confirmed_pending_window = bool(
        confirmed_exchange_side is not None
        and confirmed_exchange_qty > 0.0
        and (
            state.position_confirmation_level in {'POSITION_CONFIRMED', 'TRADES_CONFIRMED'}
            or bool(state.needs_trade_reconciliation)
            or state.pending_execution_phase in {
                'confirmed',
                'position_confirmed_pending_trades',
                'protection_pending_confirm',
                'entry_confirmed_pending_protective',
                'management_stop_update_pending_protective',
            }
        )
    )

    # open 后短窗口里，position fact 可能先于下一次 account snapshot 持久化到本地状态。
    # 这时优先沿用已确认的本地 exchange fact，避免拿 active_side/base_quantity 与临时 0 qty 快照直接对撞成 freeze。
    if in_confirmed_pending_window:
        return confirmed_exchange_side, confirmed_exchange_qty

    if state.active_side is not None:
        for value in (state.base_quantity, state.exchange_position_qty):
            if value is not None and float(value) > 0:
                return state.active_side, float(value)
        return state.active_side, 0.0

    # 允许在“仓位已确认但本地策略位尚未来得及完全写回”时，用本地保存的 exchange facts 兜底，避免把真实已持仓误判成 presence mismatch。
    if state.position_confirmation_level in {'POSITION_CONFIRMED', 'TRADES_CONFIRMED'} and confirmed_exchange_side is not None and confirmed_exchange_qty > 0.0:
        return confirmed_exchange_side, confirmed_exchange_qty
    if state.pending_execution_phase in {
        'confirmed',
        'position_confirmed_pending_trades',
        'protection_pending_confirm',
        'entry_confirmed_pending_protective',
        'management_stop_update_pending_protective',
        'none',
        None,
    } and confirmed_exchange_side is not None and confirmed_exchange_qty > 0.0:
        return confirmed_exchange_side, confirmed_exchange_qty
    return None, 0.0


def _has_pending_orders(open_orders: Iterable[OrderSnapshot]) -> tuple[bool, list[str]]:
    pending_ids: list[str] = []
    for order in open_orders:
        status = str(order.status or 'UNKNOWN').upper()
        if status in PENDING_ORDER_STATUSES:
            pending_ids.append(order.order_id)
    return bool(pending_ids), pending_ids


def _protective_validation_for_state(
    state: LiveStateSnapshot,
    exchange_side: str | None,
    exchange_qty: float,
    open_orders: Iterable[OrderSnapshot],
) -> tuple[bool, str | None, list[str]]:
    protective_validation = validate_protective_orders(
        strategy=(state.active_strategy if state.active_strategy != 'none' else None),
        position_side=exchange_side,
        position_qty=exchange_qty,
        stop_price=state.stop_price,
        tp_price=state.tp_price,
        open_orders=open_orders,
    )
    return protective_validation.ok, protective_validation.freeze_reason, protective_validation.notes


def reconcile_pre_run(payload: ReconcileInput) -> ReconcileDecision:
    state = payload.state
    exchange = payload.exchange
    exchange_side = exchange.position.side
    exchange_qty = float(exchange.position.qty)
    local_side, local_qty = _local_position_semantics(state)

    notes: list[str] = []
    partial_protective_missing_reason: str | None = None

    def _decision(*, status: str, freeze_reason: str | None, can_open_new_position: bool, can_modify_position: bool, open_order_ids: list[str]) -> ReconcileDecision:
        risk = classify_reconcile_risk(
            freeze_reason=freeze_reason,
            pending_execution_phase=state.pending_execution_phase,
            notes=notes,
        )
        return ReconcileDecision(
            status=status,
            freeze_reason=risk['freeze_reason'],
            can_open_new_position=can_open_new_position,
            can_modify_position=can_modify_position,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            risk_action=risk['risk_action'],
            recover_policy=risk['recover_policy'],
            recover_stage=risk['recover_stage'],
            stop_condition=risk['stop_condition'],
            open_order_ids=open_order_ids,
            notes=notes,
        )

    account_invalid_reasons = list(getattr(exchange.account, 'invalid_reasons', ()) or [])
    if account_invalid_reasons:
        notes.extend(account_invalid_reasons)
        notes.append('account_snapshot_invalid')
        return _decision(
            status=RECONCILE_MISMATCH,
            freeze_reason='readonly_account_snapshot_invalid',
            can_open_new_position=False,
            can_modify_position=False,
            open_order_ids=[],
        )
    protective_open_orders, regular_open_orders = split_open_orders(exchange.open_orders)
    pending_orders, pending_ids = _has_pending_orders(regular_open_orders)
    if pending_orders:
        notes.append('open_orders_present')

    management_stop_update_pending = state.pending_execution_phase == 'management_stop_update_pending_protective'
    management_pending_with_visible_protective = bool(
        management_stop_update_pending
        and exchange_side in {'long', 'short'}
        and exchange_qty > payload.qty_tolerance
        and any(
            bool(getattr(order, 'close_position', None))
            and str(getattr(order, 'status', None) or '').upper() in PENDING_ORDER_STATUSES
            for order in protective_open_orders
        )
    )
    management_pending_without_visible_protective = bool(
        management_stop_update_pending
        and exchange_side in {'long', 'short'}
        and exchange_qty > payload.qty_tolerance
        and not protective_open_orders
    )
    protective_ok, protective_freeze_reason, protective_notes = _protective_validation_for_state(
        state,
        exchange_side,
        exchange_qty,
        protective_open_orders,
    )
    notes.extend(protective_notes)
    if (
        not management_stop_update_pending
        and exchange_side in {'long', 'short'}
        and exchange_qty > payload.qty_tolerance
        and len(protective_open_orders) == 1
    ):
        only_order = protective_open_orders[0]
        if bool(getattr(only_order, 'close_position', None)) and str(getattr(only_order, 'status', None) or '').upper() in PENDING_ORDER_STATUSES:
            partial_protective_missing_reason = STOP_CONDITION_PROTECTION_TP_MISSING
    if management_pending_with_visible_protective and not protective_ok:
        notes.append('management_stop_update_exchange_protective_visible')
        protective_ok = True
        protective_freeze_reason = None
    elif management_pending_without_visible_protective and not protective_ok and protective_freeze_reason == 'protective_order_missing':
        notes.append('management_stop_update_protective_refresh_gap_observed')
        protective_ok = True
        protective_freeze_reason = None
    elif not management_stop_update_pending and protective_freeze_reason in {
        STOP_CONDITION_PROTECTION_STOP_MISSING,
        STOP_CONDITION_PROTECTION_TP_MISSING,
    }:
        partial_protective_missing_reason = protective_freeze_reason
        notes.append(protective_freeze_reason)
        protective_ok = True
        protective_freeze_reason = None

    if state.freeze_reason and state.consistency_status == RECONCILE_FREEZE:
        notes.append('existing_freeze_state')
        return _decision(
            status=RECONCILE_FREEZE,
            freeze_reason=state.freeze_reason,
            can_open_new_position=False,
            can_modify_position=False,
            open_order_ids=pending_ids,
        )

    if not protective_ok:
        notes.append('protective_orders_invalid')
        return _decision(
            status=RECONCILE_MISMATCH,
            freeze_reason=protective_freeze_reason,
            can_open_new_position=False,
            can_modify_position=False,
            open_order_ids=pending_ids,
        )

    if pending_orders:
        freeze_reason = None
        if payload.last_result is not None and payload.last_result.should_freeze:
            freeze_reason = payload.last_result.freeze_reason or 'pending_order_after_execution'
        return _decision(
            status=RECONCILE_PENDING_ORDER,
            freeze_reason=freeze_reason,
            can_open_new_position=False,
            can_modify_position=False,
            open_order_ids=pending_ids,
        )

    if management_pending_without_visible_protective:
        return _decision(
            status=RECONCILE_PENDING_ORDER,
            freeze_reason='management_stop_update_pending_protective',
            can_open_new_position=False,
            can_modify_position=False,
            open_order_ids=[],
        )

    has_recent_confirmed_position_fact = bool(
        payload.last_result is not None
        and payload.last_result.confirmation_status in {'CONFIRMED', 'POSITION_CONFIRMED'}
        and payload.last_result.post_position_side in {'long', 'short'}
        and float(payload.last_result.post_position_qty or 0.0) > payload.qty_tolerance
    )

    in_confirmed_pending_window = bool(
        state.exchange_position_side in {'long', 'short'}
        and float(state.exchange_position_qty or 0.0) > payload.qty_tolerance
        and (
            state.position_confirmation_level in {'POSITION_CONFIRMED', 'TRADES_CONFIRMED'}
            or bool(state.needs_trade_reconciliation)
            or (
                state.pending_execution_phase in {'entry_confirmed_pending_protective', 'management_stop_update_pending_protective'}
                and payload.last_result is not None
                and payload.last_result.confirmation_status in {'CONFIRMED', 'POSITION_CONFIRMED'}
                and payload.last_result.post_position_side in {'long', 'short'}
                and float(payload.last_result.post_position_qty or 0.0) > payload.qty_tolerance
            )
            or state.pending_execution_phase in {
                'confirmed',
                'position_confirmed_pending_trades',
                'protection_pending_confirm',
                'entry_confirmed_pending_protective',
                'management_stop_update_pending_protective',
            }
        )
    )
    if in_confirmed_pending_window and exchange_side is None and exchange_qty <= payload.qty_tolerance:
        refresh_gap_supported = bool(
            state.position_confirmation_level in {'POSITION_CONFIRMED', 'TRADES_CONFIRMED'}
            or bool(state.needs_trade_reconciliation)
            or has_recent_confirmed_position_fact
            or state.pending_execution_phase in {
                'position_confirmed_pending_trades',
                'protection_pending_confirm',
                'entry_confirmed_pending_protective',
                'management_stop_update_pending_protective',
            }
        )
        if refresh_gap_supported:
            notes.append('exchange_position_refresh_gap_observed')
            return _decision(
                status=RECONCILE_PENDING_ORDER,
                freeze_reason='position_confirmed_pending_trades',
                can_open_new_position=False,
                can_modify_position=False,
                open_order_ids=[],
            )

    if local_side is None and exchange_side is None:
        return _decision(
            status=RECONCILE_OK,
            freeze_reason=None,
            can_open_new_position=True,
            can_modify_position=True,
            open_order_ids=[],
        )

    if partial_protective_missing_reason is not None:
        return ReconcileDecision(
            status=RECONCILE_OK,
            freeze_reason=partial_protective_missing_reason,
            can_open_new_position=False,
            can_modify_position=False,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            risk_action='RECOVER_PROTECTION',
            recover_policy='manual_review',
            recover_stage='protection_partial_missing',
            stop_condition=partial_protective_missing_reason,
            open_order_ids=[],
            notes=notes + [partial_protective_missing_reason, 'partial_protective_missing'],
        )

    stale_local_flat_reset_allowed = state.pending_execution_phase in {
        FLAT_READY_PENDING_PHASE,
        'none',
        None,
        'confirmed',
    }

    if exchange_side is None and exchange_qty <= payload.qty_tolerance and (
        state.exchange_position_side is not None
        or state.active_side is not None
        or state.active_strategy not in {None, 'none'}
        or bool(state.exchange_protective_orders)
        or state.protective_order_status not in {None, 'NONE'}
        or state.protective_phase_status not in {None, 'NONE'}
    ) and stale_local_flat_reset_allowed and not (
        state.position_confirmation_level in {'POSITION_CONFIRMED', 'TRADES_CONFIRMED'}
        or bool(state.needs_trade_reconciliation)
        or has_recent_confirmed_position_fact
    ):
        notes.append('stale_local_state_reset_to_flat_ready')
        return ReconcileDecision(
            status=RECONCILE_OK,
            freeze_reason=None,
            can_open_new_position=True,
            can_modify_position=True,
            local_position_side=None,
            local_position_qty=0.0,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            risk_action='OBSERVE',
            recover_policy='ready_only',
            recover_stage='recover_ready',
            stop_condition='terminal_confirmation_reached',
            open_order_ids=[],
            notes=notes,
        )

    if (local_side is None) != (exchange_side is None):
        notes.append('position_presence_mismatch')
        return _decision(
            status=RECONCILE_MISMATCH,
            freeze_reason='local_exchange_position_presence_mismatch',
            can_open_new_position=False,
            can_modify_position=False,
            open_order_ids=[],
        )

    if local_side != exchange_side:
        notes.append('position_side_mismatch')
        return ReconcileDecision(
            status=RECONCILE_MISMATCH,
            freeze_reason='local_exchange_position_side_mismatch',
            can_open_new_position=False,
            can_modify_position=False,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            open_order_ids=[],
            notes=notes,
        )

    if abs(local_qty - exchange_qty) > payload.qty_tolerance:
        notes.append('position_qty_mismatch')
        return _decision(
            status=RECONCILE_MISMATCH,
            freeze_reason='local_exchange_position_qty_mismatch',
            can_open_new_position=False,
            can_modify_position=False,
            open_order_ids=[],
        )

    return _decision(
        status=RECONCILE_OK,
        freeze_reason=None,
        can_open_new_position=True,
        can_modify_position=True,
        open_order_ids=[],
    )
