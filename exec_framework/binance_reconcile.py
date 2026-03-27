from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .binance_readonly import AccountSnapshot, OrderSnapshot, PositionSnapshot
from .models import ExecutionResult, LiveStateSnapshot

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


def _strategy_position_qty(state: LiveStateSnapshot) -> float:
    if state.active_side is None:
        return 0.0
    for value in (state.base_quantity, state.exchange_position_qty):
        if value is not None and float(value) > 0:
            return float(value)
    return 0.0


def _has_pending_orders(open_orders: Iterable[OrderSnapshot]) -> tuple[bool, list[str]]:
    pending_ids: list[str] = []
    for order in open_orders:
        status = str(order.status or 'UNKNOWN').upper()
        if status in PENDING_ORDER_STATUSES:
            pending_ids.append(order.order_id)
    return bool(pending_ids), pending_ids


def reconcile_pre_run(payload: ReconcileInput) -> ReconcileDecision:
    state = payload.state
    exchange = payload.exchange
    exchange_side = exchange.position.side
    exchange_qty = float(exchange.position.qty)
    local_side = state.active_side
    local_qty = _strategy_position_qty(state)

    notes: list[str] = []
    pending_orders, pending_ids = _has_pending_orders(exchange.open_orders)
    if pending_orders:
        notes.append('open_orders_present')

    if state.freeze_reason and state.consistency_status == RECONCILE_FREEZE:
        notes.append('existing_freeze_state')
        return ReconcileDecision(
            status=RECONCILE_FREEZE,
            freeze_reason=state.freeze_reason,
            can_open_new_position=False,
            can_modify_position=False,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            open_order_ids=pending_ids,
            notes=notes,
        )

    if pending_orders:
        freeze_reason = None
        if payload.last_result is not None and payload.last_result.should_freeze:
            freeze_reason = payload.last_result.freeze_reason or 'pending_order_after_execution'
        return ReconcileDecision(
            status=RECONCILE_PENDING_ORDER,
            freeze_reason=freeze_reason,
            can_open_new_position=False,
            can_modify_position=False,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            open_order_ids=pending_ids,
            notes=notes,
        )

    if local_side is None and exchange_side is None:
        return ReconcileDecision(
            status=RECONCILE_OK,
            freeze_reason=None,
            can_open_new_position=True,
            can_modify_position=True,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            open_order_ids=[],
            notes=notes,
        )

    if (local_side is None) != (exchange_side is None):
        notes.append('position_presence_mismatch')
        return ReconcileDecision(
            status=RECONCILE_MISMATCH,
            freeze_reason='local_exchange_position_presence_mismatch',
            can_open_new_position=False,
            can_modify_position=False,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            open_order_ids=[],
            notes=notes,
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
        return ReconcileDecision(
            status=RECONCILE_MISMATCH,
            freeze_reason='local_exchange_position_qty_mismatch',
            can_open_new_position=False,
            can_modify_position=False,
            local_position_side=local_side,
            local_position_qty=local_qty,
            exchange_position_side=exchange_side,
            exchange_position_qty=exchange_qty,
            open_order_ids=[],
            notes=notes,
        )

    return ReconcileDecision(
        status=RECONCILE_OK,
        freeze_reason=None,
        can_open_new_position=True,
        can_modify_position=True,
        local_position_side=local_side,
        local_position_qty=local_qty,
        exchange_position_side=exchange_side,
        exchange_position_qty=exchange_qty,
        open_order_ids=[],
        notes=notes,
    )
