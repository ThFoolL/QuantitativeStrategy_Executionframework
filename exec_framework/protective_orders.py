from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .binance_readonly import OrderSnapshot

PROTECTIVE_PENDING_STATUSES = {
    'NEW',
    'PARTIALLY_FILLED',
    'PENDING_CANCEL',
    'ACCEPTED',
    'CALCULATED',
}

PROTECTIVE_ORDER_TYPES = {
    'STOP',
    'STOP_MARKET',
    'TAKE_PROFIT',
    'TAKE_PROFIT_MARKET',
    'TRAILING_STOP_MARKET',
}


@dataclass(frozen=True)
class ProtectiveOrderIntent:
    kind: str
    trigger_price: float
    quantity: float
    side: str
    order_type: str
    reduce_only: bool = True
    close_position: bool = True


@dataclass(frozen=True)
class ProtectiveOrdersSnapshot:
    orders: list[dict[str, Any]]
    hard_stop: dict[str, Any] | None
    take_profit: dict[str, Any] | None


@dataclass(frozen=True)
class ProtectiveOrdersValidation:
    ok: bool
    freeze_reason: str | None
    status: str
    notes: list[str]
    summary: dict[str, Any]
    validation_level: str
    risk_class: str
    mismatch_class: str | None


def is_protective_order(order: OrderSnapshot) -> bool:
    order_type = str(getattr(order, 'type', None) or getattr(order, 'orig_type', None) or '').upper()
    client_order_id = str(getattr(order, 'client_order_id', None) or '').lower()
    if order_type in PROTECTIVE_ORDER_TYPES or client_order_id.startswith('protect-'):
        return True
    if not bool(getattr(order, 'reduce_only', None)) and not bool(getattr(order, 'close_position', None)):
        return False
    side = str(getattr(order, 'side', None) or '').lower()
    has_trigger = getattr(order, 'stop_price', None) is not None or order_type in PROTECTIVE_ORDER_TYPES
    return side in {'buy', 'sell', 'long', 'short'} and has_trigger


def normalize_order_kind(order: OrderSnapshot) -> str | None:
    order_type = str(getattr(order, 'type', None) or getattr(order, 'orig_type', None) or '').upper()
    if order_type in {'STOP', 'STOP_MARKET', 'TRAILING_STOP_MARKET'}:
        return 'hard_stop'
    if order_type in {'TAKE_PROFIT', 'TAKE_PROFIT_MARKET'}:
        return 'take_profit'

    client_order_id = str(getattr(order, 'client_order_id', None) or '').lower()
    if 'protect-hard-stop' in client_order_id or 'protect-har' in client_order_id or client_order_id.endswith('-pstop'):
        return 'hard_stop'
    if 'protect-take-profit' in client_order_id or 'protect-tak' in client_order_id or client_order_id.endswith('-ptp'):
        return 'take_profit'
    return None


def serialize_protective_order(order: OrderSnapshot) -> dict[str, Any]:
    return {
        'order_id': getattr(order, 'order_id', None),
        'client_order_id': getattr(order, 'client_order_id', None),
        'status': getattr(order, 'status', None),
        'side': getattr(order, 'side', None),
        'position_side': getattr(order, 'position_side', None),
        'type': getattr(order, 'type', None),
        'orig_type': getattr(order, 'orig_type', None),
        'qty': getattr(order, 'qty', None),
        'executed_qty': getattr(order, 'executed_qty', None),
        'price': getattr(order, 'price', None),
        'avg_price': getattr(order, 'avg_price', None),
        'stop_price': getattr(order, 'stop_price', None),
        'working_type': getattr(order, 'working_type', None),
        'activate_price': getattr(order, 'activate_price', None),
        'price_protect': getattr(order, 'price_protect', None),
        'reduce_only': getattr(order, 'reduce_only', None),
        'close_position': getattr(order, 'close_position', None),
        'is_algo_order': bool((getattr(order, 'raw', {}) or {}).get('is_algo_order')),
        'kind': normalize_order_kind(order),
        'update_time_ms': getattr(order, 'update_time_ms', None),
    }


def split_open_orders(open_orders: Iterable[OrderSnapshot]) -> tuple[list[OrderSnapshot], list[OrderSnapshot]]:
    protective: list[OrderSnapshot] = []
    regular: list[OrderSnapshot] = []
    for order in open_orders:
        status = str(getattr(order, 'status', None) or '').upper()
        if is_protective_order(order) and status in PROTECTIVE_PENDING_STATUSES:
            protective.append(order)
        else:
            regular.append(order)
    return protective, regular


def snapshot_protective_orders(open_orders: Iterable[OrderSnapshot]) -> ProtectiveOrdersSnapshot:
    protective_orders, _ = split_open_orders(open_orders)
    serialized = [serialize_protective_order(item) for item in protective_orders]
    hard_stop = next((item for item in serialized if item.get('kind') == 'hard_stop'), None)
    take_profit = next((item for item in serialized if item.get('kind') == 'take_profit'), None)
    return ProtectiveOrdersSnapshot(orders=serialized, hard_stop=hard_stop, take_profit=take_profit)


def build_protective_order_intents(
    *,
    strategy: str | None,
    position_side: str | None,
    quantity: float,
    stop_price: float | None,
    tp_price: float | None,
) -> list[ProtectiveOrderIntent]:
    if strategy not in {'trend', 'rev'}:
        return []
    if position_side not in {'long', 'short'}:
        return []
    if quantity <= 0:
        return []

    exit_side = 'SELL' if position_side == 'long' else 'BUY'
    intents: list[ProtectiveOrderIntent] = []
    if stop_price is not None:
        intents.append(
            ProtectiveOrderIntent(
                kind='hard_stop',
                trigger_price=float(stop_price),
                quantity=float(quantity),
                side=exit_side,
                order_type='STOP_MARKET',
            )
        )
    if strategy == 'rev' and tp_price is not None:
        intents.append(
            ProtectiveOrderIntent(
                kind='take_profit',
                trigger_price=float(tp_price),
                quantity=float(quantity),
                side=exit_side,
                order_type='TAKE_PROFIT_MARKET',
            )
        )
    return intents


def _classify_validation_notes(notes: list[str]) -> tuple[str, str, str | None, str]:
    if not notes:
        return 'OK', 'OK', None, 'OK'

    if any(item.startswith('missing:') for item in notes):
        return 'MISSING', 'MISSING', 'MISSING', 'protective_order_missing'

    structural_prefixes = (
        'unexpected:',
        'status_invalid:',
        'not_reduce_only:',
    )
    if any(item.startswith(prefix) for item in notes for prefix in structural_prefixes):
        return 'MISMATCH', 'STRUCTURAL_MISMATCH', 'STRUCTURAL_MISMATCH', 'protective_order_mismatch'

    semantic_prefixes = (
        'side_mismatch:',
        'qty_mismatch:',
        'price_mismatch:',
    )
    if any(item.startswith(prefix) for item in notes for prefix in semantic_prefixes):
        return 'MISMATCH', 'SEMANTIC_MISMATCH', 'SEMANTIC_MISMATCH', 'protective_order_semantic_mismatch'

    return 'MISMATCH', 'STRUCTURAL_MISMATCH', 'STRUCTURAL_MISMATCH', 'protective_order_mismatch'


def validate_protective_orders(
    *,
    strategy: str | None,
    position_side: str | None,
    position_qty: float,
    stop_price: float | None,
    tp_price: float | None,
    open_orders: Iterable[OrderSnapshot],
    qty_tolerance: float = 1e-9,
    price_tolerance: float = 1e-9,
) -> ProtectiveOrdersValidation:
    snapshot = snapshot_protective_orders(open_orders)
    intents = build_protective_order_intents(
        strategy=strategy,
        position_side=position_side,
        quantity=position_qty,
        stop_price=stop_price,
        tp_price=tp_price,
    )
    notes: list[str] = []
    expected_by_kind = {item.kind: item for item in intents}
    actual_by_kind = {item.get('kind'): item for item in snapshot.orders if item.get('kind')}
    summary = {
        'expected': [intent.__dict__ for intent in intents],
        'actual': snapshot.orders,
        'protective_order_count': len(snapshot.orders),
    }

    if position_side not in {'long', 'short'} or position_qty <= qty_tolerance:
        if snapshot.orders:
            return ProtectiveOrdersValidation(
                ok=False,
                freeze_reason='protective_orders_present_while_flat',
                status='UNEXPECTED_WHILE_FLAT',
                notes=['protective_orders_present_while_flat'],
                summary={
                    **summary,
                    'validation_level': 'UNEXPECTED_WHILE_FLAT',
                    'risk_class': 'UNEXPECTED_WHILE_FLAT',
                    'mismatch_class': 'UNEXPECTED_WHILE_FLAT',
                },
                validation_level='UNEXPECTED_WHILE_FLAT',
                risk_class='UNEXPECTED_WHILE_FLAT',
                mismatch_class='UNEXPECTED_WHILE_FLAT',
            )
        return ProtectiveOrdersValidation(
            ok=True,
            freeze_reason=None,
            status='FLAT_OK',
            notes=[],
            summary={
                **summary,
                'validation_level': 'FLAT_OK',
                'risk_class': 'OK',
                'mismatch_class': None,
            },
            validation_level='FLAT_OK',
            risk_class='OK',
            mismatch_class=None,
        )

    for kind, intent in expected_by_kind.items():
        actual = actual_by_kind.get(kind)
        if actual is None:
            notes.append(f'missing:{kind}')
            continue
        if str(actual.get('side') or '').upper() != intent.side:
            notes.append(f'side_mismatch:{kind}')
        actual_close_position = bool(actual.get('close_position'))
        actual_qty = float(actual.get('qty') or 0.0)
        if not actual_close_position and abs(actual_qty - intent.quantity) > qty_tolerance:
            notes.append(f'qty_mismatch:{kind}')
        actual_trigger = actual.get('stop_price')
        if actual_trigger is None or abs(float(actual_trigger) - intent.trigger_price) > price_tolerance:
            notes.append(f'price_mismatch:{kind}')
        status = str(actual.get('status') or '').upper()
        if status not in PROTECTIVE_PENDING_STATUSES:
            notes.append(f'status_invalid:{kind}:{status or "UNKNOWN"}')
        if not actual.get('reduce_only') and not actual.get('close_position'):
            notes.append(f'not_reduce_only:{kind}')

    for kind in actual_by_kind:
        if kind not in expected_by_kind:
            notes.append(f'unexpected:{kind}')

    partial_missing_only = set(notes) in ({'missing:take_profit'}, {'missing:hard_stop'})
    if partial_missing_only and snapshot.orders:
        missing_kind = 'take_profit' if 'missing:take_profit' in notes else 'hard_stop'
        freeze_reason = 'protection_tp_missing' if missing_kind == 'take_profit' else 'protection_stop_missing'
        return ProtectiveOrdersValidation(
            ok=False,
            freeze_reason=freeze_reason,
            status='MISMATCH',
            notes=notes + [freeze_reason, 'partial_protective_missing'],
            summary={
                **summary,
                'validation_level': 'MISSING',
                'risk_class': 'MISSING',
                'mismatch_class': 'MISSING',
            },
            validation_level='MISSING',
            risk_class='MISSING',
            mismatch_class='MISSING',
        )

    validation_level, risk_class, mismatch_class, freeze_reason = _classify_validation_notes(notes)
    if notes:
        return ProtectiveOrdersValidation(
            ok=False,
            freeze_reason=freeze_reason,
            status='MISMATCH',
            notes=notes,
            summary={
                **summary,
                'validation_level': validation_level,
                'risk_class': risk_class,
                'mismatch_class': mismatch_class,
            },
            validation_level=validation_level,
            risk_class=risk_class,
            mismatch_class=mismatch_class,
        )

    return ProtectiveOrdersValidation(
        ok=True,
        freeze_reason=None,
        status='OK',
        notes=[],
        summary={
            **summary,
            'validation_level': validation_level,
            'risk_class': risk_class,
            'mismatch_class': mismatch_class,
        },
        validation_level=validation_level,
        risk_class=risk_class,
        mismatch_class=mismatch_class,
    )
