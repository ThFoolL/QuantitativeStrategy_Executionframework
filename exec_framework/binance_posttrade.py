from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from .binance_readonly import BinanceReadOnlyClient, OrderSnapshot, PositionSnapshot, UserTradeSnapshot
from .models import MarketSnapshot


ORDER_STATUS_SUBMITTED = 'SUBMITTED'
ORDER_STATUS_PENDING = 'PENDING'
ORDER_STATUS_PARTIALLY_FILLED = 'PARTIALLY_FILLED'
ORDER_STATUS_FILLED = 'FILLED'
ORDER_STATUS_CANCELED = 'CANCELED'
ORDER_STATUS_REJECTED = 'REJECTED'
ORDER_STATUS_UNKNOWN = 'UNKNOWN'

RECONCILE_OK = 'OK'
RECONCILE_PENDING = 'PENDING_CONFIRMATION'
RECONCILE_MISMATCH = 'POST_TRADE_MISMATCH'
RECONCILE_QUERY_FAILED = 'POST_TRADE_QUERY_FAILED'

CONFIRM_CATEGORY_CONFIRMED = 'confirmed'
CONFIRM_CATEGORY_PENDING = 'pending'
CONFIRM_CATEGORY_QUERY_FAILED = 'query_failed'
CONFIRM_CATEGORY_MISMATCH = 'mismatch'
CONFIRM_CATEGORY_REJECTED = 'rejected'

BINANCE_PENDING_ORDER_STATUSES = {
    'NEW',
    'PENDING_CANCEL',
    'ACCEPTED',
    'CALCULATED',
}

BINANCE_REJECTED_ORDER_STATUSES = {
    'REJECTED',
    'EXPIRED',
    'EXPIRED_IN_MATCH',
}


class OrderRequestLike(Protocol):
    symbol: str
    client_order_id: str
    side: str
    quantity: float | None
    reduce_only: bool


@dataclass(frozen=True)
class SimulatedExecutionReceipt:
    client_order_id: str
    exchange_order_id: str | None = None
    acknowledged: bool = False
    submitted_qty: float | None = None
    submitted_side: str | None = None
    submit_status: str = 'UNSUBMITTED'
    exchange_status: str | None = None
    transact_time_ms: int | None = None
    request_payload: dict[str, Any] | None = None
    response_payload: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ConfirmedTradeFill:
    order_id: str
    client_order_id: str | None
    qty: float
    price: float
    fee: float
    fee_asset: str | None
    realized_pnl: float | None
    side: str | None
    maker: bool | None
    time_ms: int | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class PostTradeConfirmation:
    confirmation_status: str
    confirmation_category: str
    order_status: str
    exchange_order_ids: list[str]
    executed_qty: float
    avg_fill_price: float | None
    fees: float
    fee_assets: list[str]
    fill_count: int
    post_position_side: str | None
    post_position_qty: float
    post_entry_price: float | None
    reconcile_status: str
    should_freeze: bool
    freeze_reason: str | None
    notes: list[str]
    trade_summary: dict[str, Any]

    @property
    def is_confirmed(self) -> bool:
        return self.confirmation_status == 'CONFIRMED'


class BinancePostTradeConfirmer:
    """执行后确认。

    当前口径：
    - order status 只作为确认入口，不直接替代成交事实
    - fill 以 `/fapi/v1/userTrades` 为准汇总 executed_qty / avg_fill_price / fees
    - position 以 `/fapi/v2/positionRisk` 为 post-position 事实来源
    - 任何 query 失败、未知状态、部分成交、挂单未完成，一律走 freeze/pending 路径
    - fee 仅做成交痕迹汇总；若出现多 fee asset，不把其伪装成统一 quote 成本
    """

    def __init__(self, readonly_client: BinanceReadOnlyClient):
        self.readonly_client = readonly_client

    def confirm(
        self,
        *,
        market: MarketSnapshot,
        order_requests: list[OrderRequestLike],
        simulated_receipts: list[SimulatedExecutionReceipt] | None = None,
    ) -> PostTradeConfirmation:
        notes: list[str] = []
        receipts = {item.client_order_id: item for item in (simulated_receipts or [])}
        order_statuses: list[str] = []
        exchange_order_ids: list[str] = []
        fills: list[ConfirmedTradeFill] = []
        query_failed = False
        submit_query_blocked = False
        order_fact_rows: list[dict[str, Any]] = []
        open_order_rows: list[dict[str, Any]] = []

        for request in order_requests:
            receipt = receipts.get(request.client_order_id)
            if receipt is not None and receipt.exchange_order_id:
                exchange_order_ids.append(str(receipt.exchange_order_id))
            if receipt is not None:
                order_fact_rows.append(
                    {
                        'client_order_id': receipt.client_order_id,
                        'submit_status': receipt.submit_status,
                        'acknowledged': receipt.acknowledged,
                        'exchange_order_id': receipt.exchange_order_id,
                        'exchange_status': receipt.exchange_status,
                        'transact_time_ms': receipt.transact_time_ms,
                        'error_code': receipt.error_code,
                    }
                )
                if not receipt.acknowledged and receipt.error_code:
                    submit_query_blocked = True
                    order_statuses.append(ORDER_STATUS_UNKNOWN)
                    notes.append(f'submit_not_acknowledged:{receipt.client_order_id}:{receipt.error_code}')
                    continue
            try:
                snapshot = self.readonly_client.get_order(
                    symbol=request.symbol,
                    order_id=receipt.exchange_order_id if receipt and receipt.exchange_order_id else None,
                    client_order_id=None if receipt and receipt.exchange_order_id else request.client_order_id,
                )
                status = self._normalize_order_status(snapshot.status)
                order_statuses.append(status)
                if snapshot.order_id:
                    exchange_order_ids.append(str(snapshot.order_id))
                snapshot_executed_qty = getattr(snapshot, 'executed_qty', None)
                snapshot_qty = getattr(snapshot, 'qty', None)
                if snapshot_executed_qty is not None and snapshot_qty is not None:
                    notes.extend(self._derive_execution_notes(status, snapshot_executed_qty, snapshot_qty))
                order_fact_rows.append(
                    {
                        'client_order_id': request.client_order_id,
                        'lookup_key': 'order_id' if receipt and receipt.exchange_order_id else 'client_order_id',
                        'exchange_order_id': getattr(snapshot, 'order_id', None),
                        'order_status': status,
                        'orig_qty': snapshot_qty,
                        'executed_qty': snapshot_executed_qty,
                        'avg_price': getattr(snapshot, 'avg_price', None),
                        'reduce_only': getattr(snapshot, 'reduce_only', None),
                        'side': getattr(snapshot, 'side', None),
                        'position_side': getattr(snapshot, 'position_side', None),
                        'update_time_ms': getattr(snapshot, 'update_time_ms', None),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                query_failed = True
                order_statuses.append(ORDER_STATUS_UNKNOWN)
                notes.append(f'order_query_failed:{request.client_order_id}:{exc.__class__.__name__}')

        dedup_order_ids = sorted(set(exchange_order_ids))
        query_failed = query_failed or submit_query_blocked

        if not query_failed:
            try:
                fills = self._collect_fills(market.symbol, dedup_order_ids)
                if dedup_order_ids and not fills:
                    notes.append('no_matching_user_trades')
            except Exception as exc:  # noqa: BLE001
                query_failed = True
                notes.append(f'trade_query_failed:{exc.__class__.__name__}')

        try:
            post_position = self.readonly_client.get_position_snapshot(market.symbol)
        except Exception as exc:  # noqa: BLE001
            query_failed = True
            notes.append(f'position_query_failed:{exc.__class__.__name__}')
            post_position = PositionSnapshot(
                symbol=market.symbol,
                side=None,
                qty=0.0,
                entry_price=None,
                break_even_price=None,
                mark_price=None,
                unrealized_pnl=None,
                leverage=None,
                margin_type=None,
                position_side_mode=None,
                raw={},
            )

        try:
            open_orders = self._collect_open_orders(market.symbol)
            open_order_rows = [self._serialize_open_order(item) for item in open_orders]
        except Exception as exc:  # noqa: BLE001
            query_failed = True
            open_orders = []
            notes.append(f'open_orders_query_failed:{exc.__class__.__name__}')

        order_status = self._aggregate_order_status(order_statuses)
        executed_qty = sum(fill.qty for fill in fills)
        fee_total = sum(fill.fee for fill in fills)
        fee_assets = sorted({fill.fee_asset for fill in fills if fill.fee_asset})
        avg_fill_price = None
        if executed_qty > 0:
            avg_fill_price = sum(fill.qty * fill.price for fill in fills) / executed_qty

        validation = self._validate_confirmation(
            order_requests=order_requests,
            order_status=order_status,
            executed_qty=executed_qty,
            avg_fill_price=avg_fill_price,
            fee_assets=fee_assets,
            fills=fills,
            post_position=post_position,
            open_orders=open_orders,
            notes=notes,
        )
        query_failed = query_failed or validation['query_failed']
        should_freeze = validation['should_freeze']
        freeze_reason = validation['freeze_reason']

        if query_failed:
            confirmation_status = 'UNCONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_QUERY_FAILED
            reconcile_status = RECONCILE_QUERY_FAILED
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_query_failed'
        elif order_status == ORDER_STATUS_FILLED and not should_freeze:
            confirmation_status = 'CONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_CONFIRMED
            reconcile_status = RECONCILE_OK
            freeze_reason = None
        elif order_status in {ORDER_STATUS_PARTIALLY_FILLED, ORDER_STATUS_PENDING, ORDER_STATUS_SUBMITTED}:
            confirmation_status = 'PENDING'
            confirmation_category = CONFIRM_CATEGORY_PENDING
            reconcile_status = RECONCILE_PENDING
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_pending_confirmation'
        elif order_status in {ORDER_STATUS_CANCELED, ORDER_STATUS_REJECTED}:
            confirmation_status = 'UNCONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_REJECTED
            reconcile_status = RECONCILE_MISMATCH
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_rejected_or_canceled'
        else:
            confirmation_status = 'UNCONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_MISMATCH
            reconcile_status = RECONCILE_MISMATCH
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_unknown_state'

        fill_rows = [
            {
                'order_id': item.order_id,
                'client_order_id': item.client_order_id,
                'qty': item.qty,
                'price': item.price,
                'fee': item.fee,
                'fee_asset': item.fee_asset,
                'realized_pnl': item.realized_pnl,
                'side': item.side,
                'maker': item.maker,
                'time_ms': item.time_ms,
            }
            for item in fills
        ]
        notes = sorted(set(notes))

        requested_qty = sum(float(getattr(request, 'quantity', 0.0) or 0.0) for request in order_requests)
        requested_reduce_only = all(bool(getattr(request, 'reduce_only', False)) for request in order_requests) if order_requests else False

        return PostTradeConfirmation(
            confirmation_status=confirmation_status,
            confirmation_category=confirmation_category,
            order_status=order_status,
            exchange_order_ids=dedup_order_ids,
            executed_qty=executed_qty,
            avg_fill_price=avg_fill_price,
            fees=fee_total,
            fee_assets=fee_assets,
            fill_count=len(fills),
            post_position_side=post_position.side,
            post_position_qty=post_position.qty,
            post_entry_price=post_position.entry_price,
            reconcile_status=reconcile_status,
            should_freeze=should_freeze,
            freeze_reason=freeze_reason,
            notes=notes,
            trade_summary={
                'fills_count': len(fills),
                'order_requests': [request.client_order_id for request in order_requests],
                'order_statuses': order_statuses,
                'confirmation_category': confirmation_category,
                'query_failed': query_failed,
                'submit_query_blocked': submit_query_blocked,
                'fee_assets': fee_assets,
                'requested_qty': requested_qty,
                'executed_qty': executed_qty,
                'requested_reduce_only': requested_reduce_only,
                'position_side_mode': getattr(post_position, 'position_side_mode', None),
                'position_mark_price': getattr(post_position, 'mark_price', None),
                'position_unrealized_pnl': getattr(post_position, 'unrealized_pnl', None),
                'position_entry_price': getattr(post_position, 'entry_price', None),
                'post_position_qty': getattr(post_position, 'qty', None),
                'post_position_side': getattr(post_position, 'side', None),
                'open_orders_count': len(open_order_rows),
                'open_orders': open_order_rows,
                'has_open_orders': bool(open_order_rows),
                'order_facts': order_fact_rows,
                'fills': fill_rows,
                'fee_total_mixed_asset_sum': fee_total,
                'fee_asset_count': len(fee_assets),
                'freeze_rule': freeze_reason,
                'notes': notes,
            },
        )

    def _collect_open_orders(self, symbol: str) -> list[OrderSnapshot]:
        getter = getattr(self.readonly_client, 'get_open_orders', None)
        if not callable(getter):
            return []
        rows = getter(symbol)
        return [item for item in rows if getattr(item, 'order_id', None)]

    @staticmethod
    def _serialize_open_order(order: OrderSnapshot) -> dict[str, Any]:
        return {
            'order_id': order.order_id,
            'client_order_id': order.client_order_id,
            'status': order.status,
            'side': order.side,
            'position_side': order.position_side,
            'qty': order.qty,
            'executed_qty': order.executed_qty,
            'price': order.price,
            'avg_price': order.avg_price,
            'reduce_only': order.reduce_only,
            'close_position': order.close_position,
            'update_time_ms': order.update_time_ms,
        }

    def _collect_fills(self, symbol: str, exchange_order_ids: Iterable[str]) -> list[ConfirmedTradeFill]:
        order_ids = [str(item) for item in exchange_order_ids if item]
        matched: list[ConfirmedTradeFill] = []
        if order_ids:
            seen_trade_ids: set[str] = set()
            for order_id in order_ids:
                rows = self.readonly_client.get_recent_trades(symbol=symbol, limit=1000, order_id=order_id)
                for row in rows:
                    trade = self._coerce_trade_snapshot(row)
                    if trade.trade_id in seen_trade_ids:
                        continue
                    seen_trade_ids.add(trade.trade_id)
                    matched.append(
                        ConfirmedTradeFill(
                            order_id=trade.order_id,
                            client_order_id=trade.client_order_id,
                            qty=trade.qty,
                            price=trade.price,
                            fee=trade.fee,
                            fee_asset=trade.fee_asset,
                            realized_pnl=trade.realized_pnl,
                            side=trade.side,
                            maker=trade.maker,
                            time_ms=trade.time_ms,
                            raw=trade.raw,
                        )
                    )
            matched.sort(key=lambda item: (item.time_ms or 0, item.order_id, item.qty))
            return matched

        rows = self.readonly_client.get_recent_trades(symbol=symbol, limit=100)
        for row in rows:
            trade = self._coerce_trade_snapshot(row)
            matched.append(
                ConfirmedTradeFill(
                    order_id=trade.order_id,
                    client_order_id=trade.client_order_id,
                    qty=trade.qty,
                    price=trade.price,
                    fee=trade.fee,
                    fee_asset=trade.fee_asset,
                    realized_pnl=trade.realized_pnl,
                    side=trade.side,
                    maker=trade.maker,
                    time_ms=trade.time_ms,
                    raw=trade.raw,
                )
            )
        matched.sort(key=lambda item: (item.time_ms or 0, item.order_id, item.qty))
        return matched

    def _coerce_trade_snapshot(self, row: Any) -> UserTradeSnapshot:
        if isinstance(row, UserTradeSnapshot):
            return row
        parser = getattr(self.readonly_client, '_parse_user_trade_snapshot', None)
        if callable(parser):
            return parser(row)
        return UserTradeSnapshot(
            trade_id=str(row.get('id')),
            order_id=str(row.get('orderId')),
            client_order_id=row.get('clientOrderId'),
            symbol=row.get('symbol'),
            side=(str(row.get('side')).lower() if row.get('side') else None),
            position_side=(str(row.get('positionSide')).lower() if row.get('positionSide') else None),
            qty=float(row.get('qty', 0.0)),
            price=float(row.get('price', 0.0)),
            realized_pnl=(float(row['realizedPnl']) if row.get('realizedPnl') not in (None, '', 'NULL') else None),
            fee=float(row.get('commission', 0.0)),
            fee_asset=row.get('commissionAsset'),
            maker=row.get('maker'),
            buyer=row.get('buyer'),
            time_ms=(int(row['time']) if row.get('time') not in (None, '', 'NULL') else None),
            raw=row,
        )

    def _aggregate_order_status(self, statuses: list[str]) -> str:
        normalized = [self._normalize_order_status(item or ORDER_STATUS_UNKNOWN) for item in statuses]
        if not normalized:
            return ORDER_STATUS_UNKNOWN
        if any(item == ORDER_STATUS_UNKNOWN for item in normalized):
            return ORDER_STATUS_UNKNOWN
        if any(item == ORDER_STATUS_PARTIALLY_FILLED for item in normalized):
            return ORDER_STATUS_PARTIALLY_FILLED
        if all(item == ORDER_STATUS_FILLED for item in normalized):
            return ORDER_STATUS_FILLED
        if any(item == ORDER_STATUS_REJECTED for item in normalized):
            return ORDER_STATUS_REJECTED
        if any(item == ORDER_STATUS_CANCELED for item in normalized):
            return ORDER_STATUS_CANCELED
        if any(item == ORDER_STATUS_PENDING for item in normalized):
            return ORDER_STATUS_PENDING
        return ORDER_STATUS_SUBMITTED

    def _normalize_order_status(self, status: str | None) -> str:
        text = str(status or ORDER_STATUS_UNKNOWN).upper()
        if text in {'FILLED'}:
            return ORDER_STATUS_FILLED
        if text in {'PARTIALLY_FILLED'}:
            return ORDER_STATUS_PARTIALLY_FILLED
        if text in {'PENDING'} or text in BINANCE_PENDING_ORDER_STATUSES:
            return ORDER_STATUS_PENDING
        if text in {'CANCELED'}:
            return ORDER_STATUS_CANCELED
        if text in BINANCE_REJECTED_ORDER_STATUSES:
            return ORDER_STATUS_REJECTED
        if text in {'NOT_SUBMITTED', 'SUBMITTED'}:
            return ORDER_STATUS_SUBMITTED
        return ORDER_STATUS_UNKNOWN

    def _validate_confirmation(
        self,
        *,
        order_requests: list[OrderRequestLike],
        order_status: str,
        executed_qty: float,
        avg_fill_price: float | None,
        fee_assets: list[str],
        fills: list[ConfirmedTradeFill],
        post_position: PositionSnapshot,
        open_orders: list[OrderSnapshot],
        notes: list[str],
    ) -> dict[str, Any]:
        should_freeze = False
        freeze_reason = None
        query_failed = False

        if order_status == ORDER_STATUS_FILLED and executed_qty <= 0:
            should_freeze = True
            freeze_reason = 'posttrade_missing_fills'
            query_failed = True
            notes.append('filled_without_user_trades')

        if order_status == ORDER_STATUS_PARTIALLY_FILLED:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_partial_fill_requires_manual_reconcile'
            notes.append('partial_fill_requires_freeze')

        requested_qty = sum(float(getattr(request, 'quantity', 0.0) or 0.0) for request in order_requests)
        requested_reduce_only = all(bool(getattr(request, 'reduce_only', False)) for request in order_requests) if order_requests else False
        qty_tolerance = 1e-9

        # Terminal exchange statuses can still carry partial fills; keep them frozen for manual reconcile.
        if order_status in {ORDER_STATUS_CANCELED, ORDER_STATUS_REJECTED} and executed_qty > qty_tolerance:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_terminal_status_with_partial_fills'
            notes.append('terminal_status_with_partial_fills')

        pending_open_orders = [item for item in open_orders if str(item.status or '').upper() in BINANCE_PENDING_ORDER_STATUSES]
        residual_open_orders = [
            item
            for item in pending_open_orders
            if order_status == ORDER_STATUS_FILLED and str(item.order_id) not in {fill.order_id for fill in fills}
        ]
        conflicting_open_orders = []
        for item in pending_open_orders:
            if item.side and any(fill.side and str(fill.side).lower() != str(item.side).lower() for fill in fills):
                conflicting_open_orders.append(item)
                continue
            if item.qty is not None and requested_qty > 0 and float(item.qty) - requested_qty > qty_tolerance:
                conflicting_open_orders.append(item)

        if order_status in {ORDER_STATUS_PENDING, ORDER_STATUS_PARTIALLY_FILLED} and pending_open_orders:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_open_orders_still_pending'
            notes.append('open_orders_still_pending')

        if order_status == ORDER_STATUS_FILLED and pending_open_orders:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_filled_but_open_orders_still_live'
            notes.append('filled_but_open_orders_still_live')

        if residual_open_orders:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_residual_open_orders_after_fill'
            notes.append('residual_open_orders_after_fill')

        if conflicting_open_orders:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_open_orders_side_or_qty_conflict'
            notes.append('open_orders_side_or_qty_conflict')

        if executed_qty > 0 and avg_fill_price is None:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_avg_price_missing'
            notes.append('avg_fill_price_missing')

        if len(fee_assets) > 1:
            notes.append('mixed_fee_assets_not_normalized')

        if order_status == ORDER_STATUS_UNKNOWN:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_unknown_state'
            notes.append('order_status_unknown_requires_freeze')

        if order_status == ORDER_STATUS_FILLED and executed_qty > 0 and post_position.qty < 0:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_invalid_position_qty'
            query_failed = True
            notes.append('negative_post_position_qty_invalid')

        if requested_qty > 0 and executed_qty - requested_qty > qty_tolerance:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_executed_qty_exceeds_request'
            notes.append('executed_qty_exceeds_requested_qty')

        if order_status == ORDER_STATUS_FILLED and requested_qty > 0 and executed_qty + qty_tolerance < requested_qty:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_filled_but_qty_mismatch'
            notes.append('filled_but_executed_qty_less_than_requested')

        if (
            order_status == ORDER_STATUS_FILLED
            and not requested_reduce_only
            and requested_qty > 0
            and post_position.qty - executed_qty > qty_tolerance
        ):
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_position_changed_late_vs_fills'
            notes.append('filled_but_position_changed_late')

        if requested_reduce_only and pending_open_orders:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_reduce_only_open_orders_still_live'
            notes.append('reduce_only_open_orders_still_live')

        if order_status == ORDER_STATUS_FILLED and requested_reduce_only and post_position.qty > qty_tolerance:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_reduce_only_position_not_flat'
            notes.append('reduce_only_filled_but_position_not_flat')

        return {
            'should_freeze': should_freeze,
            'freeze_reason': freeze_reason,
            'query_failed': query_failed,
        }

    @staticmethod
    def _derive_execution_notes(status: str, executed_qty: float, orig_qty: float) -> list[str]:
        notes: list[str] = []
        if status == ORDER_STATUS_PENDING and executed_qty <= 0:
            notes.append('pending_no_fill_yet')
        if status == ORDER_STATUS_PARTIALLY_FILLED and 0 < executed_qty < orig_qty:
            notes.append('partial_fill_detected')
        if status == ORDER_STATUS_FILLED and executed_qty <= 0:
            notes.append('filled_without_executed_qty_needs_check')
        return notes
