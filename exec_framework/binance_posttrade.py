from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol

import json

from .binance_readonly import BinanceReadOnlyClient, OrderSnapshot, PositionSnapshot, UserTradeSnapshot
from .models import MarketSnapshot
from .position_fact_reconciler import PositionFactReconciler
from .protective_orders import snapshot_protective_orders, split_open_orders, validate_protective_orders
from .strategy_protection_intent import build_strategy_protection_intent


ORDER_STATUS_SUBMITTED = 'SUBMITTED'
ORDER_STATUS_PENDING = 'PENDING'
ORDER_STATUS_PARTIALLY_FILLED = 'PARTIALLY_FILLED'
ORDER_STATUS_FILLED = 'FILLED'
ORDER_STATUS_CANCELED = 'CANCELED'
ORDER_STATUS_REJECTED = 'REJECTED'
ORDER_STATUS_UNKNOWN = 'UNKNOWN'

TRADE_LOOKBACK_WINDOW_MS = 5 * 60 * 1000
TRADE_FORWARD_WINDOW_MS = 30 * 1000
TRADE_RECENT_LIMIT = 1000

RECONCILE_OK = 'OK'
RECONCILE_PENDING = 'PENDING_CONFIRMATION'
RECONCILE_MISMATCH = 'POST_TRADE_MISMATCH'
RECONCILE_QUERY_FAILED = 'POST_TRADE_QUERY_FAILED'

CONFIRM_CATEGORY_CONFIRMED = 'confirmed'
CONFIRM_CATEGORY_POSITION_CONFIRMED = 'position_confirmed'
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

ORDER_LOOKUP_NOT_FOUND_MARKERS = {
    'code":-2013',
    'order does not exist',
    'plain order not found',
}

ALGO_ORDER_LOOKUP_NOT_FOUND_MARKERS = {
    'code":-4165',
    'clientalgoid invalid',
    'algo order does not exist',
    'algo order not found',
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


def build_confirm_context(
    *,
    phase: str,
    confirmation: PostTradeConfirmation | None = None,
    attempts_used: int | None = None,
    max_attempts: int | None = None,
    retry_interval_seconds: float | None = None,
    retried: bool | None = None,
    attempt_trace: list[dict[str, Any]] | None = None,
    retry_budget: dict[str, Any] | None = None,
    stop_reason: str | None = None,
    stop_condition: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        'confirm_attempted': confirmation is not None,
        'confirm_path': phase,
        'confirm_phase': phase,
    }
    if confirmation is not None:
        context.update(
            {
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
                'exchange_order_ids': list(confirmation.exchange_order_ids or []),
                'notes': list(confirmation.notes or []),
            }
        )
    if attempts_used is not None:
        context['attempts_used'] = attempts_used
    if max_attempts is not None:
        context['max_attempts'] = max_attempts
    if retry_interval_seconds is not None:
        context['retry_interval_seconds'] = retry_interval_seconds
    if retried is not None:
        context['retried'] = retried
    if attempt_trace is not None:
        context['attempt_trace'] = list(attempt_trace)
    if retry_budget is not None:
        context['retry_budget'] = dict(retry_budget)
    if stop_reason is not None:
        context['stop_reason'] = stop_reason
    if stop_condition is not None:
        context['stop_condition'] = stop_condition
    if extra:
        context.update(dict(extra))
    return context

    @property
    def is_confirmed(self) -> bool:
        return self.confirmation_status == 'CONFIRMED'


def _compact_exception_detail(exc: Exception) -> str:
    text = str(exc).strip()
    if not text:
        return exc.__class__.__name__
    try:
        payload = json.loads(text)
    except Exception:  # noqa: BLE001
        payload = None
    if isinstance(payload, dict):
        parts: list[str] = []
        kind = payload.get('kind')
        path = payload.get('path')
        status = payload.get('status')
        body = payload.get('payload')
        if kind:
            parts.append(str(kind))
        if path:
            parts.append(str(path))
        if status is not None:
            parts.append(f'status={status}')
        if isinstance(body, dict):
            code = body.get('code')
            msg = body.get('msg')
            if code is not None:
                parts.append(f'code={code}')
            if msg:
                parts.append(f'msg={msg}')
        elif body is not None:
            parts.append(str(body))
        compact = ','.join(part for part in parts if part)
        if compact:
            return compact[:240]
    return text[:240]


def _is_order_lookup_not_found_error(exc: Exception) -> bool:
    detail = str(exc).strip().lower()
    return any(marker in detail for marker in ORDER_LOOKUP_NOT_FOUND_MARKERS)


def _is_algo_order_lookup_not_found_error(exc: Exception) -> bool:
    detail = str(exc).strip().lower()
    return any(marker in detail for marker in ALGO_ORDER_LOOKUP_NOT_FOUND_MARKERS)


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
        self.position_fact_reconciler = PositionFactReconciler()

    def confirm(
        self,
        *,
        market: MarketSnapshot,
        order_requests: list[OrderRequestLike],
        simulated_receipts: list[SimulatedExecutionReceipt] | None = None,
        allow_unacknowledged_lookup: bool = False,
    ) -> PostTradeConfirmation:
        notes: list[str] = []
        receipts = {item.client_order_id: item for item in (simulated_receipts or [])}
        order_statuses: list[str] = []
        primary_order_statuses: list[str] = []
        exchange_order_ids: list[str] = []
        primary_exchange_order_ids: list[str] = []
        fills: list[ConfirmedTradeFill] = []
        query_failed = False
        submit_query_blocked = False
        order_fact_rows: list[dict[str, Any]] = []
        open_order_rows: list[dict[str, Any]] = []
        order_lookup_missing_only = False
        protective_plain_lookup_not_found = False
        protective_algo_lookup_not_found = False
        algo_query_failed = False
        algo_fact_rows: list[dict[str, Any]] = []

        request_context_rows: list[dict[str, Any]] = []
        for request in order_requests:
            receipt = receipts.get(request.client_order_id)
            request_metadata = dict(getattr(request, 'metadata', {}) or {})
            receipt_metadata = dict((receipt.metadata if receipt is not None else {}) or {})
            is_algo_request = bool(
                request_metadata.get('protective_order')
                or request_metadata.get('algo_order')
                or receipt_metadata.get('protective_order')
                or receipt_metadata.get('algo_order')
            )
            is_close_position_request = bool(getattr(request, 'close_position', False))
            request_context_rows.append(
                {
                    'request': request,
                    'receipt': receipt,
                    'request_metadata': request_metadata,
                    'receipt_metadata': receipt_metadata,
                    'is_algo_request': is_algo_request,
                    'is_close_position_request': is_close_position_request,
                }
            )
            if receipt is not None and receipt.exchange_order_id:
                exchange_order_ids.append(str(receipt.exchange_order_id))
                if not request_metadata.get('protective_order'):
                    primary_exchange_order_ids.append(str(receipt.exchange_order_id))
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
                if not receipt.acknowledged and receipt.error_code and not allow_unacknowledged_lookup:
                    submit_query_blocked = True
                    order_statuses.append(ORDER_STATUS_UNKNOWN)
                    notes.append(f'submit_not_acknowledged:{receipt.client_order_id}:{receipt.error_code}')
                    continue

            algo_snapshot = None
            algo_lookup_error: Exception | None = None
            lookup_order_id = None if is_algo_request else (receipt.exchange_order_id if receipt and receipt.exchange_order_id else None)
            lookup_client_order_id = request.client_order_id if is_algo_request or not (receipt and receipt.exchange_order_id) else None
            lookup_key = 'client_order_id' if lookup_client_order_id else 'order_id'
            algo_lookup_expected = bool(is_algo_request)
            if algo_lookup_expected:
                algo_lookup_order_id = receipt.exchange_order_id if receipt and receipt.exchange_order_id else None
                algo_lookup_client_order_id = request.client_order_id
                algo_lookup_by_id = bool(algo_lookup_order_id) and not bool(getattr(request, 'close_position', False))
                algo_lookup_key = 'algo_id' if algo_lookup_by_id else 'client_algo_id'
                try:
                    algo_snapshot = self._get_algo_order_snapshot(
                        symbol=request.symbol,
                        order_id=algo_lookup_order_id if algo_lookup_by_id else None,
                        client_order_id=algo_lookup_client_order_id,
                    )
                    status = self._normalize_order_status(algo_snapshot.status)
                    order_statuses.append(status)
                    if algo_snapshot.order_id:
                        exchange_order_ids.append(str(algo_snapshot.order_id))
                    snapshot_executed_qty = getattr(algo_snapshot, 'executed_qty', None)
                    snapshot_qty = getattr(algo_snapshot, 'qty', None)
                    if snapshot_executed_qty is not None and snapshot_qty is not None:
                        notes.extend(self._derive_execution_notes(status, snapshot_executed_qty, snapshot_qty))
                    algo_fact_rows.append(
                        {
                            'client_order_id': request.client_order_id,
                            'lookup_key': algo_lookup_key,
                            'exchange_order_id': getattr(algo_snapshot, 'order_id', None),
                            'client_algo_id': getattr(algo_snapshot, 'client_order_id', None),
                            'order_status': status,
                            'orig_qty': snapshot_qty,
                            'executed_qty': snapshot_executed_qty,
                            'avg_price': getattr(algo_snapshot, 'avg_price', None),
                            'reduce_only': getattr(algo_snapshot, 'reduce_only', None),
                            'side': getattr(algo_snapshot, 'side', None),
                            'position_side': getattr(algo_snapshot, 'position_side', None),
                            'update_time_ms': getattr(algo_snapshot, 'update_time_ms', None),
                            'source': 'algo_order',
                        }
                    )
                    continue
                except Exception as exc:  # noqa: BLE001
                    algo_lookup_error = exc
                    compact_error = _compact_exception_detail(exc)
                    notes.append(f'algo_order_query_failed:{request.client_order_id}:{compact_error}')
                    if self._is_algo_order_lookup_not_found_error(exc):
                        protective_algo_lookup_not_found = True
                    else:
                        algo_query_failed = True

            try:
                snapshot = self.readonly_client.get_order(
                    symbol=request.symbol,
                    order_id=lookup_order_id,
                    client_order_id=lookup_client_order_id,
                )
                status = self._normalize_order_status(snapshot.status)
                order_statuses.append(status)
                if not request_metadata.get('protective_order'):
                    primary_order_statuses.append(status)
                if snapshot.order_id:
                    exchange_order_ids.append(str(snapshot.order_id))
                    if not request_metadata.get('protective_order'):
                        primary_exchange_order_ids.append(str(snapshot.order_id))
                snapshot_executed_qty = getattr(snapshot, 'executed_qty', None)
                snapshot_qty = getattr(snapshot, 'qty', None)
                if snapshot_executed_qty is not None and snapshot_qty is not None:
                    notes.extend(self._derive_execution_notes(status, snapshot_executed_qty, snapshot_qty))
                order_fact_rows.append(
                    {
                        'client_order_id': request.client_order_id,
                        'lookup_key': lookup_key,
                        'exchange_order_id': getattr(snapshot, 'order_id', None),
                        'order_status': status,
                        'orig_qty': snapshot_qty,
                        'executed_qty': snapshot_executed_qty,
                        'avg_price': getattr(snapshot, 'avg_price', None),
                        'reduce_only': getattr(snapshot, 'reduce_only', None),
                        'side': getattr(snapshot, 'side', None),
                        'position_side': getattr(snapshot, 'position_side', None),
                        'update_time_ms': getattr(snapshot, 'update_time_ms', None),
                        'source': 'plain_order',
                    }
                )
            except Exception as exc:  # noqa: BLE001
                compact_error = _compact_exception_detail(exc)
                notes.append(f'order_query_failed:{request.client_order_id}:{compact_error}')
                is_lookup_not_found = _is_order_lookup_not_found_error(exc)
                if is_lookup_not_found:
                    order_lookup_missing_only = True
                    if is_algo_request:
                        protective_plain_lookup_not_found = True
                        if algo_lookup_error is not None and self._is_algo_order_lookup_not_found_error(algo_lookup_error):
                            order_statuses.append(ORDER_STATUS_UNKNOWN)
                    else:
                        order_statuses.append(ORDER_STATUS_UNKNOWN)
                else:
                    query_failed = True
                    order_lookup_missing_only = False
                    order_statuses.append(ORDER_STATUS_UNKNOWN)
                    if algo_lookup_expected and algo_lookup_error is not None:
                        algo_query_failed = True

        dedup_order_ids = sorted(set(exchange_order_ids))
        dedup_primary_order_ids = sorted(set(primary_exchange_order_ids))
        query_failed = query_failed or submit_query_blocked or algo_query_failed

        if not query_failed:
            try:
                fills = self._collect_fills(
                    market.symbol,
                    dedup_primary_order_ids or dedup_order_ids,
                    order_fact_rows=order_fact_rows,
                )
                if dedup_order_ids and not fills:
                    notes.append('no_matching_user_trades')
            except Exception as exc:  # noqa: BLE001
                query_failed = True
                notes.append(f'trade_query_failed:{_compact_exception_detail(exc)}')

        try:
            post_position = self.readonly_client.get_position_snapshot(market.symbol)
        except Exception as exc:  # noqa: BLE001
            query_failed = True
            notes.append(f'position_query_failed:{_compact_exception_detail(exc)}')
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
            open_orders = self._collect_open_orders(market.symbol, order_requests)
            open_order_rows = [self._serialize_open_order(item) for item in open_orders]
        except Exception as exc:  # noqa: BLE001
            query_failed = True
            open_orders = []
            notes.append(f'open_orders_query_failed:{_compact_exception_detail(exc)}')

        primary_request_context_rows = [
            row for row in request_context_rows if not bool(row['request_metadata'].get('protective_order'))
        ]
        primary_client_order_ids = {str(row['request'].client_order_id) for row in primary_request_context_rows if getattr(row['request'], 'client_order_id', None)}
        primary_fills = [
            fill for fill in fills if not primary_client_order_ids or (fill.client_order_id in primary_client_order_ids)
        ]
        protective_open_orders, regular_open_orders = split_open_orders(open_orders)
        protective_snapshot = snapshot_protective_orders(protective_open_orders)
        protective_order_requested = any(
            bool(row['request_metadata'].get('protective_order'))
            for row in request_context_rows
        )
        protective_only_requests = bool(order_requests) and all(
            bool(row['request_metadata'].get('protective_order'))
            for row in request_context_rows
        )
        close_position_only_requests = bool(order_requests) and all(
            bool(row['is_close_position_request'])
            for row in request_context_rows
        )
        protective_algo_lookup_expected = bool(protective_order_requested)

        order_status = self._aggregate_order_status(primary_order_statuses or order_statuses)
        executed_qty = sum(fill.qty for fill in primary_fills)
        fee_total = sum(fill.fee for fill in primary_fills)
        fee_assets = sorted({fill.fee_asset for fill in primary_fills if fill.fee_asset})
        avg_fill_price = None
        if executed_qty > 0:
            avg_fill_price = sum(fill.qty * fill.price for fill in primary_fills) / executed_qty

        requested_qty = sum(float(getattr(row['request'], 'quantity', 0.0) or 0.0) for row in primary_request_context_rows)
        requested_reduce_only = all(bool(getattr(row['request'], 'reduce_only', False)) for row in primary_request_context_rows) if primary_request_context_rows else False
        protective_summary, protective_ok, protective_freeze_reason, protective_notes, protective_phase_deferred = self._build_protective_validation(
            order_requests=order_requests,
            post_position=post_position,
            open_orders=protective_open_orders,
        )

        protective_order_lookup_missing_fallback = bool(
            query_failed
            and order_lookup_missing_only
            and protective_algo_lookup_expected
            and close_position_only_requests
            and protective_ok
            and bool(protective_snapshot.orders)
        )
        if protective_order_lookup_missing_fallback:
            order_statuses = [self._normalize_order_status(item.get('status')) for item in protective_snapshot.orders]
            order_status = self._aggregate_order_status(order_statuses)
            query_failed = False
            notes.append('close_position_protection_snapshot_reused_after_order_missing_lookup')
            for item in protective_snapshot.orders:
                if item.get('order_id'):
                    exchange_order_ids.append(str(item.get('order_id')))
            dedup_order_ids = sorted(set(exchange_order_ids))

        validation = self._validate_confirmation(
            order_requests=order_requests,
            order_status=order_status,
            executed_qty=executed_qty,
            avg_fill_price=avg_fill_price,
            fee_assets=fee_assets,
            fills=primary_fills,
            post_position=post_position,
            open_orders=regular_open_orders,
            notes=notes,
        )
        query_failed = query_failed or validation['query_failed']
        should_freeze = validation['should_freeze']
        freeze_reason = validation['freeze_reason']

        position_resolution = self.position_fact_reconciler.resolve_posttrade(
            order_status=order_status,
            position=post_position,
            open_orders=open_orders,
            executed_qty=executed_qty,
            fill_count=len(primary_fills),
            requested_reduce_only=requested_reduce_only,
        )
        protective_submit_acked = any(
            bool((row['receipt'] is not None) and row['receipt'].acknowledged)
            for row in request_context_rows
            if bool(row['request_metadata'].get('protective_order'))
        )
        protective_request_client_ids = {
            str(request.client_order_id)
            for request in order_requests
            if getattr(request, 'client_order_id', None)
            and bool((dict(getattr(request, 'metadata', {}) or {})).get('protective_order'))
        }
        protective_receipt_order_ids = {
            str(row['receipt'].exchange_order_id)
            for row in request_context_rows
            if bool(row['request_metadata'].get('protective_order'))
            and row['receipt'] is not None
            and row['receipt'].exchange_order_id is not None
        }
        requested_protective_orders = [
            item
            for item in protective_snapshot.orders
            if str(item.get('client_order_id') or '') in protective_request_client_ids
            or str(item.get('order_id') or '') in protective_receipt_order_ids
            or str(item.get('order_id') or '') in set(dedup_order_ids)
        ]
        protective_exchange_visible = bool(requested_protective_orders)
        protective_requested_open_order_visible = bool(
            not requested_protective_orders
            and protective_order_requested
            and close_position_only_requests
            and post_position.side in {'long', 'short'}
            and float(post_position.qty or 0.0) > 0.0
            and protective_snapshot.orders
            and all(
                bool(item.get('close_position'))
                and str(item.get('status') or '').upper() in BINANCE_PENDING_ORDER_STATUSES
                for item in protective_snapshot.orders
            )
        )
        if protective_requested_open_order_visible:
            requested_protective_orders = list(protective_snapshot.orders)
            protective_exchange_visible = True
        protective_exchange_visible_confirmed = bool(
            protective_only_requests
            and protective_algo_lookup_expected
            and not protective_phase_deferred
            and protective_exchange_visible
            and all(
                bool(item.get('close_position'))
                and str(item.get('status') or '').upper() in BINANCE_PENDING_ORDER_STATUSES
                for item in requested_protective_orders
            )
            and post_position.side in {'long', 'short'}
            and float(post_position.qty or 0.0) > 0.0
        )
        protective_lookup_missing_only = bool(
            protective_algo_lookup_expected
            and (protective_algo_lookup_not_found or order_lookup_missing_only)
            and not protective_exchange_visible
            and protective_freeze_reason == 'protective_order_missing'
        )
        protective_pending_confirm = bool(
            protective_order_requested
            and protective_submit_acked
            and not protective_phase_deferred
            and post_position.side in {'long', 'short'}
            and float(post_position.qty or 0.0) > 0.0
            and not protective_exchange_visible_confirmed
            and (
                (
                    not protective_exchange_visible
                    and not protective_ok
                    and protective_freeze_reason == 'protective_order_missing'
                )
                or protective_lookup_missing_only
            )
        )
        protective_position_preserved = bool(
            (protective_pending_confirm or protective_exchange_visible)
            and post_position.side in {'long', 'short'}
            and float(post_position.qty or 0.0) > 0.0
        )
        protective_algo_visible = any(bool(item.get('is_algo_order')) for item in requested_protective_orders or protective_snapshot.orders)
        protective_exchange_source = ('algo_order' if protective_algo_visible else ('open_orders' if protective_snapshot.orders else None))
        if protective_exchange_source is None and algo_fact_rows:
            protective_exchange_source = 'algo_order'
        protective_exchange_visibility = {
            'exchange_visible': protective_exchange_visible,
            'confirmed_via_exchange_visibility': protective_exchange_visible_confirmed,
            'source': protective_exchange_source,
            'fallback_from_order_lookup_missing': protective_order_lookup_missing_fallback,
            'requested_open_order_visible': protective_requested_open_order_visible,
            'order_lookup_missing_only': protective_lookup_missing_only,
            'plain_order_lookup_not_found': protective_plain_lookup_not_found,
            'algo_lookup_not_found': protective_algo_lookup_not_found,
            'algo_lookup_expected': protective_algo_lookup_expected,
            'order_count': len(requested_protective_orders or protective_snapshot.orders),
        }
        normalized_protective_ok = protective_ok
        normalized_protective_freeze_reason = protective_freeze_reason
        normalized_protective_status = protective_summary.get('status') if isinstance(protective_summary, dict) else None
        normalized_protective_validation_level = protective_summary.get('validation_level') if isinstance(protective_summary, dict) else None
        normalized_protective_notes = list(protective_notes)
        if protective_exchange_visible_confirmed:
            normalized_protective_ok = True
            normalized_protective_freeze_reason = None
            normalized_protective_status = 'OK'
            normalized_protective_validation_level = 'EXCHANGE_VISIBLE'
            normalized_protective_notes = [note for note in normalized_protective_notes if not str(note).startswith('missing:')]
        elif protective_pending_confirm:
            normalized_protective_ok = True
            normalized_protective_freeze_reason = None
            normalized_protective_status = 'PENDING_CONFIRM'
            normalized_protective_validation_level = 'PENDING_CONFIRM'
        strategy = None
        stop_price = None
        tp_price = None
        for request in order_requests:
            metadata = dict(getattr(request, 'metadata', {}) or {})
            strategy = metadata.get('strategy') or strategy
            stop_price = metadata.get('protective_stop_price', metadata.get('stop_price', stop_price))
            tp_price = metadata.get('protective_tp_price', tp_price)
        notes.extend(protective_notes)

        if order_status in {ORDER_STATUS_CANCELED, ORDER_STATUS_REJECTED}:
            query_failed = False
            confirmation_status = 'UNCONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_REJECTED
            reconcile_status = RECONCILE_MISMATCH
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_rejected_or_canceled'
        elif protective_exchange_visible_confirmed:
            confirmation_status = 'CONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_CONFIRMED
            reconcile_status = RECONCILE_OK
            should_freeze = False
            freeze_reason = None
            notes.append('close_position_protection_exchange_visible_confirmed')
            if protective_algo_visible:
                notes.append('protective_algo_exchange_visible_confirmed')
        elif order_status in {ORDER_STATUS_PARTIALLY_FILLED, ORDER_STATUS_PENDING, ORDER_STATUS_SUBMITTED} and not query_failed:
            if protective_pending_confirm and (position_resolution.position_confirmed or protective_position_preserved):
                confirmation_status = 'POSITION_CONFIRMED'
                confirmation_category = CONFIRM_CATEGORY_POSITION_CONFIRMED
                reconcile_status = RECONCILE_PENDING
                should_freeze = False
                freeze_reason = None
                notes.append('protective_orders_pending_exchange_confirm')
            else:
                confirmation_status = 'PENDING'
                confirmation_category = CONFIRM_CATEGORY_PENDING
                reconcile_status = RECONCILE_PENDING
                should_freeze = True
                freeze_reason = freeze_reason or 'posttrade_pending_confirmation'
        elif query_failed:
            if (position_resolution.position_confirmed or protective_position_preserved) and (protective_ok or protective_phase_deferred or protective_pending_confirm):
                confirmation_status = 'POSITION_CONFIRMED'
                confirmation_category = CONFIRM_CATEGORY_POSITION_CONFIRMED
                reconcile_status = RECONCILE_OK
                should_freeze = False
                freeze_reason = None
                notes.append('position_confirmed_without_trade_rows')
                if protective_pending_confirm:
                    notes.append('protective_orders_pending_exchange_confirm')
            else:
                confirmation_status = 'UNCONFIRMED'
                confirmation_category = CONFIRM_CATEGORY_QUERY_FAILED
                reconcile_status = RECONCILE_QUERY_FAILED
                should_freeze = True
                freeze_reason = freeze_reason or protective_freeze_reason or 'posttrade_query_failed'
        elif order_status == ORDER_STATUS_PARTIALLY_FILLED and position_resolution.position_confirmed:
            confirmation_status = 'POSITION_CONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_POSITION_CONFIRMED
            reconcile_status = RECONCILE_PENDING
            should_freeze = False
            freeze_reason = None
            notes.append('partial_fill_position_working')
        elif order_status == ORDER_STATUS_FILLED and not should_freeze:
            if protective_pending_confirm and (position_resolution.position_confirmed or protective_position_preserved):
                confirmation_status = 'POSITION_CONFIRMED'
                confirmation_category = CONFIRM_CATEGORY_POSITION_CONFIRMED
                reconcile_status = RECONCILE_PENDING
                should_freeze = False
                freeze_reason = None
                notes.append('protective_orders_pending_exchange_confirm')
            else:
                confirmation_status = 'CONFIRMED'
                confirmation_category = CONFIRM_CATEGORY_CONFIRMED
                reconcile_status = RECONCILE_OK
                freeze_reason = None
        else:
            if protective_pending_confirm and (position_resolution.position_confirmed or protective_position_preserved):
                confirmation_status = 'POSITION_CONFIRMED'
                confirmation_category = CONFIRM_CATEGORY_POSITION_CONFIRMED
                reconcile_status = RECONCILE_PENDING
                should_freeze = False
                freeze_reason = None
                notes.append('protective_orders_pending_exchange_confirm')
                notes.append('protective_pending_confirm_prevents_mismatch_freeze')
            else:
                confirmation_status = 'UNCONFIRMED'
                confirmation_category = CONFIRM_CATEGORY_MISMATCH
                reconcile_status = RECONCILE_MISMATCH
                should_freeze = True
                freeze_reason = freeze_reason or 'posttrade_unknown_state'

        is_open_like_request = any(not bool(getattr(request, 'reduce_only', False)) for request in order_requests)
        if not protective_ok and not protective_phase_deferred and not protective_order_requested and is_open_like_request and confirmation_category in {CONFIRM_CATEGORY_CONFIRMED, CONFIRM_CATEGORY_POSITION_CONFIRMED}:
            confirmation_status = 'UNCONFIRMED'
            confirmation_category = CONFIRM_CATEGORY_MISMATCH
            reconcile_status = RECONCILE_MISMATCH
            should_freeze = True
            freeze_reason = protective_freeze_reason or 'protective_order_mismatch'
            notes.append('protective_orders_invalid_after_fill')

        protective_intent_payload = build_strategy_protection_intent(
            runtime_mode='FROZEN' if should_freeze else 'ACTIVE',
            position_side=post_position.side,
            position_qty=post_position.qty,
            active_strategy=strategy,
            stop_price=stop_price,
            tp_price=tp_price,
            pending_execution_phase=(
                'entry_confirmed_pending_protective'
                if protective_phase_deferred
                else ('protection_pending_confirm' if protective_pending_confirm else None)
            ),
            pending_execution_block_reason=freeze_reason or (None if protective_pending_confirm else protective_freeze_reason),
            protective_order_status=(
                'PENDING_SUBMIT'
                if protective_phase_deferred and post_position.side in {'long', 'short'} and float(post_position.qty or 0.0) > 0.0
                else ('PENDING_CONFIRM' if protective_pending_confirm else ('ACTIVE' if protective_ok and protective_snapshot.orders else ('MISSING' if post_position.side in {'long', 'short'} and float(post_position.qty or 0.0) > 0.0 else ('UNEXPECTED_WHILE_FLAT' if protective_snapshot.orders else 'NONE'))))
            ),
            protective_phase_status=('DEFERRED' if protective_phase_deferred else ('PENDING_CONFIRM' if protective_pending_confirm else ('FROZEN' if should_freeze else 'NONE'))),
            protective_orders=protective_snapshot.orders,
            protective_validation={
                'ok': normalized_protective_ok,
                'freeze_reason': normalized_protective_freeze_reason,
                'status': normalized_protective_status,
                'validation_level': normalized_protective_validation_level,
                'risk_class': protective_summary.get('risk_class') if isinstance(protective_summary, dict) else None,
                'mismatch_class': protective_summary.get('mismatch_class') if isinstance(protective_summary, dict) else None,
                'notes': normalized_protective_notes,
                'summary': protective_summary,
                'phase_deferred': protective_phase_deferred,
            },
            confirmation_category=confirmation_category,
            freeze_reason=freeze_reason,
            last_eval_ts=getattr(market, 'decision_ts', None),
        )
        protective_intent_state = protective_intent_payload.get('intent_state')

        if should_freeze and freeze_reason:
            notes.append(f'block_reason:{freeze_reason}')

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
            for item in primary_fills
        ]
        notes = sorted(set(notes))

        return PostTradeConfirmation(
            confirmation_status=confirmation_status,
            confirmation_category=confirmation_category,
            order_status=order_status,
            exchange_order_ids=dedup_order_ids,
            executed_qty=executed_qty,
            avg_fill_price=avg_fill_price,
            fees=fee_total,
            fee_assets=fee_assets,
            fill_count=len(primary_fills),
            post_position_side=post_position.side,
            post_position_qty=post_position.qty,
            post_entry_price=post_position.entry_price,
            reconcile_status=reconcile_status,
            should_freeze=should_freeze,
            freeze_reason=freeze_reason,
            notes=notes,
            trade_summary={
                'fills_count': len(primary_fills),
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
                'protective_orders_count': len(protective_snapshot.orders),
                'protective_orders': protective_snapshot.orders,
                'has_protective_orders': bool(protective_snapshot.orders),
                'protective_validation': {
                    'ok': normalized_protective_ok,
                    'freeze_reason': normalized_protective_freeze_reason,
                    'status': normalized_protective_status,
                    'validation_level': normalized_protective_validation_level,
                    'notes': normalized_protective_notes,
                    'summary': protective_summary,
                    'phase_deferred': protective_phase_deferred,
                    'pending_confirm': protective_pending_confirm,
                    'intent_state': protective_intent_state,
                    'intent_status': protective_intent_payload.get('intent_status'),
                    'pending_action': protective_intent_payload.get('pending_action'),
                    'expected_protection': protective_intent_payload.get('expected_protection'),
                    'exchange_visibility': protective_exchange_visibility,
                },
                'protective_exchange_visibility': protective_exchange_visibility,
                'protective_phase_deferred': protective_phase_deferred,
                'protective_order_requested': protective_order_requested,
                'protective_pending_confirm': protective_pending_confirm,
                'order_facts': order_fact_rows,
                'algo_order_facts': algo_fact_rows,
                'fills': fill_rows,
                'primary_order_ids': dedup_primary_order_ids,
                'protective_algo_order_ids': sorted({str(item.get('exchange_order_id')) for item in algo_fact_rows if item.get('exchange_order_id')}),
                'fee_total_mixed_asset_sum': fee_total,
                'fee_asset_count': len(fee_assets),
                'freeze_rule': freeze_reason,
                'position_fact_resolution': {
                    'position_confirmed': position_resolution.position_confirmed,
                    'side': position_resolution.side,
                    'qty': position_resolution.qty,
                    'entry_price': position_resolution.entry_price,
                    'open_orders_conflict': position_resolution.open_orders_conflict,
                    'needs_trade_reconciliation': position_resolution.needs_trade_reconciliation,
                    'reason': position_resolution.reason,
                },
                'notes': notes,
            },
        )

    def _collect_open_orders(self, symbol: str, order_requests: list[OrderRequestLike] | None = None) -> list[OrderSnapshot]:
        getter = getattr(self.readonly_client, 'get_open_orders', None)
        if not callable(getter):
            return []
        protection_ids = [
            str(getattr(request, 'client_order_id', None))
            for request in (order_requests or [])
            if getattr(request, 'client_order_id', None)
            and bool((dict(getattr(request, 'metadata', {}) or {})).get('protective_order'))
        ]
        try:
            rows = getter(symbol, client_order_ids=protection_ids)
        except TypeError:
            rows = getter(symbol)
        return [item for item in rows if getattr(item, 'order_id', None)]

    @staticmethod
    def _build_protective_validation(
        *,
        order_requests: list[OrderRequestLike],
        post_position: PositionSnapshot,
        open_orders: list[OrderSnapshot],
    ) -> tuple[dict[str, Any], bool, str | None, list[str], bool]:
        strategy = None
        stop_price = None
        tp_price = None
        protective_order_requested = False
        for request in order_requests:
            metadata = dict(getattr(request, 'metadata', {}) or {})
            strategy = metadata.get('strategy') or strategy
            stop_price = metadata.get('protective_stop_price', metadata.get('stop_price', stop_price))
            tp_price = metadata.get('protective_tp_price', tp_price)
            protective_order_requested = protective_order_requested or bool(metadata.get('protective_order'))

        # Entry phase 只确认真实持仓；如果本轮 submit 中根本没提交 protective order，
        # 则保护单校验延后到 protective phase，避免把两阶段流程误判为缺保护单。
        if (
            not protective_order_requested
            and strategy in {'trend', 'rev'}
            and post_position.side in {'long', 'short'}
            and float(post_position.qty or 0.0) > 0.0
            and stop_price is not None
        ):
            notes = ['protective_phase_deferred_until_position_rebuilt']
            return (
                {
                    'expected': [],
                    'actual': [self._serialize_open_order(item) for item in open_orders],
                    'protective_order_count': len(open_orders),
                    'status': 'DEFERRED',
                },
                True,
                None,
                notes,
                True,
            )

        validation = validate_protective_orders(
            strategy=strategy,
            position_side=post_position.side,
            position_qty=float(post_position.qty or 0.0),
            stop_price=(float(stop_price) if stop_price is not None else None),
            tp_price=(float(tp_price) if tp_price is not None else None),
            open_orders=open_orders,
        )
        return validation.summary, validation.ok, validation.freeze_reason, validation.notes, False

    def _get_algo_order_snapshot(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> OrderSnapshot:
        getter = getattr(self.readonly_client, '_get_algo_order', None)
        parser = getattr(self.readonly_client, '_parse_order_snapshot', None)
        if not callable(getter):
            raise RuntimeError('algo order lookup unavailable')
        row = getter(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
        if not isinstance(row, dict) or not row:
            raise RuntimeError('algo order not found')
        if callable(parser):
            return parser(row)
        return self.readonly_client._parse_order_snapshot(row)

    @staticmethod
    def _is_algo_order_lookup_not_found_error(exc: Exception) -> bool:
        return _is_algo_order_lookup_not_found_error(exc)

    @staticmethod
    def _serialize_open_order(order: OrderSnapshot) -> dict[str, Any]:
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
            'reduce_only': getattr(order, 'reduce_only', None),
            'close_position': getattr(order, 'close_position', None),
            'update_time_ms': getattr(order, 'update_time_ms', None),
        }

    def _collect_fills(
        self,
        symbol: str,
        exchange_order_ids: Iterable[str],
        *,
        order_fact_rows: Iterable[dict[str, Any]] | None = None,
    ) -> list[ConfirmedTradeFill]:
        order_ids = [str(item) for item in exchange_order_ids if item]
        matched: list[ConfirmedTradeFill] = []
        if order_ids:
            seen_trade_ids: set[str] = set()
            order_fact_index = {
                str(row.get('exchange_order_id')): row
                for row in (order_fact_rows or [])
                if row.get('exchange_order_id') is not None
            }
            for order_id in order_ids:
                rows = self.readonly_client.get_recent_trades(symbol=symbol, limit=TRADE_RECENT_LIMIT, order_id=order_id)
                if not rows:
                    rows = self._lookup_recent_trades_with_time_window(
                        symbol=symbol,
                        order_id=order_id,
                        order_fact=order_fact_index.get(str(order_id)),
                    )
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

    def _lookup_recent_trades_with_time_window(
        self,
        *,
        symbol: str,
        order_id: str,
        order_fact: dict[str, Any] | None,
    ) -> list[UserTradeSnapshot]:
        update_time_ms = order_fact.get('update_time_ms') if isinstance(order_fact, dict) else None
        if update_time_ms in (None, '', 'NULL'):
            return []
        center_ms = int(update_time_ms)
        start_time_ms = max(0, center_ms - TRADE_LOOKBACK_WINDOW_MS)
        end_time_ms = center_ms + TRADE_FORWARD_WINDOW_MS
        rows = self.readonly_client.get_recent_trades(
            symbol=symbol,
            limit=TRADE_RECENT_LIMIT,
            order_id=order_id,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        matched: list[UserTradeSnapshot] = []
        for row in rows:
            trade = self._coerce_trade_snapshot(row)
            if str(trade.order_id or '') == str(order_id):
                matched.append(trade)
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

        # Terminal rejects/cancels are authoritative classifications; keep them out of query-failed.
        if order_status in {ORDER_STATUS_CANCELED, ORDER_STATUS_REJECTED}:
            query_failed = False

        if order_status == ORDER_STATUS_PARTIALLY_FILLED:
            notes.append('partial_fill_detected')

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

        if order_status == ORDER_STATUS_PENDING and pending_open_orders:
            should_freeze = True
            freeze_reason = freeze_reason or 'posttrade_open_orders_still_pending'
            notes.append('open_orders_still_pending')
            # Live open orders confirm the order is still working, not unknown.
            query_failed = False
        if order_status == ORDER_STATUS_PARTIALLY_FILLED and pending_open_orders:
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
            # Reduce-only close can legitimately flatten first and backfill trades later.
            if requested_reduce_only and post_position.qty <= qty_tolerance:
                notes.append('filled_qty_pending_trade_rows_after_flatten')
            else:
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
