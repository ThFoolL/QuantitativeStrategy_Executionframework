from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError

from .runtime_env import BinanceEnvConfig


@dataclass(frozen=True)
class ExchangeSymbolRules:
    symbol: str
    price_tick: float | None
    qty_step: float | None
    min_qty: float | None
    min_notional: float | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class AccountSnapshot:
    account_equity: float
    available_margin: float
    raw: dict[str, Any]
    validity_status: str = 'OK'
    invalid_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    side: str | None
    qty: float
    entry_price: float | None
    break_even_price: float | None = None
    mark_price: float | None = None
    unrealized_pnl: float | None = None
    leverage: float | None = None
    margin_type: str | None = None
    position_side_mode: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderSnapshot:
    order_id: str
    client_order_id: str | None
    status: str
    type: str | None = None
    time_in_force: str | None = None
    side: str | None = None
    position_side: str | None = None
    qty: float | None = None
    executed_qty: float | None = None
    price: float | None = None
    avg_price: float | None = None
    cum_quote: float | None = None
    stop_price: float | None = None
    working_type: str | None = None
    orig_type: str | None = None
    activate_price: float | None = None
    price_protect: bool | None = None
    reduce_only: bool | None = None
    close_position: bool | None = None
    update_time_ms: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class UserTradeSnapshot:
    trade_id: str
    order_id: str
    client_order_id: str | None
    symbol: str | None
    side: str | None
    position_side: str | None
    qty: float
    price: float
    realized_pnl: float | None
    fee: float
    fee_asset: str | None
    maker: bool | None
    buyer: bool | None
    time_ms: int | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class KlineSnapshot:
    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float | None
    trade_count: int | None
    taker_buy_base_volume: float | None
    taker_buy_quote_volume: float | None
    is_closed: bool
    raw: list[Any]

    @property
    def open_time_iso(self) -> str:
        return datetime.fromtimestamp(self.open_time_ms / 1000.0, tz=timezone.utc).isoformat()

    @property
    def close_time_iso(self) -> str:
        return datetime.fromtimestamp(self.close_time_ms / 1000.0, tz=timezone.utc).isoformat()


class BinanceReadOnlyClient:
    """Binance futures 只读客户端。

    设计目标：
    - 仅覆盖 public market data + signed readonly account/order/trade 查询
    - 保留原始 payload，避免后续字段口径分歧时无法追查
    - 不提供任何真实下单能力
    """

    def __init__(self, config: BinanceEnvConfig, *, timeout_seconds: float = 10.0, recv_window_ms: int | None = None):
        self.config = config
        self.timeout_seconds = timeout_seconds
        self.recv_window_ms = int(recv_window_ms if recv_window_ms is not None else config.recv_window_ms)
        self._server_time_offset_ms = 0
        self._server_time_offset_synced = False

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000) + self._server_time_offset_ms

    def _sign_params(self, params: dict[str, Any]) -> str:
        query = urllib.parse.urlencode(params, doseq=True)
        signature = hmac.new(
            self.config.api_secret.encode('utf-8'),
            query.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()
        return f'{query}&signature={signature}'

    def _request_json(self, path: str, *, params: dict[str, Any] | None = None, signed: bool = False) -> Any:
        _, _, payload = self.request_with_meta(path, params=params, signed=signed)
        return payload

    def _sync_server_time_offset(self) -> int:
        payload = self._request_json('/fapi/v1/time', signed=False)
        server_time_ms = int(payload['serverTime'])
        self._server_time_offset_ms = server_time_ms - int(time.time() * 1000)
        self._server_time_offset_synced = True
        return self._server_time_offset_ms

    @staticmethod
    def _is_timestamp_window_error(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        try:
            return int(payload.get('code')) == -1021
        except (TypeError, ValueError):
            return False

    def request_with_meta(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> tuple[int, str, Any]:
        params = dict(params or {})
        if signed:
            params.setdefault('timestamp', self._timestamp_ms())
            params.setdefault('recvWindow', self.recv_window_ms)
            query = self._sign_params(params)
        else:
            query = urllib.parse.urlencode(params, doseq=True)

        url = f'{self.config.base_url}{path}'
        if query:
            url = f'{url}?{query}'

        request = urllib.request.Request(url)
        request.add_header('Accept', 'application/json')
        if signed:
            request.add_header('X-MBX-APIKEY', self.config.api_key)

        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload_text = response.read().decode('utf-8')
                payload = json.loads(payload_text)
                return int(getattr(response, 'status', 200) or 200), url, payload
        except HTTPError as exc:
            payload_text = exc.read().decode('utf-8', errors='replace') if exc.fp else ''
            try:
                payload = json.loads(payload_text) if payload_text else {'message': str(exc)}
            except json.JSONDecodeError:
                payload = {'message': payload_text or str(exc)}
            if signed and self._is_timestamp_window_error(payload):
                self._sync_server_time_offset()
                retry_params = dict(params)
                retry_params['timestamp'] = self._timestamp_ms()
                retry_params.setdefault('recvWindow', self.recv_window_ms)
                retry_query = self._sign_params(retry_params)
                retry_url = f'{self.config.base_url}{path}?{retry_query}'
                retry_request = urllib.request.Request(retry_url)
                retry_request.add_header('Accept', 'application/json')
                retry_request.add_header('X-MBX-APIKEY', self.config.api_key)
                try:
                    with urllib.request.urlopen(retry_request, timeout=self.timeout_seconds) as response:
                        payload_text = response.read().decode('utf-8')
                        retry_payload = json.loads(payload_text)
                        return int(getattr(response, 'status', 200) or 200), retry_url, retry_payload
                except HTTPError as retry_exc:
                    retry_payload_text = retry_exc.read().decode('utf-8', errors='replace') if retry_exc.fp else ''
                    try:
                        payload = json.loads(retry_payload_text) if retry_payload_text else {'message': str(retry_exc)}
                    except json.JSONDecodeError:
                        payload = {'message': retry_payload_text or str(retry_exc)}
                    exc = retry_exc
            raise RuntimeError(
                json.dumps(
                    {
                        'kind': 'http_error',
                        'path': path,
                        'status': exc.code,
                        'payload': payload,
                    },
                    ensure_ascii=False,
                )
            ) from exc
        except URLError as exc:
            raise RuntimeError(
                json.dumps(
                    {
                        'kind': 'network_error',
                        'path': path,
                        'reason': str(getattr(exc, 'reason', exc)),
                    },
                    ensure_ascii=False,
                )
            ) from exc

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, '', 'NULL'):
            return None
        return float(value)

    @classmethod
    def _pick_first_valid_float(cls, payload: dict[str, Any], *keys: str) -> tuple[float | None, str | None]:
        for key in keys:
            value = cls._safe_float(payload.get(key))
            if value is not None:
                return value, key
        return None, None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value in (None, '', 'NULL'):
            return None
        return int(value)

    @staticmethod
    def _normalize_side(value: Any) -> str | None:
        if not value:
            return None
        text = str(value).upper()
        if text == 'BUY':
            return 'buy'
        if text == 'SELL':
            return 'sell'
        if text == 'LONG':
            return 'long'
        if text == 'SHORT':
            return 'short'
        if text == 'BOTH':
            return 'both'
        return str(value).lower()

    def get_server_time_ms(self) -> int:
        payload = self._request_json('/fapi/v1/time', signed=False)
        return int(payload['serverTime'])

    def get_server_time_iso(self) -> str:
        return datetime.fromtimestamp(self.get_server_time_ms() / 1000.0, tz=timezone.utc).isoformat()

    def get_exchange_info(self, symbol: str | None = None) -> ExchangeSymbolRules:
        resolved_symbol = symbol or self.config.symbol
        payload = self._request_json('/fapi/v1/exchangeInfo', params={'symbol': resolved_symbol}, signed=False)
        symbol_rows = payload.get('symbols', [])
        if not symbol_rows:
            raise ValueError(f'symbol not found in exchange info: {resolved_symbol}')
        row = next((item for item in symbol_rows if str(item.get('symbol')) == resolved_symbol), None)
        if row is None:
            raise ValueError(f'symbol not found in exchange info payload: {resolved_symbol}')
        filters = {item.get('filterType'): item for item in row.get('filters', [])}
        price_filter = filters.get('PRICE_FILTER', {})
        lot_size_filter = filters.get('LOT_SIZE', {})
        min_notional_filter = filters.get('MIN_NOTIONAL', {})
        return ExchangeSymbolRules(
            symbol=resolved_symbol,
            price_tick=self._safe_float(price_filter.get('tickSize')),
            qty_step=self._safe_float(lot_size_filter.get('stepSize')),
            min_qty=self._safe_float(lot_size_filter.get('minQty')),
            min_notional=self._safe_float(min_notional_filter.get('notional')),
            raw=row,
        )

    def get_klines(
        self,
        symbol: str | None = None,
        *,
        interval: str = '5m',
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[KlineSnapshot]:
        resolved_symbol = symbol or self.config.symbol
        params: dict[str, Any] = {
            'symbol': resolved_symbol,
            'interval': interval,
            'limit': limit,
        }
        if start_time_ms is not None:
            params['startTime'] = int(start_time_ms)
        if end_time_ms is not None:
            params['endTime'] = int(end_time_ms)
        payload = self._request_json('/fapi/v1/klines', params=params, signed=False)
        now_ms = self.get_server_time_ms()
        out: list[KlineSnapshot] = []
        for row in payload:
            close_time_ms = int(row[6])
            out.append(
                KlineSnapshot(
                    symbol=resolved_symbol,
                    interval=interval,
                    open_time_ms=int(row[0]),
                    close_time_ms=close_time_ms,
                    open_price=float(row[1]),
                    high_price=float(row[2]),
                    low_price=float(row[3]),
                    close_price=float(row[4]),
                    volume=float(row[5]),
                    quote_volume=self._safe_float(row[7]),
                    trade_count=self._safe_int(row[8]),
                    taker_buy_base_volume=self._safe_float(row[9]),
                    taker_buy_quote_volume=self._safe_float(row[10]),
                    is_closed=close_time_ms < now_ms,
                    raw=list(row),
                )
            )
        return out

    def get_account_snapshot(self) -> AccountSnapshot:
        payload = self._request_json('/fapi/v2/account', signed=True)
        multi_assets_margin = bool(payload.get('multiAssetsMargin'))

        account_equity, account_equity_source = self._pick_first_valid_float(
            payload,
            'totalWalletBalance',
            'totalMarginBalance',
            'totalCrossWalletBalance',
        )
        available_margin, available_margin_source = self._pick_first_valid_float(
            payload,
            'availableBalance',
            'totalAvailableBalance',
            'maxWithdrawAmount',
        )

        invalid_reasons: list[str] = []
        if account_equity is None:
            invalid_reasons.append('account_equity_missing_or_non_numeric')
        elif account_equity <= 0:
            invalid_reasons.append('account_equity_non_positive')
        if available_margin is None:
            invalid_reasons.append('available_margin_missing_or_non_numeric')
        elif available_margin <= 0:
            invalid_reasons.append('available_margin_non_positive')

        raw_payload = dict(payload)
        raw_payload['_account_snapshot_sources'] = {
            'account_equity': account_equity_source,
            'available_margin': available_margin_source,
            'multi_assets_margin': multi_assets_margin,
        }

        return AccountSnapshot(
            account_equity=float(account_equity or 0.0),
            available_margin=float(available_margin or 0.0),
            raw=raw_payload,
            validity_status='INVALID' if invalid_reasons else 'OK',
            invalid_reasons=tuple(invalid_reasons),
        )

    def get_position_risk_rows(self, symbol: str | None = None) -> list[dict[str, Any]]:
        resolved_symbol = symbol or self.config.symbol
        payload = self._request_json('/fapi/v2/positionRisk', signed=True)
        return [row for row in payload if row.get('symbol') == resolved_symbol]

    def get_position_mode(self) -> dict[str, Any]:
        payload = self._request_json('/fapi/v1/positionSide/dual', signed=True)
        return dict(payload)

    def get_position_snapshot(self, symbol: str | None = None) -> PositionSnapshot:
        resolved_symbol = symbol or self.config.symbol
        matches = self.get_position_risk_rows(resolved_symbol)
        if not matches:
            return PositionSnapshot(
                symbol=resolved_symbol,
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

        selected = self._select_position_row(matches)
        amt = float(selected.get('positionAmt', 0.0))
        side = None
        if amt > 0:
            side = 'long'
        elif amt < 0:
            side = 'short'
        else:
            explicit_side = self._normalize_side(selected.get('positionSide'))
            if explicit_side in {'long', 'short'}:
                side = explicit_side
        return PositionSnapshot(
            symbol=resolved_symbol,
            side=side,
            qty=abs(amt),
            entry_price=self._safe_float(selected.get('entryPrice')),
            break_even_price=self._safe_float(selected.get('breakEvenPrice')),
            mark_price=self._safe_float(selected.get('markPrice')),
            unrealized_pnl=self._safe_float(selected.get('unRealizedProfit')),
            leverage=self._safe_float(selected.get('leverage')),
            margin_type=(str(selected.get('marginType')).lower() if selected.get('marginType') else None),
            position_side_mode=self._derive_position_side_mode(matches),
            raw=selected,
        )

    def _derive_position_side_mode(self, rows: list[dict[str, Any]]) -> str | None:
        position_sides = {str(row.get('positionSide', '')).upper() for row in rows if row.get('positionSide')}
        if {'LONG', 'SHORT'} & position_sides:
            return 'hedge'
        if 'BOTH' in position_sides:
            return 'one_way'
        return None

    def _select_position_row(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        non_zero = [row for row in rows if abs(float(row.get('positionAmt', 0.0))) > 0]
        if len(non_zero) == 1:
            return non_zero[0]
        if len(non_zero) > 1:
            raise ValueError('multiple non-zero positionRisk rows found; hedge-mode aggregation not supported yet')
        preferred = [row for row in rows if str(row.get('positionSide', '')).upper() == 'BOTH']
        if preferred:
            return preferred[0]
        return rows[0]

    def get_open_orders(
        self,
        symbol: str | None = None,
        *,
        order_ids: list[str | int] | tuple[str | int, ...] | set[str | int] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> list[OrderSnapshot]:
        resolved_symbol = symbol or self.config.symbol
        payload = self._request_json('/fapi/v1/openOrders', params={'symbol': resolved_symbol}, signed=True)
        rows = [self._parse_order_snapshot(row) for row in payload]

        # Binance algo protective orders may not show up in the regular openOrders view.
        # Only merge algo rows that still look active; precise lookup for terminal/missing
        # algo orders belongs to get_order(), not the open-orders view.
        algo_rows = self._get_algo_orders(symbol=resolved_symbol, order_ids=order_ids, client_order_ids=client_order_ids)
        rows.extend(
            self._parse_order_snapshot(row)
            for row in algo_rows
            if self._is_active_algo_order_row(row)
        )
        return self._dedupe_order_snapshots(rows)

    def get_order(self, *, symbol: str | None = None, order_id: str | int | None = None, client_order_id: str | None = None) -> OrderSnapshot:
        resolved_symbol = symbol or self.config.symbol
        if order_id is None and client_order_id is None:
            raise ValueError('either order_id or client_order_id is required')

        params: dict[str, Any] = {'symbol': resolved_symbol}
        if order_id is not None:
            params['orderId'] = order_id
        elif client_order_id is not None:
            params['origClientOrderId'] = client_order_id

        try:
            payload = self._request_json('/fapi/v1/order', params=params, signed=True)
            return self._parse_order_snapshot(payload)
        except Exception as exc:
            algo_payload = self._get_algo_order(symbol=resolved_symbol, order_id=order_id, client_order_id=client_order_id)
            if algo_payload is None:
                raise exc
            return self._parse_order_snapshot(algo_payload)

    def get_recent_trades(
        self,
        symbol: str | None = None,
        *,
        limit: int = 50,
        order_id: str | int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[UserTradeSnapshot]:
        resolved_symbol = symbol or self.config.symbol
        params: dict[str, Any] = {'symbol': resolved_symbol, 'limit': limit}
        if order_id is not None:
            params['orderId'] = order_id
        if start_time_ms is not None:
            params['startTime'] = int(start_time_ms)
        if end_time_ms is not None:
            params['endTime'] = int(end_time_ms)
        payload = self._request_json('/fapi/v1/userTrades', params=params, signed=True)
        return [self._parse_user_trade_snapshot(row) for row in payload]

    def _get_algo_order(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any] | None:
        rows = self._get_algo_orders(symbol=symbol, order_ids=[order_id] if order_id is not None else None, client_order_ids=[client_order_id] if client_order_id else None)
        if order_id is not None:
            order_id_text = str(order_id)
            for row in rows:
                algo_id = row.get('algoId')
                if algo_id is not None and str(algo_id) == order_id_text:
                    return row
        if client_order_id is not None:
            for row in rows:
                cid = row.get('clientAlgoId') or row.get('clientOrderId')
                if cid == client_order_id:
                    return row
        return rows[0] if rows else None

    @staticmethod
    def _is_active_algo_order_row(row: dict[str, Any]) -> bool:
        status = (
            str(row.get('algoStatus')).upper()
            if row.get('algoStatus')
            else str(row.get('status', row.get('orderStatus', 'UNKNOWN'))).upper()
        )
        return status in {'NEW', 'PARTIALLY_FILLED', 'PENDING_CANCEL', 'ACCEPTED', 'CALCULATED'}

    def _get_algo_orders(
        self,
        *,
        symbol: str,
        order_ids: list[str | int] | tuple[str | int, ...] | set[str | int] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> list[dict[str, Any]]:
        candidate_paths = ('/fapi/v1/algoOrder',)
        ordered_algo_ids = [str(item) for item in (order_ids or []) if item is not None and str(item)]
        ordered_algo_ids = list(dict.fromkeys(ordered_algo_ids))
        ordered_client_ids = [str(item) for item in (client_order_ids or []) if item]
        ordered_client_ids = list(dict.fromkeys(ordered_client_ids))
        matched: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        def _append_rows(
            payload: Any,
            *,
            requested_algo_id: str | None = None,
            requested_client_order_id: str | None = None,
        ) -> None:
            rows: list[dict[str, Any]]
            if isinstance(payload, list):
                rows = [row for row in payload if isinstance(row, dict)]
            elif isinstance(payload, dict) and payload:
                rows = [payload]
            else:
                rows = []
            for row in rows:
                cid = row.get('clientAlgoId') or row.get('clientOrderId')
                algo_id = row.get('algoId')
                algo_id_text = str(algo_id) if algo_id is not None else None
                if ordered_algo_ids and requested_algo_id is not None and algo_id_text != requested_algo_id:
                    continue
                if ordered_client_ids and requested_client_order_id is not None and cid != requested_client_order_id:
                    continue
                dedupe_key = str(algo_id if algo_id is not None else (cid or json.dumps(row, sort_keys=True)))
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                matched.append(row)

        for path in candidate_paths:
            if ordered_algo_ids or ordered_client_ids:
                for algo_id in ordered_algo_ids:
                    try:
                        payload = self._request_json(path, params={'symbol': symbol, 'algoId': algo_id}, signed=True)
                    except Exception:  # noqa: BLE001
                        continue
                    _append_rows(payload, requested_algo_id=algo_id)
                for client_order_id in ordered_client_ids:
                    try:
                        payload = self._request_json(path, params={'symbol': symbol, 'clientAlgoId': client_order_id}, signed=True)
                    except Exception:  # noqa: BLE001
                        continue
                    _append_rows(payload, requested_client_order_id=client_order_id)
            else:
                try:
                    payload = self._request_json(path, params={'symbol': symbol}, signed=True)
                except Exception:  # noqa: BLE001
                    continue
                _append_rows(payload)
        return matched

    @staticmethod
    def _dedupe_order_snapshots(rows: list[OrderSnapshot]) -> list[OrderSnapshot]:
        deduped: list[OrderSnapshot] = []
        seen: set[tuple[str | None, str | None]] = set()
        for row in rows:
            key = (getattr(row, 'order_id', None), getattr(row, 'client_order_id', None))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def _parse_order_snapshot(self, row: dict[str, Any]) -> OrderSnapshot:
        avg_price = self._safe_float(row.get('avgPrice'))
        price = self._safe_float(row.get('price'))
        stop_price = self._safe_float(row.get('stopPrice'))
        if stop_price is None:
            stop_price = self._safe_float(row.get('triggerPrice'))
        order_type = (
            str(row.get('orderType')).upper()
            if row.get('orderType')
            else (str(row.get('origType')).upper() if row.get('origType') else (str(row.get('type')).upper() if row.get('type') else None))
        )
        order_status = (
            str(row.get('algoStatus')).upper()
            if row.get('algoStatus')
            else str(row.get('status', row.get('orderStatus', 'UNKNOWN'))).upper()
        )
        return OrderSnapshot(
            order_id=str(row.get('orderId') if row.get('orderId') is not None else row.get('algoId')),
            client_order_id=row.get('clientOrderId') or row.get('clientAlgoId'),
            status=order_status,
            type=order_type,
            time_in_force=(str(row.get('timeInForce')).upper() if row.get('timeInForce') else None),
            side=self._normalize_side(row.get('side')),
            position_side=self._normalize_side(row.get('positionSide')),
            qty=self._safe_float(row.get('origQty') if row.get('origQty') is not None else row.get('quantity')),
            executed_qty=self._safe_float(row.get('executedQty')),
            price=price,
            avg_price=avg_price,
            cum_quote=self._safe_float(row.get('cumQuote')),
            stop_price=stop_price,
            working_type=(str(row.get('workingType')).upper() if row.get('workingType') else None),
            orig_type=order_type,
            activate_price=self._safe_float(row.get('activatePrice')),
            price_protect=row.get('priceProtect'),
            reduce_only=row.get('reduceOnly'),
            close_position=row.get('closePosition'),
            update_time_ms=self._safe_int(row.get('updateTime')),
            raw=row,
        )

    def _parse_user_trade_snapshot(self, row: dict[str, Any]) -> UserTradeSnapshot:
        return UserTradeSnapshot(
            trade_id=str(row.get('id')),
            order_id=str(row.get('orderId')),
            client_order_id=row.get('clientOrderId'),
            symbol=row.get('symbol'),
            side=self._normalize_side(row.get('side')),
            position_side=self._normalize_side(row.get('positionSide')),
            qty=float(row.get('qty', 0.0)),
            price=float(row.get('price', 0.0)),
            realized_pnl=self._safe_float(row.get('realizedPnl')),
            fee=float(row.get('commission', 0.0)),
            fee_asset=row.get('commissionAsset'),
            maker=row.get('maker'),
            buyer=row.get('buyer'),
            time_ms=self._safe_int(row.get('time')),
            raw=row,
        )
