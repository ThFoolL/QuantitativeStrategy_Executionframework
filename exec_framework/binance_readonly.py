from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
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


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    side: str | None
    qty: float
    entry_price: float | None
    break_even_price: float | None
    mark_price: float | None
    unrealized_pnl: float | None
    leverage: float | None
    margin_type: str | None
    position_side_mode: str | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OrderSnapshot:
    order_id: str
    client_order_id: str | None
    status: str
    type: str | None
    time_in_force: str | None
    side: str | None
    position_side: str | None
    qty: float | None
    executed_qty: float | None
    price: float | None
    avg_price: float | None
    cum_quote: float | None
    reduce_only: bool | None
    close_position: bool | None
    update_time_ms: int | None
    raw: dict[str, Any]


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

    def __init__(self, config: BinanceEnvConfig, *, timeout_seconds: float = 10.0, recv_window_ms: int = 5000):
        self.config = config
        self.timeout_seconds = timeout_seconds
        self.recv_window_ms = recv_window_ms

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000)

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
        row = symbol_rows[0]
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
        return AccountSnapshot(
            account_equity=float(payload.get('totalWalletBalance', 0.0)),
            available_margin=float(payload.get('availableBalance', 0.0)),
            raw=payload,
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

    def get_open_orders(self, symbol: str | None = None) -> list[OrderSnapshot]:
        resolved_symbol = symbol or self.config.symbol
        payload = self._request_json('/fapi/v1/openOrders', params={'symbol': resolved_symbol}, signed=True)
        return [self._parse_order_snapshot(row) for row in payload]

    def get_order(self, *, symbol: str | None = None, order_id: str | int | None = None, client_order_id: str | None = None) -> OrderSnapshot:
        resolved_symbol = symbol or self.config.symbol
        params: dict[str, Any] = {'symbol': resolved_symbol}
        if order_id is not None:
            params['orderId'] = order_id
        elif client_order_id is not None:
            params['origClientOrderId'] = client_order_id
        else:
            raise ValueError('either order_id or client_order_id is required')
        payload = self._request_json('/fapi/v1/order', params=params, signed=True)
        return self._parse_order_snapshot(payload)

    def get_recent_trades(
        self,
        symbol: str | None = None,
        *,
        limit: int = 50,
        order_id: str | int | None = None,
    ) -> list[UserTradeSnapshot]:
        resolved_symbol = symbol or self.config.symbol
        params: dict[str, Any] = {'symbol': resolved_symbol, 'limit': limit}
        if order_id is not None:
            params['orderId'] = order_id
        payload = self._request_json('/fapi/v1/userTrades', params=params, signed=True)
        return [self._parse_user_trade_snapshot(row) for row in payload]

    def _parse_order_snapshot(self, row: dict[str, Any]) -> OrderSnapshot:
        avg_price = self._safe_float(row.get('avgPrice'))
        price = self._safe_float(row.get('price'))
        return OrderSnapshot(
            order_id=str(row.get('orderId')),
            client_order_id=row.get('clientOrderId'),
            status=str(row.get('status', 'UNKNOWN')).upper(),
            type=(str(row.get('type')).upper() if row.get('type') else None),
            time_in_force=(str(row.get('timeInForce')).upper() if row.get('timeInForce') else None),
            side=self._normalize_side(row.get('side')),
            position_side=self._normalize_side(row.get('positionSide')),
            qty=self._safe_float(row.get('origQty')),
            executed_qty=self._safe_float(row.get('executedQty')),
            price=price,
            avg_price=avg_price,
            cum_quote=self._safe_float(row.get('cumQuote')),
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
