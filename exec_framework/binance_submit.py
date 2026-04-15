from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError

from .runtime_env import BinanceEnvConfig


BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN = 'ENABLE_BINANCE_FUTURES_LIVE_SUBMIT'


class BinanceSubmitError(RuntimeError):
    """submit 链路统一基类。"""

    def __init__(self, message: str, *, category: str, detail: dict[str, Any] | None = None):
        super().__init__(message)
        self.category = category
        self.detail = detail or {}


class BinanceSubmitGateBlockedError(BinanceSubmitError):
    pass


class BinanceSubmitHttpDisabledError(BinanceSubmitError):
    pass


class BinanceSubmitHttpError(BinanceSubmitError):
    pass


class BinanceSubmitNetworkError(BinanceSubmitError):
    pass


class BinanceSubmitDecodeError(BinanceSubmitError):
    pass


@dataclass(frozen=True)
class BinanceSignedOrderCancelRequest:
    symbol: str
    order_id: str | None
    client_order_id: str | None
    recv_window_ms: int
    timestamp_ms: int
    metadata: dict[str, Any]
    is_algo_order: bool = False


@dataclass(frozen=True)
class BinanceSignedOrderSubmitRequest:
    symbol: str
    side: str
    order_type: str
    quantity: float | None
    reduce_only: bool
    position_side: str | None
    client_order_id: str
    stop_price: float | None = None
    close_position: bool = False
    working_type: str | None = None
    price_protect: bool | None = None
    time_in_force: str | None = None
    price: float | None = None
    is_algo_order: bool = False
    recv_window_ms: int = 10000
    timestamp_ms: int = 0
    metadata: dict[str, Any] = None


@dataclass(frozen=True)
class PreparedSignedPost:
    method: str
    path: str
    url: str
    body: str
    headers: dict[str, str]
    body_redacted: dict[str, Any]


@dataclass(frozen=True)
class BinanceCancelResponse:
    status_code: int
    order_id: str | None
    client_order_id: str | None
    status: str | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class BinanceSubmitResponse:
    status_code: int
    order_id: str | None
    client_order_id: str | None
    status: str | None
    transact_time_ms: int | None
    executed_qty: float | None
    avg_price: float | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class BinanceCancelReceipt:
    client_order_id: str | None
    exchange_order_id: str | None
    canceled: bool
    cancel_status: str
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] | None
    metadata: dict[str, Any]
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class BinanceSubmitReceipt:
    client_order_id: str
    exchange_order_id: str | None
    acknowledged: bool
    submitted_qty: float | None
    submitted_side: str | None
    submit_status: str
    exchange_status: str | None
    transact_time_ms: int | None
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] | None
    metadata: dict[str, Any]
    error_code: str | None = None
    error_message: str | None = None


class BinanceSignedSubmitClient:
    """Binance futures signed POST client 骨架。

    安全约束：
    - 默认不允许真实 HTTP POST
    - 即便 submit_enabled=True，也还需要 unlock token + allow_live_submit_call=True
    - 本轮只允许把签名、request/response/receipt、异常分类接好；默认仍不可达
    """

    submit_path = '/fapi/v1/order'
    algo_submit_path = '/fapi/v1/algoOrder'
    server_time_path = '/fapi/v1/time'

    def __init__(
        self,
        config: BinanceEnvConfig,
        *,
        timeout_seconds: float = 10.0,
        recv_window_ms: int | None = None,
        allow_live_submit_call: bool = False,
    ):
        self.config = config
        self.timeout_seconds = timeout_seconds
        self.recv_window_ms = int(recv_window_ms if recv_window_ms is not None else config.recv_window_ms)
        self.allow_live_submit_call = allow_live_submit_call
        self._server_time_offset_ms = 0
        self._server_time_offset_synced = False

    def timestamp_ms(self) -> int:
        return int(time.time() * 1000) + self._server_time_offset_ms

    def build_cancel_request(
        self,
        *,
        symbol: str,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> BinanceSignedOrderCancelRequest:
        if order_id is None and not client_order_id:
            raise ValueError('either order_id or client_order_id is required for cancel request')
        request_metadata = dict(metadata or {})
        return BinanceSignedOrderCancelRequest(
            symbol=str(symbol),
            order_id=(str(order_id) if order_id is not None else None),
            client_order_id=(str(client_order_id) if client_order_id else None),
            recv_window_ms=self.recv_window_ms,
            timestamp_ms=self.timestamp_ms(),
            metadata=request_metadata,
            is_algo_order=bool(request_metadata.get('algo_order')),
        )

    def build_submit_request(self, order_payload: dict[str, Any], *, metadata: dict[str, Any] | None = None) -> BinanceSignedOrderSubmitRequest:
        request_metadata = dict(metadata or {})
        resolved_client_order_id = order_payload.get('newClientOrderId') or order_payload.get('clientAlgoId')
        if resolved_client_order_id in (None, ''):
            raise ValueError('submit request requires newClientOrderId or clientAlgoId')
        return BinanceSignedOrderSubmitRequest(
            symbol=str(order_payload['symbol']),
            side=str(order_payload['side']).upper(),
            order_type=str(order_payload['type']).upper(),
            quantity=(float(order_payload['quantity']) if order_payload.get('quantity') is not None else None),
            reduce_only=(str(order_payload.get('reduceOnly', '')).lower() == 'true'),
            position_side=(str(order_payload['positionSide']).upper() if order_payload.get('positionSide') else None),
            client_order_id=str(resolved_client_order_id),
            stop_price=(float(order_payload['triggerPrice']) if order_payload.get('triggerPrice') is not None else (float(order_payload['stopPrice']) if order_payload.get('stopPrice') is not None else None)),
            close_position=(str(order_payload.get('closePosition', '')).lower() == 'true'),
            working_type=(str(order_payload['workingType']).upper() if order_payload.get('workingType') else None),
            price_protect=(str(order_payload.get('priceProtect')).upper() == 'TRUE' if order_payload.get('priceProtect') is not None else None),
            time_in_force=(str(order_payload['timeInForce']).upper() if order_payload.get('timeInForce') else None),
            price=(float(order_payload['price']) if order_payload.get('price') is not None else None),
            is_algo_order=bool(order_payload.get('algoType')) or bool(order_payload.get('clientAlgoId')) or bool(request_metadata.get('algo_order')),
            recv_window_ms=self.recv_window_ms,
            timestamp_ms=self.timestamp_ms(),
            metadata=request_metadata,
        )

    def _fetch_server_time_ms(self) -> int:
        request = urllib.request.Request(f'{self.config.base_url}{self.server_time_path}')
        request.add_header('Accept', 'application/json')
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode('utf-8'))
        return int(payload['serverTime'])

    def sync_server_time_offset(self) -> int:
        server_time_ms = self._fetch_server_time_ms()
        self._server_time_offset_ms = server_time_ms - int(time.time() * 1000)
        self._server_time_offset_synced = True
        return self._server_time_offset_ms

    @staticmethod
    def _is_invalid_timestamp_payload(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        try:
            return int(payload.get('code')) == -1021
        except (TypeError, ValueError):
            return False

    def refresh_cancel_request_timestamp(self, request: BinanceSignedOrderCancelRequest) -> BinanceSignedOrderCancelRequest:
        return BinanceSignedOrderCancelRequest(
            symbol=request.symbol,
            order_id=request.order_id,
            client_order_id=request.client_order_id,
            recv_window_ms=request.recv_window_ms,
            timestamp_ms=self.timestamp_ms(),
            metadata=dict(request.metadata or {}),
            is_algo_order=request.is_algo_order,
        )

    def refresh_request_timestamp(self, request: BinanceSignedOrderSubmitRequest) -> BinanceSignedOrderSubmitRequest:
        return BinanceSignedOrderSubmitRequest(
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            reduce_only=request.reduce_only,
            position_side=request.position_side,
            client_order_id=request.client_order_id,
            stop_price=request.stop_price,
            close_position=request.close_position,
            working_type=request.working_type,
            price_protect=request.price_protect,
            time_in_force=request.time_in_force,
            price=request.price,
            is_algo_order=request.is_algo_order,
            recv_window_ms=request.recv_window_ms,
            timestamp_ms=self.timestamp_ms(),
            metadata=dict(request.metadata or {}),
        )

    def gate_context(self) -> dict[str, Any]:
        unlock_value = (self.config.submit_unlock_token or '').strip()
        return {
            'dry_run': self.config.dry_run,
            'submit_enabled': self.config.submit_enabled,
            'submit_unlock_token_present': bool(unlock_value),
            'submit_unlock_token_valid': unlock_value == BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN,
            'allow_live_submit_call': self.allow_live_submit_call,
            'http_post_allowed': bool(
                (not self.config.dry_run)
                and self.config.submit_enabled
                and unlock_value == BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN
                and self.allow_live_submit_call
            ),
        }

    def assert_submit_allowed(self) -> None:
        context = self.gate_context()
        if self.config.dry_run:
            raise BinanceSubmitGateBlockedError(
                'submit blocked because dry_run=True',
                category='submit_gate_blocked',
                detail=context,
            )
        if not self.config.submit_enabled:
            raise BinanceSubmitGateBlockedError(
                'submit blocked because submit_enabled=False',
                category='submit_gate_blocked',
                detail=context,
            )
        if not context['submit_unlock_token_valid']:
            raise BinanceSubmitGateBlockedError(
                'submit blocked because unlock token is missing or invalid',
                category='submit_gate_blocked',
                detail=context,
            )
        if not self.allow_live_submit_call:
            raise BinanceSubmitHttpDisabledError(
                'live HTTP POST remains hard-disabled by code-level guard',
                category='submit_http_disabled',
                detail=context,
            )

    def prepare_signed_cancel(self, request: BinanceSignedOrderCancelRequest) -> PreparedSignedPost:
        path = self.algo_submit_path if request.is_algo_order else self.submit_path
        params: dict[str, Any] = {
            'recvWindow': request.recv_window_ms,
            'timestamp': request.timestamp_ms,
        }
        if request.is_algo_order:
            if request.order_id is not None:
                params['algoId'] = request.order_id
            if request.client_order_id is not None:
                params['clientAlgoId'] = request.client_order_id
        else:
            params['symbol'] = request.symbol
            if request.order_id is not None:
                params['orderId'] = request.order_id
            if request.client_order_id is not None:
                params['origClientOrderId'] = request.client_order_id

        query = urllib.parse.urlencode(params, doseq=True)
        signature = hmac.new(
            self.config.api_secret.encode('utf-8'),
            query.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()
        body = f'{query}&signature={signature}'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-MBX-APIKEY': self.config.api_key,
        }
        return PreparedSignedPost(
            method='DELETE',
            path=path,
            url=f'{self.config.base_url}{path}',
            body=body,
            headers=headers,
            body_redacted={
                'symbol': request.symbol,
                'orderId': request.order_id,
                'origClientOrderId': request.client_order_id,
                'algoId': request.order_id if request.is_algo_order else None,
                'clientAlgoId': request.client_order_id if request.is_algo_order else None,
                'isAlgoOrder': request.is_algo_order,
                'recvWindow': request.recv_window_ms,
                'timestamp': request.timestamp_ms,
                'has_signature': True,
            },
        )

    def prepare_signed_post(self, request: BinanceSignedOrderSubmitRequest) -> PreparedSignedPost:
        if request.is_algo_order:
            path = self.algo_submit_path
            params: dict[str, Any] = {
                'algoType': 'CONDITIONAL',
                'symbol': request.symbol,
                'side': request.side,
                'type': request.order_type,
                'clientAlgoId': request.client_order_id,
                'recvWindow': request.recv_window_ms,
                'timestamp': request.timestamp_ms,
            }
            if request.quantity is not None:
                params['quantity'] = request.quantity
            if request.position_side is not None:
                params['positionSide'] = request.position_side
            if request.stop_price is not None:
                params['triggerPrice'] = request.stop_price
            if request.close_position:
                params['closePosition'] = 'true'
            if request.working_type is not None:
                params['workingType'] = request.working_type
            if request.price_protect is not None:
                params['priceProtect'] = 'TRUE' if request.price_protect else 'FALSE'
            if request.reduce_only and not request.close_position:
                params['reduceOnly'] = 'true'
        else:
            path = self.submit_path
            params = {
                'symbol': request.symbol,
                'side': request.side,
                'type': request.order_type,
                'newClientOrderId': request.client_order_id,
                'recvWindow': request.recv_window_ms,
                'timestamp': request.timestamp_ms,
            }
            if request.quantity is not None:
                params['quantity'] = request.quantity
            if request.reduce_only:
                params['reduceOnly'] = 'true'
            if request.position_side is not None:
                params['positionSide'] = request.position_side
            if request.stop_price is not None:
                params['stopPrice'] = request.stop_price
            if request.close_position:
                params['closePosition'] = 'true'
            if request.time_in_force is not None:
                params['timeInForce'] = request.time_in_force
            if request.price is not None:
                params['price'] = request.price
            if request.working_type is not None:
                params['workingType'] = request.working_type
            if request.price_protect is not None:
                params['priceProtect'] = 'TRUE' if request.price_protect else 'FALSE'

        query = urllib.parse.urlencode(params, doseq=True)
        signature = hmac.new(
            self.config.api_secret.encode('utf-8'),
            query.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()
        body = f'{query}&signature={signature}'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-MBX-APIKEY': self.config.api_key,
        }
        return PreparedSignedPost(
            method='POST',
            path=path,
            url=f'{self.config.base_url}{path}',
            body=body,
            headers=headers,
            body_redacted={
                'symbol': request.symbol,
                'side': request.side,
                'type': request.order_type,
                'newClientOrderId': request.client_order_id if not request.is_algo_order else None,
                'clientAlgoId': request.client_order_id if request.is_algo_order else None,
                'algoType': 'CONDITIONAL' if request.is_algo_order else None,
                'quantity': request.quantity,
                'reduceOnly': request.reduce_only,
                'positionSide': request.position_side,
                'triggerPrice': request.stop_price if request.is_algo_order else None,
                'stopPrice': request.stop_price if not request.is_algo_order else None,
                'closePosition': request.close_position,
                'workingType': request.working_type,
                'priceProtect': request.price_protect,
                'timeInForce': request.time_in_force,
                'price': request.price,
                'isAlgoOrder': request.is_algo_order,
                'recvWindow': request.recv_window_ms,
                'timestamp': request.timestamp_ms,
                'has_signature': True,
            },
        )

    def cancel_order(self, request: BinanceSignedOrderCancelRequest) -> tuple[BinanceCancelResponse, BinanceCancelReceipt]:
        self.assert_submit_allowed()
        active_request = request
        prepared = self.prepare_signed_cancel(active_request)

        for attempt in range(2):
            http_request = urllib.request.Request(
                prepared.url,
                data=prepared.body.encode('utf-8'),
                method=prepared.method,
            )
            for key, value in prepared.headers.items():
                http_request.add_header(key, value)

            try:
                with urllib.request.urlopen(http_request, timeout=self.timeout_seconds) as response:
                    raw_text = response.read().decode('utf-8')
                    try:
                        raw_payload = json.loads(raw_text)
                    except json.JSONDecodeError as exc:
                        raise BinanceSubmitDecodeError(
                            'cancel response JSON decode failed',
                            category='cancel_decode_error',
                            detail={
                                'status_code': int(getattr(response, 'status', 200) or 200),
                                'body_preview': raw_text[:500],
                                'client_order_id': active_request.client_order_id,
                                'order_id': active_request.order_id,
                            },
                        ) from exc
                    cancel_response = self._parse_cancel_response(int(getattr(response, 'status', 200) or 200), raw_payload)
                    return cancel_response, self.cancel_response_to_receipt(active_request, prepared, cancel_response)
            except HTTPError as exc:
                payload = self._read_error_payload(exc)
                if attempt == 0 and self._is_invalid_timestamp_payload(payload):
                    self.sync_server_time_offset()
                    active_request = self.refresh_cancel_request_timestamp(active_request)
                    prepared = self.prepare_signed_cancel(active_request)
                    continue
                raise BinanceSubmitHttpError(
                    'cancel HTTP error',
                    category='cancel_http_error',
                    detail={
                        'status_code': exc.code,
                        'path': prepared.path,
                        'client_order_id': active_request.client_order_id,
                        'order_id': active_request.order_id,
                        'payload': payload,
                        'server_time_offset_ms': self._server_time_offset_ms,
                        'server_time_offset_synced': self._server_time_offset_synced,
                    },
                ) from exc
            except URLError as exc:
                raise BinanceSubmitNetworkError(
                    'cancel network error',
                    category='cancel_network_error',
                    detail={
                        'path': prepared.path,
                        'client_order_id': active_request.client_order_id,
                        'order_id': active_request.order_id,
                        'reason': str(getattr(exc, 'reason', exc)),
                    },
                ) from exc

        raise BinanceSubmitHttpError(
            'cancel HTTP error',
            category='cancel_http_error',
            detail={
                'path': prepared.path,
                'client_order_id': active_request.client_order_id,
                'order_id': active_request.order_id,
                'payload': {'code': -1021, 'msg': 'Timestamp retry loop exhausted'},
                'server_time_offset_ms': self._server_time_offset_ms,
                'server_time_offset_synced': self._server_time_offset_synced,
            },
        )

    def submit_order(self, request: BinanceSignedOrderSubmitRequest) -> tuple[BinanceSubmitResponse, BinanceSubmitReceipt]:
        self.assert_submit_allowed()
        active_request = request
        prepared = self.prepare_signed_post(active_request)

        for attempt in range(2):
            http_request = urllib.request.Request(
                prepared.url,
                data=prepared.body.encode('utf-8'),
                method=prepared.method,
            )
            for key, value in prepared.headers.items():
                http_request.add_header(key, value)

            try:
                with urllib.request.urlopen(http_request, timeout=self.timeout_seconds) as response:
                    raw_text = response.read().decode('utf-8')
                    try:
                        raw_payload = json.loads(raw_text)
                    except json.JSONDecodeError as exc:
                        raise BinanceSubmitDecodeError(
                            'submit response JSON decode failed',
                            category='submit_decode_error',
                            detail={
                                'status_code': int(getattr(response, 'status', 200) or 200),
                                'body_preview': raw_text[:500],
                                'client_order_id': active_request.client_order_id,
                            },
                        ) from exc
                    submit_response = self._parse_submit_response(int(getattr(response, 'status', 200) or 200), raw_payload)
                    return submit_response, self.response_to_receipt(active_request, prepared, submit_response)
            except HTTPError as exc:
                payload = self._read_error_payload(exc)
                if attempt == 0 and self._is_invalid_timestamp_payload(payload):
                    self.sync_server_time_offset()
                    active_request = self.refresh_request_timestamp(active_request)
                    prepared = self.prepare_signed_post(active_request)
                    continue
                raise BinanceSubmitHttpError(
                    'submit HTTP error',
                    category='submit_http_error',
                    detail={
                        'status_code': exc.code,
                        'path': prepared.path,
                        'client_order_id': active_request.client_order_id,
                        'payload': payload,
                        'server_time_offset_ms': self._server_time_offset_ms,
                        'server_time_offset_synced': self._server_time_offset_synced,
                    },
                ) from exc
            except URLError as exc:
                raise BinanceSubmitNetworkError(
                    'submit network error',
                    category='submit_network_error',
                    detail={
                        'path': prepared.path,
                        'client_order_id': active_request.client_order_id,
                        'reason': str(getattr(exc, 'reason', exc)),
                    },
                ) from exc

        raise BinanceSubmitHttpError(
            'submit HTTP error',
            category='submit_http_error',
            detail={
                'path': prepared.path,
                'client_order_id': active_request.client_order_id,
                'payload': {'code': -1021, 'msg': 'Timestamp retry loop exhausted'},
                'server_time_offset_ms': self._server_time_offset_ms,
                'server_time_offset_synced': self._server_time_offset_synced,
            },
        )

    def cancel_response_to_receipt(
        self,
        request: BinanceSignedOrderCancelRequest,
        prepared: PreparedSignedPost,
        response: BinanceCancelResponse,
    ) -> BinanceCancelReceipt:
        metadata = {
            'request_method': prepared.method,
            'request_path': prepared.path,
            'request_headers': {
                'Accept': prepared.headers.get('Accept'),
                'Content-Type': prepared.headers.get('Content-Type'),
                'X-MBX-APIKEY': 'present' if prepared.headers.get('X-MBX-APIKEY') else 'missing',
            },
            'gate_context': self.gate_context(),
            'submit_mode': 'live_http_delete',
        }
        return BinanceCancelReceipt(
            client_order_id=response.client_order_id or request.client_order_id,
            exchange_order_id=response.order_id or request.order_id,
            canceled=response.status_code < 400,
            cancel_status='CANCELED' if response.status_code < 400 else 'HTTP_ERROR',
            request_payload=prepared.body_redacted,
            response_payload=response.raw,
            metadata=metadata,
        )

    def response_to_receipt(
        self,
        request: BinanceSignedOrderSubmitRequest,
        prepared: PreparedSignedPost,
        response: BinanceSubmitResponse,
    ) -> BinanceSubmitReceipt:
        metadata = {
            'request_method': prepared.method,
            'request_path': prepared.path,
            'request_headers': {
                'Accept': prepared.headers.get('Accept'),
                'Content-Type': prepared.headers.get('Content-Type'),
                'X-MBX-APIKEY': 'present' if prepared.headers.get('X-MBX-APIKEY') else 'missing',
            },
            'gate_context': self.gate_context(),
            'submit_mode': 'live_http_post',
        }
        return BinanceSubmitReceipt(
            client_order_id=request.client_order_id,
            exchange_order_id=response.order_id,
            acknowledged=response.status_code < 400,
            submitted_qty=request.quantity,
            submitted_side=request.side,
            submit_status='ACKNOWLEDGED' if response.status_code < 400 else 'HTTP_ERROR',
            exchange_status=response.status,
            transact_time_ms=response.transact_time_ms,
            request_payload=prepared.body_redacted,
            response_payload=response.raw,
            metadata=metadata,
        )

    def _parse_cancel_response(self, status_code: int, payload: dict[str, Any]) -> BinanceCancelResponse:
        return BinanceCancelResponse(
            status_code=status_code,
            order_id=(str(payload['orderId']) if payload.get('orderId') is not None else (str(payload['algoId']) if payload.get('algoId') is not None else None)),
            client_order_id=payload.get('clientOrderId') or payload.get('clientAlgoId'),
            status=(str(payload['status']).upper() if payload.get('status') else None),
            raw=payload,
        )

    def _parse_submit_response(self, status_code: int, payload: dict[str, Any]) -> BinanceSubmitResponse:
        return BinanceSubmitResponse(
            status_code=status_code,
            order_id=(str(payload['orderId']) if payload.get('orderId') is not None else (str(payload['algoId']) if payload.get('algoId') is not None else None)),
            client_order_id=payload.get('clientOrderId') or payload.get('clientAlgoId'),
            status=(str(payload['status']).upper() if payload.get('status') else None),
            transact_time_ms=(int(payload['updateTime']) if payload.get('updateTime') is not None else None),
            executed_qty=(float(payload['executedQty']) if payload.get('executedQty') not in (None, '') else None),
            avg_price=(float(payload['avgPrice']) if payload.get('avgPrice') not in (None, '') else None),
            raw=payload,
        )

    @staticmethod
    def _read_error_payload(exc: HTTPError) -> dict[str, Any]:
        raw_text = exc.read().decode('utf-8', errors='replace') if exc.fp else ''
        if not raw_text:
            return {'message': str(exc)}
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            return {'message': raw_text[:500]}
