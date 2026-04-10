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
class BinanceSignedOrderSubmitRequest:
    symbol: str
    side: str
    order_type: str
    quantity: float | None
    reduce_only: bool
    position_side: str | None
    client_order_id: str
    recv_window_ms: int
    timestamp_ms: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class PreparedSignedPost:
    method: str
    path: str
    url: str
    body: str
    headers: dict[str, str]
    body_redacted: dict[str, Any]


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

    def __init__(
        self,
        config: BinanceEnvConfig,
        *,
        timeout_seconds: float = 10.0,
        recv_window_ms: int = 5000,
        allow_live_submit_call: bool = False,
    ):
        self.config = config
        self.timeout_seconds = timeout_seconds
        self.recv_window_ms = recv_window_ms
        self.allow_live_submit_call = allow_live_submit_call

    def timestamp_ms(self) -> int:
        return int(time.time() * 1000)

    def build_submit_request(self, order_payload: dict[str, Any], *, metadata: dict[str, Any] | None = None) -> BinanceSignedOrderSubmitRequest:
        return BinanceSignedOrderSubmitRequest(
            symbol=str(order_payload['symbol']),
            side=str(order_payload['side']).upper(),
            order_type=str(order_payload['type']).upper(),
            quantity=(float(order_payload['quantity']) if order_payload.get('quantity') is not None else None),
            reduce_only=(str(order_payload.get('reduceOnly', '')).lower() == 'true'),
            position_side=(str(order_payload['positionSide']).upper() if order_payload.get('positionSide') else None),
            client_order_id=str(order_payload['newClientOrderId']),
            recv_window_ms=self.recv_window_ms,
            timestamp_ms=self.timestamp_ms(),
            metadata=dict(metadata or {}),
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

    def prepare_signed_post(self, request: BinanceSignedOrderSubmitRequest) -> PreparedSignedPost:
        params: dict[str, Any] = {
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
            path=self.submit_path,
            url=f'{self.config.base_url}{self.submit_path}',
            body=body,
            headers=headers,
            body_redacted={
                'symbol': request.symbol,
                'side': request.side,
                'type': request.order_type,
                'newClientOrderId': request.client_order_id,
                'quantity': request.quantity,
                'reduceOnly': request.reduce_only,
                'positionSide': request.position_side,
                'recvWindow': request.recv_window_ms,
                'timestamp': request.timestamp_ms,
                'has_signature': True,
            },
        )

    def submit_order(self, request: BinanceSignedOrderSubmitRequest) -> tuple[BinanceSubmitResponse, BinanceSubmitReceipt]:
        self.assert_submit_allowed()
        prepared = self.prepare_signed_post(request)
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
                            'client_order_id': request.client_order_id,
                        },
                    ) from exc
                submit_response = self._parse_submit_response(int(getattr(response, 'status', 200) or 200), raw_payload)
                return submit_response, self.response_to_receipt(request, prepared, submit_response)
        except HTTPError as exc:
            payload = self._read_error_payload(exc)
            raise BinanceSubmitHttpError(
                'submit HTTP error',
                category='submit_http_error',
                detail={
                    'status_code': exc.code,
                    'path': self.submit_path,
                    'client_order_id': request.client_order_id,
                    'payload': payload,
                },
            ) from exc
        except URLError as exc:
            raise BinanceSubmitNetworkError(
                'submit network error',
                category='submit_network_error',
                detail={
                    'path': self.submit_path,
                    'client_order_id': request.client_order_id,
                    'reason': str(getattr(exc, 'reason', exc)),
                },
            ) from exc

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

    def _parse_submit_response(self, status_code: int, payload: dict[str, Any]) -> BinanceSubmitResponse:
        return BinanceSubmitResponse(
            status_code=status_code,
            order_id=(str(payload['orderId']) if payload.get('orderId') is not None else None),
            client_order_id=payload.get('clientOrderId'),
            status=(str(payload['status']).upper() if payload.get('status') else None),
            transact_time_ms=(
                int(payload['transactTime'])
                if payload.get('transactTime') is not None
                else (int(payload['updateTime']) if payload.get('updateTime') is not None else None)
            ),
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
