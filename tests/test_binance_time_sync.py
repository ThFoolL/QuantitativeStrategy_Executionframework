from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import BinanceReadOnlyClient
from exec_framework.binance_submit import BinanceSignedOrderCancelRequest, BinanceSignedOrderSubmitRequest, BinanceSignedSubmitClient
from exec_framework.runtime_env import BinanceEnvConfig


class BinanceSignedSubmitClientTimeSyncCase(unittest.TestCase):
    def test_prepare_signed_post_keeps_request_timestamp_and_recv_window(self) -> None:
        client = BinanceSignedSubmitClient(BinanceEnvConfig(api_key='k', api_secret='s', recv_window_ms=12000))
        request = BinanceSignedOrderSubmitRequest(
            symbol='ETHUSDT',
            side='BUY',
            order_type='MARKET',
            quantity=0.01,
            reduce_only=False,
            position_side=None,
            client_order_id='cid-1',
            recv_window_ms=12000,
            timestamp_ms=1711380000123,
            metadata={},
        )

        prepared = client.prepare_signed_post(request)

        self.assertIn('timestamp=1711380000123', prepared.body)
        self.assertIn('recvWindow=12000', prepared.body)
        self.assertEqual(prepared.body_redacted['timestamp'], 1711380000123)
        self.assertEqual(prepared.body_redacted['recvWindow'], 12000)

    def test_prepare_signed_post_uses_algo_endpoint_for_protective_order(self) -> None:
        client = BinanceSignedSubmitClient(BinanceEnvConfig(api_key='k', api_secret='s', recv_window_ms=12000))
        request = BinanceSignedOrderSubmitRequest(
            symbol='ETHUSDT',
            side='SELL',
            order_type='STOP_MARKET',
            quantity=None,
            reduce_only=True,
            position_side=None,
            client_order_id='protect-stop',
            stop_price=2050.5,
            close_position=True,
            working_type='MARK_PRICE',
            price_protect=False,
            is_algo_order=True,
            recv_window_ms=12000,
            timestamp_ms=1711380000123,
            metadata={'protective_order': True},
        )

        prepared = client.prepare_signed_post(request)

        self.assertEqual(prepared.path, '/fapi/v1/algoOrder')
        self.assertIn('algoType=CONDITIONAL', prepared.body)
        self.assertIn('clientAlgoId=protect-stop', prepared.body)
        self.assertIn('triggerPrice=2050.5', prepared.body)
        self.assertNotIn('newClientOrderId=', prepared.body)
        self.assertIn('closePosition=true', prepared.body)
        self.assertNotIn('quantity=', prepared.body)
        self.assertNotIn('reduceOnly=true', prepared.body)

    def test_build_submit_request_preserves_limit_price_and_time_in_force(self) -> None:
        client = BinanceSignedSubmitClient(BinanceEnvConfig(api_key='k', api_secret='s'))

        request = client.build_submit_request(
            {
                'symbol': 'ETHUSDT',
                'side': 'BUY',
                'type': 'LIMIT',
                'newClientOrderId': 'residual-limit',
                'quantity': 0.01,
                'price': 2000.5,
                'timeInForce': 'GTC',
            }
        )
        prepared = client.prepare_signed_post(request)

        self.assertEqual(request.time_in_force, 'GTC')
        self.assertEqual(request.price, 2000.5)
        self.assertIn('timeInForce=GTC', prepared.body)
        self.assertIn('price=2000.5', prepared.body)
        self.assertEqual(prepared.body_redacted['timeInForce'], 'GTC')
        self.assertEqual(prepared.body_redacted['price'], 2000.5)

    def test_build_submit_request_accepts_client_algo_id(self) -> None:
        client = BinanceSignedSubmitClient(BinanceEnvConfig(api_key='k', api_secret='s'))

        request = client.build_submit_request(
            {
                'symbol': 'ETHUSDT',
                'side': 'SELL',
                'type': 'STOP_MARKET',
                'clientAlgoId': 'protect-stop',
                'algoType': 'CONDITIONAL',
                'triggerPrice': 2050.5,
                'closePosition': 'true',
            }
        )

        self.assertEqual(request.client_order_id, 'protect-stop')
        self.assertTrue(request.is_algo_order)
        self.assertEqual(request.stop_price, 2050.5)
        self.assertTrue(request.close_position)
        self.assertIsNone(request.quantity)

    def test_submit_order_retries_once_after_invalid_timestamp(self) -> None:
        client = BinanceSignedSubmitClient(
            BinanceEnvConfig(
                api_key='k',
                api_secret='s',
                recv_window_ms=15000,
                dry_run=False,
                submit_enabled=True,
                submit_unlock_token='ENABLE_BINANCE_FUTURES_LIVE_SUBMIT',
            ),
            allow_live_submit_call=True,
        )
        def fake_sync_server_time_offset() -> int:
            client._server_time_offset_ms = 4321
            client._server_time_offset_synced = True
            return 4321

        client.sync_server_time_offset = fake_sync_server_time_offset
        client._server_time_offset_ms = 0
        client._server_time_offset_synced = False
        timestamps: list[int] = []

        def fake_prepare(request: BinanceSignedOrderSubmitRequest):
            timestamps.append(request.timestamp_ms)
            return type(
                'Prepared',
                (),
                {
                    'url': 'https://example.test/fapi/v1/order',
                    'body': 'symbol=ETHUSDT',
                    'method': 'POST',
                    'path': '/fapi/v1/order',
                    'headers': {'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded', 'X-MBX-APIKEY': 'k'},
                    'body_redacted': {'timestamp': request.timestamp_ms, 'recvWindow': request.recv_window_ms},
                },
            )()

        seq = iter([
            HTTPError(
                url='https://example.test/fapi/v1/order',
                code=400,
                msg='Bad Request',
                hdrs=None,
                fp=None,
            ),
            type(
                'Response',
                (),
                {
                    'status': 200,
                    '__enter__': lambda self: self,
                    '__exit__': lambda self, exc_type, exc, tb: None,
                    'read': lambda self: json.dumps({'orderId': 1, 'clientOrderId': 'cid-1', 'status': 'NEW', 'updateTime': 1711380000999}).encode('utf-8'),
                },
            )(),
        ])

        def fake_read_error_payload(exc: HTTPError):
            return {'code': -1021, 'msg': 'Timestamp for this request is outside of the recvWindow.'}

        client.prepare_signed_post = fake_prepare
        client._read_error_payload = fake_read_error_payload
        original_urlopen = __import__('urllib.request', fromlist=['urlopen']).urlopen
        import urllib.request

        def fake_urlopen(request, timeout=10.0):
            item = next(seq)
            if isinstance(item, Exception):
                raise item
            return item

        urllib.request.urlopen = fake_urlopen
        try:
            client.timestamp_ms = lambda: 1711380004322
            request = BinanceSignedOrderSubmitRequest(
                symbol='ETHUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.01,
                reduce_only=False,
                position_side=None,
                client_order_id='cid-1',
                recv_window_ms=15000,
                timestamp_ms=1711380000001,
                metadata={},
            )
            response, receipt = client.submit_order(request)
        finally:
            urllib.request.urlopen = original_urlopen

        self.assertEqual(response.order_id, '1')
        self.assertTrue(receipt.acknowledged)
        self.assertEqual(len(timestamps), 2)
        self.assertEqual(timestamps[0], 1711380000001)
        self.assertEqual(timestamps[1], 1711380004322)


    def test_prepare_signed_cancel_uses_algo_endpoint_for_protective_order(self) -> None:
        client = BinanceSignedSubmitClient(BinanceEnvConfig(api_key='k', api_secret='s', recv_window_ms=12000))
        request = BinanceSignedOrderCancelRequest(
            symbol='ETHUSDT',
            order_id='12345',
            client_order_id='protect-stop',
            recv_window_ms=12000,
            timestamp_ms=1711380000123,
            metadata={'algo_order': True},
            is_algo_order=True,
        )

        prepared = client.prepare_signed_cancel(request)

        self.assertEqual(prepared.path, '/fapi/v1/algoOrder')
        self.assertIn('algoId=12345', prepared.body)
        self.assertIn('clientAlgoId=protect-stop', prepared.body)
        self.assertNotIn('origClientOrderId=', prepared.body)
        self.assertNotIn('symbol=', prepared.body)

    def test_cancel_order_retries_once_after_invalid_timestamp(self) -> None:
        client = BinanceSignedSubmitClient(
            BinanceEnvConfig(
                api_key='k',
                api_secret='s',
                recv_window_ms=15000,
                dry_run=False,
                submit_enabled=True,
                submit_unlock_token='ENABLE_BINANCE_FUTURES_LIVE_SUBMIT',
            ),
            allow_live_submit_call=True,
        )

        def fake_sync_server_time_offset() -> int:
            client._server_time_offset_ms = 4321
            client._server_time_offset_synced = True
            return 4321

        client.sync_server_time_offset = fake_sync_server_time_offset
        client._server_time_offset_ms = 0
        client._server_time_offset_synced = False
        timestamps: list[int] = []

        def fake_prepare(request: BinanceSignedOrderCancelRequest):
            timestamps.append(request.timestamp_ms)
            return type(
                'Prepared',
                (),
                {
                    'url': 'https://example.test/fapi/v1/order',
                    'body': 'symbol=ETHUSDT',
                    'method': 'DELETE',
                    'path': '/fapi/v1/order',
                    'headers': {'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded', 'X-MBX-APIKEY': 'k'},
                    'body_redacted': {'timestamp': request.timestamp_ms, 'recvWindow': request.recv_window_ms},
                },
            )()

        seq = iter([
            HTTPError(
                url='https://example.test/fapi/v1/order',
                code=400,
                msg='Bad Request',
                hdrs=None,
                fp=None,
            ),
            type(
                'Response',
                (),
                {
                    'status': 200,
                    '__enter__': lambda self: self,
                    '__exit__': lambda self, exc_type, exc, tb: None,
                    'read': lambda self: json.dumps({'orderId': 1, 'clientOrderId': 'protect-stop', 'status': 'CANCELED'}).encode('utf-8'),
                },
            )(),
        ])

        def fake_read_error_payload(exc: HTTPError):
            return {'code': -1021, 'msg': 'Timestamp for this request is outside of the recvWindow.'}

        client.prepare_signed_cancel = fake_prepare
        client._read_error_payload = fake_read_error_payload
        original_urlopen = __import__('urllib.request', fromlist=['urlopen']).urlopen
        import urllib.request

        def fake_urlopen(request, timeout=10.0):
            item = next(seq)
            if isinstance(item, Exception):
                raise item
            return item

        urllib.request.urlopen = fake_urlopen
        try:
            client.timestamp_ms = lambda: 1711380004322
            request = BinanceSignedOrderCancelRequest(
                symbol='ETHUSDT',
                order_id='123',
                client_order_id='protect-stop',
                recv_window_ms=15000,
                timestamp_ms=1711380000001,
                metadata={},
            )
            response, receipt = client.cancel_order(request)
        finally:
            urllib.request.urlopen = original_urlopen

        self.assertEqual(response.order_id, '1')
        self.assertTrue(receipt.canceled)
        self.assertEqual(len(timestamps), 2)
        self.assertEqual(timestamps[0], 1711380000001)
        self.assertEqual(timestamps[1], 1711380004322)


class BinanceReadOnlyClientTimeSyncCase(unittest.TestCase):
    def test_signed_readonly_request_retries_after_invalid_timestamp(self) -> None:
        client = BinanceReadOnlyClient(BinanceEnvConfig(api_key='k', api_secret='s', recv_window_ms=16000))
        request_timestamps: list[int] = []
        sync_calls: list[int] = []

        def fake_sign(params):
            request_timestamps.append(int(params['timestamp']))
            return f"timestamp={params['timestamp']}&recvWindow={params['recvWindow']}&signature=x"

        class FakeHttpError(HTTPError):
            def __init__(self):
                super().__init__('https://example.test', 400, 'Bad Request', None, None)

            def read(self):
                return json.dumps({'code': -1021, 'msg': 'Timestamp for this request is outside of the recvWindow.'}).encode('utf-8')

        responses = iter([
            FakeHttpError(),
            type(
                'Response',
                (),
                {
                    'status': 200,
                    '__enter__': lambda self: self,
                    '__exit__': lambda self, exc_type, exc, tb: None,
                    'read': lambda self: json.dumps({'assets': [], 'positions': []}).encode('utf-8'),
                },
            )(),
        ])

        client._sign_params = fake_sign
        client._timestamp_ms = lambda: 1711380000001 if not request_timestamps else 1711380009876

        def fake_sync_server_time_offset() -> int:
            sync_calls.append(1)
            client._server_time_offset_ms = 9875
            client._server_time_offset_synced = True
            return 9875

        client._sync_server_time_offset = fake_sync_server_time_offset
        import urllib.request
        original_urlopen = urllib.request.urlopen

        def fake_urlopen(request, timeout=10.0):
            item = next(responses)
            if isinstance(item, Exception):
                raise item
            return item

        urllib.request.urlopen = fake_urlopen
        try:
            status, url, payload = client.request_with_meta('/fapi/v2/account', signed=True)
        finally:
            urllib.request.urlopen = original_urlopen

        self.assertEqual(status, 200)
        self.assertEqual(payload, {'assets': [], 'positions': []})
        self.assertEqual(sync_calls, [1])
        self.assertEqual(request_timestamps, [1711380000001, 1711380009876])
        self.assertIn('recvWindow=16000', url)


if __name__ == '__main__':
    unittest.main()
