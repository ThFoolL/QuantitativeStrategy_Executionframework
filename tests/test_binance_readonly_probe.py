from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly_probe import _classify_error, _mask_value, _resolve_order_probe_inputs, run_probe


class FakeProbeClient:
    def __init__(self, config, *, timeout_seconds=10.0, recv_window_ms=5000):
        self.config = config

    def request_with_meta(self, path, *, params=None, signed=False):
        assert path == '/fapi/v1/time'
        return 200, 'https://example.test/fapi/v1/time', {'serverTime': 1711380000000}

    def get_exchange_info(self, symbol):
        return type('ExchangeInfo', (), {'symbol': symbol, 'price_tick': 0.1, 'qty_step': 0.001, 'min_qty': 0.001, 'min_notional': 100.0})()

    def get_account_snapshot(self):
        return type(
            'Account',
            (),
            {
                'account_equity': 1234.5,
                'available_margin': 1000.0,
                'raw': {
                    'assets': [{'walletBalance': '1234.5', 'availableBalance': '1000'}],
                    'positions': [{'symbol': 'BTCUSDT', 'positionAmt': '0'}],
                    'canTrade': True,
                    'multiAssetsMargin': False,
                },
            },
        )()

    def get_position_mode(self):
        return {'dualSidePosition': False}

    def get_position_risk_rows(self, symbol):
        return [
            {
                'symbol': symbol,
                'positionSide': 'BOTH',
                'positionAmt': '0',
                'entryPrice': '0.0',
                'breakEvenPrice': '0.0',
                'markPrice': '2050.1',
                'unRealizedProfit': '0.0',
                'marginType': 'cross',
                'leverage': '5',
            }
        ]

    def get_open_orders(self, symbol):
        return []

    def get_recent_trades(self, symbol, limit=10):
        return [
            type(
                'Trade',
                (),
                {
                    'trade_id': '1',
                    'order_id': '9001',
                    'client_order_id': 'abc123456',
                    'side': 'buy',
                    'position_side': 'both',
                    'qty': 0.02,
                    'price': 2050.0,
                    'realized_pnl': 0.0,
                    'fee': 0.1,
                    'fee_asset': 'USDT',
                    'maker': False,
                    'time_ms': 1711380000001,
                },
            )()
        ]

    def get_order(self, symbol=None, order_id=None, client_order_id=None):
        assert symbol == 'BTCUSDT'
        assert order_id == '9001' or client_order_id == 'abc123456'
        return type(
            'Order',
            (),
            {
                'order_id': str(order_id or '9001'),
                'client_order_id': client_order_id or 'abc123456',
                'status': 'FILLED',
                'type': 'MARKET',
                'side': 'buy',
                'position_side': 'both',
                'qty': 0.02,
                'executed_qty': 0.02,
                'price': 0.0,
                'avg_price': 2050.0,
                'reduce_only': False,
                'close_position': False,
            },
        )()


class BinanceReadonlyProbeCase(unittest.TestCase):
    def test_resolve_order_probe_inputs_prefers_explicit_and_rejects_both(self):
        order_id, client_order_id, source = _resolve_order_probe_inputs(
            explicit_order_id='123',
            explicit_client_order_id=None,
            recent_trades=[],
        )
        self.assertEqual((order_id, client_order_id, source), ('123', None, 'explicit_order_id'))

        with self.assertRaises(ValueError):
            _resolve_order_probe_inputs(
                explicit_order_id='123',
                explicit_client_order_id='abc',
                recent_trades=[],
            )

    def test_resolve_order_probe_inputs_can_fallback_to_recent_trade(self):
        trade = type('Trade', (), {'order_id': '9001', 'client_order_id': 'abc123456'})()
        order_id, client_order_id, source = _resolve_order_probe_inputs(
            explicit_order_id=None,
            explicit_client_order_id=None,
            recent_trades=[trade],
        )
        self.assertEqual((order_id, client_order_id, source), ('9001', None, 'recent_trade_order_id'))

    def test_mask_value_does_not_expose_full_secret(self):
        masked = _mask_value('ABCDEFGHIJKL')
        self.assertEqual(masked, 'ABC***KL')
        self.assertNotIn('DEFGHIJ', masked)

    def test_classify_error_maps_auth_http_error(self):
        err = RuntimeError(json.dumps({'kind': 'http_error', 'status': 401, 'payload': {'code': -2015, 'msg': 'Invalid API-key'}}))
        info = _classify_error(err)
        self.assertEqual(info['category'], 'auth')
        self.assertEqual(info['status'], 401)
        self.assertEqual(info['code'], -2015)

    def test_run_probe_output_is_sanitized(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / 'binance_api.env'
            env_path.write_text(
                '\n'.join(
                    [
                        'BINANCE_API_KEY=AKIA_TEST_KEY_123456',
                        'BINANCE_API_SECRET=SECRET_TEST_VALUE_987654',
                        'BINANCE_SYMBOL=BTCUSDT',
                    ]
                ),
                encoding='utf-8',
            )

            import exec_framework.binance_readonly_probe as probe_module

            original = probe_module.BinanceReadOnlyClient
            probe_module.BinanceReadOnlyClient = FakeProbeClient
            try:
                result = run_probe(env_path)
            finally:
                probe_module.BinanceReadOnlyClient = original

            rendered = json.dumps(result, ensure_ascii=False)
            self.assertTrue(result['summary']['connectivity_ok'])
            self.assertTrue(result['summary']['account_readable'])
            self.assertEqual(result['summary']['account_mode'], 'one_way')
            self.assertFalse(result['summary']['has_positions'])
            self.assertTrue(result['summary']['order_readable'])
            self.assertEqual(result['endpoints']['order']['summary']['probe_source'], 'recent_trade_order_id')
            self.assertNotIn('AKIA_TEST_KEY_123456', rendered)
            self.assertNotIn('SECRET_TEST_VALUE_987654', rendered)
            self.assertEqual(result['credential_presence']['api_key_masked'], 'AKI***56')
            self.assertEqual(result['credential_presence']['api_secret_masked'], 'SEC***54')
            self.assertEqual(result['endpoints']['user_trades']['summary']['recent_trades'][0]['client_order_id_masked'], 'abc***56')


if __name__ == '__main__':
    unittest.main()
