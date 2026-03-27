from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly_sample_capture import (
    build_sample_pack,
    sanitize_order_row,
    sanitize_position_row,
    sanitize_trade_row,
)


class BinanceReadonlySampleCaptureCase(unittest.TestCase):
    def test_sanitize_order_row_masks_sensitive_ids(self) -> None:
        row = sanitize_order_row({'orderId': '1234567890', 'clientOrderId': 'cli-abcdef123456', 'symbol': 'BTCUSDT', 'status': 'FILLED'})
        self.assertEqual(row['order_id_masked'], '123***90')
        self.assertEqual(row['client_order_id_masked'], 'cli***56')
        self.assertEqual(row['status'], 'FILLED')

    def test_sanitize_trade_row_keeps_trade_facts_but_masks_ids(self) -> None:
        row = sanitize_trade_row({'id': '99887766', 'orderId': '1234567890', 'clientOrderId': 'cli-abcdef123456', 'qty': '0.5', 'price': '2100.1'})
        self.assertEqual(row['trade_id_masked'], '998***66')
        self.assertEqual(row['order_id_masked'], '123***90')
        self.assertEqual(row['qty'], '0.5')

    def test_sanitize_position_row_does_not_introduce_secret_fields(self) -> None:
        row = sanitize_position_row({'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'positionAmt': '0.4', 'entryPrice': '2101.2'})
        self.assertEqual(row['symbol'], 'BTCUSDT')
        self.assertNotIn('api_key', row)
        self.assertEqual(row['positionAmt'], '0.4')

    def test_build_sample_pack_contains_all_required_sections(self) -> None:
        pack = build_sample_pack(
            symbol='BTCUSDT',
            source_label='fixture_pack',
            order={'orderId': '1234567890', 'clientOrderId': 'cli-abcdef123456', 'status': 'FILLED'},
            user_trades=[{'id': '99887766', 'orderId': '1234567890', 'clientOrderId': 'cli-abcdef123456', 'qty': '0.5', 'price': '2100.1'}],
            position_risk=[{'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'positionAmt': '0.5'}],
            open_orders=[],
            meta={
                'order_lookup': {
                    'resolved': {'source': 'recent_trades_inferred_order_id'},
                },
            },
        )
        self.assertTrue(pack['collection_meta']['readonly_only'])
        self.assertEqual(pack['order']['order_id_masked'], '123***90')
        self.assertEqual(len(pack['user_trades']), 1)
        self.assertEqual(pack['position_risk'][0]['positionAmt'], '0.5')
        self.assertEqual(pack['collection_meta']['order_lookup']['resolved']['source'], 'recent_trades_inferred_order_id')


if __name__ == '__main__':
    unittest.main()
