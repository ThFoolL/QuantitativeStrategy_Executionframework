from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly_pack import adapt_readonly_pack, validate_readonly_pack


def make_pack() -> dict:
    return {
        'collection_meta': {
            'readonly_only': True,
            'source_label': 'fixture_pack',
            'symbol': 'BTCUSDT',
            'sensitive_fields_removed': ['orderId', 'clientOrderId', 'id', 'api_key', 'api_secret'],
        },
        'order': {
            'order_id_masked': '910***14',
            'client_order_id_masked': 'cli***56',
            'symbol': 'BTCUSDT',
            'status': 'CANCELED',
            'type': 'LIMIT',
            'side': 'BUY',
            'positionSide': 'BOTH',
            'origQty': '0.5',
            'executedQty': '0.2',
            'price': '2099.3',
            'avgPrice': '2099.3',
            'reduceOnly': False,
            'closePosition': False,
            'updateTime': 1711380011000,
        },
        'user_trades': [
            {
                'trade_id_masked': '904***05',
                'order_id_masked': '910***14',
                'client_order_id_masked': 'cli***56',
                'symbol': 'BTCUSDT',
                'side': 'BUY',
                'positionSide': 'BOTH',
                'qty': '0.2',
                'price': '2099.3',
                'commission': '0.08',
                'commissionAsset': 'USDT',
                'realizedPnl': '0',
                'maker': False,
                'buyer': True,
                'time': 1711380011000,
            }
        ],
        'position_risk': [
            {
                'symbol': 'BTCUSDT',
                'positionSide': 'BOTH',
                'positionAmt': '0.2',
                'entryPrice': '2099.3',
                'breakEvenPrice': '2099.5',
                'markPrice': '2100.1',
                'unRealizedProfit': '0.16',
                'marginType': 'cross',
                'leverage': '10',
                'isolatedMargin': '0',
                'liquidationPrice': '0',
                'updateTime': 1711380011010,
            }
        ],
        'open_orders': [],
    }


class BinanceReadonlyPackCase(unittest.TestCase):
    def test_validate_readonly_pack_accepts_redacted_pack(self) -> None:
        report = validate_readonly_pack(make_pack())
        self.assertTrue(report['ok'])
        self.assertTrue(report['ready_for_posttrade_fixture'])
        self.assertTrue(report['ready_for_operator_drill'])
        self.assertEqual(report['facts_summary']['order_status'], 'CANCELED')

    def test_validate_readonly_pack_rejects_unmasked_and_secret_fields(self) -> None:
        pack = make_pack()
        pack['order']['order_id_masked'] = '9100014'
        pack['collection_meta']['readonly_only'] = False
        pack['api_secret'] = 'should-not-appear'
        report = validate_readonly_pack(pack)
        self.assertFalse(report['ok'])
        self.assertIn('collection_meta.readonly_only_must_be_true', report['errors'])
        self.assertIn('order.order_id_masked_not_masked', report['errors'])
        self.assertIn('forbidden_secret_key_present:api_secret', report['errors'])

    def test_adapt_readonly_pack_builds_posttrade_fixture_shape(self) -> None:
        adapted = adapt_readonly_pack(make_pack(), scenario_name='readonly_canceled_partial_fill')
        self.assertEqual(adapted['adapter_version'], 'readonly_pack_to_fixture_v1')
        self.assertTrue(adapted['validation']['ok'])
        fixture = adapted['posttrade_fixture']
        self.assertEqual(fixture['name'], 'readonly_canceled_partial_fill')
        self.assertEqual(fixture['request']['client_order_id'], 'cli***56')
        self.assertEqual(fixture['receipt']['exchange_order_id'], '910***14')
        self.assertEqual(fixture['order']['status'], 'CANCELED')
        self.assertEqual(fixture['order']['side'], 'buy')
        self.assertEqual(fixture['position']['side'], 'long')
        self.assertAlmostEqual(fixture['position']['qty'], 0.2)
        self.assertEqual(fixture['trades'][0]['orderId'], '910***14')
        self.assertEqual(adapted['operator_context']['fact_sources'], ['order', 'user_trades', 'position_risk', 'open_orders'])
        self.assertTrue(adapted['operator_context']['freeze_recover_hint']['should_review_freeze'])


if __name__ == '__main__':
    unittest.main()
