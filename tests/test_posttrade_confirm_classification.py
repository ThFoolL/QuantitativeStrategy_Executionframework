from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
from exec_framework.executor_real import BinanceOrderRequest


class StubReadOnlyFilledNoTrades:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'FILLED',
                'order_id': '2001',
                'executed_qty': 0.5,
                'qty': 0.5,
                'avg_price': 2100.0,
                'reduce_only': False,
                'side': 'buy',
                'position_side': 'both',
                'update_time_ms': 1711380000001,
            },
        )()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return []

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.5, 'entry_price': 2100.0, 'position_side_mode': 'one_way'})()


class StubReadOnlyFilledQtyMismatch:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'FILLED',
                'order_id': '2002',
                'executed_qty': 0.5,
                'qty': 0.5,
                'avg_price': 2100.0,
                'reduce_only': False,
                'side': 'buy',
                'position_side': 'both',
                'update_time_ms': 1711380000001,
            },
        )()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return [
            {
                'id': 1,
                'orderId': '2002',
                'clientOrderId': 'cid-qty-mismatch',
                'symbol': 'BTCUSDT',
                'positionSide': 'BOTH',
                'qty': '0.3',
                'price': '2100',
                'commission': '0.3',
                'commissionAsset': 'USDT',
                'realizedPnl': '0',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380000001,
            }
        ]

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.3, 'entry_price': 2100.0, 'position_side_mode': 'one_way'})()


class StubReadOnlyReduceOnlyFilledNotFlat:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'FILLED',
                'order_id': '2003',
                'executed_qty': 0.4,
                'qty': 0.4,
                'avg_price': 2100.0,
                'reduce_only': True,
                'side': 'sell',
                'position_side': 'both',
                'update_time_ms': 1711380000001,
            },
        )()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return [
            {
                'id': 1,
                'orderId': '2003',
                'clientOrderId': 'cid-reduce-only',
                'symbol': 'BTCUSDT',
                'positionSide': 'BOTH',
                'qty': '0.4',
                'price': '2100',
                'commission': '0.3',
                'commissionAsset': 'USDT',
                'realizedPnl': '1.2',
                'side': 'SELL',
                'maker': False,
                'buyer': False,
                'time': 1711380000001,
            }
        ]

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.1, 'entry_price': 2095.0, 'position_side_mode': 'one_way'})()


class PostTradeConfirmClassificationCase(unittest.TestCase):
    def make_market(self):
        return type('Market', (), {'symbol': 'BTCUSDT'})()

    def test_filled_without_matching_trades_becomes_query_failed(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyFilledNoTrades())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='BTCUSDT',
                    side='BUY',
                    order_type='MARKET',
                    quantity=0.5,
                    reduce_only=False,
                    position_side=None,
                    client_order_id='cid-no-trades',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-no-trades', exchange_order_id='2001', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_status, 'UNCONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'query_failed')
        self.assertEqual(confirmation.reconcile_status, 'POST_TRADE_QUERY_FAILED')
        self.assertEqual(confirmation.freeze_reason, 'posttrade_missing_fills')
        self.assertIn('filled_without_user_trades', confirmation.notes)
        self.assertIn('no_matching_user_trades', confirmation.notes)

    def test_filled_but_executed_qty_less_than_requested_is_mismatch(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyFilledQtyMismatch())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='BTCUSDT',
                    side='BUY',
                    order_type='MARKET',
                    quantity=0.5,
                    reduce_only=False,
                    position_side=None,
                    client_order_id='cid-qty-mismatch',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-qty-mismatch', exchange_order_id='2002', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_status, 'UNCONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'mismatch')
        self.assertEqual(confirmation.freeze_reason, 'posttrade_filled_but_qty_mismatch')
        self.assertIn('filled_but_executed_qty_less_than_requested', confirmation.notes)
        self.assertEqual(confirmation.trade_summary['requested_qty'], 0.5)
        self.assertEqual(confirmation.trade_summary['executed_qty'], 0.3)

    def test_reduce_only_filled_but_position_not_flat_requires_freeze(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyReduceOnlyFilledNotFlat())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='BTCUSDT',
                    side='SELL',
                    order_type='MARKET',
                    quantity=0.4,
                    reduce_only=True,
                    position_side=None,
                    client_order_id='cid-reduce-only',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-reduce-only', exchange_order_id='2003', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_status, 'UNCONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'mismatch')
        self.assertEqual(confirmation.freeze_reason, 'posttrade_reduce_only_position_not_flat')
        self.assertIn('reduce_only_filled_but_position_not_flat', confirmation.notes)
        self.assertTrue(confirmation.should_freeze)


if __name__ == '__main__':
    unittest.main()
