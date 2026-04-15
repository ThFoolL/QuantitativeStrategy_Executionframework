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

    def get_recent_trades(self, symbol=None, limit=100, order_id=None, start_time_ms=None, end_time_ms=None):
        return []

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.5, 'entry_price': 2100.0, 'position_side_mode': 'one_way'})()


class StubReadOnlyFilledTradesOnlyInTimeWindow(StubReadOnlyFilledNoTrades):
    def get_recent_trades(self, symbol=None, limit=100, order_id=None, start_time_ms=None, end_time_ms=None):
        if start_time_ms is None or end_time_ms is None:
            return []
        return [
            {
                'id': 11,
                'orderId': '2001',
                'clientOrderId': 'cid-no-trades',
                'symbol': 'ETHUSDT',
                'positionSide': 'BOTH',
                'qty': '0.5',
                'price': '2100',
                'commission': '0.2',
                'commissionAsset': 'USDT',
                'realizedPnl': '0',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380000005,
            }
        ]


class StubReadOnlyFilledSplitTrades:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'FILLED',
                'order_id': '2007',
                'executed_qty': 0.01,
                'qty': 0.01,
                'avg_price': 2330.17,
                'reduce_only': True,
                'side': 'buy',
                'position_side': 'both',
                'update_time_ms': 1711380000001,
            },
        )()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return [
            {
                'id': 21,
                'orderId': '2007',
                'clientOrderId': 'cid-split-fill',
                'symbol': 'ETHUSDT',
                'positionSide': 'BOTH',
                'qty': '0.009',
                'price': '2330.17',
                'commission': '0.01048576',
                'commissionAsset': 'USDT',
                'realizedPnl': '-0.01269',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380000001,
            },
            {
                'id': 22,
                'orderId': '2007',
                'clientOrderId': 'cid-split-fill',
                'symbol': 'ETHUSDT',
                'positionSide': 'BOTH',
                'qty': '0.001',
                'price': '2330.17',
                'commission': '0.00116508',
                'commissionAsset': 'USDT',
                'realizedPnl': '-0.00141',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380000001,
            },
        ]

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': None, 'qty': 0.0, 'entry_price': None, 'position_side_mode': 'one_way'})()


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
                'symbol': 'ETHUSDT',
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
                'symbol': 'ETHUSDT',
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


class StubReadOnlyReduceOnlyFilled:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'FILLED',
                'order_id': '2004',
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
        return []

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': None, 'qty': 0.0, 'entry_price': 0.0, 'position_side_mode': 'one_way'})()


class StubReadOnlyRejectedOrder:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'REJECTED',
                'order_id': '2005',
                'executed_qty': 0.0,
                'qty': 0.4,
                'avg_price': None,
                'reduce_only': True,
                'side': 'sell',
                'position_side': 'both',
                'update_time_ms': 1711380000002,
            },
        )()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return []

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.4, 'entry_price': 2099.0, 'position_side_mode': 'one_way'})()

    def get_open_orders(self, symbol=None, client_order_ids=None):
        return []


class StubReadOnlyPendingWithOpenOrders:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'NEW',
                'order_id': '2006',
                'executed_qty': 0.0,
                'qty': 0.5,
                'avg_price': None,
                'reduce_only': False,
                'side': 'buy',
                'position_side': 'both',
                'update_time_ms': 1711380000003,
            },
        )()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None, start_time_ms=None, end_time_ms=None):
        return []

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': None, 'qty': 0.0, 'entry_price': None, 'position_side_mode': 'one_way'})()

    def get_open_orders(self, symbol=None, client_order_ids=None):
        return [
            type(
                'OpenOrder',
                (),
                {
                    'order_id': '2006',
                    'client_order_id': 'cid-pending-open',
                    'status': 'NEW',
                    'side': 'buy',
                    'position_side': 'both',
                    'type': 'LIMIT',
                    'orig_type': 'LIMIT',
                    'qty': 0.5,
                    'executed_qty': 0.0,
                    'price': 2097.5,
                    'avg_price': 0.0,
                    'stop_price': None,
                    'working_type': None,
                    'reduce_only': False,
                    'close_position': False,
                    'update_time_ms': 1711380000003,
                },
            )()
        ]


class PostTradeConfirmClassificationCase(unittest.TestCase):
    def make_market(self):
        return type('Market', (), {'symbol': 'ETHUSDT'})()

    def test_filled_without_matching_trades_becomes_position_confirmed_when_position_present(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyFilledNoTrades())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
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
        self.assertEqual(confirmation.confirmation_status, 'POSITION_CONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'position_confirmed')
        self.assertEqual(confirmation.reconcile_status, 'OK')
        self.assertFalse(confirmation.should_freeze)
        self.assertIsNone(confirmation.freeze_reason)
        self.assertIn('filled_without_user_trades', confirmation.notes)
        self.assertIn('no_matching_user_trades', confirmation.notes)
        self.assertIn('position_confirmed_without_trade_rows', confirmation.notes)

    def test_filled_with_trades_found_in_time_window_becomes_confirmed(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyFilledTradesOnlyInTimeWindow())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
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
        self.assertEqual(confirmation.fill_count, 1)
        self.assertEqual(confirmation.executed_qty, 0.5)
        self.assertEqual(confirmation.avg_fill_price, 2100.0)
        self.assertNotIn('no_matching_user_trades', confirmation.notes)
        self.assertNotIn('position_confirmed_without_trade_rows', confirmation.notes)

    def test_filled_with_split_trade_rows_still_counts_all_fills(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyFilledSplitTrades())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
                    side='SELL',
                    order_type='MARKET',
                    quantity=0.01,
                    reduce_only=True,
                    position_side=None,
                    client_order_id='cid-split-fill',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-split-fill', exchange_order_id='2007', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_status, 'CONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'confirmed')
        self.assertEqual(confirmation.fill_count, 2)
        self.assertAlmostEqual(confirmation.executed_qty or 0.0, 0.01, places=8)
        self.assertAlmostEqual(confirmation.avg_fill_price or 0.0, 2330.17, places=2)
        self.assertEqual(confirmation.fee_assets, ['USDT'])
        self.assertNotIn('no_matching_user_trades', confirmation.notes)

    def test_filled_but_executed_qty_less_than_requested_is_mismatch(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyFilledQtyMismatch())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
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
        self.assertEqual(confirmation.trade_summary['protective_validation']['intent_state'], 'mismatch')

    def test_reduce_only_filled_but_position_not_flat_requires_freeze(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyReduceOnlyFilledNotFlat())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
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

    def test_reduce_only_filled_without_trades_but_flat_stays_position_confirmed(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyReduceOnlyFilled())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
                    side='SELL',
                    order_type='MARKET',
                    quantity=0.4,
                    reduce_only=True,
                    position_side=None,
                    client_order_id='cid-reduce-only-flat',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-reduce-only-flat', exchange_order_id='2004', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_status, 'POSITION_CONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'position_confirmed')
        self.assertIsNone(confirmation.freeze_reason)
        self.assertFalse(confirmation.should_freeze)
        self.assertIn('filled_qty_pending_trade_rows_after_flatten', confirmation.notes)
        self.assertIn('position_confirmed_without_trade_rows', confirmation.notes)

    def test_rejected_order_keeps_terminal_rejected_classification(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyRejectedOrder())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
                    side='SELL',
                    order_type='MARKET',
                    quantity=0.4,
                    reduce_only=True,
                    position_side=None,
                    client_order_id='cid-rejected',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-rejected', exchange_order_id='2005', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_status, 'UNCONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'rejected')
        self.assertEqual(confirmation.reconcile_status, 'POST_TRADE_MISMATCH')
        self.assertEqual(confirmation.freeze_reason, 'posttrade_rejected_or_canceled')
        self.assertFalse(confirmation.trade_summary['query_failed'])

    def test_pending_new_with_open_orders_still_live_stays_pending(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyPendingWithOpenOrders())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[
                BinanceOrderRequest(
                    symbol='ETHUSDT',
                    side='BUY',
                    order_type='LIMIT',
                    quantity=0.5,
                    reduce_only=False,
                    position_side=None,
                    client_order_id='cid-pending-open',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-pending-open', exchange_order_id='2006', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_status, 'PENDING')
        self.assertEqual(confirmation.confirmation_category, 'pending')
        self.assertEqual(confirmation.reconcile_status, 'PENDING_CONFIRMATION')
        self.assertEqual(confirmation.freeze_reason, 'posttrade_open_orders_still_pending')
        self.assertTrue(confirmation.should_freeze)
        self.assertFalse(confirmation.trade_summary['query_failed'])
        self.assertIn('pending_no_fill_yet', confirmation.notes)
        self.assertIn('open_orders_still_pending', confirmation.notes)


if __name__ == '__main__':
    unittest.main()
