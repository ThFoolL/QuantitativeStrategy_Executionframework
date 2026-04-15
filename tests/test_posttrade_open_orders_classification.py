from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
from exec_framework.executor_real import BinanceOrderRequest


class _BaseReadOnly:
    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.5, 'entry_price': 2100.0, 'position_side_mode': 'one_way'})()

    def _open_order(self, **overrides):
        base = {
            'order_id': 'oo-1',
            'client_order_id': 'cid-open',
            'status': 'NEW',
            'side': 'buy',
            'position_side': 'both',
            'qty': 0.5,
            'executed_qty': 0.0,
            'price': 2099.0,
            'avg_price': 0.0,
            'reduce_only': False,
            'close_position': False,
            'update_time_ms': 1711381000000,
        }
        base.update(overrides)
        return type('OpenOrder', (), base)()


class StubPendingWithOpenOrders(_BaseReadOnly):
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type('Order', (), {'status': 'NEW', 'order_id': '3001', 'executed_qty': 0.0, 'qty': 0.5, 'avg_price': None, 'reduce_only': False, 'side': 'buy', 'position_side': 'both', 'update_time_ms': 1711381000000})()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return []

    def get_open_orders(self, symbol=None):
        return [self._open_order(order_id='3001', client_order_id='cid-pending', qty=0.5)]

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': None, 'qty': 0.0, 'entry_price': None, 'position_side_mode': 'one_way'})()


class StubFilledResidualOpenOrders(_BaseReadOnly):
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type('Order', (), {'status': 'FILLED', 'order_id': '3002', 'executed_qty': 0.5, 'qty': 0.5, 'avg_price': 2101.0, 'reduce_only': False, 'side': 'buy', 'position_side': 'both', 'update_time_ms': 1711381000100})()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return [{'id': 1, 'orderId': '3002', 'clientOrderId': 'cid-filled-residual', 'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'qty': '0.5', 'price': '2101', 'commission': '0.2', 'commissionAsset': 'USDT', 'realizedPnl': '0', 'side': 'BUY', 'maker': False, 'buyer': True, 'time': 1711381000100}]

    def get_open_orders(self, symbol=None):
        return [self._open_order(order_id='3999', client_order_id='cid-stop-left', side='sell', qty=0.5, price=0.0)]


class StubOpenOrdersConflict(_BaseReadOnly):
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type('Order', (), {'status': 'FILLED', 'order_id': '3003', 'executed_qty': 0.4, 'qty': 0.4, 'avg_price': 2102.0, 'reduce_only': False, 'side': 'buy', 'position_side': 'both', 'update_time_ms': 1711381000200})()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return [{'id': 2, 'orderId': '3003', 'clientOrderId': 'cid-conflict', 'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'qty': '0.4', 'price': '2102', 'commission': '0.2', 'commissionAsset': 'USDT', 'realizedPnl': '0', 'side': 'BUY', 'maker': False, 'buyer': True, 'time': 1711381000200}]

    def get_open_orders(self, symbol=None):
        return [self._open_order(order_id='3998', client_order_id='cid-conflict-open', side='sell', qty=0.8, price=2110.0)]

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.4, 'entry_price': 2102.0, 'position_side_mode': 'one_way'})()


class StubReduceOnlyOpenOrders(_BaseReadOnly):
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type('Order', (), {'status': 'FILLED', 'order_id': '3004', 'executed_qty': 0.4, 'qty': 0.4, 'avg_price': 2100.0, 'reduce_only': True, 'side': 'sell', 'position_side': 'both', 'update_time_ms': 1711381000300})()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return [{'id': 3, 'orderId': '3004', 'clientOrderId': 'cid-reduce-open', 'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'qty': '0.4', 'price': '2100', 'commission': '0.18', 'commissionAsset': 'USDT', 'realizedPnl': '1.5', 'side': 'SELL', 'maker': False, 'buyer': False, 'time': 1711381000300}]

    def get_open_orders(self, symbol=None):
        return [self._open_order(order_id='3997', client_order_id='cid-reduce-remain', side='sell', qty=0.1, price=0.0, reduce_only=True)]

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.1, 'entry_price': 2095.0, 'position_side_mode': 'one_way'})()


class PostTradeOpenOrdersClassificationCase(unittest.TestCase):
    def make_market(self):
        return type('Market', (), {'symbol': 'BTCUSDT'})()

    def test_pending_with_open_orders_stays_pending_and_freezes(self):
        confirmer = BinancePostTradeConfirmer(StubPendingWithOpenOrders())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[BinanceOrderRequest(symbol='BTCUSDT', side='BUY', order_type='MARKET', quantity=0.5, reduce_only=False, position_side=None, client_order_id='cid-pending', metadata={})],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-pending', exchange_order_id='3001', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_category, 'pending')
        self.assertEqual(confirmation.freeze_reason, 'posttrade_open_orders_still_pending')
        self.assertIn('open_orders_still_pending', confirmation.notes)
        self.assertTrue(confirmation.trade_summary['has_open_orders'])
        self.assertEqual(confirmation.trade_summary['open_orders_count'], 1)

    def test_filled_with_residual_open_orders_becomes_mismatch(self):
        confirmer = BinancePostTradeConfirmer(StubFilledResidualOpenOrders())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[BinanceOrderRequest(symbol='BTCUSDT', side='BUY', order_type='MARKET', quantity=0.5, reduce_only=False, position_side=None, client_order_id='cid-filled-residual', metadata={})],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-filled-residual', exchange_order_id='3002', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_category, 'mismatch')
        self.assertEqual(confirmation.freeze_reason, 'posttrade_filled_but_open_orders_still_live')
        self.assertIn('filled_but_open_orders_still_live', confirmation.notes)
        self.assertIn('residual_open_orders_after_fill', confirmation.notes)

    def test_filled_with_open_orders_side_or_qty_conflict_becomes_mismatch(self):
        confirmer = BinancePostTradeConfirmer(StubOpenOrdersConflict())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[BinanceOrderRequest(symbol='BTCUSDT', side='BUY', order_type='MARKET', quantity=0.4, reduce_only=False, position_side=None, client_order_id='cid-conflict', metadata={})],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-conflict', exchange_order_id='3003', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_category, 'mismatch')
        self.assertIn('open_orders_side_or_qty_conflict', confirmation.notes)
        self.assertEqual(confirmation.trade_summary['open_orders'][0]['side'], 'sell')

    def test_reduce_only_with_open_orders_still_live_remains_frozen(self):
        confirmer = BinancePostTradeConfirmer(StubReduceOnlyOpenOrders())
        confirmation = confirmer.confirm(
            market=self.make_market(),
            order_requests=[BinanceOrderRequest(symbol='BTCUSDT', side='SELL', order_type='MARKET', quantity=0.4, reduce_only=True, position_side=None, client_order_id='cid-reduce-open', metadata={})],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-reduce-open', exchange_order_id='3004', acknowledged=True)],
        )
        self.assertEqual(confirmation.confirmation_category, 'mismatch')
        self.assertIn('reduce_only_open_orders_still_live', confirmation.notes)
        self.assertIn('reduce_only_filled_but_position_not_flat', confirmation.notes)
        self.assertTrue(confirmation.should_freeze)


if __name__ == '__main__':
    unittest.main()
