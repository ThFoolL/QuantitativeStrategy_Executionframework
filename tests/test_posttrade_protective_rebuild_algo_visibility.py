from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
from exec_framework.executor_real import BinanceOrderRequest


class AlgoVisibleProtectiveReadOnlyClient:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        raise RuntimeError('{"kind":"http_error","path":"/fapi/v1/order","status":400,"payload":{"code":-2013,"msg":"Order does not exist."}}')

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        return []

    def get_position_snapshot(self, symbol=None):
        return type(
            'Pos',
            (),
            {
                'symbol': symbol or 'BTCUSDT',
                'side': 'long',
                'qty': 0.5,
                'entry_price': 2100.0,
                'break_even_price': None,
                'mark_price': None,
                'unrealized_pnl': None,
                'leverage': None,
                'margin_type': None,
                'position_side_mode': 'one_way',
                'raw': {},
            },
        )()

    def get_open_orders(self, symbol=None, client_order_ids=None):
        return []

    def _get_algo_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return {
            'orderId': '88001',
            'clientOrderId': client_order_id,
            'status': 'NEW',
            'type': 'STOP_MARKET',
            'origType': 'STOP_MARKET',
            'side': 'SELL',
            'positionSide': 'BOTH',
            'origQty': '0',
            'executedQty': '0',
            'avgPrice': '0',
            'reduceOnly': True,
            'closePosition': True,
            'updateTime': 1711380010000,
            'stopPrice': '2050',
        }

    def _parse_order_snapshot(self, row: dict):
        return type(
            'AlgoOrder',
            (),
            {
                'order_id': str(row.get('orderId')),
                'client_order_id': row.get('clientOrderId'),
                'status': str(row.get('status') or '').upper(),
                'type': row.get('type'),
                'orig_type': row.get('origType'),
                'side': str(row.get('side') or '').lower(),
                'position_side': str(row.get('positionSide') or '').lower(),
                'qty': float(row.get('origQty', 0.0)),
                'executed_qty': float(row.get('executedQty', 0.0)),
                'avg_price': float(row.get('avgPrice', 0.0)),
                'reduce_only': row.get('reduceOnly'),
                'close_position': row.get('closePosition'),
                'update_time_ms': int(row.get('updateTime')),
                'stop_price': float(row.get('stopPrice', 0.0)),
                'raw': {'is_algo_order': True},
            },
        )()


class ProtectiveRebuildAlgoVisibilityCase(unittest.TestCase):
    def test_algo_lookup_visible_keeps_protective_rebuild_confirmed(self) -> None:
        confirmer = BinancePostTradeConfirmer(AlgoVisibleProtectiveReadOnlyClient())
        request = BinanceOrderRequest(
            symbol='BTCUSDT',
            side='SELL',
            order_type='STOP_MARKET',
            quantity=None,
            reduce_only=True,
            position_side=None,
            client_order_id='protect-hard-stop-cid',
            metadata={
                'protective_order': True,
                'algo_order': True,
                'protective_kind': 'hard_stop',
            },
            close_position=True,
            stop_price=2050.0,
        )
        confirmation = confirmer.confirm(
            market=type('Market', (), {'symbol': 'BTCUSDT'})(),
            order_requests=[request],
            simulated_receipts=[
                SimulatedExecutionReceipt(
                    client_order_id='protect-hard-stop-cid',
                    exchange_order_id='88001',
                    acknowledged=True,
                    metadata={'protective_order': True, 'algo_order': True},
                )
            ],
        )

        self.assertEqual(confirmation.confirmation_status, 'CONFIRMED')
        self.assertEqual(confirmation.reconcile_status, 'OK')
        self.assertFalse(confirmation.should_freeze)
        self.assertIsNone(confirmation.freeze_reason)
        trade_summary = confirmation.trade_summary or {}
        protective_validation = trade_summary.get('protective_validation') or {}
        exchange_visibility = protective_validation.get('exchange_visibility') or {}
        self.assertTrue(exchange_visibility.get('exchange_visible'))
        self.assertEqual(exchange_visibility.get('source'), 'algo_order')
        self.assertTrue(protective_validation.get('ok'))
        self.assertIn('exchange_protective_fact_visible', protective_validation.get('notes') or [])
        self.assertNotIn('protective_order_missing', protective_validation.get('notes') or [])


if __name__ == '__main__':
    unittest.main()
