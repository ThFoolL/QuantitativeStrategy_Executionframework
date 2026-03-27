from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
from exec_framework.executor_real import BinanceOrderRequest
from exec_framework.runtime_status_cli import _build_operator_compact_view


SCENARIOS = [
    {
        'name': 'confirmed_open_complete',
        'request': {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': 0.5,
            'reduce_only': False,
            'client_order_id': 'sample-confirmed-open',
        },
        'receipt': {
            'client_order_id': 'sample-confirmed-open',
            'exchange_order_id': '91001',
            'acknowledged': True,
        },
        'order': {
            'status': 'FILLED',
            'order_id': '91001',
            'executed_qty': 0.5,
            'qty': 0.5,
            'avg_price': 2100.5,
            'reduce_only': False,
            'side': 'buy',
            'position_side': 'both',
            'update_time_ms': 1711380001000,
        },
        'trades': [
            {
                'id': 501,
                'orderId': '91001',
                'clientOrderId': 'sample-confirmed-open',
                'symbol': 'BTCUSDT',
                'positionSide': 'BOTH',
                'qty': '0.5',
                'price': '2100.5',
                'commission': '0.21',
                'commissionAsset': 'USDT',
                'realizedPnl': '0',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380001000,
            }
        ],
        'position': {
            'side': 'long',
            'qty': 0.5,
            'entry_price': 2100.5,
            'position_side_mode': 'one_way',
        },
        'expected': {
            'confirmation_status': 'CONFIRMED',
            'confirmation_category': 'confirmed',
            'reconcile_status': 'OK',
            'freeze_reason': None,
            'should_freeze': False,
            'notes_contains': [],
            'next_focus_contains': '继续观察',
        },
    },
    {
        'name': 'pending_partial_fill',
        'request': {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': 0.5,
            'reduce_only': False,
            'client_order_id': 'sample-pending-partial',
        },
        'receipt': {
            'client_order_id': 'sample-pending-partial',
            'exchange_order_id': '91002',
            'acknowledged': True,
        },
        'order': {
            'status': 'PARTIALLY_FILLED',
            'order_id': '91002',
            'executed_qty': 0.2,
            'qty': 0.5,
            'avg_price': 2098.0,
            'reduce_only': False,
            'side': 'buy',
            'position_side': 'both',
            'update_time_ms': 1711380002000,
        },
        'trades': [
            {
                'id': 601,
                'orderId': '91002',
                'clientOrderId': 'sample-pending-partial',
                'symbol': 'BTCUSDT',
                'positionSide': 'BOTH',
                'qty': '0.2',
                'price': '2098.0',
                'commission': '0.08',
                'commissionAsset': 'USDT',
                'realizedPnl': '0',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380002000,
            }
        ],
        'position': {
            'side': 'long',
            'qty': 0.2,
            'entry_price': 2098.0,
            'position_side_mode': 'one_way',
        },
        'expected': {
            'confirmation_status': 'PENDING',
            'confirmation_category': 'pending',
            'reconcile_status': 'PENDING_CONFIRMATION',
            'freeze_reason': 'posttrade_partial_fill_requires_manual_reconcile',
            'should_freeze': True,
            'notes_contains': ['partial_fill_detected', 'partial_fill_requires_freeze'],
            'next_focus_contains': 'openOrders + userTrades + positionRisk',
        },
    },
    {
        'name': 'query_failed_trade_lookup',
        'request': {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': 0.3,
            'reduce_only': False,
            'client_order_id': 'sample-query-failed',
        },
        'receipt': {
            'client_order_id': 'sample-query-failed',
            'exchange_order_id': '91003',
            'acknowledged': True,
        },
        'order': {
            'status': 'FILLED',
            'order_id': '91003',
            'executed_qty': 0.3,
            'qty': 0.3,
            'avg_price': 2102.0,
            'reduce_only': False,
            'side': 'buy',
            'position_side': 'both',
            'update_time_ms': 1711380003000,
        },
        'trades_error': 'temporary userTrades outage',
        'position': {
            'side': 'long',
            'qty': 0.3,
            'entry_price': 2102.0,
            'position_side_mode': 'one_way',
        },
        'expected': {
            'confirmation_status': 'UNCONFIRMED',
            'confirmation_category': 'query_failed',
            'reconcile_status': 'POST_TRADE_QUERY_FAILED',
            'freeze_reason': 'posttrade_missing_fills',
            'should_freeze': True,
            'notes_contains': ['trade_query_failed:RuntimeError'],
            'next_focus_contains': '优先补查 order / userTrades / positionRisk',
        },
    },
    {
        'name': 'filled_with_residual_open_orders',
        'request': {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': 0.5,
            'reduce_only': False,
            'client_order_id': 'sample-residual-open-orders',
        },
        'receipt': {
            'client_order_id': 'sample-residual-open-orders',
            'exchange_order_id': '91008',
            'acknowledged': True,
        },
        'order': {
            'status': 'FILLED',
            'order_id': '91008',
            'executed_qty': 0.5,
            'qty': 0.5,
            'avg_price': 2103.0,
            'reduce_only': False,
            'side': 'buy',
            'position_side': 'both',
            'update_time_ms': 1711380008000,
        },
        'trades': [
            {
                'id': 901,
                'orderId': '91008',
                'clientOrderId': 'sample-residual-open-orders',
                'symbol': 'BTCUSDT',
                'positionSide': 'BOTH',
                'qty': '0.5',
                'price': '2103.0',
                'commission': '0.18',
                'commissionAsset': 'USDT',
                'realizedPnl': '0',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380008000,
            }
        ],
        'open_orders': [
            {
                'orderId': '91009',
                'clientOrderId': 'sample-protective-order',
                'status': 'NEW',
                'type': 'STOP_MARKET',
                'timeInForce': 'GTC',
                'side': 'SELL',
                'positionSide': 'BOTH',
                'origQty': '0.5',
                'executedQty': '0.0',
                'price': '0',
                'avgPrice': '0',
                'cumQuote': '0',
                'reduceOnly': False,
                'closePosition': False,
                'updateTime': 1711380008010,
            }
        ],
        'position': {
            'side': 'long',
            'qty': 0.5,
            'entry_price': 2103.0,
            'position_side_mode': 'one_way',
        },
        'expected': {
            'confirmation_status': 'UNCONFIRMED',
            'confirmation_category': 'mismatch',
            'reconcile_status': 'POST_TRADE_MISMATCH',
            'freeze_reason': 'posttrade_filled_but_open_orders_still_live',
            'should_freeze': True,
            'notes_contains': ['filled_but_open_orders_still_live', 'residual_open_orders_after_fill'],
            'fill_count': 1,
            'executed_qty': 0.5,
            'next_focus_contains': 'requested_qty / executed_qty / post_position / openOrders',
        },
    },
    {
        'name': 'rejected_order',
        'request': {
            'symbol': 'BTCUSDT',
            'side': 'SELL',
            'quantity': 0.4,
            'reduce_only': True,
            'client_order_id': 'sample-rejected',
        },
        'receipt': {
            'client_order_id': 'sample-rejected',
            'exchange_order_id': '91005',
            'acknowledged': True,
        },
        'order': {
            'status': 'REJECTED',
            'order_id': '91005',
            'executed_qty': 0.0,
            'qty': 0.4,
            'avg_price': None,
            'reduce_only': True,
            'side': 'sell',
            'position_side': 'both',
            'update_time_ms': 1711380005000,
        },
        'trades': [],
        'position': {
            'side': 'long',
            'qty': 0.4,
            'entry_price': 2099.0,
            'position_side_mode': 'one_way',
        },
        'expected': {
            'confirmation_status': 'UNCONFIRMED',
            'confirmation_category': 'rejected',
            'reconcile_status': 'POST_TRADE_MISMATCH',
            'freeze_reason': 'posttrade_rejected_or_canceled',
            'should_freeze': True,
            'notes_contains': [],
            'next_focus_contains': '确认是否仅拒单',
        },
    },
]


class FixtureReadOnlyClient:
    def __init__(self, scenario: dict):
        self.scenario = scenario

    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type('Order', (), dict(self.scenario['order']))()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        if self.scenario.get('trades_error'):
            raise RuntimeError(self.scenario['trades_error'])
        return list(self.scenario.get('trades') or [])

    def get_position_snapshot(self, symbol=None):
        position = dict(self.scenario.get('position') or {})
        return type('Pos', (), position)()

    def get_open_orders(self, symbol=None):
        rows = list(self.scenario.get('open_orders') or [])
        parser = getattr(self, '_parse_order_snapshot', None)
        if callable(parser):
            return [parser(row) for row in rows]
        return [type('OpenOrder', (), row)() for row in rows]

    def _parse_order_snapshot(self, row: dict):
        return type(
            'OpenOrder',
            (),
            {
                'order_id': str(row.get('orderId')),
                'client_order_id': row.get('clientOrderId'),
                'status': str(row.get('status', 'UNKNOWN')).upper(),
                'side': (str(row.get('side')).lower() if row.get('side') else None),
                'position_side': (str(row.get('positionSide')).lower() if row.get('positionSide') else None),
                'qty': float(row.get('origQty', 0.0)) if row.get('origQty') not in (None, '', 'NULL') else None,
                'executed_qty': float(row.get('executedQty', 0.0)) if row.get('executedQty') not in (None, '', 'NULL') else None,
                'price': float(row.get('price', 0.0)) if row.get('price') not in (None, '', 'NULL') else None,
                'avg_price': float(row.get('avgPrice', 0.0)) if row.get('avgPrice') not in (None, '', 'NULL') else None,
                'reduce_only': row.get('reduceOnly'),
                'close_position': row.get('closePosition'),
                'update_time_ms': int(row.get('updateTime')) if row.get('updateTime') not in (None, '', 'NULL') else None,
            },
        )()


class PostTradeConfirmationScenariosCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scenarios = list(SCENARIOS)

    def make_market(self):
        return type('Market', (), {'symbol': 'BTCUSDT'})()

    def make_request(self, payload: dict) -> BinanceOrderRequest:
        return BinanceOrderRequest(
            symbol=payload['symbol'],
            side=payload['side'],
            order_type='MARKET',
            quantity=payload['quantity'],
            reduce_only=payload['reduce_only'],
            position_side=None,
            client_order_id=payload['client_order_id'],
            metadata={},
        )

    def test_minimal_scenarios_cover_confirmation_categories(self) -> None:
        seen_categories = set()
        for scenario in self.scenarios:
            with self.subTest(name=scenario['name']):
                confirmer = BinancePostTradeConfirmer(FixtureReadOnlyClient(scenario))
                confirmation = confirmer.confirm(
                    market=self.make_market(),
                    order_requests=[self.make_request(scenario['request'])],
                    simulated_receipts=[SimulatedExecutionReceipt(**scenario['receipt'])],
                )
                expected = scenario['expected']
                self.assertEqual(confirmation.confirmation_status, expected['confirmation_status'])
                self.assertEqual(confirmation.confirmation_category, expected['confirmation_category'])
                self.assertEqual(confirmation.reconcile_status, expected['reconcile_status'])
                self.assertEqual(confirmation.freeze_reason, expected['freeze_reason'])
                self.assertEqual(confirmation.should_freeze, expected['should_freeze'])
                for note in expected.get('notes_contains', []):
                    self.assertIn(note, confirmation.notes)
                if 'fill_count' in expected:
                    self.assertEqual(confirmation.fill_count, expected['fill_count'])
                if 'executed_qty' in expected:
                    self.assertAlmostEqual(confirmation.executed_qty, expected['executed_qty'])
                compact_view = _build_operator_compact_view(
                    runtime={'phase': 'completed'},
                    submit_gate=None,
                    freeze={
                        'runtime_mode': 'FROZEN' if expected['should_freeze'] else 'ACTIVE',
                        'freeze_status': 'ACTIVE' if expected['should_freeze'] else 'NONE',
                        'freeze_reason': confirmation.freeze_reason,
                        'last_recover_result': None,
                    },
                    confirm_summary={
                        'confirmation_category': confirmation.confirmation_category,
                        'confirmed_order_status': confirmation.order_status,
                        'freeze_reason': confirmation.freeze_reason,
                    },
                    position={
                        'exchange_position_side': confirmation.post_position_side,
                        'exchange_position_qty': confirmation.post_position_qty,
                    },
                    recover_check=None,
                    recover_timeline=None,
                )
                self.assertIn(expected['next_focus_contains'], compact_view['next_focus'])
                self.assertEqual(compact_view['confirmation_category'], expected['confirmation_category'])
                self.assertEqual(compact_view['hard_blocker'], expected['freeze_reason'])
                self.assertEqual(compact_view['recover_state'], None)
                seen_categories.add(confirmation.confirmation_category)

        self.assertTrue({'confirmed', 'pending', 'query_failed', 'mismatch', 'rejected'}.issubset(seen_categories))


if __name__ == '__main__':
    unittest.main()
