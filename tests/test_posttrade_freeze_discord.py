from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
from exec_framework.discord_publisher import DiscordPublisher
from exec_framework.discord_sender_bridge import MessageToolDiscordSender
from exec_framework.executor_real import BinanceOrderRequest
from exec_framework.models import ExecutionResult, LiveStateSnapshot, MarketSnapshot
from exec_framework.runtime_guard import RuntimeFreezeController


class StubReadOnlyPending:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type('Order', (), {'status': 'NEW', 'order_id': '1001'})()

    def get_recent_trades(self, symbol=None, limit=100):
        return []

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': None, 'qty': 0.0, 'entry_price': None})()


class StubReadOnlyFilled:
    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type(
            'Order',
            (),
            {
                'status': 'FILLED',
                'order_id': '1002',
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
                'orderId': '1002',
                'clientOrderId': 'cid-1',
                'symbol': 'BTCUSDT',
                'positionSide': 'BOTH',
                'qty': '0.5',
                'price': '2100',
                'commission': '0.9',
                'commissionAsset': 'USDT',
                'realizedPnl': '0',
                'side': 'BUY',
                'maker': False,
                'buyer': True,
                'time': 1711380000001,
            }
        ]

    def get_position_snapshot(self, symbol=None):
        return type('Pos', (), {'side': 'long', 'qty': 0.5, 'entry_price': 2100.0, 'position_side_mode': 'one_way'})()


class PostTradeFreezeDiscordCase(unittest.TestCase):
    def make_market(self) -> MarketSnapshot:
        return MarketSnapshot(
            decision_ts='2026-03-25T15:00:33+00:00',
            bar_ts='2026-03-25T15:00:00+00:00',
            strategy_ts=None,
            execution_attributed_bar=None,
            symbol='BTCUSDT',
            preclose_offset_seconds=27,
            current_price=2100.0,
            source_status='OK',
        )

    def make_state(self, **overrides) -> LiveStateSnapshot:
        base = dict(
            state_ts='2026-03-25T15:00:33+00:00',
            consistency_status='OK',
            freeze_reason=None,
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side=None,
            exchange_position_qty=0.0,
            exchange_entry_price=None,
            active_strategy='none',
            active_side=None,
            strategy_entry_time=None,
            strategy_entry_price=None,
            stop_price=None,
            risk_fraction=None,
            runtime_mode='ACTIVE',
            freeze_status='NONE',
            last_freeze_reason=None,
            last_freeze_at=None,
            last_recover_at=None,
            last_recover_result=None,
            recover_attempt_count=0,
            pending_execution_phase=None,
            last_confirmed_order_ids=[],
        )
        base.update(overrides)
        return LiveStateSnapshot(**base)

    def make_confirmed_result(self, *, action_type: str = 'open') -> ExecutionResult:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyFilled())
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
                    client_order_id='cid-1',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-1', exchange_order_id='1002', acknowledged=True)],
        )
        return ExecutionResult(
            result_ts='2026-03-25T15:00:33+00:00',
            bar_ts='2026-03-25T15:00:00+00:00',
            status='FILLED',
            action_type=action_type,
            executed_side='long' if action_type == 'open' else 'short',
            executed_qty=confirmation.executed_qty,
            avg_fill_price=confirmation.avg_fill_price,
            fees=confirmation.fees,
            exchange_order_ids=confirmation.exchange_order_ids,
            post_position_side=confirmation.post_position_side if action_type == 'open' else None,
            post_position_qty=confirmation.post_position_qty if action_type == 'open' else 0.0,
            post_entry_price=confirmation.post_entry_price if action_type == 'open' else None,
            reconcile_status=confirmation.reconcile_status,
            should_freeze=confirmation.should_freeze,
            freeze_reason=confirmation.freeze_reason,
            execution_phase='confirmed',
            confirmation_status=confirmation.confirmation_status,
            confirmed_order_status=confirmation.order_status,
            trade_summary=confirmation.trade_summary,
        )

    def test_posttrade_unconfirmed_cannot_build_execution_confirmation(self) -> None:
        confirmer = BinancePostTradeConfirmer(StubReadOnlyPending())
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
                    client_order_id='cid-1',
                    metadata={},
                )
            ],
            simulated_receipts=[SimulatedExecutionReceipt(client_order_id='cid-1', exchange_order_id='1001', acknowledged=True)],
        )
        result = ExecutionResult(
            result_ts='2026-03-25T15:00:33+00:00',
            bar_ts='2026-03-25T15:00:00+00:00',
            status='NEW',
            action_type='open',
            executed_side='long',
            executed_qty=confirmation.executed_qty,
            avg_fill_price=confirmation.avg_fill_price,
            fees=confirmation.fees,
            exchange_order_ids=confirmation.exchange_order_ids,
            post_position_side=confirmation.post_position_side,
            post_position_qty=confirmation.post_position_qty,
            post_entry_price=confirmation.post_entry_price,
            reconcile_status=confirmation.reconcile_status,
            should_freeze=confirmation.should_freeze,
            freeze_reason=confirmation.freeze_reason,
            execution_phase='submitted',
            confirmation_status=confirmation.confirmation_status,
            confirmed_order_status=confirmation.order_status,
            trade_summary=confirmation.trade_summary,
        )
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        with self.assertRaises(ValueError):
            publisher.build_execution_confirmation(market=self.make_market(), state=self.make_state(), result=result)

    def test_freeze_state_cannot_build_normal_execution_confirmation(self) -> None:
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        result = ExecutionResult(
            result_ts='2026-03-25T15:00:33+00:00',
            bar_ts='2026-03-25T15:00:00+00:00',
            status='FROZEN',
            action_type='open',
            executed_side='long',
            executed_qty=0.5,
            avg_fill_price=2100.0,
            fees=0.9,
            exchange_order_ids=['1002'],
            post_position_side='long',
            post_position_qty=0.5,
            post_entry_price=2100.0,
            reconcile_status='OK',
            should_freeze=True,
            freeze_reason='posttrade_pending_confirmation',
            execution_phase='frozen',
            confirmation_status='PENDING',
            confirmed_order_status='PARTIALLY_FILLED',
        )
        with self.assertRaises(ValueError):
            publisher.build_execution_confirmation(
                market=self.make_market(),
                state=self.make_state(runtime_mode='FROZEN', freeze_status='ACTIVE', freeze_reason='posttrade_pending_confirmation'),
                result=result,
            )

    def test_recover_preconditions(self) -> None:
        controller = RuntimeFreezeController()
        blocked = controller.evaluate_recover(
            self.make_state(runtime_mode='FROZEN', freeze_status='ACTIVE', consistency_status='MISMATCH', pending_execution_phase='frozen')
        )
        self.assertFalse(blocked.allowed)
        self.assertIn('consistency_not_ok', blocked.reason)

        allowed = controller.evaluate_recover(
            self.make_state(runtime_mode='FROZEN', freeze_status='ACTIVE', consistency_status='OK', pending_execution_phase=None)
        )
        self.assertTrue(allowed.allowed)
        self.assertEqual(allowed.result, 'RECOVERED')

    def test_confirmed_result_can_build_payload(self) -> None:
        result = self.make_confirmed_result()
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        payload = publisher.build_execution_confirmation(market=self.make_market(), state=self.make_state(), result=result)
        self.assertEqual(result.trade_summary['confirmation_category'], 'confirmed')
        self.assertEqual(payload.channel_id, 'DISCORD_CHANNEL_ID_PLACEHOLDER')
        self.assertIn('执行确认', payload.content)
        self.assertIn('交易对: BTCUSDT', payload.content)
        self.assertIn('手续费: 0.9', payload.content)

    def test_close_confirmation_payload_contains_realized_pnl_in_chinese(self) -> None:
        result = self.make_confirmed_result(action_type='close')
        result.trade_summary = {'fills': [{'realized_pnl': 12.3}, {'realized_pnl': -0.3}]}
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        payload = publisher.build_execution_confirmation(market=self.make_market(), state=self.make_state(), result=result)
        self.assertIn('盈亏: 12.0', payload.content)
        self.assertIn('动作: close', payload.content)

    def test_rehearsal_message_schema_and_preview_are_separate(self) -> None:
        result = self.make_confirmed_result()
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        payload = publisher.build_rehearsal_message(market=self.make_market(), state=self.make_state(), result=result)
        self.assertEqual(payload.metadata['kind'], 'rehearsal_notification')
        self.assertTrue(payload.metadata['rehearsal'])
        self.assertIn('【演练】【非真实发单】', payload.content)
        self.assertNotIn('【执行确认】', payload.content)
        self.assertIn('不是成交回报', payload.content)
        preview = publisher.build_rehearsal_preview(market=self.make_market(), state=self.make_state(), result=result)
        self.assertEqual(preview['kind'], 'rehearsal_notification')
        self.assertIn('preview', preview['payload_preview']['metadata'])

    def test_build_dispatch_audit_contains_full_preview_and_idempotency_key(self) -> None:
        result = self.make_confirmed_result()
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        audit = publisher.build_dispatch_audit(market=self.make_market(), state=self.make_state(), result=result)
        self.assertTrue(audit['eligible'])
        self.assertEqual(audit['dispatch']['target'], 'DISCORD_CHANNEL_ID_PLACEHOLDER')
        self.assertEqual(audit['kind'], 'execution_confirmation')
        self.assertIn('idempotency_key', audit)
        self.assertTrue(audit['idempotency_key'].startswith('discord:BTCUSDT:'))
        self.assertFalse(audit['minimum_live_send_config']['discord_real_send_enabled'])
        self.assertEqual(audit['rehearsal_preview']['kind'], 'rehearsal_notification')

    def test_confirmed_result_can_enter_sender_bridge_but_not_real_send(self) -> None:
        result = self.make_confirmed_result()
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        sender = MessageToolDiscordSender()
        attempt = publisher.send_via_bridge(
            market=self.make_market(),
            state=self.make_state(),
            result=result,
            sender=sender,
        )
        self.assertTrue(attempt.eligible)
        self.assertIsNotNone(attempt.payload)
        self.assertFalse(attempt.sender_result['sent'])
        self.assertEqual(attempt.sender_result['dispatch']['target'], 'DISCORD_CHANNEL_ID_PLACEHOLDER')

    def test_rehearsal_result_can_enter_sender_bridge_but_stays_blocked_by_default(self) -> None:
        result = self.make_confirmed_result()
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        sender = MessageToolDiscordSender()
        attempt = publisher.send_rehearsal_via_bridge(
            market=self.make_market(),
            state=self.make_state(),
            result=result,
            sender=sender,
        )
        self.assertTrue(attempt.eligible)
        self.assertEqual(attempt.payload.metadata['kind'], 'rehearsal_notification')
        self.assertFalse(attempt.sender_result['sent'])
        self.assertIn('discord_real_send_disabled', attempt.sender_result['send_gate']['blockers'])

    def test_unconfirmed_result_cannot_enter_sender_bridge(self) -> None:
        publisher = DiscordPublisher('DISCORD_CHANNEL_ID_PLACEHOLDER')
        sender = MessageToolDiscordSender()
        result = ExecutionResult(
            result_ts='2026-03-25T15:00:33+00:00',
            bar_ts='2026-03-25T15:00:00+00:00',
            status='DRY_RUN',
            action_type='open',
            executed_side='long',
            executed_qty=0.0,
            avg_fill_price=None,
            fees=0.0,
            exchange_order_ids=[],
            post_position_side=None,
            post_position_qty=0.0,
            post_entry_price=None,
            reconcile_status='DRY_RUN',
            should_freeze=False,
            freeze_reason=None,
            execution_phase='planned',
            confirmation_status='UNCONFIRMED',
            confirmed_order_status='NOT_SUBMITTED',
        )
        attempt = publisher.send_via_bridge(
            market=self.make_market(),
            state=self.make_state(),
            result=result,
            sender=sender,
        )
        self.assertFalse(attempt.eligible)
        self.assertIsNone(attempt.payload)
        self.assertIsNone(attempt.sender_result)


if __name__ == '__main__':
    unittest.main()
