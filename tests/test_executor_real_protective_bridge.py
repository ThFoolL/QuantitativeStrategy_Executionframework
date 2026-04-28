from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_posttrade import PostTradeConfirmation
from exec_framework.executor_real import BinanceRealExecutor
from exec_framework.models import FinalActionPlan, LiveStateSnapshot, MarketSnapshot
from exec_framework.runtime_env import BinanceEnvConfig, LIVE_SUBMIT_MANUAL_ACK_TOKEN, LIVE_SUBMIT_UNLOCK_TOKEN
from tests.test_executor_real_submit_gate import StubReadonlyClient


class ProtectiveBridgeCase(unittest.TestCase):
    def _build_state(self) -> LiveStateSnapshot:
        return LiveStateSnapshot(
            state_ts='2026-04-27T11:17:34+00:00',
            consistency_status='OK',
            freeze_reason=None,
            account_equity=1000.0,
            available_margin=1000.0,
            exchange_position_side='long',
            exchange_position_qty=0.01,
            exchange_entry_price=2317.11,
            active_strategy='trend',
            active_side='long',
            strategy_entry_time='2026-04-27T11:17:34+00:00',
            strategy_entry_price=2317.11,
            stop_price=2298.83,
            risk_fraction=0.1,
            tp_price=2330.0,
        )

    def _build_plan(self) -> FinalActionPlan:
        return FinalActionPlan(
            plan_ts='2026-04-27T11:18:00+00:00',
            bar_ts='2026-04-27T11:15:00+00:00',
            action_type='protective_rebuild',
            target_strategy='trend',
            target_side='long',
            reason='protective_rebuild_after_management_stop_update',
            qty_mode='exchange_position',
            qty=None,
            stop_price=2298.83,
            risk_fraction=0.1,
            conflict_context={'tp_price': 2330.0},
            requires_execution=True,
        )

    def _build_market(self) -> MarketSnapshot:
        return MarketSnapshot(
            decision_ts='2026-04-27T11:18:00+00:00',
            bar_ts='2026-04-27T11:15:00+00:00',
            strategy_ts=None,
            execution_attributed_bar=None,
            symbol='ETHUSDT',
            preclose_offset_seconds=27,
            current_price=2317.0,
            source_status='OK',
        )

    def test_protective_rebuild_pending_confirm_does_not_bridge_stale_stop_price(self) -> None:
        config = BinanceEnvConfig(
            api_key='k', api_secret='s', dry_run=False, submit_enabled=True,
            submit_unlock_token=LIVE_SUBMIT_UNLOCK_TOKEN, submit_symbol_allowlist=('ETHUSDT',),
            submit_max_qty=10.0, submit_max_notional=1_000_000.0, discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        state = self._build_state()
        state.exchange_position_side = 'long'
        state.exchange_position_qty = 0.01
        state.active_strategy = 'trend'
        state.active_side = 'long'
        state.exchange_protective_orders = [{'kind': 'hard_stop', 'stop_price': 2293.83}]

        plan = self._build_plan()
        confirmation = PostTradeConfirmation(
            confirmation_status='POSITION_CONFIRMED',
            confirmation_category='position_confirmed',
            order_status='FILLED',
            exchange_order_ids=['oid-protect-rebuild'],
            executed_qty=0.0,
            avg_fill_price=None,
            fees=0.0,
            fee_assets=[],
            fill_count=0,
            post_position_side='long',
            post_position_qty=0.01,
            post_entry_price=2317.11,
            reconcile_status='PENDING_CONFIRMATION',
            should_freeze=False,
            freeze_reason=None,
            notes=[],
            trade_summary={
                'protective_order_requested': True,
                'protective_pending_confirm': True,
                'protective_orders': [],
                'protective_validation': {
                    'ok': True,
                    'freeze_reason': None,
                    'exchange_visibility': {'exchange_visible': False, 'confirmed_via_exchange_visibility': False},
                },
                'protective_recover': {
                    'result': 'NEEDS_SUBMIT_CONFIRM',
                    'state_updates': {},
                },
            },
        )

        result = executor._build_execution_result_from_confirmation(
            market=self._build_market(),
            plan=plan,
            confirmation=confirmation,
            state=state,
            status='OK',
            execution_phase='protection_pending_confirm',
            error_code=None,
            error_message=None,
        )

        self.assertEqual(result.state_updates['protective_order_status'], 'PENDING_CONFIRM')
        self.assertEqual(result.state_updates['exchange_protective_orders'], [])
        self.assertEqual(result.state_updates['strategy_protection_intent']['stop_price'], 2298.83)

    def test_protective_rebuild_pending_confirm_does_not_bridge_stale_recover_update_orders(self) -> None:
        config = BinanceEnvConfig(
            api_key='k', api_secret='s', dry_run=False, submit_enabled=True,
            submit_unlock_token=LIVE_SUBMIT_UNLOCK_TOKEN, submit_symbol_allowlist=('ETHUSDT',),
            submit_max_qty=10.0, submit_max_notional=1_000_000.0, discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        state = self._build_state()
        state.exchange_protective_orders = []
        plan = self._build_plan()
        confirmation = PostTradeConfirmation(
            confirmation_status='POSITION_CONFIRMED',
            confirmation_category='position_confirmed',
            order_status='FILLED',
            exchange_order_ids=['oid-protect-rebuild'],
            executed_qty=0.0,
            avg_fill_price=None,
            fees=0.0,
            fee_assets=[],
            fill_count=0,
            post_position_side='long',
            post_position_qty=0.01,
            post_entry_price=2317.11,
            reconcile_status='PENDING_CONFIRMATION',
            should_freeze=False,
            freeze_reason=None,
            notes=[],
            trade_summary={
                'protective_order_requested': True,
                'protective_pending_confirm': True,
                'protective_orders': [],
                'protective_validation': {
                    'ok': True,
                    'freeze_reason': None,
                    'exchange_visibility': {'exchange_visible': False, 'confirmed_via_exchange_visibility': False},
                },
                'protective_recover': {
                    'result': 'NEEDS_SUBMIT_CONFIRM',
                    'state_updates': {
                        'exchange_protective_orders': [{'kind': 'hard_stop', 'stop_price': 2293.83}],
                    },
                },
            },
        )

        result = executor._build_execution_result_from_confirmation(
            market=self._build_market(),
            plan=plan,
            confirmation=confirmation,
            state=state,
            status='OK',
            execution_phase='protection_pending_confirm',
            error_code=None,
            error_message=None,
        )

        self.assertEqual(result.state_updates['protective_order_status'], 'PENDING_CONFIRM')
        self.assertEqual(result.state_updates['exchange_protective_orders'], [])
        self.assertEqual(result.state_updates['strategy_protection_intent']['stop_price'], 2298.83)

    def test_dangerous_missing_protection_marks_force_close_recover_policy(self) -> None:
        config = BinanceEnvConfig(
            api_key='k', api_secret='s', dry_run=False, submit_enabled=True,
            submit_unlock_token=LIVE_SUBMIT_UNLOCK_TOKEN, submit_symbol_allowlist=('ETHUSDT',),
            submit_max_qty=10.0, submit_max_notional=1_000_000.0, discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        state = self._build_state()
        recover = {
            'result': 'NO_PROTECTIVE_ON_EXCHANGE',
            'remaining_risk': 'position_open_without_protection',
            'position_fact': {'side': 'long', 'qty': 0.01, 'entry_price': 2317.11},
            'state_updates': {},
            'attempts': [],
        }

        updates = executor._build_protective_recover_state_updates(
            state=state,
            market=self._build_market(),
            plan=self._build_plan(),
            recover=recover,
            result='BLOCKED',
            reason='protective_order_missing',
            allowed=False,
        )

        recover_check = updates['recover_check']
        self.assertEqual(recover_check['recover_policy'], 'keep_frozen')
        self.assertEqual(recover_check['recover_policy_display'], 'force_close')
        self.assertEqual(recover_check['recover_stage'], 'force_close_without_protection')
        self.assertEqual(recover_check['risk_action'], 'FORCE_CLOSE')
        self.assertEqual(recover_check['stop_condition'], 'position_open_without_protection')


if __name__ == '__main__':
    unittest.main()
