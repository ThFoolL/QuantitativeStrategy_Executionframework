from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_submit import BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN, BinanceSubmitError
from exec_framework.executor_real import BinanceOrderRequest, BinanceRealExecutor
from exec_framework.models import FinalActionPlan, LiveStateSnapshot, MarketSnapshot
from exec_framework.runtime_env import BinanceEnvConfig, LIVE_SUBMIT_MANUAL_ACK_TOKEN


class StubRules:
    qty_step = 0.001
    min_qty = 0.001
    min_notional = 5.0


class StubReadonlyClient:
    def get_exchange_info(self, symbol):
        return StubRules()


class ExecutorSubmitGateCase(unittest.TestCase):
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

    def make_state(self) -> LiveStateSnapshot:
        return LiveStateSnapshot(
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
            base_quantity=0.01,
        )

    def make_plan(self) -> FinalActionPlan:
        return FinalActionPlan(
            plan_ts='2026-03-25T15:00:33+00:00',
            bar_ts='2026-03-25T15:00:00+00:00',
            action_type='open',
            target_strategy='trend',
            target_side='long',
            reason='test_open',
            qty_mode='fixed',
            qty=0.01,
            requires_execution=True,
        )

    def test_submit_disabled_stays_planned_dry_run(self) -> None:
        config = BinanceEnvConfig(api_key='k', api_secret='s', dry_run=True, submit_enabled=False)
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        result = executor.execute(self.make_plan(), self.make_market(), self.make_state())

        self.assertEqual(result.status, 'DRY_RUN')
        self.assertEqual(result.execution_phase, 'planned')
        self.assertEqual(result.confirmation_status, 'UNCONFIRMED')
        self.assertEqual(result.confirmed_order_status, 'NOT_SUBMITTED')
        self.assertEqual(result.reconcile_status, 'DRY_RUN')
        self.assertFalse(result.should_freeze)
        self.assertEqual(result.executed_qty, 0.0)
        self.assertIsNone(result.avg_fill_price)
        self.assertEqual(result.state_updates['pending_execution_phase'], 'planned')
        self.assertIn('submit_gate', result.trade_summary)
        self.assertFalse(result.trade_summary['submit_gate']['submit_allowed'])
        self.assertFalse(result.trade_summary['confirm_context']['confirm_attempted'])
        self.assertIn('request_payloads', result.trade_summary['request_context'])

    def test_submit_payload_serializer_keeps_required_fields(self) -> None:
        payload = BinanceRealExecutor._serialize_submit_payload(
            BinanceOrderRequest(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.015,
                reduce_only=False,
                position_side=None,
                client_order_id='cid-1',
                metadata={'phase': 'open'},
            )
        )
        self.assertEqual(payload['symbol'], 'BTCUSDT')
        self.assertEqual(payload['side'], 'BUY')
        self.assertEqual(payload['type'], 'MARKET')
        self.assertEqual(payload['newClientOrderId'], 'cid-1')
        self.assertEqual(payload['quantity'], 0.015)
        self.assertNotIn('reduceOnly', payload)

    def test_submit_orders_raises_when_gate_open_but_http_not_implemented(self) -> None:
        config = BinanceEnvConfig(
            api_key='k',
            api_secret='s',
            symbol='ETHUSDT',
            dry_run=False,
            submit_enabled=True,
            submit_unlock_token=BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN,
            submit_symbol_allowlist=('ETHUSDT',),
            submit_max_qty=0.02,
            submit_max_notional=50.0,
            discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        with self.assertRaises(NotImplementedError) as ctx:
            executor._submit_orders(
                [
                    BinanceOrderRequest(
                        symbol='BTCUSDT',
                        side='BUY',
                        order_type='MARKET',
                        quantity=0.01,
                        reduce_only=False,
                        position_side=None,
                        client_order_id='cid-2',
                        metadata={},
                    )
                ]
            )
        self.assertIn('intentionally unreachable', str(ctx.exception))

    def test_submit_exception_metadata_shape_is_stable_for_upstream(self) -> None:
        config = BinanceEnvConfig(
            api_key='k',
            api_secret='s',
            dry_run=False,
            submit_enabled=True,
            submit_unlock_token=BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN,
            submit_symbol_allowlist=('BTCUSDT',),
            submit_max_qty=0.02,
            submit_max_notional=50.0,
            discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        request = BinanceOrderRequest(
            symbol='BTCUSDT',
            side='BUY',
            order_type='MARKET',
            quantity=0.01,
            reduce_only=False,
            position_side=None,
            client_order_id='cid-meta',
            metadata={},
        )
        detail = {'payload': {'code': -1007, 'msg': 'execution status unknown'}}

        def _raise_submit(_signed_request):
            raise BinanceSubmitError('execution status unknown', category='network_timeout', detail=detail)

        executor.submit_client.submit_order = _raise_submit  # type: ignore[method-assign]

        with self.assertRaises(NotImplementedError) as ctx:
            executor._submit_orders([request])
        text = str(ctx.exception)
        self.assertIn('policy_action=readonly_recheck', text)
        self.assertIn('policy_alert=on_exhausted_retry_or_unresolved', text)

        metadata = executor._build_submit_exception_metadata(
            category='network_timeout',
            submit_gate={'submit_allowed': False},
            exception_policy={'action': 'readonly_recheck', 'source_key': '-1007'},
            exception_policy_view={
                'policy': 'readonly_recheck',
                'action': 'readonly_recheck',
                'reason': 'execution status unknown',
                'next_action': 'order / userTrades / positionRisk / openOrders',
                'should_alert': True,
            },
            exception_helper_plan={'helper_name': 'exception_helper_readonly_recheck', 'dry_run_only': True},
        )
        self.assertEqual(metadata['submit_exception_category'], 'network_timeout')
        self.assertEqual(metadata['exception_policy_view']['policy'], 'readonly_recheck')
        self.assertTrue(metadata['exception_policy_view']['should_alert'])
        self.assertEqual(metadata['exception_helper_plan']['helper_name'], 'exception_helper_readonly_recheck')

    def test_submit_client_still_blocked_without_unlock_token(self) -> None:
        config = BinanceEnvConfig(api_key='k', api_secret='s', dry_run=False, submit_enabled=True, submit_unlock_token=None)
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        gate = executor._build_submit_gate_context(blocked_reason='test')
        self.assertFalse(gate['submit_unlock_token_present'])
        self.assertFalse(gate['submit_unlock_token_valid'])
        self.assertFalse(gate['submit_allowed'])

    def test_submit_client_still_blocked_even_with_unlock_token_because_code_guard_closed(self) -> None:
        config = BinanceEnvConfig(
            api_key='k',
            api_secret='s',
            dry_run=False,
            submit_enabled=True,
            submit_unlock_token=BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        gate = executor._build_submit_gate_context(blocked_reason='test')
        self.assertTrue(gate['submit_unlock_token_present'])
        self.assertTrue(gate['submit_unlock_token_valid'])
        self.assertFalse(gate['allow_live_submit_call'])
        self.assertFalse(gate['submit_allowed'])

    def test_guardrail_blocks_market_symbol_mismatch_with_runtime_config(self) -> None:
        config = BinanceEnvConfig(
            api_key='k',
            api_secret='s',
            symbol='ETHUSDT',
            dry_run=False,
            submit_enabled=True,
            submit_unlock_token=BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN,
            submit_max_qty=0.02,
            submit_max_notional=50.0,
            discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        market = self.make_market()
        market.symbol = 'BTCUSDT'
        requests = [
            BinanceOrderRequest(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.01,
                reduce_only=False,
                position_side=None,
                client_order_id='cid-mismatch',
                metadata={},
            )
        ]
        gate = executor._evaluate_submit_gate(market=market, state=self.make_state(), order_requests=requests)
        self.assertFalse(gate['submit_allowed'])
        self.assertIn('config_symbol_mismatch:BTCUSDT!=ETHUSDT', gate['guardrail_blockers'])
        self.assertIn('symbol_not_allowed:BTCUSDT', gate['guardrail_blockers'])

    def test_guardrail_blocks_symbol_not_in_allowlist(self) -> None:
        config = BinanceEnvConfig(
            api_key='k',
            api_secret='s',
            symbol='ETHUSDT',
            dry_run=False,
            submit_enabled=True,
            submit_unlock_token=BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN,
            submit_symbol_allowlist=('ETHUSDT',),
            submit_max_qty=0.02,
            submit_max_notional=50.0,
            discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        requests = [
            BinanceOrderRequest(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.01,
                reduce_only=False,
                position_side=None,
                client_order_id='cid-3',
                metadata={},
            )
        ]
        gate = executor._evaluate_submit_gate(market=self.make_market(), state=self.make_state(), order_requests=requests)
        self.assertFalse(gate['submit_allowed'])
        self.assertIn('symbol_not_allowed:BTCUSDT', gate['guardrail_blockers'])

    def test_guardrail_blocks_when_pending_execution_exists(self) -> None:
        config = BinanceEnvConfig(
            api_key='k',
            api_secret='s',
            dry_run=False,
            submit_enabled=True,
            submit_unlock_token=BINANCE_LIVE_SUBMIT_UNLOCK_TOKEN,
            submit_symbol_allowlist=('BTCUSDT',),
            submit_max_qty=0.02,
            submit_max_notional=50.0,
            discord_audit_enabled=True,
            submit_manual_ack_token=LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        )
        executor = BinanceRealExecutor(config=config, readonly_client=StubReadonlyClient())
        state = self.make_state()
        state.pending_execution_phase = 'submitted'
        requests = [
            BinanceOrderRequest(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.01,
                reduce_only=False,
                position_side=None,
                client_order_id='cid-4',
                metadata={},
            )
        ]
        gate = executor._evaluate_submit_gate(market=self.make_market(), state=state, order_requests=requests)
        self.assertFalse(gate['submit_allowed'])
        self.assertIn('pending_execution_phase:submitted', gate['guardrail_blockers'])


if __name__ == '__main__':
    unittest.main()
