from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unittest.mock import patch

from exec_framework.async_operation import attach_execution_confirm_async_operation
from exec_framework.models import ExecutionResult, LiveStateSnapshot
from exec_framework.runtime_worker import RuntimeWorker, ReadonlyRecheckDecision
from exec_framework.executor_real import BinanceRealExecutor


class RuntimeWorkerPublishableSelectionCase(unittest.TestCase):
    def test_select_publishable_output_uses_cached_open_for_same_bar_protective_rebuild(self) -> None:
        cached_open = {
            'status': 'FILLED',
            'action_type': 'open',
            'confirmation_status': 'POSITION_CONFIRMED',
            'confirmed_order_status': 'FILLED',
            'execution_phase': 'position_confirmed_pending_trades',
            'post_position_side': 'long',
            'post_position_qty': 0.802,
            'post_entry_price': 2290.76,
            'reconcile_status': 'OK',
            'bar_ts': '2026-05-12T08:45:00+00:00',
        }

        class StubStore:
            def __init__(self):
                self._state = SimpleNamespace(last_publishable_result=cached_open)

            def load_state(self):
                return self._state

            def save_state(self, new_state):
                self._state = new_state

        worker_like = SimpleNamespace(
            state_store=StubStore(),
            _is_publishable_execution_result=lambda result: RuntimeWorker._is_publishable_execution_result(result),
            _is_publishable_open_candidate=lambda result: RuntimeWorker._is_publishable_open_candidate(result),
            _build_async_protective_close_publishable_candidate=lambda **kwargs: RuntimeWorker._build_async_protective_close_publishable_candidate(**kwargs),
        )

        output = {
            'state': {
                'runtime_mode': 'ACTIVE',
                'freeze_status': 'NONE',
                'freeze_reason': None,
                'exchange_position_side': 'long',
                'exchange_position_qty': 0.802,
                'last_publishable_result': cached_open,
            },
            'plan': {
                'action_type': 'protective_rebuild',
                'reason': 'protective_rebuild_after_entry_confirmation',
            },
            'result': {
                'status': 'CONFIRMED',
                'action_type': 'protective_rebuild',
                'confirmation_status': 'CONFIRMED',
                'confirmed_order_status': 'FILLED',
                'execution_phase': 'confirmed',
                'reconcile_status': 'OK',
                'bar_ts': '2026-05-12T08:45:00+00:00',
                'post_position_side': 'long',
                'post_position_qty': 0.802,
                'trade_summary': {
                    'protective_orders_count': 2,
                },
            },
        }

        publishable = RuntimeWorker._select_publishable_output(worker_like, output)

        self.assertEqual(publishable['result']['action_type'], 'open')
        self.assertEqual(publishable['result']['bar_ts'], '2026-05-12T08:45:00+00:00')
        self.assertEqual(publishable['result']['post_position_side'], 'long')


class RuntimeWorkerProtectiveCleanupCase(unittest.TestCase):
    def test_cleanup_lingering_protective_orders_falls_back_to_open_orders_probe(self) -> None:
        state = LiveStateSnapshot(
            state_ts='2026-05-12T13:41:00+00:00',
            consistency_status='MISMATCH',
            freeze_reason='local_exchange_position_presence_mismatch',
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side=None,
            exchange_position_qty=0.0,
            exchange_entry_price=None,
            active_side=None,
            strategy_entry_time='2026-05-12T11:45:00+00:00',
            strategy_entry_price=2285.55,
            stop_price=2276.31,
            risk_fraction=0.1,
            exchange_protective_orders=[
                {
                    'order_id': '1000001627480047',
                    'client_order_id': '202605121145000000-protect-har',
                    'kind': 'hard_stop',
                },
                {
                    'order_id': '1000001627480062',
                    'client_order_id': '202605121145000000-protect-tak',
                    'kind': 'take_profit',
                },
            ],
            active_strategy='trend',
            runtime_mode='ACTIVE',
            freeze_status='ACTIVE',
            pending_execution_phase='frozen',
            protective_order_status='PENDING_CONFIRM',
        )

        cancel_requests_seen = []
        readonly_calls = []

        class StubExecutorModule(BinanceRealExecutor):
            def __init__(self):
                pass

            def _cancel_existing_protective_orders(self, cancel_requests):
                cancel_requests_seen.extend(cancel_requests)
                return {
                    'ok': False,
                    'reason': 'cancel_rejected',
                    'receipts': [],
                }

        class StubReadonlyClient:
            def get_open_orders(self, symbol):
                readonly_calls.append(symbol)
                return []

        worker = RuntimeWorker(
            config=SimpleNamespace(symbol='ETHUSDT'),
            market_provider=SimpleNamespace(),
            engine=SimpleNamespace(
                executor_module=StubExecutorModule(),
                pre_run_reconcile_module=SimpleNamespace(readonly_client=StubReadonlyClient()),
            ),
            state_store=SimpleNamespace(save_state=lambda state: None, load_last_result=lambda: None),
            status_store=SimpleNamespace(path=Path('runtime/runtime_status.json'), write=lambda payload: None),
            event_log=SimpleNamespace(path=Path('runtime/event_log.jsonl'), append=lambda *args, **kwargs: None),
            scheduler=SimpleNamespace(config=SimpleNamespace(max_backoff_seconds=60)),
        )

        result = worker._maybe_cleanup_lingering_protective_orders(state=state, run_id='run-protective-cleanup')

        self.assertIsNotNone(result)
        self.assertTrue(result['allowed'])
        self.assertEqual(result['result'], 'RECOVERED')
        self.assertEqual(readonly_calls, ['ETHUSDT'])
        self.assertEqual(len(cancel_requests_seen), 2)


class RuntimeWorkerForceCloseProjectionCase(unittest.TestCase):
    def test_position_confirmed_open_candidate_is_publishable(self) -> None:
        result_payload = {
            'status': 'POSITION_CONFIRMED',
            'action_type': 'open',
            'confirmation_status': 'POSITION_CONFIRMED',
            'confirmed_order_status': 'FILLED',
            'execution_phase': 'position_confirmed_pending_trades',
            'reconcile_status': 'OK',
            'executed_qty': 0,
            'post_position_side': 'long',
            'post_position_qty': 0.021,
            'post_entry_price': 2281.83,
            'avg_fill_price': None,
        }

        self.assertTrue(RuntimeWorker._is_publishable_open_candidate(result_payload))

    def test_entry_confirmed_pending_protective_open_candidate_is_publishable(self) -> None:
        result_payload = {
            'status': 'POSITION_CONFIRMED',
            'action_type': 'open',
            'confirmation_status': 'POSITION_CONFIRMED',
            'confirmed_order_status': 'FILLED',
            'execution_phase': 'entry_confirmed_pending_protective',
            'reconcile_status': 'OK',
            'executed_qty': 0,
            'post_position_side': 'long',
            'post_position_qty': 0.824,
            'post_entry_price': 2285.55,
            'avg_fill_price': None,
        }

        self.assertTrue(RuntimeWorker._is_publishable_open_candidate(result_payload))

    def test_run_once_refreshes_output_result_after_recover_close_saved_last_result(self) -> None:
        state = LiveStateSnapshot(
            state_ts='2026-04-28T00:20:00+00:00',
            consistency_status='OK',
            freeze_reason='protective_order_missing',
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side='short',
            exchange_position_qty=0.021,
            exchange_entry_price=2289.58,
            active_strategy='manual_protective_missing_probe',
            active_side='short',
            strategy_entry_time='2026-04-28T00:19:00+00:00',
            strategy_entry_price=2289.58,
            stop_price=None,
            risk_fraction=0.1,
            runtime_mode='FROZEN',
            freeze_status='ACTIVE',
            pending_execution_phase='confirmed',
            protective_order_status='MISSING',
        )
        saved_result = ExecutionResult(
            result_ts='2026-04-28T00:20:05+00:00',
            bar_ts='2026-04-28T00:20:00+00:00',
            status='FILLED',
            action_type='close',
            executed_side='short',
            exchange_order_ids=['oid-close'],
            post_position_side=None,
            post_position_qty=0.0,
            reconcile_status='OK',
            confirmation_status='POSITION_CONFIRMED',
            confirmed_order_status='FILLED',
            state_updates={'recover_check': {'risk_action': 'FORCE_CLOSE'}},
        )

        class StubStore:
            def __init__(self, initial_state, last_result):
                self.state = initial_state
                self.last_result = last_result

            def load_state(self):
                return self.state

            def save_state(self, new_state):
                self.state = new_state

            def load_last_result(self):
                return self.last_result

        store = StubStore(state, saved_result)
        worker = RuntimeWorker(
            config=SimpleNamespace(
                symbol='ETHUSDT',
                state_path='runtime/state.json',
                dry_run=False,
                submit_enabled=True,
                submit_http_post_enabled=True,
                discord_real_send_enabled=False,
                discord_message_tool_enabled=False,
                discord_rehearsal_real_send_enabled=False,
                discord_execution_confirmation_real_send_enabled=False,
                discord_transport='production',
                discord_send_ledger_path='runtime/discord_send_ledger.json',
                discord_send_receipt_log_path='runtime/discord_send_receipts.jsonl',
                submit_symbol_allowlist=('ETHUSDT',),
                submit_max_qty=10000.0,
                submit_max_notional=10000000.0,
                submit_manual_ack_token='I_ACK_SMALL_CAPITAL_REAL_SUBMIT_CHECKLIST',
                submit_unlock_token='ENABLE_BINANCE_FUTURES_LIVE_SUBMIT',
                discord_audit_enabled=True,
                strategy_adapter='la_free_v1',
            ),
            market_provider=SimpleNamespace(),
            engine=SimpleNamespace(run_once=lambda market: {
                'state': {'consistency_status': 'OK', 'runtime_mode': 'FROZEN', 'freeze_status': 'ACTIVE', 'freeze_reason': 'protective_order_missing'},
                'plan': {'action_type': 'hold', 'target_strategy': None, 'target_side': None, 'reason': 'no_rev_candidate', 'requires_execution': False},
                'result': {'status': 'FROZEN', 'action_type': 'hold', 'confirmation_status': 'FROZEN', 'execution_phase': 'frozen', 'trade_summary': {}},
            }, executor_module=None, pre_run_reconcile_module=SimpleNamespace(readonly_client=None)),
            state_store=store,
            status_store=SimpleNamespace(path=Path('runtime/runtime_status.json'), write=lambda payload: None),
            event_log=SimpleNamespace(path=Path('runtime/event_log.jsonl'), append=lambda *args, **kwargs: None),
            scheduler=SimpleNamespace(config=SimpleNamespace(max_backoff_seconds=60)),
        )
        worker._maybe_advance_execution_orchestration = lambda market, output: output
        worker._persist_non_execution_strategy_state = lambda output: output
        worker._attach_execution_retry_backoff = lambda output: output
        worker._maybe_attempt_recover = lambda updated_state, run_id: {'allowed': False, 'result': 'BLOCKED', 'reason': 'forced_reduce_only_close_after_protective_missing', 'recover_check': {'risk_action': 'FORCE_CLOSE'}}
        worker._maybe_run_readonly_recheck = lambda run_id, market, output: None
        worker._write_audit_artifacts = lambda **kwargs: {}
        worker._select_publishable_output = lambda output: output
        worker._write_dispatch_preview_audit = lambda **kwargs: None
        worker._write_runtime_status = lambda **kwargs: None
        worker._maybe_run_discord_sender = lambda output: None
        worker._record_run_summary = lambda **kwargs: None
        worker._mark_strategy_ts_processed = lambda strategy_ts: None
        worker._load_market = lambda decision_time=None: SimpleNamespace(
            symbol='ETHUSDT',
            decision_ts='2026-04-28T00:20:05+00:00',
            bar_ts='2026-04-28T00:20:00+00:00',
            strategy_ts='2026-04-28T00:20:00+00:00',
            execution_attributed_bar='2026-04-28T00:20:00+00:00',
            source_status='OK',
        )

        with patch('exec_framework.runtime_worker.validate_runtime_config', return_value=SimpleNamespace(as_dict=lambda: {}, ok=True)):
            with patch('exec_framework.runtime_worker.build_market_snapshot', return_value=SimpleNamespace(
                symbol='ETHUSDT',
                decision_ts='2026-04-28T00:20:05+00:00',
                bar_ts='2026-04-28T00:20:00+00:00',
                strategy_ts='2026-04-28T00:20:00+00:00',
                execution_attributed_bar='2026-04-28T00:20:00+00:00',
                source_status='OK',
                current_price=2289.74,
            )):
                out = worker.run_once(None)

        self.assertEqual(out['result']['status'], 'FILLED')
        self.assertEqual(out['result']['action_type'], 'close')
        self.assertEqual(out['result']['confirmed_order_status'], 'FILLED')

    def test_execution_confirm_async_does_not_override_force_close_fact(self) -> None:
        state_payload = {
            'state_ts': '2026-04-28T00:20:00+00:00',
            'symbol': 'ETHUSDT',
            'pending_execution_phase': 'planned',
            'recover_check': {
                'checked_at': '2026-04-28T00:20:00+00:00',
                'source': 'protective_order_recover',
                'result': 'RECOVERED',
                'allowed': False,
                'reason': 'protective_order_missing',
                'pending_execution_phase': 'none',
                'consistency_status': 'OK',
                'runtime_mode': 'ACTIVE',
                'recover_ready': False,
                'requires_manual_resume': True,
                'guard_decision': 'protective_recover_first',
                'recover_policy': 'keep_frozen',
                'recover_policy_display': 'force_close',
                'legacy_recover_policy': 'keep_frozen',
                'recover_stage': 'force_close_without_protection',
                'risk_action': 'FORCE_CLOSE',
                'stop_reason': 'protective_order_missing',
                'stop_category': 'frozen',
                'freeze_reason': 'protective_order_missing',
                'action_type': 'close',
                'remaining_risk': 'position_open_without_protection',
                'stop_condition': 'position_open_without_protection',
            },
        }
        result_payload = {
            'result_ts': '2026-04-28T00:20:05+00:00',
            'trade_summary': {
                'pending_execution_phase': 'planned',
                'confirm_context': {
                    'confirm_phase': 'execution_confirm',
                    'stop_reason': 'recover_ready',
                    'stop_condition': 'await_more_exchange_facts',
                },
            },
            'state_updates': {
                'recover_check': dict(state_payload['recover_check']),
            },
        }

        next_state, next_result, operation = attach_execution_confirm_async_operation(
            market_decision_ts='2026-04-28T00:20:05+00:00',
            symbol='ETHUSDT',
            strategy_ts='2026-04-28T00:20:00+00:00',
            state_payload=state_payload,
            result_payload=result_payload,
        )

        self.assertIsNone(operation)
        self.assertEqual(next_state.get('recover_check', {}).get('risk_action'), 'FORCE_CLOSE')
        self.assertEqual(next_result.get('state_updates', {}).get('recover_check', {}).get('risk_action'), 'FORCE_CLOSE')
        self.assertEqual(next_result.get('state_updates', {}).get('recover_check', {}).get('stop_condition'), 'position_open_without_protection')

    def test_apply_readonly_recheck_output_keeps_force_close_fact(self) -> None:
        state = LiveStateSnapshot(
            state_ts='2026-04-28T00:20:00+00:00',
            consistency_status='OK',
            freeze_reason='protective_order_missing',
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side='short',
            exchange_position_qty=0.021,
            exchange_entry_price=2275.0,
            active_strategy='manual_protective_missing_probe',
            active_side='short',
            strategy_entry_time='2026-04-28T00:19:00+00:00',
            strategy_entry_price=2275.0,
            stop_price=None,
            risk_fraction=0.1,
            runtime_mode='FROZEN',
            freeze_status='ACTIVE',
            pending_execution_phase='frozen',
            protective_order_status='MISSING',
        )
        worker = RuntimeWorker(
            config=SimpleNamespace(symbol='ETHUSDT'),
            market_provider=None,
            engine=SimpleNamespace(executor_module=None),
            state_store=SimpleNamespace(load_state=lambda: state, save_state=lambda new_state: None),
            status_store=SimpleNamespace(path=Path('runtime/runtime_status.json'), write=lambda payload: None),
            event_log=SimpleNamespace(path=Path('runtime/event_log.jsonl'), append=lambda *args, **kwargs: None),
            scheduler=SimpleNamespace(),
        )

        output = {
            'state': {
                'state_ts': '2026-04-28T00:20:00+00:00',
                'symbol': 'ETHUSDT',
                'consistency_status': 'OK',
                'runtime_mode': 'FROZEN',
                'freeze_status': 'ACTIVE',
                'freeze_reason': 'protective_order_missing',
                'pending_execution_phase': 'frozen',
                'protective_order_status': 'MISSING',
                'exchange_protective_orders': [],
                'strategy_protection_intent': {},
                'recover_check': {},
                'recover_timeline': [],
            },
            'result': {
                'result_ts': '2026-04-28T00:20:00+00:00',
                'status': 'FROZEN',
                'freeze_reason': 'protective_order_missing',
                'trade_summary': {
                    'protective_validation': {
                        'ok': True,
                        'exchange_visibility': {
                            'exchange_visible': True,
                            'confirmed_via_exchange_visibility': True,
                        },
                        'summary': {},
                    },
                    'protective_recover': {
                        'result': 'NO_PROTECTIVE_ON_EXCHANGE',
                    },
                },
                'state_updates': {
                    'protective_order_status': 'ACTIVE',
                },
            },
        }
        decision = ReadonlyRecheckDecision(
            status='readonly_recheck_freeze',
            action='freeze',
            summary={'confirmed_flat': False},
            state_updates={},
            result_updates={},
            should_freeze=True,
            freeze_reason='protective_order_missing',
            recover_check={
                'checked_at': '2026-04-28T00:20:00+00:00',
                'source': 'readonly_recheck',
                'result': 'BLOCKED',
                'allowed': False,
                'reason': 'protective_order_missing',
                'pending_execution_phase': 'frozen',
                'consistency_status': 'OK',
                'runtime_mode': 'FROZEN',
                'recover_ready': False,
                'requires_manual_resume': True,
                'guard_decision': 'keep_frozen_protection_missing',
                'recover_policy': 'keep_frozen',
                'recover_policy_display': 'force_close',
                'legacy_recover_policy': 'keep_frozen',
                'recover_stage': 'force_close_without_protection',
                'risk_action': 'FORCE_CLOSE',
                'stop_reason': 'protective_order_missing',
                'stop_category': 'frozen',
                'freeze_reason': 'protective_order_missing',
                'stop_condition': 'position_open_without_protection',
            },
        )

        updated = worker._apply_readonly_recheck_output(output, decision)
        recover_check = updated['state']['recover_check']

        self.assertEqual(recover_check['recover_policy'], 'keep_frozen')
        self.assertEqual(recover_check['recover_stage'], 'force_close_without_protection')
        self.assertEqual(recover_check['risk_action'], 'FORCE_CLOSE')
        self.assertEqual(recover_check['stop_condition'], 'position_open_without_protection')

    def test_lingering_protective_cleanup_reaches_flat_reset_when_cancel_failed_but_exchange_has_no_orders(self) -> None:
        state = LiveStateSnapshot(
            state_ts='2026-05-11T20:15:00+00:00',
            consistency_status='MISMATCH',
            freeze_reason='local_exchange_position_presence_mismatch',
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side=None,
            exchange_position_qty=0.0,
            exchange_entry_price=0.0,
            active_strategy='rev',
            active_side='short',
            strategy_entry_time='2026-05-11T19:00:00+00:00',
            strategy_entry_price=2335.06,
            stop_price=2343.31,
            tp_price=2325.19,
            risk_fraction=0.1,
            runtime_mode='FROZEN',
            freeze_status='ACTIVE',
            pending_execution_phase='frozen',
            base_quantity=0.864,
            protective_order_status='ACTIVE',
            exchange_protective_orders=[
                {'order_id': 'oid-stop', 'client_order_id': 'cid-stop', 'kind': 'hard_stop'},
                {'order_id': 'oid-tp', 'client_order_id': 'cid-tp', 'kind': 'take_profit'},
            ],
            recover_check={
                'checked_at': '2026-05-11T19:00:00+00:00',
                'source': 'execution_confirm_async_operation',
                'result': 'READY',
                'allowed': True,
                'reason': 'recover_ready',
                'pending_execution_phase': 'confirmed',
                'consistency_status': 'OK',
                'runtime_mode': 'ACTIVE',
            },
        )

        class StubStore:
            def __init__(self, initial_state):
                self.state = initial_state

            def load_state(self):
                return self.state

            def save_state(self, new_state):
                self.state = new_state

        class StubExecutor:
            def _cancel_existing_protective_orders(self, cancel_requests):
                return {
                    'ok': False,
                    'reason': 'protective_cancel_failed',
                    'cancel_count': len(cancel_requests),
                    'receipts': [
                        {
                            'client_order_id': 'cid-stop',
                            'exchange_order_id': 'oid-stop',
                            'canceled': False,
                            'cancel_status': 'ERROR',
                            'error_message': 'Order does not exist.',
                        }
                    ],
                }

        event_rows = []
        store = StubStore(state)
        with patch('exec_framework.runtime_worker.BinanceRealExecutor', StubExecutor):
            worker = RuntimeWorker(
                config=SimpleNamespace(symbol='ETHUSDT'),
                market_provider=None,
                engine=SimpleNamespace(executor_module=StubExecutor()),
                state_store=store,
                status_store=SimpleNamespace(path=Path('runtime/runtime_status.json'), write=lambda payload: None),
                event_log=SimpleNamespace(path=Path('runtime/event_log.jsonl'), append=lambda *args, **kwargs: event_rows.append((args, kwargs))),
                scheduler=SimpleNamespace(),
            )
            worker.freeze_controller = SimpleNamespace(evaluate_recover=lambda current_state: None)
            worker._fetch_exchange_open_orders = lambda symbol: []

            result = worker._maybe_cleanup_lingering_protective_orders(state=state, run_id='run-1')

        self.assertIsNotNone(result)
        self.assertEqual(result['result'], 'RECOVERED')
        self.assertEqual(result['reason'], 'canceled_lingering_protective_orders_after_flat')
        next_state = store.state
        self.assertEqual(next_state.runtime_mode, 'ACTIVE')
        self.assertEqual(next_state.freeze_status, 'NONE')
        self.assertIsNone(next_state.freeze_reason)
        self.assertIsNone(next_state.pending_execution_phase)
        self.assertEqual(next_state.exchange_protective_orders, [])
        self.assertEqual(next_state.protective_order_status, 'NONE')


if __name__ == '__main__':
    unittest.main()
