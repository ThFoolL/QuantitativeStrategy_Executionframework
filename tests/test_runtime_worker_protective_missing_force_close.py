from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.market_data import MarketFrameBundle
from exec_framework.models import ExecutionResult, LiveStateSnapshot, MarketSnapshot
from exec_framework.runtime_worker import RuntimeWorker


class RuntimeWorkerProtectiveMissingForceCloseCase(unittest.TestCase):
    def test_force_close_uses_decision_ts_when_market_bundle_has_no_bar_ts(self) -> None:
        state = LiveStateSnapshot(
            state_ts='2026-04-27T13:45:00+00:00',
            consistency_status='OK',
            freeze_reason='protective_order_missing',
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side='short',
            exchange_position_qty=0.021,
            exchange_entry_price=2314.31,
            active_strategy='manual_protective_missing_probe',
            active_side='short',
            strategy_entry_time='2026-04-27T13:44:00+00:00',
            strategy_entry_price=2314.31,
            stop_price=None,
            risk_fraction=0.1,
            runtime_mode='FROZEN',
            freeze_status='ACTIVE',
        )

        class StubStateStore:
            def save_result(self, prev_state, result):
                self.result = result

            def load_state(self):
                return state

        executor = Mock()
        executor.execute.return_value = ExecutionResult(
            result_ts='2026-04-27T13:46:00+00:00',
            bar_ts='2026-04-27T13:46:00+00:00',
            status='FILLED',
            action_type='close',
            executed_side=None,
            exchange_order_ids=['oid-close'],
            post_position_side=None,
            post_position_qty=0.0,
            reconcile_status='OK',
            confirmation_status='POSITION_CONFIRMED',
            confirmed_order_status='FILLED',
            state_updates={},
        )

        worker = RuntimeWorker(
            config=SimpleNamespace(symbol='ETHUSDT'),
            market_provider=SimpleNamespace(
                load=lambda symbol, decision_time: MarketFrameBundle(
                    symbol='ETHUSDT',
                    decision_ts='2026-04-27T13:46:00+00:00',
                    current_price=2314.88,
                    fast_5m={'close': 2314.88, 'low': 2310.0, 'high': 2316.0},
                    signal_15m={'close': 2314.88, 'low': 2310.0, 'high': 2316.0},
                    signal_15m_ts='2026-04-27T13:45:00+00:00',
                    trend_1h={'close': 2314.88},
                    trend_1h_ts='2026-04-27T13:00:00+00:00',
                    signal_15m_history=[],
                    metadata={},
                )
            ),
            engine=SimpleNamespace(executor_module=executor),
            state_store=StubStateStore(),
            status_store=SimpleNamespace(path=Path('runtime/runtime_status.json'), write=lambda payload: None),
            event_log=SimpleNamespace(path=Path('runtime/event_log.jsonl'), append=lambda *args, **kwargs: None),
            scheduler=SimpleNamespace(),
        )

        out = worker._maybe_force_reduce_only_close_without_protection(state=state, run_id='run-2a-market-bundle')

        self.assertIsNotNone(out)
        executor.execute.assert_called_once()
        plan, market, _ = executor.execute.call_args[0]
        self.assertEqual(plan.bar_ts, market.bar_ts)
        self.assertEqual(market.strategy_ts, market.bar_ts)
        self.assertTrue(str(market.bar_ts))
        self.assertEqual(plan.close_reason, 'emergency_close_after_protective_missing')

    def test_force_close_falls_back_to_exchange_position_when_local_state_is_stale(self) -> None:
        state = LiveStateSnapshot(
            state_ts='2026-04-27T13:45:00+00:00',
            consistency_status='OK',
            freeze_reason='protective_order_missing',
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side=None,
            exchange_position_qty=0.0,
            exchange_entry_price=None,
            active_strategy='manual_protective_missing_probe',
            active_side='short',
            strategy_entry_time='2026-04-27T13:44:00+00:00',
            strategy_entry_price=2314.31,
            stop_price=None,
            risk_fraction=0.1,
            runtime_mode='FROZEN',
            freeze_status='ACTIVE',
        )

        saved_result = {}
        reloaded_state = LiveStateSnapshot(
            **{**state.__dict__, 'runtime_mode': 'ACTIVE', 'last_recover_result': 'ALLOWED', 'recover_check': {'result': 'ALLOWED'}}
        )

        class StubStateStore:
            def save_result(self, prev_state, result):
                saved_result['result'] = result

            def load_state(self):
                return reloaded_state

        executor = Mock()
        executor.execute.return_value = ExecutionResult(
            result_ts='2026-04-27T13:46:00+00:00',
            bar_ts='2026-04-27T13:45:00+00:00',
            status='FILLED',
            action_type='close',
            executed_side=None,
            exchange_order_ids=['oid-close'],
            post_position_side=None,
            post_position_qty=0.0,
            reconcile_status='OK',
            confirmation_status='POSITION_CONFIRMED',
            confirmed_order_status='FILLED',
            state_updates={},
        )

        worker = RuntimeWorker(
            config=SimpleNamespace(symbol='ETHUSDT'),
            market_provider=SimpleNamespace(
                load=lambda symbol, decision_time: MarketSnapshot(
                    decision_ts='2026-04-27T13:46:00+00:00',
                    bar_ts='2026-04-27T13:45:00+00:00',
                    strategy_ts=None,
                    execution_attributed_bar=None,
                    symbol='ETHUSDT',
                    preclose_offset_seconds=27,
                    current_price=2314.88,
                    source_status='OK',
                )
            ),
            engine=SimpleNamespace(
                executor_module=executor,
                pre_run_reconcile_module=SimpleNamespace(
                    readonly_client=SimpleNamespace(
                        get_position_snapshot=lambda symbol: SimpleNamespace(side='short', qty=0.021, entry_price=2314.31)
                    )
                ),
            ),
            state_store=StubStateStore(),
            status_store=SimpleNamespace(path=Path('runtime/runtime_status.json'), write=lambda payload: None),
            event_log=SimpleNamespace(path=Path('runtime/event_log.jsonl'), append=lambda *args, **kwargs: None),
            scheduler=SimpleNamespace(),
        )

        out = worker._maybe_force_reduce_only_close_without_protection(state=state, run_id='run-2a')

        self.assertIsNotNone(out)
        executor.execute.assert_called_once()
        plan, market, effective_state = executor.execute.call_args[0]
        self.assertEqual(plan.close_reason, 'emergency_close_after_protective_missing')
        self.assertEqual(plan.target_side, 'short')
        self.assertEqual(effective_state.exchange_position_side, 'short')
        self.assertEqual(effective_state.exchange_position_qty, 0.021)
        self.assertEqual(saved_result['result'].action_type, 'close')


if __name__ == '__main__':
    unittest.main()
