from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.engine import LiveEngine
from exec_framework.models import FinalActionPlan, LiveStateSnapshot, MarketSnapshot
from exec_framework.mock_modules import InMemoryStateStore


class _RecordingStrategyModule:
    def __init__(self, plan: FinalActionPlan):
        self.plan_to_return = plan
        self.seen_states: list[LiveStateSnapshot] = []

    def plan(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan:
        self.seen_states.append(state)
        return self.plan_to_return


class _RaisingExecutorModule:
    def __init__(self, error: Exception):
        self.error = error
        self.calls = 0

    def execute(self, plan: FinalActionPlan, market: MarketSnapshot, state: LiveStateSnapshot):
        self.calls += 1
        raise self.error


class _StubReconcileModule:
    def __init__(self, reconciled_state: LiveStateSnapshot):
        self.reconciled_state = reconciled_state
        self.calls = 0

    def reconcile(self, market: MarketSnapshot, state: LiveStateSnapshot) -> LiveStateSnapshot:
        self.calls += 1
        return self.reconciled_state


class EngineRunOnceCase(unittest.TestCase):
    def make_market(self) -> MarketSnapshot:
        return MarketSnapshot(
            decision_ts='2026-04-09T14:00:33+00:00',
            bar_ts='2026-04-09T14:00:00+00:00',
            strategy_ts='2026-04-09T14:00:00+00:00',
            execution_attributed_bar='2026-04-09T14:00:00+00:00',
            symbol='BTCUSDT',
            preclose_offset_seconds=27,
            current_price=82000.0,
            source_status='OK',
        )

    def make_state(self, **overrides) -> LiveStateSnapshot:
        payload = dict(
            state_ts='2026-04-09T13:45:33+00:00',
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
        payload.update(overrides)
        return LiveStateSnapshot(**payload)

    def make_plan(self) -> FinalActionPlan:
        return FinalActionPlan(
            plan_ts='2026-04-09T14:00:33+00:00',
            bar_ts='2026-04-09T14:00:00+00:00',
            action_type='open',
            target_strategy='trend',
            target_side='long',
            reason='test',
            qty_mode='fixed',
            qty=0.01,
            requires_execution=True,
        )

    def test_run_once_persists_reconciled_state_before_executor_exception(self) -> None:
        initial_state = self.make_state(state_ts='2026-04-09T13:45:33+00:00', consistency_status='UNKNOWN')
        reconciled_state = self.make_state(
            state_ts='2026-04-09T14:00:33+00:00',
            consistency_status='MISMATCH',
            freeze_reason='reconcile_mismatch',
            runtime_mode='FROZEN',
            freeze_status='ACTIVE',
            can_open_new_position=False,
            can_modify_position=False,
        )
        store = InMemoryStateStore(initial_state)
        strategy = _RecordingStrategyModule(self.make_plan())
        executor = _RaisingExecutorModule(RuntimeError('submit failed'))
        reconcile = _StubReconcileModule(reconciled_state)
        engine = LiveEngine(
            state_store=store,
            strategy_module=strategy,
            executor_module=executor,
            pre_run_reconcile_module=reconcile,
        )

        with self.assertRaisesRegex(RuntimeError, 'submit failed'):
            engine.run_once(self.make_market())

        self.assertEqual(reconcile.calls, 1)
        self.assertEqual(executor.calls, 1)
        self.assertEqual(strategy.seen_states[0], reconciled_state)
        self.assertEqual(store.load_state(), reconciled_state)
        self.assertIsNone(store.last_result)


if __name__ == '__main__':
    unittest.main()
