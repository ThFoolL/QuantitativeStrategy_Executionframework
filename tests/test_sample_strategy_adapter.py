from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from examples.sample_strategy_adapter import SampleStrategyAdapter
from exec_framework.models import LiveStateSnapshot, MarketSnapshot


class SampleStrategyAdapterCase(unittest.TestCase):
    def make_market(self, *, source_status: str = 'OK') -> MarketSnapshot:
        return MarketSnapshot(
            decision_ts='2026-03-27T09:00:33+00:00',
            bar_ts='2026-03-27T09:00:00+00:00',
            strategy_ts=None,
            execution_attributed_bar=None,
            symbol='BTCUSDT',
            preclose_offset_seconds=27,
            current_price=2100.0,
            source_status=source_status,
        )

    def make_state(self, *, consistency_status: str = 'OK', freeze_reason: str | None = None, active_side: str | None = None, exchange_position_qty: float = 0.0) -> LiveStateSnapshot:
        return LiveStateSnapshot(
            state_ts='2026-03-27T09:00:33+00:00',
            consistency_status=consistency_status,
            freeze_reason=freeze_reason,
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side=active_side,
            exchange_position_qty=exchange_position_qty,
            exchange_entry_price=None,
            active_strategy='none',
            active_side=active_side,
            strategy_entry_time=None,
            strategy_entry_price=None,
            stop_price=None,
            risk_fraction=None,
        )

    def test_hold_when_state_not_ok(self) -> None:
        plan = SampleStrategyAdapter().plan(self.make_market(), self.make_state(consistency_status='MISMATCH'))
        self.assertEqual(plan.action_type, 'hold')
        self.assertFalse(plan.requires_execution)

    def test_hold_when_position_exists(self) -> None:
        plan = SampleStrategyAdapter().plan(self.make_market(), self.make_state(active_side='long', exchange_position_qty=0.01))
        self.assertEqual(plan.action_type, 'hold')
        self.assertEqual(plan.reason, 'position_already_present')

    def test_open_placeholder_when_inputs_ready(self) -> None:
        plan = SampleStrategyAdapter().plan(self.make_market(), self.make_state())
        self.assertEqual(plan.action_type, 'open')
        self.assertEqual(plan.target_strategy, 'sample_placeholder')
        self.assertEqual(plan.target_side, 'long')
        self.assertTrue(plan.requires_execution)


if __name__ == '__main__':
    unittest.main()
