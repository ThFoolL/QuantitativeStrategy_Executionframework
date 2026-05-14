from __future__ import annotations

import unittest

from exec_framework.models import LiveStateSnapshot, MarketSnapshot
from exec_framework.v6c_adapter_baseline import V6CBaselineLiveAdapter


class BaselineShortOpenImpulseParityCase(unittest.TestCase):
    def test_short_does_not_exit_via_open_impulse_early_fail(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        state = LiveStateSnapshot(
            state_ts='2026-02-11T17:00:00+00:00',
            consistency_status='OK',
            freeze_reason=None,
            account_equity=100.0,
            available_margin=100.0,
            exchange_position_side='short',
            exchange_position_qty=1.0,
            exchange_entry_price=1909.9,
            active_strategy='trend',
            active_side='short',
            strategy_entry_time='2026-02-11T17:00:00+00:00',
            strategy_entry_price=1909.9,
            stop_price=1981.41,
            risk_fraction=0.16,
            quality_bucket='HIGH',
            degrade_state='ATTACK',
            add_on_count=0,
            base_quantity=1.0,
            equity_at_entry=100.0,
            risk_amount=16.0,
            risk_per_unit=71.51,
            execution_entry_price=1909.9,
            execution_risk_per_unit=71.51,
            execution_quantity=1.0,
            execution_notional=1909.9,
            execution_risk_amount=16.0,
            execution_high_water_r=0.0,
            high_water_r=0.0,
            p1_armed=False,
            p2_armed=False,
            profit_defense_start_pct=0.32,
            profit_defense_giveback_pct=0.33,
            p1_trigger_r=1.0,
            p2_trigger_r=2.0,
        )
        market = MarketSnapshot(
            decision_ts='2026-02-11T17:30:00+00:00',
            bar_ts='2026-02-11T17:30:00+00:00',
            strategy_ts='2026-02-11T17:30:00+00:00',
            execution_attributed_bar='2026-02-11T17:30:00+00:00',
            symbol='ETHUSDT',
            preclose_offset_seconds=0,
            current_price=1935.31,
            source_status='OK',
            fast_5m={'close': 1935.31, 'high': 1937.99, 'low': 1929.02},
            signal_15m={'close': 1935.31},
            trend_1h={'ema_fast': 1931.0, 'ema_slow': 1948.0, 'adx': 30.0, 'structure_tag': 'TREND_CONT'},
        )

        plan = adapter._manage_trend_position(market, state)
        self.assertIsNotNone(plan)
        self.assertNotEqual(plan.reason, 'open_impulse_early_fail')
        self.assertEqual(plan.action_type, 'state_update')


if __name__ == '__main__':
    unittest.main()
