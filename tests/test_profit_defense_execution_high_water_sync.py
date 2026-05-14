from __future__ import annotations

import unittest

from exec_framework.engine import LiveEngine
from exec_framework.mock_modules import InMemoryStateStore, MockExecutorModule
from exec_framework.models import LiveStateSnapshot, MarketSnapshot
from exec_framework.v6c_adapter_baseline import V6CBaselineLiveAdapter


class BaselineProfitDefenseExecutionHighWaterSyncCase(unittest.TestCase):
    def _make_market(
        self,
        *,
        close: float,
        high: float,
        low: float,
        bar_ts: str,
        ema_fast: float = 3219.898549831269,
        ema_slow: float = 3191.028591207069,
        adx: float = 30.33779225838012,
        structure_tag: str = 'TREND_CONT',
    ) -> MarketSnapshot:
        return MarketSnapshot(
            decision_ts=bar_ts,
            bar_ts=bar_ts,
            strategy_ts=bar_ts,
            execution_attributed_bar=bar_ts,
            symbol='ETHUSDT',
            preclose_offset_seconds=0,
            current_price=close,
            source_status='OK',
            fast_5m={'close': close, 'high': high, 'low': low},
            signal_15m={'close': close},
            trend_1h={'ema_fast': ema_fast, 'ema_slow': ema_slow, 'adx': adx, 'structure_tag': structure_tag},
        )

    def test_state_update_persists_execution_high_water_for_next_bar(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        executor = MockExecutorModule()
        initial_state = LiveStateSnapshot(
            state_ts='2026-01-06T10:15:00+00:00',
            consistency_status='OK',
            freeze_reason=None,
            account_equity=100.0,
            available_margin=100.0,
            exchange_position_side='long',
            exchange_position_qty=0.15165629690235066,
            exchange_entry_price=3212.852442,
            active_strategy='trend',
            active_side='long',
            strategy_entry_time='2026-01-05T18:00:00+00:00',
            strategy_entry_price=3212.852442,
            stop_price=3146.9138677089577,
            risk_fraction=0.1,
            quality_bucket='MEDIUM',
            degrade_state='HOLD',
            add_on_count=0,
            base_quantity=0.15165629690235066,
            equity_at_entry=100.0,
            risk_amount=10.0,
            risk_per_unit=65.93857429104219,
            strategy_ref_entry_price=3212.852442,
            strategy_ref_risk_per_unit=65.93857429104219,
            strategy_ref_base_quantity=0.15165629690235066,
            strategy_ref_notional=487.24930384739434,
            strategy_ref_risk_amount=10.0,
            execution_entry_price=3212.852442,
            execution_risk_per_unit=65.93857429104219,
            execution_quantity=0.15165629690235066,
            execution_notional=487.24930384739434,
            execution_risk_amount=10.0,
            execution_high_water_r=0.0,
            high_water_r=0.0,
            p1_armed=False,
            p2_armed=False,
            profit_defense_start_pct=0.32,
            profit_defense_giveback_pct=0.33,
            p1_trigger_r=1.0,
            p2_trigger_r=2.0,
        )
        store = InMemoryStateStore(initial_state)
        engine = LiveEngine(store, adapter, executor)

        hold_out = engine.run_once(
            self._make_market(
                close=3273.96,
                high=3283.0,
                low=3251.82,
                bar_ts='2026-01-06T14:15:00+00:00',
            )
        )
        self.assertEqual(hold_out['plan']['action_type'], 'state_update')
        self.assertAlmostEqual(hold_out['state']['high_water_r'], 0.9267345959025637)
        self.assertAlmostEqual(hold_out['state']['execution_high_water_r'], 0.9267345959025637)

    def test_profit_defense_prefers_execution_high_water_when_present(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        state = LiveStateSnapshot(
            state_ts='2026-01-06T14:15:00+00:00',
            consistency_status='OK',
            freeze_reason=None,
            account_equity=100.0,
            available_margin=100.0,
            exchange_position_side='long',
            exchange_position_qty=0.15165629690235066,
            exchange_entry_price=3212.852442,
            active_strategy='trend',
            active_side='long',
            strategy_entry_time='2026-01-05T18:00:00+00:00',
            strategy_entry_price=3212.852442,
            stop_price=3229.3370855727603,
            risk_fraction=0.1,
            quality_bucket='MEDIUM',
            degrade_state='HOLD',
            add_on_count=0,
            base_quantity=0.15165629690235066,
            equity_at_entry=100.0,
            risk_amount=10.0,
            risk_per_unit=65.93857429104219,
            execution_entry_price=3212.852442,
            execution_risk_per_unit=65.93857429104219,
            execution_quantity=0.15165629690235066,
            execution_notional=487.24930384739434,
            execution_risk_amount=10.0,
            execution_high_water_r=3.3,
            high_water_r=1.1698396398370279,
            p1_armed=True,
            p2_armed=False,
            profit_defense_start_pct=0.32,
            profit_defense_giveback_pct=0.33,
            p1_trigger_r=1.0,
            p2_trigger_r=2.0,
        )
        market = self._make_market(
            close=3289.99,
            high=3304.99,
            low=3287.83,
            bar_ts='2026-01-06T14:45:00+00:00',
        )

        plan = adapter._manage_trend_position(market, state)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.action_type, 'close')
        self.assertEqual(plan.reason, 'profit_defense_exit')


if __name__ == '__main__':
    unittest.main()
