from __future__ import annotations

import unittest

from exec_framework.models import LiveStateSnapshot, MarketSnapshot
from exec_framework.v6c_adapter_baseline import V6CBaselineLiveAdapter


class RevSameBarReopenCase(unittest.TestCase):
    def test_same_bar_tp_close_does_not_reopen_rev_in_live_baseline(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        state = LiveStateSnapshot(
            state_ts='2025-08-31T11:10:00+00:00',
            consistency_status='OK',
            freeze_reason=None,
            account_equity=296.5644657610763,
            available_margin=296.5644657610763,
            exchange_position_side='short',
            exchange_position_qty=1.343656103888553,
            exchange_entry_price=4458.88,
            active_strategy='rev',
            active_side='short',
            strategy_entry_time='2025-08-31T10:00:00+00:00',
            strategy_entry_price=4458.88,
            stop_price=4475.89838,
            risk_fraction=0.08,
            tp_price=4441.861620000001,
            hold_bars=14,
            rev_window=32,
            high_water_r=0.0,
            execution_high_water_r=0.0,
            can_open_new_position=True,
            can_modify_position=True,
        )
        market = MarketSnapshot(
            decision_ts='2025-08-31T11:14:59.999000+00:00',
            bar_ts='2025-08-31T11:15:00+00:00',
            strategy_ts='2025-08-31T11:15:00+00:00',
            execution_attributed_bar='2025-08-31T11:15:00+00:00',
            symbol='ETHUSDT',
            preclose_offset_seconds=0,
            current_price=4454.92,
            source_status='OK',
            fast_5m={'open': 4444.23, 'high': 4457.87, 'low': 4441.55, 'close': 4454.92},
            signal_15m={'close': 4454.92, 'low': 4441.55, 'high': 4457.87},
            signal_15m_ts='2025-08-31T11:14:59.999000+00:00',
            trend_1h={'close': 4454.92, 'ema_fast': 4475.0, 'ema_slow': 4488.0, 'adx': 18.0, 'atr_rank': 0.2, 'structure_tag': 'CHOP'},
            trend_1h_ts='2025-08-31T10:59:59.999000+00:00',
            signal_15m_history=[
                {'close': 4470.0, 'low': 4464.0, 'high': 4476.0},
                {'close': 4463.0, 'low': 4458.0, 'high': 4467.0},
                {'close': 4459.0, 'low': 4453.0, 'high': 4461.0},
                {'close': 4456.0, 'low': 4449.0, 'high': 4459.0},
                {'close': 4450.0, 'low': 4445.0, 'high': 4453.0},
                {'close': 4454.92, 'low': 4441.55, 'high': 4457.87},
            ],
            rev_candidate={
                'ts': '2025-08-31T11:15:00+00:00',
                'side': 'long',
                'entry': 4454.92,
                'stop': 4441.155359999999,
                'tp1': 4468.684640000001,
                'risk': 13.764640000001236,
                'value_window_15m': 24,
            },
            event_tag='NO_EVENT',
        )

        plan = adapter.plan(market, state)
        self.assertEqual(plan.action_type, 'close')
        self.assertEqual(plan.target_strategy, 'rev')
        self.assertEqual(plan.target_side, 'short')
        self.assertEqual(plan.reason, 'tp1_hit')


if __name__ == '__main__':
    unittest.main()
