from __future__ import annotations

import unittest

from exec_framework.models import LiveStateSnapshot, MarketSnapshot
from exec_framework.strategy_adapter_selector import build_strategy_adapter
from exec_framework.v6c_adapter_wide_range_guard_w1 import V6CWideRangeGuardW1LiveAdapter


def _state(**overrides) -> LiveStateSnapshot:
    base = dict(
        state_ts='2026-05-12T12:00:00+00:00',
        consistency_status='OK',
        freeze_reason=None,
        account_equity=100.0,
        available_margin=100.0,
        exchange_position_side=None,
        exchange_position_qty=0.0,
        exchange_entry_price=None,
        active_strategy='none',
        active_side=None,
        strategy_entry_time=None,
        strategy_entry_price=None,
        stop_price=None,
        risk_fraction=None,
    )
    base.update(overrides)
    return LiveStateSnapshot(**base)


def _market(**overrides) -> MarketSnapshot:
    hist = [
        {'close': 98.0, 'low': 97.0, 'high': 99.0},
        {'close': 99.0, 'low': 98.0, 'high': 100.0},
        {'close': 100.0, 'low': 99.0, 'high': 101.0},
        {'close': 102.0, 'low': 101.0, 'high': 103.0},
    ]
    base = dict(
        decision_ts='2026-05-12T13:00:00+00:00',
        bar_ts='2026-05-12T13:00:00+00:00',
        strategy_ts='2026-05-12T13:00:00+00:00',
        execution_attributed_bar='2026-05-12T13:00:00+00:00',
        symbol='ETHUSDT',
        preclose_offset_seconds=0,
        current_price=102.0,
        source_status='OK',
        fast_5m={'close': 102.0, 'low': 101.0, 'high': 103.0},
        signal_15m={'close': 102.0, 'low': 101.0, 'high': 103.0},
        signal_15m_ts='2026-05-12T13:00:00+00:00',
        trend_1h={
            'close': 110.0,
            'ema_fast': 100.0,
            'ema_slow': 90.0,
            'adx': 30.0,
            'atr_rank': 0.75,
            'structure_tag': 'EXPANSION',
        },
        trend_1h_ts='2026-05-12T13:00:00+00:00',
        signal_15m_history=hist,
        rev_candidate=None,
        event_tag='NO_EVENT',
    )
    base.update(overrides)
    return MarketSnapshot(**base)


class WideRangeGuardW1AdapterCase(unittest.TestCase):
    def test_selector_builds_w1_adapter(self) -> None:
        self.assertIsInstance(build_strategy_adapter('wide_range_guard_w1'), V6CWideRangeGuardW1LiveAdapter)
        self.assertIsInstance(build_strategy_adapter('runtime_v6c_wide_range_guard_w1'), V6CWideRangeGuardW1LiveAdapter)

    def test_w1_blocks_trend_entry_when_precomputed_features_hit_threshold(self) -> None:
        adapter = V6CWideRangeGuardW1LiveAdapter()
        market = _market(
            fast_5m={
                'close': 102.0,
                'low': 101.0,
                'high': 103.0,
                'wr_box_width_atr': 3.4,
                'wr_trend_efficiency': 0.31,
            }
        )
        plan = adapter._plan_trend_entry(market, _state())
        self.assertEqual(plan.action_type, 'hold')
        self.assertEqual(plan.reason, 'wide_range_guard_w1_block')
        self.assertFalse(plan.requires_execution)
        self.assertEqual(plan.conflict_context.get('blocked_plan'), 'trend_long_entry')
        self.assertEqual(plan.conflict_context.get('blocked_side'), 'long')
        self.assertTrue(plan.conflict_context.get('wide_range_guard_w1'))

    def test_w1_missing_features_falls_back_to_baseline_open(self) -> None:
        adapter = V6CWideRangeGuardW1LiveAdapter()
        plan = adapter._plan_trend_entry(_market(), _state())
        self.assertEqual(plan.action_type, 'open')
        self.assertEqual(plan.reason, 'trend_long_entry')
        self.assertEqual(plan.conflict_context.get('wide_range_guard_w1'), 'missing_features')

    def test_w1_can_compute_from_fast_5m_history(self) -> None:
        adapter = V6CWideRangeGuardW1LiveAdapter()
        market = _market()
        history = []
        for idx in range(48):
            close = 100.0 + (0.1 if idx % 2 == 0 else -0.1)
            if idx == 0:
                low, high = 94.0, 95.0
            elif idx == 1:
                low, high = 105.0, 106.0
            else:
                low, high = close - 0.5, close + 0.5
            history.append(
                {
                    'open': close,
                    'high': high,
                    'low': low,
                    'close': close,
                }
            )
        setattr(market, 'fast_5m_history', history)
        plan = adapter._plan_trend_entry(market, _state())
        self.assertEqual(plan.action_type, 'hold')
        self.assertEqual(plan.reason, 'wide_range_guard_w1_block')
        self.assertEqual(plan.conflict_context.get('wr_source'), 'fast_5m_history')


if __name__ == '__main__':
    unittest.main()
