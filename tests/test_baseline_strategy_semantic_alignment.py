from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.models import LiveStateSnapshot, MarketSnapshot
from exec_framework.strategy_adapter_selector import build_strategy_adapter
from exec_framework.v6c_adapter_baseline import V6CBaselineLiveAdapter


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


class BaselineStrategySemanticAlignmentCase(unittest.TestCase):
    def test_default_baseline_selector_uses_baseline_adapter(self) -> None:
        self.assertIsInstance(build_strategy_adapter('baseline'), V6CBaselineLiveAdapter)
        self.assertIsInstance(build_strategy_adapter(None), V6CBaselineLiveAdapter)

    def test_rev_risk_fraction_matches_baseline_window_mapping(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        for window, expected in [(24, 0.06), (32, 0.08), (48, 0.10)]:
            market = _market(
                rev_candidate={
                    'side': 'long',
                    'entry': 100.0,
                    'stop': 95.0,
                    'tp1': 105.0,
                    'value_window_15m': window,
                    'ts': '2026-05-12T13:00:00+00:00',
                }
            )
            plan = adapter._plan_rev_entry(market, _state())
            self.assertEqual(plan.action_type, 'open')
            self.assertAlmostEqual(plan.risk_fraction or 0.0, expected)
            self.assertEqual(plan.conflict_context.get('rev_window'), window)

    def test_duplicate_rev_signal_ts_is_blocked_after_first_consumption(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        market = _market(
            rev_candidate={
                'side': 'short',
                'entry': 100.0,
                'stop': 105.0,
                'tp1': 95.0,
                'value_window_15m': 24,
                'ts': '2026-05-12T13:00:00+00:00',
            }
        )
        plan = adapter._plan_rev_entry(market, _state(last_rev_signal_ts='2026-05-12T13:00:00+00:00'))
        self.assertEqual(plan.action_type, 'hold')
        self.assertEqual(plan.reason, 'duplicate_rev_signal_ts')

    def test_trend_long_entry_uses_baseline_slippage_reference_prices(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        market = _market()
        plan = adapter._plan_trend_entry(market, _state())
        self.assertEqual(plan.action_type, 'open')
        self.assertEqual(plan.target_strategy, 'trend')
        self.assertEqual(plan.target_side, 'long')
        self.assertAlmostEqual(plan.price_hint or 0.0, 102.0 * 1.0002)
        self.assertAlmostEqual(plan.stop_price or 0.0, min(97.0, 100.0 * (1 - 0.007)) * (1 - 0.0002))

    def test_event_live_blocks_trend_entry_like_baseline_grade_c_gate(self) -> None:
        adapter = V6CBaselineLiveAdapter()
        plan = adapter._plan_trend_entry(_market(event_tag='EVENT_LIVE'), _state())
        self.assertEqual(plan.action_type, 'hold')
        self.assertEqual(plan.reason, 'grade_c_block')


if __name__ == '__main__':
    unittest.main()
