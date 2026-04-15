from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.engine import LiveEngine
from exec_framework.mock_modules import InMemoryStateStore, MockExecutorModule
from exec_framework.models import MarketSnapshot, LiveStateSnapshot
from exec_framework.strategy_adapter_selector import build_strategy_adapter


def build_sample_market() -> MarketSnapshot:
    return MarketSnapshot(
        decision_ts='2026-03-24T17:30:00+08:00',
        bar_ts='2026-03-24T17:30:00+08:00',
        strategy_ts='2026-03-24T17:30:00+08:00',
        execution_attributed_bar='2026-03-24T17:30:00+08:00',
        symbol='ETHUSDT',
        preclose_offset_seconds=0,
        current_price=2105.0,
        source_status='OK',
        signal_15m={'close': 2105.0, 'low': 2098.0, 'high': 2108.0},
        trend_1h={
            'close': 2105.0,
            'ema_fast': 2050.0,
            'ema_slow': 2000.0,
            'adx': 40.0,
            'atr_rank': 0.8,
            'structure_tag': 'TREND_CONT',
        },
        signal_15m_history=[
            {'close': 2060.0, 'low': 2055.0, 'high': 2065.0},
            {'close': 2075.0, 'low': 2070.0, 'high': 2080.0},
            {'close': 2090.0, 'low': 2088.0, 'high': 2095.0},
            {'close': 2105.0, 'low': 2098.0, 'high': 2108.0},
        ],
    )


def build_sample_state() -> LiveStateSnapshot:
    return LiveStateSnapshot(
        state_ts='2026-03-24T17:25:00+08:00',
        consistency_status='OK',
        freeze_reason=None,
        account_equity=1000.0,
        available_margin=1000.0,
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


def run_smoke() -> dict:
    engine = LiveEngine(
        state_store=InMemoryStateStore(build_sample_state()),
        strategy_module=build_strategy_adapter(),
        executor_module=MockExecutorModule(),
    )
    return engine.run_once(build_sample_market())


if __name__ == '__main__':
    result = run_smoke()
    print(result['plan']['action_type'], result['plan']['reason'])
    print(result['state']['active_strategy'], result['state']['active_side'])
