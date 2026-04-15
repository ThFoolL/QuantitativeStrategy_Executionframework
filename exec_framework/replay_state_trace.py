from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.engine import LiveEngine
from exec_framework.mock_modules import InMemoryStateStore, MockExecutorModule
from exec_framework.preclose_replay import build_initial_state, build_market_from_frames
from exec_framework.strategy_adapter_selector import DEFAULT_STRATEGY_ADAPTER, build_strategy_adapter
from tmp.bt_s1a_formal_v6c import build_reversal_state, load_trend_ctx


def trace_window(data_path: Path, start_ts: str, end_ts: str, strategy_adapter: str = DEFAULT_STRATEGY_ADAPTER) -> pd.DataFrame:
    trend_ctx = load_trend_ctx(data_path)
    rev_ctx = build_reversal_state(data_path)
    fast_df = trend_ctx['fast_df']
    signal_df = trend_ctx['signal_df']
    trend_df = trend_ctx['trend_df']
    rev_signals = rev_ctx['signals']

    start = pd.Timestamp(start_ts)
    end = pd.Timestamp(end_ts)
    state_store = InMemoryStateStore(build_initial_state())
    engine = LiveEngine(state_store, build_strategy_adapter(strategy_adapter), MockExecutorModule())
    rows = []

    for idx, bar_ts in enumerate(fast_df.index):
        if bar_ts > end:
            break
        market = build_market_from_frames(fast_df, signal_df, trend_df, rev_signals, idx, trend_ctx=trend_ctx)
        if market is None:
            continue
        out = engine.run_once(market)
        if bar_ts < start:
            continue
        state = out['state']
        plan = out['plan']
        rows.append({
            'bar_ts': out['market']['bar_ts'],
            'close_fast': out['market']['fast_5m'].get('close'),
            'close_signal': out['market']['signal_15m'].get('close'),
            'stop_price': state.get('stop_price'),
            'entry_price': state.get('strategy_entry_price'),
            'high_water_r': state.get('high_water_r'),
            'p1_armed': state.get('p1_armed'),
            'p2_armed': state.get('p2_armed'),
            'add_on_count': state.get('add_on_count'),
            'degrade_state': state.get('degrade_state'),
            'risk_fraction': state.get('risk_fraction'),
            'risk_amount': state.get('risk_amount'),
            'equity_at_entry': state.get('equity_at_entry'),
            'plan_action': plan.get('action_type'),
            'plan_reason': plan.get('reason'),
        })
    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Trace replay state over a fixed offline window')
    parser.add_argument('--data-path', type=Path, default=ROOT / 'data' / 'ethusdt_5m_202409_202503_is_std.csv')
    parser.add_argument('--start-ts', default='2024-09-06 14:15:00+00:00')
    parser.add_argument('--end-ts', default='2024-09-06 17:40:00+00:00')
    parser.add_argument(
        '--strategy-adapter',
        default=DEFAULT_STRATEGY_ADAPTER,
        help=f"Strategy adapter selector name (default: {DEFAULT_STRATEGY_ADAPTER})",
    )
    parser.add_argument('--out', type=Path, default=ROOT / 'out' / '_timing_tmp' / 'replay_state_trace.csv')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    df = trace_window(args.data_path, args.start_ts, args.end_ts, strategy_adapter=args.strategy_adapter)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(df.to_string(index=False))
    print('trace_csv', args.out)
