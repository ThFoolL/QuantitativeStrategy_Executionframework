from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.engine import LiveEngine
from exec_framework.mock_modules import InMemoryStateStore, MockExecutorModule
from exec_framework.models import MarketSnapshot, LiveStateSnapshot
from exec_framework.strategy_adapter_selector import DEFAULT_STRATEGY_ADAPTER, build_strategy_adapter
from tmp.bt_s1a_formal_v6c import build_reversal_state, load_trend_ctx


def build_initial_state() -> LiveStateSnapshot:
    return LiveStateSnapshot(
        state_ts='1970-01-01T00:00:00+00:00',
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


def _last_row_le(df: pd.DataFrame, ts: pd.Timestamp) -> pd.Series | None:
    sub = df.loc[:ts]
    if sub.empty:
        return None
    return sub.iloc[-1]


def _row_to_dict(row: pd.Series | None, keys: list[str]) -> dict:
    if row is None:
        return {}
    out = {}
    for key in keys:
        if key in row.index and pd.notna(row[key]):
            val = row[key]
            out[key] = float(val) if isinstance(val, (int, float)) else val
    return out


def _has_required_trend_fields(trend_row: pd.Series | None) -> bool:
    if trend_row is None:
        return False
    required = ['close', 'ema_fast', 'ema_slow', 'adx', 'atr_rank']
    return all((key in trend_row.index and pd.notna(trend_row[key])) for key in required)


def _rev_candidate_at_or_before(signals: pd.DataFrame, ts: pd.Timestamp, max_age: pd.Timedelta = pd.Timedelta(minutes=15)) -> dict | None:
    if signals.empty:
        return None
    sub = signals[signals['ts'] <= ts]
    if sub.empty:
        return None
    row = sub.iloc[-1]
    sig_ts = pd.Timestamp(row['ts'])
    if ts - sig_ts > max_age:
        return None
    return {
        'ts': sig_ts.isoformat(),
        'side': str(row['side']),
        'entry': float(row['entry']),
        'stop': float(row['stop']),
        'tp1': float(row['tp1']),
        'value_window_15m': int(row['value_window_15m']),
        'score': float(row['score']),
    }


def build_market_from_frames(
    fast_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    trend_df: pd.DataFrame,
    rev_signals: pd.DataFrame,
    idx: int,
    preclose_offset_seconds: int = 0,
    trend_ctx: dict | None = None,
) -> MarketSnapshot | None:
    if idx < 3:
        return None

    fast_row = fast_df.iloc[idx]
    bar_ts = pd.Timestamp(fast_df.index[idx])
    decision_ts = bar_ts - pd.Timedelta(seconds=preclose_offset_seconds)
    strategy_ts = bar_ts

    signal_row = _last_row_le(signal_df, strategy_ts)
    trend_row = _last_row_le(trend_df, strategy_ts)
    if signal_row is None or trend_row is None or not _has_required_trend_fields(trend_row):
        return None

    signal_hist_df = signal_df.loc[:strategy_ts].tail(4)
    if len(signal_hist_df) < 4:
        return None
    signal_hist = [
        {
            'close': float(r['close']),
            'low': float(r['low']),
            'high': float(r['high']),
        }
        for _, r in signal_hist_df.iterrows()
    ]

    structure_tag = 'CHOP'
    if trend_ctx is not None:
        trend_slice = trend_df.loc[:strategy_ts]
        structure_tag = trend_ctx['ns']['classify_structure'](trend_slice, trend_ctx['cfg'])

    return MarketSnapshot(
        decision_ts=decision_ts.isoformat(),
        bar_ts=bar_ts.isoformat(),
        strategy_ts=strategy_ts.isoformat(),
        execution_attributed_bar=bar_ts.isoformat(),
        symbol='ETHUSDT',
        preclose_offset_seconds=preclose_offset_seconds,
        current_price=float(fast_row['close']),
        source_status='OK',
        fast_5m=_row_to_dict(fast_row, ['open', 'high', 'low', 'close', 'volume']),
        signal_15m=_row_to_dict(signal_row, ['open', 'high', 'low', 'close', 'volume']),
        signal_15m_ts=str(signal_row.name) if signal_row is not None else None,
        trend_1h={
            **_row_to_dict(trend_row, ['open', 'high', 'low', 'close', 'volume', 'ema_fast', 'ema_slow', 'adx', 'atr_rank']),
            'structure_tag': structure_tag,
        },
        trend_1h_ts=str(trend_row.name) if trend_row is not None else None,
        signal_15m_history=signal_hist,
        rev_candidate=_rev_candidate_at_or_before(rev_signals, bar_ts),
    )


def run_replay(
    path: str | Path,
    start: int | None = None,
    limit: int | None = 200,
    strategy_adapter: str = DEFAULT_STRATEGY_ADAPTER,
) -> dict:
    trend_ctx = load_trend_ctx(Path(path))
    rev_ctx = build_reversal_state(Path(path))
    fast_df = trend_ctx['fast_df']
    signal_df = trend_ctx['signal_df']
    trend_df = trend_ctx['trend_df']
    rev_signals = rev_ctx['signals']

    ready_mask = trend_df[['close', 'ema_fast', 'ema_slow', 'adx', 'atr_rank']].notna().all(axis=1)
    first_ready_ts = trend_df.index[ready_mask.argmax()] if ready_mask.any() else fast_df.index[0]
    auto_start = int(fast_df.index.searchsorted(first_ready_ts))
    start_idx = auto_start if start is None else max(start, auto_start)

    state_store = InMemoryStateStore(build_initial_state())
    engine = LiveEngine(state_store, build_strategy_adapter(strategy_adapter), MockExecutorModule())

    results = []
    action_counts = Counter()
    reason_counts = Counter()
    action_rows = []
    end = len(fast_df) if limit is None else min(len(fast_df), start_idx + limit)
    for idx in range(start_idx, end):
        market = build_market_from_frames(fast_df, signal_df, trend_df, rev_signals, idx, trend_ctx=trend_ctx)
        if market is None:
            continue
        result = engine.run_once(market)
        results.append(result)
        plan = result['plan']
        action_counts[plan['action_type']] += 1
        reason_counts[plan['reason']] += 1
        if plan['action_type'] != 'hold':
            action_rows.append({
                'decision_ts': result['market']['decision_ts'],
                'strategy_ts': result['market'].get('strategy_ts') or result['market']['bar_ts'],
                'bar_ts': result['market']['bar_ts'],
                'execution_attributed_bar': result['market'].get('execution_attributed_bar') or result['market']['bar_ts'],
                'action_type': plan['action_type'],
                'reason': plan['reason'],
                'target_strategy': plan['target_strategy'],
                'target_side': plan['target_side'],
                'price_hint': plan['price_hint'],
                'stop_price': plan['stop_price'],
                'plan_ts': plan['plan_ts'],
                'result_status': result['result']['status'],
                'requires_execution': plan['requires_execution'],
                'close_reason': plan.get('close_reason'),
            })

    final_state = state_store.load_state()
    return {
        'auto_start_ts': str(first_ready_ts),
        'auto_start_idx': int(start_idx),
        'bars_processed': len(results),
        'action_counts': dict(action_counts),
        'reason_counts_top10': dict(reason_counts.most_common(10)),
        'action_rows': action_rows,
        'last_plan': results[-1]['plan'] if results else None,
        'final_state': final_state,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Offline preclose replay for strategy-layer inspection')
    parser.add_argument('--data-path', type=Path, default=ROOT / 'data' / 'ethusdt_5m_202409_202503_is_std.csv')
    parser.add_argument('--start', type=int)
    parser.add_argument('--limit', type=int, default=200)
    parser.add_argument(
        '--strategy-adapter',
        default=DEFAULT_STRATEGY_ADAPTER,
        help=f"Strategy adapter selector name (default: {DEFAULT_STRATEGY_ADAPTER})",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    out = run_replay(args.data_path, start=args.start, limit=args.limit, strategy_adapter=args.strategy_adapter)
    print('auto_start_ts', out['auto_start_ts'])
    print('auto_start_idx', out['auto_start_idx'])
    print('bars_processed', out['bars_processed'])
    print('action_counts', out['action_counts'])
    print('reason_counts_top10', out['reason_counts_top10'])
    print('action_rows_sample', out['action_rows'][:10])
    if out['last_plan']:
        print('last_plan', out['last_plan']['action_type'], out['last_plan']['reason'])
    print('final_state', out['final_state'].active_strategy, out['final_state'].active_side)
