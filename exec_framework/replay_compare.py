from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.preclose_replay import run_replay

REPLAY_OUT = ROOT / 'out' / '_timing_tmp' / 'preclose_replay_actions.csv'
COMPARE_OUT = ROOT / 'out' / '_timing_tmp' / 'preclose_replay_compare.csv'
SUMMARY_OUT = ROOT / 'out' / '_timing_tmp' / 'preclose_replay_compare_summary.json'
BACKTEST_LOG = ROOT / 'out' / 'bt_s1a_formal_v6c' / 'is' / 'logs.csv'
DATA_PATH = ROOT / 'data' / 'ethusdt_5m_202409_202503_is_std.csv'


def save_replay_actions(action_rows: list[dict]) -> None:
    REPLAY_OUT.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(action_rows)
    df.to_csv(REPLAY_OUT, index=False)


def _normalize_replay_action(action_type: str) -> str:
    if action_type == 'state_update':
        return 'state_update'
    return action_type


def _action_rows_df(action_rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(action_rows)
    if df.empty:
        return df
    for col in ['decision_ts', 'strategy_ts', 'bar_ts', 'execution_attributed_bar']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
    df['normalized_action'] = df['action_type'].map(_normalize_replay_action)
    return df


def load_backtest_slice(start_ts: str, end_ts: str) -> pd.DataFrame:
    rows = []
    with BACKTEST_LOG.open() as f:
        r = csv.DictReader(f)
        for row in r:
            ts = pd.Timestamp(row['timestamp'])
            if pd.Timestamp(start_ts) <= ts <= pd.Timestamp(end_ts):
                rows.append(row)
    df = pd.DataFrame(rows)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df


def normalize_backtest_kind(kind: str) -> str:
    if kind.endswith('_OPEN'):
        return 'open'
    if kind.endswith('_CLOSE'):
        return 'close'
    if kind.endswith('_ADD'):
        return 'add'
    if kind.endswith('_TRIM'):
        return 'trim'
    return kind.lower()


def build_compare(limit: int = 200) -> dict:
    replay = run_replay(DATA_PATH, limit=limit)
    save_replay_actions(replay['action_rows'])
    if not replay['action_rows']:
        out = {
            'replay': replay,
            'compare_rows': [],
            'backtest_counts': {},
            'summary': {},
        }
        SUMMARY_OUT.write_text(json.dumps(out['summary'], ensure_ascii=False, indent=2))
        return out

    start_ts = replay['action_rows'][0]['bar_ts']
    end_ts = replay['action_rows'][-1]['bar_ts']
    backtest_df = load_backtest_slice(start_ts, end_ts)

    replay_df = _action_rows_df(replay['action_rows'])

    compare_rows = []
    if not replay_df.empty:
        for _, row in replay_df.iterrows():
            ts = row['execution_attributed_bar'] if 'execution_attributed_bar' in row and pd.notna(row['execution_attributed_bar']) else row['bar_ts']
            matched = backtest_df[backtest_df['timestamp'] == ts] if not backtest_df.empty else pd.DataFrame()
            compare_rows.append({
                'decision_ts': row['decision_ts'].isoformat() if pd.notna(row['decision_ts']) else '',
                'strategy_ts': row['strategy_ts'].isoformat() if 'strategy_ts' in row and pd.notna(row['strategy_ts']) else '',
                'execution_attributed_bar': ts.isoformat(),
                'replay_action': row['action_type'],
                'replay_action_group': row['normalized_action'],
                'replay_reason': row['reason'],
                'replay_side': row['target_side'],
                'backtest_kinds': '|'.join(matched['kind'].tolist()) if not matched.empty else '',
                'backtest_action_groups': '|'.join(matched['kind'].map(normalize_backtest_kind).tolist()) if not matched.empty else '',
                'backtest_reasons': '|'.join(matched['reason'].fillna('').tolist()) if not matched.empty and 'reason' in matched.columns else '',
                'backtest_sides': '|'.join(matched['side'].fillna('').tolist()) if not matched.empty and 'side' in matched.columns else '',
                'matched_on_execution_bar': bool(not matched.empty),
            })

    compare_df = pd.DataFrame(compare_rows)
    COMPARE_OUT.parent.mkdir(parents=True, exist_ok=True)
    compare_df.to_csv(COMPARE_OUT, index=False)

    backtest_counts = {}
    if not backtest_df.empty:
        kinds = backtest_df['kind'].map(normalize_backtest_kind)
        backtest_counts = kinds.value_counts().to_dict()

    replay_counts = replay_df['normalized_action'].value_counts().to_dict() if not replay_df.empty else {}
    replay_trade_counts = replay_df[replay_df['normalized_action'].isin(['open', 'close', 'add', 'trim', 'flip', 'update_stop'])]['normalized_action'].value_counts().to_dict() if not replay_df.empty else {}
    matched_count = int(compare_df['matched_on_execution_bar'].sum()) if not compare_df.empty else 0
    unmatched_count = int((~compare_df['matched_on_execution_bar']).sum()) if not compare_df.empty else 0
    summary = {
        'time_semantics': {
            'decision_ts': '默认与 strategy_ts 对齐；仅在显式传入 offset 时提前',
            'strategy_ts': '策略语义所属 5m bar',
            'execution_attributed_bar': 'replay / compare / report 统一记账归属 bar；当前与 strategy_ts 同义',
        },
        'window': {
            'start_ts': start_ts,
            'end_ts': end_ts,
        },
        'replay_counts_all_non_hold': replay_counts,
        'replay_trade_like_counts': replay_trade_counts,
        'backtest_counts': backtest_counts,
        'compare_match_stats': {
            'matched_on_execution_bar': matched_count,
            'unmatched_on_execution_bar': unmatched_count,
        },
    }
    SUMMARY_OUT.write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    return {
        'replay': replay,
        'compare_rows': compare_rows,
        'backtest_counts': backtest_counts,
        'summary': summary,
    }


if __name__ == '__main__':
    out = build_compare(limit=200)
    print('replay_action_counts', out['replay']['action_counts'])
    print('backtest_counts', out['backtest_counts'])
    print('summary', out['summary'])
    print('compare_sample', out['compare_rows'][:10])
    print('replay_csv', REPLAY_OUT)
    print('compare_csv', COMPARE_OUT)
    print('summary_json', SUMMARY_OUT)
