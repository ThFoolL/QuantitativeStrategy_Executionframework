from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%SZ')


def build_paths(base_dir: Path, symbol: str, ts: str) -> dict[str, str]:
    stem = f"{ts}_{symbol}"
    return {
        'result_md': str(base_dir / f"{stem}_minimal_real_trade_sampling_result.md"),
        'pretrade_pack': str(base_dir / f"{stem}_pretrade_readonly_pack.json"),
        'open_pack': str(base_dir / f"{stem}_open_confirm_pack.json"),
        'open_fixture': str(base_dir / f"{stem}_open_confirm_pack.fixture.json"),
        'open_operator_log': str(base_dir / f"{stem}_open_confirm_pack.operator_log_draft.md"),
        'close_pack': str(base_dir / f"{stem}_close_confirm_pack.json"),
        'close_fixture': str(base_dir / f"{stem}_close_confirm_pack.fixture.json"),
        'close_operator_log': str(base_dir / f"{stem}_close_confirm_pack.operator_log_draft.md"),
    }


def render_command_plan(env_file: str, symbol: str, paths: dict[str, str]) -> dict[str, object]:
    return {
        'note': '仅生成最小真实试单采样的命令草稿与归档路径；本脚本不会提交订单，不会触发真实交易。',
        'safety': {
            'submit_live_order': False,
            'execution_confirmation_real_send': False,
            'purpose': 'prepare_only',
        },
        'commands': {
            'copy_result_template': (
                'cp docs/deploy_v6c/samples/real_trade_sampling/minimal_real_trade_sampling_result.template.md '
                f"{paths['result_md']}"
            ),
            'capture_pretrade_pack': (
                'python3 live/binance_readonly_sample_capture.py '
                f"--env-file {env_file} --symbol {symbol} --out {paths['pretrade_pack']}"
            ),
            'validate_pretrade_pack': (
                'python3 live/binance_readonly_pack.py validate '
                f"--in {paths['pretrade_pack']}"
            ),
            'capture_open_confirm_pack_template': (
                'python3 live/binance_readonly_sample_capture.py '
                f"--env-file {env_file} --symbol {symbol} --order-id <OPEN_ORDER_ID> --out {paths['open_pack']}"
            ),
            'adapt_open_confirm_pack': (
                'python3 live/binance_readonly_pack.py adapt '
                f"--in {paths['open_pack']} --out {paths['open_fixture']}"
            ),
            'draft_open_operator_log': (
                'python3 live/operator_log_draft.py '
                f"--adapted-in {paths['open_fixture']} --out {paths['open_operator_log']} --format md"
            ),
            'capture_close_confirm_pack_template': (
                'python3 live/binance_readonly_sample_capture.py '
                f"--env-file {env_file} --symbol {symbol} --order-id <CLOSE_ORDER_ID> --out {paths['close_pack']}"
            ),
            'adapt_close_confirm_pack': (
                'python3 live/binance_readonly_pack.py adapt '
                f"--in {paths['close_pack']} --out {paths['close_fixture']}"
            ),
            'draft_close_operator_log': (
                'python3 live/operator_log_draft.py '
                f"--adapted-in {paths['close_fixture']} --out {paths['close_operator_log']} --format md"
            ),
            'inspect_runtime_status': (
                'python3 -m exec_framework.runtime_status_cli '
                '--runtime-status runtime/runtime_status.json --pretty'
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='最小真实试单采样 helper（只生成命令草稿，不发单）')
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--env-file', default='/root/.openclaw/workspace-mike/secrets/binance_api.env')
    parser.add_argument('--base-dir', default='docs/deploy_v6c/samples/real_trade_sampling')
    parser.add_argument('--timestamp', default=None, help='可选：指定 UTC 时间戳，格式如 2026-03-26T120000Z')
    parser.add_argument('--out', default='-', help='输出到文件；默认 stdout')
    args = parser.parse_args()

    ts = args.timestamp or utc_ts()
    base_dir = Path(args.base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    paths = build_paths(base_dir, args.symbol, ts)
    payload = {
        'timestamp': ts,
        'symbol': args.symbol,
        'paths': paths,
        'plan': render_command_plan(args.env_file, args.symbol, paths),
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2) + '\n'
    if args.out == '-':
        print(text, end='')
    else:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding='utf-8')
        print(json.dumps({'ok': True, 'out': str(out_path), 'timestamp': ts, 'symbol': args.symbol}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
