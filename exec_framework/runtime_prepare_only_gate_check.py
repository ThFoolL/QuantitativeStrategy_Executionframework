from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from .runtime_env import load_binance_env, validate_runtime_config
    from .runtime_status_cli import build_runtime_status_summary
except ImportError:  # pragma: no cover
    from runtime_env import load_binance_env, validate_runtime_config
    from runtime_status_cli import build_runtime_status_summary


EXPECTED_SYMBOL = 'ETHUSDT'
EXPECTED_CONFIRM_OK = {'confirmed', None}
EXPECTED_PENDING_OK = {None, 'none'}
AUDIT_ONLY_PENDING_PHASES = {'strict_flat_ready_reset', 'reset_local_runtime_exception_context'}


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _check(name: str, ok: bool, actual: Any, expected: str, note: str | None = None) -> dict[str, Any]:
    payload = {
        'name': name,
        'ok': bool(ok),
        'actual': actual,
        'expected': expected,
    }
    if note:
        payload['note'] = note
    return payload


def _normalize_pending_phase_for_prepare_gate(value: Any) -> str | None:
    normalized = _normalize_text(value)
    if normalized in AUDIT_ONLY_PENDING_PHASES:
        return None
    return normalized


def build_prepare_only_gate_report(
    *,
    runtime_status_path: str | Path,
    env_file: str | Path | None = None,
    allow_execution_confirmation_real_send: bool = False,
) -> dict[str, Any]:
    runtime_summary = build_runtime_status_summary(runtime_status_path=runtime_status_path, tail=10)
    runtime = runtime_summary.get('runtime') or {}
    freeze = runtime_summary.get('freeze') or {}
    confirm_summary = runtime_summary.get('confirm_summary') or {}
    submit_gate = runtime_summary.get('submit_gate') or {}
    env_gate_summary = runtime_summary.get('env_gate_summary') or {}
    runtime_config_validation = runtime_summary.get('runtime_config_validation') or {}
    operator_compact_view = runtime_summary.get('operator_compact_view') or {}

    env_validation = None
    env_validation_dict: dict[str, Any] | None = None
    if env_file:
        env_validation = validate_runtime_config(load_binance_env(env_file))
        env_validation_dict = env_validation.as_dict()

    pending_execution_phase = _normalize_pending_phase_for_prepare_gate(freeze.get('pending_execution_phase'))
    submit_allowed_now = submit_gate.get('submit_allowed')
    if submit_allowed_now is None:
        submit_allowed_now = (env_gate_summary.get('binance_submit_env') or {}).get('submit_allowed_now')
    if submit_allowed_now is None:
        runtime_mode = _normalize_text(freeze.get('runtime_mode'))
        freeze_status = _normalize_text(freeze.get('freeze_status'))
        submit_allowed_now = bool(
            runtime_mode == 'ACTIVE'
            and freeze_status in {'NONE', None}
            and pending_execution_phase in EXPECTED_PENDING_OK
            and bool((env_gate_summary.get('binance_submit_env') or {}).get('ready_by_env'))
        )

    checks = [
        _check(
            'symbol_is_ethusdt',
            _normalize_text(runtime.get('symbol')) == EXPECTED_SYMBOL,
            runtime.get('symbol'),
            EXPECTED_SYMBOL,
        ),
        _check(
            'runtime_config_validation_ok',
            bool(runtime_config_validation.get('ok')),
            runtime_config_validation.get('ok'),
            'true',
            note='先确认当前运行档位不是伪装成可开门状态。',
        ),
        _check(
            'submit_env_ready_by_env',
            bool((env_gate_summary.get('binance_submit_env') or {}).get('ready_by_env')),
            (env_gate_summary.get('binance_submit_env') or {}).get('ready_by_env'),
            'true',
            note='这里只检查 env 层是否已准备，不代表允许真实 submit。',
        ),
        _check(
            'submit_allowed_now',
            bool(submit_allowed_now),
            submit_allowed_now,
            'true',
            note='必须在运行态 guardrail 全清空后才算真正可进入现场动作；若最近一帧是已闭环 close，则允许从 env-ready + ACTIVE/NONE/无 pending 推断当前可提交。',
        ),
        _check(
            'submit_guardrail_blockers_empty',
            not (submit_gate.get('guardrail_blockers') or []),
            submit_gate.get('guardrail_blockers') or [],
            '[]',
        ),
        _check(
            'runtime_mode_active',
            _normalize_text(freeze.get('runtime_mode')) == 'ACTIVE',
            freeze.get('runtime_mode'),
            'ACTIVE',
        ),
        _check(
            'freeze_status_none',
            _normalize_text(freeze.get('freeze_status')) in {'NONE', None},
            freeze.get('freeze_status'),
            'NONE',
        ),
        _check(
            'pending_execution_phase_clear',
            pending_execution_phase in EXPECTED_PENDING_OK,
            freeze.get('pending_execution_phase'),
            'none|<empty>',
        ),
        _check(
            'latest_confirm_category_safe',
            _normalize_text(confirm_summary.get('confirmation_category')) in EXPECTED_CONFIRM_OK,
            confirm_summary.get('confirmation_category'),
            'confirmed|<empty>',
        ),
        _check(
            'latest_reconcile_ok',
            _normalize_text(confirm_summary.get('reconcile_status')) in {'OK', None},
            confirm_summary.get('reconcile_status'),
            'OK|<empty>',
        ),
        _check(
            'execution_confirmation_real_send_closed',
            bool((env_gate_summary.get('discord_execution_confirmation_env') or {}).get('open_by_env')) if allow_execution_confirmation_real_send else (not bool((env_gate_summary.get('discord_execution_confirmation_env') or {}).get('open_by_env'))),
            (env_gate_summary.get('discord_execution_confirmation_env') or {}).get('open_by_env'),
            'true' if allow_execution_confirmation_real_send else 'false',
            note='默认首轮最小真实采样前此开关应继续关闭；若本轮目标是专项验证 execution_confirmation 真发送，则此项改为要求 env 已显式打开。',
        ),
    ]

    blockers = [item['name'] for item in checks if not item['ok']]
    facts = {
        'runtime_mode': freeze.get('runtime_mode'),
        'freeze_status': freeze.get('freeze_status'),
        'pending_execution_phase': pending_execution_phase,
        'pending_execution_phase_raw': freeze.get('pending_execution_phase'),
        'confirmation_category': confirm_summary.get('confirmation_category'),
        'reconcile_status': confirm_summary.get('reconcile_status'),
        'submit_allowed': submit_allowed_now,
        'guardrail_blockers': submit_gate.get('guardrail_blockers') or [],
        'env_open_summary': {
            'binance_submit_env': (env_gate_summary.get('binance_submit_env') or {}).get('open_by_env'),
            'binance_submit_ready': (env_gate_summary.get('binance_submit_env') or {}).get('ready_by_env'),
            'discord_rehearsal_env': (env_gate_summary.get('discord_rehearsal_env') or {}).get('open_by_env'),
            'discord_execution_confirmation_env': (env_gate_summary.get('discord_execution_confirmation_env') or {}).get('open_by_env'),
        },
        'operator_next_focus': operator_compact_view.get('next_focus'),
        'audit_artifact_paths': runtime_summary.get('audit_artifact_paths') or {},
    }

    result = {
        'ok': not blockers,
        'mode': runtime_config_validation.get('mode'),
        'blockers': blockers,
        'checks': checks,
        'facts': facts,
        'runtime_status_path': str(runtime_status_path),
        'env_file': None if env_file is None else str(env_file),
        'allow_execution_confirmation_real_send': allow_execution_confirmation_real_send,
        'note': 'prepare_only gate 检查只做现场清单对照，不触发真实交易；是否要求 execution_confirmation 真发送关闭/打开，由本轮目标显式决定。',
    }
    if env_validation_dict is not None:
        result['env_validation'] = env_validation_dict
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='首轮 50U 最小真实采样前 prepare-only gate 检查')
    parser.add_argument('--runtime-status', default='runtime/runtime_status.json', help='runtime_status.json 路径')
    parser.add_argument('--env-file', default=None, help='可选：env 文件路径，用于额外校验 env 层准备度')
    parser.add_argument('--pretty', action='store_true', help='格式化输出 JSON')
    parser.add_argument('--allow-execution-confirmation-real-send', action='store_true', help='本轮若明确要验证 execution_confirmation 真发送，则允许并要求该 env gate 打开')
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = build_prepare_only_gate_report(
        runtime_status_path=args.runtime_status,
        env_file=args.env_file,
        allow_execution_confirmation_real_send=bool(args.allow_execution_confirmation_real_send),
    )
    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
