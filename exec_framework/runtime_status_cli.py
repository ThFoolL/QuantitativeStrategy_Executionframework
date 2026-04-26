from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from .discord_publisher import DiscordPublisher
    from .models import ExecutionResult, LiveStateSnapshot, MarketSnapshot
    from .runtime_guard import derive_recover_state
except ImportError:  # pragma: no cover
    from discord_publisher import DiscordPublisher
    from models import ExecutionResult, LiveStateSnapshot, MarketSnapshot
    from runtime_guard import derive_recover_state


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding='utf-8'))


def _load_jsonl_tail(path: Path, limit: int = 5) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]
    return rows[-limit:]


def _find_last_event(events: list[dict[str, Any]], event_type: str) -> dict[str, Any] | None:
    for row in reversed(events):
        if row.get('event_type') == event_type:
            return row
    return None


def _safe_tail_jsonl(path: Path | None, limit: int = 20) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    try:
        return _load_jsonl_tail(path, limit=limit)
    except (json.JSONDecodeError, OSError, ValueError):
        return []


def _safe_load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        return _load_json(path)
    except (json.JSONDecodeError, OSError, ValueError):
        return {}


def _truncate_text(value: Any, max_len: int = 160) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + '...'


def _resolve_optional_path(path_value: str | None, *, base_dir: Path) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    candidates = [base_dir / path, base_dir.parent / path]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _summarize_receipt(receipt: dict[str, Any] | None) -> dict[str, Any] | None:
    if not receipt:
        return None
    return {
        'status': receipt.get('status'),
        'payload_kind': receipt.get('payload_kind'),
        'idempotency_key': receipt.get('idempotency_key'),
        'message_id': receipt.get('message_id') or receipt.get('provider_message_id'),
        'channel_id': receipt.get('provider_channel_id') or receipt.get('channel_id') or receipt.get('target'),
        'transport_name': receipt.get('transport_name'),
        'provider_status': receipt.get('provider_status'),
        'failure_category': receipt.get('failure_category'),
        'failure_code': receipt.get('failure_code'),
        'reason': receipt.get('reason'),
        'sent': receipt.get('sent'),
        'retryable': receipt.get('retryable'),
        'attempt_count': receipt.get('attempt_count'),
        'sent_at': receipt.get('sent_at'),
        'recorded_at': receipt.get('recorded_at'),
        'provider_response_excerpt': receipt.get('provider_response_excerpt'),
    }


def _build_ledger_summary(ledger_payload: dict[str, Any]) -> dict[str, Any]:
    entries = ledger_payload.get('entries') or {}
    items = list(entries.items())
    sent_count = 0
    blocked_count = 0
    duplicate_count = 0
    failed_count = 0
    inflight_count = 0
    latest_key = None
    latest_entry = None
    latest_ts = ''
    for key, entry in items:
        status = entry.get('status')
        reason = str(entry.get('reason') or '')
        if entry.get('sent'):
            sent_count += 1
        if status == 'blocked':
            blocked_count += 1
        if status in {'failed', 'failed_retryable'}:
            failed_count += 1
        if status == 'inflight':
            inflight_count += 1
        if 'duplicate' in reason or reason == 'duplicate_idempotency_key':
            duplicate_count += 1
        candidate_ts = str(entry.get('recorded_at') or entry.get('sent_at') or '')
        if candidate_ts >= latest_ts:
            latest_ts = candidate_ts
            latest_key = key
            latest_entry = entry
    return {
        'path': ledger_payload.get('_path'),
        'updated_at': ledger_payload.get('updated_at'),
        'entries_count': len(items),
        'sent_count': sent_count,
        'blocked_count': blocked_count,
        'failed_count': failed_count,
        'duplicate_like_count': duplicate_count,
        'inflight_count': inflight_count,
        'latest_key': latest_key,
        'latest_receipt_summary': _summarize_receipt(latest_entry),
    }


def _build_receipt_log_summary(receipt_rows: list[dict[str, Any]], receipt_store_path: str | None = None) -> dict[str, Any]:
    if not receipt_rows:
        return {
            'path': receipt_store_path,
            'rows_count': 0,
            'status_counts': {},
            'recent_receipt_summary': None,
            'recent_sent_receipt_summary': None,
            'recent_blocked_receipt_summary': None,
            'recent_duplicate_receipt_summary': None,
        }
    status_counts: dict[str, int] = {}
    for row in receipt_rows:
        status = row.get('status') or 'UNKNOWN'
        status_counts[status] = status_counts.get(status, 0) + 1
    recent_sent = next((row for row in reversed(receipt_rows) if row.get('sent')), None)
    recent_blocked = next((row for row in reversed(receipt_rows) if row.get('status') == 'blocked'), None)
    recent_duplicate = next(
        (
            row
            for row in reversed(receipt_rows)
            if 'duplicate' in str(row.get('reason') or '') or row.get('reason') == 'duplicate_idempotency_key'
        ),
        None,
    )
    return {
        'path': receipt_store_path,
        'rows_count': len(receipt_rows),
        'status_counts': status_counts,
        'recent_receipt_summary': _summarize_receipt(receipt_rows[-1]),
        'recent_sent_receipt_summary': _summarize_receipt(recent_sent),
        'recent_blocked_receipt_summary': _summarize_receipt(recent_blocked),
        'recent_duplicate_receipt_summary': _summarize_receipt(recent_duplicate),
    }


def _extract_exception_policy(result: dict[str, Any] | None) -> dict[str, Any] | None:
    result = result or {}
    trade_summary = result.get('trade_summary') or {}
    policy = (
        trade_summary.get('exception_policy_view')
        or trade_summary.get('submit_exception_policy')
        or result.get('exception_policy')
    )
    if isinstance(policy, dict):
        return dict(policy)
    return None


def _build_exception_policy_brief(policy: dict[str, Any] | None) -> dict[str, Any] | None:
    if not policy:
        return None
    return {
        'policy': policy.get('policy') or policy.get('action') or policy.get('source_key'),
        'action': policy.get('action'),
        'reason': policy.get('reason') or _truncate_text('; '.join(policy.get('notes') or []), max_len=240),
        'next_action': policy.get('next_action')
        or (' -> '.join(policy.get('auto_repair_steps') or []) if policy.get('auto_repair_steps') else None)
        or (' / '.join(policy.get('readonly_checks') or []) if policy.get('readonly_checks') else None),
        'should_alert': policy.get('should_alert') if policy.get('should_alert') is not None else (policy.get('alert') not in {None, 'none'}),
        'alert': policy.get('alert'),
        'scope': policy.get('scope'),
        'source_key': policy.get('source_key'),
        'retryable': policy.get('retryable'),
        'should_freeze_runtime': policy.get('should_freeze_runtime'),
        'readonly_checks': policy.get('readonly_checks'),
        'auto_repair_steps': policy.get('auto_repair_steps'),
        'notes': policy.get('notes'),
    }


def _build_offline_live_misalignment_summary(
    *,
    latest_result_summary: dict[str, Any] | None,
    submit_gate: dict[str, Any] | None,
    freeze_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    latest_result_summary = latest_result_summary or {}
    submit_gate = submit_gate or {}
    freeze_summary = freeze_summary or {}
    plan_action = latest_result_summary.get('plan_action')
    if not plan_action:
        plan_debug = latest_result_summary.get('plan_debug') or {}
        final_plan = plan_debug.get('final_plan') or {}
        plan_action = final_plan.get('action_type') or final_plan.get('action')
    plan_reason = latest_result_summary.get('plan_reason')
    freeze_reason = freeze_summary.get('freeze_reason') or latest_result_summary.get('freeze_reason')
    pending_block_reason = freeze_summary.get('pending_execution_block_reason') or latest_result_summary.get('pending_execution_block_reason')
    blocked_reason = submit_gate.get('blocked_reason') or pending_block_reason or freeze_reason
    runtime_mode = freeze_summary.get('runtime_mode')
    freeze_status = freeze_summary.get('freeze_status')
    has_offline_open_candidate = plan_action in {'open', 'flip', 'add'}
    execution_phase = latest_result_summary.get('execution_phase')
    confirmation_status = latest_result_summary.get('confirmation_status')
    result_status = latest_result_summary.get('result_status')
    live_execution_was_blocked = bool(blocked_reason) or runtime_mode == 'FROZEN' or freeze_status not in {None, 'NONE'}
    blocked_before_live_submit = execution_phase in {'blocked', None} or confirmation_status == 'NOT_SUBMITTED' or result_status == 'BLOCKED'
    runtime_compatible_verdict = None
    if has_offline_open_candidate and live_execution_was_blocked and blocked_before_live_submit:
        runtime_compatible_verdict = 'STRATEGY_OPEN_BUT_RUNTIME_FROZEN' if runtime_mode == 'FROZEN' or freeze_status not in {None, 'NONE'} else 'LIVE_EXECUTION_EXPECTED_BUT_NOT_SUBMITTED'
    return {
        'detected': bool(runtime_compatible_verdict),
        'has_offline_open_candidate': has_offline_open_candidate,
        'live_execution_was_blocked': live_execution_was_blocked,
        'runtime_compatible_verdict': runtime_compatible_verdict,
        'blocked_before_live_submit': blocked_before_live_submit,
        'strategy_expected_action': plan_action if has_offline_open_candidate else None,
        'final_action': plan_action,
        'final_reason': plan_reason,
        'reason_code': blocked_reason,
        'freeze_reason': freeze_reason,
        'submit_blocked_reason': submit_gate.get('blocked_reason'),
        'pending_execution_block_reason': pending_block_reason,
        'runtime_mode': runtime_mode,
        'freeze_status': freeze_status,
    }


def _is_terminal_flat_trade_reconciliation_pending(*, state: dict[str, Any] | None, latest_result_summary: dict[str, Any] | None, confirm_summary: dict[str, Any] | None) -> bool:
    state = state or {}
    latest_result_summary = latest_result_summary or {}
    confirm_summary = confirm_summary or {}
    if state.get('pending_execution_phase') not in {None, '', 'none'}:
        return False
    if state.get('exchange_position_side') not in {None, ''}:
        return False
    if float(state.get('exchange_position_qty') or 0.0) > 0.0:
        return False
    if list(state.get('exchange_protective_orders') or []):
        return False
    if str(state.get('protective_order_status') or '').upper() not in {'', 'NONE'}:
        return False
    if not bool(state.get('needs_trade_reconciliation')):
        return False
    stop_reason = (
        ((confirm_summary.get('confirm_context') or {}).get('stop_reason'))
        or (((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_reason'))
        or latest_result_summary.get('execution_phase')
    )
    return stop_reason == 'flat_ready_trade_reconciliation_pending'


def _is_cleanup_audit_execution_phase(value: Any) -> bool:
    return str(value or '').strip().lower() == 'reset_local_runtime_exception_context'


def build_execution_confirm_summary(result: dict[str, Any] | None) -> dict[str, Any]:
    result = result or {}
    trade_summary = result.get('trade_summary') or {}
    fills = trade_summary.get('fills') or []
    pnl_values = [item.get('realized_pnl') for item in fills if item.get('realized_pnl') is not None]
    readonly_recheck = trade_summary.get('readonly_recheck')
    return {
        'confirmation_status': result.get('confirmation_status'),
        'confirmation_category': trade_summary.get('confirmation_category'),
        'confirm_context': trade_summary.get('confirm_context'),
        'confirmed_order_status': result.get('confirmed_order_status'),
        'executed_qty': result.get('executed_qty'),
        'requested_qty': trade_summary.get('requested_qty'),
        'avg_fill_price': result.get('avg_fill_price'),
        'exchange_order_ids': result.get('exchange_order_ids'),
        'reconcile_status': result.get('reconcile_status'),
        'freeze_reason': result.get('freeze_reason'),
        'fills_count': trade_summary.get('fills_count'),
        'query_failed': trade_summary.get('query_failed'),
        'fee_assets': trade_summary.get('fee_assets'),
        'realized_pnl': (sum(float(v) for v in pnl_values) if pnl_values else None),
        'notes': trade_summary.get('notes'),
        'protective_orders_count': trade_summary.get('protective_orders_count'),
        'protective_orders': trade_summary.get('protective_orders'),
        'has_protective_orders': trade_summary.get('has_protective_orders'),
        'protective_validation': trade_summary.get('protective_validation'),
        'submit_exception_policy': _extract_exception_policy(result),
        'exception_policy_brief': _build_exception_policy_brief(_extract_exception_policy(result)),
        'submit_exception_metadata': trade_summary.get('submit_exception_metadata'),
        'readonly_recheck': readonly_recheck,
    }


def _build_retry_budget_summary(confirm_context: dict[str, Any] | None, readonly_recheck: dict[str, Any] | None) -> dict[str, Any] | None:
    confirm_context = dict(confirm_context or {})
    readonly_recheck = dict(readonly_recheck or {})
    retry_budget = dict(confirm_context.get('retry_budget') or readonly_recheck.get('retry_budget') or {})
    if not retry_budget and readonly_recheck:
        retry_budget = {
            'attempts_used': 0,
            'attempts_remaining': None,
            'max_attempts': 0,
        }
    if not retry_budget:
        return None
    attempts_used = int(retry_budget.get('attempts_used') or 0)
    max_attempts = int(retry_budget.get('max_attempts') or 0)
    attempts_remaining = retry_budget.get('attempts_remaining')
    return {
        'attempts_used': attempts_used,
        'max_attempts': max_attempts,
        'attempts_remaining': attempts_remaining,
        'current_bar_ts': retry_budget.get('current_bar_ts'),
        'retry_interval_seconds': retry_budget.get('retry_interval_seconds') or confirm_context.get('retry_interval_seconds'),
        'budget_scope': 'shared_readonly_recheck' if readonly_recheck else 'confirm_window',
        'exhausted': attempts_remaining == 0 if attempts_remaining is not None else (max_attempts > 0 and attempts_used >= max_attempts),
    }


_NEGATIVE_PROTECTIVE_RECOVER_RISKS = {
    'replace_invalid_protective_orders',
    'will_replace_existing_protective_orders_during_submit',
    'position_open_without_protection',
    'cannot_safely_cancel_existing_protective_orders',
}
_NEGATIVE_PROTECTIVE_VALIDATION_LEVELS = {
    'MISMATCH',
    'STRUCTURAL_MISMATCH',
    'SEMANTIC_MISMATCH',
    'MISSING',
    'INVALID',
}


def _recover_record_has_negative_management_projection(record: dict[str, Any] | None) -> bool:
    if not isinstance(record, dict) or not record:
        return False
    remaining_risk = str(record.get('remaining_risk') or '').strip()
    if remaining_risk in _NEGATIVE_PROTECTIVE_RECOVER_RISKS:
        return True
    result_detail = str(record.get('result_detail') or '').strip()
    if result_detail == 'CANCEL_USING_EXCHANGE_FACTS':
        return True
    validation_level = str(record.get('validation_level') or '').upper()
    if validation_level in _NEGATIVE_PROTECTIVE_VALIDATION_LEVELS:
        return True
    for attempt in list(record.get('attempts') or []):
        if str((attempt or {}).get('step') or '') == 'protective_rebuild_validate' and str((attempt or {}).get('result') or '') == 'invalid':
            return True
    return False


def _sanitize_negative_management_recover_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return record
    sanitized = dict(record)
    if not _recover_record_has_negative_management_projection(sanitized):
        return sanitized
    sanitized['allowed'] = False
    sanitized['recover_ready'] = False
    sanitized['requires_manual_resume'] = True
    sanitized['recover_policy'] = 'manual_review'
    sanitized['recover_policy_display'] = 'manual_review'
    sanitized['legacy_recover_policy'] = 'manual_review'
    sanitized['recover_stage'] = 'recover_review_required'
    sanitized['stop_category'] = 'manual_review'
    sanitized['stop_condition'] = 'protective_rebuild_negative_projection'
    sanitized['stop_reason'] = str(sanitized.get('reason') or sanitized.get('result_detail') or 'protective_rebuild_negative_projection')
    sanitized['freeze_reason'] = str(sanitized.get('freeze_reason') or sanitized.get('remaining_risk') or sanitized['stop_reason'])
    sanitized['guard_decision'] = 'keep_frozen_negative_protective_projection'
    sanitized['risk_action'] = 'MANUAL_REVIEW'
    return sanitized


def _sanitize_negative_management_recover_timeline(timeline: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        _sanitize_negative_management_recover_record(item) if isinstance(item, dict) else item
        for item in list(timeline or [])
    ]


def _resolve_protection_projection(*, stop_condition: str | None, recover_check: dict[str, Any] | None, position: dict[str, Any], confirm_summary: dict[str, Any]) -> dict[str, Any]:
    recover_check = dict(recover_check or {})
    protection_intent = dict(position.get('strategy_protection_intent') or {})
    protective_validation = dict(confirm_summary.get('protective_validation') or {})
    validation_summary = dict(protective_validation.get('summary') or {})
    validation_level = protective_validation.get('validation_level') or validation_summary.get('validation_level')
    risk_class = protective_validation.get('risk_class') or validation_summary.get('risk_class')
    mismatch_class = protective_validation.get('mismatch_class') or validation_summary.get('mismatch_class')
    legacy_stop_category = recover_check.get('stop_category')

    missing_conditions = {
        'protection_orders_missing',
        'protection_stop_missing',
        'protection_tp_missing',
    }
    mismatch_conditions = {
        'protection_semantic_mismatch',
        'protection_semantic_position_side_mismatch',
        'protection_semantic_type_mismatch',
        'protection_semantic_stop_payload_mismatch',
        'protection_semantic_tp_payload_mismatch',
        'protection_submit_gate_blocked',
        'relapse_after_recover_ready_mismatch',
        'relapse_after_recover_ready_protection_missing',
    }

    projection = {
        'validation_level': validation_level,
        'risk_class': risk_class,
        'mismatch_class': mismatch_class,
        'legacy_stop_category': legacy_stop_category,
        'stop_category': legacy_stop_category,
        'summary_family': None,
        'pending_action': protection_intent.get('pending_action'),
    }

    if validation_level == 'MISSING' or mismatch_class == 'MISSING' or stop_condition in missing_conditions:
        projection['stop_category'] = 'recover_protection'
        projection['summary_family'] = 'protection_missing'
        if projection['pending_action'] is None:
            projection['pending_action'] = 'protective_rebuild'
    elif validation_level in {'STRUCTURAL_MISMATCH', 'SEMANTIC_MISMATCH'} or mismatch_class in {'STRUCTURAL_MISMATCH', 'SEMANTIC_MISMATCH'} or stop_condition in mismatch_conditions:
        projection['stop_category'] = 'manual_review'
        projection['summary_family'] = 'protection_mismatch'
        if projection['pending_action'] is None:
            projection['pending_action'] = 'manual_review'

    return projection


def _select_effective_recover_check(recover_check: dict[str, Any] | None, recover_timeline: list[dict[str, Any]] | None) -> dict[str, Any]:
    current = _sanitize_negative_management_recover_record(dict(recover_check or {}))
    for row in reversed(list(recover_timeline or [])):
        candidate = dict(row or {})
        if not candidate:
            continue
        if candidate.get('risk_action') == 'FORCE_CLOSE' and candidate.get('stop_reason') == 'protective_order_missing':
            return candidate
    return current


def _build_operator_compact_view(
    *,
    runtime: dict[str, Any],
    submit_gate: dict[str, Any] | None,
    freeze: dict[str, Any],
    confirm_summary: dict[str, Any],
    position: dict[str, Any],
    async_operation_view: dict[str, Any] | None = None,
    recover_check: dict[str, Any] | None,
    recover_timeline: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    category = confirm_summary.get('confirmation_category')
    readonly_recheck = confirm_summary.get('readonly_recheck') or {}
    recheck_action = readonly_recheck.get('action')
    recheck_status = readonly_recheck.get('status')
    async_operation_view = dict(async_operation_view or {})
    active_async_operation = async_operation_view.get('latest') or {}
    freeze_reason = freeze.get('freeze_reason') or confirm_summary.get('freeze_reason')
    blocked_reason = (submit_gate or {}).get('blocked_reason')
    exception_policy = confirm_summary.get('submit_exception_policy') or {}
    exception_policy_brief = confirm_summary.get('exception_policy_brief') or _build_exception_policy_brief(exception_policy) or {}
    policy_action = exception_policy_brief.get('action') or exception_policy.get('action')
    readonly_checks = exception_policy.get('readonly_checks') or []
    auto_repair_steps = exception_policy.get('auto_repair_steps') or []
    confirm_context = dict(confirm_summary.get('confirm_context') or readonly_recheck.get('confirm_context') or {})
    retry_budget = _build_retry_budget_summary(confirm_context, readonly_recheck)
    recover_check = _select_effective_recover_check(recover_check, recover_timeline)
    readonly_confirm_context = dict(readonly_recheck.get('confirm_context') or {})
    stop_reason = recover_check.get('stop_reason') or recover_check.get('reason') or confirm_context.get('stop_reason') or readonly_confirm_context.get('stop_reason') or readonly_recheck.get('stop_reason') or active_async_operation.get('stop_reason')
    stop_condition = recover_check.get('stop_condition') or confirm_context.get('stop_condition') or readonly_confirm_context.get('stop_condition') or readonly_recheck.get('stop_condition') or active_async_operation.get('stop_condition')
    recover_state = derive_recover_state(
        freeze=freeze,
        recover_check=recover_check,
        recover_timeline=recover_timeline,
        readonly_recheck=readonly_recheck,
    )
    protective_semantic_detail = _build_protective_semantic_detail(
        stop_condition=stop_condition,
        stop_reason=stop_reason,
        confirm_summary=confirm_summary,
        position=position,
    )
    protection_projection = _resolve_protection_projection(
        stop_condition=stop_condition,
        recover_check=recover_check,
        position=position,
        confirm_summary=confirm_summary,
    )
    recover_policy_display = recover_check.get('recover_policy_display') or protection_projection.get('stop_category') or recover_check.get('recover_policy')
    legacy_recover_policy = recover_check.get('legacy_recover_policy')
    if legacy_recover_policy is None and recover_policy_display != recover_check.get('recover_policy'):
        legacy_recover_policy = recover_check.get('recover_policy')
    next_focus = '先补齐 confirm_summary 与交易所只读事实'
    if recover_state == 'recover_relapse':
        next_focus = '先按 relapse 处理：复核新出现的 openOrders / userTrades / positionRisk，禁止按已恢复口径继续运行'
    elif recheck_status == 'readonly_recheck_recover_ready' or recheck_action == 'recover_ready':
        next_focus = 'readonly_recheck 已补齐 order / userTrades / positionRisk / openOrders，当前进入 recover_ready；这只代表恢复条件具备，guard 不放开真实 submit / resubmit'
    elif recheck_status == 'readonly_recheck_query_failed':
        next_focus = 'readonly_recheck 查询未补齐；当前按 recover_blocked 处理，继续 freeze 并优先补查 order / userTrades / positionRisk / openOrders'
    elif recover_state == 'recover_ready':
        next_focus = '当前处 recover_ready：只代表只读事实已补齐且 guard 可放行恢复；先继续观察 reconcile_status=OK，仍不要真实 submit / resubmit'
    elif recover_state == 'recover_blocked':
        next_focus = '恢复仍被阻塞：当前处 recover_blocked；先消除 pending_execution_phase / consistency mismatch / 查询缺口，再谈 recover'
    elif recheck_status == 'readonly_recheck_pending' or recheck_action == 'observe' or recover_state == 'recover_observe':
        next_focus = 'readonly_recheck 显示仍处 observe：继续只读观察并维持 freeze，等待挂单/部分成交事实收敛，不要改写策略持仓语义'
    if stop_condition == 'manual_position_flat_confirmed':
        next_focus = '检测到 manual flat：交易所已被外部手动平仓，本地不能按普通 mismatch 继续；保持 freeze，先人工确认是否做 flat reset 与状态回收'
    elif stop_condition == 'external_position_override':
        next_focus = '检测到 external override：交易所仓位被外部改仓或替换；保持 freeze，先人工确认来源与保护链，再决定是否重建本地状态'
    elif stop_condition == 'protection_orders_missing':
        next_focus = '仓位已确认但保护单全部缺失；当前转 manual_review，禁止继续运行，优先核对保护链是否被手动撤销或 rebuild 未落地'
    elif stop_condition == 'protection_stop_missing':
        next_focus = '仓位已确认但 stop 缺失；当前转 manual_review，优先补查保护单查询与外部撤单痕迹'
    elif stop_condition == 'protection_tp_missing':
        next_focus = '仓位已确认但 take profit 缺失；当前转 manual_review，先确认这是策略允许的形态还是外部改动'
    elif stop_condition == 'protection_submit_gate_blocked':
        next_focus = 'protective rebuild 已触发但被 submit gate 拦住；当前不能按已恢复处理，先排查 gate blocker 与保护链落地差异'
    elif stop_condition == 'protection_semantic_position_side_mismatch':
        next_focus = '保护单可见，但 position_side 与当前仓位方向不一致；当前转 manual_review，先核对 long/short 方向与 Binance 持仓模式后再谈恢复'
    elif stop_condition == 'protection_semantic_type_mismatch':
        next_focus = '保护单可见，但 protective type 不对；先核对 stop / take profit 应使用的订单类型，再决定是否重建保护单'
    elif stop_condition == 'protection_semantic_stop_payload_mismatch':
        next_focus = '保护单可见，但 stop payload 语义不对；优先核对 closePosition / reduceOnly / side / qty 是否与预期 stop 保护一致'
    elif stop_condition == 'protection_semantic_tp_payload_mismatch':
        next_focus = '保护单可见，但 take profit payload 语义不对；优先核对 closePosition / reduceOnly / side / qty 是否与预期 TP 保护一致'
    elif stop_condition == 'protection_semantic_mismatch':
        next_focus = '保护单可见但语义不对（方向 / type / stop payload / tp payload）；当前转 manual_review，必须先纠正保护语义再谈恢复'
    elif stop_condition == 'relapse_after_recover_ready_mismatch':
        next_focus = 'recover_ready 后再次出现 mismatch；按 relapse 处理，禁止沿用已恢复口径继续运行'
    elif stop_condition == 'relapse_after_recover_ready_protection_missing':
        next_focus = 'recover_ready 后再次出现保护链缺失；按 relapse 处理，优先查保护单是否被外部撤销或 rebuild 失败'
    elif stop_condition == 'relapse_after_recover_ready_query_failed':
        next_focus = 'recover_ready 后再次出现 query_failed；按 relapse 处理，先补齐只读事实，避免误判为稳定恢复'
    elif stop_condition == 'partial_fill_position_working':
        next_focus = '检测到部分成交且仓位/挂单仍在演化；优先核对 userTrades / openOrders / positionRisk，避免把工作中状态误当成最终结果'
    elif stop_condition == 'avg_fill_price_missing_after_fills':
        next_focus = '已有 fills 但 avg_fill_price 缺失；优先补查 userTrades 与成交均价归集，确认是否只是成交明细未补账'
    elif stop_condition == 'trade_rows_missing_after_fill':
        next_focus = '已有成交但 trade rows 缺失；优先补查 userTrades / commission / realized pnl，确认是否只是成交明细延迟'
    elif stop_condition == 'fee_reconciliation_pending':
        next_focus = '成交已确认但 fees 仍未补齐；继续只读补查 userTrades / commission 归集，维持 freeze 直到费用对平'
    elif not readonly_recheck and category == 'confirmed':
        next_focus = '继续观察 reconcile_status / positionRisk；当前不等于已放开真实发单'
    elif not readonly_recheck and category == 'pending':
        next_focus = '优先核对 openOrders + userTrades + positionRisk，确认是否部分成交或残单未清'
    elif not readonly_recheck and category == 'position_confirmed':
        next_focus = '继续观察仓位事实并补齐 userTrades / fees / avg_fill_price；当前可承认仓位已建立，但交易明细仍待补账'
    elif not readonly_recheck and category == 'query_failed':
        next_focus = '优先补查 order / userTrades / positionRisk，事实未补齐前维持 freeze'
    elif not readonly_recheck and category == 'mismatch':
        next_focus = '优先比对 requested_qty / executed_qty / post_position / openOrders'
    elif not readonly_recheck and category == 'rejected':
        next_focus = '确认是否仅拒单，还是同时存在持仓/残单残留'
    elif policy_action == 'auto_repair' and auto_repair_steps and not readonly_recheck:
        next_focus = f"按异常策略做受 guard 保护的自动修复骨架：{' -> '.join(auto_repair_steps)}；完成后必须重新只读对账"
    elif policy_action == 'readonly_recheck' and readonly_checks and not readonly_recheck:
        next_focus = f"先按异常策略补查：{' / '.join(readonly_checks)}；事实补齐前不要推进策略状态"
    elif policy_action == 'retry' and not readonly_recheck:
        next_focus = '按异常策略执行限次退避重试；每次重试前先确认当前对账状态未恶化'
    elif policy_action == 'freeze_and_alert' and not readonly_recheck:
        next_focus = '按异常策略保持 freeze；只允许人工复核权限/资金/参数问题，告警判定对齐 Discord 监控频道口径'
    protection_intent = position.get('strategy_protection_intent') or None
    confirm_phase = confirm_context.get('confirm_phase') or ((readonly_recheck.get('confirm_context') or {}).get('confirm_phase')) or recover_check.get('confirm_phase') or ('readonly_recheck' if readonly_recheck else None)
    recover_policy = recover_check.get('recover_policy')
    if recover_policy is None and readonly_recheck.get('status') == 'readonly_recheck_pending':
        recover_policy = 'observe_only'
    if recover_policy is None and stop_condition in {
        'protection_orders_missing',
        'protection_stop_missing',
        'protection_tp_missing',
        'protection_submit_gate_blocked',
        'protection_semantic_mismatch',
        'protection_semantic_position_side_mismatch',
        'protection_semantic_type_mismatch',
        'protection_semantic_stop_payload_mismatch',
        'protection_semantic_tp_payload_mismatch',
        'manual_position_flat_confirmed',
        'external_position_override',
        'manual_open_orders_side_or_qty_conflict',
        'manual_reduce_only_position_not_flat',
        'relapse_after_recover_ready_mismatch',
        'relapse_after_recover_ready_protection_missing',
        'relapse_after_recover_ready_query_failed',
    }:
        recover_policy = 'manual_review'
    elif recover_policy is None and recover_state == 'recover_observe':
        recover_policy = 'observe_only'
    elif recover_policy is None and recover_state == 'recover_ready':
        recover_policy = 'ready_only'
    elif recover_policy is None and recover_state == 'recover_blocked':
        recover_policy = 'keep_frozen'
    recover_stage = recover_check.get('recover_stage')
    if recover_stage is None and readonly_recheck.get('status') == 'readonly_recheck_pending':
        recover_stage = 'observe_pending'
    if recover_state is None and recover_policy == 'manual_review':
        recover_state = 'recover_blocked'
    guard_decision = recover_check.get('guard_decision') or readonly_recheck.get('guard_decision')
    guard_result = recover_check.get('decision') or recover_check.get('result')
    return {
        'runtime_mode': freeze.get('runtime_mode') or runtime.get('phase'),
        'freeze_status': freeze.get('freeze_status'),
        'async_operation': active_async_operation or None,
        'async_operation_count': async_operation_view.get('active_count') or 0,
        'async_operation_family': active_async_operation.get('family'),
        'async_operation_action_type': active_async_operation.get('action_type'),
        'async_operation_kind': active_async_operation.get('kind'),
        'async_operation_status': active_async_operation.get('status'),
        'async_operation_pending_execution_phase_view': active_async_operation.get('pending_execution_phase_view'),
        'async_operation_stop_reason': active_async_operation.get('stop_reason'),
        'async_operation_stop_condition': active_async_operation.get('stop_condition'),
        'async_operation_decision_ts': active_async_operation.get('decision_ts'),
        'async_operation_started_at': active_async_operation.get('started_at'),
        'async_operation_resolved_at': active_async_operation.get('resolved_at'),
        'async_operation_trigger_phase': active_async_operation.get('trigger_phase'),
        'async_operation_attempt_no': active_async_operation.get('attempt_no'),
        'async_operation_latest_observation': active_async_operation.get('latest_observation'),
        'async_operation_budget': active_async_operation.get('budget'),
        'async_operation_operation_id': active_async_operation.get('operation_id'),
        'recover_state': recover_state,
        'manual_review_required': recover_policy_display == 'manual_review',
        'stop_category': protection_projection.get('stop_category') or recover_check.get('stop_category'),
        'legacy_stop_category': protection_projection.get('legacy_stop_category'),
        'protection_summary_family': protection_projection.get('summary_family'),
        'protection_pending_action': protection_projection.get('pending_action'),
        'protective_validation_level': protection_projection.get('validation_level'),
        'protective_validation_risk_class': protection_projection.get('risk_class'),
        'protective_validation_mismatch_class': protection_projection.get('mismatch_class'),
        'recover_policy': recover_policy_display or recover_policy,
        'legacy_recover_policy': legacy_recover_policy,
        'effective_recover_policy': recover_policy,
        'recover_stage': recover_stage,
        'guard_decision': guard_decision,
        'guard_result': guard_result,
        'confirmation_category': category,
        'confirmed_order_status': confirm_summary.get('confirmed_order_status'),
        'hard_blocker': freeze_reason or freeze.get('pending_execution_block_reason') or blocked_reason,
        'exchange_position_side': position.get('exchange_position_side'),
        'exchange_position_qty': position.get('exchange_position_qty'),
        'protective_order_status': position.get('protective_order_status'),
        'protective_orders_count': len(position.get('exchange_protective_orders') or []),
        'strategy_protection_intent': protection_intent,
        'strategy_protection_state': None if not protection_intent else (protection_intent.get('intent_state') or protection_intent.get('lifecycle_status') or protection_intent.get('intent_status')),
        'execution_retry_backoff': position.get('execution_retry_backoff') or None,
        'submit_exception_policy': exception_policy or None,
        'exception_policy_brief': exception_policy_brief or None,
        'confirm_phase': confirm_phase,
        'confirm_attempted': confirm_context.get('confirm_attempted'),
        'confirm_context': confirm_context or None,
        'stop_reason': stop_reason,
        'stop_condition': stop_condition,
        'retry_budget': retry_budget,
        'readonly_recheck': readonly_recheck or None,
        'protective_semantic_detail': protective_semantic_detail,
        'next_focus': next_focus,
        'operator_open_rule': '先对账，再决策；先执行确认，再写策略状态；默认不放开真实 submit / execution_confirmation 真发送',
    }

def _to_market_snapshot(config: Any, result: dict[str, Any] | None, state: dict[str, Any] | None) -> MarketSnapshot:
    result = result or {}
    state = state or {}
    latest_market_summary = getattr(config, '_runtime_latest_market_summary', None) or {}
    trade_summary = result.get('trade_summary') or {}
    execution_ref = trade_summary.get('execution_ref') or {}
    result_bar_ts = result.get('bar_ts') or execution_ref.get('bar_ts') or execution_ref.get('decision_ts') or result.get('result_ts') or ''
    decision_ts = latest_market_summary.get('decision_ts') or execution_ref.get('decision_ts') or result.get('result_ts') or result_bar_ts
    bar_ts = latest_market_summary.get('bar_ts') or result_bar_ts
    strategy_ts = latest_market_summary.get('strategy_ts') or execution_ref.get('plan_ts') or bar_ts or None
    execution_attributed_bar = latest_market_summary.get('execution_attributed_bar') or execution_ref.get('bar_ts') or bar_ts or None
    return MarketSnapshot(
        decision_ts=decision_ts,
        bar_ts=bar_ts,
        strategy_ts=strategy_ts,
        execution_attributed_bar=execution_attributed_bar,
        symbol=getattr(config, 'symbol', 'UNKNOWN'),
        preclose_offset_seconds=int(latest_market_summary.get('preclose_offset_seconds') or 0),
        current_price=0.0,
        source_status=latest_market_summary.get('source_status') or state.get('source_status') or 'UNKNOWN',
    )


def _to_live_state_snapshot(state: dict[str, Any] | None) -> LiveStateSnapshot:
    state = dict(state or {})
    return LiveStateSnapshot(
        state_ts=state.get('state_ts') or '',
        consistency_status=state.get('consistency_status') or 'UNKNOWN',
        freeze_reason=state.get('freeze_reason'),
        account_equity=float(state.get('account_equity') or 0.0),
        available_margin=float(state.get('available_margin') or 0.0),
        exchange_position_side=state.get('exchange_position_side'),
        exchange_position_qty=float(state.get('exchange_position_qty') or 0.0),
        exchange_entry_price=state.get('exchange_entry_price'),
        active_strategy=state.get('active_strategy') or 'none',
        active_side=state.get('active_side'),
        strategy_entry_time=state.get('strategy_entry_time'),
        strategy_entry_price=state.get('strategy_entry_price'),
        stop_price=state.get('stop_price'),
        risk_fraction=state.get('risk_fraction'),
        tp_price=state.get('tp_price'),
        hold_bars=int(state.get('hold_bars') or 0),
        rev_window=state.get('rev_window'),
        add_on_count=int(state.get('add_on_count') or 0),
        degrade_state=state.get('degrade_state') or 'ATTACK',
        quality_bucket=state.get('quality_bucket') or 'MEDIUM',
        base_quantity=state.get('base_quantity'),
        equity_at_entry=state.get('equity_at_entry'),
        risk_amount=state.get('risk_amount'),
        risk_per_unit=state.get('risk_per_unit'),
        p1_armed=bool(state.get('p1_armed', False)),
        p2_armed=bool(state.get('p2_armed', False)),
        high_water_r=float(state.get('high_water_r') or 0.0),
        last_signal_bar=state.get('last_signal_bar'),
        last_trend_signal_ts=state.get('last_trend_signal_ts'),
        last_conflict_resolution=state.get('last_conflict_resolution'),
        can_open_new_position=bool(state.get('can_open_new_position', True)),
        can_modify_position=bool(state.get('can_modify_position', True)),
        adx_long_threshold=float(state.get('adx_long_threshold') or 20.0),
        adx_short_threshold=float(state.get('adx_short_threshold') or 22.0),
        atr_rank_long_threshold=float(state.get('atr_rank_long_threshold') or 0.45),
        atr_rank_short_threshold=float(state.get('atr_rank_short_threshold') or 0.55),
        adx_trend_cont_long_threshold=float(state.get('adx_trend_cont_long_threshold') or 35.0),
        atr_rank_trend_cont_long_threshold=float(state.get('atr_rank_trend_cont_long_threshold') or 0.6),
        adx_trend_cont_short_threshold=float(state.get('adx_trend_cont_short_threshold') or 28.0),
        atr_rank_trend_cont_short_threshold=float(state.get('atr_rank_trend_cont_short_threshold') or 0.5),
        risk_fraction_medium=float(state.get('risk_fraction_medium') or 0.1),
        risk_fraction_high=float(state.get('risk_fraction_high') or 0.2),
        risk_fraction_extreme=float(state.get('risk_fraction_extreme') or 0.3),
        p1_trigger_r=float(state.get('p1_trigger_r') or 1.0),
        p2_trigger_r=float(state.get('p2_trigger_r') or 2.0),
        profit_defense_start_pct=float(state.get('profit_defense_start_pct') or 0.32),
        profit_defense_giveback_pct=float(state.get('profit_defense_giveback_pct') or 0.33),
        break_even_buffer=float(state.get('break_even_buffer') or 0.002),
        trim_fraction=float(state.get('trim_fraction') or 0.3),
        add_trigger_r_first=float(state.get('add_trigger_r_first') or 1.5),
        add_trigger_r_second=float(state.get('add_trigger_r_second') or 2.5),
        runtime_mode=state.get('runtime_mode') or 'ACTIVE',
        freeze_status=state.get('freeze_status') or 'NONE',
        last_freeze_reason=state.get('last_freeze_reason'),
        last_freeze_at=state.get('last_freeze_at'),
        last_recover_at=state.get('last_recover_at'),
        last_recover_result=state.get('last_recover_result'),
        recover_attempt_count=int(state.get('recover_attempt_count') or 0),
        pending_execution_phase=state.get('pending_execution_phase'),
        position_confirmation_level=state.get('position_confirmation_level') or 'NONE',
        trade_confirmation_level=state.get('trade_confirmation_level') or 'NONE',
        needs_trade_reconciliation=bool(state.get('needs_trade_reconciliation', False)),
        fills_reconciled=bool(state.get('fills_reconciled', False)),
        last_confirmed_order_ids=list(state.get('last_confirmed_order_ids') or []),
        protective_order_status=state.get('protective_order_status') or 'NONE',
        exchange_protective_orders=list(state.get('exchange_protective_orders') or []),
        protective_order_last_sync_ts=state.get('protective_order_last_sync_ts'),
        protective_order_last_sync_action=state.get('protective_order_last_sync_action'),
        protective_order_freeze_reason=state.get('protective_order_freeze_reason'),
        protective_phase_status=state.get('protective_phase_status') or 'NONE',
        strategy_protection_intent=dict(state.get('strategy_protection_intent') or {}),
        execution_retry_backoff=dict(state.get('execution_retry_backoff') or {}),
        pending_execution_block_reason=state.get('pending_execution_block_reason'),
        recover_check=dict(state.get('recover_check') or {}),
        recover_timeline=list(state.get('recover_timeline') or []),
    )


def _to_execution_result(result: dict[str, Any] | None) -> ExecutionResult:
    result = result or {}
    return ExecutionResult(
        result_ts=result.get('result_ts') or '',
        bar_ts=result.get('bar_ts') or '',
        status=result.get('status') or 'UNKNOWN',
        action_type=result.get('action_type') or 'none',
        executed_side=result.get('executed_side'),
        executed_qty=float(result.get('executed_qty') or 0.0),
        avg_fill_price=result.get('avg_fill_price'),
        fees=float(result.get('fees') or 0.0),
        exchange_order_ids=list(result.get('exchange_order_ids') or []),
        post_position_side=result.get('post_position_side'),
        post_position_qty=float(result.get('post_position_qty') or 0.0),
        post_entry_price=result.get('post_entry_price'),
        reconcile_status=result.get('reconcile_status') or 'UNKNOWN',
        error_code=result.get('error_code'),
        error_message=result.get('error_message'),
        should_freeze=bool(result.get('should_freeze', False)),
        freeze_reason=result.get('freeze_reason'),
        state_updates=result.get('state_updates'),
        execution_phase=result.get('execution_phase') or 'none',
        confirmation_status=result.get('confirmation_status') or 'UNSPECIFIED',
        confirmed_order_status=result.get('confirmed_order_status'),
        trade_summary=result.get('trade_summary'),
    )


def build_dispatch_preview(config: Any, result: dict[str, Any] | None, state: dict[str, Any] | None) -> dict[str, Any]:
    publisher = DiscordPublisher(getattr(config, 'discord_execution_channel_id', None) or '1486034825830727710')
    market_obj = _to_market_snapshot(config, result, state)
    state_obj = _to_live_state_snapshot(state)
    result_obj = _to_execution_result(result)
    preview = publisher.build_dispatch_audit(market=market_obj, state=state_obj, result=result_obj)
    preview['send_gate'] = {
        'discord_real_send_enabled': bool(getattr(config, 'discord_real_send_enabled', False)),
        'discord_message_tool_enabled': bool(getattr(config, 'discord_message_tool_enabled', False)),
        'discord_send_require_idempotency': bool(getattr(config, 'discord_send_require_idempotency', True)),
        'discord_send_ledger_path': getattr(config, 'discord_send_ledger_path', None),
        'discord_send_receipt_log_path': getattr(config, 'discord_send_receipt_log_path', None),
        'discord_send_retry_limit': int(getattr(config, 'discord_send_retry_limit', 3) or 3),
        'discord_transport': getattr(config, 'discord_transport', 'unconfigured'),
        'discord_rehearsal_real_send_enabled': bool(getattr(config, 'discord_rehearsal_real_send_enabled', False)),
        'discord_execution_confirmation_real_send_enabled': bool(getattr(config, 'discord_execution_confirmation_real_send_enabled', False)),
    }
    return preview


def _build_runtime_sendability_summary(
    *,
    dispatch_preview: dict[str, Any] | None,
    confirm_summary: dict[str, Any],
    freeze_summary: dict[str, Any],
    env_gate_summary: dict[str, Any],
) -> dict[str, Any]:
    dispatch_preview = dispatch_preview or {}
    primary_preview = dispatch_preview.get('primary_preview') or {}
    primary_kind = dispatch_preview.get('primary_kind') or dispatch_preview.get('kind') or 'not_sendable'
    execution_env = env_gate_summary.get('discord_execution_confirmation_env') or {}
    rehearsal_env = env_gate_summary.get('discord_rehearsal_env') or {}
    runtime_mode = freeze_summary.get('runtime_mode')
    freeze_status = freeze_summary.get('freeze_status')
    freeze_reason = freeze_summary.get('freeze_reason')
    execution_confirmation = {
        'kind': primary_kind if primary_kind == 'execution_confirmation' else 'execution_confirmation',
        'visible_as_primary': primary_kind == 'execution_confirmation',
        'confirmation_status': confirm_summary.get('confirmation_status'),
        'confirmation_category': confirm_summary.get('confirmation_category'),
        'reconcile_status': confirm_summary.get('reconcile_status'),
        'real_send_env_open': bool(execution_env.get('open_by_env')),
        'real_send_ready_by_env': bool(execution_env.get('ready_by_env')),
        'real_send_enabled': execution_env.get('execution_confirmation_real_send_enabled'),
        'current_sendable': primary_kind == 'execution_confirmation' and bool(dispatch_preview.get('eligible')),
        'blocked_reasons': list(primary_preview.get('blocked_reasons') or dispatch_preview.get('blocked_reasons') or []),
        'idempotency_key': primary_preview.get('idempotency_key') or dispatch_preview.get('idempotency_key'),
    }
    risk_alert = {
        'visible_as_primary': primary_kind == 'risk_alert',
        'runtime_mode': runtime_mode,
        'freeze_status': freeze_status,
        'freeze_reason': freeze_reason,
        'current_sendable': primary_kind == 'risk_alert' and bool(dispatch_preview.get('eligible')),
    }
    live_send = {
        'current_primary_kind': primary_kind,
        'current_sendable': bool(dispatch_preview.get('eligible')),
        'current_reason': dispatch_preview.get('reason'),
        'execution_confirmation_real_send_open_by_env': bool(execution_env.get('open_by_env')),
        'execution_confirmation_ready_by_env': bool(execution_env.get('ready_by_env')),
        'rehearsal_real_send_open_by_env': bool(rehearsal_env.get('open_by_env')),
        'rehearsal_ready_by_env': bool(rehearsal_env.get('ready_by_env')),
    }
    return {
        'primary_kind': primary_kind,
        'execution_confirmation': execution_confirmation,
        'risk_alert': risk_alert,
        'live_send': live_send,
        'debug_auxiliary': {
            'has_rehearsal_preview': bool(dispatch_preview.get('rehearsal_preview')),
            'rehearsal_kind': ((dispatch_preview.get('rehearsal_preview') or {}).get('kind')),
            'rehearsal_visible_only_in_auxiliary': True,
            'plan_debug': None,
        },
    }


def _build_env_gate_summary(
    runtime_config_validation: dict[str, Any] | None,
    submit_gate: dict[str, Any] | None,
    discord_send_gate: dict[str, Any] | None,
) -> dict[str, Any]:
    runtime_config_validation = runtime_config_validation or {}
    facts = runtime_config_validation.get('facts') or {}
    gate_status = facts.get('gate_status') or {}
    operator_open_summary = facts.get('operator_open_summary') or {}
    submit_guard = (submit_gate or {}).get('guardrail_checks') or {}
    discord_operator_open_summary = (discord_send_gate or {}).get('operator_open_summary') or {}
    return {
        'mode': runtime_config_validation.get('mode'),
        'severity': runtime_config_validation.get('severity'),
        'binance_submit_env': {
            'open_by_env': bool(gate_status.get('binance_submit_env_open', False)),
            'ready_by_env': bool(gate_status.get('binance_submit_ready', False)),
            'dry_run': facts.get('dry_run'),
            'submit_http_post_enabled': bool(gate_status.get('binance_submit_http_post_enabled', False)),
            'unlock_token_present': bool(gate_status.get('binance_submit_unlock_token_present', False)),
            'unlock_token_valid': bool(gate_status.get('binance_submit_unlock_token_valid', False)),
            'manual_ack_present': bool(gate_status.get('binance_submit_manual_ack_present', False)),
            'manual_ack_valid': bool(gate_status.get('binance_submit_manual_ack_valid', False)),
            'operator_open_summary': operator_open_summary.get('binance_submit'),
            'submit_allowed_now': (submit_gate or {}).get('submit_allowed'),
            'blocked_reason': (submit_gate or {}).get('blocked_reason'),
            'guardrail_blockers': (submit_gate or {}).get('guardrail_blockers') or [],
            'symbol_allowed': submit_guard.get('symbol_allowed'),
            'consistency_status': submit_guard.get('consistency_status'),
            'pending_execution_phase': submit_guard.get('pending_execution_phase'),
        },
        'discord_rehearsal_env': {
            'open_by_env': bool(gate_status.get('discord_rehearsal_real_send_env_open', False)),
            'ready_by_env': bool(gate_status.get('discord_rehearsal_ready', False)),
            'operator_open_summary': operator_open_summary.get('discord_rehearsal_send') or discord_operator_open_summary.get('rehearsal_real_send'),
            'real_send_enabled': (discord_send_gate or {}).get('real_send_enabled') if (discord_send_gate or {}).get('real_send_enabled') is not None else bool(gate_status.get('discord_real_send_env_open', False)),
            'message_tool_enabled': (discord_send_gate or {}).get('message_tool_enabled') if (discord_send_gate or {}).get('message_tool_enabled') is not None else bool(gate_status.get('discord_message_tool_ready', False)),
            'transport_ready': (discord_send_gate or {}).get('transport_ready') if (discord_send_gate or {}).get('transport_ready') is not None else bool(gate_status.get('discord_transport_ready', False)),
            'blocked': (discord_send_gate or {}).get('blocked'),
            'blockers': (discord_send_gate or {}).get('blockers') or [],
        },
        'discord_execution_confirmation_env': {
            'open_by_env': bool(gate_status.get('discord_execution_confirmation_real_send_env_open', False)),
            'ready_by_env': bool(gate_status.get('discord_execution_confirmation_ready', False)),
            'operator_open_summary': operator_open_summary.get('discord_execution_confirmation_send') or discord_operator_open_summary.get('execution_confirmation_real_send'),
            'execution_confirmation_real_send_enabled': (discord_send_gate or {}).get('execution_confirmation_real_send_enabled') if (discord_send_gate or {}).get('execution_confirmation_real_send_enabled') is not None else bool(gate_status.get('discord_execution_confirmation_real_send_env_open', False)),
            'blocked': (discord_send_gate or {}).get('blocked'),
            'blockers': (discord_send_gate or {}).get('blockers') or [],
            'default_safety': 'closed',
        },
    }


def _normalize_strategy_protection_intent(intent: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(intent or {})
    if not payload:
        return {}
    state = payload.get('intent_state') or payload.get('lifecycle_status')
    if state is None and payload.get('intent_status') is not None:
        state = str(payload.get('intent_status')).lower()
    if state is not None:
        payload.setdefault('intent_state', state)
        payload.setdefault('lifecycle_status', state)
        payload.setdefault('intent_status', str(state).upper())
    return payload


def _summarize_async_operation(async_operation: dict[str, Any] | None) -> dict[str, Any] | None:
    payload = dict(async_operation or {})
    if not payload:
        return None
    stop_condition_payload = payload.get('stop_condition') or {}
    latest_observation = payload.get('latest_observation') or {}
    budget = payload.get('budget') or {}
    return {
        'operation_id': payload.get('operation_id'),
        'family': payload.get('family'),
        'action_type': payload.get('action_type') or payload.get('kind'),
        'kind': payload.get('kind') or payload.get('action_type'),
        'status': payload.get('status'),
        'decision_ts': payload.get('decision_ts'),
        'started_at': payload.get('started_at'),
        'resolved_at': payload.get('resolved_at'),
        'trigger_phase': payload.get('trigger_phase'),
        'attempt_no': payload.get('attempt_no'),
        'pending_execution_phase_view': payload.get('pending_execution_phase_view') or latest_observation.get('pending_execution_phase'),
        'stop_reason': stop_condition_payload.get('current_reason') or latest_observation.get('stop_reason'),
        'stop_condition': stop_condition_payload.get('current_condition') or latest_observation.get('stop_condition'),
        'is_primary': bool(payload.get('is_primary')),
        'drives_summary': bool(payload.get('drives_summary')),
        'superseded_by_operation_id': payload.get('superseded_by_operation_id'),
        'latest_observation': latest_observation or None,
        'budget': {
            'attempts_used': budget.get('attempts_used'),
            'attempts_remaining': budget.get('attempts_remaining'),
            'max_attempts': budget.get('max_attempts'),
            'retry_interval_seconds': budget.get('retry_interval_seconds'),
            'window_started_at': budget.get('window_started_at'),
            'budget_window_bar_count': budget.get('budget_window_bar_count'),
            'budget_window_start_ts': budget.get('budget_window_start_ts'),
            'budget_window_end_ts': budget.get('budget_window_end_ts'),
        },
    }


def _build_async_operation_view(
    *,
    state: dict[str, Any] | None,
    latest_result_summary: dict[str, Any] | None,
    confirm_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    try:
        from .async_operation import summarize_async_operations
    except ImportError:  # pragma: no cover
        from async_operation import summarize_async_operations

    state = dict(state or {})
    latest_result_summary = dict(latest_result_summary or {})
    confirm_summary = dict(confirm_summary or {})
    state_async_operations = dict(state.get('async_operations') or {})
    arbitration = summarize_async_operations(state_async_operations)
    active_operations = [dict(item) for item in (arbitration.get('active') or []) if isinstance(item, dict)]
    latest_trade_summary = dict((latest_result_summary.get('trade_summary') or {}))
    latest_async_operation = (
        _summarize_async_operation(confirm_summary.get('async_operation'))
        or _summarize_async_operation(latest_trade_summary.get('async_operation'))
        or _summarize_async_operation(arbitration.get('primary'))
    )
    return {
        'has_active': bool(active_operations),
        'active_count': len(active_operations),
        'active': [_summarize_async_operation(item) for item in active_operations],
        'latest': latest_async_operation,
    }


def _summarize_protective_order_payload(order: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(order, dict) or not order:
        return None
    return {
        'kind': order.get('kind'),
        'type': order.get('type') or order.get('orig_type') or order.get('origType'),
        'side': order.get('side'),
        'position_side': order.get('position_side') or order.get('positionSide'),
        'close_position': order.get('close_position'),
        'reduce_only': order.get('reduce_only'),
        'qty': order.get('qty'),
        'stop_price': order.get('stop_price') or order.get('stopPrice') or order.get('trigger_price') or order.get('triggerPrice'),
        'price': order.get('price'),
        'order_id': order.get('order_id') or order.get('orderId'),
        'client_order_id': order.get('client_order_id') or order.get('clientOrderId'),
        'status': order.get('status'),
    }


def _build_protective_semantic_detail(
    *,
    stop_condition: str | None,
    stop_reason: str | None,
    confirm_summary: dict[str, Any],
    position: dict[str, Any],
) -> dict[str, Any] | None:
    semantic_conditions = {
        'protection_semantic_mismatch',
        'protection_semantic_position_side_mismatch',
        'protection_semantic_type_mismatch',
        'protection_semantic_stop_payload_mismatch',
        'protection_semantic_tp_payload_mismatch',
    }
    semantic_reason = stop_condition or stop_reason
    if semantic_reason not in semantic_conditions:
        return None

    trade_summary = ((confirm_summary or {}).get('protective_validation') or {}).get('summary') or {}
    trade_orders = list((confirm_summary or {}).get('protective_orders') or [])
    exchange_orders = list((position or {}).get('exchange_protective_orders') or [])
    orders = trade_orders or exchange_orders
    stop_order = next((item for item in orders if str(item.get('kind') or '') == 'hard_stop'), None)
    tp_order = next((item for item in orders if str(item.get('kind') or '') == 'take_profit'), None)

    expected_position_side = str(trade_summary.get('expected_position_side') or '').lower() or None
    expected_stop_type = trade_summary.get('expected_stop_order_type') or trade_summary.get('expected_stop_type')
    expected_tp_type = trade_summary.get('expected_take_profit_order_type') or trade_summary.get('expected_tp_order_type')
    expected_close_position = trade_summary.get('expected_close_position')
    if expected_close_position is None:
        expected_close_position = trade_summary.get('expected_close_position_protection')
    expected_reduce_only = trade_summary.get('expected_reduce_only')
    if expected_reduce_only is None:
        expected_reduce_only = bool(expected_close_position)

    sub_type = semantic_reason
    if semantic_reason == 'protection_semantic_mismatch':
        if stop_condition in semantic_conditions and stop_condition != 'protection_semantic_mismatch':
            sub_type = stop_condition
        elif stop_reason in semantic_conditions and stop_reason != 'protection_semantic_mismatch':
            sub_type = stop_reason

    return {
        'semantic_reason': semantic_reason,
        'semantic_subtype': sub_type,
        'expected': {
            'position_side': expected_position_side,
            'stop_type': expected_stop_type,
            'tp_type': expected_tp_type,
            'close_position': expected_close_position,
            'reduce_only': expected_reduce_only,
            'tp_required': trade_summary.get('tp_required'),
        },
        'actual': {
            'exchange_position_side': position.get('exchange_position_side'),
            'stop_payload': _summarize_protective_order_payload(stop_order),
            'tp_payload': _summarize_protective_order_payload(tp_order),
            'protective_orders_count': len(orders),
        },
    }


def build_runtime_status_summary(
    *,
    runtime_status_path: str | Path,
    event_log_path: str | Path | None = None,
    state_path: str | Path | None = None,
    tail: int = 5,
) -> dict[str, Any]:
    runtime_status = _load_json(Path(runtime_status_path))
    resolved_event_log_path = Path(event_log_path) if event_log_path else None
    resolved_state_path = Path(state_path) if state_path else None

    if resolved_event_log_path is None and runtime_status.get('event_log_path'):
        resolved_event_log_path = Path(runtime_status['event_log_path'])
    if resolved_state_path is None:
        runtime_dir = Path(runtime_status_path).parent
        candidate = runtime_dir / 'state.json'
        if candidate.exists():
            resolved_state_path = candidate

    state_payload = _safe_load_json(resolved_state_path) if resolved_state_path else {}
    state = state_payload.get('state', {})
    last_result = state_payload.get('last_result') or {}
    recent_events = _safe_tail_jsonl(resolved_event_log_path, limit=tail) if resolved_event_log_path else []
    last_freeze = _find_last_event(recent_events, 'freeze')
    last_execution = _find_last_event(recent_events, 'execute_summary')
    last_recover = _find_last_event(recent_events, 'recover_result')
    last_confirm = _find_last_event(recent_events, 'confirm_summary')
    last_dispatch_preview = _find_last_event(recent_events, 'sender_dispatch_preview')
    last_discord_receipt_event = _find_last_event(recent_events, 'discord_send_receipt')

    discord_send_gate = runtime_status.get('discord_send_gate') or {}
    latest_result_summary = runtime_status.get('latest_result_summary') or {}
    last_discord_send_attempt = runtime_status.get('last_discord_send_attempt') or {}
    latest_discord_receipt = runtime_status.get('latest_discord_receipt')
    latest_discord_receipt_summary = runtime_status.get('latest_discord_receipt_summary') or last_discord_receipt_event
    receipt_store_path = (
        last_discord_send_attempt.get('receipt_store_path')
        or (latest_discord_receipt_summary or {}).get('receipt_store_path')
        or discord_send_gate.get('discord_send_receipt_log_path')
    )
    ledger_path = (
        discord_send_gate.get('discord_send_ledger_path')
        or (last_discord_send_attempt.get('send_gate') or {}).get('ledger_path')
        or (last_discord_send_attempt.get('receipt') or {}).get('ledger_path')
    )
    runtime_dir = Path(runtime_status_path).parent
    resolved_receipt_store_path = _resolve_optional_path(receipt_store_path, base_dir=runtime_dir)
    resolved_ledger_path = _resolve_optional_path(ledger_path, base_dir=runtime_dir)
    receipt_rows = _safe_tail_jsonl(resolved_receipt_store_path, limit=max(tail, 20)) if resolved_receipt_store_path else []
    ledger_payload = _safe_load_json(resolved_ledger_path) if resolved_ledger_path else {}
    if ledger_payload:
        ledger_payload['_path'] = None if resolved_ledger_path is None else str(resolved_ledger_path)
    ledger_summary = _build_ledger_summary(ledger_payload) if ledger_payload else _build_ledger_summary({'entries': {}, '_path': None if resolved_ledger_path is None else str(resolved_ledger_path)})
    receipt_log_summary = _build_receipt_log_summary(receipt_rows, receipt_store_path=None if resolved_receipt_store_path is None else str(resolved_receipt_store_path))

    submit_gate = last_result.get('trade_summary', {}).get('submit_gate') or latest_result_summary.get('submit_gate')
    dispatch_preview = latest_result_summary.get('sender_dispatch_preview')
    if dispatch_preview is None and last_dispatch_preview is not None:
        dispatch_preview = {
            'eligible': last_dispatch_preview.get('eligible'),
            'channel': last_dispatch_preview.get('channel'),
            'target': last_dispatch_preview.get('target'),
            'sent': last_dispatch_preview.get('sent'),
            'reason': last_dispatch_preview.get('reason'),
            'kind': last_dispatch_preview.get('kind'),
        }

    runtime_summary = {
        'phase': runtime_status.get('phase'),
        'symbol': runtime_status.get('symbol'),
        'dry_run': runtime_status.get('dry_run'),
        'submit_enabled': runtime_status.get('submit_enabled'),
        'consecutive_failures': runtime_status.get('consecutive_failures'),
        'backoff_seconds': runtime_status.get('backoff_seconds'),
        'last_started_at': runtime_status.get('last_started_at'),
        'last_completed_at': runtime_status.get('last_completed_at'),
    }
    latest_execution_phase = latest_result_summary.get('execution_phase')
    if latest_execution_phase == 'confirmed' and state.get('pending_execution_phase') in {None, '', 'none'}:
        latest_execution_phase = None
    if _is_cleanup_audit_execution_phase(latest_execution_phase):
        latest_execution_phase = None
    freeze_summary = {
        'runtime_mode': state.get('runtime_mode') or latest_result_summary.get('runtime_mode'),
        'freeze_status': state.get('freeze_status') or latest_result_summary.get('freeze_status'),
        'freeze_reason': state.get('freeze_reason') or latest_result_summary.get('freeze_reason'),
        'last_freeze_reason': state.get('last_freeze_reason'),
        'last_recover_result': state.get('last_recover_result'),
        'recover_attempt_count': state.get('recover_attempt_count'),
        'pending_execution_phase': state.get('pending_execution_phase') or latest_execution_phase,
        'pending_execution_block_reason': state.get('pending_execution_block_reason'),
        'position_confirmation_level': state.get('position_confirmation_level'),
        'trade_confirmation_level': state.get('trade_confirmation_level'),
        'needs_trade_reconciliation': state.get('needs_trade_reconciliation'),
        'fills_reconciled': state.get('fills_reconciled'),
        'cleanup_audit_execution_phase': latest_result_summary.get('execution_phase') if _is_cleanup_audit_execution_phase(latest_result_summary.get('execution_phase')) else None,
    }
    confirm_summary = build_execution_confirm_summary(last_result or latest_result_summary.get('confirm_summary'))
    if _is_terminal_flat_trade_reconciliation_pending(
        state=state,
        latest_result_summary=latest_result_summary,
        confirm_summary=confirm_summary,
    ):
        latest_execution_phase = None
        freeze_summary['pending_execution_phase'] = None
    position_summary = {
        'exchange_position_side': state.get('exchange_position_side'),
        'exchange_position_qty': state.get('exchange_position_qty'),
        'exchange_entry_price': state.get('exchange_entry_price'),
        'active_strategy': state.get('active_strategy'),
        'active_side': state.get('active_side'),
        'base_quantity': state.get('base_quantity'),
        'local_stop_price': state.get('stop_price'),
        'local_tp_price': state.get('tp_price'),
        'protective_order_status': state.get('protective_order_status'),
        'exchange_protective_orders': state.get('exchange_protective_orders') or [],
        'protective_order_last_sync_ts': state.get('protective_order_last_sync_ts'),
        'protective_order_last_sync_action': state.get('protective_order_last_sync_action'),
        'protective_order_freeze_reason': state.get('protective_order_freeze_reason'),
        'strategy_protection_intent': _normalize_strategy_protection_intent(state.get('strategy_protection_intent')),
        'execution_retry_backoff': state.get('execution_retry_backoff') or {},
    }

    recover_check = _sanitize_negative_management_recover_record(
        runtime_status.get('recover_check') or state.get('recover_check') or state_payload.get('recover_check')
    )
    recover_timeline = _sanitize_negative_management_recover_timeline(
        runtime_status.get('recover_timeline') or state.get('recover_timeline') or state_payload.get('recover_timeline') or []
    )
    recover_check = _select_effective_recover_check(recover_check, recover_timeline)

    runtime_config_validation = runtime_status.get('runtime_config_validation')
    async_operation_view = _build_async_operation_view(
        state=state,
        latest_result_summary=latest_result_summary,
        confirm_summary=confirm_summary,
    )
    effective_discord_send_gate = discord_send_gate or (dispatch_preview or {}).get('send_gate')
    env_gate_summary = _build_env_gate_summary(
        runtime_config_validation=runtime_config_validation,
        submit_gate=submit_gate,
        discord_send_gate=effective_discord_send_gate,
    )
    runtime_sendability_summary = _build_runtime_sendability_summary(
        dispatch_preview=dispatch_preview,
        confirm_summary=confirm_summary,
        freeze_summary=freeze_summary,
        env_gate_summary=env_gate_summary,
    )
    offline_live_misalignment = _build_offline_live_misalignment_summary(
        latest_result_summary=latest_result_summary,
        submit_gate=submit_gate,
        freeze_summary=freeze_summary,
    )
    plan_debug = latest_result_summary.get('plan_debug') or (last_execution or {}).get('plan_debug') or (last_confirm or {}).get('plan_debug')
    runtime_sendability_summary['debug_auxiliary']['plan_debug'] = plan_debug

    return {
        'runtime': runtime_summary,
        'runtime_config_validation': runtime_config_validation,
        'env_gate_summary': env_gate_summary,
        'audit_artifact_paths': runtime_status.get('audit_artifact_paths') or latest_result_summary.get('audit_artifact_paths') or {},
        'discord_send_gate': effective_discord_send_gate,
        'last_discord_send_attempt': last_discord_send_attempt,
        'latest_discord_receipt': latest_discord_receipt,
        'latest_discord_receipt_summary': latest_discord_receipt_summary,
        'ledger_summary': ledger_summary,
        'receipt_log_summary': receipt_log_summary,
        'operator_summary': {
            'primary_runtime_view': runtime_sendability_summary,
            'offline_live_misalignment': offline_live_misalignment,
            'latest_receipt': receipt_log_summary.get('recent_receipt_summary') or _summarize_receipt(latest_discord_receipt),
            'latest_sent_receipt': receipt_log_summary.get('recent_sent_receipt_summary'),
            'latest_blocked_receipt': receipt_log_summary.get('recent_blocked_receipt_summary'),
            'latest_duplicate_receipt': receipt_log_summary.get('recent_duplicate_receipt_summary'),
            'latest_ledger_key': ledger_summary.get('latest_key'),
            'ledger_dedup_status': {
                'entries_count': ledger_summary.get('entries_count'),
                'sent_count': ledger_summary.get('sent_count'),
                'blocked_count': ledger_summary.get('blocked_count'),
                'duplicate_like_count': ledger_summary.get('duplicate_like_count'),
                'inflight_count': ledger_summary.get('inflight_count'),
            },
            'receipt_status_counts': receipt_log_summary.get('status_counts'),
            'submit_exception_policy': confirm_summary.get('submit_exception_policy'),
            'exception_policy_brief': confirm_summary.get('exception_policy_brief'),
            'async_operation': async_operation_view.get('latest'),
            'async_operations_active': async_operation_view.get('active'),
            'debug_auxiliary': runtime_sendability_summary.get('debug_auxiliary'),
        },
        'submit_gate': submit_gate,
        'freeze': freeze_summary,
        'latest_run': {
            'decision_ts': runtime_status.get('latest_market_summary', {}).get('decision_ts'),
            'bar_ts': runtime_status.get('latest_market_summary', {}).get('bar_ts'),
            'strategy_ts': runtime_status.get('latest_market_summary', {}).get('strategy_ts'),
            'consistency_status': latest_result_summary.get('consistency_status'),
            'plan_action': latest_result_summary.get('plan_action'),
            'plan_reason': latest_result_summary.get('plan_reason'),
            'plan_debug': latest_result_summary.get('plan_debug'),
            'result_status': latest_result_summary.get('result_status'),
            'confirmation_status': latest_result_summary.get('confirmation_status'),
            'execution_phase': latest_execution_phase,
        },
        'position': position_summary,
        'async_operations': async_operation_view,
        'operator_compact_view': {
            **_build_operator_compact_view(
                runtime=runtime_summary,
                submit_gate=submit_gate,
                freeze=freeze_summary,
                confirm_summary=confirm_summary,
                position=position_summary,
                async_operation_view=async_operation_view,
                recover_check=recover_check,
                recover_timeline=recover_timeline,
            ),
            'offline_live_misalignment_detected': offline_live_misalignment.get('detected'),
            'offline_live_misalignment_verdict': offline_live_misalignment.get('runtime_compatible_verdict'),
            'offline_live_misalignment_reason': offline_live_misalignment.get('reason_code'),
            'primary_dispatch_kind': runtime_sendability_summary.get('primary_kind'),
            'execution_sendable_now': runtime_sendability_summary.get('execution_confirmation', {}).get('current_sendable'),
            'risk_alert_sendable_now': runtime_sendability_summary.get('risk_alert', {}).get('current_sendable'),
            'live_sendable_now': runtime_sendability_summary.get('live_send', {}).get('current_sendable'),
            'debug_auxiliary': runtime_sendability_summary.get('debug_auxiliary'),
        },
        'latest_execution': {
            'status': last_result.get('status'),
            'action_type': last_result.get('action_type'),
            'executed_side': last_result.get('executed_side'),
            'executed_qty': last_result.get('executed_qty'),
            'avg_fill_price': last_result.get('avg_fill_price'),
            'reconcile_status': last_result.get('reconcile_status'),
            'confirmation_status': last_result.get('confirmation_status'),
            'confirmed_order_status': last_result.get('confirmed_order_status'),
            'exchange_order_ids': last_result.get('exchange_order_ids'),
            'freeze_reason': last_result.get('freeze_reason'),
            'trade_summary': last_result.get('trade_summary'),
        },
        'confirm_summary': confirm_summary,
        'offline_live_misalignment': offline_live_misalignment,
        'runtime_sendability_summary': runtime_sendability_summary,
        'recover_check': recover_check,
        'recover_timeline': recover_timeline,
        'recover_summary': {
            'recover_state': derive_recover_state(
                freeze=freeze_summary,
                recover_check=recover_check,
                recover_timeline=recover_timeline,
                readonly_recheck=confirm_summary.get('readonly_recheck'),
            ),
            'guard_decision': (recover_check or {}).get('guard_decision') or (confirm_summary.get('readonly_recheck') or {}).get('guard_decision'),
            'guard_result': (recover_check or {}).get('decision') or (recover_check or {}).get('result'),
            'recover_policy': (recover_check or {}).get('recover_policy_display')
            or _resolve_protection_projection(
                stop_condition=(
                    (confirm_summary.get('confirm_context') or {}).get('stop_condition')
                    or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_condition')
                    or (recover_check or {}).get('stop_condition')
                    or ('await_more_exchange_facts' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None)
                ),
                recover_check=recover_check,
                position=position_summary,
                confirm_summary=confirm_summary,
            ).get('stop_category')
            or (recover_check or {}).get('recover_policy')
            or ('observe_only' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None),
            'legacy_recover_policy': (recover_check or {}).get('legacy_recover_policy')
            or (
                (recover_check or {}).get('recover_policy')
                if (
                    ((recover_check or {}).get('recover_policy_display') and (recover_check or {}).get('recover_policy_display') != (recover_check or {}).get('recover_policy'))
                    or (
                        _resolve_protection_projection(
                            stop_condition=(
                                (confirm_summary.get('confirm_context') or {}).get('stop_condition')
                                or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_condition')
                                or (recover_check or {}).get('stop_condition')
                                or ('await_more_exchange_facts' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None)
                            ),
                            recover_check=recover_check,
                            position=position_summary,
                            confirm_summary=confirm_summary,
                        ).get('stop_category')
                        and _resolve_protection_projection(
                            stop_condition=(
                                (confirm_summary.get('confirm_context') or {}).get('stop_condition')
                                or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_condition')
                                or (recover_check or {}).get('stop_condition')
                                or ('await_more_exchange_facts' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None)
                            ),
                            recover_check=recover_check,
                            position=position_summary,
                            confirm_summary=confirm_summary,
                        ).get('stop_category')
                        != (recover_check or {}).get('recover_policy')
                    )
                )
                else None
            ),
            'effective_recover_policy': (recover_check or {}).get('recover_policy')
            or ('observe_only' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None),
            'recover_stage': (recover_check or {}).get('recover_stage')
            or ('observe_pending' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None),
            'stop_category': _resolve_protection_projection(
                stop_condition=(
                    (confirm_summary.get('confirm_context') or {}).get('stop_condition')
                    or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_condition')
                    or (recover_check or {}).get('stop_condition')
                    or ('await_more_exchange_facts' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None)
                ),
                recover_check=recover_check,
                position=position_summary,
                confirm_summary=confirm_summary,
            ).get('stop_category') or (recover_check or {}).get('stop_category'),
            'legacy_stop_category': (recover_check or {}).get('stop_category'),
            'protection_summary_family': _resolve_protection_projection(
                stop_condition=(
                    (confirm_summary.get('confirm_context') or {}).get('stop_condition')
                    or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_condition')
                    or (recover_check or {}).get('stop_condition')
                    or ('await_more_exchange_facts' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None)
                ),
                recover_check=recover_check,
                position=position_summary,
                confirm_summary=confirm_summary,
            ).get('summary_family'),
            'protection_pending_action': _resolve_protection_projection(
                stop_condition=(
                    (confirm_summary.get('confirm_context') or {}).get('stop_condition')
                    or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_condition')
                    or (recover_check or {}).get('stop_condition')
                    or ('await_more_exchange_facts' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None)
                ),
                recover_check=recover_check,
                position=position_summary,
                confirm_summary=confirm_summary,
            ).get('pending_action'),
            'stop_reason': (
                (recover_check or {}).get('stop_reason')
                or (recover_check or {}).get('reason')
                or (confirm_summary.get('confirm_context') or {}).get('stop_reason')
                or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_reason')
            ),
            'stop_condition': (
                (recover_check or {}).get('stop_condition')
                or (confirm_summary.get('confirm_context') or {}).get('stop_condition')
                or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('stop_condition')
                or ('await_more_exchange_facts' if (confirm_summary.get('readonly_recheck') or {}).get('status') == 'readonly_recheck_pending' else None)
            ),
            'confirm_phase': (
                (confirm_summary.get('confirm_context') or {}).get('confirm_phase')
                or ((confirm_summary.get('readonly_recheck') or {}).get('confirm_context') or {}).get('confirm_phase')
                or (recover_check or {}).get('confirm_phase')
                or ('readonly_recheck' if (confirm_summary.get('readonly_recheck') or {}) else None)
            ),
            'retry_budget': _build_retry_budget_summary(
                confirm_summary.get('confirm_context'),
                confirm_summary.get('readonly_recheck'),
            ),
            'recover_attempt_count': state.get('recover_attempt_count'),
            'pending_execution_phase': freeze_summary.get('pending_execution_phase'),
            'position_confirmation_level': freeze_summary.get('position_confirmation_level'),
            'trade_confirmation_level': freeze_summary.get('trade_confirmation_level'),
            'needs_trade_reconciliation': freeze_summary.get('needs_trade_reconciliation'),
            'fills_reconciled': freeze_summary.get('fills_reconciled'),
        },
        'sender_dispatch_preview': dispatch_preview,
        'dispatch_preview_audit_path': latest_result_summary.get('dispatch_preview_audit_path'),
        'recent_summary': {
            'last_freeze_event': last_freeze,
            'last_execution_event': last_execution,
            'last_recover_event': last_recover,
            'last_confirm_event': last_confirm,
            'last_sender_dispatch_preview': last_dispatch_preview,
            'last_discord_receipt_event': last_discord_receipt_event,
        },
        'paths': {
            'runtime_status_path': str(Path(runtime_status_path)),
            'event_log_path': None if resolved_event_log_path is None else str(resolved_event_log_path),
            'state_path': None if resolved_state_path is None else str(resolved_state_path),
        },
        'recent_events': recent_events,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='只读查看 runtime_status / event_log / state 摘要')
    parser.add_argument('--runtime-status', default='runtime/runtime_status.json', help='runtime_status.json 路径')
    parser.add_argument('--event-log', default=None, help='event_log.jsonl 路径；默认从 runtime_status.json 推导')
    parser.add_argument('--state', default=None, help='state.json 路径；默认尝试同目录推导')
    parser.add_argument('--tail', type=int, default=5, help='最近事件条数')
    parser.add_argument('--pretty', action='store_true', help='格式化输出 JSON')
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = build_runtime_status_summary(
        runtime_status_path=args.runtime_status,
        event_log_path=args.event_log,
        state_path=args.state,
        tail=max(1, int(args.tail)),
    )
    if args.pretty:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
