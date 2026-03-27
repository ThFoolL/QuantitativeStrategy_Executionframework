from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from .discord_publisher import DiscordPublisher
    from .models import ExecutionResult, LiveStateSnapshot, MarketSnapshot
except ImportError:  # pragma: no cover
    from discord_publisher import DiscordPublisher
    from models import ExecutionResult, LiveStateSnapshot, MarketSnapshot


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



def build_execution_confirm_summary(result: dict[str, Any] | None) -> dict[str, Any]:
    result = result or {}
    trade_summary = result.get('trade_summary') or {}
    fills = trade_summary.get('fills') or []
    pnl_values = [item.get('realized_pnl') for item in fills if item.get('realized_pnl') is not None]
    return {
        'confirmation_status': result.get('confirmation_status'),
        'confirmation_category': trade_summary.get('confirmation_category'),
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
        'submit_exception_policy': _extract_exception_policy(result),
        'exception_policy_brief': _build_exception_policy_brief(_extract_exception_policy(result)),
        'submit_exception_metadata': trade_summary.get('submit_exception_metadata'),
    }


def _derive_recover_state(
    *,
    freeze: dict[str, Any],
    recover_check: dict[str, Any] | None,
    recover_timeline: list[dict[str, Any]] | None,
) -> str | None:
    timeline = list(recover_timeline or [])
    timeline_results = [str(item.get('result') or '').upper() for item in timeline]
    if 'RECOVERED' in timeline_results and timeline_results and timeline_results[-1] == 'BLOCKED':
        return 'recover_relapse'
    if freeze.get('freeze_status') == 'NONE' and freeze.get('last_recover_result') == 'RECOVERED':
        return 'recover_ready'
    if (recover_check or {}).get('allowed') is False or freeze.get('last_recover_result') == 'BLOCKED':
        return 'recover_blocked'
    return None


def _build_operator_compact_view(
    *,
    runtime: dict[str, Any],
    submit_gate: dict[str, Any] | None,
    freeze: dict[str, Any],
    confirm_summary: dict[str, Any],
    position: dict[str, Any],
    recover_check: dict[str, Any] | None,
    recover_timeline: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    category = confirm_summary.get('confirmation_category')
    freeze_reason = freeze.get('freeze_reason') or confirm_summary.get('freeze_reason')
    blocked_reason = (submit_gate or {}).get('blocked_reason')
    exception_policy = confirm_summary.get('submit_exception_policy') or {}
    exception_policy_brief = confirm_summary.get('exception_policy_brief') or _build_exception_policy_brief(exception_policy) or {}
    policy_action = exception_policy_brief.get('action') or exception_policy.get('action')
    readonly_checks = exception_policy.get('readonly_checks') or []
    auto_repair_steps = exception_policy.get('auto_repair_steps') or []
    recover_state = _derive_recover_state(
        freeze=freeze,
        recover_check=recover_check,
        recover_timeline=recover_timeline,
    )
    if recover_state == 'recover_relapse':
        next_focus = '先按 relapse 处理：复核新出现的 openOrders / userTrades / positionRisk，禁止按已恢复口径继续运行'
    elif recover_state == 'recover_ready':
        next_focus = '保持只读观察；确认 reconcile_status=OK 且未重新出现 openOrders / side mismatch'
    elif recover_state == 'recover_blocked':
        next_focus = '恢复仍被阻塞；先消除 pending_execution_phase / consistency mismatch，再谈 recover'
    elif category == 'confirmed':
        next_focus = '继续观察 reconcile_status / positionRisk；当前不等于已放开真实发单'
    elif category == 'pending':
        next_focus = '优先核对 openOrders + userTrades + positionRisk，确认是否部分成交或残单未清'
    elif category == 'query_failed':
        next_focus = '优先补查 order / userTrades / positionRisk，事实未补齐前维持 freeze'
    elif category == 'mismatch':
        next_focus = '优先比对 requested_qty / executed_qty / post_position / openOrders'
    elif category == 'rejected':
        next_focus = '确认是否仅拒单，还是同时存在持仓/残单残留'
    elif policy_action == 'auto_repair' and auto_repair_steps:
        next_focus = f"按异常策略做受 guard 保护的自动修复骨架：{' -> '.join(auto_repair_steps)}；完成后必须重新只读对账"
    elif policy_action == 'readonly_recheck' and readonly_checks:
        next_focus = f"先按异常策略补查：{' / '.join(readonly_checks)}；事实补齐前不要推进策略状态"
    elif policy_action == 'retry':
        next_focus = '按异常策略执行限次退避重试；每次重试前先确认当前对账状态未恶化'
    elif policy_action == 'freeze_and_alert':
        next_focus = '按异常策略保持 freeze；只允许人工复核权限/资金/参数问题，告警判定对齐 Discord 监控频道口径'
    else:
        next_focus = '先补齐 confirm_summary 与交易所只读事实'
    return {
        'runtime_mode': freeze.get('runtime_mode') or runtime.get('phase'),
        'freeze_status': freeze.get('freeze_status'),
        'recover_state': recover_state,
        'confirmation_category': category,
        'confirmed_order_status': confirm_summary.get('confirmed_order_status'),
        'hard_blocker': freeze_reason or blocked_reason,
        'exchange_position_side': position.get('exchange_position_side'),
        'exchange_position_qty': position.get('exchange_position_qty'),
        'submit_exception_policy': exception_policy or None,
        'exception_policy_brief': exception_policy_brief or None,
        'next_focus': next_focus,
    }

def _to_market_snapshot(config: Any, result: dict[str, Any] | None, state: dict[str, Any] | None) -> MarketSnapshot:
    result = result or {}
    state = state or {}
    latest_market_summary = getattr(config, '_runtime_latest_market_summary', None) or {}
    return MarketSnapshot(
        decision_ts=latest_market_summary.get('decision_ts') or '',
        bar_ts=latest_market_summary.get('bar_ts') or '',
        strategy_ts=latest_market_summary.get('strategy_ts'),
        execution_attributed_bar=latest_market_summary.get('execution_attributed_bar'),
        symbol=getattr(config, 'symbol', 'UNKNOWN'),
        preclose_offset_seconds=27,
        current_price=0.0,
        source_status=latest_market_summary.get('source_status') or 'UNKNOWN',
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
        last_confirmed_order_ids=list(state.get('last_confirmed_order_ids') or []),
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
    publisher = DiscordPublisher(getattr(config, 'discord_execution_channel_id', None) or 'DISCORD_CHANNEL_ID_PLACEHOLDER')
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
    }
    return preview


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
    last_result = state_payload.get('last_result', {})
    recent_events = _safe_tail_jsonl(resolved_event_log_path, limit=tail) if resolved_event_log_path else []
    last_freeze = _find_last_event(recent_events, 'freeze')
    last_execution = _find_last_event(recent_events, 'execute_summary')
    last_recover = _find_last_event(recent_events, 'recover_result')
    last_confirm = _find_last_event(recent_events, 'confirm_summary')
    last_dispatch_preview = _find_last_event(recent_events, 'sender_dispatch_preview')
    last_discord_receipt_event = _find_last_event(recent_events, 'discord_send_receipt')

    discord_send_gate = runtime_status.get('discord_send_gate') or {}
    latest_result_summary = runtime_status.get('latest_result_summary', {})
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
    confirm_summary = build_execution_confirm_summary(last_result or latest_result_summary.get('confirm_summary'))
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
    freeze_summary = {
        'runtime_mode': state.get('runtime_mode') or latest_result_summary.get('runtime_mode'),
        'freeze_status': state.get('freeze_status') or latest_result_summary.get('freeze_status'),
        'freeze_reason': state.get('freeze_reason') or latest_result_summary.get('freeze_reason'),
        'last_freeze_reason': state.get('last_freeze_reason'),
        'last_recover_result': state.get('last_recover_result'),
        'recover_attempt_count': state.get('recover_attempt_count'),
        'pending_execution_phase': state.get('pending_execution_phase') or latest_result_summary.get('execution_phase'),
    }
    position_summary = {
        'exchange_position_side': state.get('exchange_position_side'),
        'exchange_position_qty': state.get('exchange_position_qty'),
        'exchange_entry_price': state.get('exchange_entry_price'),
        'active_strategy': state.get('active_strategy'),
        'active_side': state.get('active_side'),
        'base_quantity': state.get('base_quantity'),
    }

    recover_check = runtime_status.get('recover_check') or state_payload.get('recover_check')
    recover_timeline = runtime_status.get('recover_timeline') or state_payload.get('recover_timeline') or []

    return {
        'runtime': runtime_summary,
        'discord_send_gate': discord_send_gate or (dispatch_preview or {}).get('send_gate'),
        'last_discord_send_attempt': last_discord_send_attempt,
        'latest_discord_receipt': latest_discord_receipt,
        'latest_discord_receipt_summary': latest_discord_receipt_summary,
        'ledger_summary': ledger_summary,
        'receipt_log_summary': receipt_log_summary,
        'operator_summary': {
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
            'result_status': latest_result_summary.get('result_status'),
            'confirmation_status': latest_result_summary.get('confirmation_status'),
            'execution_phase': latest_result_summary.get('execution_phase'),
        },
        'position': position_summary,
        'operator_compact_view': _build_operator_compact_view(
            runtime=runtime_summary,
            submit_gate=submit_gate,
            freeze=freeze_summary,
            confirm_summary=confirm_summary,
            position=position_summary,
            recover_check=recover_check,
            recover_timeline=recover_timeline,
        ),
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
        'recover_check': recover_check,
        'recover_timeline': recover_timeline,
        'sender_dispatch_preview': dispatch_preview,
        'dispatch_preview_audit_path': runtime_status.get('latest_result_summary', {}).get('dispatch_preview_audit_path'),
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
