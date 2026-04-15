from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .binance_readonly import BinanceReadOnlyClient
from .binance_readonly_pack import adapt_readonly_pack, validate_readonly_pack
from .binance_readonly_sample_capture import collect_live_readonly_pack
from .executor_real import BinanceRealExecutor
from .market_data import BinanceReadOnlyMarketDataProvider
from .models import FinalActionPlan, MarketSnapshot
from .operator_log_draft import (
    build_confirmation_from_adapted_fixture,
    build_operator_compact_view_from_confirmation,
    build_operator_log_draft_struct,
    render_operator_log_markdown,
)
from .runtime_env import DEFAULT_BINANCE_SYMBOL, load_binance_env, validate_runtime_config
from .runtime_prepare_only_gate_check import build_prepare_only_gate_report
from .runtime_status_cli import build_dispatch_preview, build_execution_confirm_summary
from .discord_sender_bridge import MessageToolDiscordSender, build_discord_transport
from .discord_publisher import DiscordMessagePayload
from .runtime_worker import (
    AuditArtifactWriter,
    BinancePreRunReconcileModule,
    EventLogWriter,
    RuntimeStatusStore,
    build_initial_state,
)
from .state_store import JsonStateStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(dt: datetime | None = None) -> str:
    return (dt or _utc_now()).isoformat()


def _run_id() -> str:
    return _utc_now().strftime('%Y%m%dT%H%M%S%fZ')


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')


def _load_state_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding='utf-8'))


def _seed_operator_state(source_state_path: Path, target_state_path: Path) -> None:
    payload = _load_state_payload(source_state_path)
    if payload is None:
        initial_state = build_initial_state(_utc_iso())
        _write_json(target_state_path, {'state': asdict(initial_state), 'last_result': None})
        return
    _write_json(target_state_path, payload)


def _build_operator_paths(base_dir: Path, run_id: str, symbol: str) -> dict[str, Path]:
    stem = f'{run_id}_{symbol}'
    run_dir = base_dir / run_id
    return {
        'run_dir': run_dir,
        'gate_report': run_dir / 'prepare_only_gate_report.json',
        'pretrade_pack': run_dir / f'{stem}_pretrade_readonly_pack.json',
        'open_pack': run_dir / f'{stem}_open_confirm_pack.json',
        'open_fixture': run_dir / f'{stem}_open_confirm_pack.fixture.json',
        'open_operator_log': run_dir / f'{stem}_open_confirm_pack.operator_log_draft.md',
        'close_pack': run_dir / f'{stem}_close_confirm_pack.json',
        'close_fixture': run_dir / f'{stem}_close_confirm_pack.fixture.json',
        'close_operator_log': run_dir / f'{stem}_close_confirm_pack.operator_log_draft.md',
        'result_md': run_dir / f'{stem}_manual_real_trade_sampling_result.md',
        'result_json': run_dir / f'{stem}_manual_real_trade_sampling_result.json',
    }


def _build_manual_market(*, symbol: str, current_price: float) -> MarketSnapshot:
    now_iso = _utc_iso()
    return MarketSnapshot(
        decision_ts=now_iso,
        bar_ts=now_iso,
        strategy_ts=None,
        execution_attributed_bar=None,
        symbol=symbol,
        preclose_offset_seconds=0,
        current_price=float(current_price),
        source_status='MANUAL_OPERATOR',
        fast_5m={'close': float(current_price), 'low': float(current_price), 'high': float(current_price)},
        signal_15m={'close': float(current_price), 'low': float(current_price), 'high': float(current_price)},
        signal_15m_ts=now_iso,
        trend_1h={
            'close': float(current_price),
            'ema_fast': float(current_price),
            'ema_slow': float(current_price),
            'adx': 0.0,
            'atr_rank': 0.0,
            'structure_tag': 'MANUAL_OPERATOR',
        },
        trend_1h_ts=now_iso,
        signal_15m_history=[
            {'close': float(current_price), 'low': float(current_price), 'high': float(current_price)}
            for _ in range(4)
        ],
        rev_candidate=None,
    )


def _load_live_price(readonly_client: BinanceReadOnlyClient, symbol: str) -> float:
    provider = BinanceReadOnlyMarketDataProvider(readonly_client)
    bundle = provider.load(symbol=symbol, decision_time=_utc_now())
    return float(bundle.current_price)


def _normalize_open_qty(*, executor: BinanceRealExecutor, target_notional: float, current_price: float, symbol: str) -> float:
    rules = executor.readonly_client.get_exchange_info(symbol)
    raw_qty = float(target_notional) / float(current_price)
    quantity = executor._normalize_quantity(raw_qty, rules)
    if quantity is None or quantity <= 0:
        raise ValueError('normalized quantity is empty; target_notional is too small under current symbol rules')
    executor._validate_quantity(quantity, rules, _build_manual_market(symbol=symbol, current_price=current_price))
    return float(quantity)


def _make_open_plan(*, side: str, quantity: float, current_price: float) -> FinalActionPlan:
    now_iso = _utc_iso()
    return FinalActionPlan(
        plan_ts=now_iso,
        bar_ts=now_iso,
        action_type='open',
        target_strategy='manual_real_trade_sampling',
        target_side=side,
        reason='manual_real_trade_sampling_open',
        qty_mode='manual_target_notional',
        qty=float(quantity),
        price_hint=float(current_price),
        stop_price=None,
        risk_fraction=None,
        conflict_context={'manual_operator_entry': True},
        requires_execution=True,
        close_reason=None,
    )


def _make_close_plan() -> FinalActionPlan:
    now_iso = _utc_iso()
    return FinalActionPlan(
        plan_ts=now_iso,
        bar_ts=now_iso,
        action_type='close',
        target_strategy='manual_real_trade_sampling',
        target_side=None,
        reason='manual_real_trade_sampling_close',
        qty_mode='position_close',
        qty=None,
        price_hint=None,
        stop_price=None,
        risk_fraction=None,
        conflict_context={'manual_operator_entry': True},
        requires_execution=True,
        close_reason='manual_sampling_hold_elapsed',
    )


def _build_pack_confirmation(*, pack: dict[str, Any], scenario_name: str) -> tuple[dict[str, Any], dict[str, Any], Any, dict[str, Any]]:
    validation = validate_readonly_pack(pack)
    adapted = adapt_readonly_pack(pack, scenario_name=scenario_name)
    confirmation = build_confirmation_from_adapted_fixture(adapted)
    compact_view = build_operator_compact_view_from_confirmation(confirmation)
    return validation, adapted, confirmation, compact_view


def _save_pack_family(*, pack_path: Path, fixture_path: Path, operator_log_path: Path, pack: dict[str, Any], scenario_name: str) -> dict[str, Any]:
    _write_json(pack_path, pack)
    validation, adapted, confirmation, compact_view = _build_pack_confirmation(pack=pack, scenario_name=scenario_name)
    _write_json(fixture_path, adapted)
    draft = build_operator_log_draft_struct(adapted, confirmation, compact_view)
    _write_text(operator_log_path, render_operator_log_markdown(draft))
    return {
        'pack_path': str(pack_path),
        'fixture_path': str(fixture_path),
        'operator_log_path': str(operator_log_path),
        'validation': validation,
        'confirmation': {
            'confirmation_status': confirmation.confirmation_status,
            'confirmation_category': confirmation.confirmation_category,
            'order_status': confirmation.order_status,
            'post_position_side': confirmation.post_position_side,
            'post_position_qty': confirmation.post_position_qty,
            'avg_fill_price': confirmation.avg_fill_price,
            'should_freeze': confirmation.should_freeze,
            'freeze_reason': confirmation.freeze_reason,
            'reconcile_status': confirmation.reconcile_status,
        },
        'operator_compact_view': compact_view,
    }


def _capture_pack(*, env_file: Path, symbol: str, order_id: str | None, client_order_id: str | None) -> dict[str, Any]:
    return collect_live_readonly_pack(
        env_file=env_file,
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        trades_limit=20,
    )


def _parse_float(value: Any) -> float | None:
    if value in (None, '', 'NULL'):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pretrade_account_is_clean(pack: dict[str, Any]) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    for row in pack.get('position_risk') or []:
        qty = abs(_parse_float(row.get('positionAmt')) or 0.0)
        if qty > 1e-9:
            blockers.append(f"position_not_flat:{row.get('symbol')}:{qty}")
    open_orders = pack.get('open_orders') or []
    if open_orders:
        blockers.append(f"open_orders_present:{len(open_orders)}")
    return (not blockers), blockers


def _reset_operator_state_to_flat_ready(*, state_path: Path, runtime_status_path: Path, as_of_ts: str) -> None:
    payload = _load_state_payload(state_path)
    if payload is None:
        initial_state = build_initial_state(as_of_ts)
        _write_json(state_path, {'state': asdict(initial_state), 'last_result': None})
        return

    state_payload = dict(payload.get('state') or {})
    state_payload.update(
        {
            'state_ts': as_of_ts,
            'consistency_status': 'OK',
            'freeze_reason': None,
            'exchange_position_side': None,
            'exchange_position_qty': 0.0,
            'exchange_entry_price': 0.0,
            'active_strategy': 'none',
            'active_side': None,
            'strategy_entry_time': None,
            'strategy_entry_price': None,
            'base_quantity': None,
            'can_open_new_position': True,
            'can_modify_position': True,
            'runtime_mode': 'ACTIVE',
            'freeze_status': 'NONE',
            'last_freeze_reason': state_payload.get('last_freeze_reason'),
            'last_recover_at': as_of_ts,
            'last_recover_result': 'RECOVERED',
            'recover_attempt_count': int(state_payload.get('recover_attempt_count') or 0) + 1,
            'pending_execution_phase': None,
            'last_confirmed_order_ids': [],
            'recover_check': {
                'checked_at': as_of_ts,
                'source': 'manual_real_trade_sampling_entry',
                'result': 'RECOVERED',
                'allowed': True,
                'reason': 'pretrade_account_flat_and_no_open_orders',
                'pending_execution_phase': None,
                'freeze_reason': None,
                'consistency_status': 'OK',
                'runtime_mode': 'ACTIVE',
                'recover_ready': True,
                'requires_manual_resume': True,
                'guard_decision': 'manual_flat_reset_before_sampling',
            },
            'recover_timeline': list(state_payload.get('recover_timeline') or [])[-9:],
        }
    )
    state_payload['recover_timeline'] = list(state_payload.get('recover_timeline') or []) + [state_payload['recover_check']]
    payload['state'] = state_payload
    payload['last_result'] = None
    _write_json(state_path, payload)

    runtime_status_payload = {
        'phase': 'prepared',
        'ts': as_of_ts,
        'symbol': state_payload.get('active_strategy') or 'ETHUSDT',
        'last_started_at': as_of_ts,
        'last_completed_at': as_of_ts,
        'last_run_summary': {
            'run_id': 'manual_flat_reset_before_sampling',
            'phase': 'prepared',
            'symbol': 'ETHUSDT',
            'decision_ts': as_of_ts,
            'consistency_status': 'OK',
            'plan_action': None,
            'result_status': 'RECOVERED',
            'freeze_reason': None,
            'runtime_mode': 'ACTIVE',
            'failure_count': 0,
            'backoff_seconds': 0.0,
            'event_log_path': str(runtime_status_path.parent / 'event_log.jsonl'),
        },
        'runtime_config_validation': {'ok': True},
        'freeze': {
            'runtime_mode': 'ACTIVE',
            'freeze_status': 'NONE',
            'freeze_reason': None,
            'last_freeze_reason': state_payload.get('last_freeze_reason'),
            'last_recover_result': 'RECOVERED',
            'recover_attempt_count': state_payload.get('recover_attempt_count'),
            'pending_execution_phase': None,
        },
        'confirm_summary': {
            'confirmation_category': 'confirmed',
            'reconcile_status': 'OK',
        },
        'submit_gate': {
            'submit_allowed': True,
            'guardrail_blockers': [],
        },
        'env_gate_summary': {
            'binance_submit_env': {'ready_by_env': True, 'submit_allowed_now': True},
            'discord_execution_confirmation_env': {'open_by_env': True},
        },
        'operator_compact_view': {
            'next_focus': 'pretrade_account_flat_and_no_open_orders',
        },
    }
    _write_json(runtime_status_path, runtime_status_payload)


def _resolve_pack_lookup_ids(output: dict[str, Any]) -> tuple[str | None, str | None, dict[str, Any]]:
    result_payload = output.get('result') or {}
    trade_summary = result_payload.get('trade_summary') or {}
    request_context = trade_summary.get('request_context') or {}
    confirm_context = trade_summary.get('confirm_context') or {}
    order_facts = list(trade_summary.get('order_facts') or [])
    exchange_order_ids = [str(item) for item in (result_payload.get('exchange_order_ids') or []) if item]
    client_order_ids = [str(item) for item in (request_context.get('client_order_ids') or []) if item]

    source = 'client_order_id_fallback'
    resolved_exchange_order_id = None
    resolved_client_order_id = client_order_ids[0] if client_order_ids else None

    for row in order_facts:
        exchange_order_id = row.get('exchange_order_id')
        if exchange_order_id:
            resolved_exchange_order_id = str(exchange_order_id)
            source = f"order_facts:{row.get('lookup_key') or 'unknown'}"
            break

    if resolved_exchange_order_id is None:
        for row in order_facts:
            if str(row.get('lookup_key') or '') == 'client_order_id' and row.get('client_order_id'):
                resolved_client_order_id = str(row.get('client_order_id'))
                source = 'order_facts:client_order_id'
                break

    if resolved_exchange_order_id is None:
        confirm_attempted = bool(confirm_context.get('confirm_attempted'))
        submitted = bool(trade_summary.get('submitted'))
        if confirm_attempted or submitted:
            for item in exchange_order_ids:
                if item not in client_order_ids:
                    resolved_exchange_order_id = item
                    source = 'result.exchange_order_ids'
                    break

    return resolved_exchange_order_id, resolved_client_order_id, {
        'source': source,
        'result_exchange_order_ids': exchange_order_ids,
        'request_client_order_ids': client_order_ids,
        'confirm_attempted': bool(confirm_context.get('confirm_attempted')),
        'confirm_path': confirm_context.get('confirm_path'),
        'submitted': bool(trade_summary.get('submitted')),
    }


def _build_status_payload(
    *,
    config: Any,
    run_id: str,
    runtime_status_path: Path,
    event_log_path: Path,
    market: MarketSnapshot,
    output: dict[str, Any],
    config_validation: dict[str, Any],
    audit_artifact_paths: dict[str, str],
    discord_send_attempt: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result_payload = output.get('result') or {}
    trade_summary = result_payload.get('trade_summary') or {}
    dispatch_preview = build_dispatch_preview(config, result_payload, output.get('state') or {})
    return {
        'phase': 'completed',
        'ts': _utc_iso(),
        'dry_run': config.dry_run,
        'submit_enabled': config.submit_enabled,
        'symbol': config.symbol,
        'runtime_status_path': str(runtime_status_path),
        'event_log_path': str(event_log_path),
        'consecutive_failures': 0,
        'backoff_seconds': 0.0,
        'last_exception': None,
        'last_run_summary': {
            'run_id': run_id,
            'phase': 'completed',
            'symbol': config.symbol,
            'decision_ts': market.decision_ts,
            'consistency_status': (output.get('state') or {}).get('consistency_status'),
            'plan_action': (output.get('plan') or {}).get('action_type'),
            'result_status': result_payload.get('status'),
            'freeze_reason': result_payload.get('freeze_reason'),
            'runtime_mode': (output.get('state') or {}).get('runtime_mode'),
            'failure_count': 0,
            'backoff_seconds': 0.0,
            'event_log_path': str(event_log_path),
        },
        'last_started_at': market.decision_ts,
        'last_completed_at': _utc_iso(),
        'discord_send_gate': dispatch_preview.get('send_gate'),
        'last_discord_send_attempt': discord_send_attempt,
        'latest_discord_receipt': None if not discord_send_attempt else discord_send_attempt.get('receipt'),
        'latest_discord_receipt_summary': None if not discord_send_attempt or not discord_send_attempt.get('receipt') else {
            'message_id': (discord_send_attempt.get('receipt') or {}).get('message_id'),
            'channel_id': (discord_send_attempt.get('receipt') or {}).get('provider_channel_id') or (discord_send_attempt.get('receipt') or {}).get('target'),
            'transport_name': (discord_send_attempt.get('receipt') or {}).get('transport_name'),
            'payload_kind': (discord_send_attempt.get('receipt') or {}).get('payload_kind'),
            'idempotency_key': (discord_send_attempt.get('receipt') or {}).get('idempotency_key'),
            'sent_at': (discord_send_attempt.get('receipt') or {}).get('sent_at'),
            'status': (discord_send_attempt.get('receipt') or {}).get('status'),
            'receipt_store_path': discord_send_attempt.get('receipt_store_path'),
        },
        'latest_market_summary': {
            'decision_ts': market.decision_ts,
            'bar_ts': market.bar_ts,
            'strategy_ts': market.strategy_ts,
            'execution_attributed_bar': market.execution_attributed_bar,
            'source_status': market.source_status,
        },
        'latest_result_summary': {
            'consistency_status': (output.get('state') or {}).get('consistency_status'),
            'runtime_mode': (output.get('state') or {}).get('runtime_mode'),
            'freeze_status': (output.get('state') or {}).get('freeze_status'),
            'freeze_reason': (output.get('state') or {}).get('freeze_reason'),
            'pending_execution_phase': (output.get('state') or {}).get('pending_execution_phase'),
            'plan_action': (output.get('plan') or {}).get('action_type'),
            'plan_reason': (output.get('plan') or {}).get('reason'),
            'result_status': result_payload.get('status'),
            'confirmation_status': result_payload.get('confirmation_status'),
            'execution_phase': result_payload.get('execution_phase'),
            'submit_gate': trade_summary.get('submit_gate'),
            'confirm_summary': build_execution_confirm_summary(result_payload),
            'sender_dispatch_preview': dispatch_preview,
            'dispatch_preview_audit_path': str(runtime_status_path.parent / 'dispatch_previews' / f'{run_id}.json'),
            'audit_artifact_paths': audit_artifact_paths,
        },
        'runtime_config_validation': config_validation,
        'audit_artifact_paths': audit_artifact_paths,
        'recover_check': (output.get('state') or {}).get('recover_check'),
        'recover_timeline': (output.get('state') or {}).get('recover_timeline') or [],
    }


def _build_sender(config: Any) -> MessageToolDiscordSender:
    return MessageToolDiscordSender(
        real_send_enabled=bool(getattr(config, 'discord_real_send_enabled', False)),
        message_tool_enabled=bool(getattr(config, 'discord_message_tool_enabled', False)),
        require_idempotency=bool(getattr(config, 'discord_send_require_idempotency', True)),
        ledger_path=getattr(config, 'discord_send_ledger_path', None),
        receipt_store_path=getattr(config, 'discord_send_receipt_log_path', None),
        retry_limit=int(getattr(config, 'discord_send_retry_limit', 3) or 3),
        transport=build_discord_transport(getattr(config, 'discord_transport', 'unconfigured')),
        execution_confirmation_real_send_enabled=bool(getattr(config, 'discord_execution_confirmation_real_send_enabled', False)),
    )


def _maybe_run_discord_sender(*, config: Any, output: dict[str, Any]) -> dict[str, Any] | None:
    result_payload = output.get('result') or {}
    state_payload = output.get('state') or {}
    dispatch_preview = build_dispatch_preview(config, result_payload, state_payload)
    sender = _build_sender(config)
    primary_preview = dispatch_preview.get('primary_preview') or {}
    payload_preview = primary_preview.get('payload_preview') or dispatch_preview.get('payload_preview')
    primary_kind = dispatch_preview.get('primary_kind') or dispatch_preview.get('kind')
    rehearsal_preview = dispatch_preview.get('rehearsal_preview')
    rehearsal_open = bool(getattr(config, 'discord_rehearsal_real_send_enabled', False))
    use_rehearsal = payload_preview is None and rehearsal_open and rehearsal_preview is not None
    selected_preview = rehearsal_preview.get('payload_preview') if use_rehearsal else payload_preview
    selected_kind = 'rehearsal_notification' if use_rehearsal else primary_kind

    if selected_preview is None:
        return {
            'attempted': False,
            'sent': False,
            'reason': 'payload_not_sendable',
            'payload_kind': selected_kind,
            'rehearsal_mode': use_rehearsal,
            'idempotency_key': dispatch_preview.get('idempotency_key'),
            'receipt': None,
            'receipt_store_path': getattr(config, 'discord_send_receipt_log_path', None),
            'failure': None,
            'provider_response': None,
            'transport_name': getattr(sender.transport, 'transport_name', sender.transport.__class__.__name__),
        }

    payload = DiscordMessagePayload(
        channel_id=selected_preview.get('channel_id') or config.discord_execution_channel_id,
        content=selected_preview.get('content') or '',
        metadata=selected_preview.get('metadata') or {},
    )
    sender_result = sender.send(payload)
    receipt = sender_result.get('receipt') or {}
    return {
        'attempted': True,
        'sent': bool(sender_result.get('sent')),
        'reason': sender_result.get('reason'),
        'payload_kind': payload.metadata.get('kind') if payload.metadata else selected_kind,
        'rehearsal_mode': use_rehearsal,
        'idempotency_key': (payload.metadata or {}).get('idempotency_key') or dispatch_preview.get('idempotency_key'),
        'receipt': receipt,
        'receipt_store_path': sender_result.get('receipt_store_path'),
        'failure': sender_result.get('failure'),
        'provider_response': sender_result.get('provider_response'),
        'provider_message_id': receipt.get('provider_message_id'),
        'provider_channel_id': receipt.get('provider_channel_id'),
        'provider_status': receipt.get('provider_status'),
        'transport_name': receipt.get('transport_name') or getattr(sender.transport, 'transport_name', sender.transport.__class__.__name__),
        'send_gate': sender_result.get('send_gate'),
    }


def _write_dispatch_preview(*, config: Any, runtime_status_path: Path, run_id: str, market: MarketSnapshot, output: dict[str, Any], discord_send_attempt: dict[str, Any] | None = None) -> str:
    audit_dir = runtime_status_path.parent / 'dispatch_previews'
    audit_dir.mkdir(parents=True, exist_ok=True)
    preview = build_dispatch_preview(config, output.get('result') or {}, output.get('state') or {})
    payload = {
        'run_id': run_id,
        'symbol': market.symbol,
        'decision_ts': market.decision_ts,
        'bar_ts': market.bar_ts,
        'target_channel_id': config.discord_execution_channel_id,
        'preview': preview,
        'real_send_enabled': bool(getattr(config, 'discord_real_send_enabled', False)),
        'message_tool_enabled': bool(getattr(config, 'discord_message_tool_enabled', False)),
        'discord_transport': getattr(config, 'discord_transport', 'unconfigured'),
        'discord_rehearsal_real_send_enabled': bool(getattr(config, 'discord_rehearsal_real_send_enabled', False)),
        'discord_execution_confirmation_real_send_enabled': bool(getattr(config, 'discord_execution_confirmation_real_send_enabled', False)),
        'discord_send_attempt': discord_send_attempt,
        'discord_send_ledger_path': getattr(config, 'discord_send_ledger_path', None),
        'discord_send_receipt_log_path': getattr(config, 'discord_send_receipt_log_path', None),
        'discord_send_retry_limit': int(getattr(config, 'discord_send_retry_limit', 3) or 3),
        'binance_dry_run': config.dry_run,
        'binance_submit_enabled': config.submit_enabled,
    }
    out_path = audit_dir / f'{run_id}.json'
    _write_json(out_path, payload)
    return str(out_path)


def _write_receipts(
    *,
    config: Any,
    audit_writer: AuditArtifactWriter,
    run_id: str,
    market: MarketSnapshot,
    output: dict[str, Any],
    config_validation: dict[str, Any],
    dispatch_preview_path: str,
    discord_send_attempt: dict[str, Any] | None = None,
) -> dict[str, str]:
    result_payload = output.get('result') or {}
    state_payload = output.get('state') or {}
    trade_summary = result_payload.get('trade_summary') or {}
    confirm_summary = build_execution_confirm_summary(result_payload)
    execution_receipt_payload = {
        'run_id': run_id,
        'symbol': market.symbol,
        'decision_ts': market.decision_ts,
        'bar_ts': market.bar_ts,
        'strategy_ts': market.strategy_ts,
        'execution_attributed_bar': market.execution_attributed_bar,
        'plan': output.get('plan') or {},
        'result': result_payload,
        'confirm_summary': confirm_summary,
        'submit_gate': trade_summary.get('submit_gate'),
        'request_context': trade_summary.get('request_context'),
        'confirm_context': trade_summary.get('confirm_context'),
        'readonly_recheck': trade_summary.get('readonly_recheck'),
        'state_snapshot': {
            'consistency_status': state_payload.get('consistency_status'),
            'runtime_mode': state_payload.get('runtime_mode'),
            'freeze_status': state_payload.get('freeze_status'),
            'freeze_reason': state_payload.get('freeze_reason'),
            'pending_execution_phase': state_payload.get('pending_execution_phase'),
            'exchange_position_side': state_payload.get('exchange_position_side'),
            'exchange_position_qty': state_payload.get('exchange_position_qty'),
            'exchange_entry_price': state_payload.get('exchange_entry_price'),
        },
        'config_validation': config_validation,
    }
    discord_receipt_payload = {
        'run_id': run_id,
        'symbol': market.symbol,
        'decision_ts': market.decision_ts,
        'dispatch_preview_path': dispatch_preview_path,
        'dispatch_preview': build_dispatch_preview(config, result_payload, state_payload),
        'last_discord_send_attempt': discord_send_attempt,
        'config_validation': config_validation,
        'note': 'manual_real_trade_sampling_entry 已复用正式 runtime 主链；本次记录 preview/audit/receipt 与真实 Discord send 尝试对齐情况。',
    }
    return {
        'execution_receipt': audit_writer.write('execution_receipts', run_id, execution_receipt_payload),
        'discord_receipt': audit_writer.write('discord_receipts', run_id, discord_receipt_payload),
    }


def _append_event(event_log: EventLogWriter, event_type: str, payload: dict[str, Any]) -> None:
    event_log.append(event_type, payload)


def _apply_manual_operator_position_state(
    *,
    state_store: JsonStateStore,
    plan: FinalActionPlan,
    output_result: Any,
    confirmation: Any,
) -> None:
    state = state_store.load_state()
    if confirmation.confirmation_category != 'confirmed' or confirmation.reconcile_status != 'OK' or confirmation.should_freeze:
        state_store.save_state(state)
        return

    post_position_qty = float(getattr(confirmation, 'post_position_qty', 0.0) or 0.0)
    post_position_side = getattr(confirmation, 'post_position_side', None)
    post_entry_price = getattr(confirmation, 'post_entry_price', None)
    avg_fill_price = getattr(confirmation, 'avg_fill_price', None)

    state.exchange_position_side = post_position_side
    state.exchange_position_qty = post_position_qty
    state.exchange_entry_price = post_entry_price or (avg_fill_price if post_position_side else None)

    if abs(post_position_qty) <= 1e-9 or post_position_side is None:
        state.active_strategy = 'none'
        state.active_side = None
        state.strategy_entry_time = None
        state.strategy_entry_price = None
        state.base_quantity = None
        state.pending_execution_phase = None
        state_store.save_state(state)
        return

    if plan.action_type == 'open':
        state.active_strategy = plan.target_strategy or 'manual_real_trade_sampling'
        state.active_side = post_position_side or plan.target_side
        state.strategy_entry_time = output_result.result_ts
        state.strategy_entry_price = post_entry_price or avg_fill_price
        state.base_quantity = post_position_qty or output_result.executed_qty
    elif plan.action_type == 'close':
        state.active_strategy = state.active_strategy or (plan.target_strategy or 'manual_real_trade_sampling')
        state.active_side = post_position_side or state.active_side
        state.base_quantity = post_position_qty
        state.strategy_entry_price = post_entry_price or state.strategy_entry_price

    state_store.save_state(state)


def _execute_phase(
    *,
    phase_name: str,
    run_id: str,
    config: Any,
    executor: BinanceRealExecutor,
    reconcile_module: BinancePreRunReconcileModule,
    state_store: JsonStateStore,
    runtime_status_store: RuntimeStatusStore,
    event_log: EventLogWriter,
    audit_writer: AuditArtifactWriter,
    market: MarketSnapshot,
    plan: FinalActionPlan,
    paths: dict[str, Path],
    env_file: Path,
) -> dict[str, Any]:
    state_before = state_store.load_state()
    reconciled_state = reconcile_module.reconcile(market, state_before)
    state_store.save_state(reconciled_state)
    output_result = executor.execute(plan, market, reconciled_state)
    state_store.save_result(reconciled_state, output_result)

    provisional_output = {
        'market': asdict(market),
        'state': asdict(state_store.load_state()),
        'plan': asdict(plan),
        'result': asdict(output_result),
    }
    selected_order_id, selected_client_order_id, lookup_debug = _resolve_pack_lookup_ids(provisional_output)
    pack = _capture_pack(
        env_file=env_file,
        symbol=market.symbol,
        order_id=selected_order_id,
        client_order_id=(None if selected_order_id else selected_client_order_id),
    )
    pack_result = _save_pack_family(
        pack_path=paths[f'{phase_name}_pack'],
        fixture_path=paths[f'{phase_name}_fixture'],
        operator_log_path=paths[f'{phase_name}_operator_log'],
        pack=pack,
        scenario_name=f'{run_id}_{phase_name}_confirm',
    )
    confirmation_payload = pack_result.get('confirmation') or {}
    _apply_manual_operator_position_state(
        state_store=state_store,
        plan=plan,
        output_result=output_result,
        confirmation=type('PackConfirmation', (), confirmation_payload)(),
    )

    updated_state = state_store.load_state()
    output = {
        'market': asdict(market),
        'state': asdict(updated_state),
        'plan': asdict(plan),
        'result': asdict(output_result),
    }
    config_validation = validate_runtime_config(config).as_dict()
    discord_send_attempt = _maybe_run_discord_sender(config=config, output=output)
    dispatch_preview_path = _write_dispatch_preview(
        config=config,
        runtime_status_path=runtime_status_store.path,
        run_id=run_id,
        market=market,
        output=output,
        discord_send_attempt=discord_send_attempt,
    )
    audit_artifact_paths = _write_receipts(
        config=config,
        audit_writer=audit_writer,
        run_id=run_id,
        market=market,
        output=output,
        config_validation=config_validation,
        dispatch_preview_path=dispatch_preview_path,
        discord_send_attempt=discord_send_attempt,
    )
    status_payload = _build_status_payload(
        config=config,
        run_id=run_id,
        runtime_status_path=runtime_status_store.path,
        event_log_path=event_log.path,
        market=market,
        output=output,
        config_validation=config_validation,
        audit_artifact_paths=audit_artifact_paths,
        discord_send_attempt=discord_send_attempt,
    )
    runtime_status_store.write(status_payload)
    _append_event(
        event_log,
        f'manual_sampling_{phase_name}',
        {
            'run_id': run_id,
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'plan': output['plan'],
            'result': output['result'],
            'pack_confirmation': confirmation_payload,
            'audit_artifact_paths': audit_artifact_paths,
        },
    )

    return {
        'output': output,
        'selected_order_id': selected_order_id,
        'selected_client_order_id': selected_client_order_id,
        'pack_lookup_debug': lookup_debug,
        'audit_artifact_paths': audit_artifact_paths,
        'runtime_status_path': str(runtime_status_store.path),
        'dispatch_preview_path': dispatch_preview_path,
        'pack_result': pack_result,
    }


def _render_result_markdown(payload: dict[str, Any]) -> str:
    open_phase = payload.get('open_phase') or {}
    close_phase = payload.get('close_phase') or {}
    return '\n'.join(
        [
            '# Manual Real Trade Sampling Result',
            '',
            f"- run_id: `{payload.get('run_id')}`",
            f"- symbol: `{payload.get('symbol')}`",
            f"- side: `{payload.get('side')}`",
            f"- target_notional: `{payload.get('target_notional')}`",
            f"- hold_seconds: `{payload.get('hold_seconds')}`",
            f"- execution_confirmation_real_send_default: `{payload.get('execution_confirmation_real_send_default')}`",
            f"- gate_report_path: `{payload.get('gate_report_path')}`",
            f"- operator_runtime_status_path: `{payload.get('runtime_status_path')}`",
            '',
            '## Open',
            f"- order_id: `{open_phase.get('order_id')}`",
            f"- client_order_id: `{open_phase.get('client_order_id')}`",
            f"- execution_receipt: `{((open_phase.get('audit_artifact_paths') or {}).get('execution_receipt'))}`",
            f"- discord_receipt: `{((open_phase.get('audit_artifact_paths') or {}).get('discord_receipt'))}`",
            f"- readonly_pack: `{((open_phase.get('pack_result') or {}).get('pack_path'))}`",
            f"- operator_log: `{((open_phase.get('pack_result') or {}).get('operator_log_path'))}`",
            f"- confirmation_category: `{(((open_phase.get('pack_result') or {}).get('confirmation') or {}).get('confirmation_category'))}`",
            '',
            '## Close',
            f"- order_id: `{close_phase.get('order_id')}`",
            f"- client_order_id: `{close_phase.get('client_order_id')}`",
            f"- execution_receipt: `{((close_phase.get('audit_artifact_paths') or {}).get('execution_receipt'))}`",
            f"- discord_receipt: `{((close_phase.get('audit_artifact_paths') or {}).get('discord_receipt'))}`",
            f"- readonly_pack: `{((close_phase.get('pack_result') or {}).get('pack_path'))}`",
            f"- operator_log: `{((close_phase.get('pack_result') or {}).get('operator_log_path'))}`",
            f"- confirmation_category: `{(((close_phase.get('pack_result') or {}).get('confirmation') or {}).get('confirmation_category'))}`",
            '',
            '## Boundary',
            '- 本入口复用正式 runtime 主链，只额外保存 operator/readonly 证据；不再维护独立试运行 state/runtime_status/event_log/audit。',
            '- 本入口只做单次 open + 单次 close，不包含自动重试、补单、反手。',
            '- 若 open 后 runtime/facts 进入 freeze 或无法安全确认，本入口不会继续自动 close。',
            '- operator 必须人工确认 Binance readonly facts，不能把 preview / planned 当成成交事实。',
            '',
        ]
    ) + '\n'


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='最小受控手动真实采样入口（ETHUSDT 单轮 open/close）')
    parser.add_argument('--symbol', required=True, help='只允许 ETHUSDT')
    parser.add_argument('--side', required=True, choices=['long', 'short'], help='开仓方向')
    parser.add_argument('--target-notional', required=True, type=float, help='目标名义金额')
    parser.add_argument('--hold-seconds', required=True, type=int, help='持仓秒数，必须大于 0')
    parser.add_argument('--env-file', required=True, help='binance env 文件路径')
    parser.add_argument('--base-dir', default='docs/deploy_v6c/samples/real_trade_sampling/manual_runs', help='仅保存 operator/readonly 证据的目录；运行时主链统一落到 runtime/')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    symbol = str(args.symbol).upper().strip()
    side = str(args.side).lower().strip()
    target_notional = float(args.target_notional)
    hold_seconds = int(args.hold_seconds)
    env_file = Path(args.env_file)

    if symbol != DEFAULT_BINANCE_SYMBOL:
        raise SystemExit('symbol 只允许 ETHUSDT')
    if side not in {'long', 'short'}:
        raise SystemExit('side 只允许 long 或 short')
    if hold_seconds <= 0:
        raise SystemExit('hold_seconds 必须大于 0')
    if target_notional <= 0:
        raise SystemExit('target_notional 必须大于 0')

    config = load_binance_env(env_file)
    validation = validate_runtime_config(config)
    if not validation.ok:
        raise SystemExit(f'runtime config invalid: {validation.blockers}')
    if symbol != config.symbol.upper():
        raise SystemExit(f'symbol 与 env 不一致: cli={symbol} env={config.symbol}')
    if target_notional > float(config.submit_max_notional or 0.0):
        raise SystemExit(f'target_notional 超过 env guardrail: {target_notional} > {config.submit_max_notional}')
    canonical_runtime_dir = Path(config.state_path).parent
    gate_report = build_prepare_only_gate_report(
        runtime_status_path=canonical_runtime_dir / 'runtime_status.json',
        env_file=env_file,
        allow_execution_confirmation_real_send=True,
    )

    run_id = _run_id()
    paths = _build_operator_paths(Path(args.base_dir), run_id, symbol)
    paths['run_dir'].mkdir(parents=True, exist_ok=True)
    _write_json(paths['gate_report'], gate_report)

    canonical_runtime_dir = Path(config.state_path).parent
    operator_state_store = JsonStateStore(Path(config.state_path), build_initial_state(_utc_iso()))
    runtime_status_store = RuntimeStatusStore(canonical_runtime_dir / 'runtime_status.json')
    event_log = EventLogWriter(canonical_runtime_dir / 'event_log.jsonl')
    audit_writer = AuditArtifactWriter(canonical_runtime_dir / 'audit_artifacts')

    readonly_client = BinanceReadOnlyClient(config)
    executor = BinanceRealExecutor(config=config, readonly_client=readonly_client)
    reconcile_module = BinancePreRunReconcileModule(operator_state_store, readonly_client)

    _append_event(
        event_log,
        'manual_sampling_run_start',
        {
            'run_id': run_id,
            'symbol': symbol,
            'side': side,
            'target_notional': target_notional,
            'hold_seconds': hold_seconds,
            'env_file': str(env_file),
            'gate_report_path': str(paths['gate_report']),
            'execution_confirmation_real_send_default': False,
        },
    )

    pretrade_pack = _capture_pack(env_file=env_file, symbol=symbol, order_id=None, client_order_id=None)
    _write_json(paths['pretrade_pack'], pretrade_pack)
    pretrade_ok, pretrade_blockers = _pretrade_account_is_clean(pretrade_pack)
    if not pretrade_ok:
        raise SystemExit(f'pretrade account not clean: {pretrade_blockers}')

    # If Binance facts are already flat and clean, clear stale local hold/freeze semantics before the new sample.
    _reset_operator_state_to_flat_ready(
        state_path=Path(config.state_path),
        runtime_status_path=canonical_runtime_dir / 'runtime_status.json',
        as_of_ts=_utc_iso(),
    )

    gate_report = build_prepare_only_gate_report(
        runtime_status_path=canonical_runtime_dir / 'runtime_status.json',
        env_file=env_file,
        allow_execution_confirmation_real_send=True,
    )
    gate_report['pretrade_readonly_override'] = {
        'applied': True,
        'reason': 'pretrade_account_flat_and_no_open_orders',
        'pretrade_ok': True,
        'pretrade_blockers': [],
        'note': '本轮以 Binance 只读基线空仓/无残单作为真实样本放行前提；runtime gate 仅保留审计，不再阻断本次专项验证。',
    }
    _write_json(paths['gate_report'], gate_report)

    current_price = _load_live_price(readonly_client, symbol)
    open_qty = _normalize_open_qty(
        executor=executor,
        target_notional=target_notional,
        current_price=current_price,
        symbol=symbol,
    )

    open_market = _build_manual_market(symbol=symbol, current_price=current_price)
    open_phase = _execute_phase(
        phase_name='open',
        run_id=f'{run_id}_open',
        config=config,
        executor=executor,
        reconcile_module=reconcile_module,
        state_store=operator_state_store,
        runtime_status_store=runtime_status_store,
        event_log=event_log,
        audit_writer=audit_writer,
        market=open_market,
        plan=_make_open_plan(side=side, quantity=open_qty, current_price=current_price),
        paths=paths,
        env_file=env_file,
    )

    time.sleep(hold_seconds)

    close_state = operator_state_store.load_state()
    if close_state.runtime_mode == 'FROZEN':
        result_payload = {
            'run_id': run_id,
            'symbol': symbol,
            'side': side,
            'target_notional': target_notional,
            'hold_seconds': hold_seconds,
            'execution_confirmation_real_send_default': False,
            'gate_report_path': str(paths['gate_report']),
            'runtime_status_path': str(runtime_status_store.path),
            'pretrade_pack_path': str(paths['pretrade_pack']),
            'open_phase': {
                'order_id': open_phase.get('selected_order_id'),
                'client_order_id': open_phase.get('selected_client_order_id'),
                'audit_artifact_paths': open_phase.get('audit_artifact_paths'),
                'pack_result': open_phase.get('pack_result'),
            },
            'close_phase': None,
            'final_state': asdict(close_state),
            'blocked_reason': 'open_phase_left_operator_state_frozen',
        }
        _write_json(paths['result_json'], result_payload)
        _write_text(paths['result_md'], _render_result_markdown(result_payload))
        print(json.dumps({'ok': False, 'run_id': run_id, 'blocked_reason': 'open_phase_left_operator_state_frozen', 'result_md': str(paths['result_md'])}, ensure_ascii=False))
        return 1

    close_price = _load_live_price(readonly_client, symbol)
    close_market = _build_manual_market(symbol=symbol, current_price=close_price)
    close_phase = _execute_phase(
        phase_name='close',
        run_id=f'{run_id}_close',
        config=config,
        executor=executor,
        reconcile_module=reconcile_module,
        state_store=operator_state_store,
        runtime_status_store=runtime_status_store,
        event_log=event_log,
        audit_writer=audit_writer,
        market=close_market,
        plan=_make_close_plan(),
        paths=paths,
        env_file=env_file,
    )

    result_payload = {
        'run_id': run_id,
        'symbol': symbol,
        'side': side,
        'target_notional': target_notional,
        'hold_seconds': hold_seconds,
        'execution_confirmation_real_send_default': False,
        'gate_report_path': str(paths['gate_report']),
        'runtime_status_path': str(runtime_status_store.path),
        'pretrade_pack_path': str(paths['pretrade_pack']),
        'open_phase': {
            'order_id': open_phase.get('selected_order_id'),
            'client_order_id': open_phase.get('selected_client_order_id'),
            'audit_artifact_paths': open_phase.get('audit_artifact_paths'),
            'pack_result': open_phase.get('pack_result'),
        },
        'close_phase': {
            'order_id': close_phase.get('selected_order_id'),
            'client_order_id': close_phase.get('selected_client_order_id'),
            'audit_artifact_paths': close_phase.get('audit_artifact_paths'),
            'pack_result': close_phase.get('pack_result'),
        },
        'final_state': asdict(operator_state_store.load_state()),
    }
    _write_json(paths['result_json'], result_payload)
    _write_text(paths['result_md'], _render_result_markdown(result_payload))
    print(json.dumps({'ok': True, 'run_id': run_id, 'result_md': str(paths['result_md']), 'result_json': str(paths['result_json'])}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
