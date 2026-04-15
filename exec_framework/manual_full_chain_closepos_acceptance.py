from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .binance_readonly import BinanceReadOnlyClient
from .executor_real import BinanceRealExecutor
from .market_data import BinanceReadOnlyMarketDataProvider
from .models import FinalActionPlan, MarketSnapshot
from .runtime_env import load_binance_env, validate_runtime_config
from .runtime_status_cli import build_dispatch_preview, build_execution_confirm_summary
from .discord_publisher import DiscordMessagePayload
from .runtime_worker import (
    AuditArtifactWriter,
    BinancePreRunReconcileModule,
    EventLogWriter,
    RuntimeStatusStore,
    RuntimeWorker,
    WorkerRunSummary,
    build_initial_state,
)
from .state_store import JsonStateStore, apply_flat_reset_to_result


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(dt: datetime | None = None) -> str:
    return (dt or _utc_now()).isoformat()


def _run_id() -> str:
    return _utc_now().strftime('%Y%m%dT%H%M%S%fZ')


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _safe_float(value: Any) -> float | None:
    if value in (None, '', 'NULL'):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_manual_market(*, symbol: str, current_price: float) -> MarketSnapshot:
    now_iso = _utc_iso()
    price = float(current_price)
    return MarketSnapshot(
        decision_ts=now_iso,
        bar_ts=now_iso,
        strategy_ts=now_iso,
        execution_attributed_bar=now_iso,
        symbol=symbol,
        preclose_offset_seconds=0,
        current_price=price,
        source_status='MANUAL_FULL_CHAIN_ACCEPTANCE',
        fast_5m={'close': price, 'low': price, 'high': price},
        signal_15m={'close': price, 'low': price, 'high': price},
        signal_15m_ts=now_iso,
        trend_1h={
            'close': price,
            'ema_fast': price,
            'ema_slow': price,
            'adx': 30.0,
            'atr_rank': 0.6,
            'structure_tag': 'MANUAL_FULL_CHAIN_ACCEPTANCE',
        },
        trend_1h_ts=now_iso,
        signal_15m_history=[
            {'close': price, 'low': price, 'high': price}
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
        raise ValueError('normalized quantity is empty')
    executor._validate_quantity(quantity, rules, _build_manual_market(symbol=symbol, current_price=current_price))
    return float(quantity)


def _ensure_flat(readonly_client: BinanceReadOnlyClient, symbol: str) -> dict[str, Any]:
    position = readonly_client.get_position_snapshot(symbol)
    open_orders = readonly_client.get_open_orders(symbol)
    return {
        'position': asdict(position),
        'open_orders': [item.raw for item in open_orders],
        'is_flat': abs(float(position.qty or 0.0)) <= 0.0,
        'open_orders_count': len(open_orders),
    }


def _snapshot_runtime_files(runtime_dir: Path) -> dict[str, Any]:
    state_path = runtime_dir / 'state.json'
    runtime_status_path = runtime_dir / 'runtime_status.json'
    state_payload = json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else None
    runtime_status_payload = json.loads(runtime_status_path.read_text(encoding='utf-8')) if runtime_status_path.exists() else None
    return {
        'state_path': str(state_path),
        'runtime_status_path': str(runtime_status_path),
        'state': state_payload,
        'runtime_status': runtime_status_payload,
    }


def _is_runtime_strict_flat_ready(runtime_files: dict[str, Any]) -> bool:
    state = ((runtime_files.get('state') or {}).get('state')) or {}
    latest_result = (runtime_files.get('runtime_status') or {}).get('latest_result_summary') or {}
    intent = state.get('strategy_protection_intent') or {}
    return (
        state.get('runtime_mode') == 'ACTIVE'
        and state.get('freeze_status') == 'NONE'
        and state.get('freeze_reason') is None
        and state.get('consistency_status') == 'OK'
        and float(state.get('exchange_position_qty') or 0.0) == 0.0
        and state.get('exchange_position_side') in {None, ''}
        and state.get('pending_execution_phase') in {None, '', 'none'}
        and intent.get('pending_execution_phase') in {None, '', 'none'}
        and state.get('protective_order_status') == 'NONE'
        and list(state.get('exchange_protective_orders') or []) == []
        and latest_result.get('freeze_status') in {None, 'NONE'}
        and latest_result.get('freeze_reason') is None
    )


def _find_latest_artifact(path_str: str | None) -> dict[str, Any] | None:
    if not path_str:
        return None
    path = Path(path_str)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def _make_open_plan(*, side: str, quantity: float, current_price: float, stop_price: float) -> FinalActionPlan:
    now_iso = _utc_iso()
    return FinalActionPlan(
        plan_ts=now_iso,
        bar_ts=now_iso,
        action_type='open',
        target_strategy='trend',
        target_side=side,
        reason='manual_full_chain_acceptance_open',
        qty_mode='manual_target_notional',
        qty=float(quantity),
        price_hint=float(current_price),
        stop_price=float(stop_price),
        risk_fraction=0.1,
        conflict_context={'manual_full_chain_acceptance': True},
        requires_execution=True,
        close_reason=None,
    )


def _make_close_plan(*, clear_protective_orders: bool = False) -> FinalActionPlan:
    now_iso = _utc_iso()
    conflict_context = {
        'manual_full_chain_acceptance': True,
        'protective_missing_force_close': True,
    }
    if clear_protective_orders:
        conflict_context['clear_exchange_protective_orders'] = True
    return FinalActionPlan(
        plan_ts=now_iso,
        bar_ts=now_iso,
        action_type='close',
        target_strategy='trend',
        target_side=None,
        reason='manual_full_chain_acceptance_close',
        qty_mode='position_close',
        qty=None,
        price_hint=None,
        stop_price=None,
        risk_fraction=None,
        conflict_context=conflict_context,
        requires_execution=True,
        close_reason='manual_full_chain_acceptance_cleanup_close',
    )


def _make_management_stop_update_plan(*, state: dict[str, Any], updated_stop_price: float) -> FinalActionPlan:
    now_iso = _utc_iso()
    return FinalActionPlan(
        plan_ts=now_iso,
        bar_ts=now_iso,
        action_type='state_update',
        target_strategy=(state.get('active_strategy') or 'trend'),
        target_side=state.get('active_side'),
        reason='manual_management_stop_update_validation',
        qty_mode='state_only',
        qty=None,
        price_hint=None,
        stop_price=None,
        risk_fraction=state.get('risk_fraction'),
        conflict_context={
            'manual_full_chain_acceptance': True,
            'pending_execution_phase': 'management_stop_update_pending_protective',
            'stop_price': float(updated_stop_price),
        },
        requires_execution=False,
        close_reason=None,
    )


def _make_protective_rebuild_plan(*, state: dict[str, Any]) -> FinalActionPlan:
    now_iso = _utc_iso()
    intent = dict((state.get('strategy_protection_intent') or {}))
    conflict_context = {'manual_full_chain_acceptance': True}
    tp_price = state.get('tp_price')
    if tp_price is None:
        tp_price = intent.get('tp_price')
    if tp_price is not None:
        conflict_context['tp_price'] = float(tp_price)
    return FinalActionPlan(
        plan_ts=now_iso,
        bar_ts=now_iso,
        action_type='protective_rebuild',
        target_strategy=(state.get('active_strategy') or intent.get('strategy') or 'trend'),
        target_side=(state.get('active_side') or intent.get('position_side')),
        reason='manual_full_chain_acceptance_protective_rebuild',
        qty_mode='exchange_position',
        qty=None,
        price_hint=None,
        stop_price=float(state.get('stop_price') or intent.get('stop_price') or 0.0),
        risk_fraction=0.1,
        conflict_context=conflict_context,
        requires_execution=True,
        close_reason=None,
    )


def _should_run_protective_rebuild(open_phase: dict[str, Any]) -> bool:
    open_output = (open_phase.get('output') or {})
    state = open_output.get('state') or {}
    result = open_output.get('result') or {}
    pending_phase = state.get('pending_execution_phase')
    protective_status = state.get('protective_order_status')
    protective_validation = (result.get('trade_summary') or {}).get('protective_validation') or {}
    return bool(
        pending_phase in {'entry_confirmed_pending_protective', 'protection_pending_confirm'}
        or protective_status in {'PENDING_SUBMIT', 'PENDING_CONFIRM'}
        or protective_validation.get('pending_action') == 'protective_rebuild'
    )


def _prepare_runtime(config: Any) -> dict[str, Any]:
    runtime_dir = Path(config.state_path).parent
    state_store = JsonStateStore(Path(config.state_path), build_initial_state(_utc_iso()))
    runtime_status_store = RuntimeStatusStore(runtime_dir / 'runtime_status.json')
    event_log = EventLogWriter(runtime_dir / 'event_log.jsonl')
    audit_writer = AuditArtifactWriter(runtime_dir / 'audit_artifacts')
    readonly_client = BinanceReadOnlyClient(config)
    executor = BinanceRealExecutor(config=config, readonly_client=readonly_client)
    reconcile_module = BinancePreRunReconcileModule(state_store, readonly_client)
    worker = RuntimeWorker(
        config=config,
        state_store=state_store,
        engine=type('DummyEngine', (), {'executor_module': executor})(),
        market_provider=BinanceReadOnlyMarketDataProvider(readonly_client),
        status_store=runtime_status_store,
        event_log=event_log,
    )
    return {
        'runtime_dir': runtime_dir,
        'state_store': state_store,
        'runtime_status_store': runtime_status_store,
        'event_log': event_log,
        'audit_writer': audit_writer,
        'readonly_client': readonly_client,
        'executor': executor,
        'reconcile_module': reconcile_module,
        'worker': worker,
    }


def _save_status_and_artifacts(*, worker: RuntimeWorker, run_id: str, market: MarketSnapshot, output: dict[str, Any], config_validation: dict[str, Any], phase: str) -> dict[str, Any]:
    result_payload = output.get('result') or {}
    confirm_summary = build_execution_confirm_summary(result_payload)
    publishable_output = worker._select_publishable_output(output)
    if phase == 'manual_acceptance_protective_rebuild':
        dispatch_preview = {
            'eligible': False,
            'channel': 'discord',
            'target': worker.config.discord_execution_channel_id,
            'sent': False,
            'reason': 'protective_phase_preview_suppressed',
            'kind': 'not_sendable',
            'primary_kind': 'not_sendable',
            'blocked_reasons': ['protective_phase_preview_suppressed'],
            'dispatch': None,
            'payload_preview': None,
            'rehearsal_preview': None,
            'auxiliary_previews': {},
            'runtime': {
                'runtime_mode': (output.get('state') or {}).get('runtime_mode'),
                'freeze_status': (output.get('state') or {}).get('freeze_status'),
                'freeze_reason': (output.get('result') or {}).get('freeze_reason') or (output.get('state') or {}).get('freeze_reason'),
                'consistency_status': (output.get('result') or {}).get('reconcile_status') or (output.get('state') or {}).get('consistency_status'),
            },
            'confirm': {
                'execution_phase': (output.get('result') or {}).get('execution_phase'),
                'confirmation_status': (output.get('result') or {}).get('confirmation_status'),
                'confirmed_order_status': (output.get('result') or {}).get('confirmed_order_status'),
                'avg_fill_price': (output.get('result') or {}).get('avg_fill_price'),
                'executed_qty': (output.get('result') or {}).get('executed_qty'),
                'executed_side': (output.get('result') or {}).get('executed_side'),
                'exchange_order_ids': (output.get('result') or {}).get('exchange_order_ids') or [],
            },
        }
    else:
        dispatch_preview = build_dispatch_preview(worker.config, publishable_output.get('result') or {}, publishable_output.get('state') or {})
    discord_send_attempt = None
    if phase != 'manual_acceptance_protective_rebuild':
        sender = worker._build_sender()
        payload_preview = ((dispatch_preview.get('primary_preview') or {}).get('payload_preview')) or dispatch_preview.get('payload_preview')
        if payload_preview is not None:
            payload = DiscordMessagePayload(
                channel_id=payload_preview.get('channel_id') or worker.config.discord_execution_channel_id,
                content=payload_preview.get('content') or '',
                metadata=payload_preview.get('metadata') or {},
            )
            sender_result = sender.send(payload)
            receipt = sender_result.get('receipt') or {}
            discord_send_attempt = {
                'attempted': True,
                'sent': bool(sender_result.get('sent')),
                'reason': sender_result.get('reason'),
                'payload_kind': payload.metadata.get('kind'),
                'rehearsal_mode': False,
                'idempotency_key': (payload.metadata or {}).get('idempotency_key'),
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
        else:
            discord_send_attempt = {
                'attempted': False,
                'sent': False,
                'reason': 'payload_not_sendable',
                'payload_kind': dispatch_preview.get('primary_kind') or dispatch_preview.get('kind'),
                'rehearsal_mode': False,
                'idempotency_key': dispatch_preview.get('idempotency_key'),
                'receipt': None,
                'receipt_store_path': getattr(worker.config, 'discord_send_receipt_log_path', None),
                'failure': None,
                'provider_response': None,
                'transport_name': getattr(sender.transport, 'transport_name', sender.transport.__class__.__name__),
                'send_gate': dispatch_preview.get('send_gate'),
            }
        setattr(worker, '_last_discord_send_attempt', discord_send_attempt)

    artifact_paths = worker._write_audit_artifacts(
        run_id=run_id,
        market=market,
        output=output,
        confirm_summary=confirm_summary,
        dispatch_preview=dispatch_preview,
        config_validation=config_validation,
    )
    summary = WorkerRunSummary(
        run_id=run_id,
        phase=phase,
        symbol=market.symbol,
        decision_ts=market.decision_ts,
        consistency_status=(output.get('state') or {}).get('consistency_status'),
        plan_action=(output.get('plan') or {}).get('action_type'),
        result_status=result_payload.get('status'),
        freeze_reason=(output.get('state') or {}).get('freeze_reason'),
        runtime_mode=(output.get('state') or {}).get('runtime_mode'),
        failure_count=0,
        backoff_seconds=0.0,
        event_log_path=str(worker.event_log_path),
    )
    worker._write_status(
        phase=phase,
        summary=summary,
        last_exception=None,
        started_at=_utc_now(),
        completed_at=_utc_now(),
        market=market,
        output=output,
        config_validation=config_validation,
        audit_artifact_paths=artifact_paths,
    )
    worker._write_dispatch_preview_audit(run_id=run_id, market=market, output=publishable_output)
    return {
        'confirm_summary': confirm_summary,
        'dispatch_preview': dispatch_preview,
        'audit_artifact_paths': artifact_paths,
        'discord_send_attempt': discord_send_attempt,
    }


def _execute_phase(*, phase_name: str, run_id: str, runtime: dict[str, Any], market: MarketSnapshot, plan: FinalActionPlan, config_validation: dict[str, Any]) -> dict[str, Any]:
    state_store: JsonStateStore = runtime['state_store']
    reconcile_module: BinancePreRunReconcileModule = runtime['reconcile_module']
    executor: BinanceRealExecutor = runtime['executor']
    worker: RuntimeWorker = runtime['worker']
    event_log: EventLogWriter = runtime['event_log']

    state_before = state_store.load_state()
    reconciled_state = reconcile_module.reconcile(market, state_before)
    state_store.save_state(reconciled_state)
    result = executor.execute(plan, market, reconciled_state)
    state_store.save_result(reconciled_state, result)
    output = {
        'market': asdict(market),
        'state': asdict(state_store.load_state()),
        'plan': asdict(plan),
        'result': asdict(result),
    }
    if phase_name in {'open', 'management_stop_update'}:
        output = worker._maybe_advance_execution_orchestration(market=market, output=output)
        output = worker._persist_non_execution_strategy_state(output)
        output = worker._attach_execution_retry_backoff(output)
    else:
        output = worker._attach_execution_retry_backoff(output)

    final_state = type(state_store.load_state())(**output['state'])
    final_result = type(result)(**output['result'])
    if phase_name == 'cleanup_close' and _is_runtime_strict_flat_ready({'state': {'state': output['state']}}):
        final_result = apply_flat_reset_to_result(
            final_result,
            state=final_state,
            state_ts=final_state.state_ts,
            account_equity=final_state.account_equity,
            available_margin=final_state.available_margin,
        )
        output['result'] = asdict(final_result)
    state_store.save_state(final_state)
    state_store.save_result(state_store.load_state(), final_result)

    artifacts = _save_status_and_artifacts(
        worker=worker,
        run_id=run_id,
        market=market,
        output=output,
        config_validation=config_validation,
        phase=f'manual_acceptance_{phase_name}',
    )
    event_log.append(
        f'manual_acceptance_{phase_name}',
        {
            'run_id': run_id,
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'plan': output['plan'],
            'result': output['result'],
            'audit_artifact_paths': artifacts['audit_artifact_paths'],
        },
    )
    runtime_snapshot = _snapshot_runtime_files(runtime['runtime_dir'])
    return {
        'output': output,
        'artifacts': artifacts,
        'runtime_snapshot': runtime_snapshot,
    }


def _attempt_cleanup_close(*, runtime: dict[str, Any], symbol: str, config_validation: dict[str, Any], summary: dict[str, Any], run_id: str) -> dict[str, Any] | None:
    readonly_client: BinanceReadOnlyClient = runtime['readonly_client']
    after = _ensure_flat(readonly_client, symbol)
    if after.get('is_flat') and after.get('open_orders_count') == 0:
        return None
    close_price = _load_live_price(readonly_client, symbol)
    close_market = _build_manual_market(symbol=symbol, current_price=close_price)
    close_plan = _make_close_plan()
    cleanup_phase = _execute_phase(
        phase_name='cleanup_close',
        run_id=f'{run_id}_cleanup_close',
        runtime=runtime,
        market=close_market,
        plan=close_plan,
        config_validation=config_validation,
    )
    summary['cleanup_phase'] = cleanup_phase
    summary['after_cleanup_retry'] = _ensure_flat(readonly_client, symbol)
    return cleanup_phase


def main() -> int:
    parser = argparse.ArgumentParser(description='专项真实验收入口：真实开单 + closePosition protection + readonly/reconcile/writeback/收尾')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--side', choices=['long', 'short'], default='long')
    parser.add_argument('--target-notional', type=float, default=21.0)
    parser.add_argument('--hold-seconds', type=float, default=2.0)
    parser.add_argument('--out-dir', default='docs/deploy_v6c/samples/real_trade_sampling/manual_runs')
    args = parser.parse_args()

    env_file = Path(args.env_file)
    config = load_binance_env(env_file)
    config_validation = validate_runtime_config(config).as_dict()
    if not config_validation.get('ok'):
        raise SystemExit(f'runtime config invalid: {config_validation.get("blockers") or []}')
    if str(args.symbol).upper() != str(config.symbol).upper():
        raise SystemExit(f'symbol mismatch: cli={args.symbol} env={config.symbol}')

    runtime = _prepare_runtime(config)
    readonly_client: BinanceReadOnlyClient = runtime['readonly_client']
    executor: BinanceRealExecutor = runtime['executor']
    runtime_dir: Path = runtime['runtime_dir']

    run_id = _run_id()
    out_dir = Path(args.out_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / f'{run_id}_{args.symbol}_full_chain_acceptance_summary.json'

    summary: dict[str, Any] = {
        'run_id': run_id,
        'symbol': str(args.symbol).upper(),
        'side': args.side,
        'target_notional': float(args.target_notional),
        'hold_seconds': float(args.hold_seconds),
        'env_file': str(env_file),
        'runtime_dir': str(runtime_dir),
        'config_validation': config_validation,
    }

    before = _ensure_flat(readonly_client, args.symbol)
    summary['before'] = before
    if not before['is_flat'] or before['open_orders_count'] != 0:
        summary['aborted'] = True
        summary['abort_reason'] = 'precheck_not_flat_or_has_open_orders'
        _write_json(summary_path, summary)
        print(json.dumps({'ok': False, 'summary_path': str(summary_path), 'abort_reason': summary['abort_reason']}, ensure_ascii=False))
        return 2

    open_price = _load_live_price(readonly_client, args.symbol)
    quantity = _normalize_open_qty(
        executor=executor,
        target_notional=float(args.target_notional),
        current_price=open_price,
        symbol=args.symbol,
    )
    stop_price = round(open_price * (0.99 if args.side == 'long' else 1.01), 2)
    open_market = _build_manual_market(symbol=args.symbol, current_price=open_price)
    open_plan = _make_open_plan(side=args.side, quantity=quantity, current_price=open_price, stop_price=stop_price)

    open_phase = _execute_phase(
        phase_name='open',
        run_id=f'{run_id}_open',
        runtime=runtime,
        market=open_market,
        plan=open_plan,
        config_validation=config_validation,
    )
    summary['open_phase'] = open_phase

    protective_phase = None
    if _should_run_protective_rebuild(open_phase):
        protective_price = _load_live_price(readonly_client, args.symbol)
        protective_market = _build_manual_market(symbol=args.symbol, current_price=protective_price)
        protective_plan = _make_protective_rebuild_plan(state=((open_phase.get('runtime_snapshot') or {}).get('state') or {}).get('state') or {})
        protective_phase = _execute_phase(
            phase_name='protective_rebuild',
            run_id=f'{run_id}_protective_rebuild',
            runtime=runtime,
            market=protective_market,
            plan=protective_plan,
            config_validation=config_validation,
        )
        summary['protective_phase'] = protective_phase

    time.sleep(float(args.hold_seconds))

    management_stop_update_phase = None
    updated_stop_price = None
    rebuild_after_management_stop_update_phase = None
    protective_anchor_for_close = protective_phase or open_phase
    protective_anchor_state = (((protective_anchor_for_close or {}).get('runtime_snapshot') or {}).get('state') or {}).get('state') or {}
    if protective_anchor_state.get('active_side') in {'long', 'short'} and protective_anchor_state.get('stop_price') is not None:
        current_stop = float(protective_anchor_state['stop_price'])
        updated_stop_price = round(current_stop + (-5.0 if args.side == 'short' else 5.0), 2)
        management_price = _load_live_price(readonly_client, args.symbol)
        management_market = _build_manual_market(symbol=args.symbol, current_price=management_price)
        management_plan = _make_management_stop_update_plan(state=protective_anchor_state, updated_stop_price=updated_stop_price)
        management_stop_update_phase = _execute_phase(
            phase_name='management_stop_update',
            run_id=f'{run_id}_management_stop_update',
            runtime=runtime,
            market=management_market,
            plan=management_plan,
            config_validation=config_validation,
        )
        summary['management_stop_update_phase'] = management_stop_update_phase

        orchestration = ((((management_stop_update_phase.get('output') or {}).get('result')) or {}).get('trade_summary') or {}).get('orchestration') or {}
        if orchestration.get('auto_advanced'):
            rebuild_after_management_stop_update_phase = {
                'output': {
                    'market': management_stop_update_phase['output'].get('market'),
                    'state': management_stop_update_phase['output'].get('state'),
                    'plan': orchestration.get('rebuild_plan'),
                    'result': orchestration.get('rebuild_result'),
                },
                'artifacts': management_stop_update_phase.get('artifacts'),
                'runtime_snapshot': management_stop_update_phase.get('runtime_snapshot'),
            }
            summary['rebuild_after_management_stop_update_phase'] = rebuild_after_management_stop_update_phase
            protective_anchor_for_close = rebuild_after_management_stop_update_phase

    close_price = _load_live_price(readonly_client, args.symbol)
    close_market = _build_manual_market(symbol=args.symbol, current_price=close_price)
    close_plan = _make_close_plan(clear_protective_orders=True)
    close_phase = _execute_phase(
        phase_name='close',
        run_id=f'{run_id}_close',
        runtime=runtime,
        market=close_market,
        plan=close_plan,
        config_validation=config_validation,
    )
    summary['close_phase'] = close_phase

    final_flat = _ensure_flat(readonly_client, args.symbol)
    if not final_flat.get('is_flat') or final_flat.get('open_orders_count') != 0:
        _attempt_cleanup_close(runtime=runtime, symbol=args.symbol, config_validation=config_validation, summary=summary, run_id=run_id)
        final_flat = _ensure_flat(readonly_client, args.symbol)
    summary['after_cleanup'] = final_flat
    summary['runtime_files_final'] = _snapshot_runtime_files(runtime_dir)

    open_result = (((open_phase.get('output') or {}).get('result')) or {})
    open_trade_summary = open_result.get('trade_summary') or {}
    open_state = (((open_phase.get('output') or {}).get('state')) or {})
    protective_result = (((protective_phase or {}).get('output') or {}).get('result')) or {}
    protective_trade_summary = protective_result.get('trade_summary') or {}
    rebuild_after_management_stop_update_output = ((rebuild_after_management_stop_update_phase or {}).get('output') or {})
    rebuild_after_management_stop_update_state = rebuild_after_management_stop_update_output.get('state') or {}
    rebuild_after_management_stop_update_result = (rebuild_after_management_stop_update_output.get('result') or {})
    rebuild_after_management_stop_update_summary = rebuild_after_management_stop_update_result.get('trade_summary') or {}
    protection_anchor_result = rebuild_after_management_stop_update_result or protective_result or open_result
    protection_anchor_summary = rebuild_after_management_stop_update_summary or protective_trade_summary or open_trade_summary
    runtime_files_final = summary['runtime_files_final']
    runtime_state_final = ((runtime_files_final.get('state') or {}).get('state')) or {}
    runtime_latest_result = (runtime_files_final.get('runtime_status') or {}).get('latest_result_summary') or {}
    runtime_latest_trade_summary = runtime_latest_result.get('trade_summary') or {}
    writeback_state = rebuild_after_management_stop_update_state or runtime_state_final or open_state
    writeback_protective_orders = list(writeback_state.get('exchange_protective_orders') or [])
    writeback_protective_validation = runtime_latest_trade_summary.get('protective_validation') or protection_anchor_summary.get('protective_validation') or {}
    close_result = (((close_phase.get('output') or {}).get('result')) or {})
    close_trade_summary = close_result.get('trade_summary') or {}

    summary['management_stop_update_target_stop_price'] = updated_stop_price

    management_orchestration = (((((management_stop_update_phase or {}).get('output') or {}).get('result')) or {}).get('trade_summary') or {}).get('orchestration') or {}

    protection_submitted_via_initial_followup = bool(
        (protective_result.get('status') not in {'DRY_RUN', 'SKIPPED', None})
        and (protective_result.get('exchange_order_ids') or protective_trade_summary.get('exchange_order_ids'))
    )
    protection_submitted_via_management_rebuild = bool(
        (rebuild_after_management_stop_update_result.get('status') not in {'DRY_RUN', 'SKIPPED', None})
        and (rebuild_after_management_stop_update_result.get('exchange_order_ids') or rebuild_after_management_stop_update_summary.get('exchange_order_ids'))
    )
    protection_visible_in_writeback = bool(((writeback_protective_validation.get('summary') or {}).get('protective_order_count', 0) >= 1) and writeback_protective_validation.get('ok'))

    checks = {
        'real_open_submitted': bool((open_result.get('status') not in {'DRY_RUN', 'SKIPPED'}) and (open_result.get('exchange_order_ids') or open_trade_summary.get('exchange_order_ids'))),
        'real_protection_submitted': bool(
            protection_submitted_via_initial_followup
            or protection_submitted_via_management_rebuild
            or protection_visible_in_writeback
        ),
        'management_phase_present': management_stop_update_phase is not None,
        'management_trigger_phase': management_orchestration.get('trigger_phase') == 'management_stop_update_pending_protective',
        'rebuild_auto_advanced': bool(management_orchestration.get('auto_advanced')),
        'rebuild_submitted': bool((rebuild_after_management_stop_update_result.get('status') not in {'DRY_RUN', 'SKIPPED', None}) and rebuild_after_management_stop_update_result.get('exchange_order_ids')),
        'rebuild_validated': protection_visible_in_writeback,
        'updated_stop_synced': bool(
            updated_stop_price is not None
            and abs(float(((((management_stop_update_phase or {}).get('output') or {}).get('state')) or {}).get('stop_price') or 0.0) - float(updated_stop_price)) <= 1e-9
            and abs(float(writeback_state.get('stop_price') or 0.0) - float(updated_stop_price)) <= 1e-9
            and any(abs(float(item.get('stop_price') or 0.0) - float(updated_stop_price)) <= 1e-9 for item in writeback_protective_orders)
        ),
        'readonly_algo_bridge_readback': bool(writeback_protective_validation.get('ok')),
        'state_and_runtime_writeback': bool(
            runtime_state_final.get('protective_order_status') in {'ACTIVE', 'NONE'}
            and runtime_latest_result is not None
            and (
                runtime_state_final.get('protective_order_status') == 'NONE'
                or len(list(runtime_state_final.get('exchange_protective_orders') or [])) >= 1
            )
        ),
        'posttrade_confirm': bool(protection_anchor_result.get('confirmation_status') in {'CONFIRMED', 'POSITION_CONFIRMED'}),
        'reconcile': bool(protection_anchor_result.get('reconcile_status') == 'OK' and close_result.get('reconcile_status') == 'OK'),
        'cleanup_flat_no_open_orders': bool(final_flat.get('is_flat') and final_flat.get('open_orders_count') == 0),
        'cleanup_runtime_strict_flat_ready': _is_runtime_strict_flat_ready(runtime_files_final),
    }
    summary['checks'] = checks
    summary['passed'] = all(checks.values())

    _write_json(summary_path, summary)
    print(json.dumps({'ok': summary['passed'], 'summary_path': str(summary_path)}, ensure_ascii=False))
    return 0 if summary['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
