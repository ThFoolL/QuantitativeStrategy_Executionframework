from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .binance_exception_policy import ACTION_READONLY_RECHECK
from .binance_readonly import BinanceReadOnlyClient
from .binance_reconcile import ExchangeSnapshot, ReconcileInput, reconcile_pre_run
from .engine import LiveEngine
from .executor_real import BinanceCancelOrderRequest, BinanceOrderRequest, BinanceRealExecutor
from .protective_orders import split_open_orders
from .binance_posttrade import SimulatedExecutionReceipt, build_confirm_context
from .feature_builder import LiveFeatureBuilder
from .market_data import BinanceReadOnlyMarketDataProvider, MarketDataProvider, StubMarketDataProvider, build_market_snapshot
from .models import FinalActionPlan, LiveStateSnapshot, MarketSnapshot
from .runtime_env import BinanceEnvConfig, load_binance_env, validate_runtime_config
from .runtime_status_cli import build_dispatch_preview, build_execution_confirm_summary
from .state_store import build_flat_reset_state_updates
from .strategy_protection_intent import (
    build_strategy_protection_intent,
    derive_protective_rebuild_block_reason,
)
from .async_operation import attach_execution_confirm_async_operation, attach_protection_followup_async_operation
from .discord_sender_bridge import MessageToolDiscordSender, build_discord_transport
from .runtime_guard import (
    RECOVER_RESULT_ALLOWED,
    RECOVER_RESULT_BLOCKED,
    RuntimeFreezeController,
    append_recover_record,
    build_readonly_recheck_recover_check,
)
from .unified_risk_action import (
    RECOVER_STAGE_PROTECTION_PENDING_CONFIRM,
    RECOVER_STAGE_RECOVER_READY,
    RISK_ACTION_OBSERVE,
    RISK_ACTION_RECOVER_PROTECTION,
    classify_manual_review_from_notes,
    derive_recover_stage,
    derive_risk_action,
)


READONLY_RECHECK_PENDING = 'readonly_recheck_pending'
READONLY_RECHECK_QUERY_FAILED = 'readonly_recheck_query_failed'
READONLY_RECHECK_OBSERVE = 'readonly_recheck_observe'
READONLY_RECHECK_RECOVER_READY = 'readonly_recheck_recover_ready'
READONLY_RECHECK_FREEZE = 'readonly_recheck_freeze'
READONLY_RECHECK_SHARED_MAX_ATTEMPTS = 30
READONLY_RECHECK_RETRY_INTERVAL_SECONDS = 5.0
READONLY_RECHECK_RETRYABLE_CATEGORIES = {'pending', 'query_failed'}
# Only local strategy-management fields are allowed to persist on non-execution rounds.
LOCAL_STRATEGY_STATE_UPDATE_WHITELIST = {
    'signal_ts',
    'tp_price',
    'rev_window',
    'hold_bars',
    'high_water_r',
    'degrade_state',
    'p1_armed',
    'p2_armed',
    'stop_price',
    'equity_at_entry',
    'risk_amount',
    'risk_per_unit',
    'last_conflict_resolution',
}
from .state_store import JsonStateStore, apply_flat_reset_to_state, apply_pre_run_reconcile
from .strategy_adapter_selector import build_strategy_adapter_from_config, normalize_strategy_adapter_name


class BinancePreRunReconcileModule:
    MAX_ACCOUNT_SNAPSHOT_RETRIES = 5
    ACCOUNT_SNAPSHOT_RETRY_WINDOW_SECONDS = 60.0

    def __init__(self, state_store: JsonStateStore, readonly_client: BinanceReadOnlyClient):
        self.state_store = state_store
        self.readonly_client = readonly_client
        self.last_account_snapshot_summary: dict[str, Any] = {}

    def reconcile(self, market: MarketSnapshot, state: LiveStateSnapshot) -> LiveStateSnapshot:
        account, retry_trace = self._load_account_snapshot_with_retries()
        position = self.readonly_client.get_position_snapshot(market.symbol)
        protection_ids = [
            str(item.get('client_order_id'))
            for item in (state.exchange_protective_orders or [])
            if isinstance(item, dict) and item.get('client_order_id')
        ]
        open_orders = self.readonly_client.get_open_orders(market.symbol, client_order_ids=protection_ids)
        open_orders = self._merge_bootstrap_protective_orders(symbol=market.symbol, state=state, open_orders=open_orders)
        self.last_account_snapshot_summary = self._summarize_account_snapshot(account, retry_trace=retry_trace)
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(account=account, position=position, open_orders=open_orders),
                last_result=self.state_store.load_last_result(),
            )
        )

        account_equity = account.account_equity
        available_margin = account.available_margin
        if getattr(account, 'validity_status', 'OK') != 'OK':
            account_equity = state.account_equity
            available_margin = state.available_margin

        return apply_pre_run_reconcile(
            state,
            self.state_store.load_last_result(),
            state_ts=market.decision_ts,
            consistency_status=decision.status,
            account_equity=account_equity,
            available_margin=available_margin,
            exchange_position_side=position.side,
            exchange_position_qty=position.qty,
            exchange_entry_price=position.entry_price,
            freeze_reason=decision.freeze_reason,
            can_open_new_position=decision.can_open_new_position,
            can_modify_position=decision.can_modify_position,
        )

    def _load_account_snapshot_with_retries(self) -> tuple[Any, list[dict[str, Any]]]:
        trace: list[dict[str, Any]] = []
        started = time.monotonic()
        last_snapshot = None
        for attempt in range(1, self.MAX_ACCOUNT_SNAPSHOT_RETRIES + 1):
            snapshot = self.readonly_client.get_account_snapshot()
            last_snapshot = snapshot
            validity_status = getattr(snapshot, 'validity_status', 'OK')
            invalid_reasons = list(getattr(snapshot, 'invalid_reasons', ()) or [])
            trace.append(
                {
                    'attempt': attempt,
                    'validity_status': validity_status,
                    'invalid_reasons': invalid_reasons,
                    'account_equity': getattr(snapshot, 'account_equity', None),
                    'available_margin': getattr(snapshot, 'available_margin', None),
                }
            )
            if validity_status == 'OK':
                return snapshot, trace
            if attempt >= self.MAX_ACCOUNT_SNAPSHOT_RETRIES:
                break
            elapsed = time.monotonic() - started
            if elapsed >= self.ACCOUNT_SNAPSHOT_RETRY_WINDOW_SECONDS:
                break
            remaining_attempts = self.MAX_ACCOUNT_SNAPSHOT_RETRIES - attempt
            remaining_window = max(self.ACCOUNT_SNAPSHOT_RETRY_WINDOW_SECONDS - elapsed, 0.0)
            sleep_seconds = min(1.0, remaining_window / max(remaining_attempts, 1))
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        return last_snapshot, trace

    def _merge_bootstrap_protective_orders(self, *, symbol: str, state: LiveStateSnapshot, open_orders: list[Any]) -> list[Any]:
        protective_open_orders, regular_open_orders = split_open_orders(open_orders)
        if protective_open_orders:
            return list(open_orders)
        if state.active_strategy not in {'trend', 'rev'}:
            return list(open_orders)
        if state.exchange_position_side not in {'long', 'short'}:
            return list(open_orders)
        if float(state.exchange_position_qty or 0.0) <= 0.0:
            return list(open_orders)
        if not state.strategy_entry_time or state.stop_price is None:
            return list(open_orders)

        candidates = ['hard_stop']
        if state.active_strategy == 'rev' and state.tp_price is not None:
            candidates.append('take_profit')

        bootstrap_orders = []
        for kind in candidates:
            client_order_id = BinanceRealExecutor._build_client_order_id(state.strategy_entry_time, f'protect-{kind}')
            try:
                snapshot = self.readonly_client.get_order(symbol=symbol, client_order_id=client_order_id)
            except Exception:
                continue
            bootstrap_orders.append(snapshot)
        return [*regular_open_orders, *bootstrap_orders]

    @staticmethod
    def _summarize_account_snapshot(account: Any, *, retry_trace: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        raw = getattr(account, 'raw', {}) or {}
        assets = raw.get('assets') if isinstance(raw, dict) else []
        nonzero_asset_count = 0
        for asset in assets or []:
            try:
                wallet = float(asset.get('walletBalance', 0.0))
            except (TypeError, ValueError):
                wallet = 0.0
            try:
                available = float(asset.get('availableBalance', 0.0))
            except (TypeError, ValueError):
                available = 0.0
            if abs(wallet) > 0 or abs(available) > 0:
                nonzero_asset_count += 1
        key_fields = {}
        for key in (
            'totalWalletBalance',
            'availableBalance',
            'totalMarginBalance',
            'totalCrossWalletBalance',
            'totalCrossUnPnl',
            'totalUnrealizedProfit',
            'multiAssetsMargin',
        ):
            if isinstance(raw, dict) and key in raw:
                key_fields[key] = raw.get(key)
        return {
            'account_equity': getattr(account, 'account_equity', None),
            'available_margin': getattr(account, 'available_margin', None),
            'validity_status': getattr(account, 'validity_status', None),
            'invalid_reasons': list(getattr(account, 'invalid_reasons', ()) or []),
            'assets_count': len(assets or []),
            'nonzero_asset_count': nonzero_asset_count,
            'positions_count': len((raw.get('positions') or [])) if isinstance(raw, dict) else None,
            'key_fields': key_fields,
            'retry_trace': list(retry_trace or []),
            'retry_attempts': len(retry_trace or []),
        }


class RuntimeStatusStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, payload: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(self.path.suffix + '.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        tmp.replace(self.path)


class EventLogWriter:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event_type: str, payload: dict[str, Any]) -> None:
        row = {
            'event_ts': datetime.now(timezone.utc).isoformat(),
            'event_type': event_type,
            **payload,
        }
        with self.path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + '\n')


class AuditArtifactWriter:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, category: str, run_id: str, payload: dict[str, Any]) -> str:
        category_dir = self.root / category
        category_dir.mkdir(parents=True, exist_ok=True)
        path = category_dir / f'{run_id}.json'
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(path)


@dataclass(frozen=True)
class WorkerRunSummary:
    run_id: str
    phase: str
    symbol: str
    decision_ts: str | None
    consistency_status: str | None
    plan_action: str | None
    result_status: str | None
    freeze_reason: str | None
    runtime_mode: str | None
    failure_count: int
    backoff_seconds: float
    event_log_path: str


@dataclass(frozen=True)
class SchedulerConfig:
    interval_seconds: int = 300
    max_backoff_seconds: int = 300


class FixedIntervalScheduler:
    def __init__(self, config: SchedulerConfig | None = None):
        self.config = config or SchedulerConfig()

    def next_run_at(self, now: datetime) -> datetime:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        interval = max(1, int(self.config.interval_seconds))
        epoch = int(now.timestamp())
        next_epoch = ((epoch // interval) + 1) * interval
        return datetime.fromtimestamp(next_epoch, tz=timezone.utc)

    def sleep_seconds_until_next_run(self, now: datetime) -> float:
        return max((self.next_run_at(now) - now).total_seconds(), 0.0)


@dataclass(frozen=True)
class ReadonlyRecheckDecision:
    status: str
    action: str
    summary: dict[str, Any]
    state_updates: dict[str, Any]
    result_updates: dict[str, Any]
    should_freeze: bool
    freeze_reason: str | None
    recover_check: dict[str, Any]


class RuntimeWorker:
    ORCHESTRATION_ENTRY_PENDING_PROTECTIVE = 'entry_confirmed_pending_protective'
    ORCHESTRATION_MANAGEMENT_PENDING_PROTECTIVE = 'management_stop_update_pending_protective'
    ORCHESTRATION_PROTECTIVE_REBUILD_REASON = 'protective_rebuild_after_entry_confirmation'
    ORCHESTRATION_PROTECTIVE_REBUILD_MANAGEMENT_REASON = 'protective_rebuild_after_management_stop_update'
    ORCHESTRATION_PROTECTIVE_REBUILD_BLOCK_REASON = 'protective_rebuild_blocked'

    def __init__(
        self,
        *,
        config: BinanceEnvConfig,
        state_store: JsonStateStore,
        engine: LiveEngine,
        market_provider: MarketDataProvider,
        status_store: RuntimeStatusStore,
        event_log: EventLogWriter,
        scheduler: FixedIntervalScheduler | None = None,
        freeze_controller: RuntimeFreezeController | None = None,
    ):
        self.config = config
        self.state_store = state_store
        self.engine = engine
        self.market_provider = market_provider
        self.status_store = status_store
        self.event_log = event_log
        self.scheduler = scheduler or FixedIntervalScheduler()
        self.freeze_controller = freeze_controller or RuntimeFreezeController()
        self.runtime_status_path = self.status_store.path
        self.event_log_path = self.event_log.path
        self.audit_writer = AuditArtifactWriter(self.runtime_status_path.parent / 'audit_artifacts')
        self.last_runtime_config_validation: dict[str, Any] = {}

    def _strategy_ts_already_processed(self, strategy_ts: str | None) -> bool:
        if not strategy_ts:
            return False
        state = self.state_store.load_state()
        return getattr(state, 'last_processed_strategy_ts', None) == strategy_ts

    def _mark_strategy_ts_processed(self, strategy_ts: str | None) -> None:
        if not strategy_ts:
            return
        state = self.state_store.load_state()
        if getattr(state, 'last_processed_strategy_ts', None) == strategy_ts:
            return
        state.last_processed_strategy_ts = strategy_ts
        save_state = getattr(self.state_store, 'save_state', None)
        if callable(save_state):
            save_state(state)

    def _persist_non_execution_strategy_state(self, output: dict[str, Any]) -> dict[str, Any]:
        plan_payload = dict(output.get('plan') or {})
        result_payload = dict(output.get('result') or {})
        if bool(plan_payload.get('requires_execution')):
            return output
        if plan_payload.get('action_type') not in {'state_update', 'hold'}:
            return output

        raw_updates = dict(result_payload.get('state_updates') or {})
        state_updates = {
            key: value
            for key, value in raw_updates.items()
            if key in LOCAL_STRATEGY_STATE_UPDATE_WHITELIST
        }
        if not state_updates and raw_updates == state_updates:
            return output

        current_state = self.state_store.load_state()
        next_state = replace(current_state)
        for key, value in state_updates.items():
            setattr(next_state, key, value)

        save_state = getattr(self.state_store, 'save_state', None)
        if callable(save_state):
            save_state(next_state)

        updated = dict(output)
        updated['state'] = asdict(next_state)
        result_payload['state_updates'] = {
            **raw_updates,
            **state_updates,
        }
        updated['result'] = result_payload
        return updated

    def run_once(self, decision_time: datetime | None = None) -> dict[str, Any]:
        now = decision_time or datetime.now(timezone.utc)
        run_id = now.strftime('%Y%m%dT%H%M%S%fZ')
        started_at = datetime.now(timezone.utc)
        failure_count = self._read_failure_count()
        market: MarketSnapshot | None = None

        try:
            config_validation = validate_runtime_config(self.config)
            config_validation_dict = {
                **config_validation.as_dict(),
                'strategy_adapter': {
                    'selected': normalize_strategy_adapter_name(self.config.strategy_adapter),
                    'requested': self.config.strategy_adapter,
                },
            }
            self.last_runtime_config_validation = config_validation_dict
            market = build_market_snapshot(
                provider=self.market_provider,
                symbol=self.config.symbol,
                decision_time=now,
            )
            self.event_log.append(
                'run_start',
                {
                    'run_id': run_id,
                    'symbol': self.config.symbol,
                    'decision_ts': market.decision_ts,
                    'dry_run': self.config.dry_run,
                    'submit_enabled': self.config.submit_enabled,
                    'strategy_ts': market.strategy_ts,
                    'execution_attributed_bar': market.execution_attributed_bar,
                    'config_validation': config_validation_dict,
                },
            )
            self._write_status(
                phase='starting',
                summary=WorkerRunSummary(
                    run_id=run_id,
                    phase='starting',
                    symbol=self.config.symbol,
                    decision_ts=market.decision_ts,
                    consistency_status=None,
                    plan_action=None,
                    result_status=None,
                    freeze_reason=None,
                    runtime_mode=None,
                    failure_count=failure_count,
                    backoff_seconds=0.0,
                    event_log_path=str(self.event_log_path),
                ),
                last_exception=None,
                config_validation=config_validation_dict,
            )

            if self._strategy_ts_already_processed(market.strategy_ts):
                duplicate_output = {
                    'market': asdict(market),
                    'state': asdict(self.state_store.load_state()),
                    'plan': {
                        'plan_ts': market.decision_ts,
                        'bar_ts': market.bar_ts,
                        'action_type': 'hold',
                        'target_strategy': None,
                        'target_side': None,
                        'reason': 'duplicate_strategy_ts_blocked_by_runtime',
                        'qty_mode': 'none',
                        'qty': None,
                        'price_hint': None,
                        'stop_price': None,
                        'risk_fraction': None,
                        'conflict_context': {'strategy_ts': market.strategy_ts},
                        'requires_execution': False,
                        'close_reason': None,
                    },
                    'plan_debug': None,
                    'result': {
                        'result_ts': market.decision_ts,
                        'bar_ts': market.bar_ts,
                        'status': 'SKIPPED',
                        'action_type': 'hold',
                        'executed_side': None,
                        'executed_qty': 0.0,
                        'avg_fill_price': None,
                        'fees': 0.0,
                        'exchange_order_ids': None,
                        'post_position_side': None,
                        'post_position_qty': 0.0,
                        'post_entry_price': None,
                        'reconcile_status': 'OK',
                        'error_code': None,
                        'error_message': None,
                        'should_freeze': False,
                        'freeze_reason': None,
                        'state_updates': None,
                        'execution_phase': 'blocked_duplicate_strategy_ts',
                        'confirmation_status': 'NOT_REQUIRED',
                        'confirmed_order_status': 'NOT_REQUIRED',
                        'trade_summary': {
                            'execution_ref': {
                                'symbol': market.symbol,
                                'decision_ts': market.decision_ts,
                                'bar_ts': market.bar_ts,
                                'plan_ts': market.strategy_ts,
                                'action_type': 'hold',
                                'target_side': None,
                            },
                            'notes': ['duplicate_strategy_ts_blocked_by_runtime'],
                        },
                    },
                }
                duplicate_output['result']['reconcile_status'] = duplicate_output['state'].get('consistency_status', 'UNKNOWN')
                output = duplicate_output
            else:
                output = self.engine.run_once(market)
                self._mark_strategy_ts_processed(market.strategy_ts)
            output = self._maybe_advance_execution_orchestration(market=market, output=output)
            output = self._persist_non_execution_strategy_state(output)
            output = self._attach_execution_retry_backoff(output)
            updated_state = self.state_store.load_state()
            recover_info = self._maybe_attempt_recover(updated_state, run_id=run_id)
            if recover_info is not None:
                updated_state = self.state_store.load_state()
                output['state'] = asdict(updated_state)
                load_last_result = getattr(self.state_store, 'load_last_result', None)
                if callable(load_last_result):
                    refreshed_result = load_last_result()
                    if refreshed_result is not None:
                        output['result'] = asdict(refreshed_result)

            self.event_log.append(
                'reconcile_result',
                {
                    'run_id': run_id,
                    'consistency_status': output['state'].get('consistency_status'),
                    'freeze_reason': output['state'].get('freeze_reason'),
                    'can_open_new_position': output['state'].get('can_open_new_position'),
                    'can_modify_position': output['state'].get('can_modify_position'),
                },
            )
            self.event_log.append(
                'plan_summary',
                {
                    'run_id': run_id,
                    'action_type': output['plan'].get('action_type'),
                    'target_strategy': output['plan'].get('target_strategy'),
                    'target_side': output['plan'].get('target_side'),
                    'reason': output['plan'].get('reason'),
                    'requires_execution': output['plan'].get('requires_execution'),
                    'plan_debug': output.get('plan_debug'),
                },
            )
            self.event_log.append(
                'execute_summary',
                {
                    'run_id': run_id,
                    'status': output['result'].get('status'),
                    'execution_phase': output['result'].get('execution_phase'),
                    'confirmation_status': output['result'].get('confirmation_status'),
                    'freeze_reason': output['result'].get('freeze_reason'),
                    'trade_summary': output['result'].get('trade_summary'),
                },
            )
            result_payload = output.get('result') or {}
            readonly_recheck = self._maybe_run_readonly_recheck(
                run_id=run_id,
                market=market,
                output=output,
            )
            if readonly_recheck is not None:
                output = self._apply_readonly_recheck_output(output, readonly_recheck)
                output = self._attach_execution_retry_backoff(output)
                state_after_recheck = output.get('state') or {}
                save_state = getattr(self.state_store, 'save_state', None)
                if callable(save_state):
                    current_state = self.state_store.load_state()
                    next_state = replace(current_state)
                    for key, value in state_after_recheck.items():
                        if hasattr(next_state, key):
                            setattr(next_state, key, value)
                    save_state(next_state)
                    if next_state.runtime_mode == 'FROZEN':
                        recover_info = self._maybe_attempt_recover(next_state, run_id=run_id)
                        if recover_info is not None:
                            refreshed_state = self.state_store.load_state()
                            output['state'] = asdict(refreshed_state)
                            load_last_result = getattr(self.state_store, 'load_last_result', None)
                            if callable(load_last_result):
                                refreshed_result = load_last_result()
                                if refreshed_result is not None:
                                    output['result'] = asdict(refreshed_result)
                result_payload = output.get('result') or {}
            confirm_summary = build_execution_confirm_summary(result_payload)
            self.event_log.append(
                'confirm_summary',
                {
                    'run_id': run_id,
                    **confirm_summary,
                },
            )
            object.__setattr__(self.config, '_runtime_latest_market_summary', {
                'decision_ts': market.decision_ts,
                'bar_ts': market.bar_ts,
                'strategy_ts': market.strategy_ts,
                'execution_attributed_bar': market.execution_attributed_bar,
                'source_status': market.source_status,
            })
            publishable_payload = self._select_publishable_output(output)
            dispatch_preview = build_dispatch_preview(self.config, publishable_payload['result'], publishable_payload['state'])
            self.event_log.append(
                'sender_dispatch_preview',
                {
                    'run_id': run_id,
                    'publish_source_action': publishable_payload['result'].get('action_type'),
                    **dispatch_preview,
                },
            )
            artifact_paths = self._write_audit_artifacts(
                run_id=run_id,
                market=market,
                output=output,
                confirm_summary=confirm_summary,
                dispatch_preview=dispatch_preview,
                config_validation=config_validation_dict,
            )
            self.event_log.append(
                'discord_send_gate',
                {
                    'run_id': run_id,
                    **(dispatch_preview.get('send_gate') or {}),
                    'idempotency_key': dispatch_preview.get('idempotency_key'),
                },
            )
            should_attempt_send = self._should_attempt_discord_send(output=output, publishable_output=publishable_payload)
            discord_send_attempt = self._maybe_run_discord_sender(publishable_payload) if should_attempt_send else {
                'attempted': False,
                'sent': False,
                'reason': 'no_new_publishable_event',
                'payload_kind': None,
                'rehearsal_mode': False,
                'idempotency_key': None,
                'receipt': None,
                'receipt_store_path': getattr(self.config, 'discord_send_receipt_log_path', None),
                'failure': None,
                'provider_response': None,
                'transport_name': getattr(self._build_sender().transport, 'transport_name', self._build_sender().transport.__class__.__name__),
            }
            if discord_send_attempt is not None:
                self.event_log.append(
                    'discord_send_attempt',
                    {
                        'run_id': run_id,
                        **discord_send_attempt,
                    },
                )
                receipt = discord_send_attempt.get('receipt') or {}
                if receipt:
                    self.event_log.append(
                        'discord_send_receipt',
                        {
                            'run_id': run_id,
                            'payload_kind': receipt.get('payload_kind'),
                            'idempotency_key': receipt.get('idempotency_key'),
                            'message_id': receipt.get('message_id'),
                            'channel_id': receipt.get('provider_channel_id') or receipt.get('target'),
                            'transport_name': receipt.get('transport_name'),
                            'sent_at': receipt.get('sent_at'),
                            'status': receipt.get('status'),
                            'receipt_store_path': discord_send_attempt.get('receipt_store_path'),
                        },
                    )
            if output['state'].get('runtime_mode') == 'FROZEN':
                self.event_log.append(
                    'freeze',
                    {
                        'run_id': run_id,
                        'freeze_reason': output['state'].get('freeze_reason'),
                        'pending_execution_phase': output['state'].get('pending_execution_phase'),
                        'recover_check': output['state'].get('recover_check'),
                    },
                )

            summary = WorkerRunSummary(
                run_id=run_id,
                phase='completed',
                symbol=self.config.symbol,
                decision_ts=market.decision_ts,
                consistency_status=output['state'].get('consistency_status'),
                plan_action=output['plan'].get('action_type'),
                result_status=output['result'].get('status'),
                freeze_reason=output['state'].get('freeze_reason'),
                runtime_mode=output['state'].get('runtime_mode'),
                failure_count=0,
                backoff_seconds=0.0,
                event_log_path=str(self.event_log_path),
            )
            self._write_status(
                phase='completed',
                summary=summary,
                last_exception=None,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                market=market,
                output=output,
                config_validation=config_validation_dict,
                audit_artifact_paths=artifact_paths,
            )
            self._write_dispatch_preview_audit(run_id=run_id, market=market, output=publishable_payload)
            return output
        except Exception as exc:
            failure_count = failure_count + 1
            backoff_seconds = self._compute_backoff_seconds(failure_count)
            last_exception = {
                'type': exc.__class__.__name__,
                'message': str(exc),
                'ts': datetime.now(timezone.utc).isoformat(),
            }
            self.event_log.append(
                'exception',
                {
                    'run_id': run_id,
                    'exception_type': last_exception['type'],
                    'exception_message': last_exception['message'],
                    'failure_count': failure_count,
                    'backoff_seconds': backoff_seconds,
                },
            )
            summary = WorkerRunSummary(
                run_id=run_id,
                phase='failed',
                symbol=self.config.symbol,
                decision_ts=None if market is None else market.decision_ts,
                consistency_status=None,
                plan_action=None,
                result_status='EXCEPTION',
                freeze_reason=None,
                runtime_mode=None,
                failure_count=failure_count,
                backoff_seconds=backoff_seconds,
                event_log_path=str(self.event_log_path),
            )
            self._write_status(
                phase='failed',
                summary=summary,
                last_exception=last_exception,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                market=market,
                output=None,
                config_validation={
                    **validate_runtime_config(self.config).as_dict(),
                    'strategy_adapter': {
                        'selected': normalize_strategy_adapter_name(self.config.strategy_adapter),
                        'requested': self.config.strategy_adapter,
                    },
                },
            )
            raise

    def run_daemon(self, *, max_cycles: int | None = None, sleep: bool = True) -> int:
        cycles = 0
        while max_cycles is None or cycles < max_cycles:
            now = datetime.now(timezone.utc)
            wait_delay_seconds = self._select_runtime_wait_delay_seconds()
            if wait_delay_seconds > 0 and sleep:
                time.sleep(wait_delay_seconds)
            scheduled_run = self.scheduler.next_run_at(datetime.now(timezone.utc))
            if sleep:
                time.sleep(max((scheduled_run - datetime.now(timezone.utc)).total_seconds(), 0.0))
            self.run_once(scheduled_run)
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
        return cycles

    def _read_failure_count(self) -> int:
        status = self._load_status()
        return int(status.get('consecutive_failures', 0))

    def _read_backoff_seconds(self) -> float:
        status = self._load_status()
        return float(status.get('backoff_seconds', 0.0) or 0.0)

    def _read_execution_retry_backoff(self) -> dict[str, Any]:
        status = self._load_status()
        latest_result_summary = status.get('latest_result_summary') or {}
        backoff = latest_result_summary.get('execution_retry_backoff') or {}
        return dict(backoff or {})

    def _select_runtime_wait_delay_seconds(self) -> float:
        base_backoff_seconds = self._read_backoff_seconds()
        execution_retry_backoff = self._read_execution_retry_backoff()
        phase = str(execution_retry_backoff.get('phase') or '').strip().lower()
        suggested_delay = execution_retry_backoff.get('next_delay_seconds')
        blocked_reason = execution_retry_backoff.get('blocked_reason')

        try:
            suggested_delay_value = float(suggested_delay or 0.0)
        except (TypeError, ValueError):
            suggested_delay_value = 0.0

        if suggested_delay_value <= 0.0:
            return base_backoff_seconds

        if phase in {'observe_pending', 'readonly_recheck', 'retry', 'frozen'}:
            return max(base_backoff_seconds, suggested_delay_value)

        if phase == 'steady':
            return base_backoff_seconds

        if blocked_reason:
            return max(base_backoff_seconds, suggested_delay_value)

        return base_backoff_seconds

    def _load_status(self) -> dict[str, Any]:
        if not self.runtime_status_path.exists():
            return {}
        return json.loads(self.runtime_status_path.read_text(encoding='utf-8'))

    def _maybe_advance_execution_orchestration(
        self,
        *,
        market: MarketSnapshot,
        output: dict[str, Any],
    ) -> dict[str, Any]:
        state_payload = dict(output.get('state') or {})
        result_payload = dict(output.get('result') or {})
        merged_state_payload = dict(state_payload)
        merged_state_payload.update(dict(result_payload.get('state_updates') or {}))
        merged_state_payload.setdefault(
            'strategy_protection_intent',
            self._build_strategy_protection_intent(merged_state_payload, result_payload),
        )
        if merged_state_payload.get('runtime_mode') == 'FROZEN':
            output['state'], output['result'], _ = attach_protection_followup_async_operation(
                market_decision_ts=market.decision_ts,
                symbol=market.symbol,
                strategy_ts=market.strategy_ts,
                state_payload=output.get('state') or {},
                result_payload=output.get('result') or {},
            )
            return output

        state_payload = merged_state_payload

        orchestration_reason = self._detect_protective_rebuild_orchestration_reason(
            market=market,
            output=output,
            state_payload=state_payload,
            result_payload=result_payload,
        )
        if orchestration_reason is None:
            output['state'], output['result'], _ = attach_protection_followup_async_operation(
                market_decision_ts=market.decision_ts,
                symbol=market.symbol,
                strategy_ts=market.strategy_ts,
                state_payload=output.get('state') or {},
                result_payload=output.get('result') or {},
            )
            return output

        rebuild_plan = self._build_protective_rebuild_plan(
            market=market,
            state_payload=state_payload,
            reason=orchestration_reason,
        )
        if rebuild_plan is None:
            intent = self._build_strategy_protection_intent(state_payload, result_payload)
            intent.update({
                'intent_status': 'BLOCKED',
                'pending_action': 'protective_rebuild',
                'reason': self.ORCHESTRATION_PROTECTIVE_REBUILD_BLOCK_REASON,
                'block_reason': self._derive_protective_rebuild_block_reason(state_payload),
                'last_eval_ts': market.decision_ts,
            })
            state_payload['strategy_protection_intent'] = intent
            state_payload['pending_execution_block_reason'] = intent['block_reason']
            state_payload['protective_phase_status'] = 'BLOCKED'
            output['state'] = state_payload
            result_updates = dict(result_payload.get('state_updates') or {})
            result_updates.update({
                'strategy_protection_intent': intent,
                'pending_execution_block_reason': intent['block_reason'],
                'protective_phase_status': 'BLOCKED',
            })
            result_payload['state_updates'] = result_updates
            trade_summary = dict(result_payload.get('trade_summary') or {})
            trade_summary['orchestration'] = {
                **dict(trade_summary.get('orchestration') or {}),
                'auto_advanced': False,
                'trigger_phase': state_payload.get('pending_execution_phase'),
                'next_action': 'protective_rebuild',
                'blocked_reason': intent['block_reason'],
            }
            result_payload['trade_summary'] = trade_summary
            output['result'] = result_payload
            output['state'], output['result'], _ = attach_protection_followup_async_operation(
                market_decision_ts=market.decision_ts,
                symbol=market.symbol,
                strategy_ts=market.strategy_ts,
                state_payload=output.get('state') or {},
                result_payload=output.get('result') or {},
            )
            return output

        executor = getattr(self.engine, 'executor_module', None)
        if not isinstance(executor, BinanceRealExecutor):
            output['state'], output['result'], _ = attach_protection_followup_async_operation(
                market_decision_ts=market.decision_ts,
                symbol=market.symbol,
                strategy_ts=market.strategy_ts,
                state_payload=output.get('state') or {},
                result_payload=output.get('result') or {},
            )
            return output

        state = self.state_store.load_state()
        cached_publishable_result = dict(getattr(state, 'last_publishable_result', {}) or {})
        if not cached_publishable_result and (self._is_publishable_execution_result(result_payload) or self._is_publishable_open_candidate(result_payload)):
            cached_publishable_result = dict(result_payload)
            save_state = getattr(self.state_store, 'save_state', None)
            if callable(save_state):
                next_state = replace(state)
                next_state.last_publishable_result = cached_publishable_result
                save_state(next_state)
                state = self.state_store.load_state()
            state_payload['last_publishable_result'] = cached_publishable_result
        state_payload['protective_phase_status'] = 'REBUILDING'
        state_payload['strategy_protection_intent'] = self._build_strategy_protection_intent(state_payload, result_payload)
        effective_state = replace(state)
        for key, value in state_payload.items():
            if hasattr(effective_state, key):
                setattr(effective_state, key, value)
        rebuild_result = executor.execute(rebuild_plan, market, effective_state)
        self.state_store.save_result(state, rebuild_result)
        updated_state = self.state_store.load_state()

        merged_trade_summary = {
            **(result_payload.get('trade_summary') or {}),
            'orchestration': {
                'auto_advanced': True,
                'trigger_phase': state_payload.get('pending_execution_phase'),
                'next_action': 'protective_rebuild',
                'rebuild_plan': asdict(rebuild_plan),
                'rebuild_result': asdict(rebuild_result),
                'blocked_reason': None,
            },
        }
        output['state'] = asdict(updated_state)
        if cached_publishable_result:
            output['state']['last_publishable_result'] = cached_publishable_result
        output['state']['strategy_protection_intent'] = self._build_strategy_protection_intent(output['state'], output['result'])
        output['state']['pending_execution_block_reason'] = None
        output['result'] = {
            **result_payload,
            **asdict(rebuild_result),
            'state_updates': {
                **dict((rebuild_result.state_updates or {})),
                'strategy_protection_intent': self._build_strategy_protection_intent(output['state'], output['result']),
                'pending_execution_block_reason': None,
            },
            'trade_summary': merged_trade_summary,
        }
        output['plan'] = {
            **(output.get('plan') or {}),
            'orchestration_followup': asdict(rebuild_plan),
        }
        output['state'], output['result'], _ = attach_protection_followup_async_operation(
            market_decision_ts=market.decision_ts,
            symbol=market.symbol,
            strategy_ts=market.strategy_ts,
            state_payload=output.get('state') or {},
            result_payload=output.get('result') or {},
        )
        return output

    @staticmethod
    def _is_publishable_execution_result(result_payload: dict[str, Any]) -> bool:
        action_type = result_payload.get('action_type')
        return (
            action_type in {'open', 'close', 'flip', 'add', 'trim'}
            and result_payload.get('confirmation_status') == 'CONFIRMED'
            and result_payload.get('execution_phase') == 'confirmed'
            and result_payload.get('confirmed_order_status') in {'FILLED', 'PARTIALLY_FILLED', 'CONFIRMED'}
            and result_payload.get('reconcile_status') == 'OK'
            and result_payload.get('avg_fill_price') is not None
        )

    @staticmethod
    def _is_publishable_open_candidate(result_payload: dict[str, Any]) -> bool:
        action_type = result_payload.get('action_type')
        if action_type not in {'open', 'flip', 'add', 'trim'}:
            return False
        if result_payload.get('reconcile_status') != 'OK':
            return False
        confirmation_status = str(result_payload.get('confirmation_status') or '')
        execution_phase = str(result_payload.get('execution_phase') or '')
        executed_qty = result_payload.get('executed_qty')
        post_position_qty = result_payload.get('post_position_qty')
        avg_fill_price = result_payload.get('avg_fill_price')
        post_entry_price = result_payload.get('post_entry_price')
        post_position_side = result_payload.get('post_position_side')
        has_position_fact = post_position_side in {'long', 'short'} and (post_position_qty is not None and float(post_position_qty or 0.0) > 0.0)
        has_fill_or_entry_price = avg_fill_price is not None or post_entry_price is not None
        has_execution_evidence = (executed_qty is not None and float(executed_qty or 0.0) > 0.0) or has_position_fact
        if not (has_position_fact and has_fill_or_entry_price and has_execution_evidence):
            return False
        if confirmation_status == 'CONFIRMED' and execution_phase == 'confirmed':
            return True
        return confirmation_status == 'POSITION_CONFIRMED' and execution_phase == 'position_confirmed_pending_trades'

    @staticmethod
    def _build_async_protective_close_publishable_candidate(
        *,
        current_result: dict[str, Any],
        current_state: dict[str, Any],
        cached_result: dict[str, Any],
        plan_action: str | None,
    ) -> dict[str, Any] | None:
        if plan_action == 'close':
            return None
        if current_result.get('action_type') in {'close', 'flip'}:
            return None
        if current_result.get('reconcile_status') != 'OK':
            return None
        if bool(current_result.get('should_freeze')):
            return None
        if current_state.get('runtime_mode') == 'FROZEN' or current_state.get('freeze_status') not in {None, '', 'NONE'}:
            return None
        prev_side = cached_result.get('post_position_side')
        prev_qty = float(cached_result.get('post_position_qty') or 0.0)
        if prev_side not in {'long', 'short'} or prev_qty <= 0.0:
            return None
        current_side = current_state.get('exchange_position_side')
        current_qty = float(current_state.get('exchange_position_qty') or 0.0)
        if current_side not in {None, '', 'flat'} or current_qty > 0.0:
            return None
        if cached_result.get('action_type') not in {'open', 'flip', 'add', 'trim'}:
            return None

        current_trade_summary = dict(current_result.get('trade_summary') or {})
        cached_trade_summary = dict(cached_result.get('trade_summary') or {})
        protective_orders = list(current_trade_summary.get('protective_orders') or cached_trade_summary.get('protective_orders') or [])
        protective_validation = dict(current_trade_summary.get('protective_validation') or cached_trade_summary.get('protective_validation') or {})
        protective_cancel_summary = dict(current_trade_summary.get('protective_cancel_summary') or {})
        exchange_protective_orders = list(current_state.get('exchange_protective_orders') or [])
        evidence_notes = []
        if protective_orders:
            evidence_notes.append('protective_orders_visible_in_confirm_summary')
        if protective_validation:
            evidence_notes.append('protective_validation_present')
        if exchange_protective_orders:
            evidence_notes.append('state_exchange_protective_orders_present')
        if protective_cancel_summary:
            evidence_notes.append('protective_cancel_summary_present')
        if not evidence_notes:
            return None

        return {
            'result_ts': current_result.get('result_ts') or current_state.get('state_ts') or cached_result.get('result_ts') or cached_result.get('bar_ts') or '',
            'bar_ts': current_result.get('bar_ts') or cached_result.get('bar_ts') or '',
            'status': 'ASYNC_CONFIRMED',
            'action_type': 'protective_async_close',
            'executed_side': prev_side,
            'executed_qty': prev_qty,
            'avg_fill_price': current_result.get('avg_fill_price') or cached_result.get('avg_fill_price') or cached_result.get('post_entry_price'),
            'fees': current_result.get('fees') or 0.0,
            'exchange_order_ids': current_result.get('exchange_order_ids') or [],
            'post_position_side': None,
            'post_position_qty': 0.0,
            'post_entry_price': None,
            'reconcile_status': 'OK',
            'should_freeze': False,
            'freeze_reason': None,
            'execution_phase': 'protective_async_close_confirmed',
            'confirmation_status': 'CONFIRMED',
            'confirmed_order_status': 'ASYNC_PROTECTIVE_TRIGGERED',
            'trade_summary': {
                **current_trade_summary,
                'confirmation_category': 'protective_async_close_confirmed',
                'async_close_source': 'exchange_fact_reconcile',
                'protective_async_close': {
                    'triggered': True,
                    'source': 'exchange_fact_reconcile',
                    'evidence_notes': evidence_notes,
                    'previous_position_side': prev_side,
                    'previous_position_qty': prev_qty,
                    'cached_open_action_type': cached_result.get('action_type'),
                    'protective_orders_count': len(protective_orders),
                    'exchange_protective_orders_count': len(exchange_protective_orders),
                    'protective_validation_status': protective_validation.get('status'),
                    'protective_validation_level': protective_validation.get('validation_level'),
                    'protective_validation_risk_class': protective_validation.get('risk_class'),
                    'protective_validation_mismatch_class': protective_validation.get('mismatch_class'),
                    'protective_validation_ok': protective_validation.get('ok'),
                    'protective_cancel_count': protective_cancel_summary.get('cancel_count'),
                },
                'protective_orders': protective_orders,
                'protective_validation': protective_validation,
                'protective_cancel_summary': protective_cancel_summary,
            },
        }

    def _select_publishable_output(self, output: dict[str, Any]) -> dict[str, Any]:
        result_payload = dict(output.get('result') or {})
        state_payload = dict(output.get('state') or {})
        plan_payload = dict(output.get('plan') or {})
        plan_action = plan_payload.get('action_type')
        current_action = result_payload.get('action_type')
        if self._is_publishable_execution_result(result_payload) or self._is_publishable_open_candidate(result_payload):
            state_payload['last_publishable_result'] = result_payload
            output['state'] = state_payload
            save_state = getattr(self.state_store, 'save_state', None)
            if callable(save_state):
                current_state = self.state_store.load_state()
                next_state = replace(current_state)
                next_state.last_publishable_result = result_payload
                save_state(next_state)
            return {
                **output,
                'state': state_payload,
                'result': result_payload,
            }

        cached_result = dict(state_payload.get('last_publishable_result') or {})
        if not cached_result:
            current_state = self.state_store.load_state()
            cached_result = dict(getattr(current_state, 'last_publishable_result', {}) or {})
            if cached_result:
                state_payload['last_publishable_result'] = cached_result
                output['state'] = state_payload
        if not cached_result:
            return output
        async_protective_close_candidate = self._build_async_protective_close_publishable_candidate(
            current_result=result_payload,
            current_state=state_payload,
            cached_result=cached_result,
            plan_action=plan_action,
        )
        if async_protective_close_candidate is not None:
            return {
                **output,
                'state': state_payload,
                'result': async_protective_close_candidate,
            }
        if plan_action == 'protective_rebuild':
            return output

        runtime_mode = state_payload.get('runtime_mode')
        exchange_position_side = state_payload.get('exchange_position_side')
        exchange_position_qty = state_payload.get('exchange_position_qty')
        is_flat_on_exchange = exchange_position_side in {None, '', 'flat'} or float(exchange_position_qty or 0.0) <= 0.0
        is_terminal_current_result = bool(
            current_action in {'close', 'state_update'}
            and is_flat_on_exchange
            and result_payload.get('reconcile_status') == 'OK'
        )
        # Keep cached context in state for inspection, but do not let frozen/flat/terminal rounds re-anchor preview payloads to an old fill.
        if current_action in {'hold', None} or runtime_mode == 'FROZEN' or is_flat_on_exchange or is_terminal_current_result:
            return output
        return {
            **output,
            'state': state_payload,
            'result': cached_result,
        }

    def _should_attempt_discord_send(self, *, output: dict[str, Any], publishable_output: dict[str, Any]) -> bool:
        publishable_result = dict(publishable_output.get('result') or {})
        publishable_state = dict(publishable_output.get('state') or {})
        if not publishable_result and not publishable_state:
            return False

        # Use the same preview decision path as the sender so frozen risk alerts are
        # not blocked by the outer action gate when the current round is hold.
        dispatch_preview = build_dispatch_preview(self.config, publishable_result, publishable_state)
        primary_preview = dispatch_preview.get('primary_preview') or {}
        payload_preview = primary_preview.get('payload_preview') or dispatch_preview.get('payload_preview')
        if payload_preview is not None:
            return True

        rehearsal_preview = dispatch_preview.get('rehearsal_preview') or {}
        rehearsal_open = bool(getattr(self.config, 'discord_rehearsal_real_send_enabled', False))
        rehearsal_payload = rehearsal_preview.get('payload_preview')
        return rehearsal_open and rehearsal_payload is not None

    def _build_strategy_protection_intent(
        self,
        state_payload: dict[str, Any],
        result_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result_payload = dict(result_payload or {})
        trade_summary = dict(result_payload.get('trade_summary') or {})
        return build_strategy_protection_intent(
            runtime_mode=state_payload.get('runtime_mode'),
            position_side=state_payload.get('exchange_position_side'),
            position_qty=state_payload.get('exchange_position_qty'),
            active_strategy=state_payload.get('active_strategy'),
            stop_price=state_payload.get('stop_price'),
            tp_price=state_payload.get('tp_price'),
            pending_execution_phase=state_payload.get('pending_execution_phase'),
            pending_execution_block_reason=state_payload.get('pending_execution_block_reason'),
            protective_order_status=state_payload.get('protective_order_status'),
            protective_phase_status=state_payload.get('protective_phase_status'),
            protective_orders=list(state_payload.get('exchange_protective_orders') or []),
            protective_validation=dict(trade_summary.get('protective_validation') or {}),
            confirmation_category=trade_summary.get('confirmation_category'),
            freeze_reason=state_payload.get('freeze_reason') or result_payload.get('freeze_reason'),
            last_eval_ts=state_payload.get('state_ts'),
            orchestration_entry_pending_protective=self.ORCHESTRATION_ENTRY_PENDING_PROTECTIVE,
            orchestration_management_pending_protective=self.ORCHESTRATION_MANAGEMENT_PENDING_PROTECTIVE,
        )

    @staticmethod
    def _derive_protective_rebuild_block_reason(state_payload: dict[str, Any]) -> str:
        return derive_protective_rebuild_block_reason(state_payload)

    def _detect_protective_rebuild_orchestration_reason(
        self,
        *,
        market: MarketSnapshot,
        output: dict[str, Any],
        state_payload: dict[str, Any],
        result_payload: dict[str, Any],
    ) -> str | None:
        pending_phase = state_payload.get('pending_execution_phase')
        if (
            pending_phase == self.ORCHESTRATION_ENTRY_PENDING_PROTECTIVE
            and result_payload.get('execution_phase') == self.ORCHESTRATION_ENTRY_PENDING_PROTECTIVE
        ):
            return self.ORCHESTRATION_PROTECTIVE_REBUILD_REASON

        state_updates = dict(result_payload.get('state_updates') or {})
        if result_payload.get('action_type') != 'state_update':
            return None
        if bool(((output.get('plan') or {})).get('requires_execution')):
            return None
        if pending_phase != self.ORCHESTRATION_MANAGEMENT_PENDING_PROTECTIVE:
            return None
        if result_payload.get('execution_phase') not in {None, '', 'state_updated', 'management_state_updated'}:
            return None
        if 'stop_price' not in state_updates:
            return None

        next_stop_price = self._safe_float(state_updates.get('stop_price'))
        if next_stop_price is None:
            return None

        previous_stop_price = self._safe_float((output.get('state') or {}).get('stop_price'))
        if previous_stop_price is None:
            previous_stop_price = self._safe_float(self.state_store.load_state().stop_price)
        if previous_stop_price is not None and abs(previous_stop_price - next_stop_price) > 1e-12:
            return self.ORCHESTRATION_PROTECTIVE_REBUILD_MANAGEMENT_REASON

        # Management state updates may already be merged into persisted state before
        # orchestration runs; in that case, treat an explicit stop_price update as the trigger.
        return self.ORCHESTRATION_PROTECTIVE_REBUILD_MANAGEMENT_REASON

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _build_protective_rebuild_plan(
        self,
        *,
        market: MarketSnapshot,
        state_payload: dict[str, Any],
        reason: str | None = None,
    ) -> FinalActionPlan | None:
        position_side = state_payload.get('exchange_position_side')
        position_qty = float(state_payload.get('exchange_position_qty') or 0.0)
        active_strategy = state_payload.get('active_strategy')
        stop_price = state_payload.get('stop_price')
        if position_side not in {'long', 'short'}:
            return None
        if position_qty <= 0.0:
            return None
        if active_strategy in {None, 'none'}:
            return None
        if stop_price is None:
            return None
        return FinalActionPlan(
            plan_ts=market.decision_ts,
            bar_ts=market.bar_ts,
            action_type='protective_rebuild',
            target_strategy=active_strategy,
            target_side=position_side,
            reason=reason or self.ORCHESTRATION_PROTECTIVE_REBUILD_REASON,
            qty_mode='exchange_position',
            qty=None,
            price_hint=market.current_price,
            stop_price=float(stop_price),
            risk_fraction=state_payload.get('risk_fraction'),
            conflict_context={'tp_price': state_payload.get('tp_price')},
            requires_execution=True,
            close_reason=None,
        )

    def _compute_backoff_seconds(self, failure_count: int) -> float:
        limit = max(1, int(self.scheduler.config.max_backoff_seconds))
        return float(min(2 ** max(failure_count - 1, 0), limit))

    def _normalize_readonly_retry_budget(self, budget: dict[str, Any] | None, *, bar_ts: str | None) -> dict[str, Any]:
        current_bar_ts = str(bar_ts or '')
        current = dict(budget or {})
        if str(current.get('current_bar_ts') or '') != current_bar_ts:
            used_attempts = 0
        else:
            used_attempts = int(current.get('attempts_used') or 0)
        max_attempts = max(1, int(current.get('max_attempts') or READONLY_RECHECK_SHARED_MAX_ATTEMPTS))
        used_attempts = max(0, min(used_attempts, max_attempts))
        return {
            'current_bar_ts': current_bar_ts,
            'budget_scope': 'per_bar_shared',
            'retry_interval_seconds': READONLY_RECHECK_RETRY_INTERVAL_SECONDS,
            'max_attempts': max_attempts,
            'attempts_used': used_attempts,
            'attempts_remaining': max(0, max_attempts - used_attempts),
            'last_retryable_category': current.get('last_retryable_category'),
            'last_freeze_reason': current.get('last_freeze_reason'),
        }

    def _read_readonly_retry_budget(self, *, bar_ts: str | None) -> dict[str, Any]:
        try:
            state = self.state_store.load_state()
            existing = dict(getattr(state, 'execution_retry_backoff', {}) or {})
        except Exception:
            existing = {}
        return self._normalize_readonly_retry_budget(existing, bar_ts=bar_ts)

    @staticmethod
    def _consume_readonly_retry_budget(budget: dict[str, Any], *, confirmation_category: str | None, freeze_reason: str | None) -> dict[str, Any]:
        updated = dict(budget)
        max_attempts = max(1, int(updated.get('max_attempts') or READONLY_RECHECK_SHARED_MAX_ATTEMPTS))
        used_attempts = min(max_attempts, max(0, int(updated.get('attempts_used') or 0)) + 1)
        updated['attempts_used'] = used_attempts
        updated['attempts_remaining'] = max(0, max_attempts - used_attempts)
        if confirmation_category:
            updated['last_retryable_category'] = confirmation_category
        if freeze_reason:
            updated['last_freeze_reason'] = freeze_reason
        return updated

    @staticmethod
    def _is_retryable_readonly_confirmation(confirmation: Any) -> bool:
        return str(getattr(confirmation, 'confirmation_category', '') or '') in READONLY_RECHECK_RETRYABLE_CATEGORIES

    def _build_execution_retry_backoff(self, output: dict[str, Any]) -> dict[str, Any]:
        result_payload = dict(output.get('result') or {})
        trade_summary = dict(result_payload.get('trade_summary') or {})
        policy = self._exception_policy_from_output(output)
        readonly_recheck = dict(trade_summary.get('readonly_recheck') or {})
        failure_count = self._read_failure_count()
        blocked_reason = None
        phase = 'steady'
        next_delay_seconds = 0.0
        policy_action = policy.get('action') or policy.get('policy')
        state_payload = dict(output.get('state') or {})
        existing_budget = dict(state_payload.get('execution_retry_backoff') or {})
        if readonly_recheck.get('status') in {READONLY_RECHECK_PENDING, READONLY_RECHECK_FREEZE, READONLY_RECHECK_RECOVER_READY}:
            raw_budget = readonly_recheck.get('retry_budget') if isinstance(readonly_recheck.get('retry_budget'), dict) else existing_budget
        else:
            raw_budget = {}
        backoff = self._normalize_readonly_retry_budget(
            raw_budget,
            bar_ts=result_payload.get('bar_ts'),
        )
        if readonly_recheck.get('status') == READONLY_RECHECK_RECOVER_READY:
            phase = 'steady'
            next_delay_seconds = 0.0
            blocked_reason = None
        elif readonly_recheck.get('status') == READONLY_RECHECK_PENDING:
            phase = 'observe_pending'
            next_delay_seconds = READONLY_RECHECK_RETRY_INTERVAL_SECONDS
            blocked_reason = readonly_recheck.get('freeze_reason') or readonly_recheck.get('reason') or 'readonly_recheck_pending_confirmation'
        elif policy_action == ACTION_READONLY_RECHECK:
            phase = 'readonly_recheck'
            next_delay_seconds = READONLY_RECHECK_RETRY_INTERVAL_SECONDS
            blocked_reason = readonly_recheck.get('freeze_reason') or result_payload.get('freeze_reason')
        elif policy_action == 'retry':
            phase = 'retry'
            next_delay_seconds = min(max(5.0, self._compute_backoff_seconds(max(failure_count, 1))), 120.0)
            blocked_reason = result_payload.get('error_code') or result_payload.get('freeze_reason')
        elif result_payload.get('status') == 'FROZEN':
            phase = 'frozen'
            next_delay_seconds = min(max(30.0, self._compute_backoff_seconds(max(failure_count, 1))), 120.0)
            blocked_reason = result_payload.get('freeze_reason')
        backoff.update({
            'phase': phase,
            'policy_action': policy_action,
            'failure_count': failure_count,
            'next_delay_seconds': next_delay_seconds,
            'blocked_reason': blocked_reason,
            'readonly_recheck_status': readonly_recheck.get('status'),
        })
        return backoff

    def _attach_execution_retry_backoff(self, output: dict[str, Any]) -> dict[str, Any]:
        state_payload = dict(output.get('state') or {})
        result_payload = dict(output.get('result') or {})
        backoff = self._build_execution_retry_backoff(output)
        state_payload['execution_retry_backoff'] = backoff
        state_payload.setdefault('strategy_protection_intent', self._build_strategy_protection_intent(state_payload, result_payload))
        if backoff.get('blocked_reason'):
            state_payload['pending_execution_block_reason'] = backoff.get('blocked_reason')
        elif backoff.get('phase') == 'steady':
            state_payload['pending_execution_block_reason'] = None
        output['state'] = state_payload
        state_updates = dict(result_payload.get('state_updates') or {})
        state_updates['execution_retry_backoff'] = backoff
        state_updates.setdefault('strategy_protection_intent', state_payload.get('strategy_protection_intent'))
        state_updates['pending_execution_block_reason'] = state_payload.get('pending_execution_block_reason')
        result_payload['state_updates'] = state_updates
        output['result'] = result_payload
        return output

    def _exception_policy_from_output(self, output: dict[str, Any]) -> dict[str, Any]:
        result_payload = output.get('result') or {}
        trade_summary = result_payload.get('trade_summary') or {}
        policy = (
            trade_summary.get('exception_policy_view')
            or trade_summary.get('submit_exception_policy')
            or result_payload.get('exception_policy')
            or {}
        )
        return dict(policy or {})

    def _maybe_run_readonly_recheck(
        self,
        *,
        run_id: str,
        market: MarketSnapshot,
        output: dict[str, Any],
    ) -> ReadonlyRecheckDecision | None:
        result_payload = output.get('result') or {}
        policy = self._exception_policy_from_output(output)
        if (policy.get('action') or policy.get('policy')) != ACTION_READONLY_RECHECK:
            return None

        trade_summary = result_payload.get('trade_summary') or {}
        request_payloads = (((trade_summary.get('request_context') or {}).get('request_payloads')) or [])
        order_requests = [self._deserialize_order_request(market.symbol, row) for row in request_payloads if isinstance(row, dict)]
        if not order_requests:
            return ReadonlyRecheckDecision(
                status=READONLY_RECHECK_QUERY_FAILED,
                action='freeze',
                summary={
                    'status': READONLY_RECHECK_QUERY_FAILED,
                    'action': 'freeze',
                    'reason': 'missing_request_context',
                    'policy_action': ACTION_READONLY_RECHECK,
                    'policy_source_key': policy.get('source_key'),
                    'readonly_checks': policy.get('readonly_checks') or [],
                    'query_attempted': False,
                    'notes': ['readonly_recheck_missing_request_context'],
                },
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': 'readonly_recheck_missing_request_context',
                    'last_freeze_reason': 'readonly_recheck_missing_request_context',
                    'pending_execution_phase': 'frozen',
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
                result_updates={
                    'status': 'FROZEN',
                    'should_freeze': True,
                    'freeze_reason': 'readonly_recheck_missing_request_context',
                    'execution_phase': 'frozen',
                },
                should_freeze=True,
                freeze_reason='readonly_recheck_missing_request_context',
                recover_check=build_readonly_recheck_recover_check(
                    decision={
                        'status': READONLY_RECHECK_QUERY_FAILED,
                        'freeze_reason': 'readonly_recheck_missing_request_context',
                        'checked_at': market.decision_ts,
                    }
                ),
            )

        confirmer = getattr(getattr(self.engine, 'executor_module', None), 'posttrade_confirmer', None)
        if confirmer is None:
            return None

        submit_metadata = trade_summary.get('submit_exception_metadata') or {}
        simulated_receipts = self._build_recheck_receipts(order_requests, submit_metadata)
        self.event_log.append(
            'readonly_recheck_start',
            {
                'run_id': run_id,
                'policy_action': policy.get('action') or policy.get('policy'),
                'policy_source_key': policy.get('source_key'),
                'readonly_checks': policy.get('readonly_checks') or [],
                'client_order_ids': [item.client_order_id for item in order_requests],
            },
        )
        attempt_trace: list[dict[str, Any]] = []
        retry_budget = self._read_readonly_retry_budget(bar_ts=market.bar_ts)
        try:
            decision = None
            final_confirmation = None
            attempts_remaining = int(retry_budget.get('attempts_remaining') or 0)
            attempts_allowed = max(0, attempts_remaining)
            stop_reason = 'retry_budget_exhausted'
            stop_condition = 'max_attempts_reached'
            for attempt in range(1, attempts_allowed + 1):
                confirmation = confirmer.confirm(
                    market=market,
                    order_requests=order_requests,
                    simulated_receipts=simulated_receipts,
                    allow_unacknowledged_lookup=True,
                )
                final_confirmation = confirmation
                if self._is_retryable_readonly_confirmation(confirmation):
                    retry_budget = self._consume_readonly_retry_budget(
                        retry_budget,
                        confirmation_category=confirmation.confirmation_category,
                        freeze_reason=confirmation.freeze_reason,
                    )
                decision = self._classify_readonly_recheck_decision(policy=policy, confirmation=confirmation)
                attempt_trace.append(
                    {
                        'attempt': attempt,
                        'confirmation_category': confirmation.confirmation_category,
                        'confirmation_status': confirmation.confirmation_status,
                        'confirmed_order_status': confirmation.order_status,
                        'reconcile_status': confirmation.reconcile_status,
                        'should_freeze': bool(confirmation.should_freeze),
                        'freeze_reason': confirmation.freeze_reason,
                        'executed_qty': confirmation.executed_qty,
                        'fill_count': getattr(confirmation, 'fill_count', None),
                        'post_position_side': confirmation.post_position_side,
                        'post_position_qty': confirmation.post_position_qty,
                        'budget_attempts_used': retry_budget.get('attempts_used'),
                        'budget_attempts_remaining': retry_budget.get('attempts_remaining'),
                    }
                )
                if not self._is_retryable_readonly_confirmation(confirmation):
                    stop_reason, stop_condition = self._derive_readonly_recheck_stop(confirmation=confirmation)
                    break
                if int(retry_budget.get('attempts_remaining') or 0) <= 0:
                    stop_reason = 'retry_budget_exhausted'
                    stop_condition = 'shared_budget_exhausted'
                    break
                time.sleep(READONLY_RECHECK_RETRY_INTERVAL_SECONDS)
            if final_confirmation is not None and decision is not None:
                decision = self._decorate_readonly_recheck_decision(
                    decision=decision,
                    confirmation=final_confirmation,
                    attempt_trace=attempt_trace,
                    retry_budget=retry_budget,
                    stop_reason=stop_reason,
                    stop_condition=stop_condition,
                )
            if decision is None:
                exhausted_reason = 'readonly_recheck_retry_budget_exhausted'
                decision = ReadonlyRecheckDecision(
                    status=READONLY_RECHECK_FREEZE,
                    action='freeze',
                    summary={
                        'status': READONLY_RECHECK_FREEZE,
                        'action': 'freeze',
                        'reason': exhausted_reason,
                        'policy_action': ACTION_READONLY_RECHECK,
                        'policy_source_key': policy.get('source_key'),
                        'readonly_checks': policy.get('readonly_checks') or [],
                        'query_attempted': False,
                        'notes': [exhausted_reason],
                    },
                    state_updates={
                        'runtime_mode': 'FROZEN',
                        'freeze_status': 'ACTIVE',
                        'freeze_reason': exhausted_reason,
                        'last_freeze_reason': exhausted_reason,
                        'pending_execution_phase': 'frozen',
                        'can_open_new_position': False,
                        'can_modify_position': False,
                    },
                    result_updates={
                        'status': 'FROZEN',
                        'should_freeze': True,
                        'freeze_reason': exhausted_reason,
                        'execution_phase': 'frozen',
                    },
                    should_freeze=True,
                    freeze_reason=exhausted_reason,
                    recover_check=build_readonly_recheck_recover_check(
                        decision={
                            'status': READONLY_RECHECK_FREEZE,
                            'freeze_reason': exhausted_reason,
                            'checked_at': market.decision_ts,
                        }
                    ),
                )
            decision.summary['attempt_count'] = len(attempt_trace)
            decision.summary['max_attempts'] = int(retry_budget.get('max_attempts') or READONLY_RECHECK_SHARED_MAX_ATTEMPTS)
            decision.summary['attempt_trace'] = attempt_trace
            decision.summary['retry_budget'] = retry_budget
        except Exception as exc:
            decision = ReadonlyRecheckDecision(
                status=READONLY_RECHECK_QUERY_FAILED,
                action='freeze',
                summary={
                    'status': READONLY_RECHECK_QUERY_FAILED,
                    'action': 'freeze',
                    'reason': 'readonly_query_exception',
                    'policy_action': ACTION_READONLY_RECHECK,
                    'policy_source_key': policy.get('source_key'),
                    'readonly_checks': policy.get('readonly_checks') or [],
                    'query_attempted': True,
                    'query_exception_type': exc.__class__.__name__,
                    'query_exception_message': str(exc),
                    'notes': [f'readonly_recheck_exception:{exc.__class__.__name__}'],
                    'attempt_count': len(attempt_trace),
                    'max_attempts': int(retry_budget.get('max_attempts') or READONLY_RECHECK_SHARED_MAX_ATTEMPTS),
                    'attempt_trace': attempt_trace,
                    'retry_budget': retry_budget,
                },
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': 'readonly_recheck_query_exception',
                    'last_freeze_reason': 'readonly_recheck_query_exception',
                    'pending_execution_phase': 'frozen',
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
                result_updates={
                    'status': 'FROZEN',
                    'should_freeze': True,
                    'freeze_reason': 'readonly_recheck_query_exception',
                    'execution_phase': 'frozen',
                },
                should_freeze=True,
                freeze_reason='readonly_recheck_query_exception',
                recover_check=build_readonly_recheck_recover_check(
                    decision={
                        'status': READONLY_RECHECK_QUERY_FAILED,
                        'freeze_reason': 'readonly_recheck_query_exception',
                        'checked_at': market.decision_ts,
                    }
                ),
            )
        self.event_log.append(
            'readonly_recheck_result',
            {
                'run_id': run_id,
                **decision.summary,
            },
        )
        return decision

    @staticmethod
    def _deserialize_order_request(symbol: str, payload: dict[str, Any]) -> BinanceOrderRequest:
        return BinanceOrderRequest(
            symbol=payload.get('symbol') or symbol,
            side=str(payload.get('side') or '').upper(),
            order_type=str(payload.get('type') or payload.get('order_type') or 'MARKET').upper(),
            quantity=(float(payload['quantity']) if payload.get('quantity') is not None else None),
            reduce_only=bool(payload.get('reduceOnly') == 'true' or payload.get('reduce_only')),
            position_side=(str(payload.get('positionSide')).lower() if payload.get('positionSide') else payload.get('position_side')),
            client_order_id=payload.get('newClientOrderId') or payload.get('client_order_id') or '',
            metadata={},
        )

    @staticmethod
    def _build_recheck_receipts(order_requests: list[BinanceOrderRequest], submit_metadata: dict[str, Any]) -> list[Any]:
        error_code = submit_metadata.get('submit_exception_category') or 'submit_unknown'
        return [
            SimulatedExecutionReceipt(
                client_order_id=request.client_order_id,
                exchange_order_id=None,
                acknowledged=False,
                submitted_qty=request.quantity,
                submitted_side=request.side,
                submit_status='UNKNOWN',
                exchange_status=None,
                transact_time_ms=None,
                error_code=error_code,
                error_message='readonly_recheck_followup',
            )
            for request in order_requests
        ]

    @staticmethod
    def _derive_protection_semantic_stop(*, trade_summary: dict[str, Any], notes: set[str]) -> tuple[str, str] | None:
        protective_orders = [item for item in (trade_summary.get('protective_orders') or []) if isinstance(item, dict)]
        if not protective_orders:
            if 'protection_semantic_mismatch' in notes:
                return 'protection_semantic_mismatch', 'protection_semantic_mismatch'
            return None

        expected_close_position = bool(trade_summary.get('expected_close_position_protection'))
        expected_reduce_only = bool(trade_summary.get('expected_reduce_only_protection'))
        expected_position_side = str(trade_summary.get('expected_position_side') or '').lower()
        expected_stop_type = str(trade_summary.get('expected_stop_order_type') or trade_summary.get('expected_stop_type') or '').upper()
        expected_tp_type = str(trade_summary.get('expected_take_profit_order_type') or trade_summary.get('expected_tp_order_type') or '').upper()

        expected_side = ''
        if expected_position_side == 'long':
            expected_side = 'SELL'
        elif expected_position_side == 'short':
            expected_side = 'BUY'

        stop_order = next((item for item in protective_orders if str(item.get('kind') or '') == 'hard_stop'), None)
        tp_order = next((item for item in protective_orders if str(item.get('kind') or '') == 'take_profit'), None)

        if stop_order is None:
            return 'protection_stop_missing', 'protection_stop_missing'
        if bool(trade_summary.get('tp_required')) and tp_order is None:
            return 'protection_tp_missing', 'protection_tp_missing'

        position_side_values = {
            str(item.get('position_side') or item.get('positionSide') or '').lower()
            for item in protective_orders
            if str(item.get('position_side') or item.get('positionSide') or '').strip()
        }
        if expected_position_side and position_side_values and expected_position_side not in position_side_values:
            return 'protection_semantic_mismatch', 'protection_semantic_position_side_mismatch'

        def _payload_mismatch(order: dict[str, Any] | None, *, expected_type: str, kind: str) -> tuple[str, str] | None:
            if order is None:
                return None
            order_type = str(order.get('type') or order.get('orig_type') or order.get('origType') or '').upper()
            close_position = bool(order.get('close_position'))
            reduce_only = bool(order.get('reduce_only'))
            side = str(order.get('side') or '').upper()
            qty = order.get('qty')
            stop_condition = 'protection_semantic_stop_payload_mismatch' if kind == 'hard_stop' else 'protection_semantic_tp_payload_mismatch'

            if expected_type and order_type and order_type != expected_type:
                return 'protection_semantic_mismatch', 'protection_semantic_type_mismatch'
            if expected_close_position and not close_position:
                return 'protection_semantic_mismatch', stop_condition
            if expected_reduce_only and not (reduce_only or close_position):
                return 'protection_semantic_mismatch', stop_condition
            if expected_close_position and not close_position and qty not in {None, 0, 0.0, '0', '0.0'}:
                return 'protection_semantic_mismatch', stop_condition
            if expected_side and side not in {expected_side, ''}:
                return 'protection_semantic_mismatch', stop_condition
            return None

        stop_payload_mismatch = _payload_mismatch(stop_order, expected_type=expected_stop_type, kind='hard_stop')
        if stop_payload_mismatch is not None:
            return stop_payload_mismatch
        tp_payload_mismatch = _payload_mismatch(tp_order, expected_type=expected_tp_type, kind='take_profit')
        if tp_payload_mismatch is not None:
            return tp_payload_mismatch

        if 'protection_semantic_mismatch' in notes:
            return 'protection_semantic_mismatch', 'protection_semantic_mismatch'
        return None

    @staticmethod
    def _derive_readonly_recheck_stop(*, confirmation: Any) -> tuple[str, str]:
        category = str(getattr(confirmation, 'confirmation_category', '') or '')
        status = str(getattr(confirmation, 'confirmation_status', '') or '')
        trade_summary = getattr(confirmation, 'trade_summary', {}) or {}
        notes = set(trade_summary.get('notes') or getattr(confirmation, 'notes', []) or [])

        if category == 'confirmed' and status == 'CONFIRMED':
            return 'recover_ready', 'trades_confirmed'
        if 'manual_position_flat_confirmed' in notes:
            return 'manual_position_flat_confirmed', 'manual_position_flat_confirmed'
        if 'external_position_override' in notes:
            return 'external_position_override', 'external_position_override'
        if 'open_orders_side_or_qty_conflict' in notes:
            return 'manual_open_orders_side_or_qty_conflict', 'manual_open_orders_side_or_qty_conflict'
        if 'reduce_only_filled_but_position_not_flat' in notes:
            return 'manual_reduce_only_position_not_flat', 'manual_reduce_only_position_not_flat'
        if 'protection_orders_missing' in notes:
            return 'protection_missing', 'protection_orders_missing'
        semantic_stop = RuntimeWorker._derive_protection_semantic_stop(trade_summary=trade_summary, notes=notes)
        if semantic_stop is not None:
            return semantic_stop
        if 'protection_submit_gate_blocked' in notes:
            return 'protection_submit_gate_blocked', 'protection_submit_gate_blocked'
        if category == 'position_confirmed':
            if 'partial_fill_position_working' in notes or str(getattr(confirmation, 'order_status', '') or '').upper() == 'PARTIALLY_FILLED':
                return 'partial_position_working', 'partial_fill_position_working'
            flat_after_reduce_only_close = (
                getattr(confirmation, 'post_position_side', None) is None
                and float(getattr(confirmation, 'post_position_qty', 0.0) or 0.0) <= 0.0
                and str(getattr(confirmation, 'order_status', '') or '').upper() == 'FILLED'
                and bool(trade_summary.get('requested_reduce_only'))
                and not bool(trade_summary.get('has_open_orders'))
                and int(trade_summary.get('open_orders_count') or 0) == 0
                and not list(trade_summary.get('protective_orders') or [])
            )
            if flat_after_reduce_only_close:
                return 'flat_ready_trade_reconciliation_pending', 'flat_ready_trade_reconciliation_pending'
            if bool(trade_summary.get('protective_pending_confirm')):
                return 'protection_pending_confirm', 'position_confirmed_but_protection_pending'
            if getattr(confirmation, 'executed_qty', 0.0) and not int(trade_summary.get('fills_count') or 0):
                return 'position_confirmed_pending_trades', 'trade_rows_missing_after_fill'
            if getattr(confirmation, 'executed_qty', 0.0) and getattr(confirmation, 'avg_fill_price', None) is None:
                return 'avg_fill_price_missing', 'avg_fill_price_missing_after_fills'
            if getattr(confirmation, 'executed_qty', 0.0) and not list(getattr(confirmation, 'fee_assets', []) or []):
                return 'fee_reconciliation_pending', 'fee_reconciliation_pending'
            return 'recover_ready', 'position_fact_confirmed_before_trade_rows'
        if category == 'mismatch':
            return 'readonly_recheck_freeze', 'non_retryable_confirmation_category'
        if category == 'pending':
            return 'pending', 'await_more_exchange_facts'
        if category == 'query_failed':
            return 'query_failed', 'readonly_query_failed'
        return 'readonly_recheck_freeze', 'unresolved_confirmation_state'

    @staticmethod
    def _decorate_readonly_recheck_decision(
        *,
        decision: ReadonlyRecheckDecision,
        confirmation: Any,
        attempt_trace: list[dict[str, Any]],
        retry_budget: dict[str, Any],
        stop_reason: str,
        stop_condition: str,
    ) -> ReadonlyRecheckDecision:
        summary = dict(decision.summary or {})
        summary['confirm_context'] = build_confirm_context(
            phase='readonly_recheck',
            confirmation=confirmation,
            attempts_used=len(attempt_trace),
            max_attempts=int(retry_budget.get('max_attempts') or READONLY_RECHECK_SHARED_MAX_ATTEMPTS),
            retry_interval_seconds=float(retry_budget.get('retry_interval_seconds') or READONLY_RECHECK_RETRY_INTERVAL_SECONDS),
            retried=len(attempt_trace) > 1,
            attempt_trace=attempt_trace,
            retry_budget=retry_budget,
            stop_reason=stop_reason,
            stop_condition=stop_condition,
            extra={
                'readonly_recheck_status': summary.get('status'),
                'readonly_recheck_action': summary.get('action'),
            },
        )
        summary['stop_reason'] = stop_reason
        summary['stop_condition'] = stop_condition
        return ReadonlyRecheckDecision(
            status=decision.status,
            action=decision.action,
            summary=summary,
            state_updates=dict(decision.state_updates or {}),
            result_updates=dict(decision.result_updates or {}),
            should_freeze=decision.should_freeze,
            freeze_reason=decision.freeze_reason,
            recover_check=dict(decision.recover_check or {}),
        )

    def _classify_readonly_recheck_decision(
        self,
        *,
        policy: dict[str, Any],
        confirmation: Any,
    ) -> ReadonlyRecheckDecision:
        confirm_summary = build_execution_confirm_summary(
            {
                'confirmation_status': confirmation.confirmation_status,
                'confirmed_order_status': confirmation.order_status,
                'executed_qty': confirmation.executed_qty,
                'avg_fill_price': confirmation.avg_fill_price,
                'exchange_order_ids': confirmation.exchange_order_ids,
                'reconcile_status': confirmation.reconcile_status,
                'freeze_reason': confirmation.freeze_reason,
                'trade_summary': {
                    **(confirmation.trade_summary or {}),
                    'confirmation_category': confirmation.confirmation_category,
                    'exception_policy_view': policy,
                },
            }
        )
        notes = list((confirmation.trade_summary or {}).get('notes') or confirmation.notes or [])
        trade_summary = confirmation.trade_summary or {}
        orchestration = dict(trade_summary.get('orchestration') or {})
        trigger_phase = str(orchestration.get('trigger_phase') or '')

        base_summary = {
            'policy_action': ACTION_READONLY_RECHECK,
            'policy_source_key': policy.get('source_key'),
            'readonly_checks': policy.get('readonly_checks') or [],
            'query_attempted': True,
            'confirmation_status': confirmation.confirmation_status,
            'confirmation_category': confirmation.confirmation_category,
            'confirmed_order_status': confirmation.order_status,
            'reconcile_status': confirmation.reconcile_status,
            'freeze_reason': confirmation.freeze_reason,
            'executed_qty': confirmation.executed_qty,
            'post_position_side': confirmation.post_position_side,
            'post_position_qty': confirmation.post_position_qty,
            'has_open_orders': (confirmation.trade_summary or {}).get('has_open_orders'),
            'open_orders_count': (confirmation.trade_summary or {}).get('open_orders_count'),
            'notes': notes,
            'confirm_summary': confirm_summary,
            'trigger_phase': trigger_phase or None,
        }
        semantic_stop = self._derive_protection_semantic_stop(trade_summary=(confirmation.trade_summary or {}), notes=set(notes))
        manual_review_reason, manual_review_stop_condition, manual_review_recover_stage = classify_manual_review_from_notes(
            notes=notes,
            trade_summary=(confirmation.trade_summary or {}),
            semantic_stop=semantic_stop,
        )

        if confirmation.confirmation_category == 'confirmed' and confirmation.reconcile_status == 'OK' and not confirmation.should_freeze:
            ready_recover_stage = RECOVER_STAGE_PROTECTION_PENDING_CONFIRM if trigger_phase in {self.ORCHESTRATION_ENTRY_PENDING_PROTECTIVE, 'management_stop_update_pending_protective'} else RECOVER_STAGE_RECOVER_READY
            risk_action = derive_risk_action(
                recover_policy='ready_only',
                recover_stage=ready_recover_stage,
                stop_condition='trades_confirmed',
            ) if ready_recover_stage == RECOVER_STAGE_RECOVER_READY else RISK_ACTION_RECOVER_PROTECTION
            confirmed_flat = confirmation.post_position_side is None and float(getattr(confirmation, 'post_position_qty', 0.0) or 0.0) <= 0.0
            state_updates = {
                'pending_execution_phase': 'confirmed',
                'position_confirmation_level': 'TRADES_CONFIRMED',
                'trade_confirmation_level': 'TRADES_CONFIRMED',
                'needs_trade_reconciliation': False,
                'fills_reconciled': True,
                'can_open_new_position': False,
                'can_modify_position': False,
            }
            result_updates = {
                'status': 'RECOVER_READY',
                'should_freeze': False,
                'freeze_reason': None,
                'execution_phase': 'confirmed',
            }
            if confirmed_flat:
                state_updates.update(
                    build_flat_reset_state_updates(
                        state=self.state_store.load_state(),
                        state_ts=str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                    )
                )
                # Terminal flat facts win over earlier confirmed-phase hints.
                state_updates['position_confirmation_level'] = 'TRADES_CONFIRMED'
                state_updates['trade_confirmation_level'] = 'TRADES_CONFIRMED'
            return ReadonlyRecheckDecision(
                status=READONLY_RECHECK_RECOVER_READY,
                action='recover_ready',
                summary={**base_summary, 'status': READONLY_RECHECK_RECOVER_READY, 'action': 'recover_ready', 'risk_action': risk_action, 'confirmed_flat': confirmed_flat},
                state_updates=state_updates,
                result_updates=result_updates,
                should_freeze=False,
                freeze_reason=None,
                recover_check=build_readonly_recheck_recover_check(
                    decision={
                        'status': READONLY_RECHECK_RECOVER_READY,
                        'freeze_reason': None,
                        'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                    }
                ),
            )

        if manual_review_reason is not None:
            unresolved_reason = confirmation.freeze_reason or manual_review_reason
            if manual_review_reason == 'protection_missing':
                unresolved_reason = 'protective_order_missing'
                pending_phase = 'frozen'
                return ReadonlyRecheckDecision(
                    status=READONLY_RECHECK_FREEZE,
                    action='freeze',
                    summary={
                        **base_summary,
                        'status': READONLY_RECHECK_FREEZE,
                        'action': 'freeze',
                        'reason': manual_review_reason,
                        'risk_action': 'FORCE_CLOSE',
                        'confirmed_flat': False,
                    },
                    state_updates={
                        'runtime_mode': 'FROZEN',
                        'freeze_status': 'ACTIVE',
                        'freeze_reason': unresolved_reason,
                        'last_freeze_reason': unresolved_reason,
                        'pending_execution_phase': pending_phase,
                        'can_open_new_position': False,
                        'can_modify_position': False,
                    },
                    result_updates={
                        'status': 'FROZEN',
                        'should_freeze': True,
                        'freeze_reason': unresolved_reason,
                        'execution_phase': pending_phase,
                    },
                    should_freeze=True,
                    freeze_reason=unresolved_reason,
                    recover_check=build_readonly_recheck_recover_check(
                        decision={
                            'status': READONLY_RECHECK_FREEZE,
                            'freeze_reason': unresolved_reason,
                            'reason': manual_review_reason,
                            'stop_reason': 'protective_order_missing',
                            'stop_condition': 'position_open_without_protection',
                            'recover_policy': 'keep_frozen',
                            'recover_stage': 'force_close_without_protection',
                            'pending_execution_phase': 'frozen',
                            'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                        }
                    ),
                )
            return ReadonlyRecheckDecision(
                status=READONLY_RECHECK_FREEZE,
                action='freeze',
                summary={**base_summary, 'status': READONLY_RECHECK_FREEZE, 'action': 'freeze', 'reason': manual_review_reason, 'risk_action': 'MANUAL_REVIEW'},
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': unresolved_reason,
                    'last_freeze_reason': unresolved_reason,
                    'pending_execution_phase': 'frozen',
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
                result_updates={
                    'status': 'FROZEN',
                    'should_freeze': True,
                    'freeze_reason': unresolved_reason,
                    'execution_phase': 'frozen',
                },
                should_freeze=True,
                freeze_reason=unresolved_reason,
                recover_check=build_readonly_recheck_recover_check(
                    decision={
                        'status': READONLY_RECHECK_FREEZE,
                        'freeze_reason': unresolved_reason,
                        'reason': manual_review_reason,
                        'stop_reason': manual_review_reason,
                        'stop_condition': manual_review_stop_condition,
                        'recover_stage': manual_review_recover_stage,
                        'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                    }
                ),
            )

        if confirmation.confirmation_category == 'position_confirmed' and not confirmation.should_freeze:
            pending_phase = 'position_confirmed_pending_trades'
            observe_reason = 'position_confirmed_pending_trades'
            stop_condition = 'position_fact_confirmed_before_trade_rows'
            protective_pending_confirm = bool(trade_summary.get('protective_pending_confirm'))
            fills_count = int((trade_summary.get('fills_count')) or 0)
            orchestration = dict(trade_summary.get('orchestration') or {})
            trigger_phase = str(orchestration.get('trigger_phase') or '')
            flat_after_reduce_only_close = (
                confirmation.post_position_side is None
                and float(confirmation.post_position_qty or 0.0) <= 0.0
                and confirmation.order_status == 'FILLED'
                and bool(trade_summary.get('requested_reduce_only'))
                and not bool(trade_summary.get('has_open_orders'))
                and int(trade_summary.get('open_orders_count') or 0) == 0
                and not list(trade_summary.get('protective_orders') or [])
            )
            if confirmation.order_status == 'PARTIALLY_FILLED':
                pending_phase = 'position_working_partial_fill'
                observe_reason = 'partial_position_working'
                stop_condition = 'partial_fill_position_working'
            elif protective_pending_confirm:
                pending_phase = 'protection_pending_confirm'
                observe_reason = 'protection_pending_confirm'
                stop_condition = 'position_confirmed_but_protection_pending'
            elif trigger_phase == 'management_stop_update_pending_protective':
                pending_phase = 'management_stop_update_pending_protective'
                observe_reason = 'management_stop_update_pending_protective'
                stop_condition = 'position_confirmed_but_protection_pending'
            elif trigger_phase == self.ORCHESTRATION_ENTRY_PENDING_PROTECTIVE:
                pending_phase = self.ORCHESTRATION_ENTRY_PENDING_PROTECTIVE
                observe_reason = 'entry_confirmed_pending_protective'
                stop_condition = 'position_confirmed_but_protection_pending'
            elif flat_after_reduce_only_close:
                flat_state_updates = build_flat_reset_state_updates(
                    state=self.state_store.load_state(),
                    state_ts=str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                )
                flat_state_updates.update(
                    {
                        'position_confirmation_level': 'POSITION_CONFIRMED',
                        'trade_confirmation_level': 'PENDING',
                        'needs_trade_reconciliation': True,
                        'fills_reconciled': False,
                        'can_open_new_position': True,
                        'can_modify_position': True,
                    }
                )
                return ReadonlyRecheckDecision(
                    status=READONLY_RECHECK_PENDING,
                    action='observe',
                    summary={
                        **base_summary,
                        'status': READONLY_RECHECK_PENDING,
                        'action': 'observe',
                        'reason': 'flat_ready_trade_reconciliation_pending',
                        'risk_action': RISK_ACTION_OBSERVE,
                        'confirmed_flat': True,
                    },
                    state_updates=flat_state_updates,
                    result_updates={
                        'status': 'POSITION_CONFIRMED',
                        'should_freeze': False,
                        'freeze_reason': None,
                        'execution_phase': None,
                    },
                    should_freeze=False,
                    freeze_reason=None,
                    recover_check=build_readonly_recheck_recover_check(
                        decision={
                            'status': READONLY_RECHECK_PENDING,
                            'freeze_reason': None,
                            'reason': 'flat_ready_trade_reconciliation_pending',
                            'stop_reason': 'flat_ready_trade_reconciliation_pending',
                            'stop_condition': 'flat_ready_trade_reconciliation_pending',
                            'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                        }
                    ),
                )
            elif confirmation.executed_qty and fills_count == 0:
                pending_phase = 'position_confirmed_pending_trades'
                observe_reason = 'position_confirmed_pending_trades'
                stop_condition = 'trade_rows_missing_after_fill'
            elif confirmation.executed_qty and confirmation.avg_fill_price is None:
                pending_phase = 'position_confirmed_pending_trades'
                observe_reason = 'avg_fill_price_missing'
                stop_condition = 'avg_fill_price_missing_after_fills'
            elif confirmation.executed_qty and not list(confirmation.fee_assets or []):
                pending_phase = 'position_confirmed_pending_trades'
                observe_reason = 'fee_reconciliation_pending'
                stop_condition = 'fee_reconciliation_pending'
            else:
                pending_phase = 'confirmed'
                observe_reason = 'recover_ready'
                stop_condition = 'position_fact_confirmed_before_trade_rows'
            observe_recover_stage = derive_recover_stage(
                result='READY' if pending_phase == 'confirmed' else 'OBSERVE',
                stop_reason=observe_reason,
                stop_condition=stop_condition,
                pending_execution_phase=pending_phase,
                freeze_reason=None,
            )
            risk_action = RISK_ACTION_RECOVER_PROTECTION if observe_recover_stage == RECOVER_STAGE_PROTECTION_PENDING_CONFIRM else RISK_ACTION_OBSERVE
            result_updates = {
                'status': 'POSITION_CONFIRMED',
                'should_freeze': False,
                'freeze_reason': None,
                'execution_phase': None if pending_phase == 'confirmed' else pending_phase,
            }
            state_updates = {
                'pending_execution_phase': None if pending_phase == 'confirmed' else pending_phase,
                'position_confirmation_level': 'POSITION_CONFIRMED',
                'trade_confirmation_level': 'PENDING' if pending_phase != 'confirmed' else 'POSITION_CONFIRMED',
                'needs_trade_reconciliation': pending_phase != 'confirmed',
                'fills_reconciled': pending_phase == 'confirmed',
                'can_open_new_position': False,
                'can_modify_position': False,
            }
            return ReadonlyRecheckDecision(
                status=READONLY_RECHECK_RECOVER_READY if pending_phase == 'confirmed' else READONLY_RECHECK_PENDING,
                action='recover_ready' if pending_phase == 'confirmed' else 'observe',
                summary={**base_summary, 'status': READONLY_RECHECK_RECOVER_READY if pending_phase == 'confirmed' else READONLY_RECHECK_PENDING, 'action': 'recover_ready' if pending_phase == 'confirmed' else 'observe', 'reason': observe_reason, 'risk_action': risk_action, 'confirmed_flat': False},
                state_updates=state_updates,
                result_updates=result_updates,
                should_freeze=False,
                freeze_reason=None,
                recover_check=build_readonly_recheck_recover_check(
                    decision={
                        'status': READONLY_RECHECK_RECOVER_READY if pending_phase == 'confirmed' else READONLY_RECHECK_PENDING,
                        'freeze_reason': None,
                        'reason': observe_reason,
                        'stop_reason': observe_reason,
                        'stop_condition': stop_condition,
                        'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                    }
                ),
            )

        if confirmation.confirmation_category == 'pending':
            pending_reason = confirmation.freeze_reason or 'readonly_recheck_pending_confirmation'
            return ReadonlyRecheckDecision(
                status=READONLY_RECHECK_PENDING,
                action='observe',
                summary={**base_summary, 'status': READONLY_RECHECK_PENDING, 'action': 'observe', 'risk_action': 'OBSERVE'},
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': pending_reason,
                    'last_freeze_reason': pending_reason,
                    'pending_execution_phase': 'submitted',
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
                result_updates={
                    'status': 'FROZEN',
                    'should_freeze': True,
                    'freeze_reason': pending_reason,
                    'execution_phase': 'frozen',
                },
                should_freeze=True,
                freeze_reason=pending_reason,
                recover_check=build_readonly_recheck_recover_check(
                    decision={
                        'status': READONLY_RECHECK_PENDING,
                        'freeze_reason': pending_reason,
                        'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                    }
                ),
            )

        if confirmation.confirmation_category == 'query_failed':
            query_failed_reason = confirmation.freeze_reason or 'readonly_recheck_query_failed'
            return ReadonlyRecheckDecision(
                status=READONLY_RECHECK_PENDING,
                action='observe',
                summary={**base_summary, 'status': READONLY_RECHECK_PENDING, 'action': 'observe', 'reason': query_failed_reason, 'risk_action': 'OBSERVE'},
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': query_failed_reason,
                    'last_freeze_reason': query_failed_reason,
                    'pending_execution_phase': 'submitted',
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
                result_updates={
                    'status': 'FROZEN',
                    'should_freeze': True,
                    'freeze_reason': query_failed_reason,
                    'execution_phase': 'frozen',
                },
                should_freeze=True,
                freeze_reason=query_failed_reason,
                recover_check=build_readonly_recheck_recover_check(
                    decision={
                        'status': READONLY_RECHECK_PENDING,
                        'freeze_reason': query_failed_reason,
                        'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                    }
                ),
            )

        unresolved_reason = confirmation.freeze_reason or 'readonly_recheck_unresolved'
        return ReadonlyRecheckDecision(
            status=READONLY_RECHECK_FREEZE,
            action='freeze',
            summary={**base_summary, 'status': READONLY_RECHECK_FREEZE, 'action': 'freeze', 'risk_action': 'FORCE_CLOSE'},
            state_updates={
                'runtime_mode': 'FROZEN',
                'freeze_status': 'ACTIVE',
                'freeze_reason': unresolved_reason,
                'last_freeze_reason': unresolved_reason,
                'pending_execution_phase': 'frozen',
                'can_open_new_position': False,
                'can_modify_position': False,
            },
            result_updates={
                'status': 'FROZEN',
                'should_freeze': True,
                'freeze_reason': unresolved_reason,
                'execution_phase': 'frozen',
            },
            should_freeze=True,
            freeze_reason=unresolved_reason,
            recover_check=build_readonly_recheck_recover_check(
                decision={
                    'status': READONLY_RECHECK_FREEZE,
                    'freeze_reason': unresolved_reason,
                    'checked_at': str((confirmation.trade_summary or {}).get('decision_ts') or ''),
                }
            ),
        )

    def _apply_readonly_recheck_output(
        self,
        output: dict[str, Any],
        decision: ReadonlyRecheckDecision,
    ) -> dict[str, Any]:
        updated = dict(output)
        state_payload = dict(updated.get('state') or {})
        state_payload.update(decision.state_updates)
        updated['state'] = state_payload

        result_payload = dict(updated.get('result') or {})
        trade_summary = dict(result_payload.get('trade_summary') or {})
        trade_summary['readonly_recheck'] = decision.summary
        trade_summary['confirm_context'] = dict(decision.summary.get('confirm_context') or trade_summary.get('confirm_context') or {})
        if decision.summary.get('confirmation_category') is not None:
            trade_summary['confirmation_category'] = decision.summary.get('confirmation_category')
        result_payload['trade_summary'] = trade_summary
        if decision.summary.get('confirmation_status') is not None:
            result_payload['confirmation_status'] = decision.summary.get('confirmation_status')
        if decision.summary.get('confirmed_order_status') is not None:
            result_payload['confirmed_order_status'] = decision.summary.get('confirmed_order_status')
        if decision.summary.get('reconcile_status') is not None:
            result_payload['reconcile_status'] = decision.summary.get('reconcile_status')
        result_payload.update(dict(decision.result_updates or {}))

        projected_pending_phase = (
            dict(result_payload.get('state_updates') or {}).get('pending_execution_phase')
            or state_payload.get('pending_execution_phase')
            or trade_summary['confirm_context'].get('pending_execution_phase')
            or decision.summary.get('pending_execution_phase')
        )

        if decision.should_freeze:
            result_payload.setdefault('status', 'FROZEN')
            result_payload.setdefault('should_freeze', True)
            result_payload.setdefault('freeze_reason', decision.freeze_reason)
            result_payload.setdefault('execution_phase', 'frozen')
        else:
            confirmation_category = str(decision.summary.get('confirmation_category') or '')
            confirmation_status = str(decision.summary.get('confirmation_status') or '')
            confirmed_order_status = decision.summary.get('confirmed_order_status')
            executed_qty = decision.summary.get('executed_qty')
            avg_fill_price = decision.summary.get('confirm_summary', {}).get('avg_fill_price')
            if avg_fill_price is None:
                avg_fill_price = result_payload.get('avg_fill_price')
            exchange_order_ids = decision.summary.get('confirm_summary', {}).get('exchange_order_ids')
            if exchange_order_ids is None:
                exchange_order_ids = result_payload.get('exchange_order_ids')
            confirmed_flat = bool(decision.summary.get('confirmed_flat'))

            if confirmation_category == 'confirmed' and confirmation_status == 'CONFIRMED':
                result_payload['status'] = 'FILLED'
                result_payload['should_freeze'] = False
                result_payload['freeze_reason'] = None
                result_payload['execution_phase'] = 'confirmed'
                if confirmed_order_status is not None:
                    result_payload['confirmed_order_status'] = confirmed_order_status
                if executed_qty is not None:
                    result_payload['executed_qty'] = executed_qty
                if avg_fill_price is not None:
                    result_payload['avg_fill_price'] = avg_fill_price
                if exchange_order_ids is not None:
                    result_payload['exchange_order_ids'] = exchange_order_ids
            elif confirmation_category == 'position_confirmed':
                result_payload['status'] = 'POSITION_CONFIRMED'
                result_payload['should_freeze'] = False
                result_payload['freeze_reason'] = None
                # Keep result projection aligned with readonly recheck/state updates instead of stale pre-recheck frozen phase.
                pending_phase = projected_pending_phase
                # Terminal flat facts outrank audit hints: keep close-flat reconciliation rounds phase-less.
                if confirmed_flat or pending_phase in {None, '', 'confirmed'}:
                    result_payload['execution_phase'] = None
                elif pending_phase in {
                    'protection_pending_confirm',
                    'entry_confirmed_pending_protective',
                    'management_stop_update_pending_protective',
                    'position_working_partial_fill',
                    'position_confirmed_pending_trades',
                }:
                    result_payload['execution_phase'] = pending_phase
                else:
                    result_payload['execution_phase'] = 'position_confirmed_pending_trades'
            else:
                result_payload.setdefault('status', 'RECOVER_READY')
                result_payload.setdefault('should_freeze', False)
                result_payload.setdefault('freeze_reason', None)
                result_payload.setdefault('execution_phase', 'confirmed')

        # Terminal flat facts outrank stale pending/audit phase hints during readonly recheck merge.
        if bool(decision.summary.get('confirmed_flat')):
            result_payload['execution_phase'] = None
            state_updates = dict(result_payload.get('state_updates') or {})
            state_updates['pending_execution_phase'] = None
            result_payload['state_updates'] = state_updates

        state_payload['strategy_protection_intent'] = self._build_strategy_protection_intent(state_payload, result_payload)
        updated['state'] = state_payload

        recover_check = dict(decision.recover_check or {})
        if not recover_check.get('checked_at'):
            recover_check['checked_at'] = state_payload.get('state_ts') or result_payload.get('result_ts')

        previous_recover_result = str(state_payload.get('last_recover_result') or '')
        previous_recover_check = dict(state_payload.get('recover_check') or {})
        current_stop_condition = recover_check.get('stop_condition')
        positive_protective_fact = False
        manual_review_stage_by_stop_condition = {
            'manual_open_orders_side_or_qty_conflict': 'manual_open_orders_side_or_qty_conflict',
            'manual_reduce_only_position_not_flat': 'manual_reduce_only_position_not_flat',
        }
        if recover_check.get('recover_policy') == 'manual_review':
            mapped_manual_review_stage = manual_review_stage_by_stop_condition.get(str(current_stop_condition or ''))
            if mapped_manual_review_stage:
                recover_check['recover_stage'] = mapped_manual_review_stage
        if current_stop_condition in {
            'protection_orders_missing',
            'protection_stop_missing',
            'protection_tp_missing',
        }:
            recover_check['recover_policy_display'] = 'recover_protection'
            recover_check.setdefault('legacy_recover_policy', recover_check.get('recover_policy'))
        elif current_stop_condition in {
            'protection_submit_gate_blocked',
            'protection_semantic_mismatch',
            'protection_semantic_position_side_mismatch',
            'protection_semantic_type_mismatch',
            'protection_semantic_stop_payload_mismatch',
            'protection_semantic_tp_payload_mismatch',
            'relapse_after_recover_ready_mismatch',
            'relapse_after_recover_ready_protection_missing',
            'relapse_after_recover_ready_query_failed',
        }:
            recover_check['recover_policy_display'] = 'manual_review'
            recover_check.setdefault('legacy_recover_policy', recover_check.get('recover_policy'))
        protective_validation = dict((result_payload.get('trade_summary') or {}).get('protective_validation') or {})
        protective_visibility = dict(protective_validation.get('exchange_visibility') or {})
        protective_recover_summary = dict((result_payload.get('trade_summary') or {}).get('protective_recover') or {})
        has_negative_recover_fact = BinanceRealExecutor._protective_recover_has_negative_fact(
            recover=protective_recover_summary,
            protective_validation=protective_validation,
            submit_readback_empty=bool(dict(protective_validation.get('summary') or {}).get('submit_readback_empty')),
        )
        positive_protective_fact = bool(
            not has_negative_recover_fact
            and protective_validation.get('ok')
            and (
                protective_visibility.get('confirmed_via_exchange_visibility')
                or protective_visibility.get('exchange_visible')
                or state_payload.get('protective_order_status') == 'ACTIVE'
                or dict(result_payload.get('state_updates') or {}).get('protective_order_status') == 'ACTIVE'
            )
        )
        if previous_recover_result in {'READY', 'RECOVERED'} and not positive_protective_fact:
            if current_stop_condition == 'protection_orders_missing':
                recover_check['recover_stage'] = 'recover_relapse'
                recover_check['recover_policy'] = 'manual_review'
                recover_check['recover_policy_display'] = 'manual_review'
                recover_check['legacy_recover_policy'] = 'manual_review'
                recover_check['stop_category'] = 'manual_review'
                recover_check['stop_condition'] = 'relapse_after_recover_ready_protection_missing'
                recover_check['stop_reason'] = 'protection_missing'
            elif current_stop_condition == 'readonly_query_failed':
                recover_check['recover_stage'] = 'recover_relapse'
                recover_check['recover_policy'] = 'manual_review'
                recover_check['recover_policy_display'] = 'manual_review'
                recover_check['legacy_recover_policy'] = 'manual_review'
                recover_check['stop_category'] = 'manual_review'
                recover_check['stop_condition'] = 'relapse_after_recover_ready_query_failed'
                recover_check['stop_reason'] = 'query_failed'
            elif current_stop_condition in {
                'protection_semantic_mismatch',
                'protection_semantic_position_side_mismatch',
                'protection_semantic_type_mismatch',
                'protection_semantic_stop_payload_mismatch',
                'protection_semantic_tp_payload_mismatch',
                'non_retryable_confirmation_category',
                'unresolved_confirmation_state',
                'shared_budget_exhausted',
            } or previous_recover_check.get('recover_ready'):
                recover_check['recover_stage'] = 'recover_relapse'
                recover_check['recover_policy'] = 'manual_review'
                recover_check['recover_policy_display'] = 'manual_review'
                recover_check['legacy_recover_policy'] = 'manual_review'
                recover_check['stop_category'] = 'manual_review'
                recover_check['stop_condition'] = 'relapse_after_recover_ready_mismatch'
                recover_check['stop_reason'] = str(recover_check.get('stop_reason') or 'readonly_recheck_freeze')

        recover_timeline = append_recover_record(state_payload.get('recover_timeline'), recover_check)
        state_payload['recover_check'] = recover_check
        state_payload['recover_timeline'] = recover_timeline
        updated['state'] = state_payload
        state_updates = dict(result_payload.get('state_updates') or {})
        state_updates.update(decision.state_updates)
        protective_validation = dict((result_payload.get('trade_summary') or {}).get('protective_validation') or {})
        protective_visibility = dict(protective_validation.get('exchange_visibility') or {})
        protective_recover_summary = dict((result_payload.get('trade_summary') or {}).get('protective_recover') or {})
        has_negative_recover_fact = BinanceRealExecutor._protective_recover_has_negative_fact(
            recover=protective_recover_summary,
            protective_validation=protective_validation,
            submit_readback_empty=bool(dict(protective_validation.get('summary') or {}).get('submit_readback_empty')),
        )
        positive_protective_fact = bool(
            not has_negative_recover_fact
            and protective_validation.get('ok')
            and (
                protective_visibility.get('confirmed_via_exchange_visibility')
                or protective_visibility.get('exchange_visible')
                or state_payload.get('protective_order_status') == 'ACTIVE'
                or state_updates.get('protective_order_status') == 'ACTIVE'
            )
        )
        force_close_terminal_fact = bool(
            current_stop_condition == 'position_open_without_protection'
            or recover_check.get('risk_action') == 'FORCE_CLOSE'
            or recover_check.get('recover_stage') == 'force_close_without_protection'
        )
        if positive_protective_fact and not force_close_terminal_fact:
            recover_check['recover_ready'] = True
            recover_check['recover_stage'] = 'recover_ready'
            recover_check['recover_policy'] = 'recover_ready'
            recover_check['recover_policy_display'] = 'recover_ready'
            recover_check['legacy_recover_policy'] = 'recover_ready'
            recover_check['stop_category'] = 'observe_only'
            recover_check['stop_condition'] = 'protective_order_visible_on_exchange'
            recover_check['stop_reason'] = 'success_protective_visible'
            state_updates['pending_execution_block_reason'] = None
            state_payload['pending_execution_block_reason'] = None
            state_payload['strategy_protection_intent'] = self._build_strategy_protection_intent(state_payload, result_payload)
        # Close-after-flat terminal settle must not re-import stale pending/execution hints from the confirmer.
        if bool(decision.summary.get('confirmed_flat')):
            state_updates['pending_execution_phase'] = None
            result_payload['execution_phase'] = None
        elif positive_protective_fact and state_updates.get('pending_execution_phase') in {
            'protection_pending_confirm',
            'entry_confirmed_pending_protective',
            'management_stop_update_pending_protective',
        }:
            state_updates['pending_execution_phase'] = None
        state_updates['strategy_protection_intent'] = state_payload.get('strategy_protection_intent')
        result_payload['state_updates'] = state_updates
        config_symbol = getattr(getattr(self, 'config', None), 'symbol', None)
        _, result_payload, _ = attach_execution_confirm_async_operation(
            market_decision_ts=(result_payload.get('trade_summary') or {}).get('decision_ts') or state_payload.get('state_ts') or result_payload.get('result_ts'),
            symbol=state_payload.get('symbol') or config_symbol,
            strategy_ts=(result_payload.get('trade_summary') or {}).get('strategy_ts') or state_payload.get('strategy_ts'),
            state_payload=state_payload,
            result_payload=result_payload,
        )
        state_updates = dict(result_payload.get('state_updates') or {})
        state_payload.update({key: value for key, value in state_updates.items() if key != 'strategy_protection_intent'})
        if state_updates.get('strategy_protection_intent') is not None:
            state_payload['strategy_protection_intent'] = state_updates.get('strategy_protection_intent')
        updated['state'] = state_payload
        updated['result'] = result_payload

        save_state = getattr(self.state_store, 'save_state', None)
        if callable(save_state):
            current_state = self.state_store.load_state()
            next_state = replace(current_state)
            for key, value in updated['state'].items():
                if hasattr(next_state, key):
                    setattr(next_state, key, value)
            save_state(next_state)
            updated['state'] = asdict(next_state)

        return updated

    def _maybe_attempt_recover(self, state: LiveStateSnapshot, *, run_id: str) -> dict[str, Any] | None:
        if state.runtime_mode != 'FROZEN':
            return None
        self.event_log.append(
            'recover_attempt',
            {
                'run_id': run_id,
                'consistency_status': state.consistency_status,
                'pending_execution_phase': state.pending_execution_phase,
                'freeze_reason': state.freeze_reason,
                'latest_recover_check': state.recover_check,
            },
        )
        protective_cleanup = self._maybe_cleanup_lingering_protective_orders(state=state, run_id=run_id)
        if protective_cleanup is not None:
            return protective_cleanup
        reduce_only_close = self._maybe_force_reduce_only_close_without_protection(state=state, run_id=run_id)
        if reduce_only_close is not None:
            return reduce_only_close
        decision = self.freeze_controller.evaluate_recover(state)
        next_state = state
        for key, value in decision.state_updates.items():
            setattr(next_state, key, value)
        if next_state.recover_check:
            next_state.recover_check['decision'] = decision.result
        if decision.allowed and decision.result == RECOVER_RESULT_ALLOWED:
            next_state.freeze_reason = None
        self.state_store.save_state(next_state)
        self.event_log.append(
            'recover_result',
            {
                'run_id': run_id,
                'allowed': decision.allowed,
                'result': decision.result,
                'reason': decision.reason,
                'recover_check': next_state.recover_check,
                'recover_timeline_tail': next_state.recover_timeline[-3:],
            },
        )
        return {
            'allowed': decision.allowed,
            'result': decision.result,
            'reason': decision.reason,
            'recover_check': next_state.recover_check,
        }

    def _maybe_cleanup_lingering_protective_orders(self, *, state: LiveStateSnapshot, run_id: str) -> dict[str, Any] | None:
        protective_orders = list(state.exchange_protective_orders or [])
        position_side = state.exchange_position_side
        position_qty = float(state.exchange_position_qty or 0.0)
        flat_on_exchange = position_side in {None, '', 'flat'} and position_qty <= 0.0
        legacy_freeze_reason_match = state.freeze_reason == 'protective_orders_present_while_flat'
        fact_cleanup_candidate = flat_on_exchange and bool(protective_orders)

        self.event_log.append(
            'protective_cleanup_probe',
            {
                'run_id': run_id,
                'freeze_reason': state.freeze_reason,
                'exchange_position_side': position_side,
                'exchange_position_qty': position_qty,
                'protective_order_count': len(protective_orders),
                'flat_on_exchange': flat_on_exchange,
                'legacy_freeze_reason_match': legacy_freeze_reason_match,
                'fact_cleanup_candidate': fact_cleanup_candidate,
            },
        )
        if not legacy_freeze_reason_match and not fact_cleanup_candidate:
            return None
        if not protective_orders:
            return None
        if not flat_on_exchange:
            return None

        executor = getattr(self.engine, 'executor_module', None)
        if not isinstance(executor, BinanceRealExecutor):
            return None

        self.event_log.append(
            'protective_cleanup_start',
            {
                'run_id': run_id,
                'protective_orders': protective_orders,
            },
        )
        cancel_requests = []
        for order in protective_orders:
            cancel_requests.append(
                BinanceCancelOrderRequest(
                    symbol=self.config.symbol,
                    order_id=(str(order.get('order_id')) if order.get('order_id') is not None else None),
                    client_order_id=(str(order.get('client_order_id')) if order.get('client_order_id') else None),
                    metadata={
                        'phase': 'protective_cleanup_after_flat',
                        'protective_kind': order.get('kind'),
                        'algo_order': True,
                    },
                )
            )
        cancel_result = executor._cancel_existing_protective_orders(cancel_requests)
        if not cancel_result.get('ok'):
            reason = f"protective_cleanup_cancel_failed:{cancel_result.get('reason') or 'unknown'}"
            self.event_log.append(
                'recover_result',
                {
                    'run_id': run_id,
                    'allowed': False,
                    'result': 'BLOCKED',
                    'reason': reason,
                    'recover_check': state.recover_check,
                    'recover_timeline_tail': state.recover_timeline[-3:],
                },
            )
            return {
                'allowed': False,
                'result': 'BLOCKED',
                'reason': reason,
                'recover_check': state.recover_check,
            }
        receipts = [
            {
                'ok': True,
                'client_order_id': row.get('client_order_id'),
                'order_id': row.get('exchange_order_id'),
                'receipt': row,
            }
            for row in (cancel_result.get('receipts') or [])
        ]

        next_state = apply_flat_reset_to_state(
            state,
            state_ts=state.state_ts,
            account_equity=state.account_equity,
            available_margin=state.available_margin,
        )
        next_state.freeze_reason = None
        next_state.runtime_mode = 'ACTIVE'
        next_state.freeze_status = 'NONE'
        next_state.last_recover_result = RECOVER_RESULT_ALLOWED
        next_state.last_recover_at = state.state_ts
        next_state.recover_attempt_count = int(state.recover_attempt_count or 0) + 1
        next_state.recover_check = {
            'checked_at': state.state_ts,
            'source': 'protective_cleanup_after_flat',
            'result': RECOVER_RESULT_ALLOWED,
            'allowed': True,
            'reason': 'canceled_lingering_protective_orders_after_flat',
            'pending_execution_phase': None,
            'consistency_status': 'OK',
            'runtime_mode': 'ACTIVE',
            'recover_ready': True,
            'requires_manual_resume': False,
            'guard_decision': 'cleanup_then_flat_reset',
            'receipts': receipts,
        }
        next_state.recover_timeline = append_recover_record(state.recover_timeline, next_state.recover_check)
        self.state_store.save_state(next_state)
        self.event_log.append(
            'protective_cleanup_success',
            {
                'run_id': run_id,
                'receipts': receipts,
            },
        )
        self.event_log.append(
            'recover_result',
            {
                'run_id': run_id,
                'allowed': True,
                'result': RECOVER_RESULT_ALLOWED,
                'reason': 'canceled_lingering_protective_orders_after_flat',
                'recover_check': next_state.recover_check,
                'recover_timeline_tail': next_state.recover_timeline[-3:],
            },
        )
        return {
            'allowed': True,
            'result': RECOVER_RESULT_ALLOWED,
            'reason': 'canceled_lingering_protective_orders_after_flat',
            'recover_check': next_state.recover_check,
        }

    def _maybe_force_reduce_only_close_without_protection(self, *, state: LiveStateSnapshot, run_id: str) -> dict[str, Any] | None:
        effective_position_side = state.exchange_position_side
        effective_position_qty = float(state.exchange_position_qty or 0.0)
        effective_entry_price = state.exchange_entry_price
        if state.freeze_reason == 'protective_order_missing' and (effective_position_side not in {'long', 'short'} or effective_position_qty <= 0.0):
            readonly_client = getattr(getattr(self.engine, 'pre_run_reconcile_module', None), 'readonly_client', None)
            try:
                exchange_position = readonly_client.get_position_snapshot(self.config.symbol) if readonly_client is not None else None
            except Exception:
                exchange_position = None
            if exchange_position is not None and exchange_position.side in {'long', 'short'} and float(exchange_position.qty or 0.0) > 0.0:
                effective_position_side = exchange_position.side
                effective_position_qty = float(exchange_position.qty or 0.0)
                effective_entry_price = exchange_position.entry_price

        self.event_log.append(
            'reduce_only_close_probe',
            {
                'run_id': run_id,
                'freeze_reason': state.freeze_reason,
                'exchange_position_side': state.exchange_position_side,
                'exchange_position_qty': state.exchange_position_qty,
                'effective_position_side': effective_position_side,
                'effective_position_qty': effective_position_qty,
                'active_strategy': state.active_strategy,
            },
        )
        if state.freeze_reason != 'protective_order_missing':
            return None
        if effective_position_side not in {'long', 'short'} or effective_position_qty <= 0.0:
            return None

        executor = getattr(self.engine, 'executor_module', None)
        if executor is None or not hasattr(executor, 'execute'):
            return None

        market_provider = self.market_provider
        if market_provider is None:
            return None

        market = build_market_snapshot(
            provider=market_provider,
            symbol=self.config.symbol,
            decision_time=datetime.now(timezone.utc),
        )
        plan = FinalActionPlan(
            plan_ts=market.decision_ts,
            bar_ts=market.bar_ts,
            action_type='close',
            target_strategy=state.active_strategy or 'manual_residual_cleanup',
            target_side=effective_position_side,
            reason='emergency_close_after_protective_missing',
            qty_mode='full_close',
            qty=None,
            price_hint=market.current_price,
            stop_price=None,
            risk_fraction=state.risk_fraction,
            conflict_context={'protective_missing_force_close': True},
            requires_execution=True,
            close_reason='emergency_close_after_protective_missing',
        )
        self.event_log.append(
            'reduce_only_close_start',
            {
                'run_id': run_id,
                'plan': asdict(plan),
            },
        )
        effective_state = replace(state)
        effective_state.exchange_position_side = effective_position_side
        effective_state.exchange_position_qty = effective_position_qty
        effective_state.exchange_entry_price = effective_entry_price
        result = executor.execute(plan, market, effective_state)
        self.state_store.save_result(state, result)
        updated_state = self.state_store.load_state()
        self.event_log.append(
            'reduce_only_close_result',
            {
                'run_id': run_id,
                'status': result.status,
                'freeze_reason': result.freeze_reason,
                'execution_phase': result.execution_phase,
                'confirmation_status': result.confirmation_status,
            },
        )
        return {
            'allowed': updated_state.runtime_mode == 'ACTIVE',
            'result': updated_state.last_recover_result,
            'reason': 'forced_reduce_only_close_after_protective_missing',
            'recover_check': updated_state.recover_check,
        }

    def _build_sender(self) -> MessageToolDiscordSender:
        return MessageToolDiscordSender(
            real_send_enabled=bool(getattr(self.config, 'discord_real_send_enabled', False)),
            message_tool_enabled=bool(getattr(self.config, 'discord_message_tool_enabled', False)),
            require_idempotency=bool(getattr(self.config, 'discord_send_require_idempotency', True)),
            ledger_path=getattr(self.config, 'discord_send_ledger_path', None),
            receipt_store_path=getattr(self.config, 'discord_send_receipt_log_path', None),
            retry_limit=int(getattr(self.config, 'discord_send_retry_limit', 3) or 3),
            transport=build_discord_transport(getattr(self.config, 'discord_transport', 'unconfigured')),
            execution_confirmation_real_send_enabled=bool(getattr(self.config, 'discord_execution_confirmation_real_send_enabled', False)),
        )

    def _maybe_run_discord_sender(self, output: dict[str, Any]) -> dict[str, Any] | None:
        result_payload = output.get('result') or {}
        state_payload = output.get('state') or {}
        publishable_output = self._select_publishable_output({'result': result_payload, 'state': state_payload})
        dispatch_preview = build_dispatch_preview(self.config, publishable_output.get('result') or {}, publishable_output.get('state') or {})
        sender = self._build_sender()
        primary_preview = dispatch_preview.get('primary_preview') or {}
        payload_preview = primary_preview.get('payload_preview') or dispatch_preview.get('payload_preview')
        primary_kind = dispatch_preview.get('primary_kind') or dispatch_preview.get('kind')

        rehearsal_preview = dispatch_preview.get('rehearsal_preview')
        rehearsal_open = bool(getattr(self.config, 'discord_rehearsal_real_send_enabled', False))
        use_rehearsal = payload_preview is None and rehearsal_open and rehearsal_preview is not None
        selected_preview = rehearsal_preview.get('payload_preview') if use_rehearsal else payload_preview
        selected_kind = 'rehearsal_notification' if use_rehearsal else primary_kind

        if selected_preview is None:
            attempt = {
                'attempted': False,
                'sent': False,
                'reason': 'payload_not_sendable',
                'payload_kind': selected_kind,
                'rehearsal_mode': use_rehearsal,
                'idempotency_key': dispatch_preview.get('idempotency_key'),
                'receipt': None,
                'receipt_store_path': getattr(self.config, 'discord_send_receipt_log_path', None),
                'failure': None,
                'provider_response': None,
                'transport_name': getattr(sender.transport, 'transport_name', sender.transport.__class__.__name__),
            }
            setattr(self, '_last_discord_send_attempt', attempt)
            return attempt

        from .discord_publisher import DiscordMessagePayload
        payload = DiscordMessagePayload(
            channel_id=selected_preview.get('channel_id') or self.config.discord_execution_channel_id,
            content=selected_preview.get('content') or '',
            metadata=selected_preview.get('metadata') or {},
        )
        sender_result = sender.send(payload)
        receipt = sender_result.get('receipt') or {}
        attempt = {
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
        setattr(self, '_last_discord_send_attempt', attempt)
        return attempt

    def _write_dispatch_preview_audit(self, *, run_id: str, market: MarketSnapshot, output: dict[str, Any]) -> None:
        runtime_dir = Path(self.runtime_status_path).parent
        audit_dir = runtime_dir / 'dispatch_previews'
        audit_dir.mkdir(parents=True, exist_ok=True)
        publishable_output = self._select_publishable_output(output)
        preview = build_dispatch_preview(self.config, publishable_output.get('result'), publishable_output.get('state'))
        payload = {
            'run_id': run_id,
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'bar_ts': market.bar_ts,
            'target_channel_id': self.config.discord_execution_channel_id,
            'preview': preview,
            'real_send_enabled': bool(getattr(self.config, 'discord_real_send_enabled', False)),
            'message_tool_enabled': bool(getattr(self.config, 'discord_message_tool_enabled', False)),
            'discord_transport': getattr(self.config, 'discord_transport', 'unconfigured'),
            'discord_rehearsal_real_send_enabled': bool(getattr(self.config, 'discord_rehearsal_real_send_enabled', False)),
            'discord_send_ledger_path': getattr(self.config, 'discord_send_ledger_path', None),
            'discord_send_receipt_log_path': getattr(self.config, 'discord_send_receipt_log_path', None),
            'discord_send_retry_limit': int(getattr(self.config, 'discord_send_retry_limit', 3) or 3),
            'binance_dry_run': self.config.dry_run,
            'binance_submit_enabled': self.config.submit_enabled,
        }
        (audit_dir / f'{run_id}.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    def _write_audit_artifacts(
        self,
        *,
        run_id: str,
        market: MarketSnapshot,
        output: dict[str, Any],
        confirm_summary: dict[str, Any],
        dispatch_preview: dict[str, Any],
        config_validation: dict[str, Any],
    ) -> dict[str, str]:
        result_payload = output.get('result') or {}
        state_payload = output.get('state') or {}
        trade_summary = result_payload.get('trade_summary') or {}
        pre_run_reconcile_module = getattr(self.engine, 'pre_run_reconcile_module', None)
        account_snapshot_summary = getattr(pre_run_reconcile_module, 'last_account_snapshot_summary', None)
        execution_receipt_payload = {
            'run_id': run_id,
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'bar_ts': market.bar_ts,
            'strategy_ts': market.strategy_ts,
            'execution_attributed_bar': market.execution_attributed_bar,
            'plan': output.get('plan') or {},
            'result': {
                'status': result_payload.get('status'),
                'action_type': result_payload.get('action_type'),
                'execution_phase': result_payload.get('execution_phase'),
                'confirmation_status': result_payload.get('confirmation_status'),
                'confirmed_order_status': result_payload.get('confirmed_order_status'),
                'reconcile_status': result_payload.get('reconcile_status'),
                'freeze_reason': result_payload.get('freeze_reason'),
                'exchange_order_ids': result_payload.get('exchange_order_ids') or [],
                'executed_qty': result_payload.get('executed_qty'),
                'avg_fill_price': result_payload.get('avg_fill_price'),
                'fees': result_payload.get('fees'),
                'post_position_side': result_payload.get('post_position_side'),
                'post_position_qty': result_payload.get('post_position_qty'),
                'post_entry_price': result_payload.get('post_entry_price'),
                'error_code': result_payload.get('error_code'),
                'error_message': result_payload.get('error_message'),
            },
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
                'position_confirmation_level': state_payload.get('position_confirmation_level'),
                'trade_confirmation_level': state_payload.get('trade_confirmation_level'),
                'needs_trade_reconciliation': state_payload.get('needs_trade_reconciliation'),
                'fills_reconciled': state_payload.get('fills_reconciled'),
                'exchange_position_side': state_payload.get('exchange_position_side'),
                'exchange_position_qty': state_payload.get('exchange_position_qty'),
                'exchange_entry_price': state_payload.get('exchange_entry_price'),
            },
            'account_snapshot_summary': account_snapshot_summary,
            'config_validation': config_validation,
        }
        discord_receipt_payload = {
            'run_id': run_id,
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'dispatch_preview': dispatch_preview,
            'last_discord_send_attempt': getattr(self, '_last_discord_send_attempt', None),
            'config_validation': config_validation,
        }
        return {
            'execution_receipt': self.audit_writer.write('execution_receipts', run_id, execution_receipt_payload),
            'discord_receipt': self.audit_writer.write('discord_receipts', run_id, discord_receipt_payload),
        }

    def _write_status(
        self,
        *,
        phase: str,
        summary: WorkerRunSummary,
        last_exception: dict[str, Any] | None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        market: MarketSnapshot | None = None,
        output: dict[str, Any] | None = None,
        config_validation: dict[str, Any] | None = None,
        audit_artifact_paths: dict[str, str] | None = None,
    ) -> None:
        result_payload = {} if output is None else (output.get('result') or {})
        trade_summary = result_payload.get('trade_summary') or {}
        publishable_output = None if output is None else self._select_publishable_output(output)
        dispatch_preview = None if publishable_output is None else build_dispatch_preview(self.config, publishable_output.get('result') or {}, publishable_output.get('state') or {})
        last_send_attempt = getattr(self, '_last_discord_send_attempt', None)
        last_receipt = None if not last_send_attempt else last_send_attempt.get('receipt')
        payload = {
            'phase': phase,
            'ts': datetime.now(timezone.utc).isoformat(),
            'dry_run': self.config.dry_run,
            'submit_enabled': self.config.submit_enabled,
            'symbol': self.config.symbol,
            'runtime_status_path': str(self.runtime_status_path),
            'event_log_path': str(self.event_log_path),
            'consecutive_failures': summary.failure_count,
            'backoff_seconds': summary.backoff_seconds,
            'last_exception': last_exception,
            'last_run_summary': asdict(summary),
            'last_started_at': started_at.isoformat() if started_at else None,
            'last_completed_at': completed_at.isoformat() if completed_at else None,
            'discord_send_gate': None if dispatch_preview is None else dispatch_preview.get('send_gate'),
            'last_discord_send_attempt': last_send_attempt,
            'latest_discord_receipt': last_receipt,
            'latest_discord_receipt_summary': None if not last_receipt else {
                'message_id': last_receipt.get('message_id'),
                'channel_id': last_receipt.get('provider_channel_id') or last_receipt.get('target'),
                'transport_name': last_receipt.get('transport_name'),
                'payload_kind': last_receipt.get('payload_kind'),
                'idempotency_key': last_receipt.get('idempotency_key'),
                'sent_at': last_receipt.get('sent_at'),
                'status': last_receipt.get('status'),
                'receipt_store_path': None if not last_send_attempt else last_send_attempt.get('receipt_store_path'),
            },
            'latest_market_summary': None if market is None else {
                'decision_ts': market.decision_ts,
                'bar_ts': market.bar_ts,
                'strategy_ts': market.strategy_ts,
                'execution_attributed_bar': market.execution_attributed_bar,
                'source_status': market.source_status,
            },
            'latest_result_summary': None if output is None else {
                'consistency_status': output['state'].get('consistency_status'),
                'runtime_mode': output['state'].get('runtime_mode'),
                'freeze_status': output['state'].get('freeze_status'),
                'freeze_reason': output['state'].get('freeze_reason'),
                'pending_execution_phase': output['state'].get('pending_execution_phase'),
                'pending_execution_block_reason': output['state'].get('pending_execution_block_reason'),
                'position_confirmation_level': output['state'].get('position_confirmation_level'),
                'trade_confirmation_level': output['state'].get('trade_confirmation_level'),
                'needs_trade_reconciliation': output['state'].get('needs_trade_reconciliation'),
                'fills_reconciled': output['state'].get('fills_reconciled'),
                'strategy_protection_intent': output['state'].get('strategy_protection_intent'),
                'execution_retry_backoff': output['state'].get('execution_retry_backoff'),
                'plan_action': output['plan'].get('action_type'),
                'plan_reason': output['plan'].get('reason'),
                'plan_debug': output.get('plan_debug'),
                'result_status': result_payload.get('status'),
                'confirmation_status': result_payload.get('confirmation_status'),
                'execution_phase': result_payload.get('execution_phase'),
                'submit_gate': trade_summary.get('submit_gate'),
                'confirm_summary': build_execution_confirm_summary(result_payload),
                'sender_dispatch_preview': dispatch_preview,
                'dispatch_preview_audit_path': None if output is None else str(Path(self.runtime_status_path).parent / 'dispatch_previews' / f"{summary.run_id}.json"),
                'audit_artifact_paths': audit_artifact_paths or {},
            },
            'runtime_config_validation': config_validation,
            'audit_artifact_paths': audit_artifact_paths or {},
            'recover_check': None if output is None else output['state'].get('recover_check'),
            'recover_timeline': [] if output is None else (output['state'].get('recover_timeline') or []),
        }
        self.status_store.write(payload)


def build_initial_state(now_iso: str) -> LiveStateSnapshot:
    return LiveStateSnapshot(
        state_ts=now_iso,
        consistency_status='OK',
        freeze_reason=None,
        account_equity=0.0,
        available_margin=0.0,
        exchange_position_side=None,
        exchange_position_qty=0.0,
        exchange_entry_price=None,
        active_strategy='none',
        active_side=None,
        strategy_entry_time=None,
        strategy_entry_price=None,
        stop_price=None,
        risk_fraction=None,
        runtime_mode='ACTIVE',
        freeze_status='NONE',
        last_freeze_reason=None,
        last_freeze_at=None,
        last_recover_at=None,
        last_recover_result=None,
        recover_attempt_count=0,
        pending_execution_phase=None,
        last_publishable_result={},
        position_confirmation_level='NONE',
        trade_confirmation_level='NONE',
        needs_trade_reconciliation=False,
        fills_reconciled=False,
        last_confirmed_order_ids=[],
        protective_order_status='NONE',
        exchange_protective_orders=[],
        protective_order_last_sync_ts=None,
        protective_order_last_sync_action=None,
        protective_order_freeze_reason=None,
        protective_phase_status='NONE',
        strategy_protection_intent={},
        execution_retry_backoff={},
        pending_execution_block_reason=None,
        last_processed_strategy_ts=None,
        recover_check={},
        recover_timeline=[],
    )


def build_placeholder_market(symbol: str, now_iso: str) -> MarketSnapshot:
    # 兼容旧测试导入；仅返回不接交易所的占位 market snapshot。
    price = 0.0
    return MarketSnapshot(
        decision_ts=now_iso,
        bar_ts=now_iso,
        strategy_ts=now_iso,
        execution_attributed_bar=now_iso,
        symbol=symbol,
        preclose_offset_seconds=0,
        current_price=price,
        source_status='PLACEHOLDER',
        fast_5m={'close': price, 'low': price, 'high': price},
        signal_15m={'close': price, 'low': price, 'high': price},
        signal_15m_ts=now_iso,
        trend_1h={
            'close': price,
            'ema_fast': price,
            'ema_slow': price,
            'adx': 0.0,
            'atr_rank': 0.0,
            'structure_tag': 'PLACEHOLDER',
        },
        trend_1h_ts=now_iso,
        signal_15m_history=[
            {'close': price, 'low': price, 'high': price},
            {'close': price, 'low': price, 'high': price},
            {'close': price, 'low': price, 'high': price},
            {'close': price, 'low': price, 'high': price},
        ],
        rev_candidate=None,
    )


def _build_runtime_components(config: BinanceEnvConfig):
    state_path = Path(config.state_path)
    initial_state = build_initial_state(datetime.now(timezone.utc).isoformat())
    state_store = JsonStateStore(state_path, initial_state)

    readonly_client = BinanceReadOnlyClient(config, recv_window_ms=config.recv_window_ms)
    market_provider = BinanceReadOnlyMarketDataProvider(readonly_client)
    strategy_module = build_strategy_adapter_from_config(config)
    executor_module = BinanceRealExecutor(config=config, readonly_client=readonly_client)
    engine = LiveEngine(
        state_store=state_store,
        strategy_module=strategy_module,
        executor_module=executor_module,
        pre_run_reconcile_module=BinancePreRunReconcileModule(state_store, readonly_client),
    )
    runtime_dir = state_path.parent
    runtime_worker = RuntimeWorker(
        config=config,
        state_store=state_store,
        engine=engine,
        market_provider=market_provider,
        status_store=RuntimeStatusStore(runtime_dir / 'runtime_status.json'),
        event_log=EventLogWriter(runtime_dir / 'event_log.jsonl'),
        scheduler=FixedIntervalScheduler(),
    )
    runtime_worker.last_runtime_config_validation = {
        'strategy_adapter': {
            'selected': normalize_strategy_adapter_name(config.strategy_adapter),
            'requested': config.strategy_adapter,
        },
    }
    return runtime_worker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Run autonomous runtime worker for ETHUSDT')
    parser.add_argument('--env', required=True, help='Path to binance_api.env')
    parser.add_argument('--once', action='store_true', help='Run one cycle only')
    parser.add_argument('--daemon', action='store_true', help='Run in daemon mode (kept for systemd/template compatibility)')
    parser.add_argument('--max-cycles', type=int, default=None, help='Optional max cycles for daemon mode')
    parser.add_argument('--decision-time', default=None, help='Override decision time (ISO8601) for one-shot run')
    parser.add_argument('--use-stub-market', action='store_true', help='Use stub market provider instead of readonly client')
    parser.add_argument('--readonly-market', action='store_true', help='Use Binance readonly market provider (default; kept for compatibility)')
    args = parser.parse_args(argv)

    config = load_binance_env(args.env)
    worker = _build_runtime_components(config)
    if args.use_stub_market:
        worker.market_provider = StubMarketDataProvider()

    if args.once:
        decision_time = None if args.decision_time is None else datetime.fromisoformat(args.decision_time)
        worker.run_once(decision_time)
        return 0

    worker.run_daemon(max_cycles=args.max_cycles)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
