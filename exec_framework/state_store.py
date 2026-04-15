from __future__ import annotations

import json
from dataclasses import asdict, replace
from typing import Any
from pathlib import Path

from .models import ExecutionResult, LiveStateSnapshot
from .strategy_protection_intent import build_strategy_protection_intent


FLAT_READY_PENDING_PHASE = None


def build_flat_reset_state_updates(
    *,
    state: LiveStateSnapshot,
    state_ts: str,
    account_equity: float | None = None,
    available_margin: float | None = None,
) -> dict[str, Any]:
    updates: dict[str, Any] = {
        'state_ts': state_ts,
        'consistency_status': 'OK',
        'freeze_reason': None,
        'account_equity': state.account_equity if account_equity is None else account_equity,
        'available_margin': state.available_margin if available_margin is None else available_margin,
        'exchange_position_side': None,
        'exchange_position_qty': 0.0,
        'exchange_entry_price': None,
        'active_strategy': 'none',
        'active_side': None,
        'strategy_entry_time': None,
        'strategy_entry_price': None,
        'stop_price': None,
        'tp_price': None,
        'base_quantity': None,
        'risk_fraction': None,
        'risk_amount': None,
        'risk_per_unit': None,
        'equity_at_entry': None,
        'can_open_new_position': True,
        'can_modify_position': True,
        'runtime_mode': 'ACTIVE',
        'freeze_status': 'NONE',
        'last_freeze_reason': state.last_freeze_reason,
        'pending_execution_phase': FLAT_READY_PENDING_PHASE,
        'position_confirmation_level': 'NONE',
        'trade_confirmation_level': 'NONE',
        'needs_trade_reconciliation': False,
        'fills_reconciled': False,
        'last_confirmed_order_ids': [],
        'protective_order_status': 'NONE',
        'exchange_protective_orders': [],
        'protective_order_last_sync_ts': state_ts,
        'protective_order_last_sync_action': 'flat_reset',
        'protective_order_freeze_reason': None,
        'protective_phase_status': 'NONE',
        'pending_execution_block_reason': None,
        'last_recover_at': state_ts,
        'last_recover_result': 'RECOVERED',
        'recover_attempt_count': int(state.recover_attempt_count or 0) + 1,
        'last_publishable_result': {},
    }
    updates['strategy_protection_intent'] = build_strategy_protection_intent(
        runtime_mode=updates['runtime_mode'],
        position_side=updates['exchange_position_side'],
        position_qty=updates['exchange_position_qty'],
        active_strategy=updates['active_strategy'],
        stop_price=updates['stop_price'],
        tp_price=updates['tp_price'],
        pending_execution_phase=updates['pending_execution_phase'],
        pending_execution_block_reason=updates['pending_execution_block_reason'],
        protective_order_status=updates['protective_order_status'],
        protective_phase_status=updates['protective_phase_status'],
        protective_orders=updates['exchange_protective_orders'],
        freeze_reason=updates['freeze_reason'],
        last_eval_ts=state_ts,
    )
    updates['async_operations'] = {'active': [], 'history': []}
    return updates


def apply_flat_reset_to_state(
    state: LiveStateSnapshot,
    *,
    state_ts: str,
    account_equity: float | None = None,
    available_margin: float | None = None,
) -> LiveStateSnapshot:
    return replace(
        state,
        **build_flat_reset_state_updates(
            state=state,
            state_ts=state_ts,
            account_equity=account_equity,
            available_margin=available_margin,
        ),
    )


def apply_flat_reset_to_result(
    result: ExecutionResult,
    *,
    state: LiveStateSnapshot,
    state_ts: str,
    account_equity: float | None = None,
    available_margin: float | None = None,
) -> ExecutionResult:
    flat_state_updates = build_flat_reset_state_updates(
        state=state,
        state_ts=state_ts,
        account_equity=account_equity,
        available_margin=available_margin,
    )
    trade_summary = dict(result.trade_summary or {})
    trade_summary['strategy_protection_intent'] = flat_state_updates['strategy_protection_intent']
    trade_summary['runtime_mode'] = flat_state_updates['runtime_mode']
    trade_summary['freeze_status'] = flat_state_updates['freeze_status']
    trade_summary['freeze_reason'] = flat_state_updates['freeze_reason']
    trade_summary['pending_execution_phase'] = flat_state_updates['pending_execution_phase']
    trade_summary['pending_execution_block_reason'] = flat_state_updates['pending_execution_block_reason']
    trade_summary['protective_order_status'] = flat_state_updates['protective_order_status']
    trade_summary['protective_phase_status'] = flat_state_updates['protective_phase_status']
    trade_summary['exchange_protective_orders'] = flat_state_updates['exchange_protective_orders']
    return replace(
        result,
        status='RECOVERED',
        execution_phase='flat_reset',
        confirmation_status='NOT_REQUIRED',
        confirmed_order_status=None,
        should_freeze=False,
        freeze_reason=None,
        state_updates=flat_state_updates,
        trade_summary=trade_summary,
    )


def apply_pre_run_reconcile(
    state: LiveStateSnapshot,
    last_result: ExecutionResult | None,
    *,
    state_ts: str,
    consistency_status: str,
    account_equity: float,
    available_margin: float,
    exchange_position_side: str | None,
    exchange_position_qty: float,
    exchange_entry_price: float | None,
    freeze_reason: str | None = None,
    can_open_new_position: bool | None = None,
    can_modify_position: bool | None = None,
) -> LiveStateSnapshot:
    resolved_freeze_reason = freeze_reason
    if consistency_status != 'OK' and resolved_freeze_reason is None and last_result is not None:
        resolved_freeze_reason = last_result.freeze_reason
    if consistency_status == 'OK' and state.runtime_mode != 'FROZEN':
        resolved_freeze_reason = None
    if consistency_status == 'OK' and exchange_position_side is None and float(exchange_position_qty or 0.0) <= 0.0:
        return apply_flat_reset_to_state(
            state,
            state_ts=state_ts,
            account_equity=account_equity,
            available_margin=available_margin,
        )

    resolved_can_open = can_open_new_position if can_open_new_position is not None else (consistency_status == 'OK')
    resolved_can_modify = can_modify_position if can_modify_position is not None else (consistency_status == 'OK')
    runtime_mode = state.runtime_mode
    freeze_status = state.freeze_status
    last_freeze_reason = state.last_freeze_reason
    if resolved_freeze_reason:
        runtime_mode = 'FROZEN'
        freeze_status = 'ACTIVE'
        last_freeze_reason = resolved_freeze_reason
    elif consistency_status == 'OK' and state.runtime_mode != 'FROZEN':
        runtime_mode = 'ACTIVE'
        freeze_status = 'NONE'
    return replace(
        state,
        state_ts=state_ts,
        consistency_status=consistency_status,
        freeze_reason=resolved_freeze_reason,
        account_equity=account_equity,
        available_margin=available_margin,
        exchange_position_side=exchange_position_side,
        exchange_position_qty=exchange_position_qty,
        exchange_entry_price=exchange_entry_price,
        can_open_new_position=resolved_can_open,
        can_modify_position=resolved_can_modify,
        runtime_mode=runtime_mode,
        freeze_status=freeze_status,
        last_freeze_reason=last_freeze_reason,
    )


class JsonStateStore:
    def __init__(self, path: str | Path, initial_state: LiveStateSnapshot):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write_json({'state': asdict(initial_state), 'last_result': None})

    def _read_json(self) -> dict:
        return json.loads(self.path.read_text())

    def _write_json(self, payload: dict) -> None:
        tmp = self.path.with_suffix(self.path.suffix + '.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        tmp.replace(self.path)

    def load_payload(self) -> dict:
        return self._read_json()

    def load_state(self) -> LiveStateSnapshot:
        payload = self.load_payload()
        return LiveStateSnapshot(**payload['state'])

    def save_state(self, state: LiveStateSnapshot) -> None:
        payload = self.load_payload()
        payload['state'] = asdict(state)
        self._write_json(payload)

    def load_last_result(self) -> ExecutionResult | None:
        payload = self.load_payload()
        last_result = payload.get('last_result')
        if last_result is None:
            return None
        return ExecutionResult(**last_result)

    def save_result(self, state: LiveStateSnapshot, result: ExecutionResult) -> None:
        if result.state_updates:
            for key, value in result.state_updates.items():
                setattr(state, key, value)
        payload = self.load_payload()
        payload['state'] = asdict(state)
        payload['last_result'] = asdict(result)
        self._write_json(payload)
