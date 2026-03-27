from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path

from .models import ExecutionResult, LiveStateSnapshot


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
