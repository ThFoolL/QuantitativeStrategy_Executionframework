from __future__ import annotations

from dataclasses import dataclass

from .models import ExecutionResult, LiveStateSnapshot

FREEZE_REASON_RECONCILE_MISMATCH = 'reconcile_mismatch'
FREEZE_REASON_POSTTRADE_PENDING = 'posttrade_pending_confirmation'
FREEZE_REASON_POSTTRADE_QUERY_FAILED = 'posttrade_query_failed'
FREEZE_REASON_POSTTRADE_REJECTED = 'posttrade_rejected_or_canceled'
FREEZE_REASON_MANUAL = 'manual_freeze'
FREEZE_REASON_UNKNOWN = 'unknown_risk_state'

RECOVER_RESULT_ALLOWED = 'RECOVERED'
RECOVER_RESULT_BLOCKED = 'BLOCKED'


@dataclass(frozen=True)
class FreezeDecision:
    should_freeze: bool
    freeze_reason: str | None
    runtime_mode: str
    freeze_status: str
    state_updates: dict[str, object]


@dataclass(frozen=True)
class RecoverDecision:
    allowed: bool
    result: str
    reason: str
    state_updates: dict[str, object]


class RuntimeFreezeController:
    def freeze_from_result(self, state: LiveStateSnapshot, result: ExecutionResult) -> FreezeDecision:
        reason = result.freeze_reason or state.freeze_reason
        if result.should_freeze or state.consistency_status not in {'OK', 'DRY_RUN'}:
            resolved_reason = reason or self._reason_from_state(state)
            return FreezeDecision(
                should_freeze=True,
                freeze_reason=resolved_reason,
                runtime_mode='FROZEN',
                freeze_status='ACTIVE',
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': resolved_reason,
                    'last_freeze_reason': resolved_reason,
                    'last_freeze_at': result.result_ts,
                    'pending_execution_phase': result.execution_phase,
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
            )
        return FreezeDecision(
            should_freeze=False,
            freeze_reason=None,
            runtime_mode=state.runtime_mode,
            freeze_status=state.freeze_status,
            state_updates={},
        )

    def evaluate_recover(self, state: LiveStateSnapshot) -> RecoverDecision:
        blocked_reason = self._recover_block_reason(state)
        if blocked_reason is not None:
            return RecoverDecision(
                allowed=False,
                result=RECOVER_RESULT_BLOCKED,
                reason=blocked_reason,
                state_updates={
                    'last_recover_result': RECOVER_RESULT_BLOCKED,
                    'recover_attempt_count': int(state.recover_attempt_count) + 1,
                },
            )
        return RecoverDecision(
            allowed=True,
            result=RECOVER_RESULT_ALLOWED,
            reason='reconcile_ok_and_no_pending_execution',
            state_updates={
                'runtime_mode': 'ACTIVE',
                'freeze_status': 'NONE',
                'freeze_reason': None,
                'last_recover_result': RECOVER_RESULT_ALLOWED,
                'last_recover_at': state.state_ts,
                'recover_attempt_count': int(state.recover_attempt_count) + 1,
                'pending_execution_phase': None,
                'can_open_new_position': True,
                'can_modify_position': True,
            },
        )

    def _recover_block_reason(self, state: LiveStateSnapshot) -> str | None:
        if state.consistency_status != 'OK':
            return f'consistency_not_ok:{state.consistency_status}'
        if state.pending_execution_phase not in {None, 'confirmed', 'none'}:
            return f'pending_execution_phase:{state.pending_execution_phase}'
        if state.exchange_position_qty > 0 and state.exchange_position_side is None:
            return 'exchange_position_side_missing'
        return None

    def _reason_from_state(self, state: LiveStateSnapshot) -> str:
        if state.consistency_status == 'MISMATCH':
            return FREEZE_REASON_RECONCILE_MISMATCH
        return state.freeze_reason or FREEZE_REASON_UNKNOWN
