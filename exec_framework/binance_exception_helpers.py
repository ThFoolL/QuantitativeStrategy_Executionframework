from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .binance_exception_policy import (
    ACTION_AUTO_REPAIR,
    ACTION_FREEZE_AND_ALERT,
    ACTION_READONLY_RECHECK,
    ACTION_RETRY,
    BinanceExceptionAction,
)


@dataclass(frozen=True)
class GuardedExceptionPlan:
    policy: dict[str, Any]
    helper_name: str
    enabled: bool
    dry_run_only: bool
    requires_manual_ack: bool
    safe_to_execute_now: bool
    blocked_reason: str | None
    steps: list[str]
    readonly_checks: list[str]
    alert_channel_id: str
    alert_should_send: bool
    notes: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            'policy': dict(self.policy),
            'helper_name': self.helper_name,
            'enabled': self.enabled,
            'dry_run_only': self.dry_run_only,
            'requires_manual_ack': self.requires_manual_ack,
            'safe_to_execute_now': self.safe_to_execute_now,
            'blocked_reason': self.blocked_reason,
            'steps': list(self.steps),
            'readonly_checks': list(self.readonly_checks),
            'alert_channel_id': self.alert_channel_id,
            'alert_should_send': self.alert_should_send,
            'notes': list(self.notes),
        }


_DISCORD_MONITOR_CHANNEL = '1486034825830727710'


def _normalize_exception_policy_view(policy: BinanceExceptionAction) -> dict[str, Any]:
    policy_dict = policy.as_dict()
    return {
        **policy_dict,
        'policy': policy.action,
        'action': policy.action,
        'reason': '; '.join(item for item in policy.notes if item) or None,
        'next_action': (
            ' -> '.join(policy.auto_repair_steps)
            if policy.auto_repair_steps
            else (' / '.join(policy.readonly_checks) if policy.readonly_checks else policy.action)
        ),
        'should_alert': policy.alert != 'none',
    }


def build_guarded_exception_plan(
    policy: BinanceExceptionAction,
    *,
    runtime_mode: str | None = None,
    manual_ack_present: bool = False,
    automation_enabled: bool = False,
) -> GuardedExceptionPlan:
    helper_name = f"exception_helper_{policy.action}"
    readonly_checks = list(policy.readonly_checks or [])
    steps = list(policy.auto_repair_steps or [])
    notes = list(policy.notes or [])
    blocked_reason = None
    safe_to_execute_now = False

    if policy.action == ACTION_AUTO_REPAIR:
        if not automation_enabled:
            blocked_reason = 'auto_repair_helper_disabled_by_default'
        elif runtime_mode == 'FROZEN':
            blocked_reason = 'runtime_frozen_requires_manual_review'
        elif not manual_ack_present:
            blocked_reason = 'manual_ack_missing'
        else:
            safe_to_execute_now = True
    elif policy.action in {ACTION_READONLY_RECHECK, ACTION_RETRY}:
        safe_to_execute_now = False
        blocked_reason = 'helper_is_plan_only_until_runtime_hooked'
    elif policy.action == ACTION_FREEZE_AND_ALERT:
        blocked_reason = 'freeze_and_alert_requires_human_handling'

    policy_view = _normalize_exception_policy_view(policy)
    plan_notes = list(notes)
    if blocked_reason:
        plan_notes.append(f'guard_blocked:{blocked_reason}')
    return GuardedExceptionPlan(
        policy=policy_view,
        helper_name=helper_name,
        enabled=bool(automation_enabled),
        dry_run_only=not safe_to_execute_now,
        requires_manual_ack=True,
        safe_to_execute_now=safe_to_execute_now,
        blocked_reason=blocked_reason,
        steps=steps,
        readonly_checks=readonly_checks,
        alert_channel_id=_DISCORD_MONITOR_CHANNEL,
        alert_should_send=False,
        notes=plan_notes,
    )


def execute_guarded_exception_plan(plan: GuardedExceptionPlan) -> dict[str, Any]:
    next_actions = plan.steps or plan.readonly_checks
    policy = dict(plan.policy or {})
    return {
        'ok': False,
        'executed': False,
        'helper_name': plan.helper_name,
        'policy': policy,
        'action': policy.get('action') or policy.get('policy'),
        'reason': policy.get('reason'),
        'next_action': policy.get('next_action') or (' -> '.join(next_actions) if next_actions else None),
        'should_alert': bool(policy.get('should_alert')),
        'blocked_reason': plan.blocked_reason or 'not_runtime_hooked',
        'safe_to_execute_now': plan.safe_to_execute_now,
        'dry_run_only': plan.dry_run_only,
        'next_actions': next_actions,
        'alert_channel_id': plan.alert_channel_id,
        'alert_should_send': False,
    }
