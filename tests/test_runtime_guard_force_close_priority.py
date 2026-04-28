from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.models import LiveStateSnapshot
from exec_framework.runtime_guard import _build_runtime_recover_check


class RuntimeGuardForceClosePriorityCase(unittest.TestCase):
    def test_protective_missing_open_position_ignores_stale_async_observe_projection(self) -> None:
        state = LiveStateSnapshot(
            state_ts='2026-04-28T00:40:00+00:00',
            consistency_status='OK',
            freeze_reason='protective_order_missing',
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side='short',
            exchange_position_qty=0.021,
            exchange_entry_price=2275.0,
            active_strategy='manual_protective_missing_probe',
            active_side='short',
            strategy_entry_time='2026-04-28T00:39:00+00:00',
            strategy_entry_price=2275.0,
            stop_price=None,
            risk_fraction=0.1,
            runtime_mode='FROZEN',
            freeze_status='ACTIVE',
            pending_execution_phase='frozen',
            recover_check={
                'source': 'readonly_recheck',
                'result': 'BLOCKED',
                'allowed': False,
                'reason': 'protective_order_missing',
                'pending_execution_phase': 'frozen',
                'consistency_status': 'OK',
                'runtime_mode': 'FROZEN',
                'recover_ready': False,
                'requires_manual_resume': True,
                'guard_decision': 'keep_frozen_protection_missing',
                'recover_policy': 'keep_frozen',
                'recover_policy_display': 'force_close',
                'legacy_recover_policy': 'keep_frozen',
                'recover_stage': 'force_close_without_protection',
                'risk_action': 'FORCE_CLOSE',
                'stop_reason': 'protective_order_missing',
                'stop_category': 'frozen',
                'freeze_reason': 'protective_order_missing',
                'stop_condition': 'position_open_without_protection',
            },
            async_operations={
                'active': [
                    {
                        'family': 'execution_confirm',
                        'status': 'running',
                        'trigger_phase': 'confirmed',
                        'confirm_context': {
                            'confirm_phase': 'execution_confirm',
                            'stop_reason': 'recover_ready',
                            'stop_condition': 'await_more_exchange_facts',
                        },
                    }
                ],
                'history': [],
            },
        )

        recover_check = _build_runtime_recover_check(state)

        self.assertEqual(recover_check['recover_policy'], 'keep_frozen')
        self.assertEqual(recover_check['recover_stage'], 'force_close_without_protection')
        self.assertEqual(recover_check['risk_action'], 'FORCE_CLOSE')
        self.assertEqual(recover_check['stop_condition'], 'position_open_without_protection')


if __name__ == '__main__':
    unittest.main()
