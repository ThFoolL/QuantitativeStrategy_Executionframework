from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_exception_helpers import build_guarded_exception_plan, execute_guarded_exception_plan
from exec_framework.binance_exception_policy import classify_binance_error_code, classify_binance_order_status


class BinanceExceptionHelpersCase(unittest.TestCase):
    def test_readonly_recheck_plan_exposes_oncall_view(self) -> None:
        policy = classify_binance_error_code(-1007, 'execution status unknown')
        plan = build_guarded_exception_plan(policy, runtime_mode='ACTIVE', manual_ack_present=False, automation_enabled=False)
        payload = plan.as_dict()

        self.assertEqual(payload['policy']['policy'], 'readonly_recheck')
        self.assertEqual(payload['policy']['action'], 'readonly_recheck')
        self.assertTrue(payload['policy']['should_alert'])
        self.assertIn('order', payload['policy']['next_action'])
        self.assertFalse(payload['alert_should_send'])
        self.assertEqual(payload['alert_channel_id'], 'DISCORD_CHANNEL_ID_PLACEHOLDER')

    def test_auto_repair_plan_for_reduce_only_conflict_stays_guarded(self) -> None:
        policy = classify_binance_error_code(-2022, 'ReduceOnly Order is rejected.')
        plan = build_guarded_exception_plan(policy, runtime_mode='ACTIVE', manual_ack_present=True, automation_enabled=False)
        payload = execute_guarded_exception_plan(plan)

        self.assertEqual(payload['action'], 'auto_repair')
        self.assertIn('query_open_orders', payload['next_action'])
        self.assertFalse(payload['executed'])
        self.assertFalse(payload['alert_should_send'])
        self.assertEqual(payload['blocked_reason'], 'auto_repair_helper_disabled_by_default')

    def test_expired_in_match_policy_keeps_readonly_checks(self) -> None:
        policy = classify_binance_order_status('EXPIRED_IN_MATCH')
        plan = build_guarded_exception_plan(policy, runtime_mode='ACTIVE', manual_ack_present=False, automation_enabled=False)
        self.assertEqual(plan.policy['source_key'], 'EXPIRED_IN_MATCH')
        self.assertEqual(plan.policy['policy'], 'readonly_recheck')
        self.assertIn('userTrades', plan.policy['next_action'])


if __name__ == '__main__':
    unittest.main()
