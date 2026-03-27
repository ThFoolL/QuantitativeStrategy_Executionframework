from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_exception_policy import (
    ACTION_AUTO_REPAIR,
    ACTION_FREEZE_AND_ALERT,
    ACTION_READONLY_RECHECK,
    ACTION_RETRY,
    ALERT_IMMEDIATE,
    ALERT_ON_EXHAUSTED,
    classify_binance_error_code,
    classify_binance_order_status,
    classify_submit_exception_detail,
)


class BinanceExceptionPolicyCase(unittest.TestCase):
    def test_invalid_timestamp_prefers_auto_repair(self) -> None:
        policy = classify_binance_error_code(-1021, 'Timestamp outside recvWindow')
        self.assertEqual(policy.action, ACTION_AUTO_REPAIR)
        self.assertFalse(policy.should_freeze_runtime)
        self.assertEqual(policy.alert, ALERT_ON_EXHAUSTED)
        self.assertIn('sync_server_time', policy.auto_repair_steps)

    def test_reduce_only_reject_prefers_auto_repair(self) -> None:
        policy = classify_binance_error_code(-2022, 'ReduceOnly Order is rejected.')
        self.assertEqual(policy.action, ACTION_AUTO_REPAIR)
        self.assertFalse(policy.should_freeze_runtime)
        self.assertIn('cancel_conflicting_reduce_only_orders', policy.auto_repair_steps)
        self.assertIn('openOrders', policy.readonly_checks)

    def test_timeout_prefers_readonly_recheck(self) -> None:
        policy = classify_binance_error_code(-1007, 'execution status unknown')
        self.assertEqual(policy.action, ACTION_READONLY_RECHECK)
        self.assertFalse(policy.should_freeze_runtime)
        self.assertIn('order', policy.readonly_checks)
        self.assertIn('positionRisk', policy.readonly_checks)

    def test_rate_limit_prefers_retry(self) -> None:
        policy = classify_binance_error_code(-1003, 'Too many requests')
        self.assertEqual(policy.action, ACTION_RETRY)
        self.assertTrue(policy.retryable)
        self.assertFalse(policy.should_freeze_runtime)

    def test_signature_error_requires_freeze_and_alert(self) -> None:
        policy = classify_binance_error_code(-1022, 'invalid signature')
        self.assertEqual(policy.action, ACTION_FREEZE_AND_ALERT)
        self.assertTrue(policy.should_freeze_runtime)
        self.assertEqual(policy.alert, ALERT_IMMEDIATE)

    def test_unknown_processing_code_defaults_to_readonly_recheck(self) -> None:
        policy = classify_binance_error_code(-2999, 'unknown processing issue')
        self.assertEqual(policy.action, ACTION_READONLY_RECHECK)
        self.assertFalse(policy.should_freeze_runtime)

    def test_request_parameter_error_defaults_to_freeze_and_alert(self) -> None:
        policy = classify_binance_error_code(-1102, 'mandatory param malformed')
        self.assertEqual(policy.action, ACTION_FREEZE_AND_ALERT)
        self.assertTrue(policy.should_freeze_runtime)

    def test_order_status_expired_in_match_prefers_readonly_recheck(self) -> None:
        policy = classify_binance_order_status('EXPIRED_IN_MATCH')
        self.assertEqual(policy.action, ACTION_READONLY_RECHECK)
        self.assertFalse(policy.should_freeze_runtime)
        self.assertIn('userTrades', policy.readonly_checks)

    def test_submit_exception_detail_extracts_binance_payload(self) -> None:
        policy = classify_submit_exception_detail({'payload': {'code': -2015, 'msg': 'Invalid API-key, IP, or permissions for action.'}})
        self.assertEqual(policy.action, ACTION_FREEZE_AND_ALERT)
        self.assertTrue(policy.should_freeze_runtime)


if __name__ == '__main__':
    unittest.main()
