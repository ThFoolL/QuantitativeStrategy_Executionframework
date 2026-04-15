from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.runtime_status_cli import build_runtime_status_summary


class RuntimeStatusRuntimeLikeViewsCase(unittest.TestCase):
    def _write_runtime_files(
        self,
        *,
        runtime_status: dict,
        state: dict,
        event_rows: list[dict],
    ) -> Path:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        runtime_dir = Path(tmpdir.name) / 'runtime'
        runtime_dir.mkdir(parents=True, exist_ok=True)
        runtime_status_path = runtime_dir / 'runtime_status.json'
        state_path = runtime_dir / 'state.json'
        event_log_path = runtime_dir / 'event_log.jsonl'
        runtime_status = dict(runtime_status)
        runtime_status['event_log_path'] = str(event_log_path)
        runtime_status_path.write_text(json.dumps(runtime_status, ensure_ascii=False), encoding='utf-8')
        state_path.write_text(json.dumps(state, ensure_ascii=False), encoding='utf-8')
        event_log_path.write_text(
            '\n'.join(json.dumps(row, ensure_ascii=False) for row in event_rows) + ('\n' if event_rows else ''),
            encoding='utf-8',
        )
        return runtime_status_path

    def test_protection_pending_confirm_summary_stays_observe_consistent(self) -> None:
        runtime_status_path = self._write_runtime_files(
            runtime_status={
                'phase': 'completed',
                'symbol': 'ETHUSDT',
                'dry_run': False,
                'submit_enabled': True,
                'latest_market_summary': {'bar_ts': '2026-04-12T16:00:00+00:00'},
                'latest_result_summary': {
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': 'management_stop_update_pending_protective',
                    'result_status': 'FROZEN',
                    'confirmation_status': 'PENDING',
                    'execution_phase': 'management_stop_update_pending_protective',
                },
                'recover_check': {
                    'source': 'readonly_recheck',
                    'result': 'OBSERVE',
                    'recover_policy': 'observe_only',
                    'recover_stage': 'protection_pending_confirm',
                    'stop_reason': 'management_stop_update_pending_protective',
                    'stop_condition': 'position_confirmed_but_protection_pending',
                    'confirm_phase': 'readonly_recheck',
                    'guard_decision': 'keep_frozen_protection_pending_confirm',
                },
                'recover_timeline': [
                    {
                        'source': 'readonly_recheck',
                        'result': 'OBSERVE',
                        'recover_policy': 'observe_only',
                        'recover_stage': 'protection_pending_confirm',
                        'stop_reason': 'management_stop_update_pending_protective',
                        'stop_condition': 'position_confirmed_but_protection_pending',
                        'confirm_phase': 'readonly_recheck',
                        'guard_decision': 'keep_frozen_protection_pending_confirm',
                    }
                ],
            },
            state={
                'state': {
                    'state_ts': '2026-04-12T16:00:01+00:00',
                    'consistency_status': 'OK',
                    'freeze_reason': 'management_stop_update_pending_protective',
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'pending_execution_phase': 'management_stop_update_pending_protective',
                    'exchange_position_side': 'long',
                    'exchange_position_qty': 0.5,
                    'exchange_entry_price': 2100.0,
                    'active_strategy': 'trend',
                    'active_side': 'long',
                    'stop_price': 2050.0,
                    'protective_order_status': 'PENDING_CONFIRM',
                    'recover_check': {
                        'source': 'readonly_recheck',
                        'result': 'OBSERVE',
                        'recover_policy': 'observe_only',
                        'recover_stage': 'protection_pending_confirm',
                        'stop_reason': 'management_stop_update_pending_protective',
                        'stop_condition': 'position_confirmed_but_protection_pending',
                        'confirm_phase': 'readonly_recheck',
                        'guard_decision': 'keep_frozen_protection_pending_confirm',
                    },
                    'recover_timeline': [
                        {
                            'source': 'readonly_recheck',
                            'result': 'OBSERVE',
                            'recover_policy': 'observe_only',
                            'recover_stage': 'protection_pending_confirm',
                            'stop_reason': 'management_stop_update_pending_protective',
                            'stop_condition': 'position_confirmed_but_protection_pending',
                            'confirm_phase': 'readonly_recheck',
                            'guard_decision': 'keep_frozen_protection_pending_confirm',
                        }
                    ],
                },
                'last_result': {
                    'result_ts': '2026-04-12T16:00:01+00:00',
                    'bar_ts': '2026-04-12T16:00:00+00:00',
                    'status': 'FROZEN',
                    'action_type': 'open',
                    'executed_side': 'long',
                    'executed_qty': 0.5,
                    'avg_fill_price': 2100.0,
                    'exchange_order_ids': ['oid-1'],
                    'post_position_side': 'long',
                    'post_position_qty': 0.5,
                    'post_entry_price': 2100.0,
                    'reconcile_status': 'PENDING_CONFIRMATION',
                    'should_freeze': True,
                    'freeze_reason': 'management_stop_update_pending_protective',
                    'execution_phase': 'management_stop_update_pending_protective',
                    'confirmation_status': 'PENDING',
                    'confirmed_order_status': 'PARTIALLY_FILLED',
                    'trade_summary': {
                        'confirmation_category': 'position_confirmed',
                        'confirm_context': {
                            'confirm_attempted': True,
                            'confirm_phase': 'readonly_recheck',
                            'stop_reason': 'management_stop_update_pending_protective',
                            'stop_condition': 'position_confirmed_but_protection_pending',
                        },
                        'readonly_recheck': {
                            'status': 'readonly_recheck_pending',
                            'action': 'observe',
                            'confirm_context': {
                                'confirm_phase': 'readonly_recheck',
                                'stop_reason': 'management_stop_update_pending_protective',
                                'stop_condition': 'position_confirmed_but_protection_pending',
                            },
                        },
                    },
                },
            },
            event_rows=[
                {
                    'event_type': 'confirm_summary',
                    'stop_condition': 'position_confirmed_but_protection_pending',
                    'recover_stage': 'protection_pending_confirm',
                    'risk_action': 'OBSERVE',
                },
                {
                    'event_type': 'recover_result',
                    'result': 'OBSERVE',
                    'recover_policy': 'observe_only',
                    'recover_stage': 'protection_pending_confirm',
                    'stop_condition': 'position_confirmed_but_protection_pending',
                    'risk_action': 'OBSERVE',
                },
            ],
        )
        summary = build_runtime_status_summary(runtime_status_path=runtime_status_path)
        self.assertEqual(summary['operator_compact_view']['recover_state'], 'recover_observe')
        self.assertEqual(summary['operator_compact_view']['recover_policy'], 'observe_only')
        self.assertEqual(summary['operator_compact_view']['recover_stage'], 'protection_pending_confirm')
        self.assertEqual(summary['operator_compact_view']['stop_condition'], 'position_confirmed_but_protection_pending')
        self.assertEqual(summary['recover_summary']['stop_condition'], 'position_confirmed_but_protection_pending')
        self.assertEqual(summary['recent_summary']['last_recover_event']['risk_action'], 'OBSERVE')
        self.assertIn('继续只读观察', summary['operator_compact_view']['next_focus'])
        self.assertEqual(summary['operator_compact_view']['stop_condition'], 'position_confirmed_but_protection_pending')

    def test_force_close_recover_timeline_keeps_operator_summary_on_emergency_fact(self) -> None:
        runtime_status_path = self._write_runtime_files(
            runtime_status={
                'phase': 'completed',
                'symbol': 'ETHUSDT',
                'dry_run': False,
                'submit_enabled': True,
                'latest_market_summary': {'bar_ts': '2026-04-16T00:00:00+00:00'},
                'latest_result_summary': {
                    'runtime_mode': 'ACTIVE',
                    'freeze_status': 'NONE',
                    'freeze_reason': None,
                    'result_status': 'FILLED',
                    'confirmation_status': 'CONFIRMED',
                    'execution_phase': 'confirmed',
                },
                'recover_check': {
                    'source': 'readonly_recheck',
                    'result': 'ALLOWED',
                    'recover_policy': 'ready_only',
                    'recover_stage': 'terminal_confirmation',
                    'stop_reason': 'terminal_confirmation_reached',
                    'stop_condition': 'terminal_confirmation_reached',
                    'risk_action': 'RECOVER_PROTECTION',
                },
                'recover_timeline': [
                    {
                        'source': 'protective_order_recover',
                        'result': 'BLOCKED',
                        'recover_policy': 'manual_review',
                        'recover_stage': 'force_close_without_protection',
                        'stop_reason': 'protective_order_missing',
                        'stop_condition': 'position_open_without_protection',
                        'risk_action': 'FORCE_CLOSE',
                    },
                    {
                        'source': 'readonly_recheck',
                        'result': 'ALLOWED',
                        'recover_policy': 'ready_only',
                        'recover_stage': 'terminal_confirmation',
                        'stop_reason': 'terminal_confirmation_reached',
                        'stop_condition': 'terminal_confirmation_reached',
                        'risk_action': 'RECOVER_PROTECTION',
                    },
                ],
            },
            state={
                'state': {
                    'state_ts': '2026-04-16T00:00:01+00:00',
                    'consistency_status': 'OK',
                    'freeze_reason': None,
                    'runtime_mode': 'ACTIVE',
                    'freeze_status': 'NONE',
                    'pending_execution_phase': None,
                    'exchange_position_side': None,
                    'exchange_position_qty': 0.0,
                    'exchange_entry_price': None,
                    'active_strategy': 'rev',
                    'active_side': None,
                    'stop_price': None,
                    'protective_order_status': 'NONE',
                    'recover_check': {
                        'source': 'readonly_recheck',
                        'result': 'ALLOWED',
                        'recover_policy': 'ready_only',
                        'recover_stage': 'terminal_confirmation',
                        'stop_reason': 'terminal_confirmation_reached',
                        'stop_condition': 'terminal_confirmation_reached',
                        'risk_action': 'RECOVER_PROTECTION',
                    },
                    'recover_timeline': [
                        {
                            'source': 'protective_order_recover',
                            'result': 'BLOCKED',
                            'recover_policy': 'manual_review',
                            'recover_stage': 'force_close_without_protection',
                            'stop_reason': 'protective_order_missing',
                            'stop_condition': 'position_open_without_protection',
                            'risk_action': 'FORCE_CLOSE',
                        },
                        {
                            'source': 'readonly_recheck',
                            'result': 'ALLOWED',
                            'recover_policy': 'ready_only',
                            'recover_stage': 'terminal_confirmation',
                            'stop_reason': 'terminal_confirmation_reached',
                            'stop_condition': 'terminal_confirmation_reached',
                            'risk_action': 'RECOVER_PROTECTION',
                        },
                    ],
                },
                'last_result': {
                    'result_ts': '2026-04-16T00:00:01+00:00',
                    'bar_ts': '2026-04-16T00:00:00+00:00',
                    'status': 'FILLED',
                    'action_type': 'close',
                    'executed_side': 'short',
                    'executed_qty': 0.01,
                    'avg_fill_price': 2330.0,
                    'exchange_order_ids': ['oid-force-close'],
                    'post_position_side': None,
                    'post_position_qty': 0.0,
                    'post_entry_price': None,
                    'reconcile_status': 'OK',
                    'should_freeze': False,
                    'freeze_reason': None,
                    'execution_phase': 'confirmed',
                    'confirmation_status': 'CONFIRMED',
                    'confirmed_order_status': 'FILLED',
                    'trade_summary': {
                        'confirmation_category': 'confirmed',
                        'confirm_context': {
                            'confirm_attempted': True,
                            'confirm_phase': 'terminal',
                            'stop_reason': 'terminal_confirmation_reached',
                            'stop_condition': 'terminal_confirmation_reached',
                        },
                    },
                },
            },
            event_rows=[
                {
                    'event_type': 'recover_result',
                    'result': 'BLOCKED',
                    'recover_policy': 'manual_review',
                    'recover_stage': 'force_close_without_protection',
                    'stop_reason': 'protective_order_missing',
                    'stop_condition': 'position_open_without_protection',
                    'risk_action': 'FORCE_CLOSE',
                }
            ],
        )
        summary = build_runtime_status_summary(runtime_status_path=runtime_status_path)
        self.assertEqual(summary['operator_compact_view']['recover_policy'], 'manual_review')
        self.assertEqual(summary['operator_compact_view']['stop_condition'], 'position_open_without_protection')
        self.assertEqual(summary['operator_compact_view']['stop_reason'], 'protective_order_missing')
        self.assertEqual(summary['recover_summary']['stop_condition'], 'position_open_without_protection')
        self.assertEqual(summary['recover_summary']['stop_reason'], 'protective_order_missing')

    def test_partial_protection_missing_summary_stays_manual_review_consistent(self) -> None:
        runtime_status_path = self._write_runtime_files(
            runtime_status={
                'phase': 'completed',
                'symbol': 'ETHUSDT',
                'dry_run': False,
                'submit_enabled': True,
                'latest_market_summary': {'bar_ts': '2026-04-12T16:10:00+00:00'},
                'latest_result_summary': {
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'freeze_reason': 'protective_order_missing',
                    'result_status': 'FROZEN',
                    'confirmation_status': 'PENDING',
                    'execution_phase': 'frozen',
                },
                'recover_check': {
                    'source': 'readonly_recheck',
                    'result': 'BLOCKED',
                    'recover_policy': 'manual_review',
                    'recover_stage': 'protection_partial_missing',
                    'stop_reason': 'protection_tp_missing',
                    'stop_condition': 'protection_tp_missing',
                    'confirm_phase': 'readonly_recheck',
                    'guard_decision': 'keep_frozen_protection_tp_missing',
                    'risk_action': 'MANUAL_REVIEW',
                },
            },
            state={
                'state': {
                    'state_ts': '2026-04-12T16:10:01+00:00',
                    'consistency_status': 'OK',
                    'freeze_reason': 'protective_order_missing',
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'pending_execution_phase': 'frozen',
                    'exchange_position_side': 'long',
                    'exchange_position_qty': 0.5,
                    'exchange_entry_price': 2105.0,
                    'active_strategy': 'trend',
                    'active_side': 'long',
                    'stop_price': 2050.0,
                    'protective_order_status': 'PARTIAL_MISSING',
                    'exchange_protective_orders': [
                        {'kind': 'hard_stop', 'type': 'STOP_MARKET', 'side': 'sell', 'position_side': 'both', 'close_position': True, 'reduce_only': True, 'qty': 0.5, 'stop_price': 2050.0, 'status': 'NEW'}
                    ],
                    'recover_check': {
                        'source': 'readonly_recheck',
                        'result': 'BLOCKED',
                        'recover_policy': 'manual_review',
                        'recover_stage': 'protection_partial_missing',
                        'stop_reason': 'protection_tp_missing',
                        'stop_condition': 'protection_tp_missing',
                        'confirm_phase': 'readonly_recheck',
                        'guard_decision': 'keep_frozen_protection_tp_missing',
                        'risk_action': 'MANUAL_REVIEW',
                    },
                },
                'last_result': {
                    'result_ts': '2026-04-12T16:10:01+00:00',
                    'bar_ts': '2026-04-12T16:10:00+00:00',
                    'status': 'FROZEN',
                    'action_type': 'open',
                    'executed_side': 'long',
                    'executed_qty': 0.5,
                    'avg_fill_price': 2105.0,
                    'exchange_order_ids': ['oid-2'],
                    'post_position_side': 'long',
                    'post_position_qty': 0.5,
                    'post_entry_price': 2105.0,
                    'reconcile_status': 'PENDING_CONFIRMATION',
                    'should_freeze': True,
                    'freeze_reason': 'protective_order_missing',
                    'execution_phase': 'frozen',
                    'confirmation_status': 'PENDING',
                    'confirmed_order_status': 'PARTIALLY_FILLED',
                    'trade_summary': {
                        'confirmation_category': 'position_confirmed',
                        'confirm_context': {
                            'confirm_attempted': True,
                            'confirm_phase': 'readonly_recheck',
                            'stop_reason': 'protection_tp_missing',
                            'stop_condition': 'protection_tp_missing',
                        },
                        'readonly_recheck': {
                            'status': 'readonly_recheck_freeze',
                            'action': 'manual_review',
                            'confirm_context': {
                                'confirm_phase': 'readonly_recheck',
                                'stop_reason': 'protection_tp_missing',
                                'stop_condition': 'protection_tp_missing',
                            },
                        },
                        'notes': ['protection_tp_missing', 'partial_protective_missing'],
                    },
                },
            },
            event_rows=[
                {
                    'event_type': 'confirm_summary',
                    'stop_condition': 'protection_tp_missing',
                    'recover_stage': 'protection_partial_missing',
                    'risk_action': 'MANUAL_REVIEW',
                },
                {
                    'event_type': 'recover_result',
                    'result': 'BLOCKED',
                    'recover_policy': 'manual_review',
                    'recover_stage': 'protection_partial_missing',
                    'stop_condition': 'protection_tp_missing',
                    'risk_action': 'MANUAL_REVIEW',
                },
            ],
        )
        summary = build_runtime_status_summary(runtime_status_path=runtime_status_path)
        self.assertFalse(summary['operator_compact_view']['manual_review_required'])
        self.assertEqual(summary['operator_compact_view']['recover_state'], 'recover_blocked')
        self.assertEqual(summary['operator_compact_view']['recover_policy'], 'recover_protection')
        self.assertEqual(summary['operator_compact_view']['legacy_recover_policy'], 'manual_review')
        self.assertEqual(summary['operator_compact_view']['effective_recover_policy'], 'manual_review')
        self.assertEqual(summary['operator_compact_view']['recover_stage'], 'protection_partial_missing')
        self.assertEqual(summary['operator_compact_view']['stop_condition'], 'protection_tp_missing')
        self.assertEqual(summary['operator_compact_view']['stop_category'], 'recover_protection')
        self.assertIsNone(summary['operator_compact_view']['legacy_stop_category'])
        self.assertEqual(summary['operator_compact_view']['protection_summary_family'], 'protection_missing')
        self.assertEqual(summary['operator_compact_view']['protection_pending_action'], 'protective_rebuild')
        self.assertEqual(summary['recover_summary']['stop_category'], 'recover_protection')
        self.assertIsNone(summary['recover_summary']['legacy_stop_category'])
        self.assertEqual(summary['recover_summary']['recover_policy'], 'recover_protection')
        self.assertEqual(summary['recover_summary']['legacy_recover_policy'], 'manual_review')
        self.assertEqual(summary['recover_summary']['effective_recover_policy'], 'manual_review')
        self.assertEqual(summary['recover_summary']['protection_summary_family'], 'protection_missing')
        self.assertEqual(summary['recover_summary']['protection_pending_action'], 'protective_rebuild')
        self.assertEqual(summary['recent_summary']['last_confirm_event']['risk_action'], 'MANUAL_REVIEW')
        self.assertEqual(summary['recent_summary']['last_recover_event']['risk_action'], 'MANUAL_REVIEW')
        self.assertIn('take profit 缺失', summary['operator_compact_view']['next_focus'])


if __name__ == '__main__':
    unittest.main()
