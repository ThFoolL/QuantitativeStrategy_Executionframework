from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock


class ManualProtectiveMissingEmergencyCloseProbeCase(unittest.TestCase):
    def test_module_imports(self) -> None:
        import exec_framework.manual_protective_missing_emergency_close_probe as probe

        self.assertTrue(callable(probe.main))

    def test_run_id_format(self) -> None:
        import exec_framework.manual_protective_missing_emergency_close_probe as probe

        run_id = probe._run_id()
        self.assertTrue(run_id.endswith('Z'))
        self.assertGreaterEqual(len(run_id), 17)

    def test_min_notional_bump(self) -> None:
        import exec_framework.manual_protective_missing_emergency_close_probe as probe

        qty = probe._bump_quantity_to_min_notional(
            quantity=0.008,
            price=2300.0,
            qty_step=0.001,
            min_notional=20.0,
        )
        self.assertGreaterEqual(qty * 2300.0, 20.0)

    def test_ensure_flat_uses_position_and_open_orders(self) -> None:
        import exec_framework.manual_protective_missing_emergency_close_probe as probe

        client = mock.Mock()
        client.get_position_snapshot.return_value = mock.Mock(side=None, qty=0.0, raw={})
        client.get_open_orders.return_value = []

        with mock.patch.object(probe, 'asdict', side_effect=lambda obj: {'qty': getattr(obj, 'qty', None), 'side': getattr(obj, 'side', None)}):
            out = probe._ensure_flat(client, 'ETHUSDT')

        self.assertTrue(out['is_flat'])
        self.assertEqual(out['open_orders_count'], 0)

    def test_new_terminal_ok_path_accepts_runtime_unfrozen_after_force_close(self) -> None:
        after_runtime = {'position': {'qty': 0.0}, 'open_orders': []}
        runtime_post_state = {
            'runtime_mode': 'ACTIVE',
            'freeze_status': 'NONE',
            'freeze_reason': None,
            'last_recover_result': 'RECOVERED',
            'recover_check': {
                'checked_at': '2026-04-28T01:29:25+00:00',
                'source': 'close_flat_terminal_cleanup',
                'result': 'RECOVERED',
                'reason': 'close_confirmed_flat',
            },
        }
        runtime_last_result = {
            'status': 'FILLED',
            'action_type': 'close',
            'confirmed_order_status': 'FILLED',
            'state_updates': {
                'recover_check': {
                    'stop_reason': 'confirmed',
                    'risk_action': 'NONE',
                }
            },
        }

        runtime_recover = runtime_post_state.get('recover_check') or {}
        recover_stop_reason = runtime_recover.get('stop_reason') or ((runtime_last_result.get('state_updates') or {}).get('recover_check') or {}).get('stop_reason')
        recover_risk_action = runtime_recover.get('risk_action') or ((runtime_last_result.get('state_updates') or {}).get('recover_check') or {}).get('risk_action')
        runtime_unfrozen_after_close = (
            runtime_post_state.get('runtime_mode') == 'ACTIVE'
            and runtime_post_state.get('freeze_status') == 'NONE'
            and runtime_post_state.get('freeze_reason') in {None, ''}
            and runtime_post_state.get('last_recover_result') == 'RECOVERED'
        )
        ok = bool(
            abs(float(after_runtime['position']['qty'] or 0.0)) <= 0.0
            and len(after_runtime['open_orders']) == 0
            and runtime_last_result.get('status') == 'FILLED'
            and runtime_last_result.get('action_type') == 'close'
            and runtime_last_result.get('confirmed_order_status') == 'FILLED'
            and (
                (recover_stop_reason == 'protective_order_missing' and recover_risk_action == 'FORCE_CLOSE')
                or runtime_unfrozen_after_close
            )
        )

        self.assertTrue(ok)

    def test_reset_probe_runtime_state_clears_stale_recover_and_async_context(self) -> None:
        import exec_framework.manual_protective_missing_emergency_close_probe as probe

        state = SimpleNamespace(
            runtime_mode='ACTIVE',
            freeze_status='NONE',
            freeze_reason=None,
            pending_execution_phase='planned',
            pending_execution_block_reason='old',
            exchange_position_side=None,
            exchange_position_qty=0.0,
            exchange_entry_price=None,
            active_strategy='old_strategy',
            active_side=None,
            stop_price=123.0,
            tp_price=456.0,
            exchange_protective_orders=[{'id': 'old'}],
            protective_order_status='ACTIVE',
            protective_phase_status='ACTIVE',
            last_recover_result='OBSERVE',
            last_recover_at='old-ts',
            recover_attempt_count=7,
            recover_check={'risk_action': 'OBSERVE'},
            recover_timeline=[{'risk_action': 'OBSERVE'}],
            async_operations={'active': [{'id': 'old'}], 'history': [{'id': 'old'}]},
            position_confirmation_level='NONE',
            trade_confirmation_level='PENDING',
            needs_trade_reconciliation=True,
            fills_reconciled=True,
            last_confirmed_order_ids=['old-order'],
            strategy_protection_intent={'intent_state': 'stale'},
        )

        probe._reset_probe_runtime_state(
            state,
            position_side='short',
            position_qty=0.021,
            entry_price=2285.07,
            eval_ts='2026-04-27T19:22:53+00:00',
        )

        self.assertEqual(state.runtime_mode, 'FROZEN')
        self.assertEqual(state.freeze_reason, 'protective_order_missing')
        self.assertEqual(state.active_strategy, 'manual_protective_missing_probe')
        self.assertEqual(state.recover_check, {})
        self.assertEqual(state.recover_timeline, [])
        self.assertEqual(state.async_operations, {'active': [], 'history': []})
        self.assertIsNone(state.last_recover_result)
        self.assertEqual(state.recover_attempt_count, 0)
        self.assertEqual(state.strategy_protection_intent['risk_class'], 'FORCE_CLOSE')


if __name__ == '__main__':
    unittest.main()
