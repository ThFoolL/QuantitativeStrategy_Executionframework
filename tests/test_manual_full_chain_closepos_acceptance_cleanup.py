from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework import manual_full_chain_closepos_acceptance as module
from exec_framework.models import LiveStateSnapshot


class ManualFullChainAcceptanceCleanupCase(unittest.TestCase):
    def test_attempt_cleanup_close_runs_when_close_failed_but_exchange_already_flat(self) -> None:
        runtime = {'readonly_client': object()}
        summary: dict[str, object] = {}

        with patch.object(module, '_ensure_flat', return_value={'is_flat': True, 'open_orders_count': 0}) as ensure_flat_mock, patch.object(module, '_execute_phase') as execute_phase_mock:
            result = module._attempt_cleanup_close(
                runtime=runtime,
                symbol='ETHUSDT',
                config_validation={'ok': True},
                summary=summary,
                run_id='run-1',
            )

        self.assertIsNone(result)
        execute_phase_mock.assert_not_called()
        self.assertEqual(ensure_flat_mock.call_count, 1)
        self.assertNotIn('cleanup_phase', summary)

    def test_force_runtime_flat_ready_clears_stale_state_before_acceptance(self) -> None:
        now_iso = '2026-04-16T14:00:00+00:00'
        state = module.build_initial_state('2026-04-16T12:00:00+00:00')
        state.active_strategy = 'trend'
        state.active_side = 'long'
        state.exchange_position_qty = 0.009
        state.stop_price = 2338.42
        state.pending_execution_phase = 'frozen'
        state.freeze_status = 'ACTIVE'
        state.freeze_reason = 'local_exchange_position_presence_mismatch'
        state.protective_order_status = 'MISSING'
        state.strategy_protection_intent = {
            'pending_action': 'protective_rebuild',
            'position_side': 'long',
            'position_qty': 0.009,
            'pending_execution_phase': 'frozen',
            'protective_order_status': 'MISSING',
        }

        class StubStore:
            def __init__(self, seed_state):
                self._state = seed_state
                self._payload = {'state': module.asdict(seed_state), 'last_result': {'status': 'FROZEN'}}

            def load_state(self):
                return self._state

            def load_payload(self):
                return {'state': dict(self._payload['state']), 'last_result': self._payload['last_result']}

            def _write_json(self, payload):
                self._payload = payload
                self._state = LiveStateSnapshot(**payload['state'])

        class StubRuntimeStatusStore:
            def __init__(self):
                self.path = 'runtime/runtime_status.json'
                self.written = None

            def write(self, payload):
                self.written = payload

        runtime = {
            'readonly_client': SimpleNamespace(
                get_account_snapshot=lambda: SimpleNamespace(account_equity=1000.0, available_margin=900.0)
            ),
            'state_store': StubStore(state),
            'runtime_status_store': StubRuntimeStatusStore(),
            'runtime_dir': Path('runtime'),
        }

        with patch.object(module, '_ensure_flat', return_value={'is_flat': True, 'open_orders_count': 0}), patch.object(module, '_utc_iso', return_value=now_iso), patch.object(module, '_snapshot_runtime_files', return_value={'state': {'state': {'pending_execution_phase': None}}}):
            prepare = module._force_runtime_flat_ready(
                runtime=runtime,
                symbol='ETHUSDT',
                reason='pretrade_account_flat_and_no_open_orders',
            )

        prepared_state = runtime['state_store']._state
        self.assertEqual(prepared_state.active_strategy, 'none')
        self.assertIsNone(prepared_state.active_side)
        self.assertEqual(prepared_state.exchange_position_qty, 0.0)
        self.assertIsNone(prepared_state.pending_execution_phase)
        self.assertEqual(prepared_state.freeze_status, 'NONE')
        self.assertIsNone(prepared_state.freeze_reason)
        self.assertEqual(prepared_state.protective_order_status, 'NONE')
        self.assertEqual(prepared_state.exchange_protective_orders, [])
        self.assertIsNone((prepared_state.strategy_protection_intent or {}).get('pending_action'))
        self.assertEqual(prepare['flat_probe']['open_orders_count'], 0)
        self.assertEqual(runtime['runtime_status_store'].written['phase'], 'prepared')


if __name__ == '__main__':
    unittest.main()
