from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.runtime_worker import RuntimeWorker


class _DummyStateStore:
    def __init__(self, cached: dict):
        self._cached = cached

    def load_state(self):
        return SimpleNamespace(last_publishable_result=self._cached)


class RuntimeWorkerPublishableOutputCase(unittest.TestCase):
    def test_state_update_round_does_not_reuse_cached_execution_confirmation(self) -> None:
        cached_result = {
            'action_type': 'open',
            'bar_ts': '2026-05-18T09:45:00+00:00',
            'execution_phase': 'entry_confirmed_pending_protective',
            'confirmation_status': 'POSITION_CONFIRMED',
            'confirmed_order_status': 'FILLED',
            'exchange_order_ids': ['8389766182121393416'],
        }
        output = {
            'plan': {'action_type': 'state_update', 'reason': 'degrade_to_hold'},
            'state': {
                'runtime_mode': 'ACTIVE',
                'exchange_position_side': 'short',
                'exchange_position_qty': 0.372,
                'last_publishable_result': cached_result,
            },
            'result': {
                'action_type': 'state_update',
                'status': 'SKIPPED',
                'execution_phase': 'none',
                'confirmation_status': 'NOT_REQUIRED',
                'confirmed_order_status': 'NOT_REQUIRED',
                'reconcile_status': 'OK',
                'bar_ts': '2026-05-18T12:25:00+00:00',
            },
        }
        worker = RuntimeWorker.__new__(RuntimeWorker)
        worker.state_store = _DummyStateStore(cached_result)
        selected = RuntimeWorker._select_publishable_output(worker, output)
        self.assertEqual(selected['result']['action_type'], 'state_update')
        self.assertEqual(selected['result']['confirmation_status'], 'NOT_REQUIRED')
        self.assertEqual(selected['result']['confirmed_order_status'], 'NOT_REQUIRED')


if __name__ == '__main__':
    unittest.main()
