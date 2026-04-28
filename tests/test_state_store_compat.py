from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.manual_full_chain_closepos_acceptance import build_initial_state
from exec_framework.state_store import JsonStateStore


class StateStoreCompatCase(unittest.TestCase):
    def test_load_state_ignores_unknown_runtime_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'state.json'
            initial_state = build_initial_state('2026-04-27T10:00:00+00:00')
            store = JsonStateStore(path, initial_state)
            payload = store.load_payload()
            payload['state']['latest_expected_protective'] = {'kind': 'hard_stop'}
            payload['last_result'] = {
                'result_ts': '2026-04-27T10:00:01+00:00',
                'bar_ts': '2026-04-27T10:00:00+00:00',
                'status': 'FROZEN',
                'action_type': 'open',
                'executed_side': 'long',
                'unknown_result_field': 'ignored',
            }
            store._write_json(payload)

            state = store.load_state()
            result = store.load_last_result()

            self.assertEqual(state.state_ts, '2026-04-27T10:00:00+00:00')
            self.assertFalse(hasattr(state, 'latest_expected_protective'))
            self.assertIsNotNone(result)
            self.assertEqual(result.status, 'FROZEN')
            self.assertEqual(result.action_type, 'open')


if __name__ == '__main__':
    unittest.main()
