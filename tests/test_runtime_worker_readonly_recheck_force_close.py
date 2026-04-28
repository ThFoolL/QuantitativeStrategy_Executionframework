from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.runtime_worker import RuntimeWorker, READONLY_RECHECK_FREEZE


class RuntimeWorkerReadonlyRecheckForceCloseCase(unittest.TestCase):
    def test_protection_missing_recheck_stays_force_close_oriented(self) -> None:
        worker = RuntimeWorker(
            config=SimpleNamespace(symbol='ETHUSDT'),
            market_provider=None,
            engine=SimpleNamespace(executor_module=None),
            state_store=SimpleNamespace(load_state=lambda: None),
            status_store=SimpleNamespace(path=Path('runtime/runtime_status.json'), write=lambda payload: None),
            event_log=SimpleNamespace(path=Path('runtime/event_log.jsonl'), append=lambda *args, **kwargs: None),
            scheduler=SimpleNamespace(),
        )
        confirmation = SimpleNamespace(
            confirmation_status='PENDING',
            confirmation_category='pending',
            order_status='NEW',
            reconcile_status='MISMATCH',
            freeze_reason='protective_order_missing',
            executed_qty=0.0,
            avg_fill_price=None,
            exchange_order_ids=[],
            post_position_side='short',
            post_position_qty=0.021,
            should_freeze=True,
            trade_summary={
                'notes': ['protection_orders_missing'],
                'confirmation_category': 'pending',
            },
            notes=['protection_orders_missing'],
        )
        decision = worker._classify_readonly_recheck_decision(policy={}, confirmation=confirmation)

        self.assertEqual(decision.status, READONLY_RECHECK_FREEZE)
        self.assertEqual(decision.freeze_reason, 'protective_order_missing')
        self.assertEqual(decision.summary['risk_action'], 'FORCE_CLOSE')
        self.assertEqual(decision.recover_check['stop_reason'], 'protective_order_missing')
        self.assertEqual(decision.recover_check['stop_condition'], 'position_open_without_protection')
        self.assertEqual(decision.recover_check['recover_policy'], 'keep_frozen')
        self.assertEqual(decision.recover_check['recover_stage'], 'force_close_without_protection')
        self.assertEqual(decision.state_updates['pending_execution_phase'], 'frozen')


if __name__ == '__main__':
    unittest.main()
