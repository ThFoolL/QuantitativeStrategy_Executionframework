from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import OrderSnapshot
from exec_framework.protective_orders import validate_protective_orders


class ProtectiveOrdersPriceToleranceCase(unittest.TestCase):
    def make_order(self, *, stop_price: float) -> OrderSnapshot:
        return OrderSnapshot(
            order_id='protect-1',
            client_order_id='protect-hard-stop',
            status='NEW',
            type='STOP_MARKET',
            orig_type='STOP_MARKET',
            time_in_force='GTC',
            side='buy',
            position_side='both',
            qty=0.0,
            executed_qty=0.0,
            price=0.0,
            avg_price=0.0,
            cum_quote=0.0,
            stop_price=stop_price,
            working_type='MARK_PRICE',
            activate_price=None,
            price_protect=False,
            reduce_only=True,
            close_position=True,
            update_time_ms=1711380000000,
            raw={},
        )

    def test_close_position_hard_stop_allows_small_price_drift(self) -> None:
        validation = validate_protective_orders(
            strategy='trend',
            position_side='short',
            position_qty=0.372,
            stop_price=2159.3427402324705,
            tp_price=None,
            open_orders=[self.make_order(stop_price=2159.30)],
        )
        self.assertTrue(validation.ok)
        self.assertEqual(validation.status, 'OK')
        self.assertIsNone(validation.freeze_reason)

    def test_close_position_hard_stop_still_rejects_large_price_drift(self) -> None:
        validation = validate_protective_orders(
            strategy='trend',
            position_side='short',
            position_qty=0.372,
            stop_price=2159.3427402324705,
            tp_price=None,
            open_orders=[self.make_order(stop_price=2165.0)],
        )
        self.assertFalse(validation.ok)
        self.assertEqual(validation.freeze_reason, 'protective_order_semantic_mismatch')
        self.assertIn('price_mismatch:hard_stop', validation.notes)


if __name__ == '__main__':
    unittest.main()
