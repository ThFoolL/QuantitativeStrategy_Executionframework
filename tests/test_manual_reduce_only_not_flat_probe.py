from __future__ import annotations

import unittest
from unittest import mock


class ManualReduceOnlyNotFlatProbeCase(unittest.TestCase):
    def test_module_imports(self) -> None:
        import exec_framework.manual_reduce_only_not_flat_probe as probe

        self.assertTrue(callable(probe.main))

    def test_run_id_format(self) -> None:
        import exec_framework.manual_reduce_only_not_flat_probe as probe

        run_id = probe._run_id()
        self.assertTrue(run_id.endswith('Z'))
        self.assertGreaterEqual(len(run_id), 17)

    def test_min_notional_bump(self) -> None:
        import exec_framework.manual_reduce_only_not_flat_probe as probe

        qty = probe._bump_quantity_to_min_notional(
            quantity=0.008,
            price=2300.0,
            qty_step=0.001,
            min_notional=20.0,
        )
        self.assertGreaterEqual(qty * 2300.0, 20.0)

    def test_ensure_flat_reads_position_and_open_orders(self) -> None:
        import exec_framework.manual_reduce_only_not_flat_probe as probe

        client = mock.Mock()
        client.get_position_snapshot.return_value = mock.Mock(side=None, qty=0.0, raw={})
        client.get_open_orders.return_value = []

        with mock.patch.object(probe, 'asdict', side_effect=lambda obj: {'qty': getattr(obj, 'qty', None), 'side': getattr(obj, 'side', None)}):
            out = probe._ensure_flat(client, 'ETHUSDT')

        self.assertTrue(out['is_flat'])
        self.assertEqual(out['open_orders_count'], 0)


if __name__ == '__main__':
    unittest.main()
