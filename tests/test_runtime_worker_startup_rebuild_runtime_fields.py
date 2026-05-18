from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.runtime_worker import _build_runtime_components, build_initial_state
from exec_framework.state_store import JsonStateStore


class RuntimeWorkerStartupRebuildRuntimeFieldsCase(unittest.TestCase):
    def test_startup_rebuild_restores_runtime_relevant_fields_without_fabrication(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / 'runtime' / 'state.json'
            initial = build_initial_state('2026-05-18T10:29:15+00:00')
            initial.active_strategy = 'trend'
            initial.active_side = 'short'
            initial.strategy_entry_time = '2026-05-18T09:45:00+00:00'
            initial.strategy_entry_price = 2116.5
            initial.execution_entry_price = 2116.5
            initial.strategy_ref_entry_price = 2116.5
            initial.stop_price = 2159.3427402324705
            initial.base_quantity = 0.372
            initial.execution_quantity = 0.372
            initial.high_water_r = 0.4
            initial.execution_high_water_r = 0.4
            store = JsonStateStore(state_path, initial)
            store.save_state(initial)

            config = SimpleNamespace(
                state_path=str(state_path),
                symbol='ETHUSDT',
                recv_window_ms=5000,
                strategy_adapter='baseline',
                discord_execution_channel_id='1486034825830727710',
                discord_channel_id='1486034825830727710',
                discord_real_send_enabled=False,
                discord_message_tool_enabled=False,
                discord_send_require_idempotency=True,
                discord_send_ledger_path=str(Path(tmpdir) / 'runtime' / 'discord_send_ledger.json'),
                discord_send_receipt_log_path=str(Path(tmpdir) / 'runtime' / 'discord_send_receipts.jsonl'),
                discord_send_retry_limit=3,
                discord_transport='production_placeholder',
                discord_execution_confirmation_real_send_enabled=False,
                dry_run=False,
                submit_enabled=True,
            )

            fake_account = SimpleNamespace(account_equity=100.23235305, available_margin=85.72055475, raw={})
            fake_position = SimpleNamespace(side='short', qty=0.372, entry_price=2116.5, unrealized_pnl=0.5)
            fake_order = SimpleNamespace(
                order_id='protect-1',
                client_order_id='protect-hard-stop',
                status='NEW',
                side='BUY',
                position_side='BOTH',
                type='STOP_MARKET',
                orig_type='STOP_MARKET',
                qty=0.0,
                executed_qty=0.0,
                price=0.0,
                avg_price=0.0,
                stop_price=2159.34,
                working_type='MARK_PRICE',
                activate_price=None,
                price_protect=False,
                reduce_only=True,
                close_position=True,
                update_time_ms=1711380000000,
                raw={},
            )
            k1 = SimpleNamespace(is_closed=True, close_time_iso='2026-05-18T09:50:00+00:00', close_price=2110.0)
            k2 = SimpleNamespace(is_closed=True, close_time_iso='2026-05-18T09:55:00+00:00', close_price=2100.0)
            k3 = SimpleNamespace(is_closed=True, close_time_iso='2026-05-18T10:00:00+00:00', close_price=2090.0)

            class FakeReadOnlyClient:
                def __init__(self, *_args, **_kwargs):
                    pass

                def get_account_snapshot(self):
                    return fake_account

                def get_position_snapshot(self, _symbol):
                    return fake_position

                def get_open_orders(self, _symbol, client_order_ids=None):
                    return [fake_order]

                def get_server_time_ms(self):
                    return 1779099000000

                def get_klines(self, *_args, **_kwargs):
                    return [k1, k2, k3]

            class FakeMarketProvider:
                def __init__(self, *_args, **_kwargs):
                    pass

            class FakeExecutor:
                def __init__(self, *_args, **_kwargs):
                    pass

            with patch('exec_framework.runtime_worker.BinanceReadOnlyClient', FakeReadOnlyClient), \
                 patch('exec_framework.runtime_worker.BinanceReadOnlyMarketDataProvider', FakeMarketProvider), \
                 patch('exec_framework.runtime_worker.BinanceRealExecutor', FakeExecutor), \
                 patch('exec_framework.runtime_worker.build_strategy_adapter_from_config', lambda _config: SimpleNamespace()), \
                 patch('exec_framework.runtime_worker.RuntimeWorker._send_startup_rebuild_notice', lambda self, summary: {'status': 'captured'}):
                worker = _build_runtime_components(config)

            state = worker.state_store.load_state()
            self.assertEqual(state.can_open_new_position, False)
            self.assertEqual(state.can_modify_position, True)
            self.assertEqual(state.protective_order_status, 'ACTIVE')
            self.assertEqual(state.protective_phase_status, 'ACTIVE')
            self.assertEqual(state.hold_bars, 3)
            self.assertAlmostEqual(state.equity_at_entry, 99.73235305)
            self.assertGreater(state.high_water_r, 0.4)
            self.assertAlmostEqual(state.high_water_r, state.execution_high_water_r)


if __name__ == '__main__':
    unittest.main()
