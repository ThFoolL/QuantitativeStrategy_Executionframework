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

from exec_framework.runtime_worker import _build_runtime_components


class RuntimeWorkerStartupRebuildCase(unittest.TestCase):
    def test_startup_rebuild_bootstraps_state_from_exchange_and_notifies(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / 'runtime' / 'state.json'
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

            fake_account = SimpleNamespace(account_equity=123.45, available_margin=120.0, raw={})
            fake_position = SimpleNamespace(side='short', qty=0.372, entry_price=2116.5)
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

            sent_summaries = []

            class FakeReadOnlyClient:
                def __init__(self, *_args, **_kwargs):
                    pass

                def get_account_snapshot(self):
                    return fake_account

                def get_position_snapshot(self, _symbol):
                    return fake_position

                def get_open_orders(self, _symbol, client_order_ids=None):
                    return [fake_order]

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
                 patch('exec_framework.runtime_worker.RuntimeWorker._send_startup_rebuild_notice', lambda self, summary: sent_summaries.append(summary) or {'status': 'captured'}):
                worker = _build_runtime_components(config)

            state = worker.state_store.load_state()
            self.assertEqual(state.exchange_position_side, 'short')
            self.assertEqual(state.exchange_position_qty, 0.372)
            self.assertEqual(state.exchange_entry_price, 2116.5)
            self.assertEqual(state.runtime_mode, 'ACTIVE')
            self.assertEqual(state.consistency_status, 'OK')
            self.assertEqual(state.protective_order_status, 'ACTIVE')
            self.assertEqual(len(state.exchange_protective_orders), 1)
            self.assertEqual(state.active_strategy, 'trend')
            self.assertEqual(worker.startup_rebuild_summary['rebuild_result'], 'position_rebuilt_from_exchange')
            self.assertEqual(len(sent_summaries), 1)
            self.assertEqual(sent_summaries[0]['protective_order_count'], 1)


if __name__ == '__main__':
    unittest.main()
