from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.runtime_env import (
    DEFAULT_BINANCE_SYMBOL,
    DEFAULT_DISCORD_RECEIPT_LOG_PATH,
    DEFAULT_DISCORD_TRANSPORT,
    DEFAULT_SUBMIT_MAX_NOTIONAL,
    LIVE_SUBMIT_MANUAL_ACK_TOKEN,
    load_binance_env,
)


class RuntimeEnvCase(unittest.TestCase):
    def test_empty_base_url_and_symbol_fall_back_to_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / 'binance_api.env'
            env_path.write_text(
                '\n'.join(
                    [
                        'BINANCE_API_KEY=test_key',
                        'BINANCE_API_SECRET=test_secret',
                        'BINANCE_BASE_URL=',
                        'BINANCE_SYMBOL=',
                        'LIVE_STATE_PATH=',
                        'DISCORD_MONITOR_CHANNEL_ID=',
                        'DISCORD_EXECUTION_CHANNEL_ID=',
                    ]
                ),
                encoding='utf-8',
            )

            old_env = os.environ.copy()
            try:
                for key in list(os.environ.keys()):
                    if key.startswith('BINANCE_') or key.startswith('LIVE_STATE_PATH') or key.startswith('DISCORD_'):
                        os.environ.pop(key, None)
                config = load_binance_env(env_path)
            finally:
                os.environ.clear()
                os.environ.update(old_env)

            self.assertEqual(config.base_url, 'https://fapi.binance.com')
            self.assertEqual(config.symbol, DEFAULT_BINANCE_SYMBOL)
            self.assertEqual(config.state_path, 'runtime/state.json')
            self.assertEqual(config.discord_execution_channel_id, 'DISCORD_CHANNEL_ID_PLACEHOLDER')
            self.assertFalse(config.discord_real_send_enabled)
            self.assertFalse(config.discord_message_tool_enabled)
            self.assertTrue(config.discord_send_require_idempotency)
            self.assertEqual(config.discord_transport, DEFAULT_DISCORD_TRANSPORT)
            self.assertEqual(config.discord_send_receipt_log_path, DEFAULT_DISCORD_RECEIPT_LOG_PATH)
            self.assertFalse(config.discord_rehearsal_real_send_enabled)
            self.assertEqual(config.submit_max_notional, DEFAULT_SUBMIT_MAX_NOTIONAL)

    def test_submit_guardrail_fields_parse_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / 'binance_api.env'
            env_path.write_text(
                '\n'.join(
                    [
                        'BINANCE_API_KEY=test_key',
                        'BINANCE_API_SECRET=test_secret',
                        'BINANCE_SUBMIT_SYMBOL_ALLOWLIST=btcusdt, ethusdt',
                        'BINANCE_SUBMIT_MAX_QTY=0.02',
                        'BINANCE_SUBMIT_MAX_NOTIONAL=50',
                        'BINANCE_SUBMIT_REQUIRE_RECONCILE_OK=true',
                        'BINANCE_SUBMIT_REQUIRE_ACTIVE_RUNTIME=true',
                        'BINANCE_SUBMIT_REQUIRE_NO_PENDING_EXECUTION=true',
                        'DISCORD_AUDIT_ENABLED=true',
                        'DISCORD_REAL_SEND_ENABLED=true',
                        'DISCORD_MESSAGE_TOOL_ENABLED=true',
                        'DISCORD_SEND_REQUIRE_IDEMPOTENCY=true',
                        'DISCORD_SEND_LEDGER_PATH=runtime/discord_send_ledger.json',
                        'DISCORD_SEND_RECEIPT_LOG_PATH=runtime/discord_send_receipts.jsonl',
                        'DISCORD_SEND_RETRY_LIMIT=5',
                        'DISCORD_TRANSPORT=production',
                        'DISCORD_REHEARSAL_REAL_SEND_ENABLED=true',
                        f'BINANCE_SUBMIT_MANUAL_ACK_TOKEN={LIVE_SUBMIT_MANUAL_ACK_TOKEN}',
                    ]
                ),
                encoding='utf-8',
            )

            old_env = os.environ.copy()
            try:
                for key in list(os.environ.keys()):
                    if key.startswith('BINANCE_') or key.startswith('LIVE_STATE_PATH') or key.startswith('DISCORD_'):
                        os.environ.pop(key, None)
                config = load_binance_env(env_path)
            finally:
                os.environ.clear()
                os.environ.update(old_env)

            self.assertEqual(config.submit_symbol_allowlist, ('BTCUSDT', 'ETHUSDT'))
            self.assertEqual(config.submit_max_qty, 0.02)
            self.assertEqual(config.submit_max_notional, 50.0)
            self.assertTrue(config.submit_require_reconcile_ok)
            self.assertTrue(config.submit_require_active_runtime)
            self.assertTrue(config.submit_require_no_pending_execution)
            self.assertTrue(config.discord_audit_enabled)
            self.assertTrue(config.discord_real_send_enabled)
            self.assertTrue(config.discord_message_tool_enabled)
            self.assertTrue(config.discord_send_require_idempotency)
            self.assertEqual(config.discord_send_ledger_path, 'runtime/discord_send_ledger.json')
            self.assertEqual(config.discord_send_receipt_log_path, 'runtime/discord_send_receipts.jsonl')
            self.assertEqual(config.discord_send_retry_limit, 5)
            self.assertEqual(config.discord_transport, 'production')
            self.assertTrue(config.discord_rehearsal_real_send_enabled)
            self.assertEqual(config.submit_manual_ack_token, LIVE_SUBMIT_MANUAL_ACK_TOKEN)


if __name__ == '__main__':
    unittest.main()
