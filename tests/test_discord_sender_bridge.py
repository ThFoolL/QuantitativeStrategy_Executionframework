from __future__ import annotations

import json
import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.discord_publisher import DiscordMessagePayload
from exec_framework.discord_sender_bridge import MessageToolDiscordSender, ProductionMessageToolDiscordTransport, build_discord_transport


class StubTransportSuccess:
    transport_name = 'stub_success'

    def __init__(self) -> None:
        self.calls = 0

    def send(self, *, payload, dispatch):
        self.calls += 1
        return {
            'message_id': f'msg-{self.calls}',
            'channel_id': payload.channel_id,
            'status': 'SENT',
        }


class StubTransportTimeoutOnce:
    transport_name = 'stub_timeout_once'

    def __init__(self) -> None:
        self.calls = 0

    def send(self, *, payload, dispatch):
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError('discord timeout')
        return {
            'message_id': 'msg-retry-ok',
            'channel_id': payload.channel_id,
            'status': 'SENT',
        }


class StubTransportForbidden:
    transport_name = 'stub_forbidden'

    def send(self, *, payload, dispatch):
        exc = RuntimeError('403 Forbidden: Missing Access')
        setattr(exc, 'status_code', 403)
        raise exc


class StubTransportUnknownChannel:
    transport_name = 'stub_unknown_channel'

    def send(self, *, payload, dispatch):
        exc = RuntimeError('404 Unknown Channel')
        setattr(exc, 'status_code', 404)
        raise exc


class StubTransportRateLimited:
    transport_name = 'stub_rate_limited'

    def send(self, *, payload, dispatch):
        exc = RuntimeError('429 Too Many Requests: retry after 1s')
        setattr(exc, 'status_code', 429)
        raise exc


class DiscordSenderBridgeCase(unittest.TestCase):
    def make_payload(self, *, with_idempotency: bool = True, kind: str = 'execution_confirmation') -> DiscordMessagePayload:
        metadata = {'kind': kind}
        if with_idempotency:
            metadata['idempotency_key'] = f'discord:BTCUSDT:2026-03-25T15:00:00+00:00:{kind}:deadbeefdeadbeef'
        return DiscordMessagePayload(
            channel_id='DISCORD_CHANNEL_ID_PLACEHOLDER',
            content='【演练】【非真实发单】Discord sender bridge test',
            metadata=metadata,
        )

    def test_build_transport_factory(self) -> None:
        self.assertEqual(build_discord_transport('unconfigured').transport_name, 'unconfigured_transport')
        self.assertEqual(build_discord_transport('dry_run').transport_name, 'dry_run_transport')
        self.assertIsInstance(build_discord_transport('production'), ProductionMessageToolDiscordTransport)

    def test_send_gate_default_closed(self) -> None:
        sender = MessageToolDiscordSender()
        result = sender.send(self.make_payload(kind='rehearsal_notification'))
        self.assertFalse(result['sent'])
        self.assertIn('discord_real_send_disabled', result['send_gate']['blockers'])
        self.assertEqual(result['receipt']['status'], 'blocked')
        self.assertEqual(result['send_gate']['transport_name'], 'unconfigured_transport')

    def test_execution_confirmation_real_send_stays_closed(self) -> None:
        sender = MessageToolDiscordSender(real_send_enabled=True, message_tool_enabled=True, transport=StubTransportSuccess())
        result = sender.send(self.make_payload(kind='execution_confirmation'))
        self.assertFalse(result['sent'])
        self.assertIn('execution_confirmation_real_send_not_open', result['send_gate']['blockers'])

    def test_idempotency_key_required(self) -> None:
        sender = MessageToolDiscordSender(real_send_enabled=True, message_tool_enabled=True)
        result = sender.send(self.make_payload(with_idempotency=False, kind='rehearsal_notification'))
        self.assertFalse(result['sent'])
        self.assertIn('idempotency_key_missing', result['send_gate']['blockers'])

    def test_success_send_records_ledger_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_path = Path(tmpdir) / 'discord_send_ledger.json'
            receipt_log = Path(tmpdir) / 'discord_send_receipts.jsonl'
            transport = StubTransportSuccess()
            sender = MessageToolDiscordSender(
                real_send_enabled=True,
                message_tool_enabled=True,
                ledger_path=ledger_path,
                receipt_store_path=receipt_log,
                transport=transport,
            )
            payload = self.make_payload(kind='rehearsal_notification')
            first = sender.send(payload)
            second = sender.send(payload)

            self.assertTrue(first['sent'])
            self.assertEqual(first['receipt']['status'], 'sent')
            self.assertEqual(first['receipt']['provider_status'], 'SENT')
            self.assertEqual(first['receipt']['payload_kind'], 'rehearsal_notification')
            self.assertIsNotNone(first['receipt']['sent_at'])
            self.assertEqual(first['provider_response']['message_id'], 'msg-1')
            self.assertEqual(second['reason'], 'duplicate_idempotency_key')
            self.assertEqual(transport.calls, 1)

            ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
            self.assertIn(payload.metadata['idempotency_key'], ledger['entries'])
            self.assertTrue(ledger['entries'][payload.metadata['idempotency_key']]['sent'])
            self.assertEqual(ledger['entries'][payload.metadata['idempotency_key']]['provider_message_id'], 'msg-1')

            rows = [json.loads(line) for line in receipt_log.read_text(encoding='utf-8').splitlines() if line.strip()]
            self.assertEqual(rows[-1]['payload_kind'], 'rehearsal_notification')
            self.assertEqual(rows[-1]['transport_name'], 'stub_success')

    def test_retryable_failure_classification_and_retry_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sender = MessageToolDiscordSender(
                real_send_enabled=True,
                message_tool_enabled=True,
                ledger_path=Path(tmpdir) / 'discord_send_ledger.json',
                receipt_store_path=Path(tmpdir) / 'discord_send_receipts.jsonl',
                retry_limit=2,
                transport=StubTransportTimeoutOnce(),
            )
            result = sender.send(self.make_payload(kind='rehearsal_notification'))
            self.assertTrue(result['sent'])
            self.assertEqual(result['receipt']['attempt_count'], 2)
            self.assertEqual(result['receipt']['status'], 'sent')

    def test_provider_specific_permission_mapping(self) -> None:
        sender = MessageToolDiscordSender(
            real_send_enabled=True,
            message_tool_enabled=True,
            transport=StubTransportForbidden(),
            retry_limit=1,
        )
        result = sender.send(self.make_payload(kind='rehearsal_notification'))
        self.assertFalse(result['sent'])
        self.assertEqual(result['failure']['category'], 'permission_denied')
        self.assertFalse(result['failure']['retryable'])
        self.assertEqual(result['failure']['code'], 'DISCORD_FORBIDDEN')

    def test_provider_specific_channel_not_found_mapping(self) -> None:
        sender = MessageToolDiscordSender(
            real_send_enabled=True,
            message_tool_enabled=True,
            transport=StubTransportUnknownChannel(),
            retry_limit=1,
        )
        result = sender.send(self.make_payload(kind='rehearsal_notification'))
        self.assertFalse(result['sent'])
        self.assertEqual(result['failure']['category'], 'channel_not_found')
        self.assertFalse(result['failure']['retryable'])

    def test_provider_specific_rate_limit_mapping(self) -> None:
        sender = MessageToolDiscordSender(
            real_send_enabled=True,
            message_tool_enabled=True,
            transport=StubTransportRateLimited(),
            retry_limit=1,
        )
        result = sender.send(self.make_payload(kind='rehearsal_notification'))
        self.assertFalse(result['sent'])
        self.assertEqual(result['failure']['category'], 'rate_limited')
        self.assertTrue(result['failure']['retryable'])

    def test_production_command_transport_via_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = Path(tmpdir) / 'mock_sender.py'
            capture_path = Path(tmpdir) / 'capture.json'
            script_path.write_text(
                textwrap.dedent(
                    f"""
                    import json
                    import sys
                    from pathlib import Path

                    payload = json.loads(sys.stdin.read())
                    Path({str(capture_path)!r}).write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
                    print(json.dumps({{'message_id': 'msg-cmd-1', 'channel_id': payload['target'], 'status': 'SENT'}}))
                    """
                ),
                encoding='utf-8',
            )
            old_command = os.environ.get('DISCORD_MESSAGE_TOOL_COMMAND')
            try:
                os.environ['DISCORD_MESSAGE_TOOL_COMMAND'] = f'{sys.executable} {script_path}'
                sender = MessageToolDiscordSender(
                    real_send_enabled=True,
                    message_tool_enabled=True,
                    transport=ProductionMessageToolDiscordTransport(),
                )
                result = sender.send(self.make_payload(kind='rehearsal_notification'))
            finally:
                if old_command is None:
                    os.environ.pop('DISCORD_MESSAGE_TOOL_COMMAND', None)
                else:
                    os.environ['DISCORD_MESSAGE_TOOL_COMMAND'] = old_command

            self.assertTrue(result['sent'])
            self.assertEqual(result['receipt']['transport_name'], 'message_tool_command_transport')
            self.assertEqual(result['receipt']['provider_message_id'], 'msg-cmd-1')
            captured = json.loads(capture_path.read_text(encoding='utf-8'))
            self.assertEqual(captured['target'], 'DISCORD_CHANNEL_ID_PLACEHOLDER')
            self.assertEqual(captured['payload']['metadata']['kind'], 'rehearsal_notification')

    def test_production_command_transport_missing_command_is_not_ready(self) -> None:
        old_command = os.environ.pop('DISCORD_MESSAGE_TOOL_COMMAND', None)
        try:
            sender = MessageToolDiscordSender(
                real_send_enabled=True,
                message_tool_enabled=True,
                transport=ProductionMessageToolDiscordTransport(),
                retry_limit=1,
            )
            result = sender.send(self.make_payload(kind='rehearsal_notification'))
        finally:
            if old_command is not None:
                os.environ['DISCORD_MESSAGE_TOOL_COMMAND'] = old_command

        self.assertFalse(result['sent'])
        self.assertEqual(result['failure']['category'], 'transport_not_ready')
        self.assertEqual(result['receipt']['transport_name'], 'message_tool_command_transport')


if __name__ == '__main__':
    unittest.main()
