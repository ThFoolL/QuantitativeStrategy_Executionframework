from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

from .discord_publisher import DiscordMessagePayload


@dataclass(frozen=True)
class MessageToolDispatch:
    channel: str
    target: str
    message: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class DiscordSendFailure:
    category: str
    retryable: bool
    code: str
    message: str
    details: dict[str, Any]


@dataclass(frozen=True)
class DiscordSendReceipt:
    ok: bool
    sent: bool
    status: str
    reason: str | None
    channel: str
    target: str
    idempotency_key: str | None
    payload_kind: str | None
    message_id: str | None
    sent_at: str | None
    attempt_count: int
    max_attempts: int
    retryable: bool
    failure_category: str | None
    failure_code: str | None
    transport_name: str | None
    provider_message_id: str | None
    provider_channel_id: str | None
    provider_status: str | None
    provider_response_excerpt: dict[str, Any] | None
    response_payload: dict[str, Any] | None
    dispatch: dict[str, Any]
    ledger_path: str | None
    recorded_at: str


class DiscordTransport(Protocol):
    transport_name: str

    def send(self, *, payload: DiscordMessagePayload, dispatch: MessageToolDispatch) -> dict[str, Any]: ...


@dataclass(frozen=True)
class DryRunDiscordTransport:
    transport_name: str = 'dry_run_transport'

    def send(self, *, payload: DiscordMessagePayload, dispatch: MessageToolDispatch) -> dict[str, Any]:
        return {
            'dry_run': True,
            'transport': self.transport_name,
            'status': 'DRY_RUN',
            'message_id': None,
            'channel_id': payload.channel_id,
            'preview': {
                'target': dispatch.target,
                'message': dispatch.message,
            },
        }


@dataclass(frozen=True)
class UnconfiguredDiscordTransport:
    transport_name: str = 'unconfigured_transport'

    def send(self, *, payload: DiscordMessagePayload, dispatch: MessageToolDispatch) -> dict[str, Any]:
        raise RuntimeError('message_tool_transport_not_configured')


@dataclass(frozen=True)
class ProductionMessageToolDiscordTransport:
    """通过外部命令执行的 production transport。

    设计目标：
    - 不在仓库内硬编码 OpenClaw tool/runtime 依赖
    - 通过 env 提供 sender command，实现真正外发接线
    - 默认不启用；command 缺失时明确报未接线

    约定：
    - command 从 `DISCORD_MESSAGE_TOOL_COMMAND` 读取
    - 启动命令后将 dispatch JSON 写入 stdin
    - stdout 需返回 JSON，至少包含 message_id/channel_id/status 中的可用字段
    """

    command: str | None = None
    timeout_seconds: int = 20
    transport_name: str = 'message_tool_command_transport'

    def send(self, *, payload: DiscordMessagePayload, dispatch: MessageToolDispatch) -> dict[str, Any]:
        command = (self.command or os.environ.get('DISCORD_MESSAGE_TOOL_COMMAND') or '').strip()
        if not command:
            raise RuntimeError('message_tool_command_missing')

        timeout_seconds = max(1, int(self.timeout_seconds or int(os.environ.get('DISCORD_MESSAGE_TOOL_TIMEOUT_SECONDS', '20'))))
        request_payload = {
            'channel': dispatch.channel,
            'target': dispatch.target,
            'message': dispatch.message,
            'metadata': dict(dispatch.metadata or {}),
            'payload': {
                'channel_id': payload.channel_id,
                'content': payload.content,
                'metadata': dict(payload.metadata or {}),
            },
        }
        try:
            completed = subprocess.run(
                shlex.split(command),
                input=json.dumps(request_payload, ensure_ascii=False),
                text=True,
                capture_output=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f'message tool command timeout after {timeout_seconds}s') from exc
        except OSError as exc:
            raise ConnectionError(f'message tool command failed to start: {exc}') from exc

        stdout = (completed.stdout or '').strip()
        stderr = (completed.stderr or '').strip()
        if completed.returncode != 0:
            exc = RuntimeError(stderr or stdout or f'message tool command exit {completed.returncode}')
            setattr(exc, 'status_code', completed.returncode)
            raise exc
        if not stdout:
            raise RuntimeError('message_tool_command_empty_response')
        try:
            response_payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f'message_tool_command_invalid_json: {stdout[:200]}') from exc

        response_payload.setdefault('transport', self.transport_name)
        response_payload.setdefault('status', 'SENT')
        response_payload.setdefault('channel_id', payload.channel_id)
        return response_payload


class DiscordSendLedger:
    def __init__(self, path: str | Path | None):
        self.path = None if path is None else Path(path)
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = None if self.path is None else self.path.with_suffix(self.path.suffix + '.lock')

    def load(self) -> dict[str, Any]:
        if self.path is None or not self.path.exists():
            return {'entries': {}}
        return json.loads(self.path.read_text(encoding='utf-8'))

    def get(self, idempotency_key: str | None) -> dict[str, Any] | None:
        if not idempotency_key:
            return None
        return self.load().get('entries', {}).get(idempotency_key)

    def _write_payload(self, payload: dict[str, Any]) -> None:
        if self.path is None:
            return
        with tempfile.NamedTemporaryFile('w', delete=False, encoding='utf-8', dir=str(self.path.parent)) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp_path = Path(tmp.name)
        tmp_path.replace(self.path)

    def _locked_edit(self, editor):
        if self.path is None:
            return editor({'entries': {}})
        if self._lock_path is None:
            raise RuntimeError('ledger_lock_path_missing')
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._lock_path, 'a+', encoding='utf-8') as lock_file:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            payload = self.load()
            result = editor(payload)
            self._write_payload(payload)
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            return result

    def record(self, idempotency_key: str | None, receipt: DiscordSendReceipt) -> None:
        if self.path is None or not idempotency_key:
            return

        def editor(payload: dict[str, Any]) -> None:
            payload.setdefault('entries', {})[idempotency_key] = asdict(receipt)
            payload['updated_at'] = datetime.now(timezone.utc).isoformat()

        self._locked_edit(editor)

    def begin_attempt(self, idempotency_key: str | None, dispatch: dict[str, Any]) -> dict[str, Any] | None:
        if self.path is None or not idempotency_key:
            return None

        def editor(payload: dict[str, Any]) -> dict[str, Any] | None:
            entries = payload.setdefault('entries', {})
            existing = entries.get(idempotency_key)
            if existing is not None and existing.get('sent'):
                return existing
            now = datetime.now(timezone.utc).isoformat()
            entries[idempotency_key] = {
                **(existing or {}),
                'ok': False,
                'sent': False,
                'status': 'inflight',
                'reason': 'send_inflight',
                'channel': dispatch.get('channel'),
                'target': dispatch.get('target'),
                'idempotency_key': idempotency_key,
                'payload_kind': dispatch.get('metadata', {}).get('payload_metadata', {}).get('kind') or dispatch.get('metadata', {}).get('payload_kind'),
                'message_id': None,
                'sent_at': None,
                'attempt_count': int(existing.get('attempt_count', 0) if existing else 0),
                'max_attempts': None,
                'retryable': False,
                'failure_category': None,
                'failure_code': None,
                'transport_name': dispatch.get('metadata', {}).get('transport_name'),
                'provider_message_id': None,
                'provider_channel_id': None,
                'provider_status': 'INFLIGHT',
                'provider_response_excerpt': None,
                'response_payload': None,
                'dispatch': dispatch,
                'ledger_path': None if self.path is None else str(self.path),
                'recorded_at': now,
                'reservation': {
                    'status': 'inflight',
                    'host': socket.gethostname(),
                    'pid': os.getpid(),
                    'reserved_at': now,
                },
            }
            payload['updated_at'] = now
            return None

        return self._locked_edit(editor)


class DiscordSendReceiptStore:
    def __init__(self, path: str | Path | None):
        self.path = None if path is None else Path(path)
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, receipt: DiscordSendReceipt) -> str | None:
        if self.path is None:
            return None
        with self.path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(asdict(receipt), ensure_ascii=False) + '\n')
        return str(self.path)


def build_discord_transport(transport_name: str | None) -> DiscordTransport:
    normalized = (transport_name or 'unconfigured').strip().lower()
    if normalized in {'', 'unconfigured', 'none', 'disabled'}:
        return UnconfiguredDiscordTransport()
    if normalized in {'dry_run', 'dry-run', 'preview'}:
        return DryRunDiscordTransport()
    if normalized in {'production', 'command', 'message_tool_command', 'message_tool', 'message_tool_production'}:
        return ProductionMessageToolDiscordTransport()
    if normalized in {'production_placeholder', 'message_tool_production_placeholder'}:
        return ProductionMessageToolDiscordTransport(command=None, transport_name='message_tool_command_transport')
    raise ValueError(f'unsupported discord transport: {transport_name}')


class MessageToolDiscordSender:
    """面向外部 message tool 的桥接器。"""

    def __init__(
        self,
        *,
        channel: str = 'discord',
        real_send_enabled: bool = False,
        message_tool_enabled: bool = False,
        require_idempotency: bool = True,
        ledger_path: str | Path | None = None,
        receipt_store_path: str | Path | None = None,
        retry_limit: int = 3,
        transport: DiscordTransport | None = None,
    ):
        self.channel = channel
        self.real_send_enabled = real_send_enabled
        self.message_tool_enabled = message_tool_enabled
        self.require_idempotency = require_idempotency
        self.ledger = DiscordSendLedger(ledger_path)
        self.receipt_store = DiscordSendReceiptStore(receipt_store_path)
        self.retry_limit = max(1, int(retry_limit))
        self.transport = transport if transport is not None else UnconfiguredDiscordTransport()

    def build_dispatch_preview(self, payload: DiscordMessagePayload) -> MessageToolDispatch:
        return MessageToolDispatch(
            channel=self.channel,
            target=payload.channel_id,
            message=payload.content,
            metadata={
                'dry_run_only': not self.can_real_send(payload),
                'bridge_kind': 'message_tool_dispatch',
                'payload_metadata': dict(payload.metadata or {}),
                'payload_kind': (payload.metadata or {}).get('kind'),
                'idempotency_key': (payload.metadata or {}).get('idempotency_key'),
                'message_tool_enabled': self.message_tool_enabled,
                'real_send_enabled': self.real_send_enabled,
                'transport_name': getattr(self.transport, 'transport_name', self.transport.__class__.__name__),
                'send_gate': self.build_send_gate(payload),
            },
        )

    def build_send_gate(self, payload: DiscordMessagePayload) -> dict[str, Any]:
        idempotency_key = (payload.metadata or {}).get('idempotency_key')
        payload_kind = (payload.metadata or {}).get('kind')
        blockers: list[str] = []
        if not self.real_send_enabled:
            blockers.append('discord_real_send_disabled')
        if not self.message_tool_enabled:
            blockers.append('message_tool_disabled')
        if self.require_idempotency and not idempotency_key:
            blockers.append('idempotency_key_missing')
        if payload_kind == 'execution_confirmation':
            blockers.append('execution_confirmation_real_send_not_open')
        ledger_entry = self.ledger.get(idempotency_key)
        if ledger_entry is not None and ledger_entry.get('sent'):
            blockers.append('idempotency_key_already_sent')
        return {
            'real_send_enabled': self.real_send_enabled,
            'message_tool_enabled': self.message_tool_enabled,
            'require_idempotency': self.require_idempotency,
            'idempotency_key': idempotency_key,
            'payload_kind': payload_kind,
            'ledger_path': None if self.ledger.path is None else str(self.ledger.path),
            'receipt_store_path': None if self.receipt_store.path is None else str(self.receipt_store.path),
            'ledger_hit': ledger_entry is not None,
            'blocked': bool(blockers),
            'blockers': blockers,
            'retry_limit': self.retry_limit,
            'transport_name': getattr(self.transport, 'transport_name', self.transport.__class__.__name__),
            'concurrency_scope': 'single_host_file_lock' if self.ledger.path is not None and fcntl is not None else 'single_process_best_effort',
        }

    def can_real_send(self, payload: DiscordMessagePayload) -> bool:
        return not self.build_send_gate(payload)['blocked']

    def classify_failure(self, exc: Exception) -> DiscordSendFailure:
        message = str(exc or '')
        message_lower = message.lower()
        name = exc.__class__.__name__
        name_lower = name.lower()
        status_code = getattr(exc, 'status_code', None)
        provider_code = getattr(exc, 'code', None)

        if status_code == 403 or 'forbidden' in message_lower or 'missing access' in message_lower or 'missing permissions' in message_lower or 'permission' in name_lower:
            return DiscordSendFailure(
                category='permission_denied',
                retryable=False,
                code='DISCORD_FORBIDDEN',
                message=message or 'discord permission denied',
                details={'exception_type': name, 'status_code': status_code, 'provider_code': provider_code, 'retry_advice': 'check_bot_permissions_and_channel_acl'},
            )
        if status_code == 404 or 'unknown channel' in message_lower or 'channel not found' in message_lower or 'cannot see channel' in message_lower:
            return DiscordSendFailure(
                category='channel_not_found',
                retryable=False,
                code='DISCORD_CHANNEL_NOT_FOUND',
                message=message or 'discord channel not found',
                details={'exception_type': name, 'status_code': status_code, 'provider_code': provider_code, 'retry_advice': 'verify_channel_id_and_bot_visibility'},
            )
        if status_code == 429 or provider_code == 429 or 'rate limit' in message_lower or 'retry after' in message_lower or 'too many requests' in message_lower:
            return DiscordSendFailure(
                category='rate_limited',
                retryable=True,
                code='DISCORD_RATE_LIMITED',
                message=message or 'discord rate limited',
                details={'exception_type': name, 'status_code': status_code, 'provider_code': provider_code, 'retry_advice': 'respect_retry_after_and_reduce_burst'},
            )
        if isinstance(exc, (TimeoutError, ConnectionError, OSError, socket.timeout)) or 'timeout' in name_lower or 'timeout' in message_lower or 'temporarily unavailable' in message_lower or 'connection reset' in message_lower or 'network' in message_lower:
            return DiscordSendFailure(
                category='network_failure',
                retryable=True,
                code='DISCORD_NETWORK_FAILURE',
                message=message or 'discord network failure',
                details={'exception_type': name, 'status_code': status_code, 'provider_code': provider_code, 'retry_advice': 'retry_with_backoff_after_network_check'},
            )
        if 'not implemented' in message_lower or 'not_implemented' in message_lower or 'command_missing' in message_lower:
            return DiscordSendFailure(
                category='transport_not_ready',
                retryable=False,
                code='DISCORD_TRANSPORT_NOT_READY',
                message=message or 'discord production transport not ready',
                details={'exception_type': name, 'status_code': status_code, 'provider_code': provider_code, 'retry_advice': 'wire_real_message_tool_transport_before_live_send'},
            )
        return DiscordSendFailure(
            category='provider_unknown_error',
            retryable=True,
            code='DISCORD_PROVIDER_UNKNOWN',
            message=message or 'discord provider unknown error',
            details={'exception_type': name, 'status_code': status_code, 'provider_code': provider_code, 'retry_advice': 'inspect_provider_payload_before_next_attempt'},
        )

    @staticmethod
    def _provider_response_excerpt(response_payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if not response_payload:
            return None
        return {
            'message_id': response_payload.get('message_id'),
            'channel_id': response_payload.get('channel_id'),
            'status': response_payload.get('status'),
            'transport': response_payload.get('transport'),
            'dry_run': response_payload.get('dry_run'),
        }

    def _build_receipt(
        self,
        *,
        payload: DiscordMessagePayload,
        dispatch: MessageToolDispatch,
        status: str,
        sent: bool,
        reason: str | None,
        attempt_count: int,
        retryable: bool,
        failure: DiscordSendFailure | None,
        response_payload: dict[str, Any] | None,
    ) -> DiscordSendReceipt:
        now_iso = datetime.now(timezone.utc).isoformat()
        sent_at = now_iso if sent else None
        return DiscordSendReceipt(
            ok=sent,
            sent=sent,
            status=status,
            reason=reason,
            channel=dispatch.channel,
            target=dispatch.target,
            idempotency_key=(payload.metadata or {}).get('idempotency_key'),
            payload_kind=(payload.metadata or {}).get('kind'),
            message_id=None if response_payload is None else response_payload.get('message_id'),
            sent_at=sent_at,
            attempt_count=attempt_count,
            max_attempts=self.retry_limit,
            retryable=retryable,
            failure_category=None if failure is None else failure.category,
            failure_code=None if failure is None else failure.code,
            transport_name=getattr(self.transport, 'transport_name', self.transport.__class__.__name__),
            provider_message_id=None if response_payload is None else response_payload.get('message_id'),
            provider_channel_id=None if response_payload is None else response_payload.get('channel_id'),
            provider_status=None if response_payload is None else response_payload.get('status'),
            provider_response_excerpt=self._provider_response_excerpt(response_payload),
            response_payload=response_payload,
            dispatch={
                'channel': dispatch.channel,
                'target': dispatch.target,
                'message': dispatch.message,
                'metadata': dispatch.metadata,
            },
            ledger_path=None if self.ledger.path is None else str(self.ledger.path),
            recorded_at=now_iso,
        )

    def _record_receipt(self, receipt: DiscordSendReceipt) -> str | None:
        return self.receipt_store.append(receipt)

    def _transport_send(self, payload: DiscordMessagePayload, dispatch: MessageToolDispatch) -> dict[str, Any]:
        return self.transport.send(payload=payload, dispatch=dispatch)

    def send(self, payload: DiscordMessagePayload) -> dict[str, Any]:
        dispatch = self.build_dispatch_preview(payload)
        send_gate = self.build_send_gate(payload)
        idempotency_key = (payload.metadata or {}).get('idempotency_key')
        ledger_entry = self.ledger.get(idempotency_key)
        duplicate_only = ledger_entry is not None and bool(ledger_entry.get('sent'))
        non_duplicate_blockers = [item for item in send_gate['blockers'] if item != 'idempotency_key_already_sent']
        if non_duplicate_blockers:
            receipt = self._build_receipt(
                payload=payload,
                dispatch=dispatch,
                status='blocked',
                sent=False,
                reason=';'.join(non_duplicate_blockers) or 'send_gate_blocked',
                attempt_count=0,
                retryable=False,
                failure=None,
                response_payload=None,
            )
            receipt_path = self._record_receipt(receipt)
            return {
                'dispatch': receipt.dispatch,
                'sent': False,
                'reason': receipt.reason,
                'send_gate': send_gate,
                'receipt': asdict(receipt),
                'receipt_store_path': receipt_path,
                'failure': None,
                'provider_response': None,
            }

        if duplicate_only and ledger_entry is not None:
            return {
                'dispatch': ledger_entry.get('dispatch'),
                'sent': bool(ledger_entry.get('sent')),
                'reason': 'duplicate_idempotency_key',
                'send_gate': send_gate,
                'receipt': ledger_entry,
                'receipt_store_path': None if self.receipt_store.path is None else str(self.receipt_store.path),
                'failure': None,
                'provider_response': ledger_entry.get('provider_response_excerpt') or ledger_entry.get('response_payload'),
            }

        reserved_entry = self.ledger.begin_attempt(idempotency_key, {
            'channel': dispatch.channel,
            'target': dispatch.target,
            'message': dispatch.message,
            'metadata': dispatch.metadata,
        })
        if reserved_entry is not None and reserved_entry.get('sent'):
            return {
                'dispatch': reserved_entry.get('dispatch'),
                'sent': bool(reserved_entry.get('sent')),
                'reason': 'duplicate_idempotency_key',
                'send_gate': send_gate,
                'receipt': reserved_entry,
                'receipt_store_path': None if self.receipt_store.path is None else str(self.receipt_store.path),
                'failure': None,
                'provider_response': reserved_entry.get('provider_response_excerpt') or reserved_entry.get('response_payload'),
            }

        attempt_count = 0
        while attempt_count < self.retry_limit:
            attempt_count += 1
            try:
                response_payload = self._transport_send(payload, dispatch)
                receipt = self._build_receipt(
                    payload=payload,
                    dispatch=dispatch,
                    status='sent',
                    sent=True,
                    reason=None,
                    attempt_count=attempt_count,
                    retryable=False,
                    failure=None,
                    response_payload=response_payload,
                )
                self.ledger.record(idempotency_key, receipt)
                receipt_path = self._record_receipt(receipt)
                return {
                    'dispatch': receipt.dispatch,
                    'sent': True,
                    'reason': None,
                    'send_gate': send_gate,
                    'receipt': asdict(receipt),
                    'receipt_store_path': receipt_path,
                    'failure': None,
                    'provider_response': self._provider_response_excerpt(response_payload),
                }
            except Exception as exc:
                failure = self.classify_failure(exc)
                receipt = self._build_receipt(
                    payload=payload,
                    dispatch=dispatch,
                    status='failed_retryable' if failure.retryable and attempt_count < self.retry_limit else 'failed',
                    sent=False,
                    reason=failure.message,
                    attempt_count=attempt_count,
                    retryable=failure.retryable and attempt_count < self.retry_limit,
                    failure=failure,
                    response_payload=None,
                )
                self.ledger.record(idempotency_key, receipt)
                receipt_path = self._record_receipt(receipt)
                if not failure.retryable or attempt_count >= self.retry_limit:
                    return {
                        'dispatch': receipt.dispatch,
                        'sent': False,
                        'reason': failure.message,
                        'send_gate': send_gate,
                        'receipt': asdict(receipt),
                        'receipt_store_path': receipt_path,
                        'failure': asdict(failure),
                        'provider_response': None,
                    }
        receipt = self._build_receipt(
            payload=payload,
            dispatch=dispatch,
            status='failed',
            sent=False,
            reason='retry_limit_exhausted',
            attempt_count=attempt_count,
            retryable=False,
            failure=DiscordSendFailure('retry_exhausted', False, 'RETRY_EXHAUSTED', 'retry_limit_exhausted', {}),
            response_payload=None,
        )
        self.ledger.record((payload.metadata or {}).get('idempotency_key'), receipt)
        receipt_path = self._record_receipt(receipt)
        return {
            'dispatch': receipt.dispatch,
            'sent': False,
            'reason': 'retry_limit_exhausted',
            'send_gate': send_gate,
            'receipt': asdict(receipt),
            'receipt_store_path': receipt_path,
            'failure': {
                'category': 'retry_exhausted',
                'retryable': False,
                'code': 'RETRY_EXHAUSTED',
                'message': 'retry_limit_exhausted',
                'details': {},
            },
            'provider_response': None,
        }
