from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


LIVE_SUBMIT_MANUAL_ACK_TOKEN = 'I_ACK_SMALL_CAPITAL_REAL_SUBMIT_CHECKLIST'
DEFAULT_BINANCE_SYMBOL = 'BTCUSDT'
DEFAULT_SUBMIT_MAX_NOTIONAL = 10_000_000.0
DEFAULT_DISCORD_TRANSPORT = 'unconfigured'
DEFAULT_DISCORD_RECEIPT_LOG_PATH = 'runtime/discord_send_receipts.jsonl'
DEFAULT_DISCORD_CHANNEL_PLACEHOLDER = 'DISCORD_CHANNEL_ID_PLACEHOLDER'


@dataclass(frozen=True)
class BinanceEnvConfig:
    api_key: str
    api_secret: str
    base_url: str = 'https://fapi.binance.com'
    symbol: str = DEFAULT_BINANCE_SYMBOL
    submit_enabled: bool = False
    dry_run: bool = True
    submit_unlock_token: str | None = None
    state_path: str = 'runtime/state.json'
    discord_channel_id: str | None = None
    discord_bot_token: str | None = None
    discord_execution_channel_id: str | None = None
    submit_symbol_allowlist: tuple[str, ...] = ()
    submit_max_qty: float | None = None
    submit_max_notional: float | None = None
    submit_require_reconcile_ok: bool = True
    submit_require_active_runtime: bool = True
    submit_require_no_pending_execution: bool = True
    discord_audit_enabled: bool = False
    discord_real_send_enabled: bool = False
    discord_message_tool_enabled: bool = False
    discord_send_require_idempotency: bool = True
    discord_send_ledger_path: str | None = None
    discord_send_receipt_log_path: str | None = DEFAULT_DISCORD_RECEIPT_LOG_PATH
    discord_send_retry_limit: int = 3
    discord_transport: str = DEFAULT_DISCORD_TRANSPORT
    discord_rehearsal_real_send_enabled: bool = False
    submit_manual_ack_token: str | None = None


REQUIRED_BINANCE_ENV_KEYS = (
    'BINANCE_API_KEY',
    'BINANCE_API_SECRET',
)


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {'1', 'true', 'yes', 'on'}:
        return True
    if value in {'0', 'false', 'no', 'off'}:
        return False
    return default


def _clean_str(raw: str | None, default: str) -> str:
    if raw is None:
        return default
    value = raw.strip()
    return value or default


def _parse_optional_str(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _parse_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    return float(value)


def _parse_csv_upper(raw: str | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    values = [item.strip().upper() for item in raw.split(',') if item.strip()]
    return tuple(values)


def _load_env_file(env_path: str | Path) -> None:
    path = Path(env_path)
    if not path.exists():
        raise FileNotFoundError(f'env file not found: {path}')

    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_binance_env(env_path: str | Path | None = None) -> BinanceEnvConfig:
    if env_path is not None:
        _load_env_file(env_path)

    missing = [key for key in REQUIRED_BINANCE_ENV_KEYS if not os.environ.get(key)]
    if missing:
        raise ValueError(f'missing required env keys: {", ".join(missing)}')

    return BinanceEnvConfig(
        api_key=os.environ['BINANCE_API_KEY'],
        api_secret=os.environ['BINANCE_API_SECRET'],
        base_url=_clean_str(os.environ.get('BINANCE_BASE_URL'), 'https://fapi.binance.com'),
        symbol=_clean_str(os.environ.get('BINANCE_SYMBOL'), DEFAULT_BINANCE_SYMBOL),
        submit_enabled=_parse_bool(os.environ.get('BINANCE_SUBMIT_ENABLED'), False),
        dry_run=_parse_bool(os.environ.get('BINANCE_DRY_RUN'), True),
        submit_unlock_token=os.environ.get('BINANCE_SUBMIT_UNLOCK_TOKEN'),
        state_path=_clean_str(os.environ.get('LIVE_STATE_PATH'), 'runtime/state.json'),
        discord_channel_id=os.environ.get('DISCORD_MONITOR_CHANNEL_ID'),
        discord_bot_token=os.environ.get('DISCORD_BOT_TOKEN'),
        discord_execution_channel_id=_clean_str(
            os.environ.get('DISCORD_EXECUTION_CHANNEL_ID'),
            _clean_str(os.environ.get('DISCORD_MONITOR_CHANNEL_ID'), DEFAULT_DISCORD_CHANNEL_PLACEHOLDER),
        ),
        submit_symbol_allowlist=_parse_csv_upper(os.environ.get('BINANCE_SUBMIT_SYMBOL_ALLOWLIST')),
        submit_max_qty=_parse_float(os.environ.get('BINANCE_SUBMIT_MAX_QTY')),
        submit_max_notional=_parse_float(os.environ.get('BINANCE_SUBMIT_MAX_NOTIONAL')) or DEFAULT_SUBMIT_MAX_NOTIONAL,
        submit_require_reconcile_ok=_parse_bool(os.environ.get('BINANCE_SUBMIT_REQUIRE_RECONCILE_OK'), True),
        submit_require_active_runtime=_parse_bool(os.environ.get('BINANCE_SUBMIT_REQUIRE_ACTIVE_RUNTIME'), True),
        submit_require_no_pending_execution=_parse_bool(os.environ.get('BINANCE_SUBMIT_REQUIRE_NO_PENDING_EXECUTION'), True),
        discord_audit_enabled=_parse_bool(os.environ.get('DISCORD_AUDIT_ENABLED'), False),
        discord_real_send_enabled=_parse_bool(os.environ.get('DISCORD_REAL_SEND_ENABLED'), False),
        discord_message_tool_enabled=_parse_bool(os.environ.get('DISCORD_MESSAGE_TOOL_ENABLED'), False),
        discord_send_require_idempotency=_parse_bool(os.environ.get('DISCORD_SEND_REQUIRE_IDEMPOTENCY'), True),
        discord_send_ledger_path=os.environ.get('DISCORD_SEND_LEDGER_PATH'),
        discord_send_receipt_log_path=_parse_optional_str(os.environ.get('DISCORD_SEND_RECEIPT_LOG_PATH')) or DEFAULT_DISCORD_RECEIPT_LOG_PATH,
        discord_send_retry_limit=int(_clean_str(os.environ.get('DISCORD_SEND_RETRY_LIMIT'), '3')),
        discord_transport=_clean_str(os.environ.get('DISCORD_TRANSPORT'), DEFAULT_DISCORD_TRANSPORT),
        discord_rehearsal_real_send_enabled=_parse_bool(os.environ.get('DISCORD_REHEARSAL_REAL_SEND_ENABLED'), False),
        submit_manual_ack_token=os.environ.get('BINANCE_SUBMIT_MANUAL_ACK_TOKEN'),
    )


def summarize_env_presence(keys: Iterable[str]) -> dict[str, bool]:
    return {key: bool(os.environ.get(key)) for key in keys}


def binance_env_presence_summary() -> dict[str, bool]:
    return summarize_env_presence(
        [
            'BINANCE_API_KEY',
            'BINANCE_API_SECRET',
            'BINANCE_BASE_URL',
            'BINANCE_SYMBOL',
            'BINANCE_SUBMIT_ENABLED',
            'BINANCE_DRY_RUN',
            'BINANCE_SUBMIT_UNLOCK_TOKEN',
            'LIVE_STATE_PATH',
            'DISCORD_MONITOR_CHANNEL_ID',
            'DISCORD_BOT_TOKEN',
            'DISCORD_EXECUTION_CHANNEL_ID',
            'BINANCE_SUBMIT_SYMBOL_ALLOWLIST',
            'BINANCE_SUBMIT_MAX_QTY',
            'BINANCE_SUBMIT_MAX_NOTIONAL',
            'BINANCE_SUBMIT_REQUIRE_RECONCILE_OK',
            'BINANCE_SUBMIT_REQUIRE_ACTIVE_RUNTIME',
            'BINANCE_SUBMIT_REQUIRE_NO_PENDING_EXECUTION',
            'DISCORD_AUDIT_ENABLED',
            'DISCORD_REAL_SEND_ENABLED',
            'DISCORD_MESSAGE_TOOL_ENABLED',
            'DISCORD_SEND_REQUIRE_IDEMPOTENCY',
            'DISCORD_SEND_LEDGER_PATH',
            'DISCORD_SEND_RECEIPT_LOG_PATH',
            'DISCORD_SEND_RETRY_LIMIT',
            'DISCORD_TRANSPORT',
            'DISCORD_REHEARSAL_REAL_SEND_ENABLED',
            'BINANCE_SUBMIT_MANUAL_ACK_TOKEN',
        ]
    )
