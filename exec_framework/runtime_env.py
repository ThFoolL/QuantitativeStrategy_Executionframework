from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable


LIVE_SUBMIT_MANUAL_ACK_TOKEN = 'I_ACK_SMALL_CAPITAL_REAL_SUBMIT_CHECKLIST'
DEFAULT_BINANCE_SYMBOL = 'ETHUSDT'
DEFAULT_SUBMIT_MAX_NOTIONAL = 10_000_000.0
DEFAULT_DISCORD_TRANSPORT = 'unconfigured'
DEFAULT_DISCORD_RECEIPT_LOG_PATH = 'runtime/discord_send_receipts.jsonl'
LIVE_SUBMIT_UNLOCK_TOKEN = 'ENABLE_BINANCE_FUTURES_LIVE_SUBMIT'


@dataclass(frozen=True)
class BinanceEnvConfig:
    api_key: str
    api_secret: str
    base_url: str = 'https://fapi.binance.com'
    symbol: str = DEFAULT_BINANCE_SYMBOL
    strategy_adapter: str = 'la_free_v1'
    recv_window_ms: int = 10000
    submit_enabled: bool = False
    dry_run: bool = True
    submit_unlock_token: str | None = None
    submit_http_post_enabled: bool = False
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
    discord_execution_confirmation_real_send_enabled: bool = False
    posttrade_confirm_retry_enabled: bool = True
    posttrade_confirm_retry_attempts: int = 5
    posttrade_confirm_retry_interval_seconds: float = 3.0
    submit_manual_ack_token: str | None = None


@dataclass(frozen=True)
class RuntimeConfigValidation:
    ok: bool
    severity: str
    mode: str
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    facts: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


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


def _parse_int(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    value = raw.strip()
    if not value:
        return default
    return int(value)


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
        strategy_adapter=_clean_str(os.environ.get('LIVE_STRATEGY_ADAPTER'), 'la_free_v1'),
        recv_window_ms=_parse_int(os.environ.get('BINANCE_RECV_WINDOW_MS'), 10000),
        submit_enabled=_parse_bool(os.environ.get('BINANCE_SUBMIT_ENABLED'), False),
        dry_run=_parse_bool(os.environ.get('BINANCE_DRY_RUN'), True),
        submit_unlock_token=os.environ.get('BINANCE_SUBMIT_UNLOCK_TOKEN'),
        submit_http_post_enabled=_parse_bool(os.environ.get('BINANCE_SUBMIT_HTTP_POST_ENABLED'), False),
        state_path=_clean_str(os.environ.get('LIVE_STATE_PATH'), 'runtime/state.json'),
        discord_channel_id=os.environ.get('DISCORD_MONITOR_CHANNEL_ID'),
        discord_bot_token=os.environ.get('DISCORD_BOT_TOKEN'),
        discord_execution_channel_id=_clean_str(
            os.environ.get('DISCORD_EXECUTION_CHANNEL_ID'),
            _clean_str(os.environ.get('DISCORD_MONITOR_CHANNEL_ID'), '1486034825830727710'),
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
        discord_execution_confirmation_real_send_enabled=_parse_bool(os.environ.get('DISCORD_EXECUTION_CONFIRMATION_REAL_SEND_ENABLED'), False),
        posttrade_confirm_retry_enabled=_parse_bool(os.environ.get('POSTTRADE_CONFIRM_RETRY_ENABLED'), True),
        posttrade_confirm_retry_attempts=max(1, _parse_int(os.environ.get('POSTTRADE_CONFIRM_RETRY_ATTEMPTS'), 5)),
        posttrade_confirm_retry_interval_seconds=max(0.0, float(_clean_str(os.environ.get('POSTTRADE_CONFIRM_RETRY_INTERVAL_SECONDS'), '3'))),
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
            'LIVE_STRATEGY_ADAPTER',
            'BINANCE_RECV_WINDOW_MS',
            'BINANCE_SUBMIT_ENABLED',
            'BINANCE_DRY_RUN',
            'BINANCE_SUBMIT_UNLOCK_TOKEN',
            'BINANCE_SUBMIT_HTTP_POST_ENABLED',
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
            'DISCORD_EXECUTION_CONFIRMATION_REAL_SEND_ENABLED',
            'BINANCE_SUBMIT_MANUAL_ACK_TOKEN',
        ]
    )


def validate_runtime_config(config: BinanceEnvConfig) -> RuntimeConfigValidation:
    blockers: list[str] = []
    warnings: list[str] = []

    state_path = Path(config.state_path)
    runtime_dir = state_path.parent
    ledger_path = None if not config.discord_send_ledger_path else Path(config.discord_send_ledger_path)
    receipt_path = None if not config.discord_send_receipt_log_path else Path(config.discord_send_receipt_log_path)

    symbol_upper = config.symbol.upper()
    unlock_token_present = bool((config.submit_unlock_token or '').strip())
    unlock_token_valid = (config.submit_unlock_token or '').strip() == LIVE_SUBMIT_UNLOCK_TOKEN
    manual_ack_present = bool((config.submit_manual_ack_token or '').strip())
    manual_ack_valid = (config.submit_manual_ack_token or '').strip() == LIVE_SUBMIT_MANUAL_ACK_TOKEN
    execution_confirmation_gate_open = bool(config.discord_execution_confirmation_real_send_enabled)

    if symbol_upper != DEFAULT_BINANCE_SYMBOL:
        blockers.append(f'symbol_must_remain_{DEFAULT_BINANCE_SYMBOL}:got_{symbol_upper}')

    effective_allowlist = tuple(item.upper() for item in (config.submit_symbol_allowlist or (config.symbol,)))

    if config.submit_enabled:
        if config.dry_run:
            blockers.append('submit_enabled_but_dry_run_true')
        if not unlock_token_valid:
            blockers.append('submit_enabled_without_valid_unlock_token')
        if not config.submit_http_post_enabled:
            blockers.append('submit_enabled_without_http_post_enable_flag')
        if not manual_ack_valid:
            blockers.append('submit_enabled_without_valid_manual_ack_token')
        if not config.discord_audit_enabled:
            blockers.append('submit_enabled_requires_discord_audit_enabled')
        if symbol_upper not in effective_allowlist:
            blockers.append('submit_enabled_requires_symbol_in_allowlist')
        if config.submit_max_qty is None:
            warnings.append('submit_enabled_without_submit_max_qty')
        if config.submit_max_notional is None:
            warnings.append('submit_enabled_without_submit_max_notional')
    else:
        warnings.append('binance_live_submit_remains_closed')

    if config.discord_real_send_enabled:
        if not config.discord_message_tool_enabled:
            blockers.append('discord_real_send_enabled_but_message_tool_disabled')
        if execution_confirmation_gate_open and not config.submit_http_post_enabled:
            blockers.append('execution_confirmation_real_send_requires_submit_http_post_enabled')
        if not config.discord_execution_channel_id:
            blockers.append('discord_real_send_enabled_without_execution_channel_id')
        if not config.discord_send_ledger_path:
            blockers.append('discord_real_send_enabled_without_send_ledger_path')
        if not config.discord_send_receipt_log_path:
            blockers.append('discord_real_send_enabled_without_receipt_log_path')
        if config.discord_transport in {'', 'unconfigured', 'none', 'disabled'}:
            blockers.append('discord_real_send_enabled_without_transport')
        if config.discord_send_require_idempotency and not config.discord_send_ledger_path:
            blockers.append('discord_real_send_idempotency_requires_ledger')
        if not config.discord_rehearsal_real_send_enabled:
            warnings.append('discord_real_send_open_but_rehearsal_real_send_disabled')
    else:
        warnings.append('discord_real_send_remains_closed')

    if config.discord_rehearsal_real_send_enabled and not config.discord_real_send_enabled:
        blockers.append('discord_rehearsal_real_send_enabled_requires_discord_real_send_enabled')

    if execution_confirmation_gate_open and not config.discord_real_send_enabled:
        blockers.append('execution_confirmation_real_send_requires_discord_real_send_enabled')
    if execution_confirmation_gate_open and not config.discord_message_tool_enabled:
        blockers.append('execution_confirmation_real_send_requires_message_tool_enabled')

    if ledger_path is not None and not ledger_path.is_absolute():
        ledger_path = runtime_dir.parent / ledger_path
    if receipt_path is not None and not receipt_path.is_absolute():
        receipt_path = runtime_dir.parent / receipt_path

    gate_status = {
        'binance_submit_env_open': bool(config.submit_enabled),
        'binance_submit_http_post_enabled': bool(config.submit_http_post_enabled),
        'binance_submit_unlock_token_present': unlock_token_present,
        'binance_submit_unlock_token_valid': unlock_token_valid,
        'binance_submit_manual_ack_present': manual_ack_present,
        'binance_submit_manual_ack_valid': manual_ack_valid,
        'binance_submit_ready': bool(
            config.submit_enabled
            and not config.dry_run
            and config.submit_http_post_enabled
            and unlock_token_valid
            and manual_ack_valid
            and config.discord_audit_enabled
            and symbol_upper in effective_allowlist
        ),
        'discord_real_send_env_open': bool(config.discord_real_send_enabled),
        'discord_rehearsal_real_send_env_open': bool(config.discord_rehearsal_real_send_enabled),
        'discord_execution_confirmation_real_send_env_open': execution_confirmation_gate_open,
        'discord_message_tool_ready': bool(config.discord_message_tool_enabled),
        'discord_transport_ready': config.discord_transport not in {'', 'unconfigured', 'none', 'disabled'},
        'discord_rehearsal_ready': bool(
            config.discord_real_send_enabled
            and config.discord_message_tool_enabled
            and config.discord_rehearsal_real_send_enabled
            and config.discord_transport not in {'', 'unconfigured', 'none', 'disabled'}
            and bool(config.discord_execution_channel_id)
            and bool(config.discord_send_receipt_log_path)
            and bool(config.discord_send_ledger_path)
        ),
        'discord_execution_confirmation_ready': bool(
            execution_confirmation_gate_open
            and config.discord_real_send_enabled
            and config.discord_message_tool_enabled
            and config.discord_transport not in {'', 'unconfigured', 'none', 'disabled'}
            and bool(config.discord_execution_channel_id)
            and bool(config.discord_send_receipt_log_path)
            and bool(config.discord_send_ledger_path)
            and config.submit_http_post_enabled
        ),
    }
    facts = {
        'symbol': symbol_upper,
        'recv_window_ms': config.recv_window_ms,
        'dry_run': config.dry_run,
        'submit_enabled': config.submit_enabled,
        'submit_http_post_enabled': config.submit_http_post_enabled,
        'discord_real_send_enabled': config.discord_real_send_enabled,
        'discord_message_tool_enabled': config.discord_message_tool_enabled,
        'discord_rehearsal_real_send_enabled': config.discord_rehearsal_real_send_enabled,
        'discord_execution_confirmation_real_send_enabled': execution_confirmation_gate_open,
        'discord_transport': config.discord_transport,
        'state_path': str(state_path),
        'runtime_dir': str(runtime_dir),
        'discord_send_ledger_path': None if ledger_path is None else str(ledger_path),
        'discord_send_receipt_log_path': None if receipt_path is None else str(receipt_path),
        'submit_symbol_allowlist': list(config.submit_symbol_allowlist),
        'effective_symbol_allowlist': list(effective_allowlist),
        'submit_max_qty': config.submit_max_qty,
        'submit_max_notional': config.submit_max_notional,
        'manual_ack_present': manual_ack_present,
        'manual_ack_valid': manual_ack_valid,
        'unlock_token_present': unlock_token_present,
        'unlock_token_valid': unlock_token_valid,
        'execution_confirmation_real_send_enabled': execution_confirmation_gate_open,
        'gate_status': gate_status,
        'operator_open_summary': {
            'binance_submit': 'ready_to_open_by_env_only' if gate_status['binance_submit_ready'] else 'closed_or_blocked',
            'discord_rehearsal_send': 'ready_to_open_by_env_only' if gate_status['discord_rehearsal_ready'] else 'closed_or_blocked',
            'discord_execution_confirmation_send': 'ready_to_open_by_env_only' if gate_status['discord_execution_confirmation_ready'] else 'closed_or_blocked',
        },
        'default_safety': {
            'real_submit_default': False,
            'execution_confirmation_real_send_default': False,
        },
    }

    severity = 'ok'
    if blockers:
        severity = 'blocked'
    elif warnings:
        severity = 'warning'

    if config.submit_enabled:
        mode = 'submit_candidate'
    elif config.discord_real_send_enabled:
        mode = 'discord_candidate'
    else:
        mode = 'dry_run_only'

    return RuntimeConfigValidation(
        ok=not blockers,
        severity=severity,
        mode=mode,
        blockers=blockers,
        warnings=warnings,
        facts=facts,
    )
