from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

ACTION_AUTO_REPAIR = 'auto_repair'
ACTION_READONLY_RECHECK = 'readonly_recheck'
ACTION_RETRY = 'retry'
ACTION_FREEZE_AND_ALERT = 'freeze_and_alert'

ALERT_NONE = 'none'
ALERT_ON_EXHAUSTED = 'on_exhausted_retry_or_unresolved'
ALERT_IMMEDIATE = 'immediate'


@dataclass(frozen=True)
class BinanceExceptionAction:
    scope: str
    source_key: str
    action: str
    alert: str
    should_freeze_runtime: bool
    retryable: bool
    auto_repair_steps: list[str] = field(default_factory=list)
    readonly_checks: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_ERROR_CODE_POLICIES: dict[int, BinanceExceptionAction] = {
    -1003: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1003',
        action=ACTION_RETRY,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=True,
        notes=['触发请求频率限制，优先退避重试，避免立即 freeze。'],
    ),
    -1006: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1006',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'userTrades', 'positionRisk', 'openOrders'],
        notes=['官方说明 execution status unknown，先补查真实成交事实。'],
    ),
    -1007: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1007',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'userTrades', 'positionRisk', 'openOrders'],
        notes=['官方说明 send status unknown / execution status unknown，不能直接重试或直接 freeze。'],
    ),
    -1008: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1008',
        action=ACTION_RETRY,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=True,
        notes=['系统级限流，优先退避重试。'],
    ),
    -1015: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1015',
        action=ACTION_RETRY,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=True,
        notes=['下单频率超限，优先退避重试。'],
    ),
    -1021: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1021',
        action=ACTION_AUTO_REPAIR,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=True,
        auto_repair_steps=['sync_server_time', 'refresh_timestamp', 'retry_once'],
        notes=['时间戳偏移属于可自动修复问题。'],
    ),
    -1022: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1022',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['签名错误属于凭证/实现问题，不应自动重试。'],
    ),
    -1099: BinanceExceptionAction(
        scope='submit_error',
        source_key='-1099',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['未认证/未授权，属于环境或权限错误。'],
    ),
    -1125: BinanceExceptionAction(
        scope='stream_error',
        source_key='-1125',
        action=ACTION_AUTO_REPAIR,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=True,
        auto_repair_steps=['recreate_listen_key', 'reconnect_user_data_stream'],
        notes=['`-1125 INVALID_LISTEN_KEY` 属于 user data stream listenKey 失效，不应直接投射为 submit retry。'],
    ),
    -2011: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2011',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'openOrders', 'positionRisk'],
        notes=['撤单拒绝常见于订单已终态或订单不存在，先只读补查。'],
    ),
    -2013: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2013',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'openOrders', 'positionRisk'],
        notes=['订单不存在时需先确认是否已成交/已撤。'],
    ),
    -2014: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2014',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['API Key 格式错误。'],
    ),
    -2015: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2015',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['API Key / IP / permission 错误。'],
    ),
    -2017: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2017',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['API key 被锁，无法自动恢复。'],
    ),
    -2018: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2018',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['余额不足，继续重试会重复失败。'],
    ),
    -2019: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2019',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['保证金不足，属于资金/风控问题。'],
    ),
    -2020: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2020',
        action=ACTION_RETRY,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=True,
        notes=['Unable to fill 更适合限次重试，不宜第一时间 freeze。'],
    ),
    -2021: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2021',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['参数触发条件错误，当前实现不应自动重试。'],
    ),
    -2022: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2022',
        action=ACTION_AUTO_REPAIR,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=True,
        auto_repair_steps=['query_open_orders', 'cancel_conflicting_reduce_only_orders', 'reconcile_position', 'retry_once'],
        readonly_checks=['openOrders', 'positionRisk'],
        notes=['官方明确提示与现有 open orders 冲突，应先撤冲突单再重提 reduce-only。'],
    ),
    -2023: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2023',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['清算状态，必须冻结并人工接管。'],
    ),
    -2024: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2024',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['仓位不足，需先核对实际仓位与残单。'],
    ),
    -2025: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2025',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['open order 上限命中，通常说明残单堆积或风控缺口。'],
    ),
    -2026: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2026',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['reduceOnly + order type 不支持，属于实现/参数错误。'],
    ),
    -2027: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2027',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['杠杆/名义超限，需调整 sizing 或 leverage。'],
    ),
    -2028: BinanceExceptionAction(
        scope='submit_error',
        source_key='-2028',
        action=ACTION_FREEZE_AND_ALERT,
        alert=ALERT_IMMEDIATE,
        should_freeze_runtime=True,
        retryable=False,
        notes=['杠杆不足或保证金不足，不应盲重试。'],
    ),
}


_ORDER_STATUS_POLICIES: dict[str, BinanceExceptionAction] = {
    'NEW': BinanceExceptionAction(
        scope='order_status',
        source_key='NEW',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'openOrders', 'positionRisk'],
        notes=['已进入交易所但未终态，先补查，不直接判死。'],
    ),
    'PARTIALLY_FILLED': BinanceExceptionAction(
        scope='order_status',
        source_key='PARTIALLY_FILLED',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'userTrades', 'positionRisk', 'openOrders'],
        notes=['先确认部分成交、残单、持仓三件事，再决定是否冻结。'],
    ),
    'FILLED': BinanceExceptionAction(
        scope='order_status',
        source_key='FILLED',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['userTrades', 'positionRisk', 'openOrders'],
        notes=['FILLED 仍需和成交/持仓/残单三向对齐后才可确认。'],
    ),
    'CANCELED': BinanceExceptionAction(
        scope='order_status',
        source_key='CANCELED',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['userTrades', 'positionRisk', 'openOrders'],
        notes=['取消不等于零风险，需排除“部分成交后取消”。'],
    ),
    'REJECTED': BinanceExceptionAction(
        scope='order_status',
        source_key='REJECTED',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['positionRisk', 'openOrders'],
        notes=['拒单后先确认是否确实无仓无残单。'],
    ),
    'EXPIRED': BinanceExceptionAction(
        scope='order_status',
        source_key='EXPIRED',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['userTrades', 'positionRisk', 'openOrders'],
        notes=['过期需排查是否带成交残留。'],
    ),
    'EXPIRED_IN_MATCH': BinanceExceptionAction(
        scope='order_status',
        source_key='EXPIRED_IN_MATCH',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'userTrades', 'positionRisk', 'openOrders'],
        notes=['撮合中失效可能伴随部分成交，不能直接按纯拒单处理。'],
    ),
}


def classify_binance_error_code(code: int | None, msg: str | None = None) -> BinanceExceptionAction:
    if code in _ERROR_CODE_POLICIES:
        return _ERROR_CODE_POLICIES[int(code)]

    if code is not None and -1100 >= int(code) >= -1136:
        return BinanceExceptionAction(
            scope='submit_error',
            source_key=str(code),
            action=ACTION_FREEZE_AND_ALERT,
            alert=ALERT_IMMEDIATE,
            should_freeze_runtime=True,
            retryable=False,
            notes=['11xx 参数错误：优先视为代码/配置问题。', f'msg={msg}' if msg else ''],
        )

    if code is not None and -4000 >= int(code) >= -4999:
        return BinanceExceptionAction(
            scope='submit_error',
            source_key=str(code),
            action=ACTION_FREEZE_AND_ALERT,
            alert=ALERT_IMMEDIATE,
            should_freeze_runtime=True,
            retryable=False,
            notes=['40xx filter/交易规则错误：优先视为参数或规则越界。', f'msg={msg}' if msg else ''],
        )

    return BinanceExceptionAction(
        scope='submit_error',
        source_key=str(code) if code is not None else 'unknown',
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'userTrades', 'positionRisk', 'openOrders'],
        notes=['未知错误码：默认先补查交易所事实，避免误判。', f'msg={msg}' if msg else ''],
    )


def classify_binance_order_status(status: str | None) -> BinanceExceptionAction:
    normalized = str(status or 'UNKNOWN').upper()
    if normalized in _ORDER_STATUS_POLICIES:
        return _ORDER_STATUS_POLICIES[normalized]
    return BinanceExceptionAction(
        scope='order_status',
        source_key=normalized,
        action=ACTION_READONLY_RECHECK,
        alert=ALERT_ON_EXHAUSTED,
        should_freeze_runtime=False,
        retryable=False,
        readonly_checks=['order', 'userTrades', 'positionRisk', 'openOrders'],
        notes=['未知订单状态：先补查真实事实。'],
    )


def classify_submit_exception_detail(detail: dict[str, Any] | None) -> BinanceExceptionAction:
    payload = dict(detail or {})
    raw_payload = payload.get('payload') if isinstance(payload.get('payload'), dict) else payload
    code = raw_payload.get('code') if isinstance(raw_payload, dict) else None
    msg = raw_payload.get('msg') if isinstance(raw_payload, dict) else None
    try:
        code_int = int(code) if code is not None else None
    except (TypeError, ValueError):
        code_int = None
    return classify_binance_error_code(code_int, msg)
