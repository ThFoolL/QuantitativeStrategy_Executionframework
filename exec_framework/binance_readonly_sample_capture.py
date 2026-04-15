from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import BinanceReadOnlyClient
from exec_framework.binance_readonly_probe import _mask_value
from exec_framework.runtime_env import load_binance_env

DEFAULT_ENV_PATH = Path('/root/.openclaw/workspace-mike/secrets/binance_api.env')
DEFAULT_OUT_DIR = Path('docs/deploy_v6c/samples/readonly_capture')


def _redact_text(value: Any) -> Any:
    if value in (None, ''):
        return value
    return _mask_value(str(value), head=3, tail=2)


def sanitize_order_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'order_id_masked': _redact_text(row.get('orderId') or row.get('order_id')),
        'client_order_id_masked': _redact_text(row.get('clientOrderId') or row.get('client_order_id')),
        'symbol': row.get('symbol'),
        'status': row.get('status'),
        'type': row.get('type'),
        'side': row.get('side'),
        'positionSide': row.get('positionSide') or row.get('position_side'),
        'origQty': row.get('origQty') or row.get('qty'),
        'executedQty': row.get('executedQty') or row.get('executed_qty'),
        'price': row.get('price'),
        'avgPrice': row.get('avgPrice') or row.get('avg_price'),
        'reduceOnly': row.get('reduceOnly') if 'reduceOnly' in row else row.get('reduce_only'),
        'closePosition': row.get('closePosition') if 'closePosition' in row else row.get('close_position'),
        'updateTime': row.get('updateTime') or row.get('update_time_ms'),
    }


def sanitize_trade_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'trade_id_masked': _redact_text(row.get('id') or row.get('trade_id')),
        'order_id_masked': _redact_text(row.get('orderId') or row.get('order_id')),
        'client_order_id_masked': _redact_text(row.get('clientOrderId') or row.get('client_order_id')),
        'symbol': row.get('symbol'),
        'side': row.get('side'),
        'positionSide': row.get('positionSide') or row.get('position_side'),
        'qty': row.get('qty'),
        'price': row.get('price'),
        'commission': row.get('commission') or row.get('fee'),
        'commissionAsset': row.get('commissionAsset') or row.get('fee_asset'),
        'realizedPnl': row.get('realizedPnl') or row.get('realized_pnl'),
        'maker': row.get('maker'),
        'buyer': row.get('buyer'),
        'time': row.get('time') or row.get('time_ms'),
    }


def sanitize_position_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'symbol': row.get('symbol'),
        'positionSide': row.get('positionSide') or row.get('position_side'),
        'positionAmt': row.get('positionAmt') or row.get('qty'),
        'entryPrice': row.get('entryPrice') or row.get('entry_price'),
        'breakEvenPrice': row.get('breakEvenPrice') or row.get('break_even_price'),
        'markPrice': row.get('markPrice') or row.get('mark_price'),
        'unRealizedProfit': row.get('unRealizedProfit') or row.get('unrealized_pnl'),
        'marginType': row.get('marginType') or row.get('margin_type'),
        'leverage': row.get('leverage'),
        'isolatedMargin': row.get('isolatedMargin'),
        'liquidationPrice': row.get('liquidationPrice'),
        'updateTime': row.get('updateTime') or row.get('update_time_ms'),
    }


sanitize_open_order_row = sanitize_order_row


def _build_order_lookup_meta(
    *,
    requested_order_id: str | None,
    requested_client_order_id: str | None,
    resolved_order_id: str | None,
    resolved_client_order_id: str | None,
    source: str | None,
    inference_trade: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        'requested': {
            'order_id_masked': _redact_text(requested_order_id),
            'client_order_id_masked': _redact_text(requested_client_order_id),
        },
        'resolved': {
            'order_id_masked': _redact_text(resolved_order_id),
            'client_order_id_masked': _redact_text(resolved_client_order_id),
            'source': source,
        },
        'inference': None if inference_trade is None else {
            'method': 'recent_trades_first_row',
            'trade_id_masked': _redact_text(inference_trade.get('id') or inference_trade.get('trade_id')),
            'trade_time': inference_trade.get('time') or inference_trade.get('time_ms'),
            'trade_order_id_masked': _redact_text(inference_trade.get('orderId') or inference_trade.get('order_id')),
            'trade_client_order_id_masked': _redact_text(inference_trade.get('clientOrderId') or inference_trade.get('client_order_id')),
        },
    }


def build_sample_pack(
    *,
    symbol: str,
    source_label: str,
    order: dict[str, Any] | None,
    user_trades: list[dict[str, Any]],
    position_risk: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        'collection_meta': {
            'readonly_only': True,
            'source_label': source_label,
            'symbol': symbol,
            'sensitive_fields_removed': [
                'orderId', 'clientOrderId', 'id', 'api_key', 'api_secret'
            ],
            **(meta or {}),
        },
        'order': None if order is None else sanitize_order_row(order),
        'user_trades': [sanitize_trade_row(row) for row in user_trades],
        'position_risk': [sanitize_position_row(row) for row in position_risk],
        'open_orders': [sanitize_open_order_row(row) for row in open_orders],
    }


def _resolve_order_inputs(
    *,
    explicit_order_id: str | None,
    explicit_client_order_id: str | None,
    recent_trades: list[dict[str, Any]],
) -> tuple[str | None, str | None, str | None, dict[str, Any] | None]:
    if explicit_order_id and explicit_client_order_id:
        raise ValueError('order_id 与 client_order_id 不能同时传入')
    if explicit_order_id:
        return explicit_order_id, None, 'explicit_order_id', None
    if explicit_client_order_id:
        return None, explicit_client_order_id, 'explicit_client_order_id', None
    for trade in recent_trades:
        order_id = trade.get('orderId') or trade.get('order_id')
        client_order_id = trade.get('clientOrderId') or trade.get('client_order_id')
        if order_id:
            return str(order_id), None, 'recent_trades_inferred_order_id', trade
        if client_order_id:
            return None, str(client_order_id), 'recent_trades_inferred_client_order_id', trade
    return None, None, None, None


def collect_live_readonly_pack(*, env_file: Path, symbol: str, order_id: str | None, client_order_id: str | None, trades_limit: int) -> dict[str, Any]:
    config = load_binance_env(env_file)
    if symbol:
        config = type(config)(**{**config.__dict__, 'symbol': symbol})
    client = BinanceReadOnlyClient(config=config)

    recent_trade_probe_rows = [dict(getattr(item, 'raw', {}) or {}) for item in client.get_recent_trades(config.symbol, limit=trades_limit)]
    resolved_order_id, resolved_client_order_id, resolved_source, inference_trade = _resolve_order_inputs(
        explicit_order_id=order_id,
        explicit_client_order_id=client_order_id,
        recent_trades=recent_trade_probe_rows,
    )

    order = None
    if resolved_order_id or resolved_client_order_id:
        snap = client.get_order(symbol=config.symbol, order_id=resolved_order_id, client_order_id=resolved_client_order_id)
        order = dict(getattr(snap, 'raw', {}) or {})

    if resolved_order_id:
        trades = [dict(getattr(item, 'raw', {}) or {}) for item in client.get_recent_trades(config.symbol, limit=trades_limit, order_id=resolved_order_id)]
        trades_source = 'user_trades_by_order_id'
    else:
        trades = list(recent_trade_probe_rows)
        trades_source = 'recent_trades_window'

    positions = [dict(item) for item in client.get_position_risk_rows(config.symbol)]
    open_orders = [dict(getattr(item, 'raw', {}) or {}) for item in client.get_open_orders(config.symbol)]

    meta = {
        'capture_method': 'binance_readonly_sample_capture_v2',
        'order_lookup': _build_order_lookup_meta(
            requested_order_id=order_id,
            requested_client_order_id=client_order_id,
            resolved_order_id=resolved_order_id,
            resolved_client_order_id=resolved_client_order_id,
            source=resolved_source,
            inference_trade=inference_trade,
        ),
        'user_trades_source': {
            'mode': trades_source,
            'requested_limit': trades_limit,
            'returned_count': len(trades),
            'probe_window_count': len(recent_trade_probe_rows),
        },
    }
    return build_sample_pack(
        symbol=config.symbol,
        source_label='live_readonly_capture',
        order=order,
        user_trades=trades,
        position_risk=positions,
        open_orders=open_orders,
        meta=meta,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description='Binance 只读脱敏样本采集脚本（默认只输出脱敏 pack）')
    parser.add_argument('--env-file', default=str(DEFAULT_ENV_PATH), help='env 文件路径')
    parser.add_argument('--symbol', default='ETHUSDT', help='交易对')
    parser.add_argument('--order-id', default=None, help='可选：指定 orderId 采集单笔 order')
    parser.add_argument('--client-order-id', default=None, help='可选：指定 clientOrderId 采集单笔 order')
    parser.add_argument('--trades-limit', type=int, default=20, help='userTrades 查询条数')
    parser.add_argument('--out', default=str(DEFAULT_OUT_DIR / 'readonly_sample_pack.template.json'), help='输出文件路径')
    args = parser.parse_args()

    pack = collect_live_readonly_pack(
        env_file=Path(args.env_file),
        symbol=args.symbol,
        order_id=args.order_id,
        client_order_id=args.client_order_id,
        trades_limit=max(1, int(args.trades_limit)),
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(pack, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'ok': True, 'out': str(out_path), 'symbol': pack['collection_meta']['symbol'], 'order_lookup_source': pack['collection_meta'].get('order_lookup', {}).get('resolved', {}).get('source')}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
