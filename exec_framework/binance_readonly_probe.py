from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import BinanceReadOnlyClient
from exec_framework.runtime_env import load_binance_env

DEFAULT_ENV_PATH = Path('/root/.openclaw/workspace-mike/secrets/binance_api.env')


def _mask_value(value: str | None, *, head: int = 3, tail: int = 2) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) <= head + tail:
        return '*' * len(text)
    return f'{text[:head]}***{text[-tail:]}'


def _to_plain(value: Any) -> Any:
    if is_dataclass(value):
        return {k: _to_plain(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    return value


def _classify_error(exc: Exception) -> dict[str, Any]:
    raw = str(exc)
    parsed: dict[str, Any] | None = None
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict):
            parsed = payload
    except json.JSONDecodeError:
        parsed = None

    if parsed:
        kind = parsed.get('kind')
        status = parsed.get('status')
        endpoint_payload = parsed.get('payload') if isinstance(parsed.get('payload'), dict) else {}
        code = endpoint_payload.get('code')
        msg = endpoint_payload.get('msg') or endpoint_payload.get('message') or parsed.get('reason')
        if kind == 'network_error':
            category = 'network'
        elif kind == 'http_error' and (status in (401, 403) or code in (-2014, -2015)):
            category = 'auth'
        elif kind == 'http_error' and status == 404:
            category = 'endpoint'
        elif kind == 'http_error':
            category = 'endpoint'
        else:
            category = 'unknown'
        return {
            'category': category,
            'kind': kind or 'exception',
            'status': status,
            'code': code,
            'message': msg or raw,
        }

    return {
        'category': 'field_assumption' if isinstance(exc, ValueError) else 'unknown',
        'kind': exc.__class__.__name__,
        'status': None,
        'code': None,
        'message': raw,
    }


def _probe_call(name: str, fn, *, payload_builder=None) -> dict[str, Any]:
    try:
        result = fn()
        summary = payload_builder(result) if payload_builder else {}
        return {
            'name': name,
            'ok': True,
            'summary': _to_plain(summary),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            'name': name,
            'ok': False,
            'error': _classify_error(exc),
        }


def _summarize_account(snapshot: Any) -> dict[str, Any]:
    raw = getattr(snapshot, 'raw', {}) or {}
    assets = raw.get('assets') if isinstance(raw, dict) else []
    positions = raw.get('positions') if isinstance(raw, dict) else []
    nonzero_assets = 0
    for asset in assets or []:
        wallet = float(asset.get('walletBalance', 0.0))
        available = float(asset.get('availableBalance', 0.0))
        if abs(wallet) > 0 or abs(available) > 0:
            nonzero_assets += 1
    nonzero_positions = 0
    for row in positions or []:
        if abs(float(row.get('positionAmt', 0.0))) > 0:
            nonzero_positions += 1
    return {
        'account_equity': snapshot.account_equity,
        'available_margin': snapshot.available_margin,
        'assets_count': len(assets or []),
        'positions_count': len(positions or []),
        'nonzero_asset_count': nonzero_assets,
        'nonzero_position_count': nonzero_positions,
        'can_trade': raw.get('canTrade'),
        'multi_assets_margin': raw.get('multiAssetsMargin'),
        'validity_status': getattr(snapshot, 'validity_status', None),
        'invalid_reasons': list(getattr(snapshot, 'invalid_reasons', ()) or []),
        'account_snapshot_sources': raw.get('_account_snapshot_sources'),
        'key_fields': {
            'totalWalletBalance': raw.get('totalWalletBalance'),
            'availableBalance': raw.get('availableBalance'),
            'totalMarginBalance': raw.get('totalMarginBalance'),
            'totalCrossWalletBalance': raw.get('totalCrossWalletBalance'),
            'totalAvailableBalance': raw.get('totalAvailableBalance'),
            'maxWithdrawAmount': raw.get('maxWithdrawAmount'),
        },
    }


def _summarize_position_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    position_sides = sorted({str(row.get('positionSide', '')).upper() for row in rows if row.get('positionSide')})
    nonzero_rows = []
    for row in rows:
        amt = float(row.get('positionAmt', 0.0))
        if abs(amt) > 0:
            nonzero_rows.append(
                {
                    'symbol': row.get('symbol'),
                    'positionSide': row.get('positionSide'),
                    'qty_abs': abs(amt),
                    'entryPrice': row.get('entryPrice'),
                    'breakEvenPrice': row.get('breakEvenPrice'),
                    'markPrice': row.get('markPrice'),
                    'unRealizedProfit': row.get('unRealizedProfit'),
                    'marginType': row.get('marginType'),
                    'leverage': row.get('leverage'),
                }
            )
    if {'LONG', 'SHORT'} & set(position_sides):
        inferred_mode = 'hedge'
    elif 'BOTH' in position_sides:
        inferred_mode = 'one_way'
    else:
        inferred_mode = 'unknown'
    return {
        'rows_count': len(rows),
        'position_sides': position_sides,
        'nonzero_rows_count': len(nonzero_rows),
        'nonzero_rows': nonzero_rows[:4],
        'inferred_mode_from_positionRisk': inferred_mode,
        'multiple_nonzero_rows': len(nonzero_rows) > 1,
    }


def _summarize_orders(rows: list[Any]) -> dict[str, Any]:
    return {
        'open_order_count': len(rows),
        'has_open_orders': bool(rows),
        'orders': [
            {
                'order_id': item.order_id,
                'status': item.status,
                'type': item.type,
                'side': item.side,
                'position_side': item.position_side,
                'qty': item.qty,
                'executed_qty': item.executed_qty,
                'reduce_only': item.reduce_only,
                'close_position': item.close_position,
                'update_time_ms': item.update_time_ms,
            }
            for item in rows[:5]
        ],
    }


def _summarize_trades(rows: list[Any]) -> dict[str, Any]:
    fee_assets = sorted({item.fee_asset for item in rows if getattr(item, 'fee_asset', None)})
    return {
        'trade_count': len(rows),
        'has_recent_trades': bool(rows),
        'fee_assets': fee_assets,
        'recent_trades': [
            {
                'trade_id': item.trade_id,
                'order_id': item.order_id,
                'client_order_id_masked': _mask_value(item.client_order_id),
                'side': item.side,
                'position_side': item.position_side,
                'qty': item.qty,
                'price': item.price,
                'realized_pnl': item.realized_pnl,
                'fee': item.fee,
                'fee_asset': item.fee_asset,
                'maker': item.maker,
                'time_ms': item.time_ms,
            }
            for item in rows[:10]
        ],
    }


def _resolve_order_probe_inputs(
    *,
    explicit_order_id: str | None,
    explicit_client_order_id: str | None,
    recent_trades: list[Any],
) -> tuple[str | None, str | None, str | None]:
    if explicit_order_id and explicit_client_order_id:
        raise ValueError('order_id 与 client_order_id 不能同时传入')
    if explicit_order_id:
        return explicit_order_id, None, 'explicit_order_id'
    if explicit_client_order_id:
        return None, explicit_client_order_id, 'explicit_client_order_id'
    if recent_trades:
        first = recent_trades[0]
        if getattr(first, 'order_id', None):
            return str(first.order_id), None, 'recent_trade_order_id'
        if getattr(first, 'client_order_id', None):
            return None, str(first.client_order_id), 'recent_trade_client_order_id'
    return None, None, None


def run_probe(env_path: Path, *, symbol: str | None = None, order_id: str | None = None, client_order_id: str | None = None, trades_limit: int = 10) -> dict[str, Any]:
    config = load_binance_env(env_path)
    if symbol:
        config = type(config)(**{**config.__dict__, 'symbol': symbol})
    client = BinanceReadOnlyClient(config=config)

    results: dict[str, Any] = {
        'probe': 'binance_readonly',
        'readonly_only': True,
        'submit_enabled_config': bool(config.submit_enabled),
        'dry_run_config': bool(config.dry_run),
        'base_url': config.base_url,
        'symbol': config.symbol,
        'credential_presence': {
            'api_key_present': bool(config.api_key),
            'api_secret_present': bool(config.api_secret),
            'api_key_masked': _mask_value(config.api_key),
            'api_secret_masked': _mask_value(config.api_secret),
        },
        'endpoints': {},
    }

    results['endpoints']['server_time'] = _probe_call(
        'server_time',
        lambda: client.request_with_meta('/fapi/v1/time', signed=False),
        payload_builder=lambda item: {'http_status': item[0], 'server_time_ms': item[2].get('serverTime')},
    )
    results['endpoints']['exchange_info'] = _probe_call(
        'exchange_info',
        lambda: client.get_exchange_info(config.symbol),
        payload_builder=lambda item: {
            'symbol': item.symbol,
            'price_tick': item.price_tick,
            'qty_step': item.qty_step,
            'min_qty': item.min_qty,
            'min_notional': item.min_notional,
        },
    )
    results['endpoints']['account'] = _probe_call('account', client.get_account_snapshot, payload_builder=_summarize_account)
    results['endpoints']['position_mode'] = _probe_call('position_mode', client.get_position_mode, payload_builder=lambda item: {'dualSidePosition': item.get('dualSidePosition')})
    results['endpoints']['position_risk'] = _probe_call(
        'position_risk',
        lambda: client.get_position_risk_rows(config.symbol),
        payload_builder=_summarize_position_rows,
    )
    results['endpoints']['open_orders'] = _probe_call(
        'open_orders',
        lambda: client.get_open_orders(config.symbol),
        payload_builder=_summarize_orders,
    )
    results['endpoints']['user_trades'] = _probe_call(
        'user_trades',
        lambda: client.get_recent_trades(config.symbol, limit=trades_limit),
        payload_builder=_summarize_trades,
    )

    recent_trade_rows: list[Any] = []
    if results['endpoints']['user_trades'].get('ok', False):
        recent_trade_rows = client.get_recent_trades(config.symbol, limit=trades_limit)

    resolved_order_id, resolved_client_order_id, order_probe_source = _resolve_order_probe_inputs(
        explicit_order_id=order_id,
        explicit_client_order_id=client_order_id,
        recent_trades=recent_trade_rows,
    )

    if resolved_order_id or resolved_client_order_id:
        results['endpoints']['order'] = _probe_call(
            'order',
            lambda: client.get_order(symbol=config.symbol, order_id=resolved_order_id, client_order_id=resolved_client_order_id),
            payload_builder=lambda item: {
                'probe_source': order_probe_source,
                'order_id': item.order_id,
                'client_order_id_masked': _mask_value(item.client_order_id),
                'status': item.status,
                'type': item.type,
                'side': item.side,
                'position_side': item.position_side,
                'qty': item.qty,
                'executed_qty': item.executed_qty,
                'price': item.price,
                'avg_price': item.avg_price,
                'reduce_only': item.reduce_only,
                'close_position': item.close_position,
            },
        )
    else:
        results['endpoints']['order'] = {
            'name': 'order',
            'ok': False,
            'skipped': True,
            'error': {
                'category': 'missing_input',
                'message': '未提供 order_id 或 client_order_id，且 recent trades 中无可安全复用的订单线索，跳过 /fapi/v1/order 探测',
            },
        }

    server_ok = results['endpoints']['server_time'].get('ok', False)
    account_ok = results['endpoints']['account'].get('ok', False)
    pos_mode_ok = results['endpoints']['position_mode'].get('ok', False)
    pos_risk_ok = results['endpoints']['position_risk'].get('ok', False)
    open_orders_ok = results['endpoints']['open_orders'].get('ok', False)
    user_trades_ok = results['endpoints']['user_trades'].get('ok', False)

    inferred_mode = None
    if pos_mode_ok:
        dual = results['endpoints']['position_mode']['summary'].get('dualSidePosition')
        inferred_mode = 'hedge' if dual is True else 'one_way' if dual is False else None
    elif pos_risk_ok:
        inferred_mode = results['endpoints']['position_risk']['summary'].get('inferred_mode_from_positionRisk')

    results['summary'] = {
        'connectivity_ok': server_ok,
        'server_time_ms': results['endpoints']['server_time'].get('summary', {}).get('server_time_ms') if server_ok else None,
        'account_readable': account_ok,
        'position_mode_readable': pos_mode_ok,
        'position_risk_readable': pos_risk_ok,
        'open_orders_readable': open_orders_ok,
        'user_trades_readable': user_trades_ok,
        'order_readable': results['endpoints']['order'].get('ok', False),
        'account_mode': inferred_mode,
        'has_positions': (results['endpoints']['position_risk'].get('summary', {}).get('nonzero_rows_count', 0) > 0) if pos_risk_ok else None,
        'has_open_orders': results['endpoints']['open_orders'].get('summary', {}).get('has_open_orders') if open_orders_ok else None,
        'failure_categories': {
            name: item.get('error', {}).get('category')
            for name, item in results['endpoints'].items()
            if not item.get('ok', False) and not item.get('skipped', False)
        },
        'field_assumption_flags': {
            'positionRisk_multiple_nonzero_rows': results['endpoints']['position_risk'].get('summary', {}).get('multiple_nonzero_rows') if pos_risk_ok else None,
            'positionRisk_position_sides': results['endpoints']['position_risk'].get('summary', {}).get('position_sides') if pos_risk_ok else None,
            'userTrades_fee_assets': results['endpoints']['user_trades'].get('summary', {}).get('fee_assets') if user_trades_ok else None,
        },
    }
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description='Binance 只读探测脚本（脱敏输出）')
    parser.add_argument('--env-file', default=str(DEFAULT_ENV_PATH), help='env 文件路径')
    parser.add_argument('--symbol', default=None, help='交易对，默认沿用 env')
    parser.add_argument('--order-id', default=None, help='仅只读查询指定 order')
    parser.add_argument('--client-order-id', default=None, help='仅只读查询指定 client order')
    parser.add_argument('--trades-limit', type=int, default=10, help='userTrades 查询条数')
    args = parser.parse_args()

    result = run_probe(
        Path(args.env_file),
        symbol=args.symbol,
        order_id=args.order_id,
        client_order_id=args.client_order_id,
        trades_limit=args.trades_limit,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
