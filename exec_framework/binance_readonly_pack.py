from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PACK_CONFIRMATION_HINTS = {
    'FILLED': 'confirmed',
    'PARTIALLY_FILLED': 'pending',
    'NEW': 'pending',
    'PENDING_CANCEL': 'pending',
    'ACCEPTED': 'pending',
    'CALCULATED': 'pending',
    'CANCELED': 'rejected',
    'REJECTED': 'rejected',
    'EXPIRED': 'rejected',
    'EXPIRED_IN_MATCH': 'rejected',
}

MASKED_ID_KEYS = {
    'order_id_masked',
    'client_order_id_masked',
    'trade_id_masked',
}

FORBIDDEN_SECRET_KEYS = {
    'api_key',
    'api_secret',
    'listenKey',
    'listen_key',
    'secret',
}

REQUIRED_TOP_LEVEL_KEYS = {
    'collection_meta',
    'order',
    'user_trades',
    'position_risk',
    'open_orders',
}


def _safe_float(value: Any) -> float | None:
    if value in (None, '', 'NULL'):
        return None
    return float(value)


def _coerce_bool(value: Any) -> bool | None:
    if value in (None, '', 'NULL'):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {'true', '1', 'yes'}:
        return True
    if text in {'false', '0', 'no'}:
        return False
    return None


def _looks_masked(value: Any) -> bool:
    text = str(value or '')
    return '***' in text and len(text) >= 5


def _normalize_side(value: Any) -> str | None:
    if value in (None, '', 'NULL'):
        return None
    text = str(value).upper()
    if text == 'BUY':
        return 'buy'
    if text == 'SELL':
        return 'sell'
    if text == 'LONG':
        return 'long'
    if text == 'SHORT':
        return 'short'
    if text == 'BOTH':
        return 'both'
    return str(value).lower()


def _derive_pack_confirmation_hint(
    *,
    order_status: Any,
    order_executed_qty: Any,
    order_orig_qty: Any,
    user_trades_count: int,
    open_orders_count: int,
    position_qty: Any,
    reduce_only: Any,
) -> str | None:
    status = str(order_status or '').upper()
    direct = PACK_CONFIRMATION_HINTS.get(status)
    executed_qty = _safe_float(order_executed_qty) or 0.0
    orig_qty = _safe_float(order_orig_qty) or 0.0
    position_abs_qty = abs(_safe_float(position_qty) or 0.0)
    reduce_only_flag = bool(_coerce_bool(reduce_only))
    qty_tolerance = 1e-9
    if status == 'FILLED' and orig_qty > 0 and executed_qty + qty_tolerance < orig_qty:
        return 'mismatch'
    if status == 'FILLED' and user_trades_count == 0:
        return 'query_failed'
    if status == 'FILLED' and open_orders_count > 0:
        return 'mismatch'
    if status == 'FILLED' and reduce_only_flag and position_abs_qty > qty_tolerance:
        return 'mismatch'
    if status == 'FILLED' and (not reduce_only_flag) and position_abs_qty - executed_qty > qty_tolerance:
        return 'mismatch'
    if status == 'UNKNOWN' or not status:
        if user_trades_count == 0 and open_orders_count == 0:
            return 'query_failed'
        return 'mismatch'
    if direct == 'rejected' and executed_qty > qty_tolerance:
        return 'mismatch'
    if direct == 'pending' and open_orders_count == 0 and executed_qty <= 0 and user_trades_count == 0:
        return 'query_failed'
    return direct


def _derive_position_side_mode(rows: list[dict[str, Any]]) -> str | None:
    sides = {str((row or {}).get('positionSide') or '').upper() for row in rows if row}
    if 'LONG' in sides or 'SHORT' in sides:
        return 'hedge'
    if 'BOTH' in sides:
        return 'one_way'
    return None


def _derive_position_snapshot(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            'side': None,
            'qty': 0.0,
            'entry_price': None,
            'position_side_mode': None,
        }

    selected = rows[0]
    non_zero = []
    for row in rows:
        amt = _safe_float((row or {}).get('positionAmt'))
        if amt is not None and abs(amt) > 0:
            non_zero.append(row)
    if len(non_zero) == 1:
        selected = non_zero[0]
    elif len(non_zero) > 1:
        # 真实 pack 若包含双向非零持仓，这里不擅自聚合，交给人工演练处理。
        selected = non_zero[0]

    amt = _safe_float(selected.get('positionAmt')) or 0.0
    explicit_side = _normalize_side(selected.get('positionSide'))
    side = None
    if amt > 0:
        side = 'long'
    elif amt < 0:
        side = 'short'
    elif explicit_side in {'long', 'short'}:
        side = explicit_side

    return {
        'side': side,
        'qty': abs(amt),
        'entry_price': _safe_float(selected.get('entryPrice')),
        'position_side_mode': _derive_position_side_mode(rows),
    }


def validate_readonly_pack(pack: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    meta = pack.get('collection_meta') or {}

    missing_top_level = sorted(REQUIRED_TOP_LEVEL_KEYS - set(pack.keys()))
    if missing_top_level:
        errors.append(f'missing_top_level_keys:{",".join(missing_top_level)}')

    if meta.get('readonly_only') is not True:
        errors.append('collection_meta.readonly_only_must_be_true')

    sensitive_removed = meta.get('sensitive_fields_removed') or []
    for key in ('orderId', 'clientOrderId', 'id', 'api_key', 'api_secret'):
        if key not in sensitive_removed:
            warnings.append(f'sensitive_fields_removed_missing_hint:{key}')

    order = pack.get('order')
    if order is not None:
        for key in ('order_id_masked', 'client_order_id_masked'):
            if order.get(key) and not _looks_masked(order.get(key)):
                errors.append(f'order.{key}_not_masked')
        for key in ('status', 'side', 'origQty', 'executedQty'):
            if key not in order:
                errors.append(f'order.missing:{key}')
        for forbidden in ('orderId', 'clientOrderId'):
            if forbidden in order:
                errors.append(f'order.forbidden_raw_key:{forbidden}')

    user_trades = pack.get('user_trades')
    if not isinstance(user_trades, list):
        errors.append('user_trades.must_be_list')
        user_trades = []
    for idx, row in enumerate(user_trades):
        if not isinstance(row, dict):
            errors.append(f'user_trades[{idx}].must_be_object')
            continue
        if row.get('trade_id_masked') and not _looks_masked(row.get('trade_id_masked')):
            errors.append(f'user_trades[{idx}].trade_id_masked_not_masked')
        if row.get('order_id_masked') and not _looks_masked(row.get('order_id_masked')):
            errors.append(f'user_trades[{idx}].order_id_masked_not_masked')
        for key in ('qty', 'price', 'side', 'time'):
            if key not in row:
                errors.append(f'user_trades[{idx}].missing:{key}')
        for forbidden in ('id', 'orderId', 'clientOrderId'):
            if forbidden in row:
                errors.append(f'user_trades[{idx}].forbidden_raw_key:{forbidden}')

    position_risk = pack.get('position_risk')
    if not isinstance(position_risk, list):
        errors.append('position_risk.must_be_list')
        position_risk = []
    if not position_risk:
        errors.append('position_risk.empty')
    for idx, row in enumerate(position_risk):
        if not isinstance(row, dict):
            errors.append(f'position_risk[{idx}].must_be_object')
            continue
        for key in ('symbol', 'positionSide', 'positionAmt'):
            if key not in row:
                errors.append(f'position_risk[{idx}].missing:{key}')

    open_orders = pack.get('open_orders')
    if not isinstance(open_orders, list):
        errors.append('open_orders.must_be_list')
        open_orders = []
    for idx, row in enumerate(open_orders):
        if not isinstance(row, dict):
            errors.append(f'open_orders[{idx}].must_be_object')
            continue
        for key in ('order_id_masked', 'client_order_id_masked'):
            if row.get(key) and not _looks_masked(row.get(key)):
                errors.append(f'open_orders[{idx}].{key}_not_masked')
        for forbidden in ('orderId', 'clientOrderId'):
            if forbidden in row:
                errors.append(f'open_orders[{idx}].forbidden_raw_key:{forbidden}')

    for forbidden in FORBIDDEN_SECRET_KEYS:
        if _contains_key(pack, forbidden):
            errors.append(f'forbidden_secret_key_present:{forbidden}')

    order_present = isinstance(order, dict)
    can_drive_posttrade = order_present and isinstance(user_trades, list) and isinstance(position_risk, list) and isinstance(open_orders, list) and bool(position_risk)
    if can_drive_posttrade and 'status' not in (order or {}):
        can_drive_posttrade = False
    if can_drive_posttrade and 'side' not in (order or {}):
        can_drive_posttrade = False

    can_drive_operator = can_drive_posttrade and any('positionAmt' in (row or {}) for row in position_risk)
    if not user_trades:
        warnings.append('user_trades.empty')
    if not open_orders:
        warnings.append('open_orders.empty')

    return {
        'ok': not errors,
        'errors': errors,
        'warnings': warnings,
        'ready_for_posttrade_fixture': can_drive_posttrade and not errors,
        'ready_for_operator_drill': can_drive_operator and not errors,
        'facts_summary': {
            'symbol': meta.get('symbol') or (order or {}).get('symbol'),
            'order_status': (order or {}).get('status') if isinstance(order, dict) else None,
            'user_trades_count': len(user_trades),
            'position_risk_count': len(position_risk),
            'open_orders_count': len(open_orders),
        },
    }


def _contains_key(value: Any, needle: str) -> bool:
    if isinstance(value, dict):
        if needle in value:
            return True
        return any(_contains_key(child, needle) for child in value.values())
    if isinstance(value, list):
        return any(_contains_key(item, needle) for item in value)
    return False


def adapt_readonly_pack(pack: dict[str, Any], *, scenario_name: str | None = None) -> dict[str, Any]:
    validation = validate_readonly_pack(pack)
    order = dict(pack.get('order') or {})
    trades = [dict(row) for row in (pack.get('user_trades') or [])]
    position_risk = [dict(row) for row in (pack.get('position_risk') or [])]
    open_orders = [dict(row) for row in (pack.get('open_orders') or [])]
    symbol = (pack.get('collection_meta') or {}).get('symbol') or order.get('symbol') or 'UNKNOWN'
    scenario_key = scenario_name or f'{symbol.lower()}_readonly_pack'

    request = {
        'symbol': symbol,
        'side': str(order.get('side') or 'BUY').upper(),
        'quantity': _safe_float(order.get('origQty')),
        'reduce_only': bool(_coerce_bool(order.get('reduceOnly'))),
        'client_order_id': order.get('client_order_id_masked') or f'{scenario_key}-masked-client-order-id',
    }
    receipt = {
        'client_order_id': request['client_order_id'],
        'exchange_order_id': order.get('order_id_masked'),
        'acknowledged': True,
    }

    scenario = {
        'name': scenario_key,
        'request': request,
        'receipt': receipt,
        'order': {
            'status': order.get('status'),
            'order_id': order.get('order_id_masked'),
            'executed_qty': _safe_float(order.get('executedQty')),
            'qty': _safe_float(order.get('origQty')),
            'avg_price': _safe_float(order.get('avgPrice')),
            'reduce_only': _coerce_bool(order.get('reduceOnly')),
            'side': _normalize_side(order.get('side')),
            'position_side': _normalize_side(order.get('positionSide')),
            'update_time_ms': order.get('updateTime'),
        },
        'trades': [
            {
                'id': row.get('trade_id_masked'),
                'orderId': row.get('order_id_masked'),
                'clientOrderId': row.get('client_order_id_masked'),
                'symbol': row.get('symbol') or symbol,
                'positionSide': row.get('positionSide'),
                'qty': row.get('qty'),
                'price': row.get('price'),
                'commission': row.get('commission'),
                'commissionAsset': row.get('commissionAsset'),
                'realizedPnl': row.get('realizedPnl'),
                'side': row.get('side'),
                'maker': row.get('maker'),
                'buyer': row.get('buyer'),
                'time': row.get('time'),
            }
            for row in trades
        ],
        'position': _derive_position_snapshot(position_risk),
        'position_risk_rows': position_risk,
        'open_orders': [
            {
                'orderId': row.get('order_id_masked'),
                'clientOrderId': row.get('client_order_id_masked'),
                'status': row.get('status'),
                'type': row.get('type'),
                'timeInForce': row.get('timeInForce'),
                'side': row.get('side'),
                'positionSide': row.get('positionSide'),
                'origQty': row.get('origQty'),
                'executedQty': row.get('executedQty'),
                'price': row.get('price'),
                'avgPrice': row.get('avgPrice'),
                'cumQuote': row.get('cumQuote'),
                'reduceOnly': row.get('reduceOnly'),
                'closePosition': row.get('closePosition'),
                'updateTime': row.get('updateTime'),
            }
            for row in open_orders
        ],
    }

    pack_confirmation_hint = _derive_pack_confirmation_hint(
        order_status=scenario['order']['status'],
        order_executed_qty=scenario['order']['executed_qty'],
        order_orig_qty=scenario['order']['qty'],
        user_trades_count=len(trades),
        open_orders_count=len(open_orders),
        position_qty=scenario['position']['qty'],
        reduce_only=scenario['order']['reduce_only'],
    )

    operator_context = {
        'event_type': 'readonly_pack_import',
        'fact_sources': ['order', 'user_trades', 'position_risk', 'open_orders'],
        'confirmation_candidate': {
            'confirmation_category': None,
            'pack_confirmation_hint': pack_confirmation_hint,
            'confirmed_order_status': scenario['order']['status'],
            'requested_qty': request['quantity'],
            'executed_qty': scenario['order']['executed_qty'],
            'open_orders_count': len(open_orders),
        },
        'position_summary': scenario['position'],
        'freeze_recover_hint': {
            'should_review_freeze': scenario['order']['status'] not in {'FILLED'} or bool(open_orders),
            'should_review_recover': scenario['order']['status'] == 'FILLED' and not open_orders,
        },
    }

    return {
        'adapter_version': 'readonly_pack_to_fixture_v1',
        'source_pack_meta': pack.get('collection_meta') or {},
        'validation': validation,
        'posttrade_fixture': scenario,
        'operator_context': operator_context,
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def _dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='readonly pack adapter / validator')
    subparsers = parser.add_subparsers(dest='command', required=True)

    adapt_parser = subparsers.add_parser('adapt', help='把 readonly pack 转成 posttrade/operator 可消费 fixture')
    adapt_parser.add_argument('--in', dest='input_path', required=True, help='输入 pack 路径')
    adapt_parser.add_argument('--out', dest='output_path', required=True, help='输出 fixture 路径')
    adapt_parser.add_argument('--scenario-name', default=None, help='可选 scenario 名称')

    validate_parser = subparsers.add_parser('validate', help='校验 readonly pack 是否脱敏且可驱动演练')
    validate_parser.add_argument('--in', dest='input_path', required=True, help='输入 pack 路径')
    validate_parser.add_argument('--out', dest='output_path', default=None, help='可选：输出校验报告路径')

    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = _load_json(Path(args.input_path))
    if args.command == 'adapt':
        adapted = adapt_readonly_pack(payload, scenario_name=args.scenario_name)
        _dump_json(Path(args.output_path), adapted)
        print(json.dumps({'ok': True, 'out': args.output_path, 'ready': adapted['validation']['ok']}, ensure_ascii=False))
        return 0

    report = validate_readonly_pack(payload)
    if args.output_path:
        _dump_json(Path(args.output_path), report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
