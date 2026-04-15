from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .binance_readonly import BinanceReadOnlyClient
    from .binance_readonly_sample_capture import collect_live_readonly_pack
    from .binance_submit import BinanceSignedSubmitClient
    from .market_data import BinanceReadOnlyMarketDataProvider
    from .runtime_env import load_binance_env
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from exec_framework.binance_readonly import BinanceReadOnlyClient
    from exec_framework.binance_readonly_sample_capture import collect_live_readonly_pack
    from exec_framework.binance_submit import BinanceSignedSubmitClient
    from exec_framework.market_data import BinanceReadOnlyMarketDataProvider
    from exec_framework.runtime_env import load_binance_env


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run_id() -> str:
    return _utc_now().strftime('%Y%m%dT%H%M%S%fZ')


def _normalize_quantity(*, raw_qty: float, qty_step: float | None, min_qty: float | None) -> float:
    qty = float(raw_qty)
    if qty_step and qty_step > 0:
        steps = int(qty / qty_step)
        qty = steps * qty_step
    if min_qty and qty < min_qty:
        qty = min_qty
    return round(qty, 8)


def _bump_quantity_to_min_notional(*, quantity: float, price: float, qty_step: float | None, min_notional: float | None) -> float:
    qty = float(quantity)
    if min_notional is None or price <= 0:
        return round(qty, 8)
    target = float(min_notional)
    if qty * price >= target:
        return round(qty, 8)
    if qty_step and qty_step > 0:
        while qty * price < target:
            qty = round(qty + qty_step, 8)
    else:
        qty = round(target / price, 8)
    return round(qty, 8)


def _ensure_flat(client: BinanceReadOnlyClient, symbol: str) -> dict[str, Any]:
    position = client.get_position_snapshot(symbol)
    open_orders = client.get_open_orders(symbol)
    return {
        'position': asdict(position),
        'open_orders': [item.raw for item in open_orders],
        'is_flat': abs(float(position.qty or 0.0)) <= 0.0,
        'open_orders_count': len(open_orders),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='最小真钱 query mismatch 样本探针')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--side', choices=['long', 'short'], default='short')
    parser.add_argument('--target-notional', type=float, default=25.0)
    parser.add_argument('--capture-delay-ms', type=int, default=150)
    parser.add_argument('--trades-limit', type=int, default=5)
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    run_id = _run_id()
    out_path = Path(args.out) if args.out else Path('docs/deploy_v6c/samples/manual_query_mismatch') / run_id / f'{run_id}_{args.symbol}_query_mismatch_summary.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)

    config = load_binance_env(Path(args.env_file))
    readonly_client = BinanceReadOnlyClient(config)
    submit_client = BinanceSignedSubmitClient(config, allow_live_submit_call=True)
    market_provider = BinanceReadOnlyMarketDataProvider(readonly_client)
    market_bundle = market_provider.load(symbol=args.symbol, decision_time=_utc_now())
    rules = readonly_client.get_exchange_info(args.symbol)

    price = float(market_bundle.current_price)
    qty = _normalize_quantity(raw_qty=float(args.target_notional) / price, qty_step=rules.qty_step, min_qty=rules.min_qty)
    qty = _bump_quantity_to_min_notional(quantity=qty, price=price, qty_step=rules.qty_step, min_notional=rules.min_notional)
    entry_side = 'BUY' if args.side == 'long' else 'SELL'
    exit_side = 'SELL' if args.side == 'long' else 'BUY'

    result: dict[str, Any] = {
        'run_id': run_id,
        'symbol': args.symbol,
        'mode': 'query_mismatch_probe',
        'requested': {
            'side': args.side,
            'target_notional': args.target_notional,
            'market_price': price,
            'quantity': qty,
            'capture_delay_ms': args.capture_delay_ms,
        },
    }

    result['before'] = _ensure_flat(readonly_client, args.symbol)
    if not result['before']['is_flat'] or result['before']['open_orders_count'] != 0:
        result['aborted'] = True
        result['abort_reason'] = 'precheck_not_flat_or_has_open_orders'
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    open_payload = {
        'symbol': args.symbol,
        'side': entry_side,
        'type': 'MARKET',
        'newClientOrderId': f'{run_id}-open',
        'quantity': qty,
    }
    open_request = submit_client.build_submit_request(open_payload)
    open_response, open_receipt = submit_client.submit_order(open_request)
    result['open_submit'] = {'response': asdict(open_response), 'receipt': asdict(open_receipt)}

    cleanup: dict[str, Any] = {}
    try:
        time.sleep(max(args.capture_delay_ms, 0) / 1000.0)
        pack = collect_live_readonly_pack(
            env_file=Path(args.env_file),
            symbol=args.symbol,
            order_id=open_receipt.exchange_order_id,
            client_order_id=None,
            trades_limit=args.trades_limit,
        )
        result['capture'] = pack

        order_row = pack.get('order') or {}
        trades = list(pack.get('user_trades') or [])
        positions = list(pack.get('position_risk') or [])
        open_orders = list(pack.get('open_orders') or [])

        has_order = bool(order_row)
        order_status = str(order_row.get('status') or '')
        has_trades = len(trades) > 0
        nonzero_positions = [row for row in positions if abs(float(row.get('positionAmt') or 0.0)) > 0.0]
        result['derived'] = {
            'has_order': has_order,
            'order_status': order_status,
            'has_trades': has_trades,
            'nonzero_position_count': len(nonzero_positions),
            'open_orders_count': len(open_orders),
        }
        result['ok'] = bool(has_order and order_status in {'NEW', 'FILLED', 'PARTIALLY_FILLED'} and not has_trades)
    except Exception as exc:  # noqa: BLE001
        result['capture_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        result['ok'] = False
    finally:
        try:
            position = readonly_client.get_position_snapshot(args.symbol)
            if abs(float(position.qty or 0.0)) > 0.0:
                close_payload = {
                    'symbol': args.symbol,
                    'side': exit_side,
                    'type': 'MARKET',
                    'newClientOrderId': f'{run_id}-cleanup-close',
                    'quantity': qty,
                    'reduceOnly': 'true',
                }
                close_request = submit_client.build_submit_request(close_payload)
                close_response, close_receipt = submit_client.submit_order(close_request)
                cleanup['close_position'] = {'response': asdict(close_response), 'receipt': asdict(close_receipt)}
                time.sleep(2.0)
        except Exception as exc:  # noqa: BLE001
            cleanup['close_position_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        result['cleanup'] = cleanup
        result['after_cleanup'] = _ensure_flat(readonly_client, args.symbol)

    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'ok': result['ok'], 'summary_path': str(out_path)}, ensure_ascii=False))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
