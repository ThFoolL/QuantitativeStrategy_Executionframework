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
    from .binance_submit import BinanceSignedSubmitClient
    from .binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
    from .market_data import BinanceReadOnlyMarketDataProvider
    from .runtime_env import load_binance_env
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from exec_framework.binance_readonly import BinanceReadOnlyClient
    from exec_framework.binance_submit import BinanceSignedSubmitClient
    from exec_framework.binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
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
    parser = argparse.ArgumentParser(description='最小真钱验证 reduce-only filled but position not flat mismatch 样本')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--side', choices=['long', 'short'], default='short')
    parser.add_argument('--target-notional', type=float, default=25.0)
    parser.add_argument('--close-ratio', type=float, default=0.9)
    parser.add_argument('--sleep-after-open', type=float, default=1.0)
    parser.add_argument('--sleep-after-close', type=float, default=1.0)
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    run_id = _run_id()
    out_path = Path(args.out) if args.out else Path('docs/deploy_v6c/samples/manual_reduce_only_not_flat') / run_id / f'{run_id}_{args.symbol}_reduce_only_not_flat_summary.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)

    config = load_binance_env(Path(args.env_file))
    readonly_client = BinanceReadOnlyClient(config)
    submit_client = BinanceSignedSubmitClient(config, allow_live_submit_call=True)
    market_provider = BinanceReadOnlyMarketDataProvider(readonly_client)
    market_bundle = market_provider.load(symbol=args.symbol, decision_time=_utc_now())
    rules = readonly_client.get_exchange_info(args.symbol)

    price = float(market_bundle.current_price)
    open_qty = _normalize_quantity(raw_qty=float(args.target_notional) / price, qty_step=rules.qty_step, min_qty=rules.min_qty)
    open_qty = _bump_quantity_to_min_notional(quantity=open_qty, price=price, qty_step=rules.qty_step, min_notional=rules.min_notional)
    close_qty = _normalize_quantity(raw_qty=open_qty * float(args.close_ratio), qty_step=rules.qty_step, min_qty=rules.min_qty)
    if close_qty >= open_qty:
        close_qty = round(max(open_qty - (rules.qty_step or 0.001), rules.min_qty or 0.001), 8)

    entry_side = 'BUY' if args.side == 'long' else 'SELL'
    exit_side = 'SELL' if args.side == 'long' else 'BUY'

    result: dict[str, Any] = {
        'run_id': run_id,
        'symbol': args.symbol,
        'mode': 'reduce_only_not_flat_probe',
        'requested': {
            'side': args.side,
            'target_notional': args.target_notional,
            'open_qty': open_qty,
            'close_qty': close_qty,
            'close_ratio': args.close_ratio,
            'market_price': price,
        },
    }

    result['before'] = _ensure_flat(readonly_client, args.symbol)
    if not result['before']['is_flat'] or result['before']['open_orders_count'] != 0:
        result['aborted'] = True
        result['abort_reason'] = 'precheck_not_flat_or_has_open_orders'
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    cleanup: dict[str, Any] = {}
    try:
        open_payload = {
            'symbol': args.symbol,
            'side': entry_side,
            'type': 'MARKET',
            'newClientOrderId': f'{run_id}-open',
            'quantity': open_qty,
        }
        open_request = submit_client.build_submit_request(open_payload)
        open_response, open_receipt = submit_client.submit_order(open_request)
        result['open_submit'] = {'response': asdict(open_response), 'receipt': asdict(open_receipt)}

        time.sleep(args.sleep_after_open)
        result['after_open'] = _ensure_flat(readonly_client, args.symbol)

        close_payload = {
            'symbol': args.symbol,
            'side': exit_side,
            'type': 'MARKET',
            'newClientOrderId': f'{run_id}-reduce-close',
            'quantity': close_qty,
            'reduceOnly': 'true',
        }
        close_request = submit_client.build_submit_request(close_payload)
        close_response, close_receipt = submit_client.submit_order(close_request)
        result['reduce_close_submit'] = {'response': asdict(close_response), 'receipt': asdict(close_receipt)}

        time.sleep(args.sleep_after_close)
        confirmer = BinancePostTradeConfirmer(readonly_client)
        confirmation = confirmer.confirm(
            market=type('Market', (), {'symbol': args.symbol})(),
            order_requests=[
                type('Req', (), {
                    'symbol': args.symbol,
                    'side': exit_side,
                    'order_type': 'MARKET',
                    'quantity': close_qty,
                    'reduce_only': True,
                    'position_side': None,
                    'client_order_id': close_receipt.client_order_id,
                    'metadata': {},
                })()
            ],
            simulated_receipts=[
                SimulatedExecutionReceipt(
                    client_order_id=close_receipt.client_order_id,
                    exchange_order_id=close_receipt.exchange_order_id,
                    acknowledged=True,
                    submitted_qty=close_qty,
                    submitted_side=exit_side,
                    submit_status='ACKNOWLEDGED',
                )
            ],
        )
        result['confirmation'] = {
            'confirmation_status': confirmation.confirmation_status,
            'confirmation_category': confirmation.confirmation_category,
            'reconcile_status': confirmation.reconcile_status,
            'should_freeze': confirmation.should_freeze,
            'freeze_reason': confirmation.freeze_reason,
            'notes': list(confirmation.notes or []),
            'fill_count': confirmation.fill_count,
            'executed_qty': confirmation.executed_qty,
            'post_position_side': confirmation.post_position_side,
            'post_position_qty': confirmation.post_position_qty,
            'avg_fill_price': confirmation.avg_fill_price,
        }
        result['after_reduce_close'] = _ensure_flat(readonly_client, args.symbol)
        result['ok'] = bool(
            'reduce_only_filled_but_position_not_flat' in (confirmation.notes or [])
            and float(confirmation.post_position_qty or 0.0) > 0.0
            and str(confirmation.post_position_side or '').lower() == args.side
        )
    finally:
        try:
            position = readonly_client.get_position_snapshot(args.symbol)
            if abs(float(position.qty or 0.0)) > 0.0:
                final_close_payload = {
                    'symbol': args.symbol,
                    'side': exit_side,
                    'type': 'MARKET',
                    'newClientOrderId': f'{run_id}-cleanup-close',
                    'quantity': abs(float(position.qty or 0.0)),
                    'reduceOnly': 'true',
                }
                final_close_request = submit_client.build_submit_request(final_close_payload)
                final_close_response, final_close_receipt = submit_client.submit_order(final_close_request)
                cleanup['cleanup_close'] = {'response': asdict(final_close_response), 'receipt': asdict(final_close_receipt)}
                time.sleep(2.0)
        except Exception as exc:  # noqa: BLE001
            cleanup['cleanup_close_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        result['cleanup'] = cleanup
        result['after_cleanup'] = _ensure_flat(readonly_client, args.symbol)

    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'ok': result.get('ok', False), 'summary_path': str(out_path)}, ensure_ascii=False))
    return 0 if result.get('ok', False) else 1


if __name__ == '__main__':
    raise SystemExit(main())
