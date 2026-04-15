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
    from .market_data import BinanceReadOnlyMarketDataProvider
    from .runtime_env import load_binance_env
    from .runtime_worker import _build_runtime_components
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from exec_framework.binance_readonly import BinanceReadOnlyClient
    from exec_framework.binance_submit import BinanceSignedSubmitClient
    from exec_framework.market_data import BinanceReadOnlyMarketDataProvider
    from exec_framework.runtime_env import load_binance_env
    from exec_framework.runtime_worker import _build_runtime_components


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


def _order_to_protective_dict(order) -> dict[str, Any]:
    raw = dict(order.raw)
    order_type = str(order.type or order.orig_type or raw.get('type') or raw.get('orderType') or '').upper()
    return {
        'kind': 'hard_stop' if order_type == 'STOP_MARKET' else 'take_profit',
        'type': order_type,
        'order_id': order.order_id,
        'client_order_id': order.client_order_id,
        'status': order.status,
        'side': order.side,
        'position_side': order.position_side,
        'qty': order.qty,
        'stop_price': order.stop_price,
        'close_position': order.close_position,
        'reduce_only': order.reduce_only,
        'raw': raw,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='最小真钱验证 flat 后 lingering protective cleanup')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--side', choices=['long', 'short'], default='short')
    parser.add_argument('--target-notional', type=float, default=25.0)
    parser.add_argument('--sleep-after-open', type=float, default=1.0)
    parser.add_argument('--sleep-after-protect', type=float, default=1.0)
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    run_id = _run_id()
    out_path = Path(args.out) if args.out else Path('docs/deploy_v6c/samples/manual_lingering_protective_cleanup') / run_id / f'{run_id}_{args.symbol}_lingering_protective_cleanup_summary.json'
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
    stop_trigger = round(price * (0.99 if args.side == 'long' else 1.01), 2)

    result: dict[str, Any] = {
        'run_id': run_id,
        'symbol': args.symbol,
        'mode': 'lingering_protective_cleanup_probe',
        'requested': {
            'side': args.side,
            'target_notional': args.target_notional,
            'quantity': qty,
            'market_price': price,
            'stop_trigger': stop_trigger,
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
        open_request = submit_client.build_submit_request({
            'symbol': args.symbol,
            'side': entry_side,
            'type': 'MARKET',
            'newClientOrderId': f'{run_id}-open',
            'quantity': qty,
        })
        open_response, open_receipt = submit_client.submit_order(open_request)
        result['open_submit'] = {'response': asdict(open_response), 'receipt': asdict(open_receipt)}
        time.sleep(args.sleep_after_open)

        stop_request = submit_client.build_submit_request(
            {
                'symbol': args.symbol,
                'side': exit_side,
                'type': 'STOP_MARKET',
                'clientAlgoId': f'{run_id}-linger-stop',
                'algoType': 'CONDITIONAL',
                'triggerPrice': stop_trigger,
                'closePosition': 'true',
                'workingType': 'MARK_PRICE',
                'priceProtect': 'FALSE',
            },
            metadata={'algo_order': True, 'protective_order': True},
        )
        stop_response, stop_receipt = submit_client.submit_order(stop_request)
        result['protect_submit'] = {'response': asdict(stop_response), 'receipt': asdict(stop_receipt)}
        time.sleep(args.sleep_after_protect)

        close_request = submit_client.build_submit_request({
            'symbol': args.symbol,
            'side': exit_side,
            'type': 'MARKET',
            'newClientOrderId': f'{run_id}-manual-close',
            'quantity': qty,
            'reduceOnly': 'true',
        })
        close_response, close_receipt = submit_client.submit_order(close_request)
        result['manual_close'] = {'response': asdict(close_response), 'receipt': asdict(close_receipt)}
        time.sleep(2.0)

        remaining_order = readonly_client.get_order(symbol=args.symbol, client_order_id=stop_receipt.client_order_id)
        lingering = [] if remaining_order is None else [_order_to_protective_dict(remaining_order)]
        result['lingering_before_cleanup'] = {
            'position': asdict(readonly_client.get_position_snapshot(args.symbol)),
            'protective_orders': lingering,
            'open_orders': [item.raw for item in readonly_client.get_open_orders(args.symbol)],
        }

        worker = _build_runtime_components(config)
        state = worker.state_store.load_state()
        state.runtime_mode = 'FROZEN'
        state.freeze_status = 'ACTIVE'
        state.freeze_reason = 'protective_orders_present_while_flat'
        state.exchange_position_side = None
        state.exchange_position_qty = 0.0
        state.exchange_protective_orders = lingering
        worker.state_store.save_state(state)
        cleanup_result = worker._maybe_cleanup_lingering_protective_orders(state=state, run_id=run_id)
        result['runtime_cleanup_result'] = cleanup_result

        time.sleep(1.0)
        result['after_runtime_cleanup'] = _ensure_flat(readonly_client, args.symbol)
        result['ok'] = bool(
            lingering
            and cleanup_result
            and cleanup_result.get('allowed') is True
            and cleanup_result.get('reason') == 'canceled_lingering_protective_orders_after_flat'
            and result['after_runtime_cleanup']['is_flat']
            and result['after_runtime_cleanup']['open_orders_count'] == 0
        )
    except Exception as exc:  # noqa: BLE001
        result['probe_error'] = {'type': type(exc).__name__, 'message': str(exc), 'category': getattr(exc, 'category', None), 'detail': getattr(exc, 'detail', None)}
        result['ok'] = False
    finally:
        try:
            for order in readonly_client.get_open_orders(args.symbol):
                cancel_req = submit_client.build_cancel_request(symbol=args.symbol, order_id=order.order_id, client_order_id=order.client_order_id, metadata={'algo_order': True})
                cancel_resp, cancel_receipt = submit_client.cancel_order(cancel_req)
                cleanup.setdefault('cancel_open_orders', []).append({'response': asdict(cancel_resp), 'receipt': asdict(cancel_receipt)})
        except Exception as exc:  # noqa: BLE001
            cleanup['cancel_open_orders_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        try:
            position = readonly_client.get_position_snapshot(args.symbol)
            if abs(float(position.qty or 0.0)) > 0.0:
                final_close_request = submit_client.build_submit_request({
                    'symbol': args.symbol,
                    'side': exit_side,
                    'type': 'MARKET',
                    'newClientOrderId': f'{run_id}-cleanup-close',
                    'quantity': abs(float(position.qty or 0.0)),
                    'reduceOnly': 'true',
                })
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
