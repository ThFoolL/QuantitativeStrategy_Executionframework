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
except ImportError:  # 兼容 `python3 live/...py` 直接执行
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from exec_framework.binance_readonly import BinanceReadOnlyClient
    from exec_framework.binance_submit import BinanceSignedSubmitClient
    from exec_framework.market_data import BinanceReadOnlyMarketDataProvider
    from exec_framework.runtime_env import load_binance_env


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run_id() -> str:
    return _utc_now().strftime('%Y%m%dT%H%M%S%fZ')


def _safe_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


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
    min_notional_value = float(min_notional)
    if qty * price >= min_notional_value:
        return round(qty, 8)

    if qty_step and qty_step > 0:
        while qty * price < min_notional_value:
            qty = round(qty + qty_step, 8)
    else:
        qty = round(min_notional_value / price, 8)
    return round(qty, 8)


def _ensure_flat(client: BinanceReadOnlyClient, symbol: str) -> dict[str, Any]:
    position = client.get_position_snapshot(symbol)
    open_orders = client.get_open_orders(symbol)
    return {
        'position': asdict(position),
        'open_orders': [item.raw for item in open_orders],
        'is_flat': abs(float(position.qty or 0.0)) <= 0,
        'open_orders_count': len(open_orders),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='最小真钱验证 closePosition=true protection 提交/只读/清理路径')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--target-notional', type=float, default=20.0)
    parser.add_argument('--side', choices=['long', 'short'], default='long')
    parser.add_argument('--sleep-after-open', type=float, default=2.0)
    parser.add_argument('--sleep-after-protect', type=float, default=1.0)
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    run_id = _run_id()
    out_path = Path(args.out) if args.out else Path('docs/deploy_v6c/samples/real_trade_sampling/manual_runs') / run_id / 'close_position_probe_summary.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)

    config = load_binance_env(Path(args.env_file))
    readonly_client = BinanceReadOnlyClient(config)
    submit_client = BinanceSignedSubmitClient(config, allow_live_submit_call=True)
    market_provider = BinanceReadOnlyMarketDataProvider(readonly_client)
    market_bundle = market_provider.load(symbol=args.symbol, decision_time=_utc_now())
    rules = readonly_client.get_exchange_info(args.symbol)

    price = float(market_bundle.current_price)
    raw_qty = float(args.target_notional) / price
    quantity = _normalize_quantity(raw_qty=raw_qty, qty_step=rules.qty_step, min_qty=rules.min_qty)
    quantity = _bump_quantity_to_min_notional(
        quantity=quantity,
        price=price,
        qty_step=rules.qty_step,
        min_notional=rules.min_notional,
    )
    notional = quantity * price
    if quantity <= 0:
        raise ValueError('quantity normalized to zero')
    if rules.min_notional is not None and notional < float(rules.min_notional):
        raise ValueError(f'normalized notional too small: {notional} < {rules.min_notional}')

    entry_side = 'BUY' if args.side == 'long' else 'SELL'
    exit_side = 'SELL' if args.side == 'long' else 'BUY'
    stop_trigger = round(price * (0.99 if args.side == 'long' else 1.01), 2)
    tp_trigger = round(price * (1.01 if args.side == 'long' else 0.99), 2)

    result: dict[str, Any] = {
        'run_id': run_id,
        'symbol': args.symbol,
        'mode': 'close_position_probe',
        'config_gate': {
            'dry_run': config.dry_run,
            'submit_enabled': config.submit_enabled,
            'submit_http_post_enabled': config.submit_http_post_enabled,
        },
        'requested': {
            'side': args.side,
            'target_notional': args.target_notional,
            'market_price': price,
            'quantity': quantity,
            'estimated_notional': notional,
            'stop_trigger': stop_trigger,
            'tp_trigger': tp_trigger,
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
        'quantity': quantity,
    }
    open_request = submit_client.build_submit_request(open_payload)
    result['open_prepared'] = submit_client.prepare_signed_post(open_request).body_redacted
    open_response, open_receipt = submit_client.submit_order(open_request)
    result['open_submit'] = {
        'response': asdict(open_response),
        'receipt': asdict(open_receipt),
    }

    time.sleep(args.sleep_after_open)
    open_order = readonly_client.get_order(symbol=args.symbol, client_order_id=open_receipt.client_order_id)
    trades = readonly_client.get_recent_trades(args.symbol, order_id=open_receipt.exchange_order_id, limit=20)
    position_after_open = readonly_client.get_position_snapshot(args.symbol)
    result['after_open'] = {
        'order': None if open_order is None else open_order.raw,
        'trades': [item.raw for item in trades],
        'position': asdict(position_after_open),
    }

    if abs(float(position_after_open.qty or 0.0)) <= 0:
        result['aborted'] = True
        result['abort_reason'] = 'position_not_open_after_market_order'
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 3

    stop_payload = {
        'symbol': args.symbol,
        'side': exit_side,
        'type': 'STOP_MARKET',
        'clientAlgoId': f'{run_id}-pstop',
        'algoType': 'CONDITIONAL',
        'triggerPrice': stop_trigger,
        'closePosition': 'true',
        'workingType': 'MARK_PRICE',
        'priceProtect': 'FALSE',
    }
    tp_payload = {
        'symbol': args.symbol,
        'side': exit_side,
        'type': 'TAKE_PROFIT_MARKET',
        'clientAlgoId': f'{run_id}-ptp',
        'algoType': 'CONDITIONAL',
        'triggerPrice': tp_trigger,
        'closePosition': 'true',
        'workingType': 'MARK_PRICE',
        'priceProtect': 'FALSE',
    }

    stop_request = submit_client.build_submit_request(stop_payload, metadata={'algo_order': True, 'protective_order': True})
    tp_request = submit_client.build_submit_request(tp_payload, metadata={'algo_order': True, 'protective_order': True})
    result['protect_prepared'] = {
        'stop': submit_client.prepare_signed_post(stop_request).body_redacted,
        'take_profit': submit_client.prepare_signed_post(tp_request).body_redacted,
    }

    stop_response = stop_receipt = tp_response = tp_receipt = None
    close_response = close_receipt = None
    cleanup: dict[str, Any] = {}

    try:
        stop_response, stop_receipt = submit_client.submit_order(stop_request)
        tp_response, tp_receipt = submit_client.submit_order(tp_request)
        result['protect_submit'] = {
            'stop': {'response': asdict(stop_response), 'receipt': asdict(stop_receipt)},
            'take_profit': {'response': asdict(tp_response), 'receipt': asdict(tp_receipt)},
        }

        time.sleep(args.sleep_after_protect)
        stop_algo = readonly_client.get_order(symbol=args.symbol, client_order_id=stop_receipt.client_order_id)
        tp_algo = readonly_client.get_order(symbol=args.symbol, client_order_id=tp_receipt.client_order_id)
        open_orders_with_algo = readonly_client.get_open_orders(
            args.symbol,
            client_order_ids=[stop_receipt.client_order_id, tp_receipt.client_order_id],
        )
        result['protect_readback'] = {
            'stop': None if stop_algo is None else stop_algo.raw,
            'take_profit': None if tp_algo is None else tp_algo.raw,
            'merged_open_orders': [item.raw for item in open_orders_with_algo],
        }
    finally:
        if tp_receipt is not None:
            try:
                cancel_req = submit_client.build_cancel_request(
                    symbol=args.symbol,
                    order_id=tp_receipt.exchange_order_id,
                    client_order_id=tp_receipt.client_order_id,
                    metadata={'algo_order': True},
                )
                cancel_resp, cancel_receipt = submit_client.cancel_order(cancel_req)
                cleanup['cancel_take_profit'] = {'response': asdict(cancel_resp), 'receipt': asdict(cancel_receipt)}
            except Exception as exc:  # noqa: BLE001
                cleanup['cancel_take_profit_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        if stop_receipt is not None:
            try:
                cancel_req = submit_client.build_cancel_request(
                    symbol=args.symbol,
                    order_id=stop_receipt.exchange_order_id,
                    client_order_id=stop_receipt.client_order_id,
                    metadata={'algo_order': True},
                )
                cancel_resp, cancel_receipt = submit_client.cancel_order(cancel_req)
                cleanup['cancel_stop'] = {'response': asdict(cancel_resp), 'receipt': asdict(cancel_receipt)}
            except Exception as exc:  # noqa: BLE001
                cleanup['cancel_stop_error'] = {'type': type(exc).__name__, 'message': str(exc)}

        try:
            position_before_close = readonly_client.get_position_snapshot(args.symbol)
            if abs(float(position_before_close.qty or 0.0)) > 0:
                close_qty = abs(float(position_before_close.qty))
                close_payload = {
                    'symbol': args.symbol,
                    'side': 'SELL' if position_before_close.side == 'long' else 'BUY',
                    'type': 'MARKET',
                    'newClientOrderId': f'{run_id}-close',
                    'quantity': close_qty,
                    'reduceOnly': 'true',
                }
                close_request = submit_client.build_submit_request(close_payload)
                close_response, close_receipt = submit_client.submit_order(close_request)
                cleanup['close_submit'] = {
                    'response': asdict(close_response),
                    'receipt': asdict(close_receipt),
                }
                time.sleep(1.0)
                close_order = readonly_client.get_order(symbol=args.symbol, client_order_id=close_receipt.client_order_id)
                close_trades = readonly_client.get_recent_trades(args.symbol, order_id=close_receipt.exchange_order_id, limit=20)
                cleanup['close_readback'] = {
                    'order': None if close_order is None else close_order.raw,
                    'trades': [item.raw for item in close_trades],
                }
        except Exception as exc:  # noqa: BLE001
            cleanup['close_error'] = {'type': type(exc).__name__, 'message': str(exc)}

        try:
            final_position = readonly_client.get_position_snapshot(args.symbol)
            final_open_orders_all = readonly_client.get_open_orders(args.symbol)
            final_open_orders_targeted = readonly_client.get_open_orders(
                args.symbol,
                client_order_ids=[f'{run_id}-pstop', f'{run_id}-ptp'],
            )
            cleanup['final_state'] = {
                'position': asdict(final_position),
                'open_orders_all': [item.raw for item in final_open_orders_all],
                'open_orders_targeted': [item.raw for item in final_open_orders_targeted],
                'is_flat': abs(float(final_position.qty or 0.0)) <= 0,
                'open_orders_all_count': len(final_open_orders_all),
                'open_orders_targeted_count': len(final_open_orders_targeted),
            }
        except Exception as exc:  # noqa: BLE001
            cleanup['final_state_error'] = {'type': type(exc).__name__, 'message': str(exc)}

    result['cleanup'] = cleanup
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
