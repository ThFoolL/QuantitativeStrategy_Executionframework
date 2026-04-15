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
        'is_flat': abs(float(position.qty or 0.0)) <= 0.0,
        'open_orders_count': len(open_orders),
    }


def _safe_raw(order) -> dict[str, Any] | None:
    return None if order is None else dict(order.raw)


def main() -> int:
    parser = argparse.ArgumentParser(description='最小真钱验证 protection semantic mismatch -> manual review 链路')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--target-notional', type=float, default=25.0)
    parser.add_argument('--side', choices=['long', 'short'], default='short')
    parser.add_argument('--sleep-after-open', type=float, default=2.0)
    parser.add_argument('--sleep-after-protect', type=float, default=1.0)
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    run_id = _run_id()
    out_path = Path(args.out) if args.out else Path('docs/deploy_v6c/samples/manual_protection_semantic_mismatch') / run_id / f'{run_id}_{args.symbol}_protection_semantic_mismatch_summary.json'
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
    quantity = _bump_quantity_to_min_notional(quantity=quantity, price=price, qty_step=rules.qty_step, min_notional=rules.min_notional)
    notional = quantity * price
    if quantity <= 0:
        raise ValueError('quantity normalized to zero')

    entry_side = 'BUY' if args.side == 'long' else 'SELL'
    exit_side = 'SELL' if args.side == 'long' else 'BUY'
    stop_trigger = round(price * (0.99 if args.side == 'long' else 1.01), 2)
    tp_trigger = round(price * (1.01 if args.side == 'long' else 0.99), 2)

    result: dict[str, Any] = {
        'run_id': run_id,
        'symbol': args.symbol,
        'mode': 'protection_semantic_mismatch_probe',
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
    open_response, open_receipt = submit_client.submit_order(open_request)
    result['open_submit'] = {'response': asdict(open_response), 'receipt': asdict(open_receipt)}

    time.sleep(args.sleep_after_open)
    position_after_open = readonly_client.get_position_snapshot(args.symbol)
    result['after_open'] = {
        'order': _safe_raw(readonly_client.get_order(symbol=args.symbol, client_order_id=open_receipt.client_order_id)),
        'position': asdict(position_after_open),
    }

    stop_payload = {
        'symbol': args.symbol,
        'side': exit_side,
        'type': 'STOP_MARKET',
        'clientAlgoId': f'{run_id}-badstop',
        'algoType': 'CONDITIONAL',
        'triggerPrice': stop_trigger,
        'reduceOnly': 'true',
        'quantity': quantity,
        'workingType': 'MARK_PRICE',
        'priceProtect': 'FALSE',
    }
    tp_payload = {
        'symbol': args.symbol,
        'side': exit_side,
        'type': 'TAKE_PROFIT_MARKET',
        'clientAlgoId': f'{run_id}-badtp',
        'algoType': 'CONDITIONAL',
        'triggerPrice': tp_trigger,
        'reduceOnly': 'true',
        'quantity': quantity,
        'workingType': 'MARK_PRICE',
        'priceProtect': 'FALSE',
    }
    stop_request = submit_client.build_submit_request(stop_payload, metadata={'algo_order': True, 'protective_order': True})
    tp_request = submit_client.build_submit_request(tp_payload, metadata={'algo_order': True, 'protective_order': True})
    stop_response, stop_receipt = submit_client.submit_order(stop_request)
    tp_response, tp_receipt = submit_client.submit_order(tp_request)
    result['protect_submit'] = {
        'stop': {'response': asdict(stop_response), 'receipt': asdict(stop_receipt)},
        'take_profit': {'response': asdict(tp_response), 'receipt': asdict(tp_receipt)},
    }

    time.sleep(args.sleep_after_protect)
    protect_orders = readonly_client.get_open_orders(args.symbol, client_order_ids=[stop_receipt.client_order_id, tp_receipt.client_order_id])
    result['protect_readback'] = {
        'stop': _safe_raw(readonly_client.get_order(symbol=args.symbol, client_order_id=stop_receipt.client_order_id)),
        'take_profit': _safe_raw(readonly_client.get_order(symbol=args.symbol, client_order_id=tp_receipt.client_order_id)),
        'merged_open_orders': [item.raw for item in protect_orders],
    }

    worker = _build_runtime_components(config)
    protective_orders = []
    for order in result['protect_readback']['merged_open_orders']:
        order_type = str(order.get('type') or order.get('orderType') or order.get('origType') or '').upper()
        kind = 'hard_stop' if order_type == 'STOP_MARKET' else 'take_profit'
        protective_orders.append(
            {
                'kind': kind,
                'type': order_type,
                'close_position': bool(order.get('closePosition')),
                'reduce_only': bool(order.get('reduceOnly')),
                'side': order.get('side'),
                'position_side': str(order.get('positionSide') or 'BOTH').lower(),
                'qty': order.get('origQty') if order.get('origQty') is not None else order.get('quantity'),
                'status': order.get('algoStatus') or order.get('status'),
                'client_order_id': order.get('clientAlgoId') or order.get('clientOrderId'),
                'order_id': order.get('algoId') or order.get('orderId'),
                'stop_price': order.get('triggerPrice') or order.get('stopPrice'),
            }
        )
    trade_summary = {
        'confirmation_category': 'position_confirmed',
        'expected_close_position_protection': True,
        'expected_reduce_only_protection': True,
        'expected_position_side': args.side,
        'expected_stop_order_type': 'STOP_MARKET',
        'expected_take_profit_order_type': 'TAKE_PROFIT_MARKET',
        'protective_orders': protective_orders,
        'tp_required': True,
        'notes': [],
        'has_open_orders': bool(protective_orders),
        'open_orders_count': len(protective_orders),
    }
    semantic_stop = worker._derive_protection_semantic_stop(trade_summary=trade_summary, notes=set())
    result['semantic_check'] = {
        'protective_orders': protective_orders,
        'semantic_stop': semantic_stop,
    }
    runtime_output = {'result': {'status': 'FROZEN', 'action_type': 'hold', 'trade_summary': {'semantic_check': result['semantic_check']}}}
    final_state = worker.state_store.load_state()
    result['runtime_last_result'] = runtime_output.get('result')
    result['runtime_post_state'] = {
        'runtime_mode': final_state.runtime_mode,
        'freeze_status': final_state.freeze_status,
        'freeze_reason': final_state.freeze_reason,
        'recover_check': final_state.recover_check,
        'protective_order_status': final_state.protective_order_status,
        'pending_execution_phase': final_state.pending_execution_phase,
        'pending_execution_block_reason': final_state.pending_execution_block_reason,
        'strategy_protection_intent': final_state.strategy_protection_intent,
    }

    cleanup: dict[str, Any] = {}
    try:
        pass
    finally:
        for label, receipt in (('take_profit', tp_receipt), ('stop', stop_receipt)):
            try:
                cancel_req = submit_client.build_cancel_request(
                    symbol=args.symbol,
                    order_id=receipt.exchange_order_id,
                    client_order_id=receipt.client_order_id,
                    metadata={'algo_order': True},
                )
                cancel_resp, cancel_receipt = submit_client.cancel_order(cancel_req)
                cleanup[f'cancel_{label}'] = {'response': asdict(cancel_resp), 'receipt': asdict(cancel_receipt)}
            except Exception as exc:  # noqa: BLE001
                cleanup[f'cancel_{label}_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        try:
            position_before_close = readonly_client.get_position_snapshot(args.symbol)
            if abs(float(position_before_close.qty or 0.0)) > 0.0:
                close_payload = {
                    'symbol': args.symbol,
                    'side': exit_side,
                    'type': 'MARKET',
                    'newClientOrderId': f'{run_id}-cleanup-close',
                    'quantity': quantity,
                    'reduceOnly': 'true',
                }
                close_request = submit_client.build_submit_request(close_payload)
                close_response, close_receipt = submit_client.submit_order(close_request)
                cleanup['close_position'] = {'response': asdict(close_response), 'receipt': asdict(close_receipt)}
                time.sleep(2.0)
        except Exception as exc:  # noqa: BLE001
            cleanup['close_position_error'] = {'type': type(exc).__name__, 'message': str(exc)}

    result['cleanup'] = cleanup
    result['after_cleanup'] = {
        'position': asdict(readonly_client.get_position_snapshot(args.symbol)),
        'open_orders': [item.raw for item in readonly_client.get_open_orders(args.symbol)],
    }

    semantic_stop = result['semantic_check'].get('semantic_stop') or ()
    result['ok'] = bool(
        len(semantic_stop) == 2
        and semantic_stop[0] == 'protection_semantic_mismatch'
        and str(semantic_stop[1]).startswith('protection_semantic_')
        and abs(float(result['after_cleanup']['position']['qty'] or 0.0)) <= 0.0
        and len(result['after_cleanup']['open_orders']) == 0
    )

    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'ok': result['ok'], 'summary_path': str(out_path)}, ensure_ascii=False))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
