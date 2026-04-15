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
    from .binance_readonly import BinanceReadOnlyClient, OrderSnapshot
    from .binance_submit import BinanceSignedSubmitClient
    from .binance_reconcile import ExchangeSnapshot, ReconcileInput, reconcile_pre_run
    from .market_data import BinanceReadOnlyMarketDataProvider
    from .runtime_env import load_binance_env
    from .models import LiveStateSnapshot
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from exec_framework.binance_readonly import BinanceReadOnlyClient, OrderSnapshot
    from exec_framework.binance_submit import BinanceSignedSubmitClient
    from exec_framework.binance_reconcile import ExchangeSnapshot, ReconcileInput, reconcile_pre_run
    from exec_framework.market_data import BinanceReadOnlyMarketDataProvider
    from exec_framework.runtime_env import load_binance_env
    from exec_framework.models import LiveStateSnapshot


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
    parser = argparse.ArgumentParser(description='最小真钱验证 partial protective missing 样本')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--side', choices=['long', 'short'], default='short')
    parser.add_argument('--target-notional', type=float, default=25.0)
    parser.add_argument('--missing-leg', choices=['stop', 'tp'], default='tp')
    parser.add_argument('--sleep-after-open', type=float, default=1.0)
    parser.add_argument('--sleep-after-protect', type=float, default=1.0)
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    run_id = _run_id()
    out_path = Path(args.out) if args.out else Path('docs/deploy_v6c/samples/manual_partial_protective_missing') / run_id / f'{run_id}_{args.symbol}_partial_protective_missing_summary.json'
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
    tp_trigger = round(price * (1.01 if args.side == 'long' else 0.99), 2)

    result: dict[str, Any] = {
        'run_id': run_id,
        'symbol': args.symbol,
        'mode': 'partial_protective_missing_probe',
        'requested': {
            'side': args.side,
            'target_notional': args.target_notional,
            'quantity': qty,
            'missing_leg': args.missing_leg,
            'market_price': price,
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

    cleanup: dict[str, Any] = {}
    try:
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
        time.sleep(args.sleep_after_open)

        stop_payload = {
            'symbol': args.symbol,
            'side': exit_side,
            'type': 'STOP_MARKET',
            'clientAlgoId': f'{run_id}-stop',
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
            'clientAlgoId': f'{run_id}-tp',
            'algoType': 'CONDITIONAL',
            'triggerPrice': tp_trigger,
            'closePosition': 'true',
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

        missing_receipt = tp_receipt if args.missing_leg == 'tp' else stop_receipt
        cancel_req = submit_client.build_cancel_request(
            symbol=args.symbol,
            order_id=missing_receipt.exchange_order_id,
            client_order_id=missing_receipt.client_order_id,
            metadata={'algo_order': True},
        )
        cancel_resp, cancel_receipt = submit_client.cancel_order(cancel_req)
        result['cancel_missing_leg'] = {'response': asdict(cancel_resp), 'receipt': asdict(cancel_receipt)}

        protective_orders = []
        remaining_open_orders: list[OrderSnapshot] = []
        remaining_receipt = stop_receipt if args.missing_leg == 'tp' else tp_receipt
        remaining_order = readonly_client.get_order(symbol=args.symbol, client_order_id=remaining_receipt.client_order_id)
        if remaining_order is not None:
            remaining_open_orders.append(remaining_order)
            raw = dict(remaining_order.raw)
            result['remaining_order_snapshot'] = {
                'order_id': remaining_order.order_id,
                'client_order_id': remaining_order.client_order_id,
                'status': remaining_order.status,
                'type': remaining_order.type,
                'orig_type': remaining_order.orig_type,
                'side': remaining_order.side,
                'position_side': remaining_order.position_side,
                'qty': remaining_order.qty,
                'executed_qty': remaining_order.executed_qty,
                'stop_price': remaining_order.stop_price,
                'reduce_only': remaining_order.reduce_only,
                'close_position': remaining_order.close_position,
                'raw': raw,
            }
            order_type = str(remaining_order.type or remaining_order.orig_type or raw.get('type') or raw.get('orderType') or raw.get('origType') or '').upper()
            protective_orders.append(
                {
                    'kind': 'hard_stop' if order_type == 'STOP_MARKET' else 'take_profit',
                    'type': order_type,
                    'close_position': bool(remaining_order.close_position if remaining_order.close_position is not None else raw.get('closePosition')),
                    'reduce_only': bool(remaining_order.reduce_only if remaining_order.reduce_only is not None else raw.get('reduceOnly')),
                    'side': remaining_order.side or raw.get('side'),
                    'position_side': str(remaining_order.position_side or raw.get('positionSide') or 'BOTH').lower(),
                    'qty': remaining_order.qty if remaining_order.qty is not None else (raw.get('origQty') if raw.get('origQty') is not None else raw.get('quantity')),
                    'status': remaining_order.status or raw.get('algoStatus') or raw.get('status'),
                    'client_order_id': remaining_order.client_order_id or raw.get('clientAlgoId') or raw.get('clientOrderId'),
                    'order_id': remaining_order.order_id or raw.get('algoId') or raw.get('orderId'),
                    'stop_price': remaining_order.stop_price if remaining_order.stop_price is not None else (raw.get('triggerPrice') or raw.get('stopPrice')),
                }
            )
        position = readonly_client.get_position_snapshot(args.symbol)
        account = readonly_client.get_account_snapshot()
        state = LiveStateSnapshot(
            state_ts=_utc_now().isoformat(),
            consistency_status='OK',
            freeze_reason=None,
            account_equity=float(account.account_equity),
            available_margin=float(account.available_margin),
            exchange_position_side=position.side,
            exchange_position_qty=float(position.qty or 0.0),
            exchange_entry_price=position.entry_price,
            active_strategy='rev',
            active_side=args.side,
            strategy_entry_time=_utc_now().isoformat(),
            strategy_entry_price=position.entry_price,
            stop_price=stop_trigger,
            risk_fraction=0.1,
            tp_price=tp_trigger,
            base_quantity=float(position.qty or 0.0),
            pending_execution_phase=None,
            position_confirmation_level='NONE',
            needs_trade_reconciliation=False,
        )
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(
                    account=account,
                    position=position,
                    open_orders=remaining_open_orders,
                ),
            )
        )
        result['reconcile'] = {
            'protective_orders': protective_orders,
            'notes': list(decision.notes or []),
            'freeze_reason': decision.freeze_reason,
            'stop_condition': decision.stop_condition,
            'risk_action': decision.risk_action,
            'order_count': len(protective_orders),
            'status': decision.status,
        }
        expected_reason = 'protection_tp_missing' if args.missing_leg == 'tp' else 'protection_stop_missing'
        result['ok'] = bool(
            expected_reason in (decision.notes or [])
            and 'partial_protective_missing' in (decision.notes or [])
            and decision.stop_condition == expected_reason
        )
    finally:
        try:
            for order in readonly_client.get_open_orders(args.symbol):
                cancel_req = submit_client.build_cancel_request(
                    symbol=args.symbol,
                    order_id=order.order_id,
                    client_order_id=order.client_order_id,
                    metadata={'algo_order': True},
                )
                cancel_resp, cancel_receipt = submit_client.cancel_order(cancel_req)
                cleanup.setdefault('cancel_open_orders', []).append({'response': asdict(cancel_resp), 'receipt': asdict(cancel_receipt)})
        except Exception as exc:  # noqa: BLE001
            cleanup['cancel_open_orders_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        try:
            position = readonly_client.get_position_snapshot(args.symbol)
            if abs(float(position.qty or 0.0)) > 0.0:
                close_payload = {
                    'symbol': args.symbol,
                    'side': exit_side,
                    'type': 'MARKET',
                    'newClientOrderId': f'{run_id}-cleanup-close',
                    'quantity': abs(float(position.qty or 0.0)),
                    'reduceOnly': 'true',
                }
                close_request = submit_client.build_submit_request(close_payload)
                close_response, close_receipt = submit_client.submit_order(close_request)
                cleanup['cleanup_close'] = {'response': asdict(close_response), 'receipt': asdict(close_receipt)}
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
