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
except ImportError:  # pragma: no cover - support direct execution
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


def _safe_order_raw(order) -> dict[str, Any] | None:
    return None if order is None else dict(order.raw)


def _reset_probe_runtime_state(state, *, position_side: str | None, position_qty: float, entry_price: float | None, eval_ts: str) -> None:
    state.runtime_mode = 'FROZEN'
    state.freeze_status = 'ACTIVE'
    state.freeze_reason = 'protective_order_missing'
    state.pending_execution_phase = 'confirmed'
    state.pending_execution_block_reason = 'protective_order_missing'
    state.exchange_position_side = position_side
    state.exchange_position_qty = position_qty
    state.exchange_entry_price = entry_price
    state.active_strategy = 'manual_protective_missing_probe'
    state.active_side = position_side
    state.stop_price = None
    state.tp_price = None
    state.exchange_protective_orders = []
    state.protective_order_status = 'MISSING'
    state.protective_phase_status = 'MISSING'
    state.last_recover_result = None
    state.last_recover_at = None
    state.recover_attempt_count = 0
    state.recover_check = {}
    state.recover_timeline = []
    state.async_operations = {'active': [], 'history': []}
    state.position_confirmation_level = 'POSITION_CONFIRMED'
    state.trade_confirmation_level = 'NONE'
    state.needs_trade_reconciliation = False
    state.fills_reconciled = False
    state.last_confirmed_order_ids = []
    state.strategy_protection_intent = {
        'intent_status': 'ACTIVE',
        'intent_state': 'protective_missing',
        'lifecycle_status': 'protective_missing',
        'pending_action': None,
        'strategy': state.active_strategy,
        'position_side': position_side,
        'position_qty': float(position_qty or 0.0),
        'stop_price': None,
        'tp_price': None,
        'pending_execution_phase': 'confirmed',
        'protective_order_status': 'MISSING',
        'protective_phase_status': 'MISSING',
        'expected_protection': True,
        'exchange_order_count': 0,
        'validation_status': 'MISSING',
        'validation_level': 'MISSING',
        'risk_class': 'FORCE_CLOSE',
        'mismatch_class': 'MISSING',
        'freeze_reason': 'protective_order_missing',
        'block_reason': 'protective_order_missing',
        'last_eval_ts': eval_ts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='最小真钱验证 protective_order_missing -> emergency reduce-only close 链路')
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--target-notional', type=float, default=25.0)
    parser.add_argument('--side', choices=['long', 'short'], default='short')
    parser.add_argument('--sleep-after-open', type=float, default=2.0)
    parser.add_argument('--sleep-after-protect', type=float, default=1.0)
    parser.add_argument('--decision-time', default=None, help='可选：传给 runtime_worker 的单轮 decision time')
    parser.add_argument('--out', default=None)
    args = parser.parse_args()

    run_id = _run_id()
    out_path = Path(args.out) if args.out else Path('docs/deploy_v6c/samples/manual_protective_missing_emergency_close') / run_id / f'{run_id}_{args.symbol}_protective_missing_emergency_close_summary.json'
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
    if rules.min_notional is not None and notional < float(rules.min_notional):
        raise ValueError(f'normalized notional too small: {notional} < {rules.min_notional}')

    entry_side = 'BUY' if args.side == 'long' else 'SELL'
    exit_side = 'SELL' if args.side == 'long' else 'BUY'
    stop_trigger = round(price * (0.99 if args.side == 'long' else 1.01), 2)
    tp_trigger = round(price * (1.01 if args.side == 'long' else 0.99), 2)

    result: dict[str, Any] = {
        'run_id': run_id,
        'symbol': args.symbol,
        'mode': 'protective_missing_emergency_close_probe',
        'requested': {
            'side': args.side,
            'target_notional': args.target_notional,
            'market_price': price,
            'quantity': quantity,
            'estimated_notional': notional,
            'stop_trigger': stop_trigger,
            'tp_trigger': tp_trigger,
        },
        'config_gate': {
            'dry_run': config.dry_run,
            'submit_enabled': config.submit_enabled,
            'submit_http_post_enabled': config.submit_http_post_enabled,
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
    position_after_open = readonly_client.get_position_snapshot(args.symbol)
    open_order = readonly_client.get_order(symbol=args.symbol, client_order_id=open_receipt.client_order_id)
    result['after_open'] = {
        'order': _safe_order_raw(open_order),
        'position': asdict(position_after_open),
    }
    if abs(float(position_after_open.qty or 0.0)) <= 0.0:
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
    stop_response, stop_receipt = submit_client.submit_order(stop_request)
    tp_response, tp_receipt = submit_client.submit_order(tp_request)
    result['protect_submit'] = {
        'stop': {'response': asdict(stop_response), 'receipt': asdict(stop_receipt)},
        'take_profit': {'response': asdict(tp_response), 'receipt': asdict(tp_receipt)},
    }

    time.sleep(args.sleep_after_protect)
    result['protect_readback_before_cancel'] = {
        'stop': _safe_order_raw(readonly_client.get_order(symbol=args.symbol, client_order_id=stop_receipt.client_order_id)),
        'take_profit': _safe_order_raw(readonly_client.get_order(symbol=args.symbol, client_order_id=tp_receipt.client_order_id)),
        'open_orders': [item.raw for item in readonly_client.get_open_orders(args.symbol)],
    }

    cancel_tp_req = submit_client.build_cancel_request(symbol=args.symbol, order_id=tp_receipt.exchange_order_id, client_order_id=tp_receipt.client_order_id, metadata={'algo_order': True})
    cancel_stop_req = submit_client.build_cancel_request(symbol=args.symbol, order_id=stop_receipt.exchange_order_id, client_order_id=stop_receipt.client_order_id, metadata={'algo_order': True})
    cancel_tp_resp, cancel_tp_receipt = submit_client.cancel_order(cancel_tp_req)
    cancel_stop_resp, cancel_stop_receipt = submit_client.cancel_order(cancel_stop_req)
    result['protect_cancel'] = {
        'take_profit': {'response': asdict(cancel_tp_resp), 'receipt': asdict(cancel_tp_receipt)},
        'stop': {'response': asdict(cancel_stop_resp), 'receipt': asdict(cancel_stop_receipt)},
    }

    result['after_cancel'] = {
        'position': asdict(readonly_client.get_position_snapshot(args.symbol)),
        'open_orders': [item.raw for item in readonly_client.get_open_orders(args.symbol)],
    }

    worker = _build_runtime_components(config)
    state = worker.state_store.load_state()
    position_before_runtime = readonly_client.get_position_snapshot(args.symbol)
    _reset_probe_runtime_state(
        state,
        position_side=position_before_runtime.side,
        position_qty=float(position_before_runtime.qty or 0.0),
        entry_price=position_before_runtime.entry_price,
        eval_ts=_utc_now().isoformat(),
    )
    worker.state_store.save_state(state)
    result['runtime_pre_state'] = {
        'runtime_mode': state.runtime_mode,
        'freeze_status': state.freeze_status,
        'freeze_reason': state.freeze_reason,
        'exchange_position_side': state.exchange_position_side,
        'exchange_position_qty': state.exchange_position_qty,
        'protective_order_status': state.protective_order_status,
        'pending_execution_block_reason': state.pending_execution_block_reason,
    }

    decision_time = None if args.decision_time is None else datetime.fromisoformat(args.decision_time)
    runtime_output = worker.run_once(decision_time)
    final_state = worker.state_store.load_state()
    result['runtime_last_result'] = runtime_output.get('result')
    result['runtime_post_state'] = {
        'runtime_mode': final_state.runtime_mode,
        'freeze_status': final_state.freeze_status,
        'freeze_reason': final_state.freeze_reason,
        'last_recover_result': final_state.last_recover_result,
        'recover_check': final_state.recover_check,
        'exchange_position_side': final_state.exchange_position_side,
        'exchange_position_qty': final_state.exchange_position_qty,
        'pending_execution_phase': final_state.pending_execution_phase,
        'pending_execution_block_reason': final_state.pending_execution_block_reason,
    }

    result['after_runtime'] = {
        'position': asdict(readonly_client.get_position_snapshot(args.symbol)),
        'open_orders': [item.raw for item in readonly_client.get_open_orders(args.symbol)],
    }

    runtime_recover = result['runtime_post_state'].get('recover_check') or {}
    runtime_result = (result.get('runtime_last_result') or {})
    recover_stop_reason = runtime_recover.get('stop_reason') or ((runtime_result.get('state_updates') or {}).get('recover_check') or {}).get('stop_reason')
    recover_risk_action = runtime_recover.get('risk_action') or ((runtime_result.get('state_updates') or {}).get('recover_check') or {}).get('risk_action')
    runtime_unfrozen_after_close = (
        result['runtime_post_state'].get('runtime_mode') == 'ACTIVE'
        and result['runtime_post_state'].get('freeze_status') == 'NONE'
        and result['runtime_post_state'].get('freeze_reason') in {None, ''}
        and result['runtime_post_state'].get('last_recover_result') == 'RECOVERED'
    )
    result['ok'] = bool(
        abs(float(result['after_runtime']['position']['qty'] or 0.0)) <= 0.0
        and len(result['after_runtime']['open_orders']) == 0
        and runtime_result.get('status') == 'FILLED'
        and runtime_result.get('action_type') == 'close'
        and runtime_result.get('confirmed_order_status') == 'FILLED'
        and (
            (recover_stop_reason == 'protective_order_missing' and recover_risk_action == 'FORCE_CLOSE')
            or runtime_unfrozen_after_close
        )
    )

    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'ok': result['ok'], 'summary_path': str(out_path)}, ensure_ascii=False))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
