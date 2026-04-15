from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone

from .runtime_env import load_binance_env
from .binance_readonly import BinanceReadOnlyClient
from .market_data import BinanceReadOnlyMarketDataProvider, build_market_snapshot
from .executor_real import BinanceRealExecutor
from .models import LiveStateSnapshot, FinalActionPlan


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description='单次受控 reduce-only 平仓当前残留仓位')
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--env-file', required=True)
    args = parser.parse_args()

    config = load_binance_env(args.env_file)
    client = BinanceReadOnlyClient(config)
    provider = BinanceReadOnlyMarketDataProvider(client)
    market = build_market_snapshot(provider=provider, symbol=args.symbol, decision_time=datetime.now(timezone.utc))
    position = client.get_position_snapshot(args.symbol)
    account = client.get_account_snapshot()
    open_orders = client.get_open_orders(args.symbol)

    if abs(float(position.qty or 0.0)) <= 0:
        print(json.dumps({'ok': True, 'skipped': True, 'reason': 'already_flat'}, ensure_ascii=False))
        return 0

    state = LiveStateSnapshot(
        state_ts=_now_iso(),
        consistency_status='OK',
        freeze_reason=None,
        account_equity=account.account_equity,
        available_margin=account.available_margin,
        exchange_position_side=position.side,
        exchange_position_qty=position.qty,
        exchange_entry_price=position.entry_price,
        active_strategy='manual_residual_cleanup',
        active_side=position.side,
        strategy_entry_time=None,
        strategy_entry_price=position.entry_price,
        stop_price=None,
        risk_fraction=None,
        runtime_mode='ACTIVE',
        freeze_status='NONE',
        pending_execution_phase='none',
        last_confirmed_order_ids=[],
        base_quantity=position.qty,
    )

    plan = FinalActionPlan(
        plan_ts=_now_iso(),
        bar_ts=_now_iso(),
        action_type='close',
        target_strategy='manual_residual_cleanup',
        target_side=None,
        reason='manual_reduce_only_cleanup',
        qty_mode='position_close',
        qty=None,
        price_hint=None,
        stop_price=None,
        risk_fraction=None,
        conflict_context={'manual_reduce_only_cleanup': True},
        requires_execution=True,
        close_reason='manual_reduce_only_cleanup',
    )

    executor = BinanceRealExecutor(config=config, readonly_client=client)
    result = executor.execute(plan, market, state)
    print(json.dumps({
        'ok': True,
        'market': {
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'bar_ts': market.bar_ts,
            'current_price': market.current_price,
        },
        'position_before': asdict(position),
        'open_orders_before': [o.raw for o in open_orders],
        'result': asdict(result),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
