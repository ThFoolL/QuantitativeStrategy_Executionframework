from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .binance_readonly import OrderSnapshot, PositionSnapshot
from .protective_orders import split_open_orders

PENDING_ORDER_STATUSES = {
    'NEW',
    'PARTIALLY_FILLED',
    'PENDING_CANCEL',
    'ACCEPTED',
    'CALCULATED',
}


@dataclass(frozen=True)
class PositionFactResolution:
    position_confirmed: bool
    side: str | None
    qty: float
    entry_price: float | None
    open_orders_conflict: bool
    working_open_orders: bool
    needs_trade_reconciliation: bool
    reason: str


class PositionFactReconciler:
    """基于仓位事实确认执行结果。

    设计原则：
    - 仓位确认优先级高于交易确认
    - 先确认风险敞口，再等待成交明细异步补齐
    - 只要 open orders 不冲突、仓位事实已明确，就允许进入 POSITION_CONFIRMED
    """

    def resolve_posttrade(
        self,
        *,
        order_status: str,
        position: PositionSnapshot,
        open_orders: Iterable[OrderSnapshot],
        executed_qty: float,
        fill_count: int,
        requested_reduce_only: bool,
    ) -> PositionFactResolution:
        _, regular_open_orders = split_open_orders(open_orders)
        pending_open_orders = [
            item for item in regular_open_orders
            if str(item.status or '').upper() in PENDING_ORDER_STATUSES
        ]
        qty = float(position.qty or 0.0)
        working_open_orders = bool(pending_open_orders) and order_status in {'PARTIALLY_FILLED', 'PENDING'}
        open_orders_conflict = bool(pending_open_orders) and not working_open_orders
        side = position.side
        entry_price = position.entry_price

        position_fact_confirms_fill = (
            order_status == 'FILLED'
            and side in {'long', 'short'}
            and qty > 0.0
            and not open_orders_conflict
        )
        partial_position_working = (
            order_status == 'PARTIALLY_FILLED'
            and side in {'long', 'short'}
            and qty > 0.0
            and working_open_orders
            and not open_orders_conflict
        )
        flat_fact_confirms_close = (
            order_status == 'FILLED'
            and requested_reduce_only
            and side is None
            and qty <= 0.0
            and not open_orders_conflict
        )

        any_position_fact_confirmed = position_fact_confirms_fill or flat_fact_confirms_close or partial_position_working
        needs_trade_reconciliation = any_position_fact_confirmed and (fill_count <= 0 or executed_qty <= 0 or order_status == 'PARTIALLY_FILLED')
        reason = 'position_unconfirmed'
        if flat_fact_confirms_close and needs_trade_reconciliation:
            reason = 'flat_confirmed_pending_trades'
        elif flat_fact_confirms_close:
            reason = 'flat_and_trades_confirmed'
        elif partial_position_working:
            reason = 'partial_position_working_pending_trades'
        elif position_fact_confirms_fill and needs_trade_reconciliation:
            reason = 'position_confirmed_pending_trades'
        elif position_fact_confirms_fill:
            reason = 'position_and_trades_confirmed'
        elif open_orders_conflict:
            reason = 'open_orders_conflict'

        return PositionFactResolution(
            position_confirmed=any_position_fact_confirmed,
            side=side,
            qty=qty,
            entry_price=entry_price,
            open_orders_conflict=open_orders_conflict,
            working_open_orders=working_open_orders,
            needs_trade_reconciliation=needs_trade_reconciliation,
            reason=reason,
        )
