from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_readonly import AccountSnapshot, OrderSnapshot, PositionSnapshot
from exec_framework.binance_reconcile import ExchangeSnapshot, ReconcileInput, reconcile_pre_run
from exec_framework.models import LiveStateSnapshot
from exec_framework.unified_risk_action import STOP_CONDITION_PROTECTION_TP_MISSING


class ReconcileProtectiveOrdersCase(unittest.TestCase):
    def make_state(self) -> LiveStateSnapshot:
        return LiveStateSnapshot(
            state_ts='2026-04-01T14:00:00+00:00',
            consistency_status='OK',
            freeze_reason=None,
            account_equity=1000.0,
            available_margin=900.0,
            exchange_position_side='long',
            exchange_position_qty=0.5,
            exchange_entry_price=2100.0,
            active_strategy='trend',
            active_side='long',
            strategy_entry_time='2026-04-01T14:00:00+00:00',
            strategy_entry_price=2100.0,
            stop_price=2050.0,
            risk_fraction=0.1,
            base_quantity=0.5,
            tp_price=None,
        )

    def protective_order(self) -> OrderSnapshot:
        return OrderSnapshot(
            order_id='protect-1',
            client_order_id='protect-hard-stop',
            status='NEW',
            type='STOP_MARKET',
            orig_type='STOP_MARKET',
            time_in_force='GTC',
            side='sell',
            position_side='both',
            qty=0.5,
            executed_qty=0.0,
            price=0.0,
            avg_price=0.0,
            cum_quote=0.0,
            stop_price=2050.0,
            working_type='MARK_PRICE',
            activate_price=None,
            price_protect=False,
            reduce_only=True,
            close_position=True,
            update_time_ms=1711380000000,
            raw={},
        )

    def test_reconcile_allows_position_with_matching_protective_order(self) -> None:
        decision = reconcile_pre_run(
            ReconcileInput(
                state=self.make_state(),
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[self.protective_order()],
                ),
            )
        )
        self.assertEqual(decision.status, 'OK')

    def test_reconcile_freezes_when_position_missing_protective_order(self) -> None:
        decision = reconcile_pre_run(
            ReconcileInput(
                state=self.make_state(),
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[],
                ),
            )
        )
        self.assertEqual(decision.status, 'MISMATCH')
        self.assertEqual(decision.freeze_reason, 'protective_order_missing')
        self.assertEqual(decision.risk_action, 'MANUAL_REVIEW')
        self.assertEqual(decision.recover_stage, 'protection_missing')
        self.assertIn('protective_orders_invalid', decision.notes)

    def test_reconcile_does_not_freeze_when_management_stop_update_still_sees_exchange_protective(self) -> None:
        state = self.make_state()
        state.pending_execution_phase = 'management_stop_update_pending_protective'
        state.stop_price = 2060.0
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[self.protective_order()],
                ),
            )
        )
        self.assertEqual(decision.status, 'OK')
        self.assertIsNone(decision.freeze_reason)
        self.assertIn('management_stop_update_exchange_protective_visible', decision.notes)

    def test_reconcile_stale_protective_missing_freeze_yields_to_exchange_visible_fact(self) -> None:
        state = self.make_state()
        state.consistency_status = 'FREEZE'
        state.freeze_reason = 'protective_order_missing'
        state.pending_execution_phase = 'frozen'
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[self.protective_order()],
                ),
            )
        )
        self.assertEqual(decision.status, 'OK')
        self.assertEqual(decision.freeze_reason, STOP_CONDITION_PROTECTION_TP_MISSING)
        self.assertNotEqual(decision.freeze_reason, 'protective_order_missing')
        self.assertEqual(decision.risk_action, 'RECOVER_PROTECTION')
        self.assertIn('exchange_protective_fact_visible', decision.notes)
        self.assertIn('stale_protective_missing_overridden_by_exchange_fact', decision.notes)

    def test_reconcile_treats_management_stop_update_missing_protective_as_pending_gap_not_freeze(self) -> None:
        state = self.make_state()
        state.pending_execution_phase = 'management_stop_update_pending_protective'
        state.stop_price = 2060.0
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[],
                ),
            )
        )
        self.assertEqual(decision.status, 'PENDING_ORDER')
        self.assertEqual(decision.freeze_reason, 'management_stop_update_pending_protective')
        self.assertEqual(decision.risk_action, 'OBSERVE')
        self.assertEqual(decision.recover_stage, 'protection_pending_confirm')
        self.assertIn('management_stop_update_protective_refresh_gap_observed', decision.notes)


    def test_reconcile_partial_protective_missing_keeps_same_manual_review_action(self) -> None:
        state = self.make_state()
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[
                        OrderSnapshot(
                            order_id='protect-stop-only',
                            client_order_id='protect-hard-stop',
                            status='NEW',
                            type='STOP_MARKET',
                            orig_type='STOP_MARKET',
                            time_in_force='GTC',
                            side='sell',
                            position_side='both',
                            qty=0.5,
                            executed_qty=0.0,
                            price=0.0,
                            avg_price=0.0,
                            cum_quote=0.0,
                            stop_price=2050.0,
                            working_type='MARK_PRICE',
                            activate_price=None,
                            price_protect=False,
                            reduce_only=True,
                            close_position=True,
                            update_time_ms=1711380000000,
                            raw={},
                        )
                    ],
                ),
            )
        )
        self.assertEqual(decision.status, 'OK')
        self.assertEqual(decision.risk_action, 'RECOVER_PROTECTION')
        self.assertEqual(decision.recover_stage, 'protection_partial_missing')
        self.assertEqual(decision.stop_condition, STOP_CONDITION_PROTECTION_TP_MISSING)
        self.assertIn('protection_tp_missing', decision.notes)
        self.assertIn('partial_protective_missing', decision.notes)

    def test_reconcile_partial_protective_missing_still_recognizes_realistic_stop_client_id_suffix(self) -> None:
        state = self.make_state()
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[
                        OrderSnapshot(
                            order_id='1000001362627129',
                            client_order_id='20260415T161129127320Z-stop',
                            status='NEW',
                            type='STOP_MARKET',
                            orig_type='STOP_MARKET',
                            time_in_force='GTC',
                            side='sell',
                            position_side='both',
                            qty=0.0,
                            executed_qty=0.0,
                            price=0.0,
                            avg_price=0.0,
                            cum_quote=0.0,
                            stop_price=2050.0,
                            working_type='MARK_PRICE',
                            activate_price=None,
                            price_protect=False,
                            reduce_only=True,
                            close_position=True,
                            update_time_ms=1711380000000,
                            raw={},
                        )
                    ],
                ),
            )
        )
        self.assertEqual(decision.status, 'OK')
        self.assertEqual(decision.stop_condition, STOP_CONDITION_PROTECTION_TP_MISSING)
        self.assertIn('partial_protective_missing', decision.notes)

    def test_reconcile_realistic_tp_missing_stop_snapshot_keeps_partial_missing_not_generic_missing(self) -> None:
        state = self.make_state()
        state.active_strategy = 'rev'
        state.tp_price = 2150.0
        decision = reconcile_pre_run(
            ReconcileInput(
                state=state,
                exchange=ExchangeSnapshot(
                    account=AccountSnapshot(account_equity=1000.0, available_margin=900.0, raw={}),
                    position=PositionSnapshot(symbol='ETHUSDT', side='long', qty=0.5, entry_price=2100.0, raw={}),
                    open_orders=[
                        OrderSnapshot(
                            order_id='1000001362989663',
                            client_order_id='20260415T165924360782Z-stop',
                            status='NEW',
                            type='STOP_MARKET',
                            orig_type='STOP_MARKET',
                            time_in_force='GTE_GTC',
                            side='sell',
                            position_side='both',
                            qty=0.0,
                            executed_qty=0.0,
                            price=0.0,
                            avg_price=0.0,
                            cum_quote=0.0,
                            stop_price=2050.0,
                            working_type='MARK_PRICE',
                            activate_price=None,
                            price_protect=False,
                            reduce_only=True,
                            close_position=True,
                            update_time_ms=1711380000000,
                            raw={},
                        )
                    ],
                ),
            )
        )
        self.assertEqual(decision.status, 'OK')
        self.assertEqual(decision.stop_condition, STOP_CONDITION_PROTECTION_TP_MISSING)
        self.assertEqual(decision.risk_action, 'RECOVER_PROTECTION')
        self.assertIn('protection_tp_missing', decision.notes)
        self.assertIn('partial_protective_missing', decision.notes)
        self.assertNotIn('protective_orders_invalid', decision.notes)


if __name__ == '__main__':
    unittest.main()
