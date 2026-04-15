from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_posttrade import BinancePostTradeConfirmer, SimulatedExecutionReceipt
from exec_framework.binance_readonly_pack import adapt_readonly_pack
from exec_framework.executor_real import BinanceOrderRequest
from exec_framework.operator_log_draft import (
    AdaptedFixtureReadOnlyClient,
    build_operator_compact_view_from_confirmation,
    build_operator_log_draft_struct,
    render_operator_log_markdown,
)


class ReadonlyPackBridgeCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sample_dir = ROOT / 'docs' / 'deploy_v6c' / 'samples' / 'readonly_capture'
        cls.sample_dir = sample_dir
        cls.sample_pack = json.loads((sample_dir / 'readonly_sample_pack.template.json').read_text(encoding='utf-8'))
        cls.query_failed_pack = json.loads((sample_dir / 'readonly_pack_query_failed.template.json').read_text(encoding='utf-8'))
        cls.mismatch_pack = json.loads((sample_dir / 'readonly_pack_mismatch.template.json').read_text(encoding='utf-8'))
        cls.recover_ready_pack = json.loads((sample_dir / 'readonly_pack_recover_ready_like.template.json').read_text(encoding='utf-8'))
        cls.real_pack_1 = json.loads((sample_dir / '2026-03-26T074833Z_ETHUSDT_live_readonly_sample_pack.json').read_text(encoding='utf-8'))
        cls.real_pack_2 = json.loads((sample_dir / '2026-03-26T0756Z_ETHUSDT_live_readonly_sample_pack_2.json').read_text(encoding='utf-8'))
        cls.real_split_fill_pack = json.loads((ROOT / 'tests' / 'fixtures' / 'readonly_pack' / 'real_split_fill_close_pack.json').read_text(encoding='utf-8'))

    def make_market(self, symbol: str = 'ETHUSDT'):
        return type('Market', (), {'symbol': symbol})()

    @staticmethod
    def make_request(payload: dict) -> BinanceOrderRequest:
        return BinanceOrderRequest(
            symbol=payload['symbol'],
            side=payload['side'],
            order_type='MARKET',
            quantity=payload['quantity'],
            reduce_only=payload['reduce_only'],
            position_side=None,
            client_order_id=payload['client_order_id'],
            metadata={},
        )

    def confirm_from_adapted(self, adapted: dict):
        scenario = adapted['posttrade_fixture']
        confirmer = BinancePostTradeConfirmer(AdaptedFixtureReadOnlyClient(scenario))
        return confirmer.confirm(
            market=self.make_market(scenario['request']['symbol']),
            order_requests=[self.make_request(scenario['request'])],
            simulated_receipts=[SimulatedExecutionReceipt(**scenario['receipt'])],
        )

    def test_adapted_fixture_drives_confirmer_and_compact_view(self) -> None:
        adapted = adapt_readonly_pack(self.sample_pack, scenario_name='readonly_template_partial_cancel')
        self.assertTrue(adapted['validation']['ok'])
        scenario = adapted['posttrade_fixture']

        confirmation = self.confirm_from_adapted(adapted)

        self.assertEqual(confirmation.confirmation_status, 'UNCONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'rejected')
        self.assertEqual(confirmation.reconcile_status, 'POST_TRADE_MISMATCH')
        self.assertTrue(confirmation.should_freeze)
        self.assertEqual(confirmation.freeze_reason, 'posttrade_terminal_status_with_partial_fills')
        self.assertIn('terminal_status_with_partial_fills', confirmation.notes)
        self.assertEqual(confirmation.trade_summary['requested_qty'], 0.5)
        self.assertEqual(confirmation.trade_summary['executed_qty'], 0.2)
        self.assertEqual(confirmation.trade_summary['open_orders_count'], 0)
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'mismatch')

        compact = build_operator_compact_view_from_confirmation(confirmation)
        self.assertEqual(compact['confirmation_category'], 'rejected')
        self.assertEqual(compact['hard_blocker'], 'posttrade_terminal_status_with_partial_fills')
        self.assertEqual(compact['recover_state'], 'recover_blocked')
        self.assertIn('恢复仍被阻塞', compact['next_focus'])
        self.assertEqual(scenario['name'], 'readonly_template_partial_cancel')

    def test_adapted_fixture_with_pending_open_orders_enters_pending_freeze_path(self) -> None:
        pack = json.loads(json.dumps(self.sample_pack))
        pack['order']['status'] = 'PARTIALLY_FILLED'
        pack['open_orders'] = [
            {
                'order_id_masked': '911***77',
                'client_order_id_masked': 'cli***89',
                'status': 'NEW',
                'type': 'LIMIT',
                'timeInForce': 'GTC',
                'side': 'BUY',
                'positionSide': 'BOTH',
                'origQty': '0.3',
                'executedQty': '0',
                'price': '2098.8',
                'avgPrice': '0',
                'cumQuote': '0',
                'reduceOnly': False,
                'closePosition': False,
                'updateTime': 1711380011020,
            }
        ]
        adapted = adapt_readonly_pack(pack, scenario_name='readonly_template_pending_open_orders')
        confirmation = self.confirm_from_adapted(adapted)

        self.assertEqual(confirmation.confirmation_status, 'POSITION_CONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'position_confirmed')
        self.assertIsNone(confirmation.freeze_reason)
        self.assertFalse(confirmation.should_freeze)
        self.assertIn('partial_fill_position_working', confirmation.notes)
        self.assertIn('open_orders_still_pending', confirmation.notes)
        self.assertEqual(confirmation.trade_summary['open_orders_count'], 1)
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'pending')

        compact = build_operator_compact_view_from_confirmation(confirmation)
        self.assertEqual(compact['confirmation_category'], 'position_confirmed')
        self.assertIsNone(compact['hard_blocker'])
        self.assertEqual(compact['recover_state'], 'recover_ready')
        self.assertIn('当前可承认仓位已建立', compact['next_focus'])

    def test_query_failed_pack_maps_to_query_failed_hint_and_confirmation(self) -> None:
        adapted = adapt_readonly_pack(self.query_failed_pack, scenario_name='readonly_query_failed_bridge')
        self.assertTrue(adapted['validation']['ok'])
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'query_failed')

        confirmation = self.confirm_from_adapted(adapted)
        self.assertEqual(confirmation.confirmation_category, 'query_failed')
        self.assertEqual(confirmation.confirmation_status, 'UNCONFIRMED')
        self.assertTrue(confirmation.should_freeze)
        self.assertEqual(confirmation.freeze_reason, 'posttrade_missing_fills')
        self.assertIn('filled_without_user_trades', confirmation.notes)

        compact = build_operator_compact_view_from_confirmation(confirmation)
        self.assertEqual(compact['confirmation_category'], 'query_failed')
        self.assertEqual(compact['recover_state'], 'recover_blocked')
        self.assertIn('继续 freeze 并优先补查 order / userTrades / positionRisk / openOrders', compact['next_focus'])

    def test_mismatch_pack_maps_to_mismatch_hint_and_confirmation(self) -> None:
        adapted = adapt_readonly_pack(self.mismatch_pack, scenario_name='readonly_mismatch_bridge')
        self.assertTrue(adapted['validation']['ok'])
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'mismatch')

        confirmation = self.confirm_from_adapted(adapted)
        self.assertEqual(confirmation.confirmation_category, 'mismatch')
        self.assertEqual(confirmation.confirmation_status, 'UNCONFIRMED')
        self.assertTrue(confirmation.should_freeze)
        self.assertEqual(confirmation.freeze_reason, 'posttrade_position_changed_late_vs_fills')
        self.assertIn('filled_but_position_changed_late', confirmation.notes)

        compact = build_operator_compact_view_from_confirmation(confirmation)
        self.assertEqual(compact['confirmation_category'], 'mismatch')
        self.assertEqual(compact['recover_state'], 'recover_blocked')
        self.assertIn('恢复仍被阻塞', compact['next_focus'])

    def test_recover_ready_like_pack_maps_to_confirmed_and_operator_log_draft(self) -> None:
        adapted = adapt_readonly_pack(self.recover_ready_pack, scenario_name='readonly_recover_ready_bridge')
        self.assertTrue(adapted['validation']['ok'])
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'confirmed')

        confirmation = self.confirm_from_adapted(adapted)
        self.assertEqual(confirmation.confirmation_category, 'confirmed')
        self.assertEqual(confirmation.confirmation_status, 'CONFIRMED')
        self.assertFalse(confirmation.should_freeze)
        self.assertIsNone(confirmation.freeze_reason)

        compact = build_operator_compact_view_from_confirmation(confirmation)
        self.assertEqual(compact['confirmation_category'], 'confirmed')
        self.assertEqual(compact['freeze_status'], 'NONE')

        draft = build_operator_log_draft_struct(adapted, confirmation, compact)
        self.assertEqual(draft['confirm_summary']['confirmation_category'], 'confirmed')
        self.assertEqual(draft['operator_conclusion']['allow_run'], 'recover_ready_only_no_resubmit')
        self.assertFalse(draft['operator_conclusion']['needs_escalation'])
        self.assertEqual(draft['confirm_summary']['discord_alert_preview']['channel_id'], '1486034825830727710')
        self.assertFalse(draft['confirm_summary']['discord_alert_preview']['would_send_now'])
        self.assertTrue(isinstance(draft['confirm_summary']['exception_policy_brief']['should_alert'], bool))
        self.assertIn('补 recover 前后事实对比', draft['follow_up_actions'])

        markdown = render_operator_log_markdown(draft)
        self.assertIn('值班记录草稿（半自动生成）', markdown)
        self.assertIn('confirmation_category：`confirmed`', markdown)
        self.assertIn('discord_monitor_channel：`1486034825830727710`', markdown)
        self.assertIn('discord_monitor_would_send_now：`False`', markdown)
        self.assertIn('这只代表恢复条件具备', markdown)
        self.assertIn('不代表允许真实 submit / resubmit', markdown)

    def test_real_pack_1_directly_drives_confirmer_as_confirmed(self) -> None:
        adapted = adapt_readonly_pack(self.real_pack_1, scenario_name='real_pack_1_confirmed')
        self.assertTrue(adapted['validation']['ok'])
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'confirmed')

        confirmation = self.confirm_from_adapted(adapted)
        self.assertEqual(confirmation.confirmation_status, 'CONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'confirmed')
        self.assertEqual(confirmation.reconcile_status, 'OK')
        self.assertFalse(confirmation.should_freeze)
        self.assertIsNone(confirmation.freeze_reason)
        self.assertEqual(confirmation.trade_summary['requested_qty'], 1.5)
        self.assertEqual(confirmation.trade_summary['executed_qty'], 1.5)
        self.assertEqual(confirmation.trade_summary['open_orders_count'], 0)
        self.assertEqual(confirmation.post_position_qty, 0.0)
        self.assertEqual(confirmation.fill_count, 26)
        self.assertEqual(confirmation.fee_assets, ['USDT'])
        self.assertAlmostEqual(confirmation.avg_fill_price or 0.0, 2179.94476, places=5)

        compact = build_operator_compact_view_from_confirmation(confirmation)
        self.assertEqual(compact['confirmation_category'], 'confirmed')
        self.assertEqual(compact['recover_state'], 'recover_ready')
        self.assertIn('这只代表恢复条件具备', compact['next_focus'])

        draft = build_operator_log_draft_struct(adapted, confirmation, compact)
        self.assertEqual(draft['operator_conclusion']['allow_run'], 'recover_ready_only_no_resubmit')
        self.assertFalse(draft['operator_conclusion']['needs_escalation'])

    def test_real_pack_2_directly_drives_confirmer_as_confirmed(self) -> None:
        adapted = adapt_readonly_pack(self.real_pack_2, scenario_name='real_pack_2_confirmed')
        self.assertTrue(adapted['validation']['ok'])
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'confirmed')

        confirmation = self.confirm_from_adapted(adapted)
        self.assertEqual(confirmation.confirmation_status, 'CONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'confirmed')
        self.assertEqual(confirmation.reconcile_status, 'OK')
        self.assertFalse(confirmation.should_freeze)
        self.assertIsNone(confirmation.freeze_reason)
        self.assertEqual(confirmation.trade_summary['requested_qty'], 2.0)
        self.assertEqual(confirmation.trade_summary['executed_qty'], 2.0)
        self.assertEqual(confirmation.trade_summary['open_orders_count'], 0)
        self.assertEqual(confirmation.post_position_qty, 0.0)
        self.assertEqual(confirmation.fill_count, 14)
        self.assertEqual(confirmation.fee_assets, ['USDT'])
        self.assertAlmostEqual(confirmation.avg_fill_price or 0.0, 2101.43323, places=4)

        compact = build_operator_compact_view_from_confirmation(confirmation)
        self.assertEqual(compact['confirmation_category'], 'confirmed')
        self.assertEqual(compact['recover_state'], 'recover_ready')
        self.assertIn('这只代表恢复条件具备', compact['next_focus'])

        draft = build_operator_log_draft_struct(adapted, confirmation, compact)
        self.assertEqual(draft['operator_conclusion']['allow_run'], 'recover_ready_only_no_resubmit')
        self.assertFalse(draft['operator_conclusion']['needs_escalation'])

    def test_real_split_fill_close_pack_still_confirms_when_trades_sum_matches(self) -> None:
        adapted = adapt_readonly_pack(self.real_split_fill_pack, scenario_name='real_split_fill_close_confirmed')
        self.assertTrue(adapted['validation']['ok'])
        self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'confirmed')

        confirmation = self.confirm_from_adapted(adapted)
        self.assertEqual(confirmation.confirmation_status, 'POSITION_CONFIRMED')
        self.assertEqual(confirmation.confirmation_category, 'position_confirmed')
        self.assertEqual(confirmation.reconcile_status, 'OK')
        self.assertFalse(confirmation.should_freeze)
        self.assertIsNone(confirmation.freeze_reason)
        self.assertEqual(confirmation.fill_count, 2)
        self.assertEqual(confirmation.trade_summary['requested_qty'], 0.01)
        self.assertEqual(confirmation.trade_summary['executed_qty'], 0.01)
        self.assertEqual(confirmation.post_position_qty, 0.0)
        self.assertEqual(confirmation.fee_assets, ['USDT'])

    def test_real_packs_capture_true_field_shape_without_client_order_ids_in_trades(self) -> None:
        for pack in (self.real_pack_1, self.real_pack_2):
            adapted = adapt_readonly_pack(pack)
            self.assertTrue(adapted['validation']['ok'])
            self.assertTrue(all(row.get('client_order_id_masked') in (None, '') for row in pack['user_trades']))
            self.assertEqual(adapted['posttrade_fixture']['position']['qty'], 0.0)
            self.assertIsNone(adapted['posttrade_fixture']['position']['side'])
            self.assertEqual(adapted['operator_context']['confirmation_candidate']['pack_confirmation_hint'], 'confirmed')


if __name__ == '__main__':
    unittest.main()
