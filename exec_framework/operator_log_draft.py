from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .binance_exception_policy import classify_binance_order_status

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from .binance_exception_policy import classify_binance_order_status
    from .binance_posttrade import BinancePostTradeConfirmer, PostTradeConfirmation, SimulatedExecutionReceipt
    from .executor_real import BinanceOrderRequest
    from .runtime_guard import build_readonly_recheck_recover_check
    from .runtime_status_cli import _build_operator_compact_view
except ImportError:  # pragma: no cover
    from exec_framework.binance_exception_policy import classify_binance_order_status
    from exec_framework.binance_posttrade import BinancePostTradeConfirmer, PostTradeConfirmation, SimulatedExecutionReceipt
    from exec_framework.executor_real import BinanceOrderRequest
    from exec_framework.runtime_guard import build_readonly_recheck_recover_check
    from exec_framework.runtime_status_cli import _build_operator_compact_view


class AdaptedFixtureReadOnlyClient:
    def __init__(self, scenario: dict[str, Any]):
        self.scenario = scenario

    def get_order(self, *, symbol=None, order_id=None, client_order_id=None):
        return type('Order', (), dict(self.scenario['order']))()

    def get_recent_trades(self, symbol=None, limit=100, order_id=None):
        rows = list(self.scenario.get('trades') or [])
        if order_id is None:
            return rows
        return [row for row in rows if str(row.get('orderId')) == str(order_id)]

    def get_position_snapshot(self, symbol=None):
        position = dict(self.scenario.get('position') or {})
        return type('Pos', (), position)()

    def get_open_orders(self, symbol=None):
        return [self._parse_open_order(row) for row in (self.scenario.get('open_orders') or [])]

    @staticmethod
    def _parse_open_order(row: dict[str, Any]):
        return type(
            'OpenOrder',
            (),
            {
                'order_id': str(row.get('orderId')),
                'client_order_id': row.get('clientOrderId'),
                'status': str(row.get('status', 'UNKNOWN')).upper(),
                'side': (str(row.get('side')).lower() if row.get('side') else None),
                'position_side': (str(row.get('positionSide')).lower() if row.get('positionSide') else None),
                'qty': float(row.get('origQty', 0.0)) if row.get('origQty') not in (None, '', 'NULL') else None,
                'executed_qty': float(row.get('executedQty', 0.0)) if row.get('executedQty') not in (None, '', 'NULL') else None,
                'price': float(row.get('price', 0.0)) if row.get('price') not in (None, '', 'NULL') else None,
                'avg_price': float(row.get('avgPrice', 0.0)) if row.get('avgPrice') not in (None, '', 'NULL') else None,
                'reduce_only': row.get('reduceOnly'),
                'close_position': row.get('closePosition'),
                'update_time_ms': int(row.get('updateTime')) if row.get('updateTime') not in (None, '', 'NULL') else None,
            },
        )()


class _Market:
    def __init__(self, symbol: str):
        self.symbol = symbol


def _make_request(payload: dict[str, Any]) -> BinanceOrderRequest:
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


def build_confirmation_from_adapted_fixture(adapted: dict[str, Any]) -> PostTradeConfirmation:
    scenario = adapted['posttrade_fixture']
    confirmer = BinancePostTradeConfirmer(AdaptedFixtureReadOnlyClient(scenario))
    return confirmer.confirm(
        market=_Market(scenario['request']['symbol']),
        order_requests=[_make_request(scenario['request'])],
        simulated_receipts=[SimulatedExecutionReceipt(**scenario['receipt'])],
    )


def build_operator_compact_view_from_confirmation(confirmation: PostTradeConfirmation) -> dict[str, Any]:
    readonly_status = None
    if confirmation.confirmation_category == 'confirmed' and confirmation.reconcile_status == 'OK' and not confirmation.should_freeze:
        readonly_status = 'readonly_recheck_recover_ready'
    elif confirmation.confirmation_category == 'pending':
        readonly_status = 'readonly_recheck_pending'
    elif confirmation.confirmation_category == 'query_failed':
        readonly_status = 'readonly_recheck_query_failed'
    elif confirmation.confirmation_category in {'mismatch', 'rejected'}:
        readonly_status = 'readonly_recheck_freeze'

    readonly_recheck = None if readonly_status is None else {
        'status': readonly_status,
        'action': 'recover_ready' if readonly_status == 'readonly_recheck_recover_ready' else ('observe' if readonly_status == 'readonly_recheck_pending' else 'freeze'),
        'freeze_reason': confirmation.freeze_reason,
    }
    recover_check = None if readonly_status is None else build_readonly_recheck_recover_check(
        decision={
            'status': readonly_status,
            'freeze_reason': confirmation.freeze_reason,
        }
    )
    return _build_operator_compact_view(
        runtime={'phase': 'completed'},
        submit_gate=None,
        freeze={
            'runtime_mode': 'FROZEN' if confirmation.should_freeze else 'ACTIVE',
            'freeze_status': 'ACTIVE' if confirmation.should_freeze else 'NONE',
            'freeze_reason': confirmation.freeze_reason,
            'last_recover_result': 'RECOVERED' if not confirmation.should_freeze else None,
        },
        confirm_summary={
            'confirmation_category': confirmation.confirmation_category,
            'confirmed_order_status': confirmation.order_status,
            'freeze_reason': confirmation.freeze_reason,
            'submit_exception_policy': classify_binance_order_status(confirmation.order_status).as_dict(),
            'readonly_recheck': readonly_recheck,
        },
        position={
            'exchange_position_side': confirmation.post_position_side,
            'exchange_position_qty': confirmation.post_position_qty,
        },
        recover_check=recover_check,
        recover_timeline=[recover_check] if recover_check is not None else None,
    )


def _build_discord_alert_preview(policy: dict[str, Any] | None) -> dict[str, Any]:
    policy = dict(policy or {})
    should_alert = bool(policy.get('should_alert')) if policy else False
    return {
        'channel_id': '1486034825830727710',
        'should_alert': should_alert,
        'would_send_now': False,
        'mode': 'preview_only_no_external_send',
        'reason': policy.get('reason'),
        'trigger': policy.get('action') or policy.get('policy'),
    }



def build_operator_log_draft_struct(
    adapted: dict[str, Any],
    confirmation: PostTradeConfirmation,
    compact_view: dict[str, Any],
) -> dict[str, Any]:
    scenario = adapted['posttrade_fixture']
    operator_context = adapted.get('operator_context') or {}
    source_meta = adapted.get('source_pack_meta') or {}
    trade_summary = confirmation.trade_summary or {}
    notes = list(confirmation.notes or [])
    requested_qty = trade_summary.get('requested_qty')
    executed_qty = trade_summary.get('executed_qty')
    open_orders_count = trade_summary.get('open_orders_count')
    facts = [
        f"order.status={confirmation.order_status}",
        f"requested_qty={requested_qty}",
        f"executed_qty={executed_qty}",
        f"position={confirmation.post_position_side or 'flat'}/{confirmation.post_position_qty}",
        f"open_orders_count={open_orders_count}",
    ]
    basis = [
        f"operator_context.confirmation_hint={operator_context.get('confirmation_candidate', {}).get('pack_confirmation_hint')}",
        f"confirm.confirmation_category={confirmation.confirmation_category}",
        f"compact.next_focus={compact_view.get('next_focus')}",
    ]
    order_policy = classify_binance_order_status(confirmation.order_status).as_dict()
    exception_policy_brief = compact_view.get('exception_policy_brief') or {
        'policy': order_policy.get('action') or order_policy.get('source_key'),
        'action': order_policy.get('action'),
        'reason': '; '.join(order_policy.get('notes') or []) or None,
        'next_action': (
            ' -> '.join(order_policy.get('auto_repair_steps') or [])
            if order_policy.get('auto_repair_steps')
            else (' / '.join(order_policy.get('readonly_checks') or []) if order_policy.get('readonly_checks') else None)
        ),
        'should_alert': order_policy.get('alert') != 'none',
        'alert': order_policy.get('alert'),
    }
    discord_alert_preview = _build_discord_alert_preview(exception_policy_brief)
    actions = [
        '补查 order / userTrades / positionRisk / openOrders',
        '补 runtime_status / event_log 留痕',
    ]
    if order_policy.get('action') == 'readonly_recheck' and order_policy.get('readonly_checks'):
        actions[0] = f"按异常策略补查 {' / '.join(order_policy.get('readonly_checks') or [])}"
    if confirmation.should_freeze:
        actions.append('维持 freeze，禁止按已恢复口径继续运行')
    else:
        actions.append('recover_ready 只代表恢复条件具备，不代表允许真实 submit / resubmit')
    if compact_view.get('recover_state'):
        actions.append('补 recover 前后事实对比')

    return {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'scenario_name': scenario.get('name'),
        'symbol': scenario['request']['symbol'],
        'event_type': source_meta.get('scenario_hint') or operator_context.get('event_type') or 'readonly_pack_import',
        'source_pack_meta': source_meta,
        'operator_context': operator_context,
        'confirm_summary': {
            'confirmation_status': confirmation.confirmation_status,
            'confirmation_category': confirmation.confirmation_category,
            'confirmed_order_status': confirmation.order_status,
            'freeze_reason': confirmation.freeze_reason,
            'requested_qty': requested_qty,
            'executed_qty': executed_qty,
            'open_orders_count': open_orders_count,
            'notes': notes,
            'submit_exception_policy': order_policy,
            'exception_policy_brief': exception_policy_brief,
            'discord_alert_preview': discord_alert_preview,
        },
        'operator_compact_view': compact_view,
        'facts': facts,
        'judgement_basis': basis,
        'operator_conclusion': {
            'human_conclusion': compact_view.get('next_focus'),
            'allow_run': 'keep_frozen' if confirmation.should_freeze else 'recover_ready_only_no_resubmit',
            'needs_escalation': confirmation.confirmation_category in {'query_failed', 'mismatch'},
            'should_alert_to_discord_monitor': discord_alert_preview['should_alert'],
            'discord_monitor_channel_id': discord_alert_preview['channel_id'],
        },
        'follow_up_actions': actions,
    }


def render_operator_log_markdown(draft: dict[str, Any]) -> str:
    confirm_summary = draft['confirm_summary']
    compact = draft['operator_compact_view']
    facts = draft.get('facts') or []
    basis = draft.get('judgement_basis') or []
    follow_up = draft.get('follow_up_actions') or []
    source_pack_meta = draft.get('source_pack_meta') or {}
    return '\n'.join(
        [
            '# 值班记录草稿（半自动生成）',
            '',
            f"- 生成时间：`{draft.get('generated_at')}`",
            f"- 场景：`{draft.get('scenario_name')}`",
            f"- 交易对：`{draft.get('symbol')}`",
            f"- source_label：`{source_pack_meta.get('source_label')}`",
            f"- scenario_hint：`{source_pack_meta.get('scenario_hint')}`",
            '',
            '## 1. 事实摘录',
            *[f'- {item}' for item in facts],
            '',
            '## 2. 判定结果',
            f"- confirmation_status：`{confirm_summary.get('confirmation_status')}`",
            f"- confirmation_category：`{confirm_summary.get('confirmation_category')}`",
            f"- freeze_reason：`{confirm_summary.get('freeze_reason')}`",
            f"- exception_policy.policy：`{((confirm_summary.get('exception_policy_brief') or {}).get('policy') or 'n/a')}`",
            f"- exception_policy.action：`{((confirm_summary.get('exception_policy_brief') or {}).get('action') or 'n/a')}`",
            f"- exception_policy.reason：`{((confirm_summary.get('exception_policy_brief') or {}).get('reason') or 'n/a')}`",
            f"- exception_policy.next_action：`{((confirm_summary.get('exception_policy_brief') or {}).get('next_action') or 'n/a')}`",
            f"- exception_policy.should_alert：`{((confirm_summary.get('exception_policy_brief') or {}).get('should_alert'))}`",
            f"- discord_monitor_channel：`{((confirm_summary.get('discord_alert_preview') or {}).get('channel_id') or '1486034825830727710')}`",
            f"- discord_monitor_would_send_now：`{((confirm_summary.get('discord_alert_preview') or {}).get('would_send_now'))}`",
            f"- recover_state：`{compact.get('recover_state') or 'n/a'}`",
            f"- next_focus：{compact.get('next_focus')}",
            "- recover_ready 口径：`只代表恢复条件具备，不代表允许真实 submit / resubmit`",
            '',
            '## 3. 判定依据',
            *[f'- {item}' for item in basis],
            '',
            '## 4. 后续动作',
            *[f'- [ ] {item}' for item in follow_up],
            '',
        ]
    ) + '\n'


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='根据 adapted readonly fixture 半自动生成 operator log draft')
    parser.add_argument('--adapted-in', required=True, help='adapt_readonly_pack 输出路径')
    parser.add_argument('--out', required=True, help='输出路径')
    parser.add_argument('--format', choices=['json', 'md'], default='md', help='输出格式')
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    adapted = _load_json(Path(args.adapted_in))
    confirmation = build_confirmation_from_adapted_fixture(adapted)
    compact_view = build_operator_compact_view_from_confirmation(confirmation)
    draft = build_operator_log_draft_struct(adapted, confirmation, compact_view)
    out_path = Path(args.out)
    if args.format == 'json':
        _write_json(out_path, draft)
    else:
        _write_text(out_path, render_operator_log_markdown(draft))
    print(json.dumps({'ok': True, 'out': str(out_path), 'format': args.format}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
