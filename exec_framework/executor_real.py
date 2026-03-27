from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any

from .binance_exception_helpers import build_guarded_exception_plan
from .binance_exception_policy import classify_submit_exception_detail
from .binance_posttrade import BinancePostTradeConfirmer, PostTradeConfirmation, SimulatedExecutionReceipt
from .binance_readonly import BinanceReadOnlyClient, ExchangeSymbolRules
from .binance_submit import (
    BinanceSignedSubmitClient,
    BinanceSubmitError,
    BinanceSubmitReceipt,
)
from .models import ExecutionResult, FinalActionPlan, LiveStateSnapshot, MarketSnapshot
from .runtime_env import BinanceEnvConfig, LIVE_SUBMIT_MANUAL_ACK_TOKEN
from .runtime_guard import RuntimeFreezeController


@dataclass(frozen=True)
class BinanceOrderRequest:
    symbol: str
    side: str
    order_type: str
    quantity: float | None
    reduce_only: bool
    position_side: str | None
    client_order_id: str
    metadata: dict[str, Any]


class BinanceRealExecutor:
    """真实执行器接口骨架。

    当前阶段默认 dry-run / no-submit：
    - 先构造稳定的订单请求对象
    - 通过 submit gate 严格拦截真实发单
    - 预留 post-trade confirm 链路，但 submit disabled 时绝不伪装为 submitted / confirmed
    - 明确写出 execution_ref / request_context / confirm_context 供后续实盘接线复用
    """

    def __init__(self, config: BinanceEnvConfig, readonly_client: BinanceReadOnlyClient | None = None):
        self.config = config
        self.readonly_client = readonly_client or BinanceReadOnlyClient(config)
        self.posttrade_confirmer = BinancePostTradeConfirmer(self.readonly_client)
        self.freeze_controller = RuntimeFreezeController()
        self.submit_client = BinanceSignedSubmitClient(config=config, allow_live_submit_call=False)

    def execute(self, plan: FinalActionPlan, market: MarketSnapshot, state: LiveStateSnapshot) -> ExecutionResult:
        if state.runtime_mode == 'FROZEN':
            result = ExecutionResult(
                result_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                status='FROZEN',
                action_type=plan.action_type,
                executed_side=plan.target_side,
                reconcile_status=state.consistency_status,
                should_freeze=True,
                freeze_reason=state.freeze_reason or state.last_freeze_reason,
                state_updates={
                    'runtime_mode': 'FROZEN',
                    'freeze_status': 'ACTIVE',
                    'pending_execution_phase': 'frozen',
                    'can_open_new_position': False,
                    'can_modify_position': False,
                },
                execution_phase='frozen',
                confirmation_status='FROZEN',
                confirmed_order_status='FROZEN',
                trade_summary={
                    'confirmation_category': 'mismatch',
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'submit_gate': self._build_submit_gate_context(),
                },
            )
            return self._apply_freeze_if_needed(state, result)

        if not plan.requires_execution:
            return ExecutionResult(
                result_ts=market.decision_ts,
                bar_ts=market.bar_ts,
                status='SKIPPED',
                action_type=plan.action_type,
                executed_side=plan.target_side,
                reconcile_status=state.consistency_status,
                should_freeze=False,
                state_updates={'pending_execution_phase': 'none'},
                execution_phase='none',
                confirmation_status='NOT_REQUIRED',
                confirmed_order_status='NOT_REQUIRED',
                trade_summary={
                    'confirmation_category': 'mismatch',
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'submit_gate': self._build_submit_gate_context(),
                },
            )

        symbol_rules = self.readonly_client.get_exchange_info(market.symbol)
        order_requests = self._build_order_requests(plan, market, state, symbol_rules)
        draft_ids = [request.client_order_id for request in order_requests]
        request_context = self._build_request_context(
            plan=plan,
            market=market,
            state=state,
            order_requests=order_requests,
        )

        submit_gate = self._evaluate_submit_gate(
            market=market,
            state=state,
            order_requests=order_requests,
            blocked_reason='dry_run_or_submit_disabled',
        )
        if self.config.dry_run or not self.config.submit_enabled or not submit_gate['submit_allowed']:
            confirmation = PostTradeConfirmation(
                confirmation_status='UNCONFIRMED',
                confirmation_category='mismatch',
                order_status='NOT_SUBMITTED',
                exchange_order_ids=draft_ids,
                executed_qty=0.0,
                avg_fill_price=None,
                fees=0.0,
                fee_assets=[],
                fill_count=0,
                post_position_side=state.exchange_position_side,
                post_position_qty=state.exchange_position_qty,
                post_entry_price=state.exchange_entry_price,
                reconcile_status='DRY_RUN',
                should_freeze=False,
                freeze_reason=None,
                notes=['dry_run_no_submit'],
                trade_summary={
                    'execution_ref': self._build_execution_ref(plan=plan, market=market),
                    'request_context': request_context,
                    'submit_gate': submit_gate,
                    'confirm_context': {
                        'confirm_attempted': False,
                        'confirm_path': 'skipped_because_not_submitted',
                    },
                    'draft_order_requests': [self._serialize_order_request(request) for request in order_requests],
                    'submitted': False,
                },
            )
            result = self._build_execution_result_from_confirmation(
                market=market,
                plan=plan,
                confirmation=confirmation,
                status='DRY_RUN',
                execution_phase='planned',
                error_code='NO_SUBMIT',
                error_message=f'draft_order_requests={draft_ids}',
            )
            return self._apply_freeze_if_needed(state, result)

        submit_receipts = self._submit_orders(order_requests)
        confirmation = self.posttrade_confirmer.confirm(
            market=market,
            order_requests=order_requests,
            simulated_receipts=submit_receipts,
        )
        result = self._build_execution_result_from_confirmation(
            market=market,
            plan=plan,
            confirmation=confirmation,
            status=confirmation.order_status,
            execution_phase=self._map_execution_phase(confirmation),
            error_code=None,
            error_message=None,
        )
        return self._apply_freeze_if_needed(state, result)

    def _submit_orders(self, order_requests: list[BinanceOrderRequest]) -> list[SimulatedExecutionReceipt]:
        receipts: list[SimulatedExecutionReceipt] = []
        for request in order_requests:
            payload = self._serialize_submit_payload(request)
            signed_request = self.submit_client.build_submit_request(payload, metadata=dict(request.metadata or {}))
            prepared = self.submit_client.prepare_signed_post(signed_request)
            try:
                _, receipt = self.submit_client.submit_order(signed_request)
                receipts.append(self._receipt_from_submit_receipt(receipt))
            except BinanceSubmitError as exc:
                policy = classify_submit_exception_detail(getattr(exc, 'detail', None))
                helper_plan = build_guarded_exception_plan(
                    policy,
                    runtime_mode='ACTIVE',
                    manual_ack_present=bool((self.config.submit_manual_ack_token or '').strip()),
                    automation_enabled=False,
                )
                exception_metadata = self._build_submit_exception_metadata(
                    category=exc.category,
                    submit_gate=self.submit_client.gate_context(),
                    exception_policy=policy.as_dict(),
                    exception_policy_view=helper_plan.policy,
                    exception_helper_plan=helper_plan.as_dict(),
                )
                receipts.append(
                    SimulatedExecutionReceipt(
                        client_order_id=request.client_order_id,
                        exchange_order_id=None,
                        acknowledged=False,
                        submitted_qty=request.quantity,
                        submitted_side=request.side,
                        submit_status='BLOCKED' if exc.category == 'submit_gate_blocked' else 'DISABLED',
                        exchange_status=None,
                        transact_time_ms=None,
                        request_payload=prepared.body_redacted,
                        response_payload=None,
                        metadata={
                            'skeleton_only': True,
                            'future_endpoint': '/fapi/v1/order',
                            **exception_metadata,
                        },
                        error_code=exc.category.upper(),
                        error_message=str(exc),
                    )
                )
                raise NotImplementedError(
                    f'real order submission remains intentionally unreachable; '
                    f'category={exc.category}; client_order_id={request.client_order_id}; '
                    f'policy_action={policy.action}; policy_alert={policy.alert}'
                ) from exc
        return receipts

    @staticmethod
    def _build_submit_exception_metadata(
        *,
        category: str,
        submit_gate: dict[str, Any],
        exception_policy: dict[str, Any],
        exception_policy_view: dict[str, Any],
        exception_helper_plan: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            'submit_exception_category': category,
            'submit_gate': submit_gate,
            'exception_policy': exception_policy,
            'exception_policy_view': exception_policy_view,
            'exception_helper_plan': exception_helper_plan,
        }

    def _build_execution_result_from_confirmation(
        self,
        *,
        market: MarketSnapshot,
        plan: FinalActionPlan,
        confirmation: PostTradeConfirmation,
        status: str,
        execution_phase: str,
        error_code: str | None,
        error_message: str | None,
    ) -> ExecutionResult:
        return ExecutionResult(
            result_ts=market.decision_ts,
            bar_ts=market.bar_ts,
            status=status,
            action_type=plan.action_type,
            executed_side=plan.target_side,
            executed_qty=confirmation.executed_qty,
            avg_fill_price=confirmation.avg_fill_price,
            fees=confirmation.fees,
            exchange_order_ids=confirmation.exchange_order_ids,
            post_position_side=confirmation.post_position_side,
            post_position_qty=confirmation.post_position_qty,
            post_entry_price=confirmation.post_entry_price,
            reconcile_status=confirmation.reconcile_status,
            error_code=error_code,
            error_message=error_message,
            should_freeze=confirmation.should_freeze,
            freeze_reason=confirmation.freeze_reason,
            state_updates={
                'pending_execution_phase': execution_phase,
                'last_confirmed_order_ids': confirmation.exchange_order_ids,
            },
            execution_phase=execution_phase,
            confirmation_status=confirmation.confirmation_status,
            confirmed_order_status=confirmation.order_status,
            trade_summary={
                **(confirmation.trade_summary or {}),
                'confirmation_category': confirmation.confirmation_category,
                'submit_exception_policy': (confirmation.trade_summary or {}).get('submit_exception_policy'),
                'submit_exception_metadata': (confirmation.trade_summary or {}).get('submit_exception_metadata'),
                'exception_policy_view': (confirmation.trade_summary or {}).get('exception_policy_view'),
                'exception_helper_plan': (confirmation.trade_summary or {}).get('exception_helper_plan'),
            },
        )

    def _apply_freeze_if_needed(self, state: LiveStateSnapshot, result: ExecutionResult) -> ExecutionResult:
        decision = self.freeze_controller.freeze_from_result(state, result)
        if not decision.should_freeze:
            return result
        merged_updates = dict(result.state_updates or {})
        merged_updates.update(decision.state_updates)
        return ExecutionResult(
            result_ts=result.result_ts,
            bar_ts=result.bar_ts,
            status='FROZEN' if result.status not in {'FROZEN'} else result.status,
            action_type=result.action_type,
            executed_side=result.executed_side,
            executed_qty=result.executed_qty,
            avg_fill_price=result.avg_fill_price,
            fees=result.fees,
            exchange_order_ids=result.exchange_order_ids,
            post_position_side=result.post_position_side,
            post_position_qty=result.post_position_qty,
            post_entry_price=result.post_entry_price,
            reconcile_status=result.reconcile_status,
            error_code=result.error_code,
            error_message=result.error_message,
            should_freeze=True,
            freeze_reason=decision.freeze_reason,
            state_updates=merged_updates,
            execution_phase='frozen',
            confirmation_status=result.confirmation_status,
            confirmed_order_status=result.confirmed_order_status,
            trade_summary=result.trade_summary,
        )

    def _map_execution_phase(self, confirmation: PostTradeConfirmation) -> str:
        if confirmation.should_freeze:
            return 'frozen'
        if confirmation.confirmation_status == 'CONFIRMED':
            return 'confirmed'
        if confirmation.confirmation_status == 'PENDING':
            return 'submitted'
        return 'submitted'

    def _build_order_requests(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
    ) -> list[BinanceOrderRequest]:
        if plan.action_type == 'flip':
            return self._build_flip_requests(plan, market, state, symbol_rules)
        return [self._build_single_order_request(plan, market, state, symbol_rules)]

    def _build_flip_requests(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
    ) -> list[BinanceOrderRequest]:
        requests: list[BinanceOrderRequest] = []
        if state.exchange_position_side and state.exchange_position_qty > 0:
            close_side = 'SELL' if state.exchange_position_side == 'long' else 'BUY'
            qty = self._normalize_quantity(state.exchange_position_qty, symbol_rules)
            requests.append(
                BinanceOrderRequest(
                    symbol=market.symbol,
                    side=close_side,
                    order_type='MARKET',
                    quantity=qty,
                    reduce_only=True,
                    position_side=None,
                    client_order_id=f'{market.bar_ts}-flip-close',
                    metadata={'phase': 'close_existing', 'close_reason': plan.close_reason},
                )
            )
        requests.append(self._build_single_order_request(plan, market, state, symbol_rules, client_order_suffix='flip-open'))
        return requests

    def _build_single_order_request(
        self,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        symbol_rules: ExchangeSymbolRules,
        *,
        client_order_suffix: str | None = None,
    ) -> BinanceOrderRequest:
        action_type = plan.action_type
        if action_type not in {'open', 'close', 'add', 'trim', 'flip'}:
            raise ValueError(f'unsupported executable action_type: {action_type}')

        if plan.target_side not in {'long', 'short'} and action_type != 'close':
            raise ValueError(f'unsupported target_side: {plan.target_side}')

        reduce_only = action_type in {'close', 'trim'}
        if action_type == 'flip':
            reduce_only = False

        side = self._resolve_order_side(action_type, plan, state)
        raw_quantity = self._resolve_order_quantity(action_type, plan, state)
        quantity = self._normalize_quantity(raw_quantity, symbol_rules)
        if quantity is not None:
            self._validate_quantity(quantity, symbol_rules, market)

        suffix = client_order_suffix or action_type
        return BinanceOrderRequest(
            symbol=market.symbol,
            side=side,
            order_type='MARKET',
            quantity=quantity,
            reduce_only=reduce_only,
            position_side=None,
            client_order_id=f'{market.bar_ts}-{suffix}',
            metadata={
                'reason': plan.reason,
                'qty_mode': plan.qty_mode,
                'price_hint': plan.price_hint,
                'stop_price': plan.stop_price,
            },
        )

    def _resolve_order_side(self, action_type: str, plan: FinalActionPlan, state: LiveStateSnapshot) -> str:
        if action_type in {'open', 'add', 'flip'}:
            return 'BUY' if plan.target_side == 'long' else 'SELL'
        if action_type in {'close', 'trim'}:
            close_side = state.exchange_position_side or state.active_side
            if close_side == 'long':
                return 'SELL'
            if close_side == 'short':
                return 'BUY'
            raise ValueError('close/trim requires an existing position side')
        raise ValueError(f'unsupported action_type for side resolution: {action_type}')

    def _resolve_order_quantity(self, action_type: str, plan: FinalActionPlan, state: LiveStateSnapshot) -> float | None:
        if action_type in {'close', 'flip'}:
            return state.exchange_position_qty or state.base_quantity
        if action_type == 'trim':
            if state.exchange_position_qty > 0 and plan.qty is not None:
                return state.exchange_position_qty * float(plan.qty)
            if state.base_quantity is not None and plan.qty is not None:
                return float(state.base_quantity) * float(plan.qty)
            return None
        if plan.qty is not None:
            return float(plan.qty)
        return state.base_quantity

    def _normalize_quantity(self, quantity: float | None, symbol_rules: ExchangeSymbolRules) -> float | None:
        if quantity is None:
            return None
        if symbol_rules.qty_step is None or symbol_rules.qty_step <= 0:
            return float(quantity)
        step = Decimal(str(symbol_rules.qty_step))
        normalized = (Decimal(str(quantity)) / step).quantize(Decimal('1'), rounding=ROUND_DOWN) * step
        return float(normalized)

    def _validate_quantity(self, quantity: float, symbol_rules: ExchangeSymbolRules, market: MarketSnapshot) -> None:
        if symbol_rules.min_qty is not None and quantity < symbol_rules.min_qty:
            raise ValueError(f'quantity below min_qty: {quantity} < {symbol_rules.min_qty}')
        if symbol_rules.min_notional is not None:
            mark_price = float(plan_price_or_market_price(None, market))
            if quantity * mark_price < symbol_rules.min_notional:
                raise ValueError(
                    f'notional below min_notional: {quantity * mark_price} < {symbol_rules.min_notional}'
                )

    def _build_execution_ref(self, *, plan: FinalActionPlan, market: MarketSnapshot) -> dict[str, Any]:
        return {
            'symbol': market.symbol,
            'decision_ts': market.decision_ts,
            'bar_ts': market.bar_ts,
            'plan_ts': plan.plan_ts,
            'action_type': plan.action_type,
            'target_side': plan.target_side,
        }

    def _build_submit_gate_context(self, *, blocked_reason: str | None = None) -> dict[str, Any]:
        context = self.submit_client.gate_context()
        context['submit_allowed'] = bool(context.get('http_post_allowed'))
        context['blocked_reason'] = blocked_reason
        return context

    def _evaluate_submit_gate(
        self,
        *,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        order_requests: list[BinanceOrderRequest],
        blocked_reason: str | None = None,
    ) -> dict[str, Any]:
        gate = self._build_submit_gate_context(blocked_reason=blocked_reason)
        guardrail_blockers: list[str] = []

        allowlist = tuple(item.upper() for item in self.config.submit_symbol_allowlist)
        effective_symbol_allowlist = allowlist or (self.config.symbol.upper(),)
        if market.symbol.upper() != self.config.symbol.upper():
            guardrail_blockers.append(f'config_symbol_mismatch:{market.symbol}!={self.config.symbol}')
        if market.symbol.upper() not in effective_symbol_allowlist:
            guardrail_blockers.append(f'symbol_not_allowed:{market.symbol}')

        total_qty = sum(float(item.quantity or 0.0) for item in order_requests)
        total_notional = sum(float(item.quantity or 0.0) * float(market.current_price or 0.0) for item in order_requests)

        if self.config.submit_max_qty is not None and total_qty > float(self.config.submit_max_qty):
            guardrail_blockers.append(f'max_qty_exceeded:{total_qty}>{self.config.submit_max_qty}')
        if self.config.submit_max_notional is not None and total_notional > float(self.config.submit_max_notional):
            guardrail_blockers.append(f'max_notional_exceeded:{total_notional}>{self.config.submit_max_notional}')
        if self.config.submit_require_reconcile_ok and state.consistency_status != 'OK':
            guardrail_blockers.append(f'consistency_not_ok:{state.consistency_status}')
        if self.config.submit_require_active_runtime and state.runtime_mode != 'ACTIVE':
            guardrail_blockers.append(f'runtime_not_active:{state.runtime_mode}')
        if self.config.submit_require_no_pending_execution and state.pending_execution_phase not in {None, 'none', 'confirmed'}:
            guardrail_blockers.append(f'pending_execution_phase:{state.pending_execution_phase}')
        if not self.config.discord_audit_enabled:
            guardrail_blockers.append('discord_audit_disabled')
        if (self.config.submit_manual_ack_token or '').strip() != LIVE_SUBMIT_MANUAL_ACK_TOKEN:
            guardrail_blockers.append('manual_ack_missing_or_invalid')

        gate['guardrail_checks'] = {
            'symbol_allowlist': list(effective_symbol_allowlist),
            'config_symbol': self.config.symbol.upper(),
            'symbol_allowed': market.symbol.upper() in effective_symbol_allowlist,
            'max_qty': self.config.submit_max_qty,
            'total_qty': total_qty,
            'max_notional': self.config.submit_max_notional,
            'total_notional': total_notional,
            'require_reconcile_ok': self.config.submit_require_reconcile_ok,
            'consistency_status': state.consistency_status,
            'require_active_runtime': self.config.submit_require_active_runtime,
            'runtime_mode': state.runtime_mode,
            'require_no_pending_execution': self.config.submit_require_no_pending_execution,
            'pending_execution_phase': state.pending_execution_phase,
            'discord_audit_enabled': self.config.discord_audit_enabled,
            'manual_ack_present': bool((self.config.submit_manual_ack_token or '').strip()),
            'manual_ack_valid': (self.config.submit_manual_ack_token or '').strip() == LIVE_SUBMIT_MANUAL_ACK_TOKEN,
        }
        gate['guardrail_blockers'] = guardrail_blockers
        gate['submit_allowed'] = bool(gate.get('http_post_allowed')) and not guardrail_blockers
        if guardrail_blockers:
            gate['blocked_reason'] = guardrail_blockers[0]
        return gate

    def _build_request_context(
        self,
        *,
        plan: FinalActionPlan,
        market: MarketSnapshot,
        state: LiveStateSnapshot,
        order_requests: list[BinanceOrderRequest],
    ) -> dict[str, Any]:
        return {
            'plan_action_type': plan.action_type,
            'plan_reason': plan.reason,
            'market_symbol': market.symbol,
            'market_price_hint': market.current_price,
            'state_position_side': state.exchange_position_side,
            'state_position_qty': state.exchange_position_qty,
            'request_count': len(order_requests),
            'client_order_ids': [item.client_order_id for item in order_requests],
            'request_payloads': [self._serialize_submit_payload(item) for item in order_requests],
        }

    @staticmethod
    def _serialize_order_request(request: BinanceOrderRequest) -> dict[str, Any]:
        return {
            'symbol': request.symbol,
            'side': request.side,
            'order_type': request.order_type,
            'quantity': request.quantity,
            'reduce_only': request.reduce_only,
            'position_side': request.position_side,
            'client_order_id': request.client_order_id,
            'metadata': dict(request.metadata or {}),
        }

    @staticmethod
    def _serialize_submit_payload(request: BinanceOrderRequest) -> dict[str, Any]:
        payload = {
            'symbol': request.symbol,
            'side': request.side,
            'type': request.order_type,
            'newClientOrderId': request.client_order_id,
        }
        if request.quantity is not None:
            payload['quantity'] = request.quantity
        if request.reduce_only:
            payload['reduceOnly'] = 'true'
        if request.position_side is not None:
            payload['positionSide'] = request.position_side.upper()
        return payload

    @staticmethod
    def _receipt_from_submit_receipt(receipt: BinanceSubmitReceipt) -> SimulatedExecutionReceipt:
        return SimulatedExecutionReceipt(
            client_order_id=receipt.client_order_id,
            exchange_order_id=receipt.exchange_order_id,
            acknowledged=receipt.acknowledged,
            submitted_qty=receipt.submitted_qty,
            submitted_side=receipt.submitted_side,
            submit_status=receipt.submit_status,
            exchange_status=receipt.exchange_status,
            transact_time_ms=receipt.transact_time_ms,
            request_payload=receipt.request_payload,
            response_payload=receipt.response_payload,
            metadata=receipt.metadata,
            error_code=receipt.error_code,
            error_message=receipt.error_message,
        )


def plan_price_or_market_price(price_hint: float | None, market: MarketSnapshot) -> float:
    if price_hint is not None:
        return float(price_hint)
    return float(market.current_price)
