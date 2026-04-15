from __future__ import annotations

from dataclasses import asdict
from typing import Any, Protocol

from .models import ExecutionResult, FinalActionPlan, LiveStateSnapshot, MarketSnapshot


class StateStore(Protocol):
    def load_state(self) -> LiveStateSnapshot: ...
    def save_result(self, state: LiveStateSnapshot, result: ExecutionResult) -> None: ...


class StrategyModule(Protocol):
    def plan(self, market: MarketSnapshot, state: LiveStateSnapshot) -> FinalActionPlan: ...


class ExecutorModule(Protocol):
    def execute(self, plan: FinalActionPlan, market: MarketSnapshot, state: LiveStateSnapshot) -> ExecutionResult: ...


class PreRunReconcileModule(Protocol):
    def reconcile(self, market: MarketSnapshot, state: LiveStateSnapshot) -> LiveStateSnapshot: ...


def _collect_plan_debug(strategy_module: Any, market: MarketSnapshot, state: LiveStateSnapshot, plan: FinalActionPlan) -> dict[str, Any] | None:
    build_plan_debug = getattr(strategy_module, 'build_plan_debug', None)
    if not callable(build_plan_debug):
        return None
    debug_payload = build_plan_debug(market=market, state=state, final_plan=plan)
    return debug_payload if isinstance(debug_payload, dict) else None


class LiveEngine:
    def __init__(
        self,
        state_store: StateStore,
        strategy_module: StrategyModule,
        executor_module: ExecutorModule,
        pre_run_reconcile_module: PreRunReconcileModule | None = None,
    ):
        self.state_store = state_store
        self.strategy_module = strategy_module
        self.executor_module = executor_module
        self.pre_run_reconcile_module = pre_run_reconcile_module

    def _save_reconciled_state_if_supported(self, state: LiveStateSnapshot) -> None:
        save_state = getattr(self.state_store, 'save_state', None)
        if callable(save_state):
            save_state(state)

    def run_once(self, market: MarketSnapshot) -> dict:
        state = self.state_store.load_state()
        if self.pre_run_reconcile_module is not None:
            state = self.pre_run_reconcile_module.reconcile(market, state)
            self._save_reconciled_state_if_supported(state)
        plan = self.strategy_module.plan(market, state)
        plan_debug = _collect_plan_debug(self.strategy_module, market, state, plan)
        result = self.executor_module.execute(plan, market, state)
        self.state_store.save_result(state, result)
        updated_state = self.state_store.load_state()
        return {
            'market': asdict(market),
            'state': asdict(updated_state),
            'plan': asdict(plan),
            'plan_debug': plan_debug,
            'result': asdict(result),
        }
