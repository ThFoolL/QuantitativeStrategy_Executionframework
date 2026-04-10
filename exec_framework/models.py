from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MarketSnapshot:
    """Execution-layer market snapshot contract.

    Note:
    - This public schema intentionally allows private callers to attach strategy-specific
      feature blobs via generic dict fields.
    - Field names such as `signal_15m` / `trend_1h` / `rev_candidate` are legacy sample
      placeholders from extraction time, not recommended public strategy conventions.
    - New public integrations should treat these as opaque caller-owned payloads and may
      replace them with neutral names in their own adapter layer.
    """
    decision_ts: str
    bar_ts: str
    strategy_ts: Optional[str]
    execution_attributed_bar: Optional[str]
    symbol: str
    preclose_offset_seconds: int
    current_price: float
    source_status: str
    fast_5m: Dict[str, Any] = field(default_factory=dict)
    signal_15m: Dict[str, Any] = field(default_factory=dict)
    signal_15m_ts: Optional[str] = None
    trend_1h: Dict[str, Any] = field(default_factory=dict)
    trend_1h_ts: Optional[str] = None
    signal_15m_history: List[Dict[str, Any]] = field(default_factory=list)
    rev_candidate: Optional[Dict[str, Any]] = None


@dataclass
class LiveStateSnapshot:
    """Execution/runtime state snapshot shared across engine modules.

    Boundary notes:
    - Core execution fields are exchange/runtime/freeze/position facts.
    - Some optional fields below are legacy caller-owned strategy state carried through the
      execution layer for compatibility with the extracted private system.
    - Public users should treat strategy thresholds / buckets / trigger parameters here as
      opaque extension state, not framework recommendations or required defaults.
    """
    state_ts: str
    consistency_status: str
    freeze_reason: Optional[str]
    account_equity: float
    available_margin: float
    exchange_position_side: Optional[str]
    exchange_position_qty: float
    exchange_entry_price: Optional[float]
    active_strategy: str
    active_side: Optional[str]
    strategy_entry_time: Optional[str]
    strategy_entry_price: Optional[float]
    stop_price: Optional[float]
    risk_fraction: Optional[float]
    tp_price: Optional[float] = None
    hold_bars: int = 0
    rev_window: Optional[int] = None
    add_on_count: int = 0
    degrade_state: str = 'ATTACK'
    quality_bucket: str = 'MEDIUM'
    base_quantity: Optional[float] = None
    equity_at_entry: Optional[float] = None
    risk_amount: Optional[float] = None
    risk_per_unit: Optional[float] = None
    p1_armed: bool = False
    p2_armed: bool = False
    high_water_r: float = 0.0
    last_signal_bar: Optional[str] = None
    last_trend_signal_ts: Optional[str] = None
    last_conflict_resolution: Optional[str] = None
    can_open_new_position: bool = True
    can_modify_position: bool = True
    adx_long_threshold: float = 20.0
    adx_short_threshold: float = 22.0
    atr_rank_long_threshold: float = 0.45
    atr_rank_short_threshold: float = 0.55
    adx_trend_cont_long_threshold: float = 35.0
    atr_rank_trend_cont_long_threshold: float = 0.6
    adx_trend_cont_short_threshold: float = 28.0
    atr_rank_trend_cont_short_threshold: float = 0.5
    risk_fraction_medium: float = 0.1
    risk_fraction_high: float = 0.2
    risk_fraction_extreme: float = 0.3
    p1_trigger_r: float = 1.0
    p2_trigger_r: float = 2.0
    profit_defense_start_pct: float = 0.32
    profit_defense_giveback_pct: float = 0.33
    break_even_buffer: float = 0.002
    trim_fraction: float = 0.3
    add_trigger_r_first: float = 1.5
    add_trigger_r_second: float = 2.5
    runtime_mode: str = 'ACTIVE'
    freeze_status: str = 'NONE'
    last_freeze_reason: Optional[str] = None
    last_freeze_at: Optional[str] = None
    last_recover_at: Optional[str] = None
    last_recover_result: Optional[str] = None
    recover_attempt_count: int = 0
    pending_execution_phase: Optional[str] = None
    last_confirmed_order_ids: List[str] = field(default_factory=list)


@dataclass
class FinalActionPlan:
    """Strategy-to-executor intent contract.

    `target_strategy` and `conflict_context` are intentionally generic transport fields.
    Public framework consumers should interpret them as caller-defined metadata rather than
    built-in strategy taxonomy.
    """
    plan_ts: str
    bar_ts: str
    action_type: str
    target_strategy: Optional[str]
    target_side: Optional[str]
    reason: str
    qty_mode: str = 'none'
    qty: Optional[float] = None
    price_hint: Optional[float] = None
    stop_price: Optional[float] = None
    risk_fraction: Optional[float] = None
    conflict_context: Optional[Dict[str, Any]] = None
    requires_execution: bool = False
    close_reason: Optional[str] = None


@dataclass
class ExecutionResult:
    result_ts: str
    bar_ts: str
    status: str
    action_type: str
    executed_side: Optional[str]
    executed_qty: float = 0.0
    avg_fill_price: Optional[float] = None
    fees: float = 0.0
    exchange_order_ids: Optional[list[str]] = None
    post_position_side: Optional[str] = None
    post_position_qty: float = 0.0
    post_entry_price: Optional[float] = None
    reconcile_status: str = 'UNKNOWN'
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    should_freeze: bool = False
    freeze_reason: Optional[str] = None
    state_updates: Optional[Dict[str, Any]] = None
    execution_phase: str = 'none'
    confirmation_status: str = 'UNSPECIFIED'
    confirmed_order_status: Optional[str] = None
    trade_summary: Optional[Dict[str, Any]] = None
