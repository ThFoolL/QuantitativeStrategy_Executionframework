from __future__ import annotations

from .runtime_env import BinanceEnvConfig
from .v6c_adapter_baseline import V6CBaselineLiveAdapter
from .v6c_la_free_v1_adapter import V6CLAFreeV1LiveAdapter
from .v6c_adapter_wide_range_guard_w1 import V6CWideRangeGuardW1LiveAdapter

DEFAULT_STRATEGY_ADAPTER = 'baseline'
SUPPORTED_STRATEGY_ADAPTERS = ('baseline', 'la_free_v1', 'wide_range_guard_w1')


class UnsupportedStrategyAdapterError(ValueError):
    pass


def normalize_strategy_adapter_name(name: str | None) -> str:
    value = (name or DEFAULT_STRATEGY_ADAPTER).strip().lower().replace('-', '_')
    if value in {'v6c', 'baseline', 'v6c_baseline'}:
        return 'baseline'
    if value in {'la_free_v1', 'v6c_la_free_v1'}:
        return 'la_free_v1'
    if value in {'wide_range_guard_w1', 'v6c_wide_range_guard_w1', 'runtime_v6c_wide_range_guard_w1'}:
        return 'wide_range_guard_w1'
    raise UnsupportedStrategyAdapterError(
        f'unsupported strategy adapter: {name!r}; expected one of {SUPPORTED_STRATEGY_ADAPTERS}'
    )


def build_strategy_adapter(name: str | None = None):
    normalized = normalize_strategy_adapter_name(name)
    if normalized == 'baseline':
        return V6CBaselineLiveAdapter()
    if normalized == 'la_free_v1':
        return V6CLAFreeV1LiveAdapter()
    if normalized == 'wide_range_guard_w1':
        return V6CWideRangeGuardW1LiveAdapter()
    raise UnsupportedStrategyAdapterError(
        f'unsupported strategy adapter: {name!r}; expected one of {SUPPORTED_STRATEGY_ADAPTERS}'
    )


def build_strategy_adapter_from_config(config: BinanceEnvConfig):
    return build_strategy_adapter(config.strategy_adapter)
