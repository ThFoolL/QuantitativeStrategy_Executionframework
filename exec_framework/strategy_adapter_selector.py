from __future__ import annotations

from .runtime_env import BinanceEnvConfig
from .v6c_adapter import V6CLiveAdapter
from .v6c_la_free_v1_adapter import V6CLAFreeV1LiveAdapter

DEFAULT_STRATEGY_ADAPTER = 'la_free_v1'
SUPPORTED_STRATEGY_ADAPTERS = ('baseline', 'la_free_v1')


class UnsupportedStrategyAdapterError(ValueError):
    pass


def normalize_strategy_adapter_name(name: str | None) -> str:
    value = (name or DEFAULT_STRATEGY_ADAPTER).strip().lower().replace('-', '_')
    if value in {'v6c', 'baseline', 'v6c_baseline'}:
        return 'baseline'
    if value in {'la_free_v1', 'v6c_la_free_v1'}:
        return 'la_free_v1'
    raise UnsupportedStrategyAdapterError(
        f'unsupported strategy adapter: {name!r}; expected one of {SUPPORTED_STRATEGY_ADAPTERS}'
    )


def build_strategy_adapter(name: str | None = None):
    normalized = normalize_strategy_adapter_name(name)
    if normalized == 'baseline':
        return V6CLiveAdapter()
    if normalized == 'la_free_v1':
        return V6CLAFreeV1LiveAdapter()
    raise UnsupportedStrategyAdapterError(
        f'unsupported strategy adapter: {name!r}; expected one of {SUPPORTED_STRATEGY_ADAPTERS}'
    )


def build_strategy_adapter_from_config(config: BinanceEnvConfig):
    return build_strategy_adapter(config.strategy_adapter)
