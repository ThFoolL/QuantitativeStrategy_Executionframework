from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol


@dataclass
class _KlineCacheState:
    symbol: str
    interval: str
    bars: list[Any]

from .feature_builder import LiveFeatureBuilder
from .models import MarketSnapshot


@dataclass(frozen=True)
class MarketDataPoint:
    ts: str
    values: dict[str, Any]


@dataclass(frozen=True)
class MarketFrameBundle:
    symbol: str
    decision_ts: str
    current_price: float
    fast_5m: dict[str, Any]
    signal_15m: dict[str, Any]
    signal_15m_ts: str | None
    trend_1h: dict[str, Any]
    trend_1h_ts: str | None
    signal_15m_history: list[dict[str, Any]]
    rev_candidate: dict[str, Any] | None = None
    source_status: str = 'OK'
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class SnapshotTimeSemantics:
    strategy_ts: str
    decision_ts: str
    execution_attributed_bar: str
    preclose_offset_seconds: int


class MarketDataProvider(Protocol):
    def load(self, *, symbol: str, decision_time: datetime) -> MarketFrameBundle:
        """返回构造 MarketSnapshot 所需的统一数据帧。"""


class StubMarketDataProvider:
    """本地 dry-run / unittest 用的最小 provider。"""

    def __init__(self, *, source_status: str = 'STUB'):
        self.source_status = source_status

    def load(self, *, symbol: str, decision_time: datetime) -> MarketFrameBundle:
        if decision_time.tzinfo is None:
            decision_time = decision_time.replace(tzinfo=timezone.utc)
        strategy_bar = align_timeframe(decision_time, minutes=5)
        signal_bar = align_timeframe(decision_time, minutes=15)
        trend_bar = align_timeframe(decision_time, minutes=60)
        strategy_ts = strategy_bar.isoformat()
        signal_ts = signal_bar.isoformat()
        trend_ts = trend_bar.isoformat()
        fast_close = 2000.0
        signal_close = 2001.0
        history = [
            {'close': 1990.0, 'low': 1988.0, 'high': 1992.0},
            {'close': 1995.0, 'low': 1993.0, 'high': 1997.0},
            {'close': 1998.0, 'low': 1996.0, 'high': 2000.0},
            {'close': signal_close, 'low': 1999.0, 'high': 2002.0},
        ]
        return MarketFrameBundle(
            symbol=symbol,
            decision_ts=decision_time.isoformat(),
            current_price=fast_close,
            fast_5m={'close': fast_close, 'low': 1999.0, 'high': 2001.0},
            signal_15m={'close': signal_close, 'low': 1999.0, 'high': 2002.0},
            signal_15m_ts=signal_ts,
            trend_1h={
                'close': 2005.0,
                'ema_fast': 1998.0,
                'ema_slow': 1988.0,
                'adx': 32.0,
                'atr_rank': 0.62,
                'structure_tag': 'TREND_CONT',
            },
            trend_1h_ts=trend_ts,
            signal_15m_history=history,
            rev_candidate=None,
            source_status=self.source_status,
            metadata={'provider': 'stub', 'strategy_ts': strategy_ts},
        )


class BinanceReadOnlyMarketDataProvider:
    """真实只读行情 provider。

    当前阶段仅落最小安全实现：
    - 启动/首次访问时对 5m / 15m / 1h 做一次 warmup 拉齐
    - 运行时优先基于内存缓存做小窗口增量补齐，而不是每轮全量重拉
    - 构造 `fast_5m / signal_15m / signal_15m_history / trend_1h`
    - 统一执行层时间语义：`strategy_ts/bar_ts/execution_attributed_bar` 全部对齐正式 5m bar

    重要限制：
    - 当前 feature builder 只复用了回测里的最小指标公式，并未完整复刻全部 live/backtest 特征工程。
    - `rev_candidate` 目前是启发式近似，仅可作为 live 最小占位输入，不应宣称正式对齐回测信号。
    - 当前缓存仍是进程内缓存；重启后会重新 warmup，不做持久化。
    """

    def __init__(
        self,
        readonly_client: Any,
        *,
        feature_builder: Any | None = None,
        warmup_limit: int = 150,
        refresh_window_limit: int = 8,
    ):
        self.readonly_client = readonly_client
        self.feature_builder = feature_builder or LiveFeatureBuilder()
        self.warmup_limit = max(32, int(warmup_limit))
        self.refresh_window_limit = max(2, int(refresh_window_limit))
        self._kline_cache: dict[tuple[str, str], _KlineCacheState] = {}

    def load(self, *, symbol: str, decision_time: datetime) -> MarketFrameBundle:
        if decision_time.tzinfo is None:
            decision_time = decision_time.replace(tzinfo=timezone.utc)

        strategy_bar = align_timeframe(decision_time, minutes=5)

        fast_klines = self._get_klines_for_snapshot(symbol=symbol, interval='5m', strategy_bar=strategy_bar)
        signal_klines = self._get_klines_for_snapshot(symbol=symbol, interval='15m', strategy_bar=strategy_bar)
        trend_klines = self._get_klines_for_snapshot(symbol=symbol, interval='1h', strategy_bar=strategy_bar)

        fast_bar = self._last_closed_at_or_before(fast_klines, strategy_bar)
        signal_bar = self._last_closed_at_or_before(signal_klines, strategy_bar)
        trend_bar = self._last_closed_at_or_before(trend_klines, strategy_bar)
        signal_history_bars = self._tail_closed_at_or_before(signal_klines, strategy_bar, size=4)

        if fast_bar is None or signal_bar is None or trend_bar is None or len(signal_history_bars) < 4:
            raise ValueError('insufficient closed klines for readonly market snapshot')

        signal_closed_bars = self._closed_at_or_before(signal_klines, strategy_bar)
        trend_closed_bars = self._closed_at_or_before(trend_klines, strategy_bar)
        trend_features = self._build_trend_features(symbol=symbol, trend_bars=trend_closed_bars)
        rev_candidate = self._build_rev_candidate(symbol=symbol, signal_bars=signal_closed_bars, trend_bars=trend_closed_bars)
        # `rev_candidate` 为空通常表示当前没有反转机会，不应被记成数据/特征缺失。
        feature_flags = trend_features['missing_fields'][:]
        source_status = 'OK' if not feature_flags else 'PARTIAL_FEATURES'

        return MarketFrameBundle(
            symbol=symbol,
            decision_ts=decision_time.isoformat(),
            current_price=float(fast_bar.close_price),
            fast_5m=self._kline_to_ohlcv_dict(fast_bar),
            signal_15m=self._kline_to_ohlcv_dict(signal_bar),
            signal_15m_ts=self._bar_open_iso(signal_bar.open_time_ms),
            trend_1h=trend_features['values'],
            trend_1h_ts=self._bar_open_iso(trend_bar.open_time_ms),
            signal_15m_history=[self._kline_to_signal_history_dict(item) for item in signal_history_bars],
            rev_candidate=rev_candidate,
            source_status=source_status,
            metadata={
                'provider': 'binance_readonly',
                'public_endpoints': ['/fapi/v1/klines'],
                'feature_builder_present': self.feature_builder is not None,
                'feature_completeness': not feature_flags,
                'feature_missing_fields': feature_flags,
                'rev_candidate_mode': None if rev_candidate is None else rev_candidate.get('approximation'),
                'strategy_ts': strategy_bar.isoformat(),
                'cache_mode': 'warmup_then_incremental_window',
                'cache_intervals': ['5m', '15m', '1h'],
                'notes': [
                    'strategy_ts/bar_ts/execution_attributed_bar 统一对齐正式 5m bar',
                    'signal_15m/trend_1h 一律按 <= strategy_ts 的最近已闭合数据对齐，禁止 future leak',
                    '首次 warmup 后按各周期小窗口增量刷新，避免每轮全量重拉',
                ],
            },
        )

    def _get_klines_for_snapshot(self, *, symbol: str, interval: str, strategy_bar: datetime) -> list[Any]:
        cache_key = (symbol, interval)
        cache = self._kline_cache.get(cache_key)
        if cache is None or not cache.bars:
            bars = self.readonly_client.get_klines(symbol=symbol, interval=interval, limit=self.warmup_limit)
            self._kline_cache[cache_key] = _KlineCacheState(symbol=symbol, interval=interval, bars=self._dedup_klines(bars))
            cache = self._kline_cache[cache_key]
        else:
            refreshed = self._refresh_cached_klines(cache=cache, strategy_bar=strategy_bar)
            cache.bars = refreshed

        return list(cache.bars)

    def _refresh_cached_klines(self, *, cache: _KlineCacheState, strategy_bar: datetime) -> list[Any]:
        bars = list(cache.bars)
        if not bars:
            return bars

        last_open_ms = max(int(getattr(item, 'open_time_ms')) for item in bars)
        strategy_bar_ms = int(strategy_bar.timestamp() * 1000)
        if last_open_ms >= strategy_bar_ms:
            return self._dedup_klines(bars)

        refreshed_tail = self.readonly_client.get_klines(
            symbol=cache.symbol,
            interval=cache.interval,
            limit=self.refresh_window_limit,
            start_time_ms=last_open_ms,
        )
        if not refreshed_tail:
            return self._dedup_klines(bars)

        merged = self._merge_klines(bars, refreshed_tail)
        return merged[-self.warmup_limit :]

    @staticmethod
    def _dedup_klines(klines: list[Any]) -> list[Any]:
        deduped: dict[int, Any] = {}
        for item in klines:
            deduped[int(getattr(item, 'open_time_ms'))] = item
        return [deduped[key] for key in sorted(deduped.keys())]

    def _merge_klines(self, base: list[Any], updates: list[Any]) -> list[Any]:
        return self._dedup_klines([*base, *updates])

    def _build_trend_features(self, *, symbol: str, trend_bars: list[Any]) -> dict[str, Any]:
        last_closed = trend_bars[-1] if trend_bars else None
        if last_closed is None:
            raise ValueError('missing closed 1h kline')

        base = self._kline_to_ohlcv_dict(last_closed)
        values = dict(base)
        missing_fields: list[str] = []

        if self.feature_builder is None:
            for field in ('ema_fast', 'ema_slow', 'adx', 'atr_rank', 'structure_tag'):
                values[field] = None if field != 'structure_tag' else 'UNAVAILABLE'
                missing_fields.append(field)
            return {
                'values': values,
                'is_complete': False,
                'missing_fields': missing_fields,
            }

        feature_values = self.feature_builder.build_trend_features(symbol=symbol, trend_bars=trend_bars)
        values.update(feature_values)
        for field in ('ema_fast', 'ema_slow', 'adx', 'atr_rank'):
            if values.get(field) in (None, ''):
                missing_fields.append(field)
        if values.get('structure_tag') in (None, '', 'UNAVAILABLE'):
            missing_fields.append('structure_tag')
        return {
            'values': values,
            'is_complete': not missing_fields,
            'missing_fields': missing_fields,
        }

    def _build_rev_candidate(self, *, symbol: str, signal_bars: list[Any], trend_bars: list[Any]) -> dict[str, Any] | None:
        if self.feature_builder is None:
            return None
        return self.feature_builder.build_rev_candidate(
            symbol=symbol,
            signal_bars=signal_bars,
            trend_bars=trend_bars,
        )

    @staticmethod
    def _closed_at_or_before(klines: list[Any], strategy_bar: datetime) -> list[Any]:
        cutoff_ms = int(strategy_bar.timestamp() * 1000)
        return [
            item for item in klines
            if getattr(item, 'is_closed', False) and int(getattr(item, 'open_time_ms')) <= cutoff_ms
        ]

    def _last_closed_at_or_before(self, klines: list[Any], strategy_bar: datetime) -> Any | None:
        closed = self._closed_at_or_before(klines, strategy_bar)
        if not closed:
            return None
        return closed[-1]

    def _tail_closed_at_or_before(self, klines: list[Any], strategy_bar: datetime, *, size: int) -> list[Any]:
        closed = self._closed_at_or_before(klines, strategy_bar)
        return closed[-size:]

    @staticmethod
    def _kline_to_ohlcv_dict(kline: Any) -> dict[str, Any]:
        return {
            'open': float(kline.open_price),
            'high': float(kline.high_price),
            'low': float(kline.low_price),
            'close': float(kline.close_price),
            'volume': float(kline.volume),
        }

    @staticmethod
    def _kline_to_signal_history_dict(kline: Any) -> dict[str, Any]:
        return {
            'close': float(kline.close_price),
            'low': float(kline.low_price),
            'high': float(kline.high_price),
        }

    @staticmethod
    def _bar_open_iso(open_time_ms: int) -> str:
        return datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc).isoformat()


def align_timeframe(value: datetime, *, minutes: int) -> datetime:
    if minutes <= 0:
        raise ValueError('minutes must be positive')
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    floored_minute = (value.minute // minutes) * minutes
    return value.replace(minute=floored_minute, second=0, microsecond=0)


def derive_snapshot_time_semantics(
    decision_time: datetime,
    *,
    strategy_interval_minutes: int = 5,
    preclose_offset_seconds: int = 0,
) -> SnapshotTimeSemantics:
    if decision_time.tzinfo is None:
        decision_time = decision_time.replace(tzinfo=timezone.utc)
    if preclose_offset_seconds != 0:
        raise ValueError('preclose_offset_seconds must be 0 under v6c formal 5m semantics')

    strategy_bar = align_timeframe(decision_time, minutes=strategy_interval_minutes)
    strategy_ts = strategy_bar.isoformat()
    return SnapshotTimeSemantics(
        strategy_ts=strategy_ts,
        decision_ts=decision_time.isoformat(),
        execution_attributed_bar=strategy_ts,
        preclose_offset_seconds=0,
    )


def build_market_snapshot(
    *,
    provider: MarketDataProvider,
    symbol: str,
    decision_time: datetime,
    strategy_interval_minutes: int = 5,
    preclose_offset_seconds: int = 0,
) -> MarketSnapshot:
    semantics = derive_snapshot_time_semantics(
        decision_time,
        strategy_interval_minutes=strategy_interval_minutes,
        preclose_offset_seconds=preclose_offset_seconds,
    )
    bundle = provider.load(symbol=symbol, decision_time=decision_time)
    return MarketSnapshot(
        decision_ts=semantics.decision_ts,
        bar_ts=semantics.strategy_ts,
        strategy_ts=semantics.strategy_ts,
        execution_attributed_bar=semantics.execution_attributed_bar,
        symbol=bundle.symbol,
        preclose_offset_seconds=semantics.preclose_offset_seconds,
        current_price=float(bundle.current_price),
        source_status=bundle.source_status,
        fast_5m=dict(bundle.fast_5m),
        signal_15m=dict(bundle.signal_15m),
        signal_15m_ts=bundle.signal_15m_ts,
        trend_1h=dict(bundle.trend_1h),
        trend_1h_ts=bundle.trend_1h_ts,
        signal_15m_history=list(bundle.signal_15m_history),
        rev_candidate=bundle.rev_candidate,
    )
