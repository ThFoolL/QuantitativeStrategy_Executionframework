from __future__ import annotations

from .models import MarketSnapshot
from .v6c_adapter_baseline import V6CBaselineLiveAdapter


class V6CLAFreeV1LiveAdapter(V6CBaselineLiveAdapter):
    """Runtime adapter aligned with backtest `v6c-LA_FREE_V1`.

    LOW_ACTIVITY semantics differ from baseline in two places:
    - baseline: LOW_ACTIVITY is graded as `C` and blocks trend entry
    - LA_FREE_V1: LOW_ACTIVITY + grade `C` is lifted to `B` for management
    - LA_FREE_V1: LOW_ACTIVITY trend entry is also allowed
    """

    def _allow_low_activity_trend_entry(self) -> bool:
        return True

    def _trend_entry_session_tags(self) -> set[str]:
        return {'US_CORE', 'NON_US_ACTIVE', 'LOW_ACTIVITY'}

    def _trade_grade(self, market: MarketSnapshot) -> str:
        grade = super()._trade_grade(market)
        if self._session_tag(market.bar_ts) == 'LOW_ACTIVITY' and grade == 'C':
            return 'B'
        return grade
