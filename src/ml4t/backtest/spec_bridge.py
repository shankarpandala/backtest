"""Pure conversions from artifact specifications into runtime contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ml4t.data.artifacts.market_data import FeedSpec, MarketDataSpec


def market_data_spec_to_feed_spec(spec: MarketDataSpec | Mapping[str, Any]) -> FeedSpec:
    """Project a market-data spec onto the backtest runtime feed contract."""
    market_spec = (
        MarketDataSpec.from_mapping({str(key): value for key, value in spec.items()})
        if isinstance(spec, Mapping)
        else spec
    )
    schema = market_spec.schema
    semantics = market_spec.semantics
    return FeedSpec(
        timestamp_col=schema.timestamp_col,
        entity_col=schema.entity_col,
        price_col=schema.price_col,
        open_col=schema.open_col,
        high_col=schema.high_col,
        low_col=schema.low_col,
        close_col=schema.close_col,
        volume_col=schema.volume_col,
        bid_col=schema.bid_col,
        ask_col=schema.ask_col,
        mid_col=schema.mid_col,
        bid_size_col=schema.bid_size_col,
        ask_size_col=schema.ask_size_col,
        calendar=semantics.calendar,
        timezone=semantics.timezone,
        data_frequency=semantics.data_frequency,
        bar_type=semantics.bar_type,
        timestamp_semantics=semantics.timestamp_semantics,
        session_start_time=semantics.session_start_time,
    )


def market_data_spec_to_runtime_metadata(
    spec: MarketDataSpec | Mapping[str, Any],
) -> dict[str, Any]:
    """Extract runtime scheduling and annualization metadata from a market-data spec."""
    feed_spec = market_data_spec_to_feed_spec(spec)
    return {
        "calendar": feed_spec.calendar,
        "timezone": feed_spec.timezone,
        "data_frequency": feed_spec.data_frequency,
        "timestamp_semantics": feed_spec.timestamp_semantics,
        "session_start_time": feed_spec.session_start_time,
        "bar_type": feed_spec.bar_type,
    }


__all__ = ["market_data_spec_to_feed_spec", "market_data_spec_to_runtime_metadata"]
