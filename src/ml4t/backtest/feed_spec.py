"""Shared feed contract for dataset schema and temporal metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any


class TimestampSemantics(str, Enum):
    """How timestamps should be interpreted downstream."""

    EVENT_TIME = "event_time"
    BAR_CLOSE = "bar_close"
    SESSION_LABEL = "session_label"


_MISSING = object()


@dataclass(frozen=True, slots=True)
class FeedSpec:
    """Dataset contract shared across ML4T libraries."""

    timestamp_col: str = "timestamp"
    entity_col: str | Sequence[str] | None = None
    price_col: str = "close"
    open_col: str = "open"
    high_col: str = "high"
    low_col: str = "low"
    close_col: str = "close"
    volume_col: str = "volume"
    bid_col: str | None = None
    ask_col: str | None = None
    mid_col: str | None = None
    bid_size_col: str | None = None
    ask_size_col: str | None = None
    calendar: str | None = None
    timezone: str | None = None
    data_frequency: Any | None = None
    bar_type: str | None = None
    timestamp_semantics: TimestampSemantics | str | None = None
    session_start_time: str | None = None

    def __post_init__(self) -> None:
        semantics = self.timestamp_semantics
        if semantics is None:
            return
        if not isinstance(semantics, TimestampSemantics):
            semantics = TimestampSemantics(str(semantics))
            object.__setattr__(self, "timestamp_semantics", semantics)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> FeedSpec:
        """Create a feed contract from a generic mapping."""
        data: dict[str, Any] = {}

        def pick(*names: str) -> Any:
            for name in names:
                if name in mapping:
                    return mapping[name]
            return _MISSING

        price_col = pick("price_col")
        close_col = pick("close_col")
        if price_col is not _MISSING:
            data["price_col"] = price_col
        if close_col is not _MISSING:
            data["close_col"] = close_col
        elif price_col is not _MISSING:
            data["close_col"] = price_col

        alias_groups = {
            "timestamp_col": ("timestamp_col", "time_col", "datetime_col"),
            "entity_col": ("entity_col", "symbol_col", "group_col", "ticker_col", "asset_col"),
            "open_col": ("open_col",),
            "high_col": ("high_col",),
            "low_col": ("low_col",),
            "volume_col": ("volume_col",),
            "bid_col": ("bid_col",),
            "ask_col": ("ask_col",),
            "mid_col": ("mid_col",),
            "bid_size_col": ("bid_size_col",),
            "ask_size_col": ("ask_size_col",),
            "calendar": ("calendar",),
            "timezone": ("timezone",),
            "data_frequency": ("data_frequency", "frequency"),
            "bar_type": ("bar_type",),
            "timestamp_semantics": ("timestamp_semantics",),
            "session_start_time": ("session_start_time",),
        }
        for field_name, aliases in alias_groups.items():
            value = pick(*aliases)
            if value is not _MISSING:
                data[field_name] = value

        return cls(**data)

    @classmethod
    def from_object(cls, value: Any) -> FeedSpec:
        """Create a feed contract from another ML4T config or metadata object."""
        if isinstance(value, FeedSpec):
            return value
        if isinstance(value, Mapping):
            return cls.from_mapping(value)

        metadata = getattr(value, "metadata", None)
        if metadata is not None:
            return cls.from_object(metadata)

        data: dict[str, Any] = {}

        def pick(*names: str) -> Any:
            for name in names:
                if hasattr(value, name):
                    return getattr(value, name)
            return _MISSING

        price_col = pick("price_col")
        close_col = pick("close_col")
        if price_col is not _MISSING:
            data["price_col"] = price_col
        if close_col is not _MISSING:
            data["close_col"] = close_col
        elif price_col is not _MISSING:
            data["close_col"] = price_col

        field_aliases = {
            "timestamp_col": ("timestamp_col", "time_col", "datetime_col"),
            "entity_col": ("entity_col", "symbol_col", "group_col", "ticker_col", "asset_col"),
            "open_col": ("open_col",),
            "high_col": ("high_col",),
            "low_col": ("low_col",),
            "volume_col": ("volume_col",),
            "bid_col": ("bid_col",),
            "ask_col": ("ask_col",),
            "mid_col": ("mid_col",),
            "bid_size_col": ("bid_size_col",),
            "ask_size_col": ("ask_size_col",),
            "calendar": ("calendar",),
            "timezone": ("timezone",),
            "data_frequency": ("data_frequency", "frequency"),
            "bar_type": ("bar_type",),
            "timestamp_semantics": ("timestamp_semantics",),
            "session_start_time": ("session_start_time",),
        }
        for field_name, aliases in field_aliases.items():
            resolved = pick(*aliases)
            if resolved is not _MISSING:
                data[field_name] = resolved

        if "data_frequency" not in data:
            bar_params = getattr(value, "bar_params", None)
            if isinstance(bar_params, Mapping) and "frequency" in bar_params:
                data["data_frequency"] = bar_params["frequency"]

        return cls(**data)

    @classmethod
    def from_any(cls, value: Any | None) -> FeedSpec:
        """Create a feed contract from a mapping, object, or existing spec."""
        if value is None:
            return cls()
        return cls.from_object(value)

    def with_overrides(self, **overrides: Any) -> FeedSpec:
        """Return a new spec with explicit non-null overrides applied."""
        updates = {key: value for key, value in overrides.items() if value is not None}
        return replace(self, **updates) if updates else self

    def resolve(self, columns: Sequence[str], entity_candidates: Sequence[str]) -> FeedSpec:
        """Resolve the entity column against an observed DataFrame schema."""
        if self.timestamp_col not in columns:
            raise ValueError(
                f"timestamp_col={self.timestamp_col!r} not found in columns {list(columns)}"
            )

        entity_col = self._coerce_entity_col(self.entity_col)
        if entity_col is not None:
            if entity_col not in columns:
                raise ValueError(f"entity_col={entity_col!r} not found in columns {list(columns)}")
            return replace(self, entity_col=entity_col)

        for candidate in entity_candidates:
            if candidate in columns:
                return replace(self, entity_col=candidate)

        raise ValueError(
            f"Cannot detect entity column. Expected one of {tuple(entity_candidates)}, "
            f"got columns {list(columns)}"
        )

    def to_backtest_frequency(self):
        """Map external frequency metadata onto backtest runtime frequency."""
        if self.data_frequency is None:
            return None

        from .config import DataFrequency

        value = self.data_frequency
        if isinstance(value, DataFrequency):
            return value
        if isinstance(value, Enum):
            value = value.value

        normalized = str(value).strip().lower()
        mapping = {
            "daily": DataFrequency.DAILY,
            "1d": DataFrequency.DAILY,
            "d": DataFrequency.DAILY,
            "weekly": DataFrequency.DAILY,
            "monthly": DataFrequency.DAILY,
            "minute": DataFrequency.MINUTE_1,
            "1m": DataFrequency.MINUTE_1,
            "1min": DataFrequency.MINUTE_1,
            "5m": DataFrequency.MINUTE_5,
            "5min": DataFrequency.MINUTE_5,
            "5minute": DataFrequency.MINUTE_5,
            "15m": DataFrequency.MINUTE_15,
            "15min": DataFrequency.MINUTE_15,
            "15minute": DataFrequency.MINUTE_15,
            "30m": DataFrequency.MINUTE_30,
            "30min": DataFrequency.MINUTE_30,
            "30minute": DataFrequency.MINUTE_30,
            "hour": DataFrequency.HOURLY,
            "hourly": DataFrequency.HOURLY,
            "1h": DataFrequency.HOURLY,
            "tick": DataFrequency.IRREGULAR,
            "second": DataFrequency.IRREGULAR,
        }
        return mapping.get(normalized, DataFrequency.IRREGULAR)

    @staticmethod
    def _coerce_entity_col(value: str | Sequence[str] | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        values = [str(item) for item in value]
        if not values:
            return None
        if len(values) != 1:
            raise ValueError(
                "ml4t-backtest currently supports a single entity column in FeedSpec.entity_col"
            )
        return values[0]


__all__ = ["FeedSpec", "TimestampSemantics"]
