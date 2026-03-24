"""Polars-based multi-asset data feed with O(1) timestamp lookups.

Memory-efficient implementation that stores partitioned DataFrames
and converts to dicts lazily at iteration time.
"""

from datetime import datetime
from typing import Any

import polars as pl
from ml4t.data.artifacts.market_data import FeedSpec


class _AssetsData(dict[str, dict[str, Any]]):
    """Internal per-bar payload with pre-extracted broker views."""

    __slots__ = ("_prices", "_opens", "_highs", "_lows", "_volumes", "_signals")

    def __init__(self):
        super().__init__()
        self._prices: dict[str, float] = {}
        self._opens: dict[str, Any] = {}
        self._highs: dict[str, Any] = {}
        self._lows: dict[str, Any] = {}
        self._volumes: dict[str, Any] = {}
        self._signals: dict[str, dict[str, Any]] = {}


class DataFeed:
    """Polars-based multi-asset data feed with signals and context.

    Pre-partitions data by timestamp at initialization for O(1) lookups
    during iteration. DataFrames are stored in their native format and
    converted to dicts only at iteration time, reducing memory usage ~10x
    for large datasets.

    Memory Efficiency:
        - 1M bars: ~100 MB (was ~1 GB with pre-converted dicts)
        - 10M bars: ~1 GB (vs ~10+ GB with dicts)

    Usage:
        feed = DataFeed(prices_df=prices, signals_df=signals)
        for timestamp, assets_data, context in feed:
            # assets_data: {"AAPL": {"close": 150.0, "signals": {...}}, ...}
            process(timestamp, assets_data)
    """

    #: Column names checked (in order) when auto-detecting the entity column.
    ENTITY_COL_CANDIDATES = ("symbol", "asset", "product", "ticker")

    def __init__(
        self,
        prices_path: str | None = None,
        signals_path: str | None = None,
        context_path: str | None = None,
        prices_df: pl.DataFrame | None = None,
        signals_df: pl.DataFrame | None = None,
        context_df: pl.DataFrame | None = None,
        *,
        feed_spec: FeedSpec | Any | None = None,
        contract: FeedSpec | Any | None = None,
        entity_col: str | None = None,
        timestamp_col: str | None = None,
        open_col: str | None = None,
        high_col: str | None = None,
        low_col: str | None = None,
        close_col: str | None = None,
        volume_col: str | None = None,
    ):
        if feed_spec is not None and contract is not None:
            raise ValueError("Pass either feed_spec or contract, not both")

        self.prices = (
            prices_df
            if prices_df is not None
            else (pl.scan_parquet(prices_path).collect() if prices_path else None)
        )
        self.signals = (
            signals_df
            if signals_df is not None
            else (pl.scan_parquet(signals_path).collect() if signals_path else None)
        )
        self.context = (
            context_df
            if context_df is not None
            else (pl.scan_parquet(context_path).collect() if context_path else None)
        )

        if self.prices is None:
            raise ValueError("prices_path or prices_df required")

        raw_spec = FeedSpec.from_any(feed_spec if feed_spec is not None else contract)
        self.feed_spec = raw_spec.with_overrides(
            entity_col=entity_col,
            timestamp_col=timestamp_col,
            open_col=open_col,
            high_col=high_col,
            low_col=low_col,
            close_col=close_col,
            volume_col=volume_col,
        ).resolve(self.prices.columns, self.ENTITY_COL_CANDIDATES)
        self.contract = self.feed_spec
        self._timestamp_col = self.feed_spec.timestamp_col
        self._entity_col = self.feed_spec.entity_col
        self._open_col = self.feed_spec.open_col
        self._high_col = self.feed_spec.high_col
        self._low_col = self.feed_spec.low_col
        self._close_col = self.feed_spec.close_col
        self._volume_col = self.feed_spec.volume_col

        # Pre-partition data by timestamp for O(1) lookups
        # Store DataFrames (memory efficient) instead of dicts (memory explosion)
        self._prices_by_ts = self._partition_by_timestamp(self.prices)
        self._signals_by_ts = (
            self._partition_by_timestamp(self.signals) if self.signals is not None else {}
        )
        self._context_by_ts = (
            self._partition_by_timestamp(self.context) if self.context is not None else {}
        )

        self._timestamps = self._get_timestamps()
        self._idx = 0
        self._signal_columns = (
            [c for c in self.signals.columns if c not in (self._timestamp_col, self._entity_col)]
            if self.signals is not None
            else []
        )
        self._context_columns = (
            [c for c in self.context.columns if c != self._timestamp_col]
            if self.context is not None
            else []
        )

        price_cols = self.prices.columns
        self._price_asset_idx = price_cols.index(self._entity_col)
        self._price_open_idx = (
            price_cols.index(self._open_col) if self._open_col in price_cols else -1
        )
        self._price_high_idx = (
            price_cols.index(self._high_col) if self._high_col in price_cols else -1
        )
        self._price_low_idx = price_cols.index(self._low_col) if self._low_col in price_cols else -1
        self._price_close_idx = (
            price_cols.index(self._close_col) if self._close_col in price_cols else -1
        )
        self._price_volume_idx = (
            price_cols.index(self._volume_col) if self._volume_col in price_cols else -1
        )

        if self.signals is not None:
            signal_cols = self.signals.columns
            if self._timestamp_col not in signal_cols:
                raise ValueError(
                    f"timestamp_col={self._timestamp_col!r} not found in signal columns {signal_cols}"
                )
            self._signal_asset_idx = signal_cols.index(self._entity_col)
            self._signal_col_indices = [signal_cols.index(c) for c in self._signal_columns]
        else:
            self._signal_asset_idx = -1
            self._signal_col_indices = []

        if self.context is not None:
            context_cols = self.context.columns
            if self._timestamp_col not in context_cols:
                raise ValueError(
                    f"timestamp_col={self._timestamp_col!r} not found in context columns {context_cols}"
                )
            self._context_col_indices = [context_cols.index(c) for c in self._context_columns]
        else:
            self._context_col_indices = []

    @classmethod
    def _resolve_entity_col(cls, explicit: str | None, columns: list[str]) -> str:
        """Determine the entity identifier column.

        If *explicit* is given, validate it exists.  Otherwise auto-detect by
        checking ``ENTITY_COL_CANDIDATES`` in order.
        """
        if explicit is not None:
            if explicit not in columns:
                raise ValueError(f"entity_col={explicit!r} not found in columns {columns}")
            return explicit
        for candidate in cls.ENTITY_COL_CANDIDATES:
            if candidate in columns:
                return candidate
        raise ValueError(
            f"Cannot detect entity column. Expected one of "
            f"{cls.ENTITY_COL_CANDIDATES}, got columns {columns}"
        )

    def _partition_by_timestamp(self, df: pl.DataFrame) -> dict[datetime, pl.DataFrame]:
        """Partition DataFrame into dict keyed by timestamp for O(1) access.

        Uses Polars partition_by which is highly optimized and maintains
        data in columnar format (minimal memory overhead).
        """
        result: dict[datetime, pl.DataFrame] = {}
        if self._timestamp_col not in df.columns:
            raise ValueError(
                f"timestamp_col={self._timestamp_col!r} not found in columns {df.columns}"
            )
        for ts_df in df.partition_by(self._timestamp_col, maintain_order=True):
            ts = ts_df[self._timestamp_col][0]
            result[ts] = ts_df
        return result

    def _get_timestamps(self) -> list[datetime]:
        """Get sorted list of all timestamps across all data sources."""
        all_ts = set(self._prices_by_ts.keys())
        all_ts.update(self._signals_by_ts.keys())
        all_ts.update(self._context_by_ts.keys())
        return sorted(all_ts)

    def __iter__(self):
        self._idx = 0
        return self

    def __len__(self) -> int:
        return len(self._timestamps)

    @property
    def n_bars(self) -> int:
        """Number of unique timestamps/bars."""
        return len(self._timestamps)

    @property
    def timestamps(self) -> tuple[datetime, ...]:
        """Unique feed timestamps in iteration order."""
        return tuple(self._timestamps)

    def __next__(self) -> tuple[datetime, dict[str, dict], dict[str, Any]]:
        if self._idx >= len(self._timestamps):
            raise StopIteration

        ts = self._timestamps[self._idx]
        self._idx += 1

        # O(1) lookup + lazy conversion to dicts (only for current bar)
        assets_data = _AssetsData()
        price_asset_idx = self._price_asset_idx
        price_open_idx = self._price_open_idx
        price_high_idx = self._price_high_idx
        price_low_idx = self._price_low_idx
        price_close_idx = self._price_close_idx
        price_volume_idx = self._price_volume_idx

        # Convert price DataFrame slice to dicts (lazy, only current bar)
        price_df = self._prices_by_ts.get(ts)
        if price_df is not None:
            for row in price_df.iter_rows(named=False):
                asset = row[price_asset_idx]
                close = row[price_close_idx] if price_close_idx >= 0 else None
                open_ = row[price_open_idx] if price_open_idx >= 0 else close
                high = row[price_high_idx] if price_high_idx >= 0 else close
                low = row[price_low_idx] if price_low_idx >= 0 else close
                volume = row[price_volume_idx] if price_volume_idx >= 0 else 0.0

                if open_ is None:
                    open_ = close
                if high is None:
                    high = close
                if low is None:
                    low = close
                if volume is None:
                    volume = 0.0

                assets_data[asset] = {
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "signals": {},
                }
                if close is not None:
                    assets_data._prices[asset] = close
                assets_data._opens[asset] = open_
                assets_data._highs[asset] = high
                assets_data._lows[asset] = low
                assets_data._volumes[asset] = volume
                assets_data._signals[asset] = assets_data[asset]["signals"]

        # Add signals for each asset - lazy conversion
        signal_df = self._signals_by_ts.get(ts)
        if signal_df is not None:
            signal_asset_idx = self._signal_asset_idx
            signal_col_indices = self._signal_col_indices
            signal_columns = self._signal_columns
            for row in signal_df.iter_rows(named=False):
                asset = row[signal_asset_idx]
                if asset in assets_data:
                    asset_signals = assets_data._signals[asset]
                    for i, col_idx in enumerate(signal_col_indices):
                        asset_signals[signal_columns[i]] = row[col_idx]

        # Get context at this timestamp - lazy conversion
        context_data: dict[str, Any] = {}
        ctx_df = self._context_by_ts.get(ts)
        if ctx_df is not None and len(ctx_df) > 0:
            row = ctx_df.row(0)
            for i, col_idx in enumerate(self._context_col_indices):
                context_data[self._context_columns[i]] = row[col_idx]

        return ts, assets_data, context_data
