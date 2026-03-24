# Changelog

## 0.1.0b10 - 2026-03-24

### Added

- `BacktestConfig` support for serialized top-level `feed` and passthrough `metadata`
  sections, enabling sparse input presets with generic provenance fields.
- `BacktestResult.to_spec_dict()` for a resolved runtime snapshot containing the full
  replayable config, library version, and realized run window.
- `spec.yaml` export alongside `config.yaml` in `BacktestResult.to_parquet()`.

### Changed

- `BacktestConfig.to_dict()` now emits plain-data feed metadata that round-trips safely
  through dict, YAML, and Parquet persistence workflows.
- `BacktestResult.from_parquet()` now falls back to `spec.yaml` when `config.yaml` is absent.
- User guides now document the config workflow for sparse input, resolved output, `feed`,
  `metadata`, and reproducibility exports.

### Validation

- `uv run ruff check src/ml4t/backtest/config.py src/ml4t/backtest/result.py tests/test_broker.py tests/test_result.py`
- `uv run pytest tests/test_broker.py tests/test_result.py -q`
- `uv run ty check`
- `uv run python -m mkdocs build --strict`

## 0.1.0b9 - 2026-03-24

### Added

- Quote-aware `DataFeed` support for `price_col`, bid, ask, midpoint, and quote-size caches.
- New `ExecutionPrice` sources: `price`, `bid`, `ask`, `quote_mid`, and `quote_side`.
- Separate `mark_price` configuration for open-position marking.
- `BacktestResult.to_fills_dataframe()` and persisted `fills.parquet` export/import.
- `BacktestResult.to_portfolio_state_dataframe()` and persisted `portfolio_state.parquet`.
- Quote context fields on `Fill` and summarized quote context on `Trade`.
- Activity and exposure metrics: `num_fills`, `num_rebalance_events`, `unique_symbols_traded`,
  `total_filled_notional`, `avg_turnover`, `max_turnover`, `avg_open_positions`,
  and `max_open_positions`.

### Changed

- `FeedSpec.price_col` now drives the broker reference price instead of being collapsed back to `close`.
- Market execution can use side-aware quotes: buys at ask, sells at bid.
- `QUOTE_SIDE` marking prices long inventory on the bid and short inventory on the ask.
- Result persistence now includes fills alongside trades, equity, daily P&L, metrics, and config.
- Result persistence now includes portfolio state alongside trades, fills, equity,
  daily P&L, metrics, and config.
- User guides and README now document quote-aware feeds, mark pricing, and fill export.
- User guides and README now document portfolio-state reporting and quote-aware audit fields.

### Performance

- Legacy OHLCV hot path remains faster than the pre-optimization baseline.
- Quote-aware execution adds moderate overhead relative to the optimized OHLCV path, while staying ahead of the legacy baseline in local benchmarks.

### Validation

- `uv run ty check`
- `pre-commit run --all-files`
- `uv run pytest tests/ -q`
