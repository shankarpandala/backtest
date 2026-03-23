"""Validation-only import bridge.

This module is intentionally not exported from ml4t.backtest root.
Validation scripts should import from here to avoid widening public API.
"""

from .broker import Broker
from .config import BacktestConfig, InitialHwmSource, WaterMarkSource
from .datafeed import DataFeed
from .engine import Engine
from .execution.impact import LinearImpact
from .execution.limits import VolumeParticipationLimit
from .execution.rebalancer import RebalanceConfig, TargetWeightExecutor
from .feed_spec import FeedSpec
from .models import (
    FixedSlippage,
    NoCommission,
    NoSlippage,
    PercentageCommission,
    PercentageSlippage,
    PerShareCommission,
)
from .strategy import Strategy
from .types import (
    ExecutionMode,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    StopFillMode,
    StopLevelBasis,
)

TrailHwmSource = WaterMarkSource

__all__ = [
    "Broker",
    "BacktestConfig",
    "DataFeed",
    "Engine",
    "FeedSpec",
    "ExecutionMode",
    "Strategy",
    "Order",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "StopFillMode",
    "StopLevelBasis",
    "NoCommission",
    "PercentageCommission",
    "PerShareCommission",
    "NoSlippage",
    "FixedSlippage",
    "PercentageSlippage",
    "VolumeParticipationLimit",
    "LinearImpact",
    "RebalanceConfig",
    "TargetWeightExecutor",
    "WaterMarkSource",
    "TrailHwmSource",
    "InitialHwmSource",
]
