"""Shared artifact specifications for persisted ML4T workflow objects."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from .feed_spec import TimestampSemantics


class ArtifactKind(str, Enum):
    """Kinds of persisted artifacts shared across ML4T workflows."""

    MARKET_DATA = "market_data"
    LABELS = "labels"
    FEATURES = "features"
    PREDICTIONS = "predictions"


@dataclass(frozen=True, slots=True)
class ArtifactStorage:
    """Storage location and serialization hints for an artifact."""

    path: str | Path = ""
    format: str = "parquet"
    partition_by: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> ArtifactStorage:
        if mapping is None:
            return cls()
        partition_by = mapping.get("partition_by", ())
        if isinstance(partition_by, str):
            partition_by = (partition_by,)
        return cls(
            path=mapping.get("path", ""),
            format=str(mapping.get("format", "parquet")),
            partition_by=tuple(str(item) for item in partition_by),
        )


@dataclass(frozen=True, slots=True)
class ArtifactProvenance:
    """Upstream lineage and content fingerprinting for an artifact."""

    source_artifacts: tuple[str, ...] = ()
    content_hash: str | None = None
    created_by: str | None = None

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> ArtifactProvenance:
        if mapping is None:
            return cls()
        source_artifacts = mapping.get("source_artifacts", ())
        if isinstance(source_artifacts, str):
            source_artifacts = (source_artifacts,)
        return cls(
            source_artifacts=tuple(str(item) for item in source_artifacts),
            content_hash=_optional_str(mapping.get("content_hash")),
            created_by=_optional_str(mapping.get("created_by")),
        )


@dataclass(frozen=True, slots=True)
class ArtifactSpec:
    """Base metadata shared by all persisted artifact specifications."""

    artifact_id: str
    kind: ArtifactKind
    version: int = 1
    storage: ArtifactStorage = field(default_factory=ArtifactStorage)
    provenance: ArtifactProvenance = field(default_factory=ArtifactProvenance)

    def to_dict(self) -> dict[str, Any]:
        return _serialize(asdict(self))


@dataclass(frozen=True, slots=True)
class MarketDataSchema:
    """Column mapping for tradable market data."""

    timestamp_col: str = "timestamp"
    entity_col: str = "symbol"
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

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> MarketDataSchema:
        if mapping is None:
            return cls()
        return cls(
            timestamp_col=str(mapping.get("timestamp_col", "timestamp")),
            entity_col=str(mapping.get("entity_col", "symbol")),
            price_col=str(mapping.get("price_col", mapping.get("close_col", "close"))),
            open_col=str(mapping.get("open_col", "open")),
            high_col=str(mapping.get("high_col", "high")),
            low_col=str(mapping.get("low_col", "low")),
            close_col=str(mapping.get("close_col", mapping.get("price_col", "close"))),
            volume_col=str(mapping.get("volume_col", "volume")),
            bid_col=_optional_str(mapping.get("bid_col")),
            ask_col=_optional_str(mapping.get("ask_col")),
            mid_col=_optional_str(mapping.get("mid_col")),
            bid_size_col=_optional_str(mapping.get("bid_size_col")),
            ask_size_col=_optional_str(mapping.get("ask_size_col")),
        )


@dataclass(frozen=True, slots=True)
class MarketDataSemantics:
    """Temporal and execution semantics for tradable market data."""

    data_frequency: str | None = None
    calendar: str | None = None
    timezone: str | None = None
    timestamp_semantics: TimestampSemantics | str | None = None
    session_start_time: str | None = None
    bar_type: str | None = None

    def __post_init__(self) -> None:
        semantics = self.timestamp_semantics
        if semantics is None or isinstance(semantics, TimestampSemantics):
            return
        object.__setattr__(self, "timestamp_semantics", TimestampSemantics(str(semantics)))

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> MarketDataSemantics:
        if mapping is None:
            return cls()
        return cls(
            data_frequency=_optional_str(mapping.get("data_frequency")),
            calendar=_optional_str(mapping.get("calendar")),
            timezone=_optional_str(mapping.get("timezone")),
            timestamp_semantics=mapping.get("timestamp_semantics"),
            session_start_time=_optional_str(mapping.get("session_start_time")),
            bar_type=_optional_str(mapping.get("bar_type")),
        )


@dataclass(frozen=True, slots=True)
class MarketDataSpec(ArtifactSpec):
    """Shared specification for tradable market data artifacts."""

    kind: ArtifactKind = field(default=ArtifactKind.MARKET_DATA, init=False)
    schema: MarketDataSchema = field(default_factory=MarketDataSchema)
    semantics: MarketDataSemantics = field(default_factory=MarketDataSemantics)

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any]) -> MarketDataSpec:
        return cls(
            artifact_id=str(mapping["artifact_id"]),
            version=int(mapping.get("version", 1)),
            storage=ArtifactStorage.from_mapping(mapping.get("storage")),
            provenance=ArtifactProvenance.from_mapping(mapping.get("provenance")),
            schema=MarketDataSchema.from_mapping(mapping.get("schema")),
            semantics=MarketDataSemantics.from_mapping(mapping.get("semantics")),
        )


@dataclass(frozen=True, slots=True)
class LabelSchema:
    """Column layout for a label artifact."""

    timestamp_col: str = "timestamp"
    entity_col: str = "symbol"
    label_col: str = "label"

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> LabelSchema:
        if mapping is None:
            return cls()
        return cls(
            timestamp_col=str(mapping.get("timestamp_col", "timestamp")),
            entity_col=str(mapping.get("entity_col", "symbol")),
            label_col=str(mapping.get("label_col", "label")),
        )


@dataclass(frozen=True, slots=True)
class LabelDefinition:
    """Definition metadata for a label artifact."""

    family: str = "forward_return"
    task_type: str = "regression"
    horizon: str | None = None
    buffer: str | None = None
    source_artifact: str | None = None
    reference_field: str | None = None
    execution_delay: str | None = None
    session_bounded: bool = False

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> LabelDefinition:
        if mapping is None:
            return cls()
        return cls(
            family=str(mapping.get("family", "forward_return")),
            task_type=str(mapping.get("task_type", "regression")),
            horizon=_optional_str(mapping.get("horizon")),
            buffer=_optional_str(mapping.get("buffer")),
            source_artifact=_optional_str(mapping.get("source_artifact")),
            reference_field=_optional_str(mapping.get("reference_field")),
            execution_delay=_optional_str(mapping.get("execution_delay")),
            session_bounded=bool(mapping.get("session_bounded", False)),
        )


@dataclass(frozen=True, slots=True)
class LabelSpec(ArtifactSpec):
    """Shared specification for persisted label artifacts."""

    kind: ArtifactKind = field(default=ArtifactKind.LABELS, init=False)
    schema: LabelSchema = field(default_factory=LabelSchema)
    definition: LabelDefinition = field(default_factory=LabelDefinition)

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any]) -> LabelSpec:
        return cls(
            artifact_id=str(mapping["artifact_id"]),
            version=int(mapping.get("version", 1)),
            storage=ArtifactStorage.from_mapping(mapping.get("storage")),
            provenance=ArtifactProvenance.from_mapping(mapping.get("provenance")),
            schema=LabelSchema.from_mapping(mapping.get("schema")),
            definition=LabelDefinition.from_mapping(mapping.get("definition")),
        )


@dataclass(frozen=True, slots=True)
class FeatureSchema:
    """Column layout for a feature artifact."""

    timestamp_col: str = "timestamp"
    entity_col: str = "symbol"
    feature_columns: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> FeatureSchema:
        if mapping is None:
            return cls()
        feature_columns = mapping.get("feature_columns", ())
        return cls(
            timestamp_col=str(mapping.get("timestamp_col", "timestamp")),
            entity_col=str(mapping.get("entity_col", "symbol")),
            feature_columns=tuple(str(item) for item in feature_columns),
        )


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    """Definition metadata for a feature artifact."""

    family: str = "financial"
    join_keys: tuple[str, ...] = ()
    source_artifacts: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> FeatureDefinition:
        if mapping is None:
            return cls()
        join_keys = mapping.get("join_keys", ())
        source_artifacts = mapping.get("source_artifacts", ())
        return cls(
            family=str(mapping.get("family", "financial")),
            join_keys=tuple(str(item) for item in join_keys),
            source_artifacts=tuple(str(item) for item in source_artifacts),
        )


@dataclass(frozen=True, slots=True)
class FeatureSpec(ArtifactSpec):
    """Shared specification for persisted feature artifacts."""

    kind: ArtifactKind = field(default=ArtifactKind.FEATURES, init=False)
    schema: FeatureSchema = field(default_factory=FeatureSchema)
    definition: FeatureDefinition = field(default_factory=FeatureDefinition)

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any]) -> FeatureSpec:
        return cls(
            artifact_id=str(mapping["artifact_id"]),
            version=int(mapping.get("version", 1)),
            storage=ArtifactStorage.from_mapping(mapping.get("storage")),
            provenance=ArtifactProvenance.from_mapping(mapping.get("provenance")),
            schema=FeatureSchema.from_mapping(mapping.get("schema")),
            definition=FeatureDefinition.from_mapping(mapping.get("definition")),
        )


@dataclass(frozen=True, slots=True)
class PredictionSchema:
    """Column layout for a prediction artifact."""

    timestamp_col: str = "timestamp"
    entity_col: str = "symbol"
    prediction_col: str = "prediction"

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> PredictionSchema:
        if mapping is None:
            return cls()
        return cls(
            timestamp_col=str(mapping.get("timestamp_col", "timestamp")),
            entity_col=str(mapping.get("entity_col", "symbol")),
            prediction_col=str(mapping.get("prediction_col", "prediction")),
        )


@dataclass(frozen=True, slots=True)
class PredictionDefinition:
    """Definition metadata for a prediction artifact."""

    split_protocol: str = "walk_forward_oos"
    label_artifact: str | None = None
    feature_artifacts: tuple[str, ...] = ()
    training_hash: str | None = None

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> PredictionDefinition:
        if mapping is None:
            return cls()
        feature_artifacts = mapping.get("feature_artifacts", ())
        if isinstance(feature_artifacts, str):
            feature_artifacts = (feature_artifacts,)
        return cls(
            split_protocol=str(mapping.get("split_protocol", "walk_forward_oos")),
            label_artifact=_optional_str(mapping.get("label_artifact")),
            feature_artifacts=tuple(str(item) for item in feature_artifacts),
            training_hash=_optional_str(mapping.get("training_hash")),
        )


@dataclass(frozen=True, slots=True)
class PredictionSpec(ArtifactSpec):
    """Shared specification for persisted prediction artifacts."""

    kind: ArtifactKind = field(default=ArtifactKind.PREDICTIONS, init=False)
    schema: PredictionSchema = field(default_factory=PredictionSchema)
    definition: PredictionDefinition = field(default_factory=PredictionDefinition)

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any]) -> PredictionSpec:
        return cls(
            artifact_id=str(mapping["artifact_id"]),
            version=int(mapping.get("version", 1)),
            storage=ArtifactStorage.from_mapping(mapping.get("storage")),
            provenance=ArtifactProvenance.from_mapping(mapping.get("provenance")),
            schema=PredictionSchema.from_mapping(mapping.get("schema")),
            definition=PredictionDefinition.from_mapping(mapping.get("definition")),
        )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _serialize(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_serialize(item) for item in value]
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    return value


__all__ = [
    "ArtifactKind",
    "ArtifactProvenance",
    "ArtifactSpec",
    "ArtifactStorage",
    "FeatureDefinition",
    "FeatureSchema",
    "FeatureSpec",
    "LabelDefinition",
    "LabelSchema",
    "LabelSpec",
    "MarketDataSchema",
    "MarketDataSemantics",
    "MarketDataSpec",
    "PredictionDefinition",
    "PredictionSchema",
    "PredictionSpec",
]
