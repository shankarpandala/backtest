"""Load and dump ML4T artifact specifications from YAML or JSON."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from .artifact_spec import (
    ArtifactKind,
    ArtifactSpec,
    FeatureSpec,
    LabelSpec,
    MarketDataSpec,
    PredictionSpec,
)


def load_spec(path_or_mapping: str | Path | Mapping[str, Any]) -> ArtifactSpec:
    """Load any artifact specification from a path or in-memory mapping."""
    if isinstance(path_or_mapping, Mapping):
        data = dict(path_or_mapping)
    else:
        path = Path(path_or_mapping)
        with path.open() as f:
            data = json.load(f) if path.suffix.lower() == ".json" else yaml.safe_load(f)
    return spec_from_mapping(data)


def load_market_data_spec(path_or_mapping: str | Path | Mapping[str, Any]) -> MarketDataSpec:
    """Load a market-data spec from YAML/JSON or an in-memory mapping."""
    spec = load_spec(path_or_mapping)
    if not isinstance(spec, MarketDataSpec):
        raise TypeError(f"Expected market_data spec, got {spec.kind.value}")
    return spec


def dump_spec(spec: ArtifactSpec, path: str | Path) -> Path:
    """Serialize an artifact specification to YAML or JSON."""
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    payload = spec.to_dict()
    with dest.open("w") as f:
        if dest.suffix.lower() == ".json":
            json.dump(payload, f, indent=2)
            f.write("\n")
        else:
            yaml.safe_dump(payload, f, sort_keys=False)
    return dest


def spec_from_mapping(mapping: Mapping[str, Any]) -> ArtifactSpec:
    """Instantiate the correct spec class from a generic mapping."""
    kind = ArtifactKind(str(mapping["kind"]))
    concrete = dict(mapping)
    if kind == ArtifactKind.MARKET_DATA:
        return MarketDataSpec.from_mapping(concrete)
    if kind == ArtifactKind.LABELS:
        return LabelSpec.from_mapping(concrete)
    if kind == ArtifactKind.FEATURES:
        return FeatureSpec.from_mapping(concrete)
    if kind == ArtifactKind.PREDICTIONS:
        return PredictionSpec.from_mapping(concrete)
    raise ValueError(f"Unsupported artifact kind: {kind}")


__all__ = ["dump_spec", "load_market_data_spec", "load_spec", "spec_from_mapping"]
