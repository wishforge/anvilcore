"""Config validation (cordis Config/Volatile, minimal Python adaptation)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class ConfigError(ValueError):
    pass


class Volatile:
    """Read-only snapshot of a resolved config."""

    def __init__(self, data: dict) -> None:
        object.__setattr__(self, "_data", dict(data))

    def __getattr__(self, name: str) -> Any:
        try:
            return object.__getattribute__(self, "_data")[name]
        except KeyError as missing:
            raise AttributeError(name) from missing

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("Volatile is read-only")

    def to_dict(self) -> dict:
        return dict(object.__getattribute__(self, "_data"))


def resolve_config(schema: Callable[[dict], dict] | None,
                   config: dict | None) -> dict:
    """Validate + normalize `config` through `schema` (a callable taking the
    raw dict and returning the validated dict, raising on invalid input).
    A None schema shallow-copies; None config yields the schema defaults."""
    raw = dict(config or {})
    if schema is None:
        return raw
    result = schema(raw)
    if not isinstance(result, dict):
        raise ConfigError("schema must return a dict")
    return result
