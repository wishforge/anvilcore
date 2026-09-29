"""Capability runtime: effect-owned, dependency-aware plugin lifecycle.

Migrated from ``docs/architecture/../python-cordis/kernel`` (kugua, 2026-08-16)
so the validated Cordis semantic layer has a canonical import path under
``src/forge`` instead of living inside archaeology docs.

``anvilcore.adapters`` deliberately stays out of this ``__init__``: it
imports AgentScope, which is optional for consumers of the core semantics.
"""

from .capability import (
    INSTALLING,
    REGISTERED,
    Capability,
    CapabilityDescriptor,
)
from .config import ConfigError, Volatile, resolve_config
from .context import Context
from .events import EventsService
from .logger import LoggerService
from .manager import CapabilityRecord, PluginManager
from .reflect import ReflectService
from .registry import RegistryService
from .semantic_layer import (
    ACTIVE,
    CLEANED,
    DISPOSED,
    DISPOSING,
    FAILED,
    DependencyLifecycle,
    Effect,
    EffectBatch,
    EffectRegistry,
    PluginScope,
)
from .service import Service

__all__ = [
    "ACTIVE",
    "CLEANED",
    "DISPOSED",
    "DISPOSING",
    "FAILED",
    "INSTALLING",
    "REGISTERED",
    "Capability",
    "CapabilityDescriptor",
    "CapabilityRecord",
    "ConfigError",
    "Context",
    "DependencyLifecycle",
    "Effect",
    "EffectBatch",
    "EffectRegistry",
    "EventsService",
    "LoggerService",
    "PluginManager",
    "PluginScope",
    "ReflectService",
    "RegistryService",
    "Service",
    "Volatile",
    "resolve_config",
]
