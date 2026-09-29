"""Context: the Cordis-style facade over the anvilcore core.

Python adaptation of context.ts (no JS Proxy): attribute reads resolve
through ReflectService with isolate-label matching, then shadow properties
from extend(meta). extend/isolate/intercept create child contexts without
mutating the parent.
"""

from __future__ import annotations

import uuid
from typing import Any

from .events import BoundEvents, EventsService
from .reflect import BoundReflect, ReflectService
from .registry import BoundRegistry, RegistryService
from .semantic_layer import PluginScope


class Context:
    def __init__(self, root: Context | None = None,
                 parent: Context | None = None,
                 scope: PluginScope | None = None) -> None:
        self.parent = parent
        self.root = root or self
        self.scope = scope if scope is not None else (
            parent.scope if parent is not None else PluginScope("root"))
        self._isolate: dict[str, str] = dict(parent._isolate) if parent else {}
        self._intercept: dict[str, dict] = dict(parent._intercept) if parent else {}
        self._shadow: dict[str, Any] = {}
        self._store = parent._store if parent is not None else ReflectService(self)
        self._events_store = parent._events_store if parent is not None else EventsService(self)
        self._registry_store = (parent._registry_store if parent is not None
                                else RegistryService())

    @property
    def registry(self) -> BoundRegistry:
        """Bound per-context view of the shared plugin registry."""
        return BoundRegistry(self._registry_store, self)

    @property
    def reflect(self) -> ReflectService:
        """Bound per-context view: registrations attach to this context."""
        return BoundReflect(self._store, self)

    @property
    def events(self) -> BoundEvents:
        """Bound per-context view of the shared event bus."""
        return BoundEvents(self._events_store, self)

    # -- event methods mixed onto ctx (cordis convention) ----------------

    def on(self, name: str, fn, once: bool = False):
        return self.events.on(name, fn, once)

    def once(self, name: str, fn):
        return self.events.once(name, fn)

    def emit(self, name: str, *args) -> None:
        self.events.emit(name, *args)

    async def parallel(self, name: str, *args) -> list:
        return await self.events.parallel(name, *args)

    async def serial(self, name: str, *args) -> list:
        return await self.events.serial(name, *args)

    def bail(self, name: str, *args):
        return self.events.bail(name, *args)

    def waterfall(self, name: str, value):
        return self.events.waterfall(name, value)

    # -- identity -------------------------------------------------------

    @classmethod
    def is_(cls, value: Any) -> bool:
        return isinstance(value, Context)

    def __repr__(self) -> str:
        return f"Context <{self.scope.name}>"

    # -- attribute resolution (the "proxy") ------------------------------

    def isolate_label(self, name: str) -> str:
        return self._isolate.get(name, "__default__")

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        # shadow properties from extend(meta) take precedence
        shadow = object.__getattribute__(self, "_shadow")
        if name in shadow:
            return shadow[name]
        reflect = object.__getattribute__(self, "reflect")
        value = reflect.get(name)
        if value is not None:
            return value
        raise AttributeError(
            f"service {name!r} is not provided in this scope")

    # -- scoped derivation ------------------------------------------------

    def extend(self, meta: dict | None = None) -> Context:
        child = Context(root=self.root, parent=self, scope=self.scope)
        if meta:
            child._shadow.update(meta)
        return child

    def isolate(self, name: str, label: str | None = None) -> Context:
        child = self.extend()
        child._isolate[name] = label or f"iso-{uuid.uuid4().hex}"
        return child

    def intercept(self, name: str, config: dict) -> Context:
        child = self.extend()
        merged = dict(self._intercept.get(name, {}))
        merged.update(config)
        child._intercept[name] = merged
        return child
