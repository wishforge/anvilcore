"""ReflectService: the named value layer behind the Context attribute chain.

Mirrors cordis reflect.ts semantics in Python:

- ``provide(name, value)`` registers a value owned by the calling context's
  scope; the returned disposer removes it, and the value also disappears when
  the owning scope is disposed (registration goes through PluginScope.own,
  so the no-ghost teardown guarantee applies unchanged).
- ``get`` resolves by isolate label: an entry is visible to a context whose
  label for that name equals the provider's label at provide time.
- Every re-provide bumps a per-service generation; watchers registered via
  ``add_watcher(name, callback)`` fire with the new generation. This backs
  the registry's epoch-driven auto-reload (cordis fiber epoch machinery).
- ``notify``/``wait`` back the registry's PENDING injection wait.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

_DEFAULT = "__default__"


@dataclass
class _Entry:
    value: Any
    scope: Any
    label: str
    generation: int


class ReflectService:
    def __init__(self, ctx) -> None:
        self.ctx = ctx
        # mirrors upstream: services live as direct map entries, not in a
        # scanned list -- {name: {isolate_label: entry}}, O(1) resolution
        self._entries: dict[str, dict[str, _Entry]] = {}
        self._waiters: dict[str, list[asyncio.Future]] = {}
        # epoch machinery: watchers fire when a service name is re-provided
        # with a NEW generation (cordis: epoch bump -> fiber reload)
        self._watchers: dict[str, list[Callable[[int], Any]]] = {}
        self._generation = 0

    # -- watchers --------------------------------------------------------

    def add_watcher(self, name: str,
                    callback: Callable[[int], Any]) -> Callable[[], None]:
        """Fire `callback(new_generation)` whenever `name` is re-provided."""
        bucket = self._watchers.setdefault(name, [])
        bucket.append(callback)

        def remove() -> None:
            if callback in bucket:
                bucket.remove(callback)

        return remove

    # -- registration ------------------------------------------------------

    def provide_for(self, ctx, name: str, value: Any = None) -> Any:
        """Register `name` -> value owned by `ctx`'s scope.

        Re-providing the same name in the same isolate label REPLACES the
        previous entry (cordis semantics: newest provide wins, and stale
        registrations cannot accumulate) and bumps the service generation,
        which wakes epoch watchers.

        Returns an idempotent disposer. The entry is also owned by the
        scope, so disposing the scope removes it automatically.
        """
        label = ctx.isolate_label(name)
        by_label = self._entries.setdefault(name, {})
        self._generation += 1
        entry = _Entry(value=value, scope=ctx.scope, label=label,
                       generation=self._generation)
        by_label[label] = entry

        def remove() -> None:
            current = self._entries.get(name)
            if current and current.get(label) is entry:
                current.pop(label, None)
                if not current:
                    self._entries.pop(name, None)
                self.notify(name)

        ctx.scope.own(f"provide:{name}", remove)
        self.notify(name)
        self._fire_watchers(name)
        return remove

    def set_for(self, ctx, name: str, value: Any) -> None:
        label = ctx.isolate_label(name)
        entry = self._entries.get(name, {}).get(label)
        if entry is None:
            raise AttributeError(f"service {name!r} is not provided in this scope")
        entry.value = value
        self.notify(name)

    def generation_of(self, ctx, name: str) -> int | None:
        entry = self._entries.get(name, {}).get(ctx.isolate_label(name))
        return entry.generation if entry is not None else None

    def _fire_watchers(self, name: str) -> None:
        entry = self._entries.get(name, {}).get(_DEFAULT)
        generation = entry.generation if entry else self._generation
        for callback in list(self._watchers.get(name, [])):
            result = callback(generation)
            if asyncio.iscoroutine(result):
                asyncio.get_running_loop().create_task(result)

    # -- resolution ------------------------------------------------------

    def get_for(self, ctx, name: str, strict: bool = True) -> Any:
        """cordis semantics (reflect.ts:233-243): `strict` means the
        provider's scope must still be ACTIVE -- a DISPOSING/DISPOSED
        provider reads as absent. Missing names return None here; the
        raise lives in Context.__getattr__."""
        label = ctx.isolate_label(name)
        entry = self._entries.get(name, {}).get(label)
        if entry is None:
            return None
        if strict and entry.scope.state != "ACTIVE":
            return None
        return entry.value

    # -- readiness ---------------------------------------------------------

    def notify(self, name: str) -> None:
        for fut in self._waiters.pop(name, []):
            if not fut.done():
                fut.set_result(None)

    async def wait_for(self, ctx, name: str) -> Any:
        existing = self.get_for(ctx, name)
        if existing is not None:
            return existing
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        bucket = self._waiters.setdefault(name, [])
        bucket.append(fut)
        try:
            value = await fut
        finally:
            # cancelled/timed-out waiters must not linger in the bucket
            if fut in bucket:
                bucket.remove(fut)
            if not bucket:
                self._waiters.pop(name, None)
        return value if value is not None else self.get_for(ctx, name)


class BoundReflect:
    """Per-context view of the shared ReflectService.

    JS relies on proxy `this` binding so `ctx.reflect.provide` attaches to
    the accessing context; Python passes the context explicitly.
    """

    def __init__(self, store: ReflectService, ctx) -> None:
        self._store = store
        self._ctx = ctx

    def provide(self, name: str, value: Any = None) -> Any:
        return self._store.provide_for(self._ctx, name, value)

    def set(self, name: str, value: Any) -> None:
        self._store.set_for(self._ctx, name, value)

    def get(self, name: str, strict: bool = True) -> Any:
        return self._store.get_for(self._ctx, name, strict)

    def wait(self, name: str) -> Any:
        return self._store.wait_for(self._ctx, name)

    def notify(self, name: str) -> None:
        self._store.notify(name)

    def add_watcher(self, name: str,
                    callback: Callable[[int], Any]) -> Callable[[], None]:
        return self._store.add_watcher(name, callback)
