"""EventsService (cordis events.ts, Python adaptation).

Five dispatch modes: emit (sync fire-and-forget), parallel (gather),
serial (await in order), bail (first truthy wins), waterfall (thread the
argument). Listeners are owned by the registering context's scope via
PluginScope.own, so they die with it. Dispatch filters by isolate labels:
a listener registered where `name` is isolated only sees emits from
contexts sharing that label (child -> parent visible, parent -> isolated
child blocked).

Storage mirrors upstream `_hooks: Record<name, Hook[]>`: listeners are
bucketed per event name, so dispatch never scans unrelated events.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class _Listener:
    fn: Callable
    once: bool
    owner: Any  # live reference to the registering context (cordis filter semantics)


class EventsService:
    def __init__(self, ctx) -> None:
        self._hooks: dict[str, list[_Listener]] = {}

    # -- registration ---------------------------------------------------

    def register(self, ctx, name: str, fn: Callable,
                 once: bool = False) -> Callable[[], bool]:
        listener = _Listener(fn=fn, once=once, owner=ctx)
        self._hooks.setdefault(name, []).append(listener)

        def remove() -> bool:
            bucket = self._hooks.get(name)
            if bucket and listener in bucket:
                bucket.remove(listener)
                if not bucket:
                    self._hooks.pop(name, None)
                return True
            return False

        ctx.scope.own(f"event:{name}", remove)
        return remove

    def _discard(self, name: str, listener: _Listener) -> None:
        bucket = self._hooks.get(name)
        if bucket and listener in bucket:
            bucket.remove(listener)
            if not bucket:
                self._hooks.pop(name, None)

    # -- dispatch -------------------------------------------------------

    def _collect(self, ctx, name: str) -> list[_Listener]:
        # cordis dispatch consults the listener's LIVE context per service
        # name (events.ts:171-174), never a frozen snapshot: a listener
        # registered under isolate A+B still fires from an emitter that
        # only isolates A, and later isolations take effect immediately.
        matched = []
        for ln in self._hooks.get(name, ()):
            owner = ln.owner
            if all(owner.isolate_label(n) == ctx.isolate_label(n)
                   for n in owner._isolate):
                matched.append(ln)
        return matched

    def _fire(self, ctx, name: str, args) -> list[Any]:
        results = []
        for ln in self._collect(ctx, name):
            if ln.once:
                self._discard(name, ln)   # dispose first: re-entrancy safe
            results.append(ln.fn(*args))
        return results

    def emit(self, ctx, name: str, *args) -> None:
        self._fire(ctx, name, args)

    async def parallel(self, ctx, name: str, *args) -> list:
        # every listener runs inside a coroutine so that sync listeners'
        # exceptions are captured by gather, not thrown during setup
        async def invoke(fn):
            out = fn(*args)
            if asyncio.iscoroutine(out):
                out = await out
            return out

        coros = []
        for ln in self._collect(ctx, name):
            if ln.once:
                self._discard(name, ln)   # dispose first (cordis once)
            coros.append(invoke(ln.fn))
        results = await asyncio.gather(*coros, return_exceptions=True)
        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            raise ExceptionGroup(f"event {name!r} listener errors", errors)
        return results

    async def serial(self, ctx, name: str, *args) -> list[Any]:
        results = []
        for ln in self._collect(ctx, name):
            if ln.once:
                self._discard(name, ln)   # dispose first (cordis once)
            out = ln.fn(*args)
            if asyncio.iscoroutine(out):
                out = await out
            results.append(out)
        return results

    def bail(self, ctx, name: str, *args) -> Any:
        for result in self._fire(ctx, name, args):
            if result is not None:
                return result
        return None

    def waterfall(self, ctx, name: str, value: Any) -> Any:
        for ln in self._collect(ctx, name):
            if ln.once:
                self._discard(name, ln)   # dispose first (cordis once)
            value = ln.fn(value)
        return value


async def _as_coro(value: Any) -> Any:
    return value


class BoundEvents:
    """Per-context view: registrations and dispatches bind to that context."""

    def __init__(self, store: EventsService, ctx) -> None:
        self._store = store
        self._ctx = ctx

    def on(self, name: str, fn: Callable, once: bool = False) -> Callable[[], bool]:
        return self._store.register(self._ctx, name, fn, once)

    def once(self, name: str, fn: Callable) -> Callable[[], bool]:
        return self.on(name, fn, once=True)

    def emit(self, name: str, *args) -> None:
        self._store.emit(self._ctx, name, *args)

    async def parallel(self, name: str, *args) -> list:
        return await self._store.parallel(self._ctx, name, *args)

    async def serial(self, name: str, *args) -> list:
        return await self._store.serial(self._ctx, name, *args)

    def bail(self, name: str, *args) -> Any:
        return self._store.bail(self._ctx, name, *args)

    def waterfall(self, name: str, value: Any) -> Any:
        return self._store.waterfall(self._ctx, name, value)
