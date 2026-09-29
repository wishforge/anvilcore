"""Service base class (cordis service.ts, Python adaptation).

A Service subclass registers itself under a name when constructed; the
registration is owned by the constructing context's scope and disappears
with it. cordis's callable-service machinery (createCallable / invoke) is
deliberately omitted -- logger uses a factory method instead.
"""

from __future__ import annotations

from .context import Context


class Service:
    """Register `self` as `name` in `ctx` on construction."""

    #: class attribute fallback for the registration name
    provide: str | None = None

    def __init__(self, ctx: Context, name: str | None = None) -> None:
        name = name or type(self).provide
        if not name:
            raise ValueError("service requires an explicit name or `provide` class attribute")
        self.ctx = ctx
        self.name = name
        self._disposer = ctx.reflect.provide(name, self)

    def unregister(self) -> None:
        self._disposer()
