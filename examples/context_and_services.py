"""01 -- Context, services and scoped isolation in 60 seconds.

Business value: a long-running agent process accumulates services
(database pools, verifiers, rate limiters). pycordis gives each one a
name, a scope to live in, and a guaranteed teardown -- install something,
and its removal is already registered.

Run: python examples/01_context_and_services.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pycordis import Context, Service


class ClockService(Service):
    """Any class becomes a named service by subclassing Service."""

    def now(self) -> int:
        return 1234


async def main() -> None:
    ctx = Context()
    ClockService(ctx, "clock")

    # attribute read == service lookup
    print("clock says:", ctx.clock.now())

    # extend() derives a child context without mutating the parent
    child = ctx.extend()
    print("child sees parent service:", child.clock.now())

    # isolate() gives a service name a separate scope: two implementations
    # of "clock" coexist without leaking into each other
    test_ctx = ctx.isolate("clock")
    ClockService(test_ctx, "clock")
    print("parent clock is:", ctx.clock.now())
    print("isolated clock is:", test_ctx.clock.now())

    # teardown + tenancy: a tenant gets its own scope AND an isolated
    # service name. Same-name same-label provide REPLACES (cordis
    # semantics); isolate() is how coexistence works.
    tenant_scope = ctx.scope.child("tenant-a")
    tenant = Context(parent=ctx, scope=tenant_scope).isolate("clock")
    ClockService(tenant, "clock")
    print("tenant clock:", tenant.clock.now())
    await tenant_scope.dispose()
    print("after tenant scope disposal, ctx.clock still works:", ctx.clock.now())

    # no-ghost: install/uninstall is symmetric by construction
    off = ctx.reflect.provide("ephemeral", "value")
    off()
    print("disposed service reads as:", ctx.reflect.get("ephemeral"))


if __name__ == "__main__":
    asyncio.run(main())
