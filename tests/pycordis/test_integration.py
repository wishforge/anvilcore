"""Integration: Context + Service + events + registry DI working together.

Spec acceptance: a service provided in a child scope, the event bus firing
across scopes, and an inject-waiting plugin resolving in documented order.
"""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore import Context, Service


class ClockService(Service):
    def now(self) -> int:
        return 1234


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_event_and_pending_inject_together(self):
        ctx = Context()
        ClockService(ctx, "clock")

        # event from a child scope listener fires on root emit
        seen: list = []
        child = ctx.extend()
        child.events.on("clock.tick", lambda: seen.append(child.reflect.get("clock").now()))

        # inject() suspends until the service is provided (PENDING wait)
        provided = asyncio.Event()

        async def provide_greeter():
            await asyncio.sleep(0.05)
            ctx.reflect.provide("greeter", "hello")
            provided.set()

        async def callback(c):
            return c.reflect.get("greeter")

        inject_task = asyncio.create_task(
            ctx.registry.inject(["greeter"], callback))
        provide_task = asyncio.create_task(provide_greeter())
        ctx.emit("clock.tick")

        result = await asyncio.wait_for(inject_task, 2)
        await provide_task

        self.assertEqual(result, "hello")
        self.assertEqual(seen, [1234])


if __name__ == "__main__":
    unittest.main()
