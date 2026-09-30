"""EventsService: cordis events.ts semantics -- five dispatch modes, scope
filtering, listeners owned by the registering context's scope."""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore.context import Context


class EventsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()
        self.seen: list = []

    async def test_emit_sync_order(self):
        self.ctx.events.on("e", lambda: self.seen.append(1))
        self.ctx.events.on("e", lambda: self.seen.append(2))
        self.ctx.emit("e")
        self.assertEqual(self.seen, [1, 2])

    async def test_once_fires_once(self):
        n = {"k": 0}
        self.ctx.events.once("e", lambda: n.__setitem__("k", n["k"] + 1))
        self.ctx.emit("e")
        self.ctx.emit("e")
        self.assertEqual(n["k"], 1)

    async def test_parallel_runs_concurrently(self):
        import time
        async def sleeper(tag, delay):
            async def run():
                await asyncio.sleep(delay)
                self.seen.append((tag, time.perf_counter()))
            return run
        s1 = await sleeper("a", 0.2)
        s2 = await sleeper("b", 0.1)
        self.ctx.events.on("e", s1)
        self.ctx.events.on("e", s2)
        t0 = time.perf_counter()
        await self.ctx.parallel("e")
        # concurrent: total ~ max(0.2, 0.1), not the sum
        self.assertLess(time.perf_counter() - t0, 0.35)
        self.assertEqual({t for t, _ in self.seen}, {"a", "b"})

    async def test_serial_runs_in_order(self):
        async def make(tag, delay):
            async def run():
                await asyncio.sleep(delay)
                self.seen.append(tag)
            return run
        self.ctx.events.on("e", await make("slow", 0.05))
        self.ctx.events.on("e", await make("fast", 0.0))
        await self.ctx.serial("e")
        self.assertEqual(self.seen, ["slow", "fast"])

    async def test_bail_stops_at_first_truthy(self):
        self.ctx.events.on("e", lambda: None)
        self.ctx.events.on("e", lambda: "found")
        self.ctx.events.on("e", lambda: "should-not-run")
        self.assertEqual(self.ctx.bail("e"), "found")

    async def test_waterfall_threads_argument(self):
        self.ctx.events.on("wf", lambda v: v + 1)
        self.ctx.events.on("wf", lambda v: v * 10)
        # 1 -> +1 -> 2 -> *10 -> 20 (chained, not per-listener on the seed)
        self.assertEqual(self.ctx.waterfall("wf", 1), 20)

    async def test_child_listener_fires_from_parent_emit(self):
        child = self.ctx.extend()
        child.events.on("e", lambda: self.seen.append("child"))
        self.ctx.emit("e")
        self.assertEqual(self.seen, ["child"])

    async def test_isolated_listener_not_fired_from_outside(self):
        child = self.ctx.isolate("e")
        child.events.on("e", lambda: self.seen.append("child"))
        self.ctx.emit("e")
        self.assertEqual(self.seen, [])
        child.emit("e")
        self.assertEqual(self.seen, ["child"])

    async def test_listener_dies_with_scope(self):
        scope = self.ctx.scope.child("owner")
        inner = Context(parent=self.ctx, scope=scope)
        inner.events.on("e", lambda: self.seen.append("x"))
        await scope.dispose()
        self.ctx.emit("e")
        self.assertEqual(self.seen, [])


if __name__ == "__main__":
    unittest.main()
