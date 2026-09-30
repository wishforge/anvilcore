"""ReflectService: named value layer with scope-owned provides and waiters."""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore.context import Context


class ReflectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()

    async def test_provide_then_get(self):
        self.ctx.reflect.provide("fs", object())
        self.assertIsNotNone(self.ctx.reflect.get("fs"))

    async def test_get_missing_returns_none(self):
        # cordis reflect.get never raises; Context.__getattr__ does
        self.assertIsNone(self.ctx.reflect.get("nope"))
        self.assertIsNone(self.ctx.reflect.get("nope", strict=True))

    async def test_strict_hides_disposing_provider(self):
        # cordis reflect.ts:233-243 -- strict requires owner scope ACTIVE;
        # the observable window is teardown-in-progress (after dispose the
        # entry is physically removed, so both modes return None)
        scope = self.ctx.scope.child("owner")
        inner = Context(parent=self.ctx, scope=scope)
        inner.reflect.provide("svc", "x")
        self.assertIsNotNone(self.ctx.reflect.get("svc"))
        scope.state = "DISPOSING"
        self.assertIsNone(self.ctx.reflect.get("svc"))           # strict (default)
        self.assertIsNotNone(self.ctx.reflect.get("svc", strict=False))

    async def test_disposer_removes_value(self):
        off = self.ctx.reflect.provide("fs", "v1")
        off()
        self.assertIsNone(self.ctx.reflect.get("fs"))

    async def test_set_overwrites(self):
        self.ctx.reflect.provide("fs", "v1")
        self.ctx.reflect.set("fs", "v2")
        self.assertEqual(self.ctx.reflect.get("fs"), "v2")

    async def test_owner_scope_dispose_removes_value(self):
        scope = self.ctx.scope.child("owner")
        inner = Context(parent=self.ctx, scope=scope)
        inner.reflect.provide("svc", "x")
        self.assertIsNotNone(self.ctx.reflect.get("svc"))
        await scope.dispose()
        self.assertIsNone(self.ctx.reflect.get("svc"))

    async def test_wait_resolves_after_provide_and_notify(self):
        task = asyncio.create_task(self.ctx.reflect.wait("late"))
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.ctx.reflect.provide("late", 42)
        self.ctx.reflect.notify("late")
        self.assertEqual(await asyncio.wait_for(task, 1), 42)


if __name__ == "__main__":
    unittest.main()
