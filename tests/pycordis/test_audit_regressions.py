"""Regression tests for the parity security/stability audit (2026-09-28).

Each test reproduces a finding from the audit and pins the fix:
  F1  registry._configs was a class attribute (cross-instance leak)
  F2  re-provide grew scope effect batches unboundedly (now: replace)
  F3  duplicate same-label entries accumulated (now: newest wins)
  F4  cancelled waiters lingered in the waiter bucket (now: cleaned)
  F5  listener removal without existence guard could raise in nested
      dispatch (now: guarded)
"""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pycordis import Context
from pycordis.registry import RegistryService


class AuditRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_f1_configs_are_per_instance(self):
        r1, r2 = RegistryService(), RegistryService()
        r1._configs["a"] = {"x": 1}
        self.assertIsNone(r2._configs.get("a"))

    async def test_f2_reprovide_does_not_grow_batches(self):
        ctx = Context()
        for _ in range(50):
            ctx.reflect.provide("svc", 1)
        self.assertEqual(len(ctx.scope.effects.batches), 1)

    async def test_f3_reprovide_replaces_entry(self):
        ctx = Context()
        for i in range(50):
            ctx.reflect.provide("dup", i)
        self.assertEqual(len(ctx._store._entries["dup"]), 1)
        self.assertEqual(ctx.reflect.get("dup"), 49)

    async def test_f4_cancelled_waiter_is_removed(self):
        ctx = Context()
        task = asyncio.create_task(
            ctx.registry.inject(["never"], lambda c: "x"))
        await asyncio.sleep(0.02)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        # fixed behavior: the whole bucket is popped once empty
        self.assertFalse(ctx._store._waiters.get("never"))

    async def test_f5_once_listener_survives_removal_race(self):
        ctx = Context()
        calls = {"n": 0}

        def fire_inner():
            ctx.emit("inner")

        ctx.events.on("inner", lambda: None, once=True)
        ctx.events.on("outer", fire_inner)
        ctx.events.on("inner", lambda: calls.__setitem__("n", calls["n"] + 1))
        ctx.emit("outer")   # inner once removed while outer dispatch alive
        ctx.emit("inner")
        # fixed behavior: no ValueError from double-removal during nested
        # dispatch; the plain (non-once) counter may legitimately fire again
        self.assertIn(calls["n"], (1, 2))


if __name__ == "__main__":
    unittest.main()
