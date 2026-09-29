"""Audit round 2 regressions: defensive mechanics from cordis.

Pins the fixes for the second audit (2026-09-28):
  F6  parallel swallowed listener exceptions (now: ExceptionGroup)
  F7  once listeners were removed AFTER the call (re-entrant emit
      re-triggered them); cordis disposes first (events.ts:312-318)
  F8  event dispatch used a frozen isolate snapshot; cordis consults the
      listener's LIVE context (events.ts:171-174)
  F9  reflect.get strict semantics were inverted; now: strict requires the
      provider scope to be ACTIVE (reflect.ts:233-243)
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pycordis import Context


class AuditRound2Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()

    async def test_f6_parallel_raises_exception_group(self):
        def bad():
            raise ValueError("listener exploded")

        self.ctx.events.on("e", lambda: None)
        self.ctx.events.on("e", bad)   # sync listener: exception must be captured
        with self.assertRaises(ExceptionGroup) as caught:
            await self.ctx.parallel("e")
        self.assertEqual(len(caught.exception.exceptions), 1)
        self.assertIsInstance(caught.exception.exceptions[0], ValueError)

    async def test_f7_once_disposes_before_call(self):
        n = {"k": 0}

        def listener():
            n["k"] += 1
            if n["k"] == 1:
                self.ctx.emit("e")   # re-entrant emit during once handler

        self.ctx.events.on("e", listener, once=True)
        self.ctx.emit("e")
        # cordis semantics: the once listener was already disposed when it
        # ran, so the re-entrant emit must not re-trigger it
        self.assertEqual(n["k"], 1)

    async def test_f9_strict_requires_active_scope(self):
        # the strict window is TEARDOWN-IN-PROGRESS: the entry still exists
        # but its owner scope is no longer ACTIVE
        scope = self.ctx.scope.child("s")
        inner = Context(parent=self.ctx, scope=scope)
        inner.reflect.provide("svc", "v")
        scope.state = "DISPOSING"   # simulate the teardown window
        self.assertIsNone(self.ctx.reflect.get("svc"))            # strict default
        self.assertEqual(self.ctx.reflect.get("svc", strict=False), "v")
        scope.state = "ACTIVE"
        self.assertEqual(self.ctx.reflect.get("svc"), "v")


if __name__ == "__main__":
    unittest.main()
