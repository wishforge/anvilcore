"""Graceful rotation: retire (stop accepting new work), drain in-flight,
then dispose. Fixes the rotation gap where the toolkit briefly loses the
tool between unloading the old generation and installing the new one."""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore import CapabilityDescriptor, Context


class ToolKitStub:
    def __init__(self):
        self.names: list = []
        self.on_change = None

    def register(self, name, fn) -> None:
        if name in self.names:               # adapter now replaces on same name
            self.names.remove(name)
        self.names.append(name)
        if self.on_change:
            self.on_change(self.names)

    def unregister(self, name) -> None:
        if name in self.names:
            self.names.remove(name)
        if self.on_change:
            self.on_change(self.names)


class GracefulRotationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()
        self.registry = self.ctx.registry
        self.toolkit = ToolKitStub()
        self.seen_toolsets: list = []
        self.toolkit.on_change = lambda names: self.seen_toolsets.append(list(names))

    def make_descriptor(self, label: str, log: list, gate: asyncio.Event,
                        inflight: dict):
        toolkit = self.toolkit

        def factory(scope):
            class Cap:
                async def install(self) -> None:
                    async def slow_verify(payload: str) -> str:
                        if not scope.acquire():          # the drain contract
                            log.append(("rejected", scope.config["label"]))
                            return "draining"
                        try:
                            log.append(("start", scope.name))
                            await gate.wait()
                            log.append(("finish", scope.name))
                        finally:
                            scope.release()
                        return "done"

                    self.slow_verify = slow_verify
                    toolkit.register("slow_verify", slow_verify)

                    async def publish(collect):
                        collect("tool", lambda: toolkit.unregister("slow_verify"))

                    await scope.effect("install", publish)

            return Cap()

        return CapabilityDescriptor(id="app", version="1", factory=factory,
                                    config={"label": label}, dependencies=("conn",))

    async def test_graceful_rotation_full_semantics(self):
        """Q1+Q2 fixed: in-flight finishes on gen1 config, no toolkit gap,
        new requests immediately served by gen2, gen1 dies only when drained."""
        ctx = self.ctx
        ctx.reflect.provide("conn", "conn-v1")
        log: list = []
        gate = asyncio.Event()
        inflight: dict = {}

        await self.registry.plugin(
            self.make_descriptor("gen1", log, gate, inflight))
        kernel_cap = self.registry.manager.get("app").instance
        cap1 = kernel_cap.instance          # business instance (gen1)
        gen1 = self.registry.manager.get("app").installation_generation

        # park an in-flight call on gen1
        task = asyncio.create_task(cap1.slow_verify("p"))
        await asyncio.sleep(0.02)
        self.assertEqual(log, [("start", "app#1")])

        # rotate: gen2 goes live WITHOUT waiting for gen1's drain
        ctx.reflect.provide("conn", "conn-v2")
        for _ in range(20):
            await asyncio.sleep(0.01)
            if self.registry.manager.get("app").installation_generation > gen1:
                break
        gen2 = self.registry.manager.get("app").installation_generation
        cap2 = self.registry.manager.get("app").instance.instance
        self.assertEqual(gen2, gen1 + 1)

        # Q2: the toolkit never went empty during the whole rotation
        self.assertFalse([s for s in self.seen_toolsets if not s],
                         f"empty toolkit observed: {self.seen_toolsets}")

        # Q3: gen1 is retired (no longer ACTIVE) but NOT disposed yet --
        #     it is draining; the parked call still runs on gen1 config
        self.assertIn(kernel_cap.state, ("RETIRING", "DISPOSING"))
        self.assertNotEqual(kernel_cap.state, "ACTIVE")
        gate.set()
        await task
        self.assertIn(("finish", "app#1"), log)
        self.assertNotIn(("finish", "app#2"), log)
        # drain complete -> gen1 disposed
        for _ in range(20):
            await asyncio.sleep(0.01)
            if kernel_cap.state == "DISPOSED":
                break
        self.assertEqual(kernel_cap.state, "DISPOSED")

        # Q4: new work goes to gen2
        task2 = asyncio.create_task(cap2.slow_verify("p2"))
        await asyncio.sleep(0.02)
        gate.set()
        await task2
        self.assertIn(("start", "app#2"), log)   # gen2 serves the new call

    async def test_retire_rejects_new_acquisitions(self):
        ctx = self.ctx
        ctx.reflect.provide("conn", "v1")
        log: list = []
        gate = asyncio.Event()
        await self.registry.plugin(
            self.make_descriptor("gen1", log, gate, {}))
        kernel_cap = self.registry.manager.get("app").instance

        self.assertTrue(kernel_cap.scope.acquire())
        kernel_cap.scope.release()
        # simulate rotation
        kernel_cap.retire()
        self.assertFalse(kernel_cap._acquire(), "retired capability must refuse new work")


if __name__ == "__main__":
    unittest.main()


class BoundedDrainTests(unittest.IsolatedAsyncioTestCase):
    """gunicorn graceful_timeout pattern: bounded wait, then force-cancel."""

    async def test_unload_deadline_cancels_hung_inflight(self):
        ctx = Context()
        registry = ctx.registry
        started = asyncio.Event()

        def factory_fixed(scope):
            class Cap:
                async def install(self) -> None:
                    async def hung() -> str:
                        if not scope.acquire():
                            return "draining"
                        try:
                            started.set()
                            await asyncio.sleep(60)
                            return "done"
                        finally:
                            scope.release()   # correct business hygiene
                        # note: even WITHOUT this finally, the framework's
                        # forced cancel zeroes the count (see _cancel_inflight)

                    async def publish(collect):
                        collect("tool:hung", lambda: None)

                    self.hung = hung
                    await scope.effect("install", publish)
            return Cap()

        d = CapabilityDescriptor(id="app", version="1", factory=factory_fixed)
        await registry.plugin(d)
        cap = registry.manager.get("app").instance.instance
        task = asyncio.create_task(cap.hung())
        await started.wait()

        # unload with a drain deadline: the hung call gets cancelled at T+0.1s
        await registry.manager.unload("app", drain_timeout=0.1)
        self.assertEqual(registry.manager.get("app").instance.state, "DISPOSED")
        with self.assertRaises(asyncio.CancelledError):
            await task   # the hung call did not survive the deadline
