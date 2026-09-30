import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore import CapabilityDescriptor, Context


class ToolKitStub:
    def __init__(self):
        self.names: list = []
        self._fns: dict = {}
        self.on_change = None

    def register(self, name, fn) -> None:
        self.names.append(name)
        self._fns[name] = fn
        if self.on_change:
            self.on_change(self.names)

    def unregister(self, name) -> None:
        if name in self.names:
            self.names.remove(name)
            del self._fns[name]
        if self.on_change:
            self.on_change(self.names)


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()
        self.registry = self.ctx.registry
        self.toolkit = ToolKitStub()
        self.seen_toolsets: list = []
        self.toolkit.on_change = lambda names: self.seen_toolsets.append(list(names))

    def make_descriptor(self, label: str, log: list, gate: asyncio.Event):
        toolkit = self.toolkit

        def factory(scope):
            class Cap:
                async def install(self) -> None:
                    async def slow_verify(payload: str) -> str:
                        # closure over the TEST's log/gate, not `self`
                        log.append(("start", scope.config["label"]))
                        await gate.wait()
                        log.append(("finish", scope.config["label"]))
                        return "done"

                    self.slow_verify = slow_verify
                    toolkit.register("slow_verify", slow_verify)

                    async def publish(collect):
                        collect("tool", lambda: toolkit.unregister("slow_verify"))

                    await scope.effect("install", publish)

            return Cap()

        return CapabilityDescriptor(id="app", version="1", factory=factory,
                                    config={"label": label})

    async def test_q1_inflight_call_survives_rotation_with_which_config(self):
        log: list = []
        gate = asyncio.Event()
        await self.registry.plugin(self.make_descriptor("gen1", log, gate))
        cap1 = self.registry.manager.get("app").instance.instance

        task = asyncio.create_task(cap1.slow_verify("p"))
        await asyncio.sleep(0.02)
        self.assertEqual(log, [("start", "gen1")], "call must start on gen1")

        await self.registry.update("app", config={"label": "gen2"})
        gen2 = self.registry.manager.get("app").installation_generation
        print(f"Q1 after rotation: gen={gen2}")

        gate.set()
        await task
        print("Q1 in-flight log:", log)
        # the parked call must finish -- and must NOT observe gen2's config
        self.assertIn(("finish", "gen1"), log)
        self.assertNotIn(("finish", "gen2"), log)

    async def test_q2_toolkit_gap_during_rotation(self):
        await self.registry.plugin(self.make_descriptor("g1", [], asyncio.Event()))
        await self.registry.update("app", config={"label": "g2"})
        empties = [s for s in self.seen_toolsets if not s]
        print("Q2 toolsets observed:", self.seen_toolsets)
        print("Q2 empty-toolkit moments:", len(empties))


if __name__ == "__main__":
    unittest.main()
