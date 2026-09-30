"""Epoch reload + config hot update (the last two parity gaps).

cordis semantics being pinned:
  - a plugin whose inject dependency is re-provided (new generation) is
    automatically reloaded, keeping its identity but bumping the generation
  - registry.update(plugin_id, config) revalidates and reloads the plugin
    in place; the new config is visible to the fresh generation
"""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore import CapabilityDescriptor, Context


class EpochReloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()

    async def test_reprovide_dependency_auto_reloads_consumer(self):
        """EPOCH RELOAD: re-provide('conn') -> 'app' reloads automatically."""
        ctx = self.ctx
        saw: list[str] = []

        def factory(scope):
            class App:
                async def install(self) -> None:
                    saw.append(ctx.reflect.get("conn"))
            return App()

        self.ctx.reflect.provide("conn", "conn-gen-1")
        descriptor = CapabilityDescriptor(
            id="app", version="1", factory=factory, dependencies=("conn",))
        await self.ctx.registry.plugin(descriptor)
        gen1 = self.ctx.registry.manager.get("app").installation_generation
        self.assertEqual(saw, ["conn-gen-1"])

        # the dependency is re-provided: a NEW generation of the service
        self.ctx.reflect.provide("conn", "conn-gen-2")

        # give the auto-reload task a chance to run
        for _ in range(20):
            await asyncio.sleep(0.01)
            if self.ctx.registry.manager.get("app").installation_generation > gen1:
                break

        record = self.ctx.registry.manager.get("app")
        self.assertGreater(record.installation_generation, gen1,
                           "consumer must auto-reload when its dependency is re-provided")
        self.assertEqual(saw[-1], "conn-gen-2",
                         "fresh generation must bind the NEW service instance")

    async def test_unrelated_notify_does_not_reload(self):
        reloads = {"n": 0}
        gen0 = None

        def factory(scope):
            class App:
                async def install(self) -> None:
                    reloads["n"] += 1
            return App()

        self.ctx.reflect.provide("conn", "v1")
        d = CapabilityDescriptor(id="app", version="1", factory=factory,
                                 dependencies=("conn",))
        await self.ctx.registry.plugin(d)
        gen0 = self.ctx.registry.manager.get("app").installation_generation

        self.ctx.reflect.provide("unrelated", "x")   # different service
        await asyncio.sleep(0.05)
        self.assertEqual(self.ctx.registry.manager.get("app").installation_generation,
                         gen0)
        self.assertEqual(reloads["n"], 1)


class ConfigHotUpdateTests(unittest.IsolatedAsyncioTestCase):
    async def test_update_reloads_with_new_config_same_identity(self):
        ctx = Context()
        seen: list[dict] = []

        def factory(scope):
            class App:
                async def install(self) -> None:
                    seen.append(dict(scope.config))
            return App()

        descriptor = CapabilityDescriptor(
            id="report", version="1", factory=factory,
            config={"greeting": "hi"})

        await ctx.registry.plugin(descriptor)
        self.assertEqual(seen, [{"greeting": "hi"}])
        gen1 = ctx.registry.manager.get("report").installation_generation

        await ctx.registry.update("report", config={"greeting": "hello"})

        record = ctx.registry.manager.get("report")
        self.assertGreater(record.installation_generation, gen1)
        self.assertEqual(seen[-1], {"greeting": "hello"})

    async def test_update_validates_through_schema(self):
        ctx = Context()

        def schema(raw: dict) -> dict:
            if "level" not in raw:
                raise ValueError("level required")
            return {"level": raw["level"]}

        seen: list[dict] = []

        def factory(scope):
            class App:
                async def install(self) -> None:
                    seen.append(dict(scope.config))
            return App()

        d = CapabilityDescriptor(id="svc", version="1", factory=factory,
                                 config={"level": "info"}, schema=schema)
        await ctx.registry.plugin(d)

        with self.assertRaises(ValueError):
            await ctx.registry.update("svc", config={})
        self.assertEqual(seen, [{"level": "info"}])   # unchanged on failure

        await ctx.registry.update("svc", config={"level": "debug"})
        self.assertEqual(seen[-1], {"level": "debug"})


if __name__ == "__main__":
    unittest.main()
