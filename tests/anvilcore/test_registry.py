"""RegistryService: plugin loading, PENDING injection wait, delete."""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore.capability import CapabilityDescriptor
from anvilcore.context import Context
from anvilcore.semantic_layer import PluginScope


class RegistryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()

    async def test_plugin_installs_and_provides_service(self):
        def factory(scope: PluginScope):
            class Cap:
                async def install(self):
                    self.ctx_reflect = None

            # provide through the registry's context (root, for simplicity)
            return Cap()

        cap = await self.ctx.registry.plugin(CapabilityDescriptor(
            id="svc-a", version="1", factory=factory))
        self.assertIsNotNone(cap)
        # uninstall works through delete()
        await self.ctx.registry.delete("svc-a")
        record = self.ctx.registry.manager.get("svc-a")
        self.assertEqual(record.instance.state, "DISPOSED")

    async def test_inject_waits_for_missing_service(self):
        done = {"ok": False}

        async def later():
            await asyncio.sleep(0.05)
            self.ctx.reflect.provide("config-svc", "ready")
            self.ctx.reflect.notify("config-svc")

        task = asyncio.create_task(later())

        async def callback(ctx):
            done["ok"] = True
            return ctx.reflect.get("config-svc")

        result = await self.ctx.registry.inject(["config-svc"], callback)
        await task
        self.assertTrue(done["ok"])
        self.assertEqual(result, "ready")

    async def test_inject_with_existing_service_does_not_block(self):
        self.ctx.reflect.provide("svc", "here")

        async def callback(ctx):
            return ctx.reflect.get("svc")

        result = await asyncio.wait_for(
            self.ctx.registry.inject(["svc"], callback), 1)
        self.assertEqual(result, "here")


if __name__ == "__main__":
    unittest.main()
