"""03 -- Capability lifecycle: install, verify, unload, zero residue.

Business value: a plugin that cannot be cleanly retracted is a liability,
not a feature. Every registration made during install() carries its own
disposer, so unloading leaves nothing behind -- verified, not promised.

Run: python examples/03_capability_lifecycle.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agentscope.tool import FunctionTool, Toolkit

from pycordis import CapabilityDescriptor, PluginManager
from pycordis.adapters.agentscope import register_tool
from pycordis.semantic_layer import PluginScope


class RateLimiterCapability:
    """Publishes a `check_rate` tool into an AgentScope Toolkit."""

    def __init__(self, scope: PluginScope, toolkit: Toolkit):
        self.scope = scope
        self.toolkit = toolkit
        self.calls = 0

    async def install(self) -> None:
        def check_rate(key: str) -> str:
            self.calls += 1
            return f"allow#{self.calls}" if self.calls <= 3 else "rate-limited"

        tool = FunctionTool(check_rate, name="check_rate")
        unregister = register_tool(self.toolkit, tool)

        async def publish(collect):
            # the rule: every registration pairs with its own disposer
            collect("tool:check_rate", unregister)

        await self.scope.effect("rate-limiter.install", publish)


def make_factory(toolkit: Toolkit):
    def factory(scope: PluginScope):
        return RateLimiterCapability(scope, toolkit)
    return factory


async def tool_names(toolkit: Toolkit) -> list:
    return [s["function"]["name"] for s in await toolkit.get_tool_schemas()]


async def main() -> None:
    toolkit = Toolkit()
    manager = PluginManager()
    descriptor = CapabilityDescriptor(
        id="rate-limiter", version="1", factory=make_factory(toolkit))
    manager.register(descriptor)

    # install: the tool appears in the model's world
    await manager.install("rate-limiter")
    gen1 = manager.get("rate-limiter").installation_generation
    print("installed, generation:", gen1)
    print("model schema:", await tool_names(toolkit))

    # unload: the tool disappears, nothing is left behind
    await manager.unload("rate-limiter")
    print("after unload, schema:", await tool_names(toolkit) or "[]")
    print("unload again (idempotent):", await manager.unload("rate-limiter"))

    # reinstall: a fresh generation, still clean
    await manager.install("rate-limiter")
    gen2 = manager.get("rate-limiter").installation_generation
    print("reinstalled, generation:", gen2, "(must be > gen1)")

    # audit: every effect batch of every disposed generation was cleaned
    ghosts = [b for r in manager.list()
              if r.instance is not None and r.instance.state == "DISPOSED"
              for b in r.instance.scope.effects.batches if b.state != "DISPOSED"]
    print("residue (ghost batches):", len(ghosts))


if __name__ == "__main__":
    asyncio.run(main())
