"""04 -- Epoch reload and config hot update, without a restart.

Business value: operations never stop the world for a config change. When
a payment platform rotates its webhook secret, or a dependency service is
upgraded, the plugins that depend on them reload themselves -- identity
preserved, generation bumped, fresh code/config visible immediately.

Run: python examples/04_epoch_reload_and_hot_config.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from anvilcore import CapabilityDescriptor, Context


async def main() -> None:
    ctx = Context()
    registry = ctx.registry
    seen: list[str] = []

    # ---- part 1: config hot update --------------------------------------
    def report_factory(scope):
        class Report:
            async def install(self) -> None:
                seen.append(f"greeting={scope.config['greeting']}")
        return Report()

    descriptor = CapabilityDescriptor(
        id="report", version="1", factory=report_factory,
        config={"greeting": "hi"})
    await registry.plugin(descriptor)
    gen1 = ctx.registry.manager.get("report").installation_generation
    print("installed:", seen[-1], "| generation:", gen1)

    # schema-validated hot update: bad config fails WITHOUT touching the
    # running plugin; good config reloads in place
    def schema(raw: dict) -> dict:
        if "greeting" not in raw:
            raise ValueError("greeting required")
        return raw

    descriptor = ctx.registry.manager.get("report").descriptor
    object.__setattr__(descriptor, "schema", schema)

    try:
        await registry.update("report", config={})
    except ValueError:
        print("bad config rejected, plugin untouched")

    await registry.update("report", config={"greeting": "hello"})
    gen2 = ctx.registry.manager.get("report").installation_generation
    print("hot-updated:", seen[-1], "| generation:", gen2, "(identity kept)")

    # ---- part 2: epoch reload -------------------------------------------
    # "app" declares a dependency on the SERVICE "conn" (a provided value,
    # not another capability). When "conn" is re-provided with a new
    # generation, app reloads automatically and binds the new instance.
    ctx.reflect.provide("conn", "conn-v1")

    bound: list[str] = []

    def app_factory(scope):
        class App:
            async def install(self) -> None:
                bound.append(ctx.reflect.get("conn"))
        return App()

    app = CapabilityDescriptor(id="app", version="1", factory=app_factory,
                               dependencies=("conn",))
    await registry.plugin(app)
    app_gen1 = ctx.registry.manager.get("app").installation_generation
    print("app installed, bound:", bound[-1], "| generation:", app_gen1)

    # the dependency is upgraded: the consumer reloads ITSELF
    ctx.reflect.provide("conn", "conn-v2")
    for _ in range(20):
        await asyncio.sleep(0.01)
        if ctx.registry.manager.get("app").installation_generation > app_gen1:
            break
    app_gen2 = ctx.registry.manager.get("app").installation_generation
    print("auto-reloaded, bound:", bound[-1], "| generation:", app_gen2)

    # an unrelated service changing does NOT reload app (watchers are
    # per-dependency, not global)
    ctx.reflect.provide("unrelated", "x")
    await asyncio.sleep(0.05)
    print("after unrelated change, generation still:", app_gen2)


if __name__ == "__main__":
    asyncio.run(main())
