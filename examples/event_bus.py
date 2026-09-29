"""02 -- The event bus: five dispatch modes, scope-filtered listeners.

Business value: agents composed from many plugins need to react to each
other without importing each other. Events decouple them; isolate
filtering keeps a verification-stage listener from hearing production
traffic.

Run: python examples/02_event_bus.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from anvilcore import Context


async def main() -> None:
    ctx = Context()

    # emit: fire synchronously, in registration order
    seen: list = []
    ctx.on("audit", lambda: seen.append("a"))
    ctx.on("audit", lambda: seen.append("b"))
    ctx.emit("audit")
    print("emit order:", seen)

    # once: auto-disposed after the first fire, re-entrancy safe
    n = {"count": 0}
    ctx.on("tick", lambda: n.__setitem__("count", n["count"] + 1), once=True)
    ctx.emit("tick")
    ctx.emit("tick")
    print("once fired:", n["count"], "time(s)")

    # serial: await listeners one by one, in order
    order: list = []

    async def slow():
        await asyncio.sleep(0.05)
        order.append("slow")

    async def fast():
        order.append("fast")

    ctx.on("run", slow)
    ctx.on("run", fast)
    await ctx.serial("run")
    print("serial order:", order)

    # parallel: run concurrently, failures surface as ExceptionGroup
    ctx.on("job", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        await ctx.parallel("job")
    except ExceptionGroup as eg:
        print("parallel surfaced:", [type(e).__name__ for e in eg.exceptions])

    # bail: first truthy result wins (feature flags, circuit breakers)
    ctx.on("check", lambda: None)
    ctx.on("check", lambda: "blocked")
    ctx.on("check", lambda: "never-reached")
    print("bail result:", ctx.bail("check"))

    # waterfall: thread a value through the chain (middleware pipeline)
    ctx.on("price", lambda v: v + 10)
    ctx.on("price", lambda v: v * 2)
    print("waterfall 5 ->", ctx.waterfall("price", 5))

    # scope filtering: a listener registered under an isolated name only
    # hears emitters sharing that isolation
    prod = ctx.isolate("trade")
    heard: list = []
    prod.on("trade", lambda: heard.append("prod-listener"))
    ctx.emit("trade")
    print("default emitter heard:", heard)
    prod.emit("trade")
    print("isolated emitter heard:", heard)


if __name__ == "__main__":
    asyncio.run(main())
