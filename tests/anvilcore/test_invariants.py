"""Phase 1's 12 Cordis semantic invariants, formalized for the Phase 2-A kernel."""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore.semantic_layer import (
    ACTIVE,
    CLEANED,
    DISPOSED,
    DISPOSING,
    FAILED,
    DependencyLifecycle,
    PluginScope,
)


class InvariantTests(unittest.IsolatedAsyncioTestCase):
    async def test_01_every_effect_has_an_owner(self) -> None:
        root = PluginScope("root")
        child = root.child("child")
        effects = []

        def setup(collect):
            effects.append(collect("e1", lambda: None))

        await root.effect("root-batch", setup)
        await child.effect("child-batch", setup)

        self.assertEqual(len(effects), 2)
        for effect in effects:
            self.assertIsNotNone(effect.owner)
            self.assertEqual(effect.state, ACTIVE)
            self.assertGreater(effect.order, 0)
        self.assertIs(effects[0].owner, root)
        self.assertIs(effects[1].owner, child)

    async def test_02_owner_has_a_lifecycle_scope(self) -> None:
        root = PluginScope("root")
        seen = []

        def setup(collect):
            collect("probe", lambda: seen.append(root.state))

        await root.effect("probe-batch", setup)
        self.assertEqual(root.state, ACTIVE)
        errors = await root.dispose()
        self.assertEqual(errors, [])
        self.assertEqual(root.state, DISPOSED)
        self.assertEqual(seen, [DISPOSING])

        with self.assertRaises(RuntimeError):
            await root.effect("late", lambda collect: None)
        with self.assertRaises(RuntimeError):
            root.child("late-child")

    async def test_03_disposing_owner_disposes_owned_effects(self) -> None:
        root = PluginScope("root")
        tools = {}
        runs = 0

        def setup(collect):
            nonlocal runs
            tools["fs_read"] = object()

            def cleanup():
                nonlocal runs
                runs += 1
                tools.pop("fs_read", None)

            collect("tool:fs_read", cleanup)

        batch = await root.effect("install", setup)
        effect = batch.effects[0]
        self.assertIn("fs_read", tools)

        errors = await root.dispose()
        self.assertEqual(errors, [])
        self.assertNotIn("fs_read", tools)
        self.assertEqual(runs, 1)
        self.assertEqual(effect.state, CLEANED)
        self.assertEqual(batch.state, DISPOSED)

    async def test_04_teardown_order_is_deterministic(self) -> None:
        root = PluginScope("root")
        log = []

        def blocked(name):
            started = asyncio.Event()
            release = asyncio.Event()

            async def cleanup():
                log.append(f"{name}:start")
                started.set()
                await release.wait()
                log.append(f"{name}:end")

            return cleanup, started, release

        release_c = asyncio.Event()
        started_c = asyncio.Event()

        async def cleanup_c():
            log.append("serial:C:start")
            started_c.set()
            await release_c.wait()
            log.append("serial:C:end")

        async def cleanup_b():
            log.append("serial:B:start")
            log.append("serial:B:end")

        async def cleanup_a():
            log.append("serial:A:start")
            log.append("serial:A:end")

        def setup_serial(collect):
            collect("A", cleanup_a)
            collect("B", cleanup_b)
            collect("C", cleanup_c)

        await root.effect("serial", setup_serial)

        cleanup_s2, started_s2, release_s2 = blocked("s2")
        cleanup_s3, started_s3, release_s3 = blocked("s3")
        await root.effect("s2", lambda collect: collect("s2", cleanup_s2))
        await root.effect("s3", lambda collect: collect("s3", cleanup_s3))

        dispose_task = asyncio.create_task(root.dispose())
        await asyncio.wait_for(started_s3.wait(), timeout=1)
        await asyncio.wait_for(started_s2.wait(), timeout=1)
        await asyncio.wait_for(started_c.wait(), timeout=1)

        # Sibling batches start in reverse registration order; the serial
        # batch's C has started too, so all three are in flight concurrently.
        self.assertEqual(log[:3], ["s3:start", "s2:start", "serial:C:start"])
        self.assertFalse(dispose_task.done())

        release_s2.set()
        release_s3.set()
        release_c.set()
        errors = await asyncio.wait_for(dispose_task, timeout=1)

        self.assertEqual(errors, [])
        self.assertEqual(
            log,
            [
                "s3:start",
                "s2:start",
                "serial:C:start",
                "s2:end",
                "s3:end",
                "serial:C:end",
                "serial:B:start",
                "serial:B:end",
                "serial:A:start",
                "serial:A:end",
            ],
        )

    async def test_05_dependency_teardown_respects_dependency_ordering(self) -> None:
        deps = DependencyLifecycle()
        fast = PluginScope("fast-dep")
        slow = PluginScope("slow-dep")
        deps.register_dependent("fs", fast)
        deps.register_dependent("fs", slow)

        log = []
        slow_started = asyncio.Event()
        release_slow = asyncio.Event()

        async def slow_cleanup():
            log.append("slow:cleanup:start")
            slow_started.set()
            await release_slow.wait()
            log.append("slow:cleanup:end")

        await fast.effect(
            "install", lambda collect: collect("e", lambda: log.append("fast:cleanup")))
        await slow.effect(
            "install", lambda collect: collect("slow", slow_cleanup))

        def finalizer():
            self.assertEqual(fast.state, DISPOSED)
            self.assertEqual(slow.state, DISPOSED)
            log.append("provider:finalizer")

        release_task = asyncio.create_task(deps.release("fs", finalizer))
        await asyncio.wait_for(slow_started.wait(), timeout=1)
        self.assertNotIn("provider:finalizer", log)
        release_slow.set()
        errors = await asyncio.wait_for(release_task, timeout=1)

        self.assertEqual(errors, [])
        self.assertEqual(log[-1], "provider:finalizer")
        self.assertIn("fast:cleanup", log)
        self.assertIn("slow:cleanup:start", log)
        self.assertIn("slow:cleanup:end", log)

    async def test_06_cleanup_is_idempotent(self) -> None:
        root = PluginScope("root")
        count = 0

        def setup(collect):
            nonlocal count

            def cleanup():
                nonlocal count
                count += 1

            collect("e", cleanup)

        await root.effect("install", setup)
        results = await asyncio.gather(root.dispose(), root.dispose())
        self.assertEqual(results, [[], []])
        self.assertEqual(count, 1)
        self.assertEqual(root.state, DISPOSED)
        self.assertEqual(await root.dispose(), [])
        self.assertEqual(count, 1)

    async def test_07_partial_initialization_rollback(self) -> None:
        root = PluginScope("root")
        tools = {}
        log = []
        boom = RuntimeError("install failed")

        def setup(collect):
            tools["t1"] = object()
            collect("t1", lambda: (log.append("t1:cleanup"), tools.pop("t1", None)))
            tools["t2"] = object()

            def failing_cleanup():
                log.append("t2:cleanup")
                tools.pop("t2", None)
                raise ValueError("cleanup boom")

            collect("t2", failing_cleanup)
            raise boom

        with self.assertRaises(RuntimeError) as cm:
            await root.effect("install", setup)

        self.assertIs(cm.exception, boom)
        self.assertEqual(log, ["t2:cleanup", "t1:cleanup"])
        self.assertEqual(tools, {})
        self.assertEqual(root.state, ACTIVE)

    async def test_08_async_tasks_and_resources_cannot_outlive_owner(self) -> None:
        root = PluginScope("root")
        log = []
        worker_stopped = asyncio.Event()
        tasks = []

        async def worker():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                worker_stopped.set()
                raise

        class Resource:
            def __init__(self):
                self.closed = asyncio.Event()

            async def __aenter__(self):
                log.append("resource:open")
                return self

            async def __aexit__(self, *exc_info):
                log.append("resource:close")
                self.closed.set()

        resource = Resource()

        async def install(collect):
            task = asyncio.create_task(worker())
            tasks.append(task)

            async def stop_task():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            collect("task", stop_task)
            await resource.__aenter__()
            collect("resource", lambda: resource.__aexit__(None, None, None))

        await root.effect("install", install)
        await root.dispose()

        self.assertTrue(tasks[0].done())
        self.assertTrue(worker_stopped.is_set())
        self.assertTrue(resource.closed.is_set())
        self.assertIn("resource:close", log)

    async def test_09_event_registrations_cannot_outlive_owner(self) -> None:
        root = PluginScope("root")
        listeners = {"fs/event": []}

        def setup(collect):
            def handler():
                pass

            listeners["fs/event"].append(handler)

            def undo():
                listeners["fs/event"].remove(handler)

            collect("event:fs/event", undo)

        await root.effect("listen", setup)
        await root.dispose()
        self.assertEqual(listeners["fs/event"], [])

    async def test_10_tool_service_registrations_cannot_outlive_owner(self) -> None:
        root = PluginScope("root")
        child = root.child("filesystem")
        tools = {}
        services = {}

        def setup(collect):
            tools["fs_read"] = object()
            collect("tool:fs_read", lambda: tools.pop("fs_read", None))
            services["fs_service"] = object()
            collect("service:fs_service", lambda: services.pop("fs_service", None))

        await child.effect("install", setup)
        self.assertIn("fs_read", tools)
        self.assertIn("fs_service", services)

        await root.dispose()
        self.assertNotIn("fs_read", tools)
        self.assertNotIn("fs_service", services)
        self.assertEqual(child.state, DISPOSED)

    async def test_11_nested_scopes_teardown_before_parent(self) -> None:
        root = PluginScope("root")
        child = root.child("child")
        log = []
        child_started = asyncio.Event()
        release_child = asyncio.Event()
        child_done = asyncio.Event()

        async def child_cleanup():
            log.append("child:start")
            child_started.set()
            await release_child.wait()
            log.append("child:end")
            child_done.set()

        def setup_child(collect):
            collect("c1", child_cleanup)

        await child.effect("install", setup_child)

        def setup_parent(collect):
            def parent_cleanup():
                self.assertTrue(child_done.is_set())
                self.assertEqual(child.state, DISPOSED)
                log.append("parent:cleanup")

            collect("p1", parent_cleanup)

        await root.effect("install", setup_parent)

        dispose_task = asyncio.create_task(root.dispose())
        await asyncio.wait_for(child_started.wait(), timeout=1)
        self.assertNotIn("parent:cleanup", log)
        release_child.set()
        errors = await asyncio.wait_for(dispose_task, timeout=1)

        self.assertEqual(errors, [])
        self.assertEqual(log, ["child:start", "child:end", "parent:cleanup"])

    async def test_12_failed_teardown_does_not_leak_remaining_effects(self) -> None:
        root = PluginScope("root")
        log = []

        def setup_failing(collect):
            collect("ok_before", lambda: log.append("ok_before"))

            def fail():
                log.append("fail")
                raise RuntimeError("cleanup boom")

            collect("fail", fail)
            collect("ok_after", lambda: log.append("ok_after"))

        failing_batch = await root.effect("failing", setup_failing)

        sibling_started = asyncio.Event()
        release_sibling = asyncio.Event()

        async def sibling_cleanup():
            log.append("sibling:start")
            sibling_started.set()
            await release_sibling.wait()
            log.append("sibling:end")

        await root.effect("sibling", lambda collect: collect("sib", sibling_cleanup))

        dispose_task = asyncio.create_task(root.dispose())
        await asyncio.wait_for(sibling_started.wait(), timeout=1)
        release_sibling.set()
        errors = await asyncio.wait_for(dispose_task, timeout=1)

        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(
            log,
            ["sibling:start", "ok_after", "fail", "ok_before", "sibling:end"],
        )
        states = {effect.identity: effect.state for effect in failing_batch.effects}
        self.assertEqual(
            states,
            {"ok_before": CLEANED, "fail": FAILED, "ok_after": CLEANED},
        )


if __name__ == "__main__":
    unittest.main()
