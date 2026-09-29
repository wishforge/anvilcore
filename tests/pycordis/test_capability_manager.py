"""Phase 2-D: PluginManager / CapabilityManager contract validation."""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from agentscope.agent import Agent
from agentscope.credential import CredentialBase
from agentscope.message import TextBlock, ToolCallBlock, UserMsg
from agentscope.model import ChatModelBase, ChatResponse
from agentscope.permission import (
    PermissionBehavior,
    PermissionContext,
    PermissionRule,
)
from agentscope.state import AgentState
from agentscope.tool import FunctionTool, Toolkit

from anvilcore.adapters.agentscope import (
    EventHub,
    register_service,
    register_tool,
    spawn,
)
from anvilcore.capability import (
    REGISTERED,
    CapabilityDescriptor,
)
from anvilcore.manager import PluginManager
from anvilcore.semantic_layer import ACTIVE, CLEANED, DISPOSED


class LogRuntime:
    """One-effect runtime that records install/cleanup order."""

    def __init__(self, scope, name: str, log: list, boom: bool = False) -> None:
        self.scope = scope
        self.name = name
        self.log = log
        self.boom = boom

    async def install(self) -> None:
        self.log.append(f"install:{self.name}")

        def setup(collect):
            collect(
                f"effect:{self.name}",
                lambda: self.log.append(f"cleanup:{self.name}"),
            )
            if self.boom:
                raise RuntimeError(f"install failed: {self.name}")

        await self.scope.effect(f"{self.name}.install", setup)


def make_log_descriptor(cid, log, deps=(), boom=False):
    def factory(scope):
        return LogRuntime(scope, cid, log, boom)

    return CapabilityDescriptor(
        id=cid,
        version="1.0",
        factory=factory,
        dependencies=deps,
    )


class EchoRuntime:
    """AgentScope-visible runtime: tool + event hook + worker + service."""

    def __init__(self, scope, toolkit: Toolkit, hub: EventHub, services: dict) -> None:
        self.scope = scope
        self.toolkit = toolkit
        self.hub = hub
        self.services = services
        self.calls = 0
        self.events: list = []
        self.worker = None
        self.worker_stopped = False
        self.worker_started = asyncio.Event()

    async def install(self) -> None:
        async def setup(collect):
            def echo(text: str) -> str:
                self.calls += 1
                return f"echo:{text}"

            tool = FunctionTool(echo, name="echo")
            collect("tool:echo", register_tool(self.toolkit, tool))
            collect("event:echo", self.hub.subscribe(self.events.append))
            self.worker = await spawn(self._worker())
            await self.worker_started.wait()
            collect("task:echo_worker", self.worker.stop)
            collect("service:echo", register_service(self.services, "echo", self))

        await self.scope.effect("echo.install", setup)

    async def _worker(self) -> None:
        try:
            self.worker_started.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.worker_stopped = True
            raise


def make_echo_descriptor(toolkit: Toolkit, hub: EventHub, services: dict):
    return CapabilityDescriptor(
        id="echo",
        version="1.0",
        factory=lambda scope: EchoRuntime(scope, toolkit, hub, services),
    )


class DeterministicModel(ChatModelBase):
    """Test-only deterministic model (AgentScope 2.0.2 has no public mock)."""

    def __init__(self) -> None:
        super().__init__(
            credential=CredentialBase(),
            model="deterministic-echo",
            parameters=ChatModelBase.Parameters(),
            stream=False,
            max_retries=0,
            retry_delay=0.0,
            context_size=1024,
        )

    async def _call_api(
        self,
        model_name: str,
        messages,
        tools=None,
        tool_choice=None,
        **kwargs,
    ) -> ChatResponse:
        if messages and messages[-1].has_content_blocks("tool_result"):
            return ChatResponse(content=[TextBlock(text="done")], is_last=True)
        name = tools[0]["function"]["name"] if tools else "echo"
        return ChatResponse(
            content=[
                ToolCallBlock(
                    id=uuid.uuid4().hex,
                    name=name,
                    input=json.dumps({"text": "hi"}),
                ),
            ],
            is_last=True,
        )


def make_agent(toolkit: Toolkit) -> Agent:
    state = AgentState(
        permission_context=PermissionContext(
            allow_rules={
                "echo": [
                    PermissionRule(
                        tool_name="echo",
                        rule_content=None,
                        behavior=PermissionBehavior.ALLOW,
                        source="capability-manager-test",
                    ),
                ],
            },
        ),
    )
    return Agent(
        name="echo-agent",
        system_prompt="You can use tools.",
        model=DeterministicModel(),
        toolkit=toolkit,
        state=state,
    )


class CapabilityManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_manager_register(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        record = manager.register(make_log_descriptor("A", log))

        self.assertIs(manager.get("A"), record)
        self.assertEqual(manager.list(), [record])
        self.assertEqual(record.state, REGISTERED)
        self.assertIsNone(record.instance)
        self.assertIsNone(record.scope)
        self.assertEqual(record.installation_generation, 0)
        with self.assertRaises(ValueError):
            manager.register(make_log_descriptor("A", log))
        with self.assertRaises(KeyError):
            manager.get("missing")

    async def test_manager_install(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        record = manager.register(make_log_descriptor("A", log))

        cap = await manager.install("A")

        self.assertIs(record.instance, cap)
        self.assertEqual(record.state, ACTIVE)
        self.assertEqual(record.installation_generation, 1)
        self.assertIs(record.scope, cap.scope)
        self.assertEqual(cap.state, ACTIVE)
        self.assertEqual(cap.scope.state, ACTIVE)
        self.assertEqual(log, ["install:A"])
        self.assertEqual(len(cap.scope.effects.batches[0].effects), 1)
        with self.assertRaises(RuntimeError):
            await manager.install("A")

    async def test_dependency_install_order(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        manager.register(make_log_descriptor("C", log))
        manager.register(make_log_descriptor("B", log, deps=("C",)))
        record_a = manager.register(make_log_descriptor("A", log, deps=("B",)))

        await manager.install("A")

        self.assertEqual(log, ["install:C", "install:B", "install:A"])
        cap_b = manager.get("B").instance
        cap_c = manager.get("C").instance
        self.assertEqual(cap_c.dependents, {"B"})
        self.assertEqual(cap_b.dependents, {"A"})
        self.assertIn(cap_b, record_a.instance.dependencies)
        self.assertIn(cap_c, cap_b.dependencies)

    async def test_dependency_unload_order(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        manager.register(make_log_descriptor("C", log))
        manager.register(make_log_descriptor("B", log, deps=("C",)))
        manager.register(make_log_descriptor("A", log, deps=("B",)))
        await manager.install("A")

        errors = await manager.unload("A")

        self.assertEqual(errors, [])
        self.assertEqual(
            log,
            [
                "install:C",
                "install:B",
                "install:A",
                "cleanup:A",
                "cleanup:B",
                "cleanup:C",
            ],
        )

    async def test_unload_with_active_dependents(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        manager.register(make_log_descriptor("B", log))
        manager.register(make_log_descriptor("A", log, deps=("B",)))
        manager.register(make_log_descriptor("X", log, deps=("B",)))
        await manager.install("A")
        await manager.install("X")

        with self.assertRaises(RuntimeError) as cm:
            await manager.unload("B")
        self.assertIn("A", str(cm.exception))
        self.assertIn("X", str(cm.exception))
        self.assertEqual(manager.get("B").state, ACTIVE)
        self.assertEqual(manager.get("A").state, ACTIVE)
        self.assertEqual(manager.get("X").state, ACTIVE)

        await manager.unload("A")
        await manager.unload("X")
        self.assertEqual(await manager.unload("B"), [])
        self.assertEqual(manager.get("B").state, DISPOSED)

    async def test_failed_dependency_rollback(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        manager.register(make_log_descriptor("C", log))
        manager.register(make_log_descriptor("B", log, deps=("C",)))
        manager.register(make_log_descriptor("A", log, deps=("B",), boom=True))

        with self.assertRaises(RuntimeError) as cm:
            await manager.install("A")

        self.assertIn("A", str(cm.exception))
        self.assertEqual(
            log,
            [
                "install:C",
                "install:B",
                "install:A",
                "cleanup:A",
                "cleanup:B",
                "cleanup:C",
            ],
        )
        for cid in ("A", "B", "C"):
            record = manager.get(cid)
            self.assertEqual(record.state, DISPOSED)
            self.assertEqual(record.instance.state, DISPOSED)
            self.assertEqual(record.instance.scope.state, DISPOSED)
            for batch in record.instance.scope.effects.batches:
                self.assertEqual(batch.state, DISPOSED)
                self.assertTrue(all(e.state == CLEANED for e in batch.effects))
        self.assertEqual(manager.deps._dependents, {})

    async def test_reinstall_new_generation(self) -> None:
        manager = PluginManager()
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        manager.register(make_echo_descriptor(toolkit, hub, services))
        record = manager.get("echo")

        cap1 = await manager.install("echo")
        batch1 = cap1.scope.effects.batches[0]
        worker1 = cap1.instance.worker
        self.assertEqual(record.installation_generation, 1)

        await manager.unload("echo")
        self.assertEqual(toolkit.tool_groups[0].tools, [])
        self.assertEqual(hub.listeners, [])
        self.assertNotIn("echo", services)
        self.assertTrue(worker1.task.done())

        cap2 = await manager.install("echo")

        self.assertEqual(record.installation_generation, 2)
        self.assertIsNot(cap2, cap1)
        self.assertIsNot(cap2.scope, cap1.scope)
        self.assertEqual(cap2.state, ACTIVE)
        self.assertEqual(len(toolkit.tool_groups[0].tools), 1)
        self.assertEqual(len(hub.listeners), 1)
        self.assertIn("echo", services)
        worker2 = cap2.instance.worker
        self.assertIsNot(worker2, worker1)
        self.assertFalse(worker2.task.done())
        batch2 = cap2.scope.effects.batches[0]
        self.assertEqual([e.order for e in batch2.effects], [1, 2, 3, 4])
        self.assertTrue(
            {id(e) for e in batch1.effects}.isdisjoint(id(e) for e in batch2.effects),
        )

        await manager.unload("echo")

    async def test_concurrent_install_single_instance(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        record = manager.register(make_log_descriptor("A", log))

        cap1, cap2 = await asyncio.gather(
            manager.install("A"),
            manager.install("A"),
        )

        self.assertIs(cap1, cap2)
        self.assertIs(record.instance, cap1)
        self.assertEqual(log, ["install:A"])
        self.assertEqual(record.state, ACTIVE)
        await manager.unload("A")

    async def test_concurrent_unload_single_dispose(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        manager.register(make_log_descriptor("A", log))
        cap = await manager.install("A")

        errors = await asyncio.gather(
            manager.unload("A"),
            manager.unload("A"),
        )

        self.assertEqual(errors, [[], []])
        self.assertEqual(cap.physical_disposes, 1)
        self.assertEqual(log, ["install:A", "cleanup:A"])
        self.assertEqual(cap.state, DISPOSED)

    async def test_no_ghost_capability(self) -> None:
        manager = PluginManager()
        log: list[str] = []
        for cid, deps in (("C", ()), ("B", ("C",)), ("A", ("B",))):
            manager.register(make_log_descriptor(cid, log, deps=deps))
        await manager.install("A")

        await manager.unload("A")

        for record in manager.list():
            self.assertEqual(record.state, DISPOSED)
            self.assertEqual(record.instance.state, DISPOSED)
            self.assertEqual(record.instance.scope.state, DISPOSED)
            self.assertFalse(record.dependents)
        self.assertEqual(manager.deps._dependents, {})

    async def test_no_ghost_effects(self) -> None:
        manager = PluginManager()
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        manager.register(make_echo_descriptor(toolkit, hub, services))
        cap = await manager.install("echo")
        batch = cap.scope.effects.batches[0]

        await manager.unload("echo")

        self.assertEqual(toolkit.tool_groups[0].tools, [])
        self.assertEqual(hub.listeners, [])
        self.assertNotIn("echo", services)
        self.assertTrue(cap.instance.worker.task.done())
        self.assertTrue(cap.instance.worker_stopped)
        self.assertTrue(all(e.state == CLEANED for e in batch.effects))
        self.assertEqual(cap.scope.state, DISPOSED)
        self.assertEqual(manager.deps._dependents, {})

    async def test_agent_visibility_after_reinstall(self) -> None:
        manager = PluginManager()
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        manager.register(make_echo_descriptor(toolkit, hub, services))
        await manager.install("echo")
        agent = make_agent(toolkit)

        reply = await agent.reply(UserMsg("user", "call echo"))
        self.assertEqual(reply.get_text_content(), "done")
        self.assertEqual(manager.get("echo").instance.instance.calls, 1)

        await manager.unload("echo")
        self.assertIsNone(await toolkit.get_tool("echo"))

        await manager.install("echo")
        self.assertIsNotNone(await toolkit.get_tool("echo"))
        reply2 = await agent.reply(UserMsg("user", "call echo again"))
        self.assertEqual(reply2.get_text_content(), "done")
        self.assertEqual(manager.get("echo").instance.instance.calls, 1)

        await manager.unload("echo")


if __name__ == "__main__":
    unittest.main()
