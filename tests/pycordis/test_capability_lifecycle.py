"""Phase 2-C: Capability lifecycle contract validation.

The Capability wrapper is intentionally test-local: it validates that
PluginScope + EffectRegistry + DependencyLifecycle already form a usable
Agent Capability Runtime contract. No production/business code is modified.
"""

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
from agentscope.event import ReplyStartEvent, ToolResultEndEvent
from agentscope.message import TextBlock, ToolCallBlock, UserMsg
from agentscope.model import ChatModelBase, ChatResponse
from agentscope.permission import (
    PermissionBehavior,
    PermissionContext,
    PermissionRule,
)
from agentscope.state import AgentState
from agentscope.tool import FunctionTool, Toolkit

from pycordis.adapters.agentscope import (
    EventHub,
    register_service,
    register_tool,
    spawn,
)
from pycordis.semantic_layer import (
    ACTIVE,
    CLEANED,
    DISPOSED,
    DISPOSING,
    FAILED,
    DependencyLifecycle,
    PluginScope,
)


class _OwnedScope(PluginScope):
    """PluginScope that mirrors full disposal back into its capability.

    The capability state machine stays honest when DependencyLifecycle
    disposes a dependent scope directly (provider -> dependent teardown),
    not only when the capability calls dispose() on itself.
    """

    def __init__(self, owner: Capability, name: str) -> None:
        super().__init__(name=name)
        self._owner = owner

    async def dispose(self) -> list[BaseException]:
        self._owner._scope_disposing()
        errors = await super().dispose()
        self._owner._scope_disposed()
        return errors


class Capability:
    """Minimal validated capability contract.

    install()/dispose() are the only lifecycle entry points. Cleanup is never
    performed by the capability itself; everything is registered through
    scope.effect() and reclaimed by PluginScope/DependencyLifecycle.
    """

    CREATED = "CREATED"
    INSTALLING = "INSTALLING"

    def __init__(
        self,
        identity: str,
        deps: DependencyLifecycle | None = None,
        dependencies: tuple[Capability, ...] = (),
    ) -> None:
        self.identity = identity
        self.deps = deps
        self.dependencies = dependencies
        self.scope = _OwnedScope(self, identity)
        self.state = self.CREATED
        self._dispose_task: asyncio.Task | None = None
        self.physical_disposes = 0

    async def install(self) -> None:
        self._transition(self.CREATED, self.INSTALLING)
        if self.deps is not None:
            for dependency in self.dependencies:
                self.deps.register_dependent(
                    dependency.identity,
                    self.scope,
                )
        try:
            await self._install()
        except BaseException:
            self.state = FAILED
            raise
        self._transition(self.INSTALLING, ACTIVE)

    async def dispose(self) -> list[BaseException]:
        if self.state == DISPOSED:
            return []
        if self._dispose_task is None:
            self._transition(ACTIVE, DISPOSING)
            self._dispose_task = asyncio.create_task(self._dispose())
        return await self._dispose_task

    async def _dispose(self) -> list[BaseException]:
        self.physical_disposes += 1
        if self.deps is None:
            return await self.scope.dispose()
        return await self.deps.release(self.identity, self.scope.dispose)

    def _scope_disposing(self) -> None:
        if self.state == ACTIVE:
            self.state = DISPOSING

    def _scope_disposed(self) -> None:
        self.state = DISPOSED

    def _transition(self, expected: str, new_state: str) -> None:
        if self.state != expected:
            raise RuntimeError(
                f"capability {self.identity!r}: illegal transition "
                f"{self.state} -> {new_state}",
            )
        self.state = new_state

    async def _install(self) -> None:  # pragma: no cover - abstract
        raise NotImplementedError


class EchoCapability(Capability):
    """Real capability: AgentScope tool + event hook + worker + service."""

    def __init__(
        self,
        toolkit: Toolkit,
        hub: EventHub,
        services: dict,
        **kwargs,
    ) -> None:
        super().__init__("echo", **kwargs)
        self.toolkit = toolkit
        self.hub = hub
        self.services = services
        self.calls = 0
        self.events: list = []
        self.worker = None
        self.worker_stopped = False
        self.worker_started = asyncio.Event()

    async def _install(self) -> None:
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


class FailingCapability(Capability):
    """Install succeeds through tool/event/worker/service, then fails."""

    def __init__(
        self,
        toolkit: Toolkit,
        hub: EventHub,
        services: dict,
    ) -> None:
        super().__init__("failing")
        self.toolkit = toolkit
        self.hub = hub
        self.services = services
        self.events: list = []
        self.worker = None
        self.worker_stopped = False
        self.worker_started = asyncio.Event()
        self.boom = RuntimeError("install step 5 failed")

    async def _install(self) -> None:
        async def setup(collect):
            tool = FunctionTool(lambda text: "failing", name="failing")
            collect("tool:failing", register_tool(self.toolkit, tool))
            collect("event:failing", self.hub.subscribe(self.events.append))
            self.worker = await spawn(self._worker())
            await self.worker_started.wait()
            collect("task:failing_worker", self.worker.stop)
            collect(
                "service:failing",
                register_service(self.services, "failing", self),
            )
            raise self.boom

        await self.scope.effect("failing.install", setup)

    async def _worker(self) -> None:
        try:
            self.worker_started.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.worker_stopped = True
            raise


class SimpleCapability(Capability):
    """One-effect capability used for dependency ordering tests."""

    def __init__(
        self,
        identity: str,
        deps: DependencyLifecycle,
        dependencies: tuple[Capability, ...] = (),
        cleanup=None,
    ) -> None:
        super().__init__(identity, deps=deps, dependencies=dependencies)
        self._cleanup = cleanup

    async def _install(self) -> None:
        def setup(collect):
            collect(f"effect:{self.identity}", self._cleanup)

        await self.scope.effect(f"{self.identity}.install", setup)


class DeterministicModel(ChatModelBase):
    """Test-only deterministic model (same pattern as the 2-B bridge test)."""

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


def make_agent(toolkit: Toolkit, tool_name: str = "echo") -> Agent:
    state = AgentState(
        permission_context=PermissionContext(
            allow_rules={
                tool_name: [
                    PermissionRule(
                        tool_name=tool_name,
                        rule_content=None,
                        behavior=PermissionBehavior.ALLOW,
                        source="capability-test",
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


class CapabilityLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_state_machine_rejects_invalid_transitions(self) -> None:
        cap = EchoCapability(Toolkit(), EventHub(), {})
        self.assertEqual(cap.state, Capability.CREATED)
        with self.assertRaises(RuntimeError):
            await cap.dispose()  # CREATED -> DISPOSING is illegal

        await cap.install()
        self.assertEqual(cap.state, ACTIVE)
        with self.assertRaises(RuntimeError):
            await cap.install()  # ACTIVE -> INSTALLING is illegal

        errors = await cap.dispose()
        self.assertEqual(errors, [])
        self.assertEqual(cap.state, DISPOSED)
        self.assertEqual(await cap.dispose(), [])  # idempotent
        with self.assertRaises(RuntimeError):
            await cap.install()  # DISPOSED -> INSTALLING is illegal

    async def test_echo_install_registers_all_effects_through_effect_registry(
        self,
    ) -> None:
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        cap = EchoCapability(toolkit, hub, services)

        await cap.install()

        self.assertEqual(cap.state, ACTIVE)
        self.assertEqual(cap.scope.state, ACTIVE)
        batch = cap.scope.effects.batches[0]
        self.assertEqual(
            {e.identity for e in batch.effects},
            {"tool:echo", "event:echo", "task:echo_worker", "service:echo"},
        )
        for effect in batch.effects:  # CAP-01 / CAP-03
            self.assertIs(effect.owner, cap.scope)

        self.assertIsNotNone(await toolkit.get_tool("echo"))
        self.assertEqual(len(hub.listeners), 1)
        self.assertFalse(cap.worker.task.done())
        self.assertIn("echo", services)

        agent = make_agent(toolkit)
        await hub.forward(agent.reply_stream(UserMsg("user", "hello")))
        self.assertGreater(len(cap.events), 0)
        self.assertTrue(
            any(isinstance(e, ReplyStartEvent) for e in cap.events),
        )
        self.assertTrue(
            any(isinstance(e, ToolResultEndEvent) for e in cap.events),
        )

    async def test_dispose_reclaims_every_effect_through_scope(self) -> None:
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        cap = EchoCapability(toolkit, hub, services)
        await cap.install()
        batch = cap.scope.effects.batches[0]

        errors = await cap.dispose()

        self.assertEqual(errors, [])
        self.assertEqual(cap.state, DISPOSED)
        self.assertEqual(cap.scope.state, DISPOSED)
        self.assertEqual(toolkit.tool_groups[0].tools, [])
        self.assertEqual(hub.listeners, [])
        self.assertTrue(cap.worker.task.done())
        self.assertTrue(cap.worker_stopped)
        self.assertNotIn("echo", services)
        self.assertTrue(all(e.state == CLEANED for e in batch.effects))

        self.assertEqual(await cap.dispose(), [])
        self.assertEqual(cap.physical_disposes, 1)

    async def test_dependency_dispose_waits_for_dependent_before_finalizer(
        self,
    ) -> None:
        deps = DependencyLifecycle()
        log: list[str] = []
        a_started = asyncio.Event()
        release_a = asyncio.Event()

        async def a_cleanup() -> None:
            log.append("A:cleanup:start")
            a_started.set()
            await release_a.wait()
            log.append("A:cleanup:end")

        b = SimpleCapability(
            "B",
            deps,
            cleanup=lambda: log.append("B:finalizer"),
        )
        a = SimpleCapability(
            "A",
            deps,
            dependencies=(b,),
            cleanup=a_cleanup,
        )
        await b.install()
        await a.install()
        self.assertEqual(a.state, ACTIVE)

        dispose_task = asyncio.create_task(b.dispose())
        await asyncio.wait_for(a_started.wait(), timeout=1)
        self.assertEqual(a.state, DISPOSING)
        self.assertEqual(a.scope.state, DISPOSING)
        self.assertNotIn("B:finalizer", log)

        release_a.set()
        errors = await asyncio.wait_for(dispose_task, timeout=1)

        self.assertEqual(errors, [])
        self.assertEqual(a.state, DISPOSED)
        self.assertEqual(b.state, DISPOSED)
        self.assertEqual(log, ["A:cleanup:start", "A:cleanup:end", "B:finalizer"])

    async def test_failed_install_rolls_back_all_effects_and_preserves_exception(
        self,
    ) -> None:
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        cap = FailingCapability(toolkit, hub, services)

        with self.assertRaises(RuntimeError) as cm:
            await cap.install()

        self.assertIs(cm.exception, cap.boom)
        self.assertEqual(cap.state, FAILED)
        self.assertEqual(toolkit.tool_groups[0].tools, [])
        self.assertEqual(hub.listeners, [])
        self.assertTrue(cap.worker.task.done())
        self.assertTrue(cap.worker_stopped)
        self.assertNotIn("failing", services)
        batch = cap.scope.effects.batches[0]
        self.assertTrue(all(e.state == CLEANED for e in batch.effects))
        with self.assertRaises(RuntimeError):
            await cap.install()

    async def test_reinstall_gets_fresh_scope_and_fresh_effect_identity(
        self,
    ) -> None:
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        cap1 = EchoCapability(toolkit, hub, services)
        await cap1.install()
        batch1 = cap1.scope.effects.batches[0]
        orders1 = [e.order for e in batch1.effects]
        worker1 = cap1.worker

        await cap1.dispose()
        self.assertEqual(toolkit.tool_groups[0].tools, [])
        self.assertEqual(hub.listeners, [])
        self.assertNotIn("echo", services)

        cap2 = EchoCapability(toolkit, hub, services)
        await cap2.install()

        self.assertIsNot(cap2.scope, cap1.scope)
        self.assertEqual(cap2.scope.state, ACTIVE)
        self.assertEqual(len(toolkit.tool_groups[0].tools), 1)
        self.assertEqual(len(hub.listeners), 1)
        self.assertIn("echo", services)
        self.assertIsNot(cap2.worker, worker1)
        self.assertFalse(cap2.worker.task.done())
        self.assertTrue(worker1.task.done())

        batch2 = cap2.scope.effects.batches[0]
        self.assertEqual([e.order for e in batch2.effects], orders1)
        self.assertEqual(len(batch2.effects), len(batch1.effects))
        self.assertTrue(
            {id(e) for e in batch1.effects}.isdisjoint(
                id(e) for e in batch2.effects
            ),
        )
        await cap2.dispose()

    async def test_concurrent_dispose_has_one_physical_teardown(self) -> None:
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        cap = EchoCapability(toolkit, hub, services)
        await cap.install()
        batch = cap.scope.effects.batches[0]

        results = await asyncio.gather(
            cap.dispose(),
            cap.dispose(),
            cap.dispose(),
        )

        self.assertEqual(results, [[], [], []])
        self.assertEqual(cap.physical_disposes, 1)
        self.assertEqual(cap.state, DISPOSED)
        self.assertEqual(cap.scope.state, DISPOSED)
        self.assertIsNotNone(cap._dispose_task)
        self.assertIsNotNone(cap.scope._dispose_task)
        self.assertEqual(toolkit.tool_groups[0].tools, [])
        self.assertEqual(hub.listeners, [])
        self.assertTrue(cap.worker.task.done())
        self.assertTrue(all(e.state == CLEANED for e in batch.effects))

    async def test_disposed_capability_cannot_register_new_effects(self) -> None:
        cap = EchoCapability(Toolkit(), EventHub(), {})
        await cap.install()
        await cap.dispose()

        with self.assertRaises(RuntimeError):
            await cap.scope.effect("late", lambda collect: None)
        with self.assertRaises(RuntimeError):
            await cap.scope.child("late")
        with self.assertRaises(RuntimeError):
            await cap.install()

    async def test_agentscope_visibility_install_dispose_reinstall(self) -> None:
        toolkit = Toolkit()
        hub = EventHub()
        services: dict = {}
        cap1 = EchoCapability(toolkit, hub, services)
        await cap1.install()
        agent = make_agent(toolkit)

        reply = await agent.reply(UserMsg("user", "call echo"))
        self.assertEqual(reply.get_text_content(), "done")
        self.assertEqual(cap1.calls, 1)

        await cap1.dispose()
        self.assertIsNone(await toolkit.get_tool("echo"))
        schemas = await toolkit.get_tool_schemas()
        self.assertNotIn("echo", [s["function"]["name"] for s in schemas])

        cap2 = EchoCapability(toolkit, hub, services)
        await cap2.install()
        self.assertIsNotNone(await toolkit.get_tool("echo"))
        schemas = await toolkit.get_tool_schemas()
        self.assertIn("echo", [s["function"]["name"] for s in schemas])

        reply2 = await agent.reply(UserMsg("user", "call echo again"))
        self.assertEqual(reply2.get_text_content(), "done")
        self.assertEqual(cap2.calls, 1)
        self.assertEqual(cap1.calls, 1)


if __name__ == "__main__":
    unittest.main()
