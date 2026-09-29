"""Phase 2-B: Semantic Layer -> AgentScope 2.0 bridge, no-ghost ownership.

Requires the installed AgentScope 2.0.2 (base conda env) and the Phase 2-A
semantic kernel. All AgentScope access is public API only.
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
from agentscope.event import (
    ReplyStartEvent,
    ToolResultEndEvent,
)
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
from pycordis.semantic_layer import DISPOSED, PluginScope


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
                    input=json.dumps({"path": "notes.txt"}),
                ),
            ],
            is_last=True,
        )


class FilesystemCapability:
    """Example capability: PluginScope + runtime resources, not a semantic
    layer primitive. dispose() releases every effect via the scope."""

    def __init__(
        self,
        scope: PluginScope,
        hub: EventHub,
        toolkit: Toolkit,
        services: dict,
    ) -> None:
        self.scope = scope
        self.hub = hub
        self.toolkit = toolkit
        self.services = services
        self.calls = 0
        self.events: list = []
        self.worker_stopped = False
        self.worker = None

    async def install(self) -> None:
        async def setup(collect):
            def fs_read(path: str) -> str:
                self.calls += 1
                return f"read:{path}"

            tool = FunctionTool(fs_read, name="fs_read")
            collect("tool:fs_read", register_tool(self.toolkit, tool))
            collect("event:fs", self.hub.subscribe(self.events.append))
            self.worker = await spawn(self._worker())
            collect("task:fs_worker", self.worker.stop)
            collect(
                "service:fs",
                register_service(self.services, "fs", object()),
            )

        await self.scope.effect("filesystem.install", setup)

    async def _worker(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.worker_stopped = True
            raise

    async def dispose(self) -> list[BaseException]:
        return await self.scope.dispose()


def make_agent(toolkit: Toolkit, tool_name: str = "fs_read") -> Agent:
    state = AgentState(
        permission_context=PermissionContext(
            allow_rules={
                tool_name: [
                    PermissionRule(
                        tool_name=tool_name,
                        rule_content=None,
                        behavior=PermissionBehavior.ALLOW,
                        source="bridge-test",
                    ),
                ],
            },
        ),
    )
    return Agent(
        name="fs-agent",
        system_prompt="You can use tools.",
        model=DeterministicModel(),
        toolkit=toolkit,
        state=state,
    )


class AgentScopeBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.scope = PluginScope("fs")
        self.hub = EventHub()
        self.toolkit = Toolkit()
        self.services: dict = {}
        self.cap = FilesystemCapability(
            self.scope,
            self.hub,
            self.toolkit,
            self.services,
        )
        await self.cap.install()

    async def asyncTearDown(self) -> None:
        await self.cap.dispose()

    async def test_tool_cannot_outlive_scope(self) -> None:
        self.assertIsNotNone(await self.toolkit.get_tool("fs_read"))
        names = [s["function"]["name"] for s in await self.toolkit.get_tool_schemas()]
        self.assertIn("fs_read", names)

        await self.cap.dispose()

        self.assertIsNone(await self.toolkit.get_tool("fs_read"))
        names = [s["function"]["name"] for s in await self.toolkit.get_tool_schemas()]
        self.assertNotIn("fs_read", names)
        self.assertEqual(
            [t.name for t in self.toolkit.tool_groups[0].tools],
            [],
        )

    async def test_event_cannot_outlive_scope(self) -> None:
        agent = make_agent(self.toolkit)
        await self.hub.forward(agent.reply_stream(UserMsg("user", "read notes.txt")))
        self.assertGreater(len(self.cap.events), 0)
        self.assertTrue(any(isinstance(e, ReplyStartEvent) for e in self.cap.events))
        self.assertTrue(any(isinstance(e, ToolResultEndEvent) for e in self.cap.events))

        await self.cap.dispose()
        self.assertEqual(self.hub.listeners, [])
        count = len(self.cap.events)
        await self.hub.forward(agent.reply_stream(UserMsg("user", "read again")))
        self.assertEqual(len(self.cap.events), count)

    async def test_worker_cannot_outlive_scope(self) -> None:
        self.assertFalse(self.cap.worker.task.done())
        await self.cap.dispose()
        self.assertTrue(self.cap.worker.task.done())
        self.assertTrue(self.cap.worker_stopped)

    async def test_service_cannot_outlive_scope(self) -> None:
        self.assertIn("fs", self.services)
        await self.cap.dispose()
        self.assertNotIn("fs", self.services)

    async def test_capability_dispose_is_complete(self) -> None:
        await self.cap.dispose()
        self.assertIsNone(await self.toolkit.get_tool("fs_read"))
        self.assertEqual(self.hub.listeners, [])
        self.assertTrue(self.cap.worker.task.done())
        self.assertNotIn("fs", self.services)
        self.assertEqual(self.scope.state, DISPOSED)

    async def test_double_install_and_double_dispose(self) -> None:
        with self.assertRaises(ValueError):
            await self.cap.install()
        self.assertEqual(len(self.toolkit.tool_groups[0].tools), 1)
        self.assertEqual(len(self.hub.listeners), 1)
        self.assertIn("fs", self.services)

        first = await self.cap.dispose()
        second = await self.cap.dispose()
        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertEqual(self.toolkit.tool_groups[0].tools, [])
        self.assertEqual(self.hub.listeners, [])
        self.assertNotIn("fs", self.services)

    async def test_agent_invokes_tool_then_dispose_removes_it(self) -> None:
        agent = make_agent(self.toolkit)
        reply = await agent.reply(UserMsg("user", "read notes.txt"))
        self.assertEqual(reply.get_text_content(), "done")
        self.assertEqual(self.cap.calls, 1)

        await self.cap.dispose()
        before = self.cap.calls
        await agent.reply(UserMsg("user", "read another file"))
        self.assertEqual(self.cap.calls, before)
        self.assertIsNone(await self.toolkit.get_tool("fs_read"))


if __name__ == "__main__":
    unittest.main()
