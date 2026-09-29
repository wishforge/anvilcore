"""Effect-owned reversible registrations into AgentScope 2.0 public API.

Dependency direction:

    semantic_layer
        -> adapters.agentscope (this module)
        -> agentscope (public API only)

``semantic_layer.core`` must never import this module.

Public API used (agentscope 2.0.2):
    - ``Toolkit.tool_groups`` / ``ToolGroup.tools`` (public attributes)
    - ``Agent.reply_stream`` / ``AgentEvent`` (streamed events)

Public API gaps handled here:
    - ``Toolkit`` has no per-tool ``unregister``; ownership is enforced by
      the returned cleanup, which removes exactly the tool this adapter added.
    - There is no core subscribe/unsubscribe event bus; the only core event
      surface is ``Agent.reply_stream``. ``EventHub`` owns listener lifecycle
      and forwards events from a reply stream supplied by the caller.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from agentscope.event import AgentEvent
from agentscope.tool import ToolBase, Toolkit

Cleanup = Callable[[], Any]


def register_tool(
    toolkit: Toolkit,
    tool: ToolBase,
    group: str = "basic",
) -> Cleanup:
    """Register one tool and return an idempotent unregister."""
    target = next(
        (g for g in toolkit.tool_groups if g.name == group),
        None,
    )
    if target is None:
        raise ValueError(f"tool group {group!r} not found")
    if any(t.name == tool.name for t in target.tools):
        raise ValueError(
            f"tool {tool.name!r} already registered in group {group!r}",
        )
    target.tools.append(tool)

    def unregister() -> None:
        if tool in target.tools:
            target.tools.remove(tool)

    return unregister


class EventHub:
    """Listener registry plus per-stream event forwarding."""

    def __init__(self) -> None:
        self.listeners: list[Callable[[AgentEvent], None]] = []

    def subscribe(self, handler: Callable[[AgentEvent], None]) -> Cleanup:
        self.listeners.append(handler)

        def unsubscribe() -> None:
            if handler in self.listeners:
                self.listeners.remove(handler)

        return unsubscribe

    async def forward(self, events: AsyncIterable[AgentEvent]) -> None:
        async for event in events:
            for handler in tuple(self.listeners):
                handler(event)


@dataclass
class Worker:
    """One owned asyncio task; ``stop`` cancels and awaits it."""

    task: asyncio.Task[Any]

    async def stop(self) -> None:
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


async def spawn(coro: Awaitable[Any]) -> Worker:
    """Start one worker task owned by the caller's scope."""
    return Worker(task=asyncio.create_task(coro))


def register_service(
    services: dict[str, Any],
    name: str,
    obj: Any,
) -> Cleanup:
    """Register into a plain service dict; return an idempotent unregister."""
    if name in services:
        raise ValueError(f"service {name!r} already registered")
    services[name] = obj

    def unregister() -> None:
        services.pop(name, None)

    return unregister
