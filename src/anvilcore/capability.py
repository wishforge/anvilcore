"""Runtime-managed Capability: one owned PluginScope + strict state machine.

Promoted from the Phase 2-C test scaffold. The manager owns records and
generations; this class owns the per-generation runtime instance, scope,
dependency edges, and dispose coalescing.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .errors import InvalidTransitionError
from .semantic_layer import (
    ACTIVE,
    DISPOSED,
    DISPOSING,
    FAILED,
    DependencyLifecycle,
    PluginScope,
)

REGISTERED = "REGISTERED"
INSTALLING = "INSTALLING"
RETIRING = "RETIRING"


@dataclass(frozen=True, slots=True)
class CapabilityDescriptor:
    id: str
    version: str
    factory: Callable[[PluginScope], Any]
    dependencies: tuple[str, ...] = ()
    # hot-update support: config is visible to the capability as
    # `scope.config`; schema validates configs on plugin()/update()
    config: dict = field(default_factory=dict)
    schema: Callable[[dict], dict] | None = None


class _OwnedScope(PluginScope):
    """PluginScope that mirrors disposal back into its capability.

    DependencyLifecycle disposes dependent scopes directly; without the
    mirror the dependent capability would stay ACTIVE while its scope is
    DISPOSED.
    """

    def __init__(self, owner: Capability, name: str) -> None:
        super().__init__(name=name)
        self._owner = owner

    # graceful-rotation surface for business code (same layer as scope.config)
    def acquire(self) -> bool:
        return self._owner._acquire()

    def release(self) -> None:
        self._owner._release()

    async def dispose(self) -> list[BaseException]:
        self._owner._begin_dispose()
        errors = await super().dispose()
        self._owner._finish_dispose()
        return errors


class Capability:
    """One installation generation of a capability."""

    def __init__(
        self,
        descriptor: CapabilityDescriptor,
        generation: int,
        deps: DependencyLifecycle,
        dependencies: tuple[Capability, ...],
        config: dict | None = None,
    ) -> None:
        self.descriptor = descriptor
        self.installation_generation = generation
        self.deps = deps
        self.dependencies = dependencies
        self.dependents: set[str] = set()
        self.scope = _OwnedScope(self, f"{descriptor.id}#{generation}")
        self.scope.config = dict(config or {})
        self.instance: Any = None
        self.state = INSTALLING
        self._dispose_task: asyncio.Task[list[BaseException]] | None = None
        self.physical_disposes = 0
        # graceful rotation: in-flight bookkeeping (see acquire/retire)
        self._inflight = 0
        self._drained: asyncio.Event = asyncio.Event()
        self._drained.set()

    async def install(self) -> None:
        for dep in self.dependencies:
            self.deps.register_dependent(dep.descriptor.id, self.scope)
            dep.dependents.add(self.descriptor.id)
        try:
            self.instance = self.descriptor.factory(self.scope)
            result = self.instance.install()
            if inspect.isawaitable(result):
                await result
        except BaseException:
            self.state = FAILED
            raise
        self._transition(INSTALLING, ACTIVE)
        self.deps.activate(self.descriptor.id)

    def retire(self) -> None:
        """Stop accepting new work; in-flight calls keep running.

        The capability leaves ACTIVE (so a replacement generation can be
        installed for the same id) but its scope stays alive until the
        in-flight count drains to zero and dispose() runs."""
        if self.state == ACTIVE:
            self._transition(ACTIVE, RETIRING)

    def _acquire(self) -> bool:
        """Claim one in-flight slot. False => this generation is retired;
        the caller should route the work to the current generation."""
        if self.state != ACTIVE:
            return False
        self._inflight += 1
        self._drained.clear()
        return True

    def _release(self) -> None:
        self._inflight = max(0, self._inflight - 1)
        if self._inflight == 0:
            self._drained.set()

    async def _wait_drained(self) -> None:
        await self._drained.wait()

    async def dispose(self) -> list[BaseException]:
        if self.state == DISPOSED:
            return []
        if self._dispose_task is None:
            self._begin_dispose()
            self._dispose_task = asyncio.create_task(self._dispose())
        return await self._dispose_task

    def _begin_dispose(self) -> None:
        if self.state in (ACTIVE, RETIRING, INSTALLING, FAILED):
            self.state = DISPOSING

    def _finish_dispose(self) -> None:
        if self.state == DISPOSING:
            self.state = DISPOSED

    async def _dispose(self) -> list[BaseException]:
        # graceful shutdown: hold the scope until in-flight calls finish
        if self._inflight > 0:
            await self._drained.wait()
        self.physical_disposes += 1
        for dep in self.dependencies:
            dep.dependents.discard(self.descriptor.id)
            self.deps.unregister_dependent(dep.descriptor.id, self.scope)
        return await self.deps.release(self.descriptor.id, self.scope.dispose)

    def _transition(self, expected: str, new_state: str) -> None:
        if self.state != expected:
            raise InvalidTransitionError(
                f"capability {self.descriptor.id!r}: illegal transition "
                f"{self.state} -> {new_state}",
            )
        self.state = new_state
