"""PluginManager: descriptor registry + dependency-aware lifecycle orchestration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .capability import (
    ACTIVE,
    DISPOSED,
    DISPOSING,
    INSTALLING,
    REGISTERED,
    Capability,
    CapabilityDescriptor,
)
from .errors import InstallBlockedError, UnloadBlockedError
from .semantic_layer import DependencyLifecycle, PluginScope


@dataclass(slots=True)
class CapabilityRecord:
    """Registry metadata; real lifecycle lives in the manager + Capability."""

    descriptor: CapabilityDescriptor
    installation_generation: int = 0
    instance: Capability | None = None

    @property
    def scope(self) -> PluginScope | None:
        return self.instance.scope if self.instance else None

    @property
    def state(self) -> str:
        return self.instance.state if self.instance else REGISTERED

    @property
    def dependencies(self) -> tuple[Capability, ...]:
        return self.instance.dependencies if self.instance else ()

    @property
    def dependents(self) -> set[str]:
        return self.instance.dependents if self.instance else set()


class PluginManager:
    def __init__(self) -> None:
        self.records: dict[str, CapabilityRecord] = {}
        self.deps = DependencyLifecycle()
        self._install_tasks: dict[str, asyncio.Task[Capability]] = {}
        self._unload_tasks: dict[str, asyncio.Task[list[BaseException]]] = {}

    def register(self, descriptor: CapabilityDescriptor) -> CapabilityRecord:
        if descriptor.id in self.records:
            raise ValueError(f"capability {descriptor.id!r} already registered")
        record = CapabilityRecord(descriptor)
        self.records[descriptor.id] = record
        return record

    def get(self, capability_id: str) -> CapabilityRecord:
        return self.records[capability_id]

    def list(self) -> list[CapabilityRecord]:
        return list(self.records.values())

    async def install(self, capability_id: str) -> Capability:
        record = self.get(capability_id)
        task = self._install_tasks.get(capability_id)
        if task is not None and not task.done():
            return await task
        inst = record.instance
        if inst is not None:
            if inst.state == ACTIVE:
                raise InstallBlockedError(f"capability {capability_id!r} is already active")
            if inst.state in (INSTALLING, DISPOSING):
                raise InstallBlockedError(f"capability {capability_id!r} is {inst.state}")
        task = asyncio.create_task(self._do_install(record))
        self._install_tasks[capability_id] = task
        try:
            return await task
        finally:
            self._install_tasks.pop(capability_id, None)

    async def unload(self, capability_id: str) -> list[BaseException]:
        record = self.get(capability_id)
        cap = record.instance
        if cap is None or cap.state == DISPOSED:
            return []
        if cap.state == INSTALLING:
            raise UnloadBlockedError(f"capability {capability_id!r} is installing")
        if cap.state == ACTIVE and cap.dependents:
            raise UnloadBlockedError(
                f"cannot unload {capability_id!r}: active dependents "
                f"{sorted(cap.dependents)}",
            )
        task = self._unload_tasks.get(capability_id)
        if task is not None and not task.done():
            return await task
        task = asyncio.create_task(self._do_unload(record))
        self._unload_tasks[capability_id] = task
        try:
            return await task
        finally:
            self._unload_tasks.pop(capability_id, None)

    async def reinstall(self, capability_id: str) -> Capability:
        await self.unload(capability_id)
        return await self.install(capability_id)

    async def _do_unload(self, record: CapabilityRecord) -> list[BaseException]:
        return await self._unload_one(record.instance)

    async def _unload_one(self, cap: Capability) -> list[BaseException]:
        if cap.state in (DISPOSED, DISPOSING):
            return []
        errors = await cap.dispose()
        for dep in cap.dependencies:
            if dep.state == ACTIVE and not dep.dependents:
                errors.extend(await self._unload_one(dep))
        return errors

    async def _do_install(self, record: CapabilityRecord) -> Capability:
        created: list[Capability] = []
        try:
            for rec in self._install_chain(record):
                inst = rec.instance
                if inst is not None and inst.state == ACTIVE:
                    continue
                if inst is not None and inst.state == INSTALLING:
                    task = self._install_tasks.get(rec.descriptor.id)
                    if task is not None and not task.done():
                        await task
                    continue
                rec.installation_generation += 1
                cap = Capability(
                    descriptor=rec.descriptor,
                    generation=rec.installation_generation,
                    deps=self.deps,
                    dependencies=tuple(
                        self.records[dep_id].instance
                        for dep_id in rec.descriptor.dependencies
                        if dep_id in self.records
                    ),
                    config=rec.descriptor.config,
                )
                rec.instance = cap
                created.append(cap)
                await cap.install()
            return record.instance
        except BaseException:
            for cap in reversed(created):
                if cap.state in (DISPOSED, DISPOSING) or cap.dependents:
                    continue
                await self._unload_one(cap)
            raise

    def _install_chain(self, record: CapabilityRecord) -> list[CapabilityRecord]:
        """Dependencies-first install order: [C, B, A] for A -> B -> C."""
        chain: list[CapabilityRecord] = []
        seen: set[str] = set()

        def visit(rec: CapabilityRecord) -> None:
            if rec.descriptor.id in seen:
                return
            seen.add(rec.descriptor.id)
            if rec.instance is not None and rec.instance.state == ACTIVE:
                return
            for dep_id in rec.descriptor.dependencies:
                if dep_id in self.records:
                    visit(self.get(dep_id))
                # else: a service-name dependency -- resolved by the
                # registry (PENDING wait / epoch reload), not the chain
            chain.append(rec)

        visit(record)
        return chain
