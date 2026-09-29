"""Phase 2-A formal semantic kernel (bootstrap subset).

PluginScope + EffectRegistry + DependencyLifecycle, stdlib only.

Teardown semantics (Phase 1 Cordis archaeology):
- inside one batch: strict serial LIFO
- across top-level batches: reverse registration-order start, concurrent,
  all awaited; one failing batch never blocks the rest
- dispose collects and returns errors instead of raising; setup failure
  rolls back already-collected effects and re-raises the original exception
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..errors import InvalidTransitionError, ProviderReleasedError

ACTIVE = "ACTIVE"
DISPOSING = "DISPOSING"
DISPOSED = "DISPOSED"
CLEANED = "CLEANED"
FAILED = "FAILED"

Cleanup = Callable[[], Any]
Collect = Callable[[str, Cleanup], "Effect"]


@dataclass(slots=True)
class Effect:
    owner: PluginScope
    identity: str
    cleanup: Cleanup
    order: int
    state: str = ACTIVE


@dataclass(slots=True)
class EffectBatch:
    owner: PluginScope
    label: str
    order: int
    effects: list[Effect] = field(default_factory=list)
    state: str = ACTIVE
    errors: list[BaseException] = field(default_factory=list)

    async def dispose(self) -> list[BaseException]:
        if self.state != ACTIVE:
            return list(self.errors)
        self.state = DISPOSING
        for effect in reversed(self.effects):
            try:
                result = effect.cleanup()
                if inspect.isawaitable(result):
                    await result
                effect.state = CLEANED
            except BaseException as exc:  # noqa: BLE001 -- teardown must collect all
                effect.state = FAILED
                self.errors.append(exc)
        self.state = DISPOSED
        return list(self.errors)


class EffectRegistry:
    def __init__(self) -> None:
        self.batches: list[EffectBatch] = []
        self._batch_order = 0
        self._effect_order = 0

    def create_batch(self, owner: PluginScope, label: str) -> EffectBatch:
        self._batch_order += 1
        batch = EffectBatch(owner=owner, label=label, order=self._batch_order)
        self.batches.append(batch)
        return batch

    def register(self, batch: EffectBatch, identity: str, cleanup: Cleanup) -> Effect:
        if batch.state != ACTIVE:
            raise InvalidTransitionError(
                f"cannot register {identity!r} in {batch.label!r}: batch is {batch.state}"
            )
        self._effect_order += 1
        effect = Effect(
            owner=batch.owner,
            identity=identity,
            cleanup=cleanup,
            order=self._effect_order,
        )
        batch.effects.append(effect)
        return effect

    async def rollback(self, batch: EffectBatch) -> list[BaseException]:
        return await batch.dispose()

    async def dispose_all(self) -> list[BaseException]:
        errors: list[BaseException] = []
        if not self.batches:
            return errors
        results = await asyncio.gather(
            *(batch.dispose() for batch in reversed(self.batches)),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                errors.append(result)
            else:
                errors.extend(result)
        return errors


class PluginScope:
    def __init__(self, name: str = "root", parent: PluginScope | None = None) -> None:
        self.name = name
        self.parent = parent
        self.children: list[PluginScope] = []
        self.effects = EffectRegistry()
        self.state = ACTIVE
        self.config: dict = {}
        self._dispose_task: asyncio.Task[list[BaseException]] | None = None
        self._own_registry: dict[str, tuple[EffectBatch, Effect]] = {}
        if parent is not None:
            parent._add_child(self)

    def _ensure_active(self) -> None:
        if self.state != ACTIVE:
            raise InvalidTransitionError(f"scope {self.name!r} is {self.state}")

    def _add_child(self, child: PluginScope) -> None:
        self._ensure_active()
        self.children.append(child)

    def child(self, name: str) -> PluginScope:
        self._ensure_active()
        return PluginScope(name, parent=self)

    def own(self, identity: str, cleanup: Cleanup) -> None:
        """Synchronously register one cleanup owned by this scope.

        The async effect() path is for plugin install bodies; this is for
        runtime registrations that happen outside an install (e.g. a
        reflect.provide) and still must die with the scope.

        Re-registering the same identity replaces the previous cleanup and
        drops its batch, so repeated runtime registrations (e.g. re-provide
        of the same service name) cannot grow the registry unboundedly.
        Callers replacing an identity are responsible for invoking any old
        disposer they hold (those are idempotent).
        """
        prior = self._own_registry.pop(identity, None)
        if prior is not None:
            batch, _effect = prior
            batch.state = DISPOSED
            if batch in self.effects.batches:
                self.effects.batches.remove(batch)
        batch = self.effects.create_batch(self, f"own:{identity}")
        effect = self.effects.register(batch, identity, cleanup)
        self._own_registry[identity] = (batch, effect)

    async def effect(
        self,
        label: str,
        setup: Callable[[Collect], Any],
    ) -> EffectBatch:
        self._ensure_active()
        batch = self.effects.create_batch(self, label)

        def collect(identity: str, cleanup: Cleanup) -> Effect:
            return self.effects.register(batch, identity, cleanup)

        try:
            result = setup(collect)
            if inspect.isawaitable(result):
                await result
        except BaseException:
            await self.effects.rollback(batch)
            raise
        return batch

    async def dispose(self) -> list[BaseException]:
        if self._dispose_task is None:
            self._dispose_task = asyncio.create_task(self._dispose())
        return await self._dispose_task

    async def _dispose(self) -> list[BaseException]:
        if self.state == DISPOSED:
            return []
        self.state = DISPOSING
        errors: list[BaseException] = []
        if self.children:
            results = await asyncio.gather(
                *(child.dispose() for child in reversed(self.children)),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    errors.append(result)
                else:
                    errors.extend(result)
        errors.extend(await self.effects.dispose_all())
        self.state = DISPOSED
        return errors


class DependencyLifecycle:
    def __init__(self) -> None:
        self._dependents: dict[str, list[PluginScope]] = {}
        self._released: set[str] = set()

    def activate(self, provider_key: str) -> None:
        """Mark a provider live again for a new installation generation.

        ``_released`` is keyed by name, so without this a capability that was
        once unloaded poisons its own name forever: reinstalling it raises
        "already released" the moment a dependent binds to it. A fresh
        generation is a different provider that happens to share the name.
        """
        self._released.discard(provider_key)

    def register_dependent(self, provider_key: str, dependent_scope: PluginScope) -> None:
        if provider_key in self._released:
            raise ProviderReleasedError(f"provider {provider_key!r} already released")
        self._dependents.setdefault(provider_key, []).append(dependent_scope)

    def unregister_dependent(self, provider_key: str, dependent_scope: PluginScope) -> None:
        """Remove one dependent registration (idempotent)."""
        dependents = self._dependents.get(provider_key)
        if dependents is not None:
            try:
                dependents.remove(dependent_scope)
            except ValueError:
                pass
            if not dependents:
                self._dependents.pop(provider_key, None)

    async def release(self, provider_key: str, finalizer: Cleanup) -> list[BaseException]:
        if provider_key in self._released:
            return []
        self._released.add(provider_key)
        errors: list[BaseException] = []
        dependents = self._dependents.pop(provider_key, [])
        if dependents:
            results = await asyncio.gather(
                *(scope.dispose() for scope in dependents),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    errors.append(result)
                else:
                    errors.extend(result)
        try:
            result = finalizer()
            if inspect.isawaitable(result):
                await result
        except BaseException as exc:  # noqa: BLE001 -- teardown must collect all
            errors.append(exc)
        return errors
