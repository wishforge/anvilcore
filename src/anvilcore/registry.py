"""RegistryService: plugin loading + dependency injection with PENDING wait.

`inject(names, callback)` suspends until every named service is provided
(cordis keeps the fiber PENDING for the same reason), then runs the
callback with the bound context. `plugin()` registers + installs a
descriptor through the existing PluginManager. `delete()` unloads.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from .capability import CapabilityDescriptor
from .manager import PluginManager


class RegistryService:
    def __init__(self, manager: PluginManager | None = None) -> None:
        self.manager = manager or PluginManager()
        self._configs: dict[str, dict] = {}

    async def plugin_for(self, ctx, descriptor: CapabilityDescriptor,
                         config: dict | None = None) -> Any:
        if config is not None:
            object.__setattr__(descriptor, "config", config)  # frozen dataclass
        self.manager.register(descriptor)
        cap = await self.manager.install(descriptor.id)
        self._configs[descriptor.id] = dict(descriptor.config)

        # epoch reload: for every dependency that is a PROVIDED SERVICE
        # (not a capability id), watch for re-provides and reload this
        # plugin automatically when the generation changes.
        store = ctx._store
        for dep in descriptor.dependencies:
            if dep not in self.manager.records and dep in store._entries:
                self._register_epoch_watcher(ctx, descriptor, dep)
        return cap

    def _register_epoch_watcher(self, ctx, descriptor: CapabilityDescriptor,
                                service_name: str) -> None:
        snapshot = {"generation": ctx.reflect._store.generation_of(ctx, service_name)}
        reload_lock = {"busy": False}

        def on_generation_change(new_generation: int) -> None:
            if reload_lock["busy"]:
                return
            if new_generation == snapshot["generation"]:
                return   # no change for this consumer's label
            current = ctx.reflect._store.generation_of(ctx, service_name)
            if current == snapshot["generation"]:
                return   # stale watcher tick
            snapshot["generation"] = current
            reload_lock["busy"] = True

            async def reload() -> None:
                try:
                    await self.manager.reinstall(descriptor.id)
                finally:
                    reload_lock["busy"] = False

            asyncio.get_running_loop().create_task(reload())

        ctx.reflect.add_watcher(service_name, on_generation_change)

    async def update_for(self, ctx, plugin_id: str,
                         config: dict | None = None) -> Any:
        """config hot update: validate -> reload in place, generation +1."""
        descriptor = self.manager.get(plugin_id).descriptor
        if descriptor.schema is not None and config is not None:
            config = descriptor.schema(dict(config))
        if config is not None:
            object.__setattr__(descriptor, "config", config)
            self._configs[plugin_id] = dict(config)
        # graceful rotation lives in manager.reinstall: the replacement goes
        # live immediately, the retired generation drains in the background
        return await self.manager.reinstall(plugin_id)

    async def delete(self, plugin_id: str) -> list[BaseException]:
        return await self.manager.unload(plugin_id)

    async def inject_for(self, ctx, names, callback: Callable) -> Any:
        for name in names:
            await ctx.reflect.wait(name)
        result = callback(ctx)
        if asyncio.iscoroutine(result):
            result = await result
        return result


class BoundRegistry:
    def __init__(self, store: RegistryService, ctx) -> None:
        self._store = store
        self._ctx = ctx

    async def plugin(self, descriptor: CapabilityDescriptor,
                     config: dict | None = None) -> Any:
        return await self._store.plugin_for(self._ctx, descriptor, config)

    async def inject(self, names, callback: Callable) -> Any:
        return await self._store.inject_for(self._ctx, names, callback)

    async def update(self, plugin_id: str,
                     config: dict | None = None) -> Any:
        return await self._store.update_for(self._ctx, plugin_id, config)

    async def delete(self, plugin_id: str) -> list[BaseException]:
        return await self._store.delete(plugin_id)

    @property
    def manager(self) -> PluginManager:
        return self._store.manager
