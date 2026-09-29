# pycordis

A Cordis-style plugin kernel for Python long-running processes.

> **Attribution:** pycordis is an independent Python implementation of the
> Cordis kernel semantics (`@cordiverse/cordis` 4.0.4). Not affiliated with
> or endorsed by the cordiverse project.

> **Rename note:** this package was previously developed as `plugin_kernel`
> inside agent-capability-forge and renamed `pycordis` before open-sourcing.

Every side effect a capability introduces — a tool entry, an event listener,
a background task — must hand over its own disposer at registration time and
live inside a container that dies with the capability. Uninstalling therefore
leaves no residue, and a failed install rolls back the effects it already
collected.

## Examples

Run any of them directly — each is standalone and prints what it proves:

| file | demonstrates |
|---|---|
| `examples/context_and_services.py` | Context attribute reads, extend/isolate, scoped tenancy, teardown |
| `examples/event_bus.py` | five dispatch modes (emit/parallel/serial/bail/waterfall), scope filtering |
| `examples/capability_lifecycle.py` | install -> unload -> reinstall with zero residue (needs `agentscope`) |
| `examples/epoch_reload_and_hot_config.py` | dependency auto-reload and schema-validated config hot update |
| `examples/webhook_verifier.py` | best-practice business plugin: HMAC webhook verification + secret rotation (needs `agentscope`) |

Start with `context_and_services.py`, then `capability_lifecycle.py`.

The core runs on the standard library alone; the two AgentScope
integration examples additionally need `pip install agentscope`.

## Usage

```python
from pycordis import CapabilityDescriptor, PluginManager

manager = PluginManager()

manager.register(CapabilityDescriptor(
    id="my-capability",
    version="1",
    factory=MyCapability,            # sync factory -> instance with install()/dispose
    dependencies=("other-capability",),
))

await manager.install("my-capability")   # dependency-first install order
await manager.unload("my-capability")    # cascades to dependents only in the right order
```

A capability's `install()` registers host resources through
`scope.effect(label, publish)`, where `publish(collect)` calls
`collect(name, disposer)` for every resource it creates. That pairing —
register and give up the way to undo it in the same moment — is the core
invariant of the whole design.

## Semantics

- dependency-first install order; unloading a provider with active
  dependents is rejected
- failed install rolls back the collected prefix of effects, then re-raises
- unload is idempotent; reinstall creates a fresh generation
- every effect is owned by exactly one scope; owner disposal reclaims it

The semantic baseline is the Cordis kernel (`cordiverse/cordis`,
TypeScript), ported to Python with documented, deliberate divergences
(sequential in-batch teardown, no HMR file watching).

## Status

0.1.0. Consumers: `agent-capability-forge` (forge pipeline + pilot).
The kernel itself has zero runtime dependencies.
