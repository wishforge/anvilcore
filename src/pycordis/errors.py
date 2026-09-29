"""Structured error hierarchy (cordis has CordisError/ValidationError).

All framework errors derive from KernelError (itself a RuntimeError, so
existing ``except RuntimeError`` call sites keep working) and carry a
stable ``code`` for programmatic branching.
"""

from __future__ import annotations


class KernelError(RuntimeError):
    code = "KERNEL_ERROR"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.code = type(self).code


class ServiceNotProvidedError(KernelError):
    """Strict context lookup found no ACTIVE provider for a service."""
    code = "SERVICE_NOT_PROVIDED"


class InvalidTransitionError(KernelError):
    """Illegal lifecycle state transition (capability or scope)."""
    code = "INVALID_TRANSITION"


class InstallBlockedError(KernelError):
    """Install refused: capability already active/installing/disposing."""
    code = "INSTALL_BLOCKED"


class UnloadBlockedError(KernelError):
    """Unload refused: installing state, or active dependents present."""
    code = "UNLOAD_BLOCKED"


class ProviderReleasedError(KernelError):
    """A dependency bound to a provider generation that was released."""
    code = "PROVIDER_RELEASED"
