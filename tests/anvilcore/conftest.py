"""Gate the three AgentScope-dependent evidence files on the optional dep.

These files are kept verbatim from the 2026-08-16 validation run, so the
dependency gate lives here rather than inside them. With AgentScope installed
they run unchanged; without it, collection is skipped instead of erroring.

Verified green against agentscope 2.0.2 on 2026-09-28: all 28 tests in these
three files pass, including the manager-level dependency ordering, rollback,
reinstall-generation and concurrency checks.
"""

import importlib.util

AGENTSCOPE_TESTS = [
    "test_agentscope_bridge.py",
    "test_capability_lifecycle.py",
    "test_capability_manager.py",
]

collect_ignore = [] if importlib.util.find_spec("agentscope") else AGENTSCOPE_TESTS
