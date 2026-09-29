"""Service base class: named registration owned by the constructing scope."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pycordis.context import Context
from pycordis.service import Service


class FsService(Service):
    def read(self, path: str) -> str:
        return f"read:{path}"


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()

    async def test_service_readable_as_ctx_attribute(self):
        FsService(self.ctx, "fs")
        self.assertEqual(self.ctx.fs.read("a.txt"), "read:a.txt")

    async def test_isolated_scopes_do_not_leak(self):
        FsService(self.ctx, "fs")
        child = self.ctx.isolate("fs")
        FsService(child, "fs")
        # child resolves its own instance; parent keeps its own
        self.assertIsNot(self.ctx.fs, child.fs)

    async def test_dispose_of_owner_scope_removes_service(self):
        scope = self.ctx.scope.child("owner")
        inner = Context(parent=self.ctx, scope=scope)
        FsService(inner, "fs")
        self.assertIsNotNone(self.ctx.reflect.get("fs"))
        await scope.dispose()
        self.assertIsNone(self.ctx.reflect.get("fs"))
        with self.assertRaises(AttributeError):
            _ = self.ctx.fs

    async def test_default_name_from_class_attribute(self):
        class Named(Service):
            provide = "named-svc"

        Named(self.ctx, None)
        self.assertIsNotNone(self.ctx.reflect.get("named-svc"))


if __name__ == "__main__":
    unittest.main()
