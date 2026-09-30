"""Config validation, Volatile snapshot, and the logger service."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from anvilcore.config import ConfigError, Volatile, resolve_config
from anvilcore.logger import LoggerService


def _schema(raw: dict) -> dict:
    return {"retries": raw.get("retries", 3), "name": raw["name"]}


class ConfigTests(unittest.TestCase):
    def test_schema_fills_defaults(self):
        out = resolve_config(_schema, {"name": "x"})
        self.assertEqual(out, {"retries": 3, "name": "x"})

    def test_schema_missing_required_raises(self):
        with self.assertRaises(KeyError):
            resolve_config(_schema, {})

    def test_schema_must_return_dict(self):
        with self.assertRaises(ConfigError):
            resolve_config(lambda raw: "nope", {})

    def test_volatile_is_read_only(self):
        v = Volatile({"a": 1})
        self.assertEqual(v.a, 1)
        with self.assertRaises(AttributeError):
            v.a = 2


class LoggerTests(unittest.TestCase):
    def test_exporter_receives_messages_and_can_detach(self):
        svc = LoggerService()
        seen: list = []
        remove = svc.exporter(seen.append)
        log = svc.logger("payment")
        log.info("hello")
        log.error("boom")
        remove()
        log.warning("after-detach")
        self.assertEqual([(m.name, m.level) for m in seen],
                         [("payment", "info"), ("payment", "error")])


if __name__ == "__main__":
    unittest.main()
