"""LoggerService (cordis logger.ts, minimal): named loggers + exporter sink."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO,
           "warning": logging.WARNING, "error": logging.ERROR}


@dataclass
class LogMessage:
    name: str
    level: str
    text: str


class LoggerService:
    def __init__(self) -> None:
        self._exporters: list[Callable[[LogMessage], None]] = []

    def exporter(self, fn: Callable[[LogMessage], None]) -> Callable[[], None]:
        self._exporters.append(fn)

        def remove() -> None:
            if fn in self._exporters:
                self._exporters.remove(fn)

        return remove

    def _emit(self, name: str, level: str, text: str) -> None:
        message = LogMessage(name=name, level=level, text=text)
        for fn in list(self._exporters):
            fn(message)

    def logger(self, name: str) -> Logger:
        return Logger(self, name)


class Logger:
    def __init__(self, service: LoggerService, name: str) -> None:
        self._service = service
        self._name = name
        self._log = logging.getLogger(f"pycordis.{name}")

    def _write(self, level: str, text: str) -> None:
        self._log.log(_LEVELS[level], text)
        self._service._emit(self._name, level, text)

    def debug(self, text: str) -> None:
        self._write("debug", text)

    def info(self, text: str) -> None:
        self._write("info", text)

    def warning(self, text: str) -> None:
        self._write("warning", text)

    def error(self, text: str) -> None:
        self._write("error", text)
