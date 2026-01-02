from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

__all__ = [
    "configure_logging",
    "log",
    "log_err",
    "_LOGGER",
    "_DEBUG_ENABLED",
    "_PREFETCH_DEBUG_ENABLED",
]

_LOGGER = logging.getLogger("shrink_media")
_DEBUG_ENABLED = False
_PREFETCH_DEBUG_ENABLED = False


class _LevelRangeFilter(logging.Filter):
    def __init__(self, *, min_level: int | None = None, max_level: int | None = None) -> None:
        super().__init__()
        self._min_level = min_level
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        if self._min_level is not None and record.levelno < self._min_level:
            return False
        if self._max_level is not None and record.levelno > self._max_level:
            return False
        return True


def configure_logging(log_file: Optional[str], *, append: bool, debug: bool, prefetch_debug: bool) -> None:
    global _DEBUG_ENABLED, _PREFETCH_DEBUG_ENABLED
    _DEBUG_ENABLED = bool(debug) or bool(log_file)
    _PREFETCH_DEBUG_ENABLED = bool(prefetch_debug)

    any_debug_console = bool(debug) or bool(prefetch_debug)
    _LOGGER.setLevel(logging.DEBUG)
    _LOGGER.propagate = False
    _LOGGER.handlers.clear()

    level = logging.DEBUG if any_debug_console else logging.INFO

    stdout_console = Console(stderr=False)
    stderr_console = Console(stderr=True)

    stdout_handler = RichHandler(
        console=stdout_console,
        show_time=True,
        show_level=True,
        show_path=False,
        rich_tracebacks=any_debug_console,
        markup=False,
    )
    stdout_handler.setLevel(level)
    stdout_handler.addFilter(_LevelRangeFilter(max_level=logging.INFO))
    _LOGGER.addHandler(stdout_handler)

    stderr_handler = RichHandler(
        console=stderr_console,
        show_time=True,
        show_level=True,
        show_path=False,
        rich_tracebacks=any_debug_console,
        markup=False,
    )
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.addFilter(_LevelRangeFilter(min_level=logging.WARNING))
    _LOGGER.addHandler(stderr_handler)

    if log_file:
        p = Path(log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append else "w"
        try:
            fh = logging.FileHandler(p, mode=mode, encoding="utf-8")
        except Exception as e:
            print(f"ERROR: cannot open --log-file {p}: {e}", file=sys.stderr, flush=True)
            sys.exit(2)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        _LOGGER.addHandler(fh)


def log(msg: str) -> None:
    _LOGGER.info(msg)


def log_err(msg: str) -> None:
    _LOGGER.error(msg)
