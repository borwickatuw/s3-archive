"""Logging setup for s3-archive.

One module-level logger per file. Console handler is attached lazily by
the CLI entry point so library callers can configure logging themselves.
"""

import logging
import sys

from tqdm import tqdm

_console_initialized = False


class _TqdmLoggingHandler(logging.Handler):
    """Route log records through tqdm.write so they don't clobber an active progress bar."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            tqdm.write(msg, file=sys.stderr)
        except Exception:  # noqa: BLE001 - defensive, matches stdlib StreamHandler
            self.handleError(record)


def setup_console(level: int = logging.INFO) -> None:
    """Attach a console handler at *level*. Idempotent."""
    global _console_initialized  # noqa: PLW0603 - module-level once-flag
    if _console_initialized:
        return
    _console_initialized = True

    handler = _TqdmLoggingHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(message)s"))

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
