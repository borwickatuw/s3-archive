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
    """Attach a console handler and set the s3_archive log *level*. Idempotent.

    Only the ``s3_archive`` package logger follows *level*; the root logger
    stays at WARNING so that ``-v`` (DEBUG) surfaces our per-file progress
    without unleashing botocore / urllib3 / s3transfer debug chatter.
    """
    global _console_initialized  # noqa: PLW0603 - module-level once-flag
    if _console_initialized:
        return
    _console_initialized = True

    # The handler passes everything through; per-logger levels do the gating.
    handler = _TqdmLoggingHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(message)s"))

    # Root (and thus third-party libraries) floor at WARNING regardless of -v.
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    root.addHandler(handler)

    # Our package alone honors the requested level (INFO, or DEBUG under -v).
    logging.getLogger("s3_archive").setLevel(level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
