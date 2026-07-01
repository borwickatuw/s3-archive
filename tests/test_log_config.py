"""Tests for console logging setup.

The key behavioral contract: ``-v`` (DEBUG) must surface s3_archive's own
per-file progress without turning on botocore / urllib3 debug chatter.
"""

import logging

import pytest

from s3_archive import log_config


@pytest.fixture
def reset_logging():
    """Reset the once-flag and restore logger/handler state around each test.

    ``setup_console`` is deliberately idempotent via a module-level flag and
    mutates the process-global root logger, so tests must isolate themselves.
    """
    root = logging.getLogger()
    pkg = logging.getLogger("s3_archive")

    saved_flag = log_config._console_initialized
    saved_root_level = root.level
    saved_root_handlers = root.handlers[:]
    saved_pkg_level = pkg.level

    log_config._console_initialized = False

    try:
        yield
    finally:
        log_config._console_initialized = saved_flag
        root.setLevel(saved_root_level)
        root.handlers[:] = saved_root_handlers
        pkg.setLevel(saved_pkg_level)


def test_verbose_enables_package_debug_but_not_third_party(reset_logging):
    """-v (DEBUG) lowers the s3_archive logger to DEBUG while root/botocore stay quiet."""
    log_config.setup_console(logging.DEBUG)

    pkg = logging.getLogger("s3_archive")
    assert pkg.isEnabledFor(logging.DEBUG)

    # Root — and therefore third-party loggers — must not drop to DEBUG.
    assert logging.getLogger().level == logging.WARNING
    assert not logging.getLogger("botocore").isEnabledFor(logging.DEBUG)
    assert not logging.getLogger("botocore").isEnabledFor(logging.INFO)


def test_non_verbose_leaves_package_at_info(reset_logging):
    """Without -v the package logs at INFO, and third-party stays at WARNING."""
    log_config.setup_console(logging.INFO)

    pkg = logging.getLogger("s3_archive")
    assert pkg.isEnabledFor(logging.INFO)
    assert not pkg.isEnabledFor(logging.DEBUG)
    assert not logging.getLogger("botocore").isEnabledFor(logging.INFO)


def test_setup_console_is_idempotent(reset_logging):
    """A second call must not stack a second handler on root."""
    log_config.setup_console(logging.INFO)
    handler_count = len(logging.getLogger().handlers)

    log_config.setup_console(logging.DEBUG)
    assert len(logging.getLogger().handlers) == handler_count
