"""Tests for s3_archive.retry.backoff_delay (the shared retry schedule)."""

from s3_archive.retry import (
    DEFAULT_RETRY_BACKOFF_FACTOR,
    DEFAULT_RETRY_DELAY_S,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    DEFAULT_RETRY_MAX_DELAY_S,
    backoff_delay,
)


def test_default_schedule_grows_then_caps():
    """Consecutive retries wait 5, 15, 45, then clamp at the 60 s cap."""
    delays = [backoff_delay(n) for n in range(1, DEFAULT_RETRY_MAX_ATTEMPTS + 1)]
    assert delays == [5, 15, 45, 60, 60]
    # Only retry_max_attempts-1 waits actually happen (the last attempt
    # raises instead of sleeping); those sum to the advertised ~125 s grace.
    assert sum(delays[: DEFAULT_RETRY_MAX_ATTEMPTS - 1]) == 125


def test_first_wait_is_the_base():
    assert backoff_delay(1) == DEFAULT_RETRY_DELAY_S


def test_zero_base_means_zero_waits():
    # Tests pass retry_delay_s=0 to keep fast; every backoff must be 0.
    assert [backoff_delay(n, base=0) for n in range(1, 6)] == [0, 0, 0, 0, 0]


def test_cap_is_honored():
    assert backoff_delay(99) == DEFAULT_RETRY_MAX_DELAY_S


def test_factor_and_base_are_overridable():
    assert backoff_delay(3, base=2, factor=10, cap=10_000) == 2 * 10**2


def test_backoff_factor_matches_schedule():
    # Guard: the module constant and the observed growth agree.
    assert backoff_delay(2) == DEFAULT_RETRY_DELAY_S * DEFAULT_RETRY_BACKOFF_FACTOR
