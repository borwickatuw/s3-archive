"""Canonical transient-error set + retry policy for S3 GET streams.

Both S3-read paths in this library survive isolated connection hiccups
by re-issuing the GET and continuing:

- the *seekable* path (:mod:`s3_archive.seven_z`) re-issues a ranged GET
  for the same byte range, and
- the *sequential* path — :func:`resumable_body_chunks`, used by
  :mod:`s3_archive.members` (archive extract) and :mod:`s3_archive.create`
  (per-source-object reads) — re-opens the stream with
  ``Range=bytes=<pos>-`` from the offset already consumed.

They share one definition of "which errors are worth retrying", one
default policy, and (for the sequential paths) one implementation, so
the behavior can't drift between them — the one-canonical-location rule
the project follows everywhere except credential resolution.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator

import botocore.exceptions

from s3_archive.log_config import get_logger

log = get_logger(__name__)

_CHUNK_SIZE = 65536

# Transient errors worth retrying — connection drops, read stalls, and
# the urllib3-level timeouts that botocore wraps. Anything else (4xx
# AccessDenied, NoSuchKey, malformed-bytes parse errors) propagates so
# the operator sees a true failure rather than a multi-minute backoff
# on a permanent problem.
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    botocore.exceptions.ReadTimeoutError,
    botocore.exceptions.ConnectTimeoutError,
    botocore.exceptions.EndpointConnectionError,
    botocore.exceptions.ConnectionClosedError,
    botocore.exceptions.IncompleteReadError,
    # ResponseStreamingError covers mid-stream urllib3 errors that
    # botocore re-raises after the headers came back.
    botocore.exceptions.ResponseStreamingError,
)

# Default retry policy for transient GET failures. Retries back off
# exponentially: wait ``base * FACTOR**(n-1)`` before the nth consecutive
# retry, capped at ``MAX_DELAY``. A small first wait means the common
# RadosGW case — a dropped connection that a fresh ranged GET immediately
# replaces — resumes in seconds rather than a flat minute; the cap keeps
# a genuinely struggling endpoint from being hammered. Because the budget
# is on *consecutive* failures (any forward progress resets it), a long
# transfer survives arbitrarily many well-separated hiccups; the attempt
# cap only bites when several retries in a row make zero progress. The
# 5/15/45/60 schedule gives ~125 s of grace before a truly dead endpoint
# is declared failed. Override via the relevant constructor / function
# parameters for tighter or more relaxed policies.
DEFAULT_RETRY_DELAY_S = 5  # base (first-retry) wait, in seconds
DEFAULT_RETRY_MAX_DELAY_S = 60  # per-wait cap
DEFAULT_RETRY_BACKOFF_FACTOR = 3
DEFAULT_RETRY_MAX_ATTEMPTS = 5


def backoff_delay(
    attempt: int,
    *,
    base: float = DEFAULT_RETRY_DELAY_S,
    factor: float = DEFAULT_RETRY_BACKOFF_FACTOR,
    cap: float = DEFAULT_RETRY_MAX_DELAY_S,
) -> float:
    """Seconds to wait before the *attempt*-th (1-based) consecutive retry.

    Exponential: ``base * factor**(attempt - 1)``, clamped to *cap*. A
    zero *base* (used by tests) makes every wait zero. Shared by both
    retry paths so their backoff can't drift.
    """
    return min(base * factor ** (attempt - 1), cap)


def resumable_body_chunks(
    client,
    bucket: str,
    key: str,
    *,
    chunk_size: int = _CHUNK_SIZE,
    retry_delay_s: float = DEFAULT_RETRY_DELAY_S,
    retry_max_attempts: int = DEFAULT_RETRY_MAX_ATTEMPTS,
    on_bytes: Callable[[int], None] | None = None,
) -> Iterator[bytes]:
    """Yield ``s3://bucket/key``'s body in chunks, resuming dropped streams.

    A forward-only consumer (tar / zip decode, or feeding an archive
    encoder) can survive an isolated mid-stream connection drop
    (``ResponseStreamingError`` / ``IncompleteRead``): re-issue
    ``get_object(Range="bytes=<pos>-")`` from the offset already emitted
    and keep reading. The consumer sees one continuous, correct byte
    stream and never learns the underlying HTTP connection was replaced.
    The break happens *during* ``read()``, before the chunk is yielded
    downstream, so *pos* (bytes already emitted) is exact — no gap, no
    overlap.

    Both the (re)open GET and the ``read()`` sit under one ``try``, so a
    failure in *either* triggers the same sleep-and-resume.

    **Retry budget resets on progress.** The cap is on *consecutive*
    failures with no forward progress — any successful chunk zeroes the
    counter. A long transfer that survives several isolated hiccups over
    hours isn't killed by a total-attempt cap, while a genuinely dead
    endpoint (no progress) still gives up promptly after
    ``retry_max_attempts``.

    *on_bytes*, if supplied, is called with the length of each yielded
    chunk (bytes of genuine forward progress — a resumed read never
    re-reports already-emitted bytes), suitable for driving a
    ``tqdm.update`` byte-progress bar.
    """
    pos = 0
    consecutive_failures = 0
    body = None
    while True:
        try:
            if body is None:  # initial open or post-drop reopen at pos
                kw = {} if pos == 0 else {"Range": f"bytes={pos}-"}
                body = client.get_object(Bucket=bucket, Key=key, **kw)["Body"]
            chunk = body.read(chunk_size)
        except TRANSIENT_ERRORS as exc:
            consecutive_failures += 1
            if consecutive_failures >= retry_max_attempts:
                raise
            delay = backoff_delay(consecutive_failures, base=retry_delay_s)
            # Concise single line (no giant IncompleteRead repr) so it
            # doesn't wrap and mangle the progress bar; explains the pause.
            log.warning(
                "s3://%s/%s: read stream dropped at byte %d (%s, attempt %d/%d); "
                "progress paused, reconnecting in %.0f s",
                bucket,
                key,
                pos,
                type(exc).__name__,
                consecutive_failures,
                retry_max_attempts,
                delay,
            )
            time.sleep(delay)
            body = None  # force a fresh ranged GET from pos
            continue
        if not chunk:
            return
        pos += len(chunk)
        consecutive_failures = 0  # forward progress resets the budget
        if on_bytes is not None:
            on_bytes(len(chunk))
        yield chunk
