"""Canonical transient-error set + retry policy for S3 GET streams.

Both S3-read paths in this library survive isolated connection hiccups
by re-issuing the GET and continuing:

- the *seekable* path (:mod:`s3_archive.seven_z`) re-issues a ranged GET
  for the same byte range, and
- the *sequential* path (:mod:`s3_archive.members`) re-opens the archive
  stream with ``Range=bytes=<pos>-`` from the offset already consumed.

They share one definition of "which errors are worth retrying" and one
default policy so the behavior can't drift between the two — the
one-canonical-location rule the project follows everywhere except
credential resolution.
"""

from __future__ import annotations

import botocore.exceptions

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

# Default retry policy for transient GET failures. Tuned for large
# (~3 GB+) archives where one stalled GET shouldn't kill an hour of
# progress. A 60 s delay matches botocore's default ``read_timeout`` —
# gives Kopah/RGW one extra grace period to recover before we try again.
# Override via the relevant constructor / function parameters for
# tighter or more relaxed policies.
DEFAULT_RETRY_DELAY_S = 60
DEFAULT_RETRY_MAX_ATTEMPTS = 3
