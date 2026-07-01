"""ETag normalization + precondition helpers for If-Match-pinned reads.

S3 surfaces ETags in quoted form (``"abc123"``) in HEAD/GET/LIST
responses, and the ``If-Match`` request header expects that same quoted
form. Callers, however, often store the quote-stripped value (e.g.
inventory snapshots strip quotes at LIST time). These helpers accept
either form so call sites don't have to care which one they're holding.
"""

import botocore.exceptions


def quote_etag(etag: str) -> str:
    """Return *etag* in the quoted form the ``If-Match`` header expects.

    Accepts either the quoted (``'"abc"'``) or stripped (``'abc'``)
    form; already-quoted values pass through unchanged.
    """
    etag = etag.strip()
    if etag.startswith('"') and etag.endswith('"') and len(etag) >= 2:
        return etag
    return f'"{etag}"'


def etags_equal(a: str, b: str) -> bool:
    """Compare two ETags ignoring surrounding quotes."""
    return a.strip().strip('"') == b.strip().strip('"')


def is_precondition_failed(exc: BaseException) -> bool:
    """True if *exc* is a botocore ClientError for a failed If-Match (HTTP 412).

    botocore has no modeled exception class for GetObject's 412 — it
    surfaces as a generic :class:`~botocore.exceptions.ClientError` with
    ``Error.Code == "PreconditionFailed"`` (some S3-compatible endpoints
    report the bare status code instead).
    """
    if not isinstance(exc, botocore.exceptions.ClientError):
        return False
    code = exc.response.get("Error", {}).get("Code", "")
    return code in ("PreconditionFailed", "412")
