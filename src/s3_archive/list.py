"""Paginating list_objects helper.

Lists every object under a prefix, transparently handling >1000-object
buckets via boto3's paginator, and skips directory markers (zero-byte
keys ending with ``/`` — the convention many S3 clients use to surface
"folders" in their UI).
"""

from collections.abc import Iterator


def list_objects(
    client,
    bucket: str,
    prefix: str = "",
    *,
    sort: bool = False,
) -> list[dict]:
    """List all objects under *prefix*, skipping directory markers.

    Returns a list of dicts with keys ``Key``, ``Size``, ``ETag``,
    ``LastModified`` (the object's mtime, or ``None`` if the response
    omits it), and ``RelativePath`` (the key with *prefix* stripped from
    the left).

    If *sort* is ``True`` the result is sorted by ``Key``; otherwise
    boto3's natural pagination order is preserved.
    """
    return list(iter_objects(client, bucket, prefix, sort=sort))


def iter_objects(
    client,
    bucket: str,
    prefix: str = "",
    *,
    sort: bool = False,
) -> Iterator[dict]:
    """Streaming variant of :func:`list_objects`.

    Yields one dict per object. ``sort=True`` necessarily materializes
    the full list first — fine for the modest sizes ``create`` deals
    with, but ``create`` callers that want true streaming should pass
    ``sort=False``.
    """
    paginator = client.get_paginator("list_objects_v2")
    rows: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            # Skip directory markers (size 0 + trailing slash).
            if size == 0 and key.endswith("/"):
                continue
            rel = key.removeprefix(prefix).lstrip("/") if prefix else key
            row = {
                "Key": key,
                "Size": size,
                "ETag": obj.get("ETag", ""),
                "LastModified": obj.get("LastModified"),
                "RelativePath": rel,
            }
            if sort:
                rows.append(row)
            else:
                yield row
    if sort:
        rows.sort(key=lambda r: r["Key"])
        yield from rows
