"""Shared pytest fixtures: in-memory S3 (moto) + archive-builder helpers."""

import io
import random
import shutil
import struct
import subprocess
import tarfile
import tempfile
import zipfile
import zlib
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from moto.server import ThreadedMotoServer

from s3_archive.s3_client import _reset_client_cache


@pytest.fixture(autouse=True)
def _clear_client_cache():
    """Drop any cached boto3 clients between tests.

    `s3_archive.s3_client` keeps a module-level dict of profile → client
    for the lifetime of the process. Without this fixture a client
    built against one test's moto context would leak into the next,
    pointing at a torn-down endpoint and producing baffling failures.
    """
    _reset_client_cache()
    yield
    _reset_client_cache()


@pytest.fixture
def aws_creds(monkeypatch):
    """Set bogus AWS credentials so moto + boto3 don't try the real chain."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_client(aws_creds):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="src-bucket")
        client.create_bucket(Bucket="dest-bucket")
        yield client


@pytest.fixture
def cross_env_clients(s3_client, monkeypatch):
    """Resolver-mock cross-endpoint fixture.

    Monkeypatches `s3_archive.s3_client.client_for` so any profile
    name returns the moto client; tests simulate cross-env by varying
    *bucket names* per side. Cheaper and fast (no extra HTTP servers),
    but doesn't exercise real two-endpoint wiring — for that, see
    `cross_env_real_endpoints`.
    """
    monkeypatch.setattr("s3_archive.s3_client.client_for", lambda _profile: s3_client)
    monkeypatch.setattr("s3_archive.cli.client_for", lambda _profile: s3_client)
    return s3_client


@pytest.fixture
def cross_env_real_endpoints(tmp_path, monkeypatch):
    """Two real moto-server endpoints + two `~/.s3cfg-*` files.

    Exercises the real two-client path end-to-end: each profile resolves
    via `~/.s3cfg-<name>` to a distinct boto3 client pointed at a
    different `ThreadedMotoServer` instance. Slower than
    :func:`cross_env_clients` — use sparingly, one acceptance test per
    module is enough.

    Yields a dict::

        {
            "src": {"profile": "src-env", "client": <boto3>, "bucket": "src-bucket"},
            "dst": {"profile": "dst-env", "client": <boto3>, "bucket": "dst-bucket"},
        }

    The two endpoints have *distinct* bucket namespaces so a test that
    accidentally talks to the wrong endpoint fails loudly (NoSuchBucket).
    """
    # Bind to ephemeral ports so concurrent test runs don't collide.
    src_server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    dst_server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    src_server.start()
    dst_server.start()
    try:
        src_port = src_server._server.socket.getsockname()[1]
        dst_port = dst_server._server.socket.getsockname()[1]
        src_endpoint = f"http://127.0.0.1:{src_port}"
        dst_endpoint = f"http://127.0.0.1:{dst_port}"

        # HOME → tmp so we don't touch the developer's real config.
        # We bypass ~/.s3cfg-<name> entirely by patching client_for to
        # a static dict of pre-built clients; that's enough to exercise
        # the dual-endpoint code path. End-to-end resolver coverage
        # lives in test_s3_client.py.
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("S3CMD_CONFIG", raising=False)

        src_client = boto3.client(
            "s3",
            endpoint_url=src_endpoint,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        dst_client = boto3.client(
            "s3",
            endpoint_url=dst_endpoint,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )

        # Create distinct buckets on each side so a cross-talk bug
        # (uploading to the wrong endpoint) fails with NoSuchBucket.
        src_client.create_bucket(Bucket="src-bucket")
        dst_client.create_bucket(Bucket="dst-bucket")

        _reset_client_cache()
        clients_by_profile = {"src-env": src_client, "dst-env": dst_client}

        def _client_for(profile):
            key = profile if profile is not None else "default"
            if key not in clients_by_profile:
                raise KeyError(f"unexpected profile {key!r} in test")
            return clients_by_profile[key]

        monkeypatch.setattr("s3_archive.s3_client.client_for", _client_for)
        monkeypatch.setattr("s3_archive.cli.client_for", _client_for)

        yield {
            "src": {"profile": "src-env", "client": src_client, "bucket": "src-bucket"},
            "dst": {"profile": "dst-env", "client": dst_client, "bucket": "dst-bucket"},
        }
    finally:
        src_server.stop()
        dst_server.stop()


# ---------------------------------------------------------------------------
# Archive construction helpers
# ---------------------------------------------------------------------------


def build_tar(
    files: dict[str, bytes],
    *,
    mode: str = "w:gz",
    wrap_prefix: str = "",
) -> bytes:
    """Serialize *files* as a tar archive in memory.

    *mode* is passed directly to :func:`tarfile.open` and selects the
    compression — ``"w"`` for plain tar, ``"w:gz"`` / ``"w:bz2"`` / ``"w:xz"``
    for the compressed variants.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode=mode) as tar:
        for name, content in files.items():
            member_name = f"{wrap_prefix}/{name}" if wrap_prefix else name
            info = tarfile.TarInfo(name=member_name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def build_tar_gz(files: dict[str, bytes], *, wrap_prefix: str = "") -> bytes:
    """Shortcut for :func:`build_tar` with ``mode="w:gz"``."""
    return build_tar(files, mode="w:gz", wrap_prefix=wrap_prefix)


def build_tar_xz_multiblock(
    files: dict[str, bytes], *, block_size: str = "1MiB", wrap_prefix: str = ""
) -> bytes:
    """Serialize *files* as a **multi-block** ``.tar.xz`` via the ``xz`` CLI.

    stdlib ``lzma`` (``build_tar(mode="w:xz")``) emits a *single* xz block,
    which resume refuses — so the resumable-xz tests need a multi-block
    archive. ``xz --block-size`` splits the stream into independently
    seekable blocks; feed it enough incompressible data (see
    :func:`incompressible_bytes`) that the split actually happens. Skipped
    when the ``xz`` CLI isn't on PATH.
    """
    if shutil.which("xz") is None:
        pytest.skip("xz CLI not installed; skipping multi-block .tar.xz fixture")
    inner = build_tar(files, mode="w", wrap_prefix=wrap_prefix)  # uncompressed tar
    # cmd is built from constants; not user input.
    cmd = ["xz", "-z", f"--block-size={block_size}", "-c", "-"]
    proc = subprocess.run(cmd, input=inner, capture_output=True, check=True)  # noqa: S603
    return proc.stdout


def incompressible_bytes(n: int, *, seed: int) -> bytes:
    """*n* deterministic pseudo-random bytes that gzip can't shrink.

    Used by the gzip ``--resume`` tests: they need archives whose
    *compressed* size exceeds :class:`SeekableS3Object`'s tail prefetch, so
    that seeking past already-done members demonstrably avoids re-reading
    the early source. Seeded so a run is reproducible.
    """
    return random.Random(seed).randbytes(n)  # noqa: S311 - test fixture, not crypto


def build_zip(files: dict[str, bytes], *, wrap_prefix: str = "") -> bytes:
    """Serialize *files* as a zip archive in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            member_name = f"{wrap_prefix}/{name}" if wrap_prefix else name
            zf.writestr(member_name, content)
    return buf.getvalue()


def build_stored_dd_zip(
    members: dict[str, bytes],
    *,
    dd_names: set[str] | None = None,
    dos_datetime: tuple[int, int, int, int, int, int] | None = None,
) -> bytes:
    """Hand-pack a zip with *stored* members, optionally data-descriptor-shaped.

    Members in *dd_names* get flag bit 3 (data descriptor) and ZERO
    sizes in the local file header — the shape SwissTransfer /
    Drive-style on-the-fly generators produce, which a forward-only
    reader cannot walk. ``dd_names=None`` means ALL file members get
    that shape. Members not in *dd_names* are written as plain stored
    entries with real local-header sizes (forward-streamable), so a
    mixed dict exercises the mid-archive streaming→seekable handoff.
    Names ending in ``/`` become directory entries (empty, never DD).

    *dos_datetime*, when given as ``(year, month, day, hour, minute,
    second)``, is stamped into every member's mod time/date fields
    (2-second DOS granularity — use an even second). The default zeros
    mean "no mtime", matching generators that don't write one.

    Names are written as raw UTF-8 with flag bit 11 UNSET — the
    flagless-UTF-8 shape many zip tools produce, which exercises the
    CD/LFH name-decode parity path for non-ASCII names.

    Stdlib ``zipfile`` always writes real LFH sizes, hence the hand
    ``struct.pack``. Everything is stored (compression=0) to keep the
    packing simple; the central directory carries the true sizes either
    way.
    """
    if dos_datetime is None:
        dos_time, dos_date = 0, 0
    else:
        year, month, day, hour, minute, second = dos_datetime
        dos_time = (hour << 11) | (minute << 5) | (second // 2)
        dos_date = ((year - 1980) << 9) | (month << 5) | day
    buf = io.BytesIO()
    cd_records = []
    for name, raw_body in members.items():
        name_b = name.encode("utf-8")
        is_dir = name.endswith("/")
        body = b"" if is_dir else raw_body
        with_dd = not is_dir and (dd_names is None or name in dd_names)
        offset = buf.tell()
        crc = zlib.crc32(body) & 0xFFFFFFFF
        flags = 0x0008 if with_dd else 0
        lfh_crc = 0 if with_dd else crc
        lfh_size = 0 if with_dd else len(body)
        buf.write(
            struct.pack(
                "<IHHHHHIIIHH",
                0x04034B50,  # local file header signature
                20,  # version needed
                flags,
                0,  # compression: stored
                dos_time,
                dos_date,
                lfh_crc,
                lfh_size,  # compressed size (0 under DD — the unstreamable part)
                lfh_size,  # uncompressed size
                len(name_b),
                0,  # extra length
            )
        )
        buf.write(name_b)
        buf.write(body)
        if with_dd:
            # Data descriptor (with the optional PK\x07\x08 signature).
            buf.write(struct.pack("<IIII", 0x08074B50, crc, len(body), len(body)))
        cd_records.append((name_b, flags, crc, len(body), offset))

    cd_start = buf.tell()
    for name_b, flags, crc, size, offset in cd_records:
        buf.write(
            struct.pack(
                "<IHHHHHHIIIHHHHHII",
                0x02014B50,  # central directory header signature
                20,  # version made by
                20,  # version needed
                flags,
                0,  # compression: stored
                dos_time,
                dos_date,
                crc,
                size,  # compressed size — the CD has the truth
                size,  # uncompressed size
                len(name_b),
                0,  # extra length
                0,  # comment length
                0,  # disk number
                0,  # internal attrs
                0,  # external attrs
                offset,
            )
        )
        buf.write(name_b)
    cd_size = buf.tell() - cd_start
    buf.write(
        struct.pack(
            "<IHHHHIIH",
            0x06054B50,  # end of central directory signature
            0,
            0,
            len(cd_records),
            len(cd_records),
            cd_size,
            cd_start,
            0,  # comment length
        )
    )
    return buf.getvalue()


# 7z archive flavors that exercise different code paths in any prospective
# streaming reader. See docs/7Z-SUPPORT.md for the design context.
SEVEN_Z_FLAVORS: dict[str, list[str]] = {
    "solid": [],
    "nonsolid": ["-ms=off"],
    "plain_header": ["-mhc=off"],
    "solid_bcj": ["-m0=BCJ", "-m1=LZMA2"],
    # `Copy + Delta` chain: no terminal compression filter, so stdlib lzma
    # rejects py7zr's translated FORMAT_RAW chain. Exercises the
    # native-decoder fallback in s3_archive.native_decoders.
    "copy_delta": ["-mx=0", "-mf=Delta:2"],
}


def build_7z(
    files: dict[str, bytes],
    *,
    flavor: str = "solid",
    wrap_prefix: str = "",
) -> bytes:
    """Serialize *files* as a .7z archive by shelling out to the ``7z`` CLI.

    *flavor* selects a preset flag set keyed in :data:`SEVEN_Z_FLAVORS`:

    - ``"solid"`` — default (solid + LZMA2 + encoded header); the common
      Preservation case.
    - ``"nonsolid"`` — ``-ms=off``; one Folder per file.
    - ``"plain_header"`` — ``-mhc=off``; non-encoded header (simpler
      parse path for comparison).
    - ``"solid_bcj"`` — ``-m0=BCJ -m1=LZMA2``; filter chain with
      interleaved pack streams.

    The 7z format can't be serialized in memory the way tar and zip can
    (the StartHeader at the front references a header at the end), so
    this helper writes the members to a temp dir, invokes ``7z a``, and
    returns the resulting archive bytes. Tests that call it are skipped
    if the ``7z`` CLI is not on ``PATH``.
    """
    if shutil.which("7z") is None:
        pytest.skip("7z CLI not installed; skipping .7z fixture")
    if flavor not in SEVEN_Z_FLAVORS:
        raise ValueError(f"Unknown 7z flavor {flavor!r}; expected one of {sorted(SEVEN_Z_FLAVORS)}")

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        src = tmpdir / "src"
        src.mkdir()
        member_names: list[str] = []
        for name, content in files.items():
            member_name = f"{wrap_prefix}/{name}" if wrap_prefix else name
            target = src / member_name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            member_names.append(member_name)

        archive = tmpdir / "out.7z"
        cmd = ["7z", "a", *SEVEN_Z_FLAVORS[flavor], str(archive), *member_names]
        # cmd is built from constants and dict keys controlled by the test;
        # not user input.
        subprocess.run(cmd, check=True, capture_output=True, cwd=src)  # noqa: S603
        return archive.read_bytes()
