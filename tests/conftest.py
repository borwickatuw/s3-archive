"""Shared pytest fixtures: in-memory S3 (moto) + archive-builder helpers."""

import io
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

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


def build_zip(files: dict[str, bytes], *, wrap_prefix: str = "") -> bytes:
    """Serialize *files* as a zip archive in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            member_name = f"{wrap_prefix}/{name}" if wrap_prefix else name
            zf.writestr(member_name, content)
    return buf.getvalue()


# 7z archive flavors that exercise different code paths in any prospective
# streaming reader. See docs/7Z-SUPPORT.md for the design context.
SEVEN_Z_FLAVORS: dict[str, list[str]] = {
    "solid": [],
    "nonsolid": ["-ms=off"],
    "plain_header": ["-mhc=off"],
    "solid_bcj": ["-m0=BCJ", "-m1=LZMA2"],
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
