"""End-to-end CLI smoke tests (with a patched S3 client + moto)."""

from unittest.mock import patch

import pytest

from s3_archive.cli import main

from .conftest import build_7z, build_tar_gz, build_zip


@pytest.fixture
def patched_client(s3_client):
    """Make load_client() (called from the CLI) return the moto client."""
    with patch("s3_archive.cli.load_client", return_value=s3_client):
        yield s3_client


def _put_archive(s3, bucket, key, body):
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def _extracted_keys(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)


class TestExtractCommand:
    def test_extract_tar_gz(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n", "b.txt": b"beta\n"}
        _put_archive(patched_client, "src-bucket", "in/archive.tar.gz", build_tar_gz(files))

        rc = main(
            [
                "extract",
                "s3://src-bucket/in/archive.tar.gz",
                "s3://dest-bucket/out/",
            ]
        )

        assert rc == 0, capsys.readouterr().err
        keys = _extracted_keys(patched_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert "out/b.txt" in keys

    def test_extract_7z(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n", "b.txt": b"beta\n"}
        _put_archive(patched_client, "src-bucket", "in/archive.7z", build_7z(files))

        rc = main(
            [
                "extract",
                "s3://src-bucket/in/archive.7z",
                "s3://dest-bucket/out/",
            ]
        )

        assert rc == 0, capsys.readouterr().err
        keys = _extracted_keys(patched_client, "dest-bucket", "out/")
        assert "out/a.txt" in keys
        assert "out/b.txt" in keys

    def test_extract_zip_dry_run(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n"}
        _put_archive(patched_client, "src-bucket", "in/archive.zip", build_zip(files))

        rc = main(
            [
                "extract",
                "--dry-run",
                "s3://src-bucket/in/archive.zip",
                "s3://dest-bucket/out/",
            ]
        )

        assert rc == 0, capsys.readouterr().err
        assert _extracted_keys(patched_client, "dest-bucket", "out/") == []


class TestLsCommand:
    def test_ls_tar_gz_prints_members_and_summary(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n", "b.txt": b"beta\n"}
        _put_archive(patched_client, "src-bucket", "in/archive.tar.gz", build_tar_gz(files))

        rc = main(["ls", "s3://src-bucket/in/archive.tar.gz"])

        out = capsys.readouterr().out
        assert rc == 0
        assert "a.txt" in out
        assert "b.txt" in out
        assert " files, " in out

    def test_ls_zip(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n"}
        _put_archive(patched_client, "src-bucket", "in/archive.zip", build_zip(files))

        rc = main(["ls", "s3://src-bucket/in/archive.zip"])

        out = capsys.readouterr().out
        assert rc == 0
        assert "a.txt" in out

    def test_ls_7z(self, patched_client, capsys):
        files = {"a.txt": b"alpha\n", "b.txt": b"beta\n"}
        _put_archive(patched_client, "src-bucket", "in/archive.7z", build_7z(files))

        rc = main(["ls", "s3://src-bucket/in/archive.7z"])

        out = capsys.readouterr().out
        assert rc == 0
        assert "a.txt" in out
        assert "b.txt" in out
        assert " files, " in out

    def test_ls_rejects_unknown_format(self, patched_client, capsys):
        rc = main(["ls", "s3://src-bucket/file.rar"])
        assert rc == 2
        assert "Cannot detect archive format" in capsys.readouterr().err


class TestCreateCommand:
    def test_create_tar_gz(self, patched_client, capsys):
        patched_client.put_object(Bucket="src-bucket", Key="src/a.txt", Body=b"alpha\n")

        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.tar.gz",
            ]
        )
        assert rc == 0, capsys.readouterr().err
        # The archive object was written.
        head = patched_client.head_object(Bucket="dest-bucket", Key="out/archive.tar.gz")
        assert head["ContentLength"] > 0

    def test_create_zip(self, patched_client, capsys):
        patched_client.put_object(Bucket="src-bucket", Key="src/a.txt", Body=b"alpha\n")

        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.zip",
            ]
        )
        assert rc == 0, capsys.readouterr().err

    def test_create_dry_run(self, patched_client, capsys):
        patched_client.put_object(Bucket="src-bucket", Key="src/a.txt", Body=b"alpha\n")

        rc = main(
            [
                "create",
                "--dry-run",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.tar.gz",
            ]
        )
        assert rc == 0, capsys.readouterr().err
        # Dry-run does not upload.
        keys = []
        paginator = patched_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="dest-bucket", Prefix="out/"):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        assert keys == []

    def test_create_rejects_bad_extension(self, patched_client, capsys):
        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.rar",
            ]
        )
        assert rc == 2
        assert "must end with" in capsys.readouterr().err

    def test_create_rejects_7z(self, patched_client, capsys):
        rc = main(
            [
                "create",
                "s3://src-bucket/src/",
                "s3://dest-bucket/out/archive.7z",
            ]
        )
        assert rc == 2
        assert ".7z create is not supported" in capsys.readouterr().err


class TestErrorHandling:
    def test_bad_extract_url_exits_2(self, patched_client, capsys):
        rc = main(["extract", "s3://src-bucket/file.rar", "s3://dest-bucket/out/"])
        assert rc == 2
        assert "Cannot detect archive format" in capsys.readouterr().err

    def test_missing_creds_exits_2(self, monkeypatch, tmp_path, capsys):
        # No patched_client — let load_client() run for real. Clear every
        # credential source so the in-memory s3-archive can't pick anything up.
        for k in (
            "S3CMD_CONFIG",
            "S3_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
        ):
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        with patch("s3_archive.s3_client.boto3.Session") as mock_session:
            mock_session.return_value.get_credentials.return_value = None
            rc = main(["ls", "s3://x/y.tar.gz"])
        assert rc == 2
        assert "No S3 credentials configured" in capsys.readouterr().err
