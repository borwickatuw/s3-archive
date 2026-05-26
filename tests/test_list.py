"""Tests for s3_archive.list."""

from unittest.mock import MagicMock

from s3_archive.list import iter_objects, list_objects


def _mock_client(pages: list[list[dict]]):
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"Contents": p} for p in pages]
    return client


class TestListObjects:
    def test_skips_directory_markers(self):
        client = _mock_client(
            [
                [
                    {"Key": "prefix/", "Size": 0},  # directory marker
                    {"Key": "prefix/file1.txt", "Size": 100, "ETag": '"abc"'},
                    {"Key": "prefix/sub/file2.txt", "Size": 200, "ETag": '"def"'},
                ]
            ]
        )
        result = list_objects(client, "bucket", "prefix/")
        assert len(result) == 2
        assert result[0]["Key"] == "prefix/file1.txt"
        assert result[0]["Size"] == 100
        assert result[0]["ETag"] == '"abc"'
        assert result[0]["RelativePath"] == "file1.txt"
        assert result[1]["RelativePath"] == "sub/file2.txt"

    def test_paginates(self):
        client = _mock_client(
            [
                [{"Key": "a.txt", "Size": 1}],
                [{"Key": "b.txt", "Size": 2}],
                [{"Key": "c.txt", "Size": 3}],
            ]
        )
        result = list_objects(client, "bucket", "")
        assert [r["Key"] for r in result] == ["a.txt", "b.txt", "c.txt"]

    def test_handles_missing_etag(self):
        client = _mock_client([[{"Key": "x.txt", "Size": 1}]])
        result = list_objects(client, "bucket", "")
        assert result[0]["ETag"] == ""

    def test_sort_orders_by_key(self):
        client = _mock_client(
            [
                [{"Key": "c.txt", "Size": 1}, {"Key": "a.txt", "Size": 1}],
                [{"Key": "b.txt", "Size": 1}],
            ]
        )
        result = list_objects(client, "bucket", "", sort=True)
        assert [r["Key"] for r in result] == ["a.txt", "b.txt", "c.txt"]

    def test_empty_prefix_relative_path_is_key(self):
        client = _mock_client([[{"Key": "top/file.txt", "Size": 1}]])
        result = list_objects(client, "bucket", "")
        assert result[0]["RelativePath"] == "top/file.txt"


class TestIterObjects:
    def test_yields_streaming(self):
        client = _mock_client(
            [
                [{"Key": "a.txt", "Size": 1}, {"Key": "b.txt", "Size": 2}],
            ]
        )
        results = list(iter_objects(client, "bucket", ""))
        assert len(results) == 2
        assert results[0]["Key"] == "a.txt"
