"""Tests for s3_archive.etag — ETag normalization + precondition helpers."""

import botocore.exceptions

from s3_archive.etag import etags_equal, is_precondition_failed, quote_etag


class TestQuoteEtag:
    def test_stripped_form_gets_quoted(self):
        assert quote_etag("abc123") == '"abc123"'

    def test_quoted_form_passes_through(self):
        assert quote_etag('"abc123"') == '"abc123"'

    def test_multipart_etag_with_dash(self):
        assert quote_etag("abc-3") == '"abc-3"'

    def test_surrounding_whitespace_stripped(self):
        assert quote_etag(' "abc" ') == '"abc"'


class TestEtagsEqual:
    def test_mixed_forms_compare_equal(self):
        assert etags_equal('"abc"', "abc")
        assert etags_equal("abc", "abc")
        assert etags_equal('"abc"', '"abc"')

    def test_different_values_unequal(self):
        assert not etags_equal('"abc"', '"def"')


def _client_error(code: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "boom"}}, "GetObject"
    )


class TestIsPreconditionFailed:
    def test_precondition_failed_code(self):
        assert is_precondition_failed(_client_error("PreconditionFailed"))

    def test_bare_412_code(self):
        assert is_precondition_failed(_client_error("412"))

    def test_other_client_error(self):
        assert not is_precondition_failed(_client_error("NoSuchKey"))

    def test_non_client_error(self):
        assert not is_precondition_failed(ValueError("nope"))
