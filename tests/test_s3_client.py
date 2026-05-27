"""Tests for s3_archive.s3_client — credential resolution + per-profile cache."""

from unittest.mock import patch

import pytest

from s3_archive import s3_client
from s3_archive.exceptions import ConfigError


@pytest.fixture
def home_tmp(tmp_path, monkeypatch):
    """Point HOME at tmp_path and clear bleed-in env vars."""
    monkeypatch.setenv("HOME", str(tmp_path))
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
    return tmp_path


def _write_s3cfg(path, *, access_key: str = "AK", secret_key: str = "SK", host_base: str = "h"):
    path.write_text(
        f"[default]\n"
        f"access_key = {access_key}\n"
        f"secret_key = {secret_key}\n"
        f"host_base = {host_base}\n"
    )


class TestLoadClientDefaultProfile:
    """The default branch is the unchanged historical chain (env > ~/.s3cfg > boto3)."""

    def test_reads_s3cmd_config_env(self, home_tmp, monkeypatch):
        path = home_tmp / "explicit.cfg"
        _write_s3cfg(path, access_key="EX", secret_key="EX", host_base="explicit.example")
        monkeypatch.setenv("S3CMD_CONFIG", str(path))

        client = s3_client.load_client()

        # The smoke test: a client was built against the host_base we set.
        assert client.meta.endpoint_url == "https://explicit.example"

    def test_reads_default_s3cfg_when_env_unset(self, home_tmp):
        default = home_tmp / ".s3cfg"
        _write_s3cfg(default, host_base="default.example")

        client = s3_client.load_client()

        assert client.meta.endpoint_url == "https://default.example"

    def test_env_wins_over_default(self, home_tmp, monkeypatch):
        default = home_tmp / ".s3cfg"
        _write_s3cfg(default, host_base="default.example")
        explicit = home_tmp / "explicit.cfg"
        _write_s3cfg(explicit, host_base="explicit.example")
        monkeypatch.setenv("S3CMD_CONFIG", str(explicit))

        client = s3_client.load_client()

        assert client.meta.endpoint_url == "https://explicit.example"

    def test_no_creds_anywhere_raises(self, home_tmp):
        # No ~/.s3cfg, no AWS_*, no $S3CMD_CONFIG. boto3.Session returns None.
        with (
            patch("s3_archive.s3_client.boto3.Session") as mock_session,
            pytest.raises(ConfigError, match="No S3 credentials configured"),
        ):
            mock_session.return_value.get_credentials.return_value = None
            s3_client.load_client()


class TestLoadClientNamedProfile:
    """Named-profile branch — reads ~/.s3cfg-<name> exclusively."""

    def test_reads_profile_file(self, home_tmp):
        path = home_tmp / ".s3cfg-kopah"
        _write_s3cfg(path, host_base="kopah.example")

        client = s3_client.load_client(profile="kopah")

        assert client.meta.endpoint_url == "https://kopah.example"

    def test_missing_file_raises_with_hint(self, home_tmp):
        with pytest.raises(ConfigError, match="Profile 'kopah'") as excinfo:
            s3_client.load_client(profile="kopah")
        msg = str(excinfo.value)
        assert ".s3cfg-kopah" in msg
        assert "s3-archive config --profile kopah" in msg

    def test_ignores_s3cmd_config_env(self, home_tmp, monkeypatch):
        # $S3CMD_CONFIG pointing at a valid file does NOT influence the
        # named-profile branch. Only ~/.s3cfg-<name> matters.
        bogus = home_tmp / "would-have-worked.cfg"
        _write_s3cfg(bogus, host_base="bogus.example")
        monkeypatch.setenv("S3CMD_CONFIG", str(bogus))

        # ~/.s3cfg-kopah doesn't exist → ConfigError, not silent fallback.
        with pytest.raises(ConfigError, match="Profile 'kopah'"):
            s3_client.load_client(profile="kopah")

    def test_ignores_s3cmd_config_env_when_profile_file_exists(self, home_tmp, monkeypatch):
        # Both files exist; the named-profile branch reads ~/.s3cfg-kopah.
        kopah = home_tmp / ".s3cfg-kopah"
        _write_s3cfg(kopah, host_base="kopah.example")

        bogus = home_tmp / "would-have-won.cfg"
        _write_s3cfg(bogus, host_base="bogus.example")
        monkeypatch.setenv("S3CMD_CONFIG", str(bogus))

        client = s3_client.load_client(profile="kopah")
        assert client.meta.endpoint_url == "https://kopah.example"

    def test_does_not_fall_back_to_boto3(self, home_tmp, monkeypatch):
        # Even with AWS_* in the env, a missing profile file raises.
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
        with pytest.raises(ConfigError, match="Profile 'kopah'"):
            s3_client.load_client(profile="kopah")


class TestClientForCache:
    def test_same_profile_returns_same_object(self, home_tmp):
        path = home_tmp / ".s3cfg-kopah"
        _write_s3cfg(path, host_base="kopah.example")

        c1 = s3_client.client_for("kopah")
        c2 = s3_client.client_for("kopah")
        assert c1 is c2

    def test_different_profiles_return_different_objects(self, home_tmp):
        default = home_tmp / ".s3cfg"
        _write_s3cfg(default, host_base="default.example")
        kopah = home_tmp / ".s3cfg-kopah"
        _write_s3cfg(kopah, host_base="kopah.example")

        c_default = s3_client.client_for("default")
        c_kopah = s3_client.client_for("kopah")
        assert c_default is not c_kopah
        assert c_default.meta.endpoint_url == "https://default.example"
        assert c_kopah.meta.endpoint_url == "https://kopah.example"

    def test_none_is_canonicalised_to_default(self, home_tmp):
        path = home_tmp / ".s3cfg"
        _write_s3cfg(path, host_base="default.example")

        c_none = s3_client.client_for(None)
        c_default = s3_client.client_for("default")
        assert c_none is c_default

    def test_reset_clears_cache(self, home_tmp):
        path = home_tmp / ".s3cfg"
        _write_s3cfg(path, host_base="default.example")

        c1 = s3_client.client_for("default")
        s3_client._reset_client_cache()
        c2 = s3_client.client_for("default")
        assert c1 is not c2
