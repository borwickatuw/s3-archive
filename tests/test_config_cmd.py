"""Tests for the interactive `s3-archive config` subcommand.

The production prompts go through `questionary`, which talks directly to
the controlling TTY. Tests patch the thin `_ask_text` / `_ask_password`
/ `_ask_confirm` helpers in `config_cmd` so we can script answers and
make assertions on the prompt text without spinning up a fake TTY.
"""

import configparser
from pathlib import Path

import pytest

from s3_archive import config_cmd
from s3_archive.exceptions import ConfigError


@pytest.fixture
def cfg_path(tmp_path, monkeypatch):
    # Point HOME at tmp_path so the early-detect check for an existing
    # `~/.s3cfg` never sees the developer's real config. We deliberately
    # pick a *non*-`.s3cfg` filename so writing to `cfg_path` doesn't
    # collide with the default-path early-detect check; tests that
    # exercise the early-detect prompt explicitly create `tmp_path / ".s3cfg"`.
    monkeypatch.setenv("HOME", str(tmp_path))
    # Clear $S3CMD_CONFIG so a developer's environment doesn't bleed in —
    # the early-detect prefers it over ~/.s3cfg.
    monkeypatch.delenv("S3CMD_CONFIG", raising=False)
    return tmp_path / "custom-s3cfg"


def _drive_prompts(
    monkeypatch,
    *,
    text: list[str] = (),
    passwords: list[str] = (),
    confirms: list[bool] = (),
) -> None:
    """Patch the three `_ask_*` helpers to consume scripted answers.

    Each fake echoes the question to stdout the way a real questionary
    prompt would render it, so capsys-based assertions on the prompt
    text (e.g. "already exists") continue to work.
    """
    text_iter = iter(text)
    password_iter = iter(passwords)
    confirm_iter = iter(confirms)

    def fake_text(question: str, default: str = "") -> str:
        print(question)
        return next(text_iter)

    def fake_password(question: str) -> str:
        print(question)
        return next(password_iter)

    def fake_confirm(question: str, *, default: bool = False) -> bool:
        print(question)
        return next(confirm_iter)

    monkeypatch.setattr(config_cmd, "_ask_text", fake_text)
    monkeypatch.setattr(config_cmd, "_ask_password", fake_password)
    monkeypatch.setattr(config_cmd, "_ask_confirm", fake_confirm)


def _disable_smoke(monkeypatch) -> None:
    """The pre-write connection check reaches into boto3; bypass it for unit tests.

    Returning ``None`` from ``_try_connect`` means "connection OK", so
    the prompt loop exits on the first try without asking "retry?".
    """
    monkeypatch.setattr(
        config_cmd,
        "_try_connect",
        lambda *_a, **_kw: None,
    )


class TestValidateProfileName:
    @pytest.mark.parametrize(
        "name",
        ["default", "kopah", "aws-prsv", "team_a", "a", "ABC123", "x-y_z"],
    )
    def test_accepts_valid(self, name):
        assert config_cmd.validate_profile_name(name) == name

    @pytest.mark.parametrize(
        "name",
        ["", "bad name", "with/slash", "with.dot", "ééé", "has space"],
    )
    def test_rejects_invalid(self, name):
        with pytest.raises(ConfigError, match="Invalid profile name"):
            config_cmd.validate_profile_name(name)


class TestDefaultPathForProfile:
    def test_default_maps_to_s3cfg(self):
        assert config_cmd._default_path_for("default") == "~/.s3cfg"

    @pytest.mark.parametrize(
        "profile,expected",
        [
            ("kopah", "~/.s3cfg-kopah"),
            ("aws-prsv", "~/.s3cfg-aws-prsv"),
            ("team_a", "~/.s3cfg-team_a"),
        ],
    )
    def test_named_profile(self, profile, expected):
        assert config_cmd._default_path_for(profile) == expected


class TestCanonicalConfigPath:
    def test_default_honors_s3cmd_config(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        explicit = tmp_path / "explicit.cfg"
        monkeypatch.setenv("S3CMD_CONFIG", str(explicit))

        assert config_cmd._canonical_config_path("default") == explicit

    def test_named_profile_ignores_s3cmd_config(self, tmp_path, monkeypatch):
        # The plan calls this out specifically: $S3CMD_CONFIG must NOT
        # apply when the operator names a profile. The named-profile
        # branch always reads/writes ~/.s3cfg-<name>.
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("S3CMD_CONFIG", "/tmp/bogus.cfg")  # noqa: S108 - test fixture path

        result = config_cmd._canonical_config_path("kopah")
        assert result == (tmp_path / ".s3cfg-kopah").absolute()


class TestRunConfigHappyPath:
    def test_writes_full_s3cmd_ini_with_endpoint(self, monkeypatch, cfg_path, capsys):
        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "ACCESS123",
                str(cfg_path),
            ],
            passwords=["SECRET456"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        assert cfg_path.exists()
        parser = configparser.ConfigParser()
        parser.read(cfg_path)
        assert parser["default"]["access_key"] == "ACCESS123"
        assert parser["default"]["secret_key"] == "SECRET456"
        assert parser["default"]["host_base"] == "s3.kopah.uw.edu"

    def test_blank_endpoint_omits_host_base(self, monkeypatch, cfg_path, capsys):
        _drive_prompts(
            monkeypatch,
            text=[
                "",  # endpoint left blank → AWS S3
                "ACCESS123",
                str(cfg_path),
            ],
            passwords=["SECRET456"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        parser = configparser.ConfigParser()
        parser.read(cfg_path)
        assert "host_base" not in parser["default"]
        out = capsys.readouterr().out
        assert "AWS S3 defaults" in out


class TestRunConfigProfileFlag:
    """`--profile NAME` selects ~/.s3cfg-NAME for both reads and writes."""

    @pytest.mark.parametrize("profile", ["default", "kopah"])
    def test_target_path_default_per_profile(self, profile, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("S3CMD_CONFIG", raising=False)
        expected = tmp_path / ".s3cfg" if profile == "default" else tmp_path / f".s3cfg-{profile}"

        # Accept the prefilled path so we land on the canonical location.
        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "ACCESS",
                str(expected),
            ],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config(profile=profile)

        assert rc == 0
        assert expected.exists()
        parser = configparser.ConfigParser()
        parser.read(expected)
        assert parser["default"]["access_key"] == "ACCESS"

    def test_named_profile_ignores_s3cmd_config(self, tmp_path, monkeypatch, capsys):
        """`$S3CMD_CONFIG=/tmp/bogus` does not influence the kopah profile."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("S3CMD_CONFIG", "/tmp/bogus.cfg")  # noqa: S108 - test fixture path
        expected = tmp_path / ".s3cfg-kopah"

        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "ACCESS",
                str(expected),
            ],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config(profile="kopah")

        assert rc == 0
        assert expected.exists()
        # Nothing was written at the bogus $S3CMD_CONFIG path.
        assert not Path("/tmp/bogus.cfg").exists()  # noqa: S108

    def test_invalid_profile_raises_early(self):
        with pytest.raises(ConfigError, match="Invalid profile name"):
            config_cmd.run_config(profile="bad name")


class TestToolNameInPrompt:
    def test_default_tool_name_is_s3_archive(self, monkeypatch, cfg_path, capsys):
        _drive_prompts(
            monkeypatch,
            text=["", "ACCESS", str(cfg_path)],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        config_cmd.run_config()
        out = capsys.readouterr().out
        assert "Configure S3 credentials for s3-archive" in out

    def test_tool_name_override(self, monkeypatch, cfg_path, capsys):
        _drive_prompts(
            monkeypatch,
            text=["", "ACCESS", str(cfg_path)],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        config_cmd.run_config(tool_name="s3-bagit")
        out = capsys.readouterr().out
        assert "Configure S3 credentials for s3-bagit" in out


class TestRunConfigOverwriteGuard:
    def test_existing_file_overwrite_declined(self, monkeypatch, cfg_path, capsys):
        cfg_path.write_text("# original content\n")
        original_mtime = cfg_path.stat().st_mtime

        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "ACCESS",
                str(cfg_path),
            ],
            passwords=["SECRET"],
            confirms=[False],  # declines the overwrite prompt
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        # File untouched.
        assert cfg_path.read_text() == "# original content\n"
        assert cfg_path.stat().st_mtime == original_mtime
        assert "Aborted" in capsys.readouterr().out

    def test_existing_file_overwrite_accepted(self, monkeypatch, cfg_path):
        cfg_path.write_text("# original content\n")
        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "ACCESS",
                str(cfg_path),
            ],
            passwords=["SECRET"],
            confirms=[True],  # accepts overwrite
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        parser = configparser.ConfigParser()
        parser.read(cfg_path)
        assert parser["default"]["access_key"] == "ACCESS"


class TestEndpointToHostBase:
    @pytest.mark.parametrize(
        "endpoint,expected",
        [
            ("https://s3.kopah.uw.edu", "s3.kopah.uw.edu"),
            ("http://localhost:9000", "localhost:9000"),
            ("s3.kopah.uw.edu", "s3.kopah.uw.edu"),
            ("https://s3.kopah.uw.edu/", "s3.kopah.uw.edu"),
        ],
    )
    def test_strips_scheme_and_path(self, endpoint, expected):
        assert config_cmd._endpoint_to_host_base(endpoint) == expected


class TestEarlyDetectExistingConfig:
    """The fast-path: detect an existing ~/.s3cfg before prompting for anything."""

    def test_decline_keeps_existing_file(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("HOME", str(tmp_path))
        default = tmp_path / ".s3cfg"
        default.write_text(
            "[default]\naccess_key = OLD\nsecret_key = OLD\nhost_base = s3.kopah.uw.edu\n"
        )
        original_mtime = default.stat().st_mtime

        _drive_prompts(monkeypatch, confirms=[False])
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        assert default.read_text().startswith("[default]")
        assert default.stat().st_mtime == original_mtime
        out = capsys.readouterr().out
        assert "already exists" in out
        assert "s3.kopah.uw.edu" in out  # URL hint is shown.
        assert "Keeping existing configuration" in out

    def test_decline_when_host_base_missing(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("HOME", str(tmp_path))
        default = tmp_path / ".s3cfg"
        # Malformed-but-parseable INI: no [default] section.
        default.write_text("# half-broken file\n")

        _drive_prompts(monkeypatch, confirms=[False])
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        out = capsys.readouterr().out
        assert "already exists" in out
        # No URL claim made when we couldn't read host_base.
        assert "pointing to" not in out

    def test_early_detect_for_named_profile_reads_s3cfg_dash_name(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        profile_cfg = tmp_path / ".s3cfg-kopah"
        profile_cfg.write_text(
            "[default]\naccess_key = OLD\nsecret_key = OLD\nhost_base = kopah.example\n"
        )

        _drive_prompts(monkeypatch, confirms=[False])
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config(profile="kopah")

        assert rc == 0
        out = capsys.readouterr().out
        assert "already exists" in out
        assert "kopah.example" in out


class TestEarlyDetectRespectsS3CMDConfig:
    """The early-detect path mirrors s3_client.load_client's precedence (default profile only)."""

    def test_existing_s3cmd_config_takes_precedence_over_default(
        self, tmp_path, monkeypatch, capsys
    ):
        # Both files exist; $S3CMD_CONFIG should win.
        monkeypatch.setenv("HOME", str(tmp_path))
        default = tmp_path / ".s3cfg"
        default.write_text(
            "[default]\naccess_key = OLD\nsecret_key = OLD\nhost_base = default.example\n"
        )
        explicit = tmp_path / "explicit.cfg"
        explicit.write_text(
            "[default]\naccess_key = EX\nsecret_key = EX\nhost_base = explicit.example\n"
        )
        monkeypatch.setenv("S3CMD_CONFIG", str(explicit))

        _drive_prompts(monkeypatch, confirms=[False])  # decline early-detect
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        out = capsys.readouterr().out
        # The explicit path (and its host) is what's surfaced, not the default.
        assert str(explicit) in out
        assert "explicit.example" in out
        assert "default.example" not in out

    def test_missing_s3cmd_config_skips_early_detect(self, tmp_path, monkeypatch, capsys):
        # $S3CMD_CONFIG set but file doesn't exist: operator wants to write
        # a new config there. No early-detect; prompts proceed.
        monkeypatch.setenv("HOME", str(tmp_path))
        target = tmp_path / "tmp-bobo"
        monkeypatch.setenv("S3CMD_CONFIG", str(target))
        assert not target.exists()

        # Even if ~/.s3cfg exists, the env-var path wins precedence and
        # the env-var path doesn't exist — so no early-detect prompt.
        (tmp_path / ".s3cfg").write_text(
            "[default]\naccess_key = X\nsecret_key = X\nhost_base = nope\n"
        )

        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "ACCESS",
                str(target),  # accept the prefilled path
            ],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        assert target.exists()
        out = capsys.readouterr().out
        # No "already exists" prompt fired — the env-var path was missing.
        assert "already exists" not in out

    def test_env_var_path_is_prompt_default(self, tmp_path, monkeypatch):
        """The 'Config file path' prompt prefills $S3CMD_CONFIG when set."""
        monkeypatch.setenv("HOME", str(tmp_path))
        target = tmp_path / "tmp-bobo"
        monkeypatch.setenv("S3CMD_CONFIG", str(target))

        seen_defaults: list[str] = []
        text_iter = iter(["https://s3.example", "ACCESS", str(target)])
        password_iter = iter(["SECRET"])

        def fake_text(question, default=""):
            seen_defaults.append(default)
            return next(text_iter)

        def fake_password(question):
            return next(password_iter)

        def fake_confirm(question, *, default=False):
            return True  # unused (no overwrite case)

        monkeypatch.setattr(config_cmd, "_ask_text", fake_text)
        monkeypatch.setattr(config_cmd, "_ask_password", fake_password)
        monkeypatch.setattr(config_cmd, "_ask_confirm", fake_confirm)
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        # The third _ask_text call is for the config path. Its default
        # must be the $S3CMD_CONFIG value, not ~/.s3cfg.
        assert seen_defaults[2] == str(target)


class TestValidateEndpoint:
    @pytest.mark.parametrize(
        "endpoint",
        [
            "",  # blank = AWS defaults
            "https://s3.kopah.uw.edu",
            "http://localhost:9000",
            "s3.kopah.uw.edu",
            "localhost:9000",
        ],
    )
    def test_accepts_valid(self, endpoint):
        assert config_cmd._validate_endpoint(endpoint) is None

    @pytest.mark.parametrize(
        "endpoint,token",
        [
            ("blark dar dar", "spaces"),
            ("https:// s3.kopah.uw.edu", "spaces"),
            ("https://", "scheme but no host"),
            ("http://", "scheme but no host"),
        ],
    )
    def test_rejects_invalid(self, endpoint, token):
        msg = config_cmd._validate_endpoint(endpoint)
        assert msg is not None
        assert token in msg


class TestRunConfigEndpointRetries:
    def test_invalid_endpoint_reprompts_until_valid(self, monkeypatch, cfg_path, capsys):
        _drive_prompts(
            monkeypatch,
            text=[
                "blark dar dar",  # rejected
                "https://",  # rejected
                "https://s3.kopah.uw.edu",  # accepted
                "ACCESS",
                str(cfg_path),
            ],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        out = capsys.readouterr().out
        assert "Endpoint must not contain spaces" in out
        assert "scheme but no host" in out
        parser = configparser.ConfigParser()
        parser.read(cfg_path)
        assert parser["default"]["host_base"] == "s3.kopah.uw.edu"


class TestTryConnectSwallowsExceptions:
    """``_try_connect`` must never raise; it returns an error string instead."""

    def test_value_error_from_boto3_returned_as_string(self, monkeypatch):
        # A `host_base` with a space crashes boto3 with ValueError. The
        # helper must catch and surface it as a one-line string so the
        # interactive loop can show it and prompt for retry.
        for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE"):
            monkeypatch.delenv(k, raising=False)

        result = config_cmd._try_connect(
            access_key="X",
            secret_key="X",
            host_base="blark dar dar",
        )

        assert result is not None
        assert "ValueError" in result or "Invalid endpoint" in result


class TestPreWriteConnectionTest:
    """The new flow: connection test happens BEFORE writing the file."""

    def test_failed_then_successful_retry_writes_second_values(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("S3CMD_CONFIG", raising=False)
        target = tmp_path / "second-attempt.cfg"

        # First call fails; second succeeds. Each call consumes one item.
        connect_results = iter(["EndpointConnectionError: nope", None])
        monkeypatch.setattr(
            config_cmd,
            "_try_connect",
            lambda *_a, **_kw: next(connect_results),
        )

        _drive_prompts(
            monkeypatch,
            text=[
                "https://broken.example",  # attempt 1 — fails
                "ACCESS1",
                "https://good.example",  # attempt 2 — succeeds
                "ACCESS2",
                str(target),  # path
            ],
            passwords=["SECRET1", "SECRET2"],
            confirms=[True],  # "Try again with different credentials?" → yes
        )

        rc = config_cmd.run_config()

        assert rc == 0
        # File was only written after the SECOND (successful) attempt,
        # and it carries the second set of values.
        assert target.exists()
        parser = configparser.ConfigParser()
        parser.read(target)
        assert parser["default"]["access_key"] == "ACCESS2"
        assert parser["default"]["host_base"] == "good.example"
        out = capsys.readouterr().out
        assert "FAILED" in out
        assert "Configured." in out

    def test_save_anyway_writes_broken_config(self, tmp_path, monkeypatch, capsys):
        """Offline-configuration case: operator explicitly opts in to saving broken values."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("S3CMD_CONFIG", raising=False)
        target = tmp_path / "offline.cfg"

        monkeypatch.setattr(
            config_cmd,
            "_try_connect",
            lambda *_a, **_kw: "EndpointConnectionError: not on the network",
        )

        _drive_prompts(
            monkeypatch,
            text=[
                "https://future.example",
                "ACCESS",
                str(target),
            ],
            passwords=["SECRET"],
            confirms=[
                False,  # "Try again?" → no
                True,  # "Save these values anyway?" → yes, explicit opt-in
            ],
        )

        rc = config_cmd.run_config()

        assert rc == 0
        assert target.exists()
        out = capsys.readouterr().out
        assert "Saving the values anyway." in out
        assert "Configured." in out

    def test_decline_both_prompts_exits_without_writing(self, tmp_path, monkeypatch, capsys):
        """The safe default: declining both 'try again' and 'save anyway' cancels cleanly."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("S3CMD_CONFIG", raising=False)
        target = tmp_path / "should-not-exist.cfg"

        monkeypatch.setattr(
            config_cmd,
            "_try_connect",
            lambda *_a, **_kw: "EndpointConnectionError: no",
        )

        _drive_prompts(
            monkeypatch,
            text=[
                "https://broken.example",
                "ACCESS",
            ],
            passwords=["SECRET"],
            confirms=[
                False,  # "Try again?" → no
                False,  # "Save anyway?" → no, exit
            ],
        )

        rc = config_cmd.run_config()

        assert rc == 0
        assert not target.exists()
        out = capsys.readouterr().out
        assert "Cancelled; no changes written." in out
        assert "Configured." not in out


class TestSummaryOrdering:
    """End-of-run summary structure: 'Configured.' then optional hints."""

    def test_non_default_path_note_appears_after_configured_line(
        self, tmp_path, monkeypatch, capsys
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("S3CMD_CONFIG", raising=False)
        # Non-default path so the note fires.
        target = tmp_path / "non-default.cfg"

        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "ACCESS",
                str(target),
            ],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()

        assert rc == 0
        out = capsys.readouterr().out
        # The non-default-path note must come AFTER the "Configured." summary.
        configured_idx = out.find("Configured.")
        hint_idx = out.find("not the default location")
        assert configured_idx != -1
        assert hint_idx != -1
        # And it must not include the old shell-snippets we removed.
        assert "export S3CMD_CONFIG=" not in out
        assert "$env:S3CMD_CONFIG" not in out
        assert hint_idx > configured_idx


class TestResolvePathDoesNotFollowSymlinks:
    def test_symlink_displayed_as_typed(self, tmp_path):
        # ~/.s3cfg → /some/real/file is the surprising case the operator hit.
        real = tmp_path / "real-config.cfg"
        real.write_text("real")
        link = tmp_path / "symlinked-config"
        link.symlink_to(real)

        resolved = config_cmd._resolve_path(str(link))

        # We preserve the path the operator typed; we do NOT show them
        # /Users/.../some-other-place that the link happens to target.
        assert resolved == link
        assert resolved != real


class TestReprompts:
    def test_blank_access_key_reprompts(self, monkeypatch, cfg_path):
        _drive_prompts(
            monkeypatch,
            text=[
                "https://s3.kopah.uw.edu",
                "",  # first access-key attempt: blank, reprompts
                "ACCESS",
                str(cfg_path),
            ],
            passwords=["SECRET"],
        )
        _disable_smoke(monkeypatch)

        rc = config_cmd.run_config()
        assert rc == 0
        parser = configparser.ConfigParser()
        parser.read(cfg_path)
        assert parser["default"]["access_key"] == "ACCESS"


class TestCtrlCSurfacesKeyboardInterrupt:
    """questionary returns None on Ctrl-C — the helpers must raise KeyboardInterrupt."""

    def test_text_helper_raises(self, monkeypatch):
        monkeypatch.setattr(
            config_cmd.questionary,
            "text",
            lambda *_a, **_kw: _StubAsk(None),
        )
        with pytest.raises(KeyboardInterrupt):
            config_cmd._ask_text("anything")

    def test_password_helper_raises(self, monkeypatch):
        monkeypatch.setattr(
            config_cmd.questionary,
            "password",
            lambda *_a, **_kw: _StubAsk(None),
        )
        with pytest.raises(KeyboardInterrupt):
            config_cmd._ask_password("anything")

    def test_confirm_helper_raises(self, monkeypatch):
        monkeypatch.setattr(
            config_cmd.questionary,
            "confirm",
            lambda *_a, **_kw: _StubAsk(None),
        )
        with pytest.raises(KeyboardInterrupt):
            config_cmd._ask_confirm("anything")


class _StubAsk:
    """Mimic the `questionary.Question` interface our helpers consume."""

    def __init__(self, value):
        self._value = value

    def ask(self):
        return self._value
