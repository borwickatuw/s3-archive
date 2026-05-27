"""Interactive ``s3-archive config`` subcommand.

Prompts for endpoint, access key, and secret key, then writes an
s3cmd-compatible INI file. The same ``[default]`` section is what
:func:`s3_archive.s3_client._from_s3cmd_config` already consumes — so
running ``s3-archive config`` is enough to bootstrap the tool on a
fresh workstation without installing s3cmd.

When invoked with ``--profile NAME`` the file is written to
``~/.s3cfg-<name>`` instead of ``~/.s3cfg``. Profile names are
restricted to ``[A-Za-z0-9_-]+``. The named-profile branch deliberately
ignores ``$S3CMD_CONFIG`` — that env var is part of the **default**
profile's resolution chain only.

Uses `questionary <https://github.com/tmbo/questionary>`_ for prompts so
the experience is consistent across terminals (arrow-key Y/N, masked
password entry, clean Ctrl-C cancellation).

This module is also reused by `s3-bagit`'s ``config`` subcommand via a
thin shim that passes ``tool_name="s3-bagit"`` — the prompt text reads
"Configure S3 credentials for {tool_name}." but the on-disk format and
flow are identical.
"""

import configparser
import contextlib
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import questionary
from botocore.exceptions import BotoCoreError, ClientError

from s3_archive.exceptions import ConfigError
from s3_archive.log_config import get_logger
from s3_archive.s3_client import build_client

log = get_logger(__name__)

_DEFAULT_PROFILE = "default"
_PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def validate_profile_name(name: str) -> str:
    """Return *name* unchanged if it matches the profile grammar; else raise.

    Used by :mod:`s3_archive.cli` as ``argparse type=`` so a malformed
    ``--profile`` aborts before any prompt fires.
    """
    if not _PROFILE_NAME_RE.match(name):
        raise ConfigError(
            f"Invalid profile name {name!r}: must match [A-Za-z0-9_-]+ "
            f"(letters, digits, underscore, hyphen)."
        )
    return name


def _default_path_for(profile: str) -> str:
    """The s3cmd INI path a given profile writes to / reads from.

    The ``default`` profile maps to ``~/.s3cfg`` to stay compatible with
    s3cmd; any other profile maps to ``~/.s3cfg-<name>``.
    """
    if profile == _DEFAULT_PROFILE:
        return "~/.s3cfg"
    return f"~/.s3cfg-{profile}"


def _ask_text(question: str, default: str = "") -> str:
    """Ask for free-text input. Ctrl-C surfaces as ``KeyboardInterrupt``."""
    answer = questionary.text(question, default=default).ask()
    if answer is None:
        raise KeyboardInterrupt
    return answer.strip()


def _ask_password(question: str) -> str:
    """Ask for a hidden-input secret. Ctrl-C surfaces as ``KeyboardInterrupt``."""
    answer = questionary.password(question).ask()
    if answer is None:
        raise KeyboardInterrupt
    return answer.strip()


def _ask_confirm(question: str, *, default: bool = False) -> bool:
    """Ask a Y/N question. Ctrl-C surfaces as ``KeyboardInterrupt``."""
    answer = questionary.confirm(question, default=default).ask()
    if answer is None:
        raise KeyboardInterrupt
    return answer


def _endpoint_to_host_base(endpoint: str) -> str:
    """Strip scheme/path off *endpoint* — s3cmd's ``host_base`` is host[:port] only."""
    parsed = urlparse(endpoint)
    if parsed.netloc:
        return parsed.netloc
    # User typed bare host like "s3.kopah.uw.edu" — urlparse puts it in path.
    return parsed.path


_URL_SCHEMES = {"http", "https"}


def _validate_endpoint(endpoint: str) -> str | None:
    """Return a one-line operator-facing error message, or ``None`` if OK.

    Blank input is allowed (= AWS S3 defaults). For non-blank input we
    require: no whitespace, and if an http/https scheme is given it
    must be followed by a host. We don't try to validate the host itself
    — boto3 will surface real-world failures (DNS, TLS) later, and the
    goal here is to catch the fat-finger cases ("blark dar dar") that
    otherwise produce a confusing
    ``ValueError: Invalid endpoint: https://blark dar dar`` crash from
    inside the smoke test.

    We only flag missing-host when the scheme is in ``_URL_SCHEMES``;
    ``urlparse`` parses bare ``host:port`` strings like ``localhost:9000``
    with ``scheme="localhost"``, which is fine for our purposes (not a
    URL scheme we recognise).
    """
    if not endpoint:
        return None
    if any(c.isspace() for c in endpoint):
        return "Endpoint must not contain spaces."
    parsed = urlparse(endpoint)
    if parsed.scheme in _URL_SCHEMES and not parsed.netloc:
        return f"Endpoint {endpoint!r} has a scheme but no host."
    return None


def _resolve_path(raw: str) -> Path:
    """Expand ``~`` and make absolute, but do **not** follow symlinks.

    Earlier versions used :meth:`Path.resolve` which dereferenced symlinks
    — that made the path the tool displayed (e.g. an Ops-managed config
    target under ``/Users/foo/code/storage-scripts/secrets/x.cfg``)
    unfamiliar to operators who only know they have a ``~/.s3cfg``.
    Showing the path the operator typed is the friendlier default;
    ``Path.open``/``Path.exists`` follow the symlink at read/write time
    anyway, so behaviour is unchanged.
    """
    return Path(os.path.expanduser(raw)).absolute()


def _canonical_config_path(profile: str) -> Path:
    """Return the path the tool *would* use for *profile*.

    For the default profile, mirrors the runtime precedence in
    :func:`s3_archive.s3_client.load_client`: ``$S3CMD_CONFIG`` wins
    over ``~/.s3cfg``.

    For named profiles, ``$S3CMD_CONFIG`` is **deliberately ignored** —
    it is part of the default-profile chain only — and we always return
    ``~/.s3cfg-<name>``.
    """
    if profile == _DEFAULT_PROFILE:
        explicit = os.environ.get("S3CMD_CONFIG", "").strip()
        if explicit:
            return _resolve_path(explicit)
    return _resolve_path(_default_path_for(profile))


def _detect_existing_config(profile: str) -> Path | None:
    """The canonical config path iff it exists on disk; otherwise ``None``.

    A set-but-missing ``$S3CMD_CONFIG`` (default profile only) returns
    ``None`` — that's the operator declaring "write a new config at
    this path", and the early-detect prompt would be noise.
    """
    canonical = _canonical_config_path(profile)
    return canonical if canonical.exists() else None


def _read_existing_host_base(cfg_path: Path) -> str | None:
    """Best-effort: read ``host_base`` from an existing s3cmd INI.

    Returns ``None`` if the file isn't parseable as an s3cmd config or
    doesn't carry a ``host_base`` — both are fine, the caller still
    surfaces "config exists" to the operator.
    """
    try:
        parser = configparser.ConfigParser()
        parser.read(cfg_path, encoding="utf-8")
        return parser["default"].get("host_base") or None
    except (configparser.Error, KeyError, OSError, UnicodeDecodeError):
        return None


def _write_config(path: Path, *, access_key: str, secret_key: str, host_base: str) -> None:
    parser = configparser.ConfigParser()
    parser["default"] = {"access_key": access_key, "secret_key": secret_key}
    if host_base:
        parser["default"]["host_base"] = host_base
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        parser.write(fh)
    # s3cmd writes 0600 — credentials belong to the user. On platforms
    # where chmod is a no-op (Windows), the OSError is harmless.
    with contextlib.suppress(OSError):
        path.chmod(0o600)


def _try_connect(*, access_key: str, secret_key: str, host_base: str) -> str | None:
    """Run a ``list_buckets`` against the in-memory credentials.

    Returns ``None`` on success or a one-line error string on failure.
    Used *before* the config is written so a misconfigured set of values
    doesn't leave a broken file behind. boto3 raises ``ValueError`` for
    malformed endpoints; the rest of the tuple covers network and
    credential failures. We must never crash the CLI — this is
    diagnostic, not load-bearing.
    """
    endpoint_url = f"https://{host_base}" if host_base else None
    try:
        client = build_client(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
        )
        client.list_buckets()
    except (BotoCoreError, ClientError, OSError, ValueError) as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def _non_default_path_note(cfg_path: Path, tool_name: str, profile: str) -> None:
    """Tell the operator the implications of choosing a non-default path.

    We deliberately don't generate shell-specific snippets or try to
    write to rc files — that requires guessing the right rc file
    (.bashrc vs .zshrc vs $PROFILE) and risks duplicate entries on
    re-run. Instead we explain the situation and offer the easy escape
    (use the default path).
    """
    if profile == _DEFAULT_PROFILE:
        print(
            f"Note: {cfg_path} is not the default location (~/.s3cfg). "
            f"Future runs of {tool_name} will only find it if $S3CMD_CONFIG "
            f"is set in your shell environment.\n"
            f"If you want this config picked up automatically (no env-var "
            f"needed), re-run `{tool_name} config` and accept the default "
            f"path `~/.s3cfg`."
        )
    else:
        # Named profile written somewhere other than ~/.s3cfg-<name>:
        # the runtime resolver reads ~/.s3cfg-<name> exclusively, so the
        # operator has to symlink or re-run with the default path.
        default_for_profile = _resolve_path(_default_path_for(profile))
        print(
            f"Note: {cfg_path} is not the default location ({default_for_profile}) "
            f"for profile {profile!r}. The named-profile resolver in {tool_name} "
            f"reads only {default_for_profile}; it does not consult $S3CMD_CONFIG "
            f"for named profiles.\n"
            f"To have this config picked up automatically, re-run "
            f"`{tool_name} config --profile {profile}` and accept the default path."
        )


def run_config(*, tool_name: str = "s3-archive", profile: str = _DEFAULT_PROFILE) -> int:
    """Drive the interactive prompts; return the CLI exit code.

    *tool_name* is used only in operator-facing prompt text — the CLI
    that invokes this passes its own program name ("s3-archive" or
    "s3-bagit"). *profile* selects the target s3cmd file (default →
    ``~/.s3cfg``, otherwise ``~/.s3cfg-<name>``).
    """
    validate_profile_name(profile)

    if profile == _DEFAULT_PROFILE:
        print(f"Configure S3 credentials for {tool_name}.")
    else:
        print(f"Configure S3 credentials for {tool_name} (profile {profile!r}).")

    existing = _detect_existing_config(profile)
    if existing is not None:
        existing_host = _read_existing_host_base(existing)
        if existing_host:
            question = (
                f"An s3cmd config already exists at {existing} "
                f"pointing to {existing_host}. Replace it?"
            )
        else:
            question = f"An s3cmd config already exists at {existing}. Replace it?"
        if not _ask_confirm(question, default=False):
            print("Keeping existing configuration; no changes written.")
            return 0

    print(
        "These values will be tested with a list-buckets call before anything is written to disk."
    )
    print()
    # Gather inputs and test the connection BEFORE writing anything to
    # disk. A broken endpoint or expired key shouldn't leave a stale
    # config file behind that the operator then has to clean up.
    while True:
        while True:
            endpoint = _ask_text(
                "S3 endpoint URL (e.g. https://s3.kopah.uw.edu; blank for AWS S3)",
            )
            err = _validate_endpoint(endpoint)
            if err is None:
                break
            print(err)

        access_key = ""
        while not access_key:
            access_key = _ask_text("Access key")
            if not access_key:
                print("Access key is required.")
        secret_key = ""
        while not secret_key:
            secret_key = _ask_password("Secret key")
            if not secret_key:
                print("Secret key is required.")

        host_base = _endpoint_to_host_base(endpoint) if endpoint else ""

        print("Testing connection...", end=" ", flush=True)
        started = time.monotonic()
        error = _try_connect(access_key=access_key, secret_key=secret_key, host_base=host_base)
        elapsed = time.monotonic() - started
        if error is None:
            print(f"OK ({elapsed:.1f}s).")
            break
        print("FAILED.")
        print(f"  {error}")
        if _ask_confirm("Try again with different credentials?", default=True):
            continue
        # Decline-to-retry doesn't mean "save broken values" — that would
        # be a surprising default. Make the operator opt into saving
        # explicitly, with the safe choice (don't save) as the default.
        # The opt-in path covers the offline-configuration case: no
        # network, behind a firewall, configuring for a future endpoint.
        if not _ask_confirm("Save these values anyway (e.g. configuring offline)?", default=False):
            print("Cancelled; no changes written.")
            return 0
        print("Saving the values anyway.")
        break

    # Prefill the path prompt with the canonical path for this profile.
    path_default = str(_canonical_config_path(profile))
    raw_path = _ask_text("Config file path", default=path_default)
    cfg_path = _resolve_path(raw_path)

    if cfg_path.exists() and not _ask_confirm(
        f"File {cfg_path} already exists. Overwrite?", default=False
    ):
        print("Aborted; no changes written.")
        return 0

    _write_config(cfg_path, access_key=access_key, secret_key=secret_key, host_base=host_base)

    # End-of-run summary: one line confirming what was saved, then any
    # follow-up notes (AWS-defaults hint, non-default-path shell export).
    print(f"Configured. Settings saved to {cfg_path}.")
    if not host_base:
        print(
            f"(No endpoint set; using AWS S3 defaults. To target a non-AWS endpoint "
            f"later, edit `host_base` in the file or rerun `{tool_name} config`.)"
        )
    if cfg_path != _resolve_path(_default_path_for(profile)):
        _non_default_path_note(cfg_path, tool_name, profile)
    return 0
