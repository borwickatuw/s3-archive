"""Build a boto3 S3 client for any S3-compatible endpoint, optionally per-profile.

Two layers:

- :func:`load_client` resolves credentials for a single named profile
  and returns a fresh boto3 client. The **default** profile's chain is:

  1. ``$S3CMD_CONFIG`` — explicit path to an s3cmd INI file.
  2. ``~/.s3cfg`` — s3cmd's default config location.
  3. boto3's default credential chain: ``~/.aws/credentials``,
     ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars,
     IAM role, AWS SSO, etc. The endpoint comes from
     ``$S3_ENDPOINT_URL`` if set, otherwise AWS S3's default.

  For any **named** profile (e.g. ``kopah``) the chain is reduced to a
  single location: ``~/.s3cfg-<name>``. ``$S3CMD_CONFIG`` is *not*
  consulted — it is part of the default-profile chain only — and there
  is no boto3 fallback. A missing ``~/.s3cfg-<name>`` raises
  :class:`ConfigError` with a hint that points the operator at
  ``s3-archive config --profile <name>``.

- :func:`client_for` is a cache in front of :func:`load_client` keyed
  by profile name. The CLI calls ``client_for(profile)`` once per
  endpoint per invocation; multiple references to the same profile
  share one boto3 client (and one connection pool). ``None`` is
  canonicalised to ``"default"`` at the lookup layer so callers don't
  have to know whether the URL carried an explicit prefix.

The boto3 config sets ``request_checksum_calculation="when_required"``
unconditionally: it is required for Ceph RadosGW (which rejects the
default SigV4 content-SHA256 handling with ``XAmzContentSHA256Mismatch``)
and harmless on AWS S3.

Test seam: :func:`_reset_client_cache` clears the module-level cache.
Each repo's ``tests/conftest.py`` has an autouse fixture that calls it
between tests so the cache doesn't leak across mock-AWS contexts.
"""

import configparser
import os
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

from s3_archive.exceptions import ConfigError

_DEFAULT_POOL = 32
_DEFAULT_PROFILE = "default"


def _default_s3cfg_path() -> Path:
    """s3cmd's default config location (default profile only)."""
    return Path.home() / ".s3cfg"


def _profile_s3cfg_path(profile: str) -> Path:
    """The s3cmd INI location for a *named* profile (``~/.s3cfg-<name>``)."""
    return Path.home() / f".s3cfg-{profile}"


def _from_s3cmd_config(cfg_path: str) -> tuple[str, str, str]:
    """Parse an s3cmd INI file into (access_key, secret_key, endpoint_url)."""
    if not Path(cfg_path).exists():
        raise ConfigError(f"s3cmd config path does not exist: {cfg_path}")
    parser = configparser.ConfigParser()
    parser.read(cfg_path)
    if "default" not in parser:
        raise ConfigError(f"{cfg_path}: missing [default] section")
    section = parser["default"]
    for key in ("access_key", "secret_key", "host_base"):
        if not section.get(key):
            raise ConfigError(f"{cfg_path}: [default] missing required key {key!r}")
    return section["access_key"], section["secret_key"], f"https://{section['host_base']}"


def _boto_config(max_pool_connections: int) -> BotoConfig:
    return BotoConfig(
        request_checksum_calculation="when_required",
        max_pool_connections=max_pool_connections,
    )


def build_client(
    *,
    access_key: str | None = None,
    secret_key: str | None = None,
    endpoint_url: str | None = None,
    max_pool_connections: int = _DEFAULT_POOL,
):
    """Construct a boto3 S3 client directly from explicit values.

    Used by :func:`load_client` after it resolves credentials from
    ``$S3CMD_CONFIG`` / ``~/.s3cfg`` / default chain. Pass
    ``access_key=None`` to let boto3 find creds via its default chain.
    """
    boto_cfg = _boto_config(max_pool_connections)
    if access_key:
        return boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            config=boto_cfg,
        )
    session = boto3.Session()
    return session.client("s3", endpoint_url=endpoint_url, config=boto_cfg)


def _load_default_profile(max_pool_connections: int):
    """Default-profile resolution (the unchanged historical chain)."""
    explicit_cfg = os.environ.get("S3CMD_CONFIG", "").strip()
    if explicit_cfg:
        access, secret, endpoint = _from_s3cmd_config(explicit_cfg)
        return build_client(
            access_key=access,
            secret_key=secret,
            endpoint_url=endpoint,
            max_pool_connections=max_pool_connections,
        )

    default_cfg = _default_s3cfg_path()
    if default_cfg.exists():
        access, secret, endpoint = _from_s3cmd_config(str(default_cfg))
        return build_client(
            access_key=access,
            secret_key=secret,
            endpoint_url=endpoint,
            max_pool_connections=max_pool_connections,
        )

    # Fall through to boto3's default credential chain. Fail fast if
    # nothing is configured — otherwise the first API call would raise
    # NoCredentialsError much later with a less clear message.
    session = boto3.Session()
    if session.get_credentials() is None:
        raise ConfigError(
            "No S3 credentials configured. Set one of:\n"
            f"  • {default_cfg} (s3cmd-style INI — includes endpoint, recommended\n"
            "    for non-AWS endpoints like Kopah, MinIO, DigitalOcean Spaces)\n"
            "  • S3CMD_CONFIG=/path/to/.s3cfg (override for a non-default location)\n"
            "  • AWS credentials (~/.aws/credentials, AWS_ACCESS_KEY_ID +\n"
            "    AWS_SECRET_ACCESS_KEY env vars, IAM role, AWS SSO, etc.)\n"
            "For a non-AWS endpoint with AWS-style credentials, also set\n"
            "$S3_ENDPOINT_URL (e.g. https://s3.kopah.uw.edu).\n"
            "See .env.example for details."
        )
    endpoint_url = os.environ.get("S3_ENDPOINT_URL", "").strip() or None
    return build_client(endpoint_url=endpoint_url, max_pool_connections=max_pool_connections)


def _load_named_profile(profile: str, max_pool_connections: int):
    """Named-profile resolution — reads ``~/.s3cfg-<name>`` exclusively.

    No ``$S3CMD_CONFIG`` fallback, no boto3 default chain. The reduced
    chain is intentional: it makes profile semantics auditable (one
    file per profile) and keeps an unrelated ``$S3CMD_CONFIG`` from
    accidentally hijacking a named profile.
    """
    cfg_path = _profile_s3cfg_path(profile)
    if not cfg_path.exists():
        raise ConfigError(
            f"Profile {profile!r}: {cfg_path} does not exist. "
            f"Run `s3-archive config --profile {profile}`."
        )
    access, secret, endpoint = _from_s3cmd_config(str(cfg_path))
    return build_client(
        access_key=access,
        secret_key=secret,
        endpoint_url=endpoint,
        max_pool_connections=max_pool_connections,
    )


def load_client(profile: str = _DEFAULT_PROFILE, max_pool_connections: int = _DEFAULT_POOL):
    """Return a configured boto3 S3 client for *profile*.

    See the module docstring for the resolution order in each branch.
    Callers that don't care about profiles should use
    :func:`client_for` (which adds a per-process cache) or pass
    ``profile="default"`` here for one-shot use.
    """
    if profile == _DEFAULT_PROFILE:
        return _load_default_profile(max_pool_connections)
    return _load_named_profile(profile, max_pool_connections)


# Module-level cache: one boto3 client per profile, per process. The
# CLI dispatches a single ``client_for(profile)`` call per endpoint per
# invocation; repeat lookups for the same profile reuse the cached
# client (and its connection pool).
_client_cache: dict[str, object] = {}


def client_for(profile: str | None):
    """Return the cached client for *profile*, building one on first use.

    ``profile=None`` is canonicalised to ``"default"`` so callers don't
    have to know whether the URL carried an explicit profile prefix.
    Cache misses go through :func:`load_client`; cache hits return the
    same object identity, which keeps a single connection pool per
    endpoint.
    """
    key = profile if profile is not None else _DEFAULT_PROFILE
    cached = _client_cache.get(key)
    if cached is not None:
        return cached
    client = load_client(key)
    _client_cache[key] = client
    return client


def _reset_client_cache() -> None:
    """Clear the module-level client cache.

    Test seam: ``tests/conftest.py`` calls this between tests so a
    cached client built against one moto context doesn't leak into the
    next. Not part of the public API.
    """
    _client_cache.clear()
