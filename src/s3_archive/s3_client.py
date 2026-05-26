"""Build a boto3 S3 client for any S3-compatible endpoint.

Credential sources, checked in order:

1. ``$S3CMD_CONFIG`` — explicit path to an s3cmd INI file.
2. ``~/.s3cfg`` — s3cmd's default config location.
3. boto3's default credential chain: ``~/.aws/credentials``,
   ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars,
   IAM role, AWS SSO, etc.

The s3cmd paths (1, 2) read both the credentials AND the endpoint
from the file's ``[default]`` section. They take precedence because
operators with an s3cmd setup against a non-AWS endpoint expect that
configuration to be honored without extra env vars.

For path 3 (boto3 default chain), the endpoint comes from
``$S3_ENDPOINT_URL`` if set, otherwise AWS S3's default.

The boto3 config sets ``request_checksum_calculation="when_required"``
unconditionally: it is required for Ceph RadosGW (which rejects the
default SigV4 content-SHA256 handling with ``XAmzContentSHA256Mismatch``)
and harmless on AWS S3.
"""

import configparser
import os
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

from s3_archive.exceptions import ConfigError

_DEFAULT_POOL = 32


def _default_s3cfg_path() -> Path:
    """s3cmd's default config location."""
    return Path.home() / ".s3cfg"


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


def load_client(max_pool_connections: int = _DEFAULT_POOL):
    """Return a configured boto3 S3 client.

    See module docstring for the resolution order.
    """
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
