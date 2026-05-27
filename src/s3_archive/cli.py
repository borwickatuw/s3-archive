"""``s3-archive`` command-line entry point.

Subcommands:

  * ``extract`` — stream an archive (tar / tar.gz / tar.bz2 /
    tar.xz / tar.zst / zip / 7z) out of S3 and upload each member to a
    destination S3 prefix.

  * ``create`` — stream the objects under an S3 prefix into a
    serialized archive (.tar.gz or .zip) at a destination S3 key.
    (.7z create is not supported — see :mod:`s3_archive.seven_z`.)

  * ``ls`` — stream-list an archive's members without extracting.

The CLI's job is to parse args, build the S3 client, dispatch, and
translate exceptions into clean stderr messages + exit codes. All real
work lives in the matching modules.
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from s3_archive import REPO_URL, __version__
from s3_archive.config_cmd import run_config
from s3_archive.config_cmd import validate_profile_name as _validate_profile_name
from s3_archive.create import create
from s3_archive.exceptions import ConfigError, UnsupportedArchiveFormatError
from s3_archive.extract import extract
from s3_archive.log_config import get_logger, setup_console
from s3_archive.ls import list_archive
from s3_archive.s3_client import load_client
from s3_archive.url import detect_format, parse_s3_prefix, parse_s3_url

log = get_logger(__name__)

# Exit codes.
_EXIT_OK = 0
_EXIT_ERROR = 1
_EXIT_CONFIG_ERROR = 2
# 128 + SIGINT(2) — the conventional POSIX exit code for "killed by Ctrl-C".
_EXIT_INTERRUPTED = 130


def _argparse_profile(value: str) -> str:
    """argparse `type=` for --profile that maps ConfigError → ArgumentTypeError.

    argparse only handles `ArgumentTypeError` / `ValueError` / `TypeError`
    from a `type=` callable cleanly; anything else surfaces as a
    traceback. We catch the library-level ConfigError here and re-raise
    in argparse's preferred form so the user sees a clean message.
    """
    try:
        return _validate_profile_name(value)
    except ConfigError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="s3-archive",
        description=(
            "Streaming S3 archive operations (extract / create / list) against any "
            "S3-compatible storage. All operations stream end-to-end — nothing is "
            "staged on local disk."
        ),
        epilog=(
            f"S3 credentials: set $S3CMD_CONFIG (path to an s3cmd INI), "
            f"$S3_ENDPOINT_URL with the standard $AWS_* env vars, or use the "
            f"boto3 default chain. See {REPO_URL} for details."
        ),
    )
    parser.add_argument("--version", action="version", version=f"s3-archive {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show per-file progress.")

    sub = parser.add_subparsers(dest="command", required=True)

    p_extract = sub.add_parser(
        "extract",
        help=(
            "Extract an archive (tar/tar.gz/tar.bz2/tar.xz/tar.zst/zip/7z) in S3 "
            "to a destination prefix."
        ),
    )
    p_extract.add_argument(
        "archive_url",
        help="Source archive URL, e.g. s3://my-bucket/incoming/archive.tar.gz",
    )
    p_extract.add_argument(
        "dest_url",
        help="Destination prefix URL, e.g. s3://my-bucket/extracted/",
    )
    p_extract.add_argument(
        "--dry-run",
        action="store_true",
        help="List members that would be written without uploading anything.",
    )

    p_create = sub.add_parser(
        "create",
        help=(
            "Stream the objects under an S3 prefix into an archive (.tar.gz or .zip) at an S3 key."
        ),
    )
    p_create.add_argument(
        "src_url",
        help="Source prefix URL, e.g. s3://my-bucket/source-dir/",
    )
    p_create.add_argument(
        "dest_url",
        help=(
            "Destination archive URL (must end in .tar.gz/.tgz or .zip), "
            "e.g. s3://my-bucket/archives/snapshot.tar.gz"
        ),
    )
    p_create.add_argument(
        "--dry-run",
        action="store_true",
        help="List source objects that would be archived without uploading anything.",
    )

    p_ls = sub.add_parser(
        "ls",
        help="List the contents of an archive in S3 without extracting it.",
    )
    p_ls.add_argument(
        "archive_url",
        help="Source archive URL, e.g. s3://my-bucket/archives/snapshot.tar.gz",
    )

    p_config = sub.add_parser(
        "config",
        help=(
            "Interactively write an s3cmd-INI credentials file "
            "(~/.s3cfg by default; ~/.s3cfg-<name> with --profile)."
        ),
    )
    p_config.add_argument(
        "--profile",
        default="default",
        type=_argparse_profile,
        help=(
            "Profile name to configure. Default profile writes ~/.s3cfg; "
            "any other name writes ~/.s3cfg-<name>. Must match [A-Za-z0-9_-]+."
        ),
    )

    return parser


def _cmd_extract(args: argparse.Namespace, client) -> int:
    src = parse_s3_url(args.archive_url)
    if not src.key:
        raise ConfigError(f"Archive URL needs a key: {args.archive_url!r}")
    dst = parse_s3_prefix(args.dest_url)
    fmt = detect_format(args.archive_url)

    extract(
        client,
        src.bucket,
        src.key,
        dst.bucket,
        dst.key,
        fmt,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    return _EXIT_OK


_CREATE_TAR_SUFFIXES = (".tar.gz", ".tgz")
_CREATE_ZIP_SUFFIXES = (".zip",)


def _cmd_create(args: argparse.Namespace, client) -> int:
    src = parse_s3_prefix(args.src_url)
    dst = parse_s3_url(args.dest_url)
    if not dst.key:
        raise ConfigError(f"Destination URL needs a key: {args.dest_url!r}")

    lower = dst.key.lower()
    if lower.endswith(_CREATE_TAR_SUFFIXES):
        fmt = "tar.gz"
    elif lower.endswith(_CREATE_ZIP_SUFFIXES):
        fmt = "zip"
    elif lower.endswith(".7z"):
        raise ConfigError(
            f".7z create is not supported; use .tar.gz or .zip (got {args.dest_url!r})."
        )
    else:
        raise ConfigError(
            f"Destination URL must end with .tar.gz/.tgz or .zip (got {args.dest_url!r})."
        )

    create(
        client,
        src.bucket,
        src.key,
        dst.bucket,
        dst.key,
        fmt,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    return _EXIT_OK


def _cmd_ls(args: argparse.Namespace, client) -> int:
    src = parse_s3_url(args.archive_url)
    if not src.key:
        raise ConfigError(f"Archive URL needs a key: {args.archive_url!r}")
    fmt = detect_format(args.archive_url)
    list_archive(client, src.bucket, src.key, fmt)
    return _EXIT_OK


def _cmd_config(args: argparse.Namespace) -> int:
    return run_config(tool_name="s3-archive", profile=args.profile)


def main(argv: list[str] | None = None) -> int:
    # Load .env from CWD if present — operators frequently invoke from
    # the repo directory; CI / Docker should rely on the real environment.
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv(env_path)

    parser = _build_parser()
    args = parser.parse_args(argv)
    setup_console(logging.DEBUG if args.verbose else logging.INFO)

    try:
        # `config` doesn't need (and shouldn't require) S3 creds.
        if args.command == "config":
            return _cmd_config(args)

        client = load_client()
        if args.command == "extract":
            return _cmd_extract(args, client)
        if args.command == "create":
            return _cmd_create(args, client)
        if args.command == "ls":
            return _cmd_ls(args, client)
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        return _EXIT_INTERRUPTED
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return _EXIT_CONFIG_ERROR
    except UnsupportedArchiveFormatError as exc:
        print(f"Unsupported archive format: {exc}", file=sys.stderr)
        return _EXIT_CONFIG_ERROR

    # Unreachable; argparse already enforced a subcommand.
    parser.error("no subcommand")
    return _EXIT_CONFIG_ERROR


if __name__ == "__main__":
    sys.exit(main())
