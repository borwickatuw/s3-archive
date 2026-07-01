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
from tqdm import tqdm

from s3_archive import REPO_URL, __version__
from s3_archive.config_cmd import run_config
from s3_archive.config_cmd import validate_profile_name as _validate_profile_name
from s3_archive.create import create
from s3_archive.exceptions import (
    ConfigError,
    UnsafeArchiveMemberError,
    UnsupportedArchiveFormatError,
)
from s3_archive.extract import extract
from s3_archive.list import list_objects
from s3_archive.log_config import get_logger, setup_console
from s3_archive.ls import list_archive
from s3_archive.s3_client import client_for
from s3_archive.url import detect_format, parse_s3_prefix, parse_s3_url

log = get_logger(__name__)


def _byte_progress_bar(desc: str, total: int | None, *, disable: bool = False) -> tqdm:
    """Build a bytes-scaled tqdm bar (%, ETA, MB/s) for a transfer.

    Auto-disabled when stderr isn't a TTY (piped / non-interactive runs
    keep clean logs) or when *disable* is set (e.g. 7z extract, whose
    random-access decode has no meaningful sequential byte total). Log
    records route through :class:`log_config._TqdmLoggingHandler`, so
    ``-v`` output doesn't clobber the bar.
    """
    return tqdm(
        total=total,
        desc=desc,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        disable=disable or not sys.stderr.isatty(),
    )


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
    p_extract.add_argument(
        "--fix-unsafe-paths",
        action="store_true",
        help=(
            "Safely collapse '..' path-traversal segments in member names "
            "instead of aborting (the default is to fail on the first such member)."
        ),
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
    p_ls.add_argument(
        "--fix-unsafe-paths",
        action="store_true",
        help=(
            "Safely collapse '..' path-traversal segments in member names "
            "instead of aborting (matches 'extract --fix-unsafe-paths' so the "
            "listing previews exactly what extract would write)."
        ),
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


def _cmd_extract(args: argparse.Namespace) -> int:
    src = parse_s3_url(args.archive_url)
    if not src.key:
        raise ConfigError(f"Archive URL needs a key: {args.archive_url!r}")
    dst = parse_s3_prefix(args.dest_url)
    fmt = detect_format(args.archive_url)

    # Resolve both clients up-front so a missing profile fails fast,
    # before any archive stream is opened.
    src_client = client_for(src.profile)
    dst_client = client_for(dst.profile)

    # Size the progress bar against the compressed archive object. 7z is
    # decoded via random-access ranged GETs, not one sequential stream,
    # so a byte total would be meaningless — skip the bar there.
    is_7z = fmt == "7z"
    total = None
    if not args.dry_run and not is_7z:
        total = src_client.head_object(Bucket=src.bucket, Key=src.key)["ContentLength"]

    with _byte_progress_bar("Extracting", total, disable=args.dry_run or is_7z) as bar:
        extract(
            src_client,
            dst_client,
            src.bucket,
            src.key,
            dst.bucket,
            dst.key,
            fmt,
            dry_run=args.dry_run,
            verbose=args.verbose,
            fix_unsafe_paths=args.fix_unsafe_paths,
            on_read=bar.update,
        )
    return _EXIT_OK


_CREATE_TAR_SUFFIXES = (".tar.gz", ".tgz")
_CREATE_ZIP_SUFFIXES = (".zip",)


def _cmd_create(args: argparse.Namespace) -> int:
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

    # Resolve both clients up-front so a missing profile fails fast,
    # before any source object is fetched.
    src_client = client_for(src.profile)
    dst_client = client_for(dst.profile)

    # Size the bar against total source bytes. This lists the prefix once
    # here to sum sizes; create() lists again to do the work. The extra
    # list is cheap next to the transfer and keeps create() UI-free.
    total = None
    if not args.dry_run:
        total = sum(
            obj["Size"]
            for obj in list_objects(src_client, src.bucket, src.key)
            if obj["RelativePath"]
        )

    with _byte_progress_bar("Archiving", total, disable=args.dry_run) as bar:
        create(
            src_client,
            dst_client,
            src.bucket,
            src.key,
            dst.bucket,
            dst.key,
            fmt,
            dry_run=args.dry_run,
            verbose=args.verbose,
            on_bytes=bar.update,
        )
    return _EXIT_OK


def _cmd_ls(args: argparse.Namespace) -> int:
    src = parse_s3_url(args.archive_url)
    if not src.key:
        raise ConfigError(f"Archive URL needs a key: {args.archive_url!r}")
    fmt = detect_format(args.archive_url)
    list_archive(
        client_for(src.profile),
        src.bucket,
        src.key,
        fmt,
        fix_unsafe_paths=args.fix_unsafe_paths,
    )
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
        # extract / create / ls each build their own client(s) via
        # `client_for(profile)` inside the dispatcher so a missing
        # profile fails before any stream opens.
        if args.command == "extract":
            return _cmd_extract(args)
        if args.command == "create":
            return _cmd_create(args)
        if args.command == "ls":
            return _cmd_ls(args)
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        return _EXIT_INTERRUPTED
    except UnsafeArchiveMemberError as exc:
        print(f"Unsafe archive member: {exc}", file=sys.stderr)
        return _EXIT_ERROR
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
