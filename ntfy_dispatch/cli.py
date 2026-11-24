"""Command line entry point."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import ConfigError
from .registry import RegistryError
from .version import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ntfy-dispatch",
        description="Route ntfy messages to registered Python handlers.",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("config.toml"),
        help="path to the TOML configuration file (default: config.toml)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate configuration and handlers, then exit without connecting",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="override the configured log level",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        from .app import run_app

        return run_app(args.config, check=args.check, log_level=args.log_level)
    except (ConfigError, RegistryError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
