from __future__ import annotations

import argparse

from fastlink_transfer.config import build_command_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fastlink-transfer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_json = subparsers.add_parser("import-json")
    import_json.set_defaults(command="import_json")
    import_json.add_argument("--file", required=True)
    import_json.add_argument("--target-parent-id", required=True)
    import_json.add_argument("--state-file", required=True)
    import_json.add_argument("--workers", type=int, default=8)
    import_json.add_argument("--max-retries", type=int, default=5)
    import_json.add_argument("--flush-every", type=int, default=100)
    import_json.add_argument("--retry-failed", action="store_true")
    import_json.add_argument("--dry-run", action="store_true")
    return parser


def parse_args(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args, build_command_config(args)
