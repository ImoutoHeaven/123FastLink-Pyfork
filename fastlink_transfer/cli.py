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

    batch_import = subparsers.add_parser("batch-import-json")
    batch_import.set_defaults(command="batch_import_json")
    batch_import.add_argument("--input-dir", required=True)
    batch_import.add_argument("--target-parent-id", required=True)
    batch_import.add_argument("--state-dir", required=True)
    batch_import.add_argument("--workers", type=int, default=8)
    batch_import.add_argument("--json-parallelism", type=int, default=2)
    batch_import.add_argument("--max-retries", type=int, default=5)
    batch_import.add_argument("--flush-every", type=int, default=100)
    batch_import.add_argument("--retry-failed", action="store_true")
    batch_import.add_argument("--dry-run", action="store_true")

    batch_check = subparsers.add_parser("batch-check-json")
    batch_check.set_defaults(command="batch_check_json")
    batch_check.add_argument("--input-dir", required=True)
    batch_check.add_argument("--target-parent-id", required=True)
    batch_check.add_argument("--state-dir", required=True)
    batch_check.add_argument("--output-dir", required=True)
    batch_check.add_argument("--workers", type=int, default=8)
    batch_check.add_argument("--json-parallelism", type=int, default=2)
    batch_check.add_argument("--max-retries", type=int, default=5)
    batch_check.add_argument("--flush-every", type=int, default=100)
    batch_check.add_argument("--exist-only", action="store_true")
    batch_check.add_argument("--with-checksum", action="store_true")

    export_json = subparsers.add_parser("export-json")
    export_json.set_defaults(command="export_json")
    export_json.add_argument("--source-parent-id", required=True)
    export_json.add_argument("--output-file", required=True)
    export_json.add_argument("--state-file", required=True)
    export_json.add_argument("--workers", type=int, default=8)
    export_json.add_argument("--max-retries", type=int, default=5)
    export_json.add_argument("--flush-every", type=int, default=100)

    return parser


def parse_args(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args, build_command_config(args)
