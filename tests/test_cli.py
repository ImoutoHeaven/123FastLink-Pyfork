import argparse

from fastlink_transfer.cli import build_parser


def test_build_parser_supports_import_json_command():
    parser = build_parser()
    args = parser.parse_args(
        [
            "import-json",
            "--file",
            "sample.json",
            "--target-parent-id",
            "12345678",
            "--state-file",
            "state.json",
        ]
    )
    assert args.command == "import_json"
    assert args.file == "sample.json"
    assert args.target_parent_id == "12345678"
    assert args.state_file == "state.json"
    assert args.workers == 8
    assert args.max_retries == 5
    assert args.flush_every == 100
    assert args.retry_failed is False
    assert args.dry_run is False


def test_parser_accepts_runtime_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "import-json",
            "--file",
            "sample.json",
            "--target-parent-id",
            "12345678",
            "--state-file",
            "state.json",
            "--workers",
            "16",
            "--max-retries",
            "7",
            "--flush-every",
            "50",
            "--retry-failed",
            "--dry-run",
        ]
    )
    assert args.workers == 16
    assert args.max_retries == 7
    assert args.flush_every == 50
    assert args.retry_failed is True
    assert args.dry_run is True


def test_build_parser_supports_batch_import_json_command():
    parser = build_parser()
    args = parser.parse_args(
        [
            "batch-import-json",
            "--input-dir",
            "exports",
            "--target-parent-id",
            "12345678",
            "--state-dir",
            ".state/batch",
        ]
    )

    assert args.command == "batch_import_json"
    assert args.input_dir == "exports"
    assert args.target_parent_id == "12345678"
    assert args.state_dir == ".state/batch"
    assert args.workers == 8
    assert args.json_parallelism == 2
    assert args.max_retries == 5
    assert args.flush_every == 100
    assert args.retry_failed is False
    assert args.dry_run is False


def test_parser_accepts_batch_runtime_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "batch-import-json",
            "--input-dir",
            "exports",
            "--target-parent-id",
            "12345678",
            "--state-dir",
            ".state/batch",
            "--workers",
            "16",
            "--json-parallelism",
            "3",
            "--max-retries",
            "7",
            "--flush-every",
            "50",
            "--retry-failed",
            "--dry-run",
        ]
    )

    assert args.workers == 16
    assert args.json_parallelism == 3
    assert args.max_retries == 7
    assert args.flush_every == 50
    assert args.retry_failed is True
    assert args.dry_run is True


def test_build_parser_supports_export_json_command():
    parser = build_parser()
    subparsers = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )

    assert "export-json" in subparsers.choices

    args = parser.parse_args(
        [
            "export-json",
            "--source-parent-id",
            "12345678",
            "--output-file",
            "out.json",
            "--state-file",
            "state.json",
        ]
    )

    assert args.command == "export_json"
    assert args.source_parent_id == "12345678"
    assert args.output_file == "out.json"
    assert args.state_file == "state.json"
    assert args.workers == 8
    assert args.max_retries == 5
    assert args.flush_every == 100
    assert not hasattr(args, "dry_run")
