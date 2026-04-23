from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fastlink_transfer.export_state import validate_export_output_file


@dataclass(frozen=True)
class ImportJsonConfig:
    file_path: Path
    target_parent_id: str
    state_file: Path
    workers: int
    max_retries: int
    flush_every: int
    retry_failed: bool
    dry_run: bool
    command: str = "import_json"


@dataclass(frozen=True)
class ExportJsonConfig:
    source_parent_id: str
    output_file: Path
    state_file: Path
    workers: int
    max_retries: int
    flush_every: int
    command: str = "export_json"


@dataclass(frozen=True)
class BatchImportJsonConfig:
    input_dir: Path
    target_parent_id: str
    state_dir: Path
    workers: int
    json_parallelism: int
    max_retries: int
    flush_every: int
    retry_failed: bool
    dry_run: bool
    command: str = "batch_import_json"


CommandConfig = ImportJsonConfig


def build_command_config(
    args,
) -> ImportJsonConfig | ExportJsonConfig | BatchImportJsonConfig:
    if args.workers < 1:
        raise ValueError("workers must be >= 1")
    if args.max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if args.flush_every < 1:
        raise ValueError("flush_every must be >= 1")

    if args.command == "import_json":
        target_parent_id = str(args.target_parent_id).strip()
        if not target_parent_id:
            raise ValueError("target_parent_id is required")

        return ImportJsonConfig(
            command="import_json",
            file_path=Path(args.file).resolve(),
            target_parent_id=target_parent_id,
            state_file=Path(args.state_file).resolve(),
            workers=int(args.workers),
            max_retries=int(args.max_retries),
            flush_every=int(args.flush_every),
            retry_failed=bool(args.retry_failed),
            dry_run=bool(args.dry_run),
        )

    if args.command == "batch_import_json":
        target_parent_id = str(args.target_parent_id).strip()
        if not target_parent_id:
            raise ValueError("target_parent_id is required")
        if args.json_parallelism < 1:
            raise ValueError("json_parallelism must be >= 1")

        return BatchImportJsonConfig(
            command="batch_import_json",
            input_dir=Path(args.input_dir).resolve(),
            target_parent_id=target_parent_id,
            state_dir=Path(args.state_dir).resolve(),
            workers=int(args.workers),
            json_parallelism=int(args.json_parallelism),
            max_retries=int(args.max_retries),
            flush_every=int(args.flush_every),
            retry_failed=bool(args.retry_failed),
            dry_run=bool(args.dry_run),
        )

    if args.command == "export_json":
        source_parent_id = str(args.source_parent_id).strip()
        if not source_parent_id:
            raise ValueError("source_parent_id is required")

        output_file = Path(args.output_file).resolve()
        state_file = Path(args.state_file).resolve()
        validate_export_output_file(state_path=state_file, output_file=output_file)

        return ExportJsonConfig(
            command="export_json",
            source_parent_id=source_parent_id,
            output_file=output_file,
            state_file=state_file,
            workers=int(args.workers),
            max_retries=int(args.max_retries),
            flush_every=int(args.flush_every),
        )

    raise ValueError(f"unsupported command: {args.command}")
