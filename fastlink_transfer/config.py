from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CommandConfig:
    file_path: Path
    target_parent_id: str
    state_file: Path
    workers: int
    max_retries: int
    flush_every: int
    retry_failed: bool
    dry_run: bool


def build_command_config(args) -> CommandConfig:
    target_parent_id = str(args.target_parent_id).strip()

    if args.workers < 1:
        raise ValueError("workers must be >= 1")
    if args.max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if args.flush_every < 1:
        raise ValueError("flush_every must be >= 1")
    if not target_parent_id:
        raise ValueError("target_parent_id is required")

    return CommandConfig(
        file_path=Path(args.file).resolve(),
        target_parent_id=target_parent_id,
        state_file=Path(args.state_file).resolve(),
        workers=int(args.workers),
        max_retries=int(args.max_retries),
        flush_every=int(args.flush_every),
        retry_failed=bool(args.retry_failed),
        dry_run=bool(args.dry_run),
    )
