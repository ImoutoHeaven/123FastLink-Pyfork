from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


MALFORMED_EXPORT_STATE_ERROR = "malformed export-state error"
MISMATCH_ERROR = "mismatch error"


class _MalformedExportStateDetails(Exception):
    pass


def _ensure(condition: bool, message: str) -> None:
    if not condition:
        raise _MalformedExportStateDetails(message)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


@dataclass
class ExportState:
    source_host: str
    source_parent_id: str
    source_root_name: str
    common_path: str
    output_file: Path
    records_file: Path
    temp_output_file: Path
    temp_sqlite_file: Path
    workers: int
    max_retries: int
    flush_every: int
    uses_base62_etags_in_export: bool
    pending_dirs: list[dict] = field(default_factory=list)
    inflight_dirs: list[dict] = field(default_factory=list)
    completed_dirs: list[dict] = field(default_factory=list)
    seen_dir_ids: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)
    last_flush_at: str | None = None
    final_output_committed: bool = False

    def recover_inflight_dirs(self) -> None:
        self.pending_dirs = list(self.inflight_dirs) + list(self.pending_dirs)
        self.inflight_dirs = []

    def flush(self, state_path: Path) -> None:
        payload = {
            "source_host": self.source_host,
            "source_parent_id": self.source_parent_id,
            "source_root_name": self.source_root_name,
            "common_path": self.common_path,
            "output_file": str(self.output_file),
            "records_file": str(self.records_file),
            "temp_output_file": str(self.temp_output_file),
            "temp_sqlite_file": str(self.temp_sqlite_file),
            "workers": self.workers,
            "max_retries": self.max_retries,
            "flush_every": self.flush_every,
            "uses_base62_etags_in_export": self.uses_base62_etags_in_export,
            "pending_dirs": self.pending_dirs,
            "inflight_dirs": self.inflight_dirs,
            "completed_dirs": self.completed_dirs,
            "seen_dir_ids": self.seen_dir_ids,
            "stats": self.stats,
            "last_flush_at": datetime.now(timezone.utc).isoformat(),
            "final_output_committed": self.final_output_committed,
        }
        state_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=state_path.parent,
            delete=False,
        ) as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
            handle.flush()
            os.fsync(handle.fileno())
            temp_name = handle.name
        os.replace(temp_name, state_path)
        _fsync_directory(state_path.parent)
        self.last_flush_at = payload["last_flush_at"]


def _records_file_path(state_path: Path) -> Path:
    return state_path.resolve().with_suffix(".records.jsonl")


def _temp_output_file_path(state_path: Path) -> Path:
    return state_path.resolve().with_suffix(".output.tmp.json")


def _temp_sqlite_file_path(state_path: Path) -> Path:
    return state_path.resolve().with_suffix(".finalize.sqlite3")


def _success_marker_file_path(state_path: Path) -> Path:
    return state_path.resolve().with_suffix(".output.committed")


def get_export_artifact_paths(*, state_path: Path, output_file: Path) -> dict[str, Path]:
    state_path = state_path.resolve()
    output_file = output_file.resolve()
    return {
        "state_file": state_path,
        "output_file": output_file,
        "records_file": _records_file_path(state_path),
        "temp_output_file": _temp_output_file_path(state_path),
        "temp_sqlite_file": _temp_sqlite_file_path(state_path),
        "success_marker_file": _success_marker_file_path(state_path),
    }


def validate_export_output_file(*, state_path: Path, output_file: Path) -> None:
    artifacts = get_export_artifact_paths(state_path=state_path, output_file=output_file)
    if len(set(artifacts.values())) != len(artifacts):
        raise ValueError("output_file must be distinct from internal export artifacts")


def _validate_export_state_payload(payload: object) -> None:
    _ensure(isinstance(payload, dict), "invalid export state payload")
    for field_name in (
        "source_host",
        "source_parent_id",
        "source_root_name",
        "common_path",
        "output_file",
        "records_file",
        "temp_output_file",
        "temp_sqlite_file",
    ):
        _ensure(isinstance(payload.get(field_name), str), f"{field_name} must be string")

    for field_name in ("workers", "max_retries", "flush_every"):
        value = payload.get(field_name)
        _ensure(isinstance(value, int) and not isinstance(value, bool), f"{field_name} must be integer")

    _ensure(
        isinstance(payload.get("uses_base62_etags_in_export"), bool),
        "uses_base62_etags_in_export must be boolean",
    )

    for field_name in ("pending_dirs", "inflight_dirs", "completed_dirs"):
        dirs = payload.get(field_name)
        _ensure(isinstance(dirs, list), f"{field_name} must be list")
        for item in dirs:
            _ensure(isinstance(item, dict), f"{field_name} items must be objects")
            _ensure(isinstance(item.get("folder_id"), str), f"{field_name} folder_id must be string")
            _ensure(isinstance(item.get("relative_dir"), str), f"{field_name} relative_dir must be string")

    seen_dir_ids = payload.get("seen_dir_ids")
    _ensure(isinstance(seen_dir_ids, list), "seen_dir_ids must be list")
    _ensure(all(isinstance(item, str) for item in seen_dir_ids), "seen_dir_ids entries must be strings")

    stats = payload.get("stats")
    _ensure(isinstance(stats, dict), "stats must be object")
    for field_name in ("files_written", "dirs_completed"):
        value = stats.get(field_name)
        _ensure(isinstance(value, int) and not isinstance(value, bool), f"stats.{field_name} must be integer")

    _ensure(payload.get("last_flush_at") is None or isinstance(payload.get("last_flush_at"), str), "last_flush_at invalid")
    _ensure(
        payload.get("final_output_committed") is None or isinstance(payload.get("final_output_committed"), bool),
        "final_output_committed must be boolean",
    )


def validate_export_state_scope(
    *,
    state_path: Path,
    source_host: str,
    source_parent_id: str,
    source_root_name: str,
    output_file: Path,
) -> dict:
    artifacts = get_export_artifact_paths(state_path=state_path, output_file=output_file)
    state_path = artifacts["state_file"]
    output_file = artifacts["output_file"]
    records_file = artifacts["records_file"]
    common_path = f"{source_root_name}/"

    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(MALFORMED_EXPORT_STATE_ERROR) from exc

    try:
        _validate_export_state_payload(payload)
    except _MalformedExportStateDetails as exc:
        raise ValueError(MALFORMED_EXPORT_STATE_ERROR) from exc

    expected_scope = {
        "source_host": source_host,
        "source_parent_id": source_parent_id,
        "source_root_name": source_root_name,
        "common_path": common_path,
        "output_file": str(output_file),
        "records_file": str(records_file),
    }
    for key, value in expected_scope.items():
        if payload.get(key) != value:
            raise ValueError(MISMATCH_ERROR)
    return payload


def load_or_initialize_export_state(
    *,
    state_path: Path,
    source_host: str,
    source_parent_id: str,
    source_root_name: str,
    output_file: Path,
    workers: int,
    max_retries: int,
    flush_every: int,
) -> ExportState:
    validate_export_output_file(state_path=state_path, output_file=output_file)
    artifacts = get_export_artifact_paths(state_path=state_path, output_file=output_file)
    state_path = artifacts["state_file"]
    output_file = artifacts["output_file"]
    records_file = artifacts["records_file"]
    temp_output_file = artifacts["temp_output_file"]
    temp_sqlite_file = artifacts["temp_sqlite_file"]
    common_path = f"{source_root_name}/"

    if state_path.exists():
        payload = validate_export_state_scope(
            state_path=state_path,
            source_host=source_host,
            source_parent_id=source_parent_id,
            source_root_name=source_root_name,
            output_file=output_file,
        )
        if not records_file.exists():
            raise ValueError(MISMATCH_ERROR)

        state = ExportState(
            source_host=payload["source_host"],
            source_parent_id=payload["source_parent_id"],
            source_root_name=payload["source_root_name"],
            common_path=payload["common_path"],
            output_file=Path(payload["output_file"]),
            records_file=Path(payload["records_file"]),
            temp_output_file=temp_output_file,
            temp_sqlite_file=temp_sqlite_file,
            workers=int(workers),
            max_retries=int(max_retries),
            flush_every=int(flush_every),
            uses_base62_etags_in_export=True,
            pending_dirs=list(payload["pending_dirs"]),
            inflight_dirs=list(payload["inflight_dirs"]),
            completed_dirs=list(payload["completed_dirs"]),
            seen_dir_ids=list(payload["seen_dir_ids"]),
            stats=dict(payload["stats"]),
            last_flush_at=payload.get("last_flush_at"),
            final_output_committed=bool(payload.get("final_output_committed", False)),
        )
        state.recover_inflight_dirs()
        return state

    if records_file.exists():
        raise ValueError(MISMATCH_ERROR)

    return ExportState(
        source_host=source_host,
        source_parent_id=source_parent_id,
        source_root_name=source_root_name,
        common_path=common_path,
        output_file=output_file,
        records_file=records_file,
        temp_output_file=temp_output_file,
        temp_sqlite_file=temp_sqlite_file,
        workers=int(workers),
        max_retries=int(max_retries),
        flush_every=int(flush_every),
        uses_base62_etags_in_export=True,
        pending_dirs=[{"folder_id": source_parent_id, "relative_dir": ""}],
        inflight_dirs=[],
        completed_dirs=[],
        seen_dir_ids=[source_parent_id],
        stats={"files_written": 0, "dirs_completed": 0},
        last_flush_at=None,
        final_output_committed=False,
    )
