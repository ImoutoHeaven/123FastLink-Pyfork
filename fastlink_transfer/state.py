from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from fastlink_transfer.importer import normalize_common_path


MALFORMED_STATE_ERROR = "malformed-state error"
MISMATCH_ERROR = "mismatch error"


class _MalformedStateDetails(Exception):
    pass


class _MismatchDetails(Exception):
    pass


@dataclass
class TransferState:
    source_file: str
    source_sha256: str
    target_parent_id: str
    common_path: str
    workers: int
    folder_map: dict[str, str] = field(default_factory=dict)
    completed: set[str] = field(default_factory=set)
    not_reusable: dict[str, dict] = field(default_factory=dict)
    failed: dict[str, dict] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=dict)
    last_flush_at: str | None = None

    def _remove_terminal_record(self, key: str) -> None:
        self.completed.discard(key)
        self.not_reusable.pop(key, None)
        self.failed.pop(key, None)
        self.stats["completed"] = len(self.completed)
        self.stats["not_reusable"] = len(self.not_reusable)
        self.stats["failed"] = len(self.failed)

    def record_completed(self, key: str) -> None:
        self._remove_terminal_record(key)
        self.completed.add(key)
        self.stats["completed"] = len(self.completed)

    def record_not_reusable(self, *, key: str, path: str, error: str) -> None:
        self._remove_terminal_record(key)
        self.not_reusable[key] = {"key": key, "path": path, "error": error}
        self.stats["not_reusable"] = len(self.not_reusable)

    def record_failed(self, *, key: str, path: str, error: str, retries: int) -> None:
        self._remove_terminal_record(key)
        self.failed[key] = {"key": key, "path": path, "error": error, "retries": retries}
        self.stats["failed"] = len(self.failed)

    def flush(self, state_path: Path) -> None:
        payload = {
            "source_file": self.source_file,
            "source_sha256": self.source_sha256,
            "target_parent_id": self.target_parent_id,
            "common_path": self.common_path,
            "workers": self.workers,
            "folder_map": self.folder_map,
            "completed": sorted(self.completed),
            "not_reusable": list(self.not_reusable.values()),
            "failed": list(self.failed.values()),
            "stats": self.stats,
            "last_flush_at": datetime.now(timezone.utc).isoformat(),
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
        self.last_flush_at = payload["last_flush_at"]


def _ensure(condition: bool, message: str, *, error_cls: type[Exception] = _MalformedStateDetails) -> None:
    if not condition:
        raise error_cls(message)


def _is_strict_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_bucket_records(records: object, *, require_retries: bool) -> None:
    _ensure(isinstance(records, list), "invalid bucket")
    for record in records:
        _ensure(isinstance(record, dict), "bucket records must be objects")
        _ensure(isinstance(record.get("key"), str), "bucket record key must be string")
        _ensure(isinstance(record.get("path"), str), "bucket record path must be string")
        if require_retries:
            _ensure(isinstance(record.get("error"), str), "failed record error must be string")
            _ensure(_is_strict_int(record.get("retries")), "failed record retries must be integer")
        elif "error" in record:
            _ensure(isinstance(record.get("error"), str), "not_reusable error must be string")


def _validate_folder_map(folder_map: object, *, target_parent_id: str) -> None:
    _ensure(isinstance(folder_map, dict), "invalid folder_map")
    _ensure(
        all(isinstance(key, str) and isinstance(value, str) for key, value in folder_map.items()),
        "folder_map must map strings to strings",
    )
    _ensure(folder_map.get("") == target_parent_id, "folder_map root mismatch")
    for key in folder_map:
        if not key:
            continue
        _ensure(not key.startswith("/"), "folder_map keys must be normalized")
        _ensure(not key.endswith("/"), "folder_map keys must be normalized")
        _ensure("//" not in key, "folder_map keys must be normalized")
        _ensure("\\" not in key, "folder_map keys must be normalized")
        parts = key.split("/")
        _ensure("." not in parts and ".." not in parts, "folder_map keys must be normalized")


def _validate_payload(
    payload: object,
    *,
    source_sha256: str,
    target_parent_id: str,
    common_path: str,
) -> None:
    _ensure(isinstance(payload, dict), "invalid state payload")
    _ensure(isinstance(payload.get("source_file"), str), "invalid source_file")
    _ensure(isinstance(payload.get("source_sha256"), str), "invalid source_sha256")
    _ensure(isinstance(payload.get("target_parent_id"), str), "invalid target_parent_id")
    _ensure(isinstance(payload.get("common_path"), str), "invalid common_path")
    try:
        normalized_common_path = normalize_common_path(payload["common_path"])
    except ValueError as exc:
        raise _MalformedStateDetails("invalid common_path") from exc
    _ensure(normalized_common_path == payload["common_path"], "invalid common_path")
    _ensure(_is_strict_int(payload.get("workers")) and payload["workers"] >= 1, "invalid workers")
    _validate_folder_map(payload.get("folder_map"), target_parent_id=payload["target_parent_id"])
    _ensure(
        isinstance(payload.get("completed"), list)
        and all(isinstance(item, str) for item in payload["completed"]),
        "completed must be a list of strings",
    )
    _validate_bucket_records(payload.get("not_reusable"), require_retries=False)
    _validate_bucket_records(payload.get("failed"), require_retries=True)

    stats = payload.get("stats")
    _ensure(isinstance(stats, dict), "invalid stats")
    for field_name in ("total", "completed", "not_reusable", "failed"):
        _ensure(_is_strict_int(stats.get(field_name)), f"stats.{field_name} must be integer")

    _ensure(payload.get("last_flush_at") is None or isinstance(payload.get("last_flush_at"), str), "invalid last_flush_at")
    _ensure(
        payload["source_sha256"] == source_sha256,
        "state file scope mismatch",
        error_cls=_MismatchDetails,
    )
    _ensure(
        payload["target_parent_id"] == target_parent_id,
        "state file scope mismatch",
        error_cls=_MismatchDetails,
    )
    _ensure(payload["common_path"] == common_path, "state common_path mismatch")


def load_or_initialize_state(
    *,
    state_path: Path,
    source_file: str,
    source_sha256: str,
    target_parent_id: str,
    common_path: str,
    workers: int,
    total_records: int,
) -> TransferState:
    if not state_path.exists():
        return TransferState(
            source_file=source_file,
            source_sha256=source_sha256,
            target_parent_id=target_parent_id,
            common_path=common_path,
            workers=workers,
            folder_map={"": target_parent_id},
            completed=set(),
            not_reusable={},
            failed={},
            stats={"total": total_records, "completed": 0, "not_reusable": 0, "failed": 0},
            last_flush_at=None,
        )

    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(MALFORMED_STATE_ERROR) from exc

    try:
        _validate_payload(
            payload,
            source_sha256=source_sha256,
            target_parent_id=target_parent_id,
            common_path=common_path,
        )

        completed = set(payload["completed"])
        not_reusable = {record["key"]: record for record in payload["not_reusable"]}
        failed = {record["key"]: record for record in payload["failed"]}
        _ensure(len(completed) == len(payload["completed"]), "duplicate completed keys")
        _ensure(len(not_reusable) == len(payload["not_reusable"]), "duplicate not_reusable keys")
        _ensure(len(failed) == len(payload["failed"]), "duplicate failed keys")

        overlap = (completed & set(not_reusable)) | (completed & set(failed)) | (set(not_reusable) & set(failed))
        _ensure(not overlap, "duplicate key across terminal buckets")

        stats = payload["stats"]
        _ensure(
            stats["completed"] == len(completed)
            and stats["not_reusable"] == len(not_reusable)
            and stats["failed"] == len(failed),
            "stats mismatch",
        )
    except _MismatchDetails as exc:
        raise ValueError(MISMATCH_ERROR) from exc
    except _MalformedStateDetails as exc:
        raise ValueError(MALFORMED_STATE_ERROR) from exc

    return TransferState(
        source_file=payload["source_file"],
        source_sha256=payload["source_sha256"],
        target_parent_id=payload["target_parent_id"],
        common_path=payload["common_path"],
        workers=workers,
        folder_map=dict(payload["folder_map"]),
        completed=completed,
        not_reusable=not_reusable,
        failed=failed,
        stats={
            "total": total_records,
            "completed": len(completed),
            "not_reusable": len(not_reusable),
            "failed": len(failed),
        },
        last_flush_at=payload.get("last_flush_at"),
    )
