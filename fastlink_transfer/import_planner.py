from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, replace
from pathlib import Path

import ijson

from fastlink_transfer.import_state import MISMATCH_ERROR
from fastlink_transfer.importer import normalize_common_path, normalize_import_record


MALFORMED_EXPORT_ERROR = "malformed export json"
SCALAR_EVENTS = {"boolean", "integer", "double", "number", "null", "string"}


@dataclass(frozen=True)
class PlanningScope:
    source_sha256: str
    common_path: str


@dataclass(frozen=True)
class PlanningSummary:
    source_sha256: str
    common_path: str
    total_files: int
    rebuilt: bool


@dataclass(frozen=True)
class PlanningResult:
    summary: PlanningSummary
    scope: PlanningScope


@dataclass(frozen=True)
class _ExportMetadata:
    source_sha256: str
    common_path: str
    uses_base62: bool


class _HashingReader:
    def __init__(self, handle, hasher: hashlib._Hash):
        self._handle = handle
        self._hasher = hasher

    def read(self, size: int = -1) -> bytes:
        chunk = self._handle.read(size)
        self._hasher.update(chunk)
        return chunk


def inspect_export_scope(*, export_path: Path) -> PlanningScope:
    metadata = _inspect_export_metadata(export_path=export_path, include_hash=True)
    return PlanningScope(source_sha256=metadata.source_sha256, common_path=metadata.common_path)


def plan_import_into_state(*, export_path: Path, state, scope: PlanningScope) -> PlanningSummary:
    metadata = _ExportMetadata(
        source_sha256=scope.source_sha256,
        common_path=scope.common_path,
        uses_base62=False,
    )

    state.reset_planning_rows()

    planned_folder_keys: set[str] = set()
    seen_paths: set[str] = set()
    total_files = 0
    failed_files = 0
    try:
        with export_path.open("rb") as handle:
            for prefix, event, value in ijson.parse(handle):
                if prefix == "usesBase62EtagsInExport":
                    if event != "boolean":
                        raise ValueError("usesBase62EtagsInExport must be boolean when present")
                    metadata = replace(metadata, uses_base62=bool(value))
                    break
                if prefix == "files" and event == "start_array":
                    break

        for folder_key, parent_key in _iter_folder_rows(
            common_path=metadata.common_path,
            relative_parent_dir="",
        ):
            if folder_key in planned_folder_keys:
                continue
            state.connection.execute(
                "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
                (folder_key, parent_key, None, "pending", None),
            )
            planned_folder_keys.add(folder_key)

        with export_path.open("rb") as handle:
            for file_entry in ijson.items(handle, "files.item"):
                if not isinstance(file_entry, dict):
                    raise ValueError("file entries must be objects")

                etag_value = file_entry.get("etag")
                path_value = file_entry.get("path")
                if not isinstance(etag_value, str) or not etag_value:
                    raise ValueError("etag must be a non-empty string")
                if not isinstance(path_value, str):
                    raise ValueError("path must be a string")

                normalized_path, etag_hex, size = normalize_import_record(
                    path_value=path_value,
                    etag_value=etag_value,
                    size_value=file_entry.get("size"),
                    uses_base62=metadata.uses_base62,
                )

                path_parts = normalized_path.split("/")
                relative_parent_dir = "/".join(path_parts[:-1])
                for folder_key, parent_key in _iter_folder_rows(
                    common_path=metadata.common_path,
                    relative_parent_dir=relative_parent_dir,
                ):
                    if folder_key in planned_folder_keys:
                        continue
                    state.connection.execute(
                        "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
                        (folder_key, parent_key, None, "pending", None),
                    )
                    planned_folder_keys.add(folder_key)

                total_files += 1
                status = "pending"
                error = None
                if normalized_path in seen_paths:
                    status = "failed"
                    error = "duplicate normalized file path"
                    failed_files += 1
                else:
                    seen_paths.add(normalized_path)

                state.connection.execute(
                    "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        _record_key(
                            source_index=total_files,
                            normalized_path=normalized_path,
                            etag_hex=etag_hex,
                            size=size,
                        ),
                        normalized_path,
                        path_parts[-1],
                        relative_parent_dir,
                        etag_hex,
                        size,
                        status,
                        error,
                        0,
                    ),
                )
        state.finish_planning(
            source_sha256=metadata.source_sha256,
            common_path=metadata.common_path,
            total_files=total_files,
            total_folders=len(planned_folder_keys),
            failed_count=failed_files,
        )
    except ijson.JSONError as exc:
        state.connection.rollback()
        raise ValueError(MALFORMED_EXPORT_ERROR) from exc
    except Exception:
        state.connection.rollback()
        raise

    return PlanningSummary(
        source_sha256=metadata.source_sha256,
        common_path=metadata.common_path,
        total_files=total_files,
        rebuilt=False,
    )


def plan_import_into_new_state(*, export_path: Path, state) -> PlanningResult:
    metadata = _inspect_export_metadata(export_path=export_path, include_hash=True)
    try:
        state.connection.execute(
            "UPDATE job SET source_sha256 = ?, common_path = ? WHERE singleton = 1",
            (metadata.source_sha256, metadata.common_path),
        )
        state.connection.commit()
        summary = plan_import_into_state(
            export_path=export_path,
            state=state,
            scope=PlanningScope(source_sha256=metadata.source_sha256, common_path=metadata.common_path),
        )
        return PlanningResult(
            summary=summary,
            scope=PlanningScope(source_sha256=summary.source_sha256, common_path=summary.common_path),
        )
    except BaseException:
        state.connection.close()
        try:
            if state.state_path.exists():
                os.unlink(state.state_path)
        except OSError:
            pass
        raise


def rebuild_incomplete_plan_if_needed(*, export_path: Path, state, scope: PlanningScope) -> PlanningSummary:
    if state.is_planning_complete():
        return PlanningSummary(
            source_sha256=state.job_scope["source_sha256"],
            common_path=state.job_scope["common_path"],
            total_files=state.stats["total"],
            rebuilt=False,
        )

    return replace(
        plan_import_into_state(export_path=export_path, state=state, scope=scope),
        rebuilt=True,
    )


def _inspect_export_metadata(*, export_path: Path, include_hash: bool) -> _ExportMetadata:
    hasher = hashlib.sha256() if include_hash else None
    common_path = ""
    uses_base62 = False
    saw_root = False
    saw_files_array = False

    try:
        with export_path.open("rb") as handle:
            stream = _HashingReader(handle, hasher) if hasher is not None else handle
            for prefix, event, value in ijson.parse(stream):
                if not saw_root:
                    if prefix != "" or event != "start_map":
                        raise ValueError("export root must be an object")
                    saw_root = True
                    continue

                if prefix == "usesBase62EtagsInExport":
                    if event != "boolean":
                        raise ValueError("usesBase62EtagsInExport must be boolean when present")
                    uses_base62 = bool(value)
                    continue

                if prefix == "commonPath":
                    if event != "string":
                        raise ValueError("commonPath must be a string")
                    common_path = normalize_common_path(value)
                    continue

                if prefix == "files":
                    if event == "start_array":
                        saw_files_array = True
                        continue
                    if event in SCALAR_EVENTS or event == "start_map":
                        raise ValueError("files must be an array")

        if not saw_root:
            raise ValueError("export root must be an object")
        if not saw_files_array:
            raise ValueError("files must be an array")
    except ijson.JSONError as exc:
        raise ValueError(MALFORMED_EXPORT_ERROR) from exc

    source_sha256 = hasher.hexdigest() if hasher is not None else ""
    return _ExportMetadata(
        source_sha256=source_sha256,
        common_path=common_path,
        uses_base62=uses_base62,
    )


def _iter_folder_rows(*, common_path: str, relative_parent_dir: str):
    common_segments = common_path.rstrip("/").split("/") if common_path else []
    for index in range(len(common_segments)):
        segments = common_segments[: index + 1]
        yield "/".join(segments), "/".join(segments[:-1])

    if not relative_parent_dir:
        return

    parent_segments = relative_parent_dir.split("/")
    for index in range(len(parent_segments)):
        segments = common_segments + parent_segments[: index + 1]
        yield "/".join(segments), "/".join(segments[:-1])


def _record_key(*, source_index: int, normalized_path: str, etag_hex: str, size: int) -> str:
    return f"{source_index}\t{normalized_path}\t{etag_hex}\t{size}"
