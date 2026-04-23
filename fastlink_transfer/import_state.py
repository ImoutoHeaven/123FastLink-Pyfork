from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from fastlink_transfer import __version__
from fastlink_transfer.importer import BASE62_ALPHABET, SourceRecord, normalize_common_path


SCHEMA_VERSION = 1
SQLITE_HEADER = b"SQLite format 3\x00"
MALFORMED_STATE_ERROR = "malformed-state error"
MISMATCH_ERROR = "mismatch error"
REQUIRED_TABLES = {"job", "folders", "files"}
ALLOWED_FOLDER_STATUSES = {"pending", "created"}
ALLOWED_FILE_STATUSES = {"pending", "completed", "not_reusable", "failed"}
EXPECTED_JOB_COLUMNS = {
    "singleton",
    "schema_version",
    "source_file",
    "source_sha256",
    "target_parent_id",
    "common_path",
    "planning_complete",
    "total_files",
    "total_folders",
    "completed_count",
    "not_reusable_count",
    "failed_count",
    "last_flush_at",
}
EXPECTED_FOLDER_COLUMNS = {"folder_key", "parent_key", "remote_folder_id", "status", "last_error"}
EXPECTED_FILE_COLUMNS = {
    "record_key",
    "path",
    "file_name",
    "relative_parent_dir",
    "etag_hex",
    "size",
    "status",
    "error",
    "retries",
}
REQUIRED_JOB_NON_NULL_COLUMNS = {
    "schema_version",
    "source_file",
    "source_sha256",
    "target_parent_id",
    "common_path",
    "planning_complete",
    "total_files",
    "total_folders",
    "completed_count",
    "not_reusable_count",
    "failed_count",
}
REQUIRED_FOLDER_NON_NULL_COLUMNS = {"folder_key", "parent_key", "status"}
REQUIRED_FILE_NON_NULL_COLUMNS = {
    "record_key",
    "path",
    "file_name",
    "relative_parent_dir",
    "etag_hex",
    "size",
    "status",
    "retries",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS job (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    target_parent_id TEXT NOT NULL,
    common_path TEXT NOT NULL,
    planning_complete INTEGER NOT NULL,
    total_files INTEGER NOT NULL,
    total_folders INTEGER NOT NULL,
    completed_count INTEGER NOT NULL,
    not_reusable_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    last_flush_at TEXT
);

CREATE TABLE IF NOT EXISTS folders (
    folder_key TEXT PRIMARY KEY,
    parent_key TEXT NOT NULL,
    remote_folder_id TEXT,
    status TEXT NOT NULL,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS files (
    record_key TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    relative_parent_dir TEXT NOT NULL,
    etag_hex TEXT NOT NULL,
    size INTEGER NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    retries INTEGER NOT NULL DEFAULT 0
);
"""


@dataclass
class ImportState:
    connection: sqlite3.Connection
    state_path: Path
    workers: int = 8
    folder_map: dict[str, str] = field(default_factory=dict)

    def _fetch_job_row(self) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT source_file, source_sha256, target_parent_id, common_path, planning_complete, total_files, total_folders, completed_count, not_reusable_count, failed_count, last_flush_at FROM job WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ValueError(MALFORMED_STATE_ERROR)
        return row

    @property
    def job_scope(self) -> dict[str, str]:
        row = self._fetch_job_row()
        return {
            "source_sha256": str(row[1]),
            "target_parent_id": str(row[2]),
            "common_path": str(row[3]),
        }

    @property
    def common_path(self) -> str:
        return self.job_scope["common_path"]

    @property
    def target_parent_id(self) -> str:
        return self.job_scope["target_parent_id"]

    @property
    def stats(self) -> dict[str, int]:
        row = self._fetch_job_row()
        return {
            "total": int(row[5]),
            "completed": int(row[7]),
            "not_reusable": int(row[8]),
            "failed": int(row[9]),
        }

    def close(self) -> None:
        self.connection.close()

    def fetch_status_counts(self) -> dict[str, int]:
        counts = {"pending": 0, "completed": 0, "not_reusable": 0, "failed": 0}
        rows = self.connection.execute("SELECT status, COUNT(*) FROM files GROUP BY status").fetchall()
        for status, count in rows:
            if status in counts:
                counts[str(status)] = int(count)
        return counts

    def fetch_folder_keys(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT folder_key FROM folders WHERE folder_key <> '' ORDER BY LENGTH(folder_key) - LENGTH(REPLACE(folder_key, '/', '')) ASC, folder_key ASC"
        ).fetchall()
        return [str(row[0]) for row in rows]

    def fetch_pending_folder_keys(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT folder_key FROM folders WHERE status = 'pending' ORDER BY LENGTH(folder_key) - LENGTH(REPLACE(folder_key, '/', '')) ASC, folder_key ASC"
        ).fetchall()
        return [str(row[0]) for row in rows]

    def refresh_folder_map(self) -> None:
        self.folder_map = _load_created_folder_map(
            connection=self.connection,
            target_parent_id=self.target_parent_id,
        )

    def fetch_pending_record_paths(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT path FROM files WHERE status = 'pending' ORDER BY path ASC"
        ).fetchall()
        return [str(row[0]) for row in rows]

    def count_pending_files(self, *, retry_failed: bool = False) -> int:
        statuses = ["pending"]
        if retry_failed:
            statuses.extend(["failed", "not_reusable"])
        placeholders = ", ".join("?" for _ in statuses)
        row = self.connection.execute(
            f"SELECT COUNT(*) FROM files WHERE status IN ({placeholders})",
            statuses,
        ).fetchone()
        return 0 if row is None else int(row[0])

    def count_pending_folders(self) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM folders WHERE status = 'pending'"
        ).fetchone()
        return 0 if row is None else int(row[0])

    def fetch_pending_records_batch(self, *, limit: int, offset: int = 0) -> list[SourceRecord]:
        rows = self.connection.execute(
            "SELECT record_key, etag_hex, size, path, file_name, relative_parent_dir FROM files WHERE status = 'pending' ORDER BY path ASC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [
            SourceRecord(
                key=str(record_key),
                etag=str(etag_hex),
                size=int(size),
                path=str(path),
                file_name=str(file_name),
                relative_parent_dir=str(relative_parent_dir),
            )
            for record_key, etag_hex, size, path, file_name, relative_parent_dir in rows
        ]

    def resolve_parent_file_id(self, *, relative_parent_dir: str) -> str:
        if self.common_path and relative_parent_dir:
            parent_key = f"{self.common_path.rstrip('/')}/{relative_parent_dir}".strip("/")
        elif self.common_path:
            parent_key = self.common_path.rstrip("/")
        else:
            parent_key = relative_parent_dir

        if not parent_key:
            return self.folder_map[""]
        try:
            return self.folder_map[parent_key]
        except KeyError as exc:
            raise RuntimeError(f"missing folder mapping for parent key: {parent_key}") from exc

    def is_planning_complete(self) -> bool:
        row = self._fetch_job_row()
        return bool(row[4])

    def reset_planning_rows(self) -> None:
        self.connection.execute("DELETE FROM folders")
        self.connection.execute("DELETE FROM files")
        self.connection.execute(
            "UPDATE job SET planning_complete = 0, total_files = 0, total_folders = 0, completed_count = 0, not_reusable_count = 0, failed_count = 0, last_flush_at = NULL WHERE singleton = 1"
        )
        self.connection.commit()
        self.folder_map = {"": self.target_parent_id}

    def finish_planning(
        self,
        *,
        source_sha256: str,
        common_path: str,
        total_files: int,
        total_folders: int,
    ) -> None:
        normalized_common_path = normalize_common_path(common_path)
        self.connection.execute(
            "UPDATE job SET source_sha256 = ?, common_path = ?, planning_complete = 1, total_files = ?, total_folders = ?, completed_count = 0, not_reusable_count = 0, failed_count = 0, last_flush_at = NULL WHERE singleton = 1",
            (source_sha256, normalized_common_path, total_files, total_folders),
        )
        self.connection.commit()

    def reset_retryable_rows(self) -> None:
        self.connection.execute(
            "UPDATE files SET status = 'pending', error = NULL, retries = 0 WHERE status IN ('failed', 'not_reusable')"
        )
        self.connection.execute(
            "UPDATE job SET not_reusable_count = 0, failed_count = 0 WHERE singleton = 1"
        )
        self.connection.commit()

    def flush(self, state_path: Path | None = None) -> None:
        del state_path
        for folder_key, remote_folder_id in self.folder_map.items():
            if folder_key == "":
                continue
            cursor = self.connection.execute(
                "UPDATE folders SET remote_folder_id = ?, status = 'created', last_error = NULL WHERE folder_key = ?",
                (remote_folder_id, folder_key),
            )
            if cursor.rowcount == 0:
                self.connection.execute(
                    "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
                    (
                        folder_key,
                        folder_key.rsplit("/", 1)[0] if "/" in folder_key else "",
                        remote_folder_id,
                        "created",
                        None,
                    ),
                )
        self.connection.execute(
            "UPDATE job SET last_flush_at = ? WHERE singleton = 1",
            (_utc_now_isoformat(),),
        )
        self.connection.commit()

    def flush_terminal_outcomes(self, outcomes: list[dict[str, str | int | None]]) -> None:
        completed_delta = 0
        not_reusable_delta = 0
        failed_delta = 0

        self.connection.execute("SAVEPOINT flush_terminal_outcomes")
        try:
            for outcome in outcomes:
                result = str(outcome["result"])
                record_key = str(outcome["record_key"])
                error = outcome["error"]
                retries = int(outcome["retries"])
                if result == "completed":
                    self.connection.execute(
                        "UPDATE files SET status = 'completed', error = NULL, retries = 0 WHERE record_key = ?",
                        (record_key,),
                    )
                    completed_delta += 1
                    continue
                if result == "not_reusable":
                    self.connection.execute(
                        "UPDATE files SET status = 'not_reusable', error = ?, retries = ? WHERE record_key = ?",
                        (error or "Reuse=false", retries, record_key),
                    )
                    not_reusable_delta += 1
                    continue
                if result == "failed":
                    self.connection.execute(
                        "UPDATE files SET status = 'failed', error = ?, retries = ? WHERE record_key = ?",
                        (error or "failed", retries, record_key),
                    )
                    failed_delta += 1
                    continue
                raise RuntimeError(f"unexpected terminal outcome: {result}")

            self.connection.execute(
                "UPDATE job SET completed_count = completed_count + ?, not_reusable_count = not_reusable_count + ?, failed_count = failed_count + ?, last_flush_at = ? WHERE singleton = 1",
                (completed_delta, not_reusable_delta, failed_delta, _utc_now_isoformat()),
            )
        except Exception:
            self.connection.execute("ROLLBACK TO SAVEPOINT flush_terminal_outcomes")
            self.connection.execute("RELEASE SAVEPOINT flush_terminal_outcomes")
            raise

        self.connection.execute("RELEASE SAVEPOINT flush_terminal_outcomes")
        self.connection.commit()

    def write_retry_export(self, output_path: Path) -> bool:
        rows = self.connection.execute(
            "SELECT path, etag_hex, size FROM files WHERE status IN ('failed', 'not_reusable') ORDER BY path ASC"
        ).fetchall()
        if not rows:
            if output_path.exists():
                output_path.unlink()
            return False

        total_size = sum(int(size) for _, _, size in rows)
        payload = {
            "scriptVersion": __version__,
            "exportVersion": "1.0",
            "usesBase62EtagsInExport": True,
            "commonPath": self.job_scope["common_path"],
            "totalFilesCount": len(rows),
            "totalSize": total_size,
            "formattedTotalSize": _format_size(total_size),
            "files": [
                {
                    "path": str(path),
                    "etag": _hex_to_base62(str(etag_hex)),
                    "size": str(int(size)),
                }
                for path, etag_hex, size in rows
            ],
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True


def _is_sqlite_file(state_path: Path) -> bool:
    if not state_path.is_file():
        return False
    with state_path.open("rb") as handle:
        return handle.read(len(SQLITE_HEADER)) == SQLITE_HEADER


def _fetch_table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _fetch_column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _validate_import_state_schema(connection: sqlite3.Connection) -> None:
    if not REQUIRED_TABLES.issubset(_fetch_table_names(connection)):
        raise ValueError(MALFORMED_STATE_ERROR)

    expected_columns_by_table = {
        "job": EXPECTED_JOB_COLUMNS,
        "folders": EXPECTED_FOLDER_COLUMNS,
        "files": EXPECTED_FILE_COLUMNS,
    }
    for table_name, expected_columns in expected_columns_by_table.items():
        if not expected_columns.issubset(_fetch_column_names(connection, table_name)):
            raise ValueError(MALFORMED_STATE_ERROR)


def _validate_import_state_rows(connection: sqlite3.Connection) -> None:
    job_rows = connection.execute("SELECT COUNT(*) FROM job").fetchone()
    if job_rows is None or int(job_rows[0]) != 1:
        raise ValueError(MALFORMED_STATE_ERROR)

    _validate_required_non_null_values(connection, "job", REQUIRED_JOB_NON_NULL_COLUMNS)
    _validate_required_non_null_values(connection, "folders", REQUIRED_FOLDER_NON_NULL_COLUMNS)
    _validate_required_non_null_values(connection, "files", REQUIRED_FILE_NON_NULL_COLUMNS)

    invalid_folder_status = connection.execute(
        "SELECT 1 FROM folders WHERE status IS NULL OR status NOT IN ('pending', 'created') LIMIT 1"
    ).fetchone()
    if invalid_folder_status is not None:
        raise ValueError(MALFORMED_STATE_ERROR)

    invalid_file_status = connection.execute(
        "SELECT 1 FROM files WHERE status IS NULL OR status NOT IN ('pending', 'completed', 'not_reusable', 'failed') LIMIT 1"
    ).fetchone()
    if invalid_file_status is not None:
        raise ValueError(MALFORMED_STATE_ERROR)

    _validate_planning_complete_job_counters(connection)
    _validate_planning_complete_folder_contract(connection)


def _validate_required_non_null_values(
    connection: sqlite3.Connection,
    table_name: str,
    required_columns: set[str],
) -> None:
    required_values = " OR ".join(
        f"{column_name} IS NULL" for column_name in sorted(required_columns)
    )
    null_value = connection.execute(
        f"SELECT 1 FROM {table_name} WHERE {required_values} LIMIT 1"
    ).fetchone()
    if null_value is not None:
        raise ValueError(MALFORMED_STATE_ERROR)


def _validate_planning_complete_folder_contract(connection: sqlite3.Connection) -> None:
    job_row = connection.execute(
        "SELECT common_path, planning_complete FROM job WHERE singleton = 1"
    ).fetchone()
    if job_row is None or not bool(job_row[1]):
        return

    try:
        common_path = normalize_common_path(str(job_row[0]))
    except ValueError as exc:
        raise ValueError(MALFORMED_STATE_ERROR) from exc

    required_folder_keys = set()
    if common_path:
        common_segments = common_path.rstrip("/").split("/")
        for index in range(len(common_segments)):
            required_folder_keys.add("/".join(common_segments[: index + 1]))

    file_rows = connection.execute(
        "SELECT relative_parent_dir FROM files ORDER BY path ASC"
    ).fetchall()
    for (relative_parent_dir,) in file_rows:
        relative_parent = str(relative_parent_dir)
        if not relative_parent:
            continue
        parent_segments = relative_parent.split("/")
        for index in range(len(parent_segments)):
            key_segments = []
            if common_path:
                key_segments.append(common_path.rstrip("/"))
            key_segments.append("/".join(parent_segments[: index + 1]))
            required_folder_keys.add("/".join(segment for segment in key_segments if segment))

    if not required_folder_keys:
        return

    persisted_folder_keys = {
        str(row[0])
        for row in connection.execute("SELECT folder_key FROM folders").fetchall()
    }
    if not required_folder_keys.issubset(persisted_folder_keys):
        raise ValueError(MALFORMED_STATE_ERROR)


def _validate_planning_complete_job_counters(connection: sqlite3.Connection) -> None:
    job_row = connection.execute(
        "SELECT planning_complete, total_files, total_folders, completed_count, not_reusable_count, failed_count FROM job WHERE singleton = 1"
    ).fetchone()
    if job_row is None or not bool(job_row[0]):
        return

    actual_total_files_row = connection.execute("SELECT COUNT(*) FROM files").fetchone()
    actual_total_folders_row = connection.execute(
        "SELECT COUNT(*) FROM folders WHERE folder_key <> ''"
    ).fetchone()
    actual_total_files = 0 if actual_total_files_row is None else int(actual_total_files_row[0])
    actual_total_folders = 0 if actual_total_folders_row is None else int(actual_total_folders_row[0])
    if actual_total_files != int(job_row[1]) or actual_total_folders != int(job_row[2]):
        raise ValueError(MALFORMED_STATE_ERROR)

    actual_counts = {
        "completed": 0,
        "not_reusable": 0,
        "failed": 0,
    }
    rows = connection.execute(
        "SELECT status, COUNT(*) FROM files WHERE status IN ('completed', 'not_reusable', 'failed') GROUP BY status"
    ).fetchall()
    for status, count in rows:
        actual_counts[str(status)] = int(count)

    if (
        actual_counts["completed"] != int(job_row[3])
        or actual_counts["not_reusable"] != int(job_row[4])
        or actual_counts["failed"] != int(job_row[5])
    ):
        raise ValueError(MALFORMED_STATE_ERROR)


def open_or_initialize_import_state(
    *,
    state_path: Path,
    source_file: str,
    source_sha256: str,
    target_parent_id: str,
    common_path: str,
) -> ImportState:
    normalized_common_path = normalize_common_path(common_path)
    state_exists = state_path.exists()
    if state_exists and not _is_sqlite_file(state_path):
        raise ValueError("unsupported state-file format")

    connection: sqlite3.Connection | None = None
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(state_path, check_same_thread=False)
        connection.execute("PRAGMA foreign_keys = OFF")

        if not state_exists:
            connection.executescript(SCHEMA_SQL)
            connection.execute(
                "INSERT INTO job (singleton, schema_version, source_file, source_sha256, target_parent_id, common_path, planning_complete, total_files, total_folders, completed_count, not_reusable_count, failed_count, last_flush_at) VALUES (1, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, NULL)",
                (SCHEMA_VERSION, source_file, source_sha256, target_parent_id, normalized_common_path),
            )
            connection.commit()
        else:
            _validate_import_state_schema(connection)
            _validate_import_state_rows(connection)

            row = connection.execute(
                "SELECT source_file, source_sha256, target_parent_id, common_path FROM job WHERE singleton = 1"
            ).fetchone()
            if row is None:
                raise ValueError(MALFORMED_STATE_ERROR)

            persisted_source_sha256 = str(row[1])
            persisted_target_parent_id = str(row[2])
            try:
                persisted_common_path = normalize_common_path(row[3])
            except ValueError as exc:
                raise ValueError(MALFORMED_STATE_ERROR) from exc
            if (
                persisted_source_sha256 != source_sha256
                or persisted_target_parent_id != target_parent_id
                or persisted_common_path != normalized_common_path
            ):
                raise ValueError(MISMATCH_ERROR)

        return ImportState(
            connection=connection,
            state_path=state_path,
            folder_map=_load_created_folder_map(
                connection=connection,
                target_parent_id=target_parent_id,
            ),
        )
    except ValueError:
        if connection is not None:
            connection.close()
        raise
    except sqlite3.DatabaseError as exc:
        if connection is not None:
            connection.close()
        raise ValueError(MALFORMED_STATE_ERROR) from exc


def _hex_to_base62(etag_hex: str) -> str:
    number = int(etag_hex, 16)
    if number == 0:
        return "0"

    chars: list[str] = []
    while number > 0:
        number, remainder = divmod(number, 62)
        chars.append(BASE62_ALPHABET[remainder])
    return "".join(reversed(chars))


def _format_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    unit_index = 0
    while unit_index < len(units) - 1 and value >= 1024.0:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{size} B"
    return f"{value:.2f} {units[unit_index]}"


def _load_created_folder_map(*, connection: sqlite3.Connection, target_parent_id: str) -> dict[str, str]:
    folder_map = {"": target_parent_id}
    rows = connection.execute(
        "SELECT folder_key, remote_folder_id FROM folders WHERE status = 'created' AND remote_folder_id IS NOT NULL ORDER BY folder_key ASC"
    ).fetchall()
    for folder_key, remote_folder_id in rows:
        folder_map[str(folder_key)] = str(remote_folder_id)
    return folder_map


def _utc_now_isoformat() -> str:
    return datetime.now(timezone.utc).isoformat()
