from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path

from fastlink_transfer.importer import (
    normalize_common_path,
    normalize_relative_path,
    parse_size,
    validate_hex_etag,
)


SCHEMA_VERSION = 1
SQLITE_HEADER = b"SQLite format 3\x00"
MALFORMED_CHECK_STATE_ERROR = "malformed check-state error"
MISMATCH_ERROR = "mismatch error"
REQUIRED_TABLES = {"job", "expected_dirs", "expected_files", "remote_dirs", "remote_files"}
EXPECTED_JOB_COLUMNS = {
    "singleton",
    "schema_version",
    "source_file",
    "source_sha256",
    "target_parent_id",
    "common_path",
    "last_compare_mode",
    "planning_complete",
    "remote_scan_complete",
    "delta_finalize_complete",
    "remote_root_missing",
    "expected_files",
    "expected_dirs",
    "remote_files",
    "remote_dirs",
    "delta_files",
    "missing_dirs",
    "last_flush_at",
}
EXPECTED_EXPECTED_DIR_COLUMNS = {"target_relative_path"}
EXPECTED_EXPECTED_FILE_COLUMNS = {
    "export_relative_path",
    "target_relative_path",
    "etag_hex",
    "size",
}
EXPECTED_REMOTE_DIR_COLUMNS = {"target_relative_path", "remote_dir_id", "status"}
EXPECTED_REMOTE_FILE_COLUMNS = {
    "export_relative_path",
    "target_relative_path",
    "etag_hex",
    "size",
}
REQUIRED_JOB_NON_NULL_COLUMNS = {
    "schema_version",
    "source_file",
    "source_sha256",
    "target_parent_id",
    "common_path",
    "planning_complete",
    "remote_scan_complete",
    "delta_finalize_complete",
    "remote_root_missing",
    "expected_files",
    "expected_dirs",
    "remote_files",
    "remote_dirs",
    "delta_files",
    "missing_dirs",
}
REQUIRED_EXPECTED_DIR_NON_NULL_COLUMNS = {"target_relative_path"}
REQUIRED_EXPECTED_FILE_NON_NULL_COLUMNS = {
    "export_relative_path",
    "target_relative_path",
    "etag_hex",
    "size",
}
REQUIRED_REMOTE_DIR_NON_NULL_COLUMNS = {"target_relative_path", "remote_dir_id", "status"}
REQUIRED_REMOTE_FILE_NON_NULL_COLUMNS = {
    "export_relative_path",
    "target_relative_path",
    "etag_hex",
    "size",
}
ALLOWED_REMOTE_DIR_STATUSES = {"pending", "inflight", "completed"}
ALLOWED_COMPARE_MODES = {"exist_only", "with_checksum"}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS job (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    target_parent_id TEXT NOT NULL,
    common_path TEXT NOT NULL,
    last_compare_mode TEXT,
    planning_complete INTEGER NOT NULL,
    remote_scan_complete INTEGER NOT NULL,
    delta_finalize_complete INTEGER NOT NULL,
    remote_root_missing INTEGER NOT NULL,
    expected_files INTEGER NOT NULL,
    expected_dirs INTEGER NOT NULL,
    remote_files INTEGER NOT NULL,
    remote_dirs INTEGER NOT NULL,
    delta_files INTEGER NOT NULL,
    missing_dirs INTEGER NOT NULL,
    last_flush_at TEXT
);

CREATE TABLE IF NOT EXISTS expected_dirs (
    target_relative_path TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS expected_files (
    export_relative_path TEXT NOT NULL UNIQUE,
    target_relative_path TEXT PRIMARY KEY,
    etag_hex TEXT NOT NULL,
    size INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS remote_dirs (
    target_relative_path TEXT PRIMARY KEY,
    remote_dir_id TEXT,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS remote_files (
    export_relative_path TEXT NOT NULL UNIQUE,
    target_relative_path TEXT PRIMARY KEY,
    etag_hex TEXT NOT NULL,
    size INTEGER NOT NULL
);
"""


@dataclass
class CheckState:
    connection: sqlite3.Connection
    state_path: Path
    workers: int = 8
    flush_every: int = 100

    def _fetch_job_row(self) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM job WHERE singleton = 1").fetchone()
        if row is None:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)
        return row

    @property
    def job_scope(self) -> dict[str, str]:
        row = self._fetch_job_row()
        return {
            "source_sha256": str(row["source_sha256"]),
            "target_parent_id": str(row["target_parent_id"]),
            "common_path": str(row["common_path"]),
        }

    @property
    def phase(self) -> dict[str, bool]:
        row = self._fetch_job_row()
        return {
            "planning_complete": bool(row["planning_complete"]),
            "remote_scan_complete": bool(row["remote_scan_complete"]),
            "delta_finalize_complete": bool(row["delta_finalize_complete"]),
        }

    @property
    def job_flags(self) -> dict[str, bool]:
        row = self._fetch_job_row()
        return {"remote_root_missing": bool(row["remote_root_missing"])}

    @property
    def job_metadata(self) -> dict[str, str | None]:
        row = self._fetch_job_row()
        last_compare_mode = row["last_compare_mode"]
        last_flush_at = row["last_flush_at"]
        return {
            "last_compare_mode": None if last_compare_mode is None else str(last_compare_mode),
            "last_flush_at": None if last_flush_at is None else str(last_flush_at),
        }

    @property
    def summary_counters(self) -> dict[str, int]:
        row = self._fetch_job_row()
        return {
            "expected_files": int(row["expected_files"]),
            "expected_dirs": int(row["expected_dirs"]),
            "remote_files": int(row["remote_files"]),
            "remote_dirs": int(row["remote_dirs"]),
            "delta_files": int(row["delta_files"]),
            "missing_dirs": int(row["missing_dirs"]),
        }

    def close(self) -> None:
        self.connection.close()

    def _now_timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def is_planning_complete(self) -> bool:
        return self.phase["planning_complete"]

    def reset_incomplete_planning_rows(self) -> None:
        if self.is_planning_complete():
            return

        self.connection.execute("DELETE FROM expected_dirs")
        self.connection.execute("DELETE FROM expected_files")
        self.connection.execute("DELETE FROM remote_dirs")
        self.connection.execute("DELETE FROM remote_files")
        self.connection.execute(
            "UPDATE job SET planning_complete = 0, remote_scan_complete = 0, delta_finalize_complete = 0, remote_root_missing = 0, expected_files = 0, expected_dirs = 0, remote_files = 0, remote_dirs = 0, delta_files = 0, missing_dirs = 0, last_compare_mode = NULL, last_flush_at = NULL WHERE singleton = 1"
        )
        self.connection.commit()

    def add_expected_dir(self, target_relative_path: str) -> bool:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO expected_dirs (target_relative_path) VALUES (?)",
            (target_relative_path,),
        )
        return cursor.rowcount > 0

    def add_expected_file(
        self,
        *,
        export_relative_path: str,
        target_relative_path: str,
        etag_hex: str,
        size: int,
    ) -> None:
        self.connection.execute(
            "INSERT INTO expected_files (export_relative_path, target_relative_path, etag_hex, size) VALUES (?, ?, ?, ?)",
            (export_relative_path, target_relative_path, etag_hex, size),
        )

    def finish_planning(self, *, expected_files: int, expected_dirs: int) -> None:
        self.connection.execute(
            "UPDATE job SET planning_complete = 1, remote_scan_complete = 0, delta_finalize_complete = 0, remote_root_missing = 0, expected_files = ?, expected_dirs = ?, remote_files = 0, remote_dirs = 0, delta_files = 0, missing_dirs = 0, last_compare_mode = NULL, last_flush_at = NULL WHERE singleton = 1",
            (expected_files, expected_dirs),
        )
        self.connection.commit()

    def recover_inflight_remote_dirs(self) -> None:
        self.connection.execute("UPDATE remote_dirs SET status = 'pending' WHERE status = 'inflight'")
        self.connection.commit()

    def fetch_remote_dir_statuses(self) -> dict[str, str]:
        rows = self.connection.execute(
            "SELECT target_relative_path, status FROM remote_dirs ORDER BY target_relative_path ASC"
        ).fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    def fetch_expected_dirs(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT target_relative_path FROM expected_dirs ORDER BY LENGTH(target_relative_path) - LENGTH(REPLACE(target_relative_path, '/', '')) ASC, target_relative_path ASC"
        ).fetchall()
        return [str(row[0]) for row in rows]

    def fetch_expected_file(self, export_relative_path: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT export_relative_path, target_relative_path, etag_hex, size FROM expected_files WHERE export_relative_path = ?",
            (export_relative_path,),
        ).fetchone()
        if row is None:
            raise KeyError(export_relative_path)
        return row

    def has_remote_index_rows(self) -> bool:
        row = self.connection.execute("SELECT 1 FROM remote_dirs LIMIT 1").fetchone()
        return row is not None

    def has_pending_remote_dirs(self) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM remote_dirs WHERE status = 'pending' LIMIT 1"
        ).fetchone()
        return row is not None

    def fetch_pending_remote_dirs(self):
        return self.connection.execute(
            "SELECT target_relative_path, remote_dir_id FROM remote_dirs WHERE status = 'pending' ORDER BY LENGTH(target_relative_path) - LENGTH(REPLACE(target_relative_path, '/', '')) ASC, target_relative_path ASC"
        )

    def fetch_next_pending_remote_dir(
        self, *, exclude_paths: set[str] | frozenset[str] | None = None
    ) -> sqlite3.Row | None:
        parameters: list[str] = []
        query = (
            "SELECT target_relative_path, remote_dir_id FROM remote_dirs WHERE status = 'pending'"
        )
        normalized_excludes = sorted(exclude_paths or ())
        if normalized_excludes:
            placeholders = ", ".join("?" for _ in normalized_excludes)
            query = f"{query} AND target_relative_path NOT IN ({placeholders})"
            parameters.extend(normalized_excludes)
        query = (
            f"{query} ORDER BY LENGTH(target_relative_path) - LENGTH(REPLACE(target_relative_path, '/', '')) ASC, "
            "target_relative_path ASC LIMIT 1"
        )
        return self.connection.execute(query, tuple(parameters)).fetchone()

    def fetch_remote_dirs(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT target_relative_path FROM remote_dirs ORDER BY LENGTH(target_relative_path) - LENGTH(REPLACE(target_relative_path, '/', '')) ASC, target_relative_path ASC"
        ).fetchall()
        return [str(row[0]) for row in rows]

    def fetch_remote_file(self, export_relative_path: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT export_relative_path, target_relative_path, etag_hex, size FROM remote_files WHERE export_relative_path = ?",
            (export_relative_path,),
        ).fetchone()
        if row is None:
            raise KeyError(export_relative_path)
        return row

    def seed_resolved_remote_dirs(
        self,
        *,
        completed_dirs: list[tuple[str, str]],
        root_dir: tuple[str, str] | None,
    ) -> None:
        inserted_remote_dirs = 0
        with self.connection:
            for target_relative_path, remote_dir_id in completed_dirs:
                existing = self.connection.execute(
                    "SELECT 1 FROM remote_dirs WHERE target_relative_path = ?",
                    (target_relative_path,),
                ).fetchone()
                if existing is None:
                    self.connection.execute(
                        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, 'completed')",
                        (target_relative_path, remote_dir_id),
                    )
                    inserted_remote_dirs += 1
                else:
                    self.connection.execute(
                        "UPDATE remote_dirs SET remote_dir_id = ?, status = 'completed' WHERE target_relative_path = ?",
                        (remote_dir_id, target_relative_path),
                    )
            if root_dir is not None:
                existing = self.connection.execute(
                    "SELECT 1 FROM remote_dirs WHERE target_relative_path = ?",
                    (root_dir[0],),
                ).fetchone()
                if existing is None:
                    self.connection.execute(
                        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, 'pending')",
                        root_dir,
                    )
                    inserted_remote_dirs += 1
                else:
                    self.connection.execute(
                        "UPDATE remote_dirs SET remote_dir_id = ?, status = 'pending' WHERE target_relative_path = ?",
                        (root_dir[1], root_dir[0]),
                    )
            self.connection.execute(
                "UPDATE job SET remote_scan_complete = 0, delta_finalize_complete = 0, remote_root_missing = 0, remote_dirs = remote_dirs + ?, delta_files = 0, missing_dirs = 0, last_compare_mode = NULL, last_flush_at = ? WHERE singleton = 1",
                (inserted_remote_dirs, self._now_timestamp()),
            )

    def mark_remote_dir_inflight(self, target_relative_path: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE remote_dirs SET status = 'inflight' WHERE target_relative_path = ?",
                (target_relative_path,),
            )
            self.connection.execute(
                "UPDATE job SET last_flush_at = ? WHERE singleton = 1",
                (self._now_timestamp(),),
            )

    def commit_remote_scan_directory(
        self,
        *,
        target_relative_path: str,
        child_dirs: list[tuple[str, str]],
        file_rows: list[tuple[str, str, str, int]],
        mark_complete: bool = True,
    ) -> None:
        inserted_remote_dirs = 0
        inserted_remote_files = 0
        with self.connection:
            for child_target_relative_path, remote_dir_id in child_dirs:
                existing = self.connection.execute(
                    "SELECT status FROM remote_dirs WHERE target_relative_path = ?",
                    (child_target_relative_path,),
                ).fetchone()
                if existing is None:
                    self.connection.execute(
                        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, 'pending')",
                        (child_target_relative_path, remote_dir_id),
                    )
                    inserted_remote_dirs += 1
                elif str(existing[0]) != "completed":
                    self.connection.execute(
                        "UPDATE remote_dirs SET remote_dir_id = ?, status = 'pending' WHERE target_relative_path = ?",
                        (remote_dir_id, child_target_relative_path),
                    )
            for export_relative_path, file_target_relative_path, etag_hex, size in file_rows:
                insert_cursor = self.connection.execute(
                    "INSERT OR IGNORE INTO remote_files (export_relative_path, target_relative_path, etag_hex, size) VALUES (?, ?, ?, ?)",
                    (export_relative_path, file_target_relative_path, etag_hex, size),
                )
                if insert_cursor.rowcount > 0:
                    inserted_remote_files += 1
                else:
                    self.connection.execute(
                        "UPDATE remote_files SET export_relative_path = ?, etag_hex = ?, size = ? WHERE target_relative_path = ?",
                        (export_relative_path, etag_hex, size, file_target_relative_path),
                    )
            if mark_complete:
                self.connection.execute(
                    "UPDATE remote_dirs SET status = 'completed' WHERE target_relative_path = ?",
                    (target_relative_path,),
                )
            self.connection.execute(
                "UPDATE job SET remote_files = remote_files + ?, remote_dirs = remote_dirs + ?, last_flush_at = ? WHERE singleton = 1",
                (inserted_remote_files, inserted_remote_dirs, self._now_timestamp()),
            )

    def finish_remote_scan(self, *, remote_root_missing: bool) -> None:
        with self.connection:
            if remote_root_missing:
                self.connection.execute(
                    "UPDATE job SET remote_scan_complete = 1, delta_finalize_complete = 0, remote_root_missing = 1, remote_files = 0, remote_dirs = 0, delta_files = 0, missing_dirs = 0, last_compare_mode = NULL, last_flush_at = ? WHERE singleton = 1",
                    (self._now_timestamp(),),
                )
            else:
                self.connection.execute(
                    "UPDATE job SET remote_scan_complete = 1, delta_finalize_complete = 0, remote_root_missing = 0, delta_files = 0, missing_dirs = 0, last_compare_mode = NULL, last_flush_at = ? WHERE singleton = 1",
                    (self._now_timestamp(),),
                )

    def fetch_delta_candidate_rows(self, *, compare_mode: str):
        if compare_mode == "exist_only":
            query = """
                SELECT e.export_relative_path, e.target_relative_path, e.etag_hex, e.size
                FROM expected_files AS e
                LEFT JOIN remote_files AS r ON r.target_relative_path = e.target_relative_path
                WHERE r.target_relative_path IS NULL
                ORDER BY e.export_relative_path ASC
            """
        else:
            query = """
                SELECT e.export_relative_path, e.target_relative_path, e.etag_hex, e.size
                FROM expected_files AS e
                LEFT JOIN remote_files AS r ON r.target_relative_path = e.target_relative_path
                WHERE r.target_relative_path IS NULL OR r.etag_hex != e.etag_hex OR r.size != e.size
                ORDER BY e.export_relative_path ASC
            """
        return self.connection.execute(query)

    def summarize_delta_candidate_rows(self, *, compare_mode: str) -> tuple[int, int]:
        if compare_mode == "exist_only":
            query = """
                SELECT COUNT(*), COALESCE(SUM(e.size), 0)
                FROM expected_files AS e
                LEFT JOIN remote_files AS r ON r.target_relative_path = e.target_relative_path
                WHERE r.target_relative_path IS NULL
            """
        else:
            query = """
                SELECT COUNT(*), COALESCE(SUM(e.size), 0)
                FROM expected_files AS e
                LEFT JOIN remote_files AS r ON r.target_relative_path = e.target_relative_path
                WHERE r.target_relative_path IS NULL OR r.etag_hex != e.etag_hex OR r.size != e.size
            """
        row = self.connection.execute(query).fetchone()
        if row is None:
            return 0, 0
        return int(row[0]), int(row[1])

        query = """
            SELECT e.export_relative_path, e.target_relative_path, e.etag_hex, e.size
            FROM expected_files AS e
            LEFT JOIN remote_files AS r ON r.target_relative_path = e.target_relative_path
            WHERE r.target_relative_path IS NULL OR r.etag_hex != e.etag_hex OR r.size != e.size
            ORDER BY e.export_relative_path ASC
        """
        return self.connection.execute(query)

    def count_missing_expected_dirs(self) -> int:
        if self.job_flags["remote_root_missing"]:
            return self.summary_counters["expected_dirs"]
        row = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM expected_dirs AS e
            LEFT JOIN remote_dirs AS r ON r.target_relative_path = e.target_relative_path
            WHERE r.target_relative_path IS NULL
            """
        ).fetchone()
        return 0 if row is None else int(row[0])

    def finish_delta_finalize(
        self,
        *,
        compare_mode: str,
        delta_files: int,
        missing_dirs: int,
    ) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE job SET last_compare_mode = ?, delta_files = ?, missing_dirs = ?, delta_finalize_complete = 1, last_flush_at = ? WHERE singleton = 1",
                (compare_mode, delta_files, missing_dirs, self._now_timestamp()),
            )

    def flush_remote_progress(self, *, state_path: Path) -> None:
        del state_path
        with self.connection:
            self.connection.execute(
                "UPDATE job SET last_flush_at = ? WHERE singleton = 1",
                (self._now_timestamp(),),
            )


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
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)


def _validate_check_state_schema(connection: sqlite3.Connection) -> None:
    if not REQUIRED_TABLES.issubset(_fetch_table_names(connection)):
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    expected_columns_by_table = {
        "job": EXPECTED_JOB_COLUMNS,
        "expected_dirs": EXPECTED_EXPECTED_DIR_COLUMNS,
        "expected_files": EXPECTED_EXPECTED_FILE_COLUMNS,
        "remote_dirs": EXPECTED_REMOTE_DIR_COLUMNS,
        "remote_files": EXPECTED_REMOTE_FILE_COLUMNS,
    }
    for table_name, expected_columns in expected_columns_by_table.items():
        if not expected_columns.issubset(_fetch_column_names(connection, table_name)):
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)


def _validate_check_state_rows(connection: sqlite3.Connection) -> None:
    job_rows = connection.execute("SELECT COUNT(*) FROM job").fetchone()
    if job_rows is None or int(job_rows[0]) != 1:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    schema_version_row = connection.execute(
        "SELECT schema_version FROM job WHERE singleton = 1"
    ).fetchone()
    if schema_version_row is None or int(schema_version_row[0]) != SCHEMA_VERSION:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    _validate_required_non_null_values(connection, "job", REQUIRED_JOB_NON_NULL_COLUMNS)
    _validate_required_non_null_values(connection, "expected_dirs", REQUIRED_EXPECTED_DIR_NON_NULL_COLUMNS)
    _validate_required_non_null_values(connection, "expected_files", REQUIRED_EXPECTED_FILE_NON_NULL_COLUMNS)
    _validate_required_non_null_values(connection, "remote_dirs", REQUIRED_REMOTE_DIR_NON_NULL_COLUMNS)
    _validate_required_non_null_values(connection, "remote_files", REQUIRED_REMOTE_FILE_NON_NULL_COLUMNS)

    invalid_remote_dir_status = connection.execute(
        "SELECT 1 FROM remote_dirs WHERE status IS NULL OR status NOT IN ('pending', 'inflight', 'completed') LIMIT 1"
    ).fetchone()
    if invalid_remote_dir_status is not None:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    _validate_expected_data_contract(connection)
    _validate_planning_complete_expected_dir_contract(connection)
    _validate_planning_complete_summary_counters(connection)
    _validate_remote_index_contract(connection)
    _validate_finalize_metadata_contract(connection)


def _normalize_persisted_relative_path(value: object) -> str:
    path_value = str(value)
    try:
        normalized_path = normalize_relative_path(path_value)
    except ValueError as exc:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc
    if normalized_path != path_value:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)
    return normalized_path


def _normalize_persisted_file_size(value: object) -> int:
    try:
        return parse_size(value)
    except ValueError as exc:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc


def _iter_common_prefix_dirs(common_path: str):
    common_prefix = common_path.rstrip("/")
    if not common_prefix:
        return
    segments = common_prefix.split("/")
    for index in range(len(segments)):
        yield "/".join(segments[: index + 1])


def _iter_parent_dir_paths(target_relative_path: str):
    segments = target_relative_path.split("/")[:-1]
    for index in range(len(segments)):
        yield "/".join(segments[: index + 1])


def _reset_temp_path_table(connection: sqlite3.Connection, table_name: str) -> None:
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.execute(f"CREATE TEMP TABLE {table_name} (target_relative_path TEXT PRIMARY KEY)")


def _populate_temp_path_table(
    connection: sqlite3.Connection,
    table_name: str,
    path_iterable,
) -> None:
    connection.executemany(
        f"INSERT OR IGNORE INTO {table_name} (target_relative_path) VALUES (?)",
        ((path,) for path in path_iterable),
    )


def _validate_persisted_file_coordinates(
    *,
    export_relative_path: object,
    target_relative_path: object,
    common_prefix: str,
) -> None:
    normalized_export_path = _normalize_persisted_relative_path(export_relative_path)
    normalized_target_path = _normalize_persisted_relative_path(target_relative_path)

    expected_target_path = normalized_export_path
    if common_prefix:
        expected_target_path = f"{common_prefix}/{normalized_export_path}"
    if normalized_target_path != expected_target_path:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)


def _validate_expected_data_contract(connection: sqlite3.Connection) -> None:
    job_row = connection.execute(
        "SELECT common_path FROM job WHERE singleton = 1"
    ).fetchone()
    if job_row is None:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    try:
        common_path = normalize_common_path(str(job_row[0]))
    except ValueError as exc:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc

    common_prefix = common_path.rstrip("/")
    for (target_relative_path,) in connection.execute("SELECT target_relative_path FROM expected_dirs"):
        _normalize_persisted_relative_path(target_relative_path)

    for export_relative_path, target_relative_path, etag_hex, size in connection.execute(
        "SELECT export_relative_path, target_relative_path, etag_hex, size FROM expected_files"
    ):
        _validate_persisted_file_coordinates(
            export_relative_path=export_relative_path,
            target_relative_path=target_relative_path,
            common_prefix=common_prefix,
        )
        try:
            validate_hex_etag(str(etag_hex))
        except ValueError as exc:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc
        _normalize_persisted_file_size(size)


def _validate_planning_complete_expected_dir_contract(connection: sqlite3.Connection) -> None:
    job_row = connection.execute(
        "SELECT common_path, planning_complete FROM job WHERE singleton = 1"
    ).fetchone()
    if job_row is None or not bool(job_row[1]):
        return

    try:
        common_path = normalize_common_path(str(job_row[0]))
    except ValueError as exc:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc

    temp_table_name = "temp_required_expected_dirs"
    _reset_temp_path_table(connection, temp_table_name)
    _populate_temp_path_table(connection, temp_table_name, _iter_common_prefix_dirs(common_path))
    _populate_temp_path_table(
        connection,
        temp_table_name,
        (
            parent_dir_path
            for (target_relative_path,) in connection.execute(
                "SELECT target_relative_path FROM expected_files"
            )
            for parent_dir_path in _iter_parent_dir_paths(str(target_relative_path))
        ),
    )

    missing_required_dir = connection.execute(
        f"""
        SELECT 1
        FROM {temp_table_name} AS required_dirs
        LEFT JOIN expected_dirs AS persisted_dirs
            ON persisted_dirs.target_relative_path = required_dirs.target_relative_path
        WHERE persisted_dirs.target_relative_path IS NULL
        LIMIT 1
        """
    ).fetchone()
    extra_persisted_dir = connection.execute(
        f"""
        SELECT 1
        FROM expected_dirs AS persisted_dirs
        LEFT JOIN {temp_table_name} AS required_dirs
            ON required_dirs.target_relative_path = persisted_dirs.target_relative_path
        WHERE required_dirs.target_relative_path IS NULL
        LIMIT 1
        """
    ).fetchone()
    if missing_required_dir is not None or extra_persisted_dir is not None:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)


def _validate_planning_complete_summary_counters(connection: sqlite3.Connection) -> None:
    job_row = connection.execute(
        "SELECT planning_complete, expected_files, expected_dirs, remote_files, remote_dirs, delta_files, missing_dirs FROM job WHERE singleton = 1"
    ).fetchone()
    if job_row is None or not bool(job_row[0]):
        return

    expected_files_row = connection.execute("SELECT COUNT(*) FROM expected_files").fetchone()
    expected_dirs_row = connection.execute("SELECT COUNT(*) FROM expected_dirs").fetchone()
    remote_files_row = connection.execute("SELECT COUNT(*) FROM remote_files").fetchone()
    remote_dirs_row = connection.execute("SELECT COUNT(*) FROM remote_dirs").fetchone()

    actual_expected_files = 0 if expected_files_row is None else int(expected_files_row[0])
    actual_expected_dirs = 0 if expected_dirs_row is None else int(expected_dirs_row[0])
    actual_remote_files = 0 if remote_files_row is None else int(remote_files_row[0])
    actual_remote_dirs = 0 if remote_dirs_row is None else int(remote_dirs_row[0])

    if (
        actual_expected_files != int(job_row[1])
        or actual_expected_dirs != int(job_row[2])
        or actual_remote_files != int(job_row[3])
        or actual_remote_dirs != int(job_row[4])
    ):
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)


def _validate_remote_index_contract(connection: sqlite3.Connection) -> None:
    job_row = connection.execute(
        "SELECT common_path, remote_scan_complete, remote_root_missing FROM job WHERE singleton = 1"
    ).fetchone()
    if job_row is None:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    try:
        common_path = normalize_common_path(str(job_row[0]))
    except ValueError as exc:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc

    remote_scan_complete = bool(job_row[1])
    remote_root_missing = bool(job_row[2])
    common_prefix = common_path.rstrip("/")
    has_remote_dirs = connection.execute("SELECT 1 FROM remote_dirs LIMIT 1").fetchone() is not None
    has_persisted_remote_files = (
        connection.execute("SELECT 1 FROM remote_files LIMIT 1").fetchone() is not None
    )

    if remote_root_missing:
        # A missing commonPath subtree is a completed terminal state with an intentionally
        # empty remote index so reruns can reuse/finalize without rescanning.
        if not remote_scan_complete or not common_prefix:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)
        if has_remote_dirs or has_persisted_remote_files:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)
        return

    temp_table_name = "temp_required_remote_dirs"
    _reset_temp_path_table(connection, temp_table_name)
    _populate_temp_path_table(connection, temp_table_name, _iter_common_prefix_dirs(common_path))
    if remote_scan_complete or has_persisted_remote_files:
        _populate_temp_path_table(connection, temp_table_name, (common_prefix,))

    for target_relative_path, _remote_dir_id, status in connection.execute(
        "SELECT target_relative_path, remote_dir_id, status FROM remote_dirs"
    ):
        path_value = str(target_relative_path)
        if path_value:
            _normalize_persisted_relative_path(path_value)
        elif common_prefix:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)
        if remote_scan_complete and str(status) != "completed":
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    for export_relative_path, target_relative_path, etag_hex, size in connection.execute(
        "SELECT export_relative_path, target_relative_path, etag_hex, size FROM remote_files"
    ):
        target_path_value = str(target_relative_path)
        try:
            _validate_persisted_file_coordinates(
                export_relative_path=export_relative_path,
                target_relative_path=target_relative_path,
                common_prefix=common_prefix,
            )
        except ValueError as exc:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc

        try:
            validate_hex_etag(str(etag_hex))
        except ValueError as exc:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)
        _normalize_persisted_file_size(size)

        _populate_temp_path_table(
            connection,
            temp_table_name,
            _iter_parent_dir_paths(target_path_value),
        )

    if (remote_scan_complete or has_persisted_remote_files) and connection.execute(
        f"""
        SELECT 1
        FROM {temp_table_name} AS required_dirs
        LEFT JOIN remote_dirs AS persisted_dirs
            ON persisted_dirs.target_relative_path = required_dirs.target_relative_path
        WHERE persisted_dirs.target_relative_path IS NULL
        LIMIT 1
        """
    ).fetchone() is not None:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)


def _validate_finalize_metadata_contract(connection: sqlite3.Connection) -> None:
    job_row = connection.execute(
        "SELECT last_compare_mode, delta_finalize_complete, delta_files, missing_dirs, remote_scan_complete, remote_root_missing, expected_dirs FROM job WHERE singleton = 1"
    ).fetchone()
    if job_row is None:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    last_compare_mode = job_row[0]
    delta_finalize_complete = bool(job_row[1])
    remote_scan_complete = bool(job_row[4])
    remote_root_missing = bool(job_row[5])
    expected_dirs = int(job_row[6])

    if last_compare_mode is not None and str(last_compare_mode) not in ALLOWED_COMPARE_MODES:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)
    if delta_finalize_complete and last_compare_mode is None:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)
    if not delta_finalize_complete:
        return
    if not remote_scan_complete:
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)

    compare_mode = str(last_compare_mode)
    if compare_mode == "exist_only":
        delta_row = connection.execute(
            """
            SELECT COUNT(*)
            FROM expected_files AS e
            LEFT JOIN remote_files AS r ON r.target_relative_path = e.target_relative_path
            WHERE r.target_relative_path IS NULL
            """
        ).fetchone()
    else:
        delta_row = connection.execute(
            """
            SELECT COUNT(*)
            FROM expected_files AS e
            LEFT JOIN remote_files AS r ON r.target_relative_path = e.target_relative_path
            WHERE r.target_relative_path IS NULL OR r.etag_hex != e.etag_hex OR r.size != e.size
            """
        ).fetchone()
    expected_delta_files = 0 if delta_row is None else int(delta_row[0])

    if remote_root_missing:
        expected_missing_dirs = expected_dirs
    else:
        missing_dirs_row = connection.execute(
            """
            SELECT COUNT(*)
            FROM expected_dirs AS e
            LEFT JOIN remote_dirs AS r ON r.target_relative_path = e.target_relative_path
            WHERE r.target_relative_path IS NULL
            """
        ).fetchone()
        expected_missing_dirs = 0 if missing_dirs_row is None else int(missing_dirs_row[0])

    if expected_delta_files != int(job_row[2]) or expected_missing_dirs != int(job_row[3]):
        raise ValueError(MALFORMED_CHECK_STATE_ERROR)


def _connect(state_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(state_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def _initialize_new_state(
    *,
    state_path: Path,
    source_file: str,
    source_sha256: str,
    target_parent_id: str,
    common_path: str,
) -> CheckState:
    connection: sqlite3.Connection | None = None
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        connection = _connect(state_path)
        connection.executescript(SCHEMA_SQL)
        connection.execute(
            "INSERT INTO job (singleton, schema_version, source_file, source_sha256, target_parent_id, common_path, last_compare_mode, planning_complete, remote_scan_complete, delta_finalize_complete, remote_root_missing, expected_files, expected_dirs, remote_files, remote_dirs, delta_files, missing_dirs, last_flush_at) VALUES (1, ?, ?, ?, ?, ?, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL)",
            (
                SCHEMA_VERSION,
                source_file,
                source_sha256,
                target_parent_id,
                common_path,
            ),
        )
        connection.commit()
        return CheckState(connection=connection, state_path=state_path)
    except BaseException:
        if connection is not None:
            connection.close()
        try:
            if state_path.exists():
                os.unlink(state_path)
        except OSError:
            pass
        raise


def open_or_initialize_check_state(
    *,
    state_path: Path,
    source_file: str,
    source_sha256: str,
    target_parent_id: str,
    common_path: str,
) -> CheckState:
    normalized_common_path = normalize_common_path(common_path)
    state_exists = state_path.exists()
    if state_exists and not _is_sqlite_file(state_path):
        raise ValueError("unsupported state-file format")

    if not state_exists:
        return _initialize_new_state(
            state_path=state_path,
            source_file=source_file,
            source_sha256=source_sha256,
            target_parent_id=target_parent_id,
            common_path=normalized_common_path,
        )

    connection: sqlite3.Connection | None = None
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        connection = _connect(state_path)
        _validate_check_state_schema(connection)
        _validate_check_state_rows(connection)

        row = connection.execute(
            "SELECT source_sha256, target_parent_id, common_path FROM job WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR)

        try:
            persisted_common_path = normalize_common_path(row["common_path"])
        except ValueError as exc:
            raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc

        if (
            str(row["source_sha256"]) != source_sha256
            or str(row["target_parent_id"]) != target_parent_id
            or persisted_common_path != normalized_common_path
        ):
            raise ValueError(MISMATCH_ERROR)

        return CheckState(connection=connection, state_path=state_path)
    except ValueError:
        if connection is not None:
            connection.close()
        raise
    except sqlite3.DatabaseError as exc:
        if connection is not None:
            connection.close()
        raise ValueError(MALFORMED_CHECK_STATE_ERROR) from exc
