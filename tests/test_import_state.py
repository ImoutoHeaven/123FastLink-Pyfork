import json
import os
import sqlite3
from pathlib import Path

import pytest

from fastlink_transfer import __version__
from fastlink_transfer.import_state import (
    MALFORMED_STATE_ERROR,
    SQLITE_HEADER,
    _is_sqlite_file,
    initialize_import_state_for_planning,
    open_or_initialize_import_state,
)


def _create_nullable_import_state_db(state_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(state_path)
    connection.execute(
        "CREATE TABLE job (singleton INTEGER PRIMARY KEY, schema_version INTEGER, source_file TEXT, source_sha256 TEXT, target_parent_id TEXT, common_path TEXT, planning_complete INTEGER, total_files INTEGER, total_folders INTEGER, completed_count INTEGER, not_reusable_count INTEGER, failed_count INTEGER, last_flush_at TEXT)"
    )
    connection.execute(
        "CREATE TABLE folders (folder_key TEXT, parent_key TEXT, remote_folder_id TEXT, status TEXT, last_error TEXT)"
    )
    connection.execute(
        "CREATE TABLE files (record_key TEXT, path TEXT, file_name TEXT, relative_parent_dir TEXT, etag_hex TEXT, size INTEGER, status TEXT, error TEXT, retries INTEGER)"
    )
    connection.execute(
        "INSERT INTO job (singleton, schema_version, source_file, source_sha256, target_parent_id, common_path, planning_complete, total_files, total_folders, completed_count, not_reusable_count, failed_count, last_flush_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1, "/tmp/export.json", "abc", "12345678", "Demo/", 0, 0, 0, 0, 0, 0, None),
    )
    return connection


def test_open_or_initialize_import_state_creates_sqlite_job_schema(tmp_path):
    state_path = tmp_path / "import.state.sqlite3"

    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )

    assert state_path.exists()
    assert state.job_scope == {
        "source_sha256": "abc",
        "target_parent_id": "12345678",
        "common_path": "Demo/",
    }
    assert state.stats == {
        "total": 0,
        "completed": 0,
        "not_reusable": 0,
        "failed": 0,
    }


def test_open_or_initialize_import_state_reopens_existing_sqlite_state_for_matching_scope(tmp_path):
    state_path = tmp_path / "import.state.sqlite3"
    first = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    first.close()

    reopened = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/copied-export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )

    assert reopened.job_scope == {
        "source_sha256": "abc",
        "target_parent_id": "12345678",
        "common_path": "Demo/",
    }


def test_initialize_import_state_for_planning_cleans_up_partial_file_on_keyboard_interrupt(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "import.state.sqlite3"
    real_connect = sqlite3.connect

    class InterruptingConnection:
        def __init__(self, path: Path):
            self._path = path

        def execute(self, sql, params=()):
            if str(sql).startswith("PRAGMA"):
                return None
            raise AssertionError(f"unexpected execute: {sql}")

        def executescript(self, sql):
            self._path.write_bytes(b"partial")
            raise KeyboardInterrupt()

        def commit(self):
            raise AssertionError("commit should not run after interrupt")

        def close(self):
            return None

    def fake_connect(path, check_same_thread=False):
        if Path(path) == state_path:
            return InterruptingConnection(state_path)
        return real_connect(path, check_same_thread=check_same_thread)

    monkeypatch.setattr(sqlite3, "connect", fake_connect)

    with pytest.raises(KeyboardInterrupt):
        initialize_import_state_for_planning(
            state_path=state_path,
            source_file="/tmp/export.json",
            target_parent_id="12345678",
        )

    assert state_path.exists() is False
    assert list(tmp_path.iterdir()) == []


def test_open_or_initialize_import_state_cleans_up_new_state_when_metadata_update_is_interrupted(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "import.state.sqlite3"
    original_initializer = initialize_import_state_for_planning

    class InterruptingImportState:
        def __init__(self, wrapped):
            self.connection = self
            self.state_path = wrapped.state_path
            self.folder_map = wrapped.folder_map

        def execute(self, sql, params=()):
            if str(sql).startswith("UPDATE job SET source_sha256"):
                raise KeyboardInterrupt()
            raise AssertionError(f"unexpected execute: {sql}")

        def commit(self):
            raise AssertionError("commit should not run after interrupt")

        def close(self):
            return None

    def fake_initializer(**kwargs):
        wrapped = original_initializer(**kwargs)
        return InterruptingImportState(wrapped)

    monkeypatch.setattr(
        "fastlink_transfer.import_state.initialize_import_state_for_planning",
        fake_initializer,
    )

    with pytest.raises(KeyboardInterrupt):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )

    assert state_path.exists() is False


def test_open_or_initialize_import_state_rejects_non_sqlite_state_file(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match=r"^unsupported state-file format$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_import_state_rejects_malformed_sqlite_state_file(tmp_path):
    state_path = tmp_path / "broken.state.sqlite3"
    state_path.write_bytes(b"SQLite format 3\x00garbage")

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_import_state_rejects_existing_sqlite_without_import_job_row(tmp_path):
    state_path = tmp_path / "foreign.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=tmp_path / "seed.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.close()

    connection = sqlite3.connect(state_path)
    connection.execute("CREATE TABLE something_else (id INTEGER PRIMARY KEY)")
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )

    connection = sqlite3.connect(state_path)
    table_names = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    connection.close()

    assert table_names == {"something_else"}


def test_open_or_initialize_import_state_rejects_existing_sqlite_with_damaged_import_schema(tmp_path):
    state_path = tmp_path / "damaged.state.sqlite3"
    connection = sqlite3.connect(state_path)
    connection.execute(
        "CREATE TABLE job (singleton INTEGER PRIMARY KEY, source_file TEXT, source_sha256 TEXT, target_parent_id TEXT, common_path TEXT)"
    )
    connection.execute("CREATE TABLE folders (folder_key TEXT PRIMARY KEY, parent_key TEXT NOT NULL)")
    connection.execute("CREATE TABLE files (record_key TEXT PRIMARY KEY, path TEXT NOT NULL)")
    connection.execute(
        "INSERT INTO job (singleton, source_file, source_sha256, target_parent_id, common_path) VALUES (1, '/tmp/export.json', 'abc', '12345678', 'Demo/')"
    )
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_is_sqlite_file_reads_only_header_bytes(tmp_path, monkeypatch):
    state_path = tmp_path / "header.state.sqlite3"
    state_path.write_bytes(SQLITE_HEADER + b"payload")
    read_sizes = []
    original_open = Path.open

    def tracking_open(self, *args, **kwargs):
        handle = original_open(self, *args, **kwargs)
        if self != state_path:
            return handle

        original_read = handle.read

        def tracking_read(size=-1):
            read_sizes.append(size)
            return original_read(size)

        handle.read = tracking_read
        return handle

    monkeypatch.setattr(Path, "open", tracking_open)

    assert _is_sqlite_file(state_path) is True
    assert read_sizes == [len(SQLITE_HEADER)]


def test_open_or_initialize_import_state_rejects_invalid_persisted_common_path_as_malformed(tmp_path):
    state_path = tmp_path / "import.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.execute("UPDATE job SET common_path = ? WHERE singleton = 1", ("Demo//",))
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


@pytest.mark.parametrize(
    ("table_name", "insert_sql", "params"),
    [
        (
            "folders",
            "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
            ("Demo", "", None, "broken", None),
        ),
        (
            "files",
            "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("k1", "1983/06/a.txt", "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", 1, "broken", None, 0),
        ),
    ],
)
def test_open_or_initialize_import_state_rejects_invalid_persisted_status_values(
    tmp_path,
    table_name,
    insert_sql,
    params,
):
    state_path = tmp_path / f"invalid-{table_name}.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.execute(insert_sql, params)
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


@pytest.mark.parametrize(
    ("state_path_name", "folders_sql", "files_sql", "insert_sql", "params"),
    [
        (
            "null-folders",
            "CREATE TABLE folders (folder_key TEXT PRIMARY KEY, parent_key TEXT, remote_folder_id TEXT, status TEXT, last_error TEXT)",
            "CREATE TABLE files (record_key TEXT PRIMARY KEY, path TEXT, file_name TEXT, relative_parent_dir TEXT, etag_hex TEXT, size INTEGER, status TEXT, error TEXT, retries INTEGER)",
            "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
            ("Demo", "", None, None, None),
        ),
        (
            "null-files",
            "CREATE TABLE folders (folder_key TEXT PRIMARY KEY, parent_key TEXT, remote_folder_id TEXT, status TEXT, last_error TEXT)",
            "CREATE TABLE files (record_key TEXT PRIMARY KEY, path TEXT, file_name TEXT, relative_parent_dir TEXT, etag_hex TEXT, size INTEGER, status TEXT, error TEXT, retries INTEGER)",
            "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("k1", "1983/06/a.txt", "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", 1, None, None, 0),
        ),
    ],
)
def test_open_or_initialize_import_state_rejects_null_persisted_status_values(
    tmp_path,
    state_path_name,
    folders_sql,
    files_sql,
    insert_sql,
    params,
):
    state_path = tmp_path / f"{state_path_name}.sqlite3"
    connection = sqlite3.connect(state_path)
    connection.execute(
        "CREATE TABLE job (singleton INTEGER PRIMARY KEY, schema_version INTEGER, source_file TEXT, source_sha256 TEXT, target_parent_id TEXT, common_path TEXT, planning_complete INTEGER, total_files INTEGER, total_folders INTEGER, completed_count INTEGER, not_reusable_count INTEGER, failed_count INTEGER, last_flush_at TEXT)"
    )
    connection.execute(folders_sql)
    connection.execute(files_sql)
    connection.execute(
        "INSERT INTO job (singleton, schema_version, source_file, source_sha256, target_parent_id, common_path, planning_complete, total_files, total_folders, completed_count, not_reusable_count, failed_count, last_flush_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1, "/tmp/export.json", "abc", "12345678", "Demo/", 0, 0, 0, 0, 0, 0, None),
    )
    connection.execute(insert_sql, params)
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_import_state_rejects_null_job_required_fields(tmp_path):
    state_path = tmp_path / "null-job.state.sqlite3"
    connection = _create_nullable_import_state_db(state_path)
    connection.execute("DELETE FROM job")
    connection.execute(
        "INSERT INTO job (singleton, schema_version, source_file, source_sha256, target_parent_id, common_path, planning_complete, total_files, total_folders, completed_count, not_reusable_count, failed_count, last_flush_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1, "/tmp/export.json", None, "12345678", "Demo/", 0, 0, 0, 0, 0, 0, None),
    )
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


@pytest.mark.parametrize(
    "params",
    [
        (None, "", None, "pending", None),
        ("Demo", None, None, "pending", None),
    ],
)
def test_open_or_initialize_import_state_rejects_null_required_folder_fields(tmp_path, params):
    state_path = tmp_path / "null-folder-required-field.sqlite3"
    connection = _create_nullable_import_state_db(state_path)
    connection.execute(
        "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        params,
    )
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


@pytest.mark.parametrize(
    "params",
    [
        (None, "1983/06/a.txt", "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
        ("k1", None, "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
        ("k1", "1983/06/a.txt", None, "1983/06", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
        ("k1", "1983/06/a.txt", "a.txt", None, "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
        ("k1", "1983/06/a.txt", "a.txt", "1983/06", None, 1, "pending", None, 0),
        ("k1", "1983/06/a.txt", "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", None, "pending", None, 0),
        ("k1", "1983/06/a.txt", "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", 1, "pending", None, None),
    ],
)
def test_open_or_initialize_import_state_rejects_null_required_file_fields(tmp_path, params):
    state_path = tmp_path / "null-file-required-field.sqlite3"
    connection = _create_nullable_import_state_db(state_path)
    connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params,
    )
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_import_state_rejects_duplicate_job_rows(tmp_path):
    state_path = tmp_path / "duplicate-job.state.sqlite3"
    connection = sqlite3.connect(state_path)
    connection.execute(
        "CREATE TABLE job (singleton INTEGER, schema_version INTEGER, source_file TEXT, source_sha256 TEXT, target_parent_id TEXT, common_path TEXT, planning_complete INTEGER, total_files INTEGER, total_folders INTEGER, completed_count INTEGER, not_reusable_count INTEGER, failed_count INTEGER, last_flush_at TEXT)"
    )
    connection.execute(
        "CREATE TABLE folders (folder_key TEXT PRIMARY KEY, parent_key TEXT, remote_folder_id TEXT, status TEXT, last_error TEXT)"
    )
    connection.execute(
        "CREATE TABLE files (record_key TEXT PRIMARY KEY, path TEXT, file_name TEXT, relative_parent_dir TEXT, etag_hex TEXT, size INTEGER, status TEXT, error TEXT, retries INTEGER)"
    )
    connection.executemany(
        "INSERT INTO job (singleton, schema_version, source_file, source_sha256, target_parent_id, common_path, planning_complete, total_files, total_folders, completed_count, not_reusable_count, failed_count, last_flush_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "/tmp/export.json", "abc", "12345678", "Demo/", 0, 0, 0, 0, 0, 0, None),
            (2, 1, "/tmp/export.json", "abc", "12345678", "Demo/", 0, 0, 0, 0, 0, 0, None),
        ],
    )
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_import_state_rejects_scope_mismatch(tmp_path):
    state_path = tmp_path / "import.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.close()

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Other/",
        )


def test_open_or_initialize_import_state_rejects_planning_complete_state_missing_required_folder_mappings(
    tmp_path,
):
    state_path = tmp_path / "damaged-planning-complete.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "k1",
            "1983/06/a.txt",
            "a.txt",
            "1983/06",
            "0123456789abcdef0123456789abcdef",
            1,
            "pending",
            None,
            0,
        ),
    )
    state.finish_planning(
        source_sha256="abc",
        common_path="Demo/",
        total_files=1,
        total_folders=0,
    )
    state.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_import_state_accepts_planning_complete_state_with_root_mapping_row(
    tmp_path,
):
    state_path = tmp_path / "planning-complete-with-root-row.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.executemany(
        "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        [
            ("", "", "12345678", "created", None),
            ("Demo", "", "folder-1", "created", None),
        ],
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "k1",
            "a.txt",
            "a.txt",
            "",
            "0123456789abcdef0123456789abcdef",
            1,
            "pending",
            None,
            0,
        ),
    )
    state.finish_planning(
        source_sha256="abc",
        common_path="Demo/",
        total_files=1,
        total_folders=1,
    )
    state.close()

    reopened = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )

    assert reopened.fetch_folder_keys() == ["Demo"]


def test_open_or_initialize_import_state_rejects_planning_complete_state_with_split_brain_job_counters(
    tmp_path,
):
    state_path = tmp_path / "split-brain.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.execute(
        "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        ("Demo", "", "folder-1", "created", None),
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "k1",
            "failed.txt",
            "failed.txt",
            "",
            "0123456789abcdef0123456789abcdef",
            1,
            "failed",
            "HTTP 500",
            2,
        ),
    )
    state.finish_planning(
        source_sha256="abc",
        common_path="Demo/",
        total_files=1,
        total_folders=1,
    )
    state.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_import_state_rejects_planning_complete_state_with_split_brain_planning_totals(
    tmp_path,
):
    state_path = tmp_path / "split-brain-planning-totals.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.execute(
        "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        ("Demo", "", "folder-1", "created", None),
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "k1",
            "a.txt",
            "a.txt",
            "",
            "0123456789abcdef0123456789abcdef",
            1,
            "pending",
            None,
            0,
        ),
    )
    state.finish_planning(
        source_sha256="abc",
        common_path="Demo/",
        total_files=1,
        total_folders=1,
    )
    state.connection.execute(
        "UPDATE job SET total_files = 99, total_folders = 77 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=rf"^{MALFORMED_STATE_ERROR}$"):
        open_or_initialize_import_state(
            state_path=state_path,
            source_file="/tmp/export.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_retry_failed_resets_failed_and_not_reusable_rows_to_pending(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.executemany(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "k1",
                "1983/06/a.txt",
                "a.txt",
                "1983/06",
                "0123456789abcdef0123456789abcdef",
                1,
                "failed",
                "HTTP 500",
                2,
            ),
            (
                "k2",
                "1983/07/b.txt",
                "b.txt",
                "1983/07",
                "fedcba9876543210fedcba9876543210",
                2,
                "not_reusable",
                "Reuse=false",
                0,
            ),
        ],
    )
    state.connection.commit()

    state.reset_retryable_rows()

    rows = state.connection.execute(
        "SELECT record_key, status, error, retries FROM files ORDER BY record_key ASC"
    ).fetchall()

    assert state.fetch_status_counts() == {
        "pending": 2,
        "completed": 0,
        "not_reusable": 0,
        "failed": 0,
    }
    assert rows == [
        ("k1", "pending", None, 0),
        ("k2", "pending", None, 0),
    ]


def test_retry_failed_keeps_duplicate_normalized_file_path_rows_terminal(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.executemany(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "k1",
                "1983/06/a.txt",
                "a.txt",
                "1983/06",
                "0123456789abcdef0123456789abcdef",
                1,
                "failed",
                "duplicate normalized file path",
                0,
            ),
            (
                "k2",
                "1983/07/b.txt",
                "b.txt",
                "1983/07",
                "fedcba9876543210fedcba9876543210",
                2,
                "failed",
                "HTTP 500",
                2,
            ),
        ],
    )
    state.connection.execute(
        "UPDATE job SET total_files = 2, failed_count = 2 WHERE singleton = 1"
    )
    state.connection.commit()

    state.reset_retryable_rows()

    rows = state.connection.execute(
        "SELECT record_key, status, error, retries FROM files ORDER BY record_key ASC"
    ).fetchall()

    assert rows == [
        ("k1", "failed", "duplicate normalized file path", 0),
        ("k2", "pending", None, 0),
    ]
    assert state.count_pending_files(retry_failed=True) == 1


def test_planning_helpers_manage_folder_and_pending_row_views(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.executemany(
        "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        [
            ("Demo/1983/06", "Demo/1983", None, "pending", None),
            ("Demo", "", None, "pending", None),
            ("Demo/1983", "Demo", None, "pending", None),
        ],
    )
    state.connection.executemany(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("k2", "1983/07/b.txt", "b.txt", "1983/07", "fedcba9876543210fedcba9876543210", 2, "completed", None, 0),
            ("k1", "1983/06/a.txt", "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
            ("k3", "1983/05/c.txt", "c.txt", "1983/05", "11111111111111111111111111111111", 3, "pending", None, 0),
        ],
    )
    state.connection.commit()

    assert state.fetch_folder_keys() == ["Demo", "Demo/1983", "Demo/1983/06"]
    assert state.fetch_pending_record_paths() == ["1983/05/c.txt", "1983/06/a.txt"]


def test_planning_helpers_finish_and_reset_planning_rows(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.execute(
        "INSERT INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        ("Demo", "", None, "pending", None),
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("k1", "1983/06/a.txt", "a.txt", "1983/06", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
    )
    state.connection.commit()

    state.finish_planning(
        source_sha256="def",
        common_path="Demo/",
        total_files=1,
        total_folders=1,
    )

    assert state.is_planning_complete() is True
    assert state.job_scope == {
        "source_sha256": "def",
        "target_parent_id": "12345678",
        "common_path": "Demo/",
    }
    assert state.stats == {
        "total": 1,
        "completed": 0,
        "not_reusable": 0,
        "failed": 0,
    }

    state.reset_planning_rows()

    assert state.is_planning_complete() is False
    assert state.fetch_folder_keys() == []
    assert state.fetch_pending_record_paths() == []
    assert state.stats == {
        "total": 0,
        "completed": 0,
        "not_reusable": 0,
        "failed": 0,
    }


def test_write_retry_export_outputs_export_json_payload(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.executemany(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "k1",
                "1983/06/a.txt",
                "a.txt",
                "1983/06",
                "0123456789abcdef0123456789abcdef",
                1,
                "failed",
                "HTTP 500",
                2,
            ),
            (
                "k2",
                "1983/07/b.txt",
                "b.txt",
                "1983/07",
                "fedcba9876543210fedcba9876543210",
                2,
                "not_reusable",
                "Reuse=false",
                0,
            ),
        ],
    )
    state.connection.commit()
    output_path = tmp_path / "retry.json"

    wrote = state.write_retry_export(output_path)

    assert wrote is True
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["scriptVersion"] == __version__
    assert payload["exportVersion"] == "1.0"
    assert payload["usesBase62EtagsInExport"] is True
    assert payload["commonPath"] == "Demo/"
    assert payload["totalFilesCount"] == 2
    assert payload["totalSize"] == 3
    assert payload["formattedTotalSize"] == "3 B"
    assert [item["path"] for item in payload["files"]] == ["1983/06/a.txt", "1983/07/b.txt"]


def test_write_retry_export_removes_stale_output_when_no_unsuccessful_rows(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    output_path = tmp_path / "retry.json"
    output_path.write_text("stale", encoding="utf-8")

    wrote = state.write_retry_export(output_path)

    assert wrote is False
    assert output_path.exists() is False
