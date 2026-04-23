import json

import pytest

from fastlink_transfer.import_planner import (
    inspect_export_scope,
    plan_import_into_new_state,
    plan_import_into_state,
    rebuild_incomplete_plan_if_needed,
)
from fastlink_transfer.import_state import (
    _hex_to_base62,
    initialize_import_state_for_planning,
    open_or_initialize_import_state,
)


def test_plan_import_writes_files_and_folders_without_returning_full_record_list(tmp_path):
    export_path = tmp_path / "sample.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [
                    {"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "1983/06/a.txt"},
                    {"etag": "fedcba9876543210fedcba9876543210", "size": "2", "path": "1983/07/b.txt"},
                ],
            }
        ),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )

    summary = plan_import_into_state(export_path=export_path, state=state, scope=scope)

    assert summary.total_files == 2
    assert summary.common_path == "Demo/"
    assert state.fetch_folder_keys() == ["Demo", "Demo/1983", "Demo/1983/06", "Demo/1983/07"]
    assert state.fetch_pending_record_paths() == ["1983/06/a.txt", "1983/07/b.txt"]


def test_plan_import_persists_common_path_root_folder_when_export_has_no_files(tmp_path):
    export_path = tmp_path / "empty.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )

    summary = plan_import_into_state(export_path=export_path, state=state, scope=scope)

    assert summary.total_files == 0
    assert summary.common_path == "Demo/"
    assert state.fetch_folder_keys() == ["Demo"]
    assert state.fetch_pending_record_paths() == []
    assert state.stats == {
        "total": 0,
        "completed": 0,
        "not_reusable": 0,
        "failed": 0,
    }


def test_plan_import_rejects_duplicate_normalized_paths(tmp_path):
    export_path = tmp_path / "duplicate.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [
                    {"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "1983\\06\\a.txt"},
                    {"etag": "fedcba9876543210fedcba9876543210", "size": "2", "path": "1983/06/a.txt"},
                ],
            }
        ),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )

    summary = plan_import_into_state(export_path=export_path, state=state, scope=scope)

    rows = state.connection.execute(
        "SELECT path, status, error FROM files ORDER BY record_key ASC"
    ).fetchall()

    assert summary.total_files == 2
    assert state.fetch_pending_record_paths() == ["1983/06/a.txt"]
    assert state.stats == {
        "total": 2,
        "completed": 0,
        "not_reusable": 0,
        "failed": 1,
    }
    assert rows == [
        ("1983/06/a.txt", "pending", None),
        ("1983/06/a.txt", "failed", "duplicate normalized file path"),
    ]


def test_plan_import_duplicate_normalized_paths_are_exported_in_retry_payload(tmp_path):
    export_path = tmp_path / "duplicate.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [
                    {"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "1983/06/a.txt"},
                    {"etag": "fedcba9876543210fedcba9876543210", "size": "2", "path": "1983\\06\\a.txt"},
                ],
            }
        ),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )

    plan_import_into_state(export_path=export_path, state=state, scope=scope)
    retry_export_path = tmp_path / "retry.json"

    assert state.write_retry_export(retry_export_path) is True
    payload = json.loads(retry_export_path.read_text(encoding="utf-8"))
    assert payload["files"] == [
        {
            "path": "1983/06/a.txt",
            "etag": _hex_to_base62("fedcba9876543210fedcba9876543210"),
            "size": "2",
        }
    ]


def test_plan_import_into_state_uses_supplied_scope_without_rescanning_metadata(monkeypatch, tmp_path):
    export_path = tmp_path / "sample.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "a.txt"}],
            }
        ),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )
    monkeypatch.setattr(
        "fastlink_transfer.import_planner._inspect_export_metadata",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("planning should use supplied scope metadata")),
    )

    summary = plan_import_into_state(export_path=export_path, state=state, scope=scope)

    assert summary.total_files == 1
    assert state.fetch_pending_record_paths() == ["a.txt"]


def test_plan_import_into_new_state_cleans_up_state_file_when_interrupted(tmp_path, monkeypatch):
    export_path = tmp_path / "sample.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "a.txt"}],
            }
        ),
        encoding="utf-8",
    )
    state_path = tmp_path / "import.state.sqlite3"
    state = initialize_import_state_for_planning(
        state_path=state_path,
        source_file=str(export_path),
        target_parent_id="12345678",
    )
    monkeypatch.setattr(
        "fastlink_transfer.import_planner.plan_import_into_state",
        lambda **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        plan_import_into_new_state(export_path=export_path, state=state)

    assert state_path.exists() is False


def test_plan_import_rebuilds_incomplete_planning_state(tmp_path):
    export_path = tmp_path / "sample.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "1983/06/a.txt"}],
            }
        ),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("stale", "old.txt", "old.txt", "", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
    )
    state.connection.execute("UPDATE job SET planning_complete = 0, total_files = 1")
    state.connection.commit()

    summary = rebuild_incomplete_plan_if_needed(export_path=export_path, state=state, scope=scope)

    assert summary.rebuilt is True
    assert state.is_planning_complete() is True
    assert state.fetch_pending_record_paths() == ["1983/06/a.txt"]
