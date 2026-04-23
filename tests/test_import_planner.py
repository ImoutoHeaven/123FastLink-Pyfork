import json

import pytest

from fastlink_transfer.import_planner import (
    inspect_export_scope,
    plan_import_into_state,
    rebuild_incomplete_plan_if_needed,
)
from fastlink_transfer.import_state import open_or_initialize_import_state


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

    with pytest.raises(ValueError, match="duplicate normalized path"):
        plan_import_into_state(export_path=export_path, state=state, scope=scope)


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
