import json
import os
from pathlib import Path

import pytest

import fastlink_transfer.check_state as check_state_module
from fastlink_transfer.batch_check import finalize_delta_export, plan_batch_check_job
from fastlink_transfer.check_state import open_or_initialize_check_state
from fastlink_transfer.import_planner import inspect_export_scope
from fastlink_transfer.importer import decode_base62_to_hex, load_export_file


def _open_check_state(
    tmp_path: Path,
    *,
    common_path: str = "Demo/",
    source_sha256: str = "abc",
):
    return open_or_initialize_check_state(
        state_path=tmp_path / "demo.check-state.sqlite3",
        source_file="/tmp/demo.json",
        source_sha256=source_sha256,
        target_parent_id="12345678",
        common_path=common_path,
    )


def _open_check_state_for_export(tmp_path: Path, export_path: Path):
    scope = inspect_export_scope(export_path=export_path)
    return open_or_initialize_check_state(
        state_path=tmp_path / "demo.check-state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )


def _seed_remote_dir_rows(
    state,
    *,
    pending: list[tuple[str, str]],
    inflight: list[tuple[str, str]],
    completed: list[tuple[str, str]],
) -> None:
    rows = [
        *( (path, remote_dir_id, "pending") for path, remote_dir_id in pending ),
        *( (path, remote_dir_id, "inflight") for path, remote_dir_id in inflight ),
        *( (path, remote_dir_id, "completed") for path, remote_dir_id in completed ),
    ]
    state.connection.executemany(
        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, ?)",
        rows,
    )
    state.connection.commit()


def _seed_partial_planning_rows(state) -> None:
    state.connection.execute(
        "INSERT INTO expected_dirs (target_relative_path) VALUES (?)",
        ("stale-partial",),
    )
    state.connection.execute(
        "INSERT INTO expected_files (export_relative_path, target_relative_path, etag_hex, size) VALUES (?, ?, ?, ?)",
        ("stale-partial/a.txt", "Demo/stale-partial/a.txt", "0" * 32, 1),
    )
    state.connection.execute(
        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, ?)",
        ("stale-partial", "200", "completed"),
    )
    state.connection.execute(
        "INSERT INTO remote_files (export_relative_path, target_relative_path, etag_hex, size) VALUES (?, ?, ?, ?)",
        ("stale-partial/a.txt", "Demo/stale-partial/a.txt", "1" * 32, 2),
    )
    state.connection.execute(
        "UPDATE job SET planning_complete = 0, remote_scan_complete = 1, delta_finalize_complete = 1, expected_files = 1, expected_dirs = 1, remote_files = 1, remote_dirs = 1, delta_files = 1, missing_dirs = 1"
    )
    state.connection.commit()


def _count_stale_partial_rows(state) -> int:
    tables_and_columns = [
        ("expected_dirs", "target_relative_path"),
        ("expected_files", "export_relative_path"),
        ("remote_dirs", "target_relative_path"),
        ("remote_files", "export_relative_path"),
    ]
    total = 0
    for table_name, column_name in tables_and_columns:
        row = state.connection.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} LIKE 'stale-partial%'"
        ).fetchone()
        total += 0 if row is None else int(row[0])
    return total


class _FetchallGuardCursor:
    def __init__(self, wrapped, *, forbid_fetchall: bool, error_message: str):
        self._wrapped = wrapped
        self._forbid_fetchall = forbid_fetchall
        self._error_message = error_message

    def __iter__(self):
        return iter(self._wrapped)

    def fetchone(self):
        return self._wrapped.fetchone()

    def fetchall(self):
        if self._forbid_fetchall:
            raise AssertionError(self._error_message)
        return self._wrapped.fetchall()

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class _FetchallGuardConnection:
    def __init__(self, wrapped, *, forbidden_fragments: tuple[str, ...], error_message: str):
        self._wrapped = wrapped
        self._forbidden_fragments = forbidden_fragments
        self._error_message = error_message

    def execute(self, query, parameters=()):
        cursor = self._wrapped.execute(query, parameters)
        forbid_fetchall = isinstance(query, str) and any(
            fragment in query for fragment in self._forbidden_fragments
        )
        return _FetchallGuardCursor(
            cursor,
            forbid_fetchall=forbid_fetchall,
            error_message=self._error_message,
        )

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def seeded_check_state(
    tmp_path: Path,
    *,
    common_path: str,
    expected_files: list[tuple[str, str, str, int]],
    remote_files: list[tuple[str, str, str, int]],
):
    state = _open_check_state(tmp_path, common_path=common_path)
    expected_dirs: set[str] = set()
    remote_dirs: set[str] = set()

    common_segments = common_path.rstrip("/").split("/") if common_path else []
    for index in range(len(common_segments)):
        expected_dirs.add("/".join(common_segments[: index + 1]))

    for export_relative_path, target_relative_path, etag_hex, size in expected_files:
        state.add_expected_file(
            export_relative_path=export_relative_path,
            target_relative_path=target_relative_path,
            etag_hex=etag_hex,
            size=size,
        )
        parent = target_relative_path.rsplit("/", 1)[0] if "/" in target_relative_path else ""
        while parent:
            expected_dirs.add(parent)
            parent = parent.rsplit("/", 1)[0] if "/" in parent else ""

    for directory_path in sorted(expected_dirs, key=lambda value: (value.count("/"), value)):
        state.add_expected_dir(directory_path)

    for export_relative_path, target_relative_path, etag_hex, size in remote_files:
        state.connection.execute(
            "INSERT INTO remote_files (export_relative_path, target_relative_path, etag_hex, size) VALUES (?, ?, ?, ?)",
            (export_relative_path, target_relative_path, etag_hex, size),
        )
        parent = target_relative_path.rsplit("/", 1)[0] if "/" in target_relative_path else ""
        while parent:
            remote_dirs.add(parent)
            parent = parent.rsplit("/", 1)[0] if "/" in parent else ""

    for directory_path in sorted(remote_dirs, key=lambda value: (value.count("/"), value)):
        state.connection.execute(
            "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, 'completed')",
            (directory_path, directory_path.replace("/", "-") or "root"),
        )

    state.connection.execute(
        "UPDATE job SET planning_complete = 1, remote_scan_complete = 1, expected_files = ?, expected_dirs = ?, remote_files = ?, remote_dirs = ?, remote_root_missing = 0 WHERE singleton = 1",
        (len(expected_files), len(expected_dirs), len(remote_files), len(remote_dirs)),
    )
    state.connection.commit()
    return state


def test_open_or_initialize_check_state_persists_job_scope(tmp_path):
    state = _open_check_state(tmp_path)

    assert state.job_scope == {
        "source_sha256": "abc",
        "target_parent_id": "12345678",
        "common_path": "Demo/",
    }
    assert state.phase["planning_complete"] is False
    assert state.phase["remote_scan_complete"] is False
    assert state.phase["delta_finalize_complete"] is False


def test_open_or_initialize_check_state_rejects_scope_mismatch(tmp_path):
    _open_check_state(tmp_path).close()

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        _open_check_state(tmp_path, common_path="Other/")


def test_recover_inflight_remote_dirs_moves_rows_back_to_pending(tmp_path):
    state = _open_check_state(tmp_path)
    _seed_remote_dir_rows(state, pending=[], inflight=[("Demo/a", "200")], completed=[])

    state.recover_inflight_remote_dirs()

    assert state.fetch_remote_dir_statuses() == {"Demo/a": "pending"}


def test_open_or_initialize_check_state_persists_phase_and_summary_metadata(tmp_path):
    state = _open_check_state(tmp_path)

    assert state.phase == {
        "planning_complete": False,
        "remote_scan_complete": False,
        "delta_finalize_complete": False,
    }
    assert state.job_metadata["last_compare_mode"] is None
    assert state.job_metadata["last_flush_at"] is None
    assert state.summary_counters == {
        "expected_files": 0,
        "expected_dirs": 0,
        "remote_files": 0,
        "remote_dirs": 0,
        "delta_files": 0,
        "missing_dirs": 0,
    }


def test_open_or_initialize_check_state_rejects_corrupt_sqlite_payload(tmp_path):
    state_path = tmp_path / "demo.check-state.sqlite3"
    state_path.write_text("not sqlite", encoding="utf-8")

    with pytest.raises(ValueError, match=r"^unsupported state-file format$|^malformed check-state error$"):
        open_or_initialize_check_state(
            state_path=state_path,
            source_file="/tmp/demo.json",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
        )


def test_open_or_initialize_check_state_rejects_unsupported_schema_version(tmp_path):
    state = _open_check_state(tmp_path)
    state.connection.execute("UPDATE job SET schema_version = ? WHERE singleton = 1", (999,))
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path)


def test_open_or_initialize_check_state_rejects_planning_complete_counter_split_brain(tmp_path):
    export_path = tmp_path / "demo.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0" * 32, "size": "5", "path": "a/b.txt"}],
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)
    state.connection.execute("UPDATE job SET expected_files = ? WHERE singleton = 1", (999,))
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state_for_export(tmp_path, export_path)


def test_open_or_initialize_check_state_rejects_planning_complete_missing_expected_dir_rows(tmp_path):
    export_path = tmp_path / "demo.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0" * 32, "size": "5", "path": "a/b.txt"}],
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)
    state.connection.execute(
        "DELETE FROM expected_dirs WHERE target_relative_path = ?",
        ("Demo/a",),
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state_for_export(tmp_path, export_path)


def test_open_or_initialize_check_state_rejects_planning_complete_extra_expected_dir_rows_with_empty_common_path(
    tmp_path,
):
    export_path = tmp_path / "demo.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)
    state.connection.execute(
        "INSERT INTO expected_dirs (target_relative_path) VALUES (?)",
        ("bogus",),
    )
    state.connection.execute(
        "UPDATE job SET expected_dirs = 1 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state_for_export(tmp_path, export_path)


def test_open_or_initialize_check_state_rejects_planning_complete_extra_expected_dir_rows_with_non_empty_common_path(
    tmp_path,
):
    export_path = tmp_path / "demo.json"
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
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)
    state.connection.execute(
        "INSERT INTO expected_dirs (target_relative_path) VALUES (?)",
        ("Demo/unexpected",),
    )
    state.connection.execute(
        "UPDATE job SET expected_dirs = 2 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state_for_export(tmp_path, export_path)


@pytest.mark.parametrize(
    ("description", "mutate_state"),
    [
        (
            "expected_dirs.target_relative_path='bad//dir'",
            lambda state: (
                state.connection.execute(
                    "INSERT INTO expected_dirs (target_relative_path) VALUES (?)",
                    ("bad//dir",),
                ),
                state.connection.execute(
                    "UPDATE job SET expected_dirs = expected_dirs + 1 WHERE singleton = 1"
                ),
            ),
        ),
        (
            "expected_files.export_relative_path='bad\\name.txt'",
            lambda state: state.connection.execute(
                "UPDATE expected_files SET export_relative_path = ?",
                ("bad\\name.txt",),
            ),
        ),
        (
            "expected_files.target_relative_path='Demo/bad//name.txt'",
            lambda state: state.connection.execute(
                "UPDATE expected_files SET target_relative_path = ?",
                ("Demo/bad//name.txt",),
            ),
        ),
        (
            "expected_files.etag_hex='bogus'",
            lambda state: state.connection.execute(
                "UPDATE expected_files SET etag_hex = ?",
                ("bogus",),
            ),
        ),
        (
            "expected_files.size=-1'",
            lambda state: state.connection.execute(
                "UPDATE expected_files SET size = ?",
                (-1,),
            ),
        ),
    ],
)
def test_open_or_initialize_check_state_rejects_malformed_expected_rows_on_reopen(
    tmp_path,
    description,
    mutate_state,
):
    export_path = tmp_path / "demo.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0" * 32, "size": "5", "path": "child/name.txt"}],
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)
    mutate_state(state)
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state_for_export(tmp_path, export_path)


def test_open_or_initialize_check_state_rejects_remote_file_with_invalid_etag_hex_on_reopen(
    tmp_path,
):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/clip.mkv", "Demo/1983/clip.mkv", "0" * 32, 9)],
        remote_files=[("1983/clip.mkv", "Demo/1983/clip.mkv", "1" * 32, 9)],
    )
    state.connection.execute(
        "UPDATE remote_files SET etag_hex = ?",
        ("bogus",),
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="Demo/")


def test_open_or_initialize_check_state_accepts_missing_remote_root_scan_state_on_reopen(
    tmp_path,
):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/Missing/",
        expected_files=[("1983/clip.mkv", "Demo/Missing/1983/clip.mkv", "0" * 32, 9)],
        remote_files=[],
    )
    state.connection.execute("DELETE FROM remote_dirs")
    state.connection.execute("DELETE FROM remote_files")
    state.connection.execute(
        "UPDATE job SET remote_scan_complete = 1, remote_root_missing = 1, remote_files = 0, remote_dirs = 0 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    reopened = _open_check_state(tmp_path, common_path="Demo/Missing/")
    output_path = tmp_path / "delta" / "demo.delta.export.json"

    result = finalize_delta_export(
        state=reopened,
        output_path=output_path,
        compare_mode="exist_only",
    )

    assert reopened.phase["remote_scan_complete"] is True
    assert reopened.job_flags["remote_root_missing"] is True
    assert result.delta_files == 1
    assert result.missing_dirs == 3
    assert result.reused_remote_index is True
    assert output_path.exists() is True


def test_open_or_initialize_check_state_reopens_large_state_without_fetchall_validation(
    tmp_path, monkeypatch
):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[
            (f"1983/file-{index}.txt", f"Demo/1983/file-{index}.txt", "0" * 32, index)
            for index in range(3)
        ],
        remote_files=[
            (f"1983/file-{index}.txt", f"Demo/1983/file-{index}.txt", "1" * 32, index)
            for index in range(3)
        ],
    )
    state.close()
    real_connect = check_state_module._connect

    def guarded_connect(state_path: Path):
        return _FetchallGuardConnection(
            real_connect(state_path),
            forbidden_fragments=(
                "SELECT target_relative_path FROM expected_dirs",
                "SELECT export_relative_path, target_relative_path, etag_hex, size FROM expected_files",
                "SELECT target_relative_path, remote_dir_id, status FROM remote_dirs",
                "SELECT export_relative_path, target_relative_path, etag_hex, size FROM remote_files",
            ),
            error_message="fetchall should not be used for check-state reopen validation",
        )

    monkeypatch.setattr(check_state_module, "_connect", guarded_connect)

    reopened = _open_check_state(tmp_path, common_path="Demo/")

    assert reopened.summary_counters["expected_files"] == 3
    assert reopened.summary_counters["remote_files"] == 3
    reopened.close()


def test_plan_batch_check_job_persists_expected_file_coordinates(tmp_path):
    export_path = tmp_path / "demo.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0" * 32, "size": "5", "path": "1983/06/a.txt"}],
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)

    row = state.fetch_expected_file("1983/06/a.txt")
    assert row["export_relative_path"] == "1983/06/a.txt"
    assert row["target_relative_path"] == "Demo/1983/06/a.txt"
    assert state.fetch_expected_dirs() == ["Demo", "Demo/1983", "Demo/1983/06"]


def test_plan_batch_check_job_rebuilds_incomplete_planning_rows(tmp_path):
    export_path = tmp_path / "demo.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0" * 32, "size": "5", "path": "1983/06/a.txt"}],
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)
    _seed_partial_planning_rows(state)

    plan_batch_check_job(export_path=export_path, state=state)

    assert state.phase["planning_complete"] is True
    assert state.phase["remote_scan_complete"] is False
    assert state.phase["delta_finalize_complete"] is False
    assert _count_stale_partial_rows(state) == 0


def test_plan_batch_check_job_zero_file_export_persists_common_path_dirs_only(tmp_path):
    export_path = tmp_path / "empty.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/Season1/",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)

    assert state.fetch_expected_dirs() == ["Demo", "Demo/Season1"]
    assert state.summary_counters["expected_files"] == 0
    assert state.summary_counters["expected_dirs"] == 2


def test_plan_batch_check_job_accepts_base62_export_when_metadata_follows_files(tmp_path):
    export_path = tmp_path / "base62-after-files.json"
    export_path.write_text(
        json.dumps(
            {
                "commonPath": "Demo/",
                "files": [{"etag": "WiAkcJukpqBeKRKuXWRec", "size": "5", "path": "1983/06/a.txt"}],
                "usesBase62EtagsInExport": True,
            }
        ),
        encoding="utf-8",
    )
    state = _open_check_state_for_export(tmp_path, export_path)

    plan_batch_check_job(export_path=export_path, state=state)

    row = state.fetch_expected_file("1983/06/a.txt")
    assert row["etag_hex"] == decode_base62_to_hex("WiAkcJukpqBeKRKuXWRec")


def test_finalize_delta_export_writes_import_compatible_payload_for_missing_files(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
        remote_files=[],
    )
    output_path = tmp_path / "delta" / "demo.delta.export.json"

    result = finalize_delta_export(state=state, output_path=output_path, compare_mode="exist_only")

    assert result.delta_files == 1
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    loaded = load_export_file(output_path)

    assert payload["usesBase62EtagsInExport"] is True
    assert payload["commonPath"] == "Demo/"
    assert payload["totalFilesCount"] == 1
    assert payload["totalSize"] == 5
    assert payload["formattedTotalSize"] == "5 B"
    assert all(set(item.keys()) == {"path", "etag", "size"} for item in payload["files"])
    assert [item["path"] for item in payload["files"]] == ["1983/a.txt"]
    assert loaded.common_path == "Demo/"


def test_finalize_delta_export_reuses_remote_index_for_checksum_mode(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
        remote_files=[("1983/a.txt", "Demo/1983/a.txt", "f" * 32, 5)],
    )

    result = finalize_delta_export(state=state, output_path=tmp_path / "delta.json", compare_mode="with_checksum")

    assert result.delta_files == 1
    assert result.reused_remote_index is True


def test_finalize_delta_export_mode_switch_rerun_reuses_remote_index_without_new_scan(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
        remote_files=[("1983/a.txt", "Demo/1983/a.txt", "f" * 32, 5)],
    )
    output_path = tmp_path / "delta" / "demo.delta.export.json"

    first = finalize_delta_export(state=state, output_path=output_path, compare_mode="exist_only")
    second = finalize_delta_export(state=state, output_path=output_path, compare_mode="with_checksum")

    assert first.reused_remote_index is True
    assert second.reused_remote_index is True
    assert state.job_metadata["last_compare_mode"] == "with_checksum"


def test_finalize_delta_export_writes_output_via_atomic_replace(tmp_path, monkeypatch):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
        remote_files=[],
    )
    output_path = tmp_path / "delta" / "demo.delta.export.json"
    replace_calls = []
    original_replace = os.replace

    def recording_replace(src, dst):
        replace_calls.append((Path(src).name, Path(dst).name))
        return original_replace(src, dst)

    monkeypatch.setattr("os.replace", recording_replace)

    finalize_delta_export(state=state, output_path=output_path, compare_mode="exist_only")

    assert replace_calls
    assert replace_calls[-1][1] == "demo.delta.export.json"


def test_finalize_delta_export_removes_stale_output_when_delta_is_empty(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
        remote_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
    )
    output_path = tmp_path / "delta" / "demo.delta.export.json"
    output_path.parent.mkdir(parents=True)
    output_path.write_text("stale", encoding="utf-8")

    result = finalize_delta_export(state=state, output_path=output_path, compare_mode="with_checksum")

    assert result.delta_files == 0
    assert output_path.exists() is False


def test_finalize_delta_export_streams_delta_rows_without_fetchall(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[
            (f"1983/file-{index}.txt", f"Demo/1983/file-{index}.txt", "0" * 32, index)
            for index in range(3)
        ],
        remote_files=[],
    )
    output_path = tmp_path / "delta" / "demo.delta.export.json"
    original_fetch = state.fetch_delta_candidate_rows
    streamed_counts: list[int] = []

    class NoLenIterator:
        def __iter__(self_inner):
            count = 0
            for row in original_fetch(compare_mode="exist_only"):
                count += 1
                yield row
            streamed_counts.append(count)

    state.fetch_delta_candidate_rows = lambda *, compare_mode: NoLenIterator()

    result = finalize_delta_export(state=state, output_path=output_path, compare_mode="exist_only")

    assert result.delta_files == 3
    assert streamed_counts == [3]


def test_fetch_delta_candidate_rows_supports_iterative_consumption_without_fetchall(tmp_path, monkeypatch):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[
            (f"1983/file-{index}.txt", f"Demo/1983/file-{index}.txt", "0" * 32, index)
            for index in range(3)
        ],
        remote_files=[],
    )
    real_connection = state.connection
    real_execute = real_connection.execute

    class FakeCursor:
        def __init__(self, rows):
            self._rows = list(rows)

        def __iter__(self):
            return iter(self._rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            raise AssertionError("fetchall should not be used for delta candidates")

    class ProxyConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def execute(self, query, parameters=()):
            cursor = real_execute(query, parameters)
            if "FROM expected_files AS e" in query and "ORDER BY e.export_relative_path ASC" in query:
                return FakeCursor(cursor.fetchall())
            return cursor

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    monkeypatch.setattr(state, "connection", ProxyConnection(real_connection))

    rows = list(state.fetch_delta_candidate_rows(compare_mode="exist_only"))

    assert [row["export_relative_path"] for row in rows] == [
        "1983/file-0.txt",
        "1983/file-1.txt",
        "1983/file-2.txt",
    ]


def test_open_or_initialize_check_state_rejects_pending_remote_dir_with_null_remote_dir_id(tmp_path):
    state = _open_check_state(tmp_path)
    state.connection.execute(
        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, 'pending')",
        ("Demo", None),
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path)


def test_open_or_initialize_check_state_rejects_remote_root_missing_without_completed_scan(
    tmp_path,
):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/Missing/",
        expected_files=[("1983/clip.mkv", "Demo/Missing/1983/clip.mkv", "0" * 32, 9)],
        remote_files=[],
    )
    state.connection.execute("DELETE FROM remote_dirs")
    state.connection.execute("DELETE FROM remote_files")
    state.connection.execute(
        "UPDATE job SET remote_scan_complete = 0, remote_root_missing = 1, remote_files = 0, remote_dirs = 0 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="Demo/Missing/")


def test_open_or_initialize_check_state_rejects_inflight_remote_dir_with_null_remote_dir_id(tmp_path):
    state = _open_check_state(tmp_path)
    state.connection.execute(
        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, 'inflight')",
        ("Demo", None),
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path)


def test_open_or_initialize_check_state_rejects_finalize_complete_with_invalid_compare_mode(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
        remote_files=[],
    )
    state.connection.execute(
        "UPDATE job SET delta_finalize_complete = 1, delta_files = 1, missing_dirs = 1, last_compare_mode = 'bogus' WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path)


def test_open_or_initialize_check_state_rejects_finalize_complete_with_counter_split_brain(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/a.txt", "Demo/1983/a.txt", "0" * 32, 5)],
        remote_files=[],
    )
    output_path = tmp_path / "delta" / "demo.delta.export.json"

    finalize_delta_export(state=state, output_path=output_path, compare_mode="exist_only")
    state.connection.execute(
        "UPDATE job SET delta_files = 999, missing_dirs = 999 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="Demo/")


def test_open_or_initialize_check_state_rejects_remote_scan_complete_missing_common_path_prefix_dirs(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/Action/",
        expected_files=[("1983/clip.mkv", "Demo/Action/1983/clip.mkv", "0" * 32, 9)],
        remote_files=[("1983/clip.mkv", "Demo/Action/1983/clip.mkv", "1" * 32, 9)],
    )
    state.connection.execute("DELETE FROM remote_dirs")
    state.connection.execute(
        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, 'completed')",
        ("Demo/Action/1983", "1983"),
    )
    state.connection.execute(
        "UPDATE job SET remote_dirs = 1, remote_files = 1, remote_scan_complete = 1 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="Demo/Action/")


def test_open_or_initialize_check_state_rejects_remote_file_without_backing_remote_dir_chain(tmp_path):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/clip.mkv", "Demo/1983/clip.mkv", "0" * 32, 9)],
        remote_files=[("1983/clip.mkv", "Demo/1983/clip.mkv", "1" * 32, 9)],
    )
    state.connection.execute("DELETE FROM remote_dirs")
    state.connection.execute(
        "UPDATE job SET remote_dirs = 0, remote_files = 1, remote_scan_complete = 1 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="Demo/")


def test_open_or_initialize_check_state_rejects_remote_scan_complete_missing_empty_common_path_root_row(
    tmp_path,
):
    state = seeded_check_state(
        tmp_path,
        common_path="",
        expected_files=[("1983/clip.mkv", "1983/clip.mkv", "0" * 32, 9)],
        remote_files=[("1983/clip.mkv", "1983/clip.mkv", "1" * 32, 9)],
    )
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="")


def test_open_or_initialize_check_state_rejects_partial_remote_scan_missing_empty_common_path_root_row(
    tmp_path,
):
    state = seeded_check_state(
        tmp_path,
        common_path="",
        expected_files=[("1983/clip.mkv", "1983/clip.mkv", "0" * 32, 9)],
        remote_files=[("1983/clip.mkv", "1983/clip.mkv", "1" * 32, 9)],
    )
    state.connection.execute("DELETE FROM remote_dirs")
    state.connection.execute(
        "UPDATE job SET remote_scan_complete = 0, remote_root_missing = 0, remote_dirs = 0 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="")


def test_open_or_initialize_check_state_rejects_partial_remote_scan_missing_backing_remote_dir_chain(
    tmp_path,
):
    state = seeded_check_state(
        tmp_path,
        common_path="Demo/",
        expected_files=[("1983/clip.mkv", "Demo/1983/clip.mkv", "0" * 32, 9)],
        remote_files=[("1983/clip.mkv", "Demo/1983/clip.mkv", "1" * 32, 9)],
    )
    state.connection.execute("DELETE FROM remote_dirs")
    state.connection.execute(
        "UPDATE job SET remote_scan_complete = 0, remote_root_missing = 0, remote_dirs = 0 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    with pytest.raises(ValueError, match=r"^malformed check-state error$"):
        _open_check_state(tmp_path, common_path="Demo/")
