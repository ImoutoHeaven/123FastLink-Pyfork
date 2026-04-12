import json
import os
from pathlib import Path

import pytest

from fastlink_transfer.export_state import (
    MALFORMED_EXPORT_STATE_ERROR,
    MISMATCH_ERROR,
    ExportState,
    load_or_initialize_export_state,
)


def test_load_or_initialize_export_state_creates_new_state(tmp_path):
    state = load_or_initialize_export_state(
        state_path=tmp_path / "export.state.json",
        source_host="https://www.123pan.com",
        source_parent_id="12345678",
        source_root_name="Movies",
        output_file=tmp_path / "out.json",
        workers=8,
        max_retries=5,
        flush_every=100,
    )

    assert state.common_path == "Movies/"
    assert state.records_file == (tmp_path / "export.state.records.jsonl").resolve()
    assert state.temp_output_file == (tmp_path / "export.state.output.tmp.json").resolve()
    assert state.temp_sqlite_file == (tmp_path / "export.state.finalize.sqlite3").resolve()
    assert state.pending_dirs == [{"folder_id": "12345678", "relative_dir": ""}]
    assert state.inflight_dirs == []
    assert state.completed_dirs == []
    assert state.seen_dir_ids == ["12345678"]


def test_load_or_initialize_export_state_rejects_state_scope_mismatch(tmp_path):
    state_path = tmp_path / "export.state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_host": "https://www.123pan.com",
                "source_parent_id": "12345678",
                "source_root_name": "Movies",
                "common_path": "Movies/",
                "output_file": str((tmp_path / "out.json").resolve()),
                "records_file": str((tmp_path / "export.state.records.jsonl").resolve()),
                "temp_output_file": str((tmp_path / "export.state.output.tmp.json").resolve()),
                "temp_sqlite_file": str((tmp_path / "export.state.finalize.sqlite3").resolve()),
                "workers": 8,
                "max_retries": 5,
                "flush_every": 100,
                "uses_base62_etags_in_export": True,
                "pending_dirs": [],
                "inflight_dirs": [],
                "completed_dirs": [],
                "seen_dir_ids": ["12345678"],
                "stats": {"files_written": 0, "dirs_completed": 0},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "export.state.records.jsonl").write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match=rf"^{MISMATCH_ERROR}$"):
        load_or_initialize_export_state(
            state_path=state_path,
            source_host="https://www.123pan.com",
            source_parent_id="99999999",
            source_root_name="Movies",
            output_file=tmp_path / "out.json",
            workers=8,
            max_retries=5,
            flush_every=100,
        )


def test_load_or_initialize_export_state_rejects_missing_sidecar_when_state_exists(tmp_path):
    state_path = tmp_path / "export.state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_host": "https://www.123pan.com",
                "source_parent_id": "12345678",
                "source_root_name": "Movies",
                "common_path": "Movies/",
                "output_file": str((tmp_path / "out.json").resolve()),
                "records_file": str((tmp_path / "export.state.records.jsonl").resolve()),
                "temp_output_file": str((tmp_path / "export.state.output.tmp.json").resolve()),
                "temp_sqlite_file": str((tmp_path / "export.state.finalize.sqlite3").resolve()),
                "workers": 8,
                "max_retries": 5,
                "flush_every": 100,
                "uses_base62_etags_in_export": True,
                "pending_dirs": [],
                "inflight_dirs": [],
                "completed_dirs": [],
                "seen_dir_ids": ["12345678"],
                "stats": {"files_written": 0, "dirs_completed": 0},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=rf"^{MISMATCH_ERROR}$"):
        load_or_initialize_export_state(
            state_path=state_path,
            source_host="https://www.123pan.com",
            source_parent_id="12345678",
            source_root_name="Movies",
            output_file=tmp_path / "out.json",
            workers=8,
            max_retries=5,
            flush_every=100,
        )


def test_load_or_initialize_export_state_rejects_output_file_colliding_with_temp_output_file(tmp_path):
    with pytest.raises(ValueError, match="output_file must be distinct from internal export artifacts"):
        load_or_initialize_export_state(
            state_path=tmp_path / "export.state.json",
            source_host="https://www.123pan.com",
            source_parent_id="12345678",
            source_root_name="Movies",
            output_file=tmp_path / "export.state.output.tmp.json",
            workers=8,
            max_retries=5,
            flush_every=100,
        )


def test_load_or_initialize_export_state_recomputes_derived_temp_artifacts_on_resume(tmp_path):
    state_path = tmp_path / "export.state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_host": "https://www.123pan.com",
                "source_parent_id": "12345678",
                "source_root_name": "Movies",
                "common_path": "Movies/",
                "output_file": str((tmp_path / "out.json").resolve()),
                "records_file": str((tmp_path / "export.state.records.jsonl").resolve()),
                "temp_output_file": str((tmp_path / "tampered.output.tmp.json").resolve()),
                "temp_sqlite_file": str((tmp_path / "tampered.finalize.sqlite3").resolve()),
                "workers": 8,
                "max_retries": 5,
                "flush_every": 100,
                "uses_base62_etags_in_export": True,
                "pending_dirs": [],
                "inflight_dirs": [],
                "completed_dirs": [],
                "seen_dir_ids": ["12345678"],
                "stats": {"files_written": 0, "dirs_completed": 0},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "export.state.records.jsonl").write_text("", encoding="utf-8")

    state = load_or_initialize_export_state(
        state_path=state_path,
        source_host="https://www.123pan.com",
        source_parent_id="12345678",
        source_root_name="Movies",
        output_file=tmp_path / "out.json",
        workers=8,
        max_retries=5,
        flush_every=100,
    )

    assert state.temp_output_file == (tmp_path / "export.state.output.tmp.json").resolve()
    assert state.temp_sqlite_file == (tmp_path / "export.state.finalize.sqlite3").resolve()


def test_load_or_initialize_export_state_rejects_parseable_schema_invalid_payload(tmp_path):
    state_path = tmp_path / "export.state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_host": "https://www.123pan.com",
                "source_parent_id": "12345678",
                "source_root_name": "Movies",
                "common_path": "Movies/",
                "output_file": str((tmp_path / "out.json").resolve()),
                "records_file": str((tmp_path / "export.state.records.jsonl").resolve()),
                "temp_output_file": str((tmp_path / "export.state.output.tmp.json").resolve()),
                "temp_sqlite_file": str((tmp_path / "export.state.finalize.sqlite3").resolve()),
                "workers": 8,
                "max_retries": 5,
                "flush_every": 100,
                "uses_base62_etags_in_export": True,
                "inflight_dirs": [],
                "completed_dirs": [],
                "seen_dir_ids": ["12345678"],
                "stats": {"files_written": 0, "dirs_completed": 0},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "export.state.records.jsonl").write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match=rf"^{MALFORMED_EXPORT_STATE_ERROR}$"):
        load_or_initialize_export_state(
            state_path=state_path,
            source_host="https://www.123pan.com",
            source_parent_id="12345678",
            source_root_name="Movies",
            output_file=tmp_path / "out.json",
            workers=8,
            max_retries=5,
            flush_every=100,
        )


def test_load_or_initialize_export_state_rejects_orphaned_sidecar_without_state(tmp_path):
    (tmp_path / "export.state.records.jsonl").write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match=rf"^{MISMATCH_ERROR}$"):
        load_or_initialize_export_state(
            state_path=tmp_path / "export.state.json",
            source_host="https://www.123pan.com",
            source_parent_id="12345678",
            source_root_name="Movies",
            output_file=tmp_path / "out.json",
            workers=8,
            max_retries=5,
            flush_every=100,
        )


def test_recover_inflight_dirs_moves_them_to_pending_front(tmp_path):
    state = ExportState(
        source_host="https://www.123pan.com",
        source_parent_id="12345678",
        source_root_name="Movies",
        common_path="Movies/",
        output_file=(tmp_path / "out.json").resolve(),
        records_file=(tmp_path / "export.state.records.jsonl").resolve(),
        temp_output_file=(tmp_path / "export.state.output.tmp.json").resolve(),
        temp_sqlite_file=(tmp_path / "export.state.finalize.sqlite3").resolve(),
        workers=8,
        max_retries=5,
        flush_every=100,
        uses_base62_etags_in_export=True,
        pending_dirs=[{"folder_id": "200", "relative_dir": "B"}],
        inflight_dirs=[{"folder_id": "100", "relative_dir": "A"}],
        completed_dirs=[],
        seen_dir_ids=["12345678", "100", "200"],
        stats={"files_written": 0, "dirs_completed": 0},
        last_flush_at=None,
    )

    state.recover_inflight_dirs()

    assert state.pending_dirs == [
        {"folder_id": "100", "relative_dir": "A"},
        {"folder_id": "200", "relative_dir": "B"},
    ]
    assert state.inflight_dirs == []


def test_export_state_flush_fsyncs_parent_directory_after_atomic_replace(monkeypatch, tmp_path):
    state = ExportState(
        source_host="https://www.123pan.com",
        source_parent_id="12345678",
        source_root_name="Movies",
        common_path="Movies/",
        output_file=(tmp_path / "out.json").resolve(),
        records_file=(tmp_path / "export.state.records.jsonl").resolve(),
        temp_output_file=(tmp_path / "export.state.output.tmp.json").resolve(),
        temp_sqlite_file=(tmp_path / "export.state.finalize.sqlite3").resolve(),
        workers=8,
        max_retries=5,
        flush_every=100,
        uses_base62_etags_in_export=True,
        pending_dirs=[{"folder_id": "12345678", "relative_dir": ""}],
        inflight_dirs=[],
        completed_dirs=[],
        seen_dir_ids=["12345678"],
        stats={"files_written": 0, "dirs_completed": 0},
        last_flush_at=None,
    )
    state_path = (tmp_path / "export.state.json").resolve()
    calls = []
    parent_fd = 987654
    original_open = os.open
    original_replace = os.replace

    def record_replace(src, dst):
        calls.append(("replace", Path(src), Path(dst)))
        return original_replace(src, dst)

    def record_open(path, flags, *args, **kwargs):
        resolved_path = Path(path).resolve()
        if resolved_path == tmp_path.resolve():
            calls.append(("open", resolved_path))
            return parent_fd
        return original_open(path, flags, *args, **kwargs)

    def record_fsync(fd):
        calls.append(("fsync", fd))

    def record_close(fd):
        calls.append(("close", fd))

    monkeypatch.setattr("fastlink_transfer.export_state.os.replace", record_replace)
    monkeypatch.setattr("fastlink_transfer.export_state.os.open", record_open)
    monkeypatch.setattr("fastlink_transfer.export_state.os.fsync", record_fsync)
    monkeypatch.setattr("fastlink_transfer.export_state.os.close", record_close)

    state.flush(state_path)

    replace_index = next(index for index, call in enumerate(calls) if call[0] == "replace")
    open_index = next(index for index, call in enumerate(calls) if call == ("open", tmp_path.resolve()))
    dir_fsync_index = next(index for index, call in enumerate(calls) if call == ("fsync", parent_fd))
    close_index = next(index for index, call in enumerate(calls) if call == ("close", parent_fd))

    assert replace_index < open_index < dir_fsync_index < close_index
    assert state_path.exists()
