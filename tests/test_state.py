import json
from pathlib import Path

import pytest

from fastlink_transfer.importer import load_export_file
from fastlink_transfer.state import TransferState, load_or_initialize_state


SAMPLE_EXPORT_FIXTURE = Path(__file__).parent / "fixtures" / "sample_export.json"


def _valid_state_payload(**overrides):
    payload = {
        "source_file": "a",
        "source_sha256": "abc",
        "target_parent_id": "12345678",
        "common_path": "Demo/",
        "workers": 8,
        "folder_map": {"": "12345678"},
        "completed": [],
        "not_reusable": [],
        "failed": [],
        "stats": {"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        "last_flush_at": None,
    }
    payload.update(overrides)
    return payload


def _write_state_payload(state_path, payload):
    state_path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_or_initialize_state_creates_new_state(tmp_path):
    state = load_or_initialize_state(
        state_path=tmp_path / "state.json",
        source_file=str(tmp_path / "export.json"),
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
        workers=8,
        total_records=2,
    )

    assert isinstance(state, TransferState)
    assert state.folder_map == {"": "12345678"}
    assert state.stats == {"total": 2, "completed": 0, "not_reusable": 0, "failed": 0}


def test_load_or_initialize_state_rejects_scope_mismatch(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_file": "a",
                "source_sha256": "abc",
                "target_parent_id": "12345678",
                "common_path": "Demo/",
                "workers": 8,
                "folder_map": {"": "12345678"},
                "completed": [],
                "not_reusable": [],
                "failed": [],
                "stats": {"total": 2, "completed": 0, "not_reusable": 0, "failed": 0},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        load_or_initialize_state(
            state_path=state_path,
            source_file="a",
            source_sha256="different",
            target_parent_id="12345678",
            common_path="Demo/",
            workers=8,
            total_records=2,
        )


def test_load_or_initialize_state_rejects_duplicate_terminal_keys(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_file": "a",
                "source_sha256": "abc",
                "target_parent_id": "12345678",
                "common_path": "Demo/",
                "workers": 8,
                "folder_map": {"": "12345678"},
                "completed": ["same"],
                "not_reusable": [{"key": "same", "path": "a", "error": "Reuse=false"}],
                "failed": [],
                "stats": {"total": 1, "completed": 1, "not_reusable": 1, "failed": 0},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"^malformed-state error$"):
        load_or_initialize_state(
            state_path=state_path,
            source_file="a",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
            workers=8,
            total_records=1,
        )


def test_load_or_initialize_state_rejects_wrong_last_flush_type(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_file": "a",
                "source_sha256": "abc",
                "target_parent_id": "12345678",
                "common_path": "Demo/",
                "workers": 8,
                "folder_map": {"": "12345678"},
                "completed": [],
                "not_reusable": [],
                "failed": [],
                "stats": {"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
                "last_flush_at": 123,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_or_initialize_state(
            state_path=state_path,
            source_file="a",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
            workers=8,
            total_records=1,
        )


def test_load_or_initialize_state_rejects_non_normalized_common_path(tmp_path):
    state_path = tmp_path / "state.json"
    _write_state_payload(
        state_path,
        _valid_state_payload(common_path="Demo"),
    )

    with pytest.raises(ValueError):
        load_or_initialize_state(
            state_path=state_path,
            source_file="a",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo",
            workers=8,
            total_records=0,
        )


def test_load_or_initialize_state_rejects_bool_workers(tmp_path):
    state_path = tmp_path / "state.json"
    _write_state_payload(
        state_path,
        _valid_state_payload(workers=True),
    )

    with pytest.raises(ValueError):
        load_or_initialize_state(
            state_path=state_path,
            source_file="a",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
            workers=8,
            total_records=0,
        )


def test_load_or_initialize_state_rejects_bool_failed_retries(tmp_path):
    state_path = tmp_path / "state.json"
    _write_state_payload(
        state_path,
        _valid_state_payload(
            failed=[{"key": "k", "path": "a", "error": "boom", "retries": True}],
            stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 1},
        ),
    )

    with pytest.raises(ValueError):
        load_or_initialize_state(
            state_path=state_path,
            source_file="a",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
            workers=8,
            total_records=1,
        )


@pytest.mark.parametrize(
    ("field_name", "completed", "not_reusable", "failed", "total_records"),
    [
        ("total", [], [], [], 0),
        ("completed", ["k"], [], [], 1),
        ("not_reusable", [], [{"key": "k", "path": "a", "error": "Reuse=false"}], [], 1),
        ("failed", [], [], [{"key": "k", "path": "a", "error": "boom", "retries": 0}], 1),
    ],
)
def test_load_or_initialize_state_rejects_bool_stats_fields(
    tmp_path,
    field_name,
    completed,
    not_reusable,
    failed,
    total_records,
):
    state_path = tmp_path / "state.json"
    stats = {
        "total": total_records,
        "completed": len(completed),
        "not_reusable": len(not_reusable),
        "failed": len(failed),
    }
    stats[field_name] = True
    _write_state_payload(
        state_path,
        _valid_state_payload(
            completed=completed,
            not_reusable=not_reusable,
            failed=failed,
            stats=stats,
        ),
    )

    with pytest.raises(ValueError):
        load_or_initialize_state(
            state_path=state_path,
            source_file="a",
            source_sha256="abc",
            target_parent_id="12345678",
            common_path="Demo/",
            workers=8,
            total_records=total_records,
        )


def test_load_or_initialize_state_accepts_not_reusable_without_error(tmp_path):
    state_path = tmp_path / "state.json"
    _write_state_payload(
        state_path,
        _valid_state_payload(
            not_reusable=[{"key": "k", "path": "a"}],
            stats={"total": 1, "completed": 0, "not_reusable": 1, "failed": 0},
        ),
    )

    state = load_or_initialize_state(
        state_path=state_path,
        source_file="a",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
        workers=8,
        total_records=1,
    )

    assert state.not_reusable == {"k": {"key": "k", "path": "a"}}


def test_load_or_initialize_state_resumes_fixture_progress(tmp_path):
    export = load_export_file(SAMPLE_EXPORT_FIXTURE)
    first_record, second_record = export.records
    state_path = tmp_path / "state.json"
    _write_state_payload(
        state_path,
        _valid_state_payload(
            source_file=str(SAMPLE_EXPORT_FIXTURE.resolve()),
            source_sha256=export.source_sha256,
            common_path=export.common_path,
            folder_map={
                "": "12345678",
                "Demo": "1",
                "Demo/1983": "2",
                "Demo/1983/06": "3",
                "Demo/1983/07": "4",
            },
            completed=[first_record.key],
            failed=[{"key": second_record.key, "path": second_record.path, "error": "HTTP 500", "retries": 2}],
            stats={"total": 2, "completed": 1, "not_reusable": 0, "failed": 1},
        ),
    )

    state = load_or_initialize_state(
        state_path=state_path,
        source_file=str(SAMPLE_EXPORT_FIXTURE.resolve()),
        source_sha256=export.source_sha256,
        target_parent_id="12345678",
        common_path=export.common_path,
        workers=8,
        total_records=2,
    )

    assert state.completed == {first_record.key}
    assert state.failed == {
        second_record.key: {
            "key": second_record.key,
            "path": second_record.path,
            "error": "HTTP 500",
            "retries": 2,
        }
    }
    assert state.folder_map["Demo/1983/07"] == "4"


def test_record_completed_replaces_old_not_reusable_record():
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
        workers=8,
        folder_map={"": "12345678"},
        completed=set(),
        not_reusable={"k": {"key": "k", "path": "a.txt", "error": "Reuse=false"}},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 1, "failed": 0},
        last_flush_at=None,
    )

    state.record_completed("k")

    assert state.completed == {"k"}
    assert state.not_reusable == {}
    assert state.stats == {"total": 1, "completed": 1, "not_reusable": 0, "failed": 0}


def test_flush_writes_json_payload(tmp_path):
    state_path = tmp_path / "state.json"
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
        workers=8,
        folder_map={"": "12345678", "Demo": "1"},
        completed={"k"},
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 1, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    state.flush(state_path)

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["completed"] == ["k"]
    assert payload["folder_map"]["Demo"] == "1"
    assert payload["workers"] == 8
