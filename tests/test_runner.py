import json
import queue
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from fastlink_transfer.api import Decision, DecisionKind, PanApiClient
from fastlink_transfer.import_state import open_or_initialize_import_state
from fastlink_transfer.importer import SourceRecord, load_export_file, select_pending_records
from fastlink_transfer.runner import (
    DirectoryCoordinator,
    GlobalPause,
    StopController,
    create_remote_directories,
    finalize_import_job,
    process_record,
    run_file_phase,
    run_file_phase_sqlite,
)
from fastlink_transfer.state import TransferState


SAMPLE_EXPORT_FIXTURE = Path(__file__).parent / "fixtures" / "sample_export.json"


class RecordingClient:
    def __init__(self):
        self.calls = []

    def mkdir(self, *, parent_file_id: str, folder_name: str):
        self.calls.append((parent_file_id, folder_name))
        return Decision(kind=DecisionKind.DIRECTORY_CREATED, file_id=str(len(self.calls)))


def test_create_remote_directories_succeeds_when_stdout_logging_fails(monkeypatch, tmp_path):
    client = RecordingClient()
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="H-SUKI/",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    def raising_print(*_args, **_kwargs):
        raise RuntimeError("broken stdout")

    monkeypatch.setattr("builtins.print", raising_print)

    create_remote_directories(
        api_client=client,
        state=state,
        folder_keys=["H-SUKI", "H-SUKI/1983", "H-SUKI/1983/06"],
        state_path=tmp_path / "state.json",
    )

    assert client.calls == [("100", "H-SUKI"), ("1", "1983"), ("2", "06")]
    assert state.folder_map["H-SUKI/1983/06"] == "3"


def test_create_remote_directories_creates_common_path_first(tmp_path):
    client = RecordingClient()
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="H-SUKI/",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    create_remote_directories(
        api_client=client,
        state=state,
        folder_keys=["H-SUKI", "H-SUKI/1983", "H-SUKI/1983/06"],
        state_path=tmp_path / "state.json",
    )

    assert client.calls == [("100", "H-SUKI"), ("1", "1983"), ("2", "06")]
    assert state.folder_map["H-SUKI/1983/06"] == "3"


def test_directory_phase_reuses_cached_parent_and_flushes_each_new_folder(monkeypatch, tmp_path):
    client = RecordingClient()
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="Demo/",
        workers=8,
        folder_map={"": "100", "Demo": "200"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    flushes = []
    original_flush = state.flush

    def wrapped_flush(state_path):
        flushes.append(sorted(state.folder_map.items()))
        original_flush(state_path)

    monkeypatch.setattr(state, "flush", wrapped_flush)

    create_remote_directories(
        api_client=client,
        state=state,
        folder_keys=["Demo", "Demo/1983", "Demo/1983/07"],
        state_path=tmp_path / "state.json",
    )

    assert client.calls == [("200", "1983"), ("1", "07")]
    assert flushes == [
        [("", "100"), ("Demo", "200"), ("Demo/1983", "1")],
        [("", "100"), ("Demo", "200"), ("Demo/1983", "1"), ("Demo/1983/07", "2")],
    ]


class SequenceDirectoryClient:
    def __init__(self, decisions):
        self.decisions = list(decisions)
        self.calls = []

    def mkdir(self, *, parent_file_id: str, folder_name: str):
        self.calls.append((parent_file_id, folder_name))
        return self.decisions.pop(0)


class ListingDirectoryClient:
    def __init__(self, *, listings=None, mkdir_decisions=None):
        self.listings = dict(listings or {})
        self.mkdir_decisions = list(mkdir_decisions or [])
        self.get_file_list_calls = []
        self.mkdir_calls = []

    def get_file_list(self, *, parent_file_id: str):
        self.get_file_list_calls.append(parent_file_id)
        items = self.listings.get(parent_file_id, [])
        return Decision(kind=DecisionKind.COMPLETED, payload={"items": items, "total": len(items)})

    def mkdir(self, *, parent_file_id: str, folder_name: str):
        self.mkdir_calls.append((parent_file_id, folder_name))
        if self.mkdir_decisions:
            return self.mkdir_decisions.pop(0)
        return Decision(kind=DecisionKind.DIRECTORY_CREATED, file_id=str(len(self.mkdir_calls)))


def test_create_remote_directories_retries_retryable_failure_before_succeeding(monkeypatch, tmp_path):
    monkeypatch.setattr("fastlink_transfer.runner.compute_backoff", lambda _attempt: 1.25)
    sleep_calls = []
    monkeypatch.setattr("fastlink_transfer.runner.time.sleep", lambda seconds: sleep_calls.append(seconds))
    client = SequenceDirectoryClient(
        [
            Decision(kind=DecisionKind.RETRYABLE, error="HTTP 429"),
            Decision(kind=DecisionKind.DIRECTORY_CREATED, file_id="7"),
        ]
    )
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="Demo/",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    create_remote_directories(
        api_client=client,
        state=state,
        folder_keys=["Demo"],
        state_path=tmp_path / "state.json",
        max_retries=1,
    )

    assert client.calls == [("100", "Demo"), ("100", "Demo")]
    assert sleep_calls == [1.25]
    assert state.folder_map["Demo"] == "7"


def test_create_remote_directories_reuses_existing_remote_directory_before_mkdir(tmp_path):
    client = ListingDirectoryClient(
        listings={
            "100": [{"FileId": 200, "Type": 1, "FileName": "Demo"}],
        }
    )
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="Demo/",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    create_remote_directories(
        api_client=client,
        state=state,
        folder_keys=["Demo"],
        state_path=tmp_path / "state.json",
    )

    assert client.get_file_list_calls == ["100"]
    assert client.mkdir_calls == []
    assert state.folder_map["Demo"] == "200"


def test_create_remote_directories_reuses_batch_global_directory_cache_across_states(tmp_path):
    coordinator = DirectoryCoordinator()
    client = ListingDirectoryClient(
        listings={
            "100": [],
            "1": [],
        },
        mkdir_decisions=[
            Decision(kind=DecisionKind.DIRECTORY_CREATED, file_id="1"),
            Decision(kind=DecisionKind.DIRECTORY_CREATED, file_id="2"),
        ],
    )
    state_one = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="Demo/",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    state_two = TransferState(
        source_file="b",
        source_sha256="def",
        target_parent_id="100",
        common_path="Demo/",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    create_remote_directories(
        api_client=client,
        state=state_one,
        folder_keys=["Demo", "Demo/1983"],
        state_path=tmp_path / "one.state.json",
        directory_coordinator=coordinator,
    )
    create_remote_directories(
        api_client=client,
        state=state_two,
        folder_keys=["Demo", "Demo/1983"],
        state_path=tmp_path / "two.state.json",
        directory_coordinator=coordinator,
    )

    assert client.get_file_list_calls == ["100", "1"]
    assert client.mkdir_calls == [("100", "Demo"), ("1", "1983")]
    assert state_two.folder_map == {"": "100", "Demo": "1", "Demo/1983": "2"}


@pytest.mark.parametrize(
    ("decision_kind", "error"),
    [
        (DecisionKind.FAILED, "bad payload"),
        (DecisionKind.CREDENTIAL_FATAL, "HTTP 401"),
    ],
)
def test_create_remote_directories_keeps_terminal_failures_terminal(
    monkeypatch, tmp_path, decision_kind, error
):
    monkeypatch.setattr(
        "fastlink_transfer.runner.time.sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("terminal mkdir failure should not sleep")),
    )
    client = SequenceDirectoryClient([Decision(kind=decision_kind, error=error)])
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="Demo/",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 0, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    with pytest.raises(RuntimeError, match=rf"directory creation failed: Demo: {error}"):
        create_remote_directories(
            api_client=client,
            state=state,
            folder_keys=["Demo"],
            state_path=tmp_path / "state.json",
            max_retries=3,
        )

    assert client.calls == [("100", "Demo")]


class SequenceClient:
    def __init__(self, decisions):
        self.decisions = decisions
        self.calls = []

    def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
        self.calls.append((etag, size, file_name, parent_file_id))
        return self.decisions.pop(0)


def test_run_file_phase_updates_sqlite_state_in_batched_writer_commits(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.executemany(
        "INSERT OR REPLACE INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        [
            ("", "", "12345678", "created", None),
            ("Demo", "", "200", "created", None),
            ("Demo/a", "Demo", "201", "created", None),
        ],
    )
    state.connection.executemany(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("k1", "a/one.txt", "one.txt", "a", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
            ("k2", "a/two.txt", "two.txt", "a", "fedcba9876543210fedcba9876543210", 2, "pending", None, 0),
        ],
    )
    state.connection.execute("UPDATE job SET planning_complete = 1, total_files = 2")
    state.connection.commit()
    state.refresh_folder_map()
    state.workers = 2
    client = SequenceClient(
        [
            Decision(kind=DecisionKind.COMPLETED),
            Decision(kind=DecisionKind.NOT_REUSABLE, error="Reuse=false"),
        ]
    )

    summary = run_file_phase_sqlite(
        api_client=client,
        state=state,
        max_retries=0,
        flush_every=2,
    )

    assert summary == {"processed": 2, "credential_fatal": False}
    assert state.fetch_status_counts() == {
        "pending": 0,
        "completed": 1,
        "not_reusable": 1,
        "failed": 0,
    }
    assert state.stats == {
        "total": 2,
        "completed": 1,
        "not_reusable": 1,
        "failed": 0,
    }


def test_run_file_phase_sqlite_rolls_back_partial_terminal_updates_when_job_counter_flush_fails(
    tmp_path,
):
    state_path = tmp_path / "import.state.sqlite3"
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.executemany(
        "INSERT OR REPLACE INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        [
            ("", "", "12345678", "created", None),
            ("Demo", "", "200", "created", None),
            ("Demo/a", "Demo", "201", "created", None),
        ],
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("k1", "a/one.txt", "one.txt", "a", "0123456789abcdef0123456789abcdef", 1, "pending", None, 0),
    )
    state.connection.execute(
        "UPDATE job SET planning_complete = 1, total_files = 1, total_folders = 2"
    )
    state.connection.execute(
        "CREATE TRIGGER fail_job_counter_update BEFORE UPDATE ON job "
        "WHEN NEW.completed_count != OLD.completed_count "
        "BEGIN SELECT RAISE(ABORT, 'forced job counter failure'); END"
    )
    state.connection.commit()
    state.refresh_folder_map()
    state.workers = 1
    client = SequenceClient([Decision(kind=DecisionKind.COMPLETED)])

    with pytest.raises(sqlite3.IntegrityError, match="forced job counter failure"):
        run_file_phase_sqlite(
            api_client=client,
            state=state,
            max_retries=0,
            flush_every=1,
        )

    state.close()

    reopened = open_or_initialize_import_state(
        state_path=state_path,
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    try:
        assert reopened.fetch_status_counts() == {
            "pending": 1,
            "completed": 0,
            "not_reusable": 0,
            "failed": 0,
        }
        assert reopened.stats == {
            "total": 1,
            "completed": 0,
            "not_reusable": 0,
            "failed": 0,
        }
    finally:
        reopened.close()


def test_finalize_import_job_writes_retry_export_for_remaining_unsuccessful_rows(tmp_path):
    state = open_or_initialize_import_state(
        state_path=tmp_path / "import.state.sqlite3",
        source_file="/tmp/export.json",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
    )
    state.connection.execute(
        "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("k1", "a/one.txt", "one.txt", "a", "0123456789abcdef0123456789abcdef", 1, "failed", "HTTP 500", 2),
    )
    state.connection.commit()
    retry_export_path = tmp_path / "remaining.json"

    wrote = finalize_import_job(state=state, retry_export_path=retry_export_path)

    assert wrote is True
    assert retry_export_path.exists()


def test_stop_controller_claim_next_work_item_preserves_pending_queue_after_shutdown_starts():
    stop_controller = StopController()
    work_queue: queue.Queue = queue.Queue()
    work_item = ("record", "100")
    work_queue.put(work_item)

    stop_controller.start_shutdown(credential_fatal=True)

    assert stop_controller.claim_next_work_item(work_queue) is None
    assert stop_controller.credential_fatal is True
    assert work_queue.get_nowait() == work_item


def test_process_record_retries_then_records_failed(monkeypatch):
    monkeypatch.setattr("fastlink_transfer.runner.compute_backoff", lambda _attempt: 0.0)
    client = SequenceClient(
        [
            Decision(kind=DecisionKind.RETRYABLE, error="HTTP 500"),
            Decision(kind=DecisionKind.FAILED, error="bad payload"),
        ]
    )
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    record = SourceRecord(
        key="k",
        etag="0123456789abcdef0123456789abcdef",
        size=1,
        path="a.txt",
        file_name="a.txt",
        relative_parent_dir="",
    )
    stop_event = StopController()
    pause = GlobalPause()

    result = process_record(
        api_client=client,
        state=state,
        record=record,
        parent_file_id="100",
        max_retries=1,
        stop_controller=stop_event,
        global_pause=pause,
    )

    assert result == "failed"
    assert state.failed["k"] == {"key": "k", "path": "a.txt", "error": "bad payload", "retries": 1}


def test_process_record_uses_global_pause_for_429(monkeypatch):
    monkeypatch.setattr("fastlink_transfer.runner.compute_backoff", lambda _attempt: 1.25)
    client = SequenceClient(
        [
            Decision(kind=DecisionKind.RETRYABLE, error="HTTP 429"),
            Decision(kind=DecisionKind.COMPLETED, file_id="1"),
        ]
    )
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    record = SourceRecord(
        key="k",
        etag="0123456789abcdef0123456789abcdef",
        size=1,
        path="a.txt",
        file_name="a.txt",
        relative_parent_dir="",
    )

    class PauseRecorder:
        def __init__(self):
            self.pauses = []

        def pause(self, seconds: float) -> None:
            self.pauses.append(seconds)

        def wait(self, *, stop_controller=None) -> bool:
            return True

    pause = PauseRecorder()

    result = process_record(
        api_client=client,
        state=state,
        record=record,
        parent_file_id="100",
        max_retries=1,
        stop_controller=StopController(),
        global_pause=pause,
    )

    assert result == "completed"
    assert pause.pauses == [1.25]
    assert state.completed == {"k"}


def test_process_record_preserves_old_bucket_when_shutdown_blocks_retry():
    client = SequenceClient([Decision(kind=DecisionKind.RETRYABLE, error="HTTP 500")])
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=8,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={"k": {"key": "k", "path": "a.txt", "error": "Reuse=false"}},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 1, "failed": 0},
        last_flush_at=None,
    )
    record = SourceRecord(
        key="k",
        etag="0123456789abcdef0123456789abcdef",
        size=1,
        path="a.txt",
        file_name="a.txt",
        relative_parent_dir="",
    )
    stop_event = StopController()
    stop_event.start_shutdown()

    result = process_record(
        api_client=client,
        state=state,
        record=record,
        parent_file_id="100",
        max_retries=3,
        stop_controller=stop_event,
        global_pause=GlobalPause(),
    )

    assert result == "deferred"
    assert state.not_reusable["k"]["error"] == "Reuse=false"


def test_process_record_does_not_start_upload_after_shutdown_begins_during_pause_wait():
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    record = SourceRecord(
        key="k",
        etag="0123456789abcdef0123456789abcdef",
        size=1,
        path="a.txt",
        file_name="a.txt",
        relative_parent_dir="",
    )
    stop_controller = StopController()

    class ShutdownStartingPause:
        def pause(self, seconds: float) -> None:  # pragma: no cover - guard
            raise AssertionError("pause() should not be used")

        def wait(self, *, stop_controller=None) -> bool:
            stop_controller.start_shutdown()
            return True

    class RecordingClient:
        def __init__(self):
            self.calls = 0

        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            self.calls += 1
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    client = RecordingClient()

    result = process_record(
        api_client=client,
        state=state,
        record=record,
        parent_file_id="100",
        max_retries=3,
        stop_controller=stop_controller,
        global_pause=ShutdownStartingPause(),
    )

    assert result == "deferred"
    assert client.calls == 0
    assert state.completed == set()


def test_run_file_phase_flushes_after_terminal_outcomes(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=2,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 2, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
        SourceRecord("k2", "fedcba9876543210fedcba9876543210", 2, "b.txt", "b.txt", ""),
    ]

    class AlwaysCompleteClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    flushes = []
    original_flush = state.flush

    def wrapped_flush(state_path):
        flushes.append(state.stats.copy())
        original_flush(state_path)

    monkeypatch.setattr(state, "flush", wrapped_flush)

    summary = run_file_phase(
        api_client=AlwaysCompleteClient(),
        state=state,
        records=records,
        state_path=tmp_path / "state.json",
        max_retries=5,
        flush_every=1,
    )

    assert summary == {"processed": 2, "credential_fatal": False}
    assert state.stats["completed"] == 2
    assert flushes == [
        {"total": 2, "completed": 1, "not_reusable": 0, "failed": 0},
        {"total": 2, "completed": 2, "not_reusable": 0, "failed": 0},
        {"total": 2, "completed": 2, "not_reusable": 0, "failed": 0},
    ]


def test_run_file_phase_does_not_dequeue_new_work_after_shutdown_begins_during_pause_wait(
    monkeypatch, tmp_path
):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]
    shutdown_started = {"value": False}

    class ShutdownStartingPause:
        def pause(self, seconds: float) -> None:  # pragma: no cover - guard
            raise AssertionError("pause() should not be used")

        def wait(self, *, stop_controller=None) -> bool:
            stop_controller.start_shutdown()
            shutdown_started["value"] = True
            return True

    class GuardedQueue:
        def __init__(self):
            self.items = []

        def put(self, item):
            self.items.append(item)

        def get_nowait(self):
            if shutdown_started["value"]:
                raise AssertionError("worker dequeued after shutdown started")
            return self.items.pop(0)

        def task_done(self):
            return None

    monkeypatch.setattr("fastlink_transfer.runner.GlobalPause", ShutdownStartingPause)
    monkeypatch.setattr("fastlink_transfer.runner.queue.Queue", GuardedQueue)

    summary = run_file_phase(
        api_client=object(),
        state=state,
        records=records,
        state_path=tmp_path / "state.json",
        max_retries=0,
        flush_every=1,
    )

    assert summary == {"processed": 0, "credential_fatal": False}
    assert state.completed == set()


def test_run_file_phase_processes_fixture_retry_failed_record_with_cached_parent_folder(tmp_path):
    export = load_export_file(SAMPLE_EXPORT_FIXTURE)
    first_record, second_record = export.records
    pending_records = select_pending_records(
        records=export.records,
        completed_keys={first_record.key},
        not_reusable_keys={second_record.key},
        failed_keys=set(),
        retry_failed=True,
    )
    state = TransferState(
        source_file=str(SAMPLE_EXPORT_FIXTURE.resolve()),
        source_sha256=export.source_sha256,
        target_parent_id="100",
        common_path=export.common_path,
        workers=1,
        folder_map={
            "": "100",
            "Demo": "101",
            "Demo/1983": "102",
            "Demo/1983/06": "103",
            "Demo/1983/07": "104",
        },
        completed={first_record.key},
        not_reusable={
            second_record.key: {
                "key": second_record.key,
                "path": second_record.path,
                "error": "Reuse=false",
            }
        },
        failed={},
        stats={"total": 2, "completed": 1, "not_reusable": 1, "failed": 0},
        last_flush_at=None,
    )

    class RecordingUploadClient:
        def __init__(self):
            self.calls = []

        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            self.calls.append((etag, size, file_name, parent_file_id))
            return Decision(kind=DecisionKind.COMPLETED, file_id="500")

    client = RecordingUploadClient()

    summary = run_file_phase(
        api_client=client,
        state=state,
        records=pending_records,
        state_path=tmp_path / "state.json",
        max_retries=0,
        flush_every=1,
    )

    assert client.calls == [
        (
            second_record.etag,
            second_record.size,
            second_record.file_name,
            "104",
        )
    ]
    assert summary == {"processed": 1, "credential_fatal": False}
    assert state.completed == {first_record.key, second_record.key}
    assert state.not_reusable == {}


def test_run_file_phase_serializes_terminal_updates_and_flushes(tmp_path):
    class OverlapDetectingState:
        def __init__(self):
            self.common_path = ""
            self.workers = 2
            self.folder_map = {"": "100"}
            self.completed = set()
            self.not_reusable = {}
            self.failed = {}
            self.stats = {"total": 2, "completed": 0, "not_reusable": 0, "failed": 0}
            self._first_mutation_started = False
            self._allow_first_mutation = threading.Event()
            self.overlap_detected = False

        def record_completed(self, key: str) -> None:
            if not self._first_mutation_started:
                self._first_mutation_started = True
                threading.Timer(0.2, self._allow_first_mutation.set).start()
                self._allow_first_mutation.wait(timeout=1)
            self.completed.add(key)
            self.stats["completed"] = len(self.completed)

        def record_not_reusable(self, *, key: str, path: str, error: str) -> None:  # pragma: no cover - unused guard
            raise AssertionError("unexpected not_reusable path")

        def record_failed(self, *, key: str, path: str, error: str, retries: int) -> None:  # pragma: no cover - unused guard
            raise AssertionError("unexpected failed path")

        def flush(self, state_path) -> None:
            if self._first_mutation_started and not self._allow_first_mutation.is_set():
                self.overlap_detected = True

    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
        SourceRecord("k2", "fedcba9876543210fedcba9876543210", 2, "b.txt", "b.txt", ""),
    ]

    class AlwaysCompleteClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    state = OverlapDetectingState()
    run_file_phase(
        api_client=AlwaysCompleteClient(),
        state=state,
        records=records,
        state_path=tmp_path / "state.json",
        max_retries=0,
        flush_every=1,
    )

    assert state.overlap_detected is False


def test_run_file_phase_surfaces_worker_exception(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class AlwaysCompleteClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    original_flush = state.flush

    def worker_only_failure(state_path):
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("worker flush exploded")
        original_flush(state_path)

    monkeypatch.setattr(state, "flush", worker_only_failure)

    with pytest.raises(RuntimeError, match="worker flush exploded"):
        run_file_phase(
            api_client=AlwaysCompleteClient(),
            state=state,
            records=records,
            state_path=tmp_path / "state.json",
            max_retries=0,
            flush_every=1,
        )


def test_run_file_phase_surfaces_global_pause_wait_exception(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class AlwaysCompleteClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    class ExplodingPause:
        def pause(self, seconds: float) -> None:
            return

        def wait(self, *, stop_controller=None) -> bool:
            raise RuntimeError("pause wait exploded")

    monkeypatch.setattr("fastlink_transfer.runner.GlobalPause", ExplodingPause)

    with pytest.raises(RuntimeError, match="pause wait exploded"):
        run_file_phase(
            api_client=AlwaysCompleteClient(),
            state=state,
            records=records,
            state_path=tmp_path / "state.json",
            max_retries=0,
            flush_every=1,
        )


def test_run_file_phase_best_effort_flush_before_reraising_worker_exception(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class AlwaysCompleteClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    flush_threads = []

    def flush_with_two_failures(state_path):
        flush_threads.append(threading.current_thread() is threading.main_thread())
        if threading.current_thread() is threading.main_thread():
            raise RuntimeError("main flush exploded")
        raise RuntimeError("worker flush exploded")

    monkeypatch.setattr(state, "flush", flush_with_two_failures)

    with pytest.raises(RuntimeError, match="worker flush exploded"):
        run_file_phase(
            api_client=AlwaysCompleteClient(),
            state=state,
            records=records,
            state_path=tmp_path / "state.json",
            max_retries=0,
            flush_every=1,
        )

    assert flush_threads == [False, True]


def test_run_file_phase_preserves_keyboard_interrupt_when_interrupt_flush_fails(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class SlowClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            raise AssertionError("worker should not reach upload during interrupt test")

    original_join = threading.Thread.join
    join_calls = {"count": 0}
    flush_calls = []
    interrupt = KeyboardInterrupt()

    def interrupting_join(self, timeout=None):
        join_calls["count"] += 1
        if join_calls["count"] == 1:
            raise interrupt
        return None

    def failing_flush(state_path):
        flush_calls.append(threading.current_thread() is threading.main_thread())
        raise RuntimeError("interrupt flush exploded")

    monkeypatch.setattr(threading.Thread, "join", interrupting_join)
    monkeypatch.setattr(state, "flush", failing_flush)

    try:
        with pytest.raises(KeyboardInterrupt) as exc_info:
            run_file_phase(
                api_client=SlowClient(),
                state=state,
                records=records,
                state_path=tmp_path / "state.json",
                max_retries=0,
                flush_every=1,
            )
    finally:
        monkeypatch.setattr(threading.Thread, "join", original_join)

    assert exc_info.value is interrupt
    assert flush_calls == [True]


def test_run_file_phase_flushes_when_thread_start_is_interrupted(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class NeverCalledClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):  # pragma: no cover - guard
            raise AssertionError("upload should not start when thread start is interrupted")

    original_start = threading.Thread.start
    flush_calls = []
    interrupt = KeyboardInterrupt("start interrupted")

    def interrupting_start(self):
        raise interrupt

    def recording_flush(state_path):
        flush_calls.append(threading.current_thread() is threading.main_thread())

    monkeypatch.setattr(threading.Thread, "start", interrupting_start)
    monkeypatch.setattr(state, "flush", recording_flush)

    try:
        with pytest.raises(KeyboardInterrupt) as exc_info:
            run_file_phase(
                api_client=NeverCalledClient(),
                state=state,
                records=records,
                state_path=tmp_path / "state.json",
                max_retries=0,
                flush_every=1,
            )
    finally:
        monkeypatch.setattr(threading.Thread, "start", original_start)

    assert exc_info.value is interrupt
    assert flush_calls == [True]


def test_run_file_phase_preserves_original_keyboard_interrupt_when_interrupt_flush_raises_keyboard_interrupt(
    monkeypatch, tmp_path
):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class SlowClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):  # pragma: no cover - guard
            raise AssertionError("worker should not reach upload during interrupt test")

    original_join = threading.Thread.join
    join_calls = {"count": 0}
    interrupt = KeyboardInterrupt("original interrupt")
    flush_interrupt = KeyboardInterrupt("flush interrupt")

    def interrupting_join(self, timeout=None):
        join_calls["count"] += 1
        if join_calls["count"] == 1:
            raise interrupt
        return None

    def failing_flush(state_path):
        raise flush_interrupt

    monkeypatch.setattr(threading.Thread, "join", interrupting_join)
    monkeypatch.setattr(state, "flush", failing_flush)

    try:
        with pytest.raises(KeyboardInterrupt) as exc_info:
            run_file_phase(
                api_client=SlowClient(),
                state=state,
                records=records,
                state_path=tmp_path / "state.json",
                max_retries=0,
                flush_every=1,
            )
    finally:
        monkeypatch.setattr(threading.Thread, "join", original_join)

    assert exc_info.value is interrupt
    notes = getattr(exc_info.value, "__notes__", [])
    assert notes == ["Best-effort keyboard interrupt flush failed: KeyboardInterrupt('flush interrupt')"]


def test_run_file_phase_preserves_worker_exception_when_final_flush_raises_keyboard_interrupt(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class AlwaysCompleteClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    original_flush = state.flush
    worker_exception = RuntimeError("worker flush exploded")
    flush_interrupt = KeyboardInterrupt("flush interrupt")

    def flush_with_keyboard_interrupt_on_main_thread(state_path):
        if threading.current_thread() is threading.main_thread():
            raise flush_interrupt
        raise worker_exception

    monkeypatch.setattr(state, "flush", flush_with_keyboard_interrupt_on_main_thread)

    with pytest.raises(RuntimeError) as exc_info:
        run_file_phase(
            api_client=AlwaysCompleteClient(),
            state=state,
            records=records,
            state_path=tmp_path / "state.json",
            max_retries=0,
            flush_every=1,
        )

    assert exc_info.value is worker_exception
    notes = getattr(exc_info.value, "__notes__", [])
    assert notes == ["Best-effort final flush failed: KeyboardInterrupt('flush interrupt')"]


def test_run_file_phase_retries_best_effort_flush_when_final_flush_is_interrupted(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    state_path = tmp_path / "state.json"
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
    ]

    class AlwaysCompleteClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id="1")

    original_flush = state.flush
    interrupt = KeyboardInterrupt("final flush interrupt")
    flush_threads = []

    def interrupting_final_flush(state_path):
        flush_threads.append(threading.current_thread() is threading.main_thread())
        if len(flush_threads) == 1:
            raise interrupt
        original_flush(state_path)

    monkeypatch.setattr(state, "flush", interrupting_final_flush)

    with pytest.raises(KeyboardInterrupt) as exc_info:
        run_file_phase(
            api_client=AlwaysCompleteClient(),
            state=state,
            records=records,
            state_path=state_path,
            max_retries=0,
            flush_every=2,
        )

    assert exc_info.value is interrupt
    assert flush_threads == [True, True]
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["completed"] == ["k1"]


def test_run_file_phase_stops_new_dequeues_immediately_after_credential_fatal(monkeypatch, tmp_path):
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=2,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 2, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    records = [
        SourceRecord("k1", "0123456789abcdef0123456789abcdef", 1, "a.txt", "a.txt", ""),
        SourceRecord("k2", "fedcba9876543210fedcba9876543210", 2, "b.txt", "b.txt", ""),
    ]

    class LaggingVisibilityStopController(StopController):
        fatal_event = threading.Event()

        def __init__(self) -> None:
            super().__init__()
            self._stale_check_served = False

        def start_shutdown(self, *, credential_fatal: bool = False) -> None:
            super().start_shutdown(credential_fatal=credential_fatal)
            if credential_fatal:
                self.fatal_event.set()

        def shutdown_started(self) -> bool:
            if self._event.is_set() and not self._stale_check_served:
                self._stale_check_served = True
                return False
            return super().shutdown_started()

    class WaitForFatalPause:
        leader_ident = None

        def pause(self, seconds: float) -> None:
            return

        def wait(self, *, stop_controller=None) -> bool:
            current_ident = threading.get_ident()
            if self.leader_ident is None:
                type(self).leader_ident = current_ident
                return True
            if current_ident == self.leader_ident:
                return True
            assert stop_controller is not None
            assert LaggingVisibilityStopController.fatal_event.wait(timeout=1)
            return True

    class GuardedQueue:
        def __init__(self):
            self.items = []

        def put(self, item):
            self.items.append(item)

        def get_nowait(self):
            if LaggingVisibilityStopController.fatal_event.is_set() and len(self.items) == 1:
                raise AssertionError("worker dequeued after credential fatal")
            if not self.items:
                raise queue.Empty
            return self.items.pop(0)

        def task_done(self):
            return None

    class CredentialFatalClient:
        def __init__(self):
            self.calls = []

        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            self.calls.append(file_name)
            return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error="HTTP 401")

    monkeypatch.setattr("fastlink_transfer.runner.StopController", LaggingVisibilityStopController)
    monkeypatch.setattr("fastlink_transfer.runner.GlobalPause", WaitForFatalPause)
    monkeypatch.setattr("fastlink_transfer.runner.queue.Queue", GuardedQueue)

    client = CredentialFatalClient()
    summary = run_file_phase(
        api_client=client,
        state=state,
        records=records,
        state_path=tmp_path / "state.json",
        max_retries=0,
        flush_every=10,
    )

    assert summary == {"processed": 0, "credential_fatal": True}
    assert client.calls == ["a.txt"]
    assert state.completed == set()
    assert state.failed == {}
    assert state.not_reusable == {}


def test_process_record_does_not_start_new_upload_after_shutdown_begins_between_wait_and_post():
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    record = SourceRecord(
        key="k",
        etag="0123456789abcdef0123456789abcdef",
        size=1,
        path="a.txt",
        file_name="a.txt",
        relative_parent_dir="",
    )
    stop_controller = StopController()

    class OpenGatePause:
        def pause(self, seconds: float) -> None:  # pragma: no cover - guard
            raise AssertionError("pause() should not be used")

        def wait(self, *, stop_controller=None) -> bool:
            return True

    class RecordingSession:
        def __init__(self):
            self.post_called = False

        def post(self, url, json):
            self.post_called = True
            return type(
                "Response",
                (),
                {"status_code": 200, "json": lambda self: {"code": 0, "data": {"Reuse": True, "Info": {"FileId": 1}}}},
            )()

    class ShutdownBeforeStartStopController(StopController):
        def begin_upload_attempt(self) -> bool:
            self.start_shutdown(credential_fatal=True)
            return False

    session = RecordingSession()
    stop_controller = ShutdownBeforeStartStopController()
    client = PanApiClient(host="https://www.123pan.com", session=session)

    result = process_record(
        api_client=client,
        state=state,
        record=record,
        parent_file_id="100",
        max_retries=0,
        stop_controller=stop_controller,
        global_pause=OpenGatePause(),
    )

    assert result == "deferred"
    assert session.post_called is False
    assert state.completed == set()


def test_stop_controller_claim_next_work_item_waits_for_pause_before_dequeue():
    stop_controller = StopController()
    work_queue: queue.Queue = queue.Queue()
    work_queue.put(("record", "100"))
    claimed = {}

    stop_controller.pause_new_work(0.2)

    def target() -> None:
        claimed["item"] = stop_controller.claim_next_work_item(work_queue)

    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    time.sleep(0.05)

    assert "item" not in claimed
    assert work_queue.qsize() == 1

    thread.join(timeout=1)
    assert thread.is_alive() is False
    assert claimed["item"] == ("record", "100")


def test_stop_controller_begin_upload_attempt_waits_for_pause_before_start():
    stop_controller = StopController()
    begin_results = {}

    stop_controller.pause_new_work(0.2)

    def target() -> None:
        begin_results["value"] = stop_controller.begin_upload_attempt()
        if begin_results["value"]:
            stop_controller.finish_upload_attempt()

    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    time.sleep(0.05)

    assert "value" not in begin_results

    thread.join(timeout=1)
    assert thread.is_alive() is False
    assert begin_results["value"] is True


def test_global_pause_wait_returns_when_shutdown_starts():
    pause = GlobalPause()
    pause.pause(5.0)
    stop_controller = StopController()
    result = {}

    def target() -> None:
        result["value"] = pause.wait(stop_controller=stop_controller)

    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    time.sleep(0.05)
    stop_controller.start_shutdown()
    thread.join(timeout=0.3)
    if thread.is_alive():
        thread.join(timeout=1)
    assert thread.is_alive() is False
    assert result["value"] is False


def test_process_record_interrupts_retry_sleep_when_shutdown_starts(monkeypatch):
    monkeypatch.setattr("fastlink_transfer.runner.compute_backoff", lambda _attempt: 1.0)
    first_call_started = threading.Event()

    class SignalingClient:
        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            first_call_started.set()
            return Decision(kind=DecisionKind.RETRYABLE, error="HTTP 500")

    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="100",
        common_path="",
        workers=1,
        folder_map={"": "100"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )
    record = SourceRecord(
        key="k",
        etag="0123456789abcdef0123456789abcdef",
        size=1,
        path="a.txt",
        file_name="a.txt",
        relative_parent_dir="",
    )
    stop_controller = StopController()
    result = {}

    def target() -> None:
        result["value"] = process_record(
            api_client=SignalingClient(),
            state=state,
            record=record,
            parent_file_id="100",
            max_retries=3,
            stop_controller=stop_controller,
            global_pause=GlobalPause(),
        )

    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    assert first_call_started.wait(timeout=1)
    stop_controller.start_shutdown()
    thread.join(timeout=0.3)
    if thread.is_alive():
        thread.join(timeout=2)
    assert thread.is_alive() is False
    assert result["value"] == "deferred"
