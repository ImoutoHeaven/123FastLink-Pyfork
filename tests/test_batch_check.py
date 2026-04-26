import json
import queue
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from fastlink_transfer.api import Decision, DecisionKind
from fastlink_transfer.batch_check import (
    BatchCheckJob,
    discover_batch_check_jobs,
    plan_batch_check_job,
    run_batch_check_cli,
    run_batch_check_jobs,
    run_remote_scan,
)
from fastlink_transfer.check_state import open_or_initialize_check_state
from fastlink_transfer.config import BatchCheckJsonConfig
from fastlink_transfer.import_planner import inspect_export_scope
from fastlink_transfer.runner import DirectoryPhaseCredentialFatalError


def _make_batch_check_config(tmp_path: Path) -> BatchCheckJsonConfig:
    return BatchCheckJsonConfig(
        command="batch_check_json",
        input_dir=(tmp_path / "exports").resolve(),
        target_parent_id="12345678",
        state_dir=(tmp_path / ".state" / "check").resolve(),
        output_dir=(tmp_path / "delta").resolve(),
        workers=8,
        json_parallelism=2,
        max_retries=5,
        flush_every=100,
        compare_mode="exist_only",
    )


def _write_export_json(tmp_path: Path, *, common_path: str) -> Path:
    export_path = tmp_path / "demo.json"
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": common_path,
                "files": [{"etag": "0" * 32, "size": "9", "path": "1983/clip.mkv"}],
            }
        ),
        encoding="utf-8",
    )
    return export_path


def write_export_json(export_path: Path, *, common_path: str, files: list[dict]) -> Path:
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": common_path,
                "files": files,
            }
        ),
        encoding="utf-8",
    )
    return export_path


def make_check_job(*, root: Path, relative_json_path: str) -> BatchCheckJob:
    relative_path = Path(relative_json_path)
    return BatchCheckJob(
        json_path=root / "exports" / relative_path,
        relative_json_path=relative_path,
        state_path=root / ".state" / "check" / relative_path.with_suffix(".check-state.sqlite3"),
        output_path=root / "delta" / relative_path.with_suffix(".delta.export.json"),
    )


def planned_state_with_common_path(tmp_path: Path, *, common_path: str):
    export_path = _write_export_json(tmp_path, common_path=common_path)
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_check_state(
        state_path=tmp_path / "demo.check-state.sqlite3",
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id="12345678",
        common_path=scope.common_path,
    )
    plan_batch_check_job(export_path=export_path, state=state)
    return state


def seed_remote_scan_statuses(
    state,
    *,
    pending: list[tuple[str, str]],
    inflight: list[tuple[str, str]],
    completed: list[tuple[str, str]],
) -> None:
    rows = [
        *((path, remote_dir_id, "pending") for path, remote_dir_id in pending),
        *((path, remote_dir_id, "inflight") for path, remote_dir_id in inflight),
        *((path, remote_dir_id, "completed") for path, remote_dir_id in completed),
    ]
    state.connection.executemany(
        "INSERT INTO remote_dirs (target_relative_path, remote_dir_id, status) VALUES (?, ?, ?)",
        rows,
    )
    state.connection.execute(
        "UPDATE job SET remote_dirs = ?, remote_scan_complete = 0 WHERE singleton = 1",
        (len(rows),),
    )
    state.connection.commit()


class FakeCheckClient:
    host = "https://www.123pan.com"

    def __init__(self) -> None:
        self.list_calls: list[str] = []
        self._items_by_parent = {
            "12345678": [{"FileName": "Demo", "FileId": "100", "Type": 1}],
            "100": [
                {"FileName": "Action", "FileId": "101", "Type": 1},
                {"FileName": "1983", "FileId": "102", "Type": 1},
                {"FileName": "a", "FileId": "200", "Type": 1},
                {"FileName": "b", "FileId": "201", "Type": 1},
            ],
            "101": [{"FileName": "1983", "FileId": "103", "Type": 1}],
            "102": [{"FileName": "clip.mkv", "Type": 0, "Etag": "1" * 32, "Size": 9}],
            "103": [{"FileName": "clip.mkv", "Type": 0, "Etag": "1" * 32, "Size": 9}],
            "200": [],
            "201": [],
        }

    def get_file_list(self, *, parent_file_id: str):
        self.list_calls.append(parent_file_id)
        items = self._items_by_parent[parent_file_id]
        return Decision(
            kind=DecisionKind.COMPLETED,
            payload={"items": list(items), "total": len(items)},
        )

    def get_file_list_page(self, *, parent_file_id: str, page: int):
        if page != 1:
            raise AssertionError(page)
        return self.get_file_list(parent_file_id=parent_file_id)


class MissingPathClient(FakeCheckClient):
    pass


class CredentialFatalClient(FakeCheckClient):
    def get_file_list(self, *, parent_file_id: str):
        self.list_calls.append(parent_file_id)
        return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error="login expired")


class RetryableFailureClient(FakeCheckClient):
    def get_file_list(self, *, parent_file_id: str):
        self.list_calls.append(parent_file_id)
        return Decision(kind=DecisionKind.RETRYABLE, error="retry me")


class NormalizingCheckClient(FakeCheckClient):
    def __init__(self) -> None:
        super().__init__()
        self._items_by_parent = {
            "12345678": [{"FileName": "Demo", "FileId": "100", "Type": 1}],
            "100": [{"FileName": "bad\\name.txt", "Type": 0, "Etag": "2" * 32, "Size": 7}],
        }


class ConcurrentCheckClient(FakeCheckClient):
    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._active_calls = 0
        self.max_active_calls = 0

    def get_file_list(self, *, parent_file_id: str):
        if parent_file_id in {"200", "201"}:
            with self._lock:
                self._active_calls += 1
                self.max_active_calls = max(self.max_active_calls, self._active_calls)
            try:
                time.sleep(0.05)
                return super().get_file_list(parent_file_id=parent_file_id)
            finally:
                with self._lock:
                    self._active_calls -= 1
        return super().get_file_list(parent_file_id=parent_file_id)


class CountingCheckClient(FakeCheckClient):
    pass


class PaginatedOnlyCheckClient:
    host = "https://www.123pan.com"

    def __init__(self) -> None:
        self.page_calls: list[tuple[str, int]] = []
        self._pages_by_parent = {
            "12345678": [
                [
                    {"FileName": f"filler-{index}", "FileId": f"9{index}", "Type": 1}
                    for index in range(100)
                ],
                [{"FileName": "Demo", "FileId": "100", "Type": 1}],
            ],
            "100": [
                [{"FileName": f"clip-{index}.mkv", "Type": 0, "Etag": "1" * 32, "Size": 9} for index in range(100)],
                [{"FileName": "clip-100.mkv", "Type": 0, "Etag": "1" * 32, "Size": 9}],
            ],
        }

    def get_file_list(self, *, parent_file_id: str):
        raise AssertionError(f"aggregated get_file_list should not be used for {parent_file_id}")

    def get_file_list_page(self, *, parent_file_id: str, page: int):
        self.page_calls.append((parent_file_id, page))
        pages = self._pages_by_parent[parent_file_id]
        items = pages[page - 1]
        total = sum(len(batch) for batch in pages)
        return Decision(
            kind=DecisionKind.COMPLETED,
            payload={"items": list(items), "total": total},
        )


def test_discover_batch_check_jobs_recurses_and_preserves_relative_layout(tmp_path):
    input_dir = tmp_path / "exports"
    (input_dir / "nested").mkdir(parents=True)
    (input_dir / "nested" / "demo.json").write_text("{}", encoding="utf-8")

    jobs = discover_batch_check_jobs(
        input_dir=input_dir,
        state_dir=tmp_path / ".state" / "check",
        output_dir=tmp_path / "delta",
    )

    assert [job.relative_json_path.as_posix() for job in jobs] == ["nested/demo.json"]
    assert jobs[0].state_path == tmp_path / ".state" / "check" / "nested" / "demo.check-state.sqlite3"
    assert jobs[0].output_path == tmp_path / "delta" / "nested" / "demo.delta.export.json"


def test_discover_batch_check_jobs_rejects_output_dir_nested_inside_input_dir(tmp_path):
    input_dir = tmp_path / "exports"
    output_dir = input_dir / "delta"
    input_dir.mkdir()
    (input_dir / "demo.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match=r"--output-dir must not be the same as or nested inside --input-dir"):
        discover_batch_check_jobs(
            input_dir=input_dir,
            state_dir=tmp_path / ".state" / "check",
            output_dir=output_dir,
        )


def test_discover_batch_check_jobs_rejects_state_dir_equal_to_input_dir(tmp_path):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    (input_dir / "demo.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match=r"--state-dir must not be the same as or nested inside --input-dir"):
        discover_batch_check_jobs(
            input_dir=input_dir,
            state_dir=input_dir,
            output_dir=tmp_path / "delta",
        )


def test_discover_batch_check_jobs_rejects_state_dir_nested_inside_input_dir(tmp_path):
    input_dir = tmp_path / "exports"
    state_dir = input_dir / ".state" / "check"
    input_dir.mkdir()
    state_dir.mkdir(parents=True)
    (input_dir / "demo.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match=r"--state-dir must not be the same as or nested inside --input-dir"):
        discover_batch_check_jobs(
            input_dir=input_dir,
            state_dir=state_dir,
            output_dir=tmp_path / "delta",
        )


def test_run_batch_check_cli_fails_when_no_json_files_exist(tmp_path, capsys):
    config = _make_batch_check_config(tmp_path)
    config.input_dir.mkdir()

    assert run_batch_check_cli(config=config) == 1
    assert "Batch startup failed: no json files found" in capsys.readouterr().out


def test_run_batch_check_cli_treats_state_root_setup_failure_as_startup_fatal(
    tmp_path, monkeypatch, capsys
):
    config = _make_batch_check_config(tmp_path)
    config.input_dir.mkdir()
    (config.input_dir / "demo.json").write_text("{}", encoding="utf-8")
    original_mkdir = Path.mkdir

    def failing_mkdir(self, *args, **kwargs):
        if self == config.state_dir:
            raise OSError("boom")
        return original_mkdir(self, *args, **kwargs)

    monkeypatch.setattr("pathlib.Path.mkdir", failing_mkdir)

    assert run_batch_check_cli(config=config) == 1
    assert "Batch startup failed:" in capsys.readouterr().out


def test_run_batch_check_cli_treats_output_root_setup_failure_as_startup_fatal(
    tmp_path, monkeypatch, capsys
):
    config = _make_batch_check_config(tmp_path)
    config.input_dir.mkdir()
    (config.input_dir / "demo.json").write_text("{}", encoding="utf-8")
    original_mkdir = Path.mkdir

    def failing_mkdir(self, *args, **kwargs):
        if self == config.output_dir:
            raise OSError("boom")
        return original_mkdir(self, *args, **kwargs)

    monkeypatch.setattr("pathlib.Path.mkdir", failing_mkdir)

    assert run_batch_check_cli(config=config) == 1
    assert "Batch startup failed:" in capsys.readouterr().out


def test_run_remote_scan_persists_resolved_common_path_prefixes_and_descendants(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/Action/")
    client = FakeCheckClient()

    run_remote_scan(api_client=client, state=state, state_path=state.state_path, max_retries=1)

    assert state.fetch_remote_dirs() == ["Demo", "Demo/Action", "Demo/Action/1983"]
    assert state.fetch_remote_file("1983/clip.mkv")["target_relative_path"] == "Demo/Action/1983/clip.mkv"


def test_run_remote_scan_recovers_inflight_rows_without_rescanning_completed_dirs(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")
    client = FakeCheckClient()
    seed_remote_scan_statuses(
        state,
        pending=[("Demo/b", "201")],
        inflight=[("Demo/a", "200")],
        completed=[("Demo", "12345678")],
    )

    run_remote_scan(api_client=client, state=state, state_path=state.state_path, max_retries=1)

    assert state.fetch_remote_dir_statuses()["Demo/a"] == "completed"
    assert state.fetch_remote_dir_statuses()["Demo"] == "completed"
    assert "12345678" not in client.list_calls


def test_run_remote_scan_marks_remote_root_missing_when_common_path_segment_is_absent(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/Missing/")

    run_remote_scan(api_client=MissingPathClient(), state=state, state_path=state.state_path, max_retries=1)

    assert state.phase["remote_scan_complete"] is True
    assert state.job_flags["remote_root_missing"] is True


def test_run_remote_scan_persists_remote_checksum_and_size_during_exist_only_mode(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")

    run_remote_scan(api_client=FakeCheckClient(), state=state, state_path=state.state_path, max_retries=1)

    remote_row = state.fetch_remote_file("1983/clip.mkv")
    assert remote_row["etag_hex"] == "1" * 32
    assert remote_row["size"] == 9
    assert state.summary_counters["remote_files"] >= 1


def test_run_remote_scan_raises_credential_fatal_distinctly(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")

    with pytest.raises(DirectoryPhaseCredentialFatalError, match="login expired"):
        run_remote_scan(api_client=CredentialFatalClient(), state=state, state_path=state.state_path, max_retries=1)


def test_run_remote_scan_keeps_retry_exhaustion_distinct_from_credential_fatal(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")

    with pytest.raises(RuntimeError, match="retry me"):
        run_remote_scan(api_client=RetryableFailureClient(), state=state, state_path=state.state_path, max_retries=1)


def test_run_remote_scan_normalizes_remote_file_coordinates_before_persisting(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")

    run_remote_scan(api_client=NormalizingCheckClient(), state=state, state_path=state.state_path, max_retries=1)

    remote_row = state.fetch_remote_file("bad/name.txt")
    assert remote_row["target_relative_path"] == "Demo/bad/name.txt"


def test_run_remote_scan_uses_worker_count_and_flush_threshold(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")
    state.workers = 2
    state.flush_every = 1
    flush_calls = []
    state.flush_remote_progress = lambda *, state_path: flush_calls.append(state_path)
    seed_remote_scan_statuses(
        state,
        pending=[("Demo/a", "200"), ("Demo/b", "201")],
        inflight=[],
        completed=[("Demo", "100")],
    )
    client = ConcurrentCheckClient()

    run_remote_scan(api_client=client, state=state, state_path=state.state_path, max_retries=1)

    assert client.max_active_calls >= 2
    assert len(flush_calls) >= 2


def test_run_remote_scan_does_not_dispatch_more_than_worker_limit(tmp_path, monkeypatch):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")
    state.workers = 1
    seed_remote_scan_statuses(
        state,
        pending=[("Demo/1983", "102"), ("Demo/a", "200"), ("Demo/b", "201")],
        inflight=[],
        completed=[("Demo", "100")],
    )
    client = FakeCheckClient()
    original_start = threading.Thread.start
    original_get = queue.Queue.get
    original_put = queue.Queue.put
    deferred_threads: list[threading.Thread] = []
    work_queue_ids: set[int] = set()
    outstanding_work_items = 0
    max_outstanding_work_items = 0

    def deferred_start(self):
        deferred_threads.append(self)

    def tracking_put(self, item, *args, **kwargs):
        nonlocal outstanding_work_items, max_outstanding_work_items
        if (
            isinstance(item, tuple)
            and len(item) == 2
            and all(isinstance(value, str) for value in item)
        ):
            work_queue_ids.add(id(self))
            outstanding_work_items += 1
            max_outstanding_work_items = max(max_outstanding_work_items, outstanding_work_items)
        return original_put(self, item, *args, **kwargs)

    def tracking_get(self, *args, **kwargs):
        nonlocal outstanding_work_items, deferred_threads
        if deferred_threads and threading.current_thread() is threading.main_thread():
            threads_to_start = deferred_threads
            deferred_threads = []
            for thread in threads_to_start:
                original_start(thread)
        item = original_get(self, *args, **kwargs)
        if (
            id(self) in work_queue_ids
            and isinstance(item, tuple)
            and len(item) == 2
            and all(isinstance(value, str) for value in item)
        ):
            outstanding_work_items -= 1
        return item

    monkeypatch.setattr(threading.Thread, "start", deferred_start)
    monkeypatch.setattr(queue.Queue, "put", tracking_put)
    monkeypatch.setattr(queue.Queue, "get", tracking_get)

    run_remote_scan(api_client=client, state=state, state_path=state.state_path, max_retries=1)

    assert max_outstanding_work_items <= 1


def test_run_remote_scan_processes_large_paginated_directory_in_bounded_batches(tmp_path):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")
    state.workers = 1
    client = PaginatedOnlyCheckClient()
    original_commit = state.commit_remote_scan_directory
    batch_sizes: list[tuple[int, int]] = []

    def bounded_commit(*, target_relative_path, child_dirs, file_rows, **kwargs):
        if child_dirs or file_rows:
            batch_sizes.append((len(child_dirs), len(file_rows)))
            assert len(child_dirs) <= 100
            assert len(file_rows) <= 100
        return original_commit(
            target_relative_path=target_relative_path,
            child_dirs=child_dirs,
            file_rows=file_rows,
            **kwargs,
        )

    state.commit_remote_scan_directory = bounded_commit

    run_remote_scan(api_client=client, state=state, state_path=state.state_path, max_retries=1)

    assert state.summary_counters["remote_files"] == 101
    assert len(batch_sizes) >= 2
    assert client.page_calls == [("12345678", 1), ("12345678", 2), ("100", 1), ("100", 2)]


def test_run_remote_scan_persists_progress_without_remote_table_recounts(tmp_path, monkeypatch):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")
    client = FakeCheckClient()
    real_connection = state.connection

    class NoRemoteCountConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def execute(self, query, parameters=()):
            if isinstance(query, str) and "SELECT COUNT(*) FROM remote_" in query:
                raise AssertionError("remote scan should not recount whole remote tables")
            return self._wrapped.execute(query, parameters)

        def __enter__(self):
            self._wrapped.__enter__()
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._wrapped.__exit__(exc_type, exc, tb)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    monkeypatch.setattr(state, "connection", NoRemoteCountConnection(real_connection))

    run_remote_scan(api_client=client, state=state, state_path=state.state_path, max_retries=1)

    assert state.summary_counters["remote_files"] == 2
    assert state.summary_counters["remote_dirs"] == 6


def test_run_remote_scan_dispatches_pending_dirs_without_fetchall(tmp_path, monkeypatch):
    state = planned_state_with_common_path(tmp_path, common_path="Demo/")
    state.workers = 1
    seed_remote_scan_statuses(
        state,
        pending=[("Demo/a", "200")],
        inflight=[],
        completed=[("Demo", "100")],
    )
    client = FakeCheckClient()
    real_connection = state.connection

    class PendingRowsCursor:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def __iter__(self):
            return iter(self._wrapped)

        def fetchone(self):
            return self._wrapped.fetchone()

        def fetchall(self):
            raise AssertionError("fetchall should not be used for pending remote dirs")

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    class PendingRowsConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def execute(self, query, parameters=()):
            cursor = self._wrapped.execute(query, parameters)
            if isinstance(query, str) and "WHERE status = 'pending'" in query:
                return PendingRowsCursor(cursor)
            return cursor

        def __enter__(self):
            self._wrapped.__enter__()
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._wrapped.__exit__(exc_type, exc, tb)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    monkeypatch.setattr(state, "connection", PendingRowsConnection(real_connection))

    run_remote_scan(api_client=client, state=state, state_path=state.state_path, max_retries=1)

    assert state.fetch_remote_dir_statuses()["Demo/a"] == "completed"
    assert client.list_calls[-1] == "200"


def test_run_batch_check_cli_returns_zero_when_jobs_align_or_produce_delta(tmp_path, monkeypatch, capsys):
    config = _make_batch_check_config(tmp_path)
    write_export_json(
        tmp_path / "exports" / "demo.json",
        common_path="Demo/",
        files=[{"etag": "0" * 32, "size": "9", "path": "1983/clip.mkv"}],
    )
    monkeypatch.setattr(
        "fastlink_transfer.batch_check.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.batch_check.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.batch_check.PanApiClient", lambda *args, **kwargs: FakeCheckClient())

    exit_code = run_batch_check_cli(config=config)

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "demo.json" in output
    assert "delta_files=" in output
    assert "missing_dirs=" in output
    assert "Batch summary:" in output
    assert "aligned=" in output
    assert "with_delta=" in output


def test_run_batch_check_cli_reports_directory_only_drift_as_aligned_file_status(
    tmp_path, monkeypatch, capsys
):
    config = _make_batch_check_config(tmp_path)
    write_export_json(
        tmp_path / "exports" / "dir-only.json",
        common_path="Demo/Season1/",
        files=[],
    )
    monkeypatch.setattr(
        "fastlink_transfer.batch_check.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.batch_check.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.batch_check.PanApiClient", lambda *args, **kwargs: FakeCheckClient())

    exit_code = run_batch_check_cli(config=config)

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "dir-only.json" in output
    assert "delta_files=0" in output
    assert "missing_dirs=" in output
    assert "aligned=" in output


def test_run_batch_check_cli_stops_launching_new_jobs_after_credential_fatal(tmp_path):
    jobs = [
        make_check_job(root=tmp_path, relative_json_path="a/one.json"),
        make_check_job(root=tmp_path, relative_json_path="b/two.json"),
    ]
    launched = []

    summary = run_batch_check_jobs(
        jobs=jobs,
        json_parallelism=1,
        run_child_job=lambda job, **kwargs: launched.append(job.relative_json_path.as_posix())
        or SimpleNamespace(
            status="failed",
            credential_fatal=job.relative_json_path.as_posix() == "a/one.json",
            delta_files=0,
            missing_dirs=0,
        ),
    )

    assert launched == ["a/one.json"]
    assert summary.jobs_failed == 1
    assert summary.credential_fatal is True


def test_run_batch_check_cli_treats_missing_remote_subtree_as_successful_delta(
    tmp_path, monkeypatch, capsys
):
    config = _make_batch_check_config(tmp_path)
    write_export_json(
        tmp_path / "exports" / "missing.json",
        common_path="Demo/Missing/",
        files=[{"etag": "0" * 32, "size": "9", "path": "1983/clip.mkv"}],
    )
    monkeypatch.setattr(
        "fastlink_transfer.batch_check.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.batch_check.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.batch_check.PanApiClient", lambda *args, **kwargs: MissingPathClient())

    exit_code = run_batch_check_cli(config=config)

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "missing.json" in output
    assert "delta produced" in output
    assert "missing_dirs=" in output


def test_run_batch_check_cli_mode_switch_rerun_skips_new_remote_scan_work(
    tmp_path, monkeypatch, capsys
):
    config_exist = _make_batch_check_config(tmp_path)
    config_checksum = BatchCheckJsonConfig(
        command="batch_check_json",
        input_dir=config_exist.input_dir,
        target_parent_id=config_exist.target_parent_id,
        state_dir=config_exist.state_dir,
        output_dir=config_exist.output_dir,
        workers=config_exist.workers,
        json_parallelism=config_exist.json_parallelism,
        max_retries=config_exist.max_retries,
        flush_every=config_exist.flush_every,
        compare_mode="with_checksum",
    )
    write_export_json(
        tmp_path / "exports" / "demo.json",
        common_path="Demo/",
        files=[{"etag": "0" * 32, "size": "9", "path": "1983/clip.mkv"}],
    )
    client = CountingCheckClient()
    monkeypatch.setattr(
        "fastlink_transfer.batch_check.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.batch_check.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.batch_check.PanApiClient", lambda *args, **kwargs: client)

    assert run_batch_check_cli(config=config_exist) == 0
    first_scan_calls = list(client.list_calls)
    capsys.readouterr()

    assert run_batch_check_cli(config=config_checksum) == 0
    assert client.list_calls == first_scan_calls


def test_run_batch_check_cli_mode_switch_rerun_reuses_persisted_remote_index_without_credentials(
    tmp_path, monkeypatch, capsys
):
    config_exist = _make_batch_check_config(tmp_path)
    config_checksum = BatchCheckJsonConfig(
        command="batch_check_json",
        input_dir=config_exist.input_dir,
        target_parent_id=config_exist.target_parent_id,
        state_dir=config_exist.state_dir,
        output_dir=config_exist.output_dir,
        workers=config_exist.workers,
        json_parallelism=config_exist.json_parallelism,
        max_retries=config_exist.max_retries,
        flush_every=config_exist.flush_every,
        compare_mode="with_checksum",
    )
    write_export_json(
        tmp_path / "exports" / "demo.json",
        common_path="Demo/",
        files=[{"etag": "0" * 32, "size": "9", "path": "1983/clip.mkv"}],
    )
    client = CountingCheckClient()
    monkeypatch.setattr(
        "fastlink_transfer.batch_check.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.batch_check.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.batch_check.PanApiClient", lambda *args, **kwargs: client)

    assert run_batch_check_cli(config=config_exist) == 0
    first_scan_calls = list(client.list_calls)
    capsys.readouterr()

    def fail_load_credentials():
        raise RuntimeError("credentials unavailable")

    monkeypatch.setattr("fastlink_transfer.batch_check.load_credentials", fail_load_credentials)
    monkeypatch.setattr(
        "fastlink_transfer.batch_check.build_session",
        lambda creds: (_ for _ in ()).throw(AssertionError("build_session should not run")),
    )
    monkeypatch.setattr(
        "fastlink_transfer.batch_check.PanApiClient",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("PanApiClient should not run")),
    )

    assert run_batch_check_cli(config=config_checksum) == 0
    output = capsys.readouterr().out
    assert "delta produced" in output
    assert client.list_calls == first_scan_calls
