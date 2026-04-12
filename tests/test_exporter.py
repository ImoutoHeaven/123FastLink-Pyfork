import errno
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from fastlink_transfer.api import Decision, DecisionKind, PanApiClient
from fastlink_transfer.export_state import load_or_initialize_export_state
from fastlink_transfer.exporter import (
    _load_export_context,
    _run_scan_loop,
    finalize_export_json,
    prepare_sidecar_for_resume,
    run_export_json,
)
from fastlink_transfer.importer import load_export_file


class RecordingSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None):
        self.calls.append(("GET", url, params))
        return self.responses.pop(0)

    def post(self, url, json):
        self.calls.append(("POST", url, json))
        return self.responses.pop(0)


def test_get_directory_identity_returns_directory_name_and_rejects_files():
    session = RecordingSession(
        [
            SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "code": 0,
                    "data": {"infoList": [{"FileId": 1, "Type": 1, "FileName": "Movies"}]},
                },
            ),
            SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "code": 0,
                    "data": {"infoList": [{"FileId": 2, "Type": 0, "FileName": "a.txt"}]},
                },
            ),
        ]
    )
    client = PanApiClient(host="https://www.123pan.com", session=session)

    ok = client.get_directory_identity(parent_file_id="1")
    bad = client.get_directory_identity(parent_file_id="2")

    assert ok.kind == DecisionKind.DIRECTORY_CREATED
    assert ok.payload == {"root_name": "Movies"}
    assert bad.kind == DecisionKind.FAILED


def test_get_file_list_accumulates_all_pages():
    session = RecordingSession(
        [
            SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "code": 0,
                    "data": {
                        "InfoList": [{"FileId": 10, "Type": 1, "FileName": "Action"}],
                        "Total": 101,
                    },
                },
            ),
            SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "code": 0,
                    "data": {
                        "InfoList": [
                            {
                                "FileId": 11,
                                "Type": 0,
                                "FileName": "b.txt",
                                "Etag": "0" * 32,
                                "Size": 5,
                            }
                        ],
                        "Total": 101,
                    },
                },
            ),
        ]
    )
    client = PanApiClient(host="https://www.123pan.com", session=session)

    decision = client.get_file_list(parent_file_id="123")

    assert decision.kind == DecisionKind.COMPLETED
    assert [item["FileId"] for item in decision.payload["items"]] == [10, 11]
    assert decision.payload["total"] == 101


class FakeExportClient:
    def __init__(self):
        self.host = "https://www.123pan.com"
        self.list_calls = []

    def get_directory_identity(self, *, parent_file_id: str):
        return Decision(kind=DecisionKind.DIRECTORY_CREATED, payload={"root_name": "Movies"})

    def get_file_list(self, *, parent_file_id: str):
        self.list_calls.append(parent_file_id)
        if parent_file_id == "12345678":
            return Decision(
                kind=DecisionKind.COMPLETED,
                payload={
                    "items": [
                        {"FileId": 200, "Type": 1, "FileName": "Action"},
                        {"FileId": 201, "Type": 0, "FileName": "root.txt", "Etag": "0" * 32, "Size": 5},
                    ],
                    "total": 2,
                },
            )
        if parent_file_id == "200":
            return Decision(
                kind=DecisionKind.COMPLETED,
                payload={
                    "items": [
                        {"FileId": 202, "Type": 0, "FileName": "clip.mkv", "Etag": "1" * 32, "Size": 9},
                    ],
                    "total": 1,
                },
            )
        raise AssertionError(parent_file_id)


def _config(tmp_path, **overrides):
    values = {
        "command": "export_json",
        "source_parent_id": "12345678",
        "output_file": (tmp_path / "out.json").resolve(),
        "state_file": (tmp_path / "export.state.json").resolve(),
        "workers": 2,
        "max_retries": 1,
        "flush_every": 2,
    }
    values.update(overrides)
    return type("Config", (), values)()


def _success_marker_path(state_file: Path) -> Path:
    return Path(state_file).resolve().with_suffix(".output.committed")


def _write_success_marker(*, state_file: Path, output_file: Path, source_parent_id: str, source_root_name: str = "Movies") -> None:
    marker_payload = {
        "source_host": "https://www.123pan.com",
        "source_parent_id": source_parent_id,
        "source_root_name": source_root_name,
        "common_path": f"{source_root_name}/",
        "output_file": str(Path(output_file).resolve()),
        "records_file": str(Path(state_file).resolve().with_suffix(".records.jsonl")),
    }
    _success_marker_path(state_file).write_text(json.dumps(marker_payload), encoding="utf-8")


def _write_legacy_success_marker(*, state_file: Path) -> None:
    _success_marker_path(state_file).write_text("committed\n", encoding="utf-8")


class MutableSingleFileExportClient(FakeExportClient):
    def __init__(self):
        super().__init__()
        self.snapshot = 1

    def get_file_list(self, *, parent_file_id: str):
        self.list_calls.append(parent_file_id)
        return Decision(
            kind=DecisionKind.COMPLETED,
            payload={
                "items": [
                    {
                        "FileId": 201,
                        "Type": 0,
                        "FileName": "root.txt",
                        "Etag": str(self.snapshot) * 32,
                        "Size": 4 + self.snapshot,
                    }
                ],
                "total": 1,
            },
        )


def _run_success_with_cleanup_warning(
    monkeypatch, *, config, failed_paths, force_cross_filesystem_replace=False
):
    original_unlink = Path.unlink
    remaining_failures = {path.resolve(): 1 for path in failed_paths}

    if force_cross_filesystem_replace:
        original_replace = os.replace

        def guarded_replace(src, dst):
            src_path = Path(src)
            dst_path = Path(dst)
            if src_path.parent != dst_path.parent:
                raise OSError(errno.EXDEV, "Invalid cross-device link")
            return original_replace(src, dst)

        monkeypatch.setattr("fastlink_transfer.exporter.os.replace", guarded_replace)

    def flaky_unlink(self, *args, **kwargs):
        path = self.resolve()
        attempts_left = remaining_failures.get(path, 0)
        if attempts_left > 0:
            remaining_failures[path] = attempts_left - 1
            raise OSError("busy")
        return original_unlink(self, *args, **kwargs)

    client = MutableSingleFileExportClient()
    monkeypatch.setattr("pathlib.Path.unlink", flaky_unlink)

    assert run_export_json(api_client=client, config=config) == 0

    return client


def test_run_scan_loop_scans_nested_directories_and_writes_sidecar(tmp_path):
    config = _config(tmp_path)
    client = FakeExportClient()
    state = _load_export_context(api_client=client, config=config)

    _run_scan_loop(api_client=client, state=state, state_path=config.state_file)

    sidecar_lines = [
        json.loads(line)
        for line in (tmp_path / "export.state.records.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert sidecar_lines == [
        {"path": "root.txt", "etag_hex": "0" * 32, "size": 5},
        {"path": "Action/clip.mkv", "etag_hex": "1" * 32, "size": 9},
    ]
    state_payload = json.loads((tmp_path / "export.state.json").read_text(encoding="utf-8"))
    assert state_payload["completed_dirs"] == [
        {"folder_id": "12345678", "relative_dir": ""},
        {"folder_id": "200", "relative_dir": "Action"},
    ]


def test_load_export_context_persists_initial_state_before_first_commit(tmp_path):
    config = _config(tmp_path, workers=1)
    state = _load_export_context(api_client=FakeExportClient(), config=config)

    assert state.records_file.exists()
    state_payload = json.loads(config.state_file.read_text(encoding="utf-8"))
    assert state_payload["pending_dirs"] == [{"folder_id": "12345678", "relative_dir": ""}]
    assert state_payload["inflight_dirs"] == []
    assert state_payload["completed_dirs"] == []
    assert state_payload["seen_dir_ids"] == ["12345678"]
    assert state_payload["stats"] == {"files_written": 0, "dirs_completed": 0}


def test_load_export_context_cleans_up_sidecar_when_initial_flush_fails_before_persist(monkeypatch, tmp_path):
    from fastlink_transfer.export_state import ExportState

    original_flush = ExportState.flush

    def broken_flush(self, state_path):
        raise OSError("disk full")

    monkeypatch.setattr("fastlink_transfer.export_state.ExportState.flush", broken_flush)
    config = _config(tmp_path, workers=1)

    with pytest.raises(OSError, match="disk full"):
        _load_export_context(api_client=FakeExportClient(), config=config)

    assert not config.state_file.exists()
    assert not config.state_file.with_suffix(".records.jsonl").exists()

    monkeypatch.setattr("fastlink_transfer.export_state.ExportState.flush", original_flush)

    resumed_state = _load_export_context(api_client=FakeExportClient(), config=config)

    assert resumed_state.records_file.exists()
    assert config.state_file.exists()


def test_load_export_context_keeps_sidecar_when_initial_flush_interrupts_after_persist(monkeypatch, tmp_path):
    from fastlink_transfer.export_state import ExportState

    original_flush = ExportState.flush
    interrupted = {"done": False}

    def interrupted_flush(self, state_path):
        original_flush(self, state_path)
        if not interrupted["done"]:
            interrupted["done"] = True
            raise KeyboardInterrupt

    monkeypatch.setattr("fastlink_transfer.export_state.ExportState.flush", interrupted_flush)
    config = _config(tmp_path, workers=1)

    with pytest.raises(KeyboardInterrupt):
        _load_export_context(api_client=FakeExportClient(), config=config)

    assert config.state_file.exists()
    assert config.state_file.with_suffix(".records.jsonl").exists()

    resumed_state = _load_export_context(api_client=FakeExportClient(), config=config)

    assert resumed_state.pending_dirs == [{"folder_id": "12345678", "relative_dir": ""}]
    assert resumed_state.completed_dirs == []


def test_run_export_json_retries_retryable_directory_listing(monkeypatch, tmp_path):
    monkeypatch.setattr("fastlink_transfer.exporter.compute_backoff", lambda _attempt: 0.0)

    class RetryClient(FakeExportClient):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def get_file_list(self, *, parent_file_id: str):
            self.calls += 1
            if self.calls == 1:
                return Decision(kind=DecisionKind.RETRYABLE, error="HTTP 429")
            return super().get_file_list(parent_file_id=parent_file_id)

    config = _config(tmp_path, workers=1)
    client = RetryClient()
    state = _load_export_context(api_client=client, config=config)

    _run_scan_loop(api_client=client, state=state, state_path=config.state_file)


def test_load_export_context_retries_retryable_root_validation(monkeypatch, tmp_path):
    monkeypatch.setattr("fastlink_transfer.exporter.compute_backoff", lambda _attempt: 0.0)

    class RetryIdentityClient(FakeExportClient):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def get_directory_identity(self, *, parent_file_id: str):
            self.calls += 1
            if self.calls == 1:
                return Decision(kind=DecisionKind.RETRYABLE, error="HTTP 429")
            return Decision(kind=DecisionKind.DIRECTORY_CREATED, payload={"root_name": "Movies"})

    client = RetryIdentityClient()
    state = _load_export_context(api_client=client, config=_config(tmp_path, workers=1))

    assert state.common_path == "Movies/"
    assert client.calls == 2


def test_load_export_context_stops_immediately_on_credential_fatal_root_validation(tmp_path):
    class FatalIdentityClient(FakeExportClient):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def get_directory_identity(self, *, parent_file_id: str):
            self.calls += 1
            return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error="HTTP 401")

    client = FatalIdentityClient()

    with pytest.raises(RuntimeError, match="HTTP 401"):
        _load_export_context(api_client=client, config=_config(tmp_path, workers=1))

    assert client.calls == 1


def test_run_export_json_stops_on_credential_fatal(tmp_path):
    class FatalClient(FakeExportClient):
        def get_file_list(self, *, parent_file_id: str):
            return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error="HTTP 401")

    with pytest.raises(RuntimeError, match="HTTP 401"):
        _run_scan_loop(
            api_client=FatalClient(),
            state=_load_export_context(api_client=FakeExportClient(), config=_config(tmp_path, workers=1)),
            state_path=(tmp_path / "export.state.json").resolve(),
        )


def test_run_export_json_extra_flush_triggers_after_threshold(monkeypatch, tmp_path):
    from fastlink_transfer import exporter as exporter_module
    from fastlink_transfer.export_state import ExportState

    flush_calls = []
    original_load = exporter_module.load_or_initialize_export_state
    original_flush = ExportState.flush

    def wrapped_flush(self, state_path):
        flush_calls.append(self.stats["files_written"])
        return original_flush(self, state_path)

    def wrapped_load(**kwargs):
        return original_load(**kwargs)

    monkeypatch.setattr("fastlink_transfer.exporter.load_or_initialize_export_state", wrapped_load)
    monkeypatch.setattr("fastlink_transfer.export_state.ExportState.flush", wrapped_flush)

    config = _config(tmp_path, flush_every=2, workers=1)
    state = _load_export_context(api_client=FakeExportClient(), config=config)

    _run_scan_loop(api_client=FakeExportClient(), state=state, state_path=config.state_file)

    assert flush_calls.count(2) >= 2


def test_run_export_json_flushes_state_on_keyboard_interrupt(monkeypatch, tmp_path):
    from fastlink_transfer import exporter as exporter_module

    flush_calls = []

    def raising_run_scan(**kwargs):
        raise KeyboardInterrupt

    original_load = exporter_module.load_or_initialize_export_state

    def wrapped_load(**kwargs):
        state = original_load(**kwargs)
        original_flush = state.flush

        def wrapped_flush(state_path):
            flush_calls.append(state_path)
            original_flush(state_path)

        state.flush = wrapped_flush
        return state

    monkeypatch.setattr("fastlink_transfer.exporter.load_or_initialize_export_state", wrapped_load)
    monkeypatch.setattr("fastlink_transfer.exporter._run_scan_loop", raising_run_scan)

    with pytest.raises(KeyboardInterrupt):
        run_export_json(api_client=FakeExportClient(), config=_config(tmp_path, workers=1))

    assert flush_calls == [
        (tmp_path / "export.state.json").resolve(),
        (tmp_path / "export.state.json").resolve(),
    ]


def test_run_export_json_preserves_keyboard_interrupt_when_best_effort_flush_fails(monkeypatch, tmp_path):
    from fastlink_transfer import exporter as exporter_module

    config = _config(tmp_path, workers=1)
    state = exporter_module._load_export_context(api_client=FakeExportClient(), config=config)

    def broken_flush(_state_path):
        raise OSError("disk full")

    state.flush = broken_flush

    monkeypatch.setattr("fastlink_transfer.exporter._load_export_context", lambda **kwargs: state)
    monkeypatch.setattr(
        "fastlink_transfer.exporter._run_scan_loop",
        lambda **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        run_export_json(api_client=FakeExportClient(), config=config)


@pytest.mark.parametrize(
    ("state_name", "output_name"),
    [
        ("export.state.json", "export.state.json"),
        ("export.state.json", "export.state.output.tmp.json"),
    ],
)
def test_run_export_json_rejects_output_file_colliding_with_state_file(tmp_path, state_name, output_name):
    state_path = (tmp_path / state_name).resolve()
    output_path = (tmp_path / output_name).resolve()

    with pytest.raises(ValueError, match="output_file must be distinct from internal export artifacts"):
        run_export_json(
            api_client=FakeExportClient(),
            config=_config(tmp_path, workers=1, output_file=output_path, state_file=state_path),
        )


def test_run_export_json_preserves_committed_directory_snapshot_when_interrupt_hits_extra_flush(
    monkeypatch, tmp_path
):
    from fastlink_transfer import exporter as exporter_module
    from fastlink_transfer.export_state import ExportState

    original_load = exporter_module.load_or_initialize_export_state
    original_flush = ExportState.flush
    flush_calls = {"count": 0}

    def wrapped_flush(self, state_path):
        flush_calls["count"] += 1
        result = original_flush(self, state_path)
        if flush_calls["count"] == 2:
            raise KeyboardInterrupt
        return result

    def wrapped_load(**kwargs):
        return original_load(**kwargs)

    monkeypatch.setattr("fastlink_transfer.exporter.load_or_initialize_export_state", wrapped_load)
    monkeypatch.setattr("fastlink_transfer.export_state.ExportState.flush", wrapped_flush)

    with pytest.raises(KeyboardInterrupt):
        run_export_json(api_client=FakeExportClient(), config=_config(tmp_path, workers=1))

    state_payload = json.loads((tmp_path / "export.state.json").read_text(encoding="utf-8"))
    assert state_payload["pending_dirs"] == [{"folder_id": "200", "relative_dir": "Action"}]
    assert state_payload["inflight_dirs"] == []
    assert state_payload["completed_dirs"] == [{"folder_id": "12345678", "relative_dir": ""}]
    assert set(state_payload["seen_dir_ids"]) == {"12345678", "200"}
    assert state_payload["stats"] == {"files_written": 1, "dirs_completed": 1}
    assert (tmp_path / "export.state.records.jsonl").read_text(encoding="utf-8").splitlines() == [
        json.dumps({"path": "root.txt", "etag_hex": "0" * 32, "size": 5})
    ]


def test_run_scan_loop_stops_workers_and_reraises_keyboard_interrupt(monkeypatch, tmp_path):
    import queue

    from fastlink_transfer import exporter as exporter_module

    client = FakeExportClient()
    config = _config(tmp_path, workers=1)
    state = _load_export_context(api_client=client, config=config)

    queue_instances = []

    class ClaimQueue(queue.Queue):
        pass

    class ResultsQueue(queue.Queue):
        def get(self, *args, **kwargs):
            raise KeyboardInterrupt

    def queue_factory():
        queue_class = ClaimQueue if not queue_instances else ResultsQueue
        instance = queue_class()
        queue_instances.append(instance)
        return instance

    monkeypatch.setattr("fastlink_transfer.exporter.queue.Queue", queue_factory)

    with pytest.raises(KeyboardInterrupt):
        exporter_module._run_scan_loop(api_client=client, state=state, state_path=config.state_file)


def test_run_scan_loop_stops_workers_and_reraises_coordinator_commit_failure(monkeypatch, tmp_path):
    from fastlink_transfer import exporter as exporter_module

    client = FakeExportClient()
    config = _config(tmp_path, workers=1)
    state = _load_export_context(api_client=client, config=config)

    monkeypatch.setattr(
        "fastlink_transfer.exporter._append_sidecar_records",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("sidecar write failed")),
    )

    with pytest.raises(OSError, match="sidecar write failed"):
        exporter_module._run_scan_loop(api_client=client, state=state, state_path=config.state_file)


def test_run_scan_loop_does_not_commit_sibling_success_after_multiworker_credential_fatal(tmp_path):
    import time
    import threading

    success_started = threading.Event()

    class RacingFatalClient(FakeExportClient):
        def get_file_list(self, *, parent_file_id: str):
            self.list_calls.append(parent_file_id)
            if parent_file_id == "12345678":
                return Decision(
                    kind=DecisionKind.COMPLETED,
                    payload={
                        "items": [
                            {"FileId": 200, "Type": 1, "FileName": "Action"},
                            {"FileId": 201, "Type": 1, "FileName": "Drama"},
                        ],
                        "total": 2,
                    },
                )
            if parent_file_id == "201":
                success_started.set()
                return Decision(
                    kind=DecisionKind.COMPLETED,
                    payload={
                        "items": [
                            {"FileId": 301, "Type": 0, "FileName": "ok.txt", "Etag": "2" * 32, "Size": 4},
                        ],
                        "total": 1,
                    },
                )
            if parent_file_id == "200":
                assert success_started.wait(timeout=1.0)
                time.sleep(0.05)
                return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error="HTTP 401")
            raise AssertionError(parent_file_id)

    config = _config(tmp_path, workers=2)
    client = RacingFatalClient()
    state = _load_export_context(api_client=client, config=config)

    with pytest.raises(RuntimeError, match="HTTP 401"):
        _run_scan_loop(api_client=client, state=state, state_path=config.state_file)

    state_payload = json.loads((tmp_path / "export.state.json").read_text(encoding="utf-8"))
    assert state_payload["completed_dirs"] == [{"folder_id": "12345678", "relative_dir": ""}]
    assert {item["folder_id"] for item in state_payload["inflight_dirs"]} == {"200", "201"}
    assert (tmp_path / "export.state.records.jsonl").read_text(encoding="utf-8") == ""


def test_run_scan_loop_does_not_wait_for_never_claimed_task_after_credential_fatal(monkeypatch, tmp_path):
    import queue as stdlib_queue
    import threading

    from fastlink_transfer import exporter as exporter_module

    result_count = {"value": 0}
    real_queue = stdlib_queue.Queue
    real_thread = threading.Thread
    thread_count = {"value": 0}

    class QueuedFatalClient(FakeExportClient):
        def get_file_list(self, *, parent_file_id: str):
            self.list_calls.append(parent_file_id)
            if parent_file_id == "12345678":
                return Decision(
                    kind=DecisionKind.COMPLETED,
                    payload={
                        "items": [
                            {"FileId": 200, "Type": 1, "FileName": "Action"},
                            {"FileId": 201, "Type": 1, "FileName": "Drama"},
                        ],
                        "total": 2,
                    },
                )
            if parent_file_id == "200":
                return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error="HTTP 401")
            if parent_file_id == "201":
                raise AssertionError("task 201 should never be claimed")
            raise AssertionError(parent_file_id)

    class ResultsQueue(real_queue):
        def get(self, *args, **kwargs):
            result_count["value"] += 1
            if result_count["value"] == 5:
                raise AssertionError("coordinator waited for a non-existent wave result")
            return super().get(*args, **kwargs)

    class DummyThread:
        def start(self):
            return None

        def join(self):
            return None

    queue_instances = []

    def patched_queue():
        instance = real_queue() if not queue_instances else ResultsQueue()
        queue_instances.append(instance)
        return instance

    def thread_factory(*args, **kwargs):
        thread_count["value"] += 1
        if thread_count["value"] == 1:
            return real_thread(*args, **kwargs)
        return DummyThread()

    monkeypatch.setattr("fastlink_transfer.exporter.queue.Queue", patched_queue)
    monkeypatch.setattr("fastlink_transfer.exporter.threading.Thread", thread_factory)

    config = _config(tmp_path, workers=2)
    client = QueuedFatalClient()
    state = _load_export_context(api_client=client, config=config)

    with pytest.raises(RuntimeError, match="HTTP 401"):
        exporter_module._run_scan_loop(api_client=client, state=state, state_path=config.state_file)

    state_payload = json.loads((tmp_path / "export.state.json").read_text(encoding="utf-8"))
    assert state_payload["completed_dirs"] == [{"folder_id": "12345678", "relative_dir": ""}]
    assert state_payload["inflight_dirs"] == [{"folder_id": "200", "relative_dir": "Action"}]
    assert state_payload["pending_dirs"] == [{"folder_id": "201", "relative_dir": "Drama"}]
    assert (tmp_path / "export.state.records.jsonl").read_text(encoding="utf-8") == ""


def test_run_export_json_rejects_zero_file_exports_and_keeps_resume_artifacts(tmp_path):
    class EmptyClient(FakeExportClient):
        def get_file_list(self, *, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, payload={"items": [], "total": 0})

    with pytest.raises(RuntimeError, match="zero files exported"):
        run_export_json(api_client=EmptyClient(), config=_config(tmp_path, workers=1))

    assert not (tmp_path / "out.json").exists()
    assert (tmp_path / "export.state.json").exists()
    records_file = tmp_path / "export.state.records.jsonl"
    assert records_file.exists()
    assert records_file.read_text(encoding="utf-8") == ""


def test_prepare_sidecar_for_resume_discards_truncated_last_line(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        json.dumps({"path": "ok.txt", "etag_hex": "0" * 32, "size": 5}) + "\n" + '{"path":"bad',
        encoding="utf-8",
    )

    prepare_sidecar_for_resume(records_file)

    lines = records_file.read_text(encoding="utf-8").splitlines()
    assert lines == [json.dumps({"path": "ok.txt", "etag_hex": "0" * 32, "size": 5})]


def test_prepare_sidecar_for_resume_rejects_parseable_schema_invalid_last_line_without_newline(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text('{"path":"a.txt"}', encoding="utf-8")

    with pytest.raises(RuntimeError, match="corrupt sidecar"):
        prepare_sidecar_for_resume(records_file)

    assert records_file.read_text(encoding="utf-8") == '{"path":"a.txt"}'


def test_prepare_sidecar_for_resume_rejects_mid_file_corruption(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        "\n".join(
            [
                json.dumps({"path": "ok.txt", "etag_hex": "0" * 32, "size": 5}),
                '{"path":"bad',
                json.dumps({"path": "later.txt", "etag_hex": "1" * 32, "size": 9}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="corrupt sidecar"):
        prepare_sidecar_for_resume(records_file)


def test_prepare_sidecar_for_resume_rejects_schema_invalid_mid_file_corruption(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        "\n".join(
            [
                json.dumps({"path": "ok.txt", "etag_hex": "0" * 32, "size": 5}),
                json.dumps({"path": "bad.txt"}),
                json.dumps({"path": "later.txt", "etag_hex": "1" * 32, "size": 9}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="corrupt sidecar"):
        prepare_sidecar_for_resume(records_file)


def test_prepare_sidecar_for_resume_adds_missing_trailing_newline_to_valid_last_record(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        json.dumps({"path": "ok.txt", "etag_hex": "0" * 32, "size": 5}),
        encoding="utf-8",
    )

    prepare_sidecar_for_resume(records_file)

    assert records_file.read_text(encoding="utf-8") == json.dumps(
        {"path": "ok.txt", "etag_hex": "0" * 32, "size": 5}
    ) + "\n"


def test_finalize_export_json_writes_v3_style_output_and_cleans_internal_files(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        "\n".join(
            [
                json.dumps({"path": "Action/a.txt", "etag_hex": "0" * 32, "size": 5}),
                json.dumps({"path": "Action/a.txt", "etag_hex": "0" * 32, "size": 5}),
                json.dumps({"path": "B/b.txt", "etag_hex": "1" * 32, "size": 7}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    state = type(
        "State",
        (),
        {
            "records_file": records_file,
            "temp_sqlite_file": tmp_path / "export.state.finalize.sqlite3",
            "temp_output_file": tmp_path / "export.state.output.tmp.json",
            "output_file": tmp_path / "out.json",
            "common_path": "Movies/",
        },
    )()

    finalize_export_json(state=state)

    payload = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
    assert payload["scriptVersion"] == "0.1.0"
    assert payload["exportVersion"] == "1.0"
    assert payload["usesBase62EtagsInExport"] is True
    assert payload["commonPath"] == "Movies/"
    assert payload["totalFilesCount"] == 2
    assert payload["totalSize"] == 12
    assert [item["path"] for item in payload["files"]] == ["Action/a.txt", "B/b.txt"]


def test_finalize_export_json_output_is_loadable_by_existing_importer(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        json.dumps({"path": "Action/a.txt", "etag_hex": "0" * 32, "size": 5}) + "\n",
        encoding="utf-8",
    )
    state = type(
        "State",
        (),
        {
            "records_file": records_file,
            "temp_sqlite_file": tmp_path / "export.state.finalize.sqlite3",
            "temp_output_file": tmp_path / "export.state.output.tmp.json",
            "output_file": tmp_path / "out.json",
            "common_path": "Movies/",
        },
    )()

    finalize_export_json(state=state)

    export_data = load_export_file(tmp_path / "out.json")

    assert export_data.common_path == "Movies/"
    assert [(record.path, record.file_name, record.etag, record.size) for record in export_data.records] == [
        ("Action/a.txt", "a.txt", "0" * 32, 5),
    ]


def test_finalize_export_json_dedupes_raw_paths_before_normalized_path_conflict_check(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        "\n".join(
            [
                json.dumps({"path": "A\\same.txt", "etag_hex": "0" * 32, "size": 5}),
                json.dumps({"path": "A\\same.txt", "etag_hex": "0" * 32, "size": 5}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    state = type(
        "State",
        (),
        {
            "records_file": records_file,
            "temp_sqlite_file": tmp_path / "export.state.finalize.sqlite3",
            "temp_output_file": tmp_path / "export.state.output.tmp.json",
            "output_file": tmp_path / "out.json",
            "common_path": "Movies/",
        },
    )()

    finalize_export_json(state=state)

    payload = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
    assert [item["path"] for item in payload["files"]] == ["A/same.txt"]


def test_finalize_export_json_rejects_conflicting_normalized_path(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        "\n".join(
            [
                json.dumps({"path": "A\\same.txt", "etag_hex": "0" * 32, "size": 5}),
                json.dumps({"path": "A/same.txt", "etag_hex": "1" * 32, "size": 9}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    state = type(
        "State",
        (),
        {
            "records_file": records_file,
            "temp_sqlite_file": tmp_path / "export.state.finalize.sqlite3",
            "temp_output_file": tmp_path / "export.state.output.tmp.json",
            "output_file": tmp_path / "out.json",
            "common_path": "Movies/",
        },
    )()

    with pytest.raises(RuntimeError, match="source-tree mutation or inconsistent sidecar content"):
        finalize_export_json(state=state)


def test_finalize_export_json_rejects_schema_invalid_sidecar_line(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        "\n".join(
            [
                json.dumps({"path": "Action/a.txt", "etag_hex": "0" * 32, "size": 5}),
                json.dumps([]),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    state = type(
        "State",
        (),
        {
            "records_file": records_file,
            "temp_sqlite_file": tmp_path / "export.state.finalize.sqlite3",
            "temp_output_file": tmp_path / "export.state.output.tmp.json",
            "output_file": tmp_path / "out.json",
            "common_path": "Movies/",
        },
    )()

    with pytest.raises(RuntimeError, match="corrupt sidecar"):
        finalize_export_json(state=state)


def test_finalize_export_json_rejects_oversized_hex_etag_before_writing_output(tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        json.dumps({"path": "Action/a.txt", "etag_hex": "f" * 64, "size": 5}) + "\n",
        encoding="utf-8",
    )
    state = type(
        "State",
        (),
        {
            "records_file": records_file,
            "temp_sqlite_file": tmp_path / "export.state.finalize.sqlite3",
            "temp_output_file": tmp_path / "export.state.output.tmp.json",
            "output_file": tmp_path / "out.json",
            "common_path": "Movies/",
        },
    )()

    with pytest.raises(RuntimeError, match="corrupt sidecar"):
        finalize_export_json(state=state)

    assert not (tmp_path / "out.json").exists()


def test_run_export_json_finalizes_and_cleans_internal_artifacts(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)

    exit_code = run_export_json(api_client=FakeExportClient(), config=config)

    assert exit_code == 0
    assert config.output_file.exists()
    assert not config.state_file.exists()
    assert not config.state_file.with_suffix(".records.jsonl").exists()
    assert not config.state_file.with_suffix(".finalize.sqlite3").exists()
    assert not config.state_file.with_suffix(".output.tmp.json").exists()


def test_run_export_json_creates_missing_output_parent(tmp_path):
    config = _config(
        tmp_path,
        workers=1,
        flush_every=1,
        output_file=(tmp_path / "missing" / "nested" / "out.json").resolve(),
    )

    exit_code = run_export_json(api_client=FakeExportClient(), config=config)

    assert exit_code == 0
    assert config.output_file.exists()


def test_finalize_export_json_uses_state_derived_temp_output_and_recovers_from_cross_filesystem_replace(
    monkeypatch, tmp_path
):
    state_dir = tmp_path / "state"
    output_dir = tmp_path / "separate" / "target"
    output_dir.mkdir(parents=True)
    state = load_or_initialize_export_state(
        state_path=state_dir / "export.state.json",
        source_host="https://www.123pan.com",
        source_parent_id="12345678",
        source_root_name="Movies",
        output_file=output_dir / "out.json",
        workers=1,
        max_retries=0,
        flush_every=1,
    )
    state.records_file.parent.mkdir(parents=True, exist_ok=True)
    state.records_file.write_text(
        json.dumps({"path": "a.txt", "etag_hex": "0" * 32, "size": 5}) + "\n",
        encoding="utf-8",
    )

    original_replace = os.replace
    replace_calls = []

    def guarded_replace(src, dst):
        src_path = Path(src)
        dst_path = Path(dst)
        replace_calls.append((src_path, dst_path))
        if src_path.parent != dst_path.parent:
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return original_replace(src, dst)

    monkeypatch.setattr("fastlink_transfer.exporter.os.replace", guarded_replace)

    finalize_export_json(state=state)

    assert state.temp_output_file == (state_dir / "export.state.output.tmp.json").resolve()
    assert replace_calls[0] == (state.temp_output_file, state.output_file)
    assert replace_calls[0][0].parent == state_dir.resolve()
    assert replace_calls[1][1] == state.output_file
    assert replace_calls[1][0].parent == output_dir.resolve()
    assert state.output_file.exists()


def test_run_export_json_returns_zero_when_cleanup_warns(monkeypatch, tmp_path, capsys):
    config = _config(tmp_path, workers=1, flush_every=1)
    original_unlink = Path.unlink

    def flaky_unlink(self, *args, **kwargs):
        if self.name.endswith(".records.jsonl"):
            raise OSError("busy")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr("pathlib.Path.unlink", flaky_unlink)

    exit_code = run_export_json(api_client=FakeExportClient(), config=config)

    assert exit_code == 0
    assert "Cleanup warning:" in capsys.readouterr().out
    assert config.output_file.exists()


def test_run_export_json_rerun_starts_fresh_after_cleanup_warning_leaves_marker_and_sidecar(
    monkeypatch, tmp_path
):
    config = _config(tmp_path, workers=1, flush_every=1)
    client = _run_success_with_cleanup_warning(
        monkeypatch,
        config=config,
        failed_paths=[config.state_file.with_suffix(".records.jsonl")],
    )

    assert not config.state_file.exists()
    assert config.state_file.with_suffix(".records.jsonl").exists()
    assert _success_marker_path(config.state_file).exists()

    client.snapshot = 2
    client.list_calls.clear()

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678"]


def test_run_export_json_rerun_starts_fresh_after_cleanup_warning_leaves_marker_and_state(
    monkeypatch, tmp_path
):
    config = _config(tmp_path, workers=1, flush_every=1)
    client = _run_success_with_cleanup_warning(
        monkeypatch,
        config=config,
        failed_paths=[config.state_file],
    )

    assert config.state_file.exists()
    assert not config.state_file.with_suffix(".records.jsonl").exists()
    assert _success_marker_path(config.state_file).exists()

    client.snapshot = 2
    client.list_calls.clear()

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678"]


def test_run_export_json_rerun_starts_fresh_after_cleanup_warning_leaves_marker_and_sqlite(
    monkeypatch, tmp_path
):
    config = _config(tmp_path, workers=1, flush_every=1)
    client = _run_success_with_cleanup_warning(
        monkeypatch,
        config=config,
        failed_paths=[config.state_file.with_suffix(".finalize.sqlite3")],
    )

    assert not config.state_file.exists()
    assert not config.state_file.with_suffix(".records.jsonl").exists()
    assert config.state_file.with_suffix(".finalize.sqlite3").exists()
    assert _success_marker_path(config.state_file).exists()

    client.snapshot = 2
    client.list_calls.clear()

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678"]


def test_run_export_json_rerun_starts_fresh_after_cleanup_warning_leaves_marker_and_temp_output(
    monkeypatch, tmp_path
):
    config = _config(
        tmp_path,
        workers=1,
        flush_every=1,
        output_file=(tmp_path / "separate" / "target" / "out.json").resolve(),
    )
    client = _run_success_with_cleanup_warning(
        monkeypatch,
        config=config,
        failed_paths=[config.state_file.with_suffix(".output.tmp.json")],
        force_cross_filesystem_replace=True,
    )

    assert not config.state_file.exists()
    assert not config.state_file.with_suffix(".records.jsonl").exists()
    assert config.state_file.with_suffix(".output.tmp.json").exists()
    assert _success_marker_path(config.state_file).exists()

    client.snapshot = 2
    client.list_calls.clear()

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678"]


def test_run_export_json_cleanup_warning_success_starts_fresh_when_previous_output_is_missing(
    monkeypatch, tmp_path, capsys
):
    config = _config(tmp_path, workers=1, flush_every=1)
    original_unlink = Path.unlink
    failed_paths = {
        config.state_file.resolve(): 1,
        config.state_file.with_suffix(".records.jsonl").resolve(): 1,
    }

    class MutableSnapshotClient(FakeExportClient):
        def __init__(self):
            super().__init__()
            self.snapshot = 1

        def get_file_list(self, *, parent_file_id: str):
            self.list_calls.append(parent_file_id)
            return Decision(
                kind=DecisionKind.COMPLETED,
                payload={
                    "items": [
                        {
                            "FileId": 201,
                            "Type": 0,
                            "FileName": "root.txt",
                            "Etag": str(self.snapshot) * 32,
                            "Size": 4 + self.snapshot,
                        }
                    ],
                    "total": 1,
                },
            )

    def flaky_unlink(self, *args, **kwargs):
        path = self.resolve()
        remaining_failures = failed_paths.get(path, 0)
        if remaining_failures > 0:
            failed_paths[path] = remaining_failures - 1
            raise OSError("busy")
        return original_unlink(self, *args, **kwargs)

    client = MutableSnapshotClient()
    monkeypatch.setattr("pathlib.Path.unlink", flaky_unlink)

    assert run_export_json(api_client=client, config=config) == 0
    assert "Cleanup warning:" in capsys.readouterr().out
    assert config.output_file.exists()
    assert config.state_file.exists()
    assert config.state_file.with_suffix(".records.jsonl").exists()

    config.output_file.unlink()
    client.snapshot = 2

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678", "12345678"]


def test_run_export_json_persists_durable_success_marker_when_final_state_flush_fails(
    monkeypatch, tmp_path, capsys
):
    from fastlink_transfer.export_state import ExportState

    config = _config(tmp_path, workers=1, flush_every=1)
    original_flush = ExportState.flush
    original_unlink = Path.unlink
    failed_paths = {
        config.state_file.resolve(): 1,
        config.state_file.with_suffix(".records.jsonl").resolve(): 1,
    }

    def flush_with_final_marker_failure(self, state_path):
        if self.final_output_committed:
            raise OSError("marker flush failed")
        return original_flush(self, state_path)

    def flaky_unlink(self, *args, **kwargs):
        path = self.resolve()
        remaining_failures = failed_paths.get(path, 0)
        if remaining_failures > 0:
            failed_paths[path] = remaining_failures - 1
            raise OSError("busy")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr("fastlink_transfer.export_state.ExportState.flush", flush_with_final_marker_failure)
    monkeypatch.setattr("pathlib.Path.unlink", flaky_unlink)

    assert run_export_json(api_client=FakeExportClient(), config=config) == 0

    persisted_state = json.loads(config.state_file.read_text(encoding="utf-8"))
    assert persisted_state["final_output_committed"] is False
    assert config.output_file.exists()
    assert _success_marker_path(config.state_file).exists()
    assert "Cleanup warning:" in capsys.readouterr().out


def test_run_export_json_starts_fresh_when_output_is_durable_but_success_leftovers_remain(
    monkeypatch, tmp_path
):
    from fastlink_transfer.export_state import ExportState

    config = _config(tmp_path, workers=1, flush_every=1)
    original_flush = ExportState.flush
    original_unlink = Path.unlink
    failed_paths = {
        config.state_file.resolve(): 1,
        config.state_file.with_suffix(".records.jsonl").resolve(): 1,
    }

    class MutableSnapshotClient(FakeExportClient):
        def __init__(self):
            super().__init__()
            self.snapshot = 1

        def get_file_list(self, *, parent_file_id: str):
            self.list_calls.append(parent_file_id)
            return Decision(
                kind=DecisionKind.COMPLETED,
                payload={
                    "items": [
                        {
                            "FileId": 201,
                            "Type": 0,
                            "FileName": "root.txt",
                            "Etag": str(self.snapshot) * 32,
                            "Size": 4 + self.snapshot,
                        }
                    ],
                    "total": 1,
                },
            )

    def flush_with_final_marker_failure(self, state_path):
        if self.final_output_committed:
            raise OSError("marker flush failed")
        return original_flush(self, state_path)

    def flaky_unlink(self, *args, **kwargs):
        path = self.resolve()
        remaining_failures = failed_paths.get(path, 0)
        if remaining_failures > 0:
            failed_paths[path] = remaining_failures - 1
            raise OSError("busy")
        return original_unlink(self, *args, **kwargs)

    client = MutableSnapshotClient()
    monkeypatch.setattr("fastlink_transfer.export_state.ExportState.flush", flush_with_final_marker_failure)
    monkeypatch.setattr("pathlib.Path.unlink", flaky_unlink)

    assert run_export_json(api_client=client, config=config) == 0
    assert json.loads(config.state_file.read_text(encoding="utf-8"))["final_output_committed"] is False

    client.snapshot = 2
    client.list_calls.clear()

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678"]


def test_run_export_json_resumes_incomplete_finalize_state_without_rescanning(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)
    state = load_or_initialize_export_state(
        state_path=config.state_file,
        source_host="https://www.123pan.com",
        source_parent_id=config.source_parent_id,
        source_root_name="Movies",
        output_file=config.output_file,
        workers=config.workers,
        max_retries=config.max_retries,
        flush_every=config.flush_every,
    )
    state.pending_dirs = []
    state.inflight_dirs = []
    state.completed_dirs = [{"folder_id": config.source_parent_id, "relative_dir": ""}]
    state.seen_dir_ids = [config.source_parent_id]
    state.stats = {"files_written": 1, "dirs_completed": 1}
    state.records_file.parent.mkdir(parents=True, exist_ok=True)
    state.records_file.write_text(
        json.dumps({"path": "resume.txt", "etag_hex": "3" * 32, "size": 7}) + "\n",
        encoding="utf-8",
    )
    config.output_file.write_text("stale output from a previous run", encoding="utf-8")
    state.flush(config.state_file)
    assert not _success_marker_path(config.state_file).exists()

    class NoRescanClient(FakeExportClient):
        def get_file_list(self, *, parent_file_id: str):
            self.list_calls.append(parent_file_id)
            raise AssertionError("incomplete finalize state should resume without rescanning")

    client = NoRescanClient()

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "resume.txt", "etag": _hex_base62(3), "size": "7"}]
    assert client.list_calls == []


def test_run_export_json_zero_file_terminal_failure_restarts_live_scan_instead_of_stale_finalize_loop(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)
    state = load_or_initialize_export_state(
        state_path=config.state_file,
        source_host="https://www.123pan.com",
        source_parent_id=config.source_parent_id,
        source_root_name="Movies",
        output_file=config.output_file,
        workers=config.workers,
        max_retries=config.max_retries,
        flush_every=config.flush_every,
    )
    state.pending_dirs = []
    state.inflight_dirs = []
    state.completed_dirs = [{"folder_id": config.source_parent_id, "relative_dir": ""}]
    state.seen_dir_ids = [config.source_parent_id]
    state.stats = {"files_written": 0, "dirs_completed": 1}
    state.records_file.parent.mkdir(parents=True, exist_ok=True)
    state.records_file.write_text("", encoding="utf-8")
    state.flush(config.state_file)

    class SecondRunClient(FakeExportClient):
        pass

    client = SecondRunClient()

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert [item["path"] for item in payload["files"]] == ["Action/clip.mkv", "root.txt"]
    assert client.list_calls == ["12345678", "200"]


def test_run_export_json_does_not_purge_mismatched_scope_before_success_marker_validation(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)
    state = load_or_initialize_export_state(
        state_path=config.state_file,
        source_host="https://www.123pan.com",
        source_parent_id=config.source_parent_id,
        source_root_name="Movies",
        output_file=config.output_file,
        workers=config.workers,
        max_retries=config.max_retries,
        flush_every=config.flush_every,
    )
    state.records_file.parent.mkdir(parents=True, exist_ok=True)
    state.records_file.write_text(
        json.dumps({"path": "resume.txt", "etag_hex": "3" * 32, "size": 7}) + "\n",
        encoding="utf-8",
    )
    state.flush(config.state_file)
    _write_success_marker(
        state_file=config.state_file,
        output_file=config.output_file,
        source_parent_id=config.source_parent_id,
    )

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        run_export_json(
            api_client=FakeExportClient(),
            config=_config(tmp_path, workers=1, flush_every=1, source_parent_id="99999999"),
        )

    assert config.state_file.exists()
    assert config.state_file.with_suffix(".records.jsonl").exists()
    assert _success_marker_path(config.state_file).exists()


def test_run_export_json_rejects_marker_only_orphan_sidecar_scope_mismatch(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)
    records_file = config.state_file.with_suffix(".records.jsonl")
    records_file.write_text(
        json.dumps({"path": "stale.txt", "etag_hex": "1" * 32, "size": 5}) + "\n",
        encoding="utf-8",
    )
    _write_success_marker(
        state_file=config.state_file,
        output_file=config.output_file,
        source_parent_id=config.source_parent_id,
    )

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        run_export_json(
            api_client=MutableSingleFileExportClient(),
            config=_config(tmp_path, workers=1, flush_every=1, source_parent_id="99999999"),
        )

    assert not config.state_file.exists()
    assert records_file.exists()
    assert records_file.read_text(encoding="utf-8") == json.dumps(
        {"path": "stale.txt", "etag_hex": "1" * 32, "size": 5}
    ) + "\n"
    assert _success_marker_path(config.state_file).exists()


def test_run_export_json_rejects_legacy_marker_only_orphan_sidecar_without_state(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)
    records_file = config.state_file.with_suffix(".records.jsonl")
    records_file.write_text(
        json.dumps({"path": "stale.txt", "etag_hex": "1" * 32, "size": 5}) + "\n",
        encoding="utf-8",
    )
    _write_legacy_success_marker(state_file=config.state_file)

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        run_export_json(api_client=MutableSingleFileExportClient(), config=config)

    assert not config.state_file.exists()
    assert records_file.exists()
    assert records_file.read_text(encoding="utf-8") == json.dumps(
        {"path": "stale.txt", "etag_hex": "1" * 32, "size": 5}
    ) + "\n"
    assert _success_marker_path(config.state_file).exists()


def test_run_export_json_starts_fresh_when_same_scope_marker_only_sidecar_leftovers_remain(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)
    records_file = config.state_file.with_suffix(".records.jsonl")
    config.output_file.write_text("old output", encoding="utf-8")
    records_file.write_text(
        json.dumps({"path": "stale.txt", "etag_hex": "1" * 32, "size": 5}) + "\n",
        encoding="utf-8",
    )
    _write_success_marker(
        state_file=config.state_file,
        output_file=config.output_file,
        source_parent_id=config.source_parent_id,
    )

    client = MutableSingleFileExportClient()
    client.snapshot = 2

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678"]
    assert not config.state_file.exists()
    assert not records_file.exists()
    assert not _success_marker_path(config.state_file).exists()


def test_run_export_json_starts_fresh_when_legacy_marker_only_leftovers_remain_without_sidecar(tmp_path):
    config = _config(tmp_path, workers=1, flush_every=1)
    temp_output_file = config.state_file.with_suffix(".output.tmp.json")
    temp_sqlite_file = config.state_file.with_suffix(".finalize.sqlite3")
    config.output_file.write_text("old output", encoding="utf-8")
    temp_output_file.write_text("stale temp output", encoding="utf-8")
    temp_sqlite_file.write_text("stale sqlite", encoding="utf-8")
    _write_legacy_success_marker(state_file=config.state_file)

    client = MutableSingleFileExportClient()
    client.snapshot = 2

    assert run_export_json(api_client=client, config=config) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["files"] == [{"path": "root.txt", "etag": _hex_base62(2), "size": "6"}]
    assert client.list_calls == ["12345678"]
    assert not config.state_file.exists()
    assert not config.state_file.with_suffix(".records.jsonl").exists()
    assert not temp_output_file.exists()
    assert not temp_sqlite_file.exists()
    assert not _success_marker_path(config.state_file).exists()


def test_finalize_export_json_fsyncs_output_parent_directory(monkeypatch, tmp_path):
    records_file = tmp_path / "export.state.records.jsonl"
    records_file.write_text(
        json.dumps({"path": "Action/a.txt", "etag_hex": "0" * 32, "size": 5}) + "\n",
        encoding="utf-8",
    )
    state = type(
        "State",
        (),
        {
            "records_file": records_file,
            "temp_sqlite_file": tmp_path / "export.state.finalize.sqlite3",
            "temp_output_file": tmp_path / "export.state.output.tmp.json",
            "output_file": tmp_path / "out.json",
            "common_path": "Movies/",
        },
    )()
    calls = []
    parent_fd = 123321
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

    monkeypatch.setattr("fastlink_transfer.exporter.os.replace", record_replace)
    monkeypatch.setattr("fastlink_transfer.exporter.os.open", record_open)
    monkeypatch.setattr("fastlink_transfer.exporter.os.fsync", record_fsync)
    monkeypatch.setattr("fastlink_transfer.exporter.os.close", record_close)

    finalize_export_json(state=state)

    replace_index = next(index for index, call in enumerate(calls) if call[0] == "replace")
    open_index = next(index for index, call in enumerate(calls) if call == ("open", tmp_path.resolve()))
    dir_fsync_index = next(index for index, call in enumerate(calls) if call == ("fsync", parent_fd))
    close_index = next(index for index, call in enumerate(calls) if call == ("close", parent_fd))

    assert replace_index < open_index < dir_fsync_index < close_index


def test_mark_final_output_committed_fsyncs_marker_parent_directory(monkeypatch, tmp_path):
    from fastlink_transfer import exporter as exporter_module

    state_path = (tmp_path / "export.state.json").resolve()
    state = load_or_initialize_export_state(
        state_path=state_path,
        source_host="https://www.123pan.com",
        source_parent_id="12345678",
        source_root_name="Movies",
        output_file=tmp_path / "out.json",
        workers=1,
        max_retries=0,
        flush_every=1,
    )
    flush_calls = []
    calls = []
    parent_fd = 456654
    original_open = os.open

    def record_flush(path):
        flush_calls.append(path)

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

    state.flush = record_flush
    monkeypatch.setattr("fastlink_transfer.exporter.os.open", record_open)
    monkeypatch.setattr("fastlink_transfer.exporter.os.fsync", record_fsync)
    monkeypatch.setattr("fastlink_transfer.exporter.os.close", record_close)

    exporter_module._mark_final_output_committed(state=state, state_path=state_path)

    marker_path = _success_marker_path(state_path)
    open_index = next(index for index, call in enumerate(calls) if call == ("open", tmp_path.resolve()))
    dir_fsync_index = next(index for index, call in enumerate(calls) if call == ("fsync", parent_fd))
    close_index = next(index for index, call in enumerate(calls) if call == ("close", parent_fd))

    assert marker_path.exists()
    assert open_index < dir_fsync_index < close_index
    assert flush_calls == [state_path]


def test_run_export_json_returns_zero_when_exdev_fallback_cleanup_warns(monkeypatch, tmp_path, capsys):
    state_dir = tmp_path / "state"
    output_dir = tmp_path / "separate" / "target"
    config = _config(
        tmp_path,
        workers=1,
        flush_every=1,
        state_file=(state_dir / "export.state.json").resolve(),
        output_file=(output_dir / "out.json").resolve(),
    )
    original_replace = os.replace
    original_unlink = Path.unlink
    temp_output_path = (state_dir / "export.state.output.tmp.json").resolve()

    def guarded_replace(src, dst):
        src_path = Path(src)
        dst_path = Path(dst)
        if src_path.parent != dst_path.parent:
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return original_replace(src, dst)

    def flaky_unlink(self, *args, **kwargs):
        if self.resolve() == temp_output_path:
            raise OSError("busy")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr("fastlink_transfer.exporter.os.replace", guarded_replace)
    monkeypatch.setattr("pathlib.Path.unlink", flaky_unlink)

    assert run_export_json(api_client=FakeExportClient(), config=config) == 0

    assert "Cleanup warning:" in capsys.readouterr().out
    assert config.output_file.exists()


def _hex_base62(digit: int) -> str:
    number = int(str(digit) * 32, 16)
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    chars = []
    while number > 0:
        number, remainder = divmod(number, 62)
        chars.append(alphabet[remainder])
    return "".join(reversed(chars))
