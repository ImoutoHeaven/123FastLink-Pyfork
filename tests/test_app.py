import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from fastlink_transfer.api import Decision, DecisionKind
from fastlink_transfer.app import run_cli, summarize_exit_code
from fastlink_transfer.importer import load_export_file


SAMPLE_EXPORT_FIXTURE = Path(__file__).parent / "fixtures" / "sample_export.json"


class RecordingState:
    def __init__(self, *, flush_error=None):
        self.common_path = "Demo/"
        self.completed = set()
        self.not_reusable = {}
        self.failed = {}
        self.folder_map = {"": "12345678"}
        self.stats = {"total": 1, "completed": 0, "not_reusable": 0, "failed": 0}
        self.flush_calls = []
        self._flush_error = flush_error

    def flush(self, state_path):
        self.flush_calls.append(state_path)
        if self._flush_error is not None:
            raise self._flush_error


def _patch_non_dry_run_dependencies(
    monkeypatch,
    tmp_path,
    *,
    state,
    dry_run=False,
    folder_keys=None,
    pending_records=None,
    create_remote_directories_impl=None,
    run_file_phase_impl=None,
):
    config = SimpleNamespace(
        file_path=tmp_path / "export.json",
        target_parent_id="12345678",
        state_file=tmp_path / "state.json",
        workers=4,
        max_retries=5,
        flush_every=100,
        retry_failed=False,
        dry_run=dry_run,
    )
    export_data = SimpleNamespace(
        source_sha256="abc",
        common_path="Demo/",
        records=[
            SimpleNamespace(
                key="k",
                path="a/b.txt",
                relative_parent_dir="a",
            )
        ],
    )
    monkeypatch.setattr("fastlink_transfer.app.parse_args", lambda argv=None: (None, config))
    monkeypatch.setattr(
        "fastlink_transfer.app.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.app.load_export_file", lambda path: export_data)
    monkeypatch.setattr("fastlink_transfer.app.load_or_initialize_state", lambda **kwargs: state)
    monkeypatch.setattr(
        "fastlink_transfer.app.collect_folder_keys",
        lambda **kwargs: folder_keys if folder_keys is not None else ["Demo", "Demo/a"],
    )
    monkeypatch.setattr(
        "fastlink_transfer.app.select_pending_records",
        lambda **kwargs: pending_records if pending_records is not None else export_data.records,
    )
    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", lambda host, session: object())
    monkeypatch.setattr(
        "fastlink_transfer.app.create_remote_directories",
        create_remote_directories_impl or (lambda **kwargs: None),
    )
    monkeypatch.setattr(
        "fastlink_transfer.app.run_file_phase",
        run_file_phase_impl or (lambda **kwargs: {"credential_fatal": False}),
    )


def test_summarize_exit_code_returns_zero_when_only_not_reusable_exists():
    assert summarize_exit_code(failed_count=0, credential_fatal=False) == 0


def test_summarize_exit_code_returns_nonzero_for_failed_items():
    assert summarize_exit_code(failed_count=1, credential_fatal=False) == 1


def test_summarize_exit_code_returns_nonzero_for_credential_fatal():
    assert summarize_exit_code(failed_count=0, credential_fatal=True) == 1


def test_run_cli_dry_run_skips_remote_mutation(monkeypatch, tmp_path, capsys):
    export_file = tmp_path / "export.json"
    export_file.write_text(
        '{"usesBase62EtagsInExport": false, "commonPath": "Demo/", "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "a/b.txt"}]}',
        encoding="utf-8",
    )
    state_file = tmp_path / "state.json"
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")

    class FailIfUsedClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("remote client should not be created during dry run")

    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", FailIfUsedClient)
    exit_code = run_cli(
        [
            "import-json",
            "--file",
            str(export_file),
            "--target-parent-id",
            "12345678",
            "--state-file",
            str(state_file),
            "--dry-run",
        ]
    )
    assert exit_code == 0
    assert "Dry run: no remote mutations performed" in capsys.readouterr().out


def test_run_cli_dry_run_reports_pending_folder_count_from_execution_plan(monkeypatch, tmp_path, capsys):
    state = RecordingState()
    state.folder_map.update({"Demo": "1", "Demo/a": "2"})

    _patch_non_dry_run_dependencies(
        monkeypatch,
        tmp_path,
        state=state,
        dry_run=True,
        folder_keys=["Demo", "Demo/a"],
    )

    assert run_cli(["import-json"]) == 0

    output = capsys.readouterr().out
    assert "Startup: files=1 pending=1 folders=0 workers=4" in output
    assert "Dry run: no remote mutations performed" in output


def test_run_cli_dry_run_uses_fixture_resume_plan_without_remote_mutation(monkeypatch, tmp_path, capsys):
    export = load_export_file(SAMPLE_EXPORT_FIXTURE)
    first_record, _ = export.records
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_file": str(SAMPLE_EXPORT_FIXTURE.resolve()),
                "source_sha256": export.source_sha256,
                "target_parent_id": "12345678",
                "common_path": export.common_path,
                "workers": 8,
                "folder_map": {
                    "": "12345678",
                    "Demo": "1",
                    "Demo/1983": "2",
                    "Demo/1983/06": "3",
                },
                "completed": [first_record.key],
                "not_reusable": [],
                "failed": [],
                "stats": {"total": 2, "completed": 1, "not_reusable": 0, "failed": 0},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")

    class FailIfUsedClient:
        def __init__(self, *args, **kwargs):  # pragma: no cover - guard
            raise AssertionError("remote client should not be created during dry run")

    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", FailIfUsedClient)

    exit_code = run_cli(
        [
            "import-json",
            "--file",
            str(SAMPLE_EXPORT_FIXTURE),
            "--target-parent-id",
            "12345678",
            "--state-file",
            str(state_path),
            "--dry-run",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Startup: files=2 pending=1 folders=1 workers=8" in output
    assert "Dry run: no remote mutations performed" in output


def test_run_cli_returns_zero_when_non_dry_run_ends_with_only_not_reusable(monkeypatch, tmp_path, capsys):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())

    class NotReusableClient:
        def __init__(self, host, session):
            self._next_folder_id = 1

        def mkdir(self, *, parent_file_id: str, folder_name: str):
            file_id = str(self._next_folder_id)
            self._next_folder_id += 1
            return Decision(kind=DecisionKind.DIRECTORY_CREATED, file_id=file_id)

        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.NOT_REUSABLE, error="Reuse=false")

    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", NotReusableClient)

    exit_code = run_cli(
        [
            "import-json",
            "--file",
            str(SAMPLE_EXPORT_FIXTURE),
            "--target-parent-id",
            "12345678",
            "--state-file",
            str(state_path),
            "--workers",
            "1",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Summary: completed=0 not_reusable=2 failed=0" in output


def test_run_cli_retry_failed_requeues_fixture_terminal_items(monkeypatch, tmp_path, capsys):
    export = load_export_file(SAMPLE_EXPORT_FIXTURE)
    first_record, second_record = export.records
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_file": str(SAMPLE_EXPORT_FIXTURE.resolve()),
                "source_sha256": export.source_sha256,
                "target_parent_id": "12345678",
                "common_path": export.common_path,
                "workers": 8,
                "folder_map": {
                    "": "12345678",
                    "Demo": "1",
                    "Demo/1983": "2",
                    "Demo/1983/06": "3",
                    "Demo/1983/07": "4",
                },
                "completed": [],
                "not_reusable": [{"key": first_record.key, "path": first_record.path, "error": "Reuse=false"}],
                "failed": [{"key": second_record.key, "path": second_record.path, "error": "HTTP 500", "retries": 2}],
                "stats": {"total": 2, "completed": 0, "not_reusable": 1, "failed": 1},
                "last_flush_at": None,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", lambda host, session: object())
    monkeypatch.setattr("fastlink_transfer.app.create_remote_directories", lambda **kwargs: None)
    captured = {}

    def fake_run_file_phase(*, state, records, **kwargs):
        captured["keys"] = [record.key for record in records]
        for record in records:
            state.record_completed(record.key)
        return {"credential_fatal": False}

    monkeypatch.setattr("fastlink_transfer.app.run_file_phase", fake_run_file_phase)

    exit_code = run_cli(
        [
            "import-json",
            "--file",
            str(SAMPLE_EXPORT_FIXTURE),
            "--target-parent-id",
            "12345678",
            "--state-file",
            str(state_path),
            "--retry-failed",
        ]
    )

    assert exit_code == 0
    assert captured["keys"] == [first_record.key, second_record.key]
    output = capsys.readouterr().out
    assert "Startup: files=2 pending=2 folders=0 workers=8" in output
    assert "Summary: completed=2 not_reusable=0 failed=0" in output


def test_run_cli_surfaces_malformed_state_error_category(monkeypatch, tmp_path):
    export = load_export_file(SAMPLE_EXPORT_FIXTURE)
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_file": str(SAMPLE_EXPORT_FIXTURE.resolve()),
                "source_sha256": export.source_sha256,
                "target_parent_id": "12345678",
                "common_path": export.common_path,
                "workers": 8,
                "folder_map": {"": "12345678"},
                "completed": [],
                "not_reusable": [],
                "failed": [],
                "stats": {"total": 2, "completed": 0, "not_reusable": 0, "failed": 0},
                "last_flush_at": 123,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())

    with pytest.raises(ValueError, match=r"^malformed-state error$"):
        run_cli(
            [
                "import-json",
                "--file",
                str(SAMPLE_EXPORT_FIXTURE),
                "--target-parent-id",
                "12345678",
                "--state-file",
                str(state_path),
                "--dry-run",
            ]
        )


def test_run_cli_surfaces_mismatch_error_category(monkeypatch, tmp_path):
    export = load_export_file(SAMPLE_EXPORT_FIXTURE)
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "source_file": str(SAMPLE_EXPORT_FIXTURE.resolve()),
                "source_sha256": "different",
                "target_parent_id": "12345678",
                "common_path": export.common_path,
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
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        run_cli(
            [
                "import-json",
                "--file",
                str(SAMPLE_EXPORT_FIXTURE),
                "--target-parent-id",
                "12345678",
                "--state-file",
                str(state_path),
                "--dry-run",
            ]
        )


def test_run_cli_preserves_directory_phase_keyboard_interrupt_when_best_effort_flush_fails(
    monkeypatch, tmp_path, capsys
):
    state = RecordingState(flush_error=RuntimeError("flush exploded"))

    def raise_interrupt(**kwargs):
        raise KeyboardInterrupt

    _patch_non_dry_run_dependencies(
        monkeypatch,
        tmp_path,
        state=state,
        create_remote_directories_impl=raise_interrupt,
    )

    with pytest.raises(KeyboardInterrupt):
        run_cli(["import-json"])

    output = capsys.readouterr().out
    assert len(state.flush_calls) == 1
    assert "State flush failed: interrupted during run: flush exploded" in output
    assert "Summary: completed=0 not_reusable=0 failed=0" in output


def test_run_cli_does_not_duplicate_file_phase_interrupt_cleanup(monkeypatch, tmp_path, capsys):
    state = RecordingState()

    def raise_interrupt(**kwargs):
        raise KeyboardInterrupt

    _patch_non_dry_run_dependencies(
        monkeypatch,
        tmp_path,
        state=state,
        run_file_phase_impl=raise_interrupt,
    )

    with pytest.raises(KeyboardInterrupt):
        run_cli(["import-json"])

    output = capsys.readouterr().out
    assert state.flush_calls == []
    assert "State flush: interrupted during run" not in output
    assert "State flush failed: interrupted during run" not in output


def test_run_cli_returns_nonzero_and_logs_mkdir_failure(monkeypatch, tmp_path, capsys):
    state = RecordingState()

    def raise_mkdir_failure(**kwargs):
        raise RuntimeError("mkdir boom")

    def fail_if_run(**kwargs):  # pragma: no cover - guard
        raise AssertionError("file phase should not run after mkdir failure")

    _patch_non_dry_run_dependencies(
        monkeypatch,
        tmp_path,
        state=state,
        create_remote_directories_impl=raise_mkdir_failure,
        run_file_phase_impl=fail_if_run,
    )

    assert run_cli(["import-json"]) == 1
    output = capsys.readouterr().out
    assert len(state.flush_calls) == 1
    assert "State flush: mkdir failure: mkdir boom" in output
    assert "Summary: completed=0 not_reusable=0 failed=0" in output


def test_run_cli_passes_max_retries_to_directory_phase(monkeypatch, tmp_path):
    state = RecordingState()
    captured = {}

    def capture_mkdir_kwargs(**kwargs):
        captured.update(kwargs)

    _patch_non_dry_run_dependencies(
        monkeypatch,
        tmp_path,
        state=state,
        create_remote_directories_impl=capture_mkdir_kwargs,
    )

    assert run_cli(["import-json"]) == 0
    assert captured["max_retries"] == 5


def test_run_cli_propagates_file_phase_runtime_error_without_mkdir_label(monkeypatch, tmp_path, capsys):
    state = RecordingState()

    def raise_file_phase_failure(**kwargs):
        raise RuntimeError("file boom")

    _patch_non_dry_run_dependencies(
        monkeypatch,
        tmp_path,
        state=state,
        run_file_phase_impl=raise_file_phase_failure,
    )

    with pytest.raises(RuntimeError, match="file boom"):
        run_cli(["import-json"])

    output = capsys.readouterr().out
    assert len(state.flush_calls) == 1
    assert "State flush: file phase failure: file boom" in output
    assert "mkdir failure" not in output
