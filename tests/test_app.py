import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from fastlink_transfer.api import Decision, DecisionKind
from fastlink_transfer.app import run_cli, summarize_exit_code
from fastlink_transfer.config import BatchImportJsonConfig, ExportJsonConfig, build_command_config
from fastlink_transfer.import_planner import inspect_export_scope, rebuild_incomplete_plan_if_needed
from fastlink_transfer.import_state import SQLITE_HEADER, open_or_initialize_import_state
from fastlink_transfer.importer import load_export_file


SAMPLE_EXPORT_FIXTURE = Path(__file__).parent / "fixtures" / "sample_export.json"


def _seed_credentials(monkeypatch) -> None:
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")


def _plan_state(*, export_path: Path, state_path: Path, target_parent_id: str = "12345678"):
    scope = inspect_export_scope(export_path=export_path)
    state = open_or_initialize_import_state(
        state_path=state_path,
        source_file=str(export_path),
        source_sha256=scope.source_sha256,
        target_parent_id=target_parent_id,
        common_path=scope.common_path,
    )
    rebuild_incomplete_plan_if_needed(export_path=export_path, state=state, scope=scope)
    return state


def _seed_created_folder_rows(state, folder_map: dict[str, str]) -> None:
    state.connection.executemany(
        "INSERT OR REPLACE INTO folders (folder_key, parent_key, remote_folder_id, status, last_error) VALUES (?, ?, ?, ?, ?)",
        [
            (folder_key, folder_key.rsplit("/", 1)[0] if "/" in folder_key else "", remote_folder_id, "created", None)
            for folder_key, remote_folder_id in folder_map.items()
        ],
    )
    state.connection.commit()
    state.refresh_folder_map()


def test_summarize_exit_code_returns_zero_when_only_not_reusable_exists():
    assert summarize_exit_code(failed_count=0, credential_fatal=False) == 0


def test_summarize_exit_code_returns_nonzero_for_failed_items():
    assert summarize_exit_code(failed_count=1, credential_fatal=False) == 1


def test_summarize_exit_code_returns_nonzero_for_credential_fatal():
    assert summarize_exit_code(failed_count=0, credential_fatal=True) == 1


def test_build_command_config_builds_export_json_config(tmp_path):
    args = SimpleNamespace(
        command="export_json",
        source_parent_id="12345678",
        output_file=str(tmp_path / "out.json"),
        state_file=str(tmp_path / "state.json"),
        workers=16,
        max_retries=7,
        flush_every=50,
    )

    config = build_command_config(args)

    assert isinstance(config, ExportJsonConfig)
    assert config.command == "export_json"
    assert config.source_parent_id == "12345678"
    assert config.output_file == (tmp_path / "out.json").resolve()
    assert config.state_file == (tmp_path / "state.json").resolve()
    assert config.workers == 16
    assert config.max_retries == 7
    assert config.flush_every == 50


def test_run_cli_routes_export_json_to_export_runner(monkeypatch, tmp_path):
    config = ExportJsonConfig(
        command="export_json",
        source_parent_id="12345678",
        output_file=(tmp_path / "out.json").resolve(),
        state_file=(tmp_path / "state.json").resolve(),
        workers=8,
        max_retries=5,
        flush_every=100,
    )
    calls = []

    monkeypatch.setattr(
        "fastlink_transfer.app.parse_args",
        lambda argv=None: (SimpleNamespace(command="export_json"), config),
    )
    monkeypatch.setattr(
        "fastlink_transfer.app.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    monkeypatch.setattr(
        "fastlink_transfer.app.run_export_json",
        lambda **kwargs: calls.append(kwargs) or 0,
    )

    exit_code = run_cli(["export-json"])

    assert exit_code == 0
    assert calls and calls[0]["config"] == config


def test_run_cli_export_json_happy_path_leaves_final_json(monkeypatch, tmp_path):
    config = ExportJsonConfig(
        command="export_json",
        source_parent_id="12345678",
        output_file=(tmp_path / "out.json").resolve(),
        state_file=(tmp_path / "export.state.json").resolve(),
        workers=1,
        max_retries=0,
        flush_every=1,
    )

    class HappyClient:
        def __init__(self, host, session):
            self.host = host

        def get_directory_identity(self, *, parent_file_id):
            return Decision(kind=DecisionKind.DIRECTORY_CREATED, payload={"root_name": "Movies"})

        def get_file_list(self, *, parent_file_id):
            return Decision(
                kind=DecisionKind.COMPLETED,
                payload={
                    "items": [
                        {
                            "FileId": 1,
                            "Type": 0,
                            "FileName": "a.txt",
                            "Etag": "0" * 32,
                            "Size": 5,
                        }
                    ],
                    "total": 1,
                },
            )

    monkeypatch.setattr(
        "fastlink_transfer.app.parse_args",
        lambda argv=None: (SimpleNamespace(command="export_json"), config),
    )
    monkeypatch.setattr(
        "fastlink_transfer.app.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", HappyClient)

    assert run_cli(["export-json"]) == 0

    payload = json.loads(config.output_file.read_text(encoding="utf-8"))
    assert payload["commonPath"] == "Movies/"
    assert payload["totalFilesCount"] == 1
    assert payload["files"] == [{"path": "a.txt", "etag": "0", "size": "5"}]
    assert not config.state_file.exists()
    assert not config.state_file.with_suffix(".records.jsonl").exists()
    assert not config.state_file.with_suffix(".finalize.sqlite3").exists()
    assert not config.state_file.with_suffix(".output.tmp.json").exists()


def test_run_cli_import_json_dry_run_builds_or_reuses_sqlite_state(monkeypatch, tmp_path, capsys):
    export_file = tmp_path / "export.json"
    export_file.write_text(
        '{"usesBase62EtagsInExport": false, "commonPath": "Demo/", "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "a/b.txt"}]}',
        encoding="utf-8",
    )
    state_file = tmp_path / "import.state.sqlite3"
    _seed_credentials(monkeypatch)

    class FailIfUsedClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("remote client should not be created during dry run")

    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", FailIfUsedClient)

    args = [
        "import-json",
        "--file",
        str(export_file),
        "--target-parent-id",
        "12345678",
        "--state-file",
        str(state_file),
        "--dry-run",
    ]

    assert run_cli(args) == 0
    assert run_cli(args) == 0

    output = capsys.readouterr().out
    assert state_file.exists()
    assert "Startup: files=1 pending=1 folders=2 workers=8" in output
    assert output.count("Dry run: no remote mutations performed") == 2


def test_run_cli_import_json_rejects_non_sqlite_state_file(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    state_file.write_text("{}", encoding="utf-8")
    _seed_credentials(monkeypatch)

    with pytest.raises(ValueError, match=r"^unsupported state-file format$"):
        run_cli(
            [
                "import-json",
                "--file",
                str(SAMPLE_EXPORT_FIXTURE),
                "--target-parent-id",
                "12345678",
                "--state-file",
                str(state_file),
                "--dry-run",
            ]
        )


def test_run_cli_import_json_surfaces_malformed_state_error_category(monkeypatch, tmp_path):
    state_file = tmp_path / "broken.state.sqlite3"
    state_file.write_bytes(SQLITE_HEADER + b"garbage")
    _seed_credentials(monkeypatch)

    with pytest.raises(ValueError, match=r"^malformed-state error$"):
        run_cli(
            [
                "import-json",
                "--file",
                str(SAMPLE_EXPORT_FIXTURE),
                "--target-parent-id",
                "12345678",
                "--state-file",
                str(state_file),
                "--dry-run",
            ]
        )


def test_run_cli_import_json_surfaces_mismatch_error_category(monkeypatch, tmp_path):
    state_file = tmp_path / "import.state.sqlite3"
    state = _plan_state(export_path=SAMPLE_EXPORT_FIXTURE, state_path=state_file)
    state.close()
    _seed_credentials(monkeypatch)

    with pytest.raises(ValueError, match=r"^mismatch error$"):
        run_cli(
            [
                "import-json",
                "--file",
                str(SAMPLE_EXPORT_FIXTURE),
                "--target-parent-id",
                "87654321",
                "--state-file",
                str(state_file),
                "--dry-run",
            ]
        )


def test_run_cli_import_json_returns_zero_when_only_not_reusable_rows_remain(monkeypatch, tmp_path, capsys):
    state_file = tmp_path / "import.state.sqlite3"
    _seed_credentials(monkeypatch)
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())

    class NotReusableClient:
        def __init__(self, host, session):
            self._next_folder_id = 200

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
            str(state_file),
            "--workers",
            "1",
        ]
    )

    retry_export_path = state_file.with_suffix(".retry.export.json")
    assert exit_code == 0
    assert retry_export_path.exists()
    payload = json.loads(retry_export_path.read_text(encoding="utf-8"))
    assert payload["usesBase62EtagsInExport"] is True
    assert payload["commonPath"] == "Demo/"
    output = capsys.readouterr().out
    assert "Summary: completed=0 not_reusable=2 failed=0" in output


def test_run_cli_import_json_retry_failed_requeues_terminal_rows(monkeypatch, tmp_path, capsys):
    state_file = tmp_path / "import.state.sqlite3"
    state = _plan_state(export_path=SAMPLE_EXPORT_FIXTURE, state_path=state_file)
    _seed_created_folder_rows(
        state,
        {
            "": "12345678",
            "Demo": "1",
            "Demo/1983": "2",
            "Demo/1983/06": "3",
            "Demo/1983/07": "4",
        },
    )
    state.connection.execute(
        "UPDATE files SET status = 'not_reusable', error = 'Reuse=false' WHERE path = ?",
        ("1983/06/example-a.rar",),
    )
    state.connection.execute(
        "UPDATE files SET status = 'failed', error = 'HTTP 500', retries = 2 WHERE path = ?",
        ("1983/07/example-b.rar",),
    )
    state.connection.execute(
        "UPDATE job SET not_reusable_count = 1, failed_count = 1 WHERE singleton = 1"
    )
    state.connection.commit()
    assert state.fetch_status_counts() == {
        "pending": 0,
        "completed": 0,
        "not_reusable": 1,
        "failed": 1,
    }
    assert state.fetch_pending_record_paths() == []
    state.close()

    _seed_credentials(monkeypatch)
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    uploaded = []

    class CompletingClient:
        def __init__(self, host, session):
            return

        def mkdir(self, *, parent_file_id: str, folder_name: str):
            raise AssertionError("directory phase should reuse persisted folder mappings")

        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            uploaded.append((file_name, parent_file_id))
            return Decision(kind=DecisionKind.COMPLETED, file_id="500")

    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", CompletingClient)

    exit_code = run_cli(
        [
            "import-json",
            "--file",
            str(SAMPLE_EXPORT_FIXTURE),
            "--target-parent-id",
            "12345678",
            "--state-file",
            str(state_file),
            "--workers",
            "1",
            "--retry-failed",
        ]
    )

    reopened = open_or_initialize_import_state(
        state_path=state_file,
        source_file=str(SAMPLE_EXPORT_FIXTURE),
        source_sha256=inspect_export_scope(export_path=SAMPLE_EXPORT_FIXTURE).source_sha256,
        target_parent_id="12345678",
        common_path="Demo/",
    )

    assert exit_code == 0
    assert uploaded == [("example-a.rar", "3"), ("example-b.rar", "4")]
    assert reopened.fetch_status_counts() == {
        "pending": 0,
        "completed": 2,
        "not_reusable": 0,
        "failed": 0,
    }
    assert state_file.with_suffix(".retry.export.json").exists() is False
    output = capsys.readouterr().out
    assert "Startup: files=2 pending=2 folders=0 workers=1" in output
    assert "Summary: completed=2 not_reusable=0 failed=0" in output


def test_run_cli_import_json_retry_failed_dry_run_does_not_mutate_terminal_rows(
    monkeypatch, tmp_path, capsys
):
    state_file = tmp_path / "import.state.sqlite3"
    state = _plan_state(export_path=SAMPLE_EXPORT_FIXTURE, state_path=state_file)
    state.connection.execute(
        "UPDATE files SET status = 'not_reusable', error = 'Reuse=false' WHERE path = ?",
        ("1983/06/example-a.rar",),
    )
    state.connection.execute(
        "UPDATE files SET status = 'failed', error = 'HTTP 500', retries = 2 WHERE path = ?",
        ("1983/07/example-b.rar",),
    )
    state.connection.execute(
        "UPDATE job SET not_reusable_count = 1, failed_count = 1 WHERE singleton = 1"
    )
    state.connection.commit()
    state.close()

    _seed_credentials(monkeypatch)

    class FailIfUsedClient:
        def __init__(self, *args, **kwargs):
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
            str(state_file),
            "--retry-failed",
            "--dry-run",
        ]
    )

    reopened = open_or_initialize_import_state(
        state_path=state_file,
        source_file=str(SAMPLE_EXPORT_FIXTURE),
        source_sha256=inspect_export_scope(export_path=SAMPLE_EXPORT_FIXTURE).source_sha256,
        target_parent_id="12345678",
        common_path="Demo/",
    )

    assert exit_code == 0
    assert reopened.fetch_status_counts() == {
        "pending": 0,
        "completed": 0,
        "not_reusable": 1,
        "failed": 1,
    }
    assert state_file.with_suffix(".retry.export.json").exists() is False
    output = capsys.readouterr().out
    assert "Startup: files=2 pending=2 folders=4 workers=8" in output
    assert "Dry run: no remote mutations performed" in output


def test_run_cli_import_json_removes_stale_retry_export_on_file_phase_error_without_unsuccessful_rows(
    monkeypatch, tmp_path
):
    state_file = tmp_path / "import.state.sqlite3"
    stale_retry_export = state_file.with_suffix(".retry.export.json")
    stale_retry_export.write_text("stale", encoding="utf-8")

    _seed_credentials(monkeypatch)
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    monkeypatch.setattr("fastlink_transfer.app.PanApiClient", lambda host, session: object())
    monkeypatch.setattr("fastlink_transfer.app.create_remote_directories", lambda **kwargs: None)

    def raise_file_phase_failure(**kwargs):
        raise RuntimeError("file boom")

    monkeypatch.setattr("fastlink_transfer.app.run_file_phase_sqlite", raise_file_phase_failure)

    with pytest.raises(RuntimeError, match="file boom"):
        run_cli(
            [
                "import-json",
                "--file",
                str(SAMPLE_EXPORT_FIXTURE),
                "--target-parent-id",
                "12345678",
                "--state-file",
                str(state_file),
                "--workers",
                "1",
            ]
        )

    assert stale_retry_export.exists() is False


def test_run_cli_batch_import_dry_run_skips_remote_mutation_and_prints_startup_summary(
    monkeypatch, tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=tmp_path / ".state" / "batch",
        workers=8,
        json_parallelism=2,
        max_retries=5,
        flush_every=100,
        retry_failed=False,
        dry_run=True,
    )
    monkeypatch.setattr("fastlink_transfer.app.parse_args", lambda argv=None: (None, config))
    monkeypatch.setattr(
        "fastlink_transfer.app.load_credentials", lambda: SimpleNamespace(host="https://www.123pan.com")
    )
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    monkeypatch.setattr(
        "fastlink_transfer.app.run_batch_import_cli",
        lambda config: print("Batch startup: jobs=1 workers_per_job=8 json_parallelism=2")
        or print("Dry run: no remote mutations performed")
        or 0,
    )

    assert run_cli(["batch-import-json"]) == 0
    output = capsys.readouterr().out
    assert "Batch startup: jobs=1 workers_per_job=8 json_parallelism=2" in output
    assert "Dry run: no remote mutations performed" in output


def test_run_cli_batch_import_returns_nonzero_after_failed_child_jobs(monkeypatch, tmp_path):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=tmp_path / ".state" / "batch",
        workers=8,
        json_parallelism=2,
        max_retries=5,
        flush_every=100,
        retry_failed=False,
        dry_run=False,
    )
    monkeypatch.setattr("fastlink_transfer.app.parse_args", lambda argv=None: (None, config))
    monkeypatch.setattr(
        "fastlink_transfer.app.load_credentials", lambda: SimpleNamespace(host="https://www.123pan.com")
    )
    monkeypatch.setattr("fastlink_transfer.app.build_session", lambda creds: object())
    monkeypatch.setattr(
        "fastlink_transfer.app.run_batch_import_cli",
        lambda config: 1,
    )

    assert run_cli(["batch-import-json"]) == 1


def test_readme_mentions_batch_import_json_and_sqlite_state():
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "batch-import-json" in readme
    assert "sqlite" in readme.lower()
    assert "json-parallelism" in readme
    assert ".state/h-suki.import-state.retry.export.json" in readme
    assert "<state-file>.retry.export.json" not in readme
