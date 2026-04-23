import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from fastlink_transfer.api import Decision, DecisionKind
from fastlink_transfer.batch_import import (
    BatchJob,
    BatchRunSummary,
    _open_planned_state_for_job,
    discover_batch_json_jobs,
    run_batch_import_cli,
    run_batch_import_jobs,
    summarize_batch_exit_code,
    validate_batch_target_collisions,
)
from fastlink_transfer.config import BatchImportJsonConfig


def _write_export_json(path: Path, *, common_path: str, files: list[dict[str, str]]) -> None:
    path.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": common_path,
                "files": files,
            }
        ),
        encoding="utf-8",
    )


def _make_batch_job(tmp_path, rel_path, retry_export_rel_path=None):
    rel = Path(rel_path)
    retry_rel = Path(retry_export_rel_path) if retry_export_rel_path is not None else rel
    return BatchJob(
        json_path=tmp_path / "inputs" / rel,
        relative_json_path=rel,
        state_path=tmp_path / ".state" / rel.with_suffix(".import-state.sqlite3"),
        retry_export_path=tmp_path / ".state" / retry_rel,
    )


def test_discover_batch_json_jobs_recurses_and_sorts_by_relative_path(tmp_path):
    input_dir = tmp_path / "exports"
    (input_dir / "b").mkdir(parents=True)
    (input_dir / "a").mkdir()
    (input_dir / "b" / "two.json").write_text("{}", encoding="utf-8")
    (input_dir / "a" / "one.json").write_text("{}", encoding="utf-8")

    jobs = discover_batch_json_jobs(input_dir=input_dir, state_dir=tmp_path / ".state")

    assert [job.relative_json_path.as_posix() for job in jobs] == ["a/one.json", "b/two.json"]
    assert jobs[0].state_path == (tmp_path / ".state" / "a" / "one.import-state.sqlite3")


def test_discover_batch_json_jobs_preserves_relative_retry_export_suffix(tmp_path):
    input_dir = tmp_path / "exports"
    (input_dir / "nested").mkdir(parents=True)
    (input_dir / "nested" / "demo.json").write_text("{}", encoding="utf-8")

    job = discover_batch_json_jobs(input_dir=input_dir, state_dir=tmp_path / ".state")[0]

    assert job.retry_export_path.is_relative_to(tmp_path / ".state")
    assert job.retry_export_path.relative_to(tmp_path / ".state") == Path("nested/demo.json")


def test_discover_batch_json_jobs_rejects_state_dir_equal_to_input_dir(tmp_path):
    (tmp_path / "a.json").write_text("{}", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=r"--state-dir must not be the same as or nested inside --input-dir",
    ):
        discover_batch_json_jobs(input_dir=tmp_path, state_dir=tmp_path)


def test_discover_batch_json_jobs_rejects_state_dir_nested_inside_input_dir(tmp_path):
    input_dir = tmp_path / "exports"
    state_dir = input_dir / ".state" / "batch"
    (input_dir / "nested").mkdir(parents=True)
    state_dir.mkdir(parents=True)
    (input_dir / "nested" / "keep.json").write_text("{}", encoding="utf-8")
    (state_dir / "retry.json").write_text("{}", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=r"--state-dir must not be the same as or nested inside --input-dir",
    ):
        discover_batch_json_jobs(input_dir=input_dir, state_dir=state_dir)


def test_discover_batch_json_jobs_fails_when_no_json_files_exist(tmp_path):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()

    with pytest.raises(ValueError, match="no json files found"):
        discover_batch_json_jobs(input_dir=input_dir, state_dir=tmp_path / ".state")


def test_preflight_collision_detection_rejects_duplicate_target_file_paths(tmp_path):
    jobs = [
        _make_batch_job(tmp_path, rel_path="a/one.json"),
        _make_batch_job(tmp_path, rel_path="b/two.json"),
    ]
    planned_files_by_job = {
        jobs[0]: ["Demo/x.txt"],
        jobs[1]: ["Demo/x.txt"],
    }

    with pytest.raises(ValueError, match="target path collision"):
        validate_batch_target_collisions(planned_files_by_job)


def test_preflight_collision_detection_rejects_file_vs_directory_conflicts(tmp_path):
    jobs = [
        _make_batch_job(tmp_path, rel_path="a/one.json"),
        _make_batch_job(tmp_path, rel_path="b/two.json"),
    ]
    planned_files_by_job = {
        jobs[0]: ["Demo/a"],
        jobs[1]: ["Demo/a/b.txt"],
    }

    with pytest.raises(ValueError, match="file-directory collision"):
        validate_batch_target_collisions(planned_files_by_job)


def test_run_batch_import_continues_other_jobs_after_child_failure(monkeypatch, tmp_path):
    del monkeypatch
    jobs = [_make_batch_job(tmp_path, rel_path="a/one.json"), _make_batch_job(tmp_path, rel_path="b/two.json")]
    calls = []

    def fake_run_child(job, **kwargs):
        del kwargs
        calls.append(job.relative_json_path.as_posix())
        if job.relative_json_path.as_posix() == "a/one.json":
            return SimpleNamespace(status="failed", credential_fatal=False)
        return SimpleNamespace(status="completed", credential_fatal=False)

    summary = run_batch_import_jobs(jobs=jobs, json_parallelism=2, run_child_job=fake_run_child)

    assert calls == ["a/one.json", "b/two.json"]
    assert summary.jobs_failed == 1
    assert summary.jobs_succeeded == 1
    assert summary.jobs_succeeded_with_not_reusable == 0


def test_run_batch_import_stops_launching_new_jobs_after_credential_fatal(monkeypatch, tmp_path):
    del monkeypatch
    jobs = [
        _make_batch_job(tmp_path, rel_path="a/one.json"),
        _make_batch_job(tmp_path, rel_path="b/two.json"),
        _make_batch_job(tmp_path, rel_path="c/three.json"),
    ]
    calls = []

    def fake_run_child(job, **kwargs):
        del kwargs
        calls.append(job.relative_json_path.as_posix())
        if job.relative_json_path.as_posix() == "a/one.json":
            return SimpleNamespace(status="failed", credential_fatal=True)
        return SimpleNamespace(status="completed", credential_fatal=False)

    summary = run_batch_import_jobs(jobs=jobs, json_parallelism=1, run_child_job=fake_run_child)

    assert calls == ["a/one.json"]
    assert summary.credential_fatal is True


def test_run_batch_import_counts_not_reusable_only_jobs_as_non_failed(tmp_path):
    jobs = [_make_batch_job(tmp_path, rel_path="a/one.json")]

    summary = run_batch_import_jobs(
        jobs=jobs,
        json_parallelism=1,
        run_child_job=lambda job, **kwargs: SimpleNamespace(
            status="completed_with_not_reusable",
            credential_fatal=False,
            retry_export_path=job.retry_export_path,
        ),
    )

    assert summary.jobs_succeeded == 0
    assert summary.jobs_succeeded_with_not_reusable == 1
    assert summary.jobs_failed == 0


def test_run_batch_import_summary_lists_retry_export_paths(tmp_path):
    jobs = [_make_batch_job(tmp_path, rel_path="a/one.json")]

    summary = run_batch_import_jobs(
        jobs=jobs,
        json_parallelism=1,
        run_child_job=lambda job, **kwargs: SimpleNamespace(
            status="completed_with_not_reusable",
            credential_fatal=False,
            retry_export_path=job.retry_export_path,
        ),
    )

    assert summary.retry_export_paths == [jobs[0].retry_export_path]


def test_run_batch_import_exit_code_is_zero_when_only_not_reusable_jobs_remain(tmp_path):
    summary = BatchRunSummary(
        jobs_total=1,
        jobs_succeeded=0,
        jobs_succeeded_with_not_reusable=1,
        jobs_failed=0,
        credential_fatal=False,
        retry_export_paths=[tmp_path / ".state" / "a/one.json"],
    )

    assert summarize_batch_exit_code(summary) == 0


def test_run_batch_import_exit_code_is_one_when_any_child_job_failed(tmp_path):
    summary = BatchRunSummary(
        jobs_total=2,
        jobs_succeeded=1,
        jobs_succeeded_with_not_reusable=0,
        jobs_failed=1,
        credential_fatal=False,
        retry_export_paths=[],
    )

    assert summarize_batch_exit_code(summary) == 1


def test_run_batch_import_cli_dry_run_skips_remote_mutation_and_prints_startup_summary(
    tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    (input_dir / "demo.json").write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": "Demo/",
                "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "a/b.txt"}],
            }
        ),
        encoding="utf-8",
    )
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

    assert run_batch_import_cli(config=config) == 0
    output = capsys.readouterr().out
    assert "Batch startup: jobs=1 workers_per_job=8 json_parallelism=2" in output
    assert "Dry run: no remote mutations performed" in output
    assert "Batch summary:" not in output
    assert "completed=" not in output


def test_run_batch_import_cli_treats_session_setup_failure_as_batch_global(
    monkeypatch, tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    for name in ["a.json", "b.json", "c.json"]:
        _write_export_json(
            input_dir / name,
            common_path="Demo/",
            files=[{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": f"{name}.txt"}],
        )

    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=tmp_path / ".state" / "batch",
        workers=8,
        json_parallelism=1,
        max_retries=5,
        flush_every=100,
        retry_failed=False,
        dry_run=False,
    )
    load_calls = []
    build_calls = []

    def fake_load_credentials():
        load_calls.append("load")
        return SimpleNamespace(host="https://www.123pan.com")

    def fake_build_session(creds):
        build_calls.append(creds.host)
        raise ValueError("session setup failed")

    monkeypatch.setattr("fastlink_transfer.batch_import.load_credentials", fake_load_credentials)
    monkeypatch.setattr("fastlink_transfer.batch_import.build_session", fake_build_session)

    assert run_batch_import_cli(config=config) == 1
    output = capsys.readouterr().out
    assert load_calls == ["load"]
    assert build_calls == ["https://www.123pan.com"]
    assert "session setup failed" in output


def test_run_batch_import_cli_dry_run_rejects_file_collision_with_zero_file_common_path_directory(
    monkeypatch, tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    _write_export_json(input_dir / "dir-only.json", common_path="Demo/a/", files=[])
    _write_export_json(
        input_dir / "file.json",
        common_path="Demo/",
        files=[{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "a"}],
    )

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
    monkeypatch.setattr(
        "fastlink_transfer.batch_import.load_credentials",
        lambda: (_ for _ in ()).throw(AssertionError("dry run should not load credentials")),
    )

    assert run_batch_import_cli(config=config) == 1
    output = capsys.readouterr().out
    assert "file-directory collision" in output


def test_run_batch_import_cli_stops_new_jobs_after_directory_phase_credential_fatal(
    monkeypatch, tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    for index in range(3):
        _write_export_json(
            input_dir / f"job-{index}.json",
            common_path="Demo/",
            files=[
                {
                    "etag": "0123456789abcdef0123456789abcdef",
                    "size": "5",
                    "path": f"file-{index}.txt",
                }
            ],
        )

    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=tmp_path / ".state" / "batch",
        workers=8,
        json_parallelism=1,
        max_retries=0,
        flush_every=100,
        retry_failed=False,
        dry_run=False,
    )
    load_calls = []
    build_calls = []
    mkdir_calls = []

    def fake_load_credentials():
        load_calls.append("load")
        return SimpleNamespace(host="https://www.123pan.com")

    def fake_build_session(creds):
        build_calls.append(creds.host)
        return object()

    class CredentialFatalDirectoryClient:
        def __init__(self, host, session):
            self.host = host
            self.session = session

        def mkdir(self, *, parent_file_id: str, folder_name: str):
            mkdir_calls.append((parent_file_id, folder_name))
            return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error="HTTP 401")

        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            raise AssertionError("file phase should not start after directory auth failure")

    monkeypatch.setattr("fastlink_transfer.batch_import.load_credentials", fake_load_credentials)
    monkeypatch.setattr("fastlink_transfer.batch_import.build_session", fake_build_session)
    monkeypatch.setattr("fastlink_transfer.batch_import.PanApiClient", CredentialFatalDirectoryClient)

    assert run_batch_import_cli(config=config) == 1
    output = capsys.readouterr().out
    assert load_calls == ["load"]
    assert build_calls == ["https://www.123pan.com"]
    assert mkdir_calls == [("12345678", "Demo")]
    assert "HTTP 401" in output


def test_run_batch_import_cli_returns_preflight_failures_without_runtime_credential_setup(
    monkeypatch, tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    (input_dir / "broken.json").write_text("{", encoding="utf-8")

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
    monkeypatch.setattr(
        "fastlink_transfer.batch_import.load_credentials",
        lambda: (_ for _ in ()).throw(AssertionError("runtime credentials should not load with zero runnable jobs")),
    )
    monkeypatch.setattr(
        "fastlink_transfer.batch_import.build_session",
        lambda creds: (_ for _ in ()).throw(AssertionError("runtime session should not build with zero runnable jobs")),
    )

    assert run_batch_import_cli(config=config) == 1
    output = capsys.readouterr().out
    assert "Batch preflight failed: broken.json" in output
    assert "Batch summary: total=1 completed=0 completed_with_not_reusable=0 failed=1" in output


def test_run_batch_import_cli_treats_shared_state_root_failure_as_batch_fatal_startup_failure(
    monkeypatch, tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    _write_export_json(
        input_dir / "one.json",
        common_path="Demo/",
        files=[{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "one.txt"}],
    )
    _write_export_json(
        input_dir / "two.json",
        common_path="Demo/",
        files=[{"etag": "fedcba9876543210fedcba9876543210", "size": "6", "path": "two.txt"}],
    )

    shared_state_root = tmp_path / "blocked-state-root"
    shared_state_root.write_text("not-a-directory", encoding="utf-8")
    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=shared_state_root,
        workers=8,
        json_parallelism=2,
        max_retries=5,
        flush_every=100,
        retry_failed=False,
        dry_run=False,
    )
    monkeypatch.setattr(
        "fastlink_transfer.batch_import.load_credentials",
        lambda: (_ for _ in ()).throw(AssertionError("startup failure should stop before credential loading")),
    )
    monkeypatch.setattr(
        "fastlink_transfer.batch_import.build_session",
        lambda creds: (_ for _ in ()).throw(AssertionError("startup failure should stop before session setup")),
    )

    assert run_batch_import_cli(config=config) == 1
    output = capsys.readouterr().out
    assert "Batch startup failed:" in output
    assert "blocked-state-root" in output
    assert "Batch preflight failed:" not in output
    assert "Batch summary:" not in output


def test_open_planned_state_for_new_job_skips_separate_scope_scan(monkeypatch, tmp_path):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    _write_export_json(
        input_dir / "demo.json",
        common_path="Demo/",
        files=[{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "a.txt"}],
    )
    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=tmp_path / ".state" / "batch",
        workers=8,
        json_parallelism=1,
        max_retries=0,
        flush_every=100,
        retry_failed=False,
        dry_run=True,
    )
    job = discover_batch_json_jobs(input_dir=input_dir, state_dir=config.state_dir)[0]
    monkeypatch.setattr(
        "fastlink_transfer.batch_import.inspect_export_scope",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("new-state planning should not do a separate scope scan")),
    )

    state = _open_planned_state_for_job(job=job, config=config)

    assert state.job_scope == {
        "source_sha256": state.job_scope["source_sha256"],
        "target_parent_id": "12345678",
        "common_path": "Demo/",
    }
    assert len(state.job_scope["source_sha256"]) == 64
    assert state.fetch_pending_record_paths() == ["a.txt"]


def test_run_batch_import_cli_reuses_shared_remote_directory_across_jobs(monkeypatch, tmp_path, capsys):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    _write_export_json(
        input_dir / "one.json",
        common_path="Demo/",
        files=[{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "one.txt"}],
    )
    _write_export_json(
        input_dir / "two.json",
        common_path="Demo/",
        files=[{"etag": "fedcba9876543210fedcba9876543210", "size": "6", "path": "two.txt"}],
    )
    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=tmp_path / ".state" / "batch",
        workers=8,
        json_parallelism=1,
        max_retries=0,
        flush_every=100,
        retry_failed=False,
        dry_run=False,
    )
    mkdir_calls = []
    list_calls = []

    monkeypatch.setattr(
        "fastlink_transfer.batch_import.load_credentials",
        lambda: SimpleNamespace(host="https://www.123pan.com"),
    )
    monkeypatch.setattr("fastlink_transfer.batch_import.build_session", lambda creds: object())

    class ReusingClient:
        def __init__(self, host, session):
            self.host = host
            self.session = session

        def get_file_list(self, *, parent_file_id: str):
            list_calls.append(parent_file_id)
            return Decision(kind=DecisionKind.COMPLETED, payload={"items": [], "total": 0})

        def mkdir(self, *, parent_file_id: str, folder_name: str):
            mkdir_calls.append((parent_file_id, folder_name))
            return Decision(kind=DecisionKind.DIRECTORY_CREATED, file_id="200")

        def rapid_upload(self, *, etag: str, size: int, file_name: str, parent_file_id: str):
            return Decision(kind=DecisionKind.COMPLETED, file_id=f"{file_name}-id")

    monkeypatch.setattr("fastlink_transfer.batch_import.PanApiClient", ReusingClient)

    assert run_batch_import_cli(config=config) == 0
    output = capsys.readouterr().out
    assert mkdir_calls == [("12345678", "Demo")]
    assert list_calls == ["12345678"]
    assert "Batch summary: total=2 completed=2 completed_with_not_reusable=0 failed=0" in output


def test_run_batch_import_cli_retry_failed_checks_collisions_for_requeued_rows(
    monkeypatch, tmp_path, capsys
):
    input_dir = tmp_path / "exports"
    input_dir.mkdir()
    _write_export_json(
        input_dir / "one.json",
        common_path="Demo/",
        files=[{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "one.txt"}],
    )
    _write_export_json(
        input_dir / "two.json",
        common_path="Demo/",
        files=[{"etag": "fedcba9876543210fedcba9876543210", "size": "6", "path": "two.txt"}],
    )
    config = BatchImportJsonConfig(
        command="batch_import_json",
        input_dir=input_dir,
        target_parent_id="12345678",
        state_dir=tmp_path / ".state" / "batch",
        workers=8,
        json_parallelism=1,
        max_retries=0,
        flush_every=100,
        retry_failed=True,
        dry_run=True,
    )
    jobs = discover_batch_json_jobs(input_dir=input_dir, state_dir=config.state_dir)
    for job in jobs:
        state = _open_planned_state_for_job(job=job, config=config)
        state.connection.execute(
            "UPDATE files SET status = 'failed', error = 'HTTP 500' WHERE path = ?",
            ("one.txt" if job.relative_json_path.name == "one.json" else "two.txt",),
        )
        state.connection.execute(
            "INSERT INTO files (record_key, path, file_name, relative_parent_dir, etag_hex, size, status, error, retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"extra-{job.relative_json_path.name}",
                "collision.txt",
                "collision.txt",
                "",
                "0123456789abcdef0123456789abcdef",
                1,
                "failed",
                "HTTP 500",
                1,
            ),
        )
        state.connection.execute(
            "UPDATE job SET total_files = total_files + 1, failed_count = failed_count + 2 WHERE singleton = 1"
        )
        state.connection.commit()
        state.close()
    monkeypatch.setattr(
        "fastlink_transfer.batch_import.load_credentials",
        lambda: (_ for _ in ()).throw(AssertionError("collision preflight should stop before credentials")),
    )

    assert run_batch_import_cli(config=config) == 1
    output = capsys.readouterr().out
    assert "target path collision: Demo/collision.txt" in output
