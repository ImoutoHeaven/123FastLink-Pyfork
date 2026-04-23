from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import threading

from fastlink_transfer.api import PanApiClient
from fastlink_transfer.auth import build_session, load_credentials
from fastlink_transfer.import_planner import inspect_export_scope, rebuild_incomplete_plan_if_needed
from fastlink_transfer.import_state import open_or_initialize_import_state
from fastlink_transfer.runner import (
    DirectoryPhaseCredentialFatalError,
    create_remote_directories,
    finalize_import_job,
    run_file_phase_sqlite,
    safe_print,
)


@dataclass(frozen=True)
class BatchJob:
    json_path: Path
    relative_json_path: Path
    state_path: Path
    retry_export_path: Path


@dataclass(frozen=True)
class BatchRunSummary:
    jobs_total: int
    jobs_succeeded: int
    jobs_succeeded_with_not_reusable: int
    jobs_failed: int
    credential_fatal: bool
    retry_export_paths: list[Path]


@dataclass(frozen=True)
class _BatchChildResult:
    status: str
    credential_fatal: bool
    retry_export_path: Path | None = None


def derive_batch_retry_export_path(*, state_dir: Path, relative_json_path: Path) -> Path:
    return state_dir / relative_json_path


def validate_batch_target_collisions(
    planned_files_by_job: dict[BatchJob, list[str]],
    planned_directories_by_job: dict[BatchJob, list[str]] | None = None,
) -> None:
    normalized_file_paths: set[str] = set()
    normalized_directory_paths: set[str] = set()

    for job in sorted(planned_files_by_job, key=lambda item: item.relative_json_path.as_posix()):
        for target_path in planned_files_by_job[job]:
            normalized_path = PurePosixPath(target_path).as_posix()
            if normalized_path in normalized_file_paths:
                raise ValueError(f"target path collision: {normalized_path}")
            normalized_file_paths.add(normalized_path)

    if planned_directories_by_job is not None:
        for job in sorted(planned_directories_by_job, key=lambda item: item.relative_json_path.as_posix()):
            for target_path in planned_directories_by_job[job]:
                normalized_path = PurePosixPath(target_path).as_posix()
                if normalized_path in ("", "."):
                    continue
                normalized_directory_paths.add(normalized_path)

    for file_path in sorted(normalized_file_paths):
        if file_path in normalized_directory_paths:
            raise ValueError(f"file-directory collision: {file_path}")
        for parent_path in _iter_parent_paths(file_path):
            if parent_path in normalized_file_paths:
                raise ValueError(f"file-directory collision: {file_path}")

    for directory_path in sorted(normalized_directory_paths):
        for parent_path in _iter_parent_paths(directory_path):
            if parent_path in normalized_file_paths:
                raise ValueError(f"file-directory collision: {directory_path}")


def discover_batch_json_jobs(*, input_dir: Path, state_dir: Path) -> list[BatchJob]:
    resolved_input_dir = input_dir.resolve()
    resolved_state_dir = state_dir.resolve()
    _validate_batch_state_dir_layout(
        input_dir=resolved_input_dir,
        state_dir=resolved_state_dir,
    )
    skip_state_tree = _is_path_within(resolved_state_dir, resolved_input_dir)
    json_paths = sorted(
        (
            path
            for path in input_dir.rglob("*.json")
            if path.is_file()
            and not (skip_state_tree and _is_path_within(path.resolve(), resolved_state_dir))
        ),
        key=lambda path: path.relative_to(input_dir).as_posix(),
    )
    if not json_paths:
        raise ValueError("no json files found")

    jobs = []
    for json_path in json_paths:
        relative_json_path = json_path.relative_to(input_dir)
        jobs.append(
            BatchJob(
                json_path=json_path,
                relative_json_path=relative_json_path,
                state_path=state_dir / relative_json_path.with_suffix(".import-state.sqlite3"),
                retry_export_path=derive_batch_retry_export_path(
                    state_dir=state_dir,
                    relative_json_path=relative_json_path,
                ),
            )
        )
    return jobs


def run_batch_import_jobs(*, jobs, json_parallelism: int, run_child_job) -> BatchRunSummary:
    ordered_jobs = list(jobs)
    if json_parallelism < 1:
        raise ValueError("json_parallelism must be >= 1")
    if not ordered_jobs:
        return BatchRunSummary(
            jobs_total=0,
            jobs_succeeded=0,
            jobs_succeeded_with_not_reusable=0,
            jobs_failed=0,
            credential_fatal=False,
            retry_export_paths=[],
        )

    jobs_succeeded = 0
    jobs_succeeded_with_not_reusable = 0
    jobs_failed = 0
    credential_fatal = False
    retry_export_paths: list[Path] = []

    launch_condition = threading.Condition()
    next_launch_index = 0

    def launch(index: int, job: BatchJob):
        nonlocal next_launch_index
        with launch_condition:
            while index != next_launch_index:
                launch_condition.wait()
            next_launch_index += 1
            launch_condition.notify_all()
        return run_child_job(job=job)

    next_job_index = 0
    active_futures: dict[Future, BatchJob] = {}
    with ThreadPoolExecutor(max_workers=json_parallelism) as executor:
        while next_job_index < len(ordered_jobs) and len(active_futures) < json_parallelism:
            future = executor.submit(launch, next_job_index, ordered_jobs[next_job_index])
            active_futures[future] = ordered_jobs[next_job_index]
            next_job_index += 1

        while active_futures:
            done, _ = wait(active_futures, return_when=FIRST_COMPLETED)
            for future in done:
                job = active_futures.pop(future)
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover - defensive boundary
                    safe_print(f"Batch job failed: {job.relative_json_path.as_posix()}: {exc}")
                    result = _BatchChildResult(status="failed", credential_fatal=False)

                status = getattr(result, "status", "failed")
                if bool(getattr(result, "credential_fatal", False)):
                    credential_fatal = True

                retry_export_path = getattr(result, "retry_export_path", None)
                if retry_export_path is not None and retry_export_path not in retry_export_paths:
                    retry_export_paths.append(retry_export_path)

                if status == "completed":
                    jobs_succeeded += 1
                elif status == "completed_with_not_reusable":
                    jobs_succeeded_with_not_reusable += 1
                else:
                    jobs_failed += 1

            while (
                not credential_fatal
                and next_job_index < len(ordered_jobs)
                and len(active_futures) < json_parallelism
            ):
                future = executor.submit(launch, next_job_index, ordered_jobs[next_job_index])
                active_futures[future] = ordered_jobs[next_job_index]
                next_job_index += 1

    return BatchRunSummary(
        jobs_total=len(ordered_jobs),
        jobs_succeeded=jobs_succeeded,
        jobs_succeeded_with_not_reusable=jobs_succeeded_with_not_reusable,
        jobs_failed=jobs_failed,
        credential_fatal=credential_fatal,
        retry_export_paths=retry_export_paths,
    )


def summarize_batch_exit_code(summary: BatchRunSummary) -> int:
    if summary.credential_fatal or summary.jobs_failed > 0:
        return 1
    return 0


def run_batch_import_cli(*, config) -> int:
    try:
        jobs = discover_batch_json_jobs(input_dir=config.input_dir, state_dir=config.state_dir)
        _ensure_batch_state_root(state_dir=config.state_dir)
    except ValueError as exc:
        safe_print(f"Batch startup failed: {exc}")
        return 1

    safe_print(
        f"Batch startup: jobs={len(jobs)} workers_per_job={config.workers} json_parallelism={config.json_parallelism}"
    )

    planned_jobs: list[BatchJob] = []
    planned_files_by_job: dict[BatchJob, list[str]] = {}
    planned_directories_by_job: dict[BatchJob, list[str]] = {}
    preflight_failed_jobs = 0
    for job in jobs:
        state = None
        try:
            state = _open_planned_state_for_job(job=job, config=config)
            planned_files_by_job[job] = _fetch_planned_target_paths(state=state)
            planned_directories_by_job[job] = _fetch_planned_directory_paths(state=state)
            planned_jobs.append(job)
        except Exception as exc:
            preflight_failed_jobs += 1
            safe_print(f"Batch preflight failed: {job.relative_json_path.as_posix()}: {exc}")
        finally:
            if state is not None:
                state.close()

    try:
        validate_batch_target_collisions(planned_files_by_job, planned_directories_by_job)
    except ValueError as exc:
        safe_print(str(exc))
        return 1

    if config.dry_run:
        summary = BatchRunSummary(
            jobs_total=len(jobs),
            jobs_succeeded=len(planned_jobs),
            jobs_succeeded_with_not_reusable=0,
            jobs_failed=preflight_failed_jobs,
            credential_fatal=False,
            retry_export_paths=[],
        )
        safe_print("Dry run: no remote mutations performed")
        return summarize_batch_exit_code(summary)

    if not planned_jobs:
        summary = BatchRunSummary(
            jobs_total=len(jobs),
            jobs_succeeded=0,
            jobs_succeeded_with_not_reusable=0,
            jobs_failed=preflight_failed_jobs,
            credential_fatal=False,
            retry_export_paths=[],
        )
        _print_batch_summary(summary=summary)
        return summarize_batch_exit_code(summary)

    try:
        creds = load_credentials()
        session = build_session(creds)
    except Exception as exc:
        safe_print(str(exc))
        return 1

    try:
        execution_summary = run_batch_import_jobs(
            jobs=planned_jobs,
            json_parallelism=config.json_parallelism,
            run_child_job=lambda job, **kwargs: _run_child_job(
                job=job,
                config=config,
                creds=creds,
                session=session,
                **kwargs,
            ),
        )
        summary = BatchRunSummary(
            jobs_total=len(jobs),
            jobs_succeeded=execution_summary.jobs_succeeded,
            jobs_succeeded_with_not_reusable=execution_summary.jobs_succeeded_with_not_reusable,
            jobs_failed=execution_summary.jobs_failed + preflight_failed_jobs,
            credential_fatal=execution_summary.credential_fatal,
            retry_export_paths=execution_summary.retry_export_paths,
        )
        _print_batch_summary(summary=summary)
        return summarize_batch_exit_code(summary)
    finally:
        close = getattr(session, "close", None)
        if callable(close):
            close()


def _child_status_for_state(*, state, credential_fatal: bool) -> str:
    if credential_fatal or state.stats["failed"] > 0:
        return "failed"
    if state.stats["not_reusable"] > 0:
        return "completed_with_not_reusable"
    return "completed"


def _fetch_planned_target_paths(*, state) -> list[str]:
    common_path = state.job_scope["common_path"].rstrip("/")
    rows = state.connection.execute("SELECT path FROM files ORDER BY path ASC").fetchall()
    paths = []
    for row in rows:
        relative_path = str(row[0])
        if common_path:
            paths.append(f"{common_path}/{relative_path}")
        else:
            paths.append(relative_path)
    return paths


def _fetch_planned_directory_paths(*, state) -> list[str]:
    return state.fetch_folder_keys()


def _iter_parent_paths(path: str):
    for parent in PurePosixPath(path).parents:
        parent_path = parent.as_posix()
        if parent_path in ("", "."):
            break
        yield parent_path


def _is_path_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _validate_batch_state_dir_layout(*, input_dir: Path, state_dir: Path) -> None:
    if _is_path_within(state_dir, input_dir):
        raise ValueError("--state-dir must not be the same as or nested inside --input-dir")


def _open_planned_state_for_job(*, job: BatchJob, config):
    scope = inspect_export_scope(export_path=job.json_path)
    state = open_or_initialize_import_state(
        state_path=job.state_path,
        source_file=str(job.json_path),
        source_sha256=scope.source_sha256,
        target_parent_id=config.target_parent_id,
        common_path=scope.common_path,
    )
    state.workers = config.workers
    rebuild_incomplete_plan_if_needed(
        export_path=job.json_path,
        state=state,
        scope=scope,
    )
    return state


def _print_batch_summary(*, summary: BatchRunSummary) -> None:
    safe_print(
        "Batch summary: "
        f"total={summary.jobs_total} "
        f"completed={summary.jobs_succeeded} "
        f"completed_with_not_reusable={summary.jobs_succeeded_with_not_reusable} "
        f"failed={summary.jobs_failed}"
    )
    for retry_export_path in summary.retry_export_paths:
        safe_print(f"Retry export: {retry_export_path}")


def _ensure_batch_state_root(*, state_dir: Path) -> None:
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ValueError(f"state-root error: {state_dir}: {exc}") from exc


def _run_child_job(*, job: BatchJob, config, creds, session) -> _BatchChildResult:
    state = None
    credential_fatal = False
    retry_export_path: Path | None = None
    status = "failed"
    finalize_failed = False
    try:
        state = _open_planned_state_for_job(job=job, config=config)
        pending_folder_keys = state.fetch_pending_folder_keys()
        if config.retry_failed:
            state.reset_retryable_rows()

        api_client = PanApiClient(host=creds.host, session=session)
        create_remote_directories(
            api_client=api_client,
            state=state,
            folder_keys=pending_folder_keys,
            state_path=job.state_path,
            max_retries=config.max_retries,
        )
        run_summary = run_file_phase_sqlite(
            api_client=api_client,
            state=state,
            max_retries=config.max_retries,
            flush_every=config.flush_every,
        )
        credential_fatal = bool(run_summary["credential_fatal"])
        status = _child_status_for_state(state=state, credential_fatal=credential_fatal)
    except DirectoryPhaseCredentialFatalError as exc:
        credential_fatal = True
        safe_print(f"Batch job failed: {job.relative_json_path.as_posix()}: {exc}")
    except Exception as exc:
        safe_print(f"Batch job failed: {job.relative_json_path.as_posix()}: {exc}")
    finally:
        if state is not None:
            try:
                wrote_retry_export = finalize_import_job(
                    state=state,
                    retry_export_path=job.retry_export_path,
                )
                if wrote_retry_export:
                    retry_export_path = job.retry_export_path
            except Exception as exc:  # pragma: no cover - defensive boundary
                finalize_failed = True
                safe_print(f"Batch finalize failed: {job.relative_json_path.as_posix()}: {exc}")
            finally:
                state.close()

    if finalize_failed:
        status = "failed"
    return _BatchChildResult(
        status=status,
        credential_fatal=credential_fatal,
        retry_export_path=retry_export_path,
    )
