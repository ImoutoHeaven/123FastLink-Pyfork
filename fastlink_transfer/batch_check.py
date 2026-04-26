from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import json
import os
import queue
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import ijson

from fastlink_transfer import __version__
from fastlink_transfer.api import DecisionKind, PanApiClient
from fastlink_transfer.auth import build_session, load_credentials
from fastlink_transfer.check_state import open_or_initialize_check_state
from fastlink_transfer.exporter import _format_size, _hex_to_base62, _replace_output_file
from fastlink_transfer.import_planner import inspect_export_scope
from fastlink_transfer.importer import (
    normalize_import_record,
    normalize_relative_path,
    parse_size,
    validate_hex_etag,
)
from fastlink_transfer.runner import DirectoryPhaseCredentialFatalError, compute_backoff, safe_print


@dataclass(frozen=True)
class BatchCheckJob:
    json_path: Path
    relative_json_path: Path
    state_path: Path
    output_path: Path


@dataclass(frozen=True)
class RemoteRootResolution:
    remote_root_missing: bool
    resolved_root_id: str | None
    resolved_root_path: str | None
    resolved_prefix_dirs: list[tuple[str, str]]


@dataclass(frozen=True)
class DeltaFinalizeResult:
    delta_files: int
    missing_dirs: int
    wrote_output: bool
    reused_remote_index: bool


@dataclass(frozen=True)
class BatchCheckRunSummary:
    jobs_total: int
    jobs_aligned: int
    jobs_with_delta: int
    jobs_failed: int
    credential_fatal: bool
    delta_files_total: int
    missing_dirs_total: int


@dataclass(frozen=True)
class _BatchCheckChildResult:
    status: str
    credential_fatal: bool
    delta_files: int = 0
    missing_dirs: int = 0


class _BatchCredentialFatalError(RuntimeError):
    pass


class _LazyBatchCheckRemoteAccess:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._creds = None
        self._session = None
        self._load_error: _BatchCredentialFatalError | None = None

    def get(self):
        with self._lock:
            if self._load_error is not None:
                raise self._load_error
            if self._creds is not None and self._session is not None:
                return self._creds, self._session
            try:
                creds = load_credentials()
                session = build_session(creds)
            except Exception as exc:
                self._load_error = _BatchCredentialFatalError(str(exc))
                raise self._load_error from exc
            self._creds = creds
            self._session = session
            return creds, session

    def close(self) -> None:
        with self._lock:
            session = self._session
            self._creds = None
            self._session = None
            self._load_error = None
        close = getattr(session, "close", None)
        if callable(close):
            close()


def discover_batch_check_jobs(*, input_dir: Path, state_dir: Path, output_dir: Path) -> list[BatchCheckJob]:
    resolved_input_dir = input_dir.resolve()
    resolved_state_dir = state_dir.resolve()
    resolved_output_dir = output_dir.resolve()
    _validate_batch_root_layout(
        input_dir=resolved_input_dir,
        state_dir=resolved_state_dir,
        output_dir=resolved_output_dir,
    )

    json_paths = sorted(
        (path for path in input_dir.rglob("*.json") if path.is_file()),
        key=lambda path: path.relative_to(input_dir).as_posix(),
    )
    if not json_paths:
        raise ValueError("no json files found")

    jobs = []
    for json_path in json_paths:
        relative_json_path = json_path.relative_to(input_dir)
        jobs.append(
            BatchCheckJob(
                json_path=json_path,
                relative_json_path=relative_json_path,
                state_path=state_dir / relative_json_path.with_suffix(".check-state.sqlite3"),
                output_path=output_dir / relative_json_path.with_suffix(".delta.export.json"),
            )
        )
    return jobs


def plan_batch_check_job(*, export_path: Path, state) -> None:
    state.reset_incomplete_planning_rows()

    expected_dir_count = 0
    expected_file_count = 0
    common_path = state.job_scope["common_path"]
    uses_base62 = _read_uses_base62_flag(export_path=export_path)

    for target_relative_path in _iter_expected_dir_paths(
        common_path=common_path,
        export_relative_path=None,
    ):
        if state.add_expected_dir(target_relative_path):
            expected_dir_count += 1

    with export_path.open("rb") as handle:
        for file_entry in ijson.items(handle, "files.item"):
            if not isinstance(file_entry, dict):
                raise ValueError("file entries must be objects")

            etag_value = file_entry.get("etag")
            path_value = file_entry.get("path")
            if not isinstance(etag_value, str) or not etag_value:
                raise ValueError("etag must be a non-empty string")
            if not isinstance(path_value, str):
                raise ValueError("path must be a string")

            export_relative_path, etag_hex, size = normalize_import_record(
                path_value=path_value,
                etag_value=etag_value,
                size_value=file_entry.get("size"),
                uses_base62=uses_base62,
            )
            target_relative_path = _to_target_relative_path(
                common_path=common_path,
                export_relative_path=export_relative_path,
            )

            for expected_dir_path in _iter_expected_dir_paths(
                common_path=common_path,
                export_relative_path=export_relative_path,
            ):
                if state.add_expected_dir(expected_dir_path):
                    expected_dir_count += 1

            state.add_expected_file(
                export_relative_path=export_relative_path,
                target_relative_path=target_relative_path,
                etag_hex=etag_hex,
                size=size,
            )
            expected_file_count += 1

    state.finish_planning(expected_files=expected_file_count, expected_dirs=expected_dir_count)


def resolve_remote_common_path(
    *,
    api_client,
    target_parent_id: str,
    common_path: str,
    max_retries: int,
) -> RemoteRootResolution:
    normalized_path = common_path.rstrip("/")
    if not normalized_path:
        return RemoteRootResolution(
            remote_root_missing=False,
            resolved_root_id=target_parent_id,
            resolved_root_path="",
            resolved_prefix_dirs=[],
        )

    parent_file_id = target_parent_id
    current_path = ""
    resolved_prefix_dirs: list[tuple[str, str]] = []
    segments = normalized_path.split("/")
    for index, segment in enumerate(segments):
        current_path = segment if not current_path else f"{current_path}/{segment}"
        matched_dir_id = None
        for items in _iter_directory_pages_with_retries(
            api_client=api_client,
            parent_file_id=parent_file_id,
            max_retries=max_retries,
        ):
            matched_dir_id = _find_remote_directory_id(items=items, directory_name=segment)
            if matched_dir_id is not None:
                break
        if matched_dir_id is None:
            return RemoteRootResolution(
                remote_root_missing=True,
                resolved_root_id=None,
                resolved_root_path=None,
                resolved_prefix_dirs=[],
            )
        if index < len(segments) - 1:
            resolved_prefix_dirs.append((current_path, matched_dir_id))
        parent_file_id = matched_dir_id

    return RemoteRootResolution(
        remote_root_missing=False,
        resolved_root_id=parent_file_id,
        resolved_root_path=current_path,
        resolved_prefix_dirs=resolved_prefix_dirs,
    )


def run_remote_scan(*, api_client, state, state_path: Path, max_retries: int) -> None:
    if state.phase["remote_scan_complete"]:
        return

    state.recover_inflight_remote_dirs()

    if not state.has_remote_index_rows():
        resolution = resolve_remote_common_path(
            api_client=api_client,
            target_parent_id=state.job_scope["target_parent_id"],
            common_path=state.job_scope["common_path"],
            max_retries=max_retries,
        )
        if resolution.remote_root_missing:
            state.finish_remote_scan(remote_root_missing=True)
            return
        state.seed_resolved_remote_dirs(
            completed_dirs=resolution.resolved_prefix_dirs,
            root_dir=(resolution.resolved_root_path or "", resolution.resolved_root_id or ""),
        )

    _run_remote_scan_loop(
        api_client=api_client,
        state=state,
        state_path=state_path,
        max_retries=max_retries,
    )

    state.finish_remote_scan(remote_root_missing=False)


def finalize_delta_export(*, state, output_path: Path, compare_mode: str) -> DeltaFinalizeResult:
    missing_dirs = state.count_missing_expected_dirs()
    reused_remote_index = state.phase["remote_scan_complete"]
    delta_files, total_size = state.summarize_delta_candidate_rows(compare_mode=compare_mode)

    if delta_files:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            _write_delta_payload(
                handle=handle,
                common_path=state.job_scope["common_path"],
                delta_files=delta_files,
                total_size=total_size,
                delta_rows=state.fetch_delta_candidate_rows(compare_mode=compare_mode),
            )
            handle.flush()
            os.fsync(handle.fileno())
            temp_output_path = Path(handle.name)
        _replace_output_file(temp_output_file=temp_output_path, output_file=output_path)
        wrote_output = True
    else:
        if output_path.exists():
            output_path.unlink()
        wrote_output = False

    state.finish_delta_finalize(
        compare_mode=compare_mode,
        delta_files=delta_files,
        missing_dirs=missing_dirs,
    )
    return DeltaFinalizeResult(
        delta_files=delta_files,
        missing_dirs=missing_dirs,
        wrote_output=wrote_output,
        reused_remote_index=reused_remote_index,
    )


def _write_delta_payload(*, handle, common_path: str, delta_files: int, total_size: int, delta_rows) -> None:
    header = {
        "scriptVersion": __version__,
        "exportVersion": "1.0",
        "usesBase62EtagsInExport": True,
        "commonPath": common_path,
        "totalFilesCount": delta_files,
        "totalSize": total_size,
        "formattedTotalSize": _format_size(total_size),
    }
    handle.write("{\n")
    for key, value in header.items():
        handle.write(f"  {json.dumps(key)}: {json.dumps(value, ensure_ascii=False)}")
        handle.write(",\n")
    handle.write('  "files": [\n')

    first = True
    for row in delta_rows:
        entry = {
            "path": str(row["export_relative_path"]),
            "etag": _hex_to_base62(str(row["etag_hex"])),
            "size": str(int(row["size"])),
        }
        if not first:
            handle.write(",\n")
        handle.write(f"    {json.dumps(entry, ensure_ascii=False)}")
        first = False

    handle.write("\n  ]\n}")


def run_batch_check_jobs(*, jobs, json_parallelism: int, run_child_job) -> BatchCheckRunSummary:
    ordered_jobs = list(jobs)
    if json_parallelism < 1:
        raise ValueError("json_parallelism must be >= 1")
    if not ordered_jobs:
        return BatchCheckRunSummary(
            jobs_total=0,
            jobs_aligned=0,
            jobs_with_delta=0,
            jobs_failed=0,
            credential_fatal=False,
            delta_files_total=0,
            missing_dirs_total=0,
        )

    jobs_aligned = 0
    jobs_with_delta = 0
    jobs_failed = 0
    credential_fatal = False
    delta_files_total = 0
    missing_dirs_total = 0

    launch_condition = threading.Condition()
    next_launch_index = 0

    def launch(index: int, job: BatchCheckJob):
        nonlocal next_launch_index
        with launch_condition:
            while index != next_launch_index:
                launch_condition.wait()
            next_launch_index += 1
            launch_condition.notify_all()
        return run_child_job(job=job)

    next_job_index = 0
    active_futures: dict[Future, BatchCheckJob] = {}
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
                    result = _BatchCheckChildResult(status="failed", credential_fatal=False)

                status = getattr(result, "status", "failed")
                if bool(getattr(result, "credential_fatal", False)):
                    credential_fatal = True

                delta_files = int(getattr(result, "delta_files", 0))
                missing_dirs = int(getattr(result, "missing_dirs", 0))

                if status == "aligned":
                    jobs_aligned += 1
                    delta_files_total += delta_files
                    missing_dirs_total += missing_dirs
                elif status == "with_delta":
                    jobs_with_delta += 1
                    delta_files_total += delta_files
                    missing_dirs_total += missing_dirs
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

    return BatchCheckRunSummary(
        jobs_total=len(ordered_jobs),
        jobs_aligned=jobs_aligned,
        jobs_with_delta=jobs_with_delta,
        jobs_failed=jobs_failed,
        credential_fatal=credential_fatal,
        delta_files_total=delta_files_total,
        missing_dirs_total=missing_dirs_total,
    )


def run_batch_check_cli(*, config) -> int:
    try:
        jobs = discover_batch_check_jobs(
            input_dir=config.input_dir,
            state_dir=config.state_dir,
            output_dir=config.output_dir,
        )
        _ensure_batch_root(path=config.state_dir, label="state-root")
        _ensure_batch_root(path=config.output_dir, label="output-root")
    except ValueError as exc:
        safe_print(f"Batch startup failed: {exc}")
        return 1

    safe_print(
        f"Batch startup: jobs={len(jobs)} workers_per_job={config.workers} json_parallelism={config.json_parallelism}"
    )
    remote_access = _LazyBatchCheckRemoteAccess()
    try:
        summary = run_batch_check_jobs(
            jobs=jobs,
            json_parallelism=config.json_parallelism,
            run_child_job=lambda job, **kwargs: _run_batch_check_child_job(
                job=job,
                config=config,
                remote_access=remote_access,
                **kwargs,
            ),
        )
        _print_batch_check_summary(summary=summary)
        return _summarize_batch_check_exit_code(summary=summary)
    finally:
        remote_access.close()


def _to_target_relative_path(*, common_path: str, export_relative_path: str) -> str:
    prefix = common_path.rstrip("/")
    if not prefix:
        return export_relative_path
    return f"{prefix}/{export_relative_path}"


def _to_export_relative_path(*, common_path: str, target_relative_path: str) -> str:
    prefix = common_path.rstrip("/")
    if not prefix:
        return target_relative_path
    return target_relative_path.removeprefix(f"{prefix}/")


def _normalize_remote_child_path(*, parent_target_relative_path: str, item_name: str) -> str:
    normalized_name = normalize_relative_path(item_name)
    if not parent_target_relative_path:
        return normalized_name
    return normalize_relative_path(f"{parent_target_relative_path}/{normalized_name}")


def _read_uses_base62_flag(*, export_path: Path) -> bool:
    uses_base62 = False
    with export_path.open("rb") as handle:
        for prefix, event, value in ijson.parse(handle):
            if prefix == "usesBase62EtagsInExport":
                if event != "boolean":
                    raise ValueError("usesBase62EtagsInExport must be boolean when present")
                uses_base62 = bool(value)
    return uses_base62


def _iter_expected_dir_paths(*, common_path: str, export_relative_path: str | None) -> list[str]:
    expected_dirs: list[str] = []
    common_segments = common_path.rstrip("/").split("/") if common_path else []
    for index in range(len(common_segments)):
        expected_dirs.append("/".join(common_segments[: index + 1]))

    if not export_relative_path or "/" not in export_relative_path:
        return expected_dirs

    parent_segments = export_relative_path.split("/")[:-1]
    for index in range(len(parent_segments)):
        target_segments = [*common_segments, *parent_segments[: index + 1]]
        expected_dirs.append("/".join(target_segments))
    return expected_dirs


def _get_directory_items_with_retries(*, api_client, parent_file_id: str, max_retries: int) -> list[dict]:
    attempts = 0
    while True:
        decision = api_client.get_file_list(parent_file_id=parent_file_id)
        if decision.kind == DecisionKind.COMPLETED:
            payload = decision.payload if isinstance(decision.payload, dict) else None
            items = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items, list):
                raise RuntimeError("directory scan failed")
            return items
        if decision.kind == DecisionKind.CREDENTIAL_FATAL:
            raise DirectoryPhaseCredentialFatalError(decision.error or "credential failure")
        if decision.kind != DecisionKind.RETRYABLE or attempts >= max_retries:
            raise RuntimeError(decision.error or "directory scan failed")
        attempts += 1
        time.sleep(compute_backoff(attempts))


def _iter_directory_pages_with_retries(*, api_client, parent_file_id: str, max_retries: int):
    if hasattr(api_client, "get_file_list_page"):
        page = 1
        page_count = None
        while page_count is None or page <= page_count:
            attempts = 0
            while True:
                decision = api_client.get_file_list_page(parent_file_id=parent_file_id, page=page)
                if decision.kind == DecisionKind.COMPLETED:
                    payload = decision.payload if isinstance(decision.payload, dict) else None
                    items = payload.get("items") if isinstance(payload, dict) else None
                    total = payload.get("total") if isinstance(payload, dict) else None
                    if not isinstance(items, list) or not isinstance(total, int):
                        raise RuntimeError("directory scan failed")
                    if page_count is None:
                        page_count = (total + 99) // 100
                    yield items
                    page += 1
                    break
                if decision.kind == DecisionKind.CREDENTIAL_FATAL:
                    raise DirectoryPhaseCredentialFatalError(decision.error or "credential failure")
                if decision.kind != DecisionKind.RETRYABLE or attempts >= max_retries:
                    raise RuntimeError(decision.error or "directory scan failed")
                attempts += 1
                time.sleep(compute_backoff(attempts))
        return

    if not hasattr(api_client, "iter_file_list_pages"):
        yield _get_directory_items_with_retries(
            api_client=api_client,
            parent_file_id=parent_file_id,
            max_retries=max_retries,
        )
        return

    for decision in api_client.iter_file_list_pages(parent_file_id=parent_file_id):
        if decision.kind == DecisionKind.COMPLETED:
            payload = decision.payload if isinstance(decision.payload, dict) else None
            items = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items, list):
                raise RuntimeError("directory scan failed")
            yield items
            continue
        if decision.kind == DecisionKind.CREDENTIAL_FATAL:
            raise DirectoryPhaseCredentialFatalError(decision.error or "credential failure")
        raise RuntimeError(decision.error or "directory scan failed")


def _find_remote_directory_id(*, items: list[dict], directory_name: str) -> str | None:
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("Type") != 1 or item.get("FileName") != directory_name:
            continue
        file_id = item.get("FileId")
        if file_id is not None:
            return str(file_id)
    return None


def _scan_remote_directory(
    *,
    api_client,
    common_path: str,
    target_relative_path: str,
    remote_dir_id: str,
    max_retries: int,
 ):
    for items in _iter_directory_pages_with_retries(
        api_client=api_client,
        parent_file_id=remote_dir_id,
        max_retries=max_retries,
    ):
        child_dirs: list[tuple[str, str]] = []
        file_rows: list[tuple[str, str, str, int]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            item_type = item.get("Type")
            item_name = item.get("FileName")
            if not isinstance(item_name, str) or not item_name:
                continue
            child_target_relative_path = _normalize_remote_child_path(
                parent_target_relative_path=target_relative_path,
                item_name=item_name,
            )
            if item_type == 1:
                file_id = item.get("FileId")
                if file_id is None:
                    continue
                child_dirs.append((child_target_relative_path, str(file_id)))
                continue
            if item_type != 0:
                continue
            etag_value = item.get("Etag")
            size_value = item.get("Size")
            file_rows.append(
                (
                    _to_export_relative_path(
                        common_path=common_path,
                        target_relative_path=child_target_relative_path,
                    ),
                    child_target_relative_path,
                    validate_hex_etag(str(etag_value)),
                    parse_size(size_value),
                )
            )
        yield child_dirs, file_rows


def _run_remote_scan_loop(*, api_client, state, state_path: Path, max_retries: int) -> None:
    result_queue: queue.Queue = queue.Queue()
    stop_event = threading.Event()
    flush_counter = {"directories": 0}
    queued_paths: set[str] = set()
    active_paths: set[str] = set()
    worker_limit = max(int(state.workers), 1)

    def dispatch_available_work(work_queue: queue.Queue) -> None:
        while not stop_event.is_set() and len(active_paths) + len(queued_paths) < worker_limit:
            next_row = state.fetch_next_pending_remote_dir(
                exclude_paths=queued_paths | active_paths
            )
            if next_row is None:
                return
            path_value = str(next_row[0])
            queued_paths.add(path_value)
            work_queue.put((path_value, str(next_row[1])))

    def worker(work_queue: queue.Queue) -> None:
        while True:
            work_item = work_queue.get()
            if work_item is None:
                work_queue.task_done()
                return
            target_relative_path, remote_dir_id = work_item
            if stop_event.is_set():
                work_queue.task_done()
                return
            try:
                result_queue.put(("claimed", target_relative_path, None))
                for child_dirs, file_rows in _scan_remote_directory(
                    api_client=api_client,
                    common_path=state.job_scope["common_path"],
                    target_relative_path=target_relative_path,
                    remote_dir_id=remote_dir_id,
                    max_retries=max_retries,
                ):
                    result_queue.put(("page", target_relative_path, child_dirs, file_rows))
                result_queue.put(("completed", target_relative_path, None))
            except BaseException as exc:
                stop_event.set()
                result_queue.put(("error", target_relative_path, exc))
                return
            finally:
                work_queue.task_done()

    work_queue: queue.Queue = queue.Queue()
    threads = [threading.Thread(target=worker, args=(work_queue,), daemon=False) for _ in range(worker_limit)]
    for thread in threads:
        thread.start()

    dispatch_available_work(work_queue)
    try:
        while state.has_pending_remote_dirs() or active_paths or queued_paths:
            event, *payload = result_queue.get()
            if event == "claimed":
                target_relative_path = str(payload[0])
                queued_paths.discard(target_relative_path)
                active_paths.add(target_relative_path)
                state.mark_remote_dir_inflight(target_relative_path)
                continue
            if event == "error":
                exc = payload[1]
                stop_event.set()
                for _ in threads:
                    work_queue.put(None)
                for thread in threads:
                    thread.join()
                raise exc

            if event == "page":
                target_relative_path, child_dirs, file_rows = payload
                state.commit_remote_scan_directory(
                    target_relative_path=str(target_relative_path),
                    child_dirs=child_dirs,
                    file_rows=file_rows,
                    mark_complete=False,
                )
                flush_counter["directories"] += 1
                if flush_counter["directories"] >= max(int(getattr(state, "flush_every", 1)), 1):
                    state.flush_remote_progress(state_path=state_path)
                    flush_counter["directories"] = 0
                continue

            target_relative_path = str(payload[0])
            active_paths.discard(target_relative_path)
            state.commit_remote_scan_directory(
                target_relative_path=target_relative_path,
                child_dirs=[],
                file_rows=[],
                mark_complete=True,
            )
            dispatch_available_work(work_queue)
    finally:
        stop_event.set()
        for _ in threads:
            work_queue.put(None)
        for thread in threads:
            thread.join()

    if flush_counter["directories"]:
        state.flush_remote_progress(state_path=state_path)


def _is_path_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _validate_batch_root_layout(*, input_dir: Path, state_dir: Path, output_dir: Path) -> None:
    if _is_path_within(state_dir, input_dir):
        raise ValueError("--state-dir must not be the same as or nested inside --input-dir")
    if _is_path_within(output_dir, input_dir):
        raise ValueError("--output-dir must not be the same as or nested inside --input-dir")


def _ensure_batch_root(*, path: Path, label: str) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ValueError(f"{label} error: {path}: {exc}") from exc


def _open_batch_check_state_for_job(*, job: BatchCheckJob, config):
    scope = inspect_export_scope(export_path=job.json_path)
    state = open_or_initialize_check_state(
        state_path=job.state_path,
        source_file=str(job.json_path),
        source_sha256=scope.source_sha256,
        target_parent_id=config.target_parent_id,
        common_path=scope.common_path,
    )
    state.workers = config.workers
    state.flush_every = config.flush_every
    if not state.phase["planning_complete"]:
        plan_batch_check_job(export_path=job.json_path, state=state)
    return state


def _run_batch_check_child_job(*, job: BatchCheckJob, config, remote_access) -> _BatchCheckChildResult:
    state = None
    try:
        state = _open_batch_check_state_for_job(job=job, config=config)
        if not state.phase["remote_scan_complete"]:
            creds, session = remote_access.get()
            api_client = PanApiClient(host=creds.host, session=session)
            run_remote_scan(
                api_client=api_client,
                state=state,
                state_path=job.state_path,
                max_retries=config.max_retries,
            )
        finalize_result = finalize_delta_export(
            state=state,
            output_path=job.output_path,
            compare_mode=config.compare_mode,
        )
        if finalize_result.delta_files > 0:
            status = "with_delta"
            outcome_label = "delta produced"
        else:
            status = "aligned"
            outcome_label = "aligned"
        safe_print(
            f"Batch job completed: {job.relative_json_path.as_posix()}: {outcome_label} "
            f"delta_files={finalize_result.delta_files} missing_dirs={finalize_result.missing_dirs}"
        )
        return _BatchCheckChildResult(
            status=status,
            credential_fatal=False,
            delta_files=finalize_result.delta_files,
            missing_dirs=finalize_result.missing_dirs,
        )
    except (DirectoryPhaseCredentialFatalError, _BatchCredentialFatalError) as exc:
        safe_print(f"Batch job failed: {job.relative_json_path.as_posix()}: {exc}")
        return _BatchCheckChildResult(status="failed", credential_fatal=True)
    except Exception as exc:
        safe_print(f"Batch job failed: {job.relative_json_path.as_posix()}: {exc}")
        return _BatchCheckChildResult(status="failed", credential_fatal=False)
    finally:
        if state is not None:
            state.close()


def _print_batch_check_summary(*, summary: BatchCheckRunSummary) -> None:
    safe_print(
        "Batch summary: "
        f"total={summary.jobs_total} "
        f"aligned={summary.jobs_aligned} "
        f"with_delta={summary.jobs_with_delta} "
        f"failed={summary.jobs_failed} "
        f"delta_files_total={summary.delta_files_total} "
        f"missing_dirs_total={summary.missing_dirs_total}"
    )


def _summarize_batch_check_exit_code(*, summary: BatchCheckRunSummary) -> int:
    if summary.credential_fatal or summary.jobs_failed > 0:
        return 1
    return 0
