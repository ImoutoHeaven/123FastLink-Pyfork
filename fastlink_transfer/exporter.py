from __future__ import annotations

import errno
import json
import os
import queue
import shutil
import sqlite3
import tempfile
import threading
import time
from dataclasses import replace
from pathlib import Path

from fastlink_transfer import __version__
from fastlink_transfer.api import DecisionKind
from fastlink_transfer.export_state import (
    MISMATCH_ERROR,
    get_export_artifact_paths,
    load_or_initialize_export_state,
    validate_export_state_scope,
    validate_export_output_file,
)
from fastlink_transfer.importer import BASE62_ALPHABET, normalize_relative_path
from fastlink_transfer.runner import compute_backoff, safe_print


class _InvalidSidecarRecord(Exception):
    pass


def _cleanup_stale_finalize_artifacts(state) -> None:
    for path in (state.temp_output_file, state.temp_sqlite_file):
        if path.exists():
            path.unlink()


def _ensure_sidecar_exists(records_file: Path) -> None:
    records_file.parent.mkdir(parents=True, exist_ok=True)
    records_file.touch(exist_ok=True)


def _cleanup_orphan_sidecar(records_file: Path) -> None:
    if records_file.exists():
        records_file.unlink()


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _is_completed_export_state(*, state) -> bool:
    if state.pending_dirs or state.inflight_dirs or not state.completed_dirs:
        return False
    return bool(state.final_output_committed)


def _state_has_committed_sidecar_records(records_file: Path) -> bool:
    if not records_file.exists():
        return False
    for _ in _iter_sidecar_records(records_file):
        return True
    return False


def _should_resume_finalize(*, state) -> bool:
    if state.pending_dirs or state.inflight_dirs:
        return False
    if not state.completed_dirs:
        return False
    if state.final_output_committed:
        return False
    return _state_has_committed_sidecar_records(state.records_file)


def _success_marker_path(*, state_path: Path) -> Path:
    return Path(state_path).resolve().with_suffix(".output.committed")


def _success_marker_scope_payload(*, state_path: Path, output_file: Path, source_host: str, source_parent_id: str, source_root_name: str) -> dict[str, str]:
    artifacts = get_export_artifact_paths(state_path=state_path, output_file=output_file)
    return {
        "source_host": source_host,
        "source_parent_id": source_parent_id,
        "source_root_name": source_root_name,
        "common_path": f"{source_root_name}/",
        "output_file": str(artifacts["output_file"]),
        "records_file": str(artifacts["records_file"]),
    }


def _validate_success_marker_scope(
    *,
    marker_path: Path,
    state_path: Path,
    output_file: Path,
    source_host: str,
    source_parent_id: str,
    source_root_name: str,
) -> bool:
    marker_text = marker_path.read_text(encoding="utf-8")
    try:
        payload = json.loads(marker_text)
    except Exception:
        if marker_text.strip() == "committed":
            return False
        raise ValueError(MISMATCH_ERROR)

    if not isinstance(payload, dict):
        raise ValueError(MISMATCH_ERROR)

    expected_scope = _success_marker_scope_payload(
        state_path=state_path,
        output_file=output_file,
        source_host=source_host,
        source_parent_id=source_parent_id,
        source_root_name=source_root_name,
    )
    for key, value in expected_scope.items():
        if payload.get(key) != value:
            raise ValueError(MISMATCH_ERROR)
    return True


def _remove_stale_success_artifacts(*, state_path: Path, output_file: Path) -> None:
    artifacts = get_export_artifact_paths(state_path=state_path, output_file=output_file)
    for path in (
        artifacts["temp_output_file"],
        artifacts["temp_sqlite_file"],
        artifacts["records_file"],
        artifacts["state_file"],
        artifacts["success_marker_file"],
    ):
        if path.exists():
            path.unlink()


def _get_directory_identity_with_retries(*, api_client, source_parent_id: str, max_retries: int):
    attempts = 0
    while True:
        decision = api_client.get_directory_identity(parent_file_id=source_parent_id)
        if decision.kind == DecisionKind.DIRECTORY_CREATED:
            return decision
        if decision.kind == DecisionKind.CREDENTIAL_FATAL:
            raise RuntimeError(decision.error or "credential failure")
        if decision.kind != DecisionKind.RETRYABLE or attempts >= max_retries:
            raise RuntimeError(decision.error or "failed to validate source directory")
        attempts += 1
        time.sleep(compute_backoff(attempts))


def _load_export_context(*, api_client, config):
    state_path = Path(config.state_file).resolve()
    success_marker = _success_marker_path(state_path=state_path)
    identity = _get_directory_identity_with_retries(
        api_client=api_client,
        source_parent_id=config.source_parent_id,
        max_retries=config.max_retries,
    )

    is_new_state = not state_path.exists()
    if success_marker.exists():
        if state_path.exists():
            validate_export_state_scope(
                state_path=state_path,
                source_host=api_client.host,
                source_parent_id=config.source_parent_id,
                source_root_name=identity.payload["root_name"],
                output_file=config.output_file,
            )
        else:
            structured_marker = _validate_success_marker_scope(
                marker_path=success_marker,
                state_path=state_path,
                output_file=config.output_file,
                source_host=api_client.host,
                source_parent_id=config.source_parent_id,
                source_root_name=identity.payload["root_name"],
            )
            if not structured_marker:
                records_file = get_export_artifact_paths(
                    state_path=state_path,
                    output_file=config.output_file,
                )["records_file"]
                if records_file.exists():
                    raise ValueError(MISMATCH_ERROR)
        _remove_stale_success_artifacts(state_path=state_path, output_file=config.output_file)
        is_new_state = True

    state = load_or_initialize_export_state(
        state_path=state_path,
        source_host=api_client.host,
        source_parent_id=config.source_parent_id,
        source_root_name=identity.payload["root_name"],
        output_file=config.output_file,
        workers=config.workers,
        max_retries=config.max_retries,
        flush_every=config.flush_every,
    )
    prepare_sidecar_for_resume(state.records_file)

    if not is_new_state and not _should_resume_finalize(state=state):
        if not state.pending_dirs and not state.inflight_dirs and state.completed_dirs:
            _remove_stale_success_artifacts(state_path=state_path, output_file=config.output_file)
            is_new_state = True
            state = load_or_initialize_export_state(
                state_path=state_path,
                source_host=api_client.host,
                source_parent_id=config.source_parent_id,
                source_root_name=identity.payload["root_name"],
                output_file=config.output_file,
                workers=config.workers,
                max_retries=config.max_retries,
                flush_every=config.flush_every,
            )

    _cleanup_stale_finalize_artifacts(state)
    _ensure_sidecar_exists(state.records_file)
    if is_new_state:
        try:
            state.flush(state_path)
        except BaseException:
            if not state_path.exists():
                _cleanup_orphan_sidecar(state.records_file)
            raise
    return state


def _flush_export_state_best_effort(*, state, state_path) -> None:
    state_path = Path(state_path).resolve()
    if state_path.exists():
        try:
            persisted_state = load_or_initialize_export_state(
                state_path=state_path,
                source_host=state.source_host,
                source_parent_id=state.source_parent_id,
                source_root_name=state.source_root_name,
                output_file=state.output_file,
                workers=state.workers,
                max_retries=state.max_retries,
                flush_every=state.flush_every,
            )
        except ValueError:
            persisted_state = None
        else:
            if persisted_state.stats != state.stats:
                return
    try:
        state.flush(state_path)
    except BaseException as exc:
        safe_print(f"State flush failed during export interruption: {exc}")


def _load_sidecar_record(raw_line: str) -> dict:
    try:
        payload = json.loads(raw_line)
    except Exception as exc:
        raise _InvalidSidecarRecord("invalid json") from exc

    if not isinstance(payload, dict):
        raise _InvalidSidecarRecord("sidecar record must be an object")

    path = payload.get("path")
    etag_hex = payload.get("etag_hex")
    size = payload.get("size")

    if not isinstance(path, str) or not path:
        raise _InvalidSidecarRecord("sidecar path must be a non-empty string")
    if not isinstance(etag_hex, str) or not etag_hex:
        raise _InvalidSidecarRecord("sidecar etag_hex must be a non-empty string")
    if len(etag_hex) != 32:
        raise _InvalidSidecarRecord("sidecar etag_hex must be exactly 32 hex chars")
    if any(character not in "0123456789abcdefABCDEF" for character in etag_hex):
        raise _InvalidSidecarRecord("sidecar etag_hex must be hex")
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise _InvalidSidecarRecord("sidecar size must be a non-negative integer")

    return {"path": path, "etag_hex": etag_hex.lower(), "size": size}


def prepare_sidecar_for_resume(records_file: Path) -> None:
    if not records_file.exists():
        return
    data = records_file.read_bytes()
    if not data:
        return

    lines = data.splitlines(keepends=True)
    rebuilt: list[bytes] = []
    for index, raw_line in enumerate(lines):
        is_last_line = index == len(lines) - 1
        ends_with_newline = raw_line.endswith(b"\n")
        try:
            decoded_line = raw_line.decode("utf-8")
            _load_sidecar_record(decoded_line)
        except UnicodeDecodeError as exc:
            if is_last_line and not ends_with_newline:
                records_file.write_bytes(
                    b"".join(line if line.endswith(b"\n") else line + b"\n" for line in rebuilt)
                )
                return
            raise RuntimeError("corrupt sidecar") from exc
        except _InvalidSidecarRecord as exc:
            if is_last_line and not ends_with_newline and str(exc) == "invalid json":
                records_file.write_bytes(
                    b"".join(line if line.endswith(b"\n") else line + b"\n" for line in rebuilt)
                )
                return
            raise RuntimeError("corrupt sidecar") from exc
        rebuilt.append(raw_line)

    if rebuilt and not rebuilt[-1].endswith(b"\n"):
        records_file.write_bytes(b"".join(line if line.endswith(b"\n") else line + b"\n" for line in rebuilt))


def _scan_directory_with_retries(*, api_client, folder_id: str, max_retries: int):
    attempts = 0
    while True:
        decision = api_client.get_file_list(parent_file_id=folder_id)
        if decision.kind == DecisionKind.COMPLETED:
            return decision
        if decision.kind == DecisionKind.CREDENTIAL_FATAL:
            raise RuntimeError(decision.error or "credential failure")
        if decision.kind != DecisionKind.RETRYABLE or attempts >= max_retries:
            raise RuntimeError(decision.error or "directory scan failed")
        attempts += 1
        time.sleep(compute_backoff(attempts))


def _build_file_record(*, item, relative_dir: str) -> dict:
    file_name = item["FileName"]
    path = file_name if not relative_dir else f"{relative_dir}/{file_name}"
    return {
        "path": path,
        "etag_hex": _load_sidecar_record(
            json.dumps({"path": path, "etag_hex": item["Etag"], "size": int(item["Size"])})
        )["etag_hex"],
        "size": int(item["Size"]),
    }


def _append_sidecar_records(records_file: Path, records: list[dict]) -> None:
    records_file.parent.mkdir(parents=True, exist_ok=True)
    with records_file.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _iter_sidecar_records(records_file: Path):
    with records_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                try:
                    yield _load_sidecar_record(line)
                except _InvalidSidecarRecord as exc:
                    raise RuntimeError("corrupt sidecar") from exc


def _hex_to_base62(value: str) -> str:
    number = int(value, 16)
    if number == 0:
        return "0"

    chars: list[str] = []
    while number > 0:
        number, remainder = divmod(number, 62)
        chars.append(BASE62_ALPHABET[remainder])
    return "".join(reversed(chars))


def _format_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    unit_index = 0
    while unit_index < len(units) - 1 and value >= 1024.0:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{size} B"
    return f"{value:.2f} {units[unit_index]}"


def _replace_output_file(*, temp_output_file: Path, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(temp_output_file, output_file)
        _fsync_directory(output_file.parent)
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise

    with tempfile.NamedTemporaryFile(
        "wb",
        dir=output_file.parent,
        prefix=f".{output_file.name}.",
        suffix=".tmp",
        delete=False,
    ) as bridge_handle:
        with temp_output_file.open("rb") as source_handle:
            shutil.copyfileobj(source_handle, bridge_handle)
        bridge_handle.flush()
        os.fsync(bridge_handle.fileno())
        bridge_path = Path(bridge_handle.name)
    try:
        os.replace(bridge_path, output_file)
        _fsync_directory(output_file.parent)
    except BaseException:
        if bridge_path.exists():
            bridge_path.unlink()
        raise


def _mark_final_output_committed(*, state, state_path: Path) -> None:
    state.final_output_committed = True
    success_marker = _success_marker_path(state_path=state_path)
    success_marker.parent.mkdir(parents=True, exist_ok=True)
    marker_payload = _success_marker_scope_payload(
        state_path=state_path,
        output_file=state.output_file,
        source_host=state.source_host,
        source_parent_id=state.source_parent_id,
        source_root_name=state.source_root_name,
    )
    with success_marker.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(marker_payload, ensure_ascii=False))
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(success_marker.parent)
    try:
        state.flush(state_path)
    except OSError as exc:
        safe_print(f"Cleanup warning: {state_path}: {exc}")


def finalize_export_json(*, state) -> None:
    if state.temp_sqlite_file.exists():
        state.temp_sqlite_file.unlink()
    if state.temp_output_file.exists():
        state.temp_output_file.unlink()

    connection = sqlite3.connect(state.temp_sqlite_file)
    try:
        connection.execute(
            "CREATE TABLE records (dedupe_key TEXT PRIMARY KEY, path TEXT NOT NULL, etag_hex TEXT NOT NULL, size INTEGER NOT NULL)"
        )
        for record in _iter_sidecar_records(state.records_file):
            raw_path = record["path"]
            etag_hex = str(record["etag_hex"]).lower()
            size = int(record["size"])
            dedupe_key = f"{raw_path}\t{etag_hex}\t{size}"
            connection.execute(
                "INSERT OR IGNORE INTO records (dedupe_key, path, etag_hex, size) VALUES (?, ?, ?, ?)",
                (dedupe_key, raw_path, etag_hex, size),
            )
        connection.commit()

        rows = connection.execute("SELECT path, etag_hex, size FROM records ORDER BY path ASC").fetchall()
        seen_paths: dict[str, tuple[str, int]] = {}
        files = []
        total_size = 0
        for raw_path, etag_hex, size in rows:
            normalized_path = normalize_relative_path(raw_path)
            current = (etag_hex, int(size))
            previous = seen_paths.get(normalized_path)
            if previous is not None and previous != current:
                raise RuntimeError(
                    f"source-tree mutation or inconsistent sidecar content: conflicting normalized path: {normalized_path}"
                )
            if previous is not None:
                continue
            seen_paths[normalized_path] = current
            total_size += int(size)
            files.append(
                {
                    "path": normalized_path,
                    "etag": _hex_to_base62(etag_hex),
                    "size": str(int(size)),
                }
            )

        payload = {
            "scriptVersion": __version__,
            "exportVersion": "1.0",
            "usesBase62EtagsInExport": True,
            "commonPath": state.common_path,
            "totalFilesCount": len(files),
            "totalSize": total_size,
            "formattedTotalSize": _format_size(total_size),
            "files": files,
        }
        state.temp_output_file.parent.mkdir(parents=True, exist_ok=True)
        with state.temp_output_file.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_output_file(temp_output_file=state.temp_output_file, output_file=state.output_file)
    finally:
        connection.close()


def _cleanup_success_artifacts(*, state, state_path) -> None:
    failed_cleanup = False
    for path in (
        state.temp_output_file,
        state.temp_sqlite_file,
        state.records_file,
        Path(state_path).resolve(),
    ):
        try:
            if path.exists():
                path.unlink()
        except OSError as exc:
            failed_cleanup = True
            safe_print(f"Cleanup warning: {path}: {exc}")

    success_marker = _success_marker_path(state_path=Path(state_path).resolve())
    if failed_cleanup:
        return
    try:
        if success_marker.exists():
            success_marker.unlink()
    except OSError as exc:
        safe_print(f"Cleanup warning: {success_marker}: {exc}")


def _shutdown_workers(*, stop_event, claim_queue, threads) -> None:
    stop_event.set()
    for _ in threads:
        claim_queue.put(None)
    for thread in threads:
        thread.join()


def _build_committed_state(*, state, dir_task, child_dirs, file_records):
    pending_dirs = list(state.pending_dirs)
    seen_dir_ids = list(state.seen_dir_ids)
    for child_dir in child_dirs:
        folder_id = child_dir["folder_id"]
        if folder_id not in seen_dir_ids:
            pending_dirs.append(child_dir)
            seen_dir_ids.append(folder_id)

    inflight_dirs = [item for item in state.inflight_dirs if item != dir_task]
    completed_dirs = list(state.completed_dirs) + [dir_task]
    stats = dict(state.stats)
    stats["dirs_completed"] += 1
    stats["files_written"] += len(file_records)
    return replace(
        state,
        pending_dirs=pending_dirs,
        inflight_dirs=inflight_dirs,
        completed_dirs=completed_dirs,
        seen_dir_ids=seen_dir_ids,
        stats=stats,
    )


def _commit_directory_result(*, state, state_path, dir_task, child_dirs, file_records, extra_flush_counter: dict) -> None:
    _append_sidecar_records(state.records_file, file_records)
    committed_state = _build_committed_state(
        state=state,
        dir_task=dir_task,
        child_dirs=child_dirs,
        file_records=file_records,
    )
    type(committed_state).flush(committed_state, state_path)

    state.pending_dirs = committed_state.pending_dirs
    state.inflight_dirs = committed_state.inflight_dirs
    state.completed_dirs = committed_state.completed_dirs
    state.seen_dir_ids = committed_state.seen_dir_ids
    state.stats = committed_state.stats
    state.last_flush_at = committed_state.last_flush_at

    extra_flush_counter["records"] += len(file_records)
    if extra_flush_counter["records"] >= state.flush_every:
        state.flush(state_path)
        extra_flush_counter["records"] = 0


def _run_scan_loop(*, api_client, state, state_path) -> None:
    claim_queue = queue.Queue()
    results_queue = queue.Queue()
    stop_event = threading.Event()
    extra_flush_counter = {"records": 0}
    queued_dirs = []

    def dispatch_available_work() -> None:
        while not stop_event.is_set() and len(queued_dirs) + len(state.inflight_dirs) < state.workers:
            next_task = None
            for dir_task in state.pending_dirs:
                if dir_task in queued_dirs or dir_task in state.inflight_dirs:
                    continue
                next_task = dir_task
                break
            if next_task is None:
                return
            queued_dirs.append(next_task)
            claim_queue.put(next_task)

    def worker() -> None:
        while True:
            dir_task = claim_queue.get()
            if dir_task is None:
                claim_queue.task_done()
                return
            if stop_event.is_set():
                claim_queue.task_done()
                return
            try:
                results_queue.put(("claimed", dir_task))
                decision = _scan_directory_with_retries(
                    api_client=api_client,
                    folder_id=dir_task["folder_id"],
                    max_retries=state.max_retries,
                )
                child_dirs = []
                file_records = []
                for item in decision.payload["items"]:
                    if item.get("Type") == 1:
                        relative_dir = item["FileName"]
                        if dir_task["relative_dir"]:
                            relative_dir = f"{dir_task['relative_dir']}/{relative_dir}"
                        child_dirs.append({"folder_id": str(item["FileId"]), "relative_dir": relative_dir})
                    elif item.get("Type") == 0:
                        file_records.append(_build_file_record(item=item, relative_dir=dir_task["relative_dir"]))
                results_queue.put(("directory", dir_task, child_dirs, file_records))
            except Exception as exc:
                stop_event.set()
                results_queue.put(("error", dir_task, str(exc)))
                return
            finally:
                claim_queue.task_done()

    threads = [threading.Thread(target=worker, daemon=False) for _ in range(state.workers)]
    for thread in threads:
        thread.start()

    try:
        dispatch_available_work()
        pending_results = []
        pending_error = None
        claimed_waiting = []
        while state.pending_dirs or state.inflight_dirs or queued_dirs:
            kind, *payload = results_queue.get()
            if kind == "claimed":
                dir_task = payload[0]
                if dir_task in queued_dirs:
                    queued_dirs.remove(dir_task)
                if dir_task in state.pending_dirs:
                    state.pending_dirs.remove(dir_task)
                if dir_task not in state.inflight_dirs:
                    state.inflight_dirs.append(dir_task)
                claimed_waiting.append(dir_task)
                continue

            if kind == "directory":
                dir_task, child_dirs, file_records = payload
                if dir_task in claimed_waiting:
                    claimed_waiting.remove(dir_task)
                pending_results.append((dir_task, child_dirs, file_records))
            elif kind == "error":
                dir_task, error_message = payload
                if dir_task in claimed_waiting:
                    claimed_waiting.remove(dir_task)
                pending_error = error_message
            else:
                raise RuntimeError(f"unexpected result kind: {kind}")

            if claimed_waiting:
                continue

            if pending_error is not None:
                _shutdown_workers(stop_event=stop_event, claim_queue=claim_queue, threads=threads)
                state.flush(state_path)
                raise RuntimeError(pending_error)

            try:
                for dir_task, child_dirs, file_records in pending_results:
                    _commit_directory_result(
                        state=state,
                        state_path=state_path,
                        dir_task=dir_task,
                        child_dirs=child_dirs,
                        file_records=file_records,
                        extra_flush_counter=extra_flush_counter,
                    )
            except Exception:
                _shutdown_workers(stop_event=stop_event, claim_queue=claim_queue, threads=threads)
                raise

            pending_results = []
            dispatch_available_work()
    except KeyboardInterrupt:
        _shutdown_workers(stop_event=stop_event, claim_queue=claim_queue, threads=threads)
        raise

    _shutdown_workers(stop_event=stop_event, claim_queue=claim_queue, threads=threads)


def run_export_json(*, api_client, config) -> int:
    validate_export_output_file(state_path=config.state_file, output_file=config.output_file)
    state = _load_export_context(api_client=api_client, config=config)
    state_path = Path(config.state_file).resolve()
    try:
        _run_scan_loop(api_client=api_client, state=state, state_path=state_path)
    except KeyboardInterrupt:
        _flush_export_state_best_effort(state=state, state_path=state_path)
        raise
    if state.stats["files_written"] == 0:
        raise RuntimeError("zero files exported")
    finalize_export_json(state=state)
    _mark_final_output_committed(state=state, state_path=state_path)
    _cleanup_success_artifacts(state=state, state_path=state_path)
    return 0
