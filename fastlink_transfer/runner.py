from __future__ import annotations

import inspect
import random
import queue
import sqlite3
import threading
import time
from pathlib import Path

from fastlink_transfer.api import DecisionKind, PanApiClient


def safe_print(message: str) -> None:
    try:
        print(message)
    except Exception:
        return


class DirectoryPhaseCredentialFatalError(RuntimeError):
    pass


def create_remote_directories(*, api_client, state, folder_keys: list[str], state_path, max_retries: int = 0) -> None:
    for folder_key in folder_keys:
        if folder_key in state.folder_map:
            continue
        parent_key = folder_key.rsplit("/", 1)[0] if "/" in folder_key else ""
        folder_name = folder_key.split("/")[-1]
        parent_file_id = state.folder_map[parent_key]
        attempts = 0
        while True:
            safe_print(f"Directory progress: creating={folder_key}")
            decision = api_client.mkdir(parent_file_id=parent_file_id, folder_name=folder_name)
            if decision.kind == DecisionKind.DIRECTORY_CREATED:
                state.folder_map[folder_key] = decision.file_id
                state.flush(state_path)
                safe_print(f"State flush: folder_map_size={len(state.folder_map)}")
                break
            if decision.kind == DecisionKind.CREDENTIAL_FATAL:
                raise DirectoryPhaseCredentialFatalError(
                    f"directory creation failed: {folder_key}: {decision.error}"
                )
            if decision.kind != DecisionKind.RETRYABLE or attempts >= max_retries:
                raise RuntimeError(f"directory creation failed: {folder_key}: {decision.error}")
            attempts += 1
            time.sleep(compute_backoff(attempts))


def compute_backoff(attempt: int) -> float:
    return min(30.0, float(2**attempt)) + random.uniform(0.0, 0.5)


class StopController:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._claim_lock = threading.Lock()
        self._active_uploads = 0
        self._pause_until = 0.0
        self.credential_fatal = False

    def start_shutdown(self, *, credential_fatal: bool = False) -> None:
        with self._claim_lock:
            if credential_fatal:
                self.credential_fatal = True
            self._event.set()

    def shutdown_started(self) -> bool:
        return self._event.is_set()

    def pause_new_work(self, seconds: float) -> None:
        with self._claim_lock:
            self._pause_until = max(self._pause_until, time.time() + seconds)

    def _wait_for_work_start_window_locked(self) -> bool:
        while True:
            if self._event.is_set():
                return False
            delay = self._pause_until - time.time()
            if delay <= 0:
                return True
            self._claim_lock.release()
            try:
                if self._event.wait(min(delay, 0.05)):
                    return False
            finally:
                self._claim_lock.acquire()

    def claim_next_work_item(self, work_queue: queue.Queue):
        with self._claim_lock:
            if not self._wait_for_work_start_window_locked():
                return None
            return work_queue.get_nowait()

    def begin_upload_attempt(self) -> bool:
        with self._claim_lock:
            if not self._wait_for_work_start_window_locked():
                return False
            self._active_uploads += 1
            return True

    def finish_upload_attempt(self) -> None:
        with self._claim_lock:
            if self._active_uploads > 0:
                self._active_uploads -= 1

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


class GlobalPause:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sleep_until = 0.0

    def pause(self, seconds: float) -> None:
        with self._lock:
            self._sleep_until = max(self._sleep_until, time.time() + seconds)

    def wait(self, *, stop_controller: StopController | None = None) -> bool:
        while True:
            if stop_controller is not None and stop_controller.shutdown_started():
                return False
            with self._lock:
                delay = self._sleep_until - time.time()
            if delay <= 0:
                return True
            if stop_controller is not None:
                if stop_controller.wait(min(delay, 0.5)):
                    return False
                continue
            time.sleep(min(delay, 0.5))


def process_record(
    *,
    api_client,
    state,
    record,
    parent_file_id: str,
    max_retries: int,
    stop_controller: StopController,
    global_pause,
    on_terminal_outcome=None,
) -> str:
    attempts = 0
    upload_started = False

    def start_upload_attempt() -> None:
        nonlocal upload_started
        if not stop_controller.begin_upload_attempt():
            raise InterruptedError
        upload_started = True

    def invoke_rapid_upload():
        if isinstance(api_client, PanApiClient):
            return api_client.rapid_upload(
                etag=record.etag,
                size=record.size,
                file_name=record.file_name,
                parent_file_id=parent_file_id,
                before_request=start_upload_attempt,
            )
        parameters = inspect.signature(api_client.rapid_upload).parameters
        if "before_request" in parameters:
            return api_client.rapid_upload(
                etag=record.etag,
                size=record.size,
                file_name=record.file_name,
                parent_file_id=parent_file_id,
                before_request=start_upload_attempt,
            )
        start_upload_attempt()
        return api_client.rapid_upload(
            etag=record.etag,
            size=record.size,
            file_name=record.file_name,
            parent_file_id=parent_file_id,
        )

    def apply_terminal_outcome(result: str, *, error: str | None = None, retries: int = attempts) -> None:
        if on_terminal_outcome is not None:
            on_terminal_outcome(result, record=record, error=error, retries=retries)
            return
        if result == "completed":
            state.record_completed(record.key)
            return
        if result == "not_reusable":
            state.record_not_reusable(
                key=record.key,
                path=record.path,
                error=error or "Reuse=false",
            )
            return
        if result == "failed":
            state.record_failed(
                key=record.key,
                path=record.path,
                error=error or "failed",
                retries=retries,
            )

    while True:
        if stop_controller.shutdown_started():
            return "deferred"
        if not global_pause.wait(stop_controller=stop_controller):
            return "deferred"
        if stop_controller.shutdown_started():
            return "deferred"

        try:
            decision = invoke_rapid_upload()
        except InterruptedError:
            return "deferred"
        finally:
            if upload_started:
                stop_controller.finish_upload_attempt()
                upload_started = False
        if decision.kind == DecisionKind.COMPLETED:
            apply_terminal_outcome("completed")
            return "completed"
        if decision.kind == DecisionKind.NOT_REUSABLE:
            apply_terminal_outcome("not_reusable", error=decision.error or "Reuse=false")
            return "not_reusable"
        if decision.kind == DecisionKind.CREDENTIAL_FATAL:
            stop_controller.start_shutdown(credential_fatal=True)
            return "credential_fatal"
        if decision.kind == DecisionKind.FAILED:
            apply_terminal_outcome("failed", error=decision.error or "failed", retries=attempts)
            return "failed"

        if stop_controller.shutdown_started():
            return "deferred"
        if attempts >= max_retries:
            apply_terminal_outcome("failed", error=decision.error or "retry exhausted", retries=attempts)
            return "failed"

        attempts += 1
        if decision.error and "429" in decision.error:
            pause_seconds = compute_backoff(attempts)
            stop_controller.pause_new_work(pause_seconds)
            global_pause.pause(pause_seconds)
        else:
            if stop_controller.wait(compute_backoff(attempts)):
                return "deferred"
        if stop_controller.shutdown_started():
            return "deferred"


def _resolve_parent_key(*, common_path: str, relative_parent_dir: str) -> str:
    if common_path and relative_parent_dir:
        return f"{common_path.rstrip('/')}/{relative_parent_dir}".strip("/")
    if common_path:
        return common_path.rstrip("/")
    return relative_parent_dir


def _add_flush_failure_note(primary_exc: BaseException, *, note_prefix: str, flush_error: BaseException | None) -> None:
    if flush_error is None:
        return
    primary_exc.add_note(f"{note_prefix}: {flush_error!r}")


def _flush_state_best_effort(*, state, state_path, terminal_lock, success_message: str) -> BaseException | None:
    with terminal_lock:
        try:
            state.flush(state_path)
            safe_print(success_message)
        except BaseException as exc:
            return exc
    return None


def run_file_phase(*, api_client, state, records, state_path, max_retries: int, flush_every: int) -> dict[str, int | bool]:
    stop_controller = StopController()
    global_pause = GlobalPause()
    work_queue: queue.Queue = queue.Queue()
    processed_count = 0
    terminal_lock = threading.Lock()
    exception_lock = threading.Lock()
    worker_exception: Exception | None = None

    for record in records:
        parent_key = _resolve_parent_key(
            common_path=state.common_path,
            relative_parent_dir=record.relative_parent_dir,
        )
        parent_file_id = state.folder_map[parent_key] if parent_key else state.folder_map[""]
        work_queue.put((record, parent_file_id))

    def record_terminal_outcome(result: str, *, record, error: str | None, retries: int) -> None:
        nonlocal processed_count
        with terminal_lock:
            if result == "completed":
                state.record_completed(record.key)
            elif result == "not_reusable":
                state.record_not_reusable(
                    key=record.key,
                    path=record.path,
                    error=error or "Reuse=false",
                )
            elif result == "failed":
                state.record_failed(
                    key=record.key,
                    path=record.path,
                    error=error or "failed",
                    retries=retries,
                )
            else:
                raise RuntimeError(f"unexpected terminal outcome: {result}")

            processed_count += 1
            safe_print(
                f"File progress: completed={state.stats['completed']} "
                f"not_reusable={state.stats['not_reusable']} failed={state.stats['failed']}"
            )
            if processed_count % flush_every == 0:
                state.flush(state_path)
                safe_print(f"State flush: terminal_outcomes={processed_count}")

    def worker() -> None:
        nonlocal worker_exception
        while True:
            got_work_item = False
            try:
                if stop_controller.shutdown_started():
                    return
                if not global_pause.wait(stop_controller=stop_controller):
                    return
                if stop_controller.shutdown_started():
                    return
                claimed_item = stop_controller.claim_next_work_item(work_queue)
                if claimed_item is None:
                    return
                record, parent_file_id = claimed_item
                got_work_item = True
                process_record(
                    api_client=api_client,
                    state=state,
                    record=record,
                    parent_file_id=parent_file_id,
                    max_retries=max_retries,
                    stop_controller=stop_controller,
                    global_pause=global_pause,
                    on_terminal_outcome=record_terminal_outcome,
                )
            except queue.Empty:
                return
            except Exception as exc:
                with exception_lock:
                    if worker_exception is None:
                        worker_exception = exc
                stop_controller.start_shutdown()
                return
            finally:
                if got_work_item:
                    work_queue.task_done()

    threads = [threading.Thread(target=worker, daemon=False) for _ in range(state.workers)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    except KeyboardInterrupt as exc:
        stop_controller.start_shutdown()
        for thread in threads:
            if thread.is_alive():
                thread.join()
        flush_error = _flush_state_best_effort(
            state=state,
            state_path=state_path,
            terminal_lock=terminal_lock,
            success_message="State flush: keyboard interrupt",
        )
        _add_flush_failure_note(
            exc,
            note_prefix="Best-effort keyboard interrupt flush failed",
            flush_error=flush_error,
        )
        raise

    if worker_exception is not None:
        flush_error = _flush_state_best_effort(
            state=state,
            state_path=state_path,
            terminal_lock=terminal_lock,
            success_message=f"State flush: worker exception terminal_outcomes={processed_count}",
        )
        _add_flush_failure_note(
            worker_exception,
            note_prefix="Best-effort final flush failed",
            flush_error=flush_error,
        )
        raise worker_exception

    try:
        with terminal_lock:
            state.flush(state_path)
            safe_print(f"State flush: final terminal_outcomes={processed_count}")
    except KeyboardInterrupt as exc:
        flush_error = _flush_state_best_effort(
            state=state,
            state_path=state_path,
            terminal_lock=terminal_lock,
            success_message="State flush: keyboard interrupt",
        )
        _add_flush_failure_note(
            exc,
            note_prefix="Best-effort keyboard interrupt flush failed",
            flush_error=flush_error,
        )
        raise
    return {"processed": processed_count, "credential_fatal": stop_controller.credential_fatal}


def finalize_import_job(*, state, retry_export_path: Path) -> bool:
    return state.write_retry_export(retry_export_path)


def run_file_phase_sqlite(*, api_client, state, max_retries: int, flush_every: int) -> dict[str, int | bool]:
    stop_controller = StopController()
    global_pause = GlobalPause()
    batch_size = max(flush_every, state.workers, 1)
    work_queue: queue.Queue = queue.Queue(maxsize=batch_size)
    outcome_queue: queue.Queue = queue.Queue()
    processed_count = 0
    processed_count_lock = threading.Lock()
    terminal_lock = threading.Lock()
    exception_lock = threading.Lock()
    worker_exception: Exception | None = None
    producer_done = threading.Event()
    workers_done = threading.Event()

    def flush_outcomes(outcomes: list[dict[str, str | int | None]], *, is_final: bool) -> None:
        nonlocal processed_count
        if outcomes:
            with terminal_lock:
                state.flush_terminal_outcomes(outcomes)
                with processed_count_lock:
                    processed_count += len(outcomes)
                    current_count = processed_count
                safe_print(
                    f"File progress: completed={state.stats['completed']} "
                    f"not_reusable={state.stats['not_reusable']} failed={state.stats['failed']}"
                )
                if not is_final:
                    safe_print(f"State flush: terminal_outcomes={current_count}")
        if is_final:
            with terminal_lock:
                state.flush(state.state_path)
                with processed_count_lock:
                    current_count = processed_count
                safe_print(f"State flush: final terminal_outcomes={current_count}")

    def producer() -> None:
        last_path: str | None = None
        read_connection: sqlite3.Connection | None = None
        try:
            read_connection = sqlite3.connect(state.state_path)
            while True:
                query = (
                    "SELECT record_key, etag_hex, size, path, file_name, relative_parent_dir "
                    "FROM files WHERE status = 'pending'"
                )
                parameters: tuple[object, ...]
                if last_path is None:
                    query += " ORDER BY path ASC LIMIT ?"
                    parameters = (batch_size,)
                else:
                    query += " AND path > ? ORDER BY path ASC LIMIT ?"
                    parameters = (last_path, batch_size)
                rows = read_connection.execute(query, parameters).fetchall()
                if not rows:
                    return
                for record_key, etag_hex, size, path, file_name, relative_parent_dir in rows:
                    record = type("Record", (), {})()
                    record.key = str(record_key)
                    record.etag = str(etag_hex)
                    record.size = int(size)
                    record.path = str(path)
                    record.file_name = str(file_name)
                    record.relative_parent_dir = str(relative_parent_dir)
                    parent_file_id = state.resolve_parent_file_id(
                        relative_parent_dir=record.relative_parent_dir
                    )
                    while True:
                        if stop_controller.shutdown_started():
                            return
                        try:
                            work_queue.put((record, parent_file_id), timeout=0.05)
                            break
                        except queue.Full:
                            continue
                last_path = str(rows[-1][3])
        except Exception as exc:
            nonlocal worker_exception
            with exception_lock:
                if worker_exception is None:
                    worker_exception = exc
            stop_controller.start_shutdown()
        finally:
            if read_connection is not None:
                read_connection.close()
            producer_done.set()

    def record_terminal_outcome(result: str, *, record, error: str | None, retries: int) -> None:
        outcome_queue.put(
            {
                "result": result,
                "record_key": record.key,
                "error": error,
                "retries": retries,
            }
        )

    def worker() -> None:
        nonlocal worker_exception
        while True:
            got_work_item = False
            try:
                if stop_controller.shutdown_started():
                    return
                if not global_pause.wait(stop_controller=stop_controller):
                    if stop_controller.shutdown_started():
                        return
                if stop_controller.shutdown_started():
                    return
                claimed_item = stop_controller.claim_next_work_item(work_queue)
                if claimed_item is None:
                    if stop_controller.shutdown_started() or producer_done.is_set():
                        return
                    continue
                record, parent_file_id = claimed_item
                got_work_item = True
                process_record(
                    api_client=api_client,
                    state=state,
                    record=record,
                    parent_file_id=parent_file_id,
                    max_retries=max_retries,
                    stop_controller=stop_controller,
                    global_pause=global_pause,
                    on_terminal_outcome=record_terminal_outcome,
                )
            except queue.Empty:
                if producer_done.is_set():
                    return
                continue
            except Exception as exc:
                with exception_lock:
                    if worker_exception is None:
                        worker_exception = exc
                stop_controller.start_shutdown()
                return
            finally:
                if got_work_item:
                    work_queue.task_done()

    def writer() -> None:
        nonlocal worker_exception
        pending_outcomes: list[dict[str, str | int | None]] = []
        try:
            while True:
                try:
                    outcome = outcome_queue.get(timeout=0.05)
                except queue.Empty:
                    if producer_done.is_set() and workers_done.is_set() and outcome_queue.empty():
                        break
                    continue
                pending_outcomes.append(outcome)
                outcome_queue.task_done()
                if len(pending_outcomes) >= flush_every:
                    flush_outcomes(pending_outcomes, is_final=False)
                    pending_outcomes = []
            flush_outcomes(pending_outcomes, is_final=True)
        except Exception as exc:
            with exception_lock:
                if worker_exception is None:
                    worker_exception = exc
            stop_controller.start_shutdown()

    producer_thread = threading.Thread(target=producer, daemon=False)
    writer_thread = threading.Thread(target=writer, daemon=False)
    worker_threads = [threading.Thread(target=worker, daemon=False) for _ in range(state.workers)]
    all_threads = [producer_thread, writer_thread, *worker_threads]

    try:
        for thread in all_threads:
            thread.start()
        for thread in worker_threads:
            thread.join()
        workers_done.set()
        producer_done.set()
        producer_thread.join()
        writer_thread.join()
    except KeyboardInterrupt as exc:
        stop_controller.start_shutdown()
        producer_done.set()
        workers_done.set()
        for thread in all_threads:
            if thread.is_alive():
                thread.join()
        flush_error = _flush_state_best_effort(
            state=state,
            state_path=state.state_path,
            terminal_lock=terminal_lock,
            success_message="State flush: keyboard interrupt",
        )
        _add_flush_failure_note(
            exc,
            note_prefix="Best-effort keyboard interrupt flush failed",
            flush_error=flush_error,
        )
        raise

    if worker_exception is not None:
        flush_error = _flush_state_best_effort(
            state=state,
            state_path=state.state_path,
            terminal_lock=terminal_lock,
            success_message=f"State flush: worker exception terminal_outcomes={processed_count}",
        )
        _add_flush_failure_note(
            worker_exception,
            note_prefix="Best-effort final flush failed",
            flush_error=flush_error,
        )
        raise worker_exception

    return {"processed": processed_count, "credential_fatal": stop_controller.credential_fatal}
