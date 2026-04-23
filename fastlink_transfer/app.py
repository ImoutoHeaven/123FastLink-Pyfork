from __future__ import annotations

from pathlib import Path

from fastlink_transfer.api import PanApiClient
from fastlink_transfer.auth import build_session, load_credentials
from fastlink_transfer.batch_import import run_batch_import_cli
from fastlink_transfer.cli import parse_args
from fastlink_transfer.exporter import run_export_json
from fastlink_transfer.import_planner import inspect_export_scope, rebuild_incomplete_plan_if_needed
from fastlink_transfer.import_state import open_or_initialize_import_state
from fastlink_transfer.runner import (
    create_remote_directories,
    finalize_import_job,
    run_file_phase_sqlite,
    safe_print,
)


def summarize_exit_code(*, failed_count: int, credential_fatal: bool) -> int:
    if credential_fatal:
        return 1
    if failed_count > 0:
        return 1
    return 0


def flush_state_best_effort(*, state, state_path, reason: str) -> None:
    try:
        state.flush(state_path)
    except BaseException as exc:
        safe_print(f"State flush failed: {reason}: {exc}")
        return
    safe_print(f"State flush: {reason}")


def print_summary(*, state) -> None:
    safe_print(
        "Summary: "
        f"completed={state.stats['completed']} "
        f"not_reusable={state.stats['not_reusable']} "
        f"failed={state.stats['failed']}"
    )


def _retry_export_path_for_state(state_file: Path) -> Path:
    return state_file.with_suffix(".retry.export.json")


def run_cli(argv=None) -> int:
    _, config = parse_args(argv)

    if config.command == "export_json":
        creds = load_credentials()
        session = build_session(creds)
        api_client = PanApiClient(host=creds.host, session=session)
        return run_export_json(api_client=api_client, config=config)

    if config.command == "batch_import_json":
        return run_batch_import_cli(config=config)

    creds = load_credentials()
    session = build_session(creds)

    scope = inspect_export_scope(export_path=config.file_path)
    state = open_or_initialize_import_state(
        state_path=config.state_file,
        source_file=str(config.file_path),
        source_sha256=scope.source_sha256,
        target_parent_id=config.target_parent_id,
        common_path=scope.common_path,
    )
    state.workers = config.workers
    rebuild_incomplete_plan_if_needed(
        export_path=config.file_path,
        state=state,
        scope=scope,
    )
    pending_folder_keys = state.fetch_pending_folder_keys()
    pending_file_count = state.count_pending_files(retry_failed=config.retry_failed)

    safe_print(
        f"Startup: files={state.stats['total']} pending={pending_file_count} "
        f"folders={len(pending_folder_keys)} workers={config.workers}"
    )

    if config.dry_run:
        safe_print("Dry run: no remote mutations performed")
        return 0

    # Dry-run should count retryable rows without rewriting persisted terminal state.
    if config.retry_failed:
        state.reset_retryable_rows()

    api_client = PanApiClient(host=creds.host, session=session)
    summary = {"credential_fatal": False}
    try:
        try:
            create_remote_directories(
                api_client=api_client,
                state=state,
                folder_keys=pending_folder_keys,
                state_path=config.state_file,
                max_retries=config.max_retries,
            )
        except KeyboardInterrupt:
            flush_state_best_effort(
                state=state,
                state_path=config.state_file,
                reason="interrupted during run",
            )
            raise
        except RuntimeError as exc:
            flush_state_best_effort(
                state=state,
                state_path=config.state_file,
                reason=f"mkdir failure: {exc}",
            )
            return 1

        safe_print(f"Directory phase complete: cached_folders={len(state.folder_map)}")

        try:
            summary = run_file_phase_sqlite(
                api_client=api_client,
                state=state,
                max_retries=config.max_retries,
                flush_every=config.flush_every,
            )
        except KeyboardInterrupt:
            raise
        except RuntimeError as exc:
            flush_state_best_effort(
                state=state,
                state_path=config.state_file,
                reason=f"file phase failure: {exc}",
            )
            raise

        return summarize_exit_code(
            failed_count=state.stats["failed"],
            credential_fatal=bool(summary["credential_fatal"]),
        )
    finally:
        finalize_import_job(
            state=state,
            retry_export_path=_retry_export_path_for_state(config.state_file),
        )
        print_summary(state=state)
