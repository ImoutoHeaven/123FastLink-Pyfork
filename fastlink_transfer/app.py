from __future__ import annotations

from fastlink_transfer.api import PanApiClient
from fastlink_transfer.auth import build_session, load_credentials
from fastlink_transfer.cli import parse_args
from fastlink_transfer.importer import collect_folder_keys, load_export_file, select_pending_records
from fastlink_transfer.runner import create_remote_directories, run_file_phase, safe_print
from fastlink_transfer.state import load_or_initialize_state


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


def run_cli(argv=None) -> int:
    _, config = parse_args(argv)
    creds = load_credentials()
    session = build_session(creds)
    export_data = load_export_file(config.file_path)
    state = load_or_initialize_state(
        state_path=config.state_file,
        source_file=str(config.file_path),
        source_sha256=export_data.source_sha256,
        target_parent_id=config.target_parent_id,
        common_path=export_data.common_path,
        workers=config.workers,
        total_records=len(export_data.records),
    )

    folder_keys = collect_folder_keys(
        common_path=export_data.common_path,
        record_parent_dirs=[record.relative_parent_dir for record in export_data.records],
    )
    pending_folder_keys = [folder_key for folder_key in folder_keys if folder_key not in state.folder_map]
    pending_records = select_pending_records(
        records=export_data.records,
        completed_keys=state.completed,
        not_reusable_keys=set(state.not_reusable),
        failed_keys=set(state.failed),
        retry_failed=config.retry_failed,
    )

    safe_print(
        f"Startup: files={len(export_data.records)} pending={len(pending_records)} "
        f"folders={len(pending_folder_keys)} workers={config.workers}"
    )

    if config.dry_run:
        safe_print("Dry run: no remote mutations performed")
        return 0

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
            summary = run_file_phase(
                api_client=api_client,
                state=state,
                records=pending_records,
                state_path=config.state_file,
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
        print_summary(state=state)
