from fastlink_transfer.state import TransferState


def test_transfer_state_record_failed_updates_failed_bucket_and_stats():
    state = TransferState(
        source_file="a",
        source_sha256="abc",
        target_parent_id="12345678",
        common_path="Demo/",
        workers=8,
        folder_map={"": "12345678"},
        completed=set(),
        not_reusable={},
        failed={},
        stats={"total": 1, "completed": 0, "not_reusable": 0, "failed": 0},
        last_flush_at=None,
    )

    state.record_failed(key="k", path="a.txt", error="boom", retries=2)

    assert state.failed == {
        "k": {"key": "k", "path": "a.txt", "error": "boom", "retries": 2}
    }
    assert state.stats == {"total": 1, "completed": 0, "not_reusable": 0, "failed": 1}
