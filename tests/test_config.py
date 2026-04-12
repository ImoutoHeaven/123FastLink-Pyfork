from argparse import Namespace

import pytest

from fastlink_transfer.config import CommandConfig, build_command_config


def test_build_command_config_normalizes_paths(tmp_path):
    export_file = tmp_path / "sample.json"
    export_file.write_text("{}", encoding="utf-8")
    args = Namespace(
        file=str(export_file),
        target_parent_id="12345678",
        state_file=str(tmp_path / "state.json"),
        workers=8,
        max_retries=5,
        flush_every=100,
        retry_failed=False,
        dry_run=False,
        command="import_json",
    )
    config = build_command_config(args)
    assert config == CommandConfig(
        file_path=export_file.resolve(),
        target_parent_id="12345678",
        state_file=(tmp_path / "state.json").resolve(),
        workers=8,
        max_retries=5,
        flush_every=100,
        retry_failed=False,
        dry_run=False,
    )


@pytest.mark.parametrize(
    "workers,max_retries,flush_every,target_parent_id",
    [
        (0, 5, 100, "12345678"),
        (8, -1, 100, "12345678"),
        (8, 5, 0, "12345678"),
        (8, 5, 100, ""),
        (8, 5, 100, "   "),
    ],
)
def test_build_command_config_rejects_invalid_values(
    tmp_path, workers, max_retries, flush_every, target_parent_id
):
    export_file = tmp_path / "sample.json"
    export_file.write_text("{}", encoding="utf-8")
    args = Namespace(
        file=str(export_file),
        target_parent_id=target_parent_id,
        state_file=str(tmp_path / "state.json"),
        workers=workers,
        max_retries=max_retries,
        flush_every=flush_every,
        retry_failed=False,
        dry_run=False,
        command="import_json",
    )
    with pytest.raises(ValueError):
        build_command_config(args)
