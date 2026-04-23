import hashlib
import json
from pathlib import Path

import pytest

from fastlink_transfer.importer import (
    collect_folder_keys,
    decode_base62_to_hex,
    load_export_file,
    normalize_import_record,
    parse_size,
    select_pending_records,
)


SAMPLE_EXPORT_FIXTURE = Path(__file__).parent / "fixtures" / "sample_export.json"


def test_decode_base62_to_hex_matches_repo_algorithm():
    assert decode_base62_to_hex("WiAkcJukpqBeKRKuXWRec") == "1ee55b32272de3728552308ab589f264"


@pytest.mark.parametrize("value", [True, False, "1.0", "1e3", -1, "-1"])
def test_parse_size_rejects_non_integer_forms(value):
    with pytest.raises(ValueError):
        parse_size(value)


@pytest.mark.parametrize("value", ["١٢٣", "１２３"])
def test_parse_size_rejects_non_ascii_digits(value):
    with pytest.raises(ValueError):
        parse_size(value)


def test_load_export_file_rejects_duplicate_paths_after_normalization(tmp_path):
    export_file = tmp_path / "export.json"
    export_file.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "files": [
                    {"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "a\\b.txt"},
                    {"etag": "fedcba9876543210fedcba9876543210", "size": "2", "path": "a/b.txt"},
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_export_file(export_file)


def test_load_export_file_rejects_invalid_base62_character(tmp_path):
    export_file = tmp_path / "export.json"
    export_file.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": True,
                "files": [{"etag": "***", "size": "1", "path": "a.txt"}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_export_file(export_file)


def test_load_export_file_rejects_explicit_null_common_path(tmp_path):
    export_file = tmp_path / "export.json"
    export_file.write_text(
        json.dumps(
            {
                "usesBase62EtagsInExport": False,
                "commonPath": None,
                "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "1", "path": "a.txt"}],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_export_file(export_file)


def test_load_export_file_uses_exact_utf8_bytes_for_digest(tmp_path):
    raw = json.dumps(
        {
            "usesBase62EtagsInExport": False,
            "commonPath": "Demo/",
            "files": [{"etag": "0123456789abcdef0123456789abcdef", "size": "5", "path": "a/b.txt"}],
        },
        ensure_ascii=False,
    )
    export_file = tmp_path / "export.json"
    export_file.write_text(raw, encoding="utf-8")

    export = load_export_file(export_file)

    assert export.source_sha256 == hashlib.sha256(raw.encode("utf-8")).hexdigest()


def test_fixture_export_loads_into_two_records():
    export = load_export_file(SAMPLE_EXPORT_FIXTURE)

    assert export.common_path == "Demo/"
    assert [(record.path, record.file_name, record.relative_parent_dir, record.key) for record in export.records] == [
        (
            "1983/06/example-a.rar",
            "example-a.rar",
            "1983/06",
            "1983/06/example-a.rar\t0123456789abcdef0123456789abcdef\t10",
        ),
        (
            "1983/07/example-b.rar",
            "example-b.rar",
            "1983/07",
            "1983/07/example-b.rar\tfedcba9876543210fedcba9876543210\t11",
        ),
    ]


def test_collect_folder_keys_creates_common_path_first_from_record_parent_dirs():
    keys = collect_folder_keys(
        common_path="H-SUKI/",
        record_parent_dirs=["1983/06", "1983/07"],
    )

    assert keys == ["H-SUKI", "H-SUKI/1983", "H-SUKI/1983/06", "H-SUKI/1983/07"]


def test_collect_folder_keys_ignores_empty_record_parent_dirs():
    keys = collect_folder_keys(
        common_path="Demo/",
        record_parent_dirs=["", "1983/06"],
    )

    assert keys == ["Demo", "Demo/1983", "Demo/1983/06"]


def test_normalize_import_record_normalizes_path_etag_and_size():
    normalized_path, etag_hex, size = normalize_import_record(
        path_value="1983\\06\\a.txt",
        etag_value="0123456789abcdef0123456789abcdef",
        size_value="5",
        uses_base62=False,
    )

    assert normalized_path == "1983/06/a.txt"
    assert etag_hex == "0123456789abcdef0123456789abcdef"
    assert size == 5


def test_normalize_import_record_decodes_base62_etags_when_requested():
    normalized_path, etag_hex, size = normalize_import_record(
        path_value="1983/06/a.txt",
        etag_value="WiAkcJukpqBeKRKuXWRec",
        size_value=9,
        uses_base62=True,
    )

    assert normalized_path == "1983/06/a.txt"
    assert etag_hex == "1ee55b32272de3728552308ab589f264"
    assert size == 9


def test_select_pending_records_skips_terminal_records_without_retry_failed():
    records = [
        type("Record", (), {"key": "done"})(),
        type("Record", (), {"key": "retry"})(),
        type("Record", (), {"key": "new"})(),
    ]

    pending = select_pending_records(
        records=records,
        completed_keys={"done"},
        not_reusable_keys={"retry"},
        failed_keys=set(),
        retry_failed=False,
    )

    assert [record.key for record in pending] == ["new"]


def test_select_pending_records_requeues_not_reusable_and_failed_with_flag():
    records = [
        type("Record", (), {"key": "done"})(),
        type("Record", (), {"key": "retry_nr"})(),
        type("Record", (), {"key": "retry_failed"})(),
    ]

    pending = select_pending_records(
        records=records,
        completed_keys={"done"},
        not_reusable_keys={"retry_nr"},
        failed_keys={"retry_failed"},
        retry_failed=True,
    )

    assert [record.key for record in pending] == ["retry_nr", "retry_failed"]
