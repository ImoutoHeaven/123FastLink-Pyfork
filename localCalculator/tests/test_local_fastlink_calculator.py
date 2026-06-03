import hashlib
import json

import pytest

from fastlink_transfer.importer import load_export_file
from localCalculator.local_fastlink_calculator import build_export_payload, main


BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def hex_to_base62(value: str) -> str:
    number = int(value, 16)
    if number == 0:
        return "0"
    chars = []
    while number:
        number, remainder = divmod(number, 62)
        chars.append(BASE62_ALPHABET[remainder])
    return "".join(reversed(chars))


def test_build_export_payload_scans_files_and_matches_export_json_shape(tmp_path):
    root = tmp_path / "Movies"
    (root / "1983" / "06").mkdir(parents=True)
    (root / "1983" / "07").mkdir(parents=True)
    (root / "1983" / "07" / "b.bin").write_bytes(b"bbb")
    (root / "1983" / "06" / "a.bin").write_bytes(b"aa")

    payload = build_export_payload(root, common_path="Movies/")

    a_md5 = hashlib.md5(b"aa").hexdigest()
    b_md5 = hashlib.md5(b"bbb").hexdigest()
    assert payload == {
        "scriptVersion": "local-calculator",
        "exportVersion": "1.0",
        "usesBase62EtagsInExport": True,
        "commonPath": "Movies/",
        "totalFilesCount": 2,
        "totalSize": 5,
        "formattedTotalSize": "5 B",
        "files": [
            {"path": "1983/06/a.bin", "etag": hex_to_base62(a_md5), "size": "2"},
            {"path": "1983/07/b.bin", "etag": hex_to_base62(b_md5), "size": "3"},
        ],
    }


def test_build_export_payload_rejects_empty_directory(tmp_path):
    root = tmp_path / "Empty"
    root.mkdir()

    with pytest.raises(ValueError, match="no files found"):
        build_export_payload(root, common_path="Empty/")


def test_main_writes_json_and_defaults_common_path_to_source_directory_name(tmp_path):
    root = tmp_path / "Movies"
    root.mkdir()
    (root / "clip.bin").write_bytes(b"clip")
    output_file = tmp_path / "movies.json"

    assert main([str(root), str(output_file)]) == 0

    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert payload["commonPath"] == "Movies/"
    assert payload["files"][0]["path"] == "clip.bin"


def test_main_accepts_custom_common_path(tmp_path):
    root = tmp_path / "Movies"
    root.mkdir()
    (root / "clip.bin").write_bytes(b"clip")
    output_file = tmp_path / "custom.json"

    assert main([str(root), str(output_file), "--common-path", "Library/"]) == 0

    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert payload["commonPath"] == "Library/"


def test_generated_json_loads_through_project_importer(tmp_path):
    root = tmp_path / "Movies"
    root.mkdir()
    (root / "clip.bin").write_bytes(b"clip")
    output_file = tmp_path / "movies.json"

    main([str(root), str(output_file)])

    export_data = load_export_file(output_file)
    assert export_data.common_path == "Movies/"
    assert len(export_data.records) == 1
    assert export_data.records[0].path == "clip.bin"
    assert export_data.records[0].etag == hashlib.md5(b"clip").hexdigest()
    assert export_data.records[0].size == 4
