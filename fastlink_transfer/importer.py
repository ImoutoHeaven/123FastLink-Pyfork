from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path


BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
HEX_RE = re.compile(r"^[0-9a-fA-F]{32}$")
ASCII_DIGITS_RE = re.compile(r"^[0-9]+$")


@dataclass(frozen=True)
class SourceRecord:
    key: str
    etag: str
    size: int
    path: str
    file_name: str
    relative_parent_dir: str


@dataclass(frozen=True)
class ExportData:
    source_sha256: str
    common_path: str
    records: list[SourceRecord]


def normalize_relative_path(value: str) -> str:
    normalized = value.replace("\\", "/")
    if not normalized or normalized.startswith("/") or normalized.endswith("/"):
        raise ValueError("invalid path")
    if "//" in normalized:
        raise ValueError("invalid path")

    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("invalid path")

    return normalized


def normalize_common_path(value: object) -> str:
    if value == "":
        return ""
    if not isinstance(value, str):
        raise ValueError("commonPath must be a string")

    normalized = value.replace("\\", "/")
    if normalized.startswith("/") or normalized.endswith("//") or "//" in normalized:
        raise ValueError("invalid commonPath")

    stripped = normalized.rstrip("/")
    if not stripped:
        return ""

    parts = stripped.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("invalid commonPath")

    return f"{stripped}/"


def decode_base62_to_hex(value: str) -> str:
    if not value:
        raise ValueError("etag must be a non-empty string")

    number = 0
    for char in value:
        if char not in BASE62_ALPHABET:
            raise ValueError("invalid base62 etag")
        number = number * 62 + BASE62_ALPHABET.index(char)

    hex_value = format(number, "x")
    if len(hex_value) % 2:
        hex_value = f"0{hex_value}"
    if len(hex_value) > 32:
        raise ValueError("decoded etag exceeds 32 hex chars")

    return hex_value.rjust(32, "0").lower()


def parse_size(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("size must not be boolean")
    if isinstance(value, int):
        if value < 0:
            raise ValueError("size must be >= 0")
        return value
    if isinstance(value, str) and ASCII_DIGITS_RE.fullmatch(value):
        return int(value)
    raise ValueError("size must be a non-negative integer or decimal-digit string")


def validate_hex_etag(value: str) -> str:
    if not value or not HEX_RE.fullmatch(value):
        raise ValueError("etag must be 32 hex chars")
    return value.lower()


def load_export_file(path: Path) -> ExportData:
    raw_bytes = path.read_bytes()
    payload = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("export root must be an object")

    files = payload.get("files")
    if not isinstance(files, list):
        raise ValueError("files must be an array")

    uses_base62 = payload.get("usesBase62EtagsInExport", False)
    if not isinstance(uses_base62, bool):
        raise ValueError("usesBase62EtagsInExport must be boolean when present")

    common_path_value = payload["commonPath"] if "commonPath" in payload else ""
    common_path = normalize_common_path(common_path_value)
    records: list[SourceRecord] = []
    seen_paths: set[str] = set()
    for file_entry in files:
        if not isinstance(file_entry, dict):
            raise ValueError("file entries must be objects")

        etag_value = file_entry.get("etag")
        path_value = file_entry.get("path")
        if not isinstance(etag_value, str) or not etag_value:
            raise ValueError("etag must be a non-empty string")
        if not isinstance(path_value, str):
            raise ValueError("path must be a string")

        normalized_path = normalize_relative_path(path_value)
        if normalized_path in seen_paths:
            raise ValueError(f"duplicate normalized path: {normalized_path}")
        seen_paths.add(normalized_path)

        etag = decode_base62_to_hex(etag_value) if uses_base62 else validate_hex_etag(etag_value)
        size = parse_size(file_entry.get("size"))
        path_parts = normalized_path.split("/")
        key = f"{normalized_path}\t{etag}\t{size}"
        records.append(
            SourceRecord(
                key=key,
                etag=etag,
                size=size,
                path=normalized_path,
                file_name=path_parts[-1],
                relative_parent_dir="/".join(path_parts[:-1]),
            )
        )

    return ExportData(
        source_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        common_path=common_path,
        records=records,
    )


def collect_folder_keys(*, common_path: str, record_parent_dirs: list[str]) -> list[str]:
    folder_keys: set[str] = set()
    common_segments = common_path.rstrip("/").split("/") if common_path else []
    for index in range(len(common_segments)):
        folder_keys.add("/".join(common_segments[: index + 1]))

    for record_parent_dir in record_parent_dirs:
        if not record_parent_dir:
            continue
        parent_segments = record_parent_dir.split("/")
        for index in range(len(parent_segments)):
            folder_keys.add("/".join(common_segments + parent_segments[: index + 1]))

    return sorted(filter(None, folder_keys), key=lambda value: (value.count("/"), value))


def select_pending_records(
    *,
    records,
    completed_keys: set[str],
    not_reusable_keys: set[str],
    failed_keys: set[str],
    retry_failed: bool,
):
    pending = []
    for record in records:
        if record.key in completed_keys:
            continue
        if record.key in not_reusable_keys or record.key in failed_keys:
            if retry_failed:
                pending.append(record)
            continue
        pending.append(record)

    return pending
