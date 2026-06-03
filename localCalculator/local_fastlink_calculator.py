from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


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


def _file_md5(path: Path) -> str:
    md5 = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            md5.update(chunk)
    return md5.hexdigest()


def _iter_files(source_dir: Path):
    for path in source_dir.rglob("*"):
        if path.is_file():
            yield path


def build_export_payload(source_dir: Path | str, *, common_path: str) -> dict:
    root = Path(source_dir)
    files = []
    total_size = 0

    for file_path in sorted(_iter_files(root), key=lambda value: value.relative_to(root).as_posix()):
        relative_path = file_path.relative_to(root).as_posix()
        size = file_path.stat().st_size
        total_size += size
        files.append(
            {
                "path": relative_path,
                "etag": _hex_to_base62(_file_md5(file_path)),
                "size": str(size),
            }
        )

    if not files:
        raise ValueError("no files found")

    return {
        "scriptVersion": "local-calculator",
        "exportVersion": "1.0",
        "usesBase62EtagsInExport": True,
        "commonPath": common_path,
        "totalFilesCount": len(files),
        "totalSize": total_size,
        "formattedTotalSize": _format_size(total_size),
        "files": files,
    }


def _default_common_path(source_dir: Path) -> str:
    return f"{source_dir.name.rstrip('/')}/"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate 123Pan export JSON from local files.")
    parser.add_argument("source_dir", type=Path)
    parser.add_argument("output_file", type=Path)
    parser.add_argument("--common-path", default=None)
    args = parser.parse_args(argv)

    common_path = args.common_path if args.common_path is not None else _default_common_path(args.source_dir)
    payload = build_export_payload(args.source_dir, common_path=common_path)
    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    args.output_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
