from __future__ import annotations


def main(argv=None) -> int:
    from fastlink_transfer.app import run_cli

    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
