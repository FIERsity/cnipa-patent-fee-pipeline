#!/usr/bin/env python3
"""Merge the prefix and tail patent master shards into one full CSV.

This is intentionally streaming and line-aware:
- writes the prefix as-is
- skips the tail header
- counts rows while copying
- prints progress so long merges are observable
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


DEFAULT_PREFIX = "outputs/patent_master_rar_prefix_1985_2014.csv"
DEFAULT_TAIL = "outputs/patent_master_rar_tail_2015_2024.csv"
DEFAULT_FULL = "outputs/patent_master_rar_full.csv"


def _copy_stream(src_path: Path, dst, *, skip_first_line: bool, label: str, chunk_size: int) -> int:
    rows = 0
    copied_bytes = 0
    next_report = 1024 * 1024 * 1024
    with src_path.open("rb") as src:
        if skip_first_line:
            src.readline()
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dst.write(chunk)
            copied_bytes += len(chunk)
            rows += chunk.count(b"\n")
            if copied_bytes >= next_report:
                print(f"{label}: {copied_bytes} bytes copied, {rows} rows", file=sys.stderr, flush=True)
                next_report += 1024 * 1024 * 1024
    print(f"{label}: done, {copied_bytes} bytes copied, {rows} rows", file=sys.stderr, flush=True)
    return rows


def merge(prefix: Path, tail: Path, full: Path, *, chunk_size: int) -> tuple[int, int]:
    if not prefix.exists():
        raise FileNotFoundError(prefix)
    if not tail.exists():
        raise FileNotFoundError(tail)

    if full.exists():
        full.unlink()

    prefix_rows = 0
    tail_rows = 0
    with full.open("wb") as dst:
        prefix_rows = _copy_stream(prefix, dst, skip_first_line=False, label="prefix", chunk_size=chunk_size)
        tail_rows = _copy_stream(tail, dst, skip_first_line=True, label="tail", chunk_size=chunk_size)

    return prefix_rows, tail_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--tail", default=DEFAULT_TAIL)
    parser.add_argument("--full", default=DEFAULT_FULL)
    parser.add_argument("--chunk-size", type=int, default=8 * 1024 * 1024)
    args = parser.parse_args()

    prefix = Path(args.prefix)
    tail = Path(args.tail)
    full = Path(args.full)

    prefix_rows, tail_rows = merge(prefix, tail, full, chunk_size=args.chunk_size)
    total_rows = prefix_rows + tail_rows
    print(f"merge complete: wrote {total_rows} data rows to {full}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
