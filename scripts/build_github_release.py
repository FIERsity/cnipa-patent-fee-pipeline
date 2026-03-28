from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEST = ROOT / "github_release"


SCRIPT_PATTERNS = [
    "scripts/*.py",
]

ROOT_FILES = [
    "README.md",
    "requirements.txt",
]

CONFIG_FILES = [
    "configs/default.json",
]

RAW_PATTERNS = [
    "raw/sample",
    "raw/patent_master_public_demo.csv",
    "raw/patentstar_sample_ids.csv",
    "raw/reference/prefecture_level_cities.csv",
    "raw/sample_extract/分年份保存数据/",
]

def _copy_file(src: Path, dst: Path, dry_run: bool) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dry_run:
        shutil.copy2(src, dst)


def _copy_tree_filtered(src_dir: Path, dst_dir: Path, patterns: List[str], dry_run: bool) -> int:
    copied = 0
    for path in src_dir.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT).as_posix()
        if not any(rel.startswith(prefix) for prefix in patterns):
            continue
        dst = dst_dir / path.relative_to(src_dir)
        _copy_file(path, dst, dry_run)
        copied += 1
    return copied


def _latest_archive_outputs() -> Path | None:
    archive_root = ROOT / "archive"
    if not archive_root.exists():
        return None
    dated = [path for path in archive_root.iterdir() if path.is_dir()]
    if not dated:
        return None
    return sorted(dated)[-1] / "outputs"


def build_release(dest: Path, dry_run: bool = False, clean: bool = True) -> Tuple[int, int]:
    if clean and dest.exists() and not dry_run:
        shutil.rmtree(dest)
    if not dry_run:
        dest.mkdir(parents=True, exist_ok=True)

    copied_files = 0
    copied_dirs = 0

    # Core text and config files.
    for rel in ROOT_FILES + CONFIG_FILES:
        src = ROOT / rel
        if src.exists():
            _copy_file(src, dest / rel, dry_run)
            copied_files += 1

    # Scripts: copy all Python files from scripts/ except bytecode.
    scripts_dir = ROOT / "scripts"
    if scripts_dir.exists():
        for path in scripts_dir.rglob("*.py"):
            rel = path.relative_to(ROOT).as_posix()
            dst = dest / path.relative_to(ROOT)
            _copy_file(path, dst, dry_run)
            copied_files += 1
            copied_dirs += 0

    # Small sample data sets and demo outputs only.
    copied_files += _copy_tree_filtered(ROOT / "raw", dest / "raw", RAW_PATTERNS, dry_run)

    outputs_dir = ROOT / "outputs"
    for path in outputs_dir.glob("*sample*.csv"):
        _copy_file(path, dest / "outputs" / path.name, dry_run)
        copied_files += 1
    for path in outputs_dir.glob("*demo*.csv"):
        _copy_file(path, dest / "outputs" / path.name, dry_run)
        copied_files += 1
    for path in outputs_dir.glob("*probe*.csv"):
        _copy_file(path, dest / "outputs" / path.name, dry_run)
        copied_files += 1

    archive_outputs = _latest_archive_outputs()
    if archive_outputs and archive_outputs.exists():
        for path in archive_outputs.iterdir():
            if not path.is_file():
                continue
            name = path.name
            if "sample" not in name and "demo" not in name:
                continue
            if path.stat().st_size > 25 * 1024 * 1024:
                continue
            _copy_file(path, dest / "outputs" / name, dry_run)
            copied_files += 1

    # Lightweight metadata for GitHub release users.
    notes = dest / "RELEASE_NOTES.txt"
    if not dry_run:
        notes.write_text(
            "\n".join(
                [
                    "GitHub release snapshot",
                    "",
                    "Included:",
                    "- scripts/",
                    "- README.md",
                    "- requirements.txt",
                    "- configs/default.json",
                    "- sample raw CSVs",
                    "- sample/demo outputs",
                    "",
                    "Excluded:",
                    "- full raw archives",
                    "- full outputs",
                    "- logs",
                    "- login/session state",
                    "- archive/",
                ]
            ),
            encoding="utf-8",
        )

    return copied_files, copied_dirs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a GitHub-ready release snapshot in a separate directory")
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-clean", action="store_true")
    args = parser.parse_args()
    copied_files, copied_dirs = build_release(args.dest, dry_run=args.dry_run, clean=not args.no_clean)
    print(f"dest={args.dest}")
    print(f"copied_files={copied_files}")
    print(f"copied_dirs={copied_dirs}")
    if args.dry_run:
        print("dry_run=True")


if __name__ == "__main__":
    main()
