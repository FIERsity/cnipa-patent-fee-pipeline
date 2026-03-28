from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path
from typing import Iterable, List


ROOT = Path(__file__).resolve().parents[1]


KEEP_OUTPUTS = {
    "patent_master_rar_full.csv",
    "patent_fee_inference_ftp_full.csv",
    "patent_legal_events_ftp_full.csv",
    "patent_fee_unmatched_to_rar_full.csv",
}


OUTPUT_PATTERNS = [
    "_*.csv",
    "*demo*.csv",
    "*sample*.csv",
    "tmp*.csv",
    "patentstar*.csv",
    "city_patent_panel*.csv",
    "patent_master_ftp*.csv",
    "patent_master_rar_prefix_*.csv",
    "patent_master_rar_tail_*.csv",
    "patent_legal_events_ftp_bibs.csv",
    "patent_legal_events_ftp_prss.csv",
    "patent_legal_events.csv",
    "patent_fee_inference.csv",
    "patent_master_public*.csv",
]


RAW_PATTERNS = [
    "ftp_probe*",
    "ftp_probe2*",
    "ftp_probe3*",
    "html_snapshots",
    "cnipa_public_samples",
    "cnipa_public_patent_samples",
    "patent_master_public*.csv",
    "patentstar_*.csv",
    "sample_patent_ids.csv",
    "sample_extract",
    "cnipa_public_catalog.csv",
    "cnipa_public_catalog.json",
]


LOG_PATTERNS = [
    "*smoke*",
    "*probe*",
    "fetch_patentstar*",
    "public*",
    "build_*",
    "merge_*",
    "run2.log",
    "run3.log",
]


STATE_PATTERNS = [
    "cpquery.state.json",
    "patentstar.state.json",
    "pss-system.state.json",
]


def _matches(path: Path, patterns: Iterable[str]) -> bool:
    return any(path.match(pattern) for pattern in patterns)


def _archive_path(archive_root: Path, rel_path: Path) -> Path:
    return archive_root / rel_path


def _move(path: Path, archive_root: Path, dry_run: bool) -> Path:
    rel = path.relative_to(ROOT)
    target = _archive_path(archive_root, rel)
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.move(str(path), str(target))
    return target


def archive_paths(paths: List[Path], archive_root: Path, dry_run: bool) -> int:
    moved = 0
    for path in paths:
        if not path.exists():
            continue
        _move(path, archive_root, dry_run)
        moved += 1
    return moved


def collect_targets() -> tuple[List[Path], List[Path], List[Path], List[Path], List[Path]]:
    output_targets: List[Path] = []
    raw_targets: List[Path] = []
    log_targets: List[Path] = []
    state_targets: List[Path] = []
    delete_targets: List[Path] = []

    outputs_dir = ROOT / "outputs"
    raw_dir = ROOT / "raw"
    logs_dir = ROOT / "logs"
    output_playwright_dir = ROOT / "output" / "playwright"

    for path in outputs_dir.glob("*.csv"):
        if path.name in KEEP_OUTPUTS:
            continue
        if _matches(path, OUTPUT_PATTERNS):
            output_targets.append(path)

    for path in raw_dir.iterdir():
        if path.name == "分年份保存数据.rar":
            continue
        if _matches(path, RAW_PATTERNS):
            raw_targets.append(path)

    for path in logs_dir.glob("*.log"):
        if _matches(path, LOG_PATTERNS):
            log_targets.append(path)

    if output_playwright_dir.exists():
        for path in output_playwright_dir.glob("*.json"):
            if _matches(path, STATE_PATTERNS):
                state_targets.append(path)

    pycache = ROOT / "scripts" / "__pycache__"
    if pycache.exists():
        delete_targets.append(pycache)

    return output_targets, raw_targets, log_targets, state_targets, delete_targets


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive exploratory outputs and remove cache/temp files")
    parser.add_argument("--archive-root", type=Path, default=ROOT / "archive" / datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_targets, raw_targets, log_targets, state_targets, delete_targets = collect_targets()
    archive_root = args.archive_root
    if not args.dry_run:
        archive_root.mkdir(parents=True, exist_ok=True)

    moved = 0
    moved += archive_paths(output_targets, archive_root, args.dry_run)
    moved += archive_paths(raw_targets, archive_root, args.dry_run)
    moved += archive_paths(log_targets, archive_root, args.dry_run)
    moved += archive_paths(state_targets, archive_root, args.dry_run)

    deleted = 0
    for path in delete_targets:
        if not path.exists():
            continue
        if not args.dry_run:
            shutil.rmtree(path)
        deleted += 1

    print(f"archive_root={archive_root}")
    print(f"moved={moved}")
    print(f"deleted={deleted}")
    if args.dry_run:
        print("dry_run=True")


if __name__ == "__main__":
    main()
