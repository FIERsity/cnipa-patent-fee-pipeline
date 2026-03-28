from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import subprocess
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from cnipa_utils import (
    configure_logger,
    ensure_dir,
    infer_city_from_text,
    normalize_input_id,
    normalize_patent_join_key,
    normalize_text_key,
    normalize_year_value,
    now_iso,
)


DEFAULT_ARCHIVE = Path("raw/分年份保存数据.rar")
DEFAULT_CITY_MASTER = Path("raw/reference/prefecture_level_cities.csv")
DEFAULT_OUTPUT = Path("outputs/patent_master_rar_compact.csv")
MUNICIPALITIES = [
    {"province_name": "北京市", "province_adcode": "110000", "city_name": "北京市", "city_short_name": "北京", "city_adcode": "110100", "city_type": "municipality"},
    {"province_name": "天津市", "province_adcode": "120000", "city_name": "天津市", "city_short_name": "天津", "city_adcode": "120100", "city_type": "municipality"},
    {"province_name": "上海市", "province_adcode": "310000", "city_name": "上海市", "city_short_name": "上海", "city_adcode": "310100", "city_type": "municipality"},
    {"province_name": "重庆市", "province_adcode": "500000", "city_name": "重庆市", "city_short_name": "重庆", "city_adcode": "500100", "city_type": "municipality"},
]


def load_city_lookup(city_master_csv: Path) -> Tuple[Dict[str, Dict[str, str]], List[Tuple[str, str]]]:
    city_rows: Dict[str, Dict[str, str]] = {}
    lookup: List[Tuple[str, str]] = []
    with city_master_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            city_name = (row.get("city_name") or "").strip()
            city_short = (row.get("city_short_name") or "").strip()
            if city_name:
                key = normalize_text_key(city_name)
                city_rows[key] = row
                lookup.append((key, city_name))
            if city_short and city_short != city_name:
                lookup.append((normalize_text_key(city_short), city_name))
    for row in MUNICIPALITIES:
        key = normalize_text_key(row["city_name"])
        city_rows[key] = row
        lookup.append((key, row["city_name"]))
        lookup.append((normalize_text_key(row["city_short_name"]), row["city_name"]))
    lookup = sorted({item for item in lookup if item[0]}, key=lambda x: (-len(x[0]), x[0]))
    return city_rows, lookup


def list_members(archive: Path) -> List[str]:
    proc = subprocess.run(["bsdtar", "-tf", str(archive)], capture_output=True, text=True, check=True)
    members = [line.strip() for line in proc.stdout.splitlines() if line.strip().endswith(".csv")]
    return members


def year_from_member(member: str) -> str:
    m = re.search(r"(\d{4})年\.csv$", member)
    return m.group(1) if m else ""


def iter_member_rows(archive: Path, member: str) -> Iterator[Dict[str, str]]:
    proc = subprocess.Popen(
        ["bsdtar", "-xOf", str(archive), member],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    text_stream = io.TextIOWrapper(proc.stdout, encoding="utf-8-sig", newline="")
    reader = csv.DictReader(text_stream)
    try:
        for row in reader:
            yield row
    finally:
        text_stream.detach()
        stderr = proc.stderr.read().decode("utf-8", errors="ignore") if proc.stderr else ""
        proc.wait()
        if proc.returncode not in (0, None):
            raise RuntimeError(f"bsdtar failed for {member}: {stderr[:1000]}")


def pick_first_nonblank(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "<na>"}:
            return text
    return ""


def infer_city(row: Dict[str, str], city_lookup: List[Tuple[str, str]]) -> str:
    city = pick_first_nonblank(row.get("申请人城市"), row.get("申请人区县"))
    if city:
        inferred = infer_city_from_text(city, city_lookup)
        if inferred:
            return inferred
    address = pick_first_nonblank(row.get("申请人地址"), row.get("申请人"))
    if address:
        inferred = infer_city_from_text(address, city_lookup)
        if inferred:
            return inferred
    return ""


def build_master_rows(
    archive: Path,
    city_master_csv: Path,
    output: Path,
    years: Optional[Sequence[str]] = None,
    log_path: Optional[Path] = None,
) -> int:
    logger = configure_logger(log_path or Path("logs/build_patent_master_from_rar.log"))
    city_rows, city_lookup = load_city_lookup(city_master_csv)
    wanted_years = {str(y) for y in years} if years else None
    members = list_members(archive)
    members = [m for m in members if year_from_member(m) and (wanted_years is None or year_from_member(m) in wanted_years)]
    logger.info("archive=%s members=%d", archive, len(members))

    ensure_dir(output.parent)
    fieldnames = [
        "input_id",
        "input_id_type",
        "application_no",
        "title",
        "applicant",
        "applicant_address",
        "province_name",
        "city_name",
        "year",
        "application_date",
        "publication_no",
        "publication_date",
        "grant_no",
        "grant_date",
        "source_archive_member",
        "crawl_time",
        "parse_status",
        "notes",
    ]
    written = 0
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for member in members:
            member_year = year_from_member(member)
            logger.info("processing %s", member)
            member_count = 0
            for row in iter_member_rows(archive, member):
                application_no = pick_first_nonblank(row.get("申请号"))
                if not application_no or application_no == "申请号":
                    continue
                city_name = infer_city(row, city_lookup)
                province_name = ""
                if city_name:
                    city_meta = city_rows.get(normalize_text_key(city_name), {})
                    province_name = pick_first_nonblank(city_meta.get("province_name"))
                    if not province_name and city_meta.get("city_name") in {"北京市", "天津市", "上海市", "重庆市"}:
                        province_name = city_meta.get("city_name", "")
                    if not province_name:
                        province_name = pick_first_nonblank(row.get("申请人地区"))
                else:
                    province_name = pick_first_nonblank(row.get("申请人地区"))
                application_date = pick_first_nonblank(row.get("申请日"))
                publication_no = pick_first_nonblank(row.get("公开公告号"))
                publication_date = pick_first_nonblank(row.get("公开公告日"))
                grant_no = pick_first_nonblank(row.get("授权公告号"))
                grant_date = pick_first_nonblank(row.get("授权公告日"))
                applicant_address = pick_first_nonblank(row.get("申请人地址"))
                title = pick_first_nonblank(row.get("专利名称"))
                if title == "专利名称":
                    continue
                year = normalize_year_value(
                    pick_first_nonblank(row.get("申请年份"), member_year, application_date[:4] if application_date else "")
                )
                output_row = {
                    "input_id": normalize_patent_join_key(application_no),
                    "input_id_type": "application_no",
                    "application_no": application_no,
                    "title": title,
                    "applicant": pick_first_nonblank(row.get("申请人")),
                    "applicant_address": applicant_address,
                    "province_name": province_name,
                    "city_name": city_name,
                    "year": year,
                    "application_date": application_date,
                    "publication_no": publication_no,
                    "publication_date": publication_date,
                    "grant_no": grant_no,
                    "grant_date": grant_date,
                    "source_archive_member": member,
                    "crawl_time": now_iso(),
                    "parse_status": "ok",
                    "notes": "; ".join(
                        [
                            f"申请人地区={pick_first_nonblank(row.get('申请人地区'))}",
                            f"申请人城市={pick_first_nonblank(row.get('申请人城市'))}",
                            f"申请人区县={pick_first_nonblank(row.get('申请人区县'))}",
                        ]
                    ),
                }
                writer.writerow(output_row)
                written += 1
                member_count += 1
                if member_count % 500000 == 0:
                    logger.info("written %s rows for %s (total=%s)", member_count, member, written)
            logger.info("finished %s rows=%s total=%s", member, member_count, written)
    logger.info("wrote %s rows to %s", written, output)
    return written


def parse_years_arg(values: Optional[Sequence[str]]) -> Optional[List[str]]:
    if not values:
        return None
    out: List[str] = []
    for value in values:
        if not value:
            continue
        for piece in value.split(","):
            piece = piece.strip()
            if piece:
                out.append(piece)
    return sorted(set(out))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a compact patent master from the yearly RAR archive")
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE, type=Path)
    parser.add_argument("--cities", default=DEFAULT_CITY_MASTER, type=Path)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    parser.add_argument("--year", action="append", help="Optional year filter; may be repeated or comma-separated")
    parser.add_argument("--log", default=Path("logs/build_patent_master_from_rar.log"), type=Path)
    args = parser.parse_args()
    years = parse_years_arg(args.year)
    build_master_rows(args.archive, args.cities, args.output, years=years, log_path=args.log)


if __name__ == "__main__":
    main()
