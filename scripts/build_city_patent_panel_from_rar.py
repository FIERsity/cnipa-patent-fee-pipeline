from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from build_patent_master_from_rar import (
    infer_city,
    iter_member_rows,
    list_members,
    load_city_lookup,
    parse_years_arg,
    pick_first_nonblank,
    year_from_member,
)
from cnipa_utils import (
    configure_logger,
    ensure_dir,
    normalize_input_id,
    normalize_patent_join_key,
    normalize_text_key,
    normalize_year_value,
    now_iso,
)


DEFAULT_ARCHIVE = Path("raw/分年份保存数据.rar")
DEFAULT_CITY_MASTER = Path("raw/reference/prefecture_level_cities.csv")
DEFAULT_FEE_INFERENCE = Path("outputs/patent_fee_inference_ftp_full.csv")
DEFAULT_OUTPUT = Path("outputs/city_patent_panel_rar_full.csv")
DEFAULT_UNMATCHED_FEE = Path("outputs/patent_fee_unmatched_to_rar_master.csv")


def load_fee_inference(path: Path) -> Dict[str, Dict[str, str]]:
    fee_rows: Dict[str, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            key = normalize_patent_join_key(row.get("input_id", ""))
            if not key:
                continue
            fee_rows[key] = row
    return fee_rows


def load_city_index(city_master_csv: Path) -> Tuple[Dict[str, Dict[str, str]], List[Tuple[str, str]]]:
    city_rows, city_lookup = load_city_lookup(city_master_csv)
    return city_rows, city_lookup


def build_panel_from_rar(
    archive: Path,
    city_master_csv: Path,
    fee_inference_csv: Path,
    output: Path,
    unmatched_fee_output: Optional[Path] = None,
    years: Optional[Sequence[str]] = None,
    fill_zeros: bool = True,
    log_path: Optional[Path] = None,
) -> int:
    logger = configure_logger(log_path or Path("logs/build_city_patent_panel_from_rar.log"))
    city_rows, city_lookup = load_city_index(city_master_csv)
    fee_rows = load_fee_inference(fee_inference_csv)
    remaining_fee_ids = set(fee_rows.keys())

    wanted_years = {str(y) for y in years} if years else None
    members = list_members(archive)
    members = [m for m in members if year_from_member(m) and (wanted_years is None or year_from_member(m) in wanted_years)]
    logger.info("archive=%s members=%d fee_rows=%d", archive, len(members), len(fee_rows))

    panel: Dict[Tuple[str, str, str, str, str, str, str], Dict[str, int]] = defaultdict(
        lambda: {
            "patent_count": 0,
            "fee_nonpayment_termination_patent_count": 0,
            "deemed_abandoned_patent_count": 0,
            "restoration_patent_count": 0,
            "unspecified_termination_patent_count": 0,
            "fee_nonpayment_excluded_patent_count": 0,
            "deemed_abandoned_excluded_patent_count": 0,
            "unspecified_termination_excluded_patent_count": 0,
            "restoration_excluded_patent_count": 0,
            "excluded_patent_count": 0,
            "kept_patent_count": 0,
            "matched_fee_patent_count": 0,
        }
    )

    seen_patents = set()
    total_rows = 0
    matched_fee_rows = 0
    matched_fee_ids = set()

    for member in members:
        member_year = year_from_member(member)
        logger.info("processing %s", member)
        member_count = 0
        for row in iter_member_rows(archive, member):
            total_rows += 1
            application_no = pick_first_nonblank(row.get("申请号"))
            title = pick_first_nonblank(row.get("专利名称"))
            if not application_no or application_no == "申请号" or title == "专利名称":
                continue
            input_id = normalize_patent_join_key(application_no)
            if not input_id or input_id in seen_patents:
                continue
            seen_patents.add(input_id)

            city_name = infer_city(row, city_lookup)
            city_meta = city_rows.get(normalize_text_key(city_name), {})
            if not city_name:
                continue
            province_name = pick_first_nonblank(city_meta.get("province_name")) or pick_first_nonblank(row.get("申请人地区"))
            city_short_name = pick_first_nonblank(city_meta.get("city_short_name"))
            city_adcode = pick_first_nonblank(city_meta.get("city_adcode"))
            province_adcode = pick_first_nonblank(city_meta.get("province_adcode"))
            city_type = pick_first_nonblank(city_meta.get("city_type")) or "prefecture"
            year = normalize_year_value(
                pick_first_nonblank(
                    row.get("申请年份"),
                    member_year,
                    (pick_first_nonblank(row.get("申请日"))[:4] if pick_first_nonblank(row.get("申请日")) else ""),
                )
            )
            if not year:
                continue

            key = (str(year), province_name, province_adcode, city_name, city_short_name, city_adcode, city_type)
            bucket = panel[key]
            bucket["patent_count"] += 1

            fee = fee_rows.get(input_id)
            if fee:
                matched_fee_rows += 1
                matched_fee_ids.add(input_id)
                remaining_fee_ids.discard(input_id)
                bucket["matched_fee_patent_count"] += 1

                has_termination = (fee.get("has_annual_fee_termination_event") or "").strip().lower() == "true"
                has_deemed_abandoned = (fee.get("has_deemed_abandoned_event") or "").strip().lower() == "true"
                has_restoration = (fee.get("has_right_restoration_event") or "").strip().lower() == "true"
                inferred_status_rule = (fee.get("inferred_fee_status_rule") or "").strip()
                panel_exclusion = (fee.get("panel_exclusion_recommendation") or "keep").strip().lower() == "exclude"

                if has_termination:
                    bucket["fee_nonpayment_termination_patent_count"] += 1
                    bucket["fee_nonpayment_excluded_patent_count"] += 1
                if has_deemed_abandoned:
                    bucket["deemed_abandoned_patent_count"] += 1
                    bucket["deemed_abandoned_excluded_patent_count"] += 1
                if has_restoration:
                    bucket["restoration_patent_count"] += 1
                    bucket["restoration_excluded_patent_count"] += 1
                if inferred_status_rule == "termination_event_without_fee_context":
                    bucket["unspecified_termination_patent_count"] += 1
                    bucket["unspecified_termination_excluded_patent_count"] += 1

                if panel_exclusion:
                    bucket["excluded_patent_count"] += 1
                else:
                    bucket["kept_patent_count"] += 1
            else:
                bucket["kept_patent_count"] += 1

            member_count += 1
            if member_count % 500000 == 0:
                logger.info(
                    "written-like progress member=%s unique_patents=%s matched_fee=%s total_rows=%s",
                    member,
                    len(seen_patents),
                    matched_fee_rows,
                    total_rows,
                )

        logger.info("finished %s member_count=%s unique_patents=%s matched_fee=%s", member, member_count, len(seen_patents), matched_fee_rows)

    fieldnames = [
        "year",
        "province_name",
        "province_adcode",
        "city_name",
        "city_short_name",
        "city_adcode",
        "city_type",
        "patent_count",
        "fee_nonpayment_termination_patent_count",
        "deemed_abandoned_patent_count",
        "restoration_patent_count",
        "unspecified_termination_patent_count",
        "fee_nonpayment_excluded_patent_count",
        "deemed_abandoned_excluded_patent_count",
        "unspecified_termination_excluded_patent_count",
        "restoration_excluded_patent_count",
        "excluded_patent_count",
        "kept_patent_count",
        "matched_fee_patent_count",
        "panel_share_excluded",
        "panel_share_fee_termination",
        "panel_share_deemed_abandoned",
        "panel_share_unspecified_termination",
        "panel_share_restoration",
        "crawl_time",
    ]

    def finalize_row(key: Tuple[str, str, str, str, str, str, str], counts: Dict[str, int]) -> Dict[str, object]:
        year, province_name, province_adcode, city_name, city_short_name, city_adcode, city_type = key
        patent_count = counts["patent_count"]
        denom = patent_count if patent_count else 1
        return {
            "year": int(normalize_year_value(year)),
            "province_name": province_name,
            "province_adcode": province_adcode,
            "city_name": city_name,
            "city_short_name": city_short_name,
            "city_adcode": city_adcode,
            "city_type": city_type,
            "patent_count": patent_count,
            "fee_nonpayment_termination_patent_count": counts["fee_nonpayment_termination_patent_count"],
            "deemed_abandoned_patent_count": counts["deemed_abandoned_patent_count"],
            "restoration_patent_count": counts["restoration_patent_count"],
            "unspecified_termination_patent_count": counts["unspecified_termination_patent_count"],
            "fee_nonpayment_excluded_patent_count": counts["fee_nonpayment_excluded_patent_count"],
            "deemed_abandoned_excluded_patent_count": counts["deemed_abandoned_excluded_patent_count"],
            "unspecified_termination_excluded_patent_count": counts["unspecified_termination_excluded_patent_count"],
            "restoration_excluded_patent_count": counts["restoration_excluded_patent_count"],
            "excluded_patent_count": counts["excluded_patent_count"],
            "kept_patent_count": counts["kept_patent_count"],
            "matched_fee_patent_count": counts["matched_fee_patent_count"],
            "panel_share_excluded": counts["excluded_patent_count"] / denom,
            "panel_share_fee_termination": counts["fee_nonpayment_termination_patent_count"] / denom,
            "panel_share_deemed_abandoned": counts["deemed_abandoned_patent_count"] / denom,
            "panel_share_unspecified_termination": counts["unspecified_termination_patent_count"] / denom,
            "panel_share_restoration": counts["restoration_patent_count"] / denom,
            "crawl_time": now_iso(),
        }

    rows = [finalize_row(key, counts) for key, counts in panel.items()]
    rows.sort(key=lambda r: (r["year"], r["province_name"], r["city_name"]))

    if fill_zeros:
        years_all = sorted({int(y) for y in (wanted_years or {r["year"] for r in rows})})
        city_basis = sorted(
            {
                (
                    str(city_rows[city]["province_name"]),
                    str(city_rows[city].get("province_adcode", "")),
                    str(city_rows[city]["city_name"]),
                    str(city_rows[city].get("city_short_name", "")),
                    str(city_rows[city].get("city_adcode", "")),
                    str(city_rows[city].get("city_type", "prefecture")),
                )
                for city in city_rows
                if city_rows[city].get("city_name")
            }
        )
        filled: Dict[Tuple[int, str, str, str, str, str, str], Dict[str, object]] = {}
        for r in rows:
            filled[(int(r["year"]), r["province_name"], r["province_adcode"], r["city_name"], r["city_short_name"], r["city_adcode"], r["city_type"])] = r
        expanded: List[Dict[str, object]] = []
        for year in years_all:
            for province_name, province_adcode, city_name, city_short_name, city_adcode, city_type in city_basis:
                key = (year, province_name, province_adcode, city_name, city_short_name, city_adcode, city_type)
                if key in filled:
                    expanded.append(filled[key])
                else:
                    expanded.append(
                        {
                            "year": year,
                            "province_name": province_name,
                            "province_adcode": province_adcode,
                            "city_name": city_name,
                            "city_short_name": city_short_name,
                            "city_adcode": city_adcode,
                            "city_type": city_type,
                            "patent_count": 0,
                            "fee_nonpayment_termination_patent_count": 0,
                            "deemed_abandoned_patent_count": 0,
                            "restoration_patent_count": 0,
                            "unspecified_termination_patent_count": 0,
                            "fee_nonpayment_excluded_patent_count": 0,
                            "deemed_abandoned_excluded_patent_count": 0,
                            "unspecified_termination_excluded_patent_count": 0,
                            "restoration_excluded_patent_count": 0,
                            "excluded_patent_count": 0,
                            "kept_patent_count": 0,
                            "matched_fee_patent_count": 0,
                            "panel_share_excluded": 0.0,
                            "panel_share_fee_termination": 0.0,
                            "panel_share_deemed_abandoned": 0.0,
                            "panel_share_unspecified_termination": 0.0,
                            "panel_share_restoration": 0.0,
                            "crawl_time": now_iso(),
                        }
                    )
        rows = expanded

    ensure_dir(output.parent)
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    if unmatched_fee_output is not None:
        unmatched_rows = []
        for fee_id in sorted(remaining_fee_ids):
            fee = fee_rows[fee_id]
            unmatched_rows.append(
                {
                    "input_id": fee_id,
                    "inferred_fee_status": fee.get("inferred_fee_status", ""),
                    "inferred_fee_status_rule": fee.get("inferred_fee_status_rule", ""),
                    "confidence_level": fee.get("confidence_level", ""),
                    "annual_fee_termination_date": fee.get("annual_fee_termination_date", ""),
                    "restoration_date": fee.get("restoration_date", ""),
                    "notes": fee.get("notes", ""),
                }
            )
        ensure_dir(unmatched_fee_output.parent)
        with unmatched_fee_output.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "input_id",
                    "inferred_fee_status",
                    "inferred_fee_status_rule",
                    "confidence_level",
                    "annual_fee_termination_date",
                    "restoration_date",
                    "notes",
                ],
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(unmatched_rows)

    logger.info(
        "wrote rows=%s output=%s matched_fee_rows=%s matched_fee_ids=%s unmatched_fee_ids=%s seen_patents=%s",
        len(rows),
        output,
        matched_fee_rows,
        len(matched_fee_ids),
        len(remaining_fee_ids),
        len(seen_patents),
    )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a prefecture-level patent panel directly from the yearly RAR archive")
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE, type=Path)
    parser.add_argument("--cities", default=DEFAULT_CITY_MASTER, type=Path)
    parser.add_argument("--fees", default=DEFAULT_FEE_INFERENCE, type=Path)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    parser.add_argument("--unmatched-fees", default=DEFAULT_UNMATCHED_FEE, type=Path)
    parser.add_argument("--year", action="append", help="Optional year filter; may be repeated or comma-separated")
    parser.add_argument("--no-fill-zeros", action="store_true", help="Do not expand the city-year grid with zero rows")
    parser.add_argument("--log", default=Path("logs/build_city_patent_panel_from_rar.log"), type=Path)
    args = parser.parse_args()
    years = parse_years_arg(args.year)
    build_panel_from_rar(
        archive=args.archive,
        city_master_csv=args.cities,
        fee_inference_csv=args.fees,
        output=args.output,
        unmatched_fee_output=args.unmatched_fees,
        years=years,
        fill_zeros=not args.no_fill_zeros,
        log_path=args.log,
    )


if __name__ == "__main__":
    main()
