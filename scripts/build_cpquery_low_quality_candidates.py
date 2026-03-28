from __future__ import annotations

import argparse
import csv
from pathlib import Path

from cnipa_utils import normalize_input_id, normalize_patent_join_key


def build_candidates(master_csv: Path, fee_csv: Path, output_csv: Path) -> int:
    fee_map = {}
    with fee_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            key = normalize_patent_join_key(row.get("input_id", ""))
            if key:
                fee_map[key] = row

    seen = set()
    count = 0
    with master_csv.open("r", encoding="utf-8-sig", newline="") as fh, output_csv.open("w", encoding="utf-8", newline="") as oh:
        reader = csv.DictReader(fh)
        writer = csv.DictWriter(oh, fieldnames=["input_id", "input_id_type", "year", "city_name", "city_adcode", "low_quality_reason"])
        writer.writeheader()
        for row in reader:
            input_id = normalize_patent_join_key(row.get("input_id", ""))
            if not input_id or input_id in seen:
                continue
            fee = fee_map.get(input_id)
            if not fee:
                continue
            is_term = (fee.get("has_annual_fee_termination_event") or "").strip().lower() == "true"
            is_abandoned = (fee.get("has_deemed_abandoned_event") or "").strip().lower() == "true"
            is_unspecified = (fee.get("inferred_fee_status_rule") or "").strip() == "termination_event_without_fee_context"
            if not (is_term or is_abandoned or is_unspecified):
                continue
            seen.add(input_id)
            reason = []
            if is_term:
                reason.append("fee_termination")
            if is_abandoned:
                reason.append("deemed_abandoned")
            if is_unspecified:
                reason.append("unspecified_termination")
            writer.writerow(
                {
                    "input_id": input_id,
                    "input_id_type": "application_no",
                    "year": row.get("year", ""),
                    "city_name": row.get("city_name", ""),
                    "city_adcode": row.get("city_adcode", ""),
                    "low_quality_reason": "+".join(reason),
                }
            )
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Build low-quality patent candidates for cpquery batch checks")
    parser.add_argument("--master", required=True, type=Path)
    parser.add_argument("--fees", required=True, type=Path)
    parser.add_argument("--output", default=Path("outputs/cpquery_low_quality_candidates.csv"), type=Path)
    args = parser.parse_args()
    count = build_candidates(args.master, args.fees, args.output)
    print(count)


if __name__ == "__main__":
    main()
