from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from cnipa_utils import INFERRED_FEE_STATUS, load_csv_rows, write_csv_rows


def infer_from_group(events: List[Dict[str, str]]) -> Dict[str, str]:
    event_categories = [r.get("event_category") or "" for r in events]
    event_texts = [((r.get("event_text_raw") or "") + " " + (r.get("event_name_raw") or "")).strip() for r in events]

    has_annual_fee_termination = any(cat.startswith("annual_fee_nonpayment_termination") for cat in event_categories)
    has_annual_fee_termination_final = any(cat == "annual_fee_nonpayment_termination_final" for cat in event_categories)
    has_annual_fee_termination_restorable = any(cat == "annual_fee_nonpayment_termination_restorable" for cat in event_categories)
    has_deemed_abandoned = any(cat == "deemed_abandoned" for cat in event_categories)
    has_restoration = any(cat == "right_restoration" for cat in event_categories)
    has_unspecified_termination = any(cat == "termination_unspecified" for cat in event_categories)

    annual_fee_termination_date = ""
    deemed_abandoned_date = ""
    restoration_date = ""
    unspecified_termination_date = ""
    for r in events:
        if not annual_fee_termination_date and r.get("event_category") == "annual_fee_nonpayment_termination":
            annual_fee_termination_date = r.get("event_date", "")
        if not deemed_abandoned_date and r.get("event_category") == "deemed_abandoned":
            deemed_abandoned_date = r.get("event_date", "")
        if not restoration_date and r.get("event_category") == "right_restoration":
            restoration_date = r.get("event_date", "")
        if not unspecified_termination_date and r.get("event_category") == "termination_unspecified":
            unspecified_termination_date = r.get("event_date", "")

    if has_restoration and annual_fee_termination_date:
        inferred = "restored_after_lapse"
        rule = "right_restoration_after_annual_fee_termination"
        confidence = "high"
    elif has_annual_fee_termination:
        inferred = "likely_stopped_payment_due_to_fee_nonpayment"
        rule = "annual_fee_nonpayment_termination_event_present"
        confidence = "high"
    elif has_deemed_abandoned:
        inferred = "deemed_abandoned"
        rule = "deemed_abandoned_event_present"
        confidence = "high"
    elif has_unspecified_termination:
        inferred = "ambiguous"
        rule = "termination_event_without_fee_context"
        confidence = "medium"
    elif not events or all((r.get("parse_status") or "") in {"no_legal_event_found", "fetch_failed"} for r in events):
        inferred = "no_legal_event_found"
        rule = "no_rows_or_fetch_failed"
        confidence = "low"
    else:
        inferred = "ambiguous"
        rule = "no_explicit_annual_fee_event_found"
        confidence = "medium"

    notes = []
    if has_annual_fee_termination_final:
        notes.append("annual_fee_termination_variant=final")
    if has_annual_fee_termination_restorable:
        notes.append("annual_fee_termination_variant=restorable")
    if restoration_date:
        notes.append(f"restoration_date={restoration_date}")
    if annual_fee_termination_date:
        notes.append(f"annual_fee_termination_date={annual_fee_termination_date}")
    if deemed_abandoned_date:
        notes.append(f"deemed_abandoned_date={deemed_abandoned_date}")
    if unspecified_termination_date:
        notes.append(f"unspecified_termination_date={unspecified_termination_date}")

    return {
        "has_annual_fee_termination_event": str(has_annual_fee_termination).lower(),
        "annual_fee_termination_date": annual_fee_termination_date,
        "has_annual_fee_termination_final_event": str(has_annual_fee_termination_final).lower(),
        "has_annual_fee_termination_restorable_event": str(has_annual_fee_termination_restorable).lower(),
        "has_deemed_abandoned_event": str(has_deemed_abandoned).lower(),
        "deemed_abandoned_date": deemed_abandoned_date,
        "has_right_restoration_event": str(has_restoration).lower(),
        "restoration_date": restoration_date,
        "inferred_fee_status": inferred,
        "inferred_fee_status_rule": rule,
        "confidence_level": confidence,
        "notes": "; ".join(notes),
        "panel_exclusion_recommendation": "exclude" if (has_annual_fee_termination or has_deemed_abandoned or has_unspecified_termination or has_restoration) else "keep",
    }


def infer_fee_status(input_csv: Path, output_csv: Path) -> List[Dict[str, str]]:
    rows = load_csv_rows(input_csv)
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    meta: Dict[str, Dict[str, str]] = {}
    for row in rows:
        key = row.get("input_id", "")
        grouped[key].append(row)
        meta.setdefault(key, {"input_id": key})

    outputs: List[Dict[str, str]] = []
    for input_id, events in grouped.items():
        result = infer_from_group(events)
        output = {"input_id": input_id, **result}
        outputs.append(output)

    fieldnames = [
        "input_id",
        "has_annual_fee_termination_event",
        "annual_fee_termination_date",
        "has_annual_fee_termination_final_event",
        "has_annual_fee_termination_restorable_event",
        "has_deemed_abandoned_event",
        "deemed_abandoned_date",
        "has_right_restoration_event",
        "restoration_date",
        "inferred_fee_status",
        "inferred_fee_status_rule",
        "confidence_level",
        "notes",
        "panel_exclusion_recommendation",
    ]
    write_csv_rows(output_csv, outputs, fieldnames)
    return outputs


def main():
    parser = argparse.ArgumentParser(description="Infer annual-fee continuity from legal events")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", default=Path("outputs/patent_fee_inference.csv"), type=Path)
    args = parser.parse_args()
    infer_fee_status(args.input, args.output)


if __name__ == "__main__":
    main()
