from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

from cnipa_utils import classify_event_text, load_csv_rows, write_csv_rows


def parse_events(input_csv: Path, output_csv: Path) -> List[Dict[str, object]]:
    rows = load_csv_rows(input_csv)
    parsed: List[Dict[str, object]] = []
    for row in rows:
        event_text = (row.get("event_text_raw") or row.get("event_name_raw") or "").strip()
        event_category = row.get("event_category") or classify_event_text(event_text)
        parsed.append(
            {
                **row,
                "event_text_raw": event_text,
                "event_category": event_category,
            }
        )
    fieldnames = [
        "input_id",
        "input_id_type",
        "matched_patent_id",
        "title",
        "applicant",
        "event_date",
        "event_name_raw",
        "event_text_raw",
        "event_category",
        "source_url",
        "crawl_time",
        "parse_status",
        "notes",
    ]
    write_csv_rows(output_csv, parsed, fieldnames)
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Normalize legal event rows")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", default=Path("outputs/patent_legal_events.csv"), type=Path)
    args = parser.parse_args()
    parse_events(args.input, args.output)


if __name__ == "__main__":
    main()
