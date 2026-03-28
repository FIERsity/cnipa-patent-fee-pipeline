from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from cnipa_public_platform import (
    BASE_URL,
    bootstrap_public_session,
    build_requests_session,
    download_resource_sample,
    extract_zip_xml_rows,
    fetch_all_catalog,
    filter_catalog,
    save_catalog,
)
from cnipa_utils import configure_logger, ensure_dir, write_csv_rows


DEFAULT_OUTPUT_ROWS = Path("outputs/cnipa_public_legal_status_events.csv")
DEFAULT_OUTPUT_CATALOG_JSON = Path("raw/cnipa_public_catalog.json")
DEFAULT_OUTPUT_CATALOG_CSV = Path("raw/cnipa_public_catalog.csv")


def run(
    *,
    data_nos: List[str],
    output_rows: Path,
    output_catalog_json: Path,
    output_catalog_csv: Path,
    download_dir: Path,
    log_path: Path,
    headless: bool,
) -> List[dict]:
    logger = configure_logger(log_path)
    logger.info("bootstrapping browser session at %s", BASE_URL)
    bootstrap = bootstrap_public_session(headless=headless)
    session = build_requests_session(bootstrap["cookies"], bootstrap["user_agent"])

    logger.info("fetching public catalog")
    catalog_rows = fetch_all_catalog(session)
    save_catalog(catalog_rows, output_catalog_json, output_catalog_csv)
    selected = filter_catalog(catalog_rows, data_nos)
    logger.info("selected %d catalog resources: %s", len(selected), ", ".join(x.get("dataNo", "") for x in selected))

    all_rows: List[dict] = []
    ensure_dir(download_dir)
    for row in selected:
        rc_id = row["rcId"]
        data_no = row["dataNo"]
        archive_path = download_dir / f"{data_no}.zip"
        logger.info("downloading %s -> %s", data_no, archive_path)
        download_resource_sample(session, rc_id, archive_path, file_type=2)
        rows = extract_zip_xml_rows(
            archive_path.read_bytes(),
            source_url=f"{BASE_URL}/public/download?rcId={rc_id}&fileType=2",
            data_no=data_no,
            rc_id=rc_id,
        )
        all_rows.extend(rows)
        logger.info("parsed %d rows from %s", len(rows), data_no)

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
    write_csv_rows(output_rows, all_rows, fieldnames)
    logger.info("wrote %s (%d rows)", output_rows, len(all_rows))
    return all_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and parse CNIPA public legal-status data from the official data resource platform")
    parser.add_argument(
        "--data-no",
        action="append",
        dest="data_nos",
        help="CNIPA dataNo to download; may be passed multiple times. Defaults to the three China legal-status datasets.",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT_ROWS, type=Path)
    parser.add_argument("--catalog-json", default=DEFAULT_OUTPUT_CATALOG_JSON, type=Path)
    parser.add_argument("--catalog-csv", default=DEFAULT_OUTPUT_CATALOG_CSV, type=Path)
    parser.add_argument("--download-dir", default=Path("raw/cnipa_public_samples"), type=Path)
    parser.add_argument("--log", default=Path("logs/public_catalog.log"), type=Path)
    parser.add_argument("--headed", action="store_true", help="Run the bootstrap browser headed for easier inspection")
    args = parser.parse_args()

    data_nos = args.data_nos or ["CN-PA-PRSS-10", "CN-PA-PRSS-20", "CN-PA-PRSS-30"]
    run(
        data_nos=data_nos,
        output_rows=args.output,
        output_catalog_json=args.catalog_json,
        output_catalog_csv=args.catalog_csv,
        download_dir=args.download_dir,
        log_path=args.log,
        headless=not args.headed,
    )


if __name__ == "__main__":
    main()
