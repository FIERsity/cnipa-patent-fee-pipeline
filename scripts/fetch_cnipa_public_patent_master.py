from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from cnipa_public_platform import (
    BASE_URL,
    bootstrap_public_session,
    build_requests_session,
    download_resource_sample,
    fetch_all_catalog,
    filter_catalog,
    iter_zip_xml_files,
    parse_bibliographic_xml,
    save_catalog,
)
from cnipa_utils import configure_logger, ensure_dir, write_csv_rows


DEFAULT_PATENT_MASTER = Path("outputs/patent_master_public.csv")
DEFAULT_CATALOG_JSON = Path("raw/cnipa_public_catalog.json")
DEFAULT_CATALOG_CSV = Path("raw/cnipa_public_catalog.csv")
DEFAULT_DOWNLOAD_DIR = Path("raw/cnipa_public_patent_samples")


def run(
    *,
    data_nos: List[str],
    output: Path,
    catalog_json: Path,
    catalog_csv: Path,
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
    save_catalog(catalog_rows, catalog_json, catalog_csv)
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
        zip_bytes = archive_path.read_bytes()
        parsed = []
        for xml_path, xml_bytes in iter_zip_xml_files(zip_bytes):
            if "INDEX" in xml_path.upper():
                continue
            parsed.extend(
                parse_bibliographic_xml(
                    xml_bytes,
                    source_url=f"{BASE_URL}/public/download?rcId={rc_id}&fileType=2",
                    data_no=data_no,
                    rc_id=rc_id,
                )
            )
        logger.info("parsed %d patent rows from %s", len(parsed), data_no)
        all_rows.extend(parsed)

    # Deduplicate by application number, preferring the first non-empty title/applicant values.
    dedup = {}
    for row in all_rows:
        key = row.get("input_id", "")
        if not key:
            continue
        if key not in dedup:
            dedup[key] = row
            continue
        existing = dedup[key]
        for field in ["title", "applicant", "applicant_address", "province_name", "city_name", "application_date", "publication_date"]:
            if not existing.get(field) and row.get(field):
                existing[field] = row[field]
    rows = list(dedup.values())
    fieldnames = [
        "input_id",
        "input_id_type",
        "application_no",
        "publication_no",
        "title",
        "applicant",
        "applicant_address",
        "province_name",
        "city_name",
        "year",
        "application_date",
        "publication_date",
        "source_url",
        "crawl_time",
        "parse_status",
        "notes",
    ]
    write_csv_rows(output, rows, fieldnames)
    logger.info("wrote %s (%d rows)", output, len(rows))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a patent master from CNIPA public bibliographic sample data")
    parser.add_argument(
        "--data-no",
        action="append",
        dest="data_nos",
        help="CNIPA dataNo to download; may be passed multiple times. Defaults to CN-PA-BIBS-ABSS-10-A/B and CN-PA-BIBS-ABSS-20-U.",
    )
    parser.add_argument("--output", default=DEFAULT_PATENT_MASTER, type=Path)
    parser.add_argument("--catalog-json", default=DEFAULT_CATALOG_JSON, type=Path)
    parser.add_argument("--catalog-csv", default=DEFAULT_CATALOG_CSV, type=Path)
    parser.add_argument("--download-dir", default=DEFAULT_DOWNLOAD_DIR, type=Path)
    parser.add_argument("--log", default=Path("logs/public_patent_master.log"), type=Path)
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()
    data_nos = args.data_nos or ["CN-PA-BIBS-ABSS-10-A", "CN-PA-BIBS-ABSS-10-B", "CN-PA-BIBS-ABSS-20-U"]
    run(
        data_nos=data_nos,
        output=args.output,
        catalog_json=args.catalog_json,
        catalog_csv=args.catalog_csv,
        download_dir=args.download_dir,
        log_path=args.log,
        headless=not args.headed,
    )


if __name__ == "__main__":
    main()
