from __future__ import annotations

import argparse
import json
import logging
import random
from pathlib import Path
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from cnipa_utils import (
    classify_input_type,
    classify_event_text,
    configure_logger,
    dedupe_rows,
    ensure_dir,
    now_iso,
    normalize_input_id,
    random_sleep,
    read_json,
    safe_filename,
)


def _parse_sw_table(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, str]] = []
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [cell.get_text(" ", strip=True) for cell in first_tr.find_all(["th", "td"])]
        if [h.strip() for h in headers] != ["序号", "申请号", "事务数据公告日", "事务数据"]:
            continue
        trs = table.find_all("tr")
        for tr in trs[1:]:
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            rows.append(
                {
                    "seq": tds[0].get_text(" ", strip=True),
                    "application_no": tds[1].get_text(" ", strip=True),
                    "event_date": tds[2].get_text(" ", strip=True),
                    "event_text": tds[3].get_text(" ", strip=True),
                }
            )
    return rows


def _save_snapshot(snapshot_dir: Path, input_id: str, attempt_name: str, html: str) -> Path:
    ensure_dir(snapshot_dir)
    path = snapshot_dir / f"{safe_filename(input_id)}__{attempt_name}.html"
    path.write_text(html, encoding="utf-8")
    return path


def _attempt_query(page, *, input_id: str, input_id_type: str, query_value: str, config: Dict, attempt_name: str, logger: logging.Logger):
    base_url = config["crawl"]["base_url"]
    page.goto(f"{base_url}/Index", wait_until="domcontentloaded", timeout=config["crawl"]["wait_timeout_ms"])
    page.wait_for_timeout(2000)
    try:
        page.get_by_role("link", name="事务查询").click(timeout=10000)
    except Exception:
        logger.info("index click failed, fallback to direct SW for %s", input_id)
    page.wait_for_timeout(2500)
    if not page.locator("#flzt").count():
        page.goto(f"{base_url}/SW", wait_until="domcontentloaded", timeout=config["crawl"]["wait_timeout_ms"])
        page.wait_for_timeout(2500)
    if not page.locator("#flzt").count():
        page.wait_for_timeout(4000)
    if not page.locator("#flzt").count():
        raise RuntimeError("transaction form #flzt not found after retries")

    # Keep the form close to the browser defaults.
    if input_id_type == "application_no":
        page.locator("#an").fill(query_value)
    else:
        page.locator("#swinfo").fill(query_value)
    page.locator("#pageSize").fill(str(config["crawl"]["page_size"]))
    page.wait_for_timeout(800)
    page.locator("#flzt").evaluate("form => form.submit()")
    page.wait_for_load_state("domcontentloaded", timeout=config["crawl"]["wait_timeout_ms"])
    page.wait_for_timeout(3000)

    html = page.content()
    snapshot_dir = Path("raw/html_snapshots")
    snapshot_path = _save_snapshot(snapshot_dir, input_id, attempt_name, html)
    logger.info("saved snapshot %s", snapshot_path)
    return html, page.url


def fetch_single(input_id: str, input_id_type: str, config: Dict, logger: logging.Logger) -> List[Dict[str, object]]:
    normalized_type = classify_input_type(input_id_type, input_id)
    normalized_id = normalize_input_id(input_id)
    fallback_terms = config["crawl"]["fallback_terms"]
    rows: List[Dict[str, object]] = []
    errors: List[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config["browser"]["headless"])
        context = browser.new_context(user_agent=config["browser"]["user_agent"], viewport={"width": 1440, "height": 900})
        page = context.new_page()
        try:
            # Direct query attempt first.
            direct_attempts = []
            if normalized_type == "application_no":
                direct_attempts.append(("direct_an", normalized_id))
            else:
                direct_attempts.append(("direct_keyword", normalized_id))

            for attempt_name, query_value in direct_attempts:
                random_sleep(config["crawl"]["inter_request_min_seconds"], config["crawl"]["inter_request_max_seconds"])
                try:
                    html, url = _attempt_query(page, input_id=input_id, input_id_type=normalized_type, query_value=query_value, config=config, attempt_name=attempt_name, logger=logger)
                    parsed = _parse_sw_table(html)
                    if parsed:
                        rows.extend(parsed)
                        break
                    errors.append(f"{attempt_name}: no table rows")
                except Exception as exc:
                    errors.append(f"{attempt_name}: {exc}")
                    logger.exception("query failed for %s (%s)", input_id, attempt_name)

            # Fallback keyword sweeps for legal-event discovery.
            if not rows:
                for term in fallback_terms:
                    random_sleep(config["crawl"]["inter_request_min_seconds"], config["crawl"]["inter_request_max_seconds"])
                    try:
                        html, url = _attempt_query(page, input_id=input_id, input_id_type="keyword", query_value=term, config=config, attempt_name=f"fallback_{safe_filename(term)}", logger=logger)
                        parsed = _parse_sw_table(html)
                        if not parsed:
                            continue
                        if normalized_type == "application_no":
                            parsed = [r for r in parsed if normalize_input_id(r["application_no"]) == normalized_id]
                        rows.extend(parsed)
                        if rows:
                            break
                    except Exception as exc:
                        errors.append(f"fallback {term}: {exc}")
                        logger.exception("fallback failed for %s with %s", input_id, term)
        finally:
            context.close()
            browser.close()

    rows = dedupe_rows(rows, ["application_no", "event_date", "event_text"])
    if not rows:
        return [
            {
                "input_id": input_id,
                "input_id_type": normalized_type,
                "matched_patent_id": "",
                "title": "",
                "applicant": "",
                "event_date": "",
                "event_name_raw": "",
                "event_text_raw": "",
                "event_category": "other",
                "source_url": "http://epub.cnipa.gov.cn/SW/SwListQuery",
                "crawl_time": now_iso(),
                "parse_status": "no_legal_event_found",
                "notes": "; ".join(errors)[:500],
            }
        ]

    event_rows: List[Dict[str, object]] = []
    for row in rows:
        event_text = row["event_text"]
        event_rows.append(
            {
                "input_id": input_id,
                "input_id_type": normalized_type,
                "matched_patent_id": row["application_no"],
                "title": "",
                "applicant": "",
                "event_date": row["event_date"],
                "event_name_raw": event_text,
                "event_text_raw": event_text,
                "event_category": classify_event_text(event_text),
                "source_url": "http://epub.cnipa.gov.cn/SW/SwListQuery",
                "crawl_time": now_iso(),
                "parse_status": "ok",
                "notes": "; ".join(errors)[:500] if errors else "",
            }
        )
    return event_rows


def fetch_batch(input_csv: Path, config_path: Path, output_csv: Path, log_path: Path) -> List[Dict[str, object]]:
    logger = configure_logger(log_path)
    config = read_json(config_path)
    from cnipa_utils import load_csv_rows, write_csv_rows

    input_rows = load_csv_rows(input_csv)
    all_rows: List[Dict[str, object]] = []
    for item in input_rows:
        input_id = item.get("input_id", "")
        input_id_type = item.get("input_id_type", "")
        if not input_id:
            continue
        logger.info("fetching %s (%s)", input_id, input_id_type)
        try:
            rows = fetch_single(input_id, input_id_type, config, logger)
            all_rows.extend(rows)
            logger.info("collected %d event rows for %s", len(rows), input_id)
        except Exception as exc:
            logger.exception("failed to fetch %s: %s", input_id, exc)
            all_rows.append(
                {
                    "input_id": input_id,
                    "input_id_type": input_id_type,
                    "matched_patent_id": "",
                    "title": "",
                    "applicant": "",
                    "event_date": "",
                    "event_name_raw": "",
                    "event_text_raw": "",
                    "event_category": "other",
                    "source_url": "http://epub.cnipa.gov.cn/SW/SwListQuery",
                    "crawl_time": now_iso(),
                    "parse_status": "fetch_failed",
                    "notes": str(exc)[:500],
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
    write_csv_rows(output_csv, all_rows, fieldnames)
    logger.info("wrote %s (%d rows)", output_csv, len(all_rows))
    return all_rows


def main():
    parser = argparse.ArgumentParser(description="Fetch CNIPA legal-status/transaction events from the public SW page")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--config", default=Path("configs/default.json"), type=Path)
    parser.add_argument("--output", default=Path("outputs/patent_legal_events.csv"), type=Path)
    parser.add_argument("--log", default=Path("logs/run.log"), type=Path)
    args = parser.parse_args()
    fetch_batch(args.input, args.config, args.output, args.log)


if __name__ == "__main__":
    main()
