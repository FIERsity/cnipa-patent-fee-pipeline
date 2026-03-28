from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Sequence

from curl_cffi import requests as creq

from cnipa_utils import (
    classify_event_category,
    classify_input_type,
    configure_logger,
    ensure_dir,
    normalize_input_id,
    normalize_patent_join_key,
    normalize_patentstar_application_no,
    now_iso,
    random_sleep,
    write_csv_rows,
)


PATENTSTAR_BASE = "https://cprs.patentstar.com.cn"
SEARCH_BY_QUERY_URL = f"{PATENTSTAR_BASE}/Search/SearchByQuery"
GET_FLZT_URL = f"{PATENTSTAR_BASE}/WebService/GetFLZT"
DEFAULT_OUTPUT = Path("outputs/patentstar_legal_events_http.csv")
DEFAULT_LOG = Path("logs/fetch_patentstar_legal_status_http.log")
DEFAULT_STATE = Path("output/playwright/patentstar_state.json")
DEFAULT_BATCH_SIZE = 3000
DEFAULT_DETAIL_WORKERS = 2
LG_LABELS = {
    "1": "有效",
    "2": "失效",
    "3": "审中",
}


def _load_state_cookies(state_path: Path):
    state = json.loads(state_path.read_text(encoding="utf-8"))
    cookies = state.get("cookies", [])
    if not cookies:
        raise RuntimeError(f"no cookies found in {state_path}")
    return cookies


class PatentStarHttpClient:
    def __init__(self, state_path: Path, logger: logging.Logger):
        self.logger = logger
        self.state_path = state_path
        self.cookies = _load_state_cookies(state_path)
        self._local = threading.local()

    def _new_session(self):
        session = creq.Session(impersonate="chrome")
        for cookie in self.cookies:
            session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain"),
                path=cookie.get("path", "/"),
            )
        return session

    def _session(self):
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._new_session()
            self._local.session = session
        return session

    def _headers(self, referer: str) -> Dict[str, str]:
        return {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": PATENTSTAR_BASE,
            "Referer": referer,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        }

    def search_by_query(self, id_list: str, row_count: int = DEFAULT_BATCH_SIZE, page_num: int = 1) -> Dict[str, object]:
        session = self._session()
        body = {
            "CurrentQuery": "",
            "OrderBy": "AD",
            "OrderByType": "DESC",
            "PageNum": str(page_num),
            "DBType": "CN",
            "RowCount": str(row_count),
            "Filter": json.dumps({"CO": "", "PT": "", "LG": ""}, ensure_ascii=False),
            "SecSearch": "",
            "IdList": id_list,
        }
        resp = session.post(
            SEARCH_BY_QUERY_URL,
            headers=self._headers(f"{PATENTSTAR_BASE}/Search/ListSearchResult?listid=local&type=cn"),
            data=body,
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("Ret") != 200:
            raise RuntimeError(f"SearchByQuery failed: {payload}")
        data = payload.get("Data") or {}
        return data

    def get_flzt(self, ane: str) -> List[Dict[str, str]]:
        last_exc = None
        for attempt in range(3):
            try:
                session = self._session()
                resp = session.post(
                    GET_FLZT_URL,
                    headers=self._headers(f"{PATENTSTAR_BASE}/Search/Detail?ANE={ane}"),
                    data={"ANE": ane},
                    timeout=60,
                )
                resp.raise_for_status()
                payload = resp.json()
                if payload.get("Ret") != 200:
                    raise RuntimeError(f"GetFLZT failed: {payload}")
                return json.loads(payload.get("Data") or "[]")
            except Exception as exc:
                last_exc = exc
                if attempt < 2:
                    time_sleep = 0.4 * (attempt + 1) + random.random() * 0.4
                    self.logger.debug("retry GetFLZT ane=%s attempt=%s sleep=%.2f", ane, attempt + 1, time_sleep)
                    time.sleep(time_sleep)
                continue
        raise RuntimeError(f"GetFLZT failed after retries for {ane}: {last_exc}")


def _normalize_patentstar_id(input_id: str, input_id_type: str = "") -> str:
    normalized_type = classify_input_type(input_id_type, input_id)
    if normalized_type == "application_no":
        return normalize_patentstar_application_no(input_id)
    return normalize_patentstar_application_no(normalize_input_id(input_id))


def _format_date(yyyymmdd: str) -> str:
    text = (yyyymmdd or "").strip()
    if len(text) != 8 or not text.isdigit():
        return text
    return f"{text[:4]}.{text[4:6]}.{text[6:8]}"


def _parse_event_row(event: Dict[str, str]) -> Dict[str, str]:
    legal_status = (event.get("LegalStatus") or "").strip()
    legal_status_info = (event.get("LegalStatusInfo") or "").strip()
    detail = (event.get("DETAIL") or "").strip()
    status_text = f"{legal_status} {legal_status_info} {detail}".strip()
    return {
        "event_date": _format_date(event.get("LegalDate", "")),
        "event_name_raw": legal_status,
        "event_text_raw": legal_status_info or legal_status,
        "event_category": classify_event_category(status_text),
        "detail_text_raw": detail,
    }


def _summary_row(
    *,
    input_id: str,
    input_id_type: str,
    matched_patent_id: str,
    title: str,
    applicant: str,
    current_status_label: str,
    source_url: str,
    current_status_code: str,
    ane: str,
    co: str,
    ct: str,
) -> Dict[str, str]:
    return {
        "input_id": input_id,
        "input_id_type": input_id_type,
        "matched_patent_id": matched_patent_id,
        "title": title,
        "applicant": applicant,
        "event_date": "",
        "event_name_raw": current_status_label,
        "event_text_raw": f"当前状态：{current_status_label}",
        "event_category": "other",
        "source_url": source_url,
        "crawl_time": now_iso(),
        "parse_status": "summary_only",
        "notes": f"ane={ane}; lg={current_status_code}; co={co}; ct={ct}",
    }


def fetch_patentstar_batch(
    *,
    input_rows: Sequence[Dict[str, str]],
    client: PatentStarHttpClient,
    logger: logging.Logger,
    detail_only_on_invalid: bool = True,
    batch_row_count: int = DEFAULT_BATCH_SIZE,
    detail_workers: int = DEFAULT_DETAIL_WORKERS,
    inter_request_min_seconds: float = 0.15,
    inter_request_max_seconds: float = 0.6,
) -> List[Dict[str, str]]:
    outputs: List[Dict[str, str]] = []

    normalized_input = []
    for row in input_rows:
        input_id = (row.get("input_id") or "").strip()
        if not input_id:
            continue
        normalized = _normalize_patentstar_id(input_id, row.get("input_id_type", ""))
        normalized_input.append(
            {
                "input_id": normalize_input_id(input_id),
                "input_id_type": classify_input_type(row.get("input_id_type", ""), input_id),
                "patentstar_id": normalized,
            }
        )

    if not normalized_input:
        return outputs

    id_list = "|".join(item["patentstar_id"] for item in normalized_input)
    input_lookup = {item["patentstar_id"]: item for item in normalized_input}
    logger.info("searching PatentStar HTTP batch with %d ids", len(normalized_input))
    data = client.search_by_query(id_list=id_list, row_count=batch_row_count, page_num=1)
    hit_count = int(data.get("HitCount") or 0)
    rows = data.get("List") or []
    all_rows = list(rows)
    logger.info("search returned page 1 with %d rows (hit_count=%d)", len(rows), hit_count)

    page_num = 1
    while rows:
        page_num += 1
        if hit_count and len(all_rows) >= hit_count:
            break
        data = client.search_by_query(id_list=id_list, row_count=batch_row_count, page_num=page_num)
        rows = data.get("List") or []
        if not rows:
            break
        all_rows.extend(rows)
        logger.info("search returned page %d with %d rows (accumulated=%d)", page_num, len(rows), len(all_rows))
        if hit_count and len(all_rows) >= hit_count:
            break

    rows = all_rows
    logger.info("search accumulated %d rows total", len(rows))

    seen = set()
    detail_targets = []
    for row in rows:
        ane = (row.get("ANE") or "").strip()
        an = normalize_input_id(row.get("AN") or "")
        current_status_code = str(row.get("LG") or "").strip()
        current_status_label = LG_LABELS.get(current_status_code, current_status_code)
        title = (row.get("TI") or "").strip()
        applicant = (row.get("PA") or "").strip()
        co = (row.get("CO") or "").strip()
        ct = str(row.get("CT") or "").strip()
        matched_id = normalize_patentstar_application_no(row.get("AN") or "")
        seen.add(matched_id)

        input_row = input_lookup.get(matched_id, {})
        summary = _summary_row(
            input_id=input_row.get("input_id", matched_id),
            input_id_type=input_row.get("input_id_type", "application_no"),
            matched_patent_id=matched_id,
            title=title,
            applicant=applicant,
            current_status_label=current_status_label,
            source_url=SEARCH_BY_QUERY_URL,
            current_status_code=current_status_code,
            ane=ane,
            co=co,
            ct=ct,
        )
        outputs.append(summary)

        if detail_only_on_invalid and current_status_code != "2":
            continue
        if not ane:
            continue
        detail_targets.append(
            {
                "input_id": input_row.get("input_id", matched_id),
                "input_id_type": input_row.get("input_id_type", "application_no"),
                "matched_patent_id": matched_id,
                "title": title,
                "applicant": applicant,
                "current_status_label": current_status_label,
                "ane": ane,
            }
        )

    def _fetch_detail(target: Dict[str, str]) -> List[Dict[str, str]]:
        ane = target["ane"]
        random_sleep(inter_request_min_seconds, inter_request_max_seconds)
        try:
            events = client.get_flzt(ane)
            if not events:
                return [
                    {
                        "input_id": target["input_id"],
                        "input_id_type": target["input_id_type"],
                        "matched_patent_id": target["matched_patent_id"],
                        "title": target["title"],
                        "applicant": target["applicant"],
                        "event_date": "",
                        "event_name_raw": "",
                        "event_text_raw": "",
                        "event_category": "other",
                        "source_url": f"{PATENTSTAR_BASE}/WebService/GetFLZT",
                        "crawl_time": now_iso(),
                        "parse_status": "detail_empty",
                        "notes": f"ane={ane}; current_status={target['current_status_label']}",
                    }
                ]
            rows_out = []
            for event in events:
                parsed = _parse_event_row(event)
                rows_out.append(
                    {
                        "input_id": target["input_id"],
                        "input_id_type": target["input_id_type"],
                        "matched_patent_id": target["matched_patent_id"],
                        "title": target["title"],
                        "applicant": target["applicant"],
                        "event_date": parsed["event_date"],
                        "event_name_raw": parsed["event_name_raw"],
                        "event_text_raw": parsed["event_text_raw"],
                        "event_category": parsed["event_category"],
                        "source_url": f"{PATENTSTAR_BASE}/WebService/GetFLZT",
                        "crawl_time": now_iso(),
                        "parse_status": "ok",
                        "notes": f"ane={ane}; current_status={target['current_status_label']}; {parsed['detail_text_raw']}",
                    }
                )
            return rows_out
        except Exception as exc:
            logger.exception("detail fetch failed for %s: %s", target["matched_patent_id"], exc)
            return [
                {
                    "input_id": target["input_id"],
                    "input_id_type": target["input_id_type"],
                    "matched_patent_id": target["matched_patent_id"],
                    "title": target["title"],
                    "applicant": target["applicant"],
                    "event_date": "",
                    "event_name_raw": "",
                    "event_text_raw": "",
                    "event_category": "other",
                    "source_url": f"{PATENTSTAR_BASE}/WebService/GetFLZT",
                    "crawl_time": now_iso(),
                    "parse_status": "detail_failed",
                    "notes": f"ane={ane}; current_status={target['current_status_label']}; {exc}",
                }
            ]

    if detail_targets:
        if detail_workers <= 1:
            for target in detail_targets:
                outputs.extend(_fetch_detail(target))
        else:
            with ThreadPoolExecutor(max_workers=detail_workers) as executor:
                futures = {executor.submit(_fetch_detail, target): target for target in detail_targets}
                for future in as_completed(futures):
                    target = futures[future]
                    try:
                        outputs.extend(future.result())
                    except Exception as exc:
                        logger.exception("detail worker crashed for %s: %s", target["matched_patent_id"], exc)
                        outputs.append(
                            {
                                "input_id": target["input_id"],
                                "input_id_type": target["input_id_type"],
                                "matched_patent_id": target["matched_patent_id"],
                                "title": target["title"],
                                "applicant": target["applicant"],
                                "event_date": "",
                                "event_name_raw": "",
                                "event_text_raw": "",
                                "event_category": "other",
                                "source_url": f"{PATENTSTAR_BASE}/WebService/GetFLZT",
                                "crawl_time": now_iso(),
                                "parse_status": "detail_failed",
                                "notes": f"ane={target['ane']}; current_status={target['current_status_label']}; {exc}",
                            }
                        )

    missing = [item for item in normalized_input if item["patentstar_id"] not in seen]
    for item in missing:
        outputs.append(
            {
                "input_id": item["input_id"],
                "input_id_type": item["input_id_type"],
                "matched_patent_id": "",
                "title": "",
                "applicant": "",
                "event_date": "",
                "event_name_raw": "",
                "event_text_raw": "",
                "event_category": "other",
                "source_url": SEARCH_BY_QUERY_URL,
                "crawl_time": now_iso(),
                "parse_status": "not_found",
                "notes": "not returned by SearchByQuery",
            }
        )

    return outputs


def run_pipeline(
    input_csv: Path,
    output_csv: Path,
    log_path: Path,
    state_path: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    detail_workers: int = DEFAULT_DETAIL_WORKERS,
    detail_only_on_invalid: bool = True,
) -> List[Dict[str, str]]:
    logger = configure_logger(log_path)
    client = PatentStarHttpClient(state_path=state_path, logger=logger)
    input_rows = []
    with input_csv.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            input_rows.append(row)
    logger.info("loaded %d input rows from %s", len(input_rows), input_csv)

    all_outputs: List[Dict[str, str]] = []
    for idx in range(0, len(input_rows), batch_size):
        batch = input_rows[idx : idx + batch_size]
        if not batch:
            continue
        logger.info("processing batch %d-%d", idx + 1, idx + len(batch))
        try:
            outputs = fetch_patentstar_batch(
                input_rows=batch,
                client=client,
                logger=logger,
                detail_only_on_invalid=detail_only_on_invalid,
                batch_row_count=batch_size,
                detail_workers=detail_workers,
            )
            all_outputs.extend(outputs)
            logger.info("batch produced %d rows", len(outputs))
        except Exception as exc:
            logger.exception("batch failed %d-%d: %s", idx + 1, idx + len(batch), exc)
            for row in batch:
                input_id = row.get("input_id", "")
                if not input_id:
                    continue
                all_outputs.append(
                    {
                        "input_id": input_id,
                        "input_id_type": row.get("input_id_type", ""),
                        "matched_patent_id": "",
                        "title": "",
                        "applicant": "",
                        "event_date": "",
                        "event_name_raw": "",
                        "event_text_raw": "",
                        "event_category": "other",
                        "source_url": SEARCH_BY_QUERY_URL,
                        "crawl_time": now_iso(),
                        "parse_status": "batch_failed",
                        "notes": str(exc),
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
    write_csv_rows(output_csv, all_outputs, fieldnames)
    logger.info("wrote %s (%d rows)", output_csv, len(all_outputs))
    return all_outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch PatentStar legal-status rows by application number over HTTP")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    parser.add_argument("--log", default=DEFAULT_LOG, type=Path)
    parser.add_argument("--state", default=DEFAULT_STATE, type=Path, help="Playwright storageState JSON exported after login")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--detail-workers", type=int, default=DEFAULT_DETAIL_WORKERS, help="Parallel workers for GetFLZT detail fetches")
    parser.add_argument("--all-details", action="store_true", help="Fetch detail JSON for all visible rows, not only invalid statuses.")
    args = parser.parse_args()
    ensure_dir(args.output.parent)
    ensure_dir(args.log.parent)
    run_pipeline(
        input_csv=args.input,
        output_csv=args.output,
        log_path=args.log,
        state_path=args.state,
        batch_size=args.batch_size,
        detail_workers=max(1, min(args.detail_workers, 4)),
        detail_only_on_invalid=not args.all_details,
    )


if __name__ == "__main__":
    main()
