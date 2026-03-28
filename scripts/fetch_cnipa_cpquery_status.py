from __future__ import annotations

import argparse
import json
import logging
import random
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from cnipa_utils import (
    classify_cpquery_case_status,
    classify_input_type,
    configure_logger,
    dedupe_rows,
    ensure_dir,
    load_csv_rows,
    now_iso,
    normalize_input_id,
    normalize_patent_join_key,
    random_sleep,
    read_json,
    safe_filename,
    write_csv_rows,
)


DEFAULT_CONFIG = Path("configs/default.json")
DEFAULT_INPUT = Path("raw/sample_patent_ids.csv")
DEFAULT_OUTPUT = Path("outputs/patent_cpquery_status.csv")
DEFAULT_LOG = Path("logs/fetch_cnipa_cpquery_status.log")
DEFAULT_STATE = Path("output/playwright/cpquery.state.json")
DEFAULT_FALLBACK_STATE = Path("output/playwright/pss-system.state.json")


def _page_is_login(page) -> bool:
    url = page.url or ""
    title = page.title() or ""
    return "tysf.cponline.cnipa.gov.cn/am/#/user/login" in url or "统一身份认证平台" in title


def _page_is_blank(page) -> bool:
    try:
        body = page.evaluate("() => document.body ? document.body.innerText.trim() : ''")
    except Exception:
        body = ""
    return not body


def _choose_state_path(state_path: Path) -> Optional[Path]:
    if state_path.exists():
        return state_path
    if DEFAULT_FALLBACK_STATE.exists():
        return DEFAULT_FALLBACK_STATE
    return None


def _save_snapshot(snapshot_dir: Path, input_id: str, attempt_name: str, page) -> Path:
    ensure_dir(snapshot_dir)
    path = snapshot_dir / f"{safe_filename(input_id)}__{attempt_name}.html"
    try:
        html = page.content()
    except Exception:
        html = ""
    path.write_text(html, encoding="utf-8")
    return path


def _wait_for_manual_login(page, timeout_seconds: int, logger: logging.Logger) -> None:
    if timeout_seconds <= 0:
        raise RuntimeError(
            "official query page redirected to login; rerun with --wait-for-login-seconds or provide a valid saved state"
        )
    logger.info(
        "official query redirected to login; please finish login in the headed browser window, then wait for the portal to load"
    )
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not _page_is_login(page) and not _page_is_blank(page):
            return
        time.sleep(2.0)
    raise RuntimeError("timeout waiting for manual login to complete")


def _discover_and_submit_query(page, query_value: str, logger: logging.Logger) -> Dict[str, object]:
    script = """
    ({value}) => {
      const visible = (el) => {
        const rect = el.getBoundingClientRect();
        const style = window.getComputedStyle(el);
        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
      };
      const setNativeValue = (el, value) => {
        const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
        const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
        if (setter) {
          setter.call(el, value);
        } else {
          el.value = value;
        }
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'Enter' }));
      };
      const tokens = ['申请号', '专利号', '案件编号', '公开', '公告', '检索', '查询', '编号'];
      const inputs = Array.from(document.querySelectorAll('input, textarea')).filter(visible);
      const scored = inputs.map((el, idx) => {
        const attrs = [
          el.placeholder || '',
          el.getAttribute('aria-label') || '',
          el.name || '',
          el.id || '',
          el.getAttribute('title') || '',
          el.className || ''
        ].join(' ');
        let score = 0;
        for (const token of tokens) {
          if (attrs.includes(token)) score += 10;
        }
        if (el.type === 'text' || el.tagName === 'TEXTAREA') score += 5;
        if (el.tagName === 'TEXTAREA') score += 1;
        return { idx, score, attrs, tag: el.tagName, type: el.type || '', placeholder: el.placeholder || '' };
      }).sort((a, b) => b.score - a.score);
      const target = scored.length ? inputs[scored[0].idx] : null;
      if (!target) {
        return { ok: false, reason: 'no visible input', inputs: scored.slice(0, 5) };
      }
      setNativeValue(target, value);
      target.focus();
      const buttons = Array.from(document.querySelectorAll('button, a, span, div')).filter(visible);
      const buttonTerms = ['查询', '检索', '搜索'];
      const clickTarget = buttons.find(el => buttonTerms.some(t => (el.textContent || '').trim() === t || (el.textContent || '').includes(t)));
      if (clickTarget) {
        clickTarget.click();
      } else if (target.form) {
        target.form.submit();
      } else {
        target.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'Enter' }));
      }
      return {
        ok: true,
        target: {
          tag: target.tagName,
          type: target.type || '',
          placeholder: target.placeholder || '',
          name: target.name || '',
          id: target.id || '',
        },
        clicked: clickTarget ? (clickTarget.textContent || '').trim().slice(0, 100) : ''
      };
    }
    """
    return page.evaluate(script, {"value": query_value})


def _parse_tables(html: str) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html, "lxml")
    tables: List[Dict[str, object]] = []
    for idx, table in enumerate(soup.find_all("table")):
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [cell.get_text(" ", strip=True) for cell in first_tr.find_all(["th", "td"])]
        headers = [h.strip() for h in headers if h and h.strip()]
        if not headers:
            continue
        records: List[Dict[str, str]] = []
        for tr in table.find_all("tr")[1:]:
            cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["td", "th"])]
            if not cells:
                continue
            record: Dict[str, str] = {}
            for i, header in enumerate(headers):
                if i < len(cells):
                    record[header] = cells[i]
            if record:
                records.append(record)
        tables.append({"index": idx, "headers": headers, "records": records})
    return tables


def _score_table(table: Dict[str, object]) -> int:
    headers = [str(h) for h in table.get("headers") or []]
    score = 0
    header_text = " ".join(headers)
    for token in ["法律状态", "当前案件状态", "案件状态"]:
        if token in header_text:
            score += 10
    for token in ["申请号", "专利号", "发明名称", "申请人", "名称"]:
        if token in header_text:
            score += 3
    score += len(table.get("records") or [])
    return score


def _extract_case_status_from_record(record: Dict[str, str]) -> str:
    for key in record.keys():
        if "当前案件状态" in key or "法律状态" in key or "案件状态" in key:
            return record.get(key, "").strip()
    return ""


def _build_rows(
    *,
    input_id: str,
    input_id_type: str,
    page_url: str,
    tables: Sequence[Dict[str, object]],
    notes: str,
) -> List[Dict[str, object]]:
    best = None
    best_score = -1
    for table in tables:
        score = _score_table(table)
        if score > best_score:
            best_score = score
            best = table
    if not best:
        return []
    rows: List[Dict[str, object]] = []
    for record in best.get("records") or []:
        if not isinstance(record, dict):
            continue
        patent_id = ""
        title = ""
        applicant = ""
        application_date = ""
        status_raw = _extract_case_status_from_record(record)
        for key, value in record.items():
            key_text = str(key)
            value_text = str(value).strip()
            if not value_text:
                continue
            if any(token in key_text for token in ["申请号", "专利号", "申请/专利号"]):
                patent_id = value_text
            elif "发明名称" in key_text or "名称" in key_text:
                title = value_text
            elif "申请人" in key_text:
                applicant = value_text
            elif "申请日" in key_text:
                application_date = value_text
            elif "法律状态" in key_text or "当前案件状态" in key_text or "案件状态" in key_text:
                status_raw = value_text
        if not patent_id and input_id_type == "application_no":
            patent_id = input_id
        status_category = classify_cpquery_case_status(status_raw)
        rows.append(
            {
                "input_id": input_id,
                "input_id_type": input_id_type,
                "matched_patent_id": normalize_input_id(patent_id) if patent_id else "",
                "title": title,
                "applicant": applicant,
                "application_date": application_date,
                "current_case_status_raw": status_raw,
                "current_case_status_category": status_category,
                "event_name_raw": status_raw,
                "event_text_raw": status_raw,
                "event_category": status_category,
                "source_url": page_url,
                "crawl_time": now_iso(),
                "parse_status": "ok",
                "notes": notes,
            }
        )
    return rows


def fetch_single(
    input_id: str,
    input_id_type: str,
    config: Dict[str, object],
    logger: logging.Logger,
    *,
    state_path: Path,
    headed: bool,
    wait_for_login_seconds: int,
) -> List[Dict[str, object]]:
    normalized_type = classify_input_type(input_id_type, input_id)
    query_value = normalize_patent_join_key(input_id) or normalize_input_id(input_id)
    base_url = config["official_query"]["base_url"]
    inter_request_min_seconds = float(config["official_query"]["inter_request_min_seconds"])
    inter_request_max_seconds = float(config["official_query"]["inter_request_max_seconds"])
    retry_count = int(config["official_query"]["retry_count"])
    wait_timeout_ms = int(config["official_query"]["wait_timeout_ms"])
    snapshot_dir = Path("raw/html_snapshots")

    state = _choose_state_path(state_path)
    rows: List[Dict[str, object]] = []
    errors: List[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        context_kwargs: Dict[str, object] = {
            "user_agent": config["browser"]["user_agent"],
            "viewport": {"width": 1440, "height": 900},
        }
        if state is not None:
            context_kwargs["storage_state"] = str(state)
        context = browser.new_context(**context_kwargs)
        page = context.new_page()
        try:
            for attempt in range(retry_count + 1):
                if attempt:
                    random_sleep(inter_request_min_seconds, inter_request_max_seconds)
                try:
                    page.goto(base_url, wait_until="domcontentloaded", timeout=wait_timeout_ms)
                    page.wait_for_timeout(2500)
                    if _page_is_login(page):
                        _save_snapshot(snapshot_dir, input_id, f"login_{attempt}", page)
                        _wait_for_manual_login(page, wait_for_login_seconds, logger)
                    if _page_is_blank(page):
                        raise RuntimeError("official query page rendered blank body")

                    query_info = _discover_and_submit_query(page, query_value, logger)
                    if not query_info.get("ok"):
                        raise RuntimeError(f"query input discovery failed: {query_info}")
                    page.wait_for_timeout(3000)
                    html = page.content()
                    _save_snapshot(snapshot_dir, input_id, f"query_{attempt}", page)
                    tables = _parse_tables(html)
                    if not tables:
                        body = page.evaluate("() => document.body ? document.body.innerText.slice(0, 2000) : ''")
                        errors.append(f"attempt {attempt}: no tables; body={body[:500]}")
                        continue
                    built = _build_rows(
                        input_id=input_id,
                        input_id_type=normalized_type,
                        page_url=page.url,
                        tables=tables,
                        notes=f"query_value={query_value}; attempt={attempt}; selected={query_info}",
                    )
                    if built:
                        rows.extend(built)
                        break
                    errors.append(f"attempt {attempt}: no rows from parsed tables")
                except Exception as exc:
                    errors.append(f"attempt {attempt}: {exc}")
                    logger.exception("query failed for %s on attempt %d", input_id, attempt)
                    if attempt >= retry_count:
                        break
        finally:
            context.close()
            browser.close()

    if not rows:
        blocked_terms = ("login", "blank body", "412", "precondition", "bad request")
        status = "login_required_or_blocked" if any(any(term in err.lower() for term in blocked_terms) for err in errors) else "no_legal_event_found"
        return [
            {
                "input_id": input_id,
                "input_id_type": normalized_type,
                "matched_patent_id": "",
                "title": "",
                "applicant": "",
                "application_date": "",
                "current_case_status_raw": "",
                "current_case_status_category": "",
                "event_name_raw": "",
                "event_text_raw": "",
                "event_category": "other",
                "source_url": base_url,
                "crawl_time": now_iso(),
                "parse_status": status,
                "notes": "; ".join(errors)[:1000],
            }
        ]
    return rows


def fetch_batch(
    input_csv: Path,
    config_path: Path,
    output_csv: Path,
    log_path: Path,
    *,
    state_path: Path,
    headed: bool,
    wait_for_login_seconds: int,
) -> List[Dict[str, object]]:
    logger = configure_logger(log_path)
    config = read_json(config_path)
    input_rows = load_csv_rows(input_csv)
    all_rows: List[Dict[str, object]] = []

    for item in input_rows:
        input_id = (item.get("input_id") or "").strip()
        input_id_type = (item.get("input_id_type") or "").strip()
        if not input_id:
            continue
        logger.info("querying %s (%s)", input_id, input_id_type)
        try:
            rows = fetch_single(
                input_id,
                input_id_type,
                config,
                logger,
                state_path=state_path,
                headed=headed,
                wait_for_login_seconds=wait_for_login_seconds,
            )
            all_rows.extend(rows)
            logger.info("collected %d rows for %s", len(rows), input_id)
        except Exception as exc:
            logger.exception("failed to query %s: %s", input_id, exc)
            all_rows.append(
                {
                    "input_id": input_id,
                    "input_id_type": input_id_type,
                    "matched_patent_id": "",
                    "title": "",
                    "applicant": "",
                    "application_date": "",
                    "current_case_status_raw": "",
                    "current_case_status_category": "",
                    "event_name_raw": "",
                    "event_text_raw": "",
                    "event_category": "other",
                    "source_url": config["official_query"]["base_url"],
                    "crawl_time": now_iso(),
                    "parse_status": "fetch_failed",
                    "notes": str(exc)[:1000],
                }
            )

    fieldnames = [
        "input_id",
        "input_id_type",
        "matched_patent_id",
        "title",
        "applicant",
        "application_date",
        "current_case_status_raw",
        "current_case_status_category",
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch official CNIPA batch review / case-status data by application number")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--headed", action="store_true", help="Launch the browser in headed mode (recommended).")
    parser.add_argument(
        "--wait-for-login-seconds",
        type=int,
        default=0,
        help="If redirected to the login page, wait this many seconds for a manual login to complete.",
    )
    args = parser.parse_args()
    fetch_batch(
        args.input,
        args.config,
        args.output,
        args.log,
        state_path=args.state,
        headed=args.headed,
        wait_for_login_seconds=args.wait_for_login_seconds,
    )


if __name__ == "__main__":
    main()
