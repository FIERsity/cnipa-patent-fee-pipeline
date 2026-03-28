from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from bs4 import BeautifulSoup

from cnipa_utils import (
    classify_event_category,
    classify_input_type,
    configure_logger,
    ensure_dir,
    infer_city_from_text,
    load_csv_rows,
    normalize_input_id,
    normalize_patent_join_key,
    normalize_patentstar_application_no,
    now_iso,
    random_sleep,
    write_csv_rows,
)


PATENTSTAR_BASE = "https://cprs.patentstar.com.cn"
LIST_SEARCH_URL = f"{PATENTSTAR_BASE}/Search/ListSearch"
RESULT_URL_PREFIX = f"{PATENTSTAR_BASE}/Search/ListSearchResult"
DETAIL_URL_TEMPLATE = f"{PATENTSTAR_BASE}/Search/Detail?ANE={{ane}}"
DEFAULT_OUTPUT = Path("outputs/patentstar_legal_events.csv")
DEFAULT_LOG = Path("logs/fetch_patentstar_legal_status.log")
DEFAULT_SESSION = "patentstar_login"


def _default_pwcli() -> Path:
    import os

    codex_home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    return codex_home / "skills" / "playwright" / "scripts" / "playwright_cli.sh"


def _extract_result_payload(stdout: str):
    marker = "### Result"
    if marker not in stdout:
        raise RuntimeError(f"playwright output did not contain a result block: {stdout[:500]}")
    tail = stdout.split(marker, 1)[1].strip()
    lines = []
    for line in tail.splitlines():
        if line.startswith("### "):
            break
        lines.append(line)
    payload = "\n".join(lines).strip()
    if not payload:
        return None
    try:
        return json.loads(payload)
    except Exception:
        return payload


class PatentStarCliSession:
    def __init__(self, session: str, pwcli: Optional[Path] = None):
        self.session = session
        self.pwcli = pwcli or _default_pwcli()

    def run(self, *args: str, check: bool = True) -> str:
        import subprocess

        cmd = [str(self.pwcli), "--session", self.session, *args]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if check and proc.returncode != 0:
            raise RuntimeError(
                f"playwright-cli failed ({proc.returncode}) for {' '.join(args)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
            )
        return proc.stdout

    def open(self, url: str) -> None:
        self.run("open", url)

    def goto(self, url: str) -> None:
        self.run("goto", url)

    def tab_new(self, url: str) -> None:
        self.run("tab-new", url)

    def tab_select(self, index: int) -> None:
        self.run("tab-select", str(index))

    def eval(self, js: str):
        stdout = self.run("eval", js)
        return _extract_result_payload(stdout)

    def run_code(self, code: str):
        stdout = self.run("run-code", code)
        return _extract_result_payload(stdout)

    def tab_list(self) -> str:
        return self.run("tab-list")

    def tab_close(self, index: int) -> None:
        self.run("tab-close", str(index))


def _normalize_patentstar_id(input_id: str, input_id_type: str = "") -> str:
    normalized_type = classify_input_type(input_id_type, input_id)
    if normalized_type == "application_no":
        return normalize_patentstar_application_no(input_id)
    return normalize_patentstar_application_no(normalize_input_id(input_id))


def _parse_kv_text(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    pending_key: Optional[str] = None
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        m = re.match(r"([^：:]+)[:：]\s*(.*)$", line)
        if m:
            key = m.group(1).strip()
            value = m.group(2).strip()
            if value:
                out[key] = value
                pending_key = None
            else:
                pending_key = key
            continue
        if pending_key:
            out[pending_key] = line
            pending_key = None
    return out


def _parse_result_page(html: str, batch_ids: Sequence[str]) -> Tuple[List[Dict[str, str]], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, str]] = []
    batch_lookup = {
        normalize_patent_join_key(normalize_patentstar_application_no(x) if x else "")
        for x in batch_ids
        if x
    }

    for patent in soup.select("div.patent"):
        title_label = patent.select_one("label.title-color")
        if not title_label:
            continue
        ane = (title_label.get("data-ane") or "").strip()
        title = (title_label.get("title") or title_label.get_text(" ", strip=True)).strip()
        current_status_el = patent.select_one("p.invcolor")
        current_status = current_status_el.get_text(" ", strip=True) if current_status_el else ""
        content = patent.select_one("div.patent-content")
        content_text = content.get_text("\n", strip=True) if content else ""
        meta = _parse_kv_text(content_text)
        application_no = meta.get("申请号", "")
        application_no_norm = normalize_input_id(application_no)
        if batch_lookup and normalize_patent_join_key(application_no) not in batch_lookup:
            continue
        row = {
            "ane": ane,
            "title": title.replace("  [发明]", "").strip(),
            "current_status": current_status,
            "application_no": application_no,
            "application_no_norm": application_no_norm,
            "application_date": meta.get("申请日", ""),
            "publication_no": meta.get("公开号", ""),
            "publication_date": meta.get("公开日", ""),
            "grant_no": meta.get("公告号", ""),
            "grant_date": meta.get("公告日", ""),
            "main_class": meta.get("主分类", ""),
            "applicant": meta.get("申请人", ""),
            "current_right_holder": meta.get("当前权利人", ""),
            "inventor": meta.get("发明人", ""),
            "address": meta.get("地址", ""),
            "abstract": meta.get("摘要", ""),
            "detail_url": DETAIL_URL_TEMPLATE.format(ane=ane) if ane else "",
        }
        rows.append(row)

    next_page_url = None
    # Best-effort next-page discovery; works when pagination anchors are present.
    for a in soup.find_all("a"):
        text = a.get_text(" ", strip=True)
        title = (a.get("title") or "").strip()
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if text in {">", ">>", "下一页", "›", "»"} or title in {">", ">>", "下一页", "›", "»"}:
            next_page_url = href
            break
    return rows, next_page_url


def _parse_detail_page(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, str]] = []
    tbody = soup.select_one("tbody#legalContainer")
    if not tbody:
        return rows
    for tr in tbody.select("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(tds) < 4:
            continue
        status_text = f"{tds[1]} {tds[2]} {tds[3]}".strip()
        rows.append(
            {
                "event_date": tds[0].strip(),
                "event_name_raw": tds[1].strip(),
                "event_text_raw": tds[2].strip(),
                "event_category": classify_event_category(status_text),
                "detail_text_raw": tds[3].strip(),
            }
        )
    return rows


def _has_suspicious_status(current_status: str) -> bool:
    text = current_status or ""
    keywords = ["失效", "终止", "放弃", "恢复"]
    return any(k in text for k in keywords)


def _ensure_legal_status_tab(session: PatentStarCliSession) -> None:
    if session.eval("document.querySelector('tbody#legalContainer') ? 'yes' : 'no'") == "yes":
        # The tbody can appear before rows are populated; fall through to row check.
        pass
    session.eval(
        """(function() {
  var nodes = Array.prototype.slice.call(document.querySelectorAll('p'));
  var target = nodes.find(function(el) {
    return el && el.textContent && el.textContent.trim() === '法律状态';
  });
  if (target) {
    target.click();
    return 'clicked';
  }
  return 'missing';
})()"""
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            if session.eval("document.querySelectorAll('tbody#legalContainer tr').length > 0 ? 'yes' : 'no'") == "yes":
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("timed out waiting for PatentStar legal-status rows")


def _set_batch_and_search(session: PatentStarCliSession, batch_text: str) -> None:
    js = f"""(document.querySelector('textarea[placeholder="请输入号单"]').value={json.dumps(batch_text, ensure_ascii=False)},
document.querySelectorAll('button')[1].click(),
'ok')"""
    session.eval(js)
    time.sleep(2.5)


def _wait_for_result_state(session: PatentStarCliSession, logger: logging.Logger, timeout_s: int = 30) -> Tuple[str, str]:
    deadline = time.time() + timeout_s
    html = ""
    url = ""
    while time.time() < deadline:
        try:
            url = session.eval("document.location.href") or ""
            html = session.eval("document.body.innerHTML") or ""
            if "/Search/ListSearchResult" in url and "div class=\"patent\"" in html:
                return html, url
        except Exception as exc:
            logger.debug("waiting for search result: %s", exc)
        time.sleep(1.0)
    raise RuntimeError("timed out waiting for PatentStar search result page")


def _latest_tab_index(session: PatentStarCliSession) -> int:
    output = session.tab_list()
    indices: List[int] = []
    for line in output.splitlines():
        m = re.match(r"-\s+(\d+):", line.strip())
        if m:
            indices.append(int(m.group(1)))
    if not indices:
        return 0
    return max(indices)


def _find_tab_index(session: PatentStarCliSession, needle: str) -> Optional[int]:
    output = session.tab_list()
    for line in output.splitlines():
        m = re.match(r"-\s+(\d+):\s+(?:\(current\)\s+)?\[(.*)\]\((.*)\)", line.strip())
        if not m:
            continue
        idx = int(m.group(1))
        title = m.group(2)
        url = m.group(3)
        if needle in title or needle in url:
            return idx
    return None


def fetch_patentstar_batch(
    *,
    input_rows: Sequence[Dict[str, str]],
    session_name: str,
    logger: logging.Logger,
    max_pages: int = 1,
    detail_only_on_suspicious: bool = True,
    inter_request_min_seconds: float = 0.8,
    inter_request_max_seconds: float = 2.2,
) -> List[Dict[str, str]]:
    session = PatentStarCliSession(session=session_name)
    outputs: List[Dict[str, str]] = []

    batch_text = "\n".join(
        _normalize_patentstar_id(row.get("input_id", ""), row.get("input_id_type", "")) for row in input_rows if row.get("input_id")
    )
    batch_ids = [normalize_input_id(row.get("input_id", "")) for row in input_rows if row.get("input_id")]
    logger.info("searching PatentStar batch with %d ids", len(batch_ids))

    session.goto(LIST_SEARCH_URL)
    time.sleep(2.0)
    _set_batch_and_search(session, batch_text)
    time.sleep(1.5)
    result_tab = _latest_tab_index(session)
    session.tab_select(result_tab)
    html, url = _wait_for_result_state(session, logger)
    logger.info("result page ready: %s", url)
    page_rows, next_page_url = _parse_result_page(html, batch_ids)
    logger.info("parsed %d visible rows", len(page_rows))

    for row in page_rows:
        summary = {
            "input_id": row["application_no_norm"] or row["application_no"],
            "input_id_type": "application_no",
            "matched_patent_id": row["application_no"],
            "title": row["title"],
            "applicant": row["applicant"],
            "event_date": "",
            "event_name_raw": row["current_status"],
            "event_text_raw": f"当前状态：{row['current_status']}",
            "event_category": "other",
            "source_url": url,
            "crawl_time": now_iso(),
            "parse_status": "summary_only",
            "notes": f"ane={row['ane']}; publication_no={row['publication_no']}; grant_no={row['grant_no']}; address={row['address']}",
        }
        outputs.append(summary)

        if detail_only_on_suspicious and not _has_suspicious_status(row["current_status"]):
            continue

        if not row.get("detail_url"):
            continue

        random_sleep(inter_request_min_seconds, inter_request_max_seconds)
        result_tab = _latest_tab_index(session)
        detail_tab = None
        try:
            session.tab_select(result_tab)
            session.eval(
                f"""(function() {{
  var target = Array.prototype.slice.call(document.querySelectorAll('label.title-color')).find(function(el) {{
    return el && el.getAttribute('data-ane') === {json.dumps(row['ane'])};
  }});
  if (target) {{
    target.click();
    return 'clicked';
  }}
  return 'missing';
}})()"""
            )
            time.sleep(2.0)
            detail_tab = _find_tab_index(session, row["ane"]) or _latest_tab_index(session)
            session.tab_select(detail_tab)
            _ensure_legal_status_tab(session)
            detail_html = session.eval("document.querySelector('tbody#legalContainer') && document.querySelector('tbody#legalContainer').outerHTML") or ""
            detail_rows = _parse_detail_page(detail_html)
            if not detail_rows:
                outputs.append(
                    {
                        "input_id": row["application_no_norm"] or row["application_no"],
                        "input_id_type": "application_no",
                        "matched_patent_id": row["application_no"],
                        "title": row["title"],
                        "applicant": row["applicant"],
                        "event_date": "",
                        "event_name_raw": "",
                        "event_text_raw": "",
                        "event_category": "other",
                        "source_url": row["detail_url"],
                        "crawl_time": now_iso(),
                        "parse_status": "detail_empty",
                        "notes": f"ane={row['ane']}; current_status={row['current_status']}",
                    }
                )
            else:
                for event in detail_rows:
                    outputs.append(
                        {
                            "input_id": row["application_no_norm"] or row["application_no"],
                            "input_id_type": "application_no",
                            "matched_patent_id": row["application_no"],
                            "title": row["title"],
                            "applicant": row["applicant"],
                            "event_date": event["event_date"],
                            "event_name_raw": event["event_name_raw"],
                            "event_text_raw": event["event_text_raw"],
                            "event_category": event["event_category"],
                            "source_url": row["detail_url"],
                            "crawl_time": now_iso(),
                            "parse_status": "ok",
                            "notes": f"ane={row['ane']}; current_status={row['current_status']}; {event['detail_text_raw']}",
                        }
                    )
        except Exception as exc:
            logger.exception("detail fetch failed for %s: %s", row["application_no"], exc)
            outputs.append(
                {
                    "input_id": row["application_no_norm"] or row["application_no"],
                    "input_id_type": "application_no",
                    "matched_patent_id": row["application_no"],
                    "title": row["title"],
                    "applicant": row["applicant"],
                    "event_date": "",
                    "event_name_raw": "",
                    "event_text_raw": "",
                    "event_category": "other",
                    "source_url": row["detail_url"],
                    "crawl_time": now_iso(),
                    "parse_status": "detail_failed",
                    "notes": f"ane={row['ane']}; current_status={row['current_status']}; {exc}",
                }
                )
        finally:
            session.tab_select(result_tab)
            if detail_tab is not None and detail_tab != result_tab:
                try:
                    session.tab_close(detail_tab)
                except Exception:
                    logger.debug("failed to close detail tab %s", detail_tab, exc_info=True)
            time.sleep(1.0)

    # Best-effort pagination if the current page exposes a next-page link.
    # This is intentionally conservative: if we cannot find a stable next-page
    # URL, we stop rather than guess and risk skipping or duplicating data.
    page_count = 1
    current_url = url
    while max_pages > 1 and next_page_url and page_count < max_pages:
        page_count += 1
        try:
            session.goto(next_page_url if next_page_url.startswith("http") else f"{PATENTSTAR_BASE}{next_page_url}")
            time.sleep(2.0)
            html, current_url = _wait_for_result_state(session, logger)
            page_rows, next_page_url = _parse_result_page(html, batch_ids)
            logger.info("page %d parsed %d rows", page_count, len(page_rows))
            for row in page_rows:
                summary = {
                    "input_id": row["application_no_norm"] or row["application_no"],
                    "input_id_type": "application_no",
                    "matched_patent_id": row["application_no"],
                    "title": row["title"],
                    "applicant": row["applicant"],
                    "event_date": "",
                    "event_name_raw": row["current_status"],
                    "event_text_raw": f"当前状态：{row['current_status']}",
                    "event_category": "other",
                    "source_url": current_url,
                    "crawl_time": now_iso(),
                    "parse_status": "summary_only",
                    "notes": f"ane={row['ane']}; publication_no={row['publication_no']}; grant_no={row['grant_no']}; address={row['address']}",
                }
                outputs.append(summary)
        except Exception as exc:
            logger.exception("pagination stopped at page %d: %s", page_count, exc)
            break

    return outputs


def run_pipeline(
    input_csv: Path,
    output_csv: Path,
    log_path: Path,
    session_name: str = DEFAULT_SESSION,
    batch_size: int = 3000,
    max_pages: int = 1,
    detail_only_on_suspicious: bool = True,
) -> List[Dict[str, str]]:
    logger = configure_logger(log_path)
    input_rows = load_csv_rows(input_csv)
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
                session_name=session_name,
                logger=logger,
                max_pages=max_pages,
                detail_only_on_suspicious=detail_only_on_suspicious,
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
                        "source_url": LIST_SEARCH_URL,
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
    parser = argparse.ArgumentParser(description="Fetch PatentStar legal-status rows by application number")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    parser.add_argument("--log", default=DEFAULT_LOG, type=Path)
    parser.add_argument("--session", default=DEFAULT_SESSION)
    parser.add_argument("--batch-size", type=int, default=3000)
    parser.add_argument("--max-pages", type=int, default=1, help="Best-effort pagination limit per batch search.")
    parser.add_argument("--all-details", action="store_true", help="Fetch detail tabs for all visible rows, not only suspicious statuses.")
    args = parser.parse_args()
    run_pipeline(
        input_csv=args.input,
        output_csv=args.output,
        log_path=args.log,
        session_name=args.session,
        batch_size=args.batch_size,
        max_pages=args.max_pages,
        detail_only_on_suspicious=not args.all_details,
    )


if __name__ == "__main__":
    main()
