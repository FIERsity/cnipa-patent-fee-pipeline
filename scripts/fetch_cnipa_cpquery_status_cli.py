from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from cnipa_utils import (
    classify_cpquery_case_status,
    classify_input_type,
    configure_logger,
    ensure_dir,
    load_csv_rows,
    now_iso,
    normalize_input_id,
    normalize_patent_join_key,
    read_json,
    safe_filename,
    write_csv_rows,
)


DEFAULT_CONFIG = Path("configs/default.json")
DEFAULT_INPUT = Path("raw/sample_patent_ids.csv")
DEFAULT_OUTPUT = Path("outputs/patent_cpquery_status.csv")
DEFAULT_LOG = Path("logs/fetch_cnipa_cpquery_status.log")
DEFAULT_STATE = Path("output/playwright/cpquery.state.json")
DEFAULT_MODE = "auto"
API_ENDPOINT = "/api/search/undomestic/publicSearch"


def _default_pwcli() -> Path:
    codex_home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    return codex_home / "skills" / "playwright" / "scripts" / "playwright_cli.sh"


def _extract_result_payload(stdout: str):
    marker = "### Result"
    if marker not in stdout:
        return None
    tail = stdout.split(marker, 1)[1].strip()
    lines = []
    for line in tail.splitlines():
        if line.startswith("### "):
            break
        lines.append(line)
    payload = "\n".join(lines).strip()
    if not payload:
        return None
    return json.loads(payload)


@dataclass
class CpqueryCliSession:
    session: str
    pwcli: Path = _default_pwcli()

    def run(self, *args: str, check: bool = True) -> str:
        cmd = [str(self.pwcli), "--session", self.session, *args]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if check and proc.returncode != 0:
            raise RuntimeError(
                f"playwright-cli failed ({proc.returncode}) for {' '.join(args)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
            )
        return proc.stdout

    def open(self, url: str, headed: bool = True) -> None:
        if headed:
            self.run("open", url, "--headed")
        else:
            self.run("open", url)

    def state_load(self, state_path: Path) -> None:
        self.run("state-load", str(state_path))

    def eval(self, js: str):
        return _extract_result_payload(self.run("eval", js))

    def snapshot(self):
        return self.run("snapshot")

    def fill(self, ref: str, text: str):
        return self.run("fill", ref, text)

    def click(self, ref: str):
        return self.run("click", ref)


def _query_js(input_value: str) -> str:
    return f"""async () => {{
      await page.goto('https://cpquery.cponline.cnipa.gov.cn/chinesepatent/index');
      await page.waitForTimeout(3000);
      const input = page.getByPlaceholder('例如: 2010101995057');
      await input.fill({json.dumps(input_value, ensure_ascii=False)});
      await page.getByRole('button', {{ name: '查询', exact: true }}).click();
      await page.waitForTimeout(5000);
      return {{
        ok: true,
        body: await page.evaluate(() => document.body.innerText),
        html: await page.evaluate(() => document.documentElement.outerHTML)
      }};
    }}"""


def _api_query_js(input_value: str) -> str:
    return f"""async () => {{
      await page.goto('https://cpquery.cponline.cnipa.gov.cn/chinesepatent/index');
      await page.waitForTimeout(1000);
      const payload = {{
        zhuanlilx: '',
        page: 1,
        size: 10,
        zhuanlisqh: {json.dumps(input_value, ensure_ascii=False)},
        sortDataName: '',
        sortType: ''
      }};
      return await page.evaluate(async (payload) => {{
        const token = localStorage.getItem('ACCESS_TOKEN') || '';
        const userType = localStorage.getItem('USER_TYPE') || '';
        const resp = await fetch('{API_ENDPOINT}', {{
          method: 'POST',
          credentials: 'include',
          headers: {{
            'Content-Type': 'application/json;charset=UTF-8',
            'Accept': 'application/json, text/plain, */*',
            'Authorization': token ? `Bearer ${{token}}` : '',
            'usertype': userType
          }},
          body: JSON.stringify(payload)
        }});
        return {{
          ok: resp.status === 200,
          status: resp.status,
          url: resp.url,
          text: await resp.text(),
          token_present: Boolean(token),
          user_type: userType
        }};
      }}, payload);
    }}"""


def _extract_body(page_text: str) -> Dict[str, str]:
    body = page_text or ""
    result: Dict[str, str] = {
        "current_case_status_raw": "",
        "title": "",
        "applicant": "",
        "application_date": "",
        "matched_patent_id": "",
    }
    row_pattern = re.search(
        r"发明专利\s+.*?申请号/专利号[:：]\s*([A-Z0-9\.]+)\s*发明名称[:：]\s*(.*?)\s*申请人[:：]\s*(.*?)\s*专利类型[:：]\s*([^\s]+)\s*申请日[:：]\s*([0-9\-.]+).*?案件状态[:：]\s*([^\s\n]+)",
        body,
        flags=re.S,
    )
    if row_pattern:
        result["matched_patent_id"] = row_pattern.group(1).strip()
        result["title"] = row_pattern.group(2).strip()
        result["applicant"] = row_pattern.group(3).strip()
        result["application_date"] = row_pattern.group(5).strip()
        status = row_pattern.group(6).strip()
        status = re.split(r"(?:授权公告日|主分类号|发明专利申请公布号|共\d+条|共查询到)", status)[0].strip()
        result["current_case_status_raw"] = status
        return result
    status_patterns = [
        r"案件状态[:：]\s*([^\n]+)",
        r"当前案件状态[:：]\s*([^\n]+)",
        r"法律状态[:：]\s*([^\n]+)",
    ]
    for pat in status_patterns:
        m = re.search(pat, body)
        if m:
            status = m.group(1).strip()
            status = re.split(r"(?:授权公告日|主分类号|发明专利申请公布号|共\d+条|共查询到)", status)[0].strip()
            result["current_case_status_raw"] = status
            break
    m = re.search(r"申请号/专利号[:：]\s*([A-Z0-9\.]+)", body)
    if m:
        result["matched_patent_id"] = m.group(1).strip()
    m = re.search(r"发明名称[:：]\s*(.+?)申请人[:：]", body, flags=re.S)
    if m:
        result["title"] = m.group(1).strip()
    m = re.search(r"申请人[:：]\s*(.+?)专利类型[:：]", body, flags=re.S)
    if m:
        result["applicant"] = m.group(1).strip()
    m = re.search(r"申请日[:：]\s*([0-9\-\.]+)", body)
    if m:
        result["application_date"] = m.group(1).strip()
    return result


def _extract_api_rows(
    api_payload: Dict[str, object],
    input_id: str,
    input_id_type: str,
    page_url: str,
    *,
    notes: str,
) -> List[Dict[str, object]]:
    data = api_payload.get("data") if isinstance(api_payload, dict) else None
    if not isinstance(data, dict):
        return []
    records = data.get("records") or []
    rows: List[Dict[str, object]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        matched = str(record.get("zhuanlisqh") or "").strip() or input_id
        current_status = str(record.get("anjianywzt") or record.get("falvzt") or "").strip()
        status_cat = classify_cpquery_case_status(current_status)
        rows.append(
            {
                "input_id": input_id,
                "input_id_type": input_id_type,
                "matched_patent_id": normalize_input_id(matched) if matched else "",
                "title": str(record.get("zhuanlimc") or "").strip(),
                "applicant": str(record.get("shenqingrxm") or "").strip(),
                "application_date": str(record.get("shenqingr") or "").strip(),
                "current_case_status_raw": current_status,
                "current_case_status_category": status_cat,
                "event_name_raw": current_status,
                "event_text_raw": current_status,
                "event_category": status_cat,
                "source_url": page_url,
                "crawl_time": now_iso(),
                "parse_status": "ok",
                "notes": notes,
            }
        )
    return rows


def query_single(
    input_id: str,
    input_id_type: str,
    session: CpqueryCliSession,
    logger,
    *,
    save_dir: Path,
    mode: str = "auto",
) -> List[Dict[str, object]]:
    normalized_type = classify_input_type(input_id_type, input_id)
    query_value = normalize_patent_join_key(input_id) or normalize_input_id(input_id)
    errors: List[str] = []
    for attempt in range(3):
        try:
            if mode in {"auto", "api"}:
                out = session.run("run-code", _api_query_js(query_value))
                payload = _extract_result_payload(out)
                if isinstance(payload, dict) and payload.get("ok"):
                    api_text = payload.get("text") or ""
                    api_payload = json.loads(api_text) if api_text else {}
                    rows = _extract_api_rows(
                        api_payload,
                        input_id=input_id,
                        input_id_type=normalized_type,
                        page_url=payload.get("url") or "https://cpquery.cponline.cnipa.gov.cn/chinesepatent/index",
                        notes=(
                            f"attempt={attempt}; fast_api=True; "
                            f"token_present={payload.get('token_present')}; user_type={payload.get('user_type')}"
                        ),
                    )
                    if rows:
                        snapshot_path = ensure_dir(save_dir) / f"{safe_filename(input_id)}__attempt{attempt}.json"
                        snapshot_path.write_text(api_text, encoding="utf-8")
                        return rows
                    errors.append(f"attempt {attempt}: api returned no rows")
                else:
                    errors.append(f"attempt {attempt}: api query did not execute: {payload}")
                # If the fast API path is blocked, fall back to the browser-rendered result page.
                # This keeps correctness first; once the signed API parameters are fully replicated
                # we can let the API path stand on its own.
                if mode == "api" and any(
                    term in " ".join(errors).lower()
                    for term in ("400", "401", "404", "blocked", "login", "precondition", "bad request")
                ):
                    logger.info("api path blocked for %s; falling back to DOM mode", input_id)

            out = session.run("run-code", _query_js(query_value))
            payload = _extract_result_payload(out)
            if not isinstance(payload, dict) or not payload.get("ok"):
                errors.append(f"attempt {attempt}: query did not execute: {payload}")
                continue
            body_text = payload.get("body") or ""
            html = payload.get("html") or ""
            parsed = _extract_body(body_text)
            current_status = parsed.get("current_case_status_raw", "")
            status_cat = classify_cpquery_case_status(current_status)
            snapshot_path = ensure_dir(save_dir) / f"{safe_filename(input_id)}__attempt{attempt}.html"
            snapshot_path.write_text(html, encoding="utf-8")
            return [
                {
                    "input_id": input_id,
                    "input_id_type": normalized_type,
                    "matched_patent_id": parsed.get("matched_patent_id") or query_value,
                    "title": parsed.get("title") or "",
                    "applicant": parsed.get("applicant") or "",
                    "application_date": parsed.get("application_date") or "",
                    "current_case_status_raw": current_status,
                    "current_case_status_category": status_cat,
                    "event_name_raw": current_status,
                    "event_text_raw": current_status,
                    "event_category": status_cat,
                    "source_url": "https://cpquery.cponline.cnipa.gov.cn/chinesepatent/index",
                    "crawl_time": now_iso(),
                    "parse_status": "ok",
                    "notes": f"attempt={attempt}; saved={snapshot_path}",
                }
            ]
        except Exception as exc:
            errors.append(f"attempt {attempt}: {exc}")
            logger.exception("query failed for %s on attempt %d", input_id, attempt)
            time.sleep(2)

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
            "source_url": "https://cpquery.cponline.cnipa.gov.cn/chinesepatent/index",
            "crawl_time": now_iso(),
            "parse_status": status,
            "notes": "; ".join(errors)[:1000],
        }
    ]


def fetch_batch(
    input_csv: Path,
    config_path: Path,
    output_csv: Path,
    log_path: Path,
    *,
    session_name: str,
    mode: str,
    headed: bool,
) -> List[Dict[str, object]]:
    logger = configure_logger(log_path)
    config = read_json(config_path)
    input_rows = load_csv_rows(input_csv)
    session = CpqueryCliSession(session=session_name)
    session.open("https://cpquery.cponline.cnipa.gov.cn/chinesepatent/index", headed=headed)
    session.state_load(DEFAULT_STATE)
    all_rows: List[Dict[str, object]] = []

    for item in input_rows:
        input_id = (item.get("input_id") or "").strip()
        input_id_type = (item.get("input_id_type") or "").strip()
        if not input_id:
            continue
        logger.info("querying %s (%s)", input_id, input_id_type)
        try:
            rows = query_single(
                input_id,
                input_id_type,
                session,
                logger,
                save_dir=Path("raw/html_snapshots/cpquery"),
                mode=mode,
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
                    "source_url": "https://cpquery.cponline.cnipa.gov.cn/chinesepatent/index",
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
    parser = argparse.ArgumentParser(description="Fetch CNIPA official query status using a headed Playwright CLI session")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--session", default="cpquery_batch_test")
    parser.add_argument("--mode", choices=["auto", "api", "dom"], default=DEFAULT_MODE)
    parser.add_argument("--headed", action="store_true", help="Open a visible browser window. Default is headless to avoid popups.")
    args = parser.parse_args()
    fetch_batch(
        args.input,
        args.config,
        args.output,
        args.log,
        session_name=args.session,
        mode=args.mode,
        headed=args.headed,
    )


if __name__ == "__main__":
    main()
