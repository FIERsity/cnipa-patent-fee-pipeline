from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from cnipa_utils import ensure_dir, infer_city_from_text, now_iso, normalize_input_id, load_csv_rows, write_csv_rows


DEFAULT_BASE_URL = "https://pss-system.cponline.cnipa.gov.cn/retrieveList?prevPageTit=gaoji"
DEFAULT_SEARCH_URL = "https://pss-system.cponline.cnipa.gov.cn/conventionalSearch"
DEFAULT_STATE = Path("output/playwright/pss-system.state.json")
DEFAULT_OUTPUT = Path("outputs/patent_master_pss.csv")

CITY_TOKEN_SOURCE = Path("raw/reference/prefecture_level_cities.csv")
_CITY_LOOKUP_CACHE: Optional[List[tuple[str, str]]] = None


def _load_city_lookup() -> List[tuple[str, str]]:
    global _CITY_LOOKUP_CACHE
    if _CITY_LOOKUP_CACHE is not None:
        return _CITY_LOOKUP_CACHE
    lookup: List[tuple[str, str]] = []
    if CITY_TOKEN_SOURCE.exists():
        import csv

        with CITY_TOKEN_SOURCE.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                city_name = (row.get("city_name") or "").strip()
                city_short = (row.get("city_short_name") or "").strip()
                if city_name:
                    lookup.append((city_name.replace(" ", ""), city_name))
                if city_short and city_short != city_name:
                    lookup.append((city_short.replace(" ", ""), city_name))
    _CITY_LOOKUP_CACHE = sorted({item for item in lookup if item[0]}, key=lambda x: (-len(x[0]), x[0]))
    return _CITY_LOOKUP_CACHE


def _default_pwcli() -> Path:
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
    return json.loads(payload)


@dataclass
class PssCliSession:
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

    def open(self, url: str = DEFAULT_BASE_URL) -> None:
        self.run("open", url)

    def state_load(self, state_path: Path) -> None:
        self.run("state-load", str(state_path))

    def goto(self, url: str) -> None:
        self.run("goto", url)

    def tab_select(self, index: int) -> None:
        self.run("tab-select", str(index))

    def eval(self, js: str):
        stdout = self.run("eval", js)
        return _extract_result_payload(stdout)

    def run_code(self, code: str):
        stdout = self.run("run-code", code)
        return stdout


def _load_results_js() -> str:
    return """async () => {
      const vm = document.querySelector('.wrap')?.__vue__;
      if (!vm) {
        return null;
      }
      if (typeof vm.executeHistorySearch === 'function') {
        await vm.executeHistorySearch();
      } else if (typeof vm.executeSecondSearch === 'function') {
        await vm.executeSecondSearch();
      }
      await new Promise(r => setTimeout(r, 3000));
      return {
        pagination: vm.obj?.pagination || null,
        rows: vm.obj?.searchResultRecord || [],
        body: document.body.innerText.slice(0, 1200)
      };
    }"""


def _normalize_record(record: Dict[str, object]) -> Dict[str, object]:
    application_no = record.get("ap") or record.get("apo") or record.get("anId") or ""
    publication_no = record.get("pn") or record.get("pnId") or ""
    title = record.get("ti") or ""
    applicant = record.get("pa") or ""
    application_date = record.get("apd") or ""
    publication_date = record.get("pd") or ""
    year = ""
    if isinstance(application_date, str) and application_date:
        year = application_date[:4].replace(".", "")
    if not year and isinstance(publication_date, str) and publication_date:
        year = publication_date[:4].replace(".", "")
    loc_detail = record.get("locDetail") or []
    loc_value = ""
    if isinstance(loc_detail, list) and loc_detail:
        first = loc_detail[0] if isinstance(loc_detail[0], dict) else {}
        loc_value = first.get("value", "") if isinstance(first, dict) else ""
    applicant_address = ""
    province_name = ""
    city_name = infer_city_from_text(applicant, _load_city_lookup())
    return {
        "input_id": normalize_input_id(str(application_no)),
        "input_id_type": "application_no",
        "application_no": str(application_no),
        "publication_no": str(publication_no),
        "title": str(title),
        "applicant": str(applicant),
        "applicant_address": applicant_address,
        "province_name": province_name,
        "city_name": city_name,
        "year": year,
        "application_date": str(application_date),
        "publication_date": str(publication_date),
        "locarno_class": loc_value,
        "source_url": DEFAULT_BASE_URL,
        "crawl_time": now_iso(),
        "parse_status": "ok",
        "notes": f"dbName={record.get('dbName', '')}; invType={record.get('invType', '')}; pnId={record.get('pnId', '')}; anId={record.get('anId', '')}",
    }


def fetch_pss_master(
    *,
    search_exp: str,
    output: Path,
    state_path: Path = DEFAULT_STATE,
    session_name: str = "default",
    max_pages: int = 1,
    page_start: int = 1,
) -> List[Dict[str, object]]:
    session = PssCliSession(session=session_name)
    session.open(DEFAULT_SEARCH_URL)
    if state_path.exists():
        session.state_load(state_path)
    search_code = f"""async () => {{
  await page.goto('https://pss-system.cponline.cnipa.gov.cn/conventionalSearch');
  await page.waitForTimeout(3000);
  await page.getByRole('textbox', {{ name: '请输入关键词、申请号/公开号、申请人/发明人、申请日/公开日、IPC分类号/CPC分类号，系统根据规则智能识别检索' }}).fill({json.dumps(search_exp, ensure_ascii=False)});
  await page.locator('section').getByText('检索', {{ exact: true }}).click();
  await page.waitForTimeout(4000);
}}"""
    session.run_code(search_code)
    session.tab_select(1)
    payload = session.eval(_load_results_js())
    if not isinstance(payload, dict):
        raise RuntimeError("result payload is empty")
    pagination = payload.get("pagination") or {}
    total_count = int(pagination.get("totalCount") or 0)
    rows: List[Dict[str, object]] = []

    def append_page(page_payload: object) -> None:
        if not isinstance(page_payload, dict):
            return
        for record in page_payload.get("rows") or []:
            if isinstance(record, dict):
                rows.append(_normalize_record(record))

    append_page(payload)
    if max_pages and max_pages > 1:
        for page_no in range(page_start + 1, page_start + max_pages):
            page_payload = session.eval(
                f"""async () => {{
  const vm = document.querySelector('.wrap')?.__vue__;
  if (!vm || !vm.obj || !vm.obj.pagination) return null;
  vm.obj.pagination.page = {page_no};
  if (typeof vm.getPagingData === 'function') {{
    const res = await vm.getPagingData(vm.obj.pagination);
    await new Promise(r => setTimeout(r, 1500));
    return {{
      pagination: vm.obj.pagination || null,
      rows: vm.obj.searchResultRecord || [],
      result: res || null
    }};
  }}
  return null;
}}"""
            )
            if not isinstance(page_payload, dict):
                break
            page_rows = page_payload.get("rows") or []
            if not page_rows:
                break
            append_page(page_payload)
    ensure_dir(output.parent)
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
        "locarno_class",
        "source_url",
        "crawl_time",
        "parse_status",
        "notes",
    ]
    write_csv_rows(output, rows, fieldnames)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch CNIPA PSS-System search results via the logged-in browser session")
    parser.add_argument("--search-exp", required=True, help="Search expression, e.g. '申请日>=2024-01-01 AND 申请日<2025-01-01'")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, type=Path)
    parser.add_argument("--state", default=DEFAULT_STATE, type=Path)
    parser.add_argument("--session", default="default")
    parser.add_argument("--max-pages", type=int, default=1, help="How many pages to collect; default is 1 page of 10 rows.")
    parser.add_argument("--page-start", type=int, default=1)
    args = parser.parse_args()
    rows = fetch_pss_master(
        search_exp=args.search_exp,
        output=args.output,
        state_path=args.state,
        session_name=args.session,
        max_pages=args.max_pages,
        page_start=args.page_start,
    )
    print(f"rows={len(rows)} output={args.output}")


if __name__ == "__main__":
    main()
