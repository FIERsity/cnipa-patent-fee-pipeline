from __future__ import annotations

import io
import json
import logging
import math
import re
import zipfile
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import requests
from lxml import etree
from playwright.sync_api import sync_playwright

from cnipa_utils import classify_event_category, extract_first_date, now_iso, normalize_input_id, ensure_dir


BASE_URL = "https://ipdps.cnipa.gov.cn"
CATALOG_BROWSE_URL = f"{BASE_URL}/public/catalogStaticBrowse"
DOWNLOAD_URL = f"{BASE_URL}/public/download"
DEFAULT_DATA_NOS = ("CN-PA-PRSS-10", "CN-PA-PRSS-20", "CN-PA-PRSS-30")
MUNICIPALITIES = {"北京市", "天津市", "上海市", "重庆市"}


def bootstrap_public_session(base_url: str = BASE_URL, headless: bool = True, timeout_ms: int = 45000) -> Dict[str, object]:
    """Open the public data portal in a browser and return cookies/user-agent for API requests."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()
        page.goto(base_url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(2500)
        cookies = {item["name"]: item["value"] for item in context.cookies()}
        user_agent = page.evaluate("() => navigator.userAgent")
        browser.close()
    return {"cookies": cookies, "user_agent": user_agent}


def build_requests_session(cookies: Dict[str, str], user_agent: str, referer: str = f"{BASE_URL}/#/catalogStatic") -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Referer": referer,
            "Origin": BASE_URL,
            "Accept": "application/json, text/plain, */*",
        }
    )
    session.cookies.update(cookies)
    return session


def fetch_catalog(session: requests.Session, page: int = 1, limit: int = 10) -> Dict:
    response = session.post(CATALOG_BROWSE_URL, json={"page": page, "limit": limit}, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not data.get("success"):
        raise RuntimeError(f"catalog request failed: {data.get('errorMsg') or data}")
    return data


def fetch_all_catalog(session: requests.Session, limit: int = 10) -> List[Dict]:
    first = fetch_catalog(session, page=1, limit=limit)
    total = int(first["data"]["total"])
    rows = list(first["data"]["list"])
    total_pages = int(math.ceil(total / float(limit)))
    for page in range(2, total_pages + 1):
        payload = fetch_catalog(session, page=page, limit=limit)
        rows.extend(payload["data"]["list"])
    return rows


def save_catalog(rows: Sequence[Dict], output_json: Path, output_csv: Optional[Path] = None) -> None:
    ensure_dir(output_json.parent)
    output_json.write_text(json.dumps(list(rows), ensure_ascii=False, indent=2), encoding="utf-8")
    if output_csv is not None:
        import csv

        ensure_dir(output_csv.parent)
        fieldnames = [
            "id",
            "rcId",
            "dataNo",
            "dataName",
            "countryName",
            "replacementCycle",
            "dataDescribe",
            "manual_url",
            "example_url",
        ]
        with output_csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                manual = (row.get("dataManual") or [{}])[0]
                example = (row.get("dataExample") or [{}])[0]
                writer.writerow(
                    {
                        "id": row.get("id", ""),
                        "rcId": row.get("rcId", ""),
                        "dataNo": row.get("dataNo", ""),
                        "dataName": row.get("dataName", ""),
                        "countryName": row.get("countryName", ""),
                        "replacementCycle": row.get("replacementCycle", ""),
                        "dataDescribe": row.get("dataDescribe", ""),
                        "manual_url": manual.get("url", ""),
                        "example_url": example.get("url", ""),
                    }
                )


def filter_catalog(rows: Sequence[Dict], data_nos: Sequence[str]) -> List[Dict]:
    wanted = {x.strip() for x in data_nos if x and x.strip()}
    if not wanted:
        return list(rows)
    return [row for row in rows if row.get("dataNo") in wanted]


def download_resource_sample(session: requests.Session, rc_id: str, output_path: Path, file_type: int = 2) -> Path:
    ensure_dir(output_path.parent)
    response = session.get(DOWNLOAD_URL, params={"rcId": rc_id, "fileType": file_type}, timeout=90)
    response.raise_for_status()
    output_path.write_bytes(response.content)
    return output_path


def _first_text(node, xpath: str, namespaces: Dict[str, str]) -> str:
    value = node.xpath(xpath, namespaces=namespaces)
    if not value:
        return ""
    item = value[0]
    if isinstance(item, etree._Element):
        return (item.text or "").strip()
    return str(item).strip()


def _first_attr(node, xpath: str, attr: str, namespaces: Dict[str, str]) -> str:
    values = node.xpath(xpath, namespaces=namespaces)
    if not values:
        return ""
    item = values[0]
    if isinstance(item, etree._Element):
        return str(item.attrib.get(attr, "")).strip()
    return ""


def _normalize_doc_number(text: str) -> str:
    return normalize_input_id(text or "")


def parse_legal_status_xml(xml_bytes: bytes, *, source_url: str, data_no: str, rc_id: str, crawl_time: Optional[str] = None) -> List[Dict[str, str]]:
    ns = {"business": "http://www.sipo.gov.cn/XMLSchema/business", "base": "http://www.sipo.gov.cn/XMLSchema/base"}
    rows: List[Dict[str, str]] = []
    for _, rec in etree.iterparse(io.BytesIO(xml_bytes), events=("end",), tag="{http://www.sipo.gov.cn/XMLSchema/business}PRSRecord"):
        standard_doc = _first_text(rec, ".//business:ApplicationReference[@dataFormat='standard']/base:DocumentID/base:DocNumber", ns)
        original_doc = _first_text(rec, ".//business:ApplicationReference[@dataFormat='original']/base:DocumentID/base:DocNumber", ns)
        application_date = _first_text(rec, ".//business:ApplicationReference[@dataFormat='standard']/base:DocumentID/base:Date", ns)
        prs_code = _first_text(rec, ".//business:PRSCode", ns)
        prs_value = _first_text(rec, ".//business:PRSValue", ns)
        prs_information = _first_text(rec, ".//business:PRSInformation", ns)
        publication_date = _first_text(rec, ".//business:PRSPublicationDate/base:Date", ns)
        raw_text = "\n".join([x for x in [prs_value, prs_information] if x]).strip()
        event_category = classify_event_category(raw_text)
        event_date = extract_first_date(raw_text) or publication_date or application_date
        matched = _normalize_doc_number(original_doc or standard_doc)
        patent_no = ""
        m_patent = re.search(r"专利号[:：]\s*([A-Z0-9.]+)", prs_information or "")
        if m_patent:
            patent_no = normalize_input_id(m_patent.group(1))
        rows.append(
            {
                "input_id": matched,
                "input_id_type": "application_no",
                "matched_patent_id": matched,
                "title": "",
                "applicant": "",
                "event_date": event_date,
                "event_name_raw": prs_value,
                "event_text_raw": raw_text,
                "event_category": event_category,
                "source_url": source_url,
                "crawl_time": crawl_time or now_iso(),
                "parse_status": "ok",
                "notes": "; ".join(
                    [
                        f"dataNo={data_no}",
                        f"rcId={rc_id}",
                        f"PRSCode={prs_code}",
                        f"standard_doc={standard_doc}",
                        f"original_doc={original_doc}",
                        f"patent_no={patent_no}" if patent_no else "",
                    ]
                ).strip("; "),
            }
        )
        rec.clear()
    return rows


def extract_zip_xml_rows(zip_bytes: bytes, *, source_url: str, data_no: str, rc_id: str, crawl_time: Optional[str] = None) -> List[Dict[str, str]]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    xml_names = [name for name in zf.namelist() if name.lower().endswith(".xml")]
    if not xml_names:
        raise RuntimeError("zip archive does not contain XML")
    rows: List[Dict[str, str]] = []
    for xml_name in xml_names:
        rows.extend(parse_legal_status_xml(zf.read(xml_name), source_url=source_url, data_no=data_no, rc_id=rc_id, crawl_time=crawl_time))
    return rows


def iter_zip_xml_files(zip_bytes: bytes, prefix: str = "") -> Iterator[Tuple[str, bytes]]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    for name in zf.namelist():
        if name.endswith("/"):
            continue
        payload = zf.read(name)
        full_name = f"{prefix}{name}" if prefix else name
        if name.lower().endswith(".zip") and payload[:4] == b"PK\x03\x04":
            yield from iter_zip_xml_files(payload, prefix=full_name + "::")
        elif name.lower().endswith(".xml"):
            yield full_name, payload


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _normalize_city_from_address(province: str, city: str) -> str:
    province = _clean_text(province)
    city = _clean_text(city)
    if province in MUNICIPALITIES:
        return province
    if not city or city in {"0", "市辖区", "县", "区"}:
        return city or province
    return city


def parse_bibliographic_xml(
    xml_bytes: bytes,
    *,
    source_url: str,
    data_no: str,
    rc_id: str,
    crawl_time: Optional[str] = None,
) -> List[Dict[str, str]]:
    ns = {"business": "http://www.sipo.gov.cn/XMLSchema/business", "base": "http://www.sipo.gov.cn/XMLSchema/base"}
    root = etree.fromstring(xml_bytes)
    record = root
    application_no = _first_text(record, ".//business:ApplicationReference[1]/base:DocumentID/base:DocNumber", ns)
    application_date = _first_text(record, ".//business:ApplicationReference[1]/base:DocumentID/base:Date", ns)
    publication_no = root.attrib.get("docNumber", "") or _first_text(record, ".//business:PublicationReference[1]/base:DocumentID/base:DocNumber", ns)
    publication_date = root.attrib.get("datePublication", "") or _first_text(record, ".//business:PublicationReference[1]/base:DocumentID/base:Date", ns)
    title = _first_text(record, ".//business:InventionTitle", ns)
    applicant_nodes = record.xpath(".//business:Applicant", namespaces=ns)
    applicant_names: List[str] = []
    applicant_province = ""
    applicant_city = ""
    applicant_address_text = ""
    for idx, node in enumerate(applicant_nodes):
        name = _clean_text(_first_text(node, ".//base:Name", ns))
        if name:
            applicant_names.append(name)
        if idx == 0:
            applicant_province = _first_text(node, ".//base:Province", ns)
            applicant_city = _first_text(node, ".//base:City", ns)
            applicant_address_text = _first_text(node, ".//base:Text", ns)
    if not applicant_names:
        org_name = _first_text(record, ".//business:Applicant/base:AddressBook/base:Name", ns)
        if org_name:
            applicant_names = [org_name]
    city_name = _normalize_city_from_address(applicant_province, applicant_city)
    if city_name == "市辖区" and applicant_province in MUNICIPALITIES:
        city_name = applicant_province
    application_year = (application_date or publication_date or "")[:4]
    return [
        {
            "input_id": application_no,
            "input_id_type": "application_no",
            "application_no": application_no,
            "publication_no": publication_no,
            "title": title,
            "applicant": "; ".join(dict.fromkeys(applicant_names)),
            "applicant_address": applicant_address_text,
            "province_name": applicant_province,
            "city_name": city_name,
            "application_date": application_date,
            "publication_date": publication_date,
            "year": application_year,
            "source_url": source_url,
            "crawl_time": crawl_time or now_iso(),
            "parse_status": "ok" if application_no else "parse_failed",
            "notes": "; ".join(
                [
                    f"dataNo={data_no}",
                    f"rcId={rc_id}",
                ]
            ),
        }
    ]
