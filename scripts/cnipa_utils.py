from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


EVENT_CATEGORY = {
    "annual_fee_nonpayment_termination",
    "annual_fee_nonpayment_termination_final",
    "annual_fee_nonpayment_termination_restorable",
    "right_restoration",
    "deemed_abandoned",
    "termination_unspecified",
    "other",
}


INFERRED_FEE_STATUS = {
    "likely_continued_payment",
    "likely_stopped_payment_due_to_fee_nonpayment",
    "deemed_abandoned",
    "ambiguous",
    "restored_after_lapse",
    "no_legal_event_found",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv_rows(path: Path, rows: Sequence[Dict[str, object]], fieldnames: Sequence[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def safe_filename(value: str) -> str:
    value = value.strip()
    value = re.sub(r"[^\w\u4e00-\u9fff.-]+", "_", value)
    return value[:120] or "sample"


def normalize_input_id(input_id: str) -> str:
    return re.sub(r"[\s\-.]", "", (input_id or "").strip()).upper()


def normalize_patent_join_key(input_id: str) -> str:
    """Normalize patent IDs for cross-source joins.

    The yearly RAR archive often stores application numbers with a leading
    country prefix such as ``CN`` while the fee/legal-state tables may omit it.
    This helper keeps the alphanumeric core aligned across sources.
    """
    text = normalize_input_id(input_id)
    if text.startswith("CN") and len(text) > 2:
        return text[2:]
    return text


def normalize_patentstar_application_no(input_id: str) -> str:
    """Normalize an application number to PatentStar's batch-search format.

    PatentStar expects standard Chinese application numbers in the form:
        CN + 12 digits + "." + check digit

    Examples:
        201610158350X -> CN201610158350.X
        CN201610158350X -> CN201610158350.X
        CN201610158350.X -> CN201610158350.X
    """
    text = normalize_input_id(input_id)
    if not text:
        return ""
    if text.startswith("CN"):
        text = text[2:]
    text = re.sub(r"[^A-Z0-9]", "", text.upper())
    if len(text) >= 13 and text[:12].isdigit():
        return f"CN{text[:12]}.{text[12]}"
    return f"CN{text}" if not text.startswith("CN") else text


def normalize_year_value(value: object) -> str:
    text = normalize_input_id(str(value) if value is not None else "")
    if not text:
        return ""
    if re.fullmatch(r"\d{4}\.0", text):
        return text[:4]
    if re.fullmatch(r"\d{4}", text):
        return text
    m = re.search(r"(\d{4})", text)
    return m.group(1) if m else text


def normalize_text_key(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = re.sub(r"\s+", "", text)
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"[,:：;；，。·/\\|_]+", "", text)
    return text


def infer_city_from_text(text: object, city_lookup: Sequence[Tuple[str, str]]) -> str:
    """Infer a prefecture-level city by searching for longest matching city tokens.

    city_lookup should be ordered from longest token to shortest token, with
    tuples of (token, canonical_city_name).
    """
    normalized = normalize_text_key(text)
    if not normalized:
        return ""
    for token, city_name in city_lookup:
        if token and token in normalized:
            return city_name
    return ""


def classify_input_type(input_id_type: str, input_id: str) -> str:
    t = (input_id_type or "").strip().lower()
    if t in {"application_no", "app_no", "an", "application"}:
        return "application_no"
    if t in {"publication_no", "pub_no", "pn", "publication"}:
        return "publication_no"
    if t in {"patent_no", "patent_number", "patent"}:
        return "patent_no"
    if re.fullmatch(r"\d{12,14}[A-Z]?", normalize_input_id(input_id)):
        return "application_no"
    return "unknown"


def looks_like_fee_termination(text: str) -> bool:
    t = text or ""
    return ("未缴年费" in t) or ("年费" in t and "终止" in t)


def classify_event_category(event_text: str) -> str:
    text = (event_text or "").strip()
    if not text:
        return "other"
    if "恢复权利" in text:
        return "right_restoration"
    if "视为放弃" in text or "视为撤回" in text:
        return "deemed_abandoned"
    if "未缴年费终止" in text:
        return "annual_fee_nonpayment_termination"
    if ("专利权终止" in text or "专利权的终止" in text or ("终止" in text and "专利权" in text)) and looks_like_fee_termination(text):
        return "annual_fee_nonpayment_termination"
    if "专利权的终止" in text or "专利权终止" in text:
        return "termination_unspecified"
    return "other"


def classify_cpquery_case_status(status_text: str) -> str:
    """Classify current-case-status text from the official review/query platform.

    The official query platform exposes current case status strings such as:
    - 未缴年费终止失效
    - 未缴年费专利权终止，等恢复
    - 视为放弃取得专利权
    - 恢复权利

    We keep the fee-termination split because it is useful for research
    sensitivity analysis, but both fee-termination variants should generally be
    treated as low-quality / drop candidates in city-level patent counts.
    """
    text = (status_text or "").strip()
    if not text:
        return "other"
    if "恢复权利" in text:
        return "right_restoration"
    if "视为放弃" in text or "视为撤回" in text:
        return "deemed_abandoned"
    if "未缴年费终止失效" in text:
        return "annual_fee_nonpayment_termination_final"
    if "未缴年费专利权终止" in text and "等恢复" in text:
        return "annual_fee_nonpayment_termination_restorable"
    if "未缴年费终止" in text:
        return "annual_fee_nonpayment_termination_final"
    if "未缴年费专利权终止" in text:
        return "annual_fee_nonpayment_termination_restorable"
    if "专利权终止" in text or "专利权的终止" in text:
        return "termination_unspecified"
    return "other"


def extract_first_date(text: str) -> str:
    """Extract an 8-digit date from Chinese legal-status text if present."""
    if not text:
        return ""
    patterns = [
        r"终止日期[:：]\s*(\d{8})",
        r"恢复权利日期[:：]\s*(\d{8})",
        r"恢复权利公告日[:：]\s*(\d{8})",
        r"视为撤回公告日[:：]\s*(\d{8})",
        r"法律状态公告日[:：]\s*(\d{8})",
        r"公告日[:：]\s*(\d{8})",
        r"生效日[:：]\s*(\d{8})",
        r"申请公布日[:：]\s*(\d{8})",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return m.group(1)
    return ""


def classify_event_text(event_text: str) -> str:
    return classify_event_category(event_text)


def dedupe_rows(rows: Sequence[Dict[str, object]], key_fields: Sequence[str]) -> List[Dict[str, object]]:
    seen = set()
    out = []
    for row in rows:
        key = tuple((row.get(k) or "") for k in key_fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def random_sleep(min_s: float, max_s: float) -> None:
    import time

    time.sleep(random.uniform(min_s, max_s))


def configure_logger(log_path: Path) -> logging.Logger:
    ensure_dir(log_path.parent)
    logger = logging.getLogger("cnipa_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    logger.propagate = False
    return logger


def is_blank_row(row: Dict[str, object]) -> bool:
    return all(not (row.get(k) or "").strip() for k in ["event_date", "event_name_raw", "event_text_raw"])
