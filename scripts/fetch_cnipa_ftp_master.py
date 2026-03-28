from __future__ import annotations

import argparse
import csv
import io
import os
import sqlite3
import subprocess
import tempfile
import zipfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from ftplib import FTP
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from Cryptodome.Cipher import AES
from Cryptodome.Hash import SHA1
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Util.Padding import unpad

from cnipa_public_platform import parse_bibliographic_xml, parse_legal_status_xml
from cnipa_utils import ensure_dir, now_iso


BASE_HOSTS = [
    "ftp1.ipdps.cnipa.gov.cn",
    "ftp2.ipdps.cnipa.gov.cn",
    "ftp3.ipdps.cnipa.gov.cn",
    "ftp4.ipdps.cnipa.gov.cn",
]
CHROME_LOGIN_DOMAIN = "ipdps.cnipa.gov.cn"


@dataclass
class FtpCredentials:
    username: str
    password: str


def _chrome_safe_storage_password() -> bytes:
    out = subprocess.check_output(
        ["security", "find-generic-password", "-s", "Chrome Safe Storage", "-g"],
        stderr=subprocess.STDOUT,
        text=True,
    )
    for line in out.splitlines():
        if line.startswith('password: "'):
            return line.split('password: "', 1)[1].rsplit('"', 1)[0].encode("utf-8")
    raise RuntimeError("Chrome Safe Storage password not found")


def _decrypt_chrome_password(password_value: bytes) -> str:
    if len(password_value) >= 3 and password_value[:3] == b"v10":
        key = PBKDF2(_chrome_safe_storage_password(), b"saltysalt", dkLen=16, count=1003, hmac_hash_module=SHA1)
        cipher = AES.new(key, AES.MODE_CBC, iv=b" " * 16)
        plaintext = unpad(cipher.decrypt(password_value[3:]), 16)
        return plaintext.decode("utf-8", errors="ignore")
    raise RuntimeError("Unsupported Chrome password format")


def _chrome_saved_account(domain: str = CHROME_LOGIN_DOMAIN) -> Optional[str]:
    storage_roots = [
        Path.home() / "Library/Application Support/Google/Chrome/Default/Local Storage/leveldb",
        Path.home() / "Library/Application Support/Google/Chrome/Default/Session Storage",
    ]
    for root in storage_roots:
        if not root.exists():
            continue
        for file in sorted(root.glob("*")):
            if not file.is_file():
                continue
            try:
                text = file.read_bytes().decode("utf-8", errors="ignore")
            except OSError:
                continue
            if "User-Info" not in text or "account" not in text:
                continue
            cleaned = text.replace("\x00", "")
            import re

            match = re.search(r'"account":"([^"]+)"', cleaned)
            if match:
                return match.group(1)
    return None


def _saved_chrome_ftp_credentials(domain: str = CHROME_LOGIN_DOMAIN) -> Optional[FtpCredentials]:
    login_db = Path.home() / "Library/Application Support/Google/Chrome/Default/Login Data"
    if not login_db.exists():
        return None
    tmp = Path(tempfile.gettempdir()) / "chrome_login_data_copy.db"
    tmp.write_bytes(login_db.read_bytes())
    try:
        conn = sqlite3.connect(tmp)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT username_value, password_value
            FROM logins
            WHERE origin_url LIKE ? OR action_url LIKE ?
            ORDER BY date_created DESC
            LIMIT 1
            """,
            (f"%{domain}%", f"%{domain}%"),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        username, password_blob = row
        account = _chrome_saved_account(domain)
        if account:
            username = account
        if not username or not password_blob:
            return None
        password = _decrypt_chrome_password(password_blob)
        return FtpCredentials(username=username, password=password)
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


def connect_ftp(host: str, creds: FtpCredentials) -> FTP:
    ftp = FTP(host, timeout=30)
    ftp.login(creds.username, creds.password)
    ftp.set_pasv(True)
    return ftp


def ftp_join(*parts: str) -> str:
    clean: List[str] = []
    for part in parts:
        if not part:
            continue
        clean.append(part.strip("/"))
    if not clean:
        return "/"
    return "/" + "/".join(clean)


def list_dirs(ftp: FTP, path: str) -> List[str]:
    return ftp.nlst(path)


def download_file_bytes(ftp: FTP, path: str) -> bytes:
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {path}", buf.write)
    return buf.getvalue()


def iter_xml_rows_from_zip(zip_bytes: bytes, *, data_no: str, rc_id: str, source_url: str, crawl_time: str) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    bib_rows: List[Dict[str, str]] = []
    prss_rows: List[Dict[str, str]] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            payload = zf.read(name)
            if b"PatentDocumentAndRelated" in payload[:400] or b"<business:PatentDocumentAndRelated" in payload[:800]:
                bib_rows.extend(
                    parse_bibliographic_xml(
                        payload,
                        source_url=source_url,
                        data_no=data_no,
                        rc_id=rc_id,
                        crawl_time=crawl_time,
                    )
                )
            elif b"PRSRecord" in payload[:400] or b"<business:PRSRecord" in payload[:800]:
                prss_rows.extend(
                    parse_legal_status_xml(
                        payload,
                        source_url=source_url,
                        data_no=data_no,
                        rc_id=rc_id,
                        crawl_time=crawl_time,
                    )
                )
            else:
                # Try both parsers defensively.
                try:
                    bib_rows.extend(
                        parse_bibliographic_xml(
                            payload,
                            source_url=source_url,
                            data_no=data_no,
                            rc_id=rc_id,
                            crawl_time=crawl_time,
                        )
                    )
                    continue
                except Exception:
                    pass
                try:
                    prss_rows.extend(
                        parse_legal_status_xml(
                            payload,
                            source_url=source_url,
                            data_no=data_no,
                            rc_id=rc_id,
                            crawl_time=crawl_time,
                        )
                    )
                except Exception:
                    continue
    return bib_rows, prss_rows


def collect_rawdata_date(
    ftp: FTP,
    *,
    host: str,
    creds: FtpCredentials,
    package_dir: str,
    date_folder: str,
    data_no: str,
    kind: str,
    crawl_time: str,
    max_zips: int = 0,
    workers: int = 1,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Collect rows from a single rawdata date folder.

    Returns (bibliographic_rows, legal_status_rows).
    """
    candidate_folders = [
        ftp_join(package_dir, "data", f"{date_folder}rawdata"),
        ftp_join(package_dir, "data", date_folder),
    ]
    folder = ""
    items: List[str] = []
    discovery_ftp = FTP(host, timeout=30)
    try:
        discovery_ftp.login(creds.username, creds.password)
        discovery_ftp.set_pasv(True)
        for candidate in candidate_folders:
            try:
                discovery_ftp.cwd(candidate)
                folder = candidate
                items = discovery_ftp.nlst()
                break
            except Exception:
                continue
    finally:
        try:
            discovery_ftp.quit()
        except Exception:
            pass
    if not folder:
        raise RuntimeError(f"could not locate rawdata folder for {package_dir} {date_folder}")
    zip_names = sorted([name for name in items if name.lower().endswith(".zip")])
    if max_zips and max_zips > 0:
        zip_names = zip_names[:max_zips]
    bib_rows: List[Dict[str, str]] = []
    prss_rows: List[Dict[str, str]] = []
    logger = logging.getLogger("cnipa_ftp")

    def process_zip(zip_name: str) -> Tuple[str, List[Dict[str, str]], List[Dict[str, str]]]:
        local_ftp = FTP(host, timeout=60)
        try:
            local_ftp.login(creds.username, creds.password)
            local_ftp.set_pasv(True)
            local_ftp.cwd(folder)
            try:
                zip_size = local_ftp.size(zip_name)
            except Exception:
                zip_size = None
            if zip_size is not None:
                logger.info("downloading %s (%s bytes)", zip_name, zip_size)
            else:
                logger.info("downloading %s", zip_name)
            source_url = f"ftp://{host}{folder}/{zip_name}"
            zip_bytes = download_file_bytes(local_ftp, zip_name)
            chunk_bib_rows, chunk_prss_rows = iter_xml_rows_from_zip(
                zip_bytes,
                data_no=data_no,
                rc_id=package_dir.split("/", 1)[-1],
                source_url=source_url,
                crawl_time=crawl_time,
            )
            return zip_name, chunk_bib_rows, chunk_prss_rows
        finally:
            try:
                local_ftp.quit()
            except Exception:
                pass

    if workers and workers > 1 and len(zip_names) > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_zip, zip_name): zip_name for zip_name in zip_names}
            for future in as_completed(futures):
                zip_name, chunk_bib_rows, chunk_prss_rows = future.result()
                bib_rows.extend(chunk_bib_rows)
                prss_rows.extend(chunk_prss_rows)
                logger.info("parsed %s: bib_rows=%s legal_rows=%s", zip_name, len(chunk_bib_rows), len(chunk_prss_rows))
    else:
        for zip_name in zip_names:
            zip_name, chunk_bib_rows, chunk_prss_rows = process_zip(zip_name)
            bib_rows.extend(chunk_bib_rows)
            prss_rows.extend(chunk_prss_rows)
            logger.info("parsed %s: bib_rows=%s legal_rows=%s", zip_name, len(chunk_bib_rows), len(chunk_prss_rows))

    return bib_rows, prss_rows


def discover_package_dir(ftp: FTP, data_no: str) -> Optional[str]:
    for item in ftp.nlst("/CN"):
        if item.startswith(data_no):
            return ftp_join("CN", item)
    return None


def parse_dates_arg(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    for value in values:
        if not value:
            continue
        for piece in value.split(","):
            piece = piece.strip()
            if piece:
                out.append(piece)
    return sorted(set(out))


def discover_rawdata_folders(ftp: FTP, data_root: str) -> List[str]:
    folders: List[str] = []
    for name in ftp.nlst(data_root):
        base = name.rsplit("/", 1)[-1]
        if base.endswith("rawdata") and base[:8].isdigit():
            folders.append(base)
    folders = sorted(set(folders))
    return folders


def write_csv(path: Path, rows: Iterable[Dict[str, str]], fieldnames: Sequence[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and parse CNIPA FTP rawdata packages.")
    parser.add_argument("--data-no", action="append", default=[], help="Data numbers to fetch. Can be repeated.")
    parser.add_argument("--date", action="append", default=[], help="Date folder(s) such as 20260324. Can be repeated or comma-separated.")
    parser.add_argument("--limit-dates", type=int, default=1, help="Limit the number of latest dates per package.")
    parser.add_argument("--output-master", default="outputs/patent_master_ftp.csv", help="Bibliographic output CSV.")
    parser.add_argument("--output-legal", default="outputs/patent_legal_events_ftp.csv", help="Legal-status output CSV.")
    parser.add_argument("--host", default=BASE_HOSTS[0], help="FTP host to use.")
    parser.add_argument("--username", default=os.environ.get("CNIPA_FTP_USER", ""), help="FTP username.")
    parser.add_argument("--password", default=os.environ.get("CNIPA_FTP_PASS", ""), help="FTP password.")
    parser.add_argument("--package-dir", default="", help="Optional full package dir under /CN to skip discovery.")
    parser.add_argument("--max-zips", type=int, default=0, help="Limit ZIP shards per date folder for sampling.")
    parser.add_argument("--workers", type=int, default=4, help="Parallel ZIP workers per rawdata folder.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("cnipa_ftp")

    default_data_nos = ["CN-PA-BIBS-ABSS-10-A", "CN-PA-PRSS-10"]
    data_nos = [item for item in dict.fromkeys(args.data_no or default_data_nos) if item]
    wanted_dates = parse_dates_arg(args.date)
    crawl_time = now_iso()

    creds = FtpCredentials(username=args.username, password=args.password) if args.username and args.password else _saved_chrome_ftp_credentials()
    if not creds:
        raise RuntimeError("No FTP credentials found. Provide CNIPA_FTP_USER/CNIPA_FTP_PASS or save credentials in Chrome.")

    all_master_rows: List[Dict[str, str]] = []
    all_legal_rows: List[Dict[str, str]] = []

    for data_no in data_nos:
        logger.info("connecting to %s for %s", args.host, data_no)
        ftp = connect_ftp(args.host, creds)
        try:
            package_dir = args.package_dir.strip() or discover_package_dir(ftp, data_no)
            if not package_dir:
                logger.warning("package dir not found for %s", data_no)
                continue
            logger.info("package dir %s", package_dir)
            data_root = ftp_join(package_dir, "data")
            if wanted_dates:
                dates = [d for d in wanted_dates if d]
            else:
                dates = discover_rawdata_folders(ftp, data_root)
                if args.limit_dates and args.limit_dates > 0:
                    dates = dates[-args.limit_dates :]
            logger.info("dates to process for %s: %s", data_no, ", ".join(dates) if dates else "(none)")
            for date_folder in dates:
                logger.info("processing %s %s", data_no, date_folder)
                bib_rows, prss_rows = collect_rawdata_date(
                    ftp,
                    host=args.host,
                    creds=creds,
                    package_dir=package_dir,
                    date_folder=date_folder,
                    data_no=data_no,
                    kind="bibs" if "BIBS" in data_no else "prss",
                    crawl_time=crawl_time,
                    max_zips=args.max_zips,
                    workers=args.workers,
                )
                all_master_rows.extend(bib_rows)
                all_legal_rows.extend(prss_rows)
                logger.info(
                    "finished %s %s: bib_rows=%s legal_rows=%s",
                    data_no,
                    date_folder,
                    len(bib_rows),
                    len(prss_rows),
                )
        finally:
            try:
                ftp.quit()
            except Exception:
                pass

    master_fields = [
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
    legal_fields = [
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
    def dedupe(rows: List[Dict[str, str]], key_fields: Sequence[str]) -> List[Dict[str, str]]:
        seen = {}
        for row in rows:
            key = tuple(row.get(field, "") for field in key_fields)
            seen[key] = row
        return list(seen.values())

    all_master_rows = dedupe(all_master_rows, ["input_id"])
    all_legal_rows = dedupe(all_legal_rows, ["input_id", "event_date", "event_name_raw", "event_text_raw"])

    write_csv(Path(args.output_master), all_master_rows, master_fields)
    write_csv(Path(args.output_legal), all_legal_rows, legal_fields)
    logger.info("master_rows=%s legal_rows=%s", len(all_master_rows), len(all_legal_rows))
    logger.info("processed data_nos=%s", ",".join(data_nos))
    print(f"master_rows={len(all_master_rows)} legal_rows={len(all_legal_rows)}")


if __name__ == "__main__":
    main()
