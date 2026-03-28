from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from cnipa_utils import ensure_dir, infer_city_from_text, load_csv_rows, normalize_input_id, write_csv_rows


DEFAULT_KEEP_STATUSES = {"keep", "likely_continued_payment"}


def _normalize_city_key(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return ""
    return text.replace(" ", "")


def load_city_master(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"city_adcode": "Int64", "province_adcode": "Int64"})
    municipalities = pd.DataFrame(
        [
            {"year": int(df["year"].max()) if "year" in df.columns else 2024, "admin_level": "prefecture", "city_type": "municipality", "province_name": "北京市", "province_adcode": 110000, "city_name": "北京市", "city_short_name": "北京", "city_adcode": 110100},
            {"year": int(df["year"].max()) if "year" in df.columns else 2024, "admin_level": "prefecture", "city_type": "municipality", "province_name": "天津市", "province_adcode": 120000, "city_name": "天津市", "city_short_name": "天津", "city_adcode": 120100},
            {"year": int(df["year"].max()) if "year" in df.columns else 2024, "admin_level": "prefecture", "city_type": "municipality", "province_name": "上海市", "province_adcode": 310000, "city_name": "上海市", "city_short_name": "上海", "city_adcode": 310100},
            {"year": int(df["year"].max()) if "year" in df.columns else 2024, "admin_level": "prefecture", "city_type": "municipality", "province_name": "重庆市", "province_adcode": 500000, "city_name": "重庆市", "city_short_name": "重庆", "city_adcode": 500100},
        ]
    )
    df = pd.concat([df, municipalities], ignore_index=True, sort=False)
    df["city_name_key"] = df["city_name"].map(_normalize_city_key)
    df["city_short_key"] = df["city_short_name"].map(_normalize_city_key)
    return df


def build_city_lookup(city_master: pd.DataFrame) -> List[tuple[str, str]]:
    lookup: List[tuple[str, str]] = []
    for _, row in city_master.dropna(subset=["city_name"]).drop_duplicates(["city_name"]).iterrows():
        city_name = str(row["city_name"]).strip()
        short_name = str(row.get("city_short_name", "") or "").strip()
        if city_name:
            lookup.append((_normalize_city_key(city_name), city_name))
        if short_name and short_name != city_name:
            lookup.append((_normalize_city_key(short_name), city_name))
    lookup = sorted({item for item in lookup if item[0]}, key=lambda x: (-len(x[0]), x[0]))
    return lookup


def load_patent_master(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    if "input_id" not in df.columns:
        raise ValueError("patent master must contain input_id")
    df["input_id"] = df["input_id"].map(normalize_input_id)
    if "year" not in df.columns:
        for col in ["patent_year", "application_year", "pub_year", "grant_year"]:
            if col in df.columns:
                df["year"] = df[col]
                break
    if "year" not in df.columns:
        raise ValueError("patent master must contain a year/application_year/patent_year column")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


def attach_city_master(patents: pd.DataFrame, city_master: pd.DataFrame) -> pd.DataFrame:
    df = patents.copy()
    if "city_adcode" in df.columns and df["city_adcode"].notna().any():
        df["city_adcode"] = pd.to_numeric(df["city_adcode"], errors="coerce").astype("Int64")
        for col in ["province_name", "city_name", "city_short_name", "city_type", "province_adcode"]:
            if col in df.columns:
                df = df.drop(columns=[col])
        df = df.merge(
            city_master.drop_duplicates(["city_adcode"])[
                ["city_adcode", "province_name", "city_name", "city_short_name", "city_type", "province_adcode"]
            ],
            on="city_adcode",
            how="left",
        )
    else:
        city_col = None
        for candidate in ["city_name", "city", "patent_city", "city_short_name"]:
            if candidate in df.columns:
                city_col = candidate
                break
        if city_col is None:
            raise ValueError(
                "patent master must contain either city_adcode or one of city_name/city/patent_city/city_short_name"
            )
        df["city_key"] = df[city_col].map(_normalize_city_key)
        for col in ["province_name", "province_adcode", "city_name", "city_short_name", "city_type", "city_adcode"]:
            if col in df.columns:
                df = df.drop(columns=[col])
        city_map = city_master[
            ["province_name", "province_adcode", "city_name", "city_short_name", "city_type", "city_adcode", "city_name_key", "city_short_key"]
        ].drop_duplicates()
        df = df.merge(
            city_map[
                ["province_name", "province_adcode", "city_name", "city_short_name", "city_type", "city_adcode", "city_name_key"]
            ],
            left_on="city_key",
            right_on="city_name_key",
            how="left",
        )
        missing = df["city_adcode"].isna()
        if missing.any():
            fallback = city_map[
                ["province_name", "province_adcode", "city_name", "city_short_name", "city_type", "city_adcode", "city_short_key"]
            ].rename(
                columns={
                    "province_name": "province_name_fb",
                    "province_adcode": "province_adcode_fb",
                    "city_name": "city_name_fb",
                    "city_short_name": "city_short_name_fb",
                    "city_type": "city_type_fb",
                    "city_adcode": "city_adcode_fb",
                    "city_short_key": "city_key",
                }
            )
            secondary = df.loc[missing, ["city_key"]].merge(fallback, on="city_key", how="left")
            for target, fb in [
                ("province_name", "province_name_fb"),
                ("province_adcode", "province_adcode_fb"),
                ("city_name", "city_name_fb"),
                ("city_short_name", "city_short_name_fb"),
                ("city_type", "city_type_fb"),
                ("city_adcode", "city_adcode_fb"),
            ]:
                df.loc[missing, target] = secondary[fb].values
        df = df.drop(columns=[c for c in ["city_key", "city_name_key", "city_short_key"] if c in df.columns], errors="ignore")
    return df


def infer_city_columns(patents: pd.DataFrame, city_master: pd.DataFrame) -> pd.DataFrame:
    lookup = build_city_lookup(city_master)
    city_map = city_master.drop_duplicates(["city_name"]).set_index("city_name")
    df = patents.copy()
    if "city_name" not in df.columns:
        df["city_name"] = ""
    if "province_name" not in df.columns:
        df["province_name"] = ""
    if "city_adcode" not in df.columns:
        df["city_adcode"] = pd.NA
    if "province_adcode" not in df.columns:
        df["province_adcode"] = pd.NA
    if "city_short_name" not in df.columns:
        df["city_short_name"] = ""
    if "city_type" not in df.columns:
        df["city_type"] = ""

    candidate_cols = [
        "city_name",
        "city_short_name",
        "province_name",
        "applicant_address",
        "applicant",
        "title",
    ]
    for idx, row in df.iterrows():
        current_city = _normalize_city_key(row.get("city_name"))
        if current_city:
            continue
        inferred = ""
        for col in candidate_cols:
            inferred = infer_city_from_text(row.get(col, ""), lookup)
            if inferred:
                break
        if not inferred:
            continue
        meta = city_map.loc[inferred] if inferred in city_map.index else None
        df.at[idx, "city_name"] = inferred
        if meta is not None:
            df.at[idx, "province_name"] = meta.get("province_name", df.at[idx, "province_name"])
            df.at[idx, "province_adcode"] = meta.get("province_adcode", df.at[idx, "province_adcode"])
            df.at[idx, "city_short_name"] = meta.get("city_short_name", df.at[idx, "city_short_name"])
            df.at[idx, "city_adcode"] = meta.get("city_adcode", df.at[idx, "city_adcode"])
            df.at[idx, "city_type"] = meta.get("city_type", df.at[idx, "city_type"])
    return df


def attach_fee_inference(patents: pd.DataFrame, fee_csv: Path) -> pd.DataFrame:
    fee = pd.read_csv(fee_csv, dtype=str)
    patents = patents.copy()
    fee = fee.copy()
    patents["join_input_id"] = patents["input_id"].map(normalize_input_id)
    fee["join_input_id"] = fee["input_id"].map(normalize_input_id)
    merged = patents.merge(fee, on="join_input_id", how="left", suffixes=("", "_fee"))
    if "input_id_fee" in merged.columns and "input_id" not in merged.columns:
        merged = merged.rename(columns={"input_id_fee": "input_id"})
    merged = merged.drop(columns=["join_input_id"], errors="ignore")
    merged["panel_exclusion_recommendation"] = merged["panel_exclusion_recommendation"].fillna("keep")
    merged["inferred_fee_status"] = merged["inferred_fee_status"].fillna("no_legal_event_found")
    merged["has_annual_fee_termination_event"] = merged["has_annual_fee_termination_event"].fillna("false")
    merged["has_deemed_abandoned_event"] = merged["has_deemed_abandoned_event"].fillna("false")
    merged["has_right_restoration_event"] = merged["has_right_restoration_event"].fillna("false")
    return merged


def build_panel_rows(df: pd.DataFrame, fill_zeros: bool = False) -> pd.DataFrame:
    required_cols = {"year", "city_name", "province_name"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"missing required columns after merge: {sorted(missing)}")

    work = df.dropna(subset=["year", "city_name", "province_name", "city_adcode"]).copy()
    work["is_fee_nonpayment_termination"] = work["has_annual_fee_termination_event"].astype(str).str.lower().eq("true")
    work["is_deemed_abandoned"] = work["has_deemed_abandoned_event"].astype(str).str.lower().eq("true")
    work["is_restoration"] = work["has_right_restoration_event"].astype(str).str.lower().eq("true")
    work["is_unspecified_termination"] = work["inferred_fee_status_rule"].fillna("").eq("termination_event_without_fee_context")
    work["is_fee_nonpayment_excluded"] = work["is_fee_nonpayment_termination"]
    work["is_deemed_abandoned_excluded"] = work["is_deemed_abandoned"]
    work["is_unspecified_termination_excluded"] = work["is_unspecified_termination"]
    work["is_restoration_excluded"] = work["is_restoration"]
    work["is_excluded"] = work["panel_exclusion_recommendation"].astype(str).str.lower().eq("exclude")
    work["keep_flag"] = ~work["is_excluded"]

    group_cols = ["year", "province_name", "province_adcode", "city_name", "city_short_name", "city_adcode", "city_type"]
    agg = (
        work.groupby(group_cols, dropna=False)
        .agg(
            patent_count=("input_id", "nunique"),
            fee_nonpayment_termination_patent_count=("is_fee_nonpayment_termination", "sum"),
            deemed_abandoned_patent_count=("is_deemed_abandoned", "sum"),
            restoration_patent_count=("is_restoration", "sum"),
            unspecified_termination_patent_count=("is_unspecified_termination", "sum"),
            fee_nonpayment_excluded_patent_count=("is_fee_nonpayment_excluded", "sum"),
            deemed_abandoned_excluded_patent_count=("is_deemed_abandoned_excluded", "sum"),
            unspecified_termination_excluded_patent_count=("is_unspecified_termination_excluded", "sum"),
            restoration_excluded_patent_count=("is_restoration_excluded", "sum"),
            excluded_patent_count=("is_excluded", "sum"),
            kept_patent_count=("keep_flag", "sum"),
        )
        .reset_index()
    )

    agg["panel_share_excluded"] = agg["excluded_patent_count"] / agg["patent_count"].where(agg["patent_count"] != 0, 1)
    agg["panel_share_fee_termination"] = agg["fee_nonpayment_termination_patent_count"] / agg["patent_count"].where(agg["patent_count"] != 0, 1)
    agg["panel_share_deemed_abandoned"] = agg["deemed_abandoned_patent_count"] / agg["patent_count"].where(agg["patent_count"] != 0, 1)
    agg["panel_share_unspecified_termination"] = agg["unspecified_termination_patent_count"] / agg["patent_count"].where(agg["patent_count"] != 0, 1)
    agg["panel_share_restoration"] = agg["restoration_patent_count"] / agg["patent_count"].where(agg["patent_count"] != 0, 1)

    if fill_zeros:
        years = sorted([int(y) for y in df["year"].dropna().unique().tolist()])
        cities = df[["province_name", "province_adcode", "city_name", "city_short_name", "city_adcode", "city_type"]].drop_duplicates()
        grid = cities.assign(key=1).merge(pd.DataFrame({"year": years, "key": 1}), on="key").drop(columns=["key"])
        agg = grid.merge(agg, on=["year", "province_name", "province_adcode", "city_name", "city_short_name", "city_adcode", "city_type"], how="left")
        count_cols = [
            "patent_count",
            "fee_nonpayment_termination_patent_count",
            "deemed_abandoned_patent_count",
            "restoration_patent_count",
            "unspecified_termination_patent_count",
            "fee_nonpayment_excluded_patent_count",
            "deemed_abandoned_excluded_patent_count",
            "unspecified_termination_excluded_patent_count",
            "restoration_excluded_patent_count",
            "excluded_patent_count",
            "kept_patent_count",
        ]
        for col in count_cols:
            agg[col] = agg[col].fillna(0).astype(int)
        for col in [
            "panel_share_excluded",
            "panel_share_fee_termination",
            "panel_share_deemed_abandoned",
            "panel_share_unspecified_termination",
            "panel_share_restoration",
        ]:
            agg[col] = agg[col].fillna(0.0)

    agg = agg.sort_values(["year", "province_adcode", "city_adcode"]).reset_index(drop=True)
    return agg


def build_city_patent_panel(
    patent_master_csv: Path,
    fee_inference_csv: Path,
    city_master_csv: Path,
    output_csv: Path,
    fill_zeros: bool = False,
) -> pd.DataFrame:
    city_master = load_city_master(city_master_csv)
    patents = load_patent_master(patent_master_csv)
    patents = infer_city_columns(patents, city_master)
    patents = attach_city_master(patents, city_master)
    patents = attach_fee_inference(patents, fee_inference_csv)
    panel = build_panel_rows(patents, fill_zeros=fill_zeros)
    ensure_dir(output_csv.parent)
    panel.to_csv(output_csv, index=False, encoding="utf-8")
    return panel


def main():
    parser = argparse.ArgumentParser(description="Build prefecture-city patent panel with fee-exclusion counts")
    parser.add_argument("--patents", required=True, type=Path, help="Patent master with input_id, year, and city fields")
    parser.add_argument("--fees", default=Path("outputs/patent_fee_inference.csv"), type=Path)
    parser.add_argument("--cities", default=Path("raw/reference/prefecture_level_cities.csv"), type=Path)
    parser.add_argument("--output", default=Path("outputs/city_patent_panel.csv"), type=Path)
    parser.add_argument("--fill-zeros", action="store_true")
    args = parser.parse_args()
    build_city_patent_panel(args.patents, args.fees, args.cities, args.output, fill_zeros=args.fill_zeros)


if __name__ == "__main__":
    main()
