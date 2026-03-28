from __future__ import annotations

import argparse
from pathlib import Path
from sysconfig import get_paths

import pandas as pd

from cnipa_utils import ensure_dir


def _find_cnloc_source() -> Path:
    purelib = Path(get_paths()["purelib"])
    candidates = sorted(purelib.glob("cnloc/data/location_year_*.csv"))
    if not candidates:
        raise FileNotFoundError(f"cnloc data file not found under {purelib}")
    return candidates[-1]


def build_prefecture_city_master(output_csv: Path) -> pd.DataFrame:
    source = _find_cnloc_source()
    df = pd.read_csv(source, low_memory=False)
    latest_year = int(df["year"].max())
    latest = df[df["year"] == latest_year].copy()
    cities = latest[latest["rank"] == 2].copy()
    cities = cities.drop_duplicates(["city_adcode"]).sort_values(["province_adcode", "city_adcode"])
    cities["city_adcode"] = cities["city_adcode"].astype("Int64")
    cities["province_adcode"] = cities["province_adcode"].astype("Int64")
    cities["year"] = latest_year
    cities["admin_level"] = "prefecture"
    cities["city_type"] = cities["city_name"].apply(
        lambda x: "municipality" if x in {"北京市", "天津市", "上海市", "重庆市"} else
        "autonomous_prefecture" if "自治州" in str(x) else
        "league" if "盟" in str(x) else
        "prefecture_city"
    )
    cities = cities[
        [
            "year",
            "admin_level",
            "city_type",
            "province_name",
            "province_adcode",
            "city_name",
            "city_short",
            "city_adcode",
        ]
    ].rename(columns={"city_short": "city_short_name"})
    ensure_dir(output_csv.parent)
    cities.to_csv(output_csv, index=False, encoding="utf-8")
    return cities


def main():
    parser = argparse.ArgumentParser(description="Build prefecture-level city master from cnloc source data")
    parser.add_argument("--output", default=Path("raw/reference/prefecture_level_cities.csv"), type=Path)
    args = parser.parse_args()
    build_prefecture_city_master(args.output)


if __name__ == "__main__":
    main()
