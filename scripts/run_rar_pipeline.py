from __future__ import annotations

import argparse
from pathlib import Path

from build_city_patent_panel_from_rar import build_panel_from_rar
from build_patent_master_from_rar import build_master_rows, parse_years_arg
from cnipa_utils import configure_logger, ensure_dir


DEFAULT_ARCHIVE = Path("raw/分年份保存数据.rar")
DEFAULT_CITY_MASTER = Path("raw/reference/prefecture_level_cities.csv")
DEFAULT_FEE_INFERENCE = Path("outputs/patent_fee_inference_ftp_full.csv")
DEFAULT_MASTER_OUTPUT = Path("outputs/patent_master_rar_full.csv")
DEFAULT_PANEL_OUTPUT = Path("outputs/city_patent_panel_rar_full.csv")
DEFAULT_UNMATCHED_FEE_OUTPUT = Path("outputs/patent_fee_unmatched_to_rar_full.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full RAR-based CNIPA patent pipeline")
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE, type=Path)
    parser.add_argument("--cities", default=DEFAULT_CITY_MASTER, type=Path)
    parser.add_argument("--fees", default=DEFAULT_FEE_INFERENCE, type=Path)
    parser.add_argument("--master-output", default=DEFAULT_MASTER_OUTPUT, type=Path)
    parser.add_argument("--panel-output", default=DEFAULT_PANEL_OUTPUT, type=Path)
    parser.add_argument("--unmatched-fee-output", default=DEFAULT_UNMATCHED_FEE_OUTPUT, type=Path)
    parser.add_argument("--year", action="append", help="Optional year filter; may be repeated or comma-separated")
    parser.add_argument("--log", default=Path("logs/run_rar_pipeline.log"), type=Path)
    args = parser.parse_args()

    years = parse_years_arg(args.year)
    ensure_dir(args.master_output.parent)
    ensure_dir(args.panel_output.parent)
    ensure_dir(args.unmatched_fee_output.parent)
    logger = configure_logger(args.log)
    logger.info(
        "starting RAR pipeline archive=%s fees=%s years=%s",
        args.archive,
        args.fees,
        ",".join(years or []) or "ALL",
    )

    master_count = build_master_rows(
        archive=args.archive,
        city_master_csv=args.cities,
        output=args.master_output,
        years=years,
        log_path=args.log,
    )
    logger.info("master rows written: %d", master_count)

    panel_count = build_panel_from_rar(
        archive=args.archive,
        city_master_csv=args.cities,
        fee_inference_csv=args.fees,
        output=args.panel_output,
        unmatched_fee_output=args.unmatched_fee_output,
        years=years,
        fill_zeros=True,
        log_path=args.log,
    )
    logger.info("panel rows written: %d", panel_count)


if __name__ == "__main__":
    main()
