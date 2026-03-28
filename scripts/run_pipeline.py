from __future__ import annotations

import argparse
from pathlib import Path

from cnipa_utils import configure_logger, ensure_dir, load_csv_rows, write_csv_rows
from fetch_cnipa_legal_status import fetch_batch
from infer_fee_status import infer_fee_status


def main():
    parser = argparse.ArgumentParser(description="Run the CNIPA legal-status to fee-status pipeline")
    parser.add_argument("--input", default=Path("raw/sample_patent_ids.csv"), type=Path)
    parser.add_argument("--config", default=Path("configs/default.json"), type=Path)
    parser.add_argument("--events-output", default=Path("outputs/patent_legal_events.csv"), type=Path)
    parser.add_argument("--fee-output", default=Path("outputs/patent_fee_inference.csv"), type=Path)
    parser.add_argument("--log", default=Path("logs/run.log"), type=Path)
    args = parser.parse_args()

    ensure_dir(Path("outputs"))
    ensure_dir(Path("logs"))
    logger = configure_logger(args.log)
    logger.info("starting pipeline with input=%s", args.input)
    events = fetch_batch(args.input, args.config, args.events_output, args.log)
    logger.info("event rows collected: %d", len(events))
    fee_rows = infer_fee_status(args.events_output, args.fee_output)
    logger.info("fee rows written: %d", len(fee_rows))


if __name__ == "__main__":
    main()
