from __future__ import annotations

import argparse
import math
import subprocess
from pathlib import Path
from typing import List

from cnipa_utils import configure_logger, ensure_dir, load_csv_rows, write_csv_rows


DEFAULT_INPUT = Path("outputs/cpquery_low_quality_candidates.csv")
DEFAULT_OUTPUT = Path("outputs/patent_cpquery_status_low_quality.csv")
DEFAULT_LOG = Path("logs/run_cpquery_parallel.log")
DEFAULT_SESSION_PREFIX = "cpquery_shard"
DEFAULT_MODE = "auto"


def _split_rows(rows: List[dict], shards: int) -> List[List[dict]]:
    size = max(1, math.ceil(len(rows) / shards))
    return [rows[i : i + size] for i in range(0, len(rows), size)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run cpquery batch queries in parallel shards")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--session-prefix", default=DEFAULT_SESSION_PREFIX)
    parser.add_argument("--per-shard-output-dir", type=Path, default=Path("outputs/cpquery_shards"))
    parser.add_argument("--mode", choices=["auto", "api", "dom"], default=DEFAULT_MODE)
    parser.add_argument("--headed", action="store_true", help="Open visible browsers for shard sessions. Default is headless.")
    args = parser.parse_args()

    logger = configure_logger(args.log)
    rows = load_csv_rows(args.input)
    ensure_dir(args.per_shard_output_dir)
    shards = _split_rows(rows, args.shards)
    logger.info("input rows=%d shards=%d", len(rows), len(shards))

    shard_inputs = []
    for idx, shard_rows in enumerate(shards):
        shard_path = args.per_shard_output_dir / f"shard_{idx:02d}.csv"
        write_csv_rows(shard_path, shard_rows, ["input_id", "input_id_type", "year", "city_name", "city_adcode", "low_quality_reason"])
        shard_inputs.append(shard_path)
        logger.info("wrote shard %s rows=%d", shard_path, len(shard_rows))

    procs = []
    for idx, shard_path in enumerate(shard_inputs):
        shard_output = args.per_shard_output_dir / f"shard_{idx:02d}_out.csv"
        shard_log = args.per_shard_output_dir / f"shard_{idx:02d}.log"
        session_name = f"{args.session_prefix}_{idx:02d}"
        cmd = [
            ".venv/bin/python",
            "scripts/fetch_cnipa_cpquery_status_cli.py",
            "--input",
            str(shard_path),
            "--output",
            str(shard_output),
            "--log",
            str(shard_log),
            "--session",
            session_name,
            "--mode",
            args.mode,
        ]
        if args.headed:
            cmd.append("--headed")
        logger.info("starting shard %d session=%s", idx, session_name)
        procs.append((idx, subprocess.Popen(cmd)))

    rc = 0
    for idx, proc in procs:
        ret = proc.wait()
        logger.info("finished shard %d rc=%d", idx, ret)
        if ret != 0:
            rc = ret

    merged: List[dict] = []
    for idx in range(len(shard_inputs)):
        shard_output = args.per_shard_output_dir / f"shard_{idx:02d}_out.csv"
        if shard_output.exists():
            merged.extend(load_csv_rows(shard_output))
    if merged:
        fieldnames = list(merged[0].keys())
        write_csv_rows(args.output, merged, fieldnames)
        logger.info("wrote merged output %s rows=%d", args.output, len(merged))
    else:
        logger.warning("no merged rows produced")

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
