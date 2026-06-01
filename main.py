"""
main.py
───────
Orchestrator: splits work across N processes, collects results.

Run
───
    python main.py --input product_ids.csv [--id-column product_id] [--resume]

Flow
────
1. Load all product IDs from CSV.
2. (Optional) Skip already-fetched IDs when --resume is set.
3. Partition IDs evenly across NUM_WORKERS processes.
4. Spawn each worker as an independent OS process.
5. Wait for all workers to finish; collect stats via Queue.
6. Merge per-worker error logs into one logs/errors.jsonl.
7. Print summary table.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import multiprocessing
import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import orjson

from config import (
    CHECKPOINT_FILE_TPL,
    LOGS_DIR,
    MERGED_ERROR_LOG,
    NUM_WORKERS,
    OUTPUT_DIR,
    PRODUCTS_PER_FILE,
)
from worker import run_worker

# ── Logging (main process only) ────────────────────────────────────────────────

def _setup_logging() -> None:
    Path(LOGS_DIR).mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  MAIN  %(levelname)-8s  %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"{LOGS_DIR}/main.log", encoding="utf-8"),
        ],
    )

logger = logging.getLogger(__name__)


# ── CSV loader ─────────────────────────────────────────────────────────────────

def _load_product_ids(csv_path: str, id_column: str) -> list[int]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    ids: list[int] = []
    with path.open(newline="", encoding="utf-8-sig") as fh:
        sample     = fh.read(4096)
        has_header = id_column in sample.split("\n")[0]
        fh.seek(0)
        reader = csv.DictReader(fh) if has_header else csv.reader(fh)
        for row in reader:
            try:
                raw = row[id_column] if has_header else row[0]   # type: ignore[index]
                ids.append(int(str(raw).strip()))
            except (KeyError, ValueError):
                continue

    logger.info("Loaded %d product IDs from %s", len(ids), csv_path)
    return ids


# ── Resume: collect IDs already present in output files ───────────────────────

def _already_fetched_ids() -> set[int]:
    seen: set[int] = set()
    for batch_file in Path(OUTPUT_DIR).glob("products_batch_*.json"):
        try:
            products = orjson.loads(batch_file.read_bytes())
            for p in products:
                if "id" in p:
                    seen.add(int(p["id"]))
        except Exception:
            pass
    if seen:
        logger.info("Resume: %d products already fetched — skipping", len(seen))
    return seen


# ── Partition helper ───────────────────────────────────────────────────────────

def _partition(lst: list[int], n: int) -> list[list[int]]:
    """Split lst into n roughly equal chunks."""
    k, rem = divmod(len(lst), n)
    parts, start = [], 0
    for i in range(n):
        size = k + (1 if i < rem else 0)
        parts.append(lst[start : start + size])
        start += size
    return parts


# ── Post-run: merge per-worker error logs ─────────────────────────────────────

def _merge_error_logs(num_workers: int) -> int:
    """Concatenate all per-worker error .jsonl files into one."""
    merged = Path(MERGED_ERROR_LOG)
    merged.parent.mkdir(parents=True, exist_ok=True)
    total_errors = 0
    with merged.open("w", encoding="utf-8") as out:
        for w in range(num_workers):
            worker_log = Path(f"logs/errors_worker_{w}.jsonl")
            if not worker_log.exists():
                continue
            for line in worker_log.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    out.write(line + "\n")
                    total_errors += 1
    logger.info("Merged error logs → %s  (%d errors total)", merged, total_errors)
    return total_errors


# ── Summary printer ────────────────────────────────────────────────────────────

def _print_summary(
    all_stats:    list[dict[str, Any]],
    total_ids:    int,
    elapsed_wall: float,
) -> None:
    total_success = sum(s["success"] for s in all_stats)
    total_errors  = sum(s["errors"]  for s in all_stats)
    total_batches = sum(s["batches_written"] for s in all_stats)
    wall_fmt      = str(timedelta(seconds=int(elapsed_wall)))
    rate          = total_success / elapsed_wall if elapsed_wall else 0

    lines = [
        "",
        "╔══════════════════════════════════════════════╗",
        "║           SCRAPER RUN SUMMARY                ║",
        "╠══════════════════════════════════════════════╣",
        f"║  Total IDs          : {total_ids:<22d}║",
        f"║  Successful         : {total_success:<22d}║",
        f"║  Errors             : {total_errors:<22d}║",
        f"║  Batches written    : {total_batches:<22d}║",
        f"║  Workers used       : {len(all_stats):<22d}║",
        f"║  Wall-clock time    : {wall_fmt:<22s}║",
        f"║  Effective rate     : {rate:<19.1f}/s  ║",
        "╠══════════════════════════════════════════════╣",
    ]

    for s in sorted(all_stats, key=lambda x: x["worker_id"]):
        w_rate = s["success"] / s["elapsed_s"] if s["elapsed_s"] else 0
        lines.append(
            f"║  Worker {s['worker_id']:02d}  ok={s['success']:<7d}"
            f" err={s['errors']:<6d} {w_rate:5.0f}/s  ║"
        )

    lines.append("╚══════════════════════════════════════════════╝")
    summary = "\n".join(lines)
    logger.info(summary)
    print(summary)


# ── Entry point ────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Tiki product scraper v2 (multiprocess)")
    parser.add_argument("--input",      required=True,        help="Path to product IDs CSV")
    parser.add_argument("--id-column",  default="product_id", help="CSV column name for IDs")
    parser.add_argument("--resume",     action="store_true",  help="Skip already-fetched IDs")
    parser.add_argument("--workers",    type=int, default=NUM_WORKERS,
                        help=f"Number of parallel workers (default: {NUM_WORKERS})")
    args = parser.parse_args(argv)

    _setup_logging()
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Load & optionally filter IDs
    all_ids = _load_product_ids(args.input, args.id_column)
    if args.resume:
        done = _already_fetched_ids()
        product_ids = [pid for pid in all_ids if pid not in done]
        logger.info("After dedup: %d IDs remain to fetch", len(product_ids))
    else:
        product_ids = all_ids

    if not product_ids:
        logger.info("Nothing to fetch — exiting.")
        return

    num_workers  = min(args.workers, len(product_ids))
    partitions   = _partition(product_ids, num_workers)
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    logger.info(
        "Starting  total=%d  workers=%d  ~%d IDs/worker",
        len(product_ids), num_workers, len(product_ids) // num_workers,
    )

    wall_start = time.monotonic()

    # Spawn worker processes
    processes = []
    for worker_id, partition in enumerate(partitions):
        if not partition:
            continue
        p = multiprocessing.Process(
            target=run_worker,
            args=(worker_id, partition, result_queue),
            daemon=False,
            name=f"tiki-worker-{worker_id:02d}",
        )
        p.start()
        processes.append(p)
        logger.info("Spawned worker %02d  (%d IDs)", worker_id, len(partition))
        # Stagger worker startup to avoid simultaneous burst that triggers CAPTCHA
        if worker_id < len(partitions) - 1:
            time.sleep(3)

    # Collect results
    all_stats: list[dict[str, Any]] = []
    for _ in processes:
        try:
            stats = result_queue.get(timeout=7200)   # 2-hour safety timeout
            all_stats.append(stats)
        except Exception as exc:
            logger.error("Worker did not report back: %s", exc)

    # Wait for all processes to exit cleanly
    for p in processes:
        p.join(timeout=30)
        if p.is_alive():
            logger.warning("Worker %s did not exit — terminating", p.name)
            p.terminate()

    wall_elapsed = time.monotonic() - wall_start

    # Merge per-worker error logs
    _merge_error_logs(num_workers)

    _print_summary(all_stats, len(product_ids), wall_elapsed)


if __name__ == "__main__":
    # Required on Windows / macOS (spawn start method)
    multiprocessing.freeze_support()
    main()
