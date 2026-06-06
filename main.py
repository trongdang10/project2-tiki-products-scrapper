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
import queue as _queue
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
    RUN_DEADLINE_SECONDS,
    WORKER_STATS_FILE_TPL,
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


# ── Stats collection (robust against interrupted/terminated workers) ──────────

def _drain_queue(q: multiprocessing.Queue, into: dict[int, dict[str, Any]]) -> None:
    """Non-blocking: pull every stats dict currently in the queue."""
    try:
        while True:
            s = q.get_nowait()
            into[s["worker_id"]] = s
    except _queue.Empty:
        pass


def _read_stats_file(worker_id: int) -> dict[str, Any] | None:
    """Read a worker's durable stats file, if it finished and wrote one."""
    path = Path(WORKER_STATS_FILE_TPL.format(worker_id=worker_id))
    if not path.exists():
        return None
    try:
        return orjson.loads(path.read_bytes())
    except Exception:
        return None


def _disk_fallback_stats(worker_id: int) -> dict[str, Any] | None:
    """
    Last resort for a worker that was killed before reporting: reconstruct
    its numbers straight from disk. Success is exact (counts products actually
    written); errors are best-effort (the append-only error log may include
    retries / earlier runs).
    """
    success, batches = 0, 0
    for f in Path(OUTPUT_DIR).glob(f"products_batch_w{worker_id:02d}_*.json"):
        try:
            success += len(orjson.loads(f.read_bytes()))
            batches += 1
        except Exception:
            pass

    errors = 0
    err_log = Path(f"logs/errors_worker_{worker_id}.jsonl")
    if err_log.exists():
        errors = sum(1 for line in err_log.read_text(encoding="utf-8").splitlines()
                     if line.strip())

    if success == 0 and errors == 0:
        return None
    return {
        "worker_id":       worker_id,
        "total":           success + errors,
        "success":         success,
        "errors":          errors,
        "batches_written": batches,
        "elapsed_s":       0,
        "_source":         "disk-fallback (worker interrupted)",
    }


def _collect_final_stats(
    num_workers: int,
    queue_stats: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Authoritative stats per worker, preferring the most reliable source:
      1. durable stats file  (worker finished cleanly)
      2. queue report        (worker reported, file missing)
      3. disk fallback       (worker killed — reconstruct from output)
    """
    stats: list[dict[str, Any]] = []
    for w in range(num_workers):
        s = _read_stats_file(w) or queue_stats.get(w) or _disk_fallback_stats(w)
        if s is not None:
            stats.append(s)
    return stats


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

    # ── Wait for workers ──────────────────────────────────────────────────────
    # Poll liveness and drain the queue opportunistically. We let workers run to
    # completion (a healthy worker is making progress); we only force-terminate
    # if the WHOLE run blows past RUN_DEADLINE_SECONDS — never just because a
    # fixed per-get timeout elapsed while workers were still working fine.
    queue_stats: dict[int, dict[str, Any]] = {}
    deadline = wall_start + RUN_DEADLINE_SECONDS
    while any(p.is_alive() for p in processes):
        _drain_queue(result_queue, queue_stats)
        if time.monotonic() > deadline:
            logger.warning("Run deadline (%ds) exceeded — terminating workers",
                           RUN_DEADLINE_SECONDS)
            for p in processes:
                if p.is_alive():
                    p.terminate()
            break
        time.sleep(5)

    # Final drain + clean join
    _drain_queue(result_queue, queue_stats)
    for p in processes:
        p.join(timeout=30)
        if p.is_alive():
            logger.warning("Worker %s did not exit — terminating", p.name)
            p.terminate()

    # Stop the queue's feeder thread from blocking interpreter shutdown
    # (the classic multiprocessing "hangs on exit" trap after terminate()).
    result_queue.cancel_join_thread()

    wall_elapsed = time.monotonic() - wall_start

    # Merge per-worker error logs
    _merge_error_logs(num_workers)

    # Build the summary from the most reliable source per worker:
    # durable stats file → queue report → on-disk reconstruction.
    all_stats = _collect_final_stats(num_workers, queue_stats)
    if any(s.get("_source") for s in all_stats):
        logger.warning("Some workers were interrupted — their numbers are "
                       "reconstructed from disk (success exact, errors approximate).")

    _print_summary(all_stats, len(product_ids), wall_elapsed)


if __name__ == "__main__":
    # Required on Windows / macOS (spawn start method)
    multiprocessing.freeze_support()
    main()
