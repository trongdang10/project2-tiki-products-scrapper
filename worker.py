"""
worker.py
─────────
One OS process = one async event loop = one aiohttp session.

Spawned by main.py via multiprocessing.Process.
Each worker:
  1. Gets its partition of product IDs.
  2. Runs its own asyncio event loop.
  3. Writes to its own batch files and error log.
  4. Reports stats back to main via a multiprocessing.Queue.

No shared state between workers — zero locks, zero coordination.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from typing import Any

import orjson

from config import MAX_CONCURRENT_REQUESTS, WORKER_STATS_FILE_TPL
from downloader import build_rate_limiter, build_session, fetch_product
from writer import AsyncBatchWriter, AsyncErrorWriter


def _write_stats_file(worker_id: int, stats: dict[str, Any]) -> None:
    """
    Durably persist this worker's final stats (atomic write).
    Main reads this file for the summary, so the report survives even if the
    result queue can't be drained (e.g. main timed out and terminated us late).
    """
    import os

    path = WORKER_STATS_FILE_TPL.format(worker_id=worker_id)
    tmp  = path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(orjson.dumps(stats, option=orjson.OPT_INDENT_2))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _setup_logging(worker_id: int) -> None:
    fmt = f"%(asctime)s  W{worker_id:02d}  %(levelname)-8s  %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"logs/worker_{worker_id:02d}.log", encoding="utf-8"),
        ],
    )


async def _run_worker(
    worker_id:   int,
    product_ids: list[int],
    result_queue: multiprocessing.Queue,
) -> None:
    _setup_logging(worker_id)
    logger = logging.getLogger(__name__)

    total     = len(product_ids)
    success   = 0
    errors    = 0
    start     = time.monotonic()

    batch_writer = AsyncBatchWriter(worker_id)
    error_writer = AsyncErrorWriter(worker_id)
    await batch_writer.init()

    semaphore    = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    rate_limiter = build_rate_limiter()

    # ProcessPoolExecutor for CPU-bound work (JSON decode + HTML normalise).
    # max_workers=1 per async worker is enough: the bottleneck is I/O, not CPU.
    # Increasing this beyond 1 per worker adds process overhead with little gain.
    with ProcessPoolExecutor(max_workers=1) as pool:
        async with build_session() as session:

            async def _handle(pid: int) -> None:
                nonlocal success, errors
                result = await fetch_product(pid, session, semaphore, rate_limiter, pool)
                if result.ok:
                    await batch_writer.add(result.data)   # type: ignore[arg-type]
                    success += 1
                else:
                    await error_writer.write(result.error.to_dict())  # type: ignore[union-attr]
                    errors += 1

            tasks = [asyncio.create_task(_handle(pid)) for pid in product_ids]

            done_count = 0
            log_every  = max(500, total // 20)   # log ~20 progress lines per worker

            for coro in asyncio.as_completed(tasks):
                await coro
                done_count += 1
                if done_count % log_every == 0 or done_count == total:
                    elapsed = time.monotonic() - start
                    rate    = done_count / elapsed if elapsed else 0
                    eta_s   = (total - done_count) / rate if rate > 0 else 0
                    logger.info(
                        "%d/%d (%.1f%%)  rate=%.0f/s  ETA=%s",
                        done_count, total,
                        100 * done_count / total,
                        rate,
                        str(timedelta(seconds=int(eta_s))),
                    )

    await batch_writer.flush()

    elapsed = time.monotonic() - start
    stats: dict[str, Any] = {
        "worker_id":      worker_id,
        "total":          total,
        "success":        success,
        "errors":         errors,
        "batches_written": batch_writer.batches_written,
        "elapsed_s":      round(elapsed, 2),
    }
    # Durable record first, then best-effort queue notification.
    _write_stats_file(worker_id, stats)
    result_queue.put(stats)
    logger.info("Done — success=%d errors=%d elapsed=%s",
                success, errors, str(timedelta(seconds=int(elapsed))))


def run_worker(
    worker_id:    int,
    product_ids:  list[int],
    result_queue: multiprocessing.Queue,
) -> None:
    """Entry point called by multiprocessing.Process."""
    asyncio.run(_run_worker(worker_id, product_ids, result_queue))
