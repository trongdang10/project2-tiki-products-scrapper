"""
writer.py
─────────
Async batch writer: aiofiles + orjson.

Changes from v1
───────────────
• All file I/O is now async (aiofiles) — never blocks the event loop.
• JSON serialisation uses orjson (5-10× faster than stdlib json).
• Atomic write pattern preserved: write .tmp → fdatasync → rename.
• Per-worker output files: products_batch_{worker_id}_{batch_index:04d}.json
  Multiple workers write to separate files — zero coordination needed.
• Error log is also async-appended line by line (.jsonl).
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import aiofiles
import aiofiles.os
import orjson

from config import (
    CHECKPOINT_FILE_TPL,
    ERROR_LOG_FILE_TPL,
    OUTPUT_DIR,
    PRODUCTS_PER_FILE,
)

logger = logging.getLogger(__name__)


class AsyncBatchWriter:
    """
    Buffers product dicts and flushes to numbered JSON files asynchronously.

    Each worker gets its own instance with a unique worker_id, so files
    never collide across processes.
    """

    def __init__(self, worker_id: int) -> None:
        self._worker_id     = worker_id
        self._buffer: list[dict[str, Any]] = []
        self._batch_index   = 0          # filled in by async _init()
        self._checkpoint    = CHECKPOINT_FILE_TPL.format(worker_id=worker_id)
        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        Path(self._checkpoint).parent.mkdir(parents=True, exist_ok=True)

    async def init(self) -> None:
        """Must be awaited once before first use (loads checkpoint)."""
        self._batch_index = await self._load_checkpoint() + 1

    # ── Public API ─────────────────────────────────────────────────────────────

    async def add(self, product: dict[str, Any]) -> None:
        """Buffer one product; auto-flush when the batch is full."""
        self._buffer.append(product)
        if len(self._buffer) >= PRODUCTS_PER_FILE:
            await self._flush()

    async def flush(self) -> None:
        """Flush remaining buffer at end of run."""
        if self._buffer:
            await self._flush()

    @property
    def batches_written(self) -> int:
        return self._batch_index - 1

    # ── Internals ──────────────────────────────────────────────────────────────

    async def _flush(self) -> None:
        filename = f"products_batch_w{self._worker_id:02d}_{self._batch_index:04d}.json"
        path     = Path(OUTPUT_DIR) / filename
        await _async_atomic_write(path, self._buffer)
        logger.info("[worker %02d] batch %04d  (%d products) → %s",
                    self._worker_id, self._batch_index, len(self._buffer), path)
        await self._save_checkpoint(self._batch_index)
        self._batch_index += 1
        self._buffer.clear()

    async def _load_checkpoint(self) -> int:
        cp = Path(self._checkpoint)
        if not cp.exists():
            return 0
        try:
            async with aiofiles.open(cp, "rb") as fh:
                data = orjson.loads(await fh.read())
            return int(data.get("last_batch", 0))
        except Exception as exc:
            logger.warning("[worker %02d] checkpoint unreadable (%s) — starting fresh",
                           self._worker_id, exc)
            return 0

    async def _save_checkpoint(self, batch_index: int) -> None:
        cp = Path(self._checkpoint)
        await _async_atomic_write(cp, {"last_batch": batch_index})


class AsyncErrorWriter:
    """
    Async append-only .jsonl error log.
    One JSON object per line; survives crashes mid-run.
    """

    def __init__(self, worker_id: int) -> None:
        self._path  = Path(ERROR_LOG_FILE_TPL.format(worker_id=worker_id))
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._count = 0
        self._lock  = asyncio.Lock()

    async def write(self, error_dict: dict[str, Any]) -> None:
        line = orjson.dumps(error_dict).decode() + "\n"
        async with self._lock:
            async with aiofiles.open(self._path, "a", encoding="utf-8") as fh:
                await fh.write(line)
        self._count += 1

    @property
    def error_count(self) -> int:
        return self._count


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _async_atomic_write(path: Path, data: Any) -> None:
    """
    Async atomic write: serialize with orjson → .tmp → fdatasync → rename.
    """
    tmp = path.with_suffix(".tmp")
    try:
        serialized = orjson.dumps(data, option=orjson.OPT_INDENT_2)
        async with aiofiles.open(tmp, "wb") as fh:
            await fh.write(serialized)
            await fh.flush()
            os.fsync(fh.fileno())
        await aiofiles.os.rename(tmp, path)
    except Exception:
        try:
            await aiofiles.os.remove(tmp)
        except OSError:
            pass
        raise
