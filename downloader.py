"""
downloader.py
─────────────
Per-worker async HTTP client.

Optimisations over v1
─────────────────────
1. HTML normalisation offloaded to a ProcessPoolExecutor so it never
   blocks the event loop (CPU-bound work runs on a separate OS process).
2. Adaptive rate limiter: backs off automatically on 429, recovers
   gradually when traffic flows normally.
3. orjson for fast JSON decoding (5-10× faster than stdlib json).
4. Typed result model unchanged — compatible with writer.py.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any

import aiohttp
import orjson

from config import (
    API_BASE_URL,
    CAPTCHA_BACKOFF_SECONDS,
    MAX_CONCURRENT_REQUESTS,
    MAX_RETRIES,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT_SECONDS,
    REQUESTS_PER_SECOND_PER_WORKER,
    RETRY_BACKOFF_BASE,
    RETRY_ON_STATUS_CODES,
    random_headers,
)
from normalizer import normalize_description

logger = logging.getLogger(__name__)


# ── Error taxonomy ─────────────────────────────────────────────────────────────

class ErrorKind(str, Enum):
    HTTP_CLIENT  = "http_client_error"
    HTTP_SERVER  = "http_server_error"
    RATE_LIMITED = "rate_limited"
    CAPTCHA      = "captcha_blocked"
    TIMEOUT      = "timeout"
    NETWORK      = "network_error"
    PARSE        = "parse_error"
    UNKNOWN      = "unknown"


@dataclass
class ProductError:
    product_id: int
    kind: ErrorKind
    message: str
    http_status: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "product_id":  self.product_id,
            "error_kind":  self.kind.value,
            "message":     self.message,
            "http_status": self.http_status,
        }


@dataclass
class ProductResult:
    product_id: int
    data:  dict[str, Any] | None = None
    error: ProductError   | None = None

    @property
    def ok(self) -> bool:
        return self.data is not None


# ── Adaptive token-bucket rate limiter ────────────────────────────────────────

class AdaptiveRateLimiter:
    """
    Token-bucket limiter that backs off on 429 and recovers gradually.

    On 429  → multiply rate by BACKOFF_FACTOR  (reduce speed)
    On success → slowly creep rate back up toward the configured ceiling
    """

    BACKOFF_FACTOR  = 0.5    # halve rate on 429
    RECOVER_FACTOR  = 1.05   # +5 % per successful request
    MIN_RATE        = 2.0    # never go below 2 req/s

    def __init__(self, rate: float) -> None:
        self._max_rate  = rate
        self._rate      = rate
        self._tokens    = rate
        self._last_time = time.monotonic()
        self._lock      = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now     = time.monotonic()
            elapsed = now - self._last_time
            self._tokens    = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_time = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
            wait = (1 - self._tokens) / self._rate
            await asyncio.sleep(wait)
            self._tokens = 0

    def on_success(self) -> None:
        self._rate = min(self._max_rate, self._rate * self.RECOVER_FACTOR)

    def on_rate_limited(self) -> None:
        self._rate = max(self.MIN_RATE, self._rate * self.BACKOFF_FACTOR)
        logger.warning("Rate limited — throttling to %.1f req/s", self._rate)

    @property
    def current_rate(self) -> float:
        return self._rate


# ── Product parser (runs in process pool) ─────────────────────────────────────

def _parse_product_worker(raw_bytes: bytes) -> dict[str, Any]:
    """
    Top-level function — must be picklable for ProcessPoolExecutor.
    Deserialises JSON bytes and runs HTML normalisation.
    Called via loop.run_in_executor(pool, ...).
    """
    raw: dict[str, Any] = orjson.loads(raw_bytes)

    images: list[str] = []
    for img in raw.get("images") or []:
        url = img.get("large_url") or img.get("base_url") or ""
        if url:
            images.append(url)

    norm = normalize_description(raw.get("description"))

    return {
        "id":                 raw["id"],
        "name":               raw.get("name", ""),
        "url_key":            raw.get("url_key", ""),
        "price":              raw.get("price"),
        "description":        norm.text,
        "images":             images,
        "description_images": norm.images_in_desc,
    }


# ── Core fetch coroutine ───────────────────────────────────────────────────────

async def fetch_product(
    product_id:   int,
    session:      aiohttp.ClientSession,
    semaphore:    asyncio.Semaphore,
    rate_limiter: AdaptiveRateLimiter | None,
    process_pool: ProcessPoolExecutor,
) -> ProductResult:
    """
    Fetch + parse one product.  Never raises — all failures → ProductResult.error.
    """
    url  = API_BASE_URL.format(product_id=product_id)
    loop = asyncio.get_running_loop()

    for attempt in range(MAX_RETRIES + 1):
        if rate_limiter:
            await rate_limiter.acquire()

        # Small random jitter to avoid synchronized bursts across coroutines
        await asyncio.sleep(random.uniform(0.05, 0.3))

        try:
            async with semaphore:
                async with session.get(
                    url,
                    headers=random_headers(),
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
                ) as resp:

                    # Non-retryable client error
                    if resp.status == 404:
                        return ProductResult(
                            product_id=product_id,
                            error=ProductError(product_id, ErrorKind.HTTP_CLIENT,
                                               "Product not found (404)", 404),
                        )

                    if resp.status not in RETRY_ON_STATUS_CODES and resp.status >= 400:
                        return ProductResult(
                            product_id=product_id,
                            error=ProductError(product_id, ErrorKind.HTTP_CLIENT,
                                               f"HTTP {resp.status}", resp.status),
                        )

                    # Retryable errors
                    if resp.status in RETRY_ON_STATUS_CODES:
                        kind = (ErrorKind.RATE_LIMITED if resp.status == 429
                                else ErrorKind.HTTP_SERVER)
                        if rate_limiter and resp.status == 429:
                            rate_limiter.on_rate_limited()
                        if attempt < MAX_RETRIES:
                            wait = RETRY_BACKOFF_BASE ** attempt + random.uniform(0, 1)
                            logger.warning("pid=%s HTTP %s attempt=%s retry in %.2fs",
                                           product_id, resp.status, attempt + 1, wait)
                            await asyncio.sleep(wait)
                            continue
                        return ProductResult(
                            product_id=product_id,
                            error=ProductError(product_id, kind,
                                               f"HTTP {resp.status} after {MAX_RETRIES} retries",
                                               resp.status),
                        )

                    # ── Success: read raw bytes, offload CPU work ──────────
                    raw_bytes = await resp.read()

                    # Detect CAPTCHA: Tiki returns HTML page (status 200) when blocking
                    content_type = resp.headers.get("Content-Type", "")
                    if b"TTGCaptcha" in raw_bytes or b"captcha" in raw_bytes[:512].lower():
                        if rate_limiter:
                            rate_limiter.on_rate_limited()
                        if attempt < MAX_RETRIES:
                            wait = CAPTCHA_BACKOFF_SECONDS + random.uniform(0, 5)
                            logger.warning("pid=%s CAPTCHA detected attempt=%s backing off %.1fs",
                                           product_id, attempt + 1, wait)
                            await asyncio.sleep(wait)
                            continue
                        return ProductResult(
                            product_id=product_id,
                            error=ProductError(product_id, ErrorKind.CAPTCHA,
                                               "Blocked by CAPTCHA after retries", resp.status),
                        )

                    if "text/html" in content_type and b"{" not in raw_bytes[:64]:
                        return ProductResult(
                            product_id=product_id,
                            error=ProductError(product_id, ErrorKind.CAPTCHA,
                                               f"Unexpected HTML response (content-type: {content_type})",
                                               resp.status),
                        )

                    try:
                        # Run JSON decode + HTML normalise in process pool
                        # so the event loop is never blocked
                        data = await loop.run_in_executor(
                            process_pool,
                            _parse_product_worker,
                            raw_bytes,
                        )
                        if rate_limiter:
                            rate_limiter.on_success()
                        return ProductResult(product_id=product_id, data=data)

                    except Exception as exc:
                        return ProductResult(
                            product_id=product_id,
                            error=ProductError(product_id, ErrorKind.PARSE,
                                               f"Parse/normalise error: {exc}", resp.status),
                        )

        except asyncio.TimeoutError:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_BASE ** attempt)
            else:
                return ProductResult(
                    product_id=product_id,
                    error=ProductError(product_id, ErrorKind.TIMEOUT,
                                       f"Timeout after {MAX_RETRIES} retries"),
                )

        except aiohttp.ClientError as exc:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_BASE ** attempt)
            else:
                return ProductResult(
                    product_id=product_id,
                    error=ProductError(product_id, ErrorKind.NETWORK, str(exc)),
                )

        except Exception as exc:  # noqa: BLE001
            return ProductResult(
                product_id=product_id,
                error=ProductError(product_id, ErrorKind.UNKNOWN, str(exc)),
            )

    return ProductResult(
        product_id=product_id,
        error=ProductError(product_id, ErrorKind.UNKNOWN,
                           "Exhausted retry loop unexpectedly"),
    )


# ── Factories ──────────────────────────────────────────────────────────────────

def build_session() -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS + 10,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    # No default headers — each request sets random headers via random_headers()
    return aiohttp.ClientSession(connector=connector)


def build_rate_limiter() -> AdaptiveRateLimiter | None:
    if not REQUESTS_PER_SECOND_PER_WORKER:
        return None
    return AdaptiveRateLimiter(rate=REQUESTS_PER_SECOND_PER_WORKER)
