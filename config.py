"""
config.py
─────────
Single source of truth for every tunable parameter.

Tuning guide
────────────
1. Start with NUM_WORKERS = cpu_count()
2. Each worker runs MAX_CONCURRENT_REQUESTS concurrently
   → total in-flight = NUM_WORKERS × MAX_CONCURRENT_REQUESTS
3. Keep REQUESTS_PER_SECOND_PER_WORKER × NUM_WORKERS ≤ ~150 to stay
   under Tiki's radar. Monitor logs/errors.jsonl for 429 spikes.
4. Lower REQUEST_TIMEOUT_SECONDS to 8 if the network is fast (fail-fast).
"""

import multiprocessing
import os
import random

# ── Multiprocessing ────────────────────────────────────────────────────────────
NUM_WORKERS: int = 4   # single worker — absolute minimum footprint

# ── Per-worker concurrency ─────────────────────────────────────────────────────
# Total in-flight  = 4 × 9 = 36
# Total req/s      = 4 × 1.0 = 4 req/s — near human-like browsing speed
MAX_CONCURRENT_REQUESTS: int = 9
REQUESTS_PER_SECOND_PER_WORKER: float = 1.0

# ── HTTP ───────────────────────────────────────────────────────────────────────
API_BASE_URL: str = "https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
REQUEST_TIMEOUT_SECONDS: int = 15

# ── Retry ──────────────────────────────────────────────────────────────────────
MAX_RETRIES: int = 5
RETRY_BACKOFF_BASE: float = 2.0      # seconds: 2 → 4 → 8 → 16
RETRY_ON_STATUS_CODES: set = {429, 500, 502, 503, 504}

# Backoff when CAPTCHA is detected (seconds)
CAPTCHA_BACKOFF_SECONDS: float = 10.0

# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR: str = "output"
LOGS_DIR: str = "logs"
PRODUCTS_PER_FILE: int = 1_000

# Per-worker checkpoint + error log (worker index appended at runtime)
CHECKPOINT_FILE_TPL: str = "logs/checkpoint_worker_{worker_id}.json"
ERROR_LOG_FILE_TPL: str = "logs/errors_worker_{worker_id}.jsonl"

# Master merged outputs (written by main process after all workers finish)
MERGED_ERROR_LOG: str = "logs/errors.jsonl"

# ── Browser session (copy từ DevTools khi browse tiki.vn bình thường) ──────────
BROWSER_COOKIE: str = (
    "s_v_web_id=verify_a160144b17f98b38388d6dbe3cae35fc; "
    "_trackity=2fc79dd9-7e34-307a-508c-d15993efa156; "
    "TOKENS={%22access_token%22:%22zx04dhUsAyI6FSHfVLQjgbnlcp2KDZv8%22%2C%22expires_in%22:157680000%2C%22expires_at%22:1938011320722%2C%22guest_token%22:%22zx04dhUsAyI6FSHfVLQjgbnlcp2KDZv8%22}; "
    "_ga=GA1.1.1917807793.1780331321; "
    "_gcl_au=1.1.527399734.1780331324; "
    "tiki_client_id=1917807793.1780331321; "
    "amp_99d374=A7fS2ZZrIocUr5F9oXavM9...1jq22haqo.1jq22j548.1b.1j.2u; "
    "_ga_S9GLR1RQFJ=GS2.1.s1780333441$o2$g1$t1780333778$j60$l0$h0"
)

GUEST_TOKEN: str = "zx04dhUsAyI6FSHfVLQjgbnlcp2KDZv8"

# ── HTTP headers ───────────────────────────────────────────────────────────────


def random_headers() -> dict:
    return {
        "User-Agent":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "Accept":             "application/json, text/plain, */*",
        "Accept-Language":    "vi-DE,vi-VN;q=0.9,vi;q=0.8,fr-FR;q=0.7,fr;q=0.6,en-US;q=0.5,en;q=0.4",
        "Accept-Encoding":    "gzip, deflate, br, zstd",
        "Origin":             "https://tiki.vn",
        "Referer":            "https://tiki.vn/",
        "Cookie":             BROWSER_COOKIE,
        "x-guest-token":      GUEST_TOKEN,
        "sec-ch-ua":          '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-site",
        "priority":           "u=1, i",
    }


REQUEST_HEADERS: dict = random_headers()
