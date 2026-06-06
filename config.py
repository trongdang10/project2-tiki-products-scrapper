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
NUM_WORKERS: int = 3   # single worker — absolute minimum footprint

# ── Per-worker concurrency ─────────────────────────────────────────────────────
# Total in-flight  = 3 × 3 = 9  reqs — still human-like, but much faster than sequential
# Total req/s      = 3 × 1.0 = 3.0  req/s — near human-like browsing speed
MAX_CONCURRENT_REQUESTS: int = 3
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

# Hard safety cap: main terminates workers only if the WHOLE run exceeds this.
# Set generously — premature termination kills healthy workers and loses their
# final report. 12h easily covers 200k IDs even at international rate limits.
RUN_DEADLINE_SECONDS: int = 12 * 3600

# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR: str = "output"
LOGS_DIR: str = "logs"
PRODUCTS_PER_FILE: int = 1_000

# Per-worker checkpoint + error log (worker index appended at runtime)
CHECKPOINT_FILE_TPL: str = "logs/checkpoint_worker_{worker_id}.json"
ERROR_LOG_FILE_TPL: str = "logs/errors_worker_{worker_id}.jsonl"

# Per-worker durable final-stats file. Written atomically when a worker finishes,
# so the summary survives even if main can't drain the result queue.
WORKER_STATS_FILE_TPL: str = "logs/stats_worker_{worker_id}.json"

# Master merged outputs (written by main process after all workers finish)
MERGED_ERROR_LOG: str = "logs/errors.jsonl"

# ── Session credentials ────────────────────────────────────────────────────────
# Refresh these from your browser when you start hitting CAPTCHAs.
# Open DevTools → Network → pick any tiki.vn XHR → copy Request Headers.
# Do NOT include s_v_web_id=verify_... (CAPTCHA fingerprint cookie).

# GUEST_TOKEN: str = "YXLS29QKiEupyAfj3ebswW1zvGPUq4CM"

# BROWSER_COOKIE: str = (
#     "_trackity=fcb40940-f4f2-8326-2039-b1174a3d87f6; "
#     "TIKI_GUEST_TOKEN=YXLS29QKiEupyAfj3ebswW1zvGPUq4CM; "
#     "TOKENS={%22access_token%22:%22YXLS29QKiEupyAfj3ebswW1zvGPUq4CM%22"
#     "%2C%22expires_in%22:157680000%2C%22expires_at%22:1938270548260"
#     "%2C%22guest_token%22:%22YXLS29QKiEupyAfj3ebswW1zvGPUq4CM%22}; "
#     "tiki_client_id=; "
#     "_ga_S9GLR1RQFJ=GS2.1.s1780590548$o1$g0$t1780590548$j60$l0$h0; "
#     "_ga=GA1.1.304378689.1780590549; "
#     "amp_99d374=GYMZlTSO5NCdduM2clbAJG...1jq9nf537.1jq9nf7rj.2.5.7; "
#     "_gcl_au=1.1.1079606638.1780590552; "
#     "_fbp=fb.1.1780590552742.896929097227643094"
# )

# ── Browser profiles ───────────────────────────────────────────────────────────
# Each profile is a consistent fingerprint for one "machine".
# User-Agent / sec-ch-ua / sec-ch-ua-platform MUST match each other —
# mismatches are a classic bot signal.
_BROWSER_PROFILES: list[dict] = [
    # Chrome 148 · macOS  (your own machine — known clean)
    {
        "User-Agent":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"macOS"',
    },
    # Chrome 148 · Windows 10
    {
        "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    # Chrome 147 · Windows 10  (slightly older build — natural version spread)
    {
        "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Chromium";v="147", "Google Chrome";v="147", "Not/A)Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    # Edge 148 · Windows 10
    {
        "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0",
        "sec-ch-ua":          '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    # Chrome 148 · Linux (common for student/dev laptops)
    {
        "User-Agent":         "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Linux"',
    },
    # Chrome 146 · macOS  (older build)
    {
        "User-Agent":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Chromium";v="146", "Google Chrome";v="146", "Not/A)Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"macOS"',
    },
]

# Rotate Accept-Language to reflect a diverse user base
_ACCEPT_LANGUAGES: list[str] = [
    "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "vi-DE,vi-VN;q=0.9,vi;q=0.8,fr-FR;q=0.7,fr;q=0.6,en-US;q=0.5,en;q=0.4",
    "vi-VN,vi;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,vi;q=0.8",
    "vi-VN,vi;q=0.9,zh-CN;q=0.8,zh;q=0.7,en;q=0.6",
]


# ── HTTP headers ───────────────────────────────────────────────────────────────


def random_headers() -> dict:
    """Pick a random browser profile + language for every request."""
    profile = random.choice(_BROWSER_PROFILES)
    lang = random.choice(_ACCEPT_LANGUAGES)
    return {
        "User-Agent":         profile["User-Agent"],
        "Accept":             "application/json, text/plain, */*",
        "Accept-Language":    lang,
        "Accept-Encoding":    "gzip, deflate, br, zstd",
        "Origin":             "https://tiki.vn",
        "Referer":            "https://tiki.vn/",
        # "Cookie":             BROWSER_COOKIE,
        # "x-guest-token":      GUEST_TOKEN,
        "sec-ch-ua":          profile["sec-ch-ua"],
        "sec-ch-ua-mobile":   profile["sec-ch-ua-mobile"],
        "sec-ch-ua-platform": profile["sec-ch-ua-platform"],
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-site",
        "priority":           "u=1, i",
    }


REQUEST_HEADERS: dict = random_headers()
