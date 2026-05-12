"""
Agent configuration — env vars with sensible host-run defaults.

All Redis addresses follow the same "host:port" convention as sync/.
"""

import os
from dataclasses import dataclass

REGIONS: tuple[str, ...] = ("us", "eu", "asia")
TIERS: tuple[str, ...] = ("free", "premium", "internal")

# Demo baselines written by `python tools/seed_policies.py --demo`.
# Much higher than gateway static fallbacks (10/100/1000) so spike
# scenarios produce visible Rule-1 → Rule-2 cycles.
DEMO_BASELINE: dict[str, int] = {
    "free":     300,
    "premium":  3_000,
    "internal": 30_000,
}

# Floor prevents Rule 1 from collapsing a tier to zero.
STATIC_FLOOR: dict[str, int] = {
    "free":     5,
    "premium":  50,
    "internal": 500,
}

# Gateway crash-safety fallback (matches policy.go).
STATIC_FALLBACK: dict[str, int] = {
    "free":     10,
    "premium":  100,
    "internal": 1_000,
}


@dataclass(frozen=True)
class Config:
    prom_url: str
    redis_us: str
    redis_eu: str
    redis_asia: str
    metrics_port: int
    log_path: str
    predictor: str
    interval_seconds: int


def load_config() -> Config:
    return Config(
        prom_url=os.getenv("PROM_URL", "http://localhost:9090"),
        redis_us=os.getenv("REDIS_US_ADDR", "localhost:6379"),
        redis_eu=os.getenv("REDIS_EU_ADDR", "localhost:6380"),
        redis_asia=os.getenv("REDIS_ASIA_ADDR", "localhost:6381"),
        metrics_port=int(os.getenv("METRICS_PORT", "9114")),
        log_path=os.getenv("LOG_PATH", "agent/decisions.jsonl"),
        predictor=os.getenv("PREDICTOR", "ewma"),
        interval_seconds=int(os.getenv("INTERVAL_SECONDS", "1")),
    )
