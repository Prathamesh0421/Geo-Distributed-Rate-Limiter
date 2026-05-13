"""
Prometheus metrics client.

Fetches three metric families each tick and assembles a FeatureSnapshot.

Top-user detection uses rl_counter_value (labels: region, tier, user_id)
rather than rl_requests_total, because rl_requests_total has no user_id
label — per gateway/internal/metrics/metrics.go.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

import httpx

log = logging.getLogger(__name__)

REGIONS: tuple[str, ...] = ("us", "eu", "asia")
TIERS: tuple[str, ...] = ("free", "premium", "internal")


@dataclass
class UserStat:
    user_id: str
    rps: float           # counter_value / 60  (approximate, window=60s)
    share_of_tier: float # fraction of this tier's observed counter total


@dataclass
class TierFeatures:
    rps: float = 0.0
    rejection_rate: float = 0.0
    top_users: list[UserStat] = field(default_factory=list)


@dataclass
class FeatureSnapshot:
    timestamp: int                                  # unix ms
    regions: dict[str, dict[str, TierFeatures]]    # [region][tier]


def _empty_regions() -> dict[str, dict[str, TierFeatures]]:
    return {r: {t: TierFeatures() for t in TIERS} for r in REGIONS}


class MetricsClient:
    """Thin async wrapper around the Prometheus HTTP query API."""

    _Q_RPS = 'sum by (region, tier) (rate(rl_requests_total[15s]))'
    _Q_DENY = 'sum by (region, tier) (rate(rl_requests_total{decision="denied"}[15s]))'
    _Q_USERS = 'sum by (region, tier, user_id) (rl_counter_value)'

    def __init__(self, prom_url: str, timeout: float = 5.0) -> None:
        self._base = prom_url.rstrip("/")
        self._timeout = timeout

    async def fetch_features(self) -> FeatureSnapshot:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            rps_raw, deny_raw, user_raw = await asyncio.gather(
                self._query(client, self._Q_RPS),
                self._query(client, self._Q_DENY),
                self._query(client, self._Q_USERS),
            )

        snap = FeatureSnapshot(
            timestamp=int(time.time() * 1000),
            regions=_empty_regions(),
        )
        self._apply_rps(snap, rps_raw)
        self._apply_rejection(snap, deny_raw)
        self._apply_top_users(snap, user_raw)
        return snap

    # ── private helpers ──────────────────────────────────────────────────────

    def _apply_rps(self, snap: FeatureSnapshot, raw: list[dict]) -> None:
        for series in raw:
            r = series["metric"].get("region")
            t = series["metric"].get("tier")
            if r in REGIONS and t in TIERS:
                snap.regions[r][t].rps = _float(series["value"][1])

    def _apply_rejection(self, snap: FeatureSnapshot, raw: list[dict]) -> None:
        for series in raw:
            r = series["metric"].get("region")
            t = series["metric"].get("tier")
            if r in REGIONS and t in TIERS:
                deny_rps = _float(series["value"][1])
                total_rps = snap.regions[r][t].rps
                snap.regions[r][t].rejection_rate = (
                    deny_rps / total_rps if total_rps > 0.0 else 0.0
                )

    def _apply_top_users(self, snap: FeatureSnapshot, raw: list[dict]) -> None:
        # Bucket raw counter values by (region, tier, user_id).
        counts: dict[tuple[str, str, str], float] = {}
        for series in raw:
            m = series["metric"]
            r, t, uid = m.get("region"), m.get("tier"), m.get("user_id")
            if r in REGIONS and t in TIERS and uid:
                counts[(r, t, uid)] = _float(series["value"][1])

        for region in REGIONS:
            for tier in TIERS:
                entries = [
                    (uid, cnt)
                    for (r, t, uid), cnt in counts.items()
                    if r == region and t == tier
                ]
                if not entries:
                    continue
                tier_total = sum(cnt for _, cnt in entries)
                top = sorted(entries, key=lambda x: -x[1])[:20]
                snap.regions[region][tier].top_users = [
                    UserStat(
                        user_id=uid,
                        rps=cnt / 60.0,
                        share_of_tier=cnt / tier_total if tier_total > 0.0 else 0.0,
                    )
                    for uid, cnt in top
                ]

    async def _query(self, client: httpx.AsyncClient, promql: str) -> list[dict]:
        resp = await client.get(
            f"{self._base}/api/v1/query",
            params={"query": promql},
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != "success":
            raise RuntimeError(f"Prometheus error: {body.get('error', body)}")
        return body["data"]["result"]


def _float(v: object) -> float:
    """Parse a Prometheus string value, returning 0.0 on NaN/error."""
    try:
        f = float(v)  # type: ignore[arg-type]
        return f if f == f else 0.0  # NaN check
    except (TypeError, ValueError):
        return 0.0
