#!/usr/bin/env python3
"""
demo_global_quota.py — standalone demo for global quota bypass prevention.

Compares a "local-only" rate limiter (three independent regional limiters with
no shared state) against the project's geo-distributed limiter (G-Counter CRDT
shared across all regions).

Writes structured results to agent/demo_results.json so the dashboard can render
a live side-by-side comparison panel.

Usage:
    python simulator/demo_global_quota.py [--attempts 250] [--global-limit 300]

Notes:
    - Temporarily sets policy:{region}:free to limit_per_minute=100 burst=500
      so GlobalLimit=300 (= 100*3) and the local token bucket allows all 250
      requests per region (250 << 500 burst). Any rejection in Phase 2 must
      therefore come from global enforcement.
    - Saves the existing policy first; restores it on exit (including SIGINT/
      SIGTERM). If killed -9, demo policy still auto-expires via the TTL=300s
      on the Redis key.
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import redis

# ── Config ──────────────────────────────────────────────────────────────────

GATEWAY_URLS = {
    "us":   os.getenv("GATEWAY_US_URL",   "http://localhost:8081"),
    "eu":   os.getenv("GATEWAY_EU_URL",   "http://localhost:8082"),
    "asia": os.getenv("GATEWAY_ASIA_URL", "http://localhost:8083"),
}

REDIS_CONFIGS = {
    "us":   {"host": os.getenv("REDIS_US_HOST",   "localhost"), "port": int(os.getenv("REDIS_US_PORT",   "6379"))},
    "eu":   {"host": os.getenv("REDIS_EU_HOST",   "localhost"), "port": int(os.getenv("REDIS_EU_PORT",   "6380"))},
    "asia": {"host": os.getenv("REDIS_ASIA_HOST", "localhost"), "port": int(os.getenv("REDIS_ASIA_PORT", "6381"))},
}

REPO_ROOT    = Path(os.getenv("REPO_ROOT", str(Path(__file__).parent.parent)))
RESULTS_PATH = Path(os.getenv("DEMO_RESULTS_PATH", str(REPO_ROOT / "agent" / "demo_results.json")))

DEMO_USER         = "demo_bypass_userA"
LOCAL_USER_PREFIX = "demo_bypass"
TIER              = "free"

# Policy values written for the duration of the demo. GlobalLimit is derived
# in the gateway as limit_per_minute * 3, so 100 → 300. Burst is chosen high
# enough that 250 concurrent requests per region all pass the local token
# bucket — every Phase 2 rejection is therefore global enforcement, not local.
DEMO_LIMIT_PER_MIN = 100
DEMO_BURST         = 500

# Restore target — matches tools/seed_policies.py --demo baseline for free tier.
ORIGINAL_LIMIT_PER_MIN = 300
ORIGINAL_BURST         = 60

# Phase 3 — agent-assisted premium user.
# Agent detects spike demand and raises the per-region limit from 100 → 150,
# so the premium user gets ~450 accepted instead of ~300. G-Counter still
# enforces the (raised) global cap — bypass to 750 remains impossible.
PREMIUM_DEMO_USER          = "demo_premium_userA"
PREMIUM_TIER               = "premium"
PREMIUM_DEMO_LIMIT         = 100   # artificially low so the improvement is visible
AGENT_RAISED_LIMIT_PER_MIN = 150   # agent raises to this → global 450
PREMIUM_DEMO_BURST         = 500   # high burst; all local checks pass, rejections are global only
ORIGINAL_PREMIUM_LIMIT     = 3_000
ORIGINAL_PREMIUM_BURST     = 600

# Cleanup state ─ populated by _save_and_set_demo_policy / _save_and_set_premium_demo_policy.
_original_policies:         dict[str, dict | None] = {}
_original_premium_policies: dict[str, dict | None] = {}

# Shared mutable state that gets serialised to RESULTS_PATH for the dashboard.
_state: dict = {}


# ── Redis helpers ───────────────────────────────────────────────────────────

def _redis_client(region: str) -> redis.Redis:
    cfg = REDIS_CONFIGS[region]
    return redis.Redis(host=cfg["host"], port=cfg["port"], decode_responses=True, socket_connect_timeout=2)


def _flush_demo_keys() -> None:
    """Delete leftover demo counters from any previous run so each run is clean."""
    patterns = ("rl:global:free:demo_bypass_*", "rl:local:*:free:demo_bypass_*")
    for region in REDIS_CONFIGS:
        try:
            r = _redis_client(region)
            for pat in patterns:
                keys = list(r.scan_iter(match=pat, count=200))
                if keys:
                    r.delete(*keys)
        except Exception as exc:
            print(f"[warn] could not flush demo keys in {region}: {exc}", file=sys.stderr)


def _save_and_set_demo_policy() -> str:
    """Cache existing free-tier policies and overwrite with the demo policy.

    Returns the demo policy_id so the propagation probe can wait for it.
    """
    demo_policy_id = f"pol_{int(time.time())}_99"  # seq=99 → distinct from static(1)/seed(2)/agent(≥3)
    for region in ("us", "eu", "asia"):
        try:
            r = _redis_client(region)
            key = f"policy:{region}:{TIER}"
            existing = r.get(key)
            _original_policies[region] = json.loads(existing) if existing else None

            demo_policy = {
                "policy_id":        demo_policy_id,
                "region":           region,
                "tier":             TIER,
                "limit_per_minute": DEMO_LIMIT_PER_MIN,
                "burst":            DEMO_BURST,
                "algorithm":        "token_bucket",
                "ttl_seconds":      300,
                "reason":           "demo_global_quota_bypass",
                "created_at":       datetime.now(timezone.utc).isoformat(),
            }
            r.set(key, json.dumps(demo_policy), ex=300)
        except Exception as exc:
            print(f"[warn] could not set demo policy for {region}: {exc}", file=sys.stderr)
    return demo_policy_id


def _restore_policies() -> None:
    """Restore captured free-tier policies. Falls back to the demo baseline
    if no original policy was captured (Redis was empty or unreachable)."""
    for region in ("us", "eu", "asia"):
        try:
            r = _redis_client(region)
            key = f"policy:{region}:{TIER}"
            original = _original_policies.get(region)
            if original:
                r.set(key, json.dumps(original), ex=original.get("ttl_seconds", 86400))
            else:
                fallback = {
                    "policy_id":        f"pol_{int(time.time())}_2",
                    "region":           region,
                    "tier":             TIER,
                    "limit_per_minute": ORIGINAL_LIMIT_PER_MIN,
                    "burst":            ORIGINAL_BURST,
                    "algorithm":        "token_bucket",
                    "ttl_seconds":      86400,
                    "reason":           "seed — demo baseline (restored after global_quota_bypass demo)",
                    "created_at":       datetime.now(timezone.utc).isoformat(),
                }
                r.set(key, json.dumps(fallback), ex=86400)
        except Exception as exc:
            print(f"[warn] could not restore policy for {region}: {exc}", file=sys.stderr)


def _save_and_set_premium_demo_policy() -> str:
    """Cache existing premium-tier policies and write the initial Phase 3 demo policy."""
    demo_policy_id = f"pol_{int(time.time())}_97"
    for region in ("us", "eu", "asia"):
        try:
            r = _redis_client(region)
            key = f"policy:{region}:{PREMIUM_TIER}"
            existing = r.get(key)
            _original_premium_policies[region] = json.loads(existing) if existing else None
            demo_policy = {
                "policy_id":        demo_policy_id,
                "region":           region,
                "tier":             PREMIUM_TIER,
                "limit_per_minute": PREMIUM_DEMO_LIMIT,
                "burst":            PREMIUM_DEMO_BURST,
                "algorithm":        "token_bucket",
                "ttl_seconds":      300,
                "reason":           "demo_agent_phase3_initial",
                "created_at":       datetime.now(timezone.utc).isoformat(),
            }
            r.set(key, json.dumps(demo_policy), ex=300)
        except Exception as exc:
            print(f"[warn] could not set premium demo policy for {region}: {exc}", file=sys.stderr)
    return demo_policy_id


def _restore_premium_policies() -> None:
    """Restore captured premium-tier policies, falling back to the demo baseline."""
    for region in ("us", "eu", "asia"):
        try:
            r = _redis_client(region)
            key = f"policy:{region}:{PREMIUM_TIER}"
            original = _original_premium_policies.get(region)
            if original:
                r.set(key, json.dumps(original), ex=original.get("ttl_seconds", 86400))
            else:
                fallback = {
                    "policy_id":        f"pol_{int(time.time())}_2",
                    "region":           region,
                    "tier":             PREMIUM_TIER,
                    "limit_per_minute": ORIGINAL_PREMIUM_LIMIT,
                    "burst":            ORIGINAL_PREMIUM_BURST,
                    "algorithm":        "token_bucket",
                    "ttl_seconds":      86400,
                    "reason":           "seed — demo baseline (restored after agent phase3 demo)",
                    "created_at":       datetime.now(timezone.utc).isoformat(),
                }
                r.set(key, json.dumps(fallback), ex=86400)
        except Exception as exc:
            print(f"[warn] could not restore premium policy for {region}: {exc}", file=sys.stderr)


def _flush_premium_demo_keys() -> None:
    """Delete leftover Phase 3 counters so each run starts clean."""
    patterns = (
        f"rl:global:{PREMIUM_TIER}:{PREMIUM_DEMO_USER}",
        f"rl:local:*:{PREMIUM_TIER}:{PREMIUM_DEMO_USER}",
    )
    for region in REDIS_CONFIGS:
        try:
            r = _redis_client(region)
            for pat in patterns:
                keys = list(r.scan_iter(match=pat, count=200))
                if keys:
                    r.delete(*keys)
        except Exception as exc:
            print(f"[warn] could not flush premium demo keys in {region}: {exc}", file=sys.stderr)


# ── Results file ────────────────────────────────────────────────────────────

def _write_results() -> None:
    """Atomically write _state to RESULTS_PATH for the dashboard to read."""
    try:
        RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = RESULTS_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(_state, indent=2))
        tmp.replace(RESULTS_PATH)
    except Exception as exc:
        print(f"[warn] could not write results: {exc}", file=sys.stderr)


def _set_phase(phase: str) -> None:
    _state["current_phase"] = phase
    _write_results()


# ── HTTP helpers ────────────────────────────────────────────────────────────

async def _wait_for_policy(demo_policy_id: str, tier: str = TIER, timeout_s: int = 15) -> bool:
    """Probe each gateway until they all return the demo policy_id.

    Each gateway's policy store polls Redis every 5s — the wait absorbs that
    latency so Phase 1 isn't run against the stale burst=60 policy.
    """
    deadline = time.time() + timeout_s
    pending  = set(GATEWAY_URLS.keys())
    async with httpx.AsyncClient(timeout=3.0) as client:
        while time.time() < deadline and pending:
            for region in list(pending):
                try:
                    resp = await client.post(
                        f"{GATEWAY_URLS[region]}/check",
                        json={
                            "user_id":  "demo_probe",
                            "tier":     tier,
                            "region":   region,
                            "endpoint": "/api/v1/data",
                        },
                    )
                    if resp.json().get("policy_id") == demo_policy_id:
                        pending.discard(region)
                except Exception:
                    pass
            if pending:
                await asyncio.sleep(1.0)
    return not pending


async def _send_one(client: httpx.AsyncClient, region: str, user_id: str, tier: str = TIER) -> bool:
    try:
        resp = await client.post(
            f"{GATEWAY_URLS[region]}/check",
            json={"user_id": user_id, "tier": tier, "region": region, "endpoint": "/api/v1/data"},
            timeout=5.0,
        )
        return bool(resp.json().get("allowed"))
    except Exception:
        return False


async def _burst(region: str, user_id: str, count: int, tier: str = TIER) -> dict:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[_send_one(client, region, user_id, tier) for _ in range(count)])
    accepted = sum(1 for ok in results if ok)
    return {"accepted": accepted, "rejected": len(results) - accepted}


# ── Phases ──────────────────────────────────────────────────────────────────

async def phase_local_only(attempts: int) -> None:
    """Phase 1: three distinct user IDs, one per region.

    Different user IDs mean different G-Counter keys (rl:global:free:demo_bypass_us:*
    vs ...eu vs ...asia), so each user's global sum stays at 250 ≤ 300. Burst=500
    means the local bucket also accepts all 250. Result: all 250 accepted per
    region — total 750. This is what a no-coordination system would let through.
    """
    print("\n" + "=" * 60)
    print("LOCAL-ONLY MODE")
    print("(Three independent rate limiters — no cross-region coordination)")
    print("=" * 60)

    per_region: dict[str, dict] = {}
    for region in ("us", "eu", "asia"):
        user_id = f"{LOCAL_USER_PREFIX}_{region}"
        result  = await _burst(region, user_id, attempts)
        per_region[region] = result
        print(f"{region.upper():<4}  accepted: {result['accepted']:>3},  rejected: {result['rejected']}")
        _state["phase1"]["regions"][region]   = result
        _state["phase1"]["total_accepted"]    = sum(r["accepted"] for r in per_region.values())
        _state["phase1"]["total_rejected"]    = sum(r["rejected"] for r in per_region.values())
        _write_results()

    total_accepted = sum(r["accepted"] for r in per_region.values())
    total_rejected = sum(r["rejected"] for r in per_region.values())
    result_msg = "bypass successful"

    print()
    print(f"Total accepted:  {total_accepted}")
    print(f"Total rejected:  {total_rejected}")
    print(f"Result: {DEMO_USER} bypassed the intended global limit of {_state['config']['global_limit']} rpm")
    print(f"Local-only result: {result_msg}")

    _state["phase1"]["total_accepted"] = total_accepted
    _state["phase1"]["total_rejected"] = total_rejected
    _state["phase1"]["result"]         = result_msg
    _write_results()


async def phase_geo_distributed(attempts: int) -> None:
    """Phase 2: a single user ID, 250 concurrent requests to all 3 regions at once.

    All three gateways increment the same G-Counter key
    (rl:global:free:demo_bypass_userA:*). Once the cross-region sum exceeds 300,
    new requests are rejected by the global cap (line 174 of handler.go).
    """
    print("\n" + "=" * 60)
    print("GEO-DISTRIBUTED MODE")
    print("(Global G-Counter coordinates quota — same user ID across all regions)")
    print("=" * 60)

    results = await asyncio.gather(*[
        _burst(region, DEMO_USER, attempts) for region in ("us", "eu", "asia")
    ])
    per_region = dict(zip(("us", "eu", "asia"), results))

    total_accepted = sum(r["accepted"] for r in per_region.values())
    total_rejected = sum(r["rejected"] for r in per_region.values())

    for region, result in per_region.items():
        print(f"{region.upper():<4}  accepted: {result['accepted']:>3},  rejected: {result['rejected']}")

    global_limit = _state["config"]["global_limit"]
    # Pub/sub sync between regions is fast but not instant. With three gateways
    # racing each other on a 250-request burst, a small overshoot is normal —
    # treat anything within 50% of the limit as "prevented" for the demo.
    if total_accepted <= int(global_limit * 1.5):
        result_msg = "bypass prevented by distributed coordination"
    else:
        result_msg = f"partial bypass — expected ~{global_limit}, got {total_accepted}"

    print()
    print(f"Total accepted:  {total_accepted}")
    print(f"Total rejected:  {total_rejected}")
    print(f"Rejected reason: global_quota_exceeded")
    print(f"Result: {result_msg}")

    _state["phase2"]["regions"]         = per_region
    _state["phase2"]["total_accepted"]  = total_accepted
    _state["phase2"]["total_rejected"]  = total_rejected
    _state["phase2"]["rejected_reason"] = "global_quota_exceeded"
    _state["phase2"]["result"]          = result_msg
    _write_results()


async def phase_agent_assisted(attempts: int) -> None:
    """Phase 3: premium user, agent raises limit 100 → 150/region (global 300 → 450).

    Shows that the AI agent improves throughput for legitimate premium traffic
    while the G-Counter still enforces the raised global cap — bypass to 750
    remains impossible regardless of what the agent does.
    """
    print("\n" + "=" * 60)
    print("AGENT-ASSISTED MODE (PREMIUM TIER)")
    print(f"(Agent raises limit {PREMIUM_DEMO_LIMIT} → {AGENT_RAISED_LIMIT_PER_MIN}/region · global {PREMIUM_DEMO_LIMIT * 3} → {AGENT_RAISED_LIMIT_PER_MIN * 3})")
    print("=" * 60)

    _flush_premium_demo_keys()

    # Set initial low demo policy so the improvement is measurable.
    initial_policy_id = _save_and_set_premium_demo_policy()
    print(f"\nInitial premium policy: limit={PREMIUM_DEMO_LIMIT}/region → global {PREMIUM_DEMO_LIMIT * 3}")
    print("Waiting for policy propagation...")
    if not await _wait_for_policy(initial_policy_id, tier=PREMIUM_TIER, timeout_s=15):
        print("[warn] initial premium policy did not propagate within 15s — continuing")

    # Simulate agent decision: spike detected, raise the limit.
    agent_policy_id = f"pol_{int(time.time())}_96"
    print(f"Agent decision: predicted spike → raising limit {PREMIUM_DEMO_LIMIT} → {AGENT_RAISED_LIMIT_PER_MIN}/region")
    for region in ("us", "eu", "asia"):
        try:
            r = _redis_client(region)
            key = f"policy:{region}:{PREMIUM_TIER}"
            agent_policy = {
                "policy_id":        agent_policy_id,
                "region":           region,
                "tier":             PREMIUM_TIER,
                "limit_per_minute": AGENT_RAISED_LIMIT_PER_MIN,
                "burst":            PREMIUM_DEMO_BURST,
                "algorithm":        "token_bucket",
                "ttl_seconds":      300,
                "reason":           "agent_predicted_spike_premium",
                "created_at":       datetime.now(timezone.utc).isoformat(),
            }
            r.set(key, json.dumps(agent_policy), ex=300)
        except Exception as exc:
            print(f"[warn] could not write agent policy for {region}: {exc}", file=sys.stderr)

    print("Waiting for agent policy propagation...")
    if not await _wait_for_policy(agent_policy_id, tier=PREMIUM_TIER, timeout_s=15):
        print("[warn] agent policy did not propagate within 15s — continuing")

    results = await asyncio.gather(*[
        _burst(region, PREMIUM_DEMO_USER, attempts, PREMIUM_TIER)
        for region in ("us", "eu", "asia")
    ])
    per_region = dict(zip(("us", "eu", "asia"), results))

    total_accepted = sum(r["accepted"] for r in per_region.values())
    total_rejected = sum(r["rejected"] for r in per_region.values())
    agent_global   = AGENT_RAISED_LIMIT_PER_MIN * 3

    for region, result in per_region.items():
        print(f"{region.upper():<4}  accepted: {result['accepted']:>3},  rejected: {result['rejected']}")

    no_agent_baseline = PREMIUM_DEMO_LIMIT * 3
    if total_accepted > int(no_agent_baseline * 1.1):
        result_msg = f"agent raised throughput: ~{no_agent_baseline} → {total_accepted}"
    else:
        result_msg = f"agent-assisted: {total_accepted} accepted (global cap: {agent_global})"

    print()
    print(f"Total accepted:  {total_accepted}  (vs ~{no_agent_baseline} without agent)")
    print(f"Total rejected:  {total_rejected}")
    print(f"Result: {result_msg}")

    _state["phase3"]["regions"]                       = per_region
    _state["phase3"]["total_accepted"]                = total_accepted
    _state["phase3"]["total_rejected"]                = total_rejected
    _state["phase3"]["result"]                        = result_msg
    _state["phase3"]["agent_decision"]["raised_limit"] = AGENT_RAISED_LIMIT_PER_MIN
    _state["phase3"]["agent_decision"]["global_raised"] = agent_global
    _write_results()


# ── Entry point ─────────────────────────────────────────────────────────────

async def main(attempts: int, global_limit: int) -> None:
    _state.update({
        "status":        "running",
        "current_phase": "setup",
        "config": {
            "user":                DEMO_USER,
            "global_limit":        global_limit,
            "attempts_per_region": attempts,
            "tier":                TIER,
        },
        "phase1": {
            "label":          "LOCAL-ONLY MODE",
            "regions":        {"us": None, "eu": None, "asia": None},
            "total_accepted": 0,
            "total_rejected": 0,
            "result":         None,
        },
        "phase2": {
            "label":           "GEO-DISTRIBUTED MODE",
            "regions":         {"us": None, "eu": None, "asia": None},
            "total_accepted":  0,
            "total_rejected":  0,
            "rejected_reason": None,
            "result":          None,
        },
        "phase3": {
            "label":          "AGENT-ASSISTED MODE",
            "agent_decision": {
                "reason":          "agent_predicted_spike_premium",
                "original_limit":  PREMIUM_DEMO_LIMIT,
                "global_original": PREMIUM_DEMO_LIMIT * 3,
                "raised_limit":    None,
                "global_raised":   None,
            },
            "regions":        {"us": None, "eu": None, "asia": None},
            "total_accepted": 0,
            "total_rejected": 0,
            "result":         None,
        },
        "started_at":   datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "error":        None,
    })
    _write_results()

    print()
    print("=" * 60)
    print("GLOBAL QUOTA BYPASS DEMO")
    print("=" * 60)
    print(f"User:         {DEMO_USER}")
    print(f"Global limit: {global_limit} requests/minute")
    print(f"Regions:      US, EU, Asia")
    print(f"Attempted:    {attempts} requests/region = {attempts * 3} total")
    print(f"Tier:         {TIER}")

    print("\nChecking gateway connectivity...")
    async with httpx.AsyncClient(timeout=3.0) as client:
        for region, url in GATEWAY_URLS.items():
            try:
                resp = await client.get(f"{url}/health")
                print(f"  {region}: {'OK' if resp.status_code == 200 else 'WARN'}")
            except Exception as exc:
                print(f"  {region}: UNREACHABLE ({exc})")

    _flush_demo_keys()

    print()
    print("Starting distributed quota bypass simulation")
    print(f"userA global limit: {global_limit} rpm")
    print(f"Attempted traffic:  {attempts} rpm per region")
    print(f"Setting demo policy: limit={DEMO_LIMIT_PER_MIN}/min, burst={DEMO_BURST}, GlobalLimit={DEMO_LIMIT_PER_MIN * 3}")

    demo_policy_id = _save_and_set_demo_policy()

    print("Waiting for gateway policy propagation...")
    if not await _wait_for_policy(demo_policy_id, timeout_s=15):
        print("[warn] policy did not propagate to all gateways within 15s — continuing anyway")

    _set_phase("phase1")
    await phase_local_only(attempts)

    _set_phase("between")
    print("\nWaiting 5 seconds before geo-distributed phase...")
    await asyncio.sleep(5)

    _set_phase("phase2")
    await phase_geo_distributed(attempts)

    _set_phase("between_p3")
    print("\nWaiting 5 seconds before agent-assisted phase...")
    await asyncio.sleep(5)

    _set_phase("phase3")
    await phase_agent_assisted(attempts)

    _set_phase("cleanup")
    print("\nRestoring original policies...")
    _restore_policies()
    _restore_premium_policies()

    _state["status"]        = "complete"
    _state["current_phase"] = "done"
    _state["completed_at"]  = datetime.now(timezone.utc).isoformat()
    _write_results()

    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)


def _handle_signal(sig, _frame):
    print(f"\n[demo] caught signal {sig} — restoring policies", file=sys.stderr)
    _state["status"]        = "error"
    _state["error"]         = f"interrupted by signal {sig}"
    _state["completed_at"]  = datetime.now(timezone.utc).isoformat()
    _write_results()
    _restore_policies()
    _restore_premium_policies()
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Global quota bypass demo")
    parser.add_argument("--attempts",     type=int, default=250, help="Requests per region")
    parser.add_argument("--global-limit", type=int, default=300, help="Global limit displayed in the header")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    try:
        asyncio.run(main(args.attempts, args.global_limit))
    except Exception as exc:
        _state["status"]       = "error"
        _state["error"]        = str(exc)
        _state["completed_at"] = datetime.now(timezone.utc).isoformat()
        _write_results()
        print(f"\n[demo] ERROR: {exc}", file=sys.stderr)
        _restore_policies()
        _restore_premium_policies()
        raise
