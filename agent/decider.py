"""
Rules-based decision engine.

Pure logic — no I/O, no Redis. The loop reads cur_policies from Redis and
passes them in; policy_writer applies the returned decisions.

All threshold comparisons are in requests-per-minute (rpm). Prometheus
gives us RPS; multiply by 60 at the entry point to this module.
"""

import logging
import time
from dataclasses import dataclass
from typing import Literal

from agent.config import DEMO_BASELINE, STATIC_FALLBACK, STATIC_FLOOR, REGIONS, TIERS
from agent.metrics_client import FeatureSnapshot
from agent.predictor import Forecast

log = logging.getLogger(__name__)


@dataclass
class Decision:
    type: Literal["policy", "override"]
    region: str
    tier: str | None        # populated for "policy" decisions
    user_id: str | None     # populated for "override" decisions
    limit_per_minute: int
    ttl: int                # seconds; policy TTL=300 so stale agent doesn't linger
    reason: str             # mandatory audit trail (Contract 4)


class Decider:
    """
    Applies four rules each tick and returns a list of Decisions.

    Hysteresis state is in-memory; a restart resets all timers (the
    60-second window is short enough that this is an acceptable gap).
    """

    _HYSTERESIS_S = 60.0
    _POLICY_TTL = 300       # policies expire if agent crashes
    _OVERRIDE_TTL = 300

    def __init__(self) -> None:
        # last wall-clock time a policy was emitted for (region, tier)
        self._policy_ts: dict[tuple[str, str], float] = {}
        # last wall-clock time an override was emitted for user_id
        self._override_ts: dict[str, float] = {}
        # Track the last time a noisy neighbor attacked a specific tier
        self._last_noisy_ts: dict[tuple[str, str], float] = {}
        # Consecutive ticks where a spike was detected without a noisy-neighbor
        # explanation. Rule 1 requires >= 2 ticks before firing, giving the
        # rl_counter_value gauge time to populate user-level concentration data.
        self._spike_watch: dict[tuple[str, str], int] = {}

    def decide(
        self,
        obs: FeatureSnapshot,
        forecasts: dict[tuple[str, str], Forecast | None],
        anomalies: dict[tuple[str, str], bool],
        cur_policies: dict[tuple[str, str], dict],
    ) -> list[Decision]:
        now = time.monotonic()
        decisions: list[Decision] = []

        for region in REGIONS:
            for tier in TIERS:
                key = (region, tier)
                cur = cur_policies.get(key, {})
                cur_limit_rpm = int(cur.get("limit_per_minute", STATIC_FALLBACK[tier]))

                # — convert all RPS observations to rpm at the boundary —
                observed_rpm = obs.regions[region][tier].rps * 60.0
                fc = forecasts.get(key)
                forecast_rpm = fc.point * 60.0 if fc is not None else observed_rpm
                rejection_rate = obs.regions[region][tier].rejection_rate
                is_anomaly = anomalies.get(key, False)
                premium_key = (region, "premium")
                # Compute spike early so _spike_watch can be updated before Rule 1.
                spike = (forecast_rpm > 0.8 * cur_limit_rpm) or is_anomaly

                # Detect noisy-neighbor pattern for this tick.
                # When any single user holds > 30% of tier traffic the pattern
                # is concentration, not a tier-wide spike — Rule 3 (per-user
                # override) is the right tool, not Rule 1 (tier-wide policy).
                # Computed here so it can gate Rule 1 before hysteresis check.
                sorted_users = sorted(
                    obs.regions[region][tier].top_users, 
                    key=lambda x: x.share_of_tier, 
                    reverse=True
                )
                
                # Calculate the combined share of the top 2 users (handles cases with 0, 1, or 2+ users)
                top_2_share = sum(u.share_of_tier for u in sorted_users[:2])
                
                # Gate Rule 1: It is a noisy neighbor attack if traffic is high and highly concentrated
                has_noisy_neighbor = (
                    observed_rpm >= 30
                    and top_2_share > 0.70
                )
                # ── The 5-Minute Predictor Cooldown ──
                # If we see an attack, log the timestamp.
                if has_noisy_neighbor:
                    self._last_noisy_ts[key] = now
                
                # Check if an attack happened in the last 300 seconds (5 minutes)
                last_noisy = self._last_noisy_ts.get(key, 0.0)
                recent_noisy_neighbor = (now - last_noisy) < 300.0

                # has_user_data is True once at least one user has crossed the
                # rl_counter_value emission threshold (sum >= Limit/2 allowed
                # requests). Until then the gauge is empty and we cannot tell
                # whether the spike is concentrated or distributed.
                has_user_data = len(obs.regions[region][tier].top_users) > 0

                # Advance the deliberation counter when a spike is active but not
                # yet attributed to a noisy neighbor. Reset on any other tick so the
                # counter always reflects *consecutive* unattributed spike ticks.
                # Used only as a fallback for genuinely distributed spikes where
                # no individual user ever crosses the gauge threshold.
                if spike and not recent_noisy_neighbor:
                    self._spike_watch[key] = self._spike_watch.get(key, 0) + 1
                else:
                    self._spike_watch[key] = 0

                # ── Rule 3 — noisy neighbor (per-user, own hysteresis) ───────
                # Runs BEFORE Rule 4's tier-policy hysteresis gate. Per-user
                # overrides are an independent concern from tier-policy churn,
                # and tying them together caused a demo-blocking timing bug:
                # if Rule 1 fired on tick N (because the predictor saw a rising
                # tier RPS before the top-N gauge sample revealed the actual
                # abuser), the entire (region, tier) iteration was skipped for
                # 60 s — meaning Rule 3 couldn't throttle the abuser the agent
                # could now plainly see. See docs/demo-prep.md §5a-B.
                #
                # Guard: skip when traffic is idle or the agent is still warming
                # up — observed_rpm < 30 means < 0.5 RPS, which produces noisy
                # counter fractions that falsely elevate share_of_tier to 1.0.
                if observed_rpm >= 30:
                    _NOISY_USER_MIN_RPM = 60   # 1 rps; below this a "30% share" is sampling noise

                    for user in obs.regions[region][tier].top_users:
                        if user.share_of_tier <= 0.30:
                            continue
                        # Sparse-traffic regions (e.g. Asia free at 3rps) can produce
                        # spurious >30% shares from any user that happens to land in
                        # the top-N gauge sample. Require the user themself to be
                        # generating meaningful traffic before throttling them.
                        if user.rps * 60.0 < _NOISY_USER_MIN_RPM:
                            continue
                        u_last = self._override_ts.get(user.user_id)
                        if u_last is not None and (now - u_last) < self._HYSTERESIS_S:
                            continue
                        # Throttle against the fixed baseline (not cur_limit_rpm).
                        # cur_limit_rpm can be reduced by Rule 1, which would make
                        # the override value shrink each cycle — 30 → 21 → 14 → …
                        # Using DEMO_BASELINE keeps it stable: 30/min for free tier.
                        throttle = max(10, DEMO_BASELINE[tier] // 10)
                        decisions.append(Decision(
                            type="override", region=region, tier=tier,
                            user_id=user.user_id,
                            limit_per_minute=throttle,
                            ttl=self._OVERRIDE_TTL,
                            reason=f"noisy_neighbor_{user.user_id}",
                        ))
                        self._override_ts[user.user_id] = now

                # ── The Safety Net (Instant Undo) ──
                # If the agent mistakenly lowered the tier limit on Tick N,
                # but catches the abuser on Tick N+1, instantly restore the capacity.
                # Also reverts any premium compensation that was emitted alongside
                # a false Rule 1 firing — without this, premium stays elevated even
                # after the noisy neighbor has been identified and overridden.
                if has_noisy_neighbor:
                    baseline_rpm = DEMO_BASELINE[tier]
                    if cur_limit_rpm < baseline_rpm:
                        decisions.append(Decision(
                            type="policy", region=region, tier=tier, user_id=None,
                            limit_per_minute=baseline_rpm,
                            ttl=self._POLICY_TTL,
                            reason=f"undo_false_spike_due_to_noisy_neighbor",
                        ))
                        self._policy_ts[key] = now
                    # Revert premium compensation only from the free-tier iteration
                    # to avoid emitting the same decision twice per region.
                    if tier == "free":
                        prem_cur = cur_policies.get(premium_key, {})
                        prem_limit = int(prem_cur.get("limit_per_minute", STATIC_FALLBACK["premium"]))
                        prem_baseline = DEMO_BASELINE["premium"]
                        if prem_limit > prem_baseline:
                            # No hysteresis check here — this is an emergency revert,
                            # not a proactive policy change. The hysteresis on proactive
                            # decisions is 60s but the noisy neighbor is confirmed just
                            # one tick (15s) after the false premium boost, so gating
                            # on hysteresis would suppress this revert entirely.
                            decisions.append(Decision(
                                type="policy", region=region, tier="premium", user_id=None,
                                limit_per_minute=prem_baseline,
                                ttl=self._POLICY_TTL,
                                reason="undo_premium_compensation_due_to_noisy_neighbor",
                            ))
                            self._policy_ts[premium_key] = now

                # Rule 4 — hysteresis gate (Rules 1 & 2 only; Rule 3 ran above)
                last_pol = self._policy_ts.get(key)
                if last_pol is not None and (now - last_pol) < self._HYSTERESIS_S:
                    continue

                # ── Rule 1 — predicted spike mitigation (free tier only) ────
                # Skipped when a noisy neighbor is present OR was recently present,
                # preventing the "throttle bounce" while the predictor cools down.
                #
                # Gate: require user-level gauge data before firing a tier-wide
                # policy change. The rl_counter_value gauge only emits a series
                # once a user has accumulated >= Limit/2 allowed requests — with a
                # 15s interval that silence window spans ~30s. Firing during it
                # causes the "false spike" the Safety Net has to undo.
                # Fallback: after 4 consecutive spike ticks (60s) without any
                # gauge data, accept that traffic is too distributed for individual
                # users to reach the emission threshold and fire anyway.
                if tier == "free" and not recent_noisy_neighbor:
                    premium_rej = obs.regions[region]["premium"].rejection_rate
                    gauge_ready = has_user_data or self._spike_watch.get(key, 0) >= 3
                    if spike and premium_rej < 0.10 and gauge_ready:
                        new_free_rpm = max(STATIC_FLOOR["free"], int(cur_limit_rpm * 0.70))
                        decisions.append(Decision(
                            type="policy", region=region, tier="free", user_id=None,
                            limit_per_minute=new_free_rpm,
                            ttl=self._POLICY_TTL,
                            reason=f"predicted_spike_{region}_free",
                        ))
                        self._policy_ts[key] = now

                        # Premium compensation — gated by its own hysteresis
                        prem_last = self._policy_ts.get(premium_key)
                        if prem_last is None or (now - prem_last) >= self._HYSTERESIS_S:
                            prem_cur = cur_policies.get(premium_key, {})
                            prem_limit = int(prem_cur.get("limit_per_minute", STATIC_FALLBACK["premium"]))
                            decisions.append(Decision(
                                type="policy", region=region, tier="premium", user_id=None,
                                limit_per_minute=int(prem_limit * 1.10),
                                ttl=self._POLICY_TTL,
                                reason=f"predicted_spike_{region}_premium_compensation",
                            ))
                            self._policy_ts[premium_key] = now

                        continue

                # ── Rule 2 — capacity restoration ────────────────────────────
                # Also fires when the policy key has expired entirely: Redis
                # returns STATIC_FALLBACK (10/100/1000) instead of the seeded
                # baseline, which makes the dashboard show "Not set" and silently
                # drops the gateway's effective limit. In that case restore to
                # baseline immediately regardless of rejection_rate.
                baseline_rpm = DEMO_BASELINE[tier]
                key_missing  = cur_limit_rpm <= STATIC_FALLBACK[tier]
                slow_restore = (
                    forecast_rpm < 0.5 * cur_limit_rpm
                    and rejection_rate > 0
                    and cur_limit_rpm < baseline_rpm
                )
                if slow_restore or (key_missing and cur_limit_rpm < baseline_rpm):
                    step_rpm = baseline_rpm if key_missing else min(baseline_rpm, int(cur_limit_rpm * 1.20))
                    decisions.append(Decision(
                        type="policy", region=region, tier=tier, user_id=None,
                        limit_per_minute=step_rpm,
                        ttl=self._POLICY_TTL,
                        reason=f"restore_capacity_{region}_{tier}",
                    ))
                    self._policy_ts[key] = now
                    continue

        return decisions
