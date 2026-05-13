"""
Dashboard API — Flask backend for the monitoring dashboard.

Read endpoints:
  GET  /api/metrics              — Prometheus summary (RPS, allow rate, active users, history)
  GET  /api/decisions            — Last 20 agent decisions from decisions.jsonl
  GET  /api/overrides            — All override:* keys from all 3 Redis instances
  GET  /api/counters             — rl:global:* slot distribution from all 3 Redis instances

Control endpoints:
  POST /api/control/scenario      — Start simulator with a named scenario
  POST /api/control/scenario/stop — Stop running simulator + clear overrides
  POST /api/control/agent/start   — Start agent in background
  POST /api/control/agent/stop    — Stop agent process
  GET  /api/control/status       — Agent/simulator/container health
  POST /api/control/policies/seed — Seed demo policies via tools/seed_policies.py
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

import redis
import requests
import yaml
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ── Configuration ────────────────────────────────────────────────────────────

def _resolve_env_vars(value: str) -> str:
    """Replace ${VAR:-default} patterns with environment variable values."""
    def _sub(match):
        var, _, default = match.group(1).partition(":-")
        return os.environ.get(var, default)
    return re.sub(r"\$\{([^}]+)\}", _sub, value)


def _resolve_config(obj):
    """Recursively resolve ${VAR:-default} placeholders in a parsed YAML structure."""
    if isinstance(obj, str):
        return _resolve_env_vars(obj)
    if isinstance(obj, dict):
        return {k: _resolve_config(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_config(item) for item in obj]
    return obj


def load_config() -> dict:
    """Load config.yaml from the repo root, resolve env-var placeholders, and return the dict."""
    repo_root = Path(__file__).parent.parent
    config_path = repo_root / "config.yaml"
    if not config_path.exists():
        # Fallback: construct minimal config from env vars so local dev works without the file
        return {
            "services": {
                "prometheus": {"url": os.getenv("PROMETHEUS_URL", os.getenv("PROM_URL", "http://localhost:9090"))},
                "redis": {
                    "us":   {"host": os.getenv("REDIS_US_HOST", "localhost"),   "port": os.getenv("REDIS_US_PORT", "6379")},
                    "eu":   {"host": os.getenv("REDIS_EU_HOST", "localhost"),   "port": os.getenv("REDIS_EU_PORT", "6380")},
                    "asia": {"host": os.getenv("REDIS_ASIA_HOST", "localhost"), "port": os.getenv("REDIS_ASIA_PORT", "6381")},
                },
            },
        }
    with open(config_path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return _resolve_config(raw)


_config = load_config()
_svc    = _config.get("services", {})

PROM_URL       = _svc.get("prometheus", {}).get("url", "http://localhost:9090")
DECISIONS_PATH = Path(os.getenv("LOG_PATH", "agent/decisions.jsonl"))

_redis_svc = _svc.get("redis", {})
REDIS_CONFIGS = {
    region: {
        "host": _redis_svc.get(region, {}).get("host", defaults[0]),
        "port": int(_redis_svc.get(region, {}).get("port", defaults[1])),
    }
    for region, defaults in (("us", ("localhost", 6379)), ("eu", ("localhost", 6380)), ("asia", ("localhost", 6381)))
}

# ── Control configuration ─────────────────────────────────────────────────────

# Repo root — overridable via env var so the containerised agent can point
# at a bind-mounted copy of the repo (e.g. REPO_ROOT=/repo).
REPO_ROOT = Path(os.getenv("REPO_ROOT", str(Path(__file__).parent.parent)))

# Configurable paths — override via env vars if the layout ever changes
SIMULATOR_SCRIPT = Path(os.getenv("SIMULATOR_SCRIPT", str(REPO_ROOT / "simulator" / "main.py")))
SEED_SCRIPT      = Path(os.getenv("SEED_SCRIPT",      str(REPO_ROOT / "tools" / "seed_policies.py")))
DEMO_BYPASS_SCRIPT = Path(os.getenv("DEMO_BYPASS_SCRIPT", str(REPO_ROOT / "simulator" / "demo_global_quota.py")))
DEMO_RESULTS_PATH  = Path(os.getenv("DEMO_RESULTS_PATH",  str(Path(__file__).parent / "demo_results.json")))
DEMO_LOG_PATH      = Path(os.getenv("DEMO_LOG_PATH",      str(Path(__file__).parent / "demo_global_quota.log")))

VALID_SCENARIOS: frozenset[str] = frozenset({"noisy_neighbor", "product_launch", "global_steady", "region_failover", "global_quota_bypass"})

# Subprocess handles — only one agent and one simulator run at a time
_procs: dict[str, subprocess.Popen | None] = {"agent": None, "simulator": None}
_proc_started: dict[str, float] = {}

# Tracks when the current scenario started, so /api/decisions can filter
# the global decisions.jsonl to entries from the active scenario only and
# the dashboard's Live Agent Decisions panel doesn't show stale reasons
# from the prior scenario. Set by /api/control/scenario; cleared by reset.
_scenario_started_ms: int | None = None
_scenario_active: str | None = None

# Agent config saved at start-time so /api/model/metrics knows what's running
_agent_config: dict = {}   # {"predictor": "ewma"|"holtwinters", "interval": 15}

# Control log — separate from the Flask/werkzeug log so actions are auditable
_ctrl_log = logging.getLogger("control")
_ctrl_log.setLevel(logging.INFO)
_ctrl_handler = logging.FileHandler(Path(__file__).parent / "control.log")
_ctrl_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
_ctrl_log.addHandler(_ctrl_handler)

# In-memory RPS history (last 20 samples)
_rps_history: deque = deque(maxlen=20)
_allow_history: deque = deque(maxlen=20)
_users_history: deque = deque(maxlen=20)

# ── Startup cleanup ───────────────────────────────────────────────────────────
# decisions.jsonl is bind-mounted from the host repo, so it survives container
# restarts. Clear it on every cold start so the dashboard shows 0 decisions
# until the agent actually runs.
def _decisions_path() -> Path:
    p = DECISIONS_PATH if DECISIONS_PATH.is_absolute() else Path.cwd() / DECISIONS_PATH
    if not p.exists():
        p = Path(__file__).parent / "decisions.jsonl"
    return p

try:
    _dp = _decisions_path()
    if _dp.exists():
        _dp.unlink()
        _ctrl_log.info("startup: cleared stale decisions.jsonl")
except Exception as _exc:
    _ctrl_log.warning("startup: could not clear decisions.jsonl: %s", _exc)

# ── Redis helpers ─────────────────────────────────────────────────────────────

def _redis_client(region: str) -> redis.Redis:
    cfg = REDIS_CONFIGS[region]
    return redis.Redis(host=cfg["host"], port=cfg["port"], decode_responses=True, socket_connect_timeout=2)


def _safe_redis(region: str, fn):
    """Execute fn(client) against a region's Redis; return None on any error."""
    try:
        client = _redis_client(region)
        return fn(client)
    except Exception:
        return None

# ── Prometheus helpers ────────────────────────────────────────────────────────

def _prom_query(promql: str) -> list:
    try:
        resp = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": promql},
            timeout=4,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") == "success":
            return body["data"]["result"]
    except Exception:
        pass
    return []


def _prom_scalar(promql: str) -> float:
    results = _prom_query(promql)
    if results:
        try:
            return float(results[0]["value"][1])
        except (IndexError, KeyError, ValueError, TypeError):
            pass
    return 0.0


def _prom_range(promql: str, step: str = "5s") -> list[dict]:
    """Fetch a range query and return [{t, v}] pairs for the first series."""
    try:
        now = time.time()
        resp = requests.get(
            f"{PROM_URL}/api/v1/query_range",
            params={
                "query": promql,
                "start": now - 90,
                "end": now,
                "step": step,
            },
            timeout=5,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") == "success":
            results = body["data"]["result"]
            if results:
                return [
                    {"t": int(pair[0] * 1000), "v": round(float(pair[1]) if float(pair[1]) == float(pair[1]) else 0.0, 3)}
                    for pair in results[0]["values"]
                ]
    except Exception:
        pass
    return []

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.route("/api/metrics")
def api_metrics():
    # Current values
    total_rps = _prom_scalar('sum(rate(rl_requests_total[1m]))')
    allowed_rps = _prom_scalar('sum(rate(rl_requests_total{decision="allowed"}[1m]))')
    active_users = _prom_scalar('count(rl_counter_value)')

    # Clip to [0, 100]: Prometheus rate() over a 1m window can briefly
    # report allowed > total during gateway restart counter-resets or when
    # series labels enter/leave the rate window at different times.
    allow_rate = round(min(100.0, max(0.0, (allowed_rps / total_rps * 100) if total_rps > 0 else 0.0)), 1)
    total_rps_r = round(total_rps, 2)

    # Range history for sparklines
    rps_spark = _prom_range('sum(rate(rl_requests_total[1m]))')
    allow_spark = _prom_range(
        'clamp_max(sum(rate(rl_requests_total{decision="allowed"}[1m])) / sum(rate(rl_requests_total[1m])) * 100, 100)'
    )
    users_spark = _prom_range('count(rl_counter_value)')

    # Decision count (last 24h decisions total)
    recent_decisions = _count_recent_decisions()

    return jsonify({
        "total_rps": total_rps_r,
        "allow_rate": allow_rate,
        "active_users": int(active_users),
        "recent_decisions": recent_decisions,
        "sparklines": {
            "rps": rps_spark[-20:] if rps_spark else [],
            "allow_rate": allow_spark[-20:] if allow_spark else [],
            "users": users_spark[-20:] if users_spark else [],
        },
        "ts": int(time.time() * 1000),
    })


@app.route("/api/decisions")
def api_decisions():
    # Filter to current-scenario entries by default so the Live Agent
    # Decisions panel doesn't show reasons from the previous scenario.
    # ?since=<ms> overrides; ?since=0 disables filtering entirely.
    since_param = request.args.get("since")
    if since_param is not None:
        try:
            since_ms = int(since_param)
        except ValueError:
            since_ms = 0
    else:
        since_ms = _scenario_started_ms or 0

    entries = []
    try:
        path = DECISIONS_PATH if DECISIONS_PATH.is_absolute() else Path.cwd() / DECISIONS_PATH
        if not path.exists():
            # Try relative to this file
            path = Path(__file__).parent / "decisions.jsonl"
        if path.exists():
            with open(path, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
            # Parse last 100 lines, extract individual decisions
            for line in lines[-100:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    tick = json.loads(line)
                    ts = tick.get("timestamp", 0)
                    if since_ms and ts < since_ms:
                        continue
                    for dec in tick.get("decisions", []):
                        entries.append({
                            "ts": ts,
                            "tick": tick.get("tick", 0),
                            "type": dec.get("type", ""),
                            "region": dec.get("region", ""),
                            "tier": dec.get("tier", ""),
                            "user_id": dec.get("user_id"),
                            "limit_per_minute": dec.get("limit_per_minute"),
                            "reason": dec.get("reason", ""),
                        })
                except (json.JSONDecodeError, KeyError):
                    continue
    except Exception as e:
        return jsonify({"error": str(e), "decisions": []}), 200

    # Return newest first, last 20
    entries.sort(key=lambda x: x["ts"], reverse=True)
    return jsonify(entries[:20])


@app.route("/api/overrides")
def api_overrides():
    overrides = []
    for region in ("us", "eu", "asia"):
        def fetch(client, r=region):
            keys = client.keys("override:*")
            result = []
            for key in keys:
                try:
                    raw = client.get(key)
                    if raw:
                        data = json.loads(raw)
                        user_id = key.split(":", 1)[1] if ":" in key else key
                        ttl = client.ttl(key)
                        # ttl == -1 means no Redis EXPIRE was set — the key never
                        # expires and represents a stale override; skip it.
                        # ttl > 0 means the override is genuinely active.
                        if ttl <= 0:
                            continue
                        result.append({
                            "user_id": user_id,
                            "limit": data.get("limit_per_minute"),
                            "reason": data.get("reason", ""),
                            "ttl": ttl,
                            "region": r,
                        })
                except (json.JSONDecodeError, Exception):
                    continue
            return result

        region_overrides = _safe_redis(region, fetch) or []
        for ov in region_overrides:
            # Deduplicate by user_id (same override may be in multiple regions)
            if not any(x["user_id"] == ov["user_id"] for x in overrides):
                overrides.append(ov)

    return jsonify(overrides)


@app.route("/api/counters")
def api_counters():
    """
    Returns top users by global counter value.
    rl:global:{tier}:{user_id} is a hash with slots: us, eu, asia.
    We scan US Redis (authoritative for global keys) and aggregate.
    """
    counters = []

    def fetch_global(client):
        keys = client.keys("rl:global:*")
        result = []
        for key in keys:
            try:
                slots = client.hgetall(key)
                parts = key.split(":")  # rl:global:{tier}:{user_id}
                if len(parts) < 4:
                    continue
                tier = parts[2]
                user_id = ":".join(parts[3:])
                us_val = int(slots.get("us", 0))
                eu_val = int(slots.get("eu", 0))
                asia_val = int(slots.get("asia", 0))
                total = us_val + eu_val + asia_val
                if total > 0:
                    result.append({
                        "user_id": user_id,
                        "tier": tier,
                        "us": us_val,
                        "eu": eu_val,
                        "asia": asia_val,
                        "total": total,
                    })
            except Exception:
                continue
        return result

    # Query all three Redis instances and merge
    seen = set()
    for region in ("us", "eu", "asia"):
        entries = _safe_redis(region, fetch_global) or []
        for entry in entries:
            key = (entry["user_id"], entry["tier"])
            if key not in seen:
                seen.add(key)
                counters.append(entry)

    # Sort by total desc, return top 20
    counters.sort(key=lambda x: -x["total"])
    return jsonify(counters[:20])


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok", "ts": int(time.time() * 1000)})


@app.route("/api/demo/results")
def api_demo_results():
    """Return the latest demo_global_quota.py results (or an idle placeholder).

    The demo script writes structured progress to DEMO_RESULTS_PATH as each
    phase completes; the dashboard polls this endpoint to render the live
    comparison panel.
    """
    try:
        if DEMO_RESULTS_PATH.exists():
            return jsonify(json.loads(DEMO_RESULTS_PATH.read_text()))
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500
    return jsonify({"status": "idle", "current_phase": None})


@app.route("/api/demo/clear", methods=["POST"])
def api_demo_clear():
    """Delete the demo results file so the panel returns to idle state."""
    try:
        if DEMO_RESULTS_PATH.exists():
            DEMO_RESULTS_PATH.unlink()
        return jsonify({"cleared": True})
    except Exception as exc:
        return jsonify({"cleared": False, "error": str(exc)}), 500


# ── Control helpers ───────────────────────────────────────────────────────────

def _is_running(key: str) -> bool:
    """Return True if the stored process is still alive."""
    proc = _procs.get(key)
    return proc is not None and proc.poll() is None


_GATEWAY_URLS = {
    "gateway-us":   _svc.get("gateways", {}).get("us",   os.getenv("GATEWAY_US_URL",   "http://localhost:8081")),
    "gateway-eu":   _svc.get("gateways", {}).get("eu",   os.getenv("GATEWAY_EU_URL",   "http://localhost:8082")),
    "gateway-asia": _svc.get("gateways", {}).get("asia", os.getenv("GATEWAY_ASIA_URL", "http://localhost:8083")),
}


def _docker_status() -> list[dict]:
    """Return service-level health for the components shown in the dashboard.

    We probe Redis instances + gateways + Prometheus directly instead of shelling
    out to the docker CLI (which isn't installed inside the api container). The
    response shape stays {name, status} so the dashboard's container-health pill
    keeps working unchanged.
    """
    out: list[dict] = []

    for region, cfg in REDIS_CONFIGS.items():
        try:
            redis.Redis(host=cfg["host"], port=cfg["port"], socket_connect_timeout=1, socket_timeout=1).ping()
            out.append({"name": f"redis-{region}", "status": "ok"})
        except Exception as exc:
            out.append({"name": f"redis-{region}", "status": f"down: {exc}"})

    for name, url in _GATEWAY_URLS.items():
        try:
            r = requests.get(f"{url.rstrip('/')}/health", timeout=1.5)
            out.append({"name": name, "status": "ok" if r.ok else f"down: HTTP {r.status_code}"})
        except Exception as exc:
            out.append({"name": name, "status": f"down: {exc}"})

    try:
        r = requests.get(f"{PROM_URL.rstrip('/')}/-/ready", timeout=1.5)
        out.append({"name": "prometheus", "status": "ok" if r.ok else f"down: HTTP {r.status_code}"})
    except Exception as exc:
        out.append({"name": "prometheus", "status": f"down: {exc}"})

    return out


# ── Control endpoints ─────────────────────────────────────────────────────────

def _clear_overrides() -> dict[str, int]:
    """Delete all override:* keys from every regional Redis. Returns per-region counts.

    Overrides have their own multi-minute TTL and survive `FLUSHDB` on the rate-limit
    counter keys, which causes prior-scenario abuser throttles to bleed into the next
    scenario. Scrub them at scenario-start so each demo is isolated.
    """
    deleted: dict[str, int] = {}
    for region in REDIS_CONFIGS:
        try:
            client = _redis_client(region)
            keys = list(client.scan_iter(match="override:*", count=200))
            deleted[region] = client.delete(*keys) if keys else 0
        except Exception as exc:
            _ctrl_log.warning("override scrub failed for %s: %s", region, exc)
            deleted[region] = -1
    return deleted


@app.route("/api/control/scenario", methods=["POST"])
def api_control_scenario():
    """Start the simulator with a named predefined scenario."""
    body     = request.get_json(silent=True) or {}
    scenario = body.get("scenario", "")
    duration = body.get("duration")  # optional, not forwarded — scenario has its own length

    if scenario not in VALID_SCENARIOS:
        return jsonify({
            "status": "error",
            "message": f"Unknown scenario '{scenario}'. Valid: {sorted(VALID_SCENARIOS)}",
        }), 400

    if _is_running("simulator"):
        return jsonify({
            "status": "error",
            "message": f"Simulator already running (pid {_procs['simulator'].pid}). Stop it first.",
        }), 400

    scrubbed = _clear_overrides()
    _ctrl_log.info("scenario %s: scrubbed overrides %s", scenario, scrubbed)

    # global_quota_bypass is a self-contained demo, not a Poisson-stream scenario.
    # It runs simulator/demo_global_quota.py and writes structured results to
    # DEMO_RESULTS_PATH for the dashboard "Global Quota Bypass" panel to read.
    extra_env = None
    if scenario == "global_quota_bypass":
        # Clear any prior results so the dashboard doesn't show stale data
        # before the new run writes its first state.
        try:
            if DEMO_RESULTS_PATH.exists():
                DEMO_RESULTS_PATH.unlink()
        except Exception as exc:
            _ctrl_log.warning("could not clear demo results: %s", exc)
        cmd        = [sys.executable, str(DEMO_BYPASS_SCRIPT)]
        log_handle = open(DEMO_LOG_PATH, "w")
        stdout, stderr = log_handle, subprocess.STDOUT
        # Pin the results path so the demo writes to the same file we read.
        # The api container has both ./agent:/app/agent and .:/repo bind-mounted,
        # so /app/agent and /repo/agent point at the same host directory — but
        # being explicit avoids any future drift.
        extra_env = {**os.environ, "DEMO_RESULTS_PATH": str(DEMO_RESULTS_PATH)}
    else:
        cmd        = [sys.executable, str(SIMULATOR_SCRIPT), "scenario", scenario]
        log_handle = None
        stdout, stderr = subprocess.DEVNULL, subprocess.DEVNULL

    _ctrl_log.info("START simulator: %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT),
            stdout=stdout,
            stderr=stderr,
            env=extra_env,
        )
        if log_handle is not None:
            # Keep the file handle alive on the subprocess; Popen retains a ref.
            log_handle.close()
        _procs["simulator"]    = proc
        _proc_started["simulator"] = time.time()
        global _scenario_started_ms, _scenario_active
        _scenario_started_ms = int(time.time() * 1000)
        _scenario_active = scenario
        _ctrl_log.info("simulator started pid=%d scenario=%s started_ms=%d", proc.pid, scenario, _scenario_started_ms)
        return jsonify({
            "status": "started", "pid": proc.pid, "scenario": scenario,
            "overrides_scrubbed": scrubbed,
            "started_ms": _scenario_started_ms,
        })
    except Exception as exc:
        _ctrl_log.error("simulator start failed: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/control/agent/start", methods=["POST"])
def api_control_agent_start():
    """Start the AI agent in background."""
    if _is_running("agent"):
        return jsonify({
            "status": "error",
            "message": f"Agent already running (pid {_procs['agent'].pid}).",
        }), 400

    body      = request.get_json(silent=True) or {}
    predictor = body.get("predictor", "ewma")
    interval  = int(body.get("interval", 15))

    if predictor not in ("ewma", "holtwinters"):
        return jsonify({"status": "error", "message": "predictor must be 'ewma' or 'holtwinters'"}), 400

    # Wipe stale decisions from any previous run so the dashboard always
    # shows only the current session (file persists on the bind-mounted volume).
    try:
        dpath = DECISIONS_PATH if DECISIONS_PATH.is_absolute() else Path.cwd() / DECISIONS_PATH
        if not dpath.exists():
            dpath = Path(__file__).parent / "decisions.jsonl"
        if dpath.exists():
            dpath.unlink()
            _ctrl_log.info("cleared stale decisions.jsonl before agent start")
    except Exception as exc:
        _ctrl_log.warning("could not clear decisions.jsonl: %s", exc)

    cmd = [sys.executable, "-m", "agent.main", "run",
           "--predictor", predictor, "--interval", str(interval)]
    _ctrl_log.info("START agent: %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _procs["agent"]    = proc
        _proc_started["agent"] = time.time()
        _agent_config.update({"predictor": predictor, "interval": interval})
        _ctrl_log.info("agent started pid=%d predictor=%s interval=%d", proc.pid, predictor, interval)
        return jsonify({"status": "started", "pid": proc.pid, "predictor": predictor, "interval": interval})
    except Exception as exc:
        _ctrl_log.error("agent start failed: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/control/agent/stop", methods=["POST"])
def api_control_agent_stop():
    """Stop the agent process if running."""
    proc = _procs.get("agent")
    if proc is None or proc.poll() is not None:
        _procs["agent"] = None
        return jsonify({"status": "stopped", "note": "agent was not running"})

    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        pid = proc.pid
        _ctrl_log.info("agent stopped pid=%d", pid)
        _procs["agent"] = None
        _proc_started.pop("agent", None)
        _agent_config.clear()

        # Delete decisions.jsonl so the next agent start has a clean warmup.
        # Use the same path resolution as _parse_recent_ticks.
        cleaned = False
        try:
            dpath = DECISIONS_PATH if DECISIONS_PATH.is_absolute() else Path.cwd() / DECISIONS_PATH
            if not dpath.exists():
                dpath = Path(__file__).parent / "decisions.jsonl"
            if dpath.exists():
                dpath.unlink()
                cleaned = True
                _ctrl_log.info("deleted decisions.jsonl — next start will warm up from scratch")
        except Exception as exc:
            _ctrl_log.warning("could not delete decisions.jsonl: %s", exc)

        return jsonify({"status": "stopped", "pid": pid, "decisions_cleared": cleaned})
    except Exception as exc:
        _ctrl_log.error("agent stop failed: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/control/scenario/stop", methods=["POST"])
def api_control_scenario_stop():
    """Stop the running simulator process and optionally clear overrides."""
    body = request.get_json(silent=True) or {}
    clear = body.get("clear_overrides", True)

    proc = _procs.get("simulator")
    if proc is None or proc.poll() is not None:
        _procs["simulator"] = None
        return jsonify({"status": "stopped", "note": "simulator was not running"})

    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        pid = proc.pid
        _ctrl_log.info("simulator stopped pid=%d", pid)
        _procs["simulator"] = None
        _proc_started.pop("simulator", None)

        # Safety net: if global_quota_bypass was active and got killed before
        # its in-process cleanup ran, re-seed policies so other scenarios
        # don't run against the temporary limit=100 policy. seed_policies.py
        # is idempotent — safe to call even if the demo restored cleanly.
        global _scenario_active
        policies_restored = False
        if _scenario_active == "global_quota_bypass":
            try:
                subprocess.run(
                    [sys.executable, str(SEED_SCRIPT), "--demo"],
                    cwd=str(REPO_ROOT),
                    capture_output=True, text=True, timeout=10,
                )
                policies_restored = True
                _ctrl_log.info("global_quota_bypass stop: re-seeded baseline policies")
            except Exception as exc:
                _ctrl_log.warning("global_quota_bypass stop: policy re-seed failed: %s", exc)
        _scenario_active = None

        scrubbed = _clear_overrides() if clear else {}
        if clear:
            _ctrl_log.info("simulator stop: scrubbed overrides %s", scrubbed)

        return jsonify({
            "status": "stopped", "pid": pid,
            "overrides_scrubbed": scrubbed,
            "policies_restored":  policies_restored,
        })
    except Exception as exc:
        _ctrl_log.error("simulator stop failed: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/control/status")
def api_control_status():
    """Return running state for agent, simulator, and all Docker containers."""
    now = time.time()

    agent_running = _is_running("agent")
    agent_info: dict = {"running": agent_running}
    if agent_running and _procs["agent"]:
        agent_info["pid"] = _procs["agent"].pid
        if "agent" in _proc_started:
            agent_info["uptime_seconds"] = int(now - _proc_started["agent"])

    sim_running = _is_running("simulator")
    sim_info: dict = {"running": sim_running}
    if sim_running and _procs["simulator"]:
        sim_info["pid"] = _procs["simulator"].pid
        if "simulator" in _proc_started:
            sim_info["uptime_seconds"] = int(now - _proc_started["simulator"])

    return jsonify({
        "agent":      agent_info,
        "simulator":  sim_info,
        "containers": _docker_status(),
    })


@app.route("/api/control/policies/seed", methods=["POST"])
def api_control_policies_seed():
    """Seed demo policies to all regional Redis instances."""
    cmd = [sys.executable, str(SEED_SCRIPT), "--demo"]
    _ctrl_log.info("SEED policies: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            _ctrl_log.error("seed_policies failed: %s", result.stderr.strip())
            return jsonify({
                "status": "error",
                "stderr": result.stderr.strip(),
                "stdout": result.stdout.strip(),
            }), 500

        # Count "  SET " lines as a proxy for the number of policies written
        policies_written = result.stdout.count("  SET ")
        _ctrl_log.info("seed_policies: %d policies written", policies_written)

        # Seeding is a manual reset action — clear the decisions log so the
        # feed doesn't mix pre-seed agent activity with the new baseline state.
        decisions_cleared = False
        try:
            dp = _decisions_path()
            if dp.exists():
                dp.unlink()
                decisions_cleared = True
                _ctrl_log.info("seed_policies: cleared decisions.jsonl")
        except Exception as exc:
            _ctrl_log.warning("seed_policies: could not clear decisions.jsonl: %s", exc)

        return jsonify({
            "status": "seeded",
            "policies_written": policies_written,
            "decisions_cleared": decisions_cleared,
            "output": result.stdout.strip(),
        })
    except subprocess.TimeoutExpired:
        return jsonify({"status": "error", "message": "seed_policies timed out after 15s"}), 500
    except Exception as exc:
        _ctrl_log.error("seed_policies exception: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500


# ── Internal helpers ──────────────────────────────────────────────────────────

def _count_recent_decisions() -> int:
    """Count decisions written in the last 24 hours."""
    cutoff = (time.time() - 86400) * 1000
    total = 0
    try:
        path = DECISIONS_PATH if DECISIONS_PATH.is_absolute() else Path.cwd() / DECISIONS_PATH
        if not path.exists():
            path = Path(__file__).parent / "decisions.jsonl"
        if path.exists():
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        tick = json.loads(line)
                        if tick.get("timestamp", 0) >= cutoff:
                            total += len(tick.get("decisions", []))
                    except json.JSONDecodeError:
                        continue
    except Exception:
        pass
    return total


# ── Model metrics helpers ─────────────────────────────────────────────────────

_ALL_KEYS = [f"{r}/{t}" for r in ("us", "eu", "asia") for t in ("free", "premium", "internal")]
# Prefer us/free for the chart since demo scenarios target it most.
_CHART_PRIORITY = ["us/free", "eu/free", "asia/free", "us/premium", "eu/premium", "asia/premium"]


def _parse_recent_ticks(n: int = 40) -> list[dict]:
    """Return the last n complete ticks from decisions.jsonl, oldest-first."""
    try:
        path = DECISIONS_PATH if DECISIONS_PATH.is_absolute() else Path.cwd() / DECISIONS_PATH
        if not path.exists():
            path = Path(__file__).parent / "decisions.jsonl"
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
        ticks = []
        for line in lines[-n:]:
            line = line.strip()
            if line:
                try:
                    ticks.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return ticks
    except Exception:
        return []


def _pick_chart_key(ticks: list[dict]) -> str:
    """Return the region/tier key with the most non-null predictions in the window."""
    counts: dict[str, int] = {k: 0 for k in _ALL_KEYS}
    for tick in ticks:
        for key in _ALL_KEYS:
            p = tick.get("predictions", {}).get(key)
            if p and isinstance(p, dict) and p.get("point") is not None:
                counts[key] += 1
    # Prefer the priority order; fall back to highest count.
    for k in _CHART_PRIORITY:
        if counts.get(k, 0) > 0:
            return k
    return max(counts, key=lambda k: counts[k]) if counts else "us/free"


@app.route("/api/model/metrics")
def api_model_metrics():
    """
    Returns rolling ML model performance metrics derived from decisions.jsonl.
    No loop.py changes needed — all prediction data is already logged there.
    """
    agent_running = _is_running("agent")
    predictor     = _agent_config.get("predictor", "ewma")
    interval      = _agent_config.get("interval", 15)
    training_window = 40 if predictor == "holtwinters" else 8

    if not agent_running:
        return jsonify({
            "running":   False,
            "predictor": predictor,
            "metrics":   None,
        })

    ticks = _parse_recent_ticks(n=40)
    if not ticks:
        return jsonify({"running": True, "predictor": predictor, "warmup": True, "metrics": None})

    chart_key = _pick_chart_key(ticks)
    last_tick  = ticks[-1]
    tick_num   = last_tick.get("tick", 0)

    # ── Warmup detection ─────────────────────────────────────────────────────
    has_any_prediction = any(
        (p := tick.get("predictions", {}).get(chart_key)) and isinstance(p, dict) and p.get("point") is not None
        for tick in ticks[-training_window:]
    )
    warmup = not has_any_prediction or tick_num < training_window

    # ── Prediction history for chart (last 20 ticks, chart_key only) ─────────
    history = []
    for t in ticks[-20:]:
        pred_raw = t.get("predictions", {}).get(chart_key)
        actual   = t.get("observed_features", {}).get(chart_key, {}).get("rps", 0.0)
        pred_pt  = None
        if pred_raw and isinstance(pred_raw, dict):
            pred_pt = pred_raw.get("point")
        history.append({
            "tick":      t.get("tick", 0),
            "ts":        t.get("timestamp", 0),
            "predicted": round(pred_pt, 3) if pred_pt is not None else None,
            "actual":    round(actual, 3),
        })

    # ── Rolling MAE across ALL keys where predictions exist ───────────────────
    errors: list[float] = []
    for t in ticks[-20:]:
        for key in _ALL_KEYS:
            p = t.get("predictions", {}).get(key)
            if p and isinstance(p, dict) and (pt := p.get("point")) is not None:
                act = t.get("observed_features", {}).get(key, {}).get("rps", 0.0)
                errors.append(abs(pt - act))
    mae = round(sum(errors) / len(errors), 4) if errors else None

    # ── Last prediction + CIs ─────────────────────────────────────────────────
    last_pred_raw = last_tick.get("predictions", {}).get(chart_key)
    last_pred_pt = last_lower = last_upper = None
    if last_pred_raw and isinstance(last_pred_raw, dict):
        last_pred_pt = last_pred_raw.get("point")
        last_lower   = last_pred_raw.get("lower")
        last_upper   = last_pred_raw.get("upper")

    last_actual = last_tick.get("observed_features", {}).get(chart_key, {}).get("rps", 0.0)

    def _r(v: float | None) -> float | None:
        return round(v, 3) if v is not None else None

    return jsonify({
        "running":   True,
        "predictor": predictor,
        "warmup":    warmup,
        "chart_key": chart_key,
        "metrics": {
            "mae":              mae,
            "last_prediction":  _r(last_pred_pt),
            "last_actual":      _r(last_actual),
            "confidence_lower": _r(last_lower),
            "confidence_upper": _r(last_upper),
            "samples_processed": tick_num,
            "training_window":  training_window,
            "last_updated":     last_tick.get("timestamp", 0),
            "prediction_history": history,
        },
        "config": {
            "alpha":            0.3 if predictor == "ewma" else None,
            "seasonal_period":  20  if predictor == "holtwinters" else None,
            "interval_seconds": interval,
        },
    })


@app.route("/api/policies/current")
def api_policies_current():
    """
    Return all active policy:{region}:{tier} keys from every Redis instance.
    Uses the existing Python redis client — no subprocess needed.
    """
    TIERS = ("free", "premium", "internal")
    policies: dict[str, dict] = {}

    for region in ("us", "eu", "asia"):
        def fetch(client, r=region):
            result = {}
            for tier in TIERS:
                key = f"policy:{r}:{tier}"
                raw = client.get(key)
                if raw:
                    try:
                        ttl = client.ttl(key)
                        result[tier] = {
                            "policy": json.loads(raw),
                            "ttl": ttl if ttl >= 0 else -1,
                        }
                    except json.JSONDecodeError:
                        result[tier] = {"error": "Invalid JSON", "ttl": -1}
                else:
                    result[tier] = None
            return result

        policies[region] = _safe_redis(region, fetch) or {t: None for t in TIERS}

    return jsonify({
        "status": "success",
        "policies": policies,
        "ts": int(time.time() * 1000),
    })


@app.route("/api/control/redis/flush", methods=["POST"])
def api_control_redis_flush():
    """FLUSHALL on every regional Redis instance via the Redis client."""
    _ctrl_log.info("FLUSH all Redis instances")
    results = {}
    errors  = []
    for region in ("us", "eu", "asia"):
        name = f"redis-{region}"
        try:
            _redis_client(region).flushall()
            results[name] = "ok"
        except Exception as exc:
            results[name] = str(exc)
            errors.append(f"{name}: {exc}")

    if errors:
        _ctrl_log.error("Redis flush partial failure: %s", errors)
        return jsonify({"status": "partial", "results": results, "errors": errors}), 500

    _ctrl_log.info("Redis flush complete: %s", results)
    return jsonify({"status": "flushed", "message": "All Redis data cleared", "results": results})


@app.route("/api/control/gateways/restart", methods=["POST"])
def api_control_gateways_restart():
    """Restart all three gateway containers via docker compose."""
    _ctrl_log.info("RESTART gateways")
    try:
        result = subprocess.run(
            ["docker", "restart", "gateway-us", "gateway-eu", "gateway-asia"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            _ctrl_log.error("gateway restart failed: %s", result.stderr.strip())
            return jsonify({"status": "error", "message": result.stderr.strip() or "docker compose restart failed"}), 500

        _ctrl_log.info("gateways restarted")
        return jsonify({"status": "restarted", "message": "All gateways restarted"})
    except subprocess.TimeoutExpired:
        return jsonify({"status": "error", "message": "Timed out after 60s"}), 500
    except Exception as exc:
        _ctrl_log.error("gateway restart exception: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/user-latency")
def api_user_latency():
    """
    Tier-congestion latency for the noisy_neighbor demo.

    Reads rl_tier_congestion{region,tier} from Prometheus.
    Implied latency = max(0, congestion_count - THRESHOLD) ms, capped at 300ms.
    This matches the gateway formula exactly so the numbers are consistent.
    """
    THRESHOLD = 250   # must match handler.go tier congestion threshold
    MAX_DELAY = 300   # must match handler.go maxDelayMs

    region = request.args.get("region", "us")
    tier   = request.args.get("tier",   "free")

    # Read the live Redis hot-key for the current 30s window — same formula as handler.go.
    # The Prometheus gauge is NOT used for the current value because it never resets to 0
    # when traffic stops (gauges hold their last written value indefinitely).
    win_id = int(time.time()) // 30
    hot_key = f"rl:tier_hot:{region}:{tier}:{win_id}"
    try:
        rdb = _safe_redis(region)
        raw = rdb.get(hot_key) if rdb else None
        congestion = float(raw) if raw else 0.0
    except Exception:
        # Fall back to Prometheus if Redis is unavailable
        congestion = _prom_scalar(
            f'rl_tier_congestion{{region="{region}",tier="{tier}"}}'
        )
    implied_ms = max(0.0, min(congestion - THRESHOLD, MAX_DELAY))

    # Sparkline history of implied latency
    raw_spark = _prom_range(
        f'clamp_max(clamp_min(rl_tier_congestion{{region="{region}",tier="{tier}"}} - {THRESHOLD}, 0), {MAX_DELAY})',
        step="3s",
    )

    # Health thresholds
    healthy   = implied_ms < 20
    degraded  = implied_ms >= 100

    # Abusers = users with active overrides in this region
    abusers: list[dict] = []
    try:
        rdb = _safe_redis(region)
        if rdb:
            for key in rdb.scan_iter("override:*"):
                uid = key.split(":", 1)[1]
                ttl = rdb.ttl(key)
                if ttl <= 0:
                    continue
                raw = rdb.get(key)
                if raw:
                    try:
                        data = json.loads(raw)
                        abusers.append({
                            "user_id": uid,
                            "limit_per_minute": data.get("limit_per_minute"),
                            "ttl": ttl,
                            "reason": data.get("reason", ""),
                        })
                    except Exception:
                        pass
    except Exception:
        pass

    return jsonify({
        "region":              region,
        "tier":                tier,
        "congestion_count":    round(congestion),
        "congestion_threshold": THRESHOLD,  # requests per 30s window
        "tier_latency_ms":     round(implied_ms, 1),
        "tier_latency_spark":  raw_spark,
        "healthy":             healthy,
        "degraded":            degraded,
        "abusers":             abusers,
    })


if __name__ == "__main__":
    port = int(os.getenv("API_PORT", "5001"))
    print(f"Dashboard API starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
