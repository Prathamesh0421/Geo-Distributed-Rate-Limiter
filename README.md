# Geo-Distributed Rate Limiter

A production-grade, geo-distributed rate limiting system with AI-driven traffic shaping. Three regional API gateways (US, EU, Asia) enforce per-tier rate limits using token-bucket and sliding-window algorithms. A CRDT-based sync service propagates counters across regions, a traffic simulator generates realistic load patterns, and an AI agent autonomously adjusts policies in real time based on EWMA/Holt-Winters traffic prediction.

---

## Team Members

| Name | GitHub | Contributions |
|------|--------|---------------|
| Prathamesh Sawant | [@prathamesh0421](https://github.com/prathamesh0421) | Project bootstrap, Go gateway core, token-bucket limiter, Docker Compose & deployment, dev tooling |
| Nikhil Raj | [@Nikhil1169](https://github.com/Nikhil1169) | AI agent service — EWMA/Holt-Winters predictor, spike detector, autonomous decider, policy writer, FastAPI control surface |
| Yashashav DK | [@yashashav-dk](https://github.com/yashashav-dk) | Cross-region sync service, distributed counter, Lua merge script, real-time monitoring dashboard, integration tests |
| Atharva Mokashi | [@Atharva31](https://github.com/Atharva31) | Traffic simulator, sliding-window limiter, gateway policy store & HTTP handler, Prometheus + Grafana observability |

---

## Architecture

```
                         ┌─────────────────────────────────┐
                         │        nginx :8080              │
                         │   (serves dashboard.html)       │
                         └────────────┬────────────────────┘
                                      │ /api/*
                         ┌────────────▼────────────────────┐
                         │       API Service :5001         │
                         │   (agent control surface)       │
                         └──┬──────────┬──────────┬────────┘
                            │          │          │
              ┌─────────────▼┐  ┌──────▼──────┐  ┌▼─────────────┐
              │ Gateway US   │  │ Gateway EU  │  │ Gateway Asia │
              │    :8081     │  │    :8082    │  │    :8083     │
              └──────┬───────┘  └──────┬──────┘  └──────┬───────┘
                     │                 │                 │
              ┌──────▼───────┐  ┌──────▼──────┐  ┌──────▼───────┐
              │  Redis US    │  │  Redis EU   │  │ Redis Asia   │
              │    :6379     │  │    :6380    │  │    :6381     │
              └──────┬───────┘  └──────┬──────┘  └──────┬───────┘
                     └─────────────────┼─────────────────┘
                                       │ CRDT sync
                         ┌─────────────▼────────────────────┐
                         │     Sync Service (×3 regions)    │
                         └──────────────────────────────────┘
                                       │ metrics
                         ┌─────────────▼────────────────────┐
                         │   Prometheus :9090 / Grafana :3000│
                         └──────────────────────────────────┘
```

### Key Design Decisions

- **Token-bucket + sliding-window** algorithms implemented atomically in Lua (Redis scripts) to prevent race conditions.
- **CRDT G-Counter** for cross-region counter merging — eventually consistent, no locking needed.
- **AI Agent** uses Holt-Winters exponential smoothing to forecast traffic and tighten/relax policies before spikes occur.
- **Per-tier policies** (free / premium / internal) stored as JSON in Redis, hot-reloaded by gateways on every request.

---

## Features

- **Multi-region rate limiting** — independent token-bucket and sliding-window limiters per region and user tier
- **Cross-region counter sync** — CRDT-based merge ensures global quotas are respected without distributed locking
- **AI traffic agent** — autonomous spike detection and policy adjustment using EWMA & Holt-Winters forecasting
- **Real-time dashboard** — single-page UI showing live request rates, tier breakdowns, sync lag, and agent decisions
- **Traffic simulator** — generates free/premium/internal traffic with configurable burst, noisy-neighbor, and steady-state scenarios
- **Observability** — Prometheus metrics + pre-built Grafana dashboard for all key signals
- **Override API** — per-user rate limit overrides with TTL for support/ops use cases

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Docker Compose v2 | Run all services |
| Go | 1.22+ | Build the gateway |
| Python | 3.11+ | Agent, sync service, simulator |
| `redis-cli` | any | Smoke checks (`brew install redis`) |

---

## Quick Start (Docker — Recommended)

```bash
# 1. Clone the repo
git clone https://github.com/Prathamesh0421/Geo-Distributed-Rate-Limiter.git
cd Geo-Distributed-Rate-Limiter

# 2. Copy environment config
cp .env.example .env

# 3. Build and start all services
docker compose -f docker-compose.production.yml up --build -d

# 4. Verify everything is healthy
docker compose -f docker-compose.production.yml ps

# 5. Seed demo rate-limit policies
curl -X POST http://localhost:5001/api/control/policies/seed

# 6. Open the dashboard
open http://localhost:8080
```

> **Tear down:** `docker compose -f docker-compose.production.yml down`

---

## Quick Start (Local Development)

```bash
# 1. Start Redis instances + Prometheus + Grafana
docker compose up -d

# 2. Start the US gateway
cd gateway && go mod tidy && go run . &   # :8081
# Repeat with REGION=eu / REGION=asia for other regions

# 3. Install Python dependencies (each service has its own venv)
python -m venv .venv && source .venv/bin/activate
pip install -r agent/requirements.txt     # covers agent + sync + simulator

# 4. Start the sync service
python sync/main.py &

# 5. Start the AI agent API
python agent/main.py &

# 6. Run the simulator (steady-state scenario)
python simulator/main.py scenario steady_state
```

---

## Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| `dashboard` (nginx) | 8080 | Real-time monitoring UI |
| `api` | 5001 | Agent control surface (REST) |
| `gateway-us` | 8081 | Rate-limiting gateway — US region |
| `gateway-eu` | 8082 | Rate-limiting gateway — EU region |
| `gateway-asia` | 8083 | Rate-limiting gateway — Asia region |
| `redis-us` | 6379 | Counter store — US |
| `redis-eu` | 6380 | Counter store — EU |
| `redis-asia` | 6381 | Counter store — Asia |
| `prometheus` | 9090 | Metrics scraping |
| `grafana` | 3000 | Dashboards (login: `admin` / `admin`) |

---

## Repository Structure

```
.
├── gateway/                    # Go API gateway (Gin)
│   ├── main.go                 # Entry point, router setup
│   ├── internal/
│   │   ├── limiter/            # Token-bucket & sliding-window (Go + Lua)
│   │   ├── policy/             # Policy store — reads/writes Redis JSON
│   │   ├── override/           # Per-user override cache
│   │   ├── handler/            # HTTP handler — /check, /health, /metrics
│   │   └── metrics/            # Prometheus metric definitions
│   ├── Dockerfile
│   └── go.mod
│
├── sync/                       # Python CRDT sync service
│   ├── sync_service.py         # Main sync loop — merges G-Counters across regions
│   ├── counter.py              # G-Counter CRDT implementation
│   ├── state.py                # Shared state management
│   ├── admin.py                # Admin REST endpoints
│   ├── merge.lua               # Atomic Redis merge script
│   └── tests/
│       ├── test_counter.py     # Unit tests for CRDT logic
│       └── test_integration.py # End-to-end sync tests
│
├── agent/                      # Python AI traffic agent
│   ├── loop.py                 # Main autonomous control loop
│   ├── predictor.py            # EWMA + Holt-Winters traffic forecasting
│   ├── detector.py             # Spike / anomaly detection
│   ├── decider.py              # Policy decision engine
│   ├── policy_writer.py        # Writes decisions back to Redis
│   ├── metrics_client.py       # Prometheus query client
│   ├── api.py                  # FastAPI control surface
│   ├── decision_log.py         # Structured decision logging
│   ├── notebooks/
│   │   └── predictor_eval.ipynb  # Holt-Winters vs EWMA evaluation
│   └── tests/
│       ├── test_predictor.py
│       ├── test_detector.py
│       ├── test_decider.py
│       └── test_policy_writer.py
│
├── simulator/                  # Python traffic simulator
│   ├── engine.py               # Core simulation engine
│   ├── population.py           # User population model (free/premium/internal)
│   ├── scenarios.py            # Scenario definitions (steady, spike, noisy-neighbor)
│   ├── stats.py                # Stats collection and reporting
│   └── tests/
│       ├── test_patterns.py
│       └── test_population.py
│
├── infra/
│   ├── prometheus.yml          # Prometheus scrape config
│   └── grafana/provisioning/   # Pre-built Grafana dashboards & datasources
│
├── docs/
│   ├── contracts.md            # API, Redis, metrics & policy contracts
│   ├── sync-design.md          # CRDT sync protocol design
│   ├── failure-modes.md        # Failure analysis & recovery
│   ├── demo-prep.md            # Demo scenario runbook
│   └── phase7/architecture.md  # AI agent architecture
│
├── tools/
│   ├── seed_policies.py        # Seed demo rate-limit policies into Redis
│   └── diagnose_ratelimit.py   # Diagnostic script for live debugging
│
├── dashboard.html              # Single-file real-time dashboard UI
├── config.yaml                 # Central config (overridable via env vars)
├── docker-compose.yml          # Development compose (infra only)
├── docker-compose.production.yml  # Full production compose (all services)
├── nginx.conf                  # nginx reverse proxy config
└── deployment-guide.md         # Detailed deployment instructions
```

---

## API Reference

### Gateway — `POST /check`

```bash
curl -X POST http://localhost:8081/check \
  -H "Content-Type: application/json" \
  -d '{"user_id": "u123", "tier": "free", "region": "us", "endpoint": "/api/data"}'
```

**Response:**
```json
{
  "allowed": true,
  "remaining": 42,
  "limit": 60,
  "retry_after_ms": 0,
  "policy_id": "pol_1746000000_1"
}
```

### Agent Control — `POST /api/control/policies/seed`

Seeds all regions with default demo policies.

```bash
curl -X POST http://localhost:5001/api/control/policies/seed
```

### Override a User's Limit

```bash
curl -X POST http://localhost:5001/api/control/override \
  -H "Content-Type: application/json" \
  -d '{"user_id": "u123", "limit_per_minute": 1000, "ttl": 3600, "reason": "VIP user"}'
```

Full API contract details in [docs/contracts.md](docs/contracts.md).

---

## Running Tests

Each service has its own test suite using `pytest`.

```bash
# Activate virtualenv first
source .venv/bin/activate

# Agent tests (predictor, detector, decider, policy writer)
pytest agent/tests/ -v

# Sync service tests (unit + integration)
pytest sync/tests/ -v

# Simulator tests (population, traffic patterns)
pytest simulator/tests/ -v

# Gateway tests (Go)
cd gateway && go test ./... -v
```

All Python tests use `fakeredis` — no live Redis needed.

---

## Simulator Scenarios

```bash
# Steady-state traffic
python simulator/main.py scenario steady_state

# Sudden spike (tests burst handling)
python simulator/main.py scenario spike

# Noisy-neighbor (one user floods, others should be protected)
python simulator/main.py scenario noisy_neighbor

# Global quota bypass attempt
python simulator/main.py scenario global_quota
```

---

## Configuration

All settings are in [`config.yaml`](config.yaml) and can be overridden with environment variables. Copy [`.env.example`](.env.example) to `.env` before starting:

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `development` | `development` or `production` |
| `REDIS_US_HOST` | `localhost` | Redis host for US region |
| `GATEWAY_US_URL` | `http://localhost:8081` | US gateway URL |
| `API_PORT` | `5001` | Agent API port |
| `DASHBOARD_API_URL` | `http://localhost:5001` | URL the dashboard polls |

---

## Observability

- **Grafana dashboard** — `http://localhost:3000` (admin/admin) — pre-provisioned with request rates, tier breakdowns, sync lag, and AI agent decisions.
- **Prometheus** — `http://localhost:9090` — raw metrics.
- **Key metrics:**
  - `rl_requests_total{region, tier, decision}` — allow/deny counters
  - `rl_counter_value{region, tier, user_id}` — live token-bucket values
  - `rl_sync_lag_seconds{from_region, to_region}` — cross-region sync lag
  - `rl_policy_version{region, tier}` — policy change tracking

---

## Deployment

See [deployment-guide.md](deployment-guide.md) for full production deployment instructions including multi-node setup, env var reference, and health check procedures.
