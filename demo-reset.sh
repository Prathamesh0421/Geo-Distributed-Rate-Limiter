#!/usr/bin/env bash
# Reset between demo runs: flush Redis + restart gateways only.
# Leaves the api/agent container running so the agent stays warm.
set -euo pipefail

echo "Flushing Redis..."
docker exec redis-us   redis-cli flushall
docker exec redis-eu   redis-cli flushall
docker exec redis-asia redis-cli flushall

echo "Restarting gateways..."
docker-compose restart gateway-us gateway-eu gateway-asia

echo '{"status": "idle"}' > "$(dirname "$0")/agent/demo_results.json"
echo "Done. Agent is still running — no warmup needed."
