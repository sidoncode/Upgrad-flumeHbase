#!/usr/bin/env bash
# One-screen health summary of the Flume agent, read from its JSON metrics.
set -euo pipefail
cd "$(dirname "$0")/.."
PORT="$(grep -E '^FLUME_METRICS_PORT=' .env 2>/dev/null | cut -d= -f2 || true)"

METRICS_JSON="$(curl -sf "http://localhost:${PORT:-34545}/metrics")" python3 - <<'PY'
import json, os

m = json.loads(os.environ["METRICS_JSON"])
src = next(v for k, v in m.items() if k.startswith("SOURCE."))
chan = next(v for k, v in m.items() if k.startswith("CHANNEL."))
sink = next(v for k, v in m.items() if k.startswith("SINK."))

print("source  events read from file :", src["EventReceivedCount"])
print("channel backlog               :", chan["ChannelSize"], "/", chan["ChannelCapacity"])
print("sink    events written to HBase:", sink["EventDrainSuccessCount"])
print("sink    connection failures   :", sink["ConnectionFailedCount"])
PY
