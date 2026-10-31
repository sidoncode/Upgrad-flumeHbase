#!/usr/bin/env bash
# Appends ONE hand-made event to the log file Flume is tailing.
#
#   ./scripts/send-mock-event.sh              # row key MOCK-<unix time>
#   ./scripts/send-mock-event.sh MOCK-hello   # choose your own row key
#
# The line uses the same nine pipe-delimited fields as the generator:
#   rowkey|timestamp|ip|user|method|path|status|bytes|latency_ms
set -euo pipefail
cd "$(dirname "$0")/.."

KEY="${1:-MOCK-$(date +%s)}"
case "${KEY}" in *"|"* | *"'"*) echo "Row key must not contain | or '"; exit 1;; esac

LINE="${KEY}|$(date -u +%Y-%m-%dT%H:%M:%S.000Z)|203.0.113.7|tester|GET|/mock/hello|200|123|42"

# Reuse the generator image: it mounts the same volume Flume reads from.
docker compose run --rm --no-deps -T generator \
  sh -c "echo '${LINE}' >> /data/logs/access.log"

echo "Sent:   ${LINE}"
echo "Verify: echo \"get 'upgrad', '${KEY}'\" | docker compose exec -T hbase hbase shell -n"
