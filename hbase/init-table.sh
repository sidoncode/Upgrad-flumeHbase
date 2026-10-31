#!/usr/bin/env bash
# Creates the target table for the Flume HBase sink. Safe to run repeatedly.
# The sink never creates tables or column families itself, so this must exist
# before the Flume agent starts.
#
# Just calling "create" and reading the outcome is more reliable than checking
# "exists" first: right after a restart HBase answers health checks before it
# has loaded its tables, so an early "exists" can wrongly say no.
set -uo pipefail

TABLE="${HBASE_TABLE:-upgrad}"
FAMILY="${HBASE_FAMILY:-cf}"
ATTEMPTS=30

for attempt in $(seq 1 "${ATTEMPTS}"); do
  if out=$(echo "create '${TABLE}', '${FAMILY}'" | hbase shell -n 2>&1); then
    echo "Created table '${TABLE}' with column family '${FAMILY}'."
    exit 0
  fi
  if grep -q "already exists" <<<"${out}"; then
    echo "Table '${TABLE}' already exists, nothing to do."
    exit 0
  fi
  echo "HBase not ready yet (attempt ${attempt}/${ATTEMPTS}), retrying in 3s..."
  sleep 3
done

echo "Could not create table '${TABLE}':"
echo "${out}"
exit 1
