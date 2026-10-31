#!/usr/bin/env bash
# Verifies that no events were lost on the way from the log file to HBase.
#
# The generator numbers every event (the part of the row key after the dash).
# Scan the row keys in HBase, and report any hole in that sequence.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T hbase hbase shell -n <<'HBASE' 2>/dev/null \
  | grep -oE '^ [0-9]+-[0-9]+' \
  | uniq \
  | awk -F- '
      { seq = $2 + 0 }
      NR == 1 { first = seq }
      NR > 1 && seq != prev + 1 { gaps = gaps sprintf("\n  missing %d..%d", prev + 1, seq - 1) }
      { prev = seq; rows = NR }
      END {
        printf "rows=%d first=%d last=%d\n", rows, first, prev
        if (gaps == "") print "No gaps: every event between first and last arrived."
        else            print "GAPS FOUND:" gaps
      }'
scan 'upgrad', {FILTER => 'KeyOnlyFilter()'}
HBASE
