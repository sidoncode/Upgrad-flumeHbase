"""Appends fake web-access events to a log file, one pipe-delimited line each.

    <rowkey>|<iso timestamp>|<ip>|<user>|<method>|<path>|<status>|<bytes>|<latency_ms>

The first field is an HBase row key: epoch milliseconds plus a sequence number,
so rows sort in arrival order. Flume tails the file and the HBase sink splits
each line into columns with a regex (see flume/flume.conf).
"""
import os
import random
import sys
import time
from datetime import datetime, timezone

LOG_FILE = os.environ.get("LOG_FILE", "/data/logs/access.log")
RATE = float(os.environ.get("RATE", "5"))  # events per second

USERS = ["alice", "bob", "carol", "dave", "erin", "frank", "-"]
PATHS = ["/", "/login", "/products", "/products/42", "/cart", "/checkout", "/api/orders", "/health"]
METHODS = ["GET"] * 8 + ["POST"] * 2
STATUSES = [200] * 14 + [301, 404, 404, 500]


def make_event(seq: int) -> str:
    now = datetime.now(timezone.utc)
    epoch_ms = int(now.timestamp() * 1000)
    fields = [
        f"{epoch_ms}-{seq:06d}",
        now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        f"10.0.{random.randint(0, 3)}.{random.randint(2, 250)}",
        random.choice(USERS),
        random.choice(METHODS),
        random.choice(PATHS),
        str(random.choice(STATUSES)),
        str(random.randint(200, 20000)),
        str(random.randint(3, 900)),
    ]
    return "|".join(fields)


def main() -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    print(f"Writing ~{RATE} events/sec to {LOG_FILE}", flush=True)
    delay = 1.0 / RATE
    seq = 0
    with open(LOG_FILE, "a", buffering=1) as out:  # line-buffered
        while True:
            seq += 1
            out.write(make_event(seq) + "\n")
            if seq % 50 == 0:
                print(f"generated {seq} events", flush=True)
            time.sleep(delay)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
