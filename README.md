# Flume to HBase: a hands-on tutorial
# Whiteboard : https://miro.com/app/board/uXjVETJY4mk=/?share_link_id=416966899554

Stream log events through Apache Flume into an Apache HBase table, using Docker. You will run the pipeline, read every moving part, break it on purpose, and watch it recover.

Time: about 90 minutes. Everything runs locally; nothing to install except Docker.

```
 generator            shared volume            Flume agent                          HBase
+---------+  append   +-------------+  tail   +--------+   +---------+   +------+   +---------------+
| python  | --------> | access.log  | ------> | TAILDIR| ->| channel | ->| HBase| ->| table         |
| script  |  5 / sec  |             |         | source |   | (buffer)|   | sink |   | upgrad   |
+---------+           +-------------+         +--------+   +---------+   +------+   +---------------+
```

## What you will learn

- The three Flume building blocks (source, channel, sink) and how events move between them.
- How the Flume HBase sink turns a text line into an HBase row with a regex serializer.
- How HBase organises data: tables, row keys, column families, columns.
- What happens when HBase goes down, and why the choice of channel decides whether you lose data.

## Prerequisites

- Docker Desktop (or Docker Engine) with Compose v2: `docker compose version` should print a version.
- About 3 GB free disk and 4 GB RAM for Docker.
- Free host ports 16011 (HBase UI) and 34545 (Flume metrics). Change them in `.env` if needed.

## Concepts in five minutes

| Term | Meaning | In this project |
|---|---|---|
| Flume agent | One JVM process that moves events | the `flume` container |
| Source | Where events come in | `TAILDIR`: follows `/data/logs/access.log` |
| Channel | Buffer between source and sink; this is where durability is decided | `memory` (later `file`) |
| Sink | Where events go out | `hbase2`: writes rows to HBase |
| Event | Headers plus a byte body | one log line |
| HBase table | Sorted map of rows | `upgrad` |
| Row key | Unique, sorted identifier of a row. HBase can only look up and range-scan efficiently by this | `<epoch_ms>-<seq>` |
| Column family | Group of columns stored together; fixed when the table is created | `cf` |
| Column (qualifier) | Created freely per row, inside a family | `cf:ip`, `cf:status`, ... |

## Project layout

```
flume-hbase-tutorial/
  docker-compose.yml        four services: hbase, hbase-init, flume, generator
  .env                      host port numbers
  docker/Dockerfile         one multi-stage file: base (Java + HBase), hbase, flume
  conf/hbase-site.xml       HBase settings, shared by every container
  flume/flume.conf          THE pipeline definition (you will edit this)
  generator/generate.py     writes fake web-access log lines
  hbase/init-table.sh       creates the upgrad table
  scripts/                  metrics.sh, check-gaps.sh, send-mock-event.sh
  Makefile                  shortcuts: make up / count / scan / metrics / gaps / mock
```

The log line format (pipe-delimited, nine fields):

```
1793447399422-000448|2026-10-31T11:49:59.422Z|10.0.1.108|erin|POST|/products/42|200|678|890
row key             |timestamp               |ip        |user|method|path     |status|bytes|latency_ms
```

---

## Lab 1: Start the stack

**WHAT.** Build the images and start all four services.

```bash
docker compose up -d --build
```

The first build downloads HBase (about 300 MB) and Flume, so allow a few minutes. Afterwards:

```bash
docker compose ps -a
```

**WHY.** Compose starts the services in dependency order, and that order is the lesson: Flume must not start until the table exists, because the HBase sink never creates tables or column families.

```
hbase  (healthy) ──> hbase-init (creates table, exits 0) ──> flume
generator starts independently; it only writes a file
```

**OBSERVE.**

- `hbase` shows `(healthy)`, `hbase-init` shows `Exited (0)`, `flume` and `generator` show `Up`.
- `docker compose logs hbase-init` ends with `Created table upgrad`.
- Open the HBase Master UI at http://localhost:16011 and find `upgrad` under User Tables.

If a container exits or a port is busy, jump to [Troubleshooting](#troubleshooting).

---

## Lab 2: Read the pipeline

**WHAT.** Open `flume/flume.conf` and map each block to the diagram at the top.

**WHY.** Flume has no code to write for a pipeline like this. The whole agent is one properties file: components are named, then wired together with `channels` / `channel` properties.

Source:

```properties
agent.sources.logsrc.type         = TAILDIR
agent.sources.logsrc.filegroups.access = /data/logs/access.log
agent.sources.logsrc.positionFile = /var/flume/taildir_position.json
```

`TAILDIR` follows a file like `tail -F`, and records how far it has read in `positionFile`. That file lives on a Docker volume, so a restarted agent resumes instead of re-reading.

Sink:

```properties
agent.sinks.hbasesink.type          = hbase2
agent.sinks.hbasesink.table         = upgrad
agent.sinks.hbasesink.columnFamily  = cf
agent.sinks.hbasesink.batchSize     = 100
agent.sinks.hbasesink.zookeeperQuorum = hbase:2181
```

- `hbase2` is the sink for HBase 2.x. (`hbase` targets HBase 1.x; using it here fails.)
- `batchSize = 100` means up to 100 events per transaction, so one HBase round trip carries many rows.
- Clients never talk to the HBase master directly to find data. They ask ZooKeeper (`hbase:2181`) where the region servers are.

Serializer, which turns text into columns:

```properties
agent.sinks.hbasesink.serializer = org.apache.flume.sink.hbase2.RegexHBase2EventSerializer
agent.sinks.hbasesink.serializer.regex = ^([^|]*)[|]([^|]*)[|]...$
agent.sinks.hbasesink.serializer.colNames = ROW_KEY,ts,ip,user,method,path,status,bytes,latency_ms
agent.sinks.hbasesink.serializer.rowKeyIndex = 0
```

The regex has nine `(...)` groups, `colNames` has nine names, and group 0 (`rowKeyIndex = 0`) becomes the row key. The name at that position must be spelled exactly `ROW_KEY`.

**OBSERVE.** Count the groups in the regex and the names in `colNames`; they must line up one to one. Then change `ROW_KEY` to `rowkey` in `colNames`, run `docker compose restart flume`, and read `docker compose logs flume`. Flume reports the mistake at start-up and drops the sink (see [Troubleshooting](#troubleshooting)). Change it back before moving on.

---

## Lab 3: See the data in HBase

**WHAT.** Query the table with the HBase shell.

```bash
make count
make scan
```

or interactively:

```bash
docker compose exec hbase hbase shell
```

Inside the shell, try each of these:

```ruby
count 'upgrad'

scan 'upgrad', {LIMIT => 2}

# Fetch one row by its key (copy a key from the scan output)
get 'upgrad', '1793447399422-000448'

# Only some columns
scan 'upgrad', {COLUMNS => ['cf:status', 'cf:path'], LIMIT => 5}

# Server-side filter: only server errors
scan 'upgrad', {FILTER => "SingleColumnValueFilter('cf','status',=,'binary:500')", LIMIT => 5}

# Key range: events from one moment onward (row keys sort as text)
scan 'upgrad', {STARTROW => '1793447400000', LIMIT => 3}

exit
```

**WHY.** Each log line became one row, each field one cell. Notice that HBase stores every cell as a separate line (`ROW, COLUMN+CELL`) and that every cell carries a timestamp: HBase is a sparse, versioned, sorted key-value store, not a grid.

```
 1793447399422-000448 column=cf:bytes,      timestamp=..., value=678
 1793447399422-000448 column=cf:ip,         timestamp=..., value=10.0.1.108
 1793447399422-000448 column=cf:latency_ms, timestamp=..., value=890
 ...
```

**OBSERVE.**

- Columns within a row are listed alphabetically, not in the order of the log line.
- `STARTROW` works because row keys are sorted bytes. That is why the row key starts with the epoch time: a time-range query becomes a cheap range scan.
- The filter query is slower in principle than a key lookup: it reads rows and discards them server-side. In HBase, the row key is the index.

---

## Lab 3b: Send a mock event

**WHAT.** Inject one hand-made event and follow it through every stage. Pausing the generator makes your event the only thing arriving, so it is easy to spot.

Step 1. Pause the generator and record the starting numbers.

```bash
docker compose stop generator
make count            # e.g. 351 row(s)
make metrics          # note "events read from file"
```

Step 2. Send one event. The script appends a single line to the log file Flume is tailing:

```bash
make mock KEY=MOCK-hello
```

```
Sent:   MOCK-hello|2026-10-31T13:13:04.000Z|203.0.113.7|tester|GET|/mock/hello|200|123|42
```

(Without `KEY=...` the row key is `MOCK-<unix time>`.) The nine fields are the same as the generator's: `rowkey|timestamp|ip|user|method|path|status|bytes|latency_ms`.

Step 3. Wait a few seconds, then check each stage.

```bash
make metrics          # "read from file" and "written to HBase" both went up by 1
make count            # one more row than in step 1
echo "get 'upgrad', 'MOCK-hello'" | docker compose exec -T hbase hbase shell -n
```

```
 cf:bytes      value=123
 cf:ip         value=203.0.113.7
 cf:latency_ms value=42
 cf:method     value=GET
 cf:path       value=/mock/hello
 cf:status     value=200
 cf:ts         value=2026-10-31T13:13:04.000Z
 cf:user       value=tester
```

The `MOCK-hello` part of your line became the row key, and the other eight fields became columns. Mock rows sort after the generated ones (letters sort after digits), so you can list them with:

```bash
echo "scan 'upgrad', {STARTROW => 'MOCK-'}" | docker compose exec -T hbase hbase shell -n
```

Step 4. Send a malformed event: a line that does not match the regex.

```bash
docker compose run --rm --no-deps -T generator \
  sh -c "echo 'this line is not pipe delimited' >> /data/logs/access.log"
sleep 8
make metrics
make count
```

```
source  events read from file : 355      <- went up by 1
channel backlog               : 0 / 10000
sink    events written to HBase: 354    <- did NOT go up
354 row(s)                              <- no new row
```

Step 5. Prove the pipeline is still healthy, then resume the generator.

```bash
make mock KEY=MOCK-after-bad
sleep 8
make count            # goes up by 1 again
docker compose start generator
```

**WHY.** One line travels: file, then source, channel, sink, serializer, and finally a Put on a row. Doing it with a single event removes the noise, so each counter moves by exactly one and you can see what each stage is responsible for.

**OBSERVE.**

- The malformed line was read by the source and taken from the channel, yet no row appeared and nothing was logged at the default INFO level. The regex serializer simply produces no Put for a line it cannot match. The only trace is that "read from file" is ahead of "written to HBase" with an empty backlog. On a real pipeline that difference is your signal that events are being discarded.
- A bad line does not block the good lines after it (step 5).
- Row key behaviour: send `make mock KEY=MOCK-hello` a second time. The count does not change. Why? (Same key, so HBase overwrites the cells, which is the idempotency idea from Lab 6.)

---

## Lab 4: Watch Flume work

**WHAT.** Read Flume's own counters.

```bash
make metrics
```

Example output (your numbers will differ):

```
source  events read from file : 1520
channel backlog               : 0 / 10000
sink    events written to HBase: 1520
sink    connection failures   : 0
```

Raw JSON: http://localhost:34545/metrics

**WHY.** Flume reports three numbers that tell you where a pipeline is stuck:

| Symptom | Meaning |
|---|---|
| source count rises, sink count flat, backlog grows | the sink is the bottleneck or HBase is down |
| source count flat | nothing new is arriving; check the generator |
| backlog stays near capacity | the source will start failing with `ChannelFullException` |

**OBSERVE.** Run `make metrics` a few times. At 5 events per second the sink counter keeps pace and the backlog hovers near zero, because the sink drains in batches faster than they arrive.

Also tail the agent log: `make logs`.

---

## Lab 5: Take HBase down

**WHAT.** Stop HBase for a minute while the generator keeps writing, then bring it back.

```bash
docker compose stop hbase
sleep 45
make metrics          # backlog grows, sink count stands still
docker compose start hbase
sleep 60
make metrics          # backlog back to 0
make gaps
```

Observed in a test run:

```
HBase stopped 45 s:   channel backlog 222, written 201 (unchanged)
HBase started 60 s:   channel backlog 0,   written 743
rows=842 first=448 last=1289
No gaps: every event between first and last arrived.
```

**WHY.** When the sink cannot write, its transaction rolls back and the events stay in the channel. The source keeps filling the channel, and when HBase returns the sink drains the backlog. That buffering is the entire purpose of a channel.

**OBSERVE.** `docker compose logs flume` shows `ReadOnlyZKClient ... CONNECTIONLOSS` warnings and `Unable to resolve address: hbase:2181` while HBase is down (Docker removes the name when the container stops). Those clear on their own; no restart of Flume is needed.

---

## Lab 6: Lose data, then fix it

**WHAT.** Repeat Lab 5, but restart Flume while HBase is down.

```bash
docker compose stop hbase
sleep 30
make metrics                       # note the backlog, e.g. 147
docker compose restart flume
docker compose start hbase
sleep 70
make gaps
```

Observed with the `memory` channel (backlog was 147 when Flume restarted):

```
rows=1062 first=1 last=1233
GAPS FOUND:
  missing 610..780
```

The events that were only in memory died with the Flume process. The source had already moved its position past them, so they were never re-read. (The gap is a little larger than the backlog because the generator kept writing while Flume was restarting.)

**WHY.** A `memory` channel is fast and simple, but it is volatile. A `file` channel writes every event to disk before acknowledging it. Flume gives at-least-once delivery only with a durable channel.

**Fix.** In `flume/flume.conf` replace the channel block with:

```properties
agent.channels.mem.type          = file
agent.channels.mem.checkpointDir = /var/flume/checkpoint
agent.channels.mem.dataDirs      = /var/flume/data
```

(The channel is still named `mem` so the source and sink lines need no change; rename it everywhere if you prefer.) Then:

```bash
docker compose restart flume
```

and repeat the experiment.

Observed with the `file` channel (backlog was 149 when Flume restarted):

```
rows=987 first=1 last=987
No gaps: every event between first and last arrived.
```

Run `make metrics` after the restart: the sink's written count is higher than the source's read count, because the sink is draining events that were saved on disk before the restart.

**OBSERVE.** At-least-once also means duplicates are possible after a crash. They are harmless here: the same row key is written again with the same values, so HBase simply overwrites the cell. If your row key were random (the serializer default), duplicates would become separate rows. A deterministic row key from the data is what makes the pipeline effectively idempotent.

---

## Lab 7: Design a different row key

**WHAT.** The row key is `<epoch_ms>-<seq>`. Time-ordered keys make time-range scans cheap, but they have a known downside.

Think about it, then discuss:

1. All new writes have increasing keys, so they all land in the same region (the last one). On a real cluster, one RegionServer takes every write. This is called hotspotting. How would you spread writes?
2. You mostly query "all events for one IP". What row key would make that a range scan?
3. What do you lose with your new key?

Common answers: salting (a small hash prefix such as `3|...`), reversing the timestamp, or `<ip>-<epoch_ms>` for per-IP queries (but then time-range queries across all IPs become a full scan).

To try one, change the generator's first field in `generator/generate.py` (`fields = [...]`) and rebuild:

```bash
docker compose up -d --build generator
```

New rows use the new key; old rows stay. Use `make clean && make up` to start fresh.

---

## Challenges

1. **Add a column.** Add a `referrer` field to the generator, regex and `colNames`. Do you need to alter the HBase table? (Hint: families are fixed, columns are not.)
2. **Second source.** Add a second `TAILDIR` file group for `/data/logs/error.log`. Which lines of `flume.conf` change?
3. **Bigger batches.** Raise `RATE` in `docker-compose.yml` to 500 and watch `make metrics`. Tune `batchSize` and `transactionCapacity` until the backlog stays at zero. What is the constraint between them? (Hint: `transactionCapacity` on the channel must be at least the sink's `batchSize`.)
4. **Raw payload.** Add a sink that stores the entire line in one column using `SimpleHBase2EventSerializer`. When is that better than parsing at write time?

---

## Troubleshooting

These are real failures encountered while building this project.

**`Bind for 0.0.0.0:16010 failed: port is already allocated`**
Another process uses the host port. Edit `HBASE_UI_PORT` in `.env`, then `docker compose up -d`.

**`hbase` container exits; log shows `ConnectException: Connection refused` to `hbase:2181`**
The embedded ZooKeeper listened only on loopback. `conf/hbase-site.xml` sets `hbase.zookeeper.property.clientPortAddress` to `0.0.0.0` to prevent this. If you edited that file, restore the property.

**`Sink hbasesink has been removed due to an error during configuration`: `Column at 0 must be ROW_KEY and is rowkey`**
The column name at `rowKeyIndex` must be exactly `ROW_KEY`. Fix `serializer.colNames`.

**Flume starts but nothing reaches HBase, and `docker compose logs flume` shows exceptions about the table or column family**
The sink never creates the table. Check `docker compose logs hbase-init`, and that `table` / `columnFamily` in `flume.conf` match `HBASE_TABLE` / `HBASE_FAMILY` in `hbase/init-table.sh` (defaults `upgrad` / `cf`).

**Class or method errors from the sink at start-up or on the first write**
Check `type = hbase2`. The plain `hbase` sink is written against the HBase 1.x client API and does not match the HBase 2.5 jars used here.

**Everything is stuck after changing the sink config**
`docker compose restart flume` re-reads `flume.conf`. The file is mounted, not baked into the image, so there is no rebuild.

**Start from nothing**
`make clean` removes containers and all three volumes (HBase data, Flume position file, shared log), then `make up`.

---

## Cleanup

```bash
make down      # stop, keep the data
make clean     # stop and delete all data
```

## Versions

| Component | Version |
|---|---|
| Apache Flume | 1.11.0 (`hbase2` sink) |
| Apache HBase | 2.5.16, standalone |
| Java | Temurin 11 |
| Generator | Python 3.12 |

Change `HBASE_VERSION` / `FLUME_VERSION` via build args in `docker/Dockerfile`.

## Notes on scope

This is a learning setup: HBase runs standalone on the container's local disk with its own ZooKeeper, and the Flume agent is a single node. A production deployment would use HDFS, a separate ZooKeeper ensemble, multiple RegionServers, and often an intermediate Kafka or a Flume tier between producers and the HBase-writing agent.
