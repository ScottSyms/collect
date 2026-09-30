# Tutorial: from a raw feed to Apache Iceberg

This walks through the project in four stages, each one strictly additive to
the last:

1. **Local collection & storage** — get data flowing into Parquet on disk.
2. **Partitioning** — understand how that output is organized (not a flag
   you add, but something already happening).
3. **S3 upload** — send the same output to object storage.
4. **Apache Iceberg** — decode it into typed, queryable tables.

Each stage is one more flag on top of the last command — nothing from an
earlier stage stops working when you add a later one. For the full flag/env
reference behind every example here, see [CLI_REFERENCE.md](CLI_REFERENCE.md).

## Which binary do I run?

Six binaries, two kinds:

| Binary | Kind | Reads from |
|---|---|---|
| [`collect-socket`](COLLECT_SOCKET.md) | collector | a TCP stream of newline-delimited data |
| [`collect-kafka`](COLLECT_KAFKA.md) | collector | a Kafka topic |
| [`collect-file`](COLLECT_FILE.md) | collector | a local file or directory (plain/gzip/bzip2/zip) |
| [`collect-aisstream`](COLLECT_AISSTREAM.md) | collector | the aisstream.io WebSocket API |
| [`ais-parse`](AIS_PARSE.md) | parser | bronze Parquet written by a collector, decodes NMEA/AIVDM |
| [`aisstream-parse`](AISSTREAM_PARSE.md) | parser | bronze Parquet written by `collect-aisstream`, decodes its JSON |

A **collector** is long-running: it ingests a live or file-based source and
writes "bronze" (raw payload + timestamp) Parquet. A **parser** is a batch
job: it reads a collector's bronze output and decodes it into typed
"silver" tables (positions, statics, meteo, binary, aids-to-navigation).
Everything below picks `collect-file` as the running example, since it
needs no live network to try — the box at the end of each stage gives the
equivalent flags for the other three collectors.

## Stage 1 — Local collection & storage

The minimum viable command:

```bash
cargo run -p collect-file -- --input mydata.txt --source mydata
```

This reads `mydata.txt`, and writes Hive-partitioned Parquet under
`./data` (the default `--output-dir`) — nothing else. No S3, no Iceberg, no
network beyond reading the file itself.

> **The other three collectors, same idea:**
> ```bash
> cargo run -p collect-socket -- --tcp-host 1.2.3.4 --tcp-port 5631 --source my-feed
> cargo run -p collect-kafka -- --kafka-brokers broker:9092 --kafka-topic my-topic --kafka-group-id collect
> cargo run -p collect-aisstream -- --api-key "$AISSTREAM_API_KEY" --bounding-boxes '[[[-90,-180],[90,180]]]'
> ```

If your input is raw NMEA/AIVDM sentences and you want multi-part message
reassembly or `$PGHP`/tag-block timestamp correction done as data lands,
add `--consolidate-ais` and/or `--process-timestamps` (available on
`collect-file`, `collect-socket`, and as a batch pass on `ais-parse`).

## Stage 2 — Partitioning (already happening)

There's no flag to turn partitioning on — every collector always writes
Hive-partitioned output; `--partition`/`PARTITION` (default `day`) only
picks the granularity: `minute`, `hour`, `day`, `month`, or `year`. Stage
1's command actually wrote:

```
data/source=mydata/year=2026/month=09/day=27/part-<timestamp>-<seq>.parquet
```

Pick a finer granularity for a high-volume feed you'll want to re-process in
small windows, coarser for a low-volume one you'll mostly query in bulk:

```bash
cargo run -p collect-file -- --input mydata.txt --source mydata --partition hour
```

The batch parsers' `--partition` must match whatever granularity the
collector actually used — it selects which directories to read, not a
transformation.

## Stage 3 — S3 upload

Set `--s3-bucket` (or the `S3_BUCKET` env var) and the same local output is
also uploaded, then deleted locally by default (`--keep-local` to retain
it):

```bash
cargo run -p collect-file -- --input mydata.txt --source mydata \
  --s3-bucket my-bucket --s3-endpoint http://localhost:9000 --s3-disable-tls \
  --s3-access-key minioadmin --s3-secret-key minioadmin
```

`--s3-bucket` accepts `bucket/prefix` shorthand: `--s3-bucket my-bucket/bronze`
uploads under `my-bucket` with every key prefixed `bronze/`. Omit
`--s3-endpoint` for real AWS S3; set it (plus `--s3-disable-tls` if needed)
for MinIO, RustFS, or any other S3-compatible store.

The batch parsers read and write S3 independently on each side, since they
read one dataset and write another: `--input-s3-bucket` / `--output-s3-bucket`
(each repeatable, so you can merge several source buckets in one run) in
place of `--s3-bucket`, using the same `--s3-endpoint`/`--s3-access-key`/
`--s3-secret-key`/`--s3-disable-tls` flags.

## Stage 4 — Apache Iceberg

Iceberg is opt-in via `--iceberg-catalog-uri` (+ required
`--iceberg-warehouse`), independent of S3 upload — you can have either,
both, or neither. There are three ways to get data into Iceberg, in
increasing order of what gets decoded:

1. **Raw registration** — any collector, with just `--iceberg-catalog-uri`
   set and no `--parser`, registers each uploaded bronze file as one row
   (`ts`, `source`, `payload`) in a `raw` Iceberg table. No decoding.
2. **Inline silver decoding** — add `--parser ais` (NMEA/AIVDM) or
   `--parser aisstream` (aisstream.io JSON) to a collector, and each ingested
   row is decoded as it arrives, straight into the six typed silver tables
   (`positions`, `statics`, `meteo`, `binary`, `atons`, `other`).
3. **Batch decoding** — run `ais-parse`/`aisstream-parse` against a
   collector's bronze output (local or S3) to decode it into the same six
   tables after the fact, independent of whether the collector did any
   inline decoding.

```bash
cargo run -p collect-file -- --input mydata.txt --source mydata --parser ais \
  --iceberg-catalog-uri http://localhost:8181/catalog --iceberg-warehouse s3://warehouse
```

Or decode existing bronze output in batch, reading and writing S3, against
a catalog that needs SigV4-signed requests (RustFS's built-in Iceberg REST
catalog, for example, has no separate token auth — it authenticates the
same way S3 itself does):

```bash
S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=root S3_SECRET_KEY=vishnu \
S3_REGION=us-east-1 S3_PATH_STYLE=true \
cargo run -p ais-parse --release -- \
  --input-s3-bucket raw/duplicate \
  --output-dir ./ais-parse-state \
  --iceberg-catalog-uri http://localhost:9000/iceberg --iceberg-warehouse data \
  --iceberg-sigv4
```

`--iceberg-sigv4` signs catalog requests with S3 credentials (service name
`s3`), which is what a catalog exposed directly by an S3-compatible store
(rather than a separate service like Lakekeeper) expects. Note these are
set as **environment variables**, not `--s3-*` flags: Iceberg mode's S3
config (both the signing above and the actual data-file writes a commit
does) is resolved from the environment only, independently of any `--s3-*`
flags you also pass for bronze upload/download — see
[CLI_REFERENCE.md](CLI_REFERENCE.md#iceberg-icebergcliargs-all-binaries)
for the full list, including `S3_PATH_STYLE`, needed here and specific to
this path.

RustFS also requires a one-time step per bucket before it can serve as a
warehouse: enable it as a "table bucket" via RustFS's web console (there's
no documented CLI/API call for it). Skip this for any other catalog
(Lakekeeper, etc.).

`--delete-after-iceberg` (collectors only, requires `--parser` and cannot be
combined with S3 upload) deletes each local bronze file once its silver rows
are committed — use it only if you don't need the raw bronze payloads kept
anywhere once they're decoded.

## What next

- **Keeping Iceberg tables fast**: ingest leaves many small files and
  snapshots. [AIS_COMPACT.md](AIS_COMPACT.md) covers `ais-compact`, which
  merges them into sorted files and cleans up after itself (RustFS's catalog
  has no compactor of its own).
- **Running unattended** (Nomad, containers, restart policies, health
  signals, log format): [CLI_REFERENCE.md](CLI_REFERENCE.md) for the full
  flag surface, [DOCKER_HEALTH_CHECK.md](DOCKER_HEALTH_CHECK.md) for health
  checks, [NOMAD.md](NOMAD.md) for cluster deployment.
- **The decoded schemas**: [AIS_PARSE.md](AIS_PARSE.md) and
  [AISSTREAM_PARSE.md](AISSTREAM_PARSE.md) document every silver table's
  columns.
