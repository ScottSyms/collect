# Docker & container health checks

Three independent, but consistent, ways to know whether a collector
(`collect-socket`, `collect-kafka`, `collect-file`, `collect-aisstream`) is
healthy — full flag reference in [CLI_REFERENCE.md](CLI_REFERENCE.md#health-signals).
The batch parsers (`ais-parse`, `aisstream-parse`) are one-shot jobs, not
long-running services, so none of this applies to them — their health
signal is just their [exit code](CLI_REFERENCE.md#exit-codes) when the run
finishes.

## 1. Process exit code (authoritative)

A collector that gives up exits non-zero with a code that says why:

| Code | Meaning |
|---|---|
| `3` | Upstream connection retry window exhausted (`--max-reconnect-seconds`) |
| `4` | No rows ingested within the drought window despite a healthy connection (`--data-drought-seconds`) |
| `1` | Any other unclassified error (bad config, etc.) |

This is what a restart policy (Nomad `restart {}`, Docker's own
`restart: unless-stopped`) actually acts on. The other two mechanisms below
are for observing a still-*running* process.

## 2. `--health-check` (exec-based, for Docker `HEALTHCHECK`)

```bash
./target/release/collect-socket --health-check
```

Reads a status file the running process refreshes every second
(`/tmp/collect-<binary>.health`, content `<status>:<unix-timestamp>:<reason>`)
and exits `0`/prints `HEALTHY` if the status is `healthy` and the timestamp
is under 60 seconds old; otherwise exits `1` and prints the reason (e.g.
`UNHEALTHY (no rows ingested for 320s (limit 300s))`).

```bash
docker run -d \
  --name data-ingest \
  --entrypoint /usr/local/bin/collect-file \
  -v $(pwd)/input:/input:ro -v $(pwd)/output:/data \
  --health-cmd "/usr/local/bin/collect-file --health-check" \
  --health-interval 30s --health-timeout 10s --health-retries 3 \
  collect --input /input/data.txt --source mydata
```

The same pattern applies to any of the four collectors — swap the binary
name in both `--entrypoint` and `--health-cmd`, and the trailing args for
that binary's own (see [CLI_REFERENCE.md](CLI_REFERENCE.md)):

```bash
# collect-socket
--entrypoint /usr/local/bin/collect-socket
--health-cmd "/usr/local/bin/collect-socket --health-check"
... collect --tcp-host 153.44.253.27 --tcp-port 5631 --source norway-tcp

# collect-kafka
--entrypoint /usr/local/bin/collect-kafka
--health-cmd "/usr/local/bin/collect-kafka --health-check"
... collect --kafka-brokers broker:9092 --kafka-topic ais-raw --kafka-group-id collect

# collect-aisstream
--entrypoint /usr/local/bin/collect-aisstream
--health-cmd "/usr/local/bin/collect-aisstream --health-check"
... collect --api-key $AISSTREAM_API_KEY --bounding-boxes '[[[-90,-180],[90,180]]]'
```

## 3. `GET /healthz` (HTTP, for Nomad/Kubernetes checks)

Opt-in via `--metrics-addr 0.0.0.0:9184` (or any address). Returns `200
healthy` or `503 unhealthy: <reason>`, the same reason text as (2), plus
`GET /metrics` for Prometheus scraping:

```bash
cargo run -p collect-socket -- --tcp-host host --tcp-port 5631 --metrics-addr 0.0.0.0:9184
curl localhost:9184/healthz
curl localhost:9184/metrics
```

A Nomad HTTP check:

```hcl
check {
  type     = "http"
  path     = "/healthz"
  port     = "metrics"
  interval = "10s"
  timeout  = "2s"
}
```

## What "unhealthy" actually means

All three surfaces above are computed from the same state, so they always
agree, whether the process has already exited or is still running with a
problem:

1. **Heartbeat stale** — the ingest loop itself is stuck or gone. This is
   the only thing the pre-existing (pre-drought-detection) health check
   ever looked at.
2. **Data drought** — the loop is ticking fine, but no row has arrived
   within `--data-drought-seconds` (default 300s, `0` disables it). Once
   confirmed, the process itself exits `4` — see (1) above — so this
   reason is really only observable in the brief window before that exit,
   or if you've raised the window past what the process's own drought
   check uses (you haven't; they read the same value).

## Troubleshooting

```bash
# Current health-file contents
docker exec data-ingest cat /tmp/collect-socket.health   # or collect-<binary>.health

# Container health check history (exec-based)
docker inspect data-ingest | jq '.State.Health.Log'

# Why did it exit?
docker inspect data-ingest --format='{{.State.ExitCode}}'
docker-compose logs -f data-ingest   # --log-format text is easier to read here;
                                      # --log-format json if you're piping into a log pipeline
```

Common causes, by exit code: `3` (upstream unreachable — check host/port/
broker reachability, credentials), `4` (data drought — check the actual
upstream is producing data; a filter like `--bounding-boxes`/
`--filter-mmsi` too narrow is a common cause on `collect-aisstream`), `1`
(check the log line right before exit — bad config is the usual cause).
