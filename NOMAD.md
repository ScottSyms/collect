# Nomad Usage

This project includes Nomad job definitions under [nomad/](nomad/) for orchestrating `collect-socket` and other binaries.

## Prerequisites

- Nomad cluster (1.4+ recommended for Nomad Variables)
- `collect-socket` binary deployed to all Nomad client nodes at `/usr/local/bin/collect-socket`
- Output directory (`/data`) writable by the Nomad task user on client nodes

## Quick Start

```bash
nomad job run nomad/collect-socket.nomad
```

This connects to the Norway TCP feed at `153.44.253.27:5631` with source label `norway-tcp` and writes Hive-partitioned Parquet files to `/data`.

## Configuration

### Job Variables

The job file uses Nomad variables with sensible defaults for the Norway feed. Override any of them at submit time with `-var`:

| Variable | Default | Description |
|----------|---------|-------------|
| `tcp_host` | `153.44.253.27` | TCP host address |
| `tcp_port` | `5631` | TCP port number |
| `source` | `norway-tcp` | Logical source label |
| `rust_log` | `INFO` | Log level |
| `max_rows` | `10000` | Max rows per Parquet file |
| `keep_local` | `false` | Keep local files after S3 upload |
| `s3_bucket` | _(empty)_ | S3 bucket name |
| `s3_region` | _(empty)_ | S3 region |
| `s3_endpoint` | _(empty)_ | S3 endpoint URL |
| `s3_access_key` | _(empty)_ | S3 access key |
| `s3_secret_key` | _(empty)_ | S3 secret key |
| `s3_disable_tls` | `false` | Disable TLS for S3 (use HTTP) |

### Example with S3

```bash
nomad job run \
  -var s3_bucket=maritime-data \
  -var s3_region=us-west-2 \
  -var s3_disable_tls=true \
  nomad/collect-socket.nomad
```

## Secret Management

Avoid putting S3 keys in plain-text `-var` flags or the job file. Use **Nomad Variables** to store secrets securely:

```bash
nomad var put nomad/jobs/collect-socket/s3 \
  access_key=AKIAIOSFODNN7EXAMPLE \
  secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Then add a `template` stanza to the job to inject them at runtime:

```hcl
template {
  data        = <<EOH
S3_ACCESS_KEY={{ with nomadVar "nomad/jobs/collect-socket/s3" }}{{ .access_key }}{{ end }}
S3_SECRET_KEY={{ with nomadVar "nomad/jobs/collect-socket/s3" }}{{ .secret_key }}{{ end }}
EOH
  destination = "local/secrets/env"
  env         = true
}
```

## Health Checks & Restart Behaviour

Nomad runs the built-in health check every 30 seconds:

```
collect-socket --health-check
```

The check script touches `/tmp/collect-socket.health` on each successful cycle.

There are **two independent restart mechanisms**:

| Trigger | Mechanism | Limit |
|---------|-----------|-------|
| **Task crash** (process exits) | `restart` block | 10 attempts per 5 min, 15s delay |
| **Hung / unhealthy task** (process alive but check fails) | `check_restart` on the health check | 3 consecutive failures before restart |

This means the task is resilient to both hard crashes and silent hangs.

## Resource Tuning

Default resource limits in the job file:

- **CPU:** 500 MHz
- **Memory:** 1024 MB

Adjust by editing the `resources` block in `nomad/collect-socket.nomad` to match your workload:

```hcl
resources {
  cpu    = 1000
  memory = 2048
}
```

See the [performance tuning](#performance-tuning) section in the main README for guidance on `MAX_ROWS`, `MAX_BATCH_BYTES`, and compression settings.
