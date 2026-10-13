# ais-compact-consolidate: hourly manifest merge for the silver tables that
# collect-aisstream (and the other live writers) append to.
#
# Every live commit is a fast_append that adds one manifest, and RustFS refuses
# any commit whose snapshot lists more than 10,000 manifests ("snapshot manifest
# count exceeds the commit limit"). At one commit a minute that is about a week
# per table, so this merges them back down well before then. See AIS_COMPACT.md.
#
# Submit with the SAME variable values as collect-aisstream (table_bucket,
# iceberg_namespace, rustfs_endpoint, ...) so it targets the same tables;
# artifact_url / artifact_sha256 point at the ais-compact binary. Secrets come
# from the Nomad Variable nomad/jobs/<job_name> (S3_ACCESS_KEY, S3_SECRET_KEY).

variable "job_name" { type = string }
variable "datacenter" { type = string }
variable "artifact_url" { type = string }
variable "artifact_sha256" { type = string }
variable "build_commit" { type = string }
variable "rustfs_endpoint" { type = string }
variable "rustfs_region" { type = string }
variable "table_bucket" { type = string }
variable "iceberg_namespace" { type = string }
variable "force_nonce" {
  type    = string
  default = ""
}

job "ais-compact-consolidate" {
  name        = var.job_name
  datacenters = [var.datacenter]
  type        = "batch"

  meta {
    build_commit = var.build_commit
    force_nonce  = var.force_nonce
  }

  periodic {
    crons            = ["17 * * * *"]
    prohibit_overlap = true
  }

  group "consolidate" {
    count = 1

    restart {
      attempts = 2
      interval = "30m"
      delay    = "1m"
      mode     = "fail"
    }

    task "ais-compact" {
      driver = "exec"

      artifact {
        source      = var.artifact_url
        destination = "local/ais-compact"
        mode        = "file"
        options {
          checksum = "sha256:${var.artifact_sha256}"
        }
      }

      # Exit 2 means every table was already consolidated: a normal quiet hour,
      # not a failure. Exit 5 (some table failed) still fails the run.
      config {
        command = "/bin/sh"
        args = ["-c", <<-EOT
          chmod 0755 local/ais-compact
          local/ais-compact consolidate --apply --min-manifests 20
          rc=$?
          [ "$rc" -eq 2 ] && exit 0
          exit "$rc"
        EOT
        ]
      }

      template {
        data        = <<-EOT
          {{- with nomadVar "nomad/jobs/${var.job_name}" -}}
          {{- range $k, $v := . }}
          {{ $k }}={{ $v | toJSON }}
          {{- end }}
          {{- end }}
        EOT
        destination = "secrets/env"
        env         = true
      }

      env {
        ICEBERG_CATALOG_URI = "${var.rustfs_endpoint}/iceberg"
        ICEBERG_WAREHOUSE   = var.table_bucket
        ICEBERG_NAMESPACE   = var.iceberg_namespace
        ICEBERG_SIGV4       = "true"

        S3_ENDPOINT   = var.rustfs_endpoint
        S3_REGION     = var.rustfs_region
        S3_PATH_STYLE = "true"
      }

      # Consolidation holds manifest entries, never row data.
      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
