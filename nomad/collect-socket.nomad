job "collect-socket" {
  datacenters = ["dc1"]
  type        = "service"

  group "norway" {
    count = 1

    network {
      port "metrics" {}
    }

    task "collect-socket" {
      driver = "exec"

      config {
        command = "/usr/local/bin/collect-socket"
      }

      # Must exceed UPLOAD_DRAIN_TIMEOUT_SECONDS (default 60s) so a stopped
      # alloc can flush its final batch and drain pending S3 uploads before
      # Nomad escalates SIGTERM to SIGKILL.
      kill_timeout = "90s"

      env {
        RUST_LOG       = var.rust_log
        TCP_HOST       = var.tcp_host
        TCP_PORT       = var.tcp_port
        SOURCE         = var.source
        OUT_DIR        = "/data"
        MAX_ROWS       = var.max_rows
        KEEP_LOCAL     = var.keep_local
        S3_BUCKET      = var.s3_bucket
        S3_REGION      = var.s3_region
        S3_ENDPOINT    = var.s3_endpoint
        S3_ACCESS_KEY  = var.s3_access_key
        S3_SECRET_KEY  = var.s3_secret_key
        S3_DISABLE_TLS = var.s3_disable_tls
        # Prometheus metrics and /healthz on the Nomad-assigned port.
        METRICS_ADDR   = "0.0.0.0:${NOMAD_PORT_metrics}"
      }

      resources {
        cpu    = 500
        memory = 1024
      }

      service {
        name = "collect-socket"
        port = "metrics"

        tags = ["prometheus"]

        check {
          type     = "http"
          path     = "/healthz"
          interval = "30s"
          timeout  = "10s"

          check_restart {
            limit           = 3
            grace           = "30s"
            ignore_warnings = false
          }
        }
      }

      restart {
        attempts = 10
        interval = "5m"
        delay    = "15s"
        mode     = "delay"
      }
    }
  }
}

variable "tcp_host" {
  default = "153.44.253.27"
}

variable "tcp_port" {
  default = "5631"
}

variable "source" {
  default = "norway-tcp"
}

variable "rust_log" {
  default = "INFO"
}

variable "max_rows" {
  default = "10000"
}

variable "keep_local" {
  default = "false"
}

variable "s3_bucket" {
  default = ""
}

variable "s3_region" {
  default = ""
}

variable "s3_endpoint" {
  default = ""
}

variable "s3_access_key" {
  default = ""
}

variable "s3_secret_key" {
  default = ""
}

variable "s3_disable_tls" {
  default = "true"
}
