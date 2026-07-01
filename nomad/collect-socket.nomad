job "collect-socket" {
  datacenters = ["dc1"]
  type        = "service"

  group "norway" {
    count = 1

    task "collect-socket" {
      driver = "exec"

      config {
        command = "/usr/local/bin/collect-socket"
      }

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
      }

      resources {
        cpu    = 500
        memory = 1024
      }

      service {
        name = "collect-socket"
        check {
          type     = "script"
          command  = "/usr/local/bin/collect-socket"
          args     = ["--health-check"]
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
