job "parse-file" {
  datacenters = ["dc1", "duncan"]
  type        = "batch"

  parameterized {
    payload       = "forbidden"
    meta_required = ["s3_bucket", "s3_key", "source", "parser"]
    meta_optional = ["s3_prefix", "batch_size", "compression_level"]
  }

  group "parse" {
    count = 1

    restart {
      attempts = 0
      mode     = "fail"
    }

    task "parse" {
      driver = "exec"

      config {
        command = "local/parse-file-worker"
      }

      artifact {
        source      = "http://192.168.99.107:9000/binaries/parse-file-worker"
        destination = "local/parse-file-worker"
        mode        = "file"
      }

      # Resolve orchestrator + lakekeeper at run time via Consul
      template {
        data        = <<EOH
{{ range service "collect-orchestrator" }}CALLBACK_URL=http://{{ .Address }}:{{ .Port }}/complete
{{ end }}{{ range service "lakekeeper" }}ICEBERG_CATALOG_URI=http://{{ .Address }}:{{ .Port }}/catalog
{{ end }}EOH
        destination = "secrets/consul.env"
        env         = true
      }

      env {
        S3_ENDPOINT      = "http://192.168.99.107:9000"
        S3_REGION        = "us-east-1"
        # S3 credentials injected via Nomad vars (nomad var put nomad/jobs/parse-file ...)
        # Template below injects S3_ACCESS_KEY / S3_SECRET_KEY / CALLBACK_TOKEN from vars
        S3_DISABLE_TLS   = "true"
        ICEBERG_WAREHOUSE = "s3://warehouse"
        ICEBERG_NAMESPACE = "ais"
        BATCH_SIZE        = "8192"
        COMPRESSION_LEVEL = "5"
        S3_PATH_STYLE           = "true"
        S3_DISABLE_EC2_METADATA = "true"
        S3_DISABLE_CONFIG_LOAD  = "true"
      }

      template {
        data        = <<EOH
{{ with nomadVar "nomad/jobs/parse-file" }}S3_ACCESS_KEY={{ .S3_ACCESS_KEY }}
S3_SECRET_KEY={{ .S3_SECRET_KEY }}
CALLBACK_TOKEN={{ .CALLBACK_TOKEN }}
{{ end }}EOH
        destination = "secrets/vars.env"
        env         = true
      }

      resources {
        cpu    = 500
        memory = 1024
      }
    }
  }
}

# Vars setup:
#   nomad var put nomad/jobs/parse-file S3_ACCESS_KEY=... S3_SECRET_KEY=... CALLBACK_TOKEN=...
# Or consolidated from ~/code/nomad/vars/prod.json:
#   cat ~/code/nomad/vars/prod.json | jq -r ... | nomad var put ...
