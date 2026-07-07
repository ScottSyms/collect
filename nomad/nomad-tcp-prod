job "tcp-ais-broker" {
  datacenters = ["dc1"]
  type = "service"

  group "broker" {
    count = 1

    network {
      port "downstream" {
        static = 7001
      }
      port "metrics" {
        static = 9101
      }
    }

    service {
      name = "tcp-ais-broker"
      port = "downstream"
      provider = "consul"

      check {
        name     = "tcp-ready"
        type     = "tcp"
        port     = "downstream"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "broker" {
      driver = "exec"
      kill_timeout = "30s"

      resources {
        cpu    = 1000
        memory = 1024
      }

      artifact {
        source      = "http://192.168.99.107:9000/binaries/tcp-ais-broker"
        destination = "local/tcp-ais-broker"
        mode        = "file"
      }

      template {
        data = <<EOH
#!/bin/bash
chmod +x local/tcp-ais-broker
exec local/tcp-ais-broker \
  --upstream-host "153.44.253.27" \
  --upstream-port "5631" \
  --listen "0.0.0.0:7001" \
  --metrics-listen "0.0.0.0:9101" \
  --framing "line" \
  --ais-multipart-mode "affinity" \
  --queue-max-messages "100000" \
  --no-consumer-policy "buffer"
EOH
        destination = "local/run.sh"
        perms       = "755"
      }

      config {
        command = "local/run.sh"
      }
    }
  }
}
