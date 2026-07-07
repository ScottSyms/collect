#!/bin/bash
set -euo pipefail

exec "$HOME/code/projects/collect/target/release/tcp-ais-broker" \
  --upstream-host "153.44.253.27" \
  --upstream-port "5631" \
  --listen "0.0.0.0:7001" \
  --metrics-listen "0.0.0.0:9101" \
  --framing "line" \
  --ais-multipart-mode "affinity" \
  --queue-max-messages "100000" \
  --no-consumer-policy "buffer"
