#!/bin/bash
cargo build --release
rclone copy target/release/aisstream-parse rustfs:binaries
rclone copy target/release/ais-parse rustfs:binaries
rclone copy target/release/collect-file rustfs:binaries
rclone copy target/release/collect-aisstream rustfs:binaries
rclone copy target/release/collect-socket rustfs:binaries
rclone copy target/release/collect-kafka rustfs:binaries
rclone copy target/release/ais-normalize rustfs:binaries

