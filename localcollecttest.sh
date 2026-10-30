export S3_ENDPOINT=http://127.0.0.1:9000
export S3_ACCESS_KEY=rustfsadmin
export S3_SECRET_KEY=rustfsadmin
export S3_REGION=us-east-1
export S3_PATH_STYLE=true

./target/release/collect-socket --tcp-host 127.0.0.1 --tcp-port 31410 --source mssis --parser ais --iceberg-catalog-uri http://127.0.0.1:9000/iceberg --iceberg-warehouse maritimedata --iceberg-namespace ais --iceberg-sigv4 --partition hour --max-rows 10000 --delete-after-iceberg --mssis --consolidate-ais
