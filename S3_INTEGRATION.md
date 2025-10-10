# S3 Remote Storage Integration

This application now supports uploading parquet files to S3-compatible object storage including AWS S3 and MinIO.

## Usage Examples

### AWS S3 (using environment variables)
```bash
# Set AWS credentials as environment variables
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Run with S3 upload enabled
./hive_parquet_ingest \
    --tcp-host 153.44.253.27 --tcp-port 5631 \
    --source norway-tcp \
    --s3-bucket my-data-bucket \
    --s3-region us-west-2
```

### AWS S3 (using command line)
```bash
./hive_parquet_ingest \
    --tcp-host 153.44.253.27 --tcp-port 5631 \
    --source norway-tcp \
    --s3-bucket my-data-bucket \
    --s3-region us-west-2 \
    --s3-access-key AKIAIOSFODNN7EXAMPLE \
    --s3-secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### MinIO (self-hosted S3-compatible storage)
```bash
./hive_parquet_ingest \
    --input data.txt \
    --source mydata \
    --s3-bucket data-lake \
    --s3-endpoint http://localhost:9000 \
    --s3-region us-east-1 \
    --s3-access-key minioadmin \
    --s3-secret-key minioadmin \
    --keep-local
```

### Other S3-compatible services (Wasabi, DigitalOcean Spaces, etc.)
```bash
./hive_parquet_ingest \
    --tcp-host 153.44.253.27 --tcp-port 5631 \
    --s3-bucket my-bucket \
    --s3-endpoint https://s3.wasabisys.com \
    --s3-region us-east-1 \
    --s3-access-key your-access-key \
    --s3-secret-key your-secret-key
```

## S3 Command Line Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--s3-bucket` | S3 bucket name | Yes (to enable S3) | None |
| `--s3-region` | AWS region | No | us-east-1 |
| `--s3-endpoint` | Custom S3 endpoint URL | No | AWS S3 |
| `--s3-access-key` | S3 access key ID | No* | From env/credentials |
| `--s3-secret-key` | S3 secret access key | No* | From env/credentials |
| `--keep-local` | Keep local files after S3 upload | No | false (delete local) |

*If not provided, will use AWS credential chain (environment variables, AWS CLI config, IAM roles, etc.)

## S3 Key Structure

Files are uploaded to S3 using the same Hive partitioning structure:

```
s3://bucket-name/source=norway-tcp/year=2025/month=10/day=15/hour=14/minute=32/part-20251015T143245123.parquet
```

## Environment Variables

The application supports standard AWS environment variables:

- `AWS_ACCESS_KEY_ID` - AWS access key ID
- `AWS_SECRET_ACCESS_KEY` - AWS secret access key  
- `AWS_DEFAULT_REGION` - Default AWS region
- `AWS_PROFILE` - AWS CLI profile to use
- `AWS_ENDPOINT_URL` - Custom S3 endpoint URL

## Docker Configuration

### docker-compose.yml with S3
```yaml
version: '3.8'
services:
  data-ingest:
    build: .
    command: [
      "--tcp-host", "153.44.253.27", 
      "--tcp-port", "5631",
      "--source", "norway-tcp",
      "--s3-bucket", "my-data-bucket",
      "--s3-region", "us-west-2"
    ]
    environment:
      - AWS_ACCESS_KEY_ID=your-access-key
      - AWS_SECRET_ACCESS_KEY=your-secret-key
    volumes:
      - ./output:/data  # Local backup (optional)
```

### MinIO docker-compose.yml
```yaml
version: '3.8'
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER=minioadmin
      - MINIO_ROOT_PASSWORD=minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data

  data-ingest:
    build: .
    depends_on:
      - minio
    command: [
      "--tcp-host", "153.44.253.27",
      "--tcp-port", "5631", 
      "--source", "norway-tcp",
      "--s3-bucket", "data-lake",
      "--s3-endpoint", "http://minio:9000",
      "--s3-access-key", "minioadmin",
      "--s3-secret-key", "minioadmin",
      "--keep-local"
    ]
    volumes:
      - ./output:/data

volumes:
  minio_data:
```

## Behavior

1. **Local First**: Files are always written locally first
2. **S3 Upload**: After successful local write, files are uploaded to S3
3. **Cleanup**: By default, local files are deleted after successful S3 upload
4. **Keep Local**: Use `--keep-local` to retain local copies
5. **Error Handling**: If S3 upload fails, local files are preserved
6. **Progress**: Upload progress is logged to stdout

## Security Notes

- Never include credentials in command line arguments in production
- Use environment variables, AWS CLI profiles, or IAM roles instead  
- For MinIO/self-hosted: ensure TLS/SSL in production environments
- Consider using IAM policies to restrict S3 access to specific buckets/paths

## Monitoring S3 Uploads

The application logs S3 operations:
```
✅ Wrote 1000 rows to data/source=norway-tcp/year=2025/month=10/day=15/hour=14/minute=32/part-20251015T143245123.parquet
✅ Uploaded data/source=norway-tcp/year=2025/month=10/day=15/hour=14/minute=32/part-20251015T143245123.parquet to S3: s3://my-bucket/source=norway-tcp/year=2025/month=10/day=15/hour=14/minute=32/part-20251015T143245123.parquet
🗑️  Removed local file: data/source=norway-tcp/year=2025/month=10/day=15/hour=14/minute=32/part-20251015T143245123.parquet
```

## Troubleshooting

### Common Issues

1. **Credentials Error**: Ensure AWS credentials are properly configured
2. **Bucket Not Found**: Create the S3 bucket before running the application  
3. **Permission Denied**: Check IAM policies for S3 access
4. **Endpoint Error**: Verify the S3 endpoint URL for custom services
5. **Region Mismatch**: Ensure the region matches your bucket's region

### Debug Commands
```bash
# Test S3 connectivity
aws s3 ls s3://your-bucket-name/ --endpoint-url http://localhost:9000

# Verify credentials
aws sts get-caller-identity

# Check local files before S3 upload
ls -la data/
```