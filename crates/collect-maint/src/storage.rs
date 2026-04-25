use crate::partition::{classify_entry, EntryKind, PartitionKey};
use crate::progress::LIST_REPORT_INTERVAL;
use anyhow::{Context, Result};
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use walkdir::WalkDir;

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub root: Option<PathBuf>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_disable_tls: bool,
}

#[derive(Clone)]
pub enum StorageLocation {
    Local(LocalStorage),
    S3(S3Storage),
}

#[derive(Clone)]
pub struct LocalStorage {
    root: PathBuf,
}

#[derive(Clone)]
pub struct S3Storage {
    client: S3Client,
    bucket: String,
    prefix: String,
}

#[derive(Clone, Debug)]
pub struct DatasetEntry {
    pub rel_path: String,
    pub size: u64,
    pub kind: EntryKind,
    pub partition: Option<PartitionKey>,
}

impl StorageConfig {
    pub async fn into_location(self) -> Result<StorageLocation> {
        if let Some(bucket) = self.s3_bucket {
            Ok(StorageLocation::S3(
                S3Storage::new(
                    bucket,
                    self.s3_prefix,
                    self.s3_region,
                    self.s3_endpoint,
                    self.s3_access_key,
                    self.s3_secret_key,
                    self.s3_disable_tls,
                )
                .await?,
            ))
        } else {
            Ok(StorageLocation::Local(LocalStorage::new(
                self.root.unwrap_or_else(|| PathBuf::from("data")),
            )))
        }
    }
}

impl StorageLocation {
    pub async fn list_entries<F>(&self, on_progress: F) -> Result<Vec<DatasetEntry>>
    where
        F: FnMut(usize),
    {
        let mut on_progress = on_progress;
        match self {
            StorageLocation::Local(local) => local.list_entries(&mut on_progress),
            StorageLocation::S3(s3) => s3.list_entries(&mut on_progress).await,
        }
    }

    pub async fn materialize(&self, entry: &DatasetEntry, workspace: &Path) -> Result<PathBuf> {
        match self {
            StorageLocation::Local(local) => Ok(local.path_for(&entry.rel_path)),
            StorageLocation::S3(s3) => s3.download_to_workspace(&entry.rel_path, workspace).await,
        }
    }

    pub async fn publish_file(&self, temp_path: &Path, rel_path: &str) -> Result<()> {
        match self {
            StorageLocation::Local(local) => local.publish_file(temp_path, rel_path).await,
            StorageLocation::S3(s3) => s3.upload_from_path(temp_path, rel_path).await,
        }
    }

    pub async fn write_bytes(&self, rel_path: &str, bytes: &[u8]) -> Result<()> {
        match self {
            StorageLocation::Local(local) => local.write_bytes(rel_path, bytes).await,
            StorageLocation::S3(s3) => s3.write_bytes(rel_path, bytes).await,
        }
    }

    pub async fn read_bytes(&self, rel_path: &str) -> Result<Vec<u8>> {
        match self {
            StorageLocation::Local(local) => local.read_bytes(rel_path).await,
            StorageLocation::S3(s3) => s3.read_bytes(rel_path).await,
        }
    }

    pub async fn delete_rel_path(&self, rel_path: &str) -> Result<()> {
        match self {
            StorageLocation::Local(local) => local.delete_rel_path(rel_path).await,
            StorageLocation::S3(s3) => s3.delete_rel_path(rel_path).await,
        }
    }

    pub async fn delete_rel_paths(&self, rel_paths: &[String]) -> Result<()> {
        for rel_path in rel_paths {
            self.delete_rel_path(rel_path).await?;
        }
        Ok(())
    }

    pub fn final_local_path(&self, rel_path: &str) -> Option<PathBuf> {
        match self {
            StorageLocation::Local(local) => Some(local.path_for(rel_path)),
            StorageLocation::S3(_) => None,
        }
    }

    pub fn temp_output_path(&self, workspace: &Path, rel_path: &str) -> PathBuf {
        match self {
            StorageLocation::Local(local) => local.temp_output_path(rel_path),
            StorageLocation::S3(_) => workspace.join(rel_path),
        }
    }

    pub fn dataset_label(&self) -> String {
        match self {
            StorageLocation::Local(local) => local.root.display().to_string(),
            StorageLocation::S3(s3) if s3.prefix.is_empty() => format!("s3://{}", s3.bucket),
            StorageLocation::S3(s3) => format!("s3://{}/{}", s3.bucket, s3.prefix),
        }
    }
}

impl LocalStorage {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path_for(&self, rel_path: &str) -> PathBuf {
        self.root.join(rel_path)
    }

    fn temp_output_path(&self, rel_path: &str) -> PathBuf {
        self.root.join(format!("{}.tmp", rel_path))
    }

    fn rel_path_from_abs(&self, path: &Path) -> Result<String> {
        Ok(path
            .strip_prefix(&self.root)
            .with_context(|| format!("{} is outside root {}", path.display(), self.root.display()))?
            .to_string_lossy()
            .replace('\\', "/"))
    }

    fn list_entries<F>(&self, on_progress: &mut F) -> Result<Vec<DatasetEntry>>
    where
        F: FnMut(usize),
    {
        if !self.root.exists() {
            return Ok(Vec::new());
        }

        if self.root.is_file() {
            let metadata = std::fs::metadata(&self.root)?;
            let rel_path = self
                .root
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("dataset.parquet")
                .to_string();
            let kind = classify_entry(&rel_path, metadata.len());
            on_progress(1);
            return Ok(vec![DatasetEntry {
                rel_path,
                size: metadata.len(),
                kind,
                partition: None,
            }]);
        }

        let mut entries = Vec::new();
        let mut last_reported = 0usize;
        for item in WalkDir::new(&self.root) {
            let item = item.context("walking dataset root")?;
            if !item.file_type().is_file() {
                continue;
            }

            let metadata = item.metadata().context("reading file metadata")?;
            let rel_path = self.rel_path_from_abs(item.path())?;
            let kind = classify_entry(&rel_path, metadata.len());
            entries.push(DatasetEntry {
                rel_path: rel_path.clone(),
                size: metadata.len(),
                kind,
                partition: PartitionKey::parse(&rel_path),
            });

            if entries.len() == 1 || entries.len() % LIST_REPORT_INTERVAL == 0 {
                on_progress(entries.len());
                last_reported = entries.len();
            }
        }

        if entries.len() != last_reported {
            on_progress(entries.len());
        }

        Ok(entries)
    }

    async fn publish_file(&self, temp_path: &Path, rel_path: &str) -> Result<()> {
        let final_path = self.path_for(rel_path);
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create {}", parent.display()))?;
        }
        if tokio::fs::try_exists(&final_path).await? {
            tokio::fs::remove_file(&final_path).await?;
        }
        tokio::fs::rename(temp_path, &final_path)
            .await
            .with_context(|| format!("rename {} to {}", temp_path.display(), final_path.display()))?;
        Ok(())
    }

    async fn write_bytes(&self, rel_path: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path_for(rel_path);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create {}", parent.display()))?;
        }
        tokio::fs::write(&path, bytes)
            .await
            .with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }

    async fn read_bytes(&self, rel_path: &str) -> Result<Vec<u8>> {
        tokio::fs::read(self.path_for(rel_path))
            .await
            .with_context(|| format!("read {}", self.path_for(rel_path).display()))
    }

    async fn delete_rel_path(&self, rel_path: &str) -> Result<()> {
        let path = self.path_for(rel_path);
        match tokio::fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl S3Storage {
    async fn new(
        bucket: String,
        prefix: String,
        region: String,
        endpoint: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        disable_tls: bool,
    ) -> Result<Self> {
        let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region));

        if let Some(mut endpoint_url) = endpoint {
            if disable_tls
                && !endpoint_url.starts_with("http://")
                && !endpoint_url.starts_with("https://")
            {
                endpoint_url = format!("http://{}", endpoint_url);
            }
            config_builder = config_builder.endpoint_url(endpoint_url);
        }

        if let (Some(access), Some(secret)) = (access_key, secret_key) {
            let credentials = Credentials::new(access, secret, None, None, "manual");
            config_builder = config_builder.credentials_provider(credentials);
        }

        let config = config_builder.load().await;
        let s3_config = S3Config::from(&config)
            .to_builder()
            .force_path_style(true)
            .build();

        Ok(Self {
            client: S3Client::from_conf(s3_config),
            bucket,
            prefix: prefix.trim_matches('/').to_string(),
        })
    }

    fn key_for(&self, rel_path: &str) -> String {
        if self.prefix.is_empty() {
            rel_path.to_string()
        } else {
            format!("{}/{}", self.prefix, rel_path)
        }
    }

    fn rel_from_key(&self, key: &str) -> Option<String> {
        if self.prefix.is_empty() {
            return Some(key.to_string());
        }

        let prefix = format!("{}/", self.prefix);
        key.strip_prefix(&prefix).map(|value| value.to_string())
    }

    async fn list_entries<F>(&self, on_progress: &mut F) -> Result<Vec<DatasetEntry>>
    where
        F: FnMut(usize),
    {
        let mut entries = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self.client.list_objects_v2().bucket(&self.bucket);
            if !self.prefix.is_empty() {
                request = request.prefix(self.prefix.clone());
            }
            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;
            for object in response.contents() {
                let Some(key) = object.key() else { continue; };
                let Some(rel_path) = self.rel_from_key(key) else { continue; };
                if rel_path.is_empty() {
                    continue;
                }
                let size = object.size().unwrap_or(0).max(0) as u64;
                entries.push(DatasetEntry {
                    rel_path: rel_path.clone(),
                    size,
                    kind: classify_entry(&rel_path, size),
                    partition: PartitionKey::parse(&rel_path),
                });
            }

            on_progress(entries.len());

            if response.is_truncated().unwrap_or(false) {
                continuation_token = response
                    .next_continuation_token()
                    .map(|value| value.to_string());
            } else {
                break;
            }
        }

        Ok(entries)
    }

    async fn download_to_workspace(&self, rel_path: &str, workspace: &Path) -> Result<PathBuf> {
        let key = self.key_for(rel_path);
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| format!("download s3://{}/{}", self.bucket, key))?;

        let path = workspace.join(rel_path);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create {}", parent.display()))?;
        }

        let mut file = tokio::fs::File::create(&path)
            .await
            .with_context(|| format!("create {}", path.display()))?;
        let mut reader = response.body.into_async_read();
        tokio::io::copy(&mut reader, &mut file)
            .await
            .with_context(|| format!("copy s3://{}/{} to {}", self.bucket, key, path.display()))?;
        file.flush().await?;
        Ok(path)
    }

    async fn upload_from_path(&self, temp_path: &Path, rel_path: &str) -> Result<()> {
        let key = self.key_for(rel_path);
        let body = ByteStream::from_path(temp_path)
            .await
            .with_context(|| format!("stream {}", temp_path.display()))?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("upload s3://{}/{}", self.bucket, key))?;

        tokio::fs::remove_file(temp_path).await.ok();
        Ok(())
    }

    async fn write_bytes(&self, rel_path: &str, bytes: &[u8]) -> Result<()> {
        let key = self.key_for(rel_path);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
            .with_context(|| format!("write s3://{}/{}", self.bucket, key))?;
        Ok(())
    }

    async fn read_bytes(&self, rel_path: &str) -> Result<Vec<u8>> {
        let key = self.key_for(rel_path);
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| format!("read s3://{}/{}", self.bucket, key))?;

        let mut bytes = Vec::new();
        let mut reader = response.body.into_async_read();
        reader.read_to_end(&mut bytes).await?;
        Ok(bytes)
    }

    async fn delete_rel_path(&self, rel_path: &str) -> Result<()> {
        let key = self.key_for(rel_path);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| format!("delete s3://{}/{}", self.bucket, key))?;
        Ok(())
    }
}
