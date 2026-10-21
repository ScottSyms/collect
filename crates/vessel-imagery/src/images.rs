//! Image bytes and the `vessel_images` rows that describe them. Bytes go to
//! the raw archive beside the tables, split by whether their licence
//! restricts them, and are named by their sha256 so a photo found twice is
//! stored once.

use std::io::Cursor;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use iceberg::table::Table;
use image::{ImageFormat, ImageReader};
use vessel_identity::columns::Reader;
use vessel_identity::fetch::sha256_hex;
use vessel_identity::ingest::archive_root;

use crate::tables::ImageRow;

/// What the bytes are: mime type, extension, width and height.
pub struct Sniffed {
    pub mime: &'static str,
    pub ext: &'static str,
    pub width: u32,
    pub height: u32,
}

pub fn sniff(bytes: &[u8]) -> Result<Sniffed> {
    let r = ImageReader::new(Cursor::new(bytes)).with_guessed_format().context("reading image")?;
    let (mime, ext) = match r.format() {
        Some(ImageFormat::Jpeg) => ("image/jpeg", "jpg"),
        Some(ImageFormat::Png) => ("image/png", "png"),
        Some(ImageFormat::WebP) => ("image/webp", "webp"),
        other => anyhow::bail!("unsupported image format {other:?}"),
    };
    let (width, height) = r.into_dimensions().context("reading image size")?;
    Ok(Sniffed { mime, ext, width, height })
}

/// Where an image's bytes go: `<root>/<free|restricted>/<aa>/<sha256>.<ext>`.
pub fn object_uri(root: &str, restricted: bool, sha256: &str, ext: &str) -> String {
    let class = if restricted { "restricted" } else { "free" };
    format!("{}/{class}/{}/{sha256}.{ext}", root.trim_end_matches('/'), &sha256[..2])
}

/// The archive root for the imagery namespace, beside `table`.
pub fn root_for(table: &Table, namespace: &str, override_root: Option<&str>) -> String {
    match override_root {
        Some(r) => r.trim_end_matches('/').to_string(),
        None => archive_root(table.metadata().location(), namespace),
    }
}

/// Writes `bytes` (unless already there); returns its sha256 and URI.
pub async fn store(table: &Table, root: &str, restricted: bool, bytes: Bytes, ext: &str) -> Result<(String, String)> {
    let sha = sha256_hex(&bytes);
    let uri = object_uri(root, restricted, &sha, ext);
    let io = table.file_io();
    if !io.exists(&uri).await.unwrap_or(false) {
        io.new_output(&uri)?.write(bytes).await.with_context(|| format!("writing {uri}"))?;
    }
    Ok((sha, uri))
}

pub async fn load(table: &Table, uri: &str) -> Result<Bytes> {
    table.file_io().new_input(uri)?.read().await.with_context(|| format!("reading {uri}"))
}

fn bools(b: &RecordBatch, name: &str) -> Result<BooleanArray> {
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>().cloned())
        .with_context(|| format!("no boolean column {name}"))
}

/// `vessel_images` rows read back.
pub fn rows(batches: &[RecordBatch]) -> Result<Vec<ImageRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let s = |n: &str| r.str(n);
        let (id, vid, imo, mmsi, basis, source) = (s("image_id")?, s("vessel_id")?, s("imo_number")?, s("mmsi")?, s("match_basis")?, s("source")?);
        let (page, orig, uri, mime, lic, lic_url, class) =
            (s("source_page_url")?, s("original_url")?, s("object_uri")?, s("mime")?, s("licence_short")?, s("licence_url")?, s("licence_class")?);
        let (author, credit) = (s("author")?, s("credit")?);
        let (w, h) = (r.int("width")?, r.int("height")?);
        let (captured, retrieved) = (r.ts("captured_at")?, r.ts("retrieved_at")?);
        let (restricted, attribution) = (bools(b, "restricted")?, bools(b, "attribution_required")?);
        for i in 0..r.rows() {
            out.push(ImageRow {
                image_id: id(i).unwrap_or_default(),
                vessel_id: vid(i).unwrap_or_default(),
                imo_number: imo(i),
                mmsi: mmsi(i),
                match_basis: basis(i).unwrap_or_default(),
                source: source(i).unwrap_or_default(),
                source_page_url: page(i),
                original_url: orig(i),
                object_uri: uri(i).unwrap_or_default(),
                mime: mime(i).unwrap_or_default(),
                width: w(i),
                height: h(i),
                licence_short: lic(i),
                licence_url: lic_url(i),
                licence_class: class(i).unwrap_or_default(),
                restricted: !restricted.is_null(i) && restricted.value(i),
                attribution_required: !attribution.is_null(i) && attribution.value(i),
                author: author(i),
                credit: credit(i),
                captured_at: captured(i),
                retrieved_at: retrieved(i).unwrap_or_default(),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uris_split_by_licence() {
        let sha = "ab".repeat(32);
        assert_eq!(object_uri("s3://b/raw_archive/imagery/", false, &sha, "jpg"), format!("s3://b/raw_archive/imagery/free/ab/{sha}.jpg"));
        assert!(object_uri("s3://b/x", true, &sha, "png").contains("/restricted/ab/"));
    }

    #[test]
    fn sniffs_png() {
        let mut out = Vec::new();
        image::RgbImage::new(300, 200).write_to(&mut Cursor::new(&mut out), ImageFormat::Png).unwrap();
        let s = sniff(&out).unwrap();
        assert_eq!((s.mime, s.ext, s.width, s.height), ("image/png", "png", 300, 200));
    }
}
