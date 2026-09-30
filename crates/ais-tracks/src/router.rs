//! Routing a day of raw reports to on-disk buckets by vessel.
//!
//! Silver is not sorted by vessel, and a day this size does not fit in memory,
//! so the reducer cannot just sort it. Instead one sequential pass over the day
//! writes every report into one of a few hundred bucket files chosen by a hash
//! of the MMSI, so all of a vessel's reports land in the same bucket. Each
//! bucket is small enough to load, sort and reduce in memory, one at a time.
//!
//! Reports are stored as fixed-width 31-byte records (positions in 1e-7
//! degrees, which is finer than AIS itself), in independently compressed
//! zstd frames, so the writer buffers only a small frame per bucket and the
//! reader can stream them. The record carries no payload text: source,
//! station and navigation status are ids into side dictionaries.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::reduce::{Dicts, RawPoint};

/// Bytes per stored report.
pub const REC_LEN: usize = 31;
/// Reports per compressed frame (about 250 KB raw).
const FRAME_RECS: usize = 8192;
const ZSTD_LEVEL: i32 = 1;

const NONE_I32: i32 = i32::MIN;
const NONE_I16: i16 = i16::MIN;
const NONE_U8: u8 = u8::MAX;
const NONE_U16: u16 = u16::MAX;

/// The columns a source must supply.
pub const SOURCE_COLUMNS: [&str; 10] = [
    "ts",
    "mmsi",
    "latitude",
    "longitude",
    "sog_knots",
    "cog",
    "heading_true",
    "nav_status",
    "source",
    "station",
];

pub fn encode(r: &RawPoint, out: &mut Vec<u8>) {
    out.extend_from_slice(&r.mmsi.to_le_bytes());
    out.extend_from_slice(&r.ts_us.to_le_bytes());
    out.extend_from_slice(&r.lat_e7.unwrap_or(NONE_I32).to_le_bytes());
    out.extend_from_slice(&r.lon_e7.unwrap_or(NONE_I32).to_le_bytes());
    out.extend_from_slice(&r.sog_dk.unwrap_or(NONE_I16).to_le_bytes());
    out.extend_from_slice(&r.cog_dd.unwrap_or(NONE_I16).to_le_bytes());
    out.extend_from_slice(&r.heading_dd.unwrap_or(NONE_I16).to_le_bytes());
    out.push(r.nav.map_or(NONE_U8, |v| v as u8));
    out.extend_from_slice(&r.source.to_le_bytes());
    out.extend_from_slice(&r.station.unwrap_or(NONE_U16).to_le_bytes());
}

pub fn decode(b: &[u8]) -> RawPoint {
    let i32_at = |o: usize| i32::from_le_bytes(b[o..o + 4].try_into().unwrap());
    let i16_at = |o: usize| i16::from_le_bytes(b[o..o + 2].try_into().unwrap());
    let u16_at = |o: usize| u16::from_le_bytes(b[o..o + 2].try_into().unwrap());
    let opt32 = |v: i32| (v != NONE_I32).then_some(v);
    let opt16 = |v: i16| (v != NONE_I16).then_some(v);
    RawPoint {
        mmsi: u32::from_le_bytes(b[0..4].try_into().unwrap()),
        ts_us: i64::from_le_bytes(b[4..12].try_into().unwrap()),
        lat_e7: opt32(i32_at(12)),
        lon_e7: opt32(i32_at(16)),
        sog_dk: opt16(i16_at(20)),
        cog_dd: opt16(i16_at(22)),
        heading_dd: opt16(i16_at(24)),
        nav: (b[26] != NONE_U8).then_some(b[26] as u16),
        source: u16_at(27),
        station: {
            let v = u16_at(29);
            (v != NONE_U16).then_some(v)
        },
    }
}

/// splitmix64: spreads MMSIs, which cluster by country prefix and fleet, evenly.
pub fn bucket_of(mmsi: u32, buckets: usize) -> usize {
    let mut z = (mmsi as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z % buckets as u64) as usize
}

/// An MMSI that can identify a vessel: nine digits at most, and not zero.
/// Anything else (unset fields, garbage) would form giant fake "vessels".
pub fn plausible_mmsi(mmsi: i64) -> bool {
    (1..=999_999_999).contains(&mmsi)
}

#[derive(Debug, Default)]
struct DictBuilder {
    ids: HashMap<String, u16>,
    names: Vec<String>,
    max: usize,
}

impl DictBuilder {
    fn new(max: usize) -> Self {
        Self {
            max,
            ..Default::default()
        }
    }

    fn id(&mut self, name: &str) -> Result<u16> {
        if let Some(&i) = self.ids.get(name) {
            return Ok(i);
        }
        if self.names.len() >= self.max {
            bail!("more than {} distinct values in a dictionary column", self.max);
        }
        let i = self.names.len() as u16;
        self.ids.insert(name.to_string(), i);
        self.names.push(name.to_string());
        Ok(i)
    }
}

/// What a finished route left on disk.
#[derive(Debug)]
pub struct Manifest {
    dir: PathBuf,
    pub buckets: usize,
    /// Reports in each bucket.
    pub counts: Vec<u64>,
    pub dicts: Dicts,
    /// Reports with an implausible MMSI, set aside and not routed.
    pub quarantined: u64,
    /// Reports outside the requested day, skipped.
    pub outside_day: u64,
    /// Reports routed to buckets.
    pub routed: u64,
    pub bytes_on_disk: u64,
}

impl Manifest {
    fn path(&self, b: usize) -> PathBuf {
        self.dir.join(format!("bucket-{b:05}.zst"))
    }

    /// Removes the scratch files.
    pub fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub struct Router {
    dir: PathBuf,
    buckets: usize,
    bufs: Vec<Vec<u8>>,
    files: Vec<BufWriter<File>>,
    counts: Vec<u64>,
    sources: DictBuilder,
    stations: DictBuilder,
    navs: DictBuilder,
    quarantined: u64,
    outside_day: u64,
    bytes: u64,
}

impl Router {
    /// Creates `buckets` empty bucket files under `dir` (created if needed).
    pub fn new(dir: &Path, buckets: usize) -> Result<Self> {
        anyhow::ensure!(buckets >= 1, "need at least one bucket");
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let mut files = Vec::with_capacity(buckets);
        for b in 0..buckets {
            let p = dir.join(format!("bucket-{b:05}.zst"));
            files.push(BufWriter::with_capacity(
                64 * 1024,
                File::create(&p).with_context(|| format!("creating {}", p.display()))?,
            ));
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            buckets,
            bufs: vec![Vec::new(); buckets],
            files,
            counts: vec![0; buckets],
            sources: DictBuilder::new(u16::MAX as usize),
            stations: DictBuilder::new(u16::MAX as usize),
            navs: DictBuilder::new(u8::MAX as usize),
            quarantined: 0,
            outside_day: 0,
            bytes: 0,
        })
    }

    fn flush_bucket(&mut self, b: usize) -> Result<()> {
        if self.bufs[b].is_empty() {
            return Ok(());
        }
        let frame = zstd::bulk::compress(&self.bufs[b], ZSTD_LEVEL)?;
        self.files[b].write_all(&(frame.len() as u32).to_le_bytes())?;
        self.files[b].write_all(&frame)?;
        self.bytes += 4 + frame.len() as u64;
        self.bufs[b].clear();
        Ok(())
    }

    /// Routes one report.
    pub fn push(&mut self, r: &RawPoint) -> Result<()> {
        let b = bucket_of(r.mmsi, self.buckets);
        encode(r, &mut self.bufs[b]);
        self.counts[b] += 1;
        if self.bufs[b].len() >= FRAME_RECS * REC_LEN {
            self.flush_bucket(b)?;
        }
        Ok(())
    }

    /// Routes every report in a batch of the [`SOURCE_COLUMNS`] whose time is in
    /// `[day_start_us, day_end_us)`. Column types are coerced, so a source may
    /// hand over whatever integer, float, timestamp and string widths it has.
    pub fn route_batch(&mut self, batch: &RecordBatch, day_start_us: i64, day_end_us: i64) -> Result<()> {
        let col = |name: &str, ty: &DataType| -> Result<_> {
            let c = batch
                .column_by_name(name)
                .with_context(|| format!("source has no column '{name}'"))?;
            Ok(cast(c, ty)?)
        };
        let ts = col("ts", &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))?;
        let ts = ts.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let mmsi = col("mmsi", &DataType::Int64)?;
        let mmsi = mmsi.as_any().downcast_ref::<Int64Array>().unwrap();
        let f = |n: &str| col(n, &DataType::Float64);
        let (lat, lon) = (f("latitude")?, f("longitude")?);
        let (sog, cog, hd) = (f("sog_knots")?, f("cog")?, f("heading_true")?);
        let (lat, lon) = (
            lat.as_any().downcast_ref::<Float64Array>().unwrap(),
            lon.as_any().downcast_ref::<Float64Array>().unwrap(),
        );
        let (sog, cog, hd) = (
            sog.as_any().downcast_ref::<Float64Array>().unwrap(),
            cog.as_any().downcast_ref::<Float64Array>().unwrap(),
            hd.as_any().downcast_ref::<Float64Array>().unwrap(),
        );
        let s = |n: &str| col(n, &DataType::Utf8);
        let (nav, src, stn) = (s("nav_status")?, s("source")?, s("station")?);
        let (nav, src, stn) = (
            nav.as_any().downcast_ref::<StringArray>().unwrap(),
            src.as_any().downcast_ref::<StringArray>().unwrap(),
            stn.as_any().downcast_ref::<StringArray>().unwrap(),
        );

        let q = |a: &Float64Array, i: usize, scale: f64, lo: f64, hi: f64| -> Option<f64> {
            (!a.is_null(i)).then(|| a.value(i) * scale).filter(|v| v.is_finite() && (lo..=hi).contains(v))
        };
        for i in 0..batch.num_rows() {
            if ts.is_null(i) || mmsi.is_null(i) {
                self.quarantined += 1;
                continue;
            }
            let t = ts.value(i);
            if t < day_start_us || t >= day_end_us {
                self.outside_day += 1;
                continue;
            }
            if !plausible_mmsi(mmsi.value(i)) {
                self.quarantined += 1;
                continue;
            }
            // Values outside what the record can hold are stored as missing
            // rather than wrapped; positions are range-checked later by the
            // reducer, so only finiteness and the storage range matter here.
            let e7 = |a: &Float64Array| q(a, i, 1e7, -2.0e9, 2.0e9).map(|v| v.round() as i32);
            let d10 = |a: &Float64Array| q(a, i, 10.0, -32_000.0, 32_000.0).map(|v| v.round() as i16);
            let rec = RawPoint {
                ts_us: t,
                mmsi: mmsi.value(i) as u32,
                lat_e7: e7(lat),
                lon_e7: e7(lon),
                sog_dk: d10(sog),
                cog_dd: d10(cog),
                heading_dd: d10(hd),
                nav: if nav.is_null(i) {
                    None
                } else {
                    Some(self.navs.id(nav.value(i))? as u16)
                },
                source: if src.is_null(i) {
                    self.sources.id("")?
                } else {
                    self.sources.id(src.value(i))?
                },
                station: if stn.is_null(i) {
                    None
                } else {
                    Some(self.stations.id(stn.value(i))?)
                },
            };
            self.push(&rec)?;
        }
        Ok(())
    }

    /// Flushes everything and returns the manifest.
    pub fn finish(mut self) -> Result<Manifest> {
        for b in 0..self.buckets {
            self.flush_bucket(b)?;
            self.files[b].flush()?;
        }
        let routed = self.counts.iter().sum();
        Ok(Manifest {
            dir: self.dir,
            buckets: self.buckets,
            counts: self.counts,
            dicts: Dicts::new(self.sources.names, self.stations.names, self.navs.names),
            quarantined: self.quarantined,
            outside_day: self.outside_day,
            routed,
            bytes_on_disk: self.bytes,
        })
    }
}

/// Reads one bucket's reports (unsorted, in arrival order).
pub fn read_bucket(m: &Manifest, b: usize) -> Result<Vec<RawPoint>> {
    let mut out = Vec::new();
    read_bucket_into(m, b, &mut out)?;
    Ok(out)
}

/// Like [`read_bucket`], but into a caller-owned buffer that is cleared first,
/// so a loop over buckets reuses one allocation instead of allocating and
/// freeing a large vector each time.
pub fn read_bucket_into(m: &Manifest, b: usize, out: &mut Vec<RawPoint>) -> Result<()> {
    out.clear();
    out.reserve(m.counts[b] as usize);
    let path = m.path(b);
    let mut f = BufReader::with_capacity(64 * 1024, File::open(&path)?);
    let mut len = [0u8; 4];
    loop {
        match f.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let mut frame = vec![0u8; u32::from_le_bytes(len) as usize];
        f.read_exact(&mut frame)?;
        let raw = zstd::bulk::decompress(&frame, FRAME_RECS * REC_LEN + 1)?;
        anyhow::ensure!(raw.len() % REC_LEN == 0, "corrupt frame in {}", path.display());
        out.extend(raw.chunks_exact(REC_LEN).map(decode));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn point(mmsi: u32, i: i64) -> RawPoint {
        RawPoint {
            ts_us: 1_700_000_000_000_000 + i * 1_000_000,
            mmsi,
            lat_e7: Some(-123_456_789 + i as i32),
            lon_e7: if i % 5 == 0 { None } else { Some(1_234_567_890 - i as i32) },
            sog_dk: Some((i % 1024) as i16),
            cog_dd: None,
            heading_dd: Some(5110),
            nav: if i % 3 == 0 { None } else { Some((i % 7) as u16) },
            source: (i % 3) as u16,
            station: if i % 2 == 0 { Some((i % 4) as u16) } else { None },
        }
    }

    #[test]
    fn records_round_trip_including_missing_values() {
        for i in 0..50 {
            let p = point(366_000_000 + i as u32, i);
            let mut b = Vec::new();
            encode(&p, &mut b);
            assert_eq!(b.len(), REC_LEN);
            assert_eq!(decode(&b), p);
        }
    }

    #[test]
    fn buckets_are_spread_evenly_even_for_clustered_mmsis() {
        let n = 64;
        let mut c = vec![0u32; n];
        for m in 0..64_000u32 {
            c[bucket_of(366_000_000 + m, n)] += 1; // one country prefix, sequential
        }
        let (min, max) = (c.iter().min().unwrap(), c.iter().max().unwrap());
        assert!(*max < *min * 2, "min {min} max {max}");
    }

    #[test]
    fn routing_keeps_each_vessel_together_and_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut r = Router::new(dir.path(), 8).unwrap();
        let mut sent = Vec::new();
        for v in 0..40u32 {
            for i in 0..600 {
                // Enough reports to force several frames per bucket.
                let p = point(366_000_000 + v, i);
                r.push(&p).unwrap();
                sent.push(p);
            }
        }
        let m = r.finish().unwrap();
        assert_eq!(m.routed, sent.len() as u64);
        let mut got = Vec::new();
        for b in 0..m.buckets {
            let pts = read_bucket(&m, b).unwrap();
            assert_eq!(pts.len() as u64, m.counts[b]);
            for p in &pts {
                assert_eq!(bucket_of(p.mmsi, m.buckets), b, "vessel in the wrong bucket");
            }
            got.extend(pts);
        }
        let key = |p: &RawPoint| (p.mmsi, p.ts_us);
        sent.sort_by_key(key);
        got.sort_by_key(key);
        assert_eq!(sent, got);
        assert!(m.bytes_on_disk < sent.len() as u64 * REC_LEN as u64, "compressed");
        m.cleanup();
    }

    fn batch(rows: &[(i64, i64, Option<f64>, &str)]) -> RecordBatch {
        // Deliberately not the router's native types: ms timestamps, f64 ids.
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())), false),
            Field::new("mmsi", DataType::Int64, false),
            Field::new("latitude", DataType::Float64, true),
            Field::new("longitude", DataType::Float64, true),
            Field::new("sog_knots", DataType::Float64, true),
            Field::new("cog", DataType::Float64, true),
            Field::new("heading_true", DataType::Float64, true),
            Field::new("nav_status", DataType::Utf8, true),
            Field::new("source", DataType::Utf8, true),
            Field::new("station", DataType::Utf8, true),
        ]));
        let n = rows.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampMillisecondArray::from_iter_values(rows.iter().map(|r| r.0))
                        .with_timezone("+00:00"),
                ),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.2))),
                Arc::new(Float64Array::from(vec![Some(4.5); n])),
                Arc::new(Float64Array::from(vec![Some(12.3); n])),
                Arc::new(Float64Array::from(vec![Some(360.0); n])),
                Arc::new(Float64Array::from(vec![Some(511.0); n])),
                Arc::new(StringArray::from(vec![Some("moored"); n])),
                Arc::new(StringArray::from_iter(rows.iter().map(|r| Some(r.3)))),
                Arc::new(StringArray::from(vec![None::<&str>; n])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_batch_is_filtered_to_the_day_and_bad_mmsis_are_set_aside() {
        let day = 1_700_000_000_000i64; // ms
        let b = batch(&[
            (day, 366_000_001, Some(51.5), "a"),
            (day + 1000, 366_000_001, Some(51.5001), "b"),
            (day - 1, 366_000_002, Some(1.0), "a"),          // the day before
            (day + 86_400_000, 366_000_003, Some(1.0), "a"), // the day after
            (day + 5, 0, Some(1.0), "a"),                    // no vessel
            (day + 5, 4_294_967_295, Some(1.0), "a"),        // garbage
            (day + 6, 366_000_004, None, "a"),               // no latitude
        ]);
        let dir = tempfile::tempdir().unwrap();
        let mut r = Router::new(dir.path(), 4).unwrap();
        r.route_batch(&b, day * 1000, (day + 86_400_000) * 1000).unwrap();
        let m = r.finish().unwrap();
        assert_eq!((m.routed, m.quarantined, m.outside_day), (3, 2, 2));
        let all: Vec<RawPoint> = (0..m.buckets).flat_map(|b| read_bucket(&m, b).unwrap()).collect();
        let a = all.iter().find(|p| p.mmsi == 366_000_001 && p.ts_us == day * 1000).unwrap();
        assert_eq!(a.lat_e7, Some(515_000_000));
        assert_eq!((a.sog_dk, a.cog_dd, a.heading_dd), (Some(123), Some(3600), Some(5110)));
        assert_eq!(m.dicts.navs[a.nav.unwrap() as usize], "moored");
        assert_eq!(all.iter().find(|p| p.mmsi == 366_000_004).unwrap().lat_e7, None);
    }
}
