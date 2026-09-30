//! `gen-day`: writes a synthetic day of AIS position reports as Parquet, for
//! testing `ais-tracks reduce-day` at scale without a catalog.
//!
//! The mix is meant to look like real silver: most vessels are moored or at
//! anchor reporting every few minutes, some are underway reporting every ten
//! seconds, some are slow class B transmitters; many reports are heard by more
//! than one receiver; a few are bad fixes, invalid values or lack a position.
//! Files hold one time slice each with the vessels in random order, so nothing
//! is sorted by vessel.

use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[derive(Parser, Debug)]
#[command(name = "gen-day", about = "Write a synthetic day of AIS positions as Parquet")]
struct Args {
    /// Output directory (Hive-style day directory is created under it).
    #[arg(long)]
    out: PathBuf,
    /// The UTC day, YYYY-MM-DD.
    #[arg(long, default_value = "2026-03-10")]
    day: NaiveDate,
    /// Approximate number of reports to write, receiver duplicates included.
    #[arg(long, default_value_t = 20_000_000)]
    rows: u64,
    /// Number of vessels; default is chosen from --rows.
    #[arg(long)]
    vessels: Option<u32>,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Length of a time slice, one file per slice, in seconds.
    #[arg(long, default_value_t = 600)]
    slice_s: i64,
}

#[derive(Clone, Copy)]
enum Kind {
    Moored,
    Underway,
    ClassB,
}

struct Vessel {
    mmsi: i64,
    kind: Kind,
    lat: f64,
    lon: f64,
    speed: f64,
    heading: f64,
    next_us: i64,
}

fn interval_s(k: Kind, speed: f64, rng: &mut StdRng) -> f64 {
    match k {
        Kind::Moored => 170.0 + rng.gen_range(0.0..20.0),
        Kind::ClassB => 30.0 + rng.gen_range(0.0..5.0),
        Kind::Underway if speed > 14.0 => 3.0 + rng.gen_range(0.0..4.0),
        Kind::Underway => 10.0 + rng.gen_range(0.0..3.0),
    }
}

fn main() -> Result<()> {
    let a = Args::parse();
    let mut rng = StdRng::seed_from_u64(a.seed);
    // Reports per vessel-day: moored ~490, class B ~2800, underway ~7000; with
    // 1.4 receivers on average.
    let mix = |v: u32| -> Vec<Kind> {
        (0..v)
            .map(|i| match i % 10 {
                0..=5 => Kind::Moored,
                6..=7 => Kind::ClassB,
                _ => Kind::Underway,
            })
            .collect()
    };
    let per_vessel = (0.6 * 490.0 + 0.2 * 2800.0 + 0.2 * 7000.0) * 1.4;
    let n = a.vessels.unwrap_or(((a.rows as f64 / per_vessel) as u32).max(10));
    let day_start = Utc.from_utc_datetime(&a.day.and_hms_opt(0, 0, 0).unwrap()).timestamp() * 1_000_000;
    let day_end = day_start + 86_400 * 1_000_000;

    let mut vessels: Vec<Vessel> = mix(n)
        .into_iter()
        .enumerate()
        .map(|(i, kind)| {
            let speed = match kind {
                Kind::Moored => rng.gen_range(0.0..0.3),
                Kind::ClassB => rng.gen_range(0.0..6.0),
                Kind::Underway => rng.gen_range(6.0..20.0),
            };
            Vessel {
                mmsi: 200_000_000 + (i as i64 * 7919) % 600_000_000,
                kind,
                lat: rng.gen_range(-60.0..60.0),
                lon: rng.gen_range(-170.0..170.0),
                speed,
                heading: rng.gen_range(0.0..360.0),
                next_us: day_start + rng.gen_range(0..200) * 1_000_000,
            }
        })
        .collect();

    let dir = a.out.join(format!(
        "year={}/month={:02}/day={:02}",
        a.day.format("%Y"),
        a.day.format("%m").to_string().parse::<u32>()?,
        a.day.format("%d").to_string().parse::<u32>()?
    ));
    fs::create_dir_all(&dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())), false),
        Field::new("mmsi", DataType::Int64, false),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("sog_knots", DataType::Float64, true),
        Field::new("cog", DataType::Float64, true),
        Field::new("heading_true", DataType::Float64, true),
        Field::new("nav_status", DataType::Utf8, true),
        Field::new("source", DataType::Utf8, false),
        Field::new("station", DataType::Utf8, true),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(128 * 1024)
        .build();

    let (mut total, mut file_no) = (0u64, 0);
    let mut slice_start = day_start;
    while slice_start < day_end {
        let slice_end = (slice_start + a.slice_s * 1_000_000).min(day_end);
        #[derive(Default)]
        struct Cols {
            ts: Vec<i64>,
            mmsi: Vec<i64>,
            lat: Vec<Option<f64>>,
            lon: Vec<Option<f64>>,
            sog: Vec<Option<f64>>,
            cog: Vec<Option<f64>>,
            hd: Vec<Option<f64>>,
            nav: Vec<Option<&'static str>>,
            src: Vec<&'static str>,
            stn: Vec<Option<&'static str>>,
        }
        let mut c = Cols::default();
        for v in vessels.iter_mut() {
            while v.next_us < slice_end {
                let t = v.next_us;
                let dt = interval_s(v.kind, v.speed, &mut rng);
                v.next_us += (dt * 1e6) as i64;
                if rng.gen_bool(0.05) {
                    v.speed = match v.kind {
                        Kind::Moored => rng.gen_range(0.0..0.3),
                        _ => (v.speed + rng.gen_range(-3.0..3.0)).clamp(0.0, 22.0),
                    };
                    v.heading = (v.heading + rng.gen_range(-30.0..30.0)).rem_euclid(360.0);
                }
                let nm = v.speed * dt / 3600.0;
                v.lat = (v.lat + nm * v.heading.to_radians().cos() / 60.0).clamp(-70.0, 70.0);
                v.lon = (v.lon + nm * v.heading.to_radians().sin() / 60.0 / v.lat.to_radians().cos().max(0.2))
                    .clamp(-179.0, 179.0);
                let (mut lat, mut lon, mut sog, mut cog, mut hd) =
                    (Some(v.lat), Some(v.lon), v.speed, v.heading, v.heading);
                let r: f64 = rng.gen();
                if r < 0.0005 {
                    lat = None;
                    lon = None;
                } else if r < 0.0007 {
                    lat = Some((v.lat + rng.gen_range(3.0..8.0)).min(85.0)); // bad fix
                }
                if rng.gen_bool(0.0005) {
                    sog = 102.3;
                }
                if rng.gen_bool(0.0005) {
                    cog = 360.0;
                }
                if rng.gen_bool(0.0005) {
                    hd = 511.0;
                }
                let nav = Some(if v.speed < 0.5 { "moored" } else { "under way using engine" });
                let stn = if rng.gen_bool(0.7) { Some("station-a") } else { None };
                let heard = 1 + (rng.gen_bool(0.3) as usize) + (rng.gen_bool(0.1) as usize);
                for k in 0..heard {
                    c.ts.push(t);
                    c.mmsi.push(v.mmsi);
                    c.lat.push(lat);
                    c.lon.push(lon);
                    c.sog.push(Some(sog));
                    c.cog.push(Some(cog));
                    c.hd.push(Some(hd));
                    c.nav.push(nav);
                    c.src.push(["terrestrial", "satellite", "aggregator"][k]);
                    c.stn.push(if k == 0 { stn } else { Some("station-b") });
                }
            }
        }
        let n_rows = c.ts.len();
        let mut order: Vec<usize> = (0..n_rows).collect();
        order.shuffle(&mut rng);
        let pick = |v: &Vec<Option<f64>>| -> ArrayRef {
            Arc::new(Float64Array::from_iter(order.iter().map(|&i| v[i])))
        };
        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(order.iter().map(|&i| c.ts[i]))
                    .with_timezone("+00:00"),
            ),
            Arc::new(Int64Array::from_iter_values(order.iter().map(|&i| c.mmsi[i]))),
            pick(&c.lat),
            pick(&c.lon),
            pick(&c.sog),
            pick(&c.cog),
            pick(&c.hd),
            Arc::new(StringArray::from_iter(order.iter().map(|&i| c.nav[i]))),
            Arc::new(StringArray::from_iter_values(order.iter().map(|&i| c.src[i]))),
            Arc::new(StringArray::from_iter(order.iter().map(|&i| c.stn[i]))),
        ];
        if n_rows > 0 {
            let batch = RecordBatch::try_new(schema.clone(), cols)?;
            let path = dir.join(format!("part-{file_no:05}.parquet"));
            let mut w = ArrowWriter::try_new(File::create(&path)?, schema.clone(), Some(props.clone()))?;
            w.write(&batch)?;
            w.close()?;
            file_no += 1;
            total += n_rows as u64;
        }
        slice_start = slice_end;
    }
    println!("wrote {total} reports for {n} vessels in {file_no} files under {}", dir.display());
    Ok(())
}
