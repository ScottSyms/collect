//! The write path against a real Iceberg table on the local filesystem:
//! `DayWriter` must produce Parquet files in the right partition holding
//! exactly the rows written, and the state fold must recover each vessel's
//! last stream row from what was written.

use std::collections::HashMap;

use ais_tracks::output::{day_partition, DayWriter};
use ais_tracks::reduce::{thin_points_schema, Dicts, RawPoint, Rules, ThinOpts};
use ais_tracks::reduce_day::reduce_all;
use ais_tracks::source::fold_states;
use collect_core::iceberg::partition_spec_for;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadataBuilder;
use iceberg::table::Table;
use iceberg::{TableCreation, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn points(n_vessels: u32, per_vessel: i64) -> Vec<RawPoint> {
    let base = 1_772_000_000_000_000i64 - 1_772_000_000_000_000i64 % 86_400_000_000; // a UTC midnight
    let mut v = Vec::new();
    for m in 0..n_vessels {
        for i in 0..per_vessel {
            v.push(RawPoint {
                ts_us: base + i * 10_000_000,
                mmsi: 366_000_000 + m,
                lat_e7: Some(100_000_000 + (i as i32) * 300),
                lon_e7: Some(200_000_000 + m as i32 * 1000),
                sog_dk: Some(120),
                cog_dd: Some(900),
                heading_dd: None,
                nav: Some(0),
                source: 0,
                station: None,
            });
        }
    }
    v
}

fn local_table(dir: &std::path::Path) -> Table {
    let schema = thin_points_schema();
    let spec = partition_spec_for(&schema, "day").unwrap().build().unwrap();
    let creation = TableCreation::builder()
        .name("track_points".to_string())
        .location(format!("file://{}/track_points", dir.display()))
        .schema(schema)
        .partition_spec(spec)
        .build();
    let metadata = TableMetadataBuilder::from_table_creation(creation)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    Table::builder()
        .metadata(metadata)
        .identifier(TableIdent::from_strs(["ns", "track_points"]).unwrap())
        .file_io(FileIO::new_with_fs())
        .metadata_location(format!("file://{}/track_points/metadata/v1.json", dir.display()))
        .build()
        .unwrap()
}

#[tokio::test]
async fn day_writer_writes_partitioned_parquet_with_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let table = local_table(dir.path());
    let dicts = Dicts::new(vec!["a".into()], vec![], vec!["moored".into()]);
    let pts = points(20, 500);
    let n = pts.len();
    let batches = reduce_all(pts, &dicts, &HashMap::new(), &Rules::default(), &ThinOpts::off()).unwrap();

    let day = 20_500; // days since 1970-01-01
    let mut w = DayWriter::new(&table, day, &["mmsi"]).await.unwrap();
    // Feed it several small batches, as the streaming reducer does.
    for b in &batches {
        for start in (0..b.num_rows()).step_by(1500) {
            let len = 1500.min(b.num_rows() - start);
            w.write(&b.slice(start, len)).await.unwrap();
        }
    }
    assert_eq!(w.rows, n);
    let files = w.finish().await.unwrap();
    assert!(!files.is_empty());

    let expect = day_partition(day);
    let mut read = 0usize;
    for f in &files {
        assert_eq!(f.partition(), &expect, "file is in the wrong partition");
        let path = f.file_path().strip_prefix("file://").unwrap_or(f.file_path());
        let r = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            let b = b.unwrap();
            assert_eq!(b.num_columns(), thin_points_schema().as_struct().fields().len());
            read += b.num_rows();
        }
    }
    assert_eq!(read, n, "every row written is in the files");
    assert_eq!(files.iter().map(|f| f.record_count()).sum::<u64>(), n as u64);
}

#[tokio::test]
async fn last_stream_rows_are_recovered_from_written_batches() {
    let dicts = Dicts::new(vec!["a".into()], vec![], vec!["moored".into()]);
    let pts = points(5, 200);
    let last_lat = 100_000_000 + 199 * 300;
    let batches = reduce_all(pts, &dicts, &HashMap::new(), &Rules::default(), &ThinOpts::default()).unwrap();
    let mut states = HashMap::new();
    for b in &batches {
        fold_states(&mut states, b).unwrap();
    }
    assert_eq!(states.len(), 5);
    for (mmsi, s) in &states {
        assert!((366_000_000..366_000_005).contains(mmsi));
        assert_eq!((s.lat * 1e7).round() as i32, last_lat, "the last row of the day is always kept");
    }
}
