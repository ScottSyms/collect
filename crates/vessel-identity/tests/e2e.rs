//! Ingest and reconcile against real Iceberg tables on the local filesystem:
//! bootstrap, repeat ingestion, an updated publication, a second source
//! with overlapping vessels, conflicts, history and quarantine.

mod support;

use std::path::Path;

use collect_core::iceberg::IcebergConfig;
use collect_maint::commit::RestClient;
use rust_xlsxwriter::Workbook;
use support::MockCatalog;

use vessel_identity::config::Catalogue;
use vessel_identity::ingest::{ingest, Options, Outcome};
use vessel_identity::load;
use vessel_identity::registry;
use vessel_identity::tables::{Store, ASSERTIONS, INGESTION_RUNS, REDIRECTS, SOURCE_RECORDS, VESSELS, VESSEL_HISTORY, VESSEL_SUBJECTS};

const HEADER: &[&str] = &[
    "Official Number", "Vessel Name", "IMO Vessel Number", "Hull Number", "Year of Build", "Year of Latest Rebuild",
    "Port of Registry", "Certificate Issuing date or Registration date", "Vessel Descriptor", "Gross Tonnage",
    "Net Tonnage", "Construction Type", "Construction Material", "Length", "Breadth", "Depth", "Engine Type",
    "Number of Engines", "Propulsion Type", "Speed (Knots)", "Propulsion Method", "Engine Propulsion Power",
];

/// (official number, name, IMO, year built, length).
type Vessel<'a> = (&'a str, &'a str, &'a str, &'a str, &'a str);

fn write_register(dir: &Path, vessels: &[Vessel]) {
    std::fs::create_dir_all(dir).unwrap();
    for f in std::fs::read_dir(dir).unwrap() {
        std::fs::remove_file(f.unwrap().path()).unwrap();
    }
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    for (j, h) in HEADER.iter().enumerate() {
        ws.write_string(1, j as u16 + 1, *h).unwrap();
    }
    for (i, (on, name, imo, year, len)) in vessels.iter().enumerate() {
        let row = [*on, *name, *imo, "", *year, "", "HALIFAX", "2019-05-01", "BULK CARRIER", "2400", "1100", "", "STEEL", *len, "12", "6", "DIESEL", "1", "", "12", "SINGLE SCREW", "900"];
        for (j, v) in row.iter().enumerate() {
            if !v.is_empty() {
                ws.write_string(i as u32 + 2, j as u16 + 1, *v).unwrap();
            }
        }
    }
    wb.save(dir.join("large-vessel-registry_dataset_en.xlsx")).unwrap();
}

fn write_iacs(dir: &Path, rows: &str) {
    std::fs::create_dir_all(dir).unwrap();
    let text = format!("IMO;SHIP NAME;CLASS;DATE OF SURVEY;DATE OF NEXT SURVEY;DATE OF LATEST STATUS;STATUS;REASON FOR THE STATUS\n{rows}");
    std::fs::write(dir.join("EquasisToIACS_20261016_981.csv"), text).unwrap();
}

/// The built-in catalogue with IACS approved, as an operator would set it.
fn config_dir(dir: &Path) -> Catalogue {
    std::fs::create_dir_all(dir).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/sources");
    for f in std::fs::read_dir(root).unwrap() {
        let p = f.unwrap().path();
        let text = std::fs::read_to_string(&p)
            .unwrap()
            .replace("access_status = \"pending\"", "access_status = \"approved\"")
            .replace("permitted_uses = [\"access\", \"automated_download\"]", "permitted_uses = [\"access\", \"automated_download\", \"retain_history\", \"combine\"]");
        std::fs::write(dir.join(p.file_name().unwrap()), text).unwrap();
    }
    Catalogue::load(Some(dir)).unwrap()
}

fn opts(dir: &Path, scratch: &Path) -> Options {
    Options {
        apply: true,
        force: false,
        accept_removals: false,
        source_dir: Some(dir.to_path_buf()),
        scratch: scratch.to_path_buf(),
        user_agent: None,
        raw_archive: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_and_reconcile_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let cat = MockCatalog::new(&tmp.path().join("warehouse"));
    let url = cat.serve().await;
    let config = IcebergConfig {
        catalog_uri: url,
        warehouse: "w".into(),
        namespace: "identity".into(),
        table_prefix: None,
        token: None,
        sigv4: false,
    };
    let rest = RestClient::connect(&config).await.unwrap();
    let store = Store { catalog: cat, rest, config };
    let catalogue = config_dir(&tmp.path().join("config"));
    let (tc_dir, iacs_dir, scratch) = (tmp.path().join("tc"), tmp.path().join("iacs"), tmp.path().join("scratch"));

    // 1. Bootstrap: init, then a complete TC register (25 vessels, one
    // with an IMO number).
    store.init().await.unwrap();
    let mut fleet: Vec<(String, String)> = (0..24).map(|i| (format!("{}", 800000 + i), format!("HARBOUR TUG {i}"))).collect();
    fleet.push(("834567".into(), "ATLANTIC STAR".into()));
    let vessels = |fleet: &[(String, String)], imo: &'static str| -> Vec<(String, String, String)> {
        fleet.iter().map(|(on, n)| (on.clone(), n.clone(), if on == "834567" { imo.to_string() } else { String::new() })).collect()
    };
    let as_rows = |v: &[(String, String, String)]| -> Vec<(String, String, String)> { v.to_vec() };
    let write = |v: &[(String, String, String)]| {
        let rows: Vec<Vessel> = v.iter().map(|(a, b, c)| (a.as_str(), b.as_str(), c.as_str(), "2005", "36.8")).collect();
        write_register(&tc_dir, &rows);
    };
    write(&as_rows(&vessels(&fleet, "9321483")));
    let o = ingest(Some(&store), &catalogue, "tc_large_vessels", &opts(&tc_dir, &scratch)).await.unwrap();
    assert_eq!(o, Outcome::Succeeded);
    let bronze = store.read(SOURCE_RECORDS, Some("tc_large_vessels")).await.unwrap();
    assert_eq!(bronze.iter().map(|b| b.num_rows()).sum::<usize>(), 25);

    // 2. The same file again: recognised by its hash, nothing added.
    let o = ingest(Some(&store), &catalogue, "tc_large_vessels", &opts(&tc_dir, &scratch)).await.unwrap();
    assert_eq!(o, Outcome::Unchanged);
    // Forced: parsed again, but every record is unchanged.
    let mut forced = opts(&tc_dir, &scratch);
    forced.force = true;
    assert_eq!(ingest(Some(&store), &catalogue, "tc_large_vessels", &forced).await.unwrap(), Outcome::Succeeded);
    let bronze = store.read(SOURCE_RECORDS, Some("tc_large_vessels")).await.unwrap();
    assert_eq!(bronze.iter().map(|b| b.num_rows()).sum::<usize>(), 25, "identical records add no rows");

    // 3. A second source with the same hull (by IMO), a different name, and
    // a vessel TC doesn't have.
    write_iacs(
        &iacs_dir,
        "9321483;NORDIC STAR(11/10/26);LRS;20250101;20300101;20250101;Delivered;\n9074729;OTHER SHIP(11/10/26);NV;20250101;20300101;20250101;Delivered;\n",
    );
    assert_eq!(ingest(Some(&store), &catalogue, "iacs_vessels_in_class", &opts(&iacs_dir, &scratch)).await.unwrap(), Outcome::Succeeded);

    let stats = registry::reconcile(&store, &catalogue, true).await.unwrap();
    assert_eq!(stats.vessels, 26, "25 TC vessels + 1 IACS-only; the shared hull is one vessel");
    assert_eq!(stats.subjects, 27);
    let vessels_table = load_vessels(&store).await;
    let star = vessels_table.iter().find(|v| v.1.as_deref() == Some("9321483")).unwrap().clone();
    assert_eq!(star.2, "confirmed");
    let conflicts = load::conflicts(&store.read(vessel_identity::tables::CONFLICTS, None).await.unwrap()).unwrap();
    assert!(conflicts.iter().any(|c| c.conflict_type == "NAME" && c.vessel_id == star.0), "both names kept, disagreement reported");

    // Reconciling again changes no ids.
    let again = registry::reconcile(&store, &catalogue, true).await.unwrap();
    assert_eq!((again.created, again.merges, again.splits), (0, 0, 0));
    assert_eq!(load_vessels(&store).await.iter().map(|v| v.0.clone()).collect::<Vec<_>>(), vessels_table.iter().map(|v| v.0.clone()).collect::<Vec<_>>());

    // 4. An updated publication: ATLANTIC STAR renamed, one tug gone.
    let mut next = fleet.clone();
    next.retain(|(on, _)| on != "800000");
    for v in next.iter_mut() {
        if v.0 == "834567" {
            v.1 = "NORDIC STAR".into();
        }
    }
    write(&as_rows(&vessels(&next, "9321483")));
    assert_eq!(ingest(Some(&store), &catalogue, "tc_large_vessels", &opts(&tc_dir, &scratch)).await.unwrap(), Outcome::Succeeded);
    let assertions = load::assertions(&store.read(ASSERTIONS, Some("tc_large_vessels")).await.unwrap()).unwrap();
    let names: Vec<_> = assertions.iter().filter(|a| a.subject_id == "834567" && a.attribute == "name").collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|a| a.value_normalized.as_deref() == Some("ATLANTIC STAR") && a.assertion_status == "superseded"));
    assert!(assertions.iter().any(|a| a.subject_id == "800000" && a.assertion_status == "withdrawn"));

    registry::reconcile(&store, &catalogue, true).await.unwrap();
    let after = load_vessels(&store).await;
    let star2 = after.iter().find(|v| v.1.as_deref() == Some("9321483")).unwrap();
    assert_eq!(star2.0, star.0, "same vessel id after the rename");
    let history = store.read(VESSEL_HISTORY, None).await.unwrap();
    let mut star_names = Vec::new();
    for b in &history {
        let r = vessel_identity::columns::Reader::new(b);
        let (vid, at, val, cur) = (r.str("vessel_id").unwrap(), r.str("attribute").unwrap(), r.str("value").unwrap(), r.str("is_current").unwrap());
        for i in 0..r.rows() {
            if vid(i).as_deref() == Some(star.0.as_str()) && at(i).as_deref() == Some("name") {
                star_names.push((val(i).unwrap(), cur(i).unwrap()));
            }
        }
    }
    star_names.sort();
    assert_eq!(star_names, [("ATLANTIC STAR".to_string(), "false".to_string()), ("NORDIC STAR".to_string(), "true".to_string())]);
    // The removed tug keeps its vessel (and id); removal isn't scrapping.
    assert_eq!(after.len(), 26);

    // 5. A truncated file is quarantined, not applied.
    write(&as_rows(&vessels(&next[..3], "9321483")));
    assert_eq!(ingest(Some(&store), &catalogue, "tc_large_vessels", &opts(&tc_dir, &scratch)).await.unwrap(), Outcome::Quarantined);
    let assertions2 = load::assertions(&store.read(ASSERTIONS, Some("tc_large_vessels")).await.unwrap()).unwrap();
    assert_eq!(assertions2.len(), assertions.len(), "nothing written");

    // 6. A manual decision splits the merged hull back apart, restoring ids.
    let subjects = load::subjects(&store.read(VESSEL_SUBJECTS, None).await.unwrap()).unwrap();
    assert!(subjects.iter().filter(|s| s.vessel_id == star.0).count() == 2);
    registry::decide(Some(&store), "KEEP_APART", &["tc_large_vessels/834567".into(), "iacs_vessels_in_class/9321483".into()], None, "test", "different hulls")
        .await
        .unwrap();
    let split = registry::reconcile(&store, &catalogue, true).await.unwrap();
    assert_eq!(split.vessels, 27);
    // They were one vessel from the start, so one side gets a new id (SPLIT)
    // and the side with more persistent identifiers keeps the old one.
    assert_eq!((split.splits, split.restores), (1, 0));
    let subjects = load::subjects(&store.read(VESSEL_SUBJECTS, None).await.unwrap()).unwrap();
    let tc = subjects.iter().find(|s| s.subject_id == "834567").unwrap();
    assert_eq!(tc.vessel_id, star.0, "TC has the registration and the IMO number");
    assert!(load::redirects(&store.read(REDIRECTS, None).await.unwrap()).unwrap().is_empty());

    let runs = load::runs(&store.read(INGESTION_RUNS, None).await.unwrap()).unwrap();
    let statuses: Vec<&str> = runs.iter().map(|r| r.status.as_str()).collect();
    assert!(statuses.contains(&"unchanged") && statuses.contains(&"quarantined"));
}

/// (vessel_id, imo_number, identity_status), sorted by id.
async fn load_vessels<C: iceberg::Catalog>(store: &Store<C>) -> Vec<(String, Option<String>, String)> {
    let mut out = Vec::new();
    for b in &store.read(VESSELS, None).await.unwrap() {
        let r = vessel_identity::columns::Reader::new(b);
        let (id, imo, st) = (r.str("vessel_id").unwrap(), r.str("imo_number").unwrap(), r.str("identity_status").unwrap());
        for i in 0..r.rows() {
            out.push((id(i).unwrap(), imo(i), st(i).unwrap()));
        }
    }
    out.sort();
    out
}
