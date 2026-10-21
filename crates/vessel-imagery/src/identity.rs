//! The vessels to find photos of, read from the identity registry
//! (`identity.vessels` and `identity.vessel_identifiers`). Photos are tied
//! to vessels by persistent identifiers only (IMO, MMSI, the registry's own
//! vessel id), never by name.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use iceberg::Catalog;
use vessel_identity::columns::Reader;
use vessel_identity::tables::{Store as IdentityStore, REDIRECTS, VESSELS, VESSEL_IDENTIFIERS};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Target {
    pub vessel_id: String,
    pub name: Option<String>,
    /// The preferred IMO number, then any others the vessel has carried.
    pub imos: Vec<String>,
    pub mmsis: Vec<String>,
}

impl Target {
    pub fn imo(&self) -> Option<&str> {
        self.imos.first().map(String::as_str)
    }
}

/// Every vessel with its IMO numbers and MMSIs.
pub struct Targets {
    pub vessels: Vec<Target>,
    by_imo: HashMap<String, usize>,
    by_mmsi: HashMap<String, Vec<usize>>,
    by_id: HashMap<String, usize>,
    redirects: HashMap<String, String>,
}

impl Targets {
    pub async fn load<C: Catalog>(store: &IdentityStore<C>) -> Result<Self> {
        let vessels = store.read(VESSELS, None).await?;
        let identifiers = store.read(VESSEL_IDENTIFIERS, None).await?;
        let redirects = store.read(REDIRECTS, None).await?;
        Self::from_batches(&vessels, &identifiers, &redirects)
    }

    pub fn from_batches(vessels: &[RecordBatch], identifiers: &[RecordBatch], redirects: &[RecordBatch]) -> Result<Self> {
        let mut found: BTreeMap<String, Target> = BTreeMap::new();
        for b in vessels {
            let r = Reader::new(b);
            let (id, name, imo, mmsi) = (r.str("vessel_id")?, r.str("preferred_name")?, r.str("imo_number")?, r.str("preferred_mmsi")?);
            for i in 0..r.rows() {
                let Some(vid) = id(i) else { continue };
                let t = found.entry(vid.clone()).or_insert_with(|| Target { vessel_id: vid, ..Default::default() });
                t.name = name(i);
                t.imos.extend(imo(i).filter(|s| valid_imo(s)));
                t.mmsis.extend(mmsi(i));
            }
        }
        for b in identifiers {
            let r = Reader::new(b);
            let (id, ty, val) = (r.str("vessel_id")?, r.str("identifier_type")?, r.str("identifier_value")?);
            for i in 0..r.rows() {
                let (Some(vid), Some(ty), Some(val)) = (id(i), ty(i), val(i)) else { continue };
                let Some(t) = found.get_mut(&vid) else { continue };
                match ty.as_str() {
                    "IMO" if valid_imo(&val) && !t.imos.contains(&val) => t.imos.push(val),
                    "MMSI" if !t.mmsis.contains(&val) => t.mmsis.push(val),
                    _ => {}
                }
            }
        }
        let mut redirect = HashMap::new();
        for b in redirects {
            let r = Reader::new(b);
            let (from, to) = (r.str("retired_vessel_id")?, r.str("surviving_vessel_id")?);
            let active = b.column_by_name("active").and_then(|c| c.as_any().downcast_ref::<arrow::array::BooleanArray>().cloned());
            for i in 0..r.rows() {
                if active.as_ref().is_some_and(|a| !a.is_null(i) && !a.value(i)) {
                    continue;
                }
                if let (Some(f), Some(t)) = (from(i), to(i)) {
                    redirect.insert(f, t);
                }
            }
        }
        Ok(Self::new(found.into_values().collect(), redirect))
    }

    pub fn new(vessels: Vec<Target>, redirects: HashMap<String, String>) -> Self {
        let mut by_imo = HashMap::new();
        let mut by_mmsi: HashMap<String, Vec<usize>> = HashMap::new();
        let mut by_id = HashMap::new();
        for (i, v) in vessels.iter().enumerate() {
            by_id.insert(v.vessel_id.clone(), i);
            for imo in &v.imos {
                by_imo.entry(imo.clone()).or_insert(i);
            }
            for m in &v.mmsis {
                by_mmsi.entry(m.clone()).or_default().push(i);
            }
        }
        Targets { vessels, by_imo, by_mmsi, by_id, redirects }
    }

    pub fn by_imo(&self, imo: &str) -> Option<&Target> {
        self.by_imo.get(imo.trim().trim_start_matches("IMO").trim()).map(|&i| &self.vessels[i])
    }

    /// The vessel with this MMSI, when exactly one has carried it (MMSIs get
    /// reused; an ambiguous one ties a photo to nothing).
    pub fn by_mmsi(&self, mmsi: &str) -> Option<&Target> {
        match self.by_mmsi.get(mmsi.trim()).map(Vec::as_slice) {
            Some([i]) => Some(&self.vessels[*i]),
            _ => None,
        }
    }

    /// The vessel with this id, following merges.
    pub fn by_id(&self, id: &str) -> Option<&Target> {
        let mut cur = id.to_string();
        let mut seen = BTreeSet::new();
        while let Some(next) = self.redirects.get(&cur) {
            if !seen.insert(cur.clone()) {
                break;
            }
            cur = next.clone();
        }
        self.by_id.get(&cur).map(|&i| &self.vessels[i])
    }
}

/// Seven digits whose check digit holds.
pub fn valid_imo(s: &str) -> bool {
    let d: Vec<u32> = s.chars().filter_map(|c| c.to_digit(10)).collect();
    if d.len() != 7 || s.len() != 7 {
        return false;
    }
    let sum: u32 = d[..6].iter().zip((2..=7).rev()).map(|(a, w)| a * w).sum();
    sum % 10 == d[6]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imo_check_digit() {
        assert!(valid_imo("9074729"));
        assert!(!valid_imo("9074728"));
        assert!(!valid_imo("907472"));
    }

    #[test]
    fn lookups() {
        let t = Targets::new(
            vec![
                Target { vessel_id: "a".into(), imos: vec!["9074729".into()], mmsis: vec!["316001325".into()], ..Default::default() },
                Target { vessel_id: "b".into(), mmsis: vec!["316001325".into(), "235000001".into()], ..Default::default() },
            ],
            HashMap::from([("old".into(), "b".into())]),
        );
        assert_eq!(t.by_imo("IMO 9074729").unwrap().vessel_id, "a");
        assert!(t.by_mmsi("316001325").is_none(), "shared MMSI is ambiguous");
        assert_eq!(t.by_mmsi("235000001").unwrap().vessel_id, "b");
        assert_eq!(t.by_id("old").unwrap().vessel_id, "b");
    }
}
