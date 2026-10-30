//! Asking the System 1 model about each photo, and reading its answers
//! back: into per-vessel profiles, and against hand labels (the eval).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use iceberg::Catalog;
use serde::Deserialize;
use vessel_identity::columns::Reader;

use crate::images;
use crate::questions::{Question, QuestionSet, QuestionType, Role};
use crate::systemone::{self, Answer, Client};
use crate::tables::{string_partition, ImageRow, ObservationRow, ProfileRow, Store, IMAGES, OBSERVATIONS, OBSERVATION_COLUMNS};

const FLUSH_EVERY: usize = 20;

/// `image_observations` rows read back.
pub fn observation_rows(batches: &[RecordBatch]) -> Result<Vec<ObservationRow>> {
    let mut out = Vec::new();
    for b in batches {
        let r = Reader::new(b);
        let s = |n: &str| r.str(n);
        let (img, vid, model, ver, q, a, ru) = (s("image_id")?, s("vessel_id")?, s("model_id")?, s("question_set_version")?, s("question")?, s("answer")?, s("runner_up")?);
        let p = b.column_by_name("probability").context("no probability")?.clone();
        let rp = b.column_by_name("runner_up_probability").context("no runner_up_probability")?.clone();
        let f = |c: &arrow::array::ArrayRef, i: usize| {
            let a = c.as_any().downcast_ref::<arrow::array::Float64Array>()?;
            (!a.is_null(i)).then(|| a.value(i))
        };
        let cal = b.column_by_name("calibrated").and_then(|c| c.as_any().downcast_ref::<BooleanArray>().cloned()).context("no calibrated")?;
        let at = r.ts("observed_at")?;
        for i in 0..r.rows() {
            out.push(ObservationRow {
                image_id: img(i).unwrap_or_default(),
                vessel_id: vid(i).unwrap_or_default(),
                model_id: model(i).unwrap_or_default(),
                question_set_version: ver(i).unwrap_or_default(),
                question: q(i).unwrap_or_default(),
                answer: a(i).unwrap_or_default(),
                probability: f(&p, i),
                runner_up: ru(i),
                runner_up_probability: f(&rp, i),
                calibrated: !cal.is_null(i) && cal.value(i),
                observed_at: at(i).unwrap_or_default(),
            });
        }
    }
    Ok(out)
}

/// The observation rows for one image's answers.
pub fn to_rows(img: &ImageRow, qs: &QuestionSet, model_id: &str, answers: &BTreeMap<String, Answer>) -> Vec<ObservationRow> {
    let now = Utc::now();
    qs.questions
        .iter()
        .filter_map(|q| {
            let a = answers.get(&q.id)?;
            let (top, second) = a.ranked();
            let (answer, probability) = match (q.kind, a.value) {
                (QuestionType::Score, Some(v)) => (format!("{v:.3}"), None),
                _ => {
                    let (l, p) = top?;
                    (l, Some(p))
                }
            };
            let keep_second = probability.is_some_and(|p| p < qs.runner_up_below);
            let (runner_up, runner_up_probability) = match second.filter(|_| keep_second) {
                Some((l, p)) => (Some(l), Some(p)),
                None => (None, None),
            };
            Some(ObservationRow {
                image_id: img.image_id.clone(),
                vessel_id: img.vessel_id.clone(),
                model_id: model_id.to_string(),
                question_set_version: qs.version.clone(),
                question: q.id.clone(),
                answer,
                probability,
                runner_up,
                runner_up_probability,
                calibrated: a.calibrated,
                observed_at: now,
            })
        })
        .collect()
}

pub struct DescribeOptions<'a> {
    pub apply: bool,
    pub limit: Option<usize>,
    pub model: &'a str,
    /// Leave restricted images out (for a model hosted somewhere their
    /// licence doesn't reach).
    pub skip_restricted: bool,
}

pub async fn describe<C: Catalog>(store: &Store<C>, client: &Client, qs: &QuestionSet, opts: &DescribeOptions<'_>) -> Result<()> {
    let imgs = images::rows(&store.read(IMAGES).await?)?;
    let done: HashSet<(String, String)> = observation_rows(&store.read(OBSERVATIONS).await?)?
        .into_iter()
        .filter(|o| o.question_set_version == qs.version)
        .map(|o| (o.image_id, o.model_id))
        .collect();
    let mut seen = HashSet::new();
    let todo: Vec<&ImageRow> = imgs
        .iter()
        .filter(|i| !(opts.skip_restricted && i.restricted))
        .filter(|i| !done.contains(&(i.image_id.clone(), opts.model.to_string())))
        // Each (image, vessel) is asked once; an image on two vessels is asked twice.
        .filter(|i| seen.insert((i.image_id.clone(), i.vessel_id.clone())))
        .take(opts.limit.unwrap_or(usize::MAX))
        .collect();
    println!("{} image(s) to describe with {} (question set {}, {} questions)", todo.len(), opts.model, qs.version, qs.questions.len());
    if !opts.apply {
        return Ok(());
    }
    let table = store.table(IMAGES).await?;
    let questions: Vec<&Question> = qs.questions.iter().collect();
    let (mut pending, mut written, mut failed) = (Vec::new(), 0usize, 0usize);
    for (n, img) in todo.iter().enumerate() {
        let result = async {
            let bytes = images::load(&table, &img.object_uri).await?;
            let prepared = systemone::prepare(&bytes)?;
            let resp = client.ask(&systemone::request(opts.model, &prepared, &questions)).await?;
            let model_id = opts.model.to_string();
            anyhow::Ok(to_rows(img, qs, &model_id, &resp.answers))
        }
        .await;
        match result {
            Ok(rows) => {
                println!("  [{}/{}] {} {}: {} answer(s)", n + 1, todo.len(), img.imo_number.as_deref().unwrap_or("-"), &img.image_id[..12], rows.len());
                pending.extend(rows);
            }
            Err(e) => {
                failed += 1;
                eprintln!("  [{}/{}] {}: {e:#}", n + 1, todo.len(), &img.image_id[..12]);
            }
        }
        if pending.len() >= FLUSH_EVERY * qs.questions.len() {
            written += store.append(OBSERVATIONS, OBSERVATION_COLUMNS, &pending, Some(string_partition(&qs.version))).await?;
            pending.clear();
        }
    }
    written += store.append(OBSERVATIONS, OBSERVATION_COLUMNS, &pending, Some(string_partition(&qs.version))).await?;
    println!("{written} observation(s) written; {failed} image(s) failed");
    Ok(())
}

pub struct ProfileOptions {
    /// Count images whose licence couldn't be classified.
    pub include_unknown_licence: bool,
}

/// Images whose gate answers all pass.
pub fn passing_images<'a>(qs: &QuestionSet, obs: &'a [ObservationRow]) -> HashSet<(&'a str, &'a str)> {
    let gates: Vec<&Question> = qs.gates().collect();
    let mut passed: HashMap<(&str, &str), usize> = HashMap::new();
    for o in obs {
        let Some(g) = gates.iter().find(|g| g.id == o.question) else { continue };
        if g.pass.contains(&o.answer) && o.probability.unwrap_or(0.0) >= qs.min_probability {
            *passed.entry((o.image_id.as_str(), o.vessel_id.as_str())).or_default() += 1;
        }
    }
    passed.into_iter().filter(|(_, n)| *n == gates.len()).map(|(k, _)| k).collect()
}

/// Per-vessel visual profile from one model's answers to one question set.
pub fn profile(qs: &QuestionSet, model_id: &str, imgs: &[ImageRow], obs: &[ObservationRow], opts: &ProfileOptions) -> Vec<ProfileRow> {
    let usable: HashMap<(&str, &str), &ImageRow> = imgs
        .iter()
        .filter(|i| opts.include_unknown_licence || i.licence_class != "unknown")
        .map(|i| ((i.image_id.as_str(), i.vessel_id.as_str()), i))
        .collect();
    let obs: Vec<&ObservationRow> = obs.iter().filter(|o| o.model_id == model_id && o.question_set_version == qs.version).collect();
    let owned: Vec<ObservationRow> = obs.iter().map(|o| (*o).clone()).collect();
    let pass = passing_images(qs, &owned);
    // (vessel, question) -> answers with their probability and image
    let mut by: BTreeMap<(&str, &str), Vec<(&ObservationRow, &ImageRow)>> = BTreeMap::new();
    for o in &obs {
        let key = (o.image_id.as_str(), o.vessel_id.as_str());
        let (Some(img), true) = (usable.get(&key), pass.contains(&key)) else { continue };
        let Some(q) = qs.get(&o.question) else { continue };
        if q.role == Role::Gate || !q.enabled {
            continue;
        }
        if q.kind != QuestionType::Score && o.probability.unwrap_or(0.0) < qs.min_probability {
            continue;
        }
        by.entry((o.vessel_id.as_str(), o.question.as_str())).or_default().push((o, img));
    }
    let now = Utc::now();
    let mut out = Vec::new();
    for ((vessel, question), answers) in by {
        let q = qs.get(question).expect("filtered above");
        let total = answers.len() as i32;
        let imo = answers[0].1.imo_number.clone();
        let base = ProfileRow {
            vessel_id: vessel.to_string(),
            imo_number: imo,
            attribute: question.to_string(),
            total_images: total,
            model_id: model_id.to_string(),
            question_set_version: qs.version.clone(),
            updated_at: now,
            ..Default::default()
        };
        if q.kind == QuestionType::Score {
            let vals: Vec<f64> = answers.iter().filter_map(|(o, _)| o.answer.parse::<f64>().ok()).collect();
            if vals.is_empty() {
                continue;
            }
            let mean = vals.iter().sum::<f64>() / vals.len() as f64;
            let sd = (vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64).sqrt();
            out.push(ProfileRow {
                value: format!("{mean:.2}"),
                support_images: vals.len() as i32,
                mean_probability: 1.0 - sd.min(1.0),
                conflicting: sd > 0.2,
                restricted_only: answers.iter().all(|(_, i)| i.restricted),
                ..base
            });
            continue;
        }
        let mut tally: BTreeMap<&str, (i32, f64, bool)> = BTreeMap::new();
        for (o, img) in &answers {
            let e = tally.entry(o.answer.as_str()).or_insert((0, 0.0, true));
            e.0 += 1;
            e.1 += o.probability.unwrap_or(0.0);
            e.2 &= img.restricted;
        }
        let mut ranked: Vec<(&str, (i32, f64, bool))> = tally.into_iter().collect();
        ranked.sort_by(|a, b| b.1 .0.cmp(&a.1 .0).then(b.1 .1.total_cmp(&a.1 .1)));
        let (value, (support, psum, restricted_only)) = ranked[0];
        let conflicting = ranked.get(1).is_some_and(|(_, (n, _, _))| *n * 2 >= support);
        out.push(ProfileRow {
            value: value.to_string(),
            support_images: support,
            mean_probability: psum / support as f64,
            conflicting,
            restricted_only,
            ..base
        });
    }
    out
}

#[derive(Debug, Deserialize)]
pub struct Label {
    pub image_id: String,
    pub question: String,
    pub expected: String,
}

/// Per question: (labelled, answered, correct).
pub fn score(labels: &[Label], obs: &[ObservationRow], model_id: &str, version: &str) -> BTreeMap<String, (usize, usize, usize)> {
    let answers: HashMap<(&str, &str), &str> = obs
        .iter()
        .filter(|o| o.model_id == model_id && o.question_set_version == version)
        .map(|o| ((o.image_id.as_str(), o.question.as_str()), o.answer.as_str()))
        .collect();
    let mut out: BTreeMap<String, (usize, usize, usize)> = BTreeMap::new();
    for l in labels {
        let e = out.entry(l.question.clone()).or_default();
        e.0 += 1;
        if let Some(a) = answers.get(&(l.image_id.as_str(), l.question.as_str())) {
            e.1 += 1;
            if a.eq_ignore_ascii_case(l.expected.trim()) {
                e.2 += 1;
            }
        }
    }
    out
}

pub fn read_labels(path: &Path) -> Result<Vec<Label>> {
    let mut r = csv::ReaderBuilder::new().trim(csv::Trim::All).comment(Some(b'#')).from_path(path).with_context(|| format!("reading {}", path.display()))?;
    r.deserialize().collect::<Result<Vec<Label>, _>>().context("parsing labels")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qs() -> QuestionSet {
        QuestionSet::parse(
            r#"
            version = "t"
            min_probability = 0.5
            runner_up_below = 0.6
            [[questions]]
            id = "single_ship"
            role = "gate"
            type = "yes_no"
            text = "?"
            pass = ["yes"]
            [[questions]]
            id = "hull_colour"
            type = "choice"
            text = "?"
            criteria = ["red", "blue", "black"]
            [[questions]]
            id = "size"
            type = "score"
            text = "?"
            criteria = ["small", "large"]
        "#,
        )
        .unwrap()
    }

    fn img(id: &str, restricted: bool) -> ImageRow {
        ImageRow { image_id: id.into(), vessel_id: "v".into(), licence_class: if restricted { "restricted" } else { "cc_by" }.into(), restricted, ..Default::default() }
    }

    fn o(img: &str, q: &str, a: &str, p: Option<f64>) -> ObservationRow {
        ObservationRow { image_id: img.into(), vessel_id: "v".into(), model_id: "m".into(), question_set_version: "t".into(), question: q.into(), answer: a.into(), probability: p, ..Default::default() }
    }

    #[test]
    fn profiles_by_majority_behind_the_gate() {
        let imgs = vec![img("1", false), img("2", true), img("3", false), img("4", false)];
        let obs = vec![
            o("1", "single_ship", "yes", Some(0.9)),
            o("2", "single_ship", "yes", Some(0.8)),
            o("3", "single_ship", "yes", Some(0.7)),
            o("4", "single_ship", "no", Some(0.9)),
            o("1", "hull_colour", "red", Some(0.8)),
            o("2", "hull_colour", "red", Some(0.6)),
            o("3", "hull_colour", "blue", Some(0.7)),
            o("4", "hull_colour", "black", Some(0.99)), // gated out
            o("1", "size", "0.700", None),
            o("3", "size", "0.800", None),
        ];
        let p = profile(&qs(), "m", &imgs, &obs, &ProfileOptions { include_unknown_licence: false });
        let hull = p.iter().find(|r| r.attribute == "hull_colour").unwrap();
        assert_eq!((hull.value.as_str(), hull.support_images, hull.total_images), ("red", 2, 3));
        assert!(hull.conflicting, "blue has half red's support");
        assert!(!hull.restricted_only);
        assert!((hull.mean_probability - 0.7).abs() < 1e-9);
        let size = p.iter().find(|r| r.attribute == "size").unwrap();
        assert_eq!(size.value, "0.75");
        assert!(!p.iter().any(|r| r.attribute == "single_ship"));
    }

    #[test]
    fn to_rows_keeps_runner_up_when_unsure() {
        let q = qs();
        let answers: BTreeMap<String, Answer> = serde_json::from_str(
            r#"{"hull_colour":{"choice":"red","probabilities":{"red":0.5,"blue":0.45,"black":0.05}},
                "single_ship":{"choice":"yes","probabilities":{"yes":0.95,"no":0.05}},
                "size":{"value":0.42,"probabilities":{"small":0.6,"large":0.4}}}"#,
        )
        .unwrap();
        let rows = to_rows(&img("1", false), &q, "m", &answers);
        let hull = rows.iter().find(|r| r.question == "hull_colour").unwrap();
        assert_eq!(hull.runner_up.as_deref(), Some("blue"));
        let gate = rows.iter().find(|r| r.question == "single_ship").unwrap();
        assert_eq!(gate.runner_up, None);
        let size = rows.iter().find(|r| r.question == "size").unwrap();
        assert_eq!((size.answer.as_str(), size.probability), ("0.420", None));
    }

    #[test]
    fn scores_against_labels() {
        let obs = vec![o("1", "hull_colour", "red", Some(0.8)), o("2", "hull_colour", "blue", Some(0.8))];
        let labels = vec![
            Label { image_id: "1".into(), question: "hull_colour".into(), expected: "Red".into() },
            Label { image_id: "2".into(), question: "hull_colour".into(), expected: "black".into() },
            Label { image_id: "3".into(), question: "hull_colour".into(), expected: "red".into() },
        ];
        assert_eq!(score(&labels, &obs, "m", "t")["hull_colour"], (3, 2, 1));
    }
}
