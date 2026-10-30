//! Question sets: the typed questions a System 1 model is asked about each
//! photo. Built in from `config/questions/*.toml`; `--config-dir` points at
//! a directory of the same files instead.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

const BUILT_IN: &[(&str, &str)] = &[("v1", include_str!("../config/questions/v1.toml"))];

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuestionType {
    Choice,
    YesNo,
    Score,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Attribute,
    Gate,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Question {
    pub id: String,
    #[serde(default)]
    pub role: Role,
    #[serde(rename = "type")]
    pub kind: QuestionType,
    pub text: String,
    #[serde(default)]
    pub criteria: Vec<String>,
    /// For gates: the answers that let an image through.
    #[serde(default)]
    pub pass: Vec<String>,
    #[serde(default = "yes")]
    pub enabled: bool,
}

fn yes() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionSet {
    pub version: String,
    pub min_probability: f64,
    pub runner_up_below: f64,
    pub questions: Vec<Question>,
}

impl QuestionSet {
    /// Question set `version`, from `dir/<version>.toml` or the built-in one.
    pub fn load(version: &str, dir: Option<&Path>) -> Result<Self> {
        let text = match dir {
            Some(d) => {
                let p = d.join("questions").join(format!("{version}.toml"));
                std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?
            }
            None => match BUILT_IN.iter().find(|(v, _)| *v == version) {
                Some((_, t)) => t.to_string(),
                None => bail!("no built-in question set {version}"),
            },
        };
        let qs = Self::parse(&text)?;
        if qs.version != version {
            bail!("question set file {version} declares version {}", qs.version);
        }
        Ok(qs)
    }

    pub fn parse(text: &str) -> Result<Self> {
        let qs: QuestionSet = toml::from_str(text).context("parsing question set")?;
        qs.validate()?;
        Ok(qs)
    }

    fn validate(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for q in &self.questions {
            if !seen.insert(q.id.as_str()) {
                bail!("question {} appears twice", q.id);
            }
            match q.kind {
                QuestionType::Choice | QuestionType::Score if q.criteria.len() < 2 => {
                    bail!("question {} needs at least two criteria", q.id)
                }
                QuestionType::YesNo if !q.criteria.is_empty() => bail!("yes_no question {} takes no criteria", q.id),
                _ => {}
            }
            if q.role == Role::Gate {
                if q.pass.is_empty() {
                    bail!("gate {} has no pass answers", q.id);
                }
                for p in &q.pass {
                    if !q.labels().contains(p) {
                        bail!("gate {}: pass answer '{p}' is not one of its labels", q.id);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn gates(&self) -> impl Iterator<Item = &Question> {
        self.questions.iter().filter(|q| q.role == Role::Gate)
    }

    pub fn get(&self, id: &str) -> Option<&Question> {
        self.questions.iter().find(|q| q.id == id)
    }
}

impl Question {
    /// The labels an answer can take.
    pub fn labels(&self) -> Vec<String> {
        match self.kind {
            QuestionType::YesNo => vec!["yes".into(), "no".into()],
            _ => self.criteria.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_sets_parse() {
        for (v, _) in BUILT_IN {
            let qs = QuestionSet::load(v, None).unwrap();
            assert!(qs.gates().count() >= 1);
            assert!(qs.get("ship_type").is_some());
        }
    }

    #[test]
    fn rejects_bad_gates() {
        let t = r#"
            version = "x"
            min_probability = 0.5
            runner_up_below = 0.6
            [[questions]]
            id = "g"
            role = "gate"
            type = "yes_no"
            text = "?"
            pass = ["maybe"]
        "#;
        assert!(QuestionSet::parse(t).is_err());
    }
}
