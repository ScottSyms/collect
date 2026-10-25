//! A client for Jev-style System 1 servers: `POST /v1/systemone` with typed
//! questions and an image asset, answered with a label and probabilities
//! per question, never free text. Laya (through `tools/laya-vision-serve`
//! or `laya-serve`) and TypeSafe's Jev both speak it.
//!
//! The request carries the image once, in `assets`, and refers to it from
//! `state` as `asset:<id>`. Servers refuse unreferenced assets, PNG/JPEG
//! only, 256-1568 px a side, 16 MiB an image.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::Engine;
use image::{imageops::FilterType, ImageFormat, ImageReader};
use serde::{Deserialize, Serialize};

use crate::questions::{Question, QuestionType};

pub const MIN_SIDE: u32 = 256;
pub const MAX_SIDE: u32 = 1568;
pub const MAX_IMAGE_BYTES: usize = 16 << 20;
const ASSET_ID: &str = "photo";

#[derive(Debug, Serialize)]
pub struct Request<'a> {
    pub model: &'a str,
    pub assets: BTreeMap<&'a str, Asset>,
    pub state: String,
    pub questions: Vec<WireQuestion<'a>>,
}

#[derive(Debug, Serialize)]
pub struct Asset {
    pub media_type: &'static str,
    pub data: String,
}

#[derive(Debug, Serialize)]
pub struct WireQuestion<'a> {
    pub id: &'a str,
    #[serde(rename = "type")]
    pub kind: QuestionType,
    pub text: &'a str,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    pub criteria: &'a [String],
}

#[derive(Debug, Deserialize)]
pub struct Response {
    #[serde(default)]
    pub model: Option<String>,
    pub answers: BTreeMap<String, Answer>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Answer {
    #[serde(default)]
    pub choice: Option<String>,
    /// Score questions: 0..1.
    #[serde(default)]
    pub value: Option<f64>,
    #[serde(default)]
    pub probabilities: BTreeMap<String, f64>,
    #[serde(default)]
    pub calibrated: bool,
}

/// A label and its probability.
pub type Ranked = Option<(String, f64)>;

impl Answer {
    /// The top label and its probability, and the runner-up.
    pub fn ranked(&self) -> (Ranked, Ranked) {
        let mut v: Vec<(&String, &f64)> = self.probabilities.iter().collect();
        v.sort_by(|a, b| b.1.total_cmp(a.1));
        let top = match &self.choice {
            Some(c) => Some((c.clone(), self.probabilities.get(c).copied().unwrap_or(f64::NAN))),
            None => v.first().map(|(l, p)| ((*l).clone(), **p)),
        };
        let second = v
            .iter()
            .find(|(l, _)| top.as_ref().is_none_or(|(t, _)| t != *l))
            .map(|(l, p)| ((*l).clone(), **p));
        (top.map(|(l, p)| (l, if p.is_nan() { 0.0 } else { p })), second)
    }
}

/// An image ready to send: PNG or JPEG within the size limits.
pub struct Prepared {
    pub media_type: &'static str,
    pub bytes: Vec<u8>,
}

/// Checks `bytes` against the server's limits, shrinking an oversized image
/// (or converting another format) to JPEG. Refuses an image too small to
/// say anything about.
pub fn prepare(bytes: &[u8]) -> Result<Prepared> {
    let reader = ImageReader::new(Cursor::new(bytes)).with_guessed_format().context("reading image")?;
    let format = reader.format();
    let (w, h) = reader.into_dimensions().context("reading image size")?;
    if w.min(h) < MIN_SIDE {
        bail!("image is {w}x{h}; the model needs at least {MIN_SIDE} px a side");
    }
    let ok_format = matches!(format, Some(ImageFormat::Jpeg) | Some(ImageFormat::Png));
    if ok_format && w.max(h) <= MAX_SIDE && bytes.len() <= MAX_IMAGE_BYTES {
        let media_type = if format == Some(ImageFormat::Png) { "image/png" } else { "image/jpeg" };
        return Ok(Prepared { media_type, bytes: bytes.to_vec() });
    }
    let img = image::load_from_memory(bytes).context("decoding image")?;
    let img = if w.max(h) > MAX_SIDE { img.resize(MAX_SIDE, MAX_SIDE, FilterType::Lanczos3) } else { img };
    if img.width().min(img.height()) < MIN_SIDE {
        bail!("image is {w}x{h}; too narrow to shrink to {MAX_SIDE} px and keep {MIN_SIDE} px");
    }
    let mut out = Vec::new();
    img.to_rgb8().write_to(&mut Cursor::new(&mut out), ImageFormat::Jpeg).context("encoding JPEG")?;
    Ok(Prepared { media_type: "image/jpeg", bytes: out })
}

pub fn request<'a>(model: &'a str, image: &Prepared, questions: &'a [&'a Question]) -> Request<'a> {
    let data = base64::engine::general_purpose::STANDARD.encode(&image.bytes);
    Request {
        model,
        assets: BTreeMap::from([(ASSET_ID, Asset { media_type: image.media_type, data })]),
        state: format!("asset:{ASSET_ID}"),
        questions: questions
            .iter()
            .map(|q| WireQuestion { id: &q.id, kind: q.kind, text: &q.text, criteria: &q.criteria })
            .collect(),
    }
}

pub struct Client {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
    max_attempts: u32,
}

impl Client {
    /// `base` is the server root (`http://localhost:8765`); `token`, when
    /// set, goes as a bearer token.
    pub fn new(base: &str, token: Option<String>) -> Result<Self> {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(300)).build()?;
        Ok(Client { http, url: format!("{}/v1/systemone", base.trim_end_matches('/')), token, max_attempts: 4 })
    }

    pub async fn ask(&self, req: &Request<'_>) -> Result<Response> {
        let mut last = None;
        for attempt in 1..=self.max_attempts {
            if attempt > 1 {
                tokio::time::sleep(Duration::from_secs(1 << (attempt - 1).min(5))).await;
            }
            let mut b = self.http.post(&self.url).header("TypeSafe-Danger-Zone-Images", "true").json(req);
            if let Some(t) = &self.token {
                b = b.bearer_auth(t);
            }
            match b.send().await {
                Ok(r) if r.status().is_success() => return r.json().await.context("parsing System 1 response"),
                Ok(r) => {
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    let err = anyhow::anyhow!("{}: HTTP {status}: {}", self.url, body.chars().take(300).collect::<String>());
                    if !(status.is_server_error() || status.as_u16() == 429) {
                        return Err(err);
                    }
                    last = Some(err);
                }
                Err(e) => last = Some(anyhow::Error::new(e).context(format!("calling {}", self.url))),
            }
        }
        Err(last.expect("at least one attempt"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::questions::QuestionSet;

    fn jpeg(w: u32, h: u32) -> Vec<u8> {
        let img = image::RgbImage::new(w, h);
        let mut out = Vec::new();
        img.write_to(&mut Cursor::new(&mut out), ImageFormat::Jpeg).unwrap();
        out
    }

    #[test]
    fn prepare_passes_shrinks_and_refuses() {
        let p = prepare(&jpeg(800, 600)).unwrap();
        assert_eq!(p.media_type, "image/jpeg");
        let big = prepare(&jpeg(3000, 2000)).unwrap();
        let (w, h) = ImageReader::new(Cursor::new(&big.bytes)).with_guessed_format().unwrap().into_dimensions().unwrap();
        assert_eq!((w, h), (1568, 1045));
        assert!(prepare(&jpeg(200, 600)).is_err());
    }

    #[test]
    fn request_shape() {
        let qs = QuestionSet::load("v1", None).unwrap();
        let qv: Vec<&Question> = qs.questions.iter().take(2).collect();
        let img = prepare(&jpeg(400, 300)).unwrap();
        let v = serde_json::to_value(request("laya-vision", &img, &qv)).unwrap();
        assert_eq!(v["state"], "asset:photo");
        assert_eq!(v["assets"]["photo"]["media_type"], "image/jpeg");
        assert_eq!(v["questions"][0]["type"], "choice");
        assert_eq!(v["questions"][1]["type"], "yes_no");
        assert!(v["questions"][1].get("criteria").is_none());
    }

    #[test]
    fn ranks_answers() {
        let a: Answer = serde_json::from_str(
            r#"{"choice":"red","probabilities":{"red":0.55,"blue":0.4,"black":0.05},"calibrated":false}"#,
        )
        .unwrap();
        let (top, second) = a.ranked();
        assert_eq!(top, Some(("red".into(), 0.55)));
        assert_eq!(second, Some(("blue".into(), 0.4)));
        let b: Answer = serde_json::from_str(r#"{"probabilities":{"yes":0.2,"no":0.8}}"#).unwrap();
        assert_eq!(b.ranked().0, Some(("no".into(), 0.8)));
    }

    #[tokio::test]
    async fn client_round_trip() {
        use axum::{http::HeaderMap, routing::post, Json, Router};
        async fn handler(headers: HeaderMap, Json(body): Json<serde_json::Value>) -> Json<serde_json::Value> {
            assert_eq!(headers.get("TypeSafe-Danger-Zone-Images").unwrap(), "true");
            assert_eq!(body["state"], "asset:photo");
            let ids: Vec<String> = body["questions"].as_array().unwrap().iter().map(|q| q["id"].as_str().unwrap().to_string()).collect();
            let answers: serde_json::Map<String, serde_json::Value> = ids
                .into_iter()
                .map(|id| (id, serde_json::json!({"choice": "yes", "probabilities": {"yes": 0.9, "no": 0.1}, "calibrated": false})))
                .collect();
            Json(serde_json::json!({"model": "mock", "answers": answers}))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, Router::new().route("/v1/systemone", post(handler))).await.unwrap() });
        let qs = QuestionSet::load("v1", None).unwrap();
        let qv: Vec<&Question> = qs.questions.iter().filter(|q| q.kind == QuestionType::YesNo).take(3).collect();
        let img = prepare(&jpeg(640, 480)).unwrap();
        let resp = Client::new(&format!("http://{addr}/"), None).unwrap().ask(&request("mock", &img, &qv)).await.unwrap();
        assert_eq!(resp.answers.len(), 3);
        assert_eq!(resp.answers[&qv[0].id].ranked().0, Some(("yes".into(), 0.9)));
    }
}
