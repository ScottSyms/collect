use anyhow::Result;
use std::collections::HashMap;

/// Resolve which parser handles a source label.
pub fn parser_for_source(source: &str, overrides: &HashMap<String, String>) -> &'static str {
    if let Some(v) = overrides.get(source) {
        if v == "aisstream-parse" {
            return "aisstream-parse";
        }
        return "ais-parse";
    }
    if source == "aisstream" || source.starts_with("aisstream-") || source.starts_with("aisstream_") {
        "aisstream-parse"
    } else {
        "ais-parse"
    }
}

pub fn load_source_map(path: Option<&str>) -> Result<HashMap<String, String>> {
    let Some(p) = path else { return Ok(HashMap::new()); };
    let contents = std::fs::read_to_string(p)?;
    // Expect flat TOML: [source_map] or bare keys
    let table: toml::Table = contents.parse()?;
    let mut map = HashMap::new();
    for (k, v) in table {
        if k == "source_map" {
            if let toml::Value::Table(inner) = v {
                for (sk, sv) in inner {
                    if let toml::Value::String(s) = sv {
                        map.insert(sk, s);
                    }
                }
            }
        } else if let toml::Value::String(s) = v {
            map.insert(k, s);
        }
    }
    Ok(map)
}

pub fn extract_source_from_key(s3_key: &str) -> Option<String> {
    for seg in s3_key.split('/') {
        if let Some(v) = seg.strip_prefix("source=") {
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_routing() {
        let map = HashMap::new();
        assert_eq!(parser_for_source("norway", &map), "ais-parse");
        assert_eq!(parser_for_source("norway-tcp", &map), "ais-parse");
        assert_eq!(parser_for_source("aisstream", &map), "aisstream-parse");
        assert_eq!(parser_for_source("aisstream-global", &map), "aisstream-parse");
    }
    #[test]
    fn override_routing() {
        let mut map = HashMap::new();
        map.insert("special".into(), "aisstream-parse".into());
        assert_eq!(parser_for_source("special", &map), "aisstream-parse");
    }
    #[test]
    fn extract_source() {
        assert_eq!(extract_source_from_key("bronze/source=norway/year=2026/month=07/day=15/part.parquet"), Some("norway".into()));
        assert_eq!(extract_source_from_key("source=aisstream/year=2026/part.parquet"), Some("aisstream".into()));
        assert_eq!(extract_source_from_key("year=2026/part.parquet"), None);
    }
}
