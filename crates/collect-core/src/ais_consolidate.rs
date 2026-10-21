//! AIS multi-part message consolidation for the ingest pipeline.
//!
//! `AisConsolidator` implements [`LineTransformer`] — it buffers fragments of
//! multi-part AIS sentences (`!AIVDM,2,1,...` + `!AIVDM,2,2,...`), combines
//! them into a single sentence when the group is complete, and handles
//! `$PGHP` timestamp lines and tag-block `c:` carry-forward.
//!
//! This is a port of ais-normalize's `PartitionProcessor`, adapted for
//! use as a per-binary `LineTransformer` in `run_ingest`.

use crate::LineTransformer;
use chrono::TimeZone;

// ---------------------------------------------------------------------------
// NMEA / tag-block parsing helpers (ported from ais-normalize/parse.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct AisSentenceMeta {
    is_ais: bool,
    group_id: Option<String>,
    fragment_count: Option<usize>,
    fragment_number: Option<usize>,
}

impl AisSentenceMeta {
    fn is_fragmented(&self) -> bool {
        matches!(self.fragment_count, Some(c) if c > 1)
    }
}

fn split_tag_block(payload: &str) -> (&str, &str) {
    let start = payload.bytes().position(|b| b == b'!' || b == b'$').unwrap_or(payload.len());
    payload.split_at(start)
}

fn nmea_sentence(payload: &str) -> &str {
    split_tag_block(payload).1
}

fn parse_tag_block_timestamp_ms(prefix: &str) -> Option<i64> {
    for raw in prefix.split(',') {
        let field = raw.trim_matches('\\').split('*').next().unwrap_or(raw).trim();
        if let Some(val) = field.strip_prefix("c:") {
            let mut secs: u64 = 0;
            for byte in val.bytes() {
                if !byte.is_ascii_digit() { break; }
                secs = secs.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
            }
            if secs > 0 { return Some((secs * 1000) as i64); }
        }
    }
    None
}

fn parse_tag_block_field<'a>(prefix: &'a str, key: &str) -> Option<&'a str> {
    for raw in prefix.split(',') {
        let field = raw.trim_matches('\\').split('*').next().unwrap_or(raw).trim();
        if let Some(val) = field.strip_prefix(key).and_then(|r| r.strip_prefix(':')) {
            if !val.is_empty() { return Some(val); }
        }
    }
    None
}

fn parse_ais_meta(sentence: &str) -> AisSentenceMeta {
    let mut fields = sentence.split(',').map(|f| f.split('*').next().unwrap_or(f).trim());
    let Some(stype) = fields.next() else { return AisSentenceMeta::default() };
    if !stype.ends_with("VDM") && !stype.ends_with("VDO") { return AisSentenceMeta::default() }

    let fc = fields.next().and_then(|v| v.parse::<usize>().ok());
    let fn_ = fields.next().and_then(|v| v.parse::<usize>().ok());
    let sid = fields.next().map(str::trim).filter(|v| !v.is_empty()).map(str::to_string);
    let ch = fields.next().unwrap_or_default();

    let gid = match (fc, sid) {
        (Some(c), Some(s)) if c > 1 => Some(format!("{stype}:{c}:{s}:{ch}")),
        _ => None,
    };
    AisSentenceMeta { is_ais: true, group_id: gid, fragment_count: fc, fragment_number: fn_ }
}

fn parse_pghp_timestamp_ms(sentence: &str) -> Option<i64> {
    if !sentence.starts_with("$PGHP") { return None; }
    let mut f = sentence.split(',').map(|v| v.split('*').next().unwrap_or(v).trim());
    if f.next()? != "$PGHP" { return None; }
    let _msg_type = f.next()?;
    let year: i32 = f.next()?.parse().ok()?;
    let month: u32 = f.next()?.parse().ok()?;
    let day: u32 = f.next()?.parse().ok()?;
    let hour: u32 = f.next()?.parse().ok()?;
    let minute: u32 = f.next()?.parse().ok()?;
    let second: u32 = f.next()?.parse().ok()?;
    let ms: u32 = f.next()?.parse().ok()?;
    if ms >= 1000 { return None; }
    let dt = chrono::Utc.with_ymd_and_hms(year, month, day, hour, minute, second).single()?;
    Some(dt.timestamp_millis().saturating_add(ms as i64))
}

/// Fields MSSIS-style feeds append after the NMEA checksum, e.g. the
/// `L10EAC,1792569950` in `!AIVDM,…,0*54,L10EAC,1792569950`. Empty when the
/// sentence ends at its checksum.
fn mssis_suffix_fields(sentence: &str) -> Vec<&str> {
    let Some(star) = sentence.rfind('*') else { return Vec::new() };
    let Some(rest) = sentence.get(star + 3..) else { return Vec::new() };
    let Some(rest) = rest.strip_prefix(',') else { return Vec::new() };
    rest.split(',').map(str::trim).collect()
}

/// Earliest receiver time accepted from an MSSIS suffix (2000-01-01).
const MSSIS_MIN_SECS: i64 = 946_684_800;

fn parse_mssis_epoch_ms(sentence: &str) -> Option<i64> {
    let last = *mssis_suffix_fields(sentence).last()?;
    if last.is_empty() || !last.bytes().all(|b| b.is_ascii_digit()) { return None; }
    let ms = match last.len() {
        9 | 10 => last.parse::<i64>().ok()?.checked_mul(1000)?,
        12 | 13 => last.parse::<i64>().ok()?,
        _ => return None,
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    if ms < MSSIS_MIN_SECS * 1000 || ms > now_ms + 86_400_000 { return None; }
    Some(ms)
}

/// The receiver/station field MSSIS places between the checksum and the
/// trailing epoch, when present.
fn parse_mssis_station(sentence: &str) -> Option<&str> {
    let fields = mssis_suffix_fields(sentence);
    let (_, rest) = fields.split_last()?;
    rest.iter().copied().find(|f| !f.is_empty() && !f.bytes().all(|b| b.is_ascii_digit()))
}

// ---------------------------------------------------------------------------
// NMEa checksum
// ---------------------------------------------------------------------------

fn nmea_checksum(body: &str) -> u8 {
    body.bytes().fold(0u8, |acc, b| acc ^ b)
}

fn extract_nmea_fields(sentence: &str) -> Option<Vec<&str>> {
    let s = nmea_sentence(sentence);
    if !s.starts_with('!') && !s.starts_with('$') { return None; }
    let fields: Vec<&str> = s.splitn(7, ',').collect();
    if fields.len() < 7 { return None; }
    Some(fields)
}

fn combine_ais_fragments(sentences: &[String], tag_block: Option<&str>) -> Option<String> {
    if sentences.is_empty() { return None; }
    let first = extract_nmea_fields(&sentences[0])?;
    let stype = first[0];
    let channel = first[4];
    let mut payload = String::new();
    let mut fill = "0";
    for (i, s) in sentences.iter().enumerate() {
        let fields = extract_nmea_fields(s)?;
        payload.push_str(fields[5]);
        if i == sentences.len() - 1 {
            fill = fields[6].split('*').next().unwrap_or("0").trim();
        }
    }
    let body = format!("{stype},1,1,,{channel},{payload},{fill}");
    let chk = nmea_checksum(body.trim_start_matches('!').trim_start_matches('$'));
    let combined = format!("{body}*{chk:02X}");
    if let Some(tb) = tag_block {
        let tc = nmea_checksum(tb);
        Some(format!("\\{tb}*{tc:02X}\\{combined}"))
    } else {
        Some(combined)
    }
}

// ---------------------------------------------------------------------------
// Fragment group buffer
// ---------------------------------------------------------------------------

const MAX_GROUPS: usize = 8192;

struct FragmentGroup {
    slots: Vec<Option<String>>,
    expected_count: usize,
    received_count: usize,
    first_ts_ms: i64,
    tag_block: Option<String>,
}

/// Configuration for `AisConsolidator`. Each feature can be independently
/// enabled. When both are disabled the transformer is a no-op.
pub struct AisConsolidatorConfig {
    /// Process `$PGHP` timestamp lines and tag-block `c:` carry-forward.
    /// When on, timestamps from `$PGHP` and `c:` override the arrival timestamp.
    pub process_timestamps: bool,
    /// Use the MSSIS receiver epoch appended after the checksum
    /// (`!AIVDM,…*CS,1792569950`, optionally `*CS,<station>,<epoch>`) as the
    /// row timestamp. The station, when present, is carried into consolidated
    /// output as tag-block `s:`. A tag-block `c:` still wins when
    /// `process_timestamps` is also on.
    pub mssis_timestamps: bool,
    /// Buffer and reassemble multi-part AIS sentences (fragments with
    /// fragment_count > 1). When off, each fragment is emitted individually.
    pub consolidate_multipart: bool,
}

impl Default for AisConsolidatorConfig {
    fn default() -> Self {
        AisConsolidatorConfig { process_timestamps: true, mssis_timestamps: false, consolidate_multipart: true }
    }
}

/// AIS multi-part message consolidator. Implements [`LineTransformer`] for use
/// in `IngestOptions.line_transformer`.
///
/// Buffers fragments of multi-part AIS sentences by `group_id`, combines them
/// when complete, handles `$PGHP` timestamp lines, and carries forward tag-
/// block `c:` and `s:` fields to the consolidated output.
pub struct AisConsolidator {
    config: AisConsolidatorConfig,
    groups: std::collections::HashMap<String, FragmentGroup>,
    pending_timestamp_ms: Option<i64>,
}

impl AisConsolidator {
    pub fn new(config: AisConsolidatorConfig) -> Self {
        AisConsolidator { config, groups: std::collections::HashMap::new(), pending_timestamp_ms: None }
    }

    fn emit_group(&mut self, group: FragmentGroup, out: &mut Vec<(i64, String)>) {
        let sentences: Vec<String> = group.slots.into_iter().flatten().collect();
        let ts = group.first_ts_ms;
        if let Some(combined) = combine_ais_fragments(&sentences, group.tag_block.as_deref()) {
            out.push((ts, combined));
        } else {
            for s in sentences {
                out.push((ts, s));
            }
        }
    }

    fn evict_oldest(&mut self, out: &mut Vec<(i64, String)>) {
        let key = self.groups.iter().min_by_key(|(_, g)| g.first_ts_ms).map(|(k, _)| k.clone());
        if let Some(k) = key {
            if let Some(g) = self.groups.remove(&k) {
                self.emit_group(g, out);
            }
        }
    }
}

impl LineTransformer for AisConsolidator {
    fn transform(&mut self, line: &str, arrival_ts_ms: i64) -> Vec<(i64, String)> {
        let mut out = Vec::new();

        let (tag_prefix, sentence) = split_tag_block(line);
        let pghp_ts = if self.config.process_timestamps { parse_pghp_timestamp_ms(sentence) } else { None };
        let tag_ts = if self.config.process_timestamps && !tag_prefix.is_empty()
            { parse_tag_block_timestamp_ms(tag_prefix) } else { None };

        // $PGHP timestamp line (only when process_timestamps is on)
        if pghp_ts.is_some() {
            self.pending_timestamp_ms = pghp_ts;
            out.push((pghp_ts.unwrap(), line.to_string()));
            return out;
        }

        let meta = parse_ais_meta(sentence);

        // Non-AIS line — pass through
        if !meta.is_ais {
            self.pending_timestamp_ms = None;
            out.push((arrival_ts_ms, line.to_string()));
            return out;
        }

        let mssis_ts = if self.config.mssis_timestamps { parse_mssis_epoch_ms(sentence) } else { None };
        let ends_group = !meta.is_fragmented() || meta.fragment_number == meta.fragment_count;

        // Resolve output timestamp: tag block c: > MSSIS epoch > pending > arrival
        let resolved_ts = if let Some(t) = tag_ts {
            if meta.is_fragmented() { self.pending_timestamp_ms = Some(t); }
            else { self.pending_timestamp_ms = None; }
            t
        } else if let Some(t) = mssis_ts {
            if ends_group { self.pending_timestamp_ms = None; }
            t
        } else if let Some(t) = self.pending_timestamp_ms.filter(|_| self.config.process_timestamps) {
            if ends_group { self.pending_timestamp_ms = None; }
            t
        } else {
            arrival_ts_ms
        };

        // When multipart consolidation is off, emit every sentence directly
        if !self.config.consolidate_multipart {
            out.push((resolved_ts, line.to_string()));
            return out;
        }

        // Single-sentence AIS — emit directly
        if !meta.is_fragmented() {
            out.push((resolved_ts, line.to_string()));
            return out;
        }

        // Multi-sentence AIS — buffer fragment
        let Some(ref gid) = meta.group_id else {
            out.push((resolved_ts, line.to_string()));
            return out;
        };

        let fc = meta.fragment_count.unwrap_or(1);
        let fn_ = meta.fragment_number.unwrap_or(1);

        if let Some(group) = self.groups.get_mut(gid.as_str()) {
            if fn_ >= 1 && fn_ <= group.expected_count && group.slots[fn_].is_none() {
                group.slots[fn_] = Some(sentence.to_string());
                group.received_count += 1;
            }
            if group.received_count >= group.expected_count {
                if let Some(g) = self.groups.remove(gid.as_str()) {
                    self.emit_group(g, &mut out);
                }
            }
        } else {
            if self.groups.len() >= MAX_GROUPS {
                self.evict_oldest(&mut out);
            }
            let mut slots = vec![None; fc + 1];
            if fn_ >= 1 && fn_ <= fc {
                slots[fn_] = Some(sentence.to_string());
            }
            // station is always preserved when consolidating, regardless of
            // process_timestamps; MSSIS suffix fields fill in when no tag
            // block carries them
            let station = parse_tag_block_field(tag_prefix, "s")
                .or_else(|| if self.config.mssis_timestamps { parse_mssis_station(sentence) } else { None });
            let tb = match (tag_ts.or(mssis_ts), station) {
                (Some(t), Some(s)) => Some(format!("c:{},s:{}", t / 1000, s)),
                (Some(t), None) => Some(format!("c:{}", t / 1000)),
                (None, Some(s)) => Some(format!("s:{}", s)),
                (None, None) => None,
            };
            self.groups.insert(gid.clone(), FragmentGroup {
                slots,
                expected_count: fc,
                received_count: if fn_ >= 1 && fn_ <= fc { 1 } else { 0 },
                first_ts_ms: resolved_ts,
                tag_block: tb,
            });
        }

        out
    }

    fn flush(&mut self) -> Vec<(i64, String)> {
        let mut out = Vec::new();
        let groups: Vec<_> = self.groups.drain().collect();
        for (_, g) in groups {
            let ts = g.first_ts_ms;
            for slot in g.slots.into_iter().flatten() {
                out.push((ts, slot));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARRIVAL: i64 = 1_800_000_000_000;
    const EPOCH_MS: i64 = 1_792_569_950_000;

    fn consolidator(process_timestamps: bool, mssis_timestamps: bool) -> AisConsolidator {
        AisConsolidator::new(AisConsolidatorConfig {
            process_timestamps,
            mssis_timestamps,
            consolidate_multipart: true,
        })
    }

    #[test]
    fn mssis_epoch_sets_single_sentence_timestamp() {
        let mut c = consolidator(false, true);
        let line = "!AIVDM,1,1,9,B,B3INcN000@3VQ761Vq?Q3woQjDa:,0*75,1792569950";
        assert_eq!(c.transform(line, ARRIVAL), vec![(EPOCH_MS, line.to_string())]);
    }

    #[test]
    fn mssis_station_and_epoch_parse() {
        let s = "!AIVDM,2,1,4,A,55?Adv,0*54,L10EAC,1792569950";
        assert_eq!(parse_mssis_epoch_ms(s), Some(EPOCH_MS));
        assert_eq!(parse_mssis_station(s), Some("L10EAC"));
        assert_eq!(parse_mssis_station("!AIVDM,1,1,,B,13Ee,0*4F,1792569950"), None);
    }

    #[test]
    fn mssis_millisecond_epoch_is_accepted() {
        assert_eq!(parse_mssis_epoch_ms("!AIVDM,1,1,,B,13Ee,0*4F,1792569950123"), Some(EPOCH_MS + 123));
    }

    #[test]
    fn mssis_group_keeps_epoch_and_station_in_tag_block() {
        let mut c = consolidator(false, true);
        assert!(c
            .transform("!AIVDM,2,1,4,A,55?Adv42;Kw7<IIKL004DLD4r0EQ18E=<000001:A@N:A6bPNB2CSECP0000,0*54,L10EAC,1792569950", ARRIVAL)
            .is_empty());
        let out = c.transform("!AIVDM,2,2,4,A,00000000000,2*20,L10EAC,1792569950", ARRIVAL);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, EPOCH_MS);
        assert!(out[0].1.starts_with("\\c:1792569950,s:L10EAC*"), "{}", out[0].1);
        assert!(out[0].1.contains("!AIVDM,1,1,,A,55?Adv"), "{}", out[0].1);
    }

    #[test]
    fn mssis_group_without_station_gets_epoch_only() {
        let mut c = consolidator(false, true);
        c.transform("!AIVDM,2,1,0,B,53FCI:82>LLHD4dJ221<58lTDq@v0@F0L4l8t61J2@m3=6`80AURAkp88888,0*07,1792569950", ARRIVAL);
        let out = c.transform("!AIVDM,2,2,0,B,88888888880,2*27,1792569950", ARRIVAL);
        assert!(out[0].1.starts_with("\\c:1792569950*"), "{}", out[0].1);
    }

    #[test]
    fn missing_or_junk_suffix_falls_back_to_arrival() {
        let mut c = consolidator(false, true);
        for line in [
            "!AIVDM,1,1,,B,13Ee`L7P0NP20WfEv94EngwP2<0D,0*4F",
            "!AIVDM,1,1,,B,13Ee`L7P0NP20WfEv94EngwP2<0D,0*4F,L10EAC",
            "!AIVDM,1,1,,B,13Ee`L7P0NP20WfEv94EngwP2<0D,0*4F,12345",
            "!AIVDM,1,1,,B,13Ee`L7P0NP20WfEv94EngwP2<0D,0*4F,9999999999",
        ] {
            assert_eq!(c.transform(line, ARRIVAL)[0].0, ARRIVAL, "{line}");
        }
    }

    #[test]
    fn mssis_flag_off_keeps_arrival() {
        let mut c = consolidator(true, false);
        let out = c.transform("!AIVDM,1,1,9,B,B3INcN000@3VQ761Vq?Q3woQjDa:,0*75,1792569950", ARRIVAL);
        assert_eq!(out[0].0, ARRIVAL);
        c.transform("!AIVDM,2,1,0,B,53FCI:82>LLHD4dJ221<58lTDq@v0@F0L4l8t61J2@m3=6`80AURAkp88888,0*07,L10EAC,1792569950", ARRIVAL);
        let out = c.transform("!AIVDM,2,2,0,B,88888888880,2*27,L10EAC,1792569950", ARRIVAL);
        assert!(out[0].1.starts_with('!'), "{}", out[0].1);
    }

    #[test]
    fn tag_block_timestamp_beats_mssis() {
        let mut c = consolidator(true, true);
        let line = "\\c:1700000000*00\\!AIVDM,1,1,,B,13Ee`L7P0NP20WfEv94EngwP2<0D,0*4F,1792569950";
        assert_eq!(c.transform(line, ARRIVAL)[0].0, 1_700_000_000_000);
    }
}
