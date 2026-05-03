use chrono::{TimeZone, Utc};

#[derive(Debug, Default)]
pub struct AisTagBlock {
    pub timestamp_ms: Option<i64>,
    pub group_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct AisSentenceMetadata {
    pub is_ais: bool,
    pub group_id: Option<String>,
    pub fragment_count: Option<usize>,
    pub fragment_number: Option<usize>,
}

impl AisSentenceMetadata {
    pub fn is_fragmented(&self) -> bool {
        matches!(self.fragment_count, Some(count) if count > 1)
    }

    pub fn is_final_fragment(&self) -> bool {
        matches!((self.fragment_count, self.fragment_number), (Some(count), Some(number)) if count == number)
    }
}

/// Returns the portion of the payload starting at the first `!` or `$`.
pub fn nmea_sentence(payload: &str) -> &str {
    let sentence_start = payload
        .char_indices()
        .find(|(_, ch)| *ch == '!' || *ch == '$')
        .map(|(idx, _)| idx)
        .unwrap_or(payload.len());

    &payload[sentence_start..]
}

pub fn parse_ais_tag_block(payload: &str) -> AisTagBlock {
    let sentence_start = payload
        .char_indices()
        .find(|(_, ch)| *ch == '!' || *ch == '$')
        .map(|(idx, _)| idx)
        .unwrap_or(payload.len());
    let prefix = &payload[..sentence_start];
    let mut tag_block = AisTagBlock::default();

    for raw_field in prefix.split(',') {
        let field = raw_field.trim_matches('\\');
        let field = field.split('*').next().unwrap_or(field).trim();

        if let Some(value) = field.strip_prefix("c:") {
            if tag_block.timestamp_ms.is_none() {
                tag_block.timestamp_ms = parse_ais_timestamp_ms(value);
            }
        } else if let Some(value) = field.strip_prefix("g:") {
            if tag_block.group_id.is_none() {
                tag_block.group_id = parse_ais_group_id(value);
            }
        }
    }

    tag_block
}

pub fn parse_ais_sentence_metadata(sentence: &str) -> AisSentenceMetadata {
    let mut fields = sentence.split(',').map(normalize_sentence_field);
    let Some(sentence_type) = fields.next() else {
        return AisSentenceMetadata::default();
    };

    if !is_ais_sentence_type(sentence_type) {
        return AisSentenceMetadata::default();
    }

    let fragment_count = fields.next().and_then(|value| value.parse::<usize>().ok());
    let fragment_number = fields.next().and_then(|value| value.parse::<usize>().ok());
    let sequence_id = fields
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let channel = fields.next().map(str::trim).unwrap_or_default();

    let group_id = match (fragment_count, sequence_id) {
        (Some(fragment_count), Some(sequence_id)) if fragment_count > 1 => Some(format!(
            "{}:{}:{}:{}",
            sentence_type, fragment_count, sequence_id, channel
        )),
        _ => None,
    };

    AisSentenceMetadata {
        is_ais: true,
        group_id,
        fragment_count,
        fragment_number,
    }
}

pub fn parse_pghp_timestamp_ms(sentence: &str) -> Option<i64> {
    let mut fields = sentence.split(',').map(normalize_sentence_field);
    let sentence_type = fields.next()?;

    if sentence_type != "$PGHP" {
        return None;
    }

    let _message_type = fields.next()?;
    let year = fields.next()?.parse::<i32>().ok()?;
    let month = fields.next()?.parse::<u32>().ok()?;
    let day = fields.next()?.parse::<u32>().ok()?;
    let hour = fields.next()?.parse::<u32>().ok()?;
    let minute = fields.next()?.parse::<u32>().ok()?;
    let second = fields.next()?.parse::<u32>().ok()?;
    let millisecond = fields.next()?.parse::<u32>().ok()?;

    if millisecond >= 1_000 {
        return None;
    }

    let timestamp_ms = Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()?
        .timestamp_millis();

    Some(timestamp_ms.saturating_add(i64::from(millisecond)))
}

pub fn parse_ais_timestamp_ms(value: &str) -> Option<i64> {
    let digits = value
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();

    if digits.is_empty() {
        return None;
    }

    let seconds = digits.parse::<u64>().ok()?;
    Some(seconds.saturating_mul(1_000) as i64)
}

pub fn parse_ais_group_id(value: &str) -> Option<String> {
    let candidate = value
        .split('-')
        .filter(|part| !part.is_empty())
        .last()
        .unwrap_or(value)
        .trim();

    if candidate.is_empty() {
        None
    } else {
        Some(candidate.to_string())
    }
}

pub fn nmea_tag_block_checksum(tag_body: &str) -> u8 {
    tag_body.bytes().fold(0u8, |checksum, byte| checksum ^ byte)
}

fn is_ais_sentence_type(sentence_type: &str) -> bool {
    sentence_type.ends_with("VDM") || sentence_type.ends_with("VDO")
}

fn normalize_sentence_field(field: &str) -> &str {
    field.split('*').next().unwrap_or(field).trim()
}

/// Extract the raw NMEA sentence fields (splitting at commas, stopping at `*checksum`).
/// Returns None if the sentence doesn't look like a VDM/VDO or has fewer than 7 fields.
/// Field layout: [type, total_count, frag_num, seq_id, channel, payload, fill*chk]
pub fn extract_nmea_fields(sentence: &str) -> Option<Vec<&str>> {
    let sentence = nmea_sentence(sentence);
    if !sentence.starts_with('!') && !sentence.starts_with('$') {
        return None;
    }

    let fields: Vec<&str> = sentence.splitn(7, ',').collect();
    if fields.len() < 7 {
        return None;
    }

    Some(fields)
}

/// Combine the bare sentences (tag blocks already stripped) of a complete multi-part AIS message.
///
/// The combined output is:
/// - `{sentence_type},1,1,,{channel},{combined_payload},{fill_bits}*{checksum}`
/// - If `tag_block` is Some, prepends `\{tag_block}*{tag_chk}\`
///
/// Returns None if any fragment is missing required fields.
pub fn combine_ais_fragments(sentences: &[String], tag_block: Option<&str>) -> Option<String> {
    if sentences.is_empty() {
        return None;
    }

    let first_fields = extract_nmea_fields(&sentences[0])?;
    let sentence_type = first_fields[0];
    let channel = first_fields[4];

    let mut combined_payload = String::new();
    let mut fill_bits = "0";

    for (i, sentence) in sentences.iter().enumerate() {
        let fields = extract_nmea_fields(sentence)?;
        combined_payload.push_str(fields[5]);
        if i == sentences.len() - 1 {
            // fill_bits is field[6], which may be "N*XX" — take the part before '*'
            fill_bits = fields[6].split('*').next().unwrap_or("0").trim();
        }
    }

    let sentence_body = format!(
        "{},1,1,,{},{},{}",
        sentence_type, channel, combined_payload, fill_bits
    );

    // NMEA checksum: XOR of bytes between '!' (or '$') and '*' exclusive
    let checksum_input = sentence_body
        .trim_start_matches('!')
        .trim_start_matches('$');
    let checksum = checksum_input
        .bytes()
        .fold(0u8, |acc, b| acc ^ b);

    let combined = format!("{}*{:02X}", sentence_body, checksum);

    if let Some(tb) = tag_block {
        let tag_checksum = nmea_tag_block_checksum(tb);
        Some(format!("\\{}*{:02X}\\{}", tb, tag_checksum, combined))
    } else {
        Some(combined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pghp_timestamp() {
        let line = "$PGHP,1,2013,1,9,4,37,45,298,,110,,1,26*19";
        let ts = parse_pghp_timestamp_ms(line).expect("should parse");
        let expected = Utc
            .with_ymd_and_hms(2013, 1, 9, 4, 37, 45)
            .single()
            .unwrap()
            .timestamp_millis()
            + 298;
        assert_eq!(ts, expected);
    }

    #[test]
    fn parses_tag_block_timestamp() {
        let payload = r"\c:1241544035*1D\!AIVDM,1,1,,B,15N4cJ,0*00";
        let tb = parse_ais_tag_block(payload);
        assert_eq!(tb.timestamp_ms, Some(1_241_544_035_000));
    }

    #[test]
    fn parses_tag_block_group_id() {
        let payload = r"\g:1-2-6287,c:1609459200*56\!AIVDM,2,1,0,A,P0,4*72";
        let tb = parse_ais_tag_block(payload);
        assert_eq!(tb.group_id.as_deref(), Some("6287"));
        assert_eq!(tb.timestamp_ms, Some(1_609_459_200_000));
    }

    #[test]
    fn combines_two_fragment_sentences() {
        let part1 = "!AIVDM,2,1,3,A,569qcJP000000000000P4V1QDr3777800000000o0p=220DP03888888,0*49".to_string();
        let part2 = "!AIVDM,2,2,3,A,88881CRR@CACP08,2*3C".to_string();

        let combined = combine_ais_fragments(&[part1, part2], None).expect("should combine");

        assert!(combined.starts_with("!AIVDM,1,1,,A,"));
        assert!(combined.contains("569qcJP000000000000P4V1QDr3777800000000o0p=220DP0388888888881CRR@CACP08"));
        // fill_bits from last fragment is 2
        let fill = combined.split(',').nth(6).unwrap_or("").split('*').next().unwrap_or("");
        assert_eq!(fill, "2");
    }

    #[test]
    fn combines_with_tag_block() {
        let part1 = "!AIVDM,2,1,0,A,AAAA,0*00".to_string();
        let part2 = "!AIVDM,2,2,0,A,BBBB,2*00".to_string();

        let combined = combine_ais_fragments(&[part1, part2], Some("c:1700000000"))
            .expect("should combine");

        assert!(combined.starts_with('\\'));
        assert!(combined.contains("c:1700000000"));
        assert!(combined.contains("!AIVDM,1,1,,A,AAAABBBB,2"));
    }
}
