//! Identifier validation and value normalization. Normalized values are for
//! matching; the original value is always kept beside them.

use unicode_normalization::UnicodeNormalization;

/// What an IMO ship number looks like on inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImoCheck {
    /// Seven digits whose check digit is right.
    Valid,
    /// Seven digits whose check digit is wrong.
    BadCheckDigit,
    /// Eight digits: the IMO scheme allows for growing to eight, but no
    /// check rule for them is published yet, so these can't be verified.
    Unverifiable,
}

impl ImoCheck {
    pub fn as_str(self) -> &'static str {
        match self {
            ImoCheck::Valid => "valid",
            ImoCheck::BadCheckDigit => "bad_check_digit",
            ImoCheck::Unverifiable => "unverifiable",
        }
    }
}

/// An IMO ship number from `s` (an `IMO` prefix, spaces and a leading `#`
/// tolerated), as a digit string, with what its check digit says. `None`
/// when `s` holds no number of an IMO number's shape (empty, `-`, `0`, too
/// short or long, or starting with 0).
pub fn imo(s: &str) -> Option<(String, ImoCheck)> {
    let t = s.trim().to_ascii_uppercase();
    let t = t.strip_prefix("IMO").unwrap_or(&t);
    let digits: String = t.chars().filter(|c| !c.is_whitespace() && *c != '#' && *c != '.' && *c != ':').collect();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || digits.starts_with('0') {
        return None;
    }
    match digits.len() {
        7 => {
            let d: Vec<u32> = digits.bytes().map(|b| (b - b'0') as u32).collect();
            let sum: u32 = d[..6].iter().zip((2..=7).rev()).map(|(x, w)| x * w).sum();
            let check = if sum % 10 == d[6] { ImoCheck::Valid } else { ImoCheck::BadCheckDigit };
            Some((digits, check))
        }
        8 => Some((digits, ImoCheck::Unverifiable)),
        _ => None,
    }
}

/// What kind of station a nine-digit MMSI identifies (ITU-R M.585).
pub fn mmsi_kind(m: &str) -> &'static str {
    let b = m.as_bytes();
    match b[0] {
        b'2'..=b'7' => "ship",
        b'0' if b[1] == b'0' => "coast_station",
        b'0' => "group_of_ships",
        b'1' if m.starts_with("111") => "sar_aircraft",
        b'8' => "handheld_vhf",
        b'9' if m.starts_with("970") => "ais_sart",
        b'9' if m.starts_with("972") => "man_overboard",
        b'9' if m.starts_with("974") => "epirb_ais",
        b'9' if m.starts_with("98") => "craft_associated",
        b'9' if m.starts_with("99") => "aid_to_navigation",
        _ => "other",
    }
}

/// A nine-digit MMSI from `s`, kept as text so leading zeros survive.
pub fn mmsi(s: &str) -> Option<String> {
    let t: String = s.trim().chars().filter(|c| !c.is_whitespace()).collect();
    (t.len() == 9 && t.bytes().all(|b| b.is_ascii_digit()) && t != "000000000").then_some(t)
}

/// A call sign, upper-cased without spaces or hyphens; placeholders such as
/// `N/A` or `NONE` are no call sign.
pub fn call_sign(s: &str) -> Option<String> {
    let t: String = s
        .trim()
        .to_ascii_uppercase()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .collect();
    let placeholder = matches!(t.as_str(), "" | "N/A" | "NA" | "NONE" | "UNKNOWN" | "0" | "-" | "NIL" | "TBA");
    (!placeholder && t.len() <= 10 && t.bytes().all(|b| b.is_ascii_alphanumeric())).then_some(t)
}

/// Prefixes that describe the vessel rather than name it.
const NAME_PREFIXES: &[&str] = &["MV", "MS", "MT", "MY", "SS", "FV", "SV", "SY", "RV", "MSV", "MFV", "TS"];

/// A vessel name reduced for matching: accents and case folded, `M.V.`-style
/// prefixes and punctuation removed, a trailing `, THE` moved to the front.
/// `None` when nothing is left.
pub fn name_key(s: &str) -> Option<String> {
    let folded: String = s.nfkd().filter(|c| !unicode_normalization::char::is_combining_mark(*c)).collect();
    let upper = folded.to_uppercase();
    let mut cleaned = String::with_capacity(upper.len());
    for c in upper.chars() {
        match c {
            // Abbreviation dots and slashes join letters: M.V. -> MV, M/V -> MV.
            '.' | '/' | '\'' | '\u{2019}' => {}
            '&' => cleaned.push_str(" AND "),
            c if c.is_alphanumeric() => cleaned.push(c),
            _ => cleaned.push(' '),
        }
    }
    let mut tokens: Vec<&str> = cleaned.split_whitespace().collect();
    if tokens.len() > 1 && NAME_PREFIXES.contains(&tokens[0]) {
        tokens.remove(0);
    }
    if tokens.len() > 1 && tokens.last() == Some(&"THE") && upper.contains(", THE") {
        tokens.pop();
        tokens.insert(0, "THE");
    }
    (!tokens.is_empty()).then(|| tokens.join(" "))
}

/// A name as published, trimmed, or `None` for an empty or placeholder name.
pub fn name_text(s: &str) -> Option<String> {
    let t = s.trim();
    let placeholder = matches!(t.to_ascii_uppercase().as_str(), "" | "-" | "N/A" | "UNKNOWN" | "UNNAMED" | "NONE");
    (!placeholder).then(|| t.split_whitespace().collect::<Vec<_>>().join(" "))
}

/// A plausible build year at the start of `s`: `1993`, `1993-05-01`, or
/// Transport Canada's `199300` (year and month).
pub fn build_year(s: &str) -> Option<i32> {
    let s = s.trim();
    let y: i32 = s.get(..4)?.parse().ok()?;
    let rest_ok = s.len() == 4 || s.len() == 6 && s[4..].bytes().all(|b| b.is_ascii_digit()) || s.as_bytes().get(4) == Some(&b'-');
    (rest_ok && (1800..=2100).contains(&y)).then_some(y)
}

/// A finite, positive measurement.
pub fn positive(s: &str) -> Option<f64> {
    s.trim().replace(',', "").parse::<f64>().ok().filter(|f| f.is_finite() && *f > 0.0)
}

pub const FEET_TO_METRES: f64 = 0.3048;

/// Formats a measurement for `value_normalized`: up to 3 decimals, no
/// trailing zeros, so equal values compare equal as text.
pub fn fmt_number(x: f64) -> String {
    let s = format!("{x:.3}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imo_check_digits() {
        assert_eq!(imo("9074729"), Some(("9074729".into(), ImoCheck::Valid)));
        assert_eq!(imo("IMO 9321487").map(|x| x.1), Some(ImoCheck::BadCheckDigit));
        assert_eq!(imo("IMO9321483").map(|x| x.1), Some(ImoCheck::Valid));
        assert_eq!(imo("8320901").map(|x| x.1), Some(ImoCheck::Valid));
        assert_eq!(imo("1000021").map(|x| x.1), Some(ImoCheck::Valid));
        assert_eq!(imo("12345678").map(|x| x.1), Some(ImoCheck::Unverifiable));
        assert_eq!(imo("0771929"), None, "IMO numbers don't start with 0");
        assert_eq!(imo("-"), None);
        assert_eq!(imo("0"), None);
        assert_eq!(imo("123456"), None);
        assert_eq!(imo("12a4567"), None);
    }

    #[test]
    fn mmsi_handling() {
        assert_eq!(mmsi(" 316001111 ").as_deref(), Some("316001111"));
        assert_eq!(mmsi("003160001").as_deref(), Some("003160001"));
        assert_eq!(mmsi("31600111"), None);
        assert_eq!(mmsi("000000000"), None);
        assert_eq!(mmsi_kind("316001111"), "ship");
        assert_eq!(mmsi_kind("003160001"), "coast_station");
        assert_eq!(mmsi_kind("031600011"), "group_of_ships");
        assert_eq!(mmsi_kind("111316001"), "sar_aircraft");
        assert_eq!(mmsi_kind("970123456"), "ais_sart");
        assert_eq!(mmsi_kind("992316001"), "aid_to_navigation");
    }

    #[test]
    fn call_signs() {
        assert_eq!(call_sign(" hp-bj ").as_deref(), Some("HPBJ"));
        assert_eq!(call_sign("C G 2 4 3 3").as_deref(), Some("CG2433"));
        assert_eq!(call_sign("N/A"), None);
        assert_eq!(call_sign("none"), None);
    }

    #[test]
    fn names() {
        assert_eq!(name_key("M.V. ATLANTIC STAR").as_deref(), Some("ATLANTIC STAR"));
        assert_eq!(name_key("ATLANTIC-STAR").as_deref(), Some("ATLANTIC STAR"));
        assert_eq!(name_key("m/v Atlantic  Star").as_deref(), Some("ATLANTIC STAR"));
        assert_eq!(name_key("S.S. PUMPER, THE").as_deref(), Some("THE PUMPER"));
        assert_eq!(name_key("Île d'Orléans").as_deref(), Some("ILE DORLEANS"));
        assert_eq!(name_key("SALT & PEPPER").as_deref(), Some("SALT AND PEPPER"));
        assert_eq!(name_key("MV").as_deref(), Some("MV"), "a prefix alone is the name");
        assert_eq!(name_key("..."), None);
        assert_eq!(name_text("  SEGWUN  II ").as_deref(), Some("SEGWUN II"));
        assert_eq!(name_text("-"), None);
    }

    #[test]
    fn years_and_numbers() {
        assert_eq!(build_year("188700"), Some(1887));
        assert_eq!(build_year("1993"), Some(1993));
        assert_eq!(build_year("2004-11-19"), Some(2004));
        assert_eq!(build_year("0"), None);
        assert_eq!(build_year("12345"), None);
        assert_eq!(positive("36.8198"), Some(36.8198));
        assert_eq!(positive("0"), None);
        assert_eq!(fmt_number(36.8198), "36.82");
        assert_eq!(fmt_number(12.0), "12");
    }
}
