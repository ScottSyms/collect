//! Minimal AIS 6-bit payload decoder.
//!
//! AIVDM sentences carry their binary message ASCII-armored: each payload
//! character encodes 6 bits. This module unpacks that into a flat bit buffer
//! and reads fixed-width fields out of it (unsigned, two's-complement signed,
//! boolean, and raw hex). It exists because the pinned `nmea-parser` has no
//! Type 8 decoder, so ais-parse decodes those messages itself.

/// The armored payload and framing fields pulled from one NMEA sentence.
pub struct AisPayload<'a> {
    /// Number of fragments this sentence is split into (`1` = single/combined).
    pub fragment_count: u32,
    /// The ASCII-armored 6-bit payload (NMEA field 5).
    pub armored: &'a str,
    /// Number of padding bits in the final character (NMEA field 6).
    pub fill_bits: u32,
}

/// Split an `!AI VDM/VDO` sentence into its payload framing fields.
///
/// Expects `!AIVDM,<count>,<num>,<seq>,<chan>,<armored>,<fill>*<cksum>`.
/// Returns `None` for anything that isn't a well-formed AIVDM/AIVDO sentence.
pub fn extract_ais_payload(sentence: &str) -> Option<AisPayload<'_>> {
    let sentence = sentence.trim();
    let body = sentence
        .strip_prefix("!AIVDM")
        .or_else(|| sentence.strip_prefix("!AIVDO"))?;
    // body now starts with the comma after the sentence id.
    let mut fields = body.split(',');
    // The strip left a leading empty field before the first comma-separated
    // value; `split` on ",1,1,,A,payload,0*XX" yields "", "1", "1", "", "A", ...
    let _empty = fields.next()?;
    let fragment_count: u32 = fields.next()?.parse().ok()?;
    let _fragment_number = fields.next()?;
    let _seq_id = fields.next()?;
    let _channel = fields.next()?;
    let armored = fields.next()?;
    let fill_field = fields.next()?;
    let fill_bits: u32 = fill_field
        .split('*')
        .next()
        .unwrap_or("0")
        .trim()
        .parse()
        .unwrap_or(0);
    if armored.is_empty() {
        return None;
    }
    Some(AisPayload {
        fragment_count,
        armored,
        fill_bits,
    })
}

/// A flat, MSB-first bit buffer unpacked from an armored AIS payload.
pub struct Bits {
    bytes: Vec<u8>,
    bit_len: usize,
}

impl Bits {
    /// Unpack an armored payload into a bit buffer, dropping `fill_bits`
    /// padding bits from the end. Returns `None` on an invalid armor char.
    pub fn from_armored(armored: &str, fill_bits: u32) -> Option<Self> {
        let mut bytes = Vec::with_capacity(armored.len() * 6 / 8 + 1);
        let mut acc: u32 = 0;
        let mut acc_bits: u32 = 0;
        for ch in armored.bytes() {
            let sixbit = armor_to_sixbit(ch)?;
            acc = (acc << 6) | u32::from(sixbit);
            acc_bits += 6;
            while acc_bits >= 8 {
                acc_bits -= 8;
                bytes.push(((acc >> acc_bits) & 0xFF) as u8);
            }
        }
        if acc_bits > 0 {
            // Left-align the remaining bits into a final byte.
            bytes.push(((acc << (8 - acc_bits)) & 0xFF) as u8);
        }
        let total_bits = armored.len() * 6;
        let bit_len = total_bits.saturating_sub(fill_bits as usize);
        Some(Bits { bytes, bit_len })
    }

    pub fn len(&self) -> usize {
        self.bit_len
    }

    #[allow(dead_code)] // paired with `len` to satisfy clippy::len_without_is_empty
    pub fn is_empty(&self) -> bool {
        self.bit_len == 0
    }

    /// Read `n` bits (n ≤ 64) starting at `off` as an unsigned integer,
    /// MSB-first. Bits past the buffer end read as 0.
    pub fn u(&self, off: usize, n: usize) -> u64 {
        debug_assert!(n <= 64);
        let mut value: u64 = 0;
        for i in 0..n {
            value = (value << 1) | u64::from(self.bit_at(off + i));
        }
        value
    }

    /// Read `n` bits as a two's-complement signed integer.
    pub fn i(&self, off: usize, n: usize) -> i64 {
        let raw = self.u(off, n);
        if n == 0 || n >= 64 {
            return raw as i64;
        }
        let sign_bit = 1u64 << (n - 1);
        if raw & sign_bit != 0 {
            (raw as i64) - (1i64 << n)
        } else {
            raw as i64
        }
    }

    pub fn boolean(&self, off: usize) -> bool {
        self.bit_at(off) != 0
    }

    /// The remaining bits from `off` to the buffer end, as an uppercase hex
    /// string (the last nibble is zero-padded if the tail isn't byte-aligned).
    pub fn hex_from(&self, off: usize) -> String {
        if off >= self.bit_len {
            return String::new();
        }
        let n = self.bit_len - off;
        let mut out = String::with_capacity(n.div_ceil(4));
        let mut i = 0;
        while i < n {
            let take = (n - i).min(4);
            let nibble = self.u(off + i, take) << (4 - take);
            out.push(char::from_digit(nibble as u32, 16).unwrap_or('0'));
            i += 4;
        }
        out.to_uppercase()
    }

    fn bit_at(&self, index: usize) -> u8 {
        let byte = index / 8;
        if byte >= self.bytes.len() {
            return 0;
        }
        let shift = 7 - (index % 8);
        (self.bytes[byte] >> shift) & 1
    }
}

/// A deliberately different (bit-vector) MSB-first packer used only in tests,
/// so decoder offset bugs can't be masked by a shared implementation. Shared
/// across `ais_bits` and `decode` test modules for building Type 8 messages.
#[cfg(test)]
pub(crate) struct BitPacker {
    bits: Vec<u8>,
}

#[cfg(test)]
impl BitPacker {
    pub fn new() -> Self {
        BitPacker { bits: Vec::new() }
    }
    /// Append `value`'s low `n` bits, MSB first.
    pub fn push(&mut self, value: u64, n: usize) {
        for i in (0..n).rev() {
            self.bits.push(((value >> i) & 1) as u8);
        }
    }
    /// Append `value` as a two's-complement signed field of `n` bits.
    pub fn push_i(&mut self, value: i64, n: usize) {
        let mask = if n >= 64 { u64::MAX } else { (1u64 << n) - 1 };
        self.push((value as u64) & mask, n);
    }
    pub fn bit_len(&self) -> usize {
        self.bits.len()
    }
    /// Re-armor the packed bits into an AIS 6-bit ASCII payload string.
    pub fn armored(&self) -> String {
        let mut s = String::new();
        let mut i = 0;
        while i < self.bits.len() {
            let mut v = 0u8;
            for j in 0..6 {
                let bit = self.bits.get(i + j).copied().unwrap_or(0);
                v = (v << 1) | bit;
            }
            s.push(if v < 40 {
                (v + 48) as char
            } else {
                (v + 56) as char
            });
            i += 6;
        }
        s
    }
    pub fn into_bits(self) -> Bits {
        let bit_len = self.bits.len();
        let mut bytes = vec![0u8; bit_len.div_ceil(8)];
        for (i, bit) in self.bits.iter().enumerate() {
            if *bit != 0 {
                bytes[i / 8] |= 1 << (7 - (i % 8));
            }
        }
        Bits { bytes, bit_len }
    }
}

/// Standard AIS armor decode: `c - 48`, and subtract a further 8 if `> 40`.
/// Valid characters map to 0..=63.
fn armor_to_sixbit(ch: u8) -> Option<u8> {
    let mut v = ch.wrapping_sub(48);
    if v > 40 {
        v = v.wrapping_sub(8);
    }
    if v <= 63 {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_payload_fields() {
        let p =
            extract_ais_payload("!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A").expect("parse");
        assert_eq!(p.fragment_count, 1);
        assert_eq!(p.fill_bits, 0);
        assert_eq!(p.armored, "15RTgt0PAso;90TKcjM8h6g208CQ");

        let multi = extract_ais_payload("!AIVDM,2,1,3,B,55P5,0*00").expect("parse");
        assert_eq!(multi.fragment_count, 2);

        assert!(extract_ais_payload("$GPGGA,foo").is_none());
        assert!(extract_ais_payload("!AIVDM,1,1,,A,,0*00").is_none());
    }

    #[test]
    fn message_type_of_position_report() {
        // A type-1 sentence: first 6 bits == 1.
        let p = extract_ais_payload("!AIVDM,1,1,,A,15RTgt0PAso;90TKcjM8h6g208CQ,0*4A").unwrap();
        let bits = Bits::from_armored(p.armored, p.fill_bits).unwrap();
        assert_eq!(bits.u(0, 6), 1);
    }

    #[test]
    fn reads_unsigned_signed_bool() {
        // Hand-pack a known bit pattern with an independent MSB-first packer.
        let mut b = BitPacker::new();
        b.push(8, 6); // 001000
        b.push(0b10, 2);
        b.push(0xABCDEF, 30);
        let bits = b.into_bits();
        assert_eq!(bits.u(0, 6), 8);
        assert_eq!(bits.u(6, 2), 0b10);
        assert_eq!(bits.u(8, 30), 0xABCDEF);
        assert!(bits.boolean(6)); // first bit of 0b10 is 1
    }

    #[test]
    fn signed_two_complement() {
        let mut b = BitPacker::new();
        // -1024 in 11 bits = 0b100_0000_0000
        b.push(0b100_0000_0000, 11);
        // +250 in 11 bits
        b.push(250, 11);
        let bits = b.into_bits();
        assert_eq!(bits.i(0, 11), -1024);
        assert_eq!(bits.i(11, 11), 250);
    }

    #[test]
    fn hex_tail() {
        let mut b = BitPacker::new();
        b.push(0xF, 4);
        b.push(0x0, 4);
        b.push(0xA, 4);
        let bits = b.into_bits();
        assert_eq!(bits.hex_from(0), "F0A");
        assert_eq!(bits.hex_from(4), "0A");
    }
}
