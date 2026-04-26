use std::fmt::Display;

pub const SCAN_REPORT_INTERVAL: usize = 100;
pub const LIST_REPORT_INTERVAL: usize = 1000;

pub fn report(stage: &str, message: impl Display) {
    eprintln!("[{}] {}", stage, message);
}

pub fn report_step(stage: &str, current: usize, total: usize, detail: impl Display) {
    if should_report(current, total, SCAN_REPORT_INTERVAL) {
        report(
            stage,
            format!("{}/{} {}", count(current), count(total), detail),
        );
    }
}

pub fn should_report(current: usize, total: usize, interval: usize) -> bool {
    current == 1 || current == total || current % interval == 0
}

pub fn count(value: impl ToString) -> String {
    format_with_thousands(&value.to_string())
}

pub fn decimal(value: f64, precision: usize) -> String {
    let formatted = format!("{:.*}", precision, value);
    format_with_thousands(&formatted)
}

fn format_with_thousands(raw: &str) -> String {
    let (sign, digits) = raw.strip_prefix('-').map_or(("", raw), |rest| ("-", rest));
    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));

    let mut grouped = String::with_capacity(integer.len() + integer.len() / 3);
    for (index, ch) in integer.chars().rev().enumerate() {
        if index != 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }

    let mut result = String::with_capacity(raw.len() + raw.len() / 3);
    result.push_str(sign);
    result.extend(grouped.chars().rev());
    if !fraction.is_empty() {
        result.push('.');
        result.push_str(fraction);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{count, decimal};

    #[test]
    fn formats_counts_with_commas() {
        assert_eq!(count(0usize), "0");
        assert_eq!(count(1_234usize), "1,234");
        assert_eq!(count(12_345_678usize), "12,345,678");
    }

    #[test]
    fn formats_decimals_with_commas() {
        assert_eq!(decimal(12.3, 1), "12.3");
        assert_eq!(decimal(1_234.5, 1), "1,234.5");
    }
}
