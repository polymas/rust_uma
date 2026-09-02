use sha3::{Digest, Keccak256};

use super::{
    DecodeError,
    common::{PolymarketAncillary, ResolutionValues},
};

pub fn parse_ancillary(raw: &[u8]) -> Result<PolymarketAncillary, DecodeError> {
    let text = std::str::from_utf8(raw).map_err(|_| DecodeError::AncillaryUtf8)?;
    let question_id: [u8; 32] = Keccak256::digest(raw).into();
    let resolution_start = find_key(text, "res_data");
    let question_start = find_key(text, "q").map(|index| value_start(text, index, "q"));
    let question_end = resolution_start.unwrap_or(text.len());
    let question = question_start
        .filter(|start| *start <= question_end)
        .map(|start| trim_value(&text[start..question_end]))
        .unwrap_or_default()
        .to_owned();
    let resolution_text = resolution_start
        .map(|index| &text[value_start(text, index, "res_data")..])
        .unwrap_or_default();

    Ok(PolymarketAncillary {
        question_id,
        question,
        resolution: ResolutionValues {
            p1: field_value(resolution_text, "p1"),
            p2: field_value(resolution_text, "p2"),
            p3: field_value(resolution_text, "p3"),
            p4: field_value(resolution_text, "p4"),
        },
        initializer: field_value(text, "initializer")
            .and_then(|value| parse_address(value.trim()).ok()),
        market_id: field_number(text, "market_id"),
    })
}

/// Like `field_value`, but bounds the value at the first non-digit character
/// instead of the next comma. `market_id` templates are not always
/// comma-terminated — real Neg Risk ancillary data has been observed as
/// `market_id: 907474 res_data: ...` (space, not comma), which would
/// otherwise pull the whole trailing res_data text into the "value" and fail
/// to parse as a number.
fn field_number(text: &str, key: &str) -> Option<u64> {
    let index = find_key(text, key)?;
    let start = value_start(text, index, key);
    let digits: String = text[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn field_value(text: &str, key: &str) -> Option<String> {
    let index = find_key(text, key)?;
    let start = value_start(text, index, key);
    let tail = &text[start..];
    let end = tail.find(',').unwrap_or(tail.len());
    let value = trim_value(&tail[..end]);
    (!value.is_empty()).then(|| value.to_owned())
}

fn find_key(text: &str, key: &str) -> Option<usize> {
    let needle = format!("{key}:");
    text.match_indices(&needle).find_map(|(index, _)| {
        let boundary = index == 0
            || text.as_bytes()[index - 1].is_ascii_whitespace()
            || text.as_bytes()[index - 1] == b',';
        boundary.then_some(index)
    })
}

fn value_start(text: &str, index: usize, key: &str) -> usize {
    let mut start = index + key.len() + 1;
    while text
        .as_bytes()
        .get(start)
        .is_some_and(u8::is_ascii_whitespace)
    {
        start += 1;
    }
    start
}

fn trim_value(value: &str) -> &str {
    value.trim().trim_matches(',').trim()
}

fn parse_address(value: &str) -> Result<[u8; 20], ()> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    hex::decode(raw).map_err(|_| ())?.try_into().map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_polymarket_business_fields() {
        let value = parse_ancillary(b"q: Will it happen? res_data: p1: 0, p2: 1, p3: 0.5, p4: -1, market_id: 42, initializer: 1111111111111111111111111111111111111111").unwrap();
        assert_eq!(value.question, "Will it happen?");
        assert_eq!(value.market_id, Some(42));
        assert_eq!(value.resolution.p1.as_deref(), Some("0"));
        assert_eq!(value.resolution.p4.as_deref(), Some("-1"));
        assert_eq!(value.initializer, Some([0x11; 20]));
    }

    #[test]
    fn parses_market_id_when_not_comma_terminated() {
        // Real Neg Risk ancillary shape: market_id is followed by a space and
        // "res_data:", not a comma (Polygon tx
        // 0x96bdaafcd0f9f498f4b76e0f7169c8e28aee2d851b39197f3cee16825a43db6d).
        let value = parse_ancillary(
            b"q: Will it happen? market_id: 907474 res_data: p1: 0, p2: 1, p3: 0.5.",
        )
        .unwrap();
        assert_eq!(value.market_id, Some(907_474));
        assert_eq!(value.resolution.p1.as_deref(), Some("0"));
    }
}
