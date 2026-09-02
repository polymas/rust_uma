use super::{
    DecodeError, RpcLog, build_chain,
    common::{DisputePrice, UmaEvent},
    parse_data, parse_request,
};

pub fn parse(raw: &RpcLog, received_at_us: u64) -> Result<UmaEvent, DecodeError> {
    if raw.topics.len() < 4 {
        return Err(DecodeError::Topic);
    }
    let data = parse_data(raw, 4)?;
    Ok(UmaEvent::DisputePrice(DisputePrice {
        chain: build_chain(raw, received_at_us)?,
        request: parse_request(raw, &data)?,
    }))
}
