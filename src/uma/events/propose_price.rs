use super::{
    DecodeError, RpcLog, build_chain,
    common::{ProposePrice, UmaEvent},
    parse_data, parse_request,
};

pub fn parse(raw: &RpcLog, received_at_us: u64) -> Result<UmaEvent, DecodeError> {
    if raw.topics.len() < 3 {
        return Err(DecodeError::Topic);
    }
    let data = parse_data(raw, 4)?;
    Ok(UmaEvent::ProposePrice(ProposePrice {
        chain: build_chain(raw, received_at_us)?,
        request: parse_request(raw, &data)?,
    }))
}
