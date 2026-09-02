use crate::model::EventKind;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainLog {
    pub contract_address: [u8; 20],
    pub block_number: u64,
    pub block_hash: [u8; 32],
    pub transaction_hash: [u8; 32],
    pub transaction_index: Option<u32>,
    pub log_index: u32,
    pub upstream_received_at_us: u64,
    pub removed: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolutionValues {
    pub p1: Option<String>,
    pub p2: Option<String>,
    pub p3: Option<String>,
    pub p4: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketAncillary {
    pub question_id: [u8; 32],
    pub question: String,
    pub resolution: ResolutionValues,
    pub initializer: Option<[u8; 20]>,
    pub market_id: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceRequest {
    pub requester: [u8; 20],
    pub proposer: [u8; 20],
    pub condition_id: [u8; 32],
    pub identifier: [u8; 32],
    pub timestamp: u64,
    pub ancillary: PolymarketAncillary,
    pub proposed_price: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposePrice {
    pub chain: ChainLog,
    pub request: PriceRequest,
    pub expiration_timestamp: u64,
    pub currency: [u8; 20],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisputePrice {
    pub chain: ChainLog,
    pub request: PriceRequest,
    pub disputer: [u8; 20],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UmaEvent {
    ProposePrice(ProposePrice),
    DisputePrice(DisputePrice),
}

impl UmaEvent {
    pub fn kind(&self) -> EventKind {
        match self {
            Self::ProposePrice(_) => EventKind::Propose,
            Self::DisputePrice(_) => EventKind::Dispute,
        }
    }

    pub fn chain(&self) -> &ChainLog {
        match self {
            Self::ProposePrice(event) => &event.chain,
            Self::DisputePrice(event) => &event.chain,
        }
    }

    pub fn request(&self) -> &PriceRequest {
        match self {
            Self::ProposePrice(event) => &event.request,
            Self::DisputePrice(event) => &event.request,
        }
    }

    pub fn market_id(&self) -> u64 {
        self.request().ancillary.market_id.unwrap_or_default()
    }

    pub fn disputer(&self) -> Option<&[u8; 20]> {
        match self {
            Self::ProposePrice(_) => None,
            Self::DisputePrice(event) => Some(&event.disputer),
        }
    }
}
