use crate::model::EventKind;

/// Only the on-chain fields the hot path (dedup, enrichment, `latest_block`
/// gauge) or the wire schema actually reads. Everything else the raw log
/// carries (contract address, block hash, transaction index, ...) is
/// deliberately not parsed out here — it was decoded, stored, and never read
/// again, purely for a wire field that no longer exists. Reconstructable via
/// `transaction_hash` from any Polygon RPC if ever needed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainLog {
    pub block_number: u64,
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub upstream_received_at_us: u64,
    pub removed: bool,
}

/// Only the ancillary fields actually used: `question_id` for the on-chain
/// binary-adapter condition_id fallback, `market_id` for enrichment lookup.
/// The question text and res_data/initializer used to be parsed here too,
/// but nothing has read them since the wire schema dropped `question`/
/// `resolution_p1..p4` — see `proto/uma.proto`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketAncillary {
    pub question_id: [u8; 32],
    pub market_id: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceRequest {
    pub requester: [u8; 20],
    pub condition_id: [u8; 32],
    pub ancillary: PolymarketAncillary,
    pub proposed_price: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposePrice {
    pub chain: ChainLog,
    pub request: PriceRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisputePrice {
    pub chain: ChainLog,
    pub request: PriceRequest,
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
}
