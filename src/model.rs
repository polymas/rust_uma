use std::sync::Arc;

use serde::Serialize;

use crate::{
    uma::events::common::{
        ChainLog, DisputePrice, PolymarketAncillary, PriceRequest, ProposePrice, UmaEvent,
    },
    wire::pb,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Propose,
    Dispute,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Propose => "propose",
            Self::Dispute => "dispute",
        }
    }

    pub fn to_proto(self) -> i32 {
        match self {
            Self::Propose => pb::EventType::Propose as i32,
            Self::Dispute => pb::EventType::Dispute as i32,
        }
    }

    pub fn from_proto(value: i32) -> Option<Self> {
        match pb::EventType::try_from(value).ok()? {
            pb::EventType::Propose => Some(Self::Propose),
            pb::EventType::Dispute => Some(Self::Dispute),
            pb::EventType::Unspecified => None,
        }
    }
}

/// Which of a binary condition's two outcome tokens this event's on-chain
/// price resolves to — see the doc comment on `UmaEvent.price_outcome` in
/// `proto/uma.proto` for the derivation and why it's only meaningful for
/// Propose events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriceOutcome {
    Unspecified,
    Token0,
    Token1,
    Tie,
}

impl PriceOutcome {
    /// `proposed_price` must come from a ProposePrice event; a DisputePrice's
    /// price is the value under dispute, not a fresh answer — callers must
    /// pass `Unspecified` for dispute events rather than calling this.
    pub fn from_propose_price(proposed_price: &[u8; 32]) -> Self {
        let mut token0 = [0u8; 32];
        token0[24..].copy_from_slice(&1_000_000_000_000_000_000u64.to_be_bytes());
        let mut tie = [0u8; 32];
        tie[24..].copy_from_slice(&500_000_000_000_000_000u64.to_be_bytes());
        if proposed_price.iter().all(|byte| *byte == 0) {
            Self::Token1
        } else if proposed_price == &token0 {
            Self::Token0
        } else if proposed_price == &tie {
            Self::Tie
        } else {
            // The adapter contract itself reverts any propose whose price
            // isn't one of the three canonical values, so a successfully
            // proposed price landing here would mean either a future/unknown
            // adapter with different semantics, or a decode bug — either way,
            // "no signal" is the only safe answer, not a guess.
            Self::Unspecified
        }
    }

    pub fn to_proto(self) -> i32 {
        match self {
            Self::Unspecified => pb::PriceOutcome::Unspecified as i32,
            Self::Token0 => pb::PriceOutcome::Token0 as i32,
            Self::Token1 => pb::PriceOutcome::Token1 as i32,
            Self::Tie => pb::PriceOutcome::Tie as i32,
        }
    }

    pub fn from_proto(value: i32) -> Self {
        match pb::PriceOutcome::try_from(value).unwrap_or(pb::PriceOutcome::Unspecified) {
            pb::PriceOutcome::Token0 => Self::Token0,
            pb::PriceOutcome::Token1 => Self::Token1,
            pb::PriceOutcome::Tie => Self::Tie,
            pb::PriceOutcome::Unspecified => Self::Unspecified,
        }
    }
}

/// Top-level market category, derived once at enrichment time from Gamma tag
/// IDs — see `crate::category::classify`. See the doc comment on
/// `UmaEvent.category` in `proto/uma.proto` for the derivation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
    Unspecified,
    Sports,
    Esports,
    Politics,
    Crypto,
    Culture,
    Weather,
    Other,
}

impl Category {
    pub fn to_proto(self) -> i32 {
        match self {
            Self::Unspecified => pb::Category::Unspecified as i32,
            Self::Sports => pb::Category::Sports as i32,
            Self::Esports => pb::Category::Esports as i32,
            Self::Politics => pb::Category::Politics as i32,
            Self::Crypto => pb::Category::Crypto as i32,
            Self::Culture => pb::Category::Culture as i32,
            Self::Weather => pb::Category::Weather as i32,
            Self::Other => pb::Category::Other as i32,
        }
    }

    pub fn from_proto(value: i32) -> Self {
        match pb::Category::try_from(value).unwrap_or(pb::Category::Unspecified) {
            pb::Category::Sports => Self::Sports,
            pb::Category::Esports => Self::Esports,
            pb::Category::Politics => Self::Politics,
            pb::Category::Crypto => Self::Crypto,
            pb::Category::Culture => Self::Culture,
            pb::Category::Weather => Self::Weather,
            pb::Category::Other => Self::Other,
            pb::Category::Unspecified => Self::Unspecified,
        }
    }
}

/// Bet-type sub-classification — see the doc comment on `UmaEvent.bet_type`
/// in `proto/uma.proto` for which `Category` each block belongs to and why
/// matching is always scoped to one group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BetType {
    Unspecified,
    // Sports/Esports block — see the numeric-grouping doc comment on
    // `BetType` in proto/uma.proto.
    Moneyline,
    Spread,
    OverUnder,
    GameWinner,
    Outright,
    SportsProp,
    // Weather block.
    TempHigh,
    TempLow,
    Precipitation,
    Storm,
    WeatherOther,
    // Crypto block.
    PriceTarget,
    PriceThreshold,
    UpDown,
    CryptoProp,
    // Politics block.
    ElectionWinner,
    FedRateDecision,
    TweetCount,
    PoliticsProp,
    // Culture block.
    AwardWinner,
    MediaMetricRange,
    CultureProp,
}

impl BetType {
    pub fn to_proto(self) -> i32 {
        match self {
            Self::Unspecified => pb::BetType::Unspecified as i32,
            Self::Moneyline => pb::BetType::Moneyline as i32,
            Self::Spread => pb::BetType::Spread as i32,
            Self::OverUnder => pb::BetType::OverUnder as i32,
            Self::GameWinner => pb::BetType::GameWinner as i32,
            Self::Outright => pb::BetType::Outright as i32,
            Self::SportsProp => pb::BetType::SportsProp as i32,
            Self::TempHigh => pb::BetType::TempHigh as i32,
            Self::TempLow => pb::BetType::TempLow as i32,
            Self::Precipitation => pb::BetType::Precipitation as i32,
            Self::Storm => pb::BetType::Storm as i32,
            Self::WeatherOther => pb::BetType::WeatherOther as i32,
            Self::PriceTarget => pb::BetType::PriceTarget as i32,
            Self::PriceThreshold => pb::BetType::PriceThreshold as i32,
            Self::UpDown => pb::BetType::UpDown as i32,
            Self::CryptoProp => pb::BetType::CryptoProp as i32,
            Self::ElectionWinner => pb::BetType::ElectionWinner as i32,
            Self::FedRateDecision => pb::BetType::FedRateDecision as i32,
            Self::TweetCount => pb::BetType::TweetCount as i32,
            Self::PoliticsProp => pb::BetType::PoliticsProp as i32,
            Self::AwardWinner => pb::BetType::AwardWinner as i32,
            Self::MediaMetricRange => pb::BetType::MediaMetricRange as i32,
            Self::CultureProp => pb::BetType::CultureProp as i32,
        }
    }

    pub fn from_proto(value: i32) -> Self {
        match pb::BetType::try_from(value).unwrap_or(pb::BetType::Unspecified) {
            pb::BetType::Moneyline => Self::Moneyline,
            pb::BetType::Spread => Self::Spread,
            pb::BetType::OverUnder => Self::OverUnder,
            pb::BetType::GameWinner => Self::GameWinner,
            pb::BetType::Outright => Self::Outright,
            pb::BetType::SportsProp => Self::SportsProp,
            pb::BetType::TempHigh => Self::TempHigh,
            pb::BetType::TempLow => Self::TempLow,
            pb::BetType::Precipitation => Self::Precipitation,
            pb::BetType::Storm => Self::Storm,
            pb::BetType::WeatherOther => Self::WeatherOther,
            pb::BetType::PriceTarget => Self::PriceTarget,
            pb::BetType::PriceThreshold => Self::PriceThreshold,
            pb::BetType::UpDown => Self::UpDown,
            pb::BetType::CryptoProp => Self::CryptoProp,
            pb::BetType::ElectionWinner => Self::ElectionWinner,
            pb::BetType::FedRateDecision => Self::FedRateDecision,
            pb::BetType::TweetCount => Self::TweetCount,
            pb::BetType::PoliticsProp => Self::PoliticsProp,
            pb::BetType::AwardWinner => Self::AwardWinner,
            pb::BetType::MediaMetricRange => Self::MediaMetricRange,
            pb::BetType::CultureProp => Self::CultureProp,
            pb::BetType::Unspecified => Self::Unspecified,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketEnrichment {
    pub market_id: u64,
    pub condition_id: [u8; 32],
    pub token_ids: Vec<[u8; 32]>,
    pub tag_ids: Vec<u32>,
    pub category: Category,
    pub bet_type: BetType,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EventKey {
    pub transaction_hash: [u8; 32],
    pub log_index: u32,
    pub removed: bool,
}

#[derive(Clone, Debug)]
pub struct EventRecord {
    pub sequence: u64,
    pub event: UmaEvent,
    pub enrichment: Option<Arc<MarketEnrichment>>,
    /// Computed once at decode time (`PriceOutcome::from_propose_price` for
    /// Propose, `Unspecified` for Dispute) and carried on the record rather
    /// than re-derived from a raw price at serialize time, since the raw
    /// price itself isn't part of this wire schema.
    pub price_outcome: PriceOutcome,
}

impl EventRecord {
    pub fn key(&self) -> EventKey {
        EventKey {
            transaction_hash: self.event.chain().transaction_hash,
            log_index: self.event.chain().log_index,
            removed: self.event.chain().removed,
        }
    }

    /// The condition_id to surface downstream: Gamma's authoritative value
    /// when enrichment resolved (correct for every adapter type), otherwise
    /// the on-chain derived binary-adapter value as a best-effort fallback.
    /// See `Catalog::resolve` for how enrichment is picked.
    pub fn resolved_condition_id(&self) -> [u8; 32] {
        self.enrichment
            .as_ref()
            .map(|enrichment| enrichment.condition_id)
            .unwrap_or_else(|| self.event.request().condition_id)
    }

    pub fn to_proto(&self) -> pb::UmaEvent {
        let enrichment = self.enrichment.as_deref();
        let chain = self.event.chain();
        let condition_id = self.resolved_condition_id();
        pb::UmaEvent {
            sequence: self.sequence,
            event_type: self.event.kind().to_proto(),
            transaction_hash: chain.transaction_hash.to_vec(),
            log_index: chain.log_index,
            market_id: self.event.market_id(),
            condition_id: condition_id.to_vec(),
            token_ids: enrichment
                .map(|v| v.token_ids.iter().map(|id| id.to_vec()).collect())
                .unwrap_or_default(),
            tag_ids: enrichment.map(|v| v.tag_ids.clone()).unwrap_or_default(),
            upstream_received_at_us: chain.upstream_received_at_us,
            removed: chain.removed,
            enrichment_status: if enrichment.is_some() {
                pb::EnrichmentStatus::Hit as i32
            } else {
                pb::EnrichmentStatus::Miss as i32
            },
            price_outcome: self.price_outcome.to_proto(),
            category: enrichment
                .map_or(Category::Unspecified, |v| v.category)
                .to_proto(),
            bet_type: enrichment
                .map_or(BetType::Unspecified, |v| v.bet_type)
                .to_proto(),
        }
    }

    /// Reconstructs a record from a persisted/wire `pb::UmaEvent`. Fields
    /// retired from the wire schema (see `proto/uma.proto`) aren't
    /// recoverable here — this only backs `EventHub` replay (dedup key,
    /// re-broadcast), which never reads them; it doesn't reconstruct the rich
    /// on-chain view that only exists transiently during live decode.
    pub fn from_proto(value: pb::UmaEvent) -> Option<Self> {
        let condition_id = fixed::<32>(&value.condition_id)?;
        let enrichment = (value.enrichment_status == pb::EnrichmentStatus::Hit as i32).then(|| {
            Arc::new(MarketEnrichment {
                market_id: value.market_id,
                condition_id,
                token_ids: value
                    .token_ids
                    .iter()
                    .filter_map(|v| fixed::<32>(v))
                    .collect(),
                tag_ids: value.tag_ids.clone(),
                category: Category::from_proto(value.category),
                bet_type: BetType::from_proto(value.bet_type),
            })
        });
        let kind = EventKind::from_proto(value.event_type)?;
        let chain = ChainLog {
            block_number: 0,
            transaction_hash: fixed::<32>(&value.transaction_hash)?,
            log_index: value.log_index,
            upstream_received_at_us: value.upstream_received_at_us,
            removed: value.removed,
        };
        let request = PriceRequest {
            requester: [0; 20],
            condition_id,
            ancillary: PolymarketAncillary {
                question_id: [0; 32],
                market_id: (value.market_id != 0).then_some(value.market_id),
            },
            proposed_price: [0; 32],
        };
        let event = match kind {
            EventKind::Propose => UmaEvent::ProposePrice(ProposePrice { chain, request }),
            EventKind::Dispute => UmaEvent::DisputePrice(DisputePrice { chain, request }),
        };
        Some(Self {
            sequence: value.sequence,
            event,
            enrichment,
            price_outcome: PriceOutcome::from_proto(value.price_outcome),
        })
    }
}

#[cfg(test)]
pub(crate) fn test_uma_event(transaction_byte: u8, market_id: u64) -> UmaEvent {
    UmaEvent::ProposePrice(ProposePrice {
        chain: ChainLog {
            block_number: 10,
            transaction_hash: [transaction_byte; 32],
            log_index: 3,
            upstream_received_at_us: 8,
            removed: false,
        },
        request: PriceRequest {
            requester: [6; 20],
            condition_id: [10; 32],
            ancillary: PolymarketAncillary {
                question_id: [3; 32],
                market_id: Some(market_id),
            },
            proposed_price: [5; 32],
        },
    })
}

pub fn fixed<const N: usize>(value: &[u8]) -> Option<[u8; N]> {
    value.try_into().ok()
}

pub fn hex_prefixed(value: &[u8]) -> String {
    let mut result = String::with_capacity(2 + value.len() * 2);
    result.push_str("0x");
    result.push_str(&hex::encode(value));
    result
}

pub fn uint256_decimal(value: &[u8; 32]) -> String {
    if value.iter().all(|byte| *byte == 0) {
        return "0".into();
    }
    let mut working = *value;
    let mut digits = Vec::with_capacity(78);
    while working.iter().any(|byte| *byte != 0) {
        let mut remainder = 0_u16;
        for byte in &mut working {
            let current = (remainder << 8) | *byte as u16;
            *byte = (current / 10) as u8;
            remainder = current % 10;
        }
        digits.push(b'0' + remainder as u8);
    }
    digits.reverse();
    String::from_utf8(digits).expect("decimal digits")
}

#[cfg(test)]
mod tests {
    use super::{
        BetType, Category, EventRecord, MarketEnrichment, PriceOutcome, pb, test_uma_event,
        uint256_decimal,
    };

    #[test]
    fn formats_uint256_decimal() {
        let mut value = [0_u8; 32];
        value[30..].copy_from_slice(&[1, 2]);
        assert_eq!(uint256_decimal(&value), "258");
    }

    #[test]
    fn price_outcome_matches_the_ctf_adapters_payout_convention() {
        // UmaCtfAdapter.sol::_constructPayouts, verified against the deployed
        // source on Polygon (both the standard and the "Neg Risk" requester
        // addresses run this exact contract): "Payouts: [YES, NO]" —
        // price==1e18 -> payouts[0] -> token_ids[0]; price==0 -> payouts[1]
        // -> token_ids[1]; price==0.5e18 is a tie, no token wins.
        let mut yes = [0u8; 32];
        yes[24..].copy_from_slice(&1_000_000_000_000_000_000u64.to_be_bytes());
        let mut tie = [0u8; 32];
        tie[24..].copy_from_slice(&500_000_000_000_000_000u64.to_be_bytes());
        let no = [0u8; 32];
        let neither_canonical = [7u8; 32];

        assert_eq!(PriceOutcome::from_propose_price(&yes), PriceOutcome::Token0);
        assert_eq!(PriceOutcome::from_propose_price(&no), PriceOutcome::Token1);
        assert_eq!(PriceOutcome::from_propose_price(&tie), PriceOutcome::Tie);
        assert_eq!(
            PriceOutcome::from_propose_price(&neither_canonical),
            PriceOutcome::Unspecified
        );
    }

    #[test]
    fn wire_fields_round_trip_through_protobuf() {
        // Only asserts on what's actually part of the wire schema — most
        // on-chain fields (block metadata, participant addresses, ancillary
        // text, ...) are intentionally not on the wire (see proto/uma.proto)
        // and `from_proto` fills them with placeholders, so a full
        // `decoded.event == record.event` equality no longer holds.
        let record = EventRecord {
            sequence: 7,
            event: test_uma_event(2, 42),
            enrichment: None,
            price_outcome: PriceOutcome::Token0,
        };
        let decoded = EventRecord::from_proto(record.to_proto()).unwrap();
        assert_eq!(decoded.sequence, 7);
        assert_eq!(decoded.event.kind(), record.event.kind());
        assert_eq!(
            decoded.event.chain().transaction_hash,
            record.event.chain().transaction_hash
        );
        assert_eq!(
            decoded.event.chain().log_index,
            record.event.chain().log_index
        );
        assert_eq!(decoded.event.market_id(), record.event.market_id());
        assert_eq!(decoded.price_outcome, PriceOutcome::Token0);
    }

    /// End-to-end proof, using the real Neg Risk event captured on Polygon
    /// (tx 0x96bdaafcd0f9f498f4b76e0f7169c8e28aee2d851b39197f3cee16825a43db6d,
    /// Gamma market 907474) and its real Gamma catalog row, that the wire
    /// output carries Gamma's authoritative condition_id and a Hit status —
    /// not the on-chain binary formula's wrong value — once market_id-first
    /// resolution runs.
    #[test]
    fn neg_risk_event_broadcasts_gamma_condition_id_after_resolution() {
        use crate::{
            enrichment::Catalog,
            uma::events::{RpcLog, TOPIC_PROPOSE_PRICE, decode_fixed, decode_signal_log},
        };

        let raw = RpcLog {
            address: "0xee3afe347d5c74317041e2618c49534daf887c24".into(),
            topics: vec![
                TOPIC_PROPOSE_PRICE.into(),
                "0x0000000000000000000000002f5e3684cb1f318ec51b00edba38d79ac2c0aa9d".into(),
                "0x000000000000000000000000ca323ed4e6dd651368c754d7c5a9d345e5c81829".into(),
            ],
            data: "0x5945535f4f525f4e4f5f51554552590000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006939e7e600000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006a978df20000000000000000000000002791bca1f2de4661ed88a30c99a7a9449aa8417400000000000000000000000000000000000000000000000000000000000003fa713a207469746c653a2057696c6c2043616e646964617465205a2077696e207468652032303236204d61737361636875736574747320476f7665726e6f722052657075626c6963616e207072696d61727920656c656374696f6e3f2c206465736372697074696f6e3a2054686973206d61726b65742077696c6c207265736f6c7665206163636f7264696e6720746f207468652077696e6e6572206f66207468652052657075626c6963616e205072696d61727920666f7220476f7665726e6f72206f66204d6173736163687573657474732c207363686564756c656420746f2074616b6520706c616365206f6e2053657074656d62657220312c20323032362e205265736f6c7574696f6e2077696c6c206265206261736564206f6e20746865206f766572616c6c2077696e6e6572206f6620746865207072696d6172792c20696e636c7564696e6720616e7920706f74656e7469616c207365636f6e6420726f756e64206f722072756e2d6f66662e0a0a4966206e6f2032303236204d6173736163687573657474732047756265726e61746f7269616c2052657075626c6963616e205072696d6172792074616b657320706c6163652c2074686973206d61726b65742077696c6c207265736f6c766520746f20e2809c4f746865722ee2809d0a0a546865207265736f6c7574696f6e20736f7572636520666f722074686973206d61726b65742077696c6c20626520746865206669727374206f6666696369616c20616e6e6f756e63656d656e74206f662074686520726573756c74732066726f6d20746865204d6173736163687573657474732052657075626c6963616e2050617274793b20686f77657665722c20616e206f7665727768656c6d696e6720636f6e73656e737573206f66206372656469626c65207265706f7274696e67206d617920737566666963652e206d61726b65745f69643a20393037343734207265735f646174613a2070313a20302c2070323a20312c2070333a20302e352e20576865726520703120636f72726573706f6e647320746f204e6f2c20703220746f205965732c20703320746f20756e6b6e6f776e2e20546869732072657175657374204d555354206f6e6c79207265736f6c766520746f207031206f722070322e202055706461746573206d61646520627920746865207175657374696f6e2063726561746f7220766961207468652062756c6c6574696e20626f617264206174203078324635653336383463623146333138656335316230304564626133386437394163326330614139642073686f756c6420626520636f6e736964657265642e2c696e697469616c697a65723a39313433306361643264333937353736363439393731376661306436366137386438313465356335000000000000".into(),
            block_number: "0x58c2286".into(),
            block_hash: "0xf50109a4c9ab6bbf0360e5909c78fadce725a2374fe01b31994bae2680a99932".into(),
            transaction_hash: "0x96bdaafcd0f9f498f4b76e0f7169c8e28aee2d851b39197f3cee16825a43db6d".into(),
            transaction_index: Some("0x6f".into()),
            log_index: "0x816".into(),
            removed: false,
        };
        let emitter = decode_fixed::<20>(&raw.address, "address").unwrap();
        let event = decode_signal_log(&raw, 1, &[emitter], true).unwrap();

        let gamma_condition_id = decode_fixed::<32>(
            "0xa50547851bf565603ad7e866d9d2aa2c6c2ee77b2d390e581bf2e8a53b466902",
            "condition_id",
        )
        .unwrap();
        let derived_condition_id = event.request().condition_id;
        assert_ne!(
            derived_condition_id, gamma_condition_id,
            "sanity check: the binary formula must disagree with Gamma for this market"
        );

        let catalog = Catalog::new(vec![MarketEnrichment {
            market_id: 907_474,
            condition_id: gamma_condition_id,
            token_ids: vec![[0x22; 32]],
            tag_ids: vec![100],
            category: Category::Unspecified,
            bet_type: BetType::Unspecified,
        }]);
        let enrichment =
            catalog.resolve(event.request().ancillary.market_id, &derived_condition_id);
        let price_outcome = PriceOutcome::from_propose_price(&event.request().proposed_price);

        let record = EventRecord {
            sequence: 1,
            event,
            enrichment,
            price_outcome,
        };
        assert_eq!(record.resolved_condition_id(), gamma_condition_id);

        let proto = record.to_proto();
        assert_eq!(proto.condition_id, gamma_condition_id.to_vec());
        assert_eq!(proto.enrichment_status, pb::EnrichmentStatus::Hit as i32);
        assert_eq!(proto.token_ids, vec![[0x22; 32].to_vec()]);
        // Real captured price for this tx was 0 (word 3 of the log data is
        // all zeros) -> per UmaCtfAdapter's payout convention that's token_ids[1].
        assert_eq!(proto.price_outcome, pb::PriceOutcome::Token1 as i32);
    }
}
