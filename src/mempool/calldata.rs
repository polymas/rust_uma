//! 从 pending 交易的 calldata 解析 OOv2 的 propose / dispute 调用，并合成一条与链上
//! ProposePrice / DisputePrice 日志等价的 `RpcLog`，交给现有 `Processor::process`。
//!
//! 支持的调用（都发往 OOv2 本体，不认识的包装合约一律跳过）：
//! - `proposePrice(address,bytes32,uint256,bytes,int256)`
//! - `proposePriceFor(address proposer,address,bytes32,uint256,bytes,int256)`
//! - `disputePrice(address,bytes32,uint256,bytes)`
//! - `disputePriceFor(address disputer,address,bytes32,uint256,bytes)`
//! - `multicall(bytes[])`，其中每一项是上面四种之一
//!
//! 合成日志的字段取值：`transactionHash` = pending 交易哈希；`logIndex` = 该调用在交易
//! 里的序号；`blockNumber` = 0（尚未进块）；`address` = 交易的目标合约。事件 data 的布局
//! 与链上一致（见 `uma/events/propose_price.rs`），所以解码、富化、编码全部原样复用。

use sha3::{Digest, Keccak256};

use crate::uma::events::{RpcLog, TOPIC_DISPUTE_PRICE, TOPIC_PROPOSE_PRICE};

const SEL_PROPOSE: [u8; 4] = [0xb8, 0xb4, 0xf9, 0x08];
const SEL_PROPOSE_FOR: [u8; 4] = [0x7c, 0x82, 0x28, 0x8f];
const SEL_DISPUTE: [u8; 4] = [0xfb, 0xa7, 0xf1, 0xe3];
const SEL_DISPUTE_FOR: [u8; 4] = [0x76, 0xc7, 0x82, 0x3f];
const SEL_MULTICALL: [u8; 4] = [0xac, 0x96, 0x50, 0xd8];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallKind {
    Propose,
    Dispute,
}

/// 一次 propose / dispute 调用里能从 calldata 拿到的全部字段。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OracleCall {
    pub kind: CallKind,
    pub requester: [u8; 20],
    pub identifier: [u8; 32],
    pub timestamp: [u8; 32],
    pub ancillary: Vec<u8>,
    /// propose 的报价；dispute 的 calldata 里没有报价，为 0。
    pub price: [u8; 32],
    /// `proposePriceFor` / `disputePriceFor` 里显式指定的提案人 / 争议人。
    pub actor: Option<[u8; 20]>,
}

impl OracleCall {
    /// 与 UMA 自己的请求编号等价的去重键：同一 requester、问题（ancillary 的 keccak）和
    /// 请求时间戳。对 Polymarket 适配合约来说即 (condition_id, 请求时间戳)。
    pub fn request_key(&self) -> RequestKey {
        RequestKey {
            requester: self.requester,
            question_id: Keccak256::digest(&self.ancillary).into(),
            timestamp: u64_word(&self.timestamp),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct RequestKey {
    pub requester: [u8; 20],
    pub question_id: [u8; 32],
    pub timestamp: u64,
}

impl RequestKey {
    /// 同一个市场（同 requester + 同问题）的不同轮请求共用这个键。
    pub fn market(&self) -> ([u8; 20], [u8; 32]) {
        (self.requester, self.question_id)
    }
}

fn u64_word(w: &[u8; 32]) -> u64 {
    u64::from_be_bytes(w[24..].try_into().expect("8 bytes"))
}

fn word(b: &[u8], i: usize) -> Option<[u8; 32]> {
    b.get(i * 32..(i + 1) * 32)
        .map(|s| s.try_into().expect("32 bytes"))
}

fn usize_at(b: &[u8], offset: usize) -> Option<usize> {
    let w: [u8; 32] = b.get(offset..offset + 32)?.try_into().ok()?;
    if w[..24].iter().any(|x| *x != 0) {
        return None;
    }
    usize::try_from(u64::from_be_bytes(w[24..].try_into().ok()?)).ok()
}

fn dynamic_bytes(args: &[u8], head_index: usize) -> Option<Vec<u8>> {
    let off = usize_at(args, head_index * 32)?;
    let len = usize_at(args, off)?;
    args.get(off + 32..off + 32 + len).map(<[u8]>::to_vec)
}

fn address(w: [u8; 32]) -> [u8; 20] {
    w[12..].try_into().expect("20 bytes")
}

fn decode_one(call: &[u8]) -> Option<OracleCall> {
    let (sel, args) = (call.get(..4)?, call.get(4..)?);
    let (kind, o) = match sel {
        s if s == SEL_PROPOSE => (CallKind::Propose, 0),
        s if s == SEL_PROPOSE_FOR => (CallKind::Propose, 1),
        s if s == SEL_DISPUTE => (CallKind::Dispute, 0),
        s if s == SEL_DISPUTE_FOR => (CallKind::Dispute, 1),
        _ => return None,
    };
    Some(OracleCall {
        kind,
        actor: if o == 1 {
            Some(address(word(args, 0)?))
        } else {
            None
        },
        requester: address(word(args, o)?),
        identifier: word(args, o + 1)?,
        timestamp: word(args, o + 2)?,
        ancillary: dynamic_bytes(args, o + 3)?,
        price: if kind == CallKind::Propose {
            word(args, o + 4)?
        } else {
            [0; 32]
        },
    })
}

/// 解析一笔交易 input；不认识的调用（包装合约、settle、requestPrice……）返回空。
pub fn decode_input(input: &[u8]) -> Vec<OracleCall> {
    if input.get(..4) == Some(&SEL_MULTICALL) {
        let args = &input[4..];
        let Some(off) = usize_at(args, 0) else {
            return Vec::new();
        };
        let Some(n) = usize_at(args, off) else {
            return Vec::new();
        };
        let base = off + 32;
        let mut out = Vec::new();
        for i in 0..n.min(256) {
            let Some(eo) = usize_at(args, base + 32 * i) else {
                break;
            };
            let Some(len) = usize_at(args, base + eo) else {
                break;
            };
            if let Some(call) = args
                .get(base + eo + 32..base + eo + 32 + len)
                .and_then(decode_one)
            {
                out.push(call);
            }
        }
        return out;
    }
    decode_one(input).into_iter().collect()
}

fn pad32(len: usize) -> usize {
    len.div_ceil(32) * 32
}

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

fn topic_addr(a: &[u8; 20]) -> String {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(a);
    hex0x(&w)
}

/// 合成与链上事件等价的日志。`from` 是交易发送方（没有显式 actor 时作为提案人 / 争议人）。
pub fn synth_log(
    call: &OracleCall,
    tx_hash: &str,
    log_index: usize,
    to: &str,
    from: &[u8; 20],
) -> RpcLog {
    let actor = call.actor.unwrap_or(*from);
    // ProposePrice data: identifier, timestamp, ancillary 偏移, proposedPrice, expiration, currency, ancillary
    // DisputePrice  data: identifier, timestamp, ancillary 偏移, proposedPrice, ancillary
    let head_words = if call.kind == CallKind::Propose { 6 } else { 4 };
    let mut data = Vec::with_capacity(head_words * 32 + 32 + pad32(call.ancillary.len()));
    data.extend_from_slice(&call.identifier);
    data.extend_from_slice(&call.timestamp);
    let mut off = [0u8; 32];
    off[24..].copy_from_slice(&((head_words * 32) as u64).to_be_bytes());
    data.extend_from_slice(&off);
    data.extend_from_slice(&call.price);
    if call.kind == CallKind::Propose {
        data.extend_from_slice(&[0u8; 64]); // expirationTimestamp、currency：pending 时未知，下游 wire 也不用
    }
    let mut len = [0u8; 32];
    len[24..].copy_from_slice(&(call.ancillary.len() as u64).to_be_bytes());
    data.extend_from_slice(&len);
    data.extend_from_slice(&call.ancillary);
    data.resize(
        data.len() + pad32(call.ancillary.len()) - call.ancillary.len(),
        0,
    );

    let mut topics = vec![
        if call.kind == CallKind::Propose {
            TOPIC_PROPOSE_PRICE
        } else {
            TOPIC_DISPUTE_PRICE
        }
        .to_owned(),
        topic_addr(&call.requester),
    ];
    if call.kind == CallKind::Propose {
        topics.push(topic_addr(&actor));
    } else {
        // DisputePrice(requester, proposer, disputer)：proposer 在 calldata 里拿不到，填 0。
        topics.push(topic_addr(&[0; 20]));
        topics.push(topic_addr(&actor));
    }
    RpcLog {
        address: to.to_owned(),
        topics,
        data: hex0x(&data),
        block_number: "0x0".to_owned(),
        block_hash: String::new(),
        transaction_hash: tx_hash.to_owned(),
        transaction_index: None,
        log_index: format!("0x{log_index:x}"),
        removed: false,
    }
}

#[cfg(test)]
mod tests {
    //! 用真实主网交易做回归（WORKFLOW 第 2 节的硬规定）：同一笔交易，从 calldata 合成的日志
    //! 与回执里真实日志解码出来的字段必须完全一致。
    use super::*;
    use crate::uma::events::decode_signal_log;
    use serde_json::Value;

    fn fixture(name: &str) -> Value {
        let raw = match name {
            "propose_direct" => include_str!("../testdata/mempool/propose_direct.json"),
            "propose_multicall" => include_str!("../testdata/mempool/propose_multicall.json"),
            "dispute_direct" => include_str!("../testdata/mempool/dispute_direct.json"),
            _ => unreachable!(),
        };
        serde_json::from_str(raw).unwrap()
    }

    fn check(name: &str, expect_calls: usize) {
        let fx = fixture(name);
        let input = hex::decode(fx["input"].as_str().unwrap().trim_start_matches("0x")).unwrap();
        let from: [u8; 20] = hex::decode(fx["from"].as_str().unwrap().trim_start_matches("0x"))
            .unwrap()
            .try_into()
            .unwrap();
        let calls = decode_input(&input);
        assert_eq!(calls.len(), expect_calls, "{name}: 解析出的调用数");
        let real_logs: Vec<RpcLog> = serde_json::from_value(fx["logs"].clone()).unwrap();
        assert_eq!(real_logs.len(), expect_calls, "{name}: 回执里的事件数");
        for (i, (call, real)) in calls.iter().zip(&real_logs).enumerate() {
            let synth = synth_log(
                call,
                fx["hash"].as_str().unwrap(),
                i,
                fx["to"].as_str().unwrap(),
                &from,
            );
            let a = decode_signal_log(&synth, 1, &[], true).expect("合成日志可解码");
            let b = decode_signal_log(real, 1, &[], true).expect("真实日志可解码");
            assert_eq!(a.kind(), b.kind(), "{name}#{i} 事件类型");
            assert_eq!(
                a.request(),
                b.request(),
                "{name}#{i} 请求字段（requester/condition_id/ancillary/报价）"
            );
            assert_eq!(a.market_id(), b.market_id(), "{name}#{i} market_id");
            assert_eq!(
                a.chain().transaction_hash,
                b.chain().transaction_hash,
                "{name}#{i} 交易哈希"
            );
        }
    }

    /// 0x51f9e6c4…：0xa0b6210f 直接调 proposePrice，报 NO，区块 94694363。
    #[test]
    fn direct_propose_matches_onchain_log() {
        check("propose_direct", 1);
    }

    /// 0x51899e19…：multicall 里批量提交 2 个 proposePrice。
    #[test]
    fn multicall_proposes_match_onchain_logs() {
        check("propose_multicall", 2);
    }

    /// 0xc073c2ef…：0x3ca4d8f5 直接调 disputePrice，争议 0xa0b6210f 的 NO 提案，区块 94695288。
    /// dispute 的报价在 calldata 里拿不到（合成为 0），所以只比较请求键和 market_id。
    #[test]
    fn direct_dispute_matches_onchain_log_request() {
        let fx = fixture("dispute_direct");
        let input = hex::decode(fx["input"].as_str().unwrap().trim_start_matches("0x")).unwrap();
        let from: [u8; 20] = hex::decode(fx["from"].as_str().unwrap().trim_start_matches("0x"))
            .unwrap()
            .try_into()
            .unwrap();
        let calls = decode_input(&input);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].kind, CallKind::Dispute);
        let real: Vec<RpcLog> = serde_json::from_value(fx["logs"].clone()).unwrap();
        let synth = synth_log(
            &calls[0],
            fx["hash"].as_str().unwrap(),
            0,
            fx["to"].as_str().unwrap(),
            &from,
        );
        let a = decode_signal_log(&synth, 1, &[], true).unwrap();
        let b = decode_signal_log(&real[0], 1, &[], true).unwrap();
        assert_eq!(a.kind(), b.kind());
        assert_eq!(a.request().condition_id, b.request().condition_id);
        assert_eq!(a.request().requester, b.request().requester);
        assert_eq!(a.market_id(), b.market_id());
    }

    #[test]
    fn unknown_selector_is_ignored() {
        assert!(decode_input(&[0xde, 0xad, 0xbe, 0xef, 0, 0]).is_empty());
        assert!(decode_input(&[]).is_empty());
    }
}
