//! 推送前过滤：按请求去重 + 提交地址打分 + 小费门槛。全部在内存里查表，不发网络请求。
//!
//! 去重键 = (requester, keccak(ancillary), 请求时间戳)，对 Polymarket 适配合约即
//! (condition_id, 请求时间戳)。每个请求的状态：
//!   未见 → 已推送（第一条 pending 推出去之后） → 已上链（链上出现成功的 ProposePrice）
//!                                              → 已争议（链上出现 DisputePrice）
//! 已推送之后同一请求的 pending 一律不推（报价相同 = 重复抢跑；不同 = 冲突，计数）；
//! 已上链 / 已争议之后的 pending 是"迟到"，不推。同一市场出现更新一轮的请求时间戳后，
//! 旧轮次的 pending 也不推。
//!
//! 打分：每个提交地址的 pending 报价与链上最终提案一致 / 不一致的次数（Beta 后验，正态近似
//! 取 95% 下限），样本数和下限都达标才算可信。小费门槛：近 1 小时里进块的 propose 交易的
//! 小费分位数，pending 的小费低于它就不推（"故意挂着不上链"的主要手法）。

use std::{
    collections::{HashMap, VecDeque},
    path::Path,
};

use serde::{Deserialize, Serialize};

use super::calldata::{CallKind, OracleCall, RequestKey};

#[derive(Clone, Debug)]
pub struct GateConfig {
    pub trust_min_n: u32,
    pub trust_min_rate: f64,
    pub tip_quantile: f64,
    pub tip_window_us: u64,
    pub tip_min_samples: usize,
    pub state_ttl_us: u64,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            trust_min_n: 50,
            trust_min_rate: 0.98,
            tip_quantile: 0.05,
            tip_window_us: 3_600_000_000,
            tip_min_samples: 20,
            state_ttl_us: 7 * 86_400_000_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
pub enum Drop {
    AlreadyProposed,
    AlreadyDisputed,
    Duplicate,
    Conflict,
    StaleRound,
    Untrusted,
    LowTip,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Verdict {
    Emit,
    Drop(Drop),
}

#[derive(Default)]
struct ReqState {
    emitted_price: Option<[u8; 32]>,
    emitted_dispute: bool,
    proposed: Option<[u8; 32]>,
    disputed: bool,
    pendings: Vec<([u8; 20], [u8; 32])>,
    touched_us: u64,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
pub struct SenderStat {
    pub agree: u32,
    pub disagree: u32,
}

pub struct Gate {
    cfg: GateConfig,
    reqs: HashMap<RequestKey, ReqState>,
    latest_round: HashMap<([u8; 20], [u8; 32]), u64>,
    senders: HashMap<[u8; 20], SenderStat>,
    tips: VecDeque<(u64, u64)>,
    tx_tip: HashMap<String, (u64, u64)>,
    pub emitted: u64,
    pub dropped: HashMap<Drop, u64>,
}

impl Gate {
    pub fn new(cfg: GateConfig) -> Self {
        Self {
            cfg,
            reqs: HashMap::new(),
            latest_round: HashMap::new(),
            senders: HashMap::new(),
            tips: VecDeque::new(),
            tx_tip: HashMap::new(),
            emitted: 0,
            dropped: HashMap::new(),
        }
    }

    pub fn trusted(&self, sender: &[u8; 20]) -> bool {
        let s = self.senders.get(sender).copied().unwrap_or_default();
        let n = s.agree + s.disagree;
        if n < self.cfg.trust_min_n {
            return false;
        }
        let p = (f64::from(s.agree) + 1.0) / (f64::from(n) + 2.0);
        let lo = p - 1.645 * (p * (1.0 - p) / (f64::from(n) + 3.0)).sqrt();
        lo >= self.cfg.trust_min_rate
    }

    pub fn tip_floor(&mut self, now_us: u64) -> Option<u64> {
        while self
            .tips
            .front()
            .is_some_and(|(t, _)| now_us.saturating_sub(*t) > self.cfg.tip_window_us)
        {
            self.tips.pop_front();
        }
        if self.tips.len() < self.cfg.tip_min_samples {
            return None;
        }
        let mut v: Vec<u64> = self.tips.iter().map(|(_, g)| *g).collect();
        v.sort_unstable();
        Some(v[((v.len() - 1) as f64 * self.cfg.tip_quantile).round() as usize])
    }

    fn drop(&mut self, why: Drop) -> Verdict {
        *self.dropped.entry(why).or_default() += 1;
        Verdict::Drop(why)
    }

    /// 一条 pending 调用要不要推。`tip_gwei` 是交易的 maxPriorityFee（gwei）。
    pub fn check(
        &mut self,
        call: &OracleCall,
        sender: [u8; 20],
        tip_gwei: u64,
        tx_hash: &str,
        now_us: u64,
    ) -> Verdict {
        self.tx_tip.insert(tx_hash.to_owned(), (now_us, tip_gwei));
        let key = call.request_key();
        let round = self
            .latest_round
            .entry(key.market())
            .or_insert(key.timestamp);
        if key.timestamp < *round {
            return self.drop(Drop::StaleRound);
        }
        *round = key.timestamp;
        let floor = self.tip_floor(now_us);
        let trusted = self.trusted(&sender);
        let st = self.reqs.entry(key).or_default();
        st.touched_us = now_us;
        match call.kind {
            CallKind::Propose => {
                if st.pendings.len() < 32 {
                    st.pendings.push((sender, call.price));
                }
                if st.disputed {
                    return self.drop(Drop::AlreadyDisputed);
                }
                if st.proposed.is_some() {
                    return self.drop(Drop::AlreadyProposed);
                }
                if let Some(p) = st.emitted_price {
                    return self.drop(if p == call.price {
                        Drop::Duplicate
                    } else {
                        Drop::Conflict
                    });
                }
                if !trusted {
                    return self.drop(Drop::Untrusted);
                }
                if floor.is_some_and(|f| tip_gwei < f) {
                    return self.drop(Drop::LowTip);
                }
                st.emitted_price = Some(call.price);
            }
            CallKind::Dispute => {
                if st.disputed {
                    return self.drop(Drop::AlreadyDisputed);
                }
                if st.emitted_dispute {
                    return self.drop(Drop::Duplicate);
                }
                // 争议人没有报价历史可打分，只用小费门槛挡"挂着不上链"的假争议。
                if floor.is_some_and(|f| tip_gwei < f) {
                    return self.drop(Drop::LowTip);
                }
                st.emitted_dispute = true;
            }
        }
        self.emitted += 1;
        Verdict::Emit
    }

    /// 链上确认的 ProposePrice / DisputePrice（来自日志订阅或启动回补）。
    pub fn on_confirmed(
        &mut self,
        kind: CallKind,
        key: RequestKey,
        price: [u8; 32],
        tx_hash: &str,
        now_us: u64,
    ) {
        let round = self
            .latest_round
            .entry(key.market())
            .or_insert(key.timestamp);
        *round = (*round).max(key.timestamp);
        if kind == CallKind::Propose
            && let Some((_, tip)) = self.tx_tip.get(tx_hash).copied()
        {
            self.tips.push_back((now_us, tip));
        }
        let st = self.reqs.entry(key).or_default();
        st.touched_us = now_us;
        match kind {
            CallKind::Propose if st.proposed.is_none() => {
                st.proposed = Some(price);
                let pend = std::mem::take(&mut st.pendings);
                for (sender, p) in pend {
                    let s = self.senders.entry(sender).or_default();
                    if p == price {
                        s.agree += 1;
                    } else {
                        s.disagree += 1;
                    }
                }
            }
            CallKind::Propose => {}
            CallKind::Dispute => st.disputed = true,
        }
    }

    pub fn prune(&mut self, now_us: u64) {
        let ttl = self.cfg.state_ttl_us;
        self.reqs
            .retain(|_, st| now_us.saturating_sub(st.touched_us) < ttl);
        let w = self.cfg.tip_window_us;
        self.tx_tip
            .retain(|_, (t, _)| now_us.saturating_sub(*t) < w);
    }

    pub fn trusted_count(&self) -> usize {
        self.senders.keys().filter(|s| self.trusted(s)).count()
    }

    pub fn requests(&self) -> usize {
        self.reqs.len()
    }

    /// 提交地址统计落盘 / 读回（`{"0x地址": [一致, 不一致]}`，与 p2p-race 的先验文件同格式）。
    pub fn save_senders(&self, path: &Path) -> std::io::Result<()> {
        let map: HashMap<String, [u32; 2]> = self
            .senders
            .iter()
            .map(|(k, v)| (format!("0x{}", hex::encode(k)), [v.agree, v.disagree]))
            .collect();
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(&map)?)?;
        std::fs::rename(tmp, path)
    }

    pub fn load_senders(&mut self, path: &Path) -> std::io::Result<usize> {
        let raw = std::fs::read(path)?;
        let map: HashMap<String, [u32; 2]> = serde_json::from_slice(&raw)?;
        for (k, [a, d]) in &map {
            if let Ok(b) = hex::decode(k.trim_start_matches("0x"))
                && let Ok(addr) = <[u8; 20]>::try_from(b.as_slice())
            {
                let s = self.senders.entry(addr).or_default();
                s.agree = s.agree.max(*a);
                s.disagree = s.disagree.max(*d);
            }
        }
        Ok(map.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(kind: CallKind, ts: u64, price: u8) -> OracleCall {
        let mut t = [0u8; 32];
        t[24..].copy_from_slice(&ts.to_be_bytes());
        let mut p = [0u8; 32];
        p[31] = price;
        OracleCall {
            kind,
            requester: [1; 20],
            identifier: [2; 32],
            timestamp: t,
            ancillary: b"q: test".to_vec(),
            price: p,
            actor: None,
        }
    }

    fn trusted_gate() -> Gate {
        let mut g = Gate::new(GateConfig {
            trust_min_n: 1,
            trust_min_rate: 0.5,
            ..GateConfig::default()
        });
        g.senders.insert(
            [9; 20],
            SenderStat {
                agree: 100,
                disagree: 0,
            },
        );
        g
    }

    #[test]
    fn first_pending_emits_then_duplicates_and_conflicts_drop() {
        let mut g = trusted_gate();
        assert_eq!(
            g.check(&call(CallKind::Propose, 10, 1), [9; 20], 100, "0xa", 1),
            Verdict::Emit
        );
        assert_eq!(
            g.check(&call(CallKind::Propose, 10, 1), [9; 20], 100, "0xb", 2),
            Verdict::Drop(Drop::Duplicate)
        );
        assert_eq!(
            g.check(&call(CallKind::Propose, 10, 0), [9; 20], 100, "0xc", 3),
            Verdict::Drop(Drop::Conflict)
        );
    }

    #[test]
    fn pending_after_onchain_proposal_is_late() {
        let mut g = trusted_gate();
        let c = call(CallKind::Propose, 10, 1);
        g.on_confirmed(CallKind::Propose, c.request_key(), c.price, "0xw", 1);
        assert_eq!(
            g.check(&c, [9; 20], 100, "0xa", 2),
            Verdict::Drop(Drop::AlreadyProposed)
        );
    }

    #[test]
    fn new_round_after_dispute_emits_and_old_round_is_stale() {
        let mut g = trusted_gate();
        let old = call(CallKind::Propose, 10, 0);
        g.on_confirmed(CallKind::Propose, old.request_key(), old.price, "0xw", 1);
        g.on_confirmed(CallKind::Dispute, old.request_key(), [0; 32], "0xd", 2);
        // 适配合约用同一个问题、新时间戳重新请求：新轮次照常推，旧轮次一律不推
        assert_eq!(
            g.check(&call(CallKind::Propose, 20, 1), [9; 20], 100, "0xa", 3),
            Verdict::Emit
        );
        assert_eq!(
            g.check(&call(CallKind::Propose, 10, 1), [9; 20], 100, "0xb", 4),
            Verdict::Drop(Drop::StaleRound)
        );
    }

    #[test]
    fn untrusted_and_low_tip_are_dropped() {
        let mut g = Gate::new(GateConfig {
            tip_min_samples: 3,
            ..GateConfig::default()
        });
        assert_eq!(
            g.check(&call(CallKind::Propose, 10, 1), [7; 20], 100, "0xa", 1),
            Verdict::Drop(Drop::Untrusted)
        );
        let mut g = trusted_gate();
        g.cfg.tip_min_samples = 3;
        for (i, tip) in [100u64, 110, 120].iter().enumerate() {
            let tx = format!("0x{i}");
            let c = call(CallKind::Propose, 100 + i as u64, 1);
            g.check(&c, [9; 20], *tip, &tx, 10);
            g.on_confirmed(CallKind::Propose, c.request_key(), c.price, &tx, 11);
        }
        assert_eq!(
            g.check(&call(CallKind::Propose, 500, 1), [9; 20], 5, "0xz", 12),
            Verdict::Drop(Drop::LowTip)
        );
    }

    #[test]
    fn confirmations_score_senders() {
        let mut g = Gate::new(GateConfig {
            trust_min_n: 2,
            trust_min_rate: 0.0,
            ..GateConfig::default()
        });
        for ts in [1u64, 2] {
            let c = call(CallKind::Propose, ts, 1);
            g.check(&c, [5; 20], 100, "0xa", 1);
            g.on_confirmed(CallKind::Propose, c.request_key(), c.price, "0xb", 2);
        }
        assert!(g.trusted(&[5; 20]));
    }
}
