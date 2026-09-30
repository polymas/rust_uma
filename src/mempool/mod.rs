//! mempool-uma：事件来源换成内存池 pending 交易的 rust-uma。
//!
//! 解码之后的整条链路（富化、分类、EventHub、批量编码、WSS）完全复用 rust-uma；这里只有
//! calldata → 合成日志、推送前过滤、以及更新过滤状态用的链上确认订阅三部分。
//! 设计与决定见知识库《mempool-uma服务方案-2026-10-01》。

pub mod calldata;
pub mod feed;
pub mod gate;
