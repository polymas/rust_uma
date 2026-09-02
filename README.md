# rust_uma

极简、高性能的 Polygon UMA 事件订阅、解析与查询服务。

## MVP 范围

- 对外实时数据仅提供 Protobuf WSS（`uma.pb.v1`）
- 只解析 `ProposePrice`、`DisputePrice`
- RPC 订阅和历史补拉只按事件 topic 过滤，不按地址；解析后的 emitter 白名单
  （`UMA_CONTRACT_ADDRESSES`）默认留空即接受任意发送方——Oracle 合约地址可能升
  级/迁移，锁死地址列表有静默漏事件的风险，代价是可能混入少量非 Polymarket
  的同 ABI 事件（会在富化阶段自然表现为 miss，不会被当成正确数据广播）
- 两类事件使用独立业务解析器，并通过公共链上元数据和价格请求结构组合复用
- `ancillaryData` 只解析出 question ID（用于标准 Adapter 的链上 condition_id
  推导兜底）和可选 market ID；question 原文、resolution p1-p4、initializer
  这些没人再读的字段，解码阶段直接不解析
- 启动时先按富化 cursor 完成 Gamma 增量缓存，再按 UMA 区块 cursor 补拉
- Gamma 增量缓存包含全部活跃（`closed=false`）市场，并额外滚动缓存最近
  `CLOSED_MARKET_LOOKBACK_DAYS`（默认 3）天内刚关闭的市场——
  `ProposePrice`/`DisputePrice` 绝大多数发生在市场刚关闭那一刻，只缓存活跃
  市场会系统性 miss 掉大部分真实事件；更早关闭的市场不再产生新事件，不纳入
  缓存
- UMA 事件解析阶段同时得到 `market_id` 和链上推导的 `condition_id`；富化优先
  按 `market_id` 查询（对任意 Adapter 类型都成立），仅 `market_id` 缺失时才
  回退到链上推导值
- 只保存 `market_id / condition_id / token_ids / tag_ids`
- `tag_ids` 合并 market 与 event tags，使用数字 ID 排序去重
- 不包含 `outcomes`、价格数组和动态 `tick_size`
- Protobuf 批量 WSS，按阈值使用 Zstd level 1
- 使用全局预编码帧环，慢客户端不阻塞采集
- 提供 HTTP 查询、健康检查和 Prometheus 指标
- 使用本地紧凑目录快照和事件 WAL

## 已确定的数据边界

市场富化只保留：

- `market_id`
- `condition_id`
- `token_ids`
- `tag_ids`

`tag_ids` 是 market tags 与所属 event tags 的并集，按官方数字 Tag ID 去重。Tag 的 label/slug 由独立字典解析，不在每条市场记录中重复保存。

服务不保存 `outcomes`、`outcome_prices` 或 `tick_size`。动态交易数据不属于 UMA 静态富化。

## WSS 协议方向

- WebSocket BinaryMessage
- Protobuf 批量格式：`proto/uma.proto`
- 小于 4 KiB 的 Protobuf payload 不压缩
- 大于等于 4 KiB 时使用 Zstd level 1
- 单批上限暂定 64 个事件或 32 KiB 未压缩数据
- 同一批数据只编码、压缩一次，供所有订阅客户端复用
- 不启用 `permessage-deflate`

服务内部通过 Polygon JSON-RPC 订阅与断线补偿采集数据；对外事件流仅提供
Protobuf + Zstd WSS，同时保留极简 HTTP 查询、健康检查和指标接口。

UMA 业务解析位于 `src/uma/events/`：`propose_price.rs` 与
`dispute_price.rs` 分别完整解析事件 ABI，`ancillary.rs` 负责 Polymarket 附加数据，
`common.rs` 定义两类事件复用的组合结构。解析层不负责富化、持久化或广播。

## 快速启动

需要 Rust 1.88 或更高版本，以及同一 Polygon 服务商的 WSS 地址；HTTP RPC
未配置时会从 WSS 地址自动推导。

```bash
cp .env.example .env
# 编辑 .env，填写 WSS_RPC 和 HTTP_RPC
set -a
source .env
set +a
cargo run --release
```

默认监听 `127.0.0.1:8011`，缓存目录为 `.cache/`。其中
`enrichment.cursor` 保存活跃市场（`closed=false`）的 Gamma
`(updatedAt, market_id)` 增量水位，`enrichment_closed.cursor` 独立保存最近关
闭市场（`closed=true`）的同类水位，`uma.cursor` 保存已完成的 Polygon 区块高
度。每次启动必须先完成两路富化增量同步，之后才建立实时订阅并补拉 UMA 数据。
首次启动默认从链头时间向前精确定位 7 天的起始区块；可通过
`INITIAL_BACKFILL_DAYS` 调整，或用 `START_BLOCK` 显式覆盖。

## 查询与订阅

```text
GET /healthz
GET /metrics
GET /llms.txt
GET /dashboard?token=<DASHBOARD_TOKEN>
GET /uma/v1/dashboard-data?token=<DASHBOARD_TOKEN>
GET /uma/v1/ws?after_sequence=0
```

业务事件只通过 `/uma/v1/ws` 分发（Protobuf，可选 Zstd），没有平行的 JSON
查询接口——所有消费者，包括调试和回放脚本，统一解 Protobuf。`/dashboard`
是一个 token 鉴权的只读运维面板，只展示聚合统计，不提供单条事件查询。

WSS 客户端必须声明子协议 `uma.pb.v1`。消息头、压缩阈值、重放语义和
下游约束见 `internal/api/llms.txt`，Protobuf schema 见 `proto/uma.proto`。
