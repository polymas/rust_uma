# rust_uma

极简、高性能的 Polygon UMA 事件订阅、解析与查询服务。

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

服务已包含 Polygon JSON-RPC 订阅与断线补偿、UMA ABI 解码、Gamma
富化目录、内存事件/帧环、Protobuf + Zstd WSS 以及极简 HTTP 查询接口。

## 快速启动

需要 Rust 1.88 或更高版本，以及同一 Polygon 服务商的 WSS 地址；HTTP RPC
未配置时会从 WSS 地址自动推导。

```bash
cp .env.example .env
# 编辑 .env，至少填写 POLYGON_WSS_URL
set -a
source .env
set +a
cargo run --release
```

默认监听 `127.0.0.1:8011`。首次启动默认先建立实时订阅，再从当前区块开始；
如需历史补拉，设置 `START_BLOCK`。重启会从本地 checkpoint 补齐断线区间。

## 查询与订阅

```text
GET /healthz
GET /metrics
GET /llms.txt
GET /uma/v1/events?after_sequence=0&limit=100&event_type=propose
GET /uma/v1/events/:transaction_hash/:log_index
GET /uma/v1/markets/:market_id
GET /uma/v1/ws?after_sequence=0
```

WSS 客户端必须声明子协议 `uma.pb.v1`。消息头、压缩阈值、重放语义和
下游约束见 `internal/api/llms.txt`，Protobuf schema 见 `proto/uma.proto`。
