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

当前仓库为项目骨架；服务实现将在后续协议与功能边界确认后加入。
