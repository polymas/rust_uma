# Changelog

记录 `rust_uma` 有实际影响的变更（协议/存储格式改动、热路径正确性修复、生产
故障修复……即 [docs/WORKFLOW.md](docs/WORKFLOW.md) 3.1 节定义的"重要变更"）。
日常小修小补不必进这里，git log 已经够用。

条目在打 tag 推送的同一个 commit 里一起加，格式：

```
## YYYY-MM-DD <tag>
- 一句话说清楚改了什么、为什么（对下游/生产的影响）
```

最新的写在最上面。

<!-- 新条目加在这行下面 -->

## 2026-09-02 20260902-a2e74b5
- 面板 tag_ids 显示人类可读标签：改到纯客户端（浏览器直接查 Polymarket 公开
  Gamma API，服务端不参与），批量 `/tags` 接口实测不可靠后改成全量分页预加载
  + 按需单查 `/tags/{id}` 兜底。
- 面板 Condition ID 列加了跳转链接（懒查 `market_id` -> slug，指到对应
  Polymarket 市场页），端到端延迟统一只用 ms/s 两档。
- 富化命中率（`enrichment_hits`/`misses`）持久化到 `.cache/enrichment_stats.json`，
  跨重启续存不再清零；新增近 1000 例滚动命中率（进程内存，重启清零重新累积，
  跟总量刻意分开展示）。
- "收到于"列固定绝对时间戳，去掉相对/绝对切换按钮。
- `deploy.sh` 健康检查轮询加每次尝试的进度提示。
