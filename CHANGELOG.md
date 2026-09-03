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

## 2026-09-03 20260903-6fc36f8
- `UmaEvent` 新增 `category`/`bet_type` 两个字段（一级分类：体育/电竞/政治/
  加密/文化/天气/其他；二级下注类型：胜负/让分/大小分/单局胜负/夺冠/最高
  温/最低温/降水/风暴等），独立映射规则文件 `config/category_rules.json` +
  `src/category.rs` 匹配逻辑，规则和回归测试都用实测抓到的真实 Gamma/链上
  数据。
- `BetType` 数值用 `CCCBBB` 编码：千位分组号 x1000 + 组内三位序号（体育/电
  竞共用 1xxx 段，天气用 2xxx 段），分组号跟 `Category` 自己的枚举值无关（
  Sports/Esports 是两个不同 Category 但共用同一个下注类型分组），每段各自
  有自己的兜底值（`SPORTS_PROP`/`WEATHER_OTHER`），不共享跨分组兜底——保证
  "某分组下的 BetType 都落在它自己的千位区间"这条不变式严格成立，以后加新
  下注类型/新可下注类目都只是加一个新分组号，不需要动已发布的值。
- `catalog.bin` 格式因为新增字段升版（`UMACAT1`->`UMACAT2`），`bet_type` 按
  4 字节存（`CCCBBB` 编码后数值超出 1 字节范围）；识别旧格式时自愈成空
  catalog 触发全量重新同步，不会导致升级后启动失败。
- 面板加"分类"/"下注类型"两列；Condition ID 跳转链接的查询键从 `market_id`
  改成 `condition_id`（更权威）。

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
