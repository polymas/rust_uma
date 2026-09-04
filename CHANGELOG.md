# Changelog

记录 `rust_uma` 有实际影响的变更（协议/存储格式改动、热路径正确性修复、生产
故障修复……即 [docs/WORKFLOW.md](docs/WORKFLOW.md) 3.1 节定义的"重要变更"）。
日常小修小补不必进这里，git log 已经够用。

条目在打 tag 推送的同一个 commit 里一起加，格式（`vX.Y.Z` 怎么定见
[docs/WORKFLOW.md](docs/WORKFLOW.md) 3.1 节的升级规则）：

```
## vX.Y.Z（YYYY-MM-DD，<commit短哈希>）
- 一句话说清楚改了什么、为什么（对下游/生产的影响）
```

最新的写在最上面。`v0.2.0` 之前的条目是旧的 `YYYYMMDD-<commit短哈希>` tag
格式（当时还没上语义化版本号），历史记录不倒改，新条目一律用上面这个格式。

<!-- 新条目加在这行下面 -->

## v0.5.0（2026-09-04，655c555）
- `UmaEvent` 新增 `neg_risk` 字段（纯加法），原样透传 Gamma 市场记录自己的
  `negRisk` 布尔值——同一个已经在拉的 `/markets/keyset` 响应体里本来就有这
  个字段，零新增请求。`catalog.bin` 魔数随之升到 `UMACAT3`。
- 顺带修了排查这次改动时发现的一个真实 bug：`catalog.bin` 旧格式自愈套路
  （认到旧魔数就丢弃、触发全量重同步）只丢了 catalog，没清掉
  `enrichment.cursor`/`enrichment_closed.cursor` 两个增量游标——游标是重启
  前几秒才落盘的、几乎最新，导致"自愈"后的全量重同步实际几乎不拉数据，内
  存 catalog 静默留空，`/healthz` 照样是 `ok`，真正补救要等
  `run_catalog_reconcile` 的全量重扫（默认 6 小时一次）。也就是说任何一次
  存储格式升级部署后，最长可能有 6 小时富化系统性 miss（不广播）且没有报
  错信号。现在 `discard_stale_catalog` 丢弃旧格式 catalog 时把两个游标一并
  删掉，让下一次同步是真正的全量冷启动。

## v0.4.0（2026-09-03，428273b）
- `BetType` 新增三组二级下注类型：`3xxx` Crypto（触价/定档/涨跌/兜底）、
  `4xxx` Politics（选举获胜/美联储利率决议/推文数量分档/兜底）、`5xxx`
  Culture（夺魁登顶/票房播放量分档/兜底）——规则和测试都来自实测抓取的真实
  Gamma 数据（Hit Price/Multi Strikes/Up or Down/Elections/Fed Rates/Tweet
  Markets 等 tag 都是 Polymarket 自己打的）。`Category` 现在把
  Sports/Esports/Weather/Crypto/Politics/Culture 六个分类都算"可下注"。
- 顺带修了一个真实 bug：体育组的 "Will X win the Y?" 文本规则以前是全分类
  共享的，会误判到措辞相似的政治选举市场上；现在按分类分组匹配，每组只用
  自己的规则，不会跨组误判。
- 面板"类型"/"富化"两列翻成中文（之前漏翻了）；`internal/api/llms.txt` 补
  上 `category`/`bet_type` 的完整下游文档。

## v0.3.0（2026-09-03，e150823）
- 富化 miss 的事件不再经 WSS 广播——没有 `token_ids` 下游拿到也没法下单，
  广播出去只是噪音；miss 事件仍然进 `EventHub` 去重环、写本地 `events.wal`、
  计入 `enrichment_misses_total`，并打一条 `WARN` 日志（原文案改成
  "not broadcasting, logged locally only"，跟实际行为对齐）。触发原因：观
  察到几条最近的 miss，是这次服务重启期间 Gamma 目录还没同步完那个窗口触
  发的，属于预期内的短暂现象，之前会被当成"广播了但没有 token_ids"的噪音
  事件推给下游。
- `pipeline.rs` 补了两条测试（之前完全没有测试）：miss 不进 `batch_tx` 但仍
  写 storage，hit 正常广播。
- `docs/WORKFLOW.md`/`internal/api/llms.txt`/`docs/ARCHITECTURE.md` 三处同步
  更新，之前都写着"miss 也会广播"。

## v0.2.0（2026-09-03）
- Tag 命名从 `YYYYMMDD-<commit短哈希>` 换成语义化版本号 `vX.Y.Z`，`Cargo.toml`
  的 `version` 字段跟 tag 保持一致；升级规则（PATCH/MINOR/MAJOR 怎么判、
  `0.y.z` 阶段的破坏性变更例外）写进了
  [docs/WORKFLOW.md](docs/WORKFLOW.md) 3.1 节。这个版本号本身对应的就是
  下面 `20260903-6fc36f8` 那次改动（`Category`/`BetType` 分类），只是补上语
  义化版本号作为新方案的起点，没有新增代码改动。

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
