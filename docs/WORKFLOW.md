# rust_uma 项目工作流规范

数据流、启动顺序、部署拓扑的图见 [ARCHITECTURE.md](ARCHITECTURE.md)。

本文档只对 `rust_uma` 这一个项目生效，覆盖开发、测试、提交、部署、升级五个环节。
目标始终是：**从链上事件到下游 WSS 广播的延迟最小化，且不能用速度换正确性**（见
"热路径铁律"）。任何改动先问一句：这会不会在事件处理的关键路径上多等一次网络
往返？

## 0. 项目现状速查

- 生产环境：`43.131.1.194`（阿里云/腾讯云主机，`ubuntu` 用户，免密 sudo），
  systemd 管理，服务名 `rust-uma.service`。
- 部署方式：**Mac 本机交叉编译**（`x86_64-unknown-linux-musl`，静态二进制，
  不依赖服务器上的 glibc 版本，也不需要在服务器装 Rust 工具链），产物用 `scp`
  推送，不使用 CI、不在服务器上编译。
- 对外端口：`8011`（HTTP 查询 + `/uma/v1/ws` Protobuf WSS 广播），已对公网开放
  （云安全组放行 `TCP:8000-8100`），服务本身没有认证 —— 这是当前的既定状态，
  不是遗漏；如果以后要收紧访问，需要显式决定（IP 白名单 / API Key / 换回
  127.0.0.1 + 反向代理），不要自作主张改回。
- 数据目录：服务器上是 `/var/lib/rust-uma`（`enrichment.cursor` /
  `enrichment_closed.cursor` / `uma.cursor` / `catalog.bin` / `events.wal`），
  本地开发默认是 `./.cache`。
- `UMA_CONTRACT_ADDRESSES` 默认留空：解码阶段不按 emitter 地址过滤，只按
  `ProposePrice`/`DisputePrice` topic 收——这是明确的既定决策（Oracle 合约地
  址可能升级/迁移，锁死地址列表有静默漏真实事件的风险），代价是可能混入少量
  非 Polymarket 的同 ABI 事件，这些会在 market_id/富化阶段自然表现为 miss，
  不会被当成正确数据广播出去。要收紧回严格校验，显式设置该变量即可（非空列
  表才启用白名单）。

## 1. 开发

### 1.1 热路径铁律（改代码前先对照这条）

这是本项目唯一的核心业务目标：**下游要靠这条链路比别人更快地知道 UMA 的
propose/dispute 结果，才能吃到尾盘 share**。所有工程决策服从这一条：

- 事件解析（`uma/events/`）到广播（`hub.rs`/`wire.rs`）之间，**禁止**引入任何
  同步的网络请求（Gamma HTTP、额外 `eth_call` 等）。富化数据必须来自已经预热
  好的本地内存（`Catalog`），miss 就是 miss，不要在热路径里补救。
- 批处理（`pipeline.rs::run_batcher`）永远不能加"攒够 N 条或等 N 毫秒"的定时
  窗口逻辑——现在的实现是"先阻塞等第一条，之后非阻塞尽量捎带"，单条事件不会被
  人为延迟，改动时保持这个语义。
- 新增 RPC 调用前，先确认它是否可以做成"启动时/后台预热到内存缓存"，而不是
  "事件来了再查"。上一次 Neg Risk 富化选型就是因为坚持了这一条，最后选了零
  额外请求的 `market_id → Gamma` 方案，而不是每条事件都发 `eth_call`。
- 允许为了正确性牺牲一点点延迟（比如 market_id 优先查、miss 才退化到链上推
  导），但绝不允许为了"看起来更完整"而加同步等待。

### 1.2 本地跑起来

```bash
cp .env.example .env   # 首次；填入真实 WSS_RPC/HTTP_RPC，不要把 .env 提交上去
cargo run
```

- `.env` 永远不进 git（`.gitignore` 已排除），也不要在任何输出、commit
  message、日志截图里贴出其中的 RPC 地址/token。
- 本地验证 NegRisk/ancillary 解析这类"真实链上格式"相关的改动时，优先用
  `eth_getLogs` 抓一条真实交易做 fixture（历史上已经因为只用手写的合成样例，
  漏掉了几个真实数据才会触发的 bug，见 1.4）。

### 1.3 目录约定

按业务域组织，不要退回到按技术层平铺：

```
src/
├── uma/
│   ├── rpc.rs            # Polygon WSS 订阅（支持多路赛马）+ HTTP 补拉
│   └── events/            # ProposePrice / DisputePrice 两个独立解析器
│       ├── common.rs       # ChainLog / PriceRequest 等组合结构
│       ├── ancillary.rs    # ancillary_data 强类型解析
│       ├── propose_price.rs
│       └── dispute_price.rs
├── enrichment.rs   # Gamma 预热缓存（Catalog），market_id 优先解析
├── pipeline.rs     # 去重、富化、批处理
├── hub.rs          # 事件环 + 预编码帧环
├── wire.rs         # Protobuf + Zstd 编码
├── storage.rs      # 双 cursor + 紧凑快照 + WAL
└── api.rs          # HTTP 查询 / 健康检查 / WSS 广播
```

新业务字段优先看它属于哪个域，不要在 `pipeline.rs` 里堆解析逻辑，也不要在
`uma/events/` 里塞富化/存储逻辑。

### 1.4 多路 RPC 与已知的四个"真实数据 bug"（避免重蹈覆辙）

- `uma/rpc.rs` 支持 `WSS_RPC_LIST`（逗号分隔多个 WSS 端点赛马，去重靠
  `EventHub` 的 `(tx_hash, log_index)`），只有一个地址时自动退化为单路。生产
  机上目前只配了一路，后续要加节点直接编辑服务器 `/etc/rust-uma/rust-uma.env`
  的 `WSS_RPC_LIST` 并重启服务，不用重新部署二进制。
- **`condition_id` 不能对所有 Adapter 无条件用二元公式推导**：Neg Risk 市场
  的 `keccak256(ancillary_data)` 只是一个 `requestId`，不是公式里的
  `questionId`，算出来的 `condition_id` 会和 Gamma 权威值不一致。现在的做法
  是 `Catalog::resolve()` 优先按 `market_id` 查（对二元和 Neg Risk 都成立，
  零额外请求），market_id 缺失才退化到链上推导。以后碰到新的 Adapter 类型，
  先假设"链上公式可能不通用"，别直接照搬二元公式。
- **`market_id` 的 ancillary 文本格式不统一**：有的市场是
  `market_id: 42, initializer:...`（逗号收尾），真实 Neg Risk 市场观察到的是
  `market_id: 907474 res_data: ...`（空格收尾，没有紧跟的逗号）。
  `ancillary.rs::field_number` 已经改成按连续数字截断而不是按逗号截断，新增
  字段解析时默认"不能假设分隔符统一"，尤其是数字字段要用专门的数字截断逻辑，
  不要复用逗号分隔的 `field_value`。
- **富化缓存只拉活跃市场，实际漏了大部分真实事件**：`ProposePrice`/
  `DisputePrice` 绝大多数发生在市场刚"结束"、Gamma 把它标成 `closed: true`
  的那一刻，而 Gamma 增量同步之前只查 `closed=false`——生产环境实测过 84%
  的 miss 率，抽样全部命中"该市场此时 `closed: true`"。现在
  `enrichment.rs::sync_both` 额外滚动缓存最近 `CLOSED_MARKET_LOOKBACK_DAYS`
  （默认 3）天内关闭的市场，用独立的 `enrichment_closed.cursor` 水位、按
  `updatedAt` 早停，不拉全部历史关闭市场（那些不会再产生新事件，缓存了也没
  用）。以后新增任何"缓存注定要被查询的实体"，先想清楚触发查询的时刻这个实
  体处于什么状态，不要想当然认为"活跃"等于"会被查询"。
- **Gamma 的 keyset 翻页会静默漏项**：修完上一条后生产实测仍有约 40% miss，
  抽样发现全部集中在"同一秒内批量创建的 Neg Risk 兄弟市场"（`updatedAt` 精确
  到毫秒级几乎相同），高度符合"翻页边界落在并列排序键中间、部分记录被跳过"
  的经典 keyset 分页问题。更麻烦的是：一旦增量水位推进过了这个时间点，
  `sync_incremental` 的 `is_older` 判断会让这条市场永久不可达——快路径没有
  "回头补漏"的能力。修复是 `run_catalog_reconcile`：独立的后台任务（不是
  `run_catalog_sync` 循环里的一个分支，避免几十分钟的全量扫描拖慢 60s 级的
  增量刷新），每隔 `CATALOG_RECONCILE_INTERVAL_HOURS`（默认 6 小时）忽略游标
  做一次全量重扫，只增量合并（`catalog.upsert` 本身幂等），**绝不写回**增量
  cursor 文件——它只负责查漏补缺，不参与、不干扰快路径的正常推进。
  `catalog_reconcile_gaps_closed_total`（`/healthz`、`/metrics`）应该趋向于
  0；如果长期非零，说明 Gamma 分页丢失是持续性的，不是偶发。
- **任何 `tokio::time::interval` 的周期都不能来自未夹紧的用户配置**：验证这
  条自愈任务时，临时把 `CATALOG_RECONCILE_INTERVAL_HOURS` 设成 `0` 想让它立
  刻触发，结果 `tokio::time::interval(Duration::ZERO)` 直接 panic——而
  `Cargo.toml` 的 `[profile.release] panic = "abort"` 让任何一个 tokio 任务
  的 panic 直接终止整个进程，systemd `Restart=always` 又立刻拉起、再次读到
  同一个 0 配置、再次 panic，生产服务在 43.131.1.194 上进了几十秒的重启死循
  环。现在 `config.rs` 里所有喂给 `tokio::time::interval` 的时长都
  `.max(1)`；新增任何周期性任务前，先确认它的间隔配置不会因为一个整数 0 就
  让进程死循环崩溃，不要假设"运维不会填 0"。

## 2. 测试

提交前必须本地跑过，任何一步不过不允许提交：

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

`deploy/deploy.sh` 默认也会先跑这三步（`--skip-checks` 才会跳过，只允许用于
"这个会话里已经手动验证过"的紧急热修复）。

补充规则：

- 凡是涉及链上事件解析（`uma/events/`）、ancillary 格式、Adapter 类型识别的
  改动，光靠手写合成样例不够，必须用真实 `eth_getLogs` 抓到的原始日志做一条
  回归测试（参考 `uma/events/mod.rs` 里
  `neg_risk_event_binary_formula_yields_wrong_condition_id` 的写法：注明来源
  交易哈希，交叉核对 Gamma API 返回值）。这条规则是用真金白银的教训换来的——
  前两个是合成样例测试全绿、换成真实数据才暴露的；第三个（富化覆盖率）是合成
  测试根本无法覆盖的一类问题——数据形状是对的，但"该缓存哪些实体"这个业务判
  断错了，只有拿生产真实流量核对命中率才能发现，见 4.4 的部署后检查清单。
- 涉及富化/广播链路的改动，尽量补一条端到端测试（解码 → `Catalog::resolve`
  → `EventRecord::resolved_condition_id`/`to_proto`），不要只测中间某一层。
- 不为了追求覆盖率去测无关紧要的 getter/Display；重点覆盖"链上数据形状会不会
  和预期不一样"这一类风险。

## 3. 代码提交

- commit message 用中文，准确概括本次改动，禁止空泛的 "fix bug"、"update"。
- 仅提交本次任务相关文件；不主动纳入无关改动；不提交 `.env`、密钥、`AGENTS.md`、
  `codex.md`（均已在 `.gitignore`）。
- 提交前完成第 2 节的验证门禁；验证失败先修复本次改动引入的问题，无法安全修
  复就说明阻碍，不带着已知失败提交。
- 结尾固定加：

  ```
  Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
  ```

- 单次改动如果同时包含"结构性重构"和"行为修复"，尽量拆成独立 commit（参考本
  仓库把"多路 WSS 赛马"和"NegRisk 富化修复"分开提交的做法），方便以后
  `git bisect`。

### 3.1 重要变更必须打 tag 并推送远程

本地 commit 完不算完——下面这几类"重要变更"，验证门禁过了之后必须
`git push` 到 `origin/main`，并在这次改动的提交上打一个 tag 一并推送：

- 会影响 `proto/uma.proto` 的 wire schema 改动（加字段、删字段、改字段号、
  加新的 `enum`……即第 5 节"升级"覆盖的范围）
- 富化/条件 ID 解析逻辑、去重逻辑这类热路径正确性改动
- `storage.rs` 的本地存储格式改动
- 真实生产故障的修复（不管故障是不是这次会话自己引入的）
- 任何单次改动导致过生产环境重启/宕机的

不确定算不算"重要"，就打；漏打的代价（多一个不必要的 tag）远小于漏推的代价
（本地一堆没推的 commit，回滚/复盘时找不到对应点，或者换一台机器/找别人接手
时看到的代码跟服务器上跑的对不上）。

Tag 命名：语义化版本号 `vX.Y.Z`（例如 `v0.2.0`），打在验证通过、决定要推的
那个 commit 上，同时把 [Cargo.toml](../Cargo.toml) 的 `version` 字段改成同一
个号（不带前导 `v`，例如 `0.2.0`）跟 tag 保持一致：

```bash
git push origin main
git tag "v0.2.0"       # 号怎么定见下面的升级规则，改完 Cargo.toml 再打 tag
git push origin --tags
```

**升级规则**（X.Y.Z 怎么定，别拍脑袋）：

- **PATCH（Z）**：纯 bug 修复（含生产故障修复）、不改变外部可见行为的重构/
  整理、面板样式或文档这类改动。
- **MINOR（Y）**：符合第 5 节"升级"纪律的正常演进——新增 Protobuf 字段（只
  加不改）、可自愈的存储格式改动（旧格式能被识别并优雅降级/重新同步，不是
  硬报错拒绝启动）、富化/条件 ID 解析逻辑变更、新功能但不破坏现有下游兼容性。
- **MAJOR（X）**：第 5 节点名的"破坏性升级"——Protobuf 字段号复用/删除导致
  下游读错数据而不是报错、存储格式改了但不能自愈迁移（旧数据只能报错拒绝或
  丢弃，没有优雅降级路径）、需要下游消费者协同升级才能完成的改动。
- **`0.y.z` 阶段的例外**：只要这个项目还没有真实生产下游消费者（现状——见
  `proto/uma.proto` 里"目前没有生产消费者，字段号可以自由排布"那条注释），
  破坏性改动允许只跳 MINOR、不用跳到 `1.0.0`——这是标准语义化版本的约定
  （`0.y.z` 阶段本身就允许破坏性变更不必等大版本号），不用为了"这算不算破
  坏性"纠结着要不要提前把大版本号跳到 1。等项目第一次有真实下游依赖接入，
  从那一刻起切到 `1.0.0`，之后严格执行"破坏性=MAJOR"。

日常的小修小补（文档措辞、面板样式微调这类）不必每次都推+打tag，攒到下一个
"重要变更"一起推也可以，但本地不要攒太久——`deploy.sh` 允许把未提交的
"-dirty"改动直接部署上生产，这本身就是一个"本地和远程/服务器三方不一致"的
风险敞口，重要变更走完验证就尽快推，不要让服务器上跑的代码只存在于本地
working tree 里。

打 tag 推送的同一个 commit 里，顺手在 [CHANGELOG.md](../CHANGELOG.md) 加一条
（见该文件里的格式说明）——这是给人看的、比 git log 更好读的重要变更记录，
git tag 本身只解决"能不能 bisect 找到点"，不解决"一眼看出这个项目改过什么"。
不方便在同一个 commit 里加的话（比如 tag 依赖的哈希只有提交完才知道），紧跟
着单独提一个只改 `CHANGELOG.md` 的 commit 补上，不需要再打 tag（参考仓库里
`a2e74b5`/`a6ee2d9` 这一对的先例）。

### 3.2 TODO 与跨会话的持久记忆

`.claude/TODO.local.md`（不进 git，见 `.gitignore`）是本机的草稿纸，放还没到
commit/tag 门槛的待办、想法、"下次记得改"。每次开始新任务前先扫一眼，任务做
完直接删条目，不追求保留历史。要不要进 git 看内容性质：会影响项目/值得团队
知道的进 `CHANGELOG.md` 或直接写进代码注释/commit message，纯个人草稿留在
`.claude/TODO.local.md`。

Claude Code 每个会话开始时会加载一份项目级持久记忆（`~/.claude/projects/`
下按项目路径隔离，不随某个对话窗口归档而丢失）。这份记忆里有一条指向本节和
上述两个文件的指针——如果某次对话积累了值得留到下次会话的待办或既定决策，
应该写回 `.claude/TODO.local.md` / `CHANGELOG.md`（文件是唯一真相来源，人和
其他工具也能看到），而不是只指望塞进那份记忆里；记忆本身只做"提醒来看这两
个文件"，不重复存内容，避免两处不同步。

## 4. 部署

### 4.1 首次搭建 Mac 交叉编译环境（一次性）

```bash
rustup target add x86_64-unknown-linux-musl
brew install messense/macos-cross-toolchains/x86_64-unknown-linux-musl
```

`.cargo/config.toml` 已经把该 target 的 linker 指到
`x86_64-unknown-linux-musl-gcc`，装完工具链直接能 `cargo build --release
--target x86_64-unknown-linux-musl`，不需要 Docker、不需要 `cross`。

### 4.2 日常部署

```bash
./deploy/deploy.sh              # 完整门禁 + 交叉编译 + 部署 + 健康检查
./deploy/deploy.sh --skip-checks  # 只在紧急热修复且已手动验证时使用
```

脚本做的事（详见 `deploy/deploy.sh` 注释）：

1. 本机跑 `cargo fmt --check && cargo clippy -D warnings && cargo test`。
2. 交叉编译 `x86_64-unknown-linux-musl` release 静态二进制。
3. `scp` 到服务器 `/opt/rust-uma/rust-uma.new`，保留上一个二进制为
   `rust-uma.prev`（供回滚），再原子替换。
4. 安装/更新 `deploy/rust-uma.service`（非 root 运行，
   `ProtectSystem=strict`，运行数据在 systemd `StateDirectory`）。
5. `systemctl restart` 后轮询 `/healthz`（首次冷启动要等 Gamma 全量目录同步，
   可能需要几分钟，脚本会等最多 3 分钟）。

**服务器上的 `/etc/rust-uma/rust-uma.env` 只在首次部署时从本地 `.env`
派生一次**（会强制覆盖 `API_ADDR=0.0.0.0:8011`、
`DATA_DIR=/var/lib/rust-uma`），之后 `deploy.sh` 不会再碰它。要改 RPC 端点、
`WSS_RPC_LIST`、`UMA_CONTRACT_ADDRESSES` 等运行参数，直接登服务器编辑这个文
件，然后 `sudo systemctl restart rust-uma.service`，不需要重新部署代码。

### 4.3 回滚

```bash
./deploy/rollback.sh
```

只保留一代回滚（`rust-uma.prev`），连续两次部署都出问题时这个脚本救不了，要
手动从上一个已知良好的 git commit 重新交叉编译部署。

### 4.4 部署前后的检查清单

- 部署前：确认 `git status` 干净（`deploy.sh` 会在脏工作区打印警告但不阻
  塞——这是有意为之，允许紧急情况下带着未提交改动部署，但正常流程应该是先
  提交再部署）。
- 部署后：`curl http://43.131.1.194:8011/healthz`，关注
  `rpc_connected`、`rpc_sources_connected`（应等于配置的 WSS 路数）、
  `enrichment_hits_via_market_id_total` 是否在增长（富化没坏掉的信号）、
  `catalog_markets` 冷启动后是否稳定在几十万量级（活跃 + 最近关闭窗口）、
  `catalog_reconcile_gaps_closed_total` 长期趋势（应该趋向 0，非零说明还在
  查漏补缺）；`decode_errors_total` 涨得快是正常的——UMA 的
  `ProposePrice`/`DisputePrice` topic 是全 Polygon 共用的，非 Polymarket 的
  UMA 请求也会先收到，解析/富化阶段自然过滤掉，这部分误差不代表故障（emitter
  白名单默认关闭，见上）。
- 每一条富化 miss 都会在默认日志级别打一条 `WARN enrichment miss: broadcasting
  without token_ids/tag_ids`（`pipeline.rs`），带 `market_id`/
  `derived_condition_id`/`requester`/`tx`/`block`，不需要开 `RUST_LOG=debug`
  就能看到：`sudo journalctl -u rust-uma.service -f | grep "enrichment miss"`。
  这是排查"为什么这条事件没富化"的第一手数据源，比事后翻监控面板的实时推送
  列表更直接——面板解析的是当前批次的 WSS 帧，不是事件处理那一刻的状态，且
  只保留打开面板之后的记录。

## 5. 升级

"升级"在这个项目里特指：换 Protobuf 协议字段、换富化/条件 ID 解析逻辑、换
存储格式这类**会影响正在运行的下游消费者或本地持久化数据**的改动，比日常部
署多几条约束：

- **Protobuf 字段只做加法**：`proto/uma.proto` 新增字段用新的字段号，不复用
  旧字段号，不删除旧字段（除非确认所有下游消费者已经切换）。已连接的下游按
  `after_sequence` 断点续传（`FrameHub::after`），协议不兼容会让它们直接读错
  数据而不是报错，风险比进程崩溃更隐蔽，升级 Protobuf 前必须人工确认下游兼容。
- **`.cache` 数据不能跨版本静默失效**：`storage.rs` 里 `catalog.bin`、
  `events.wal`、两个 cursor 文件的格式如果要改，必须能识别旧格式并给出明确
  报错或迁移路径，不能让服务用错误反序列化的数据静默启动。改存储格式前，先
  在本地用一份从生产 `scp` 下来的 `catalog.bin` 副本验证新代码能正确处理（或
  明确拒绝）旧格式。
- **富化/条件 ID 解析逻辑变更走第 2 节"真实数据回归测试"流程**，且升级后要
  像 4.4 一样盯 `enrichment_hits_via_market_id_total` 的增长趋势，不能只看
  `/healthz` 是 `ok` 就算过。
- **允许短暂重启造成的广播中断**，不追求热升级/零停机——下游本来就要实现重
  连 + `after_sequence` 续传（`/uma/v1/ws?after_sequence=N`）来应对网络抖动，
  几秒的重启窗口不是新增风险；但如果以后要做真正的滚动升级（比如两台机器切
  流量），需要先确认下游能不能接受同一批事件从两个 `sequence` 序列源收到（
  当前 `sequence` 是进程内自增，不是全局唯一，多实例部署前要先解决这个）。
- 破坏性升级（协议不兼容、无法回退的存储格式变更）之前，先确认
  `deploy/rollback.sh` 的"只保留一代"够不够用——真正高风险的升级应该手动多
  留几代二进制，而不是只依赖脚本默认行为。
