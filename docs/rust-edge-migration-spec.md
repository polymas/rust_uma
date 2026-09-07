# uma-edge / uma-console 的 Rust 迁移规格

> 目的：把 `poly_uma` 里已实现并通过集成验证的 Go 版 `cmd/uma-edge`、`cmd/uma-console`、`internal/edgeproto`
> 移植到 `rust_uma` 仓库，成为与 tinyuma 同仓库、同工具链、同发布流程的两个 binary。
> 本文是移植的唯一规格：**行为以本文和 Go 版为准，不重新设计。** 章节 §5 的不变量逐条都要有测试。

## 1. 范围与落点

| 项 | 决定 |
|---|---|
| 仓库 | `rust_uma`，新增 `src/bin/uma-edge.rs`、`src/bin/uma-console.rs`，共享代码放 `src/edge/`（或 workspace 子 crate `uma-edge-core`） |
| 复用 | `proto/uma.proto`（只读 `UmaBatch.batch_sequence`、`UmaEvent.sequence`）、`wire.rs` 的帧头常量、`zstd`（Go 版没有 zstd，Rust 版**可以**解压后读序号）、`config.rs` 的 env 读取风格、`deploy/deploy.sh` 的 musl 交叉编译与 scp 发布 |
| 依赖 | 已在 `Cargo.toml`：`axum(ws)`、`tokio`、`tokio-tungstenite`、`bytes`、`prost`、`reqwest(rustls)`、`serde/serde_json`、`tracing`、`zstd`、`url`。**不新增**运行时依赖；需要随机抖动用 `rand` 可加 |
| 端口 | edge `0.0.0.0:8012`，console `0.0.0.0:8013` |
| 不做 | 任何业务解析、字段改写、时间戳注入、sports 过滤、HTTP 历史查询、跨节点共享帧环、持久化 |

Go 参考实现（移植时逐文件对照）：

```text
poly_uma/internal/edgeproto/edgeproto.go   心跳/指令/列表 JSON 结构（字段名必须逐字一致）
poly_uma/cmd/uma-edge/frame.go             帧头 + varint 走查
poly_uma/cmd/uma-edge/ring.go              固定容量帧环
poly_uma/cmd/uma-edge/hub.go               订阅者表、非阻塞扇出、慢客户端、批量释放
poly_uma/cmd/uma-edge/upstream.go          唯一上游连接、退避重连、after_sequence、1013
poly_uma/cmd/uma-edge/main.go              HTTP/WS 路由、鉴权、_replay、drain、信号
poly_uma/cmd/uma-edge/heartbeat.go         心跳上报与指令执行
poly_uma/cmd/uma-console/registry.go       注册表、状态判定、权重、列表
poly_uma/cmd/uma-console/main.go           控制台路由、管理动作、llms 合并
poly_uma/cmd/uma-console/llms_cluster.txt  集群接入契约（原样 include_str!）
poly_uma/cmd/uma-console/index.html        状态页（原样 include_str!）
poly_uma/cmd/uma-edge/edge_test.go         单测（逐条移植）
poly_uma/cmd/uma-console/registry_test.go  单测（逐条移植）
poly_uma/tools/edge_test/*.py              集成测试脚本（直接复用）
poly_uma/deploy/systemd/uma-edge.*         systemd 与 env 示例（改路径即可）
```

## 2. 需要提供的输入

移植前确认以下信息已到位，缺一项就先补：

1. tinyuma 契约：`http://43.131.1.194:8011/llms.txt`，帧头 12 字节（`UMA1` / flags bit0=zstd / schema / 保留 / 未压缩长度 BE）、子协议 `uma.pb.v1`、`after_sequence` 语义、1013 关闭码、`sent_at_us`。
2. `proto/uma.proto` 当前版本（字段号：`UmaBatch.batch_sequence=2`、`UmaBatch.events=4`、`UmaEvent.sequence=1`）。
3. 令牌：`CONSOLE_NODE_TOKEN`（节点心跳）、`CONSOLE_ADMIN_TOKEN`（管理）、`EDGE_ADMIN_TOKEN`（节点管理）、`EDGE_CLIENT_TOKENS`（可选客户端鉴权）。
4. 控制台域名与节点公网地址/TLS 方式（决定 `EDGE_ADVERTISE_HOST/PORT/TLS`）。
5. 部署主机的 `deploy.sh` 目标目录约定（沿用 `/var/lib/rust-uma` 风格，新服务用 `/opt/uma-edge`、`/opt/uma-console` 或按 rust_uma 惯例统一）。

## 3. 模块映射与实现要点

### 3.1 `frame.rs`（← frame.go）

- `parse_frame(&[u8]) -> Result<FrameInfo, BadFrame>`：校验 `UMA1`，读 flags/schema/payload_size。
- 未压缩：手写 varint 走查（不要用 prost 全量解码，热路径只要两个字段），取 `batch_sequence` 与 `events[].sequence` 最大值。
- 压缩：Go 版跳过；Rust 版用 `zstd::bulk::decompress(body, payload_size.min(MAX_DECOMPRESSED))` 后同样走查，`MAX_DECOMPRESSED` 用 tinyuma 的 262144。解压失败不算错误，序号置 0。
- 解析失败**仍然转发**（edge 不是校验器），只计 `bad_frames_total`。

### 3.2 `ring.rs`（← ring.go）

- `VecDeque<Arc<Frame>>`，固定容量（`EDGE_RING_FRAMES`，默认 4096），满则 `pop_front`。
- `Frame { offset: u64, event_seq: u64, bytes: Bytes, recv_at_us: i64 }`；`offset` 单调自增，`event_seq` 压缩帧继承上一帧。
- `tail(n)`、`stats() -> (count, oldest_offset, latest_offset)`。
- 用 `parking_lot::RwLock` 或 `std::sync::RwLock` 均可；**不要**在持锁时做 I/O。

### 3.3 `hub.rs`（← hub.go）

两种实现都可接受，二选一并写清楚：

- **A. 每订阅者一个 `tokio::sync::mpsc` 有界通道（与 Go 版同构，推荐）**：`try_send` 失败即摘除并标记关闭码 1008；`release(n, code)`；`subscribe()` 在**同一把锁内**先取 `ring.tail(replay)` 快照再插入订阅者表（§5.3）。
- B. `tokio::sync::broadcast`：容量 = 队列大小，接收端 `Lagged` 视为慢客户端并主动关闭 1008。注意 broadcast 的容量是全局共享而非每客户端，语义与 Go 版略有差异；drain 需要额外的每客户端关闭信号通道。

共同要求：广播路径只做 `Arc` clone 与 `try_send`，绝不 `.await` 客户端；统计 `fanout_last_us/fanout_max_us`（整轮广播耗时）、`slow_clients_disconnected_total`、`deliveries_total`。

### 3.4 `upstream.rs`（← upstream.go）

- 单任务、单连接，`tokio_tungstenite::connect_async_with_config` 带请求头 `Sec-WebSocket-Protocol: uma.pb.v1`；握手后校验响应子协议一致，否则视为失败重连。
- 退避：1s 起、×2、封顶 15s、加 0–25% 抖动。
- 拨号 URL：去掉原有 `after_sequence`，若 `last_event_seq>0 && !cursor_rejected` 则加 `after_sequence=<last>`；收到 close 1013 → `cursor_rejected=true`，下次不带游标；连接成功后置回 false。
- 保活：每 20s 发 Ping；60s 没收到任何帧或 Pong 判定死连接（tungstenite 需要自己维护超时：`tokio::time::timeout` 包 `next()`）。
- 每帧：`parse_frame` → 更新 `last_event_seq` → `Arc<Frame>` → `ring.push` → `hub.broadcast` → 计数 `frames_total/bytes_total/last_frame_at_ms`。
- 只处理 Binary；Text 丢弃。

### 3.5 `server.rs`（← main.go）

axum 路由：

| 路径 | 行为 |
|---|---|
| `GET /uma/v1/ws` | 客户端鉴权 → draining 则 503 → `clients >= max` 则 503 → 必须提供子协议 `uma.pb.v1`（`WebSocketUpgrade::protocols(["uma.pb.v1"])`，未提供返回 400）→ 解析 `_replay`（`0..=4096` 或 `all`，非法 400）→ 升级 |
| `GET /edge/ready` | `!draining && upstream_connected` → 200 `ok`，否则 503 |
| `GET /edge/healthz` | 与心跳同一个 JSON |
| `GET /edge/admin/clients` | Bearer `EDGE_ADMIN_TOKEN`；返回 `{node_id, clients:[{id,ip,port,connected_at_ms,connected_seconds}]}` |
| `POST /edge/admin/rebalance` | `{"count":1..100}` → 以 1012 释放 count 个，返回 `{released}` |
| `POST /edge/admin/drain` | 触发 drain（异步），202 `{draining:true, clients}` |
| 其他 | 404；**没有 `/llms.txt`** |

每个客户端连接两个任务：

- 写任务：先顺序写回放快照（每帧写超时 5s），再循环 `rx.recv()`；通道关闭时按记录的关闭码发 Close 帧（1000 正常 / 1008 慢客户端 / 1012 摘流）；每 30s 发 Ping。
- 读任务：丢弃所有数据帧；Ping 回 Pong；90s 无任何消息（含 Pong）即断开；读结束 → 从 hub 注销。
- 客户端 IP：优先 `X-Forwarded-For` 第一个，否则 peer addr。

drain（§5.5）：`draining=true`（一次性，不可撤销，重启才恢复）→ 循环 `hub.release(batch, 1012)` 每 `interval` 一批 → 客户端归零或超时（默认 10m）→ 若由 admin 接口触发则退出进程；SIGTERM 触发的 drain 完成后退出。

### 3.6 `heartbeat.rs`（← heartbeat.go）

- 每 `EDGE_HEARTBEAT_INTERVAL`（默认 5s）`POST {console}/api/v1/nodes/heartbeat`，Bearer `EDGE_CONSOLE_TOKEN`，body 为 §7 的 Heartbeat，超时 4s。
- 失败只记日志（第 1 次和每 12 次），不影响服务。
- 响应 `Directive`：`heartbeat_seconds` 改周期；`drain=true` 且未 draining → 触发 drain（不退出进程由 systemd 决定；Go 版为不退出）。
- `EDGE_CONSOLE_URL` 为空则不上报，只打 WARN。

### 3.7 `console`（← registry.go + main.go）

- 注册表 `HashMap<node_id, Node>` 内存态；`upsert` 记录 `remote_ip`、`last_seen_ms`、`first_seen_ms`。
- **重启清 drain 规则**：同一 node_id 的 `started_at_ms` 变化 → `desired_drain=false`（§5.6）。
- 状态判定优先级（严格按序）：`stale`（>15s 无心跳）> `disabled` > `draining`（desired 或节点自报）> `not_ready` > `full`（clients ≥ max）> `serving`。
- `GET /api/v1/nodes`：只含 `serving`；`weight = max(1, max_clients - clients)`；按 weight 降序；`ttl_s` 默认 30；`subprotocol/path` 取自第一台节点；响应头 `Cache-Control: max-age=<ttl>`；`Access-Control-Allow-Origin: *`。
- 节点 URL：`{ws|wss}://{advertise_host 或 remote_ip}:{port 或 8012}{path 或 /uma/v1/ws}`。
- 管理：`GET /api/v1/admin/nodes`；`POST /api/v1/admin/nodes/{id}/{drain|undrain|disable|enable|forget}`；未知节点 404；动作写日志含来源 IP。
- `POST /api/v1/nodes/heartbeat`：Bearer 节点令牌；body ≤64KiB；`node_id` 为空 400。
- `GET /llms.txt`：`include_str!("llms_cluster.txt")` + 分隔线 + 上游 llms（`CONSOLE_LLMS_UPSTREAM`，缓存 5 分钟，失败沿用旧缓存，首次失败给占位文本）。
- `GET /`：`include_str!("index.html")`；`GET /healthz`：`{status, serving_nodes, known_nodes, version, commit, build_time, uptime_s}`。
- 两个令牌任一为空直接启动失败。

## 4. 配置（环境变量名必须与 Go 版一致）

edge：`EDGE_LISTEN_ADDR`(0.0.0.0:8012) `EDGE_UPSTREAM_URL`(ws://43.131.1.194:8011/uma/v1/ws) `EDGE_SUBPROTOCOL`(uma.pb.v1) `EDGE_CONSOLE_URL` `EDGE_CONSOLE_TOKEN` `EDGE_NODE_ID`(hostname) `EDGE_ADVERTISE_HOST` `EDGE_ADVERTISE_PORT`(取自 listen) `EDGE_ADVERTISE_TLS`(0/1) `EDGE_CLIENT_QUEUE`(128) `EDGE_RING_FRAMES`(4096) `EDGE_MAX_CLIENTS`(1000) `EDGE_ADMIN_TOKEN` `EDGE_CLIENT_TOKENS`(逗号分隔，空=不鉴权) `EDGE_DRAIN_BATCH`(20) `EDGE_DRAIN_INTERVAL`(2s) `EDGE_DRAIN_TIMEOUT`(10m) `EDGE_HEARTBEAT_INTERVAL`(5s)

console：`CONSOLE_LISTEN_ADDR`(0.0.0.0:8013) `CONSOLE_NODE_TOKEN` `CONSOLE_ADMIN_TOKEN` `CONSOLE_STALE_AFTER`(15s) `CONSOLE_LIST_TTL_SECONDS`(30) `CONSOLE_LLMS_UPSTREAM`(http://43.131.1.194:8011/llms.txt)

版本信息：`version/commit/build_time` 由构建注入（rust_uma 已有 `build.rs`，沿用），心跳与 `/healthz` 都要带。

## 5. 必须保持的不变量（每条都要有测试）

1. **上游只有一条连接**，不论下游多少客户端；重连带 `after_sequence`，1013 后一次不带。
2. **帧字节不可变、原样转发**：下游收到的 BinaryMessage 与上游逐字节相等，Text 不转发。
3. **回放与实时无缝**：`subscribe` 在同一把锁内取 `ring.tail(replay)` 并注册；先写完回放再写通道内容；不重不漏（测试：回放 5 帧后第 6 帧序号紧接）。
4. **广播永不等待客户端**：`try_send` 失败即摘除（1008）；其余客户端不受影响；`fanout_max_us` 常态 <1000。
5. **drain 语义**：draining 后 ready=503、新握手 503、按批 1012 关闭、归零或超时后退出；draining 单向。
6. **重启清 drain**：控制台看到 `started_at_ms` 变化即清 `desired_drain`；否则新进程会立刻再次 drain（这是 Go 版实测踩到的 bug）。
7. **状态优先级与权重**如 §3.7；stale 节点绝不出现在公开列表。
8. **子协议强制**：客户端未提供 `uma.pb.v1` → 400；上游未回同名子协议 → 断开重连。
9. **`_replay` 上限 4096**，`all` 等价于上限；非法值 400。
10. **不提供 `/llms.txt`**（edge）；控制台 `/llms.txt` = 集群契约 + 上游原文。
11. **鉴权**：`EDGE_CLIENT_TOKENS` 非空时 Bearer 或 `?token=` 任一匹配即可；管理接口令牌为空时一律拒绝（不是放行）。
12. **心跳字段名**与 §7 逐字一致（控制台状态页和 Worker SDK 依赖它们）。

## 6. async Rust 注意事项（移植时最容易出错的地方）

- **取消安全**：`tokio::select!` 里对 `ws.next()` / `rx.recv()` 的分支是取消安全的，但 `ws.send(..).await` 不是——写任务不要把 `send` 放进 `select!` 分支里被取消，改为顺序 `await` 并用 `timeout(5s, send)` 包裹；超时即视为慢客户端，关闭连接。
- **sink/stream 拆分**：`ws.split()` 后写端在写任务、读端在读任务；关闭时任一方退出都要通知另一方（`CancellationToken` 或 `oneshot`），否则任务泄漏——这正是"Rust 也会泄漏"的典型。
- **不要在锁内 `.await`**：hub 的订阅者表用同步锁，广播只做 `try_send`；回放快照拷贝出锁后再写。
- **背压**：每客户端 `mpsc::channel(EDGE_CLIENT_QUEUE)`；绝不用 `unbounded_channel`。
- **`Arc<Bytes>` 零拷贝**：一帧只分配一次；tungstenite `Message::Binary(Bytes)`（0.30 已支持 `Bytes`），避免每客户端 `to_vec()`。
- **Ping/Pong**：tungstenite 收到 Ping 会在下一次读时自动排队 Pong，但只有你持续调用 `next()` 才会真正发出——读任务必须一直在读。
- **读超时**：tungstenite 没有内建读超时，用 `timeout(90s, ws.next())`；上游用 60s。
- **关闭码**：`CloseFrame { code: CloseCode::Library(1012) / Policy(1008) / Normal }`；发送 Close 后给对端 1s 再断，不要立刻 drop。
- **信号**：`tokio::signal::unix::signal(SignalKind::terminate())`，收到后执行 drain 再退出；`panic = "abort"` 已在 release profile，任何 `unwrap` 都会带走整台节点的所有连接——热路径禁止 `unwrap/expect`。
- **HTTP 客户端**：心跳用一个长期复用的 `reqwest::Client`（连接池），每次新建会耗尽端口。
- **axum 的 WS 升级**：`WebSocketUpgrade::protocols(["uma.pb.v1"])` 只在客户端提供该子协议时协商成功；需要在 handler 里先检查 `Sec-WebSocket-Protocol` 头是否包含它，否则返回 400（axum 默认不会拒绝）。
- **时间戳**：所有 `*_ms/_us` 用 `SystemTime::now()` 的 Unix 时间，不用 `Instant`。
- **日志**：`tracing`，级别与 rust_uma 一致；慢客户端、drain、上游重连必须有 INFO/WARN。

## 7. 接口 JSON（字段名逐字一致）

Heartbeat（edge → console，`POST /api/v1/nodes/heartbeat`，同时也是 `GET /edge/healthz` 的响应）：

```text
node_id version commit advertise_host port tls path subprotocol
ready draining
upstream_url upstream_connected upstream_reconnects_total last_frame_at_ms frames_total bytes_total
last_event_sequence ring_frames ring_capacity
clients max_clients slow_clients_disconnected_total clients_accepted_total clients_rejected_total
fanout_last_us fanout_max_us started_at_ms sent_at_ms
```

Directive（console → edge，心跳响应）：`drain drain_batch drain_interval_ms heartbeat_seconds`

NodeList（`GET /api/v1/nodes`）：`nodes:[{node_id url weight clients}] ttl_s generated_at_ms subprotocol path`

管理视图（`GET /api/v1/admin/nodes`）：`nodes:[{heartbeat{...} remote_ip last_seen_ms first_seen_ms disabled desired_drain note status url}] now_ms`

## 8. 测试迁移

单测（从 `edge_test.go`、`registry_test.go` 逐条移植）：

- 帧解析：提取 batch/event 序号；压缩帧（Rust 版应能解压并提取）；垃圾输入与截断 varint 报错。
- 帧环：环绕、`tail` 超量、空环、`stats`。
- hub：回放快照与注册原子；队列 2 帧第 3 帧触发慢客户端摘除且关闭码 1008；`release` 使用 1012。
- 上游：跨压缩帧序号继承；拨号 URL 带/不带 `after_sequence`。
- 注册表：过滤（not_ready/full/draining/stale）、权重、URL 拼装（advertise/tls/port）、指令回传、状态优先级、`forget`、**重启清 drain**。

集成（`tools/edge_test/`，需要 `websockets`、`protobuf` 与 `uma_pb2.py`）：

| 场景 | 步骤 | 期望 |
|---|---|---|
| 注册与列表 | 起 console + 假上游 + edge | `GET /api/v1/nodes` 含节点，`/edge/ready` 200，edge `/llms.txt` 404 |
| 回放 | `edge_client.py <url>?_replay=5 replay 8` | 前 5 帧来自环，第 6 帧序号紧接 |
| 慢客户端 | `fake_upstream.py 18011 65000 0.02` + `stuck_client.py` | `slow_clients_disconnected_total=1`，`clients=0`，`fanout_max_us` 不涨 |
| 控制台 drain | 连一个客户端后 `POST /api/v1/admin/nodes/{id}/drain` | 客户端收 1012；列表立即不含该节点；新握手 503 |
| SIGTERM | `kill -TERM` | 同上并退出 |
| 上游续连 | 重启假上游 | 假上游日志出现 `after_sequence=<last>` |
| 真实上游 | `EDGE_UPSTREAM_URL=ws://43.131.1.194:8011/uma/v1/ws` | `last_event_sequence` 等于 tinyuma `/healthz.event_ring_latest_sequence` |
| 重启清 drain | drain → 重启节点 | 新进程不再 drain，回到 serving |

压测：`poly_uma/tools/uma_ws_conn_load.go` 打 1000 连接、10 帧/s、2.7KB 帧，验收 p99 <100ms、CPU 记录在报告里（用于回答"Rust 是否值得"）。

## 9. rust_uma 工程规范（必须遵守）

- 热路径铁律：上游帧到下游写之间无网络请求、无同步 I/O、无定时攒批。
- `docs/WORKFLOW.md`：先改 `internal/api/llms.txt`（rust_uma 的）再发布；语义化版本 tag；`deploy.sh` musl 静态构建、scp、健康检查、`rollback.sh`；`.env` 不进 git，不在日志/commit 里出现 token。
- 新 binary 的 systemd 单元与 env 示例放 `deploy/`，参照 `poly_uma/deploy/systemd/uma-edge.*`。
- 与 tinyuma 同机部署时，edge 用 loopback 连上游；tinyuma 安全组收紧到只放行 edge。

## 10. 验收标准

- §5 全部不变量有自动化测试且通过；§8 集成场景全部复现。
- 单节点 1000 连接压测 p99 <100ms；RSS 记录（预期 <100MB）。
- 控制台状态页能看到全部节点、drain 一台后 10s 内列表更新。
- rust_uma `llms.txt` 增加 edge/console 章节，内容与 `poly_uma/internal/api/llms.txt` 的"tinyuma 扇出集群"段一致。

## 11. 明确不做

- 不做跨节点帧环共享、不做 HTTP 历史、不做 sports 过滤、不注入时间戳。
- 不在 edge 上暴露 llms.txt。
- 不引入 io_uring 等优化，先用压测数据说话。
