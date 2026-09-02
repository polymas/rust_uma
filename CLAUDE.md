# rust_uma

开发、测试、提交、部署、升级的完整规范在 [docs/WORKFLOW.md](docs/WORKFLOW.md)，
改代码或部署前先读那份文档，尤其是"热路径铁律"一节——本项目唯一的核心目标是
链上事件到下游广播的延迟最小化，任何改动先确认没有在热路径上引入同步网络请求。

关键约定速记（细节以 WORKFLOW.md 为准）：

- 提交前必须过 `cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test`。
- 部署用 `./deploy/deploy.sh`（Mac 本机交叉编译 `x86_64-unknown-linux-musl`，
  scp 到 `43.131.1.194`，systemd 管理），不要在服务器上装 Rust 工具链编译。
- 涉及链上事件解析（`uma/events/`）的改动，必须用真实 `eth_getLogs` 抓的数
  据做回归测试，合成样例测试全绿不代表数据格式对——这是踩过两次真实坑之后的
  硬规矩。
- commit message 用中文，`.env`/`AGENTS.md`/`codex.md` 永远不提交。
