# p2p-mempool-probe

Polygon 轻量 P2P 节点（不同步链、不存状态），给 `mempool-uma` 提供 pending 交易，并做相关测量。
部署在 polytest（法兰克福），systemd 单元见 `deploy/`。

| 文件 | 作用 |
|---|---|
| `main.go` | 基于 Bor v2.10.1 的 p2p/eth 协议库：收交易公告 / 推送、新块、块头 / 块体 / 收据；`-feed-listen` 把 propose 类交易行推给本机 mempool-uma |
| `seats.go` | 节点席位：核心（≥1% 推送贡献）+ 5 个试用席位末位淘汰 + 冷却；规则见知识库《法兰克福P2P节点生命周期》 |
| `race_compare.py` + `race.html` | 对比服务（:8091）：pending / mempool-uma / rust-uma 三路信号，以链上成功提案为真值，看抢达与正确率 |
| `bayes_monitor.py` + `triangulate.py` + `dashboard.html` | 实验：增量贝叶斯提交偏好 + 多点三角定位（:8090，已停用） |

构建（Mac 交叉编译，需要 go1.26.5 工具链）：

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o p2p-mempool-probe .
```
