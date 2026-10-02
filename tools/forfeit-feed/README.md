# forfeit-feed（独立版）

> **2026-10-02 起 `/ws` 推送改为 protobuf 二进制帧（`proto/polyuma/forfeit/v1/forfeit.proto`，polymas/proto tag `polyuma/forfeit/v1.0.0`；本目录 `forfeit.proto` 是指向它的软链），
> 每条事件带该比赛在 Polymarket 上全部子市场的 `condition_ids`（整场排除名单）。下文 JSON 示例仅适用于 `/history`。**
>
> 本仓库部署：法兰克福 polytest，`wss://forfeit.chainee.space/ws?token=…`，
> systemd 单元与 Caddyfile 在 `deploy/`。相对上游 tar 包修过的问题与运维说明见知识库
> `运维/forfeit-feed弃权信号源解析与部署-2026-10-01.md`。

电竞**弃权/退赛（forfeit）实时信号源**。一旦数据源判定某一局为弃权，
1 秒内通过 WebSocket 推送，并附上该对局对应的 Polymarket 市场标识。

这套代码**完全自包含**，不依赖任何外部私有数据——只需要你自己的 PandaScore API Key。

## 组成

| 文件 | 作用 |
|---|---|
| `ps_probe.py` | 轮询 PandaScore，把「某场某局结束」写成 `result_probe.csv` |
| `pm_lookup.py` | 用 **Polymarket 公开 API** 查出对应市场的 `condition_id` 与两侧 `token_id` |
| `forfeit_ws.py` | 读 CSV，筛出 `forfeit=true`，通过 WebSocket / HTTP 对外提供 |
| `client_example.py` | 零依赖订阅示例 |
| `sample_events.jsonl` | 15 条真实历史事件样本，便于先写解析逻辑 |

**零第三方依赖**：只用 Python 标准库，WebSocket 按 RFC6455 手写。Python 3.9+ 即可。

## 跑起来

```bash
export PANDASCORE_TOKEN=你自己的key

# 终端 1：探针（持续写 CSV）
python3 ps_probe.py

# 终端 2：信号服务
python3 forfeit_ws.py
```

默认监听 `127.0.0.1:8813`，CSV 与事件库都落在当前目录。可用环境变量调整：

| 变量 | 默认 | 说明 |
|---|---|---|
| `PANDASCORE_TOKEN` | 必填 | 你的 PandaScore key |
| `PROBE_POLL_S` | `3`（部署用 5） | 轮询间隔（秒）。免费版注意配额 |
| `PROBE_GAMES` | `lol,cs2,dota2,valorant` | 关注的游戏（slug 会归一化，`cs-go`/`league-of-legends` 等写法都认） |
| `FORFEIT_FEED_HOST` / `FORFEIT_FEED_PORT` | `127.0.0.1` / `8813` | 监听地址 |
| `FORFEIT_FEED_TOKEN` | 空 | 设置后客户端须带 `?token=` 或头 `X-Auth-Token` |

> ⚠️ 若要对公网开放，**务必先设 `FORFEIT_FEED_TOKEN`**，并在前面加一层 TLS 反代。

## 接口

| | |
|---|---|
| `ws://HOST:PORT/ws` | 实时推送 |
| `ws://HOST:PORT/ws?backfill=50` | 连上先补推最近 50 条 |
| `GET /history?limit=N&since=ISO8601` | 历史查询（`limit` 必须是非负整数） |
| `GET /health` | 存活与数据新鲜度 **（探针端点，不含业务数据）** |
| `GET /schema` | 字段清单 |

**业务数据只在 `/ws` 与 `/history`**，别去 `/health` 里找 `condition_id`。

连上后先收 `{"type":"hello"}`，随后 `forfeit_backfill`（仅当带 backfill）与 `forfeit`。
服务端每 25 秒发 ping，客户端需回 pong（主流库自动处理）。

## 事件字段

```json
{
  "type": "forfeit", "seq": 15,
  "emitted_at": "2026-09-29T00:12:02.341000+00:00",
  "source": "pandascore",
  "detect_iso": "2026-09-29T00:12:02.340736+00:00",
  "game": "lol", "league": "CBLOL",
  "match_name": "Upper bracket round 1: KBM vs GL",
  "team_a": "KaBuM! Ilha das Lendas", "team_b": "Golden Lions",
  "game_num": "3", "is_series": false, "map_number": 3,
  "winner": "KaBuM! Ilha das Lendas", "loser": "Golden Lions",
  "src_end_at": "2026-09-29T00:12:01Z", "detect_lag_vs_end_s": 1.3,
  "length_s": 5863, "forfeit": true, "status": "finished", "draw": false,
  "match_id": 1691676, "match_begin_at": "2026-09-28T21:06:16Z",
  "pm_match": { }
}
```

| 字段 | 含义 |
|---|---|
| `seq` | 单调递增，断线重连后可判断漏了哪些 |
| `detect_iso` | 探测到的时刻，即「最早能知道」的时间 |
| `detect_lag_vs_end_s` | 探测滞后秒数，实测多在 1–3 秒 |
| `is_series` | `false` = 分局盘（**做分局盘只认这个**），`true` = 整个 BO |
| `length_s` | 该局时长。**异常值很有信息量**：正常 LoL 单局 1500–2400s、CS2 1500–3000s；`5863` 这种三倍时长意味着中途长时间中断，`5`/`12` 则是根本没开打 |

## `pm_match` —— 对应的 Polymarket 市场

```json
"pm_match": {
  "matched": true,
  "source": "polymarket_public_api",
  "condition_id": "0x01579d0b4d0648c373ccf83cf1619ede0483f1e8919b4b76fe79a13e2ac2dddd",
  "market_slug": "lol-kbm-gl-2026-09-28-game3",
  "pm_event_slug": "lol-kbm-gl-2026-09-28",
  "pm_question": "LoL: KaBuM! Ilha das Lendas vs Golden Lions - Game 3 Winner",
  "pm_event_url": "https://polymarket.com/event/lol-kbm-gl-2026-09-28",
  "clob_market_api": "https://clob.polymarket.com/markets/0x0157...",
  "outcomes": [
    {"pm_outcome": "KaBuM! Ilha das Lendas", "token_id": "6169867691...", "is_pandascore_winner": true},
    {"pm_outcome": "Golden Lions",           "token_id": "9521263729...", "is_pandascore_winner": false}
  ],
  "winner_token_id": "6169867691...",
  "loser_token_id":  "9521263729...",
  "closed": false, "accepting_orders": true
}
```

- **先判 `matched`**。`false` 时只有 `reason`，其余字段都不存在。
- `outcomes[]` **两个方向都给全**，`token_id` 就是 CLOB 下单要的那个 id。
- 查询走 Polymarket 公开 API（`gamma-api` 搜事件 → 取子市场）。
  **请求必须带浏览器 `User-Agent`，否则 403**，`pm_lookup.py` 里已处理。
- 结果按 `(match_id, game_num)` 缓存 1 小时，避免重复打 API。

### 关联覆盖率：大约一半会 `matched=false`

历史 15 条 forfeit 里 **8 条能关联上、7 条关联不上**——那 7 条是冷门赛事，
Polymarket 根本没开盘。**请把 `matched=false` 当成正常分支处理。**

## 订阅示例

```bash
python3 client_example.py --host 127.0.0.1 --port 8813 --backfill 20
```

```python
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://127.0.0.1:8813/ws?backfill=20") as ws:
        async for raw in ws:
            ev = json.loads(raw)
            if ev.get("type") == "forfeit" and not ev.get("is_series"):
                pm = ev["pm_match"]
                if pm["matched"]:
                    print(ev["match_name"], "map", ev["map_number"],
                          "->", pm["condition_id"], pm["winner_token_id"])

asyncio.run(main())
```

断线重连时用最后收到的 `seq` 对照 `/history` 补齐。

## 先知道这个量级再动手

16 天实测：

- `forfeit=true` 共 **15 场**，占全部赛果 3,330 行的 **0.45%**，约 **0.94 场/天**
- 游戏分布：cs2 **14** 场、lol **1** 场
- 其中约**一半 Polymarket 没开盘**，链上无法交易
- 探测延迟稳定 1–3 秒，链路不是瓶颈

**这是个低频信号。** 按「每天约一次、其中一半不可交易」来设计，
不要假设有足够样本做在线统计。
