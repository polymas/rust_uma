# rust_uma 架构图

配套阅读 [WORKFLOW.md](WORKFLOW.md)（尤其是"热路径铁律"）。本文档只画结构，
不重复解释设计取舍。

## 1. 数据流总览

链上事件到下游广播的完整路径，以及 Gamma 富化缓存如何在启动前预热、之后只
做本地内存查询（不进入热路径）。

```mermaid
%%{init: {"flowchart": {"curve": "basis", "nodeSpacing": 30, "rankSpacing": 58}} }%%
flowchart TD
    subgraph SRC[" 数据源 "]
        direction LR
        WSS1["WSS 端点 #0"]
        WSSN["WSS 端点 #N\n(WSS_RPC_LIST)"]
        HTTP["HTTP RPC"]
        GAPI["Gamma API\n/markets/keyset"]
    end

    subgraph INGEST["uma/rpc.rs · 采集"]
        direction LR
        LW["live_worker × N\n(每路独立永久重连)"]
        BF["run_backfill\n(一次性, 仅 HTTP)"]
    end

    DEC["uma/events/\ndecode_signal_log\nProposePrice / DisputePrice"]
    DROP["丢弃\n(+ duplicates 计数)"]

    subgraph HOT[" 热路径 — 禁止同步网络请求 "]
        direction TB
        DEDUP{"EventHub 去重\n(tx_hash, log_index)"}
        RESOLVE["Catalog::resolve\nmarket_id 优先 → 链上推导兜底"]
        ERING[("EventHub\n事件环")]
        BATCH["run_batcher\n阻塞等第一条 · 非阻塞捎带\n(无定时窗口)"]
        ENC["encode_frame\nProtobuf +（超阈值）Zstd"]
        FRING[("FrameHub\n预编码帧环")]

        DEDUP -->|新事件| RESOLVE --> ERING
        ERING --> BATCH --> ENC --> FRING
    end

    subgraph ENRICH["enrichment.rs · 启动前预热"]
        direction LR
        SYNC["sync_catalog_before_uma\n+ 周期增量 run_catalog_sync"]
        CAT[("Catalog\nby_condition_id\nmarket_to_condition")]
        SYNC --> CAT
    end

    subgraph STORE["storage.rs"]
        direction LR
        WAL["events.wal"]
        SNAP["catalog.bin"]
        CUR["enrichment.cursor\numa.cursor"]
    end

    subgraph API["api.rs · 对外接口"]
        direction LR
        WSAPI["/uma/v1/ws\nafter_sequence 续传"]
        HTTPAPI["/uma/v1/events\n/uma/v1/markets/:id\n/healthz /metrics"]
    end

    WSS1 & WSSN -->|eth_subscribe logs| LW
    HTTP -->|"eth_getLogs 分批"| BF
    LW --> DEC
    BF --> DEC
    DEC --> DEDUP
    DEDUP -->|重复| DROP

    GAPI --> SYNC
    SYNC -.->|落盘后才推进 cursor| SNAP
    SYNC -.-> CUR
    CAT -.->|"O(1) 内存查询\n零网络请求"| RESOLVE
    CAT --> HTTPAPI

    ERING --> WAL
    FRING --> WSAPI
    ERING --> HTTPAPI

    classDef src fill:#e9eef2,stroke:#7d8b96,color:#26313a;
    classDef hot fill:#e4efe9,stroke:#2f7d6c,stroke-width:2px,color:#1c3a32;
    classDef warm fill:#f5e9da,stroke:#a8632a,color:#4a3016;
    classDef sink fill:#efe7f5,stroke:#7c5ba8,color:#33234a;
    classDef void fill:#f0f0ee,stroke:#adaa9e,color:#6b6a5c,stroke-dasharray: 3 2;
    class WSS1,WSSN,HTTP,GAPI,LW,BF,DEC src;
    class DEDUP,RESOLVE,ERING,BATCH,ENC,FRING hot;
    class SYNC,CAT,WAL,SNAP,CUR warm;
    class WSAPI,HTTPAPI sink;
    class DROP void;

    classDef group fill:#ffffff,stroke:#c9c6b8,stroke-width:1px,color:#6b6a5c;
    classDef hotgroup fill:#f3f8f6,stroke:#2f7d6c,stroke-width:1.5px,color:#2f7d6c;
    class SRC,INGEST,ENRICH,STORE,API group;
    class HOT hotgroup;
```

阴影部分（`decode` → `EventHub` → `run_batcher` → `FrameHub`）是延迟敏感的
热路径：不落地等待、不发起同步网络请求，`Catalog` 是它唯一的外部依赖，而且
只读内存。

## 2. 启动顺序

`main.rs::run()` 强制这个顺序，任何重构都不能打乱它——先有完整可用的富化缓
存，链上事件才开始进入热路径。

```mermaid
%%{init: {"theme": "base", "themeVariables": {
    "primaryColor": "#e9eef2", "primaryBorderColor": "#7d8b96", "primaryTextColor": "#26313a",
    "actorBkg": "#e9eef2", "actorBorder": "#7d8b96", "actorTextColor": "#26313a",
    "activationBkgColor": "#e4efe9", "activationBorderColor": "#2f7d6c",
    "noteBkgColor": "#f5e9da", "noteBorderColor": "#a8632a", "noteTextColor": "#4a3016",
    "lineColor": "#8a8878", "signalColor": "#33231a", "signalTextColor": "#1c1e1a",
    "sequenceNumberColor": "#ffffff"
}}}%%
sequenceDiagram
    participant Main as main.rs
    participant Storage as storage.rs
    participant Gamma as enrichment.rs
    participant Rpc as uma/rpc.rs
    participant Live as live_worker(s)
    participant Api as api.rs

    Main->>Storage: load_catalog() / load_events() / load_uma_cursor()
    Main->>Gamma: sync_catalog_before_uma()
    activate Gamma
    Gamma->>Gamma: 读 enrichment.cursor, 增量拉 Gamma keyset
    Gamma->>Storage: save_catalog() 落盘
    Gamma->>Storage: save_enrichment_cursor() 落盘
    deactivate Gamma
    Note over Main: 此时 Catalog 已完整可查询，<br/>热路径才允许启动
    Main->>Rpc: run_rpc_loop()
    Rpc->>Live: 为 WSS_RPC_LIST 每个地址 spawn 一个 live_worker
    Live-->>Rpc: 任一 worker 订阅成功 (any_connected)
    Rpc->>Rpc: run_backfill()（一次性，HTTP，从 uma.cursor 或近 7 天边界开始）
    Note over Live,Rpc: 补拉与实时并行；重叠事件靠<br/>(tx_hash, log_index) 去重，不丢不重
    Main->>Api: serve() 开始对外提供 HTTP/WSS
```

## 3. 部署拓扑

```mermaid
%%{init: {"flowchart": {"curve": "basis", "nodeSpacing": 30, "rankSpacing": 56}} }%%
flowchart TD
    subgraph MAC[" macOS（本机） "]
        direction TB
        CODE["rust_uma 源码"]
        BUILD["cargo build --release\n--target x86_64-unknown-linux-musl\n(musl-cross linker, 无 Docker)"]
        BIN["静态二进制 rust-uma\n(~7MB, stripped)"]
        CODE --> BUILD --> BIN
    end

    subgraph SERVER[" 43.131.1.194（ubuntu, systemd） "]
        direction TB
        ENV["/etc/rust-uma/rust-uma.env\n(首次部署派生, 之后手改)"]
        SVC["rust-uma.service\nUser=ubuntu · ProtectSystem=strict"]
        STATE[("/var/lib/rust-uma\ncatalog.bin · events.wal\nenrichment.cursor · uma.cursor")]
        PORT(["0.0.0.0:8011\n云安全组放行 TCP:8000-8100"])
        ENV -.-> SVC
        SVC <--> STATE
        SVC --> PORT
    end

    subgraph DOWN[" 下游 "]
        BOT["下单 / 交易系统\nWSS 订阅 + after_sequence 续传"]
    end

    BIN ==>|"deploy/deploy.sh\nscp + systemctl restart"| SVC
    PORT ==>|"/uma/v1/ws\nProtobuf + Zstd"| BOT

    classDef src fill:#e9eef2,stroke:#7d8b96,color:#26313a;
    classDef hot fill:#e4efe9,stroke:#2f7d6c,stroke-width:2px,color:#1c3a32;
    classDef warm fill:#f5e9da,stroke:#a8632a,color:#4a3016;
    classDef sink fill:#efe7f5,stroke:#7c5ba8,color:#33234a;
    class CODE,BUILD,BIN src;
    class SVC,PORT hot;
    class ENV,STATE warm;
    class BOT sink;

    classDef group fill:#ffffff,stroke:#c9c6b8,stroke-width:1px,color:#6b6a5c;
    class MAC,SERVER,DOWN group;
```
