//! 回归：console 摘流（drain）之后再 undrain，edge 必须真的回到服务状态。
//! 旧代码里 `stats.draining` 是单向的，面板 undrain 只清 console 侧的
//! `desired_drain`，edge 一直上报 `ready:false, draining:true`，于是节点永远
//! 不回 `public_list`，也不进自动均衡的 serving 集合——流量再也切不回来，
//! 只能重启进程。

use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use rust_uma::{
    console::registry::Directive,
    edge::{
        EdgeConfig, EdgeState, EdgeStats, SUBPROTOCOL, WS_PATH,
        auth::TokenSet,
        heartbeat::{apply, build_heartbeat},
        hub::Hub,
        now_ms, server,
    },
};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

fn test_state() -> Arc<EdgeState> {
    let config = EdgeConfig {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_url: String::new(),
        console_url: None,
        console_token: String::new(),
        node_id: "test".into(),
        advertise_host: "127.0.0.1".into(),
        advertise_port: 0,
        advertise_tls: false,
        client_queue: 16,
        ring_frames: 16,
        max_clients: 10,
        admin_token: String::new(),
        static_client_tokens: vec!["secret".into()],
        drain_batch: 1,
        drain_interval: Duration::from_millis(50),
        drain_timeout: Duration::from_secs(1),
        heartbeat_interval: Duration::from_secs(5),
    };
    let stats = Arc::new(EdgeStats::default());
    stats.upstream_connected.store(true, Ordering::SeqCst);
    Arc::new(EdgeState {
        hub: Hub::new(config.ring_frames, config.client_queue, stats.clone()),
        auth: TokenSet::new(config.static_client_tokens.clone()),
        config,
        stats,
        started_at_ms: now_ms(),
        http: reqwest::Client::new(),
    })
}

fn directive(drain: bool) -> Directive {
    Directive {
        drain,
        ..Directive::default()
    }
}

async fn connect_ok(addr: SocketAddr) -> bool {
    let mut request = format!("ws://{addr}{WS_PATH}?token=secret")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", SUBPROTOCOL.parse().unwrap());
    connect_async(request).await.is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_undrain_puts_the_node_back_in_service() {
    let state = test_state();
    let listener = tokio::net::TcpListener::bind(state.config.listen_addr)
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::router(state.clone()).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut interval = state.config.heartbeat_interval;
    assert!(build_heartbeat(&state).ready);
    assert!(connect_ok(addr).await);

    // 面板点 drain：节点停止接客，心跳上报 draining。
    apply(&state, directive(true), &mut interval);
    let hb = build_heartbeat(&state);
    assert!(hb.draining && !hb.ready, "drain should stop advertising");
    assert!(!connect_ok(addr).await, "draining node must reject clients");

    // drain 循环把人放完之后仍然保持 draining（等 undrain 或运维处置）。
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(build_heartbeat(&state).draining);

    // 面板点 undrain：console 下发 drain=false，edge 必须自己也放掉这一位。
    apply(&state, directive(false), &mut interval);
    let hb = build_heartbeat(&state);
    assert!(!hb.draining && hb.ready, "undrain must restore ready");
    assert!(connect_ok(addr).await, "undrained node must accept clients");

    // 重复的 drain=false 心跳不该有副作用。
    apply(&state, directive(false), &mut interval);
    assert!(build_heartbeat(&state).ready);
}

#[tokio::test]
async fn drain_that_exits_the_process_cannot_be_cancelled() {
    // admin/SIGTERM 的 drain 走的是退进程那条路（这里不真的调 start_drain，
    // 否则测试进程会被 exit(0) 带走），console 的 drain=false 不能把它拉回来。
    let state = test_state();
    state.stats.draining.store(true, Ordering::SeqCst);
    state.stats.drain_exits.store(true, Ordering::SeqCst);

    assert!(!server::cancel_drain(&state));
    let mut interval = state.config.heartbeat_interval;
    apply(&state, directive(false), &mut interval);
    assert!(
        state.stats.draining.load(Ordering::SeqCst),
        "a node on its way out must not be pulled back into service"
    );
}
