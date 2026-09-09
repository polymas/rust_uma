//! 回归：console/admin 释放（1012）时 writer 先于 reader 结束，session 不能
//! 再次 poll 已完成的 JoinHandle（tokio 会 panic "JoinHandle polled after
//! completion"，生产上 panic=abort 直接把整个 edge 连同几百个客户端打掉）。

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use rust_uma::edge::{
    EdgeConfig, EdgeState, EdgeStats, SUBPROTOCOL, WS_PATH,
    auth::TokenSet,
    hub::{CLOSE_RESTART, Hub},
    now_ms, server,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

static PANICS: AtomicUsize = AtomicUsize::new(0);

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
        drain_interval: Duration::from_millis(100),
        drain_timeout: Duration::from_secs(1),
        heartbeat_interval: Duration::from_secs(5),
    };
    let stats = Arc::new(EdgeStats::default());
    Arc::new(EdgeState {
        hub: Hub::new(config.ring_frames, config.client_queue, stats.clone()),
        auth: TokenSet::new(config.static_client_tokens.clone()),
        config,
        stats,
        started_at_ms: now_ms(),
        http: reqwest::Client::new(),
    })
}

async fn connect(
    addr: SocketAddr,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let mut request = format!("ws://{addr}{WS_PATH}?token=secret")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", SUBPROTOCOL.parse().unwrap());
    let (ws, _) = connect_async(request).await.expect("handshake");
    ws
}

async fn wait_clients(state: &EdgeState, want: usize) {
    for _ in 0..100 {
        if state.hub.clients() == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("hub never reached {want} clients");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_finishes_session_without_polling_completed_writer() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        PANICS.fetch_add(1, Ordering::SeqCst);
        previous(info);
    }));

    let state = test_state();
    let listener = tokio::net::TcpListener::bind(state.config.listen_addr)
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::router(state.clone()).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut ws = connect(addr).await;
    wait_clients(&state, 1).await;

    // What the console heartbeat / admin rebalance / drain all end up calling.
    assert_eq!(state.hub.release(1, CLOSE_RESTART), 1);

    let close = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(frame))) => break frame,
                Some(Ok(_)) => continue,
                other => panic!("expected close frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("close frame within 5s");
    assert_eq!(u16::from(close.expect("close code").code), CLOSE_RESTART);

    // Writer sleeps 1s after the close frame, then the session task finishes
    // (this is where the old code polled the finished JoinHandle again).
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(PANICS.load(Ordering::SeqCst), 0, "session task panicked");
    assert_eq!(state.hub.clients(), 0);

    // The node must keep serving after a release.
    let _ws2 = connect(addr).await;
    wait_clients(&state, 1).await;
    assert_eq!(PANICS.load(Ordering::SeqCst), 0);
}
