//! 对外监听 socket 的公共设置。

use std::net::SocketAddr;

use axum::serve::{ListenerExt, TapIo};
use tokio::net::{TcpListener, TcpStream};
use tracing::warn;

pub type NodelayListener = TapIo<TcpListener, fn(&mut TcpStream)>;

/// 绑定监听地址，每个接入的连接都打开 `TCP_NODELAY`。
///
/// axum 0.8 的 `serve` 默认不关 Nagle。广播帧是"同一笔交易的几条事件紧挨着
/// 发几帧"的突发形态，开着 Nagle 时后一帧要等前一帧的 ACK 回来才发——
/// 法兰克福→香港 edge 实测 RTT≈200ms，这一等就是一个 RTT，整条链路唯一的
/// 目标就是延迟。tinyuma（8011）和 edge（8012）都走这里。
pub async fn bind_nodelay(addr: SocketAddr) -> std::io::Result<NodelayListener> {
    Ok(TcpListener::bind(addr)
        .await?
        .tap_io(set_nodelay as fn(&mut TcpStream)))
}

fn set_nodelay(stream: &mut TcpStream) {
    if let Err(error) = stream.set_nodelay(true) {
        warn!(%error, "failed to set TCP_NODELAY on accepted connection");
    }
}

#[cfg(test)]
mod tests {
    use axum::serve::Listener;

    use super::*;

    #[tokio::test]
    async fn accepted_connections_have_nodelay() {
        let mut listener = bind_nodelay("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = Listener::local_addr(&listener).unwrap();
        let client = tokio::spawn(TcpStream::connect(addr));
        let (stream, _) = Listener::accept(&mut listener).await;
        assert!(stream.nodelay().unwrap());
        client.await.unwrap().unwrap();
    }
}
