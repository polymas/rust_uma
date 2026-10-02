#!/usr/bin/env python3
"""forfeit-feed 最小订阅示例（零依赖，标准库手写 WebSocket 客户端）。

用法:
    python3 client_example.py --host <HOST> --port <PORT>
    python3 client_example.py --host <HOST> --port <PORT> --backfill 20
    python3 client_example.py --host <HOST> --port <PORT> --token XXX
"""
import argparse, base64, json, os, socket, struct, sys
import forfeit_pb   # 同目录，forfeit.proto 的零依赖编解码

def connect(host, port, backfill=0, token=""):
    path = "/ws"
    q = []
    if backfill: q.append("backfill=%d" % backfill)
    if token:    q.append("token=%s" % token)
    if q: path += "?" + "&".join(q)
    key = base64.b64encode(os.urandom(16)).decode()
    s = socket.create_connection((host, port), timeout=30)
    s.sendall(("GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n"
               % (path, host, port, key)).encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        c = s.recv(4096)
        if not c: raise RuntimeError("握手时连接被关闭")
        buf += c
    head, rest = buf.split(b"\r\n\r\n", 1)
    if b"101" not in head.split(b"\r\n")[0]:
        raise RuntimeError("握手失败: %s" % head.decode("latin-1")[:200])
    s.settimeout(None)
    return s, rest

def frames(s, rest=b""):
    buf = rest
    def need(n):
        nonlocal buf
        while len(buf) < n:
            c = s.recv(65536)
            if not c: raise StopIteration
            buf += c
    while True:
        try:
            need(2)
        except StopIteration:
            return
        b0, b1 = buf[0], buf[1]
        op, n, off = b0 & 0x0F, b1 & 0x7F, 2
        if n == 126:
            need(4); n = struct.unpack(">H", buf[2:4])[0]; off = 4
        elif n == 127:
            need(10); n = struct.unpack(">Q", buf[2:10])[0]; off = 10
        need(off + n)
        data, buf = buf[off:off+n], buf[off+n:]
        if op == 0x9:                                   # server ping -> pong
            s.sendall(bytes([0x8A, 0x80]) + os.urandom(4))
            continue
        if op == 0x8:
            return
        if op == 0x2:                                   # 推送是 protobuf ForfeitFrame
            yield forfeit_pb.decode("ForfeitFrame", data)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", required=True, help="服务地址，另行提供")
    p.add_argument("--port", type=int, required=True, help="服务端口，另行提供")
    p.add_argument("--backfill", type=int, default=0)
    p.add_argument("--token", default="")
    a = p.parse_args()
    s, rest = connect(a.host, a.port, a.backfill, a.token)
    print("connected ws://%s:%d" % (a.host, a.port), flush=True)
    for fr in frames(s, rest):
        if "hello" in fr:
            print("hello", fr["hello"], flush=True)
            continue
        ev = fr["event"]
        print("[%s] seq=%s %s | %s | g%s | winner=%s | lookup=%s | 排除 %d 个 condition_id"
              % ("backfill" if ev.get("backfill") else "live", ev.get("sequence"), ev.get("game"),
                 ev.get("match_name"), ev.get("forfeited_game_number") or "series", ev.get("winner"),
                 forfeit_pb.LOOKUP_STATUS_NAME.get(ev.get("lookup_status", 0)),
                 len(ev["condition_ids"])), flush=True)
        for cid in ev["condition_ids"]:
            print("    0x" + cid.hex(), flush=True)

if __name__ == "__main__":
    main()
