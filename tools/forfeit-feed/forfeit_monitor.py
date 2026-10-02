#!/usr/bin/env python3
"""forfeit-feed 订阅监控：作为一个真实下游订阅公网 wss（protobuf 二进制帧），把收到的事件落盘，并提供网页看板。

和下游走同一条链路（DNS → Caddy TLS → forfeit_ws），所以看板上"连着、有心跳"就等于
下游也能正常收到。零第三方依赖。

  MONITOR_WS_URL      wss://forfeit.chainee.space/ws
  MONITOR_FEED_TOKEN  订阅 forfeit-feed 用的 token（= FORFEIT_FEED_TOKEN）
  MONITOR_HOST/PORT   看板监听，默认 127.0.0.1:8816（外层 Caddy 反代 /dash）
  MONITOR_DASH_TOKEN  看板访问令牌（?token= 或 cookie）
  MONITOR_STORE       收到的事件，默认 ./monitor_events.jsonl
"""
import base64, json, os, socket, ssl, struct, threading, time, urllib.parse, urllib.request
import forfeit_pb
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

WS_URL     = os.environ.get("MONITOR_WS_URL", "wss://forfeit.chainee.space/ws")
FEED_TOKEN = os.environ.get("MONITOR_FEED_TOKEN", "").strip()
HOST       = os.environ.get("MONITOR_HOST", "127.0.0.1")
PORT       = int(os.environ.get("MONITOR_PORT", "8816"))
DASH_TOKEN = os.environ.get("MONITOR_DASH_TOKEN", "").strip()
STORE      = Path(os.environ.get("MONITOR_STORE", "./monitor_events.jsonl"))
HEALTH_S   = 10
DASH_HTML  = Path(__file__).with_name("dashboard.html")


def us_iso(us):
    return datetime.fromtimestamp(us / 1e6, timezone.utc).isoformat() if us else None

def pb_to_view(e):
    """解码后的 ForfeitEvent -> 看板用的结构（字段名沿用 /history 的 JSON 形态）。"""
    gn = e.get("forfeited_game_number") or 0
    status = forfeit_pb.LOOKUP_STATUS_NAME.get(e.get("lookup_status") or 0, "unspecified")
    slug = e.get("pm_event_slug") or ""
    markets = [{
        "condition_id": "0x" + m.get("condition_id", b"").hex(),
        "slug": m.get("slug", ""), "question": m.get("question", ""),
        "sports_market_type": m.get("sports_market_type", ""),
        "game_number": m.get("game_number", 0),
        "outcomes": m.get("outcomes", []),
        "token_ids": [str(int.from_bytes(t, "big")) for t in m.get("token_ids", [])],
        "is_forfeited_market": m.get("is_forfeited_market", False),
        "closed": m.get("closed", False), "accepting_orders": m.get("accepting_orders", False),
    } for m in e.get("markets", [])]
    target = next((m for m in markets if m["is_forfeited_market"]), None)
    detect_us, end_us = e.get("detect_at_us") or 0, e.get("src_end_at_us") or 0
    return {
        "seq": e.get("sequence"), "detect_iso": us_iso(detect_us), "detect_epoch": detect_us / 1e6,
        "emitted_at": us_iso(e.get("emitted_at_us")), "sent_at": us_iso(e.get("sent_at_us")),
        "game": e.get("game", ""), "league": e.get("league", ""), "match_name": e.get("match_name", ""),
        "match_id": e.get("pandascore_match_id"), "game_num": str(gn) if gn else "series",
        "is_series": gn == 0, "map_number": gn or None,
        "team_a": e.get("team_a", ""), "team_b": e.get("team_b", ""),
        "winner": e.get("winner", ""), "loser": e.get("loser", ""),
        "length_s": e.get("game_length_s") or None,
        "src_end_at": us_iso(end_us), "match_begin_at": us_iso(e.get("match_begin_at_us")),
        "detect_lag_vs_end_s": round((detect_us - end_us) / 1e6, 1) if detect_us and end_us else None,
        "status": e.get("match_status", ""),
        "frame_bytes": e.get("_frame_bytes"),
        "pm_match": {
            "matched": status == "matched", "status": status,
            "reason": {"not_found": "Polymarket 未开盘", "failed": "Polymarket 查询失败"}.get(status),
            "pm_event_slug": slug, "pm_event_url": "https://polymarket.com/event/%s" % slug if slug else None,
            "condition_ids": ["0x" + c.hex() for c in e.get("condition_ids", [])],
            "markets": markets,
            "market_slug": target["slug"] if target else None,
            "condition_id": target["condition_id"] if target else None,
            "pm_question": target["question"] if target else None,
        },
    }

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def log(*a):
    print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- 状态

class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.events = {}            # seq -> 记录
        self.conn = {"connected": False, "since": None, "connects": 0, "disconnects": 0,
                     "last_error": None, "last_error_at": None, "last_frame_at": None,
                     "last_ping_at": None, "server_last_seq": None}
        self.disconnect_log = deque(maxlen=50)
        self.health = None          # 最近一次 /health
        self.health_at = None
        self.health_error = None
        if STORE.exists():
            for line in STORE.open(encoding="utf-8"):
                try:
                    r = json.loads(line)
                    self.events[r["seq"]] = r
                except Exception:
                    pass
            log("loaded %d events" % len(self.events))

    def add(self, ev, via):
        seq = ev.get("seq")
        if seq is None:
            return False
        with self.lock:
            old = self.events.get(seq)
            if old is not None:
                # 同一 seq 重推 = 排除名单有补充（见 forfeit_ws.retry_loop），替换明细、保留首次收到时间
                if len(ev.get("pm_match", {}).get("condition_ids") or []) > \
                        len(old["event"].get("pm_match", {}).get("condition_ids") or []):
                    old["event"] = ev
                    old["updated_iso"] = utcnow()
                    with STORE.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(old, ensure_ascii=False) + "\n")
                return False
            now = time.time()
            rec = {"seq": seq, "via": via, "recv_iso": utcnow(), "recv_epoch": round(now, 3),
                   # 只有实时推送的延迟有意义；补推的是历史
                   "push_lag_s": (round(now - ev["detect_epoch"], 3)
                                  if via == "live" and ev.get("detect_epoch") else None),
                   "event": ev}
            self.events[seq] = rec
            with STORE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            return True

    def gaps(self):
        with self.lock:
            seqs = sorted(self.events)
        if not seqs:
            return []
        have = set(seqs)
        return [s for s in range(seqs[0], seqs[-1] + 1) if s not in have]

STATE = State()


# ---------------------------------------------------------------- wss 客户端

def ws_connect(url):
    u = urllib.parse.urlparse(url)
    port = u.port or (443 if u.scheme == "wss" else 80)
    raw = socket.create_connection((u.hostname, port), timeout=15)
    s = ssl.create_default_context().wrap_socket(raw, server_hostname=u.hostname) \
        if u.scheme == "wss" else raw
    key = base64.b64encode(os.urandom(16)).decode()
    path = (u.path or "/") + ("?" + u.query if u.query else "")
    s.sendall(("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
               "User-Agent: forfeit-monitor/1\r\n\r\n" % (path, u.hostname, key)).encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        c = s.recv(4096)
        if not c:
            raise ConnectionError("handshake closed")
        buf += c
    head, rest = buf.split(b"\r\n\r\n", 1)
    status = head.split(b"\r\n")[0].decode("latin-1")
    if " 101 " not in status:
        raise ConnectionError("handshake: " + status)
    s.settimeout(90)                # 服务端每 25s ping，90s 没任何帧就当断线
    return s, rest

def ws_send(s, payload, opcode):
    mask = os.urandom(4)
    n = len(payload)
    h = bytearray([0x80 | opcode])
    if n < 126:
        h.append(0x80 | n)
    elif n < 65536:
        h.append(0x80 | 126); h += struct.pack(">H", n)
    else:
        h.append(0x80 | 127); h += struct.pack(">Q", n)
    s.sendall(bytes(h) + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

def ws_frames(s, buf):
    def need(n):
        nonlocal buf
        while len(buf) < n:
            c = s.recv(65536)
            if not c:
                raise ConnectionError("server closed")
            buf += c
    while True:
        need(2)
        op, n, off = buf[0] & 0x0F, buf[1] & 0x7F, 2
        if n == 126:
            need(4); n = struct.unpack(">H", buf[2:4])[0]; off = 4
        elif n == 127:
            need(10); n = struct.unpack(">Q", buf[2:10])[0]; off = 10
        need(off + n)
        data, buf = buf[off:off + n], buf[off + n:]
        yield op, data

def ws_loop():
    backoff = 1
    while True:
        url = WS_URL + ("&" if "?" in WS_URL else "?") + "backfill=500"
        if FEED_TOKEN:
            url += "&token=" + urllib.parse.quote(FEED_TOKEN)
        try:
            s, rest = ws_connect(url)
            with STATE.lock:
                STATE.conn.update(connected=True, since=utcnow())
                STATE.conn["connects"] += 1
            log("connected")
            backoff = 1
            for op, data in ws_frames(s, rest):
                with STATE.lock:
                    STATE.conn["last_frame_at"] = utcnow()
                if op == 0x9:
                    ws_send(s, data, 0xA)
                    with STATE.lock:
                        STATE.conn["last_ping_at"] = utcnow()
                elif op == 0x8:
                    raise ConnectionError("server sent close")
                elif op == 0x2:
                    frame = forfeit_pb.decode("ForfeitFrame", data)
                    if "hello" in frame:
                        with STATE.lock:
                            STATE.conn["server_last_seq"] = frame["hello"].get("last_sequence", 0)
                    elif "event" in frame:
                        frame["event"]["_frame_bytes"] = len(data)
                        via = "backfill" if frame["event"].get("backfill") else "live"
                        ev = pb_to_view(frame["event"])
                        if STATE.add(ev, via) and via == "live":
                            log("LIVE seq=%s %s %s g%s" % (ev.get("seq"), ev.get("game"),
                                                         ev.get("match_name"), ev.get("game_num")))
                        with STATE.lock:
                            sl = STATE.conn["server_last_seq"] or 0
                            STATE.conn["server_last_seq"] = max(sl, ev.get("seq") or 0)
        except Exception as e:
            log("ws error:", repr(e))
            with STATE.lock:
                if STATE.conn["connected"]:
                    STATE.conn["disconnects"] += 1
                STATE.conn.update(connected=False, last_error=repr(e)[:200], last_error_at=utcnow())
                STATE.disconnect_log.appendleft({"at": utcnow(), "error": repr(e)[:200]})
            try:
                s.close()
            except Exception:
                pass
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)


def health_loop():
    """轮询 forfeit-feed 的 /health（同样走公网域名），拿探针心跳与服务端计数。"""
    u = urllib.parse.urlparse(WS_URL)
    base = "%s://%s" % ("https" if u.scheme == "wss" else "http", u.netloc)
    url = base + "/health" + ("?token=" + urllib.parse.quote(FEED_TOKEN) if FEED_TOKEN else "")
    while True:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "forfeit-monitor/1"})
            with urllib.request.urlopen(req, timeout=10) as r:
                h = json.loads(r.read())
            with STATE.lock:
                STATE.health, STATE.health_at, STATE.health_error = h, utcnow(), None
        except Exception as e:
            with STATE.lock:
                STATE.health_error, STATE.health_at = repr(e)[:200], utcnow()
        time.sleep(HEALTH_S)


# ---------------------------------------------------------------- 看板

def snapshot():
    with STATE.lock:
        recs = sorted(STATE.events.values(), key=lambda r: r["seq"], reverse=True)
        conn = dict(STATE.conn)
        dlog = list(STATE.disconnect_log)
        health, health_at, health_err = STATE.health, STATE.health_at, STATE.health_error
    return {"now": utcnow(), "ws_url": WS_URL, "conn": conn, "disconnects": dlog,
            "health": health, "health_at": health_at, "health_error": health_err,
            "gaps": STATE.gaps(), "events": recs}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _authed(self, qs):
        if not DASH_TOKEN:
            return True
        if qs.get("token", [""])[0] == DASH_TOKEN:
            return True
        ck = self.headers.get("Cookie", "")
        return ("ffdash=" + DASH_TOKEN) in [c.strip() for c in ck.split(";")]

    def _send(self, code, body, ctype, extra=None):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        path = u.path.rstrip("/")
        if path.startswith("/dash"):
            path = path[5:]
        if not self._authed(qs):
            return self._send(401, "unauthorized", "text/plain; charset=utf-8")
        extra = {}
        if "token" in qs:   # 首次带 token 访问后种 cookie，之后刷新/轮询不用再带
            extra["Set-Cookie"] = "ffdash=%s; Path=/dash; Max-Age=31536000; HttpOnly; Secure; SameSite=Lax" % DASH_TOKEN
        if path in ("", "/"):
            return self._send(200, DASH_HTML.read_bytes(), "text/html; charset=utf-8", extra)
        if path == "/api/state":
            return self._send(200, json.dumps(snapshot(), ensure_ascii=False),
                              "application/json; charset=utf-8", extra)
        self._send(404, "not found", "text/plain; charset=utf-8")


def main():
    threading.Thread(target=ws_loop, daemon=True, name="ws").start()
    threading.Thread(target=health_loop, daemon=True, name="health").start()
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    log("monitor on %s:%d -> %s  dash_token=%s" % (HOST, PORT, WS_URL, "on" if DASH_TOKEN else "off"))
    srv.serve_forever()

if __name__ == "__main__":
    main()
