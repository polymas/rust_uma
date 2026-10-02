#!/usr/bin/env python3
"""
弃权（forfeit）实时信号源（WebSocket + HTTP 历史查询）

读 ps_probe.py 产出的 result_probe.csv，逐条挂上对应的 Polymarket 市场标识。
不下单，不写除自身目录外的任何位置。
零第三方依赖：WebSocket 按 RFC6455 手写。

  ws://HOST:PORT/ws            订阅实时 forfeit 事件（二进制帧，protobuf ForfeitFrame，见 forfeit.proto）
  ws://HOST:PORT/ws?backfill=N 连上先补推最近 N 条历史
  GET /forfeit.proto                   推送格式定义
  GET /history?limit=N&since=ISO8601   历史事件 JSON（人看/排查用）
  GET /health                          健康检查
"""
import base64, csv, hashlib, io, json, os, socket, struct, sys, threading, time
from pm_lookup import PM_LOOKUP
import forfeit_pb
from datetime import datetime, timezone
from pathlib import Path

CSV_PATH  = Path(os.environ.get("FORFEIT_FEED_CSV", "./result_probe.csv"))
STORE     = Path(os.environ.get("FORFEIT_FEED_STORE", "./forfeit_events.jsonl"))
HOST      = os.environ.get("FORFEIT_FEED_HOST", "127.0.0.1")
PORT      = int(os.environ.get("FORFEIT_FEED_PORT", "8813"))
TOKEN     = os.environ.get("FORFEIT_FEED_TOKEN", "").strip()
POLL_S    = float(os.environ.get("FORFEIT_FEED_POLL_S", "1.0"))
PROTO_PATH = Path(__file__).with_name("forfeit.proto")
PROBE_HB  = Path(os.environ.get("PROBE_HEARTBEAT", "./probe_heartbeat.json"))
PING_S    = 25.0
GUID      = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

HDR = ["source","detect_iso","detect_epoch","game","league","match_name","team_a","team_b",
       "game_num","winner","src_end_at","detect_lag_vs_end_s","length","forfeit","status",
       "draw","match_id","match_begin_at"]

def log(*a):
    print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), *a, flush=True)

def truthy(v):
    return str(v or "").strip().lower() in {"1", "true", "yes"}

def num(v, cast=float):
    try:
        s = str(v).strip()
        return cast(s) if s else None
    except Exception:
        return None

# ---------------------------------------------------------------- 事件构造

def build_event(row):
    """CSV 行 -> 对外事件。字段命名保持与 result_probe.csv 一致，额外加派生字段。"""
    gn        = (row.get("game_num") or "").strip()
    is_series = (gn == "series")
    winner    = (row.get("winner") or "").strip()
    a         = (row.get("team_a") or "").strip()
    b         = (row.get("team_b") or "").strip()
    loser     = (b if winner == a else a) if winner in (a, b) else ""
    return {
        "type": "forfeit",
        "source": row.get("source") or "pandascore",
        "detect_iso": row.get("detect_iso"),
        "detect_epoch": num(row.get("detect_epoch")),
        "game": row.get("game"),
        "league": row.get("league"),
        "match_name": row.get("match_name"),
        "team_a": a,
        "team_b": b,
        "game_num": gn,
        "is_series": is_series,
        "map_number": None if is_series else num(gn, int),
        "winner": winner,
        "loser": loser,
        "src_end_at": row.get("src_end_at"),
        "detect_lag_vs_end_s": num(row.get("detect_lag_vs_end_s")),
        "length_s": num(row.get("length"), int),
        "forfeit": True,
        "status": row.get("status"),
        "draw": truthy(row.get("draw")),
        "match_id": num(row.get("match_id"), int),
        "match_begin_at": row.get("match_begin_at"),
    }

def event_key(e):
    return (e.get("match_id"), e.get("game_num"), e.get("src_end_at"))

# ---------------------------------------------------------------- 事件仓库

class Store:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.events = []
        self.keys = set()
        self.seq = 0
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        e = json.loads(line)
                    except Exception:
                        continue
                    self.events.append(e)
                    self.keys.add(event_key(e))
                    self.seq = max(self.seq, int(e.get("seq") or 0))
            log("store loaded events=%d seq=%d" % (len(self.events), self.seq))

    def add(self, e):
        """返回带 seq 的事件；重复则返回 None。"""
        with self.lock:
            k = event_key(e)
            if k in self.keys:
                return None
            self.seq += 1
            e["seq"] = self.seq
            e["emitted_at"] = datetime.now(timezone.utc).isoformat()
            self.keys.add(k)
            self.events.append(e)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
            return e

    def recent(self, limit=None, since=None):
        with self.lock:
            out = list(self.events)
        if since:
            out = [e for e in out if (e.get("detect_iso") or "") >= since]
        if limit is not None:
            n = int(limit)
            out = out[-n:] if n else []
        return out

STOREDB = Store(STORE)


def enrich(e):
    """挂上「我们唯一匹配到的」PM 盘口信息。

    刻意不把 pm_match 写进 forfeit_events.jsonl，而是在推送/查询时现算 ——
    这样匹配索引变好之后，历史事件回看时也跟着变好。"""
    return dict(e, pm_match=PM_LOOKUP.lookup(e))

# ---------------------------------------------------------------- CSV 追尾

class Tailer(threading.Thread):
    daemon = True

    def __init__(self, on_event):
        super().__init__(name="tailer")
        self.on_event = on_event
        self.offset = 0
        self.inode = None

    def bootstrap(self):
        """首次启动：全量扫一遍历史 forfeit，补齐仓库，然后定位到文件末尾。"""
        if not CSV_PATH.exists():
            log("bootstrap: csv missing", CSV_PATH)
            return
        st = CSV_PATH.stat()
        self.inode = st.st_ino
        added = 0
        with CSV_PATH.open("r", encoding="utf-8", errors="replace", newline="") as f:
            for row in csv.DictReader(f):
                if truthy(row.get("forfeit")):
                    if STOREDB.add(build_event(row)):
                        added += 1
            self.offset = f.tell()
        self.offset = st.st_size
        log("bootstrap: scanned csv, new historical forfeits=%d total=%d offset=%d"
            % (added, len(STOREDB.events), self.offset))

    def run(self):
        self.bootstrap()
        buf = ""
        while True:
            try:
                if not CSV_PATH.exists():
                    time.sleep(POLL_S); continue
                st = CSV_PATH.stat()
                if st.st_ino != self.inode or st.st_size < self.offset:
                    log("csv rotated/truncated -> reset (ino %s->%s size %d<%d)"
                        % (self.inode, st.st_ino, st.st_size, self.offset))
                    self.inode, self.offset, buf = st.st_ino, 0, ""
                if st.st_size > self.offset:
                    with CSV_PATH.open("r", encoding="utf-8", errors="replace", newline="") as f:
                        f.seek(self.offset)
                        chunk = f.read()
                        self.offset = f.tell()
                    buf += chunk
                    # 只处理完整行，半行留到下一轮
                    if "\n" in buf:
                        complete, buf = buf.rsplit("\n", 1)
                        for row in csv.DictReader(io.StringIO(complete), fieldnames=HDR):
                            if row.get("source") == "source":
                                continue  # 表头
                            if truthy(row.get("forfeit")):
                                e = STOREDB.add(build_event(row))
                                if e:
                                    log("FORFEIT %s %s g%s %s len=%s" %
                                        (e["game"], e["match_name"], e["game_num"],
                                         e["winner"], e["length_s"]))
                                    self.on_event(e)
            except Exception as ex:
                log("tailer error:", repr(ex))
            time.sleep(POLL_S)

# ---------------------------------------------------------------- 排除名单补全

RETRY_S        = 120
RETRY_WINDOW_S = 24 * 3600
PUSHED_CIDS    = {}     # seq -> 最近一次推送出去的 condition_id 集合

def retry_loop():
    """推送时 Polymarket 查询失败 / 还没开盘 / 后来补开了子市场 —— 名单会变。
    近 24h 的事件每 2 分钟重查一次，名单比上次推送的多就用同一个 seq 再推一次；
    下游按 condition_id 取并集，重复推送是幂等的。"""
    while True:
        time.sleep(RETRY_S)
        try:
            now = time.time()
            for e in STOREDB.recent():
                if now - (e.get("detect_epoch") or 0) > RETRY_WINDOW_S:
                    continue
                ev = enrich(e)
                cids = set(ev["pm_match"].get("condition_ids") or [])
                if cids and not cids <= PUSHED_CIDS.get(e["seq"], set()):
                    log("名单更新，重推 seq=%s %s condition_ids=%d"
                        % (e["seq"], e.get("match_name"), len(cids)))
                    broadcast(e)
        except Exception as ex:
            log("retry error:", repr(ex))

# ---------------------------------------------------------------- WebSocket

def ws_frame(payload: bytes, opcode=0x1) -> bytes:
    h = bytearray([0x80 | opcode])
    n = len(payload)
    if n < 126:
        h.append(n)
    elif n < (1 << 16):
        h.append(126); h += struct.pack(">H", n)
    else:
        h.append(127); h += struct.pack(">Q", n)
    return bytes(h) + payload

def ws_read_frame(sock):
    """返回 (opcode, payload) 或 None。"""
    def recvn(n):
        out = b""
        while len(out) < n:
            c = sock.recv(n - len(out))
            if not c:
                return None
            out += c
        return out
    hdr = recvn(2)
    if not hdr:
        return None
    b0, b1 = hdr[0], hdr[1]
    opcode = b0 & 0x0F
    masked = b1 & 0x80
    n = b1 & 0x7F
    if n == 126:
        d = recvn(2)
        if not d: return None
        n = struct.unpack(">H", d)[0]
    elif n == 127:
        d = recvn(8)
        if not d: return None
        n = struct.unpack(">Q", d)[0]
    mask = recvn(4) if masked else None
    if masked and mask is None:
        return None
    data = recvn(n) if n else b""
    if data is None:
        return None
    if masked:
        data = bytes(c ^ mask[i % 4] for i, c in enumerate(data))
    return opcode, data

class Client:
    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.lock = threading.Lock()
        self.alive = True

    def send_pb(self, payload: bytes):
        """WebSocket 推送一律是二进制帧，内容为 forfeit.proto 的 ForfeitFrame。"""
        try:
            with self.lock:
                self.sock.sendall(ws_frame(payload, 0x2))
        except Exception:
            self.alive = False

    def ping(self):
        try:
            with self.lock:
                self.sock.sendall(ws_frame(b"", 0x9))
        except Exception:
            self.alive = False

    def close(self):
        self.alive = False
        try:
            self.sock.close()
        except Exception:
            pass

CLIENTS = []
CLIENTS_LOCK = threading.Lock()

def broadcast(event):
    with CLIENTS_LOCK:
        targets = list(CLIENTS)
    enriched = enrich(event)
    PUSHED_CIDS[event["seq"]] = set(enriched["pm_match"].get("condition_ids") or [])
    payload = forfeit_pb.event_to_pb(enriched, backfill=False)
    # 每个客户端单独线程发送，慢客户端不拖累其他人
    ts = [threading.Thread(target=c.send_pb, args=(payload,), daemon=True) for c in targets]
    for t in ts:
        t.start()
    for t in ts:
        t.join(PING_S * 3 + 5)
    dead = [c for c in targets if not c.alive]
    if dead:
        with CLIENTS_LOCK:
            for c in dead:
                if c in CLIENTS:
                    CLIENTS.remove(c)

# ---------------------------------------------------------------- HTTP/WS 路由

def parse_qs(q):
    out = {}
    for part in (q or "").split("&"):
        if not part:
            continue
        k, _, v = part.partition("=")
        out[k] = v
    return out

def http_reply(sock, code, body, ctype="application/json; charset=utf-8"):
    if isinstance(body, (dict, list)):
        body = json.dumps(body, ensure_ascii=False, indent=2).encode()
    elif isinstance(body, str):
        body = body.encode()
    reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found"}.get(code, "OK")
    hdr = ("HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n"
           "Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n"
           % (code, reason, ctype, len(body)))
    try:
        sock.sendall(hdr.encode() + body)
    finally:
        sock.close()

def probe_heartbeat():
    try:
        return json.loads(PROBE_HB.read_text())
    except Exception:
        return None

def handle_conn(sock, addr):
    sock.settimeout(20)
    try:
        raw = b""
        while b"\r\n\r\n" not in raw:
            c = sock.recv(4096)
            if not c:
                sock.close(); return
            raw += c
            if len(raw) > 65536:
                sock.close(); return
        head = raw.split(b"\r\n\r\n", 1)[0].decode("latin-1")
        lines = head.split("\r\n")
        try:
            method, target, _ = lines[0].split(" ", 2)
        except ValueError:
            sock.close(); return
        headers = {}
        for ln in lines[1:]:
            k, _, v = ln.partition(":")
            headers[k.strip().lower()] = v.strip()
        path, _, query = target.partition("?")
        qs = parse_qs(query)

        if TOKEN:
            supplied = qs.get("token") or headers.get("x-auth-token", "")
            if supplied != TOKEN:
                http_reply(sock, 401, {"error": "bad or missing token"}); return

        if headers.get("upgrade", "").lower() == "websocket":
            key = headers.get("sec-websocket-key", "")
            if not key:
                http_reply(sock, 400, {"error": "missing Sec-WebSocket-Key"}); return
            accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
            sock.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                          "Connection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n" % accept).encode())
            # 服务端每 PING_S 发 ping，正常客户端会回 pong；超过 3 个周期没任何帧即视为死连接。
            # 同时让 sendall 不会被卡住的客户端无限阻塞。
            sock.settimeout(PING_S * 3)
            serve_ws(sock, addr, qs)
            return

        if path in ("/health", "/healthz"):
            csv_mtime = None
            if CSV_PATH.exists():
                csv_mtime = datetime.fromtimestamp(CSV_PATH.stat().st_mtime, timezone.utc).isoformat()
            http_reply(sock, 200, {
                "ok": True, "service": "forfeit-feed",
                "utc": datetime.now(timezone.utc).isoformat(),
                "data_updated_utc": csv_mtime,          # 上游数据最后更新时刻
                "events_total": len(STOREDB.events),
                "last_seq": STOREDB.seq,
                "probe": probe_heartbeat(),
            }); return

        if path == "/forfeit.proto":
            http_reply(sock, 200, PROTO_PATH.read_bytes(), "text/plain; charset=utf-8"); return

        if path == "/history":
            lim = qs.get("limit")
            if lim is not None and not str(lim).isdigit():
                http_reply(sock, 400, {"error": "limit must be a non-negative integer"}); return
            http_reply(sock, 200, {
                "events": [enrich(e) for e in
                           STOREDB.recent(limit=lim, since=qs.get("since"))],
            }); return

        if path in ("/", "/schema"):
            http_reply(sock, 200, {
                "service": "pandascore forfeit feed",
                "websocket": "/ws  (可选 ?backfill=N)；二进制帧，protobuf polyuma.forfeit.v1.ForfeitFrame，见 /forfeit.proto",
                "endpoints": ["/health", "/history?limit=N&since=ISO8601", "/schema"],
                "event_fields": list(build_event({}).keys()) + ["seq", "emitted_at"],
                "pm_match_fields": ["matched", "status", "pm_event_slug", "pm_event_title",
                                    "pm_event_url", "condition_ids", "markets",
                                    "condition_id", "market_slug", "pm_question", "outcomes",
                                    "winner_token_id", "loser_token_id"],
            }); return

        http_reply(sock, 404, {"error": "not found"})   # 刻意不回显请求路径
    except Exception as ex:
        log("conn error %s: %r" % (addr, ex))
        try:
            sock.close()
        except Exception:
            pass

def serve_ws(sock, addr, qs):
    c = Client(sock, addr)
    with CLIENTS_LOCK:
        CLIENTS.append(c)
    log("ws connect %s clients=%d" % (addr[0] if addr else "?", len(CLIENTS)))
    c.send_pb(forfeit_pb.hello_to_pb(STOREDB.seq, len(STOREDB.events)))
    bf = qs.get("backfill")
    if bf:
        try:
            for e in STOREDB.recent(limit=int(bf)):
                c.send_pb(forfeit_pb.event_to_pb(enrich(e), backfill=True))
        except Exception as ex:
            log("backfill failed %s: %r" % (addr, ex))

    def pinger():
        while c.alive:
            time.sleep(PING_S)
            c.ping()
    threading.Thread(target=pinger, daemon=True).start()

    try:
        while c.alive:
            fr = ws_read_frame(sock)
            if fr is None:
                break
            op, data = fr
            if op == 0x8:
                break
            if op == 0x9:
                with c.lock:
                    sock.sendall(ws_frame(data, 0xA))
    except Exception:
        pass
    finally:
        c.close()
        with CLIENTS_LOCK:
            if c in CLIENTS:
                CLIENTS.remove(c)
        log("ws disconnect %s clients=%d" % (addr[0] if addr else "?", len(CLIENTS)))

# ---------------------------------------------------------------- main

def main():
    Tailer(broadcast).start()
    threading.Thread(target=retry_loop, daemon=True, name="retry").start()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(64)
    log("forfeit-feed listening on %s:%d  token=%s" % (HOST, PORT, "on" if TOKEN else "off"))
    while True:
        try:
            cs, addr = srv.accept()
            threading.Thread(target=handle_conn, args=(cs, addr), daemon=True).start()
        except Exception as ex:
            log("accept error:", repr(ex))
            time.sleep(0.5)

if __name__ == "__main__":
    main()
