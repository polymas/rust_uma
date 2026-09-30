#!/usr/bin/env python3
"""mempool pending 信号 vs rust-uma 确认信号：抢达与正确率，以链上成功的 ProposePrice 为真值。

两路订阅：
  - pending：跟随 p2p-mempool-probe 写的 ndjson（p2p_match 记录带完整 calldata），同机同时钟；
  - rust-uma：自己连 rust-uma 的 /uma/v1/ws（uma.pb.v1），收到帧就打本机时间戳。
真值：对两路出现过的交易、以及 P2P 收到的块里的 propose 类交易，等 ≥90s（约 60 个块）后查回执，
      第一笔成功的 ProposePrice 日志即该请求的真值（报价、所在块）。

同一"请求" = (requester, identifier, timestamp, ancillaryData)。
只用标准库（Python ≥3.14，用到 compression.zstd）。
"""

import argparse
import base64
import collections
import hashlib
import json
import os
import queue
import socket
import struct
import threading
import time
import urllib.parse
import urllib.request

from compression import zstd

RPCS = ["https://polygon-bor-rpc.publicnode.com", "https://polygon.drpc.org"]
TOPIC_PROPOSE = "0x6e51dd00371aabffa82cd401592f76ed51e98a9ea4b58751c70463a2c78b5ca1"
SEL_PROPOSE, SEL_PROPOSE_FOR, SEL_MULTI = "b8b4f908", "7c82288f", "ac9650d8"
E18 = 10**18
OUTCOME = {E18: 1, 0: 2, E18 // 2: 3}  # 与 wire 的 PriceOutcome 一致：TOKEN0 / TOKEN1 / TIE
TRUTH_DELAY_S = 90       # 等这么久再查回执（≈60 个块，足够最终确认）
FINALIZE_AFTER_S = 900   # 首个信号 15 分钟后还没真值就结算为"无真值"


def now_us():
    return time.time_ns() // 1000


# ---------------------------------------------------------------- ABI 解码

def words(b):
    return [b[i:i + 32] for i in range(0, len(b) - len(b) % 32, 32)]


def s256(w):
    v = int.from_bytes(w, "big")
    return v - 2**256 if v >= 2**255 else v


def req_key(requester, ident, ts, anc):
    return f"{requester}:{ident[:16]}:{ts}:{hashlib.sha256(anc).hexdigest()[:24]}"


def dec_propose(args, has_proposer=False):
    o = 1 if has_proposer else 0
    w = words(args)
    requester = w[0 + o][12:].hex()
    ident = w[1 + o].hex()
    ts = int.from_bytes(w[2 + o], "big")
    off = int.from_bytes(w[3 + o], "big")
    price = s256(w[4 + o])
    n = int.from_bytes(args[off:off + 32], "big")
    return req_key(requester, ident, ts, args[off + 32:off + 32 + n]), price


def proposals_from_input(inp):
    try:
        b = bytes.fromhex(inp[2:] if inp.startswith("0x") else inp)
    except ValueError:
        return None
    sel, args = b[:4].hex(), b[4:]
    try:
        if sel == SEL_PROPOSE:
            return [dec_propose(args)]
        if sel == SEL_PROPOSE_FOR:
            return [dec_propose(args, True)]
        if sel == SEL_MULTI:
            w = words(args)
            off = int.from_bytes(w[0], "big")
            n = int.from_bytes(args[off:off + 32], "big")
            out = []
            for i in range(n):
                eo = int.from_bytes(args[off + 32 + 32 * i:off + 64 + 32 * i], "big") + off + 32
                ln = int.from_bytes(args[eo:eo + 32], "big")
                call = args[eo + 32:eo + 32 + ln]
                if call[:4].hex() == SEL_PROPOSE:
                    out.append(dec_propose(call[4:]))
            return out or None
    except Exception:  # noqa: BLE001
        return None
    return None


def proposals_from_receipt(rc):
    out = []
    for lg in rc.get("logs", []):
        if not lg["topics"] or lg["topics"][0] != TOPIC_PROPOSE:
            continue
        data = bytes.fromhex(lg["data"][2:])
        w = words(data)
        off = int.from_bytes(w[2], "big")
        n = int.from_bytes(data[off:off + 32], "big")
        key = req_key(lg["topics"][1][-40:], w[0].hex(), int.from_bytes(w[1], "big"), data[off + 32:off + 32 + n])
        out.append((key, s256(w[3]), int(lg["logIndex"], 16), lg["topics"][2][-40:]))
    return out


# ---------------------------------------------------------------- RPC

def rpc(method, params):
    for u in RPCS:
        try:
            req = urllib.request.Request(u, json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                                         {"Content-Type": "application/json", "User-Agent": "curl/8"})
            r = json.load(urllib.request.urlopen(req, timeout=15))
            if "result" in r:
                return r["result"]
        except Exception:  # noqa: BLE001
            continue
    return "ERR"


# ---------------------------------------------------------------- rust-uma 订阅（极简 WebSocket 客户端）

def pb_fields(b):
    i = 0
    while i < len(b):
        key, s = 0, 0
        while True:
            c = b[i]; i += 1; key |= (c & 0x7F) << s; s += 7
            if c < 0x80:
                break
        num, wt = key >> 3, key & 7
        if wt == 0:
            v, s = 0, 0
            while True:
                c = b[i]; i += 1; v |= (c & 0x7F) << s; s += 7
                if c < 0x80:
                    break
        elif wt == 2:
            n, s = 0, 0
            while True:
                c = b[i]; i += 1; n |= (c & 0x7F) << s; s += 7
                if c < 0x80:
                    break
            v, i = b[i:i + n], i + n
        elif wt == 1:
            v, i = b[i:i + 8], i + 8
        elif wt == 5:
            v, i = b[i:i + 4], i + 4
        else:
            return
        yield num, v


def ws_loop(url, on_event):
    u = urllib.parse.urlparse(url)
    while True:
        try:
            sock = socket.create_connection((u.hostname, u.port or 80), timeout=30)
            key = base64.b64encode(os.urandom(16)).decode()
            sock.sendall((f"GET {u.path} HTTP/1.1\r\nHost: {u.hostname}:{u.port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                          f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: uma.pb.v1\r\n\r\n").encode())
            f = sock.makefile("rb")
            status = f.readline()
            if b" 101 " not in status:
                raise OSError(f"upgrade failed: {status!r}")
            while f.readline() not in (b"\r\n", b""):
                pass
            sock.settimeout(90)
            print("rust-uma subscribed", flush=True)
            buf = b""
            while True:
                h = f.read(2)
                if len(h) < 2:
                    raise OSError("closed")
                fin, op, ln = h[0] & 0x80, h[0] & 0x0F, h[1] & 0x7F
                if ln == 126:
                    ln = struct.unpack(">H", f.read(2))[0]
                elif ln == 127:
                    ln = struct.unpack(">Q", f.read(8))[0]
                payload = f.read(ln)
                recv = now_us()
                if op == 9:  # ping → pong（客户端帧必须加掩码）
                    mask = os.urandom(4)
                    sock.sendall(bytes([0x8A, 0x80 | len(payload)]) + mask + bytes(c ^ mask[i % 4] for i, c in enumerate(payload)))
                    continue
                if op == 8:
                    raise OSError("server closed")
                buf += payload
                if not fin:
                    continue
                frame, buf = buf, b""
                if frame[:4] != b"UMA1":
                    continue
                body = frame[12:]
                if frame[4] & 1:
                    body = zstd.decompress(body)
                for num, ev in pb_fields(body):
                    if num != 4:
                        continue
                    e = dict(pb_fields(ev))
                    if e.get(2) == 1 and 3 in e:  # 只要 ProposePrice
                        on_event("0x" + e[3].hex(), e.get(4, 0), e.get(12, 0), e.get(5, 0), recv)
        except Exception as ex:  # noqa: BLE001
            print("rust-uma ws:", repr(ex), flush=True)
            time.sleep(3)


# ---------------------------------------------------------------- 比对核心

class Race:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.req = {}            # key -> 进行中的请求
        self.tx_req = {}         # tx hash -> 待查回执的来源信息
        self.truth_q = queue.PriorityQueue()
        self.block_first = {}    # 块 hash -> P2P 首次收到时刻
        self.records = collections.deque(maxlen=200000)
        self.sender_stat = collections.defaultdict(lambda: [0, 0])  # sender -> [一致, 不一致]
        self.undecodable = 0
        self.tx_keys = {}        # pending tx -> 按调用序号排列的请求键（给 mempool-uma 事件反查）
        self.mu_wait = {}        # 还没对上 pending 记录的 mempool-uma 事件
        path = os.path.join(args.state_dir, "race_records.ndjson")
        self.rec_path = path
        if os.path.exists(path):
            cut = time.time() - 7 * 86400
            for line in open(path):
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r["t"] >= cut:
                    self.records.append(r)
                    if r.get("pending_sender") and r.get("truth_price") is not None and r.get("pending_price") is not None:
                        self.sender_stat[r["pending_sender"]][0 if r["pending_price"] == r["truth_price"] else 1] += 1
        if args.seed_senders and os.path.exists(args.seed_senders):
            # 冷启动：用 09-28 夜里的核对结果做各地址的先验（报价与真值一致 / 不一致次数）
            for snd, (a, d) in json.load(open(args.seed_senders)).items():
                st = self.sender_stat[snd]
                st[0] += a
                st[1] += d
        self.lookup_q = queue.Queue()
        for _ in range(4):
            threading.Thread(target=self.truth_worker, daemon=True).start()
        threading.Thread(target=self.lookup_worker, daemon=True).start()

    # --- 发送方打分（Beta 后验 95% 下限，正态近似）：给"只用可信地址"的过滤口径
    def trusted(self, sender):
        a, d = self.sender_stat.get(sender, (0, 0))
        n = a + d
        if n < self.args.trust_min_n:
            return False
        p = (a + 1) / (n + 2)
        lo = p - 1.645 * (p * (1 - p) / (n + 3)) ** 0.5
        return lo >= self.args.trust_min_rate

    def entry(self, key):
        e = self.req.get(key)
        if e is None:
            e = self.req[key] = {"key": key, "t0": time.time()}
        return e

    def want_receipt(self, tx, src):
        if tx not in self.tx_req:
            self.tx_req[tx] = {"src": src}
            self.truth_q.put((time.time() + TRUTH_DELAY_S, tx, 0))

    # --- 输入：pending（probe ndjson）
    def on_probe(self, r):
        k = r.get("kind")
        if k == "block" and r.get("first_us"):
            self.block_first.setdefault(r["hash"].lower(), r["first_us"])
        elif k in ("p2p_match", "incl"):
            inp = r.get("input")
            if k == "p2p_match":
                props = proposals_from_input("0x" + inp) if inp else None
                if not props:
                    if r.get("match", "").startswith("wrapper") or r.get("selector") in (SEL_PROPOSE, SEL_MULTI):
                        self.undecodable += 1
                else:
                    trusted = self.trusted(r["from"])
                    keys = []
                    self.tx_keys[r["hash"].lower()] = [k for k, _ in props]
                    for key, price in props:
                        e = self.entry(key)
                        keys.append(key)
                        e["out"] = e.get("out", 0) + 1
                        if "pending_us" not in e:
                            e.update(pending_us=r.get("first_us") or r.get("announce_us"), pending_price=price,
                                     pending_sender=r["from"], pending_tx=r["hash"], pending_trusted=trusted,
                                     pending_tip=r.get("tip_gwei"))
                        e.setdefault("pending_prices", set()).add(price)
                    self.want_receipt(r["hash"].lower(), k)
                    self.tx_req[r["hash"].lower()].setdefault("keys", []).extend(keys)
                    for item in self.mu_wait.pop(r["hash"].lower(), []):
                        self.apply_mu(r["hash"].lower(), *item)
                    return
            self.want_receipt(r["hash"].lower(), k)

    # --- 输入：rust-uma
    def on_uma(self, tx, log_index, outcome, market_id, recv_us):
        with self.lock:
            item = {"rust_us": recv_us, "log_index": log_index, "outcome": outcome, "market_id": market_id}
            if tx not in self.tx_req:
                self.want_receipt(tx, "uma")
            self.tx_req[tx].setdefault("uma", []).append(item)

    # --- 输入：mempool-uma 对外推送（下游实际收到的 pending 信号）
    def on_mu(self, tx, log_index, outcome, market_id, recv_us):
        with self.lock:
            if tx in self.tx_keys:
                self.apply_mu(tx, log_index, outcome, market_id, recv_us)
            else:
                self.mu_wait.setdefault(tx, []).append((log_index, outcome, market_id, recv_us))

    def apply_mu(self, tx, log_index, outcome, market_id, recv_us):
        keys = self.tx_keys.get(tx, [])
        if log_index < len(keys):
            e = self.entry(keys[log_index])
            if "mu_us" not in e:
                e.update(mu_us=recv_us, mu_outcome=outcome)

    # --- 真值：查回执
    def truth_worker(self):
        """多个线程共用一个队列：到点才查；查不到（ERR 或还没有回执）就 20 秒后重试，最多 30 次。"""
        while True:
            due, tx, *rest = self.truth_q.get()
            tries = rest[0] if rest else 0
            wait = due - time.time()
            if wait > 0:  # 最早的也没到点：放回去等一会
                self.truth_q.put((due, tx, tries))
                time.sleep(min(wait, 0.5))
                continue
            rc = rpc("eth_getTransactionReceipt", [tx])
            if (rc == "ERR" or rc is None) and tries < 30:
                self.truth_q.put((time.time() + 20, tx, tries + 1))
                continue
            with self.lock:
                self.apply_receipt(tx, rc if isinstance(rc, dict) else None)

    def lookup_worker(self):
        """兜底：结算前还没真值的请求，直接按 requester 扫最近的 ProposePrice 日志（真值可能早于本服务启动、
        或落在我们没收到块体的块里）。"""
        while True:
            key, t0 = self.lookup_q.get()
            found, ts_us = None, None
            try:
                head = rpc("eth_blockNumber", [])
                head = int(head, 16) if isinstance(head, str) and head.startswith("0x") else None
                requester = key.split(":")[0]
                if head:
                    span = int((time.time() - t0) / 1.5) + 1200  # 覆盖信号前约 30 分钟到现在
                    for lo in range(head - span, head + 1, 100):
                        logs = rpc("eth_getLogs", [{"fromBlock": hex(lo), "toBlock": hex(min(lo + 99, head)),
                                                    "topics": [TOPIC_PROPOSE, "0x" + "0" * 24 + requester]}])
                        if not isinstance(logs, list):
                            continue
                        for lg in logs:  # 逐条解析：logIndex 只在块内唯一，不能跨块用它反查
                            for k, price, li, proposer in proposals_from_receipt({"logs": [lg]}):
                                if k == key:
                                    bn = int(lg["blockNumber"], 16)
                                    if found is None or (bn, li) < found[1]:
                                        found = (price, (bn, li), lg["transactionHash"], proposer, lg["blockHash"].lower())
                        if found:
                            break
                if found:
                    blk = rpc("eth_getBlockByHash", [found[4], False])
                    ts_us = int(blk["timestamp"], 16) * 1_000_000 if isinstance(blk, dict) else None
            except Exception as ex:  # noqa: BLE001
                print("lookup:", repr(ex), flush=True)
            with self.lock:
                e = self.req.get(key)
                if e is None:
                    continue
                e["looked_up"] = True
                if found and ("truth_price" not in e or found[1] < e["truth_pos"]):
                    e.update(truth_price=found[0], truth_pos=found[1], truth_tx=found[2], truth_proposer=found[3],
                             truth_block_us=self.block_first.get(found[4]) or ts_us, truth_via="lookup")

    def apply_receipt(self, tx, rc):
        src = self.tx_req.pop(tx, None)
        for key in (src or {}).get("keys", []):
            e = self.req.get(key)
            if e is not None:
                e["out"] = max(0, e.get("out", 0) - 1)
        if not isinstance(rc, dict) or rc.get("status") != "0x1":
            return
        blk = rc["blockHash"].lower()
        bn = int(rc["blockNumber"], 16)
        props = proposals_from_receipt(rc)
        by_li = {li: (key, price) for key, price, li, _ in props}
        for key, price, li, proposer in props:
            e = self.entry(key)
            cand = (bn, li)
            if "truth_price" not in e or cand < e["truth_pos"]:
                e.update(truth_price=price, truth_pos=cand, truth_tx=tx, truth_block_us=self.block_first.get(blk),
                         truth_proposer=proposer)
        if isinstance(src, dict):
            for u in src.get("uma", []):
                if u["log_index"] in by_li:
                    key, price = by_li[u["log_index"]]
                    e = self.entry(key)
                    if "rust_us" not in e or u["rust_us"] < e["rust_us"]:
                        e.update(rust_us=u["rust_us"], rust_outcome=u["outcome"], market_id=u["market_id"])

    # --- 结算：有真值且已过查询窗口，或超时
    def settle(self):
        now = time.time()
        out = []
        with self.lock:
            for d in (self.tx_keys, self.mu_wait):  # 按插入顺序丢掉最老的一半，防止无限增长
                if len(d) > 50000:
                    for k in list(d)[:25000]:
                        del d[k]
            for key, e in list(self.req.items()):
                age = now - e["t0"]
                have_truth = "truth_price" in e
                if e.get("out", 0) > 0 and age < 3600:
                    continue  # 这个请求相关的 pending 交易回执还没查完
                if not ((have_truth and age > TRUTH_DELAY_S + 60) or age > FINALIZE_AFTER_S):
                    continue
                if not have_truth and "pending_us" in e and not e.get("looked_up"):
                    if not e.get("lookup_sent"):
                        e["lookup_sent"] = True
                        self.lookup_q.put((key, e["t0"]))
                    if age < 3600:
                        continue
                del self.req[key]
                if "pending_us" not in e and "rust_us" not in e and "mu_us" not in e:
                    continue  # 只在块里出现、两路都没信号的请求不统计
                r = {"t": e["t0"], "key": key, "market_id": e.get("market_id"),
                     "truth_price": e.get("truth_price"), "truth_block_us": e.get("truth_block_us"),
                     "pending_us": e.get("pending_us"), "pending_price": e.get("pending_price"),
                     "pending_sender": e.get("pending_sender"), "pending_trusted": e.get("pending_trusted"),
                     "pending_conflict": len(e.get("pending_prices", ())) > 1,
                     "pending_tx": e.get("pending_tx"), "truth_tx": e.get("truth_tx"),
                     # 迟到：真值所在块早于 pending 首见 1 秒以上（别人早就提案了，这条 pending 没有抢跑价值）
                     "pending_late": bool(e.get("pending_us") and e.get("truth_block_us")
                                          and e["truth_block_us"] < e["pending_us"] - 1_000_000),
                     "rust_us": e.get("rust_us"), "rust_outcome": e.get("rust_outcome"),
                     "mu_us": e.get("mu_us"), "mu_outcome": e.get("mu_outcome")}
                if r["pending_sender"] and r["truth_price"] is not None and r["pending_price"] is not None:
                    self.sender_stat[r["pending_sender"]][0 if r["pending_price"] == r["truth_price"] else 1] += 1
                self.records.append(r)
                out.append(r)
        if out:
            with open(self.rec_path, "a") as f:
                for r in out:
                    f.write(json.dumps(r) + "\n")

    # --- 汇总
    def stats(self, since):
        rs = [r for r in self.records if r["t"] >= since]

        def q(xs, ps=(0.1, 0.5, 0.9)):
            xs = sorted(xs)
            return {f"p{int(p*100)}": xs[min(len(xs) - 1, int(p * (len(xs) - 1) + .5))] for p in ps} if xs else {}

        def pend_block(sub):
            n = len(sub)
            ok = sum(1 for r in sub if r["truth_price"] is not None and r["pending_price"] == r["truth_price"])
            bad = sum(1 for r in sub if r["truth_price"] is not None and r["pending_price"] != r["truth_price"])
            none = sum(1 for r in sub if r["truth_price"] is None)
            late = sum(1 for r in sub if r.get("pending_late"))
            return {"n": n, "correct": ok, "wrong": bad, "no_truth": none, "late": late,
                    "correct_rate": ok / (ok + bad) if ok + bad else None,
                    "false_rate": (bad + none) / n if n else None}

        pend = [r for r in rs if r["pending_us"] is not None]
        trusted = [r for r in pend if r["pending_trusted"]]
        rust = [r for r in rs if r["rust_us"] is not None]
        rust_ok = sum(1 for r in rust if r["truth_price"] is not None and OUTCOME.get(r["truth_price"], 0) == r["rust_outcome"])
        rust_bad = sum(1 for r in rust if r["truth_price"] is not None and OUTCOME.get(r["truth_price"], 0) != r["rust_outcome"])
        both = [r for r in rs if r["pending_us"] is not None and r["rust_us"] is not None]
        lead = [(r["rust_us"] - r["pending_us"]) / 1000 for r in both]
        truth = [r for r in rs if r["truth_price"] is not None]
        mu = [r for r in rs if r.get("mu_us") is not None]
        mu_ok = sum(1 for r in mu if r["truth_price"] is not None and OUTCOME.get(r["truth_price"], 0) == r["mu_outcome"])
        mu_bad = sum(1 for r in mu if r["truth_price"] is not None and OUTCOME.get(r["truth_price"], 0) != r["mu_outcome"])
        mu_both = [r for r in mu if r["rust_us"] is not None]
        mu_lead = [(r["rust_us"] - r["mu_us"]) / 1000 for r in mu_both]
        vs_block = [(r["truth_block_us"] - r["pending_us"]) / 1000 for r in pend if r["truth_block_us"] and r["truth_price"] is not None]
        return {
            "requests": len(rs), "truth_requests": len(truth),
            "race": {"n": len(both), "pending_first": sum(1 for x in lead if x > 0) / len(lead) if lead else None,
                     "lead_ms": q(lead), "pending_before_block": sum(1 for x in vs_block if x > 0) / len(vs_block) if vs_block else None,
                     "pending_vs_block_ms": q(vs_block)},
            "pending_all": pend_block(pend), "pending_trusted": pend_block(trusted),
            "pending_conflict": sum(1 for r in pend if r["pending_conflict"]),
            "rust": {"n": len(rust), "correct": rust_ok, "wrong": rust_bad, "no_truth": len(rust) - rust_ok - rust_bad,
                     "correct_rate": rust_ok / (rust_ok + rust_bad) if rust_ok + rust_bad else None},
            "mu": {"n": len(mu), "correct": mu_ok, "wrong": mu_bad, "no_truth": len(mu) - mu_ok - mu_bad,
                   "late": sum(1 for r in mu if r.get("pending_late")),
                   "correct_rate": mu_ok / (mu_ok + mu_bad) if mu_ok + mu_bad else None,
                   "first": sum(1 for x in mu_lead if x > 0) / len(mu_lead) if mu_lead else None, "lead_ms": q(mu_lead),
                   "coverage": len([r for r in truth if r.get("mu_us") is not None]) / len(truth) if truth else None},
            "coverage": {"pending": sum(1 for r in truth if r["pending_us"] is not None) / len(truth) if truth else None,
                         "pending_trusted": sum(1 for r in truth if r["pending_trusted"]) / len(truth) if truth else None,
                         "rust": sum(1 for r in truth if r["rust_us"] is not None) / len(truth) if truth else None},
        }

    def snapshot(self):
        now = time.time()
        buckets = collections.defaultdict(lambda: [0, 0, 0, 0, 0])  # n, 两边都有, pending 先到, 有真值的 pending, 其中正确
        for r in self.records:
            h = int((now - r["t"]) // 3600)
            if h >= 48:
                continue
            bk = buckets[h]
            bk[0] += 1
            if r["pending_us"] and r["rust_us"]:
                bk[1] += 1
                bk[2] += r["rust_us"] > r["pending_us"]
            if r["pending_us"] and r["truth_price"] is not None:
                bk[3] += 1
                bk[4] += r["pending_price"] == r["truth_price"]
        hourly = [{"t": now - h * 3600, "n": b[0], "pending_first": b[2] / b[1] if b[1] else None,
                   "pending_correct": b[4] / b[3] if b[3] else None}
                  for h, b in sorted(((h, buckets[h]) for h in range(48)), reverse=True)]
        bad = [r for r in list(self.records)[-5000:] if r["pending_us"] is not None and
               (r["truth_price"] is None or r["pending_price"] != r["truth_price"] or r.get("pending_late"))][-30:][::-1]
        trusted_n = sum(1 for s in self.sender_stat if self.trusted(s))
        return {"t": now, "h1": self.stats(now - 3600), "h24": self.stats(now - 86400), "all": self.stats(0),
                "hourly": hourly, "bad": bad, "undecodable": self.undecodable, "open": len(self.req),
                "trusted_senders": trusted_n, "senders": len(self.sender_stat),
                "trust_rule": {"min_n": self.args.trust_min_n, "min_rate": self.args.trust_min_rate}}


def tail(path, on_line, start_at_end):
    f, ino, off = None, None, None
    while True:
        try:
            st = os.stat(path)
            if f is None or st.st_ino != ino or st.st_size < off:
                if f:
                    f.close()
                f, ino = open(path), st.st_ino
                off = st.st_size if (start_at_end and off is None) else 0
                f.seek(off)
            line = f.readline()
            if line.endswith("\n"):
                off = f.tell()
                try:
                    on_line(json.loads(line))
                except (ValueError, KeyError):
                    pass
                continue
            if line:
                f.seek(off)
        except FileNotFoundError:
            pass
        time.sleep(0.3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", default="/var/lib/p2p-mempool-probe-b/probe.ndjson")
    ap.add_argument("--uma", default="ws://172.28.0.11:8011/uma/v1/ws")
    ap.add_argument("--state-dir", default="/var/lib/p2p-race")
    ap.add_argument("--web", default="0.0.0.0:8091")
    ap.add_argument("--token-file", default="/var/lib/p2p-bayes/web_token")
    ap.add_argument("--trust-min-n", type=int, default=50)
    ap.add_argument("--trust-min-rate", type=float, default=0.99)
    ap.add_argument("--seed-senders", default="", help="地址先验（{地址: [一致, 不一致]}）")
    ap.add_argument("--mempool-uma", default="", help="mempool-uma 的 WSS（对比下游实际收到的 pending 信号）")
    args = ap.parse_args()
    os.makedirs(args.state_dir, exist_ok=True)
    race = Race(args)

    def on_probe(r):
        with race.lock:
            race.on_probe(r)
    threading.Thread(target=tail, args=(args.probe, on_probe, True), daemon=True).start()
    threading.Thread(target=ws_loop, args=(args.uma, race.on_uma), daemon=True).start()
    if args.mempool_uma:
        threading.Thread(target=ws_loop, args=(args.mempool_uma, race.on_mu), daemon=True).start()

    import http.server
    token = open(args.token_file).read().strip() if os.path.exists(args.token_file) else ""
    page = os.path.join(os.path.dirname(os.path.abspath(__file__)), "race.html")
    snap = {"b": b"{}"}

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code, body, ctype):
            import gzip
            gz = "gzip" in (self.headers.get("Accept-Encoding") or "") and len(body) > 1024
            if gz:
                body = gzip.compress(body, 6)
            self.send_response(code)
            if gz:
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            if token and urllib.parse.parse_qs(u.query).get("token", [""])[0] != token:
                return self.send(401, "需要 ?token=".encode(), "text/plain; charset=utf-8")
            if u.path == "/":
                return self.send(200, open(page, "rb").read(), "text/html; charset=utf-8")
            if u.path == "/api/race":
                return self.send(200, snap["b"], "application/json; charset=utf-8")
            self.send(404, b"not found", "text/plain")

    host, port = args.web.rsplit(":", 1)
    threading.Thread(target=http.server.ThreadingHTTPServer((host, int(port)), H).serve_forever, daemon=True).start()
    print("race compare up on", args.web, flush=True)
    while True:
        time.sleep(15)
        race.settle()
        with race.lock:
            s = race.snapshot()
        snap["b"] = json.dumps(s, default=list).encode()


if __name__ == "__main__":
    main()
