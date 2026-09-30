#!/usr/bin/env python3
"""增量贝叶斯网络：边读 p2p-mempool-probe 的 ndjson 事件流边更新后验。

每来一条完整观测就更新一次，不攒批。两张网络：

块网络（每个新块一条观测）
    A 出块者 → G 首个公告该块的 peer 所在地区、N 该 peer 所属网络、K 该 peer

交易网络（每笔 propose 类交易终态确定后一条观测）
    C 市场类别、S 提交地址、R 路由合约、V 是否进公开 mempool、T 小费档位、
    K 最先送到它的 peer、G 该 peer 地区、A 打包它的出块者、O 结果、L 相对 rust-uma 的领先档位

每个条件概率表 P(child | parents) 是开放词表的层级 Dirichlet：新出现的地址 / peer /
地区自动成为新状态（先验质量 alpha 分给未见值），行内再以子变量边缘分布为基底回退。
计数带指数遗忘（按事件数计的半衰期），网络会随时间漂移而改变。

结构随数据变化：每条候选边 child←parents 都维护一个前序（prequential）对数贝叶斯因子
    log10 BF = Σ_t log10 p(x_t | pa_t, D_<t) − log10 p(x_t | D_<t)
在观测到来的那一刻用"更新前"的预测分布打分，所以它就是两种模型边际似然之比的增量形式。
BF > 10^2 视为边成立（偏好显著），< 10^-1 视为无关。

只用标准库，便于直接丢到服务器上跑。
"""

import argparse
import collections
import json
import math
import os
import queue
import random
import threading
import time
import glob
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from triangulate import Triangulator  # noqa: E402

CATEGORY = {0: "未指定", 1: "体育", 2: "电竞", 3: "政治", 4: "加密", 5: "文化", 6: "天气", 7: "其他", 8: "提及"}
RPCS = ["https://polygon-bor-rpc.publicnode.com", "https://polygon.drpc.org"]
UA = {"User-Agent": "curl/8", "Content-Type": "application/json"}


# ---------------------------------------------------------------- 贝叶斯部件


class Dir:
    """开放词表对称 Dirichlet，带惰性指数遗忘。未见值的先验质量为 alpha/vocab。"""

    __slots__ = ("c", "n", "t", "alpha", "vocab")

    def __init__(self, alpha, vocab=2000):
        self.c, self.n, self.t, self.alpha, self.vocab = {}, 0.0, 0, alpha, vocab

    def _decay(self, t, lam):
        if t > self.t and lam < 1.0 and self.n > 0:
            f = lam ** (t - self.t)
            for k in self.c:
                self.c[k] *= f
            self.n *= f
        self.t = t

    def pred(self, x, base=None):
        b = base(x) if base else 1.0 / self.vocab
        return (self.c.get(x, 0.0) + self.alpha * b) / (self.n + self.alpha)

    def update(self, x, t, lam):
        self._decay(t, lam)
        self.c[x] = self.c.get(x, 0.0) + 1.0
        self.n += 1.0

    def top(self, k, base=None):
        return sorted(self.c, key=lambda x: -self.pred(x, base))[:k]

    def beta_params(self, x, base=None):
        b = base(x) if base else 1.0 / self.vocab
        a = self.c.get(x, 0.0) + self.alpha * b
        return a, max(self.n + self.alpha - a, 1e-9)

    def dump(self):
        return {"c": self.c, "n": self.n, "t": self.t}

    def load(self, d):
        self.c, self.n, self.t = dict(d["c"]), d["n"], d["t"]


def beta_ci(a, b, draws=1500):
    xs = sorted(random.betavariate(a, b) for _ in range(draws))
    return xs[int(0.05 * draws)], xs[int(0.95 * draws)]


def p_greater(a, b, thr, draws=1500):
    return sum(random.betavariate(a, b) > thr for _ in range(draws)) / draws


class Node:
    """child ← parents 的一条候选边：条件表 + 增量对数贝叶斯因子。"""

    def __init__(self, child, parents, alpha):
        self.child, self.parents, self.alpha = child, tuple(parents), alpha
        self.rows = {}
        self.logbf = 0.0
        self.n = 0

    @property
    def name(self):
        return f"{self.child}|{','.join(self.parents)}"

    def key(self, obs):
        vals = tuple(obs.get(p) for p in self.parents)
        return None if any(v is None for v in vals) else vals

    def score_and_update(self, obs, marg, t, lam):
        x = obs.get(self.child)
        pa = self.key(obs)
        if x is None or pa is None:
            return
        row = self.rows.get(pa)
        if row is None:
            row = self.rows[pa] = Dir(self.alpha)
        p0 = marg.pred(x)
        p1 = row.pred(x, marg.pred)
        # 贝叶斯因子同样遗忘，跟随结构漂移
        self.logbf = self.logbf * lam + math.log10(p1 / p0)
        row.update(x, t, lam)
        self.n += 1

    def mi_bits(self, marg):
        """后验均值下的互信息 I(child; parents)，衡量偏好强度。"""
        tot = sum(r.n for r in self.rows.values())
        if tot <= 0:
            return 0.0
        mi = 0.0
        for r in self.rows.values():
            w = r.n / tot
            for x in r.c:
                p = r.pred(x, marg.pred)
                q = marg.pred(x)
                if p > 0 and q > 0:
                    mi += w * p * math.log2(p / q)
        return mi

    def dump(self):
        return {"logbf": self.logbf, "n": self.n, "rows": [[list(k), r.dump()] for k, r in self.rows.items()]}

    def load(self, d):
        self.logbf, self.n = d["logbf"], d["n"]
        for k, rd in d["rows"]:
            r = Dir(self.alpha)
            r.load(rd)
            self.rows[tuple(k)] = r


class Net:
    def __init__(self, name, variables, edges, half_life, alpha_marg=1.0, alpha_row=2.0):
        self.name = name
        self.lam = 0.5 ** (1.0 / half_life)
        self.marg = {v: Dir(alpha_marg) for v in variables}
        self.nodes = [Node(c, ps, alpha_row) for c, ps in edges]
        self.t = 0

    def observe(self, obs):
        self.t += 1
        for node in self.nodes:  # 先用更新前的预测分布打分（前序），再更新
            node.score_and_update(obs, self.marg[node.child], self.t, self.lam)
        for v, d in self.marg.items():
            if obs.get(v) is not None:
                d.update(obs[v], self.t, self.lam)

    def node(self, name):
        return next(n for n in self.nodes if n.name == name)

    def dump(self):
        return {"t": self.t, "marg": {k: d.dump() for k, d in self.marg.items()},
                "nodes": {n.name: n.dump() for n in self.nodes}}

    def load(self, d):
        self.t = d["t"]
        for k, md in d["marg"].items():
            if k in self.marg:
                self.marg[k].load(md)
        for n in self.nodes:
            if n.name in d["nodes"]:
                n.load(d["nodes"][n.name])


BLOCK_NET = dict(
    variables=["A", "G", "N", "K"],
    edges=[("G", ["A"]), ("N", ["A"]), ("K", ["A"])],
)
TX_NET = dict(
    variables=["C", "S", "R", "V", "T", "K", "G", "A", "O", "L"],
    edges=[
        ("S", ["C"]),        # 某类事件由谁提交
        ("R", ["S"]),        # 提交地址走哪个路由合约
        ("V", ["S"]),        # 提交地址是否走私有通道
        ("V", ["C"]),        # 某类事件是否更常走私有通道
        ("T", ["S"]),        # 小费策略
        ("K", ["S"]),        # 提交地址的交易最先从哪个 peer 冒出来（≈ 它的广播入口）
        ("K", ["S", "C"]),   # 某地址的某类事件最先往哪个节点提交
        ("G", ["S"]),        # 提交入口的地区
        ("A", ["S"]),        # 提交地址是否偏好某个出块者（私有订单流）
        ("A", ["V"]),        # 私有交易是否集中在某个出块者
        ("O", ["S"]),        # 谁更常抢跑失败
        ("O", ["V"]),
        ("L", ["K"]),        # 哪个 peer 带来的领先更多
    ],
)


# ---------------------------------------------------------------- 外部查询（后台线程，不阻塞事件处理）


def http_json(url, body=None, timeout=10):
    req = urllib.request.Request(url, json.dumps(body).encode() if body else None, UA)
    return json.load(urllib.request.urlopen(req, timeout=timeout))


def rpc(method, params):
    for u in RPCS:
        try:
            r = http_json(u, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            if "result" in r:
                return r["result"]
        except Exception:  # noqa: BLE001
            continue
    return None


class Enricher:
    def __init__(self, cache_path):
        self.cache_path = cache_path
        self.geo, self.status, self.validators = {}, {}, {}
        if os.path.exists(cache_path):
            d = json.load(open(cache_path))
            self.geo, self.validators = d.get("geo", {}), d.get("validators", {})
        self.q_geo, self.q_status = queue.Queue(), queue.Queue()
        self.pending_geo = set()
        threading.Thread(target=self._geo_loop, daemon=True).start()
        threading.Thread(target=self._status_loop, daemon=True).start()
        threading.Thread(target=self._validator_loop, daemon=True).start()

    def want_geo(self, ip):
        if ip and ip not in self.geo and ip not in self.pending_geo:
            self.pending_geo.add(ip)
            self.q_geo.put(ip)

    def want_status(self, h):
        if h not in self.status:
            self.status[h] = None
            self.q_status.put(h)

    def _geo_loop(self):
        while True:
            ip = self.q_geo.get()
            try:
                g = http_json(f"http://ip-api.com/json/{ip}?fields=status,countryCode,country,city,org,as")
                if g.get("status") == "success":
                    self.geo[ip] = {"cc": g["countryCode"], "city": g.get("city", ""),
                                    "org": (g.get("org") or g.get("as") or "?")[:40]}
                else:
                    self.geo[ip] = {"cc": "?", "city": "", "org": "?"}
            except Exception:  # noqa: BLE001
                self.pending_geo.discard(ip)
                time.sleep(5)
                self.q_geo.put(ip)
                continue
            time.sleep(1.4)  # ip-api 免费档 45 次/分钟

    def _status_loop(self):
        while True:
            h = self.q_status.get()
            rc = rpc("eth_getTransactionReceipt", [h])
            if rc is None:
                time.sleep(2)
                self.q_status.put(h)
                continue
            self.status[h] = "success" if rc.get("status") == "0x1" else "reverted"

    def _validator_loop(self):
        while True:
            names = {}
            for off in range(0, 1000, 50):
                try:
                    r = http_json(f"https://staking-api.polygon.technology/api/v2/validators?limit=50&offset={off}", timeout=20)
                except Exception:  # noqa: BLE001
                    break
                res = r.get("result") or []
                if not res:
                    break
                for v in res:
                    names[v["signer"].lower()] = f"#{v['id']} {v['name'].strip()}"
                time.sleep(0.5)
            if names:
                self.validators = names
            time.sleep(3600)

    def save(self):
        json.dump({"geo": self.geo, "validators": self.validators}, open(self.cache_path + ".tmp", "w"))
        os.replace(self.cache_path + ".tmp", self.cache_path)


# ---------------------------------------------------------------- 事件拼接


def bucket_tip(g):
    if g is None:
        return None
    for lim, name in ((50, "<50"), (200, "50-200"), (1000, "200-1k"), (5000, "1k-5k")):
        if g < lim:
            return name
    return ">=5k"


def bucket_lead(ms):
    for lim, name in ((0, "落后"), (500, "0-0.5s"), (1000, "0.5-1s"), (1500, "1-1.5s"), (2000, "1.5-2s")):
        if ms < lim:
            return name
    return ">=2s"


class Monitor:
    def __init__(self, args):
        self.args = args
        self.enr = Enricher(os.path.join(args.state_dir, "enrich_cache.json"))
        self.block = Net("block", half_life=args.block_half_life, **BLOCK_NET)
        self.tx = Net("tx", half_life=args.tx_half_life, **TX_NET)
        self.peers = {}                 # peer id -> ip
        self.block_author = {}          # number -> author
        self.pending_blocks = collections.deque()
        self.txs = {}                   # hash -> 拼接中的记录
        self.state_path = os.path.join(args.state_dir, "bayes_state.json")
        self.offset = 0
        self.stats = collections.Counter()
        self.web = None
        self.decisions = collections.deque(maxlen=200)
        self.tri = Triangulator(self.peer_geo)
        self.obs_dir = os.path.join(args.state_dir, "obs")
        os.makedirs(self.obs_dir, exist_ok=True)
        self.tails = {}  # path -> [file, offset, observer]
        offsets = {}
        if os.path.exists(self.state_path):
            d = json.load(open(self.state_path))
            self.block.load(d["block"])
            self.tx.load(d["tx"])
            self.offset = d.get("offset", 0) if d.get("file") == args.input else 0
            self.peers = d.get("peers", {})
            self.stats.update(d.get("stats", {}))
            if "tri" in d:
                self.tri.load(d["tri"])
            offsets = d.get("obs_offsets", {})
        self.obs_offsets = offsets

    # --- 变量取值
    def peer_geo(self, peer):
        ip = self.peers.get(peer)
        return self.enr.geo.get(ip) if ip else None

    def region(self, g):
        return f"{g['cc']}/{g['city']}" if g else None

    def label_author(self, a):
        return a

    # --- 事件
    def on_remote(self, r, obs):
        """其他观测点推来的事件：只进三角定位，不进本地两张网络。"""
        if r.get("kind") == "peer":
            ip = r["addr"].rsplit(":", 1)[0]
            self.peers[r["id"]] = ip
            self.enr.want_geo(ip)
        self.tri.on_record(r, obs)

    def on_record(self, r):
        self.tri.on_record(r, self.args.local_observer)
        k = r.get("kind")
        if k in ("peer_decision", "explore_report"):
            self.decisions.append(r)
            return
        if k == "peer":
            ip = r["addr"].rsplit(":", 1)[0]
            self.peers[r["id"]] = ip
            self.enr.want_geo(ip)
        elif k == "block":
            if r.get("author"):
                self.block_author[r["number"]] = r["author"]
            self.pending_blocks.append(r)
        elif k == "p2p_match":
            t = self.txs.setdefault(r["hash"], {"seen_at": time.time()})
            t.update(public=True, S=r["from"], R=r["match"], tip=r.get("tip_gwei"),
                     K=r.get("peer"), first_us=r.get("first_us") or r.get("announce_us"))
        elif k == "incl":
            t = self.txs.setdefault(r["hash"], {"seen_at": time.time()})
            t.update(incl=True, number=r["number"], S=r["from"], R=r["match"])
            t.setdefault("tip", r.get("tip_gwei"))
            if not r.get("in_mempool"):
                t["public"] = False
            self.enr.want_status(r["hash"])
        elif k == "uma":
            if r["hash"] in self.txs or True:
                t = self.txs.setdefault(r["hash"], {"seen_at": time.time()})
                t.update(uma=True, C=CATEGORY.get(r.get("category"), str(r.get("category"))), uma_recv=r["uma_recv_us"])

    def drain_blocks(self):
        """块观测要等首个 peer 的地理信息；到了就按到达顺序喂给网络。"""
        while self.pending_blocks:
            r = self.pending_blocks[0]
            g = self.peer_geo(r.get("first_peer"))
            waited = time.time() - r.setdefault("_t", time.time())
            if g is None and waited < 120:
                ip = self.peers.get(r.get("first_peer"))
                if ip:
                    self.enr.want_geo(ip)
                break
            self.pending_blocks.popleft()
            if not r.get("author"):
                continue
            self.block.observe({"A": r["author"], "G": self.region(g), "N": g["org"] if g else None,
                                "K": r.get("first_peer")})
            self.stats["blocks"] += 1

    def drain_txs(self):
        now = time.time()
        for h, t in list(self.txs.items()):
            age = now - t["seen_at"]
            done = False
            if t.get("incl"):
                st = self.enr.status.get(h)
                author = self.block_author.get(t["number"])
                if st is not None and (author or age > 300) and (t.get("uma") or st != "success" or age > 90):
                    t["O"], t["A"], done = st, author, True
            elif age > 600:
                if t.get("public"):
                    t["O"], t["A"], done = "未上链", None, True
                else:
                    del self.txs[h]  # 只有 rust-uma 事件、没进我们视野的块：丢弃
                    continue
            if not done:
                continue
            del self.txs[h]
            g = self.peer_geo(t.get("K")) if t.get("public") else None
            lead = None
            if t.get("public") and t.get("uma") and t.get("first_us"):
                lead = bucket_lead((t["uma_recv"] - t["first_us"]) / 1000)
            c = t.get("C")
            if c is None and t["O"] == "success":
                c = "未富化"
            obs = {"C": c, "S": t.get("S"), "R": t.get("R"), "V": "公开" if t.get("public") else "私有",
                   "T": bucket_tip(t.get("tip")), "K": t.get("K") if t.get("public") else None,
                   "G": self.region(g), "A": t.get("A"), "O": {"success": "成功", "reverted": "回滚"}.get(t["O"], t["O"]),
                   "L": lead}
            self.tx.observe(obs)
            self.stats["txs"] += 1

    # --- 持久化 + 报告
    def save(self):
        d = {"file": self.args.input, "offset": self.offset, "block": self.block.dump(), "tx": self.tx.dump(),
             "peers": self.peers, "stats": dict(self.stats), "saved_at": time.time(),
             "tri": self.tri.dump(), "obs_offsets": {p: t[1] for p, t in self.tails.items()}}
        json.dump(d, open(self.state_path + ".tmp", "w"))
        os.replace(self.state_path + ".tmp", self.state_path)
        self.enr.save()

    def read_remote(self, budget=20000):
        for path in glob.glob(os.path.join(self.obs_dir, "*.ndjson")):
            if path not in self.tails:
                obs = os.path.basename(path)[:-7]
                f = open(path)
                off = self.obs_offsets.get(path, 0)
                if os.path.getsize(path) < off:
                    off = 0
                f.seek(off)
                self.tails[path] = [f, off, obs]
            t = self.tails[path]
            n = 0
            while n < budget:
                line = t[0].readline()
                if not line or not line.endswith("\n"):
                    if line:
                        t[0].seek(t[1])
                    break
                t[1] = t[0].tell()
                n += 1
                try:
                    self.on_remote(json.loads(line), t[2])
                except (ValueError, KeyError):
                    pass

    def run(self):
        last_save = last_report = 0
        f = None
        while True:
            if f is None:
                try:
                    f = open(self.args.input)
                    if os.path.getsize(self.args.input) < self.offset:
                        self.offset = 0  # 文件被换掉了
                    f.seek(self.offset)
                except FileNotFoundError:
                    time.sleep(2)
                    continue
            # 本地文件一次最多读 2000 行，然后必须轮一次其他观测点和定时任务，免得本地流量大时远端被饿死
            got = 0
            while got < 2000:
                line = f.readline()
                if not line or not line.endswith("\n"):
                    if line:
                        f.seek(self.offset)
                    break
                self.offset = f.tell()
                got += 1
                try:
                    self.on_record(json.loads(line))
                except (ValueError, KeyError):
                    pass
            self.read_remote()
            self.drain_blocks()
            self.drain_txs()
            self.tri.drain()
            now = time.time()
            if now - last_save > 60:
                self.save()
                last_save = now
            if self.web and now - getattr(self, "_last_snap", 0) > self.args.snapshot_every:
                try:
                    self.web.refresh()
                except Exception as e:  # noqa: BLE001
                    print("snapshot:", repr(e), flush=True)
                self._last_snap = now
            if now - last_report > self.args.report_every:
                write_report(self, self.args.report)
                last_report = now
            if os.path.exists(self.args.input) and os.path.getsize(self.args.input) < self.offset:
                f.close()
                f, self.offset = None, 0
            if got == 0:
                time.sleep(0.5)


# ---------------------------------------------------------------- 报告

MEANING = {"S|C": "某类事件由谁提交", "R|S": "地址偏好的路由合约", "V|S": "地址是否走私有通道",
           "V|C": "某类事件是否更常走私有", "T|S": "地址的小费策略", "K|S": "地址的交易最先从哪个节点冒出",
           "K|S,C": "某地址的某类事件最先往哪个节点", "G|S": "地址的提交入口地区", "A|S": "地址偏好的出块者",
           "A|V": "私有交易是否集中到某出块者", "O|S": "谁更常抢跑失败", "O|V": "公开/私有的成败差异",
           "L|K": "哪个节点带来更多领先", "G|A": "出块者出口地区", "N|A": "出块者出口网络", "K|A": "出块者出口节点"}


def verdict(bf):
    return "成立" if bf > 2 else ("倾向成立" if bf > 0.5 else ("无关" if bf < -1 else "证据不足"))


def fmt_p(p):
    return f"{p*100:.1f}%"


def write_report(m, path):
    vn = m.enr.validators
    lines = [f"# Polygon 出块与 propose 提交偏好（增量贝叶斯）", "",
             f"更新时间 {time.strftime('%Y-%m-%d %H:%M:%S')}；块观测 {m.block.t}，交易观测 {m.tx.t}"
             f"（遗忘半衰期：块 {m.args.block_half_life}、交易 {m.args.tx_half_life} 条观测）", ""]

    def share_table(net, var, title, label=lambda x: x, k=12):
        d = net.marg[var]
        lines.extend([f"## {title}", "", "| 取值 | 后验均值 | 90% 可信区间 |", "|---|---|---|"])
        for x in d.top(k):
            a, b = d.beta_params(x)
            lo, hi = beta_ci(a, b)
            lines.append(f"| {label(x)} | {fmt_p(d.pred(x))} | {fmt_p(lo)} – {fmt_p(hi)} |")
        lines.append("")

    def who(a):
        return f"{vn.get(a, '?')} `{a[:10]}`" if a else "?"

    # 1. 出块地区：块首个公告 peer 的地区是出块者出口位置的代理量
    share_table(m.block, "G", "1. 出块集中的地区（新块最先从哪个地区的节点传来）")
    by_cc = collections.Counter()
    d = m.block.marg["G"]
    for x in d.c:
        by_cc[x.split("/")[0]] += d.pred(x)
    lines.extend(["按国家合计：" + "，".join(f"{cc} {fmt_p(p)}" for cc, p in by_cc.most_common(8)), ""])

    # 2. 出块者 / 集群
    share_table(m.block, "A", "2. 出块者份额", label=who)
    lines.extend(["### 出块者的出口位置与所属网络（P(地区|出块者)、P(网络|出块者) 的最大后验）", "",
                  "| 出块者 | 块数(遗忘后) | 最可能地区 | 概率 | 最可能网络 | 概率 |", "|---|---|---|---|---|---|"])
    ng, nn = m.block.node("G|A"), m.block.node("N|A")
    clusters = collections.defaultdict(list)
    for a in m.block.marg["A"].top(20):
        rg, rn = ng.rows.get((a,)), nn.rows.get((a,))
        if not rg or not rn:
            continue
        g_top = rg.top(1, m.block.marg["G"].pred)[0]
        n_top = rn.top(1, m.block.marg["N"].pred)[0]
        pg, pn = rg.pred(g_top, m.block.marg["G"].pred), rn.pred(n_top, m.block.marg["N"].pred)
        lines.append(f"| {who(a)} | {rg.n:.0f} | {g_top} | {fmt_p(pg)} | {n_top} | {fmt_p(pn)} |")
        clusters[n_top].append(a)
    lines.extend(["", "按出口网络聚成的集群（同一网络最先送出其块的出块者）：", ""])
    for net_name, members in sorted(clusters.items(), key=lambda kv: -len(kv[1])):
        share = sum(m.block.marg["A"].pred(a) for a in members)
        lines.append(f"- **{net_name}**：{len(members)} 个出块者，合计份额 {fmt_p(share)} — " + "、".join(who(a) for a in members))
    lines.append("")

    # 3. 结构：哪些偏好成立
    lines.extend(["## 3. 偏好是否成立（增量贝叶斯因子）", "",
                  "log10 BF > 2：强证据存在偏好；0~2：弱；< 0：数据更支持没有偏好。MI = 互信息（比特），越大偏好越强。", "",
                  "| 边 | 含义 | 观测数 | log10 BF | MI(bit) | 结论 |", "|---|---|---|---|---|---|"])
    meaning = MEANING
    for net in (m.tx, m.block):
        for node in net.nodes:
            lines.append(f"| `{node.name}` | {meaning.get(node.name, '')} | {node.n} | {node.logbf:+.1f} | "
                         f"{node.mi_bits(net.marg[node.child]):.2f} | {verdict(node.logbf)} |")
    lines.append("")

    # 4. 提交地址画像
    lines.extend(["## 4. 提交地址画像（后验）", ""])
    S = m.tx.marg["S"]

    def best(nodename, s, k=3):
        node = m.tx.node(nodename)
        row = node.rows.get((s,))
        if not row:
            return "-"
        marg = m.tx.marg[node.child]
        out = []
        for x in row.top(k, marg.pred):
            p = row.pred(x, marg.pred)
            a, b = row.beta_params(x, marg.pred)
            pg = p_greater(a, b, marg.pred(x))
            lab = who(x) if node.child == "A" else x
            out.append(f"{lab} {fmt_p(p)}（提升×{p/marg.pred(x):.1f}，P>基线 {pg:.2f}）")
        return "；".join(out)

    for s in S.top(10):
        lines.extend([f"### `{s}` — 份额 {fmt_p(S.pred(s))}", "",
                      f"- 私有通道：{best('V|S', s, 2)}",
                      f"- 路由合约：{best('R|S', s, 2)}",
                      f"- 小费档位：{best('T|S', s, 2)}",
                      f"- 最先冒出的节点：{best('K|S', s, 3)}",
                      f"- 入口地区：{best('G|S', s, 2)}",
                      f"- 打包它的出块者：{best('A|S', s, 3)}",
                      f"- 结果：{best('O|S', s, 2)}", ""])

    # 5. 事件类别 → 地址 → 节点
    lines.extend(["## 5. 某类事件由谁提交、先往哪个节点", "", "| 类别 | 最可能的提交地址 | 该地址这类事件最先出现的节点 |", "|---|---|---|"])
    nsc, nksc = m.tx.node("S|C"), m.tx.node("K|S,C")
    for c in m.tx.marg["C"].top(9):
        row = nsc.rows.get((c,))
        if not row:
            continue
        s_top = row.top(1, S.pred)[0]
        r2 = nksc.rows.get((s_top, c))
        k_txt = "-"
        if r2:
            k_top = r2.top(1, m.tx.marg["K"].pred)[0]
            g = m.peer_geo(k_top)
            k_txt = f"`{k_top}` {m.region(g) or ''} {g['org'] if g else ''}（{fmt_p(r2.pred(k_top, m.tx.marg['K'].pred))}）"
        lines.append(f"| {c} | `{s_top[:12]}` {fmt_p(row.pred(s_top, S.pred))} | {k_txt} |")
    lines.append("")
    lines.append("注：地区 = 最先把块/交易传给我们的 P2P 节点所在地（IP 归属），是出块者或提交者出口位置的代理量，"
                 "受我们节点连接面影响；私有交易没有入口节点。")
    open(path + ".tmp", "w").write("\n".join(lines))
    os.replace(path + ".tmp", path)


# ---------------------------------------------------------------- 网页（快照 + 历史 + 静态页）


def snapshot(m):
    """当前后验的可视化快照：结论、份额（含 90% 可信区间）、地址画像、类别映射、出块者出口。"""
    vn = m.enr.validators

    def lab(var, x):
        if var == "A":
            return f"{vn.get(x, '?')} {x[:10]}"
        if var == "K":
            g = m.peer_geo(x)
            return f"{x[:8]} {m.region(g) or ''} {g['org'] if g else ''}".strip()
        if var == "S":
            return x[:12]
        return x

    def shares(net, var, k=10):
        d = net.marg[var]
        out = []
        for x in d.top(k):
            a, b = d.beta_params(x)
            lo, hi = beta_ci(a, b, 600)
            out.append({"key": x, "label": lab(var, x), "p": d.pred(x), "lo": lo, "hi": hi, "n": d.c.get(x, 0)})
        return out

    def row_top(net, nodename, pa, k=3):
        node = net.node(nodename)
        row = node.rows.get(pa)
        if not row:
            return []
        marg = net.marg[node.child]
        out = []
        for x in row.top(k, marg.pred):
            p = row.pred(x, marg.pred)
            a, b = row.beta_params(x, marg.pred)
            out.append({"key": x, "label": lab(node.child, x), "p": p, "lift": p / marg.pred(x),
                        "pgt": p_greater(a, b, marg.pred(x), 600), "n": row.n})
        return out

    def row_p(net, nodename, pa, x):
        node = net.node(nodename)
        row = node.rows.get(pa)
        return row.pred(x, net.marg[node.child].pred) if row else None

    edges = []
    for net in (m.tx, m.block):
        for node in net.nodes:
            edges.append({"net": net.name, "name": node.name, "meaning": MEANING.get(node.name, ""), "n": node.n,
                          "bf": node.logbf, "mi": node.mi_bits(net.marg[node.child]), "verdict": verdict(node.logbf)})
    senders = []
    for s_ in m.tx.marg["S"].top(15):
        pa = (s_,)
        senders.append({
            "key": s_, "share": m.tx.marg["S"].pred(s_), "n": m.tx.marg["S"].c.get(s_, 0),
            "private": row_p(m.tx, "V|S", pa, "私有"), "success": row_p(m.tx, "O|S", pa, "成功"),
            "reverted": row_p(m.tx, "O|S", pa, "回滚"),
            "K": row_top(m.tx, "K|S", pa), "G": row_top(m.tx, "G|S", pa, 2), "R": row_top(m.tx, "R|S", pa, 2),
            "T": row_top(m.tx, "T|S", pa, 2), "A": row_top(m.tx, "A|S", pa, 2),
        })
    cats = []
    for c in m.tx.marg["C"].top(9):
        top = row_top(m.tx, "S|C", (c,), 3)
        k = row_top(m.tx, "K|S,C", (top[0]["key"], c), 2) if top else []
        cats.append({"key": c, "p": m.tx.marg["C"].pred(c), "senders": top, "nodes": k})
    authors = []
    for a in m.block.marg["A"].top(10):
        authors.append({"key": a, "label": lab("A", a), "share": m.block.marg["A"].pred(a),
                        "G": row_top(m.block, "G|A", (a,), 3), "N": row_top(m.block, "N|A", (a,), 3),
                        "K": row_top(m.block, "K|A", (a,), 3)})
    return {
        "t": time.time(), "blocks": m.block.t, "txs": m.tx.t,
        "half_life": {"block": m.args.block_half_life, "tx": m.args.tx_half_life},
        "edges": edges,
        "shares": {"block_G": shares(m.block, "G"), "block_A": shares(m.block, "A"), "block_N": shares(m.block, "N"),
                   "tx_S": shares(m.tx, "S"), "tx_C": shares(m.tx, "C"), "tx_V": shares(m.tx, "V"),
                   "tx_O": shares(m.tx, "O"), "tx_R": shares(m.tx, "R"), "tx_G": shares(m.tx, "G"),
                   "tx_K": shares(m.tx, "K"), "tx_L": shares(m.tx, "L")},
        "senders": senders, "cats": cats, "authors": authors,
        "tri": m.tri.snapshot(lambda a: lab("A", a), m.tx.marg["S"].top(15)),
        "seats": seat_snapshot(m),
    }


def seat_snapshot(m):
    """节点席位：读探针写的 peers.json（含状态和三项份额），加最近的席位决策。"""
    rows = []
    try:
        peers = json.load(open(m.args.peers_json))
    except (OSError, ValueError):
        peers = []
    for pr in peers:
        if not pr.get("state"):
            continue
        ip = pr.get("addr", "").rsplit(":", 1)[0]
        g = m.enr.geo.get(ip) or {}
        if ip:
            m.enr.want_geo(ip)
        rows.append({"id": pr["id"], "state": pr["state"], "connected": pr.get("connected"),
                     "a": pr.get("share_a", 0), "b": pr.get("share_b", 0), "c": pr.get("share_c", 0),
                     "name": (pr.get("name") or "").split("/")[1] if "/" in (pr.get("name") or "") else pr.get("name"),
                     "where": f"{g.get('cc', '?')}/{g.get('city', '')} {g.get('org', '')}".strip(),
                     "minutes": pr.get("connected_minutes")})
    order = {"core": 0, "trial": 1, "explore": 2, "lost": 3, "cooldown": 4, "candidate": 5}
    rows.sort(key=lambda r: (order.get(r["state"], 9), -max(r["a"], r["b"], r["c"])))
    counts = collections.Counter(r["state"] for r in rows)
    return {"counts": counts, "rows": rows[:80], "decisions": list(m.decisions)[-40:][::-1]}


def compact(snap):
    """历史里只存画趋势需要的数。"""
    return {
        "t": snap["t"], "blocks": snap["blocks"], "txs": snap["txs"],
        "bf": {e["name"]: round(e["bf"], 3) for e in snap["edges"]},
        "mi": {e["name"]: round(e["mi"], 4) for e in snap["edges"]},
        "shares": {k: {r["key"]: [round(r["p"], 4), round(r["lo"], 4), round(r["hi"], 4)] for r in v}
                   for k, v in snap["shares"].items()},
        "senders": {x["key"]: [round(x["share"], 4), x["private"] and round(x["private"], 4),
                               x["success"] and round(x["success"], 4), x["K"][0]["key"] if x["K"] else None,
                               x["K"] and round(x["K"][0]["p"], 4)] for x in snap["senders"]},
    }


class Web:
    def __init__(self, m, addr, token):
        import http.server
        import urllib.parse

        self.m, self.lock = m, threading.Lock()
        self.snap = b"{}"
        self.hist_path = os.path.join(m.args.state_dir, "history.ndjson")
        self.history = []
        if os.path.exists(self.hist_path):
            for line in open(self.hist_path):
                try:
                    self.history.append(json.loads(line))
                except ValueError:
                    pass
        page_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
        web = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body, ctype="application/json; charset=utf-8"):
                # 必须带 Content-Length：没有它时经 Clash 之类的代理访问，代理会一直等连接关闭，页面卡在"加载中"。
                import gzip
                if "gzip" in (self.headers.get("Accept-Encoding") or "") and len(body) > 1024:
                    body = gzip.compress(body, 6)
                    self.send_response(code)
                    self.send_header("Content-Encoding", "gzip")
                else:
                    self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                u = urllib.parse.urlparse(self.path)
                q = urllib.parse.parse_qs(u.query)
                if token and q.get("token", [""])[0] != token:
                    return self._send(401, b"token", "text/plain")
                obs = "".join(ch for ch in q.get("obs", [""])[0] if ch.isalnum() or ch in "-_")[:20]
                if u.path != "/ingest" or not obs:
                    return self._send(404, b"not found", "text/plain")
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                if body and not body.endswith(b"\n"):
                    body += b"\n"
                with open(os.path.join(web.m.obs_dir, obs + ".ndjson"), "ab") as f:
                    f.write(body)
                self._send(200, b"ok", "text/plain")

            def do_GET(self):
                u = urllib.parse.urlparse(self.path)
                q = urllib.parse.parse_qs(u.query)
                if token and q.get("token", [""])[0] != token:
                    return self._send(401, "需要 ?token=".encode(), "text/plain; charset=utf-8")
                if u.path == "/":
                    return self._send(200, open(page_path, "rb").read(), "text/html; charset=utf-8")
                if u.path == "/api/snapshot":
                    with web.lock:
                        body = web.snap
                    return self._send(200, body)
                if u.path == "/api/history":
                    since = float(q.get("since", ["0"])[0])
                    with web.lock:
                        rows = [h for h in web.history if h["t"] > since]
                    step = max(1, len(rows) // 1500)  # 超过 1500 个点就抽稀
                    return self._send(200, json.dumps(rows[::step] + (rows[-1:] if rows and step > 1 else [])).encode())
                self._send(404, b"not found", "text/plain")

        host, port = addr.rsplit(":", 1)
        srv = http.server.ThreadingHTTPServer((host, int(port)), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()

    def refresh(self):
        snap = snapshot(self.m)
        c = compact(snap)
        with self.lock:
            self.snap = json.dumps(snap, ensure_ascii=False).encode()
            self.history.append(c)
            if len(self.history) > 20000:
                self.history = self.history[-20000:]
        with open(self.hist_path, "a") as f:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="p2p-mempool-probe 的 ndjson")
    ap.add_argument("--state-dir", default=".")
    ap.add_argument("--report", default="bayes_report.md")
    ap.add_argument("--report-every", type=int, default=120)
    ap.add_argument("--block-half-life", type=float, default=20000)
    ap.add_argument("--tx-half-life", type=float, default=3000)
    ap.add_argument("--web", default="", help="网页监听地址，如 0.0.0.0:8090；空=不开")
    ap.add_argument("--web-token-file", default="", help="访问令牌文件，不存在就生成")
    ap.add_argument("--snapshot-every", type=int, default=60)
    ap.add_argument("--local-observer", default="fra", help="本机探针的观测点名字")
    ap.add_argument("--peers-json", default="/var/lib/p2p-mempool-probe-b/peers.json", help="探针写的节点统计（含席位状态）")
    args = ap.parse_args()
    m = Monitor(args)
    if args.web:
        token = ""
        if args.web_token_file:
            if not os.path.exists(args.web_token_file):
                import secrets
                open(args.web_token_file, "w").write(secrets.token_urlsafe(18))
                os.chmod(args.web_token_file, 0o600)
            token = open(args.web_token_file).read().strip()
        m.web = Web(m, args.web, token)
    m.run()


if __name__ == "__main__":
    main()
