"""多观测点三角定位：同一个块 / 同一笔 propose 交易到达各观测点的时间差 → 源头地区的后验。

模型（每条观测到来就更新，不攒批）：
    观测点 i 收到的时刻  t_i = t0 + d_i(r) + ε_i，   ε_i ~ Student-t(ν=3, σ)
    d_i(r) 是"地区 r → 观测点 i"的单程延迟，由各观测点对该地区 peer 的实测 RTT/2 标定（持续更新）。
    t0（源头发出时刻）未知，用残差均值消掉，只用时间差。

对每个出块者 A（以及每个提交地址 S）的地区后验：
    log P(r | A, 数据) = Σ_i λ^(n−i) · log p(t_i | r) / τ + 常数
τ（回火系数）把"同一出块者的相邻块并不独立"考虑进去，避免后验过早塌缩；λ 是遗忘。

延迟标定会随新测到的 RTT 变化，而似然依赖标定，所以每个键保留最近若干条到达记录：
每来一条就增量加上它的似然；标定一变就用新标定把保留的记录重新打分。
候选地区 R 只取"在足够多观测点都有标定"的国家，每条记录只用对 R 全部有标定的观测点，
保证所有候选地区用同一组时间差比较（否则后来才有标定的地区会白捡便宜）。
"""

import collections
import math
import time

SIGMA_MS = 12.0      # 传播抖动尺度：块要经多跳 gossip 才到观测点，不是直连
NU = 3.0             # Student-t 自由度，对个别慢到的观测更稳健
TEMPER = 4.0         # 回火：相邻观测相关
LAM = 0.5 ** (1 / 2000)
WAIT_S = 8.0         # 等其他观测点到齐的时间


def t_logpdf(x, sigma, nu=NU):
    return (math.lgamma((nu + 1) / 2) - math.lgamma(nu / 2) - 0.5 * math.log(nu * math.pi) - math.log(sigma)
            - (nu + 1) / 2 * math.log1p((x / sigma) ** 2 / nu))


class Posterior:
    def __init__(self, keep=3000):
        self.buf = collections.deque(maxlen=keep)   # 最近的到达记录 {obs: us}
        self.logw, self.version, self.n = {}, None, 0

    def update(self, arr, tri):
        self.buf.append(arr)
        self.n += 1
        if self.version == tri.version:
            ll = tri.loglik(arr)
            if ll:
                for r in self.logw:
                    self.logw[r] = self.logw[r] * LAM + ll.get(r, 0.0) / TEMPER
        # 标定变了：下次读后验时整体重算

    def probs(self, tri):
        if self.version != tri.version:
            self.logw = {r: 0.0 for r in tri.candidates()}
            for arr in self.buf:
                ll = tri.loglik(arr)
                for r in self.logw:
                    self.logw[r] = self.logw[r] * LAM + (ll.get(r, 0.0) if ll else 0.0) / TEMPER
            self.version = tri.version
        if not self.logw:
            return {}
        m = max(self.logw.values())
        z = sum(math.exp(v - m) for v in self.logw.values())
        return {r: math.exp(v - m) / z for r, v in self.logw.items()}

    def dump(self):
        return {"buf": list(self.buf)[-1500:], "n": self.n}

    def load(self, d):
        self.buf.extend(d.get("buf", []))
        self.n = d.get("n", 0)


class Triangulator:
    def __init__(self, geo_of_peer):
        self.geo_of_peer = geo_of_peer          # peer id -> {"cc":..} 或 None
        self.rtt = collections.defaultdict(lambda: collections.defaultdict(list))  # obs -> peer -> [ms]
        self.block_arr = {}                      # hash -> {"author", "arr": {obs: us}, "t"}
        self.tx_arr = {}                         # hash -> {"from", "arr": {obs: us}, "t"}
        self.post_author = collections.defaultdict(Posterior)
        self.post_sender = collections.defaultdict(Posterior)
        self.post_all = Posterior(keep=6000)
        self.version = 0
        self.first_obs = {"author": collections.defaultdict(collections.Counter),
                          "sender": collections.defaultdict(collections.Counter)}
        self.deltas = {"author": collections.defaultdict(lambda: collections.defaultdict(list)),
                       "sender": collections.defaultdict(lambda: collections.defaultdict(list))}
        self.observers = collections.Counter()
        self.last_seen = {}
        self.n_blocks = self.n_txs = 0

    # --- 输入
    def on_record(self, r, obs):
        self.observers[obs] += 1
        self.last_seen[obs] = time.time()
        k = r.get("kind")
        if k == "rtt":
            xs = self.rtt[obs][r["peer"]]
            xs.append(r["rtt_ms"])
            if len(xs) > 50:
                del xs[:20]
        elif k == "block" and r.get("first_us"):
            b = self.block_arr.setdefault(r["hash"], {"author": r.get("author"), "arr": {}, "t": time.time()})
            b["arr"].setdefault(obs, r["first_us"])
            if r.get("author"):
                b["author"] = r["author"]
        elif k == "p2p_match" and r.get("from"):
            first = r.get("first_us") or r.get("announce_us")
            if first:
                t = self.tx_arr.setdefault(r["hash"], {"from": r["from"], "arr": {}, "t": time.time()})
                t["arr"].setdefault(obs, first)

    # --- 标定：地区 -> 观测点单程延迟（RTT 的 25 分位 / 2）
    def calib_table(self):
        """按 peer 的地理归属把 RTT 聚到国家（查询时再聚，地理信息晚到也不丢样本）。每分钟重算一次。"""
        now = time.time()
        if now - getattr(self, "_calib_t", 0) < 60:
            return self._calib
        tab = {}
        old = getattr(self, "_calib", None)
        for obs, per in self.rtt.items():
            by = collections.defaultdict(list)
            for peer, xs in per.items():
                g = self.geo_of_peer(peer)
                if g and g.get("cc") and g["cc"] != "?":
                    by[g["cc"]].append(min(xs))
            tab[obs] = {cc: sorted(v)[len(v) // 4] / 2 for cc, v in by.items() if len(v) >= 2}
        # 标定变化超过 2ms 或候选集合变化才算新版本（避免每分钟都整体重算）
        changed = old is None or set(tab) != set(old) or any(
            set(tab[o]) != set(old.get(o, {})) or any(abs(tab[o][c] - old[o][c]) > 2 for c in tab[o]) for o in tab)
        if changed:
            self.version = getattr(self, "version", 0) + 1
            self._calib = tab
        self._calib_t = now
        return self._calib

    def candidates(self):
        """候选地区：至少在 3 个观测点有标定（观测点不足 3 个时要求全部都有）。"""
        tab = self.calib_table()
        need = min(3, len(tab))
        cnt = collections.Counter(cc for v in tab.values() for cc in v)
        return sorted(cc for cc, n in cnt.items() if n >= need)

    def one_way(self, obs, cc):
        return self.calib_table().get(obs, {}).get(cc)

    def loglik(self, arr):
        tab = self.calib_table()
        cands = self.candidates()
        obs = [o for o in sorted(arr) if all(cc in tab.get(o, {}) for cc in cands)]
        if len(obs) < 2 or not cands:
            return {}
        out = {}
        for cc in cands:
            res = [arr[o] / 1000 - tab[o][cc] for o in obs]
            mu = sum(res) / len(res)
            out[cc] = sum(t_logpdf(x - mu, SIGMA_MS) for x in res)
        return out

    # --- 到齐（或超时）后结算
    def drain(self):
        now = time.time()
        for store, kind in ((self.block_arr, "author"), (self.tx_arr, "sender")):
            for h, e in list(store.items()):
                if now - e["t"] < WAIT_S:
                    continue
                del store[h]
                arr = e["arr"]
                key = e.get("author") if kind == "author" else e.get("from")
                if len(arr) < 2 or not key:
                    continue
                first = min(arr, key=arr.get)
                self.first_obs[kind][key][first] += 1
                base = arr[first]
                for o, t in arr.items():
                    if o != first:
                        self.deltas[kind][key][o].append((t - base) / 1000)
                        if len(self.deltas[kind][key][o]) > 500:
                            del self.deltas[kind][key][o][:200]
                self.calib_table()
                if kind == "author":
                    self.post_author[key].update(arr, self)
                    self.post_all.update(arr, self)
                    self.n_blocks += 1
                else:
                    self.post_sender[key].update(arr, self)
                    self.n_txs += 1

    # --- 快照
    def snapshot(self, label_author, top_senders):
        def med(xs):
            s = sorted(xs)
            return s[len(s) // 2] if s else None

        def rows(kind, posts, keys, label):
            out = []
            for k in keys:
                p = posts.get(k)
                fo = self.first_obs[kind].get(k, collections.Counter())
                tot = sum(fo.values())
                out.append({
                    "key": k, "label": label(k), "n": p.n if p else 0, "seen": sum(fo.values()),
                    "regions": sorted(((r, v) for r, v in (p.probs(self) if p else {}).items()), key=lambda x: -x[1])[:4],
                    "first_obs": {o: c / tot for o, c in fo.items()} if tot else {},
                    "delta_med": {o: med(v) for o, v in self.deltas[kind].get(k, {}).items()},
                })
            return out

        fo_a = self.first_obs["author"]
        authors = sorted(set(self.post_author) | set(fo_a), key=lambda a: -sum(fo_a.get(a, {}).values()))[:10]
        senders = [s for s in top_senders if s in self.post_sender or s in self.first_obs["sender"]][:12]
        calib = {o: dict(sorted(v.items())) for o, v in self.calib_table().items()}
        return {
            "observers": [{"name": o, "events": c, "last_seen": self.last_seen.get(o)} for o, c in self.observers.items()],
            "blocks": self.n_blocks, "txs": self.n_txs,
            "all": sorted(self.post_all.probs(self).items(), key=lambda x: -x[1])[:6],
            "candidates": self.candidates(),
            "authors": rows("author", self.post_author, authors, label_author),
            "senders": rows("sender", self.post_sender, senders, lambda s: s[:12]),
            "calib": calib,
            "params": {"sigma_ms": SIGMA_MS, "nu": NU, "temper": TEMPER, "wait_s": WAIT_S},
        }

    def dump(self):
        return {"post_author": {k: v.dump() for k, v in self.post_author.items()},
                "post_sender": {k: v.dump() for k, v in self.post_sender.items()},
                "post_all": self.post_all.dump(), "n_blocks": self.n_blocks, "n_txs": self.n_txs,
                "rtt": {o: {p: xs[-10:] for p, xs in v.items()} for o, v in self.rtt.items()}}

    def load(self, d):
        for k, v in d.get("post_author", {}).items():
            self.post_author[k].load(v)
        for k, v in d.get("post_sender", {}).items():
            self.post_sender[k].load(v)
        if "post_all" in d:
            self.post_all.load(d["post_all"])
        self.n_blocks, self.n_txs = d.get("n_blocks", 0), d.get("n_txs", 0)
        for o, v in d.get("rtt", {}).items():
            for peer, xs in v.items():
                self.rtt[o][peer] = list(xs)
