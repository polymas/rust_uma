#!/usr/bin/env python3
"""把一条 forfeit 事件关联到对应的 Polymarket 市场。

全部走 Polymarket **公开 API**，不依赖任何私有数据：
  1) public-search?q=<两队队名>   -> 找到事件 slug
  2) events?slug=<slug>           -> 取该事件下所有子市场
  3) 全部子市场的 conditionId 都返回（下游整场比赛全部排除），
     并按 `-game{N}` 后缀 / moneyline 标出被判弃权的那一盘

注意：Polymarket 的 API 对无 UA 的请求会返回 403，必须带浏览器 User-Agent。
"""
import json, os, re, threading, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"
UA    = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
         "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
# 缓存策略（排除名单要尽量全、又不能每次推送/补推都打 gamma）：
#   matched   -> 近 24h 的比赛 10 分钟后重查（Polymarket 可能晚些才补开子市场），更早的永久有效
#   not_found -> 10 分钟后重查（可能只是开盘晚 / 搜索暂时没收录）
#   failed    -> 不缓存
# matched 结果落盘（PM_CACHE），重启后补推不用把历史比赛全部重查一遍。
CACHE_TTL      = 600.0
RECENT_S       = 24 * 3600
PM_CACHE_PATH  = Path(os.environ.get("PM_CACHE", "./pm_cache.json"))
TIMEOUT   = 8          # 事件推送前要等它，宁可 matched=false 也不要卡半分钟

def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def _log(*a):
    print(datetime.now(timezone.utc).strftime("%H:%M:%S"), "[pm_lookup]", *a, flush=True)

def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read())

def _jsonish(v):
    """Gamma 把 outcomes / clobTokenIds 返回成 JSON 字符串。"""
    if isinstance(v, list):
        return v
    try:
        return json.loads(v) if v else []
    except Exception:
        return []


class PMLookup:
    def __init__(self):
        self.lock = threading.Lock()
        self.cache = {}        # key -> (ts, result)
        self._load_disk()

    def _load_disk(self):
        try:
            raw = json.loads(PM_CACHE_PATH.read_text())
        except Exception:
            return
        now = time.time()
        for k, res in raw.items():
            mid, _, gn = k.partition("|")
            self.cache[(int(mid) if mid.isdigit() else mid, gn)] = (now, res)
        _log("loaded %d cached matches from %s" % (len(raw), PM_CACHE_PATH))

    def _save_disk(self):
        with self.lock:
            data = {"%s|%s" % k: v[1] for k, v in self.cache.items() if v[1].get("matched")}
        tmp = PM_CACHE_PATH.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(data, ensure_ascii=False))
            tmp.replace(PM_CACHE_PATH)
        except Exception as e:
            _log("写缓存失败:", repr(e))

    # ---------- 第 1 步：按队名搜事件 ----------
    def _find_event(self, team_a, team_b, date_hint):
        """网络错误直接抛出（由 lookup 记为 FAILED、不缓存）；没搜到返回 None。"""
        q = urllib.parse.quote("%s %s" % (team_a, team_b))
        res = _get("%s/public-search?q=%s&limit_per_type=8" % (GAMMA, q))
        na, nb = _norm(team_a), _norm(team_b)
        best, best_score = None, -1
        for ev in (res.get("events") or []):
            title = _norm(ev.get("title"))
            slug  = ev.get("slug") or ""
            score = 0
            if na and na in title: score += 2
            if nb and nb in title: score += 2
            if score < 4:                       # 两队都必须出现，避免错配
                continue
            if date_hint and date_hint in slug: # slug 里通常带日期
                score += 3
            if score > best_score:
                best, best_score = ev, score
        return best.get("slug") if best else None

    # ---------- 第 2 步：取事件下的全部子市场 ----------
    def _event(self, slug):
        evs = _get("%s/events?slug=%s" % (GAMMA, urllib.parse.quote(slug)))
        return evs[0] if evs else None

    # ---------- 对外 ----------
    def lookup(self, event):
        """输入一条 forfeit 事件，返回 pm_match（永远返回 dict，不抛异常）。

        status: matched / not_found / failed。failed（网络错误）不缓存，下次重查。"""
        gn  = (event.get("game_num") or "").strip()
        key = (event.get("match_id"), gn)
        now = time.time()
        recent = now - (event.get("detect_epoch") or 0) < RECENT_S
        with self.lock:
            hit = self.cache.get(key)
            if hit and (now - hit[0] < CACHE_TTL or (hit[1].get("matched") and not recent)):
                return hit[1]
        try:
            res = self._resolve(event, None if gn == "series" else event.get("map_number"))
        except Exception as e:
            _log("lookup 失败:", repr(e))
            return {"matched": False, "status": "failed", "reason": "Polymarket 查询失败: %r" % (e,),
                    "condition_ids": [], "markets": []}
        with self.lock:
            prev = self.cache.get(key)
            self.cache[key] = (now, res)
        if res.get("matched") and (not prev or prev[1].get("condition_ids") != res.get("condition_ids")):
            self._save_disk()
        return res

    def _resolve(self, event, map_number):
        a, b = event.get("team_a"), event.get("team_b")
        date_hint = (event.get("match_begin_at") or "")[:10]
        slug = self._find_event(a, b, date_hint)
        if not slug:
            return {"matched": False, "status": "not_found",
                    "reason": "Polymarket 上没有找到该对局（多为冷门赛事未开盘）",
                    "condition_ids": [], "markets": []}
        ev = self._event(slug) or {}
        raw = ev.get("markets") or []
        win = _norm(event.get("winner"))
        # 被判弃权的那个盘：分局弃权 = slug 以 -game{N} 结尾的单局胜负盘；整场弃权 = moneyline
        target_suffix = ("-game%d" % int(map_number)) if map_number else None

        markets, cids = [], []
        forfeited = None
        for m in raw:
            mslug = m.get("slug") or ""
            gm = re.search(r"-game(\d+)(?:-|$)", mslug)
            smt = m.get("sportsMarketType") or ""
            if target_suffix:
                is_target = mslug.endswith(target_suffix)
            else:
                is_target = smt == "moneyline" or mslug == slug
            outcomes = _jsonish(m.get("outcomes"))
            tokens   = _jsonish(m.get("clobTokenIds"))
            cid = m.get("conditionId")
            if not cid:
                continue
            rec = {
                "condition_id": cid, "slug": mslug, "question": m.get("question"),
                "sports_market_type": smt, "game_number": int(gm.group(1)) if gm else 0,
                "outcomes": outcomes, "token_ids": tokens,
                "is_forfeited_market": bool(is_target),
                "closed": bool(m.get("closed")), "accepting_orders": bool(m.get("acceptingOrders")),
            }
            markets.append(rec)
            cids.append(cid)
            if is_target and forfeited is None:
                forfeited = (m, outcomes, tokens)

        if not markets:
            return {"matched": False, "status": "not_found", "reason": "事件存在但没有子市场",
                    "pm_event_slug": slug, "pm_event_url": "https://polymarket.com/event/%s" % slug,
                    "condition_ids": [], "markets": []}

        out = {
            "matched": True, "status": "matched",
            "source": "polymarket_public_api",
            "pm_event_slug": slug,
            "pm_event_title": ev.get("title"),
            "pm_event_url": "https://polymarket.com/event/%s" % slug,
            # 下游黑名单：这场比赛在 Polymarket 上的全部子市场
            "condition_ids": cids,
            "markets": markets,
        }
        # 兼容旧字段：被判弃权那一盘的明细（找不到对应盘时这些字段为 None）
        if forfeited:
            m, outcomes, tokens = forfeited
            outs, win_tok, lose_tok = [], None, None
            for i, name in enumerate(outcomes):
                tok = tokens[i] if i < len(tokens) else None
                is_win = _norm(name) == win
                outs.append({"pm_outcome": name, "token_id": tok, "is_pandascore_winner": is_win})
                if is_win: win_tok = tok
                else:      lose_tok = tok
            out.update({
                "condition_id": m.get("conditionId"), "market_slug": m.get("slug"),
                "pm_question": m.get("question"),
                "clob_market_api": "%s/markets/%s" % (CLOB, m.get("conditionId")),
                "outcomes": outs, "winner_token_id": win_tok, "loser_token_id": lose_tok,
                "closed": m.get("closed"), "accepting_orders": m.get("acceptingOrders"),
            })
        else:
            out.update({"condition_id": None, "market_slug": None})
        return out

PM_LOOKUP = PMLookup()
