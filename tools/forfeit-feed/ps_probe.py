#!/usr/bin/env python3
"""PandaScore 弃权探针 —— 轮询比赛，把「某场某局结束」写成 CSV 一行。

需要你自己的 PandaScore API Key（环境变量 PANDASCORE_TOKEN）。
免费版即可：只用 /matches/running 与 /matches/past 两个通用端点。

输出 CSV 与 forfeit_ws.py 之间是硬契约，字段顺序不可改。
"""
import csv, json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone
from pathlib import Path

# 支持逗号分隔的多个 key：轮流使用，把请求摊到各 key 的小时配额上
TOKENS   = [t.strip() for t in os.environ.get("PANDASCORE_TOKEN", "").split(",") if t.strip()]
TOKEN    = TOKENS[0] if TOKENS else ""
CSV_PATH = Path(os.environ.get("PROBE_CSV", "./result_probe.csv"))
POLL_S   = float(os.environ.get("PROBE_POLL_S", "3"))
HEARTBEAT = Path(os.environ.get("PROBE_HEARTBEAT", "./probe_heartbeat.json"))
PAST_EVERY = int(os.environ.get("PROBE_PAST_EVERY", "10"))   # 每 N 轮补查一次 past
API      = "https://api.pandascore.co"

# PandaScore 的 videogame.slug 历史上有过多种写法（league-of-legends / cs-go / cs-2 …），
# 统一归一成短名再做过滤，避免配置写 csgo 而接口回 cs-go 时整类比赛被静默丢掉。
SLUG_ALIAS = {
    "league-of-legends": "lol", "lol": "lol",
    "cs-go": "cs2", "csgo": "cs2", "cs-2": "cs2", "cs2": "cs2", "counter-strike": "cs2",
    "dota-2": "dota2", "dota2": "dota2",
    "valorant": "valorant",
}
def norm_game(slug):
    slug = (slug or "").strip().lower()
    return SLUG_ALIAS.get(slug, slug)
GAMES    = set(norm_game(g) for g in
               filter(None, os.environ.get("PROBE_GAMES", "lol,cs2,dota2,valorant").split(",")))

FIELDS = ["source","detect_iso","detect_epoch","game","league","match_name","team_a","team_b",
          "game_num","winner","src_end_at","detect_lag_vs_end_s","length","forfeit","status",
          "draw","match_id","match_begin_at"]

def log(*a):
    print(datetime.now(timezone.utc).strftime("%H:%M:%S"), *a, flush=True)

_rr = 0
_cool = {}      # key 下标 -> 冷却到期 epoch（401/429 后暂停使用）

def _next_key():
    global _rr
    now = time.time()
    for _ in range(len(TOKENS)):
        i = _rr % len(TOKENS)
        _rr += 1
        if _cool.get(i, 0) <= now:
            return i
    return min(range(len(TOKENS)), key=lambda i: _cool.get(i, 0))   # 全在冷却：用最早到期的

def api(path, **params):
    if not TOKENS:
        raise RuntimeError("缺少 PANDASCORE_TOKEN 环境变量")
    q = "&".join("%s=%s" % (k, v) for k, v in params.items())
    url = "%s%s%s%s" % (API, path, "?" if q else "", q)
    i = _next_key()
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + TOKENS[i],
                                               "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code in (401, 403, 429):
            _cool[i] = time.time() + (3600 if e.code != 429 else 120)
            log("key#%d HTTP %d，冷却 %ss" % (i, e.code, 3600 if e.code != 429 else 120))
        raise

def team_names(m):
    out = []
    for o in (m.get("opponents") or []):
        t = o.get("opponent") or {}
        out.append({"id": t.get("id"), "name": t.get("name")})
    while len(out) < 2:
        out.append({"id": None, "name": ""})
    return out[:2]

def rows_from_match(m, seen, now):
    """把一场比赛里所有『已结束的局』+『已结束的系列赛』转成 CSV 行。"""
    game_slug = norm_game((m.get("videogame") or {}).get("slug"))
    if GAMES and game_slug not in GAMES:
        return []
    a, b = team_names(m)
    by_id = {a["id"]: a["name"], b["id"]: b["name"]}
    league = ((m.get("league") or {}).get("name") or "")
    mid    = m.get("id")
    rows   = []

    def mk(game_num, winner, end_at, length, forfeit):
        # 去重键带上 forfeit：同一局先以普通赛果出现、之后被 PandaScore 改判为弃权时
        # 也要再写一行，这正是下游最关心的"结果被改判"场景。
        key = (mid, str(game_num), end_at or "", bool(forfeit))
        if key in seen:
            return
        seen.add(key)
        iso = datetime.now(timezone.utc).isoformat()
        lag = ""
        if end_at:
            try:
                t = datetime.strptime(end_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                lag = round(now - t.timestamp(), 1)
            except Exception:
                pass
        rows.append({
            "source": "pandascore", "detect_iso": iso, "detect_epoch": round(now, 3),
            "game": game_slug, "league": league, "match_name": m.get("name") or "",
            "team_a": a["name"], "team_b": b["name"], "game_num": game_num,
            "winner": winner or "", "src_end_at": end_at or "",
            "detect_lag_vs_end_s": lag, "length": length if length is not None else "",
            "forfeit": bool(forfeit), "status": m.get("status") or "",
            "draw": bool(m.get("draw")), "match_id": mid,
            "match_begin_at": m.get("begin_at") or "",
        })

    for g in (m.get("games") or []):
        if g.get("status") != "finished":
            continue
        w = (g.get("winner") or {}).get("id")
        mk(g.get("position"), by_id.get(w, ""), g.get("end_at"),
           g.get("length"), g.get("forfeit"))

    if m.get("status") == "finished":
        w = (m.get("winner") or {}).get("id")
        mk("series", by_id.get(w, ""), m.get("end_at"), None, m.get("forfeit"))
    return rows

HB = {"started": None, "rounds": 0, "ok_rounds": 0, "last_ok": None,
      "last_error": None, "last_error_at": None, "rows_total": 0, "forfeit_rows_total": 0,
      "running_matches": 0}

def write_heartbeat():
    """每轮写一次心跳给 forfeit_ws 的 /health 读：CSV 只有新赛果才更新，没法据此判断探针是否活着。"""
    now = time.time()
    hb = dict(HB, keys_total=len(TOKENS),
              keys_cooling=sum(1 for t in _cool.values() if t > now), poll_s=POLL_S)
    tmp = HEARTBEAT.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(hb))
        tmp.replace(HEARTBEAT)
    except Exception as e:
        log("写心跳失败:", repr(e))

def main():
    if not TOKEN:
        log("缺少 PANDASCORE_TOKEN 环境变量，退出")
        raise SystemExit(2)
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    new = not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0
    f = CSV_PATH.open("a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
    if new:
        w.writeheader(); f.flush()

    seen = set()
    if not new:                                  # 重启后不重复写
        try:
            with CSV_PATH.open("r", encoding="utf-8") as rf:
                for r in csv.DictReader(rf):
                    seen.add((int(r["match_id"]) if str(r.get("match_id","")).isdigit()
                              else r.get("match_id"),
                              str(r.get("game_num")), r.get("src_end_at") or "",
                              str(r.get("forfeit")).strip().lower() == "true"))
        except Exception as e:
            log("读取历史 CSV 失败(忽略):", repr(e))
    log("启动，已知 %d 行；轮询 %ss；key %d 个；游戏 %s"
        % (len(seen), POLL_S, len(TOKENS), ",".join(sorted(GAMES))))

    n = 0
    HB["started"] = datetime.now(timezone.utc).isoformat()
    while True:
        n += 1
        HB["rounds"] = n
        batches = []
        try:
            batches.append(api("/matches/running", per_page=100))
            HB["ok_rounds"] += 1
            HB["last_ok"] = datetime.now(timezone.utc).isoformat()
            HB["running_matches"] = len(batches[0])
        except Exception as e:
            log("/matches/running 失败:", repr(e))
            HB["last_error"], HB["last_error_at"] = repr(e)[:200], datetime.now(timezone.utc).isoformat()
        if n % PAST_EVERY == 0:                  # 末局常常在 running 里来不及看到
            try:
                batches.append(api("/matches/past", per_page=50, sort="-end_at"))
            except Exception as e:
                log("/matches/past 失败:", repr(e))

        now = time.time()
        out = []
        for ms in batches:
            for m in ms:
                try:
                    out += rows_from_match(m, seen, now)
                except Exception as e:
                    log("解析比赛失败(跳过):", repr(e))
        for r in out:
            w.writerow(r)
            if r["forfeit"]:
                log("FORFEIT %s %s g%s winner=%s len=%s"
                    % (r["game"], r["match_name"], r["game_num"], r["winner"], r["length"]))
        if out:
            f.flush()
        HB["rows_total"] += len(out)
        HB["forfeit_rows_total"] += sum(1 for r in out if r["forfeit"])
        write_heartbeat()
        time.sleep(POLL_S)

if __name__ == "__main__":
    main()
