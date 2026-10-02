"""forfeit.proto（proto 子模块 polyuma/forfeit/v1，polymas/proto tag polyuma/forfeit/v1.0.0）的手写编解码（零依赖）。
字段号与 proto 严格一致，改一边必须改另一边。

只用到 proto3 的两种 wire type：varint(0) 与 length-delimited(2)，默认值不编码。
"""
from datetime import datetime, timezone

# 字段表：name -> (field_no, kind)。kind: u64/u32/bool/enum/str/bytes/msg:<Name>，前缀 "*" 表示 repeated
SCHEMA = {
    "ForfeitFrame": {"hello": (1, "msg:Hello"), "event": (2, "msg:ForfeitEvent")},
    "Hello": {"sent_at_us": (1, "u64"), "last_sequence": (2, "u64"), "events_total": (3, "u64")},
    "ForfeitEvent": {
        "sequence": (1, "u64"), "backfill": (2, "bool"),
        "detect_at_us": (3, "u64"), "emitted_at_us": (4, "u64"), "sent_at_us": (5, "u64"),
        "game": (6, "str"), "league": (7, "str"), "match_name": (8, "str"),
        "pandascore_match_id": (9, "u64"), "forfeited_game_number": (10, "u32"),
        "team_a": (11, "str"), "team_b": (12, "str"), "winner": (13, "str"), "loser": (14, "str"),
        "game_length_s": (15, "u32"), "src_end_at_us": (16, "u64"), "match_begin_at_us": (17, "u64"),
        "match_status": (18, "str"),
        "lookup_status": (20, "enum"), "pm_event_slug": (21, "str"),
        "condition_ids": (22, "*bytes"), "markets": (23, "*msg:PmMarket"),
    },
    "PmMarket": {
        "condition_id": (1, "bytes"), "slug": (2, "str"), "question": (3, "str"),
        "sports_market_type": (4, "str"), "game_number": (5, "u32"),
        "token_ids": (6, "*bytes"), "outcomes": (7, "*str"),
        "is_forfeited_market": (8, "bool"), "closed": (9, "bool"), "accepting_orders": (10, "bool"),
    },
}
LOOKUP_STATUS = {"matched": 1, "not_found": 2, "failed": 3}
LOOKUP_STATUS_NAME = {v: k for k, v in LOOKUP_STATUS.items()}


# ---------------------------------------------------------------- 编码

def _varint(n):
    out = bytearray()
    n &= (1 << 64) - 1
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def _key(no, wt):
    return _varint((no << 3) | wt)

def encode(msg_name, obj):
    schema = SCHEMA[msg_name]
    out = bytearray()
    for name, (no, kind) in sorted(schema.items(), key=lambda kv: kv[1][0]):
        v = obj.get(name)
        if v is None:
            continue
        rep = kind.startswith("*")
        kind = kind.lstrip("*")
        for item in (v if rep else [v]):
            if kind in ("u64", "u32", "enum", "bool"):
                iv = int(item)
                if iv == 0 and not rep:
                    continue
                out += _key(no, 0) + _varint(iv)
            else:
                if kind == "str":
                    b = str(item).encode("utf-8")
                elif kind == "bytes":
                    b = bytes(item)
                else:
                    b = encode(kind[4:], item)
                if not b and not rep and not kind.startswith("msg:"):
                    continue
                out += _key(no, 2) + _varint(len(b)) + b
    return bytes(out)


# ---------------------------------------------------------------- 解码

def _read_varint(buf, i):
    shift = n = 0
    while True:
        b = buf[i]; i += 1
        n |= (b & 0x7F) << shift
        if not b & 0x80:
            return n, i
        shift += 7

def decode(msg_name, buf):
    schema = SCHEMA[msg_name]
    by_no = {no: (name, kind) for name, (no, kind) in schema.items()}
    out = {}
    for name, (_, kind) in schema.items():
        if kind.startswith("*"):
            out[name] = []
    i = 0
    while i < len(buf):
        k, i = _read_varint(buf, i)
        no, wt = k >> 3, k & 7
        if wt == 0:
            v, i = _read_varint(buf, i)
        elif wt == 2:
            ln, i = _read_varint(buf, i)
            v, i = buf[i:i + ln], i + ln
        elif wt == 1:
            v, i = buf[i:i + 8], i + 8
        elif wt == 5:
            v, i = buf[i:i + 4], i + 4
        else:
            raise ValueError("unsupported wire type %d" % wt)
        if no not in by_no:
            continue                       # 未知字段跳过，向前兼容
        name, kind = by_no[no]
        rep = kind.startswith("*")
        kind = kind.lstrip("*")
        if kind == "bool":
            v = bool(v)
        elif kind == "str":
            v = bytes(v).decode("utf-8")
        elif kind == "bytes":
            v = bytes(v)
        elif kind.startswith("msg:"):
            v = decode(kind[4:], bytes(v))
        if rep:
            out[name].append(v)
        else:
            out[name] = v
    return out


# ---------------------------------------------------------------- 业务转换

def iso_to_us(iso):
    if not iso:
        return 0
    try:
        return int(datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp() * 1_000_000)
    except Exception:
        return 0

def now_us():
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

def hex32(h):
    h = (h or "").lower()
    h = h[2:] if h.startswith("0x") else h
    return bytes.fromhex(h) if len(h) == 64 else b""

def u256(dec):
    try:
        return int(str(dec)).to_bytes(32, "big")
    except Exception:
        return b""

def event_to_pb(ev, backfill):
    """forfeit_ws 内部事件（含 pm_match）-> ForfeitFrame 字节。"""
    pm = ev.get("pm_match") or {}
    gn = ev.get("game_num") or ""
    msg = {
        "sequence": ev.get("seq") or 0,
        "backfill": backfill,
        "detect_at_us": int((ev.get("detect_epoch") or 0) * 1_000_000) or iso_to_us(ev.get("detect_iso")),
        "emitted_at_us": iso_to_us(ev.get("emitted_at")),
        "sent_at_us": now_us(),
        "game": ev.get("game") or "", "league": ev.get("league") or "",
        "match_name": ev.get("match_name") or "",
        "pandascore_match_id": ev.get("match_id") or 0,
        "forfeited_game_number": 0 if gn == "series" else (ev.get("map_number") or 0),
        "team_a": ev.get("team_a") or "", "team_b": ev.get("team_b") or "",
        "winner": ev.get("winner") or "", "loser": ev.get("loser") or "",
        "game_length_s": max(0, ev.get("length_s") or 0),
        "src_end_at_us": iso_to_us(ev.get("src_end_at")),
        "match_begin_at_us": iso_to_us(ev.get("match_begin_at")),
        "match_status": ev.get("status") or "",
        "lookup_status": LOOKUP_STATUS.get(pm.get("status"), 0),
        "pm_event_slug": pm.get("pm_event_slug") or "",
        "condition_ids": [hex32(c) for c in pm.get("condition_ids") or []],
        "markets": [{
            "condition_id": hex32(m.get("condition_id")),
            "slug": m.get("slug") or "", "question": m.get("question") or "",
            "sports_market_type": m.get("sports_market_type") or "",
            "game_number": m.get("game_number") or 0,
            "token_ids": [u256(t) for t in m.get("token_ids") or []],
            "outcomes": [str(o) for o in m.get("outcomes") or []],
            "is_forfeited_market": m.get("is_forfeited_market"),
            "closed": m.get("closed"), "accepting_orders": m.get("accepting_orders"),
        } for m in pm.get("markets") or []],
    }
    return encode("ForfeitFrame", {"event": msg})

def hello_to_pb(last_seq, total):
    return encode("ForfeitFrame", {"hello": {"sent_at_us": now_us(), "last_sequence": last_seq,
                                             "events_total": total}})
