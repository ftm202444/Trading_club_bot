import os
import json
import time
import threading
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# CONFIG
# ============================================================
FUTURES_URL = "https://contract.mexc.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8657201723:AAHAHgT24ycT7fevozCQAWDbzoD8aCkEv_Q")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT", "8447349421")
CHANNEL_NAME = "🎯 *نادي التداول - Trading club*"

# Scanner
MAX_SYMBOLS = 100
MIN_QUOTE_VOLUME = 10_000_000
LOOP_SECONDS = 30
MAX_PENDING = 10
MAX_ACTIVE = 6
MAX_SIGNALS_PER_CYCLE = 3

# Detection
MOM_VOL_RATIO = 2.5
MOM_MOVE_MIN = 1.5
MOM_MOVE_MAX = 8.0
BO_VOL_RATIO = 1.8
BO_MIN_PCT = 0.6
BO_LOOKBACK = 8
DETECT_LOOKBACK = 20

# HTF
HTF_FAST = 20
HTF_SLOW = 50
HTF_CACHE_TTL = 300

# Entry / Risk
PULLBACK_MIN = 0.35
PULLBACK_MAX = 0.65
PENDING_TIMEOUT = 30 * 60
ACTIVE_TIMEOUT = 12 * 3600

SL_PCT = 6.0                          # وقف ثابت 6%
TP_PCTS = [2.0, 4.0, 6.0, 9.0]        # 4 أهداف
PARTIAL_EXITS = [0.30, 0.30, 0.20, 0.20]  # إغلاق جزئي
TRAILING_AFTER_TP = 3                 # Trailing بعد TP3
TRAILING_GAP = 1.0                    # 1% تراجع من القمة
LEV = 20
MIN_SCORE = 55

# State
STATE_FILE = "whale_state.json"
LOCK = threading.Lock()
FAILED = {}
FAILED_COOLDOWN = 300
ERR = {"dns": 0, "timeout": 0, "other": 0}

DBG = {"scanned": 0, "raw_signals": 0, "rejected_score": 0,
       "created": 0, "activated": 0, "expired": 0,
       "tp_hit": 0, "sl_hit": 0, "trail_exit": 0}

STOCK = {
    "TESLA","NVDA","AAPL","MSFT","GOOGL","AMZN","META","NFLX","AMD",
    "INTC","TSM","BABA","COIN","MSTR","SPX500","SP500","NDX","DJI",
    "MU","MUU","GOLD","SILVER","OIL","XAU","XAG","WTI","SNXX","SNX",
    "BRENT","USOIL","UKOIL","PLTR","UBER","DIS","BA","JPM","V","MA",
}

FALLBACK = [
    "BTC_USDT","ETH_USDT","SOL_USDT","BNB_USDT","XRP_USDT","DOGE_USDT",
    "ADA_USDT","AVAX_USDT","DOT_USDT","LINK_USDT","LTC_USDT","TRX_USDT",
    "ATOM_USDT","NEAR_USDT","APT_USDT","ARB_USDT","OP_USDT","SUI_USDT",
    "INJ_USDT","TIA_USDT","SEI_USDT","FIL_USDT","ETC_USDT","UNI_USDT",
    "AAVE_USDT","MKR_USDT","CRV_USDT","SAND_USDT","GALA_USDT","PEPE_USDT",
    "SHIB_USDT","WIF_USDT","BONK_USDT","FLOKI_USDT","ORDI_USDT","TAO_USDT",
    "ENA_USDT","WLD_USDT","JUP_USDT","PYTH_USDT",
]

state = {
    "pending": {},
    "active": {},
    "last_alert": {},
    "stats": {
        "created": 0, "activated": 0, "expired": 0,
        "won": 0, "lost": 0, "targets_hit": 0,
        "total_realized": 0.0,
    },
}

# ============================================================
# HTTP
# ============================================================
def http_get(url, timeout=10, retries=2):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e
            s = str(e).lower()
            if "no address" in s or "hostname" in s:
                ERR["dns"] += 1
            elif "timed out" in s:
                ERR["timeout"] += 1
            else:
                ERR["other"] += 1
            time.sleep(0.6 * (i + 1))
    raise last

def tg_send(msg, reply_to=None):
    if not TELEGRAM_TOKEN or "ضع_" in TELEGRAM_TOKEN:
        print("[TG] token missing")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT, "text": msg,
               "parse_mode": "Markdown", "disable_web_page_preview": True}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read().decode())
        if resp.get("ok"):
            return resp["result"]["message_id"]
    except Exception as e:
        print("[TG Send]", e)
    return None

def tg_edit(mid, msg):
    if not mid: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    body = json.dumps({"chat_id": TELEGRAM_CHAT, "message_id": mid,
        "text": msg, "parse_mode": "Markdown",
        "disable_web_page_preview": True}, ensure_ascii=False).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json; charset=utf-8"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print("[TG Edit]", e)
    return False

# ============================================================
# HELPERS
# ============================================================
def disp(s): return s.replace("_", "/")

def fprice(p):
    if p >= 1000: return f"{p:.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"

def fnum(v, d=0.0):
    try: return float(v)
    except: return d

def ema(vals, period):
    if len(vals) < period: return None
    m = 2 / (period + 1)
    e = sum(vals[:period]) / period
    for v in vals[period:]:
        e = (v - e) * m + e
    return e

# ============================================================
# SYMBOLS
# ============================================================
_cached_symbols = {"data": [], "ts": 0}

def load_symbols():
    now = time.time()
    if _cached_symbols["data"] and now - _cached_symbols["ts"] < 300:
        return _cached_symbols["data"]
    detail = http_get(f"{FUTURES_URL}/api/v1/contract/detail")
    tickers = http_get(f"{FUTURES_URL}/api/v1/contract/ticker")
    if not detail.get("success") or not tickers.get("success"):
        return _cached_symbols["data"] or []
    tmap = {t.get("symbol"): t for t in tickers.get("data", []) if t.get("symbol")}
    pairs = []
    for c in detail.get("data", []):
        sym = c.get("symbol", "")
        if c.get("quoteCoin") != "USDT": continue
        if not c.get("apiAllowed", True): continue
        if "STOCK" in sym.upper(): continue
        base = sym.replace("_USDT", "").upper()
        if base in STOCK: continue
        t = tmap.get(sym)
        if not t: continue
        vol = fnum(t.get("amount24"))
        if vol < MIN_QUOTE_VOLUME: continue
        pairs.append((sym, vol))
    pairs.sort(key=lambda x: x[1], reverse=True)
    out = [s for s, _ in pairs[:MAX_SYMBOLS]]
    _cached_symbols["data"] = out
    _cached_symbols["ts"] = now
    return out

# ============================================================
# OHLCV (closed candles only)
# ============================================================
def fetch_ohlcv(symbol, interval, limit=100):
    step = 60 if interval == "Min1" else 5 * 60 if interval == "Min5" else 15 * 60
    now = int(time.time())
    start = now - step * (limit + 5)
    url = (f"{FUTURES_URL}/api/v1/contract/kline/{urllib.parse.quote(symbol)}"
           f"?interval={interval}&start={start}&end={now}")
    data = http_get(url)
    if not data.get("success"): return []
    d = data.get("data", {})
    t = d.get("time", []); o = d.get("open", []); c = d.get("close", [])
    h = d.get("high", []); l = d.get("low", []); v = d.get("vol", [])
    n = min(len(t), len(o), len(c), len(h), len(l), len(v))
    out = []
    for i in range(n):
        out.append({"time": int(fnum(t[i])), "open": fnum(o[i]), "high": fnum(h[i]),
                    "low": fnum(l[i]), "close": fnum(c[i]), "vol": fnum(v[i])})
    out.sort(key=lambda x: x["time"])
    cur_bucket = (now // step) * step
    out = [x for x in out if x["time"] < cur_bucket]
    return out[-limit:]

# ============================================================
# HTF
# ============================================================
_htf_cache = {}

def htf_trend(symbol):
    now = time.time()
    c = _htf_cache.get(symbol)
    if c and now - c["ts"] < HTF_CACHE_TTL:
        return c["trend"]
    try:
        c15 = fetch_ohlcv(symbol, "Min15", limit=100)
        if len(c15) < HTF_SLOW + 5:
            return "unknown"
        closes = [x["close"] for x in c15]
        f = ema(closes, HTF_FAST)
        s = ema(closes, HTF_SLOW)
        if f is None or s is None: return "unknown"
        last = closes[-1]
        if f > s and last > f: trend = "bullish"
        elif f < s and last < f: trend = "bearish"
        else: trend = "neutral"
        _htf_cache[symbol] = {"trend": trend, "ts": now}
        return trend
    except:
        return "unknown"

# ============================================================
# DETECTION
# ============================================================
def detect_impulse(c1):
    if len(c1) < DETECT_LOOKBACK + 3: return None
    recent = c1[-3:-1]
    if len(recent) < 2: return None

    green = all(c["close"] > c["open"] for c in recent)
    red = all(c["close"] < c["open"] for c in recent)
    if not green and not red: return None

    if green:
        direction, ar = "bullish", "شراء"
        move = (recent[-1]["close"] - recent[0]["open"]) / recent[0]["open"] * 100
    else:
        direction, ar = "bearish", "بيع"
        move = (recent[0]["open"] - recent[-1]["close"]) / recent[0]["open"] * 100

    if move < MOM_MOVE_MIN or move > MOM_MOVE_MAX: return None

    prior = c1[-DETECT_LOOKBACK-1:-3]
    if len(prior) < 10: return None
    avg_vol = sum(x["vol"] for x in prior) / len(prior)
    if avg_vol <= 0: return None
    recent_vol = sum(x["vol"] for x in recent) / len(recent)
    vol_ratio = recent_vol / avg_vol
    if vol_ratio < MOM_VOL_RATIO: return None

    imp_low = min(x["low"] for x in recent)
    imp_high = max(x["high"] for x in recent)
    imp_range = imp_high - imp_low
    if imp_range <= 0: return None

    return {
        "direction": direction, "signal_ar": ar,
        "move_pct": move, "vol_ratio": vol_ratio,
        "imp_low": imp_low, "imp_high": imp_high,
        "imp_range": imp_range, "signal_type": "momentum",
    }

def detect_breakout(c5):
    if len(c5) < BO_LOOKBACK + 3: return None
    current = c5[-1]
    prior = c5[-BO_LOOKBACK-1:-1]
    if len(prior) < BO_LOOKBACK: return None

    avg_vol = sum(x["vol"] for x in prior) / len(prior)
    if avg_vol <= 0: return None
    vol_ratio = current["vol"] / avg_vol
    if vol_ratio < BO_VOL_RATIO: return None

    max_high = max(x["high"] for x in prior)
    min_low = min(x["low"] for x in prior)

    direction = None
    move = 0.0
    if current["close"] > max_high:
        move = (current["close"] - max_high) / max_high * 100
        direction, ar = "bullish", "شراء"
    elif current["close"] < min_low:
        move = (min_low - current["close"]) / min_low * 100
        direction, ar = "bearish", "بيع"
    else:
        return None

    if move < BO_MIN_PCT: return None

    body = abs(current["close"] - current["open"])
    rng = current["high"] - current["low"]
    if rng <= 0: return None
    body_ratio = body / rng
    if body_ratio < 0.45: return None

    imp_low = min(current["low"], prior[-1]["low"])
    imp_high = max(current["high"], prior[-1]["high"])
    imp_range = imp_high - imp_low
    if imp_range <= 0: return None

    return {
        "direction": direction, "signal_ar": ar,
        "move_pct": move, "vol_ratio": vol_ratio,
        "imp_low": imp_low, "imp_high": imp_high,
        "imp_range": imp_range, "body_ratio": body_ratio,
        "signal_type": "breakout",
    }

# ============================================================
# SCORE
# ============================================================
def compute_score(imp, htf):
    score = 0
    score += min(30, int(imp["vol_ratio"] * 10))
    score += min(20, int(imp["move_pct"] * 6))
    if htf == imp["direction"]: score += 25
    elif htf == "neutral": score += 10
    elif htf == "unknown": score += 5
    else: score -= 15
    if imp["signal_type"] == "breakout":
        score += min(10, int(imp.get("body_ratio", 0.5) * 15))
    else:
        score += 5
    score += 10
    return max(0, min(100, score))

# ============================================================
# LEVELS — SL ثابت 6%
# ============================================================
def build_levels(imp):
    d = imp["direction"]
    lo, hi, rng = imp["imp_low"], imp["imp_high"], imp["imp_range"]

    if d == "bullish":
        zone_high = hi - rng * PULLBACK_MIN
        zone_low = hi - rng * PULLBACK_MAX
    else:
        zone_low = lo + rng * PULLBACK_MIN
        zone_high = lo + rng * PULLBACK_MAX

    zl, zh = min(zone_low, zone_high), max(zone_low, zone_high)
    entry_ref = (zl + zh) / 2

    # SL ثابت 6% من منتصف المنطقة
    if d == "bullish":
        sl = entry_ref * (1 - SL_PCT / 100)
        tps = [entry_ref * (1 + p / 100) for p in TP_PCTS]
    else:
        sl = entry_ref * (1 + SL_PCT / 100)
        tps = [entry_ref * (1 - p / 100) for p in TP_PCTS]

    return zl, zh, entry_ref, sl, tps, SL_PCT

# ============================================================
# MESSAGES
# ============================================================
def msg_pending(sym, imp, zl, zh, sl, tps, score):
    arrow = "🟢" if imp["direction"] == "bullish" else "🔴"
    type_tag = "⚡ Momentum" if imp["signal_type"] == "momentum" else "🐋 Breakout"
    stars = "⭐" * (score // 20) + "☆" * (5 - score // 20)

    lines = [CHANNEL_NAME, f"⏳ *{type_tag} — بانتظار الدخول*", "━━━━━━━━━━━━━━━"]
    lines.append(f"{arrow} *{imp['signal_ar']}* — `{disp(sym)}`")
    lines.append(f"التقييم: {stars} `{score}/100`")
    lines.append(f"📊 حجم: `{imp['vol_ratio']:.1f}x` | حركة: `+{imp['move_pct']:.1f}%`")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append("💵 *منطقة الدخول المُنتظرة:*")
    lines.append(f"▫️ `{fprice(zl)}` — `{fprice(zh)}`")
    lines.append("")
    lines.append("🎯 *الأهداف (إغلاق جزئي):*")
    for i, tp in enumerate(tps, 1):
        p = TP_PCTS[i-1]
        portion = int(PARTIAL_EXITS[i-1] * 100)
        lines.append(f"{i}. `{fprice(tp)}` (+{p}% | إغلاق {portion}% | x{LEV} = +{p*LEV:.0f}%)")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append(f"🛑 الوقف: `{fprice(sl)}` (-{SL_PCT:.1f}%)")
    lines.append(f"💎 R:R = 1:{TP_PCTS[0]/SL_PCT:.2f} (TP1)")
    lines.append(f"💎 R:R = 1:{TP_PCTS[3]/SL_PCT:.2f} (TP4)")
    lines.append("")
    lines.append("⚠️ *لا تدخل الآن — انتظر وصول السعر للمنطقة*")
    return "\n".join(lines)

def msg_active(sym, info, hit, sl_hit):
    arrow = "🟢" if info["direction"] == "bullish" else "🔴"
    lines = [CHANNEL_NAME, "✅ *إشارة نشطة*", "━━━━━━━━━━━━━━━"]
    lines.append(f"{arrow} *{info['signal_ar']}* — `{disp(sym)}`")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append(f"💵 الدخول: `{fprice(info['entry'])}`")
    realized = 0.0
    for i, tp in enumerate(info["tps"], 1):
        p = TP_PCTS[i-1]
        portion = int(PARTIAL_EXITS[i-1] * 100)
        if i in hit:
            realized += PARTIAL_EXITS[i-1] * p
            lines.append(f"🎯 هدف {i}: `{fprice(tp)}` ✅ (+{p}% × {portion}%)")
        else:
            lines.append(f"🎯 هدف {i}: `{fprice(tp)}` (+{p}% × {portion}%)")
    lines.append("━━━━━━━━━━━━━━━")
    sl_p = SL_PCT
    sl_tag = ""
    if sl_hit:
        sl_tag = " ❌"
    elif info.get("trailing_active"):
        sl_tag = " 🔒"
    lines.append(f"🛑 الوقف: `{fprice(info['sl'])}` (-{sl_p:.1f}%){sl_tag}")
    if realized > 0:
        lines.append(f"💰 مُحقَّق جزئياً: `+{realized:.2f}%`")
    if sl_hit:
        lines.append("")
        if realized > 0:
            lines.append("🔒 *خروج جزئي مُحقَّق*")
        else:
            lines.append("❌ *وقف*")
    elif len(hit) == len(info["tps"]):
        lines.append("")
        lines.append("🏆 *كل الأهداف*")
    return "\n".join(lines)

# ============================================================
# PERSISTENCE
# ============================================================
def save_state():
    try:
        with LOCK:
            data = {
                "pending": dict(state["pending"]),
                "active": {},
                "last_alert": dict(state["last_alert"]),
                "stats": dict(state["stats"]),
            }
            for sym, info in state["active"].items():
                copy = dict(info)
                copy["hit_tps"] = list(info.get("hit_tps", set()))
                data["active"][sym] = copy
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print("[Save]", e)

def load_state():
    if not os.path.exists(STATE_FILE): return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        now = time.time()
        with LOCK:
            state["last_alert"] = data.get("last_alert", {})
            stats = data.get("stats", {})
            for k in state["stats"]:
                if k in stats: state["stats"][k] = stats[k]
            for sym, info in data.get("pending", {}).items():
                if now - info.get("created", 0) < PENDING_TIMEOUT:
                    state["pending"][sym] = info
            for sym, info in data.get("active", {}).items():
                if now - info.get("created", 0) < ACTIVE_TIMEOUT:
                    info["hit_tps"] = set(info.get("hit_tps", []))
                    state["active"][sym] = info
        print(f"[State] Restored {len(state['active'])} active, {len(state['pending'])} pending")
    except Exception as e:
        print("[Load]", e)

# ============================================================
# SCAN → CREATE PENDING
# ============================================================
def analyze(symbol):
    try:
        now = time.time()
        if now - FAILED.get(symbol, 0) < FAILED_COOLDOWN: return None
        if symbol in state["pending"] or symbol in state["active"]: return None
        if now - state["last_alert"].get(symbol, 0) < 1800: return None

        DBG["scanned"] += 1

        c1 = fetch_ohlcv(symbol, "Min1", limit=40)
        imp = None
        if c1 and len(c1) >= 25:
            imp = detect_impulse(c1)
            if imp:
                c5 = fetch_ohlcv(symbol, "Min5", limit=15)
                if c5 and len(c5) >= BO_LOOKBACK + 3:
                    bo = detect_breakout(c5)
                    if bo and bo["direction"] == imp["direction"]:
                        imp["vol_ratio"] = max(imp["vol_ratio"], bo["vol_ratio"])
                        imp["signal_type"] = "breakout"

        if not imp:
            c5 = fetch_ohlcv(symbol, "Min5", limit=15)
            if c5 and len(c5) >= BO_LOOKBACK + 3:
                imp = detect_breakout(c5)

        if not imp: return None

        DBG["raw_signals"] += 1
        htf = htf_trend(symbol)
        score = compute_score(imp, htf)
        if score < MIN_SCORE:
            DBG["rejected_score"] += 1
            return None

        zl, zh, eref, sl, tps, sl_pct = build_levels(imp)
        price = (c1[-1]["close"] if c1 else imp["imp_high"])

        if zl <= price <= zh: return None
        if imp["direction"] == "bullish" and price < zl: return None
        if imp["direction"] == "bearish" and price > zh: return None

        return {
            "symbol": symbol, "signal_ar": imp["signal_ar"],
            "direction": imp["direction"], "signal_type": imp["signal_type"],
            "zone_low": zl, "zone_high": zh, "entry_ref": eref,
            "sl": sl, "tps": tps, "sl_pct": sl_pct,
            "score": score, "move_pct": imp["move_pct"],
            "vol_ratio": imp["vol_ratio"], "htf": htf,
            "created": now,
        }
    except Exception as e:
        FAILED[symbol] = time.time()
        return None

def run_scan(symbols):
    with LOCK:
        pend_count = len(state["pending"])
        act_count = len(state["active"])
    if pend_count >= MAX_PENDING or act_count >= MAX_ACTIVE: return

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(analyze, s): s for s in symbols}
        results = []
        for fut in as_completed(futs):
            try:
                r = fut.result()
                if r: results.append(r)
            except: pass

    results.sort(key=lambda x: -x["score"])

    created = 0
    for r in results:
        if created >= MAX_SIGNALS_PER_CYCLE: break
        with LOCK:
            if len(state["pending"]) >= MAX_PENDING: break
        msg = msg_pending(r["symbol"], r, r["zone_low"], r["zone_high"],
                          r["sl"], r["tps"], r["score"])
        mid = tg_send(msg)
        if not mid: continue
        r["message_id"] = mid
        with LOCK:
            state["pending"][r["symbol"]] = r
            state["last_alert"][r["symbol"]] = time.time()
            state["stats"]["created"] += 1
        DBG["created"] += 1
        created += 1
        print(f"⏳ [{r['score']}] {r['signal_ar']} {disp(r['symbol'])} | {r['signal_type']}")
        time.sleep(0.3)

# ============================================================
# CHECK PENDING → ACTIVATE
# ============================================================
def check_pending():
    with LOCK:
        symbols = list(state["pending"].keys())
    if not symbols: return
    changed = False
    for sym in symbols:
        with LOCK:
            info = state["pending"].get(sym)
        if not info: continue
        try:
            if time.time() - info["created"] > PENDING_TIMEOUT:
                with LOCK:
                    state["pending"].pop(sym, None)
                    state["stats"]["expired"] += 1
                DBG["expired"] += 1
                tg_send("⏱ *انتهت صلاحية الإشارة* على `" + disp(sym) + "`", reply_to=info["message_id"])
                tg_edit(info["message_id"], CHANNEL_NAME + f"\n\n⏱ *منتهية* — `{disp(sym)}`")
                changed = True
                continue

            c1 = fetch_ohlcv(sym, "Min1", limit=3)
            if not c1: continue
            price = c1[-1]["close"]
            d = info["direction"]

            activated = False
            if d == "bullish" and price <= info["zone_high"]:
                activated = True
            elif d == "bearish" and price >= info["zone_low"]:
                activated = True

            if not activated:
                if d == "bullish" and price > info["zone_high"] * 1.02:
                    with LOCK:
                        state["pending"].pop(sym, None)
                        state["stats"]["expired"] += 1
                    tg_send("❌ *إلغاء* — طار السعر", reply_to=info["message_id"])
                    changed = True
                elif d == "bearish" and price < info["zone_low"] * 0.98:
                    with LOCK:
                        state["pending"].pop(sym, None)
                        state["stats"]["expired"] += 1
                    tg_send("❌ *إلغاء* — انهار السعر", reply_to=info["message_id"])
                    changed = True
                continue

            entry = price
            if d == "bullish":
                sl = entry * (1 - SL_PCT / 100)
                tps = [entry * (1 + p / 100) for p in TP_PCTS]
            else:
                sl = entry * (1 + SL_PCT / 100)
                tps = [entry * (1 - p / 100) for p in TP_PCTS]

            with LOCK:
                state["pending"].pop(sym, None)
                state["active"][sym] = {
                    "message_id": info["message_id"],
                    "signal_ar": info["signal_ar"],
                    "direction": d,
                    "signal_type": info["signal_type"],
                    "entry": entry, "sl": sl, "tps": tps,
                    "score": info["score"],
                    "hit_tps": set(),
                    "trailing_active": False,
                    "peak_price": 0.0,
                    "created": time.time(),
                }
                state["stats"]["activated"] += 1
            DBG["activated"] += 1
            new_msg = msg_active(sym, state["active"][sym], set(), False)
            tg_edit(info["message_id"], new_msg)
            tg_send(f"✅ *تم التفعيل* على `{disp(sym)}`\n💵 الدخول: `{fprice(entry)}`", reply_to=info["message_id"])
            print(f"✅ ACTIVATED {disp(sym)} @ {fprice(entry)}")
            changed = True
        except Exception as e:
            print(f"[Pending {sym}]", e)
    if changed: save_state()

# ============================================================
# CHECK ACTIVE — إدارة الصفقة
# ============================================================
def check_active():
    with LOCK:
        symbols = list(state["active"].keys())
    if not symbols: return
    changed = False

    for sym in symbols:
        with LOCK:
            info = state["active"].get(sym)
        if not info: continue
        try:
            if time.time() - info["created"] > ACTIVE_TIMEOUT:
                with LOCK: state["active"].pop(sym, None)
                changed = True
                continue

            c1 = fetch_ohlcv(sym, "Min1", limit=3)
            if not c1: continue
            last = c1[-1]
            d = info["direction"]
            hit = info["hit_tps"]
            new_hits = []
            sl_hit = False

            if d == "bullish":
                if last["low"] <= info["sl"]:
                    sl_hit = True
                else:
                    for i, tp in enumerate(info["tps"]):
                        idx = i + 1
                        if idx not in hit and last["high"] >= tp:
                            hit.add(idx); new_hits.append(idx)
            else:
                if last["high"] >= info["sl"]:
                    sl_hit = True
                else:
                    for i, tp in enumerate(info["tps"]):
                        idx = i + 1
                        if idx not in hit and last["low"] <= tp:
                            hit.add(idx); new_hits.append(idx)

            # === Process new hits ===
            if new_hits:
                with LOCK:
                    state["stats"]["targets_hit"] += len(new_hits)
                DBG["tp_hit"] += len(new_hits)
                for idx in new_hits:
                    p = TP_PCTS[idx-1]
                    portion = int(PARTIAL_EXITS[idx-1] * 100)
                    tg_send(f"✅ *هدف {idx}* على `{disp(sym)}`\n"
                            f"🎯 `{fprice(info['tps'][idx-1])}`\n"
                            f"📈 `+{p}%` | إغلاق `{portion}%`",
                            reply_to=info["message_id"])
                # Activate trailing after TP3
                if 3 in hit and not info.get("trailing_active"):
                    info["trailing_active"] = True
                    if d == "bullish":
                        info["peak_price"] = max(last["high"], info["tps"][2])
                    else:
                        info["peak_price"] = min(last["low"], info["tps"][2])
                    tg_send(f"🔒 *تفعيل Trailing* على `{disp(sym)}`\nحماية آخر 20% بفارق 1%",
                            reply_to=info["message_id"])

                msg = msg_active(sym, info, hit, False)
                tg_edit(info["message_id"], msg)
                with LOCK:
                    state["active"][sym] = info
                changed = True

            # === Trailing Check ===
            if info.get("trailing_active") and not sl_hit:
                peak = info.get("peak_price", 0.0)
                if peak > 0:
                    if d == "bullish":
                        if last["high"] > peak:
                            info["peak_price"] = last["high"]
                            peak = last["high"]
                        trail_level = peak * (1 - TRAILING_GAP / 100)
                        if last["low"] <= trail_level:
                            realized = sum(PARTIAL_EXITS[i-1] * TP_PCTS[i-1] for i in hit)
                            remaining = 1.0 - sum(PARTIAL_EXITS[i-1] for i in hit)
                            trail_profit = (trail_level - info["entry"]) / info["entry"] * 100
                            final = realized + remaining * trail_profit
                            with LOCK:
                                state["active"].pop(sym, None)
                                state["stats"]["won"] += 1
                                state["stats"]["total_realized"] += final
                            DBG["trail_exit"] += 1
                            msg = msg_active(sym, info, hit, True)
                            tg_edit(info["message_id"], msg)
                            tg_send(f"🔒 *Trailing Stop* على `{disp(sym)}`\n"
                                    f"💰 إجمالي الصفقة: `+{final:.2f}%`",
                                    reply_to=info["message_id"])
                            changed = True
                            continue
                    else:
                        if last["low"] < peak:
                            info["peak_price"] = last["low"]
                            peak = last["low"]
                        trail_level = peak * (1 + TRAILING_GAP / 100)
                        if last["high"] >= trail_level:
                            realized = sum(PARTIAL_EXITS[i-1] * TP_PCTS[i-1] for i in hit)
                            remaining = 1.0 - sum(PARTIAL_EXITS[i-1] for i in hit)
                            trail_profit = (info["entry"] - trail_level) / info["entry"] * 100
                            final = realized + remaining * trail_profit
                            with LOCK:
                                state["active"].pop(sym, None)
                                state["stats"]["won"] += 1
                                state["stats"]["total_realized"] += final
                            DBG["trail_exit"] += 1
                            msg = msg_active(sym, info, hit, True)
                            tg_edit(info["message_id"], msg)
                            tg_send(f"🔒 *Trailing Stop* على `{disp(sym)}`\n"
                                    f"💰 إجمالي الصفقة: `+{final:.2f}%`",
                                    reply_to=info["message_id"])
                            changed = True
                            continue

            # === SL hit ===
            if sl_hit:
                realized = sum(PARTIAL_EXITS[i-1] * TP_PCTS[i-1] for i in hit)
                remaining = 1.0 - sum(PARTIAL_EXITS[i-1] for i in hit)
                if remaining > 0:
                    final = realized + remaining * (-SL_PCT)
                else:
                    final = realized
                with LOCK:
                    state["active"].pop(sym, None)
                    if final > 0:
                        state["stats"]["won"] += 1
                    else:
                        state["stats"]["lost"] += 1
                    state["stats"]["total_realized"] += final
                DBG["sl_hit"] += 1
                msg = msg_active(sym, info, hit, True)
                tg_edit(info["message_id"], msg)
                if final > 0:
                    tg_send(f"🛡️ *خروج بربح* على `{disp(sym)}`\n"
                            f"💰 إجمالي: `+{final:.2f}%`", reply_to=info["message_id"])
                else:
                    tg_send(f"❌ *وقف* على `{disp(sym)}`\n"
                            f"💰 إجمالي: `{final:.2f}%`", reply_to=info["message_id"])
                changed = True
                continue

            # === All TPs hit ===
            if len(hit) == len(info["tps"]):
                realized = sum(PARTIAL_EXITS[i-1] * TP_PCTS[i-1] for i in hit)
                with LOCK:
                    state["active"].pop(sym, None)
                    state["stats"]["won"] += 1
                    state["stats"]["total_realized"] += realized
                tg_send(f"🏆 *كل الأهداف* على `{disp(sym)}`\n💰 إجمالي: `+{realized:.2f}%`",
                        reply_to=info["message_id"])
                changed = True

        except Exception as e:
            print(f"[Active {sym}]", e)
    if changed: save_state()

# ============================================================
# TELEGRAM COMMANDS
# ============================================================
_last_upd = [0]

def handle_cmd(text, chat_id):
    if chat_id and str(chat_id) != str(TELEGRAM_CHAT): return
    cmd = text.split()[0].split("@")[0].lower()

    if cmd == "/status":
        with LOCK:
            p = len(state["pending"]); a = len(state["active"])
            s = dict(state["stats"])
        msg = (CHANNEL_NAME + "\n━━━━━━━━━━━━━━━\n🤖 *الحالة*\n"
               f"⏳ معلقة: `{p}/{MAX_PENDING}`\n"
               f"✅ نشطة: `{a}/{MAX_ACTIVE}`\n"
               f"📈 إجمالي: `{s['created']}`\n"
               f"✔️ مُفعّلة: `{s['activated']}`")
        tg_send(msg)

    elif cmd == "/active":
        with LOCK:
            items = list(state["active"].items())
        if not items:
            tg_send("📭 لا إشارات نشطة")
            return
        lines = [CHANNEL_NAME, "━━━━━━━━━━━━━━━", "📊 *النشطة:*"]
        for sym, info in items:
            hits = ",".join(str(x) for x in sorted(info["hit_tps"])) or "-"
            trail = " 🔒" if info.get("trailing_active") else ""
            lines.append(f"• `{disp(sym)}` {info['signal_ar']} | {hits}/4{trail}")
        tg_send("\n".join(lines))

    elif cmd == "/stats":
        with LOCK: s = dict(state["stats"])
        total_closed = s["won"] + s["lost"]
        wr = (s["won"] / total_closed * 100) if total_closed > 0 else 0
        msg = (CHANNEL_NAME + "\n━━━━━━━━━━━━━━━\n📈 *الإحصائيات*\n"
               f"📊 مُنشأة: `{s['created']}`\n"
               f"✔️ مُفعّلة: `{s['activated']}`\n"
               f"⏱ منتهية: `{s['expired']}`\n"
               f"🏆 مكتملة: `{s['won']}`\n"
               f"❌ خاسرة: `{s['lost']}`\n"
               f"📊 نسبة الإكمال: `{wr:.1f}%`\n"
               f"🎯 أهداف: `{s['targets_hit']}`\n"
               f"💰 إجمالي مُحقَّق: `{s['total_realized']:+.2f}%`")
        tg_send(msg)

    elif cmd == "/debug":
        msg = ("═══ DEBUG ═══\n"
               f"Scanned: {DBG['scanned']}\n"
               f"Raw: {DBG['raw_signals']}\n"
               f"Rejected score: {DBG['rejected_score']}\n"
               f"Created: {DBG['created']}\n"
               f"Activated: {DBG['activated']}\n"
               f"Expired: {DBG['expired']}\n"
               f"TP hits: {DBG['tp_hit']}\n"
               f"SL hits: {DBG['sl_hit']}\n"
               f"Trail exits: {DBG['trail_exit']}")
        tg_send(msg)

    elif cmd == "/help":
        tg_send(CHANNEL_NAME + "\n━━━━━━━━━━━━━━━\n📚 *الأوامر:*\n"
                "`/status`\n`/active`\n`/stats`\n`/debug`\n`/help`")

def poll_tg():
    while True:
        try:
            url = (f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
                   f"?timeout=20&offset={_last_upd[0]+1}")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                data = json.loads(r.read().decode())
            if data.get("ok"):
                for u in data.get("result", []):
                    _last_upd[0] = u["update_id"]
                    m = u.get("message", {})
                    txt = m.get("text", "")
                    cid = m.get("chat", {}).get("id")
                    if txt.startswith("/"):
                        handle_cmd(txt, cid)
        except: time.sleep(3)
        time.sleep(1)

# ============================================================
# WEB SERVER (for Render)
# ============================================================
from http.server import HTTPServer, BaseHTTPRequestHandler

class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is alive")
    def log_message(self, format, *args):
        pass

def run_web_server():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), PingHandler)
    print(f"[Web] Listening on port {port}")
    server.serve_forever()

# ============================================================
# MAIN
# ============================================================
def main():
    print("🎯 Trading Club Bot — starting...")
    load_state()

    tg_send(CHANNEL_NAME + "\n\n🚀 *تم التشغيل*\n"
            "⏳ إشارات معلقة بمنطقة دخول\n"
            "🛑 وقف ثابت 6%\n"
            "🎯 4 أهداف (2%, 4%, 6%, 9%)\n"
            "📊 إغلاق جزئي: 30/30/20/20\n"
            "🔒 Trailing بعد الهدف الثالث\n\n"
            "اكتب /help")

    symbols = []
    for attempt in range(3):
        try:
            symbols = load_symbols()
            if symbols: break
        except Exception as e:
            print(f"Load attempt {attempt+1}: {e}")
        time.sleep(5)

    if not symbols:
        symbols = FALLBACK
        tg_send("⚠️ استخدام قائمة احتياطية")

    tg_send(f"✅ يراقب `{len(symbols)}` عقد")
    print(f"Watching {len(symbols)} symbols")

    threading.Thread(target=poll_tg, daemon=True).start()
    threading.Thread(target=run_web_server, daemon=True).start()

    while True:
        t0 = time.time()
        try:
            run_scan(symbols)
            check_pending()
            check_active()
            save_state()
        except Exception as e:
            print("[MAIN]", e)

        elapsed = time.time() - t0
        with LOCK:
            p = len(state["pending"]); a = len(state["active"])
        print(f"[Cycle] {len(symbols)} sym in {elapsed:.1f}s | P:{p} A:{a} | "
              f"DNS:{ERR['dns']} TO:{ERR['timeout']}")
        ERR["dns"] = 0; ERR["timeout"] = 0; ERR["other"] = 0

        time.sleep(max(1, LOOP_SECONDS - elapsed))

if __name__ == "__main__":
    main()