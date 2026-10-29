# ---- Gold CFD Bot (Bitget CFD data): 3M and 5M together, BUY + SELL ----
# PAPER mode only: uses real Bitget CFD bid/ask prices and never sends an order.
#
# Candle 1 and candle 2 pass the rules, then the bot enters right after candle 2 closes.
# Each timeframe (3M, 5M) runs two independent paper "streams" on the same market:
#   BASELINE = takes every normal signal (the control)
#   AFTER-SL = same signals, plus the after-stop-loss rules:
#     * after a stop-loss, signals in the SAME direction are skipped until the market gives
#       5 straight candles in that direction (falling after a sell SL, rising after a buy SL);
#       a same-direction signal during the count is skipped and restarts the count
#     * while that is not yet met, an OPPOSITE trade is taken when candle 1 is bullish (sell)
#       or bearish (buy) and candle 1's close is candle 2's open; no other rules are checked
# One trade at a time per stream, so 3M and 5M (and both streams) can all be open together.
#
# Env vars (Railway -> Variables):
#   BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
#   CFD_SYMBOL   (optional, default XAUUSD)
#   DB_PATH      (optional, default gold_3m5m.db; use /data/gold_3m5m.db on Railway)
#   MODE         (optional, default PAPER)
# requirements.txt: requests

import os
import sys
import time
import hmac
import hashlib
import base64
import sqlite3
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

BASE_URL = "https://api.bitget.com"

# ---- per timeframe settings (USD per ounce = "points") ----
SETTINGS = {
    3: {"min_move": 5.0, "scan_window": 90},
    5: {"min_move": 10.0, "scan_window": 120},
}
TIMEFRAMES = [3, 5]
STRAIGHT_CANDLES = 3          # straight candles needed to lift a block after a stop-loss
LABELS = {"BASELINE": "BASELINE", "AFTER_SL": "AFTER-SL", "FILTERED": "FILTERED"}

# ---- strategy settings (same for both timeframes) ----
RUNUP_WINDOW = 12             # look back 12 candles for the run-up / run-down
TOP_LOOKBACK = 5              # candle 1 must break the last 5 closes
GAP_TOLERANCE = 0.10          # max |close of candle 1 - open of candle 2|
MIN_CLOSE_BEYOND = 0.30       # candle 2 must close at least this far beyond candle 1's close
SL_POINTS = 3.0
TP_POINTS = 3.0
MAX_SPREAD = 0.50             # skip a signal if bid/ask spread is wider than this

MONITOR_INTERVAL_SECONDS = 2
SUMMARY_INTERVAL_SECONDS = 86400

TOKEN = CHAT = DB_PATH = SYMBOL = None
API_KEY = API_SECRET = API_PASS = None
_last_logged = {}


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def log_once(key, msg):
    """Log a message only when it changes (keeps retries from flooding the log)."""
    if _last_logged.get(key) != msg:
        _last_logged[key] = msg
        log(msg)


def hm(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%m-%d %H:%M")


# ---------------- DATABASE ----------------
def db(sql, params=(), fetch=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()] if fetch else None
        conn.commit()
        return rows if fetch else cur.lastrowid
    finally:
        conn.close()


def init_db():
    cols = [r["name"] for r in db("PRAGMA table_info(trades)", fetch=True)]
    if cols and "variant" not in cols:                  # old-format table from a previous bot
        old = "trades_old_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        db(f"ALTER TABLE trades RENAME TO {old}")
        log(f"Old trades table kept as {old}; starting a fresh one")
    db("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            variant TEXT, tf INTEGER, side TEXT, entry REAL, sl REAL, tp REAL, spread REAL,
            status TEXT DEFAULT 'open', outcome TEXT, exit_price REAL, pnl REAL,
            c2_ts INTEGER, c3_pass INTEGER, opened_at TEXT, closed_at TEXT)""")
    have = [r["name"] for r in db("PRAGMA table_info(trades)", fetch=True)]
    if "htf" not in have:
        db("ALTER TABLE trades ADD COLUMN htf TEXT")      # kept so older databases keep working
    if "mode" not in have:
        db("ALTER TABLE trades ADD COLUMN mode TEXT")     # NORMAL or REVERSAL
    db("CREATE TABLE IF NOT EXISTS signal_log (signal_key TEXT PRIMARY KEY)")
    db("CREATE TABLE IF NOT EXISTS bot_meta (key TEXT PRIMARY KEY, value TEXT)")


def get_meta(key):
    rows = db("SELECT value FROM bot_meta WHERE key = ?", (key,), fetch=True)
    return rows[0]["value"] if rows else None


def set_meta(key, value):
    db("INSERT OR REPLACE INTO bot_meta (key, value) VALUES (?, ?)", (key, value))


def signaled(key):
    return bool(db("SELECT 1 FROM signal_log WHERE signal_key = ?", (key,), fetch=True))


def log_signal(key):
    db("INSERT OR IGNORE INTO signal_log (signal_key) VALUES (?)", (key,))


def open_trades(tf=None, variant=None):
    if tf is None:
        return db("SELECT * FROM trades WHERE status='open'", fetch=True)
    return db("SELECT * FROM trades WHERE status='open' AND tf=? AND variant=?",
              (tf, variant), fetch=True)


def insert_trade(variant, tf, side, entry, sl, tp, spread, c2_ts, mode):
    return db("""INSERT INTO trades (variant, tf, side, entry, sl, tp, spread, c2_ts, mode, opened_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
              (variant, tf, side, entry, sl, tp, spread, c2_ts, mode,
               datetime.now(timezone.utc).isoformat()))


def close_trade(trade_id, outcome, exit_price, pnl):
    db("""UPDATE trades SET status='closed', outcome=?, exit_price=?, pnl=?, closed_at=?
          WHERE id=?""",
       (outcome, exit_price, pnl, datetime.now(timezone.utc).isoformat(), trade_id))


# ---------------- TELEGRAM ----------------
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        log(f"[Telegram error] {e}")


# ---------------- BITGET CFD DATA ----------------
def api_get(path, params):
    query = urlencode(params)
    request_path = f"{path}?{query}"
    ts = str(int(time.time() * 1000))
    sign = base64.b64encode(
        hmac.new(API_SECRET.encode(), (ts + "GET" + request_path).encode(),
                 hashlib.sha256).digest()
    ).decode()
    headers = {
        "ACCESS-KEY": API_KEY, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS, "Content-Type": "application/json",
        "locale": "en-US",
    }
    r = requests.get(BASE_URL + request_path, headers=headers, timeout=15)
    try:
        j = r.json()
    except ValueError:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
    if j.get("code") != "00000":
        raise RuntimeError(f"Bitget {j.get('code')}: {j.get('msg')}")
    return j["data"]


def fetch_quote():
    """Returns (bid, ask)."""
    data = api_get("/api/v3/cfd/market/tickers", {"symbol": SYMBOL})
    d = data[0] if isinstance(data, list) else data
    return float(d["bid1"]), float(d["ask1"])


def parse_rows(data):
    return [[int(r[0])] + [float(x) for x in r[1:5]] for r in data]   # [ts,o,h,l,c]


def raw_1m(side, minutes):
    """1m candles for the last `minutes` minutes. Bitget returns at most 100 per
    request (the newest 100 in the range), so this pages backwards with endTime.
    side='sell' = candles used for sell signals, side='buy' = candles used for buy signals."""
    now_ms = int(time.time() * 1000)
    start = now_ms - minutes * 60000
    rows = {}

    def grab(s, e):
        data = api_get("/api/v3/cfd/market/history-candlestick", {
            "symbol": SYMBOL, "interval": "1m", "side": side,
            "startTime": str(s), "endTime": str(e), "limit": "100"})
        got = parse_rows(data) if data else []
        for r in got:
            rows[r[0]] = r
        return got

    end = now_ms + 60000
    for _ in range(6):
        got = grab(start, end)
        if not got:
            break
        oldest = min(r[0] for r in got)
        if oldest <= start or len(got) < 100 or oldest >= end:
            break
        end = oldest
    for _ in range(3):                                  # safety: fill any gap up to now
        newest = max(rows) if rows else None
        if newest is None or newest >= now_ms - 120000:
            break
        got = grab(newest + 60000, now_ms + 60000)
        if not got or max(r[0] for r in got) <= newest:
            break
    return [rows[k] for k in sorted(rows)]


def build_from_1m(rows, tf, now_ms):
    """Group 1m candles into closed candles of tf minutes aligned to the clock.
    A candle is only built if every one of its 1m candles exists."""
    tf_ms = tf * 60000
    buckets = {}
    for r in rows:
        buckets.setdefault(r[0] - r[0] % tf_ms, []).append(r)
    out = []
    for start in sorted(buckets):
        grp = sorted(buckets[start])
        if start + tf_ms > now_ms:                      # still forming
            continue
        if [g[0] for g in grp] != [start + k * 60000 for k in range(tf)]:
            continue                                    # missing minute / market break
        out.append([start, grp[0][1], max(g[2] for g in grp),
                    min(g[3] for g in grp), grp[-1][4]])
    return out


def get_candles(side, tf, now_ms):
    return build_from_1m(raw_1m(side, (RUNUP_WINDOW + 4) * tf), tf, now_ms)


# ---------------- STOP-LOSS BLOCK STATE (kept in the database) ----------------
def blk_get(tf):
    """(blocked direction or '', straight-candle count, SL time in ms, last counted candle ts)."""
    return (get_meta(f"blk_dir_{tf}") or "", int(get_meta(f"blk_count_{tf}") or 0),
            int(get_meta(f"blk_since_{tf}") or 0), int(get_meta(f"blk_last_{tf}") or 0))


def blk_set(tf, direction, count, since, last):
    set_meta(f"blk_dir_{tf}", direction)
    set_meta(f"blk_count_{tf}", str(count))
    set_meta(f"blk_since_{tf}", str(since))
    set_meta(f"blk_last_{tf}", str(last))


def other(side):
    return "buy" if side == "sell" else "sell"


def moving(direction, c):
    """A candle moving in the blocked direction: falling for 'sell', rising for 'buy'."""
    return c[4] < c[1] if direction == "sell" else c[4] > c[1]


def reversal_ok(side, series, tf):
    """Reversal rule for `side`: candle 1 is bullish (sell) or bearish (buy), candle 1's close
    is candle 2's open, and candle 2 closes back inside/through candle 1 by at least
    MIN_CLOSE_BEYOND (so a straight staircase move does not count). Nothing else is checked."""
    if len(series) < 2:
        return False
    c1, c2 = series[-2], series[-1]
    if c2[0] - c1[0] != tf * 60000:
        return False
    if side == "sell":
        colour = c1[4] > c1[1]
        back = c1[4] - c2[4]                            # c2 closed back down into/through c1
    else:
        colour = c1[4] < c1[1]
        back = c2[4] - c1[4]                            # c2 closed back up into/through c1
    return colour and abs(c1[4] - c2[1]) <= GAP_TOLERANCE and back >= MIN_CLOSE_BEYOND


# ---------------- STRATEGY ----------------
def evaluate(direction, candles, tf):
    """Candle 1 / candle 2 rules. direction 'sell' (fade a run-up) or 'buy' (fade a run-down).
    candles = closed candles, oldest first, the last one is candle 2. Returns (fired, reason)."""
    tf_ms, min_move = tf * 60000, SETTINGS[tf]["min_move"]
    need = RUNUP_WINDOW + 2
    if len(candles) < need:
        return False, f"only {len(candles)} candles"
    candles = candles[-need:]
    for a, b in zip(candles, candles[1:]):
        if b[0] - a[0] != tf_ms:
            return False, "market break inside lookback"

    closes = [c[4] for c in candles]
    c1, c2 = len(candles) - 2, len(candles) - 1
    open1, close1 = candles[c1][1], candles[c1][4]
    open2, close2 = candles[c2][1], candles[c2][4]
    gap = abs(close1 - open2)

    if direction == "sell":
        if close1 <= open1:
            return False, f"c1 not bullish (open {open1:.2f} close {close1:.2f})"
        level = max(closes[c1 - TOP_LOOKBACK:c1])
        move = close1 - min(closes[c1 - RUNUP_WINDOW:c1])
        if close1 <= level:
            return False, f"c1 {close1:.2f} not above prior {TOP_LOOKBACK} high {level:.2f}"
        if move < min_move:
            return False, f"run-up {move:.2f} < {min_move}"
        if gap > GAP_TOLERANCE:
            return False, f"gap {gap:.2f} > {GAP_TOLERANCE}"
        low12 = min(closes[c2 - RUNUP_WINDOW:c2])
        if close2 < low12:
            return False, f"c2 close {close2:.2f} is a new {RUNUP_WINDOW}-candle low"
        back = close1 - close2
    else:
        if close1 >= open1:
            return False, f"c1 not bearish (open {open1:.2f} close {close1:.2f})"
        level = min(closes[c1 - TOP_LOOKBACK:c1])
        move = max(closes[c1 - RUNUP_WINDOW:c1]) - close1
        if close1 >= level:
            return False, f"c1 {close1:.2f} not below prior {TOP_LOOKBACK} low {level:.2f}"
        if move < min_move:
            return False, f"run-down {move:.2f} < {min_move}"
        if gap > GAP_TOLERANCE:
            return False, f"gap {gap:.2f} > {GAP_TOLERANCE}"
        high12 = max(closes[c2 - RUNUP_WINDOW:c2])
        if close2 > high12:
            return False, f"c2 close {close2:.2f} is a new {RUNUP_WINDOW}-candle high"
        back = close2 - close1

    if back < MIN_CLOSE_BEYOND:
        return False, f"c2 closed only {back:.2f} back into/through c1 body, need {MIN_CLOSE_BEYOND}"
    return True, f"MATCH move {move:.2f} gap {gap:.2f} back {back:.2f}"


def one_side(sell_ok, buy_ok):
    """Returns 'sell', 'buy' or None (nothing, or both matched which we skip)."""
    if sell_ok and buy_ok:
        return None
    return "sell" if sell_ok else ("buy" if buy_ok else None)


def open_paper_trade(variant, tf, direction, entry, spread, c2_ts, mode, note):
    if direction == "sell":
        sl, tp = entry + SL_POINTS, entry - TP_POINTS
    else:
        sl, tp = entry - SL_POINTS, entry + TP_POINTS
    tid = insert_trade(variant, tf, direction, entry, sl, tp, spread, c2_ts, mode)
    icon = "🔴" if direction == "sell" else "🟢"
    extra = f"\n⚡ {note}" if note else ""
    send_telegram(
        f"{icon} *GOLD {tf}M {direction.upper()}* (#{tid}) [{LABELS[variant]}] [PAPER]\n"
        f"Entry: `{entry:.2f}`\nSL: `{sl:.2f}`\nTP: `{tp:.2f}`\n"
        f"Spread: `{spread:.2f}`{extra}\nSignal: {hm(c2_ts)} UTC")
    return tid


def scan(tf, boundary_ms):
    """Candle 2 has just closed. True = finished. False = retry (data/quote problem)."""
    tf_ms = tf * 60000
    last_closed = boundary_ms - tf_ms                   # open time of candle 2
    base_free = not open_trades(tf, "BASELINE")
    asl_free = not open_trades(tf, "AFTER_SL")
    bkey, akey = f"B-{tf}M-{last_closed}", f"A-{tf}M-{last_closed}"
    base_todo = base_free and not signaled(bkey)
    asl_todo = asl_free and not signaled(akey)
    d, count, since, last = blk_get(tf)
    need_count = bool(d) and last != last_closed
    if not (base_todo or asl_todo or need_count):
        return True

    try:
        now_ms = int(time.time() * 1000)
        sell_c = get_candles("sell", tf, now_ms)
        buy_c = get_candles("buy", tf, now_ms)
    except Exception as e:
        log_once(f"fetch-{tf}", f"{tf}M candle fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return False
    if not sell_c or not buy_c or sell_c[-1][0] != last_closed or buy_c[-1][0] != last_closed:
        log_once(f"late-{tf}", f"{tf}M latest candle {last_closed} not available yet, retrying")
        return False

    s_ok, s_why = evaluate("sell", sell_c, tf)
    b_ok, b_why = evaluate("buy", buy_c, tf)
    log(f"{tf}M [{hm(last_closed)}] sell: {s_why} | buy: {b_why}")
    side = one_side(s_ok, b_ok)

    # ---- update the straight-candle count after a stop-loss ----
    if need_count and last_closed + tf_ms > since:
        candle = (sell_c if d == "sell" else buy_c)[-1]
        same_pass = s_ok if d == "sell" else b_ok
        if last and last_closed - last > tf_ms:
            count = 0                                   # a candle was missed, not "straight"
        if same_pass:
            count = 0                                   # a same-direction signal restarts the count
        elif moving(d, candle):
            count += 1
        else:
            count = 0
        if count >= STRAIGHT_CANDLES:
            word = "falling" if d == "sell" else "rising"
            log(f"{tf}M {d} block lifted after {STRAIGHT_CANDLES} straight {word} candles")
            send_telegram(f"🟦 GOLD {tf}M {d.upper()} block lifted [AFTER-SL]\n"
                          f"{STRAIGHT_CANDLES} straight {word} candles")
            blk_set(tf, "", 0, 0, 0)
            d = ""
        else:
            blk_set(tf, d, count, since, last_closed)
            log(f"{tf}M {d} block: {count}/{STRAIGHT_CANDLES} straight candles")

    # ---- decide what each stream does ----
    plan, skip_msg = [], None
    if base_todo and side:
        plan.append(("BASELINE", side, "NORMAL", "", bkey))
    if asl_todo:
        if d:                                           # blocked after a stop-loss
            same_pass = s_ok if d == "sell" else b_ok
            opp = other(d)
            if same_pass:
                skip_msg = (f"⚪ GOLD {tf}M {d.upper()} signal skipped [AFTER-SL]\n"
                            f"Blocked after a {d} stop-loss ({count}/{STRAIGHT_CANDLES} straight candles)\n"
                            f"Signal: {hm(last_closed)} UTC")
            elif reversal_ok(opp, sell_c if opp == "sell" else buy_c, tf):
                plan.append(("AFTER_SL", opp, "REVERSAL", f"REVERSAL after {d} SL", akey))
        elif side:
            plan.append(("AFTER_SL", side, "NORMAL", "", akey))
    if not plan and not skip_msg:
        return True

    if skip_msg:
        send_telegram(skip_msg)
        log_signal(akey)
    if not plan:
        return True

    try:
        bid, ask = fetch_quote()
    except Exception as e:
        log(f"{tf}M signal matched but quote fetch failed: {e}")
        return False
    spread = ask - bid
    for variant, direction, mode, note, key in plan:
        log_signal(key)
        if spread > MAX_SPREAD:
            log(f"{tf}M {LABELS[variant]} {direction} skipped: spread {spread:.2f} > {MAX_SPREAD}")
            continue
        entry = bid if direction == "sell" else ask     # sell fills at bid, buy at ask
        open_paper_trade(variant, tf, direction, entry, spread, last_closed, mode, note)
        log(f"{tf}M [{hm(last_closed)}] {LABELS[variant]} {mode} {direction.upper()} @ {entry:.2f} "
            f"(spread {spread:.2f})")
    return True


# ---------------- MONITOR ----------------
def monitor():
    trades = open_trades()
    if not trades:
        return
    try:
        bid, ask = fetch_quote()
    except Exception as e:
        log_once("mon", f"monitor quote failed: {type(e).__name__}: {str(e)[:150]}")
        return

    for t in trades:
        if t["side"] == "sell":                         # a sell closes at the ask
            px, hit_sl, hit_tp = ask, ask >= t["sl"], ask <= t["tp"]
            pnl = t["entry"] - px
        else:                                           # a buy closes at the bid
            px, hit_sl, hit_tp = bid, bid <= t["sl"], bid >= t["tp"]
            pnl = px - t["entry"]
        if not (hit_sl or hit_tp):
            continue
        outcome = "SL" if hit_sl else "TP"
        close_trade(t["id"], outcome, px, pnl)          # recorded at the price the check saw
        icon = "✅" if outcome == "TP" else "❌"
        extra = ""
        if t["variant"] == "AFTER_SL":
            if t.get("mode") == "REVERSAL":
                extra += "\n⚡ REVERSAL trade"
            if outcome == "SL":                         # a stop-loss starts (or changes) the block
                blk_set(t["tf"], t["side"], 0, int(time.time() * 1000), 0)
                word = "falling" if t["side"] == "sell" else "rising"
                extra += f"\n⛔ {t['side'].upper()} blocked until {STRAIGHT_CANDLES} straight {word} candles"
        send_telegram(
            f"{icon} *GOLD {t['tf']}M {t['side'].upper()} #{t['id']} closed: {outcome}* "
            f"[{LABELS.get(t['variant'], t['variant'])}] [PAPER]\n"
            f"Entry `{t['entry']:.2f}` -> exit `{px:.2f}`\nResult: `{pnl:+.2f}` points\n"
            f"Signal: {hm(t['c2_ts'])} UTC{extra}")
        log(f"trade #{t['id']} {t['tf']}M {t['variant']} {outcome} entry {t['entry']:.2f} "
            f"exit {px:.2f} pnl {pnl:+.2f}")


# ---------------- SUMMARY ----------------
def summary():
    now = datetime.now(timezone.utc)
    last = get_meta("last_summary_sent")
    if last is None:
        set_meta("last_summary_sent", now.isoformat())
        return
    if (now - datetime.fromisoformat(last)).total_seconds() < SUMMARY_INTERVAL_SECONDS:
        return
    rows = db("""SELECT variant, tf, side, outcome, pnl, mode FROM trades
                 WHERE status='closed' AND closed_at>=?""", (last,), fetch=True)

    def line(label, sub):
        tp = sum(1 for r in sub if r["outcome"] == "TP")
        net = sum(r["pnl"] or 0 for r in sub)
        return f"{label}: {len(sub)} closed | TP {tp} | SL {len(sub) - tp} | Net `{net:+.2f}`"

    parts = ["📊 *Daily Summary (PAPER)*"]
    for tf in TIMEFRAMES:
        base = [r for r in rows if r["tf"] == tf and r["variant"] == "BASELINE"]
        asl = [r for r in rows if r["tf"] == tf and r["variant"] == "AFTER_SL"]
        parts += ["", f"*{tf}M*",
                  line("Baseline", base),
                  line("   sells", [r for r in base if r["side"] == "sell"]),
                  line("   buys", [r for r in base if r["side"] == "buy"]),
                  line("After-SL rules", asl),
                  line("   sells", [r for r in asl if r["side"] == "sell"]),
                  line("   buys", [r for r in asl if r["side"] == "buy"]),
                  line("   normal trades", [r for r in asl if r["mode"] != "REVERSAL"]),
                  line("   reversal trades", [r for r in asl if r["mode"] == "REVERSAL"]),
                  line("      reversal sells", [r for r in asl if r["mode"] == "REVERSAL" and r["side"] == "sell"]),
                  line("      reversal buys", [r for r in asl if r["mode"] == "REVERSAL" and r["side"] == "buy"])]
    send_telegram("\n".join(parts))
    set_meta("last_summary_sent", now.isoformat())


# ---------------- MAIN ----------------
def pick_source():
    try:
        bid, ask = fetch_quote()
        c = get_candles("sell", 3, int(time.time() * 1000))
        if not c:
            log("[SOURCE] no candles could be built")
            return False
        log(f"[SOURCE] {SYMBOL}: bid {bid:.2f} ask {ask:.2f} spread {ask - bid:.2f}; "
            f"latest closed 3M candle {hm(c[-1][0])} UTC close {c[-1][4]:.2f}")
        return True
    except Exception as e:
        log(f"[SOURCE] failed: {type(e).__name__}: {str(e)[:200]}")
        return False


def main():
    global TOKEN, CHAT, DB_PATH, SYMBOL, API_KEY, API_SECRET, API_PASS
    TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT = os.environ["TELEGRAM_CHAT_ID"]
    API_KEY = os.environ["BITGET_API_KEY"]
    API_SECRET = os.environ["BITGET_API_SECRET"]
    API_PASS = os.environ["BITGET_API_PASSPHRASE"]
    SYMBOL = os.environ.get("CFD_SYMBOL", "XAUUSD")
    DB_PATH = os.environ.get("DB_PATH", "gold_3m5m.db")
    mode = os.environ.get("MODE", "PAPER").upper()
    if mode != "PAPER":
        log(f"MODE={mode} is not supported yet. Only PAPER is available. Exiting.")
        sys.exit(1)

    init_db()
    while not pick_source():
        log("No data reachable, retrying in 60s")
        time.sleep(60)

    send_telegram(f"✅ Gold 3M + 5M bot started [PAPER]\nSymbol: {SYMBOL}\n"
                  f"3M run-up >= {SETTINGS[3]['min_move']:.0f} | 5M run-up >= {SETTINGS[5]['min_move']:.0f} "
                  f"| SL {SL_POINTS:.0f} / TP {TP_POINTS:.0f}\n"
                  f"Streams: BASELINE (control) and AFTER-SL (block after a stop-loss + reversal trades)")
    log("3M + 5M bot running in PAPER mode")

    last_scan = {tf: None for tf in TIMEFRAMES}
    last_summary = 0
    while True:
        try:
            now = time.time()
            for tf in TIMEFRAMES:
                into = now % (tf * 60)
                boundary_ms = int((now - into) * 1000)
                if boundary_ms != last_scan[tf] and into < SETTINGS[tf]["scan_window"]:
                    if scan(tf, boundary_ms):
                        last_scan[tf] = boundary_ms
            monitor()
            if time.time() - last_summary >= 3600:
                summary()
                last_summary = time.time()
        except Exception as e:
            log(f"Loop error: {type(e).__name__}: {e}")
        time.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
