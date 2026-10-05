# ---- Gold CFD 5M Bot (Bitget CFD data) : BUY + SELL, one trade at a time ----
# PAPER mode only: uses real Bitget CFD bid/ask prices, never sends an order.
#
# Env vars (Railway -> Variables):
#   BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
#   CFD_SYMBOL   (optional, default XAUUSD)
#   DB_PATH      (optional, default gold_5m.db; use /data/gold_5m.db on Railway)
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

# ---- strategy settings (USD per ounce = "points") ----
TF_MS = 300_000               # 5-minute candle
RUNUP_WINDOW = 12
TOP_LOOKBACK = 5
MIN_MOVE = 10.0
GAP_TOLERANCE = 0.10
SL_POINTS = 3.0
TP_POINTS = 3.0
MAX_SPREAD = 0.50

# ---- timing ----
SCAN_WINDOW_SECONDS = 60
MONITOR_INTERVAL_SECONDS = 1
SUMMARY_INTERVAL_SECONDS = 86400

MIN_CLOSE_BEYOND = 0.50       # c2 must close at least this far beyond c1's close, in the trade direction


TOKEN = CHAT = DB_PATH = SYMBOL = None
API_KEY = API_SECRET = API_PASS = None
_last_logged = {}
CANDLE_MODE = None


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def log_once(key, msg):
    if _last_logged.get(key) != msg:
        _last_logged[key] = msg
        log(msg)


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
    db("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            side TEXT, entry REAL, sl REAL, tp REAL, spread REAL,
            status TEXT DEFAULT 'open', outcome TEXT, exit_price REAL, pnl REAL,
            signal_time TEXT, opened_at TEXT, closed_at TEXT)""")
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


def open_trades():
    return db("SELECT * FROM trades WHERE status='open'", fetch=True)


def insert_trade(side, entry, sl, tp, spread, signal_ts):
    return db("""INSERT INTO trades (side, entry, sl, tp, spread, signal_time, opened_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
              (side, entry, sl, tp, spread, str(signal_ts),
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


def raw_1m(side):
    """Last ~100 one-minute candles. side='sell' = bid-based, 'buy' = ask-based."""
    now_ms = int(time.time() * 1000)
    data = api_get("/api/v3/cfd/market/history-candlestick", {
        "symbol": SYMBOL, "interval": "1m", "side": side,
        "startTime": str(now_ms - 100 * 60000), "limit": "100"})
    rows = [[int(r[0])] + [float(x) for x in r[1:5]] for r in data]  # [ts,o,h,l,c]
    return sorted(rows, key=lambda r: r[0])


def build_from_1m(rows, now_ms):
    """Fallback: group 1m candles into closed 5M candles aligned to :00, :05, :10 ...
    A 5M candle is only built if all 5 one-minute candles exist."""
    n = TF_MS // 60000
    buckets = {}
    for r in rows:
        buckets.setdefault(r[0] - r[0] % TF_MS, []).append(r)
    out = []
    for start in sorted(buckets):
        grp = sorted(buckets[start])
        if start + TF_MS > now_ms:                      # still forming
            continue
        if [g[0] for g in grp] != [start + k * 60000 for k in range(n)]:
            continue                                    # missing minute / market break
        out.append([start, grp[0][1], max(g[2] for g in grp),
                    min(g[3] for g in grp), grp[-1][4]])
    return out


def native_5m(side, now_ms):
    data = api_get("/api/v3/cfd/market/history-candlestick", {
        "symbol": SYMBOL, "interval": "5m", "side": side,
        "startTime": str(now_ms - 40 * TF_MS), "limit": "100"})
    rows = sorted([[int(r[0])] + [float(x) for x in r[1:5]] for r in data],
                  key=lambda r: r[0])
    return [r for r in rows if r[0] + TF_MS <= now_ms]  # drop the forming candle


def get_candles(side, now_ms):
    """Bitget's CFD API has no 5m interval, so 5M candles are built from 1m."""
    global CANDLE_MODE
    if CANDLE_MODE is None:
        log("[SOURCE] 5M candles: built from 1m")
        CANDLE_MODE = "built from 1m"
    return build_from_1m(raw_1m(side), now_ms)


# ---------------- STRATEGY ----------------
def evaluate(direction, candles):
    """direction 'sell' (fade a run-up) or 'buy' (fade a run-down).
    candles = closed candles, oldest first. Returns (fired, reason)."""
    need = RUNUP_WINDOW + 2
    if len(candles) < need:
        return False, f"only {len(candles)} candles"
    candles = candles[-need:]
    for a, b in zip(candles, candles[1:]):
        if b[0] - a[0] != TF_MS:
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
        if move < MIN_MOVE:
            return False, f"run-up {move:.2f} < {MIN_MOVE}"
        if gap > GAP_TOLERANCE:
            return False, f"gap {gap:.2f} > {GAP_TOLERANCE}"
        low12 = min(closes[c2 - RUNUP_WINDOW:c2])
        if close2 < low12:
            return False, f"c2 close {close2:.2f} is a new {RUNUP_WINDOW}-candle low"
        back = close1 - close2                      # how far c2 closed back below c1's close
    else:
        if close1 >= open1:
            return False, f"c1 not bearish (open {open1:.2f} close {close1:.2f})"
        level = min(closes[c1 - TOP_LOOKBACK:c1])
        move = max(closes[c1 - RUNUP_WINDOW:c1]) - close1
        if close1 >= level:
            return False, f"c1 {close1:.2f} not below prior {TOP_LOOKBACK} low {level:.2f}"
        if move < MIN_MOVE:
            return False, f"run-down {move:.2f} < {MIN_MOVE}"
        if gap > GAP_TOLERANCE:
            return False, f"gap {gap:.2f} > {GAP_TOLERANCE}"
        high12 = max(closes[c2 - RUNUP_WINDOW:c2])
        if close2 > high12:
            return False, f"c2 close {close2:.2f} is a new {RUNUP_WINDOW}-candle high"
        back = close2 - close1                      # how far c2 closed back above c1's close

    if back < MIN_CLOSE_BEYOND:
        return False, f"c2 closed only {back:.2f} back into/through c1 body, need {MIN_CLOSE_BEYOND}"

    return True, f"MATCH move {move:.2f} gap {gap:.2f} back {back:.2f}"








def scan(boundary_ms):
    """True = scan finished for this candle. False = data not ready, retry."""
    if open_trades():
        log_once("open", "scan skipped: a trade is open")
        return True
    _ = _last_logged.pop("open", None)

    expected = boundary_ms - TF_MS                      # open time of candle 2
    key = f"5M-{expected}"
    if signaled(key):
        return True

    try:
        now_ms = int(time.time() * 1000)
        sell_c = get_candles("sell", now_ms)
        buy_c = get_candles("buy", now_ms)
    except Exception as e:
        log_once("fetch", f"candle fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return False

    if not sell_c or not buy_c or sell_c[-1][0] != expected or buy_c[-1][0] != expected:
        log_once("late", f"latest 5M candle {expected} not available yet, retrying")
        return False

    when = datetime.fromtimestamp(expected / 1000, timezone.utc).strftime("%m-%d %H:%M")
    c = sell_c[-1]
    log(f"5M [{when}] bid candle O {c[1]:.2f} H {c[2]:.2f} L {c[3]:.2f} C {c[4]:.2f}")
    s_ok, s_why = evaluate("sell", sell_c)
    b_ok, b_why = evaluate("buy", buy_c)
    log(f"5M [{when}] sell: {s_why} | buy: {b_why}")

    if s_ok and b_ok:
        log("both sides matched, skipping")
        log_signal(key)
        return True
    if not (s_ok or b_ok):
        return True

    direction = "sell" if s_ok else "buy"
    try:
        bid, ask = fetch_quote()
    except Exception as e:
        log(f"pattern matched but quote fetch failed: {e}")
        return False
    spread = ask - bid
    if spread > MAX_SPREAD:
        log(f"5M [{when}] {direction} skipped: spread {spread:.2f} > {MAX_SPREAD}")
        log_signal(key)
        return True

    entry = bid if direction == "sell" else ask         # sell fills at bid, buy at ask
    open_paper_trade(direction, entry, spread, expected)
    log_signal(key)
    log(f"5M [{when}] {direction.upper()} SIGNAL @ {entry:.2f} (spread {spread:.2f})")
    return True


def open_paper_trade(direction, entry, spread, signal_ts):
    if direction == "sell":
        sl, tp = entry + SL_POINTS, entry - TP_POINTS
    else:
        sl, tp = entry - SL_POINTS, entry + TP_POINTS
    tid = insert_trade(direction, entry, sl, tp, spread, signal_ts)
    icon = "🔴" if direction == "sell" else "🟢"
    send_telegram(
        f"{icon} *GOLD 5M {direction.upper()}* (#{tid}) [PAPER]\n"
        f"Entry: `{entry:.2f}`\nSL: `{sl:.2f}`\nTP: `{tp:.2f}`\n"
        f"Spread: `{spread:.2f}`")


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
        if outcome == "TP":                             # a TP order fills at its price
            px = t["tp"]
            pnl = TP_POINTS
        close_trade(t["id"], outcome, px, pnl)
        icon = "✅" if outcome == "TP" else "❌"
        send_telegram(
            f"{icon} *GOLD 5M {t['side'].upper()} #{t['id']} closed: {outcome}* [PAPER]\n"
            f"Entry `{t['entry']:.2f}` -> exit `{px:.2f}`\nResult: `{pnl:+.2f}` points")
        log(f"trade #{t['id']} {outcome} entry {t['entry']:.2f} exit {px:.2f} pnl {pnl:+.2f}")


# ---------------- SUMMARY ----------------
def summary():
    now = datetime.now(timezone.utc)
    last = get_meta("last_summary_sent")
    if last is None:
        set_meta("last_summary_sent", now.isoformat())
        return
    if (now - datetime.fromisoformat(last)).total_seconds() < SUMMARY_INTERVAL_SECONDS:
        return
    rows = db("SELECT side, outcome, pnl FROM trades WHERE status='closed' AND closed_at>=?",
              (last,), fetch=True)
    tp = sum(1 for r in rows if r["outcome"] == "TP")
    sl = sum(1 for r in rows if r["outcome"] == "SL")
    net = sum(r["pnl"] or 0 for r in rows)
    sells = sum(1 for r in rows if r["side"] == "sell")
    send_telegram(
        f"📊 *Daily Summary (PAPER)*\nClosed: {len(rows)} (sell {sells} / buy {len(rows) - sells})\n"
        f"TP: {tp} | SL: {sl}\nNet: `{net:+.2f}` points")
    set_meta("last_summary_sent", now.isoformat())


# ---------------- MAIN ----------------
def pick_source():
    try:
        bid, ask = fetch_quote()
        c = get_candles("sell", int(time.time() * 1000))
        if not c:
            log("[SOURCE] no 5M candles could be built")
            return False
        when = datetime.fromtimestamp(c[-1][0] / 1000, timezone.utc).strftime("%m-%d %H:%M")
        log(f"[SOURCE] {SYMBOL}: bid {bid:.2f} ask {ask:.2f} spread {ask - bid:.2f}; "
            f"latest closed 5M candle {when} UTC close {c[-1][4]:.2f}")
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
    DB_PATH = os.environ.get("DB_PATH", "gold_5m.db")
    mode = os.environ.get("MODE", "PAPER").upper()
    if mode != "PAPER":
        log(f"MODE={mode} is not supported yet. Only PAPER is available. Exiting.")
        sys.exit(1)

    init_db()
    while not pick_source():
        log("No data reachable, retrying in 60s")
        time.sleep(60)

    send_telegram(f"✅ Gold 5M bot started [PAPER]\nSymbol: {SYMBOL}\n"
                  f"Rules: run-up/down >= {MIN_MOVE}, SL {SL_POINTS} / TP {TP_POINTS}")
    log("5M bot running in PAPER mode")

    last_scan = None
    last_summary = 0
    while True:
        try:
            now = time.time()
            into = now % (TF_MS / 1000)
            boundary_ms = int((now - into) * 1000)
            if boundary_ms != last_scan and into < SCAN_WINDOW_SECONDS:
                if scan(boundary_ms):
                    last_scan = boundary_ms
            monitor()
            if time.time() - last_summary >= 3600:
                summary()
                last_summary = time.time()
        except Exception as e:
            log(f"Loop error: {type(e).__name__}: {e}")
        time.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
