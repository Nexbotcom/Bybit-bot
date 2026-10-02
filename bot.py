# ---- Gold 5M Bot (Bybit data) : BUY + SELL, one trade at a time ----
# PAPER mode only: uses real Bybit bid/ask and records simulated trades.
# It never sends an order. Bybit market data is public, so NO Bybit API keys needed.
#
# Env vars (Railway -> Variables):
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
#   BYBIT_SYMBOL (optional, default XAUUSDT)
#   BYBIT_CATEGORY (optional, default linear)
#   DB_PATH      (optional, default gold_5m_bybit.db; use /data/... on Railway)
#   MODE         (optional, default PAPER)
# requirements.txt: requests

import os
import sys
import time
import sqlite3
import requests
from datetime import datetime, timezone

BASE_URL = "https://api.bybit.com"

# ---- strategy settings (USD per ounce = "points") ----
TF_MS = 300_000               # 5-minute candle
RUNUP_WINDOW = 12             # look back 12 candles
TOP_LOOKBACK = 5              # candle 1 must break the last 5 closes
MIN_MOVE = 10.0               # candle 1 close vs lowest/highest close of last 12
GAP_TOLERANCE = 0.10          # max |close of candle 1 - open of candle 2|
SL_POINTS = 3.0
TP_POINTS = 3.0
MAX_SPREAD = 0.50             # skip a signal if spread is wider than this

# ---- timing ----
SCAN_WINDOW_SECONDS = 90      # only scan in the first 90s after a 5M candle closes
MONITOR_INTERVAL_SECONDS = 2
SUMMARY_INTERVAL_SECONDS = 86400

TOKEN = CHAT = DB_PATH = SYMBOL = CATEGORY = None
_last_logged = {}


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


# ---------------- BYBIT DATA ----------------
def api_get(path, params):
    r = requests.get(BASE_URL + path, params=params, timeout=15)
    try:
        j = r.json()
    except ValueError:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit {j.get('retCode')}: {j.get('retMsg')}")
    return j["result"]


def fetch_quote():
    """Returns (bid, ask)."""
    res = api_get("/v5/market/tickers", {"category": CATEGORY, "symbol": SYMBOL})
    d = res["list"][0]
    return float(d["bid1Price"]), float(d["ask1Price"])


def build_5m(now_ms):
    """Closed 5M candles, oldest first: [start, open, high, low, close].
    Bybit returns newest first, and the newest one is still forming."""
    res = api_get("/v5/market/kline", {
        "category": CATEGORY, "symbol": SYMBOL, "interval": "5", "limit": "60"})
    rows = [[int(r[0])] + [float(x) for x in r[1:5]] for r in res["list"]]
    rows.sort(key=lambda r: r[0])
    return [r for r in rows if r[0] + TF_MS <= now_ms]   # drop the forming candle


# ---------------- STRATEGY ----------------
def evaluate(direction, candles):
    """direction 'sell' (fade a run-up) or 'buy' (fade a run-down).
    candles = closed 5M candles, oldest first. Returns (fired, reason)."""
    need = RUNUP_WINDOW + 2
    if len(candles) < need:
        return False, f"only {len(candles)} candles"
    candles = candles[-need:]
    for a, b in zip(candles, candles[1:]):
        if b[0] - a[0] != TF_MS:
            return False, "market break inside lookback"

    closes = [c[4] for c in candles]
    c1, c2 = len(candles) - 2, len(candles) - 1
    close1, open2, close2 = candles[c1][4], candles[c2][1], candles[c2][4]
    gap = abs(close1 - open2)

    if direction == "sell":
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
    else:
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

    return True, f"MATCH move {move:.2f} gap {gap:.2f}"


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
        candles = build_5m(int(time.time() * 1000))
    except Exception as e:
        log_once("fetch", f"candle fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return False

    if not candles or candles[-1][0] != expected:
        log_once("late", f"latest 5M candle {expected} not available yet, retrying")
        return False

    when = datetime.fromtimestamp(expected / 1000, timezone.utc).strftime("%m-%d %H:%M")
    s_ok, s_why = evaluate("sell", candles)
    b_ok, b_why = evaluate("buy", candles)
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
        c = build_5m(int(time.time() * 1000))
        if not c:
            log("[SOURCE] no 5M candles returned")
            return False
        when = datetime.fromtimestamp(c[-1][0] / 1000, timezone.utc).strftime("%m-%d %H:%M")
        log(f"[SOURCE] {SYMBOL}: bid {bid:.2f} ask {ask:.2f} spread {ask - bid:.2f}; "
            f"latest closed 5M candle {when} UTC close {c[-1][4]:.2f}")
        return True
    except Exception as e:
        log(f"[SOURCE] failed: {type(e).__name__}: {str(e)[:200]}")
        return False


def main():
    global TOKEN, CHAT, DB_PATH, SYMBOL, CATEGORY
    TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT = os.environ["TELEGRAM_CHAT_ID"]
    SYMBOL = os.environ.get("BYBIT_SYMBOL", "XAUUSDT")
    CATEGORY = os.environ.get("BYBIT_CATEGORY", "linear")
    DB_PATH = os.environ.get("DB_PATH", "gold_5m_bybit.db")
    mode = os.environ.get("MODE", "PAPER").upper()
    if mode != "PAPER":
        log(f"MODE={mode} is not supported yet. Only PAPER is available. Exiting.")
        sys.exit(1)

    init_db()
    while not pick_source():
        log("No data reachable, retrying in 60s")
        time.sleep(60)

    send_telegram(f"✅ Gold 5M Bybit bot started [PAPER]\nSymbol: {SYMBOL}\n"
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
