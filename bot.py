# ---- Gold CFD Bot (Bitget CFD data): BUY + SELL, one trade at a time ----
# The SAME file is used for the 10M bot and the 15M bot. The only difference is
# the TIMEFRAME_MIN variable set in each Railway project (10 or 15).
# PAPER mode only: uses real Bitget CFD bid/ask prices and never sends an order.
#
# Env vars (Railway -> Variables):
#   TIMEFRAME_MIN                10 or 15 (required)
#   BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
#   CFD_SYMBOL   (optional, default XAUUSD)
#   DB_PATH      (optional, default gold_10m.db / gold_15m.db; use /data/... on Railway)
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

# ---- settings that depend on the timeframe (USD per ounce = "points") ----
SETTINGS = {
    10: {"min_move": 10.0, "big_move": 15.0, "scan_window": 180},
    15: {"min_move": 15.0, "big_move": 20.0, "scan_window": 240},
}
NATIVE_INTERVAL = {15: "15m"}     # Bitget has no 10m candles, so 10M is built from 1m

# ---- strategy settings (same for both timeframes) ----
RUNUP_WINDOW = 12             # look back 12 candles for the run-up / run-down
TOP_LOOKBACK = 5              # candle 1 must break the last 5 closes
GAP_TOLERANCE = 0.10          # max |close of candle 1 - open of candle 2|
MIN_CLOSE_BEYOND = 0.30       # candle 2 must close this far back beyond candle 1's close
SL_POINTS = 3.0
TP_SMALL = 3.0                # take-profit when run-up is below big_move
TP_BIG = 5.0                  # take-profit when run-up is big_move or more
MAX_SPREAD = 0.50             # skip a signal if bid/ask spread is wider than this

# ---- timing ----
MONITOR_INTERVAL_SECONDS = 2
SUMMARY_INTERVAL_SECONDS = 86400

TF_MIN = TF_MS = MIN_MOVE = BIG_MOVE = SCAN_WINDOW_SECONDS = None
TOKEN = CHAT = DB_PATH = SYMBOL = None
API_KEY = API_SECRET = API_PASS = None
_last_logged = {}
CANDLE_MODE = None


def configure(tf_min):
    global TF_MIN, TF_MS, MIN_MOVE, BIG_MOVE, SCAN_WINDOW_SECONDS
    cfg = SETTINGS[tf_min]
    TF_MIN, TF_MS = tf_min, tf_min * 60000
    MIN_MOVE, BIG_MOVE = cfg["min_move"], cfg["big_move"]
    SCAN_WINDOW_SECONDS = cfg["scan_window"]


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def log_once(key, msg):
    """Log a message only when it changes (keeps retries from flooding the log)."""
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


def parse_rows(data):
    return [[int(r[0])] + [float(x) for x in r[1:5]] for r in data]   # [ts,o,h,l,c]


def raw_1m(side, minutes):
    """1m candles for the last `minutes` minutes. Bitget returns at most 100 per
    request, so this asks again from where the last answer stopped.
    side='sell' = bid-based candles, side='buy' = ask-based candles."""
    now_ms = int(time.time() * 1000)
    start = now_ms - minutes * 60000
    rows = {}
    for _ in range(6):
        data = api_get("/api/v3/cfd/market/history-candlestick", {
            "symbol": SYMBOL, "interval": "1m", "side": side,
            "startTime": str(start), "limit": "100"})
        if not data:
            break
        got = parse_rows(data)
        for r in got:
            rows[r[0]] = r
        last = max(r[0] for r in got)
        if len(got) < 100 or last >= now_ms - 60000:
            break
        nxt = last + 60000
        if nxt <= start:                                # no progress, stop
            break
        start = nxt
    return [rows[k] for k in sorted(rows)]


def build_from_1m(rows, now_ms):
    """Group 1m candles into closed candles of TF_MIN minutes aligned to the clock.
    A candle is only built if every one of its 1m candles exists."""
    buckets = {}
    for r in rows:
        buckets.setdefault(r[0] - r[0] % TF_MS, []).append(r)
    out = []
    for start in sorted(buckets):
        grp = sorted(buckets[start])
        if start + TF_MS > now_ms:                      # still forming
            continue
        if [g[0] for g in grp] != [start + k * 60000 for k in range(TF_MIN)]:
            continue                                    # missing minute / market break
        out.append([start, grp[0][1], max(g[2] for g in grp),
                    min(g[3] for g in grp), grp[-1][4]])
    return out


def native_candles(side, now_ms, interval):
    data = api_get("/api/v3/cfd/market/history-candlestick", {
        "symbol": SYMBOL, "interval": interval, "side": side,
        "startTime": str(now_ms - 24 * TF_MS), "limit": "100"})
    rows = sorted(parse_rows(data), key=lambda r: r[0])
    return [r for r in rows if r[0] + TF_MS <= now_ms]  # drop the forming candle


def get_candles(side, now_ms):
    """15M uses Bitget's own 15m candles. 10M (or a failed native request) is built from 1m."""
    global CANDLE_MODE
    out, mode = [], "built from 1m"
    interval = NATIVE_INTERVAL.get(TF_MIN)
    if interval:
        try:
            out = native_candles(side, now_ms, interval)
            mode = f"native {interval}"
        except Exception as e:
            log_once("native", f"native {interval} failed ({str(e)[:100]}), building from 1m")
            out = []
    if not out:
        mode = "built from 1m"
        out = build_from_1m(raw_1m(side, (RUNUP_WINDOW + 4) * TF_MIN), now_ms)
    if mode != CANDLE_MODE:
        log(f"[SOURCE] {TF_MIN}M candles: {mode}")
        CANDLE_MODE = mode
    return out


# ---------------- STRATEGY ----------------
def evaluate(direction, candles):
    """direction 'sell' (fade a run-up) or 'buy' (fade a run-down).
    candles = closed candles, oldest first. Returns (fired, reason, run_move)."""
    need = RUNUP_WINDOW + 2
    if len(candles) < need:
        return False, f"only {len(candles)} candles", 0.0
    candles = candles[-need:]
    for a, b in zip(candles, candles[1:]):
        if b[0] - a[0] != TF_MS:
            return False, "market break inside lookback", 0.0

    closes = [c[4] for c in candles]
    c1, c2 = len(candles) - 2, len(candles) - 1
    open1, close1 = candles[c1][1], candles[c1][4]
    open2, close2 = candles[c2][1], candles[c2][4]
    gap = abs(close1 - open2)

    if direction == "sell":
        if close1 <= open1:
            return False, f"c1 not bullish (open {open1:.2f} close {close1:.2f})", 0.0
        level = max(closes[c1 - TOP_LOOKBACK:c1])
        move = close1 - min(closes[c1 - RUNUP_WINDOW:c1])
        if close1 <= level:
            return False, f"c1 {close1:.2f} not above prior {TOP_LOOKBACK} high {level:.2f}", move
        if move < MIN_MOVE:
            return False, f"run-up {move:.2f} < {MIN_MOVE}", move
        if gap > GAP_TOLERANCE:
            return False, f"gap {gap:.2f} > {GAP_TOLERANCE}", move
        low12 = min(closes[c2 - RUNUP_WINDOW:c2])
        if close2 < low12:
            return False, f"c2 close {close2:.2f} is a new {RUNUP_WINDOW}-candle low", move
        back = close1 - close2                          # how far c2 closed back below c1's close
    else:
        if close1 >= open1:
            return False, f"c1 not bearish (open {open1:.2f} close {close1:.2f})", 0.0
        level = min(closes[c1 - TOP_LOOKBACK:c1])
        move = max(closes[c1 - RUNUP_WINDOW:c1]) - close1
        if close1 >= level:
            return False, f"c1 {close1:.2f} not below prior {TOP_LOOKBACK} low {level:.2f}", move
        if move < MIN_MOVE:
            return False, f"run-down {move:.2f} < {MIN_MOVE}", move
        if gap > GAP_TOLERANCE:
            return False, f"gap {gap:.2f} > {GAP_TOLERANCE}", move
        high12 = max(closes[c2 - RUNUP_WINDOW:c2])
        if close2 > high12:
            return False, f"c2 close {close2:.2f} is a new {RUNUP_WINDOW}-candle high", move
        back = close2 - close1                          # how far c2 closed back above c1's close

    if back < MIN_CLOSE_BEYOND:
        return False, f"c2 closed only {back:.2f} back into/through c1 body, need {MIN_CLOSE_BEYOND}", move

    return True, f"MATCH move {move:.2f} gap {gap:.2f} back {back:.2f}", move


def scan(boundary_ms):
    """True = scan finished for this candle. False = data not ready, retry."""
    if open_trades():
        log_once("open", "scan skipped: a trade is open")
        return True
    _ = _last_logged.pop("open", None)

    expected = boundary_ms - TF_MS                      # open time of candle 2
    key = f"{TF_MIN}M-{expected}"
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
        log_once("late", f"latest {TF_MIN}M candle {expected} not available yet, retrying")
        return False

    when = datetime.fromtimestamp(expected / 1000, timezone.utc).strftime("%m-%d %H:%M")
    s_ok, s_why, s_move = evaluate("sell", sell_c)
    b_ok, b_why, b_move = evaluate("buy", buy_c)
    log(f"{TF_MIN}M [{when}] sell: {s_why} | buy: {b_why}")

    if s_ok and b_ok:
        log("both sides matched, skipping")
        log_signal(key)
        return True
    if not (s_ok or b_ok):
        return True

    direction = "sell" if s_ok else "buy"
    move = s_move if s_ok else b_move
    try:
        bid, ask = fetch_quote()
    except Exception as e:
        log(f"pattern matched but quote fetch failed: {e}")
        return False
    spread = ask - bid
    if spread > MAX_SPREAD:
        log(f"{TF_MIN}M [{when}] {direction} skipped: spread {spread:.2f} > {MAX_SPREAD}")
        log_signal(key)
        return True

    entry = bid if direction == "sell" else ask         # sell fills at bid, buy at ask
    tp_points = TP_BIG if move >= BIG_MOVE else TP_SMALL
    open_paper_trade(direction, entry, spread, expected, tp_points)
    log_signal(key)
    log(f"{TF_MIN}M [{when}] {direction.upper()} SIGNAL @ {entry:.2f} "
        f"(run {move:.2f}, TP {tp_points:.0f}, spread {spread:.2f})")
    return True


def open_paper_trade(direction, entry, spread, signal_ts, tp_points):
    if direction == "sell":
        sl, tp = entry + SL_POINTS, entry - tp_points
    else:
        sl, tp = entry - SL_POINTS, entry + tp_points
    tid = insert_trade(direction, entry, sl, tp, spread, signal_ts)
    icon = "🔴" if direction == "sell" else "🟢"
    send_telegram(
        f"{icon} *GOLD {TF_MIN}M {direction.upper()}* (#{tid}) [PAPER]\n"
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
        close_trade(t["id"], outcome, px, pnl)          # recorded at the price the check saw
        icon = "✅" if outcome == "TP" else "❌"
        send_telegram(
            f"{icon} *GOLD {TF_MIN}M {t['side'].upper()} #{t['id']} closed: {outcome}* [PAPER]\n"
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
    rows = db("""SELECT side, outcome, pnl, entry, tp FROM trades
                 WHERE status='closed' AND closed_at>=?""", (last,), fetch=True)

    def block(sub):
        tp = sum(1 for r in sub if r["outcome"] == "TP")
        sl = sum(1 for r in sub if r["outcome"] == "SL")
        net = sum(r["pnl"] or 0 for r in sub)
        return len(sub), tp, sl, net

    big = [r for r in rows if abs(r["tp"] - r["entry"]) >= 4]
    small = [r for r in rows if abs(r["tp"] - r["entry"]) < 4]
    n, tp, sl, net = block(rows)
    sells = sum(1 for r in rows if r["side"] == "sell")
    n3, tp3, sl3, net3 = block(small)
    n5, tp5, sl5, net5 = block(big)
    send_telegram(
        f"📊 *Daily Summary {TF_MIN}M (PAPER)*\n"
        f"Closed: {n} (sell {sells} / buy {n - sells})\nTP: {tp} | SL: {sl}\n"
        f"Net: `{net:+.2f}` points\n\n"
        f"*$3 target:* {n3} closed | TP {tp3} | SL {sl3} | Net `{net3:+.2f}`\n"
        f"*$5 target:* {n5} closed | TP {tp5} | SL {sl5} | Net `{net5:+.2f}`")
    set_meta("last_summary_sent", now.isoformat())


# ---------------- MAIN ----------------
def pick_source():
    try:
        bid, ask = fetch_quote()
        c = get_candles("sell", int(time.time() * 1000))
        if not c:
            log(f"[SOURCE] no {TF_MIN}M candles could be built")
            return False
        when = datetime.fromtimestamp(c[-1][0] / 1000, timezone.utc).strftime("%m-%d %H:%M")
        log(f"[SOURCE] {SYMBOL}: bid {bid:.2f} ask {ask:.2f} spread {ask - bid:.2f}; "
            f"latest closed {TF_MIN}M candle {when} UTC close {c[-1][4]:.2f}")
        return True
    except Exception as e:
        log(f"[SOURCE] failed: {type(e).__name__}: {str(e)[:200]}")
        return False


def main():
    global TOKEN, CHAT, DB_PATH, SYMBOL, API_KEY, API_SECRET, API_PASS
    tf_min = int(os.environ["TIMEFRAME_MIN"])
    if tf_min not in SETTINGS:
        log(f"TIMEFRAME_MIN={tf_min} is not supported. Use 10 or 15. Exiting.")
        sys.exit(1)
    configure(tf_min)
    TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT = os.environ["TELEGRAM_CHAT_ID"]
    API_KEY = os.environ["BITGET_API_KEY"]
    API_SECRET = os.environ["BITGET_API_SECRET"]
    API_PASS = os.environ["BITGET_API_PASSPHRASE"]
    SYMBOL = os.environ.get("CFD_SYMBOL", "XAUUSD")
    DB_PATH = os.environ.get("DB_PATH", f"gold_{tf_min}m.db")
    mode = os.environ.get("MODE", "PAPER").upper()
    if mode != "PAPER":
        log(f"MODE={mode} is not supported yet. Only PAPER is available. Exiting.")
        sys.exit(1)

    init_db()
    while not pick_source():
        log("No data reachable, retrying in 60s")
        time.sleep(60)

    send_telegram(f"✅ Gold {TF_MIN}M bot started [PAPER]\nSymbol: {SYMBOL}\n"
                  f"Run-up >= {MIN_MOVE:.0f}: TP {TP_SMALL:.0f} | >= {BIG_MOVE:.0f}: TP {TP_BIG:.0f} | SL {SL_POINTS:.0f}")
    log(f"{TF_MIN}M bot running in PAPER mode")

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
