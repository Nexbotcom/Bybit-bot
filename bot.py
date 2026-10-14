# ---- Forex CFD Bot (Bitget CFD data): SELL ONLY, 3M and 5M together, 3 pairs ----
# PAPER mode only: uses real Bitget CFD bid/ask prices and never sends an order.
# One bot watches EURUSD, USDJPY and USDCAD on both the 3M and the 5M timeframe.
# Each (pair, timeframe) combination is its own stream with its own open trade.
#
# Env vars (Railway -> Variables):
#   BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
#   DB_PATH        (optional, default forex_paper.db; use /data/forex_paper.db on Railway)
#   SYMBOL_SUFFIX  (optional, default empty = ECN symbols such as EURUSD)
#   FX_PAIRS       (optional, e.g. EURUSD,USDJPY to run fewer pairs)
#   MODE           (optional, default PAPER)
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

# per pair and timeframe (minutes): (minimum run-up in pips, SL = TP in pips, max spread in pips)
PAIR_SETTINGS = {
    "EURUSD": {3: (6.5, 2.5, 0.5), 5: (9.5, 3.0, 0.5)},
    "USDJPY": {3: (8.0, 3.0, 1.0), 5: (10.0, 4.0, 1.0)},
    "USDCAD": {3: (6.0, 2.5, 1.0), 5: (8.0, 3.0, 1.0)},
}
TIMEFRAMES = [3, 5]
SCAN_WINDOW_SECONDS = {3: 90, 5: 120}     # only scan this long after a candle closes

# ---- strategy settings (same for every pair) ----
RUNUP_WINDOW = 12             # look back 12 candles for the run-up
TOP_LOOKBACK = 5              # candle 1 must close above the last 5 closes
GAP_PIPS = 0.3                # max |close of candle 1 - open of candle 2|
MIN_BACK_PIPS = 0.3           # candle 2 must close this far back below candle 1's close
DEFAULT_BID_SIDE = "buy"      # which Bitget candle side holds bid prices (checked at start)

MONITOR_INTERVAL_SECONDS = 2
SUMMARY_INTERVAL_SECONDS = 86400

TOKEN = CHAT = DB_PATH = SUFFIX = None
API_KEY = API_SECRET = API_PASS = None
PAIRS = []
BID_SIDE = {}
_last_logged = {}


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def log_once(key, msg):
    if _last_logged.get(key) != msg:
        _last_logged[key] = msg
        log(msg)


def pip_of(pair):
    return 0.01 if "JPY" in pair else 0.0001


def fmt(pair, price):
    return f"{price:.{3 if 'JPY' in pair else 5}f}"


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
            pair TEXT, tf INTEGER, entry REAL, sl REAL, tp REAL,
            spread_pips REAL, status TEXT DEFAULT 'open', outcome TEXT,
            exit_price REAL, pnl_pips REAL,
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


def open_trades(pair=None, tf=None):
    if pair is None:
        return db("SELECT * FROM trades WHERE status='open'", fetch=True)
    return db("SELECT * FROM trades WHERE status='open' AND pair=? AND tf=?",
              (pair, tf), fetch=True)


def insert_trade(pair, tf, entry, sl, tp, spread_pips, signal_ts):
    return db("""INSERT INTO trades (pair, tf, entry, sl, tp, spread_pips, signal_time, opened_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (pair, tf, entry, sl, tp, spread_pips, str(signal_ts),
               datetime.now(timezone.utc).isoformat()))


def close_trade(trade_id, outcome, exit_price, pnl_pips):
    db("""UPDATE trades SET status='closed', outcome=?, exit_price=?, pnl_pips=?, closed_at=?
          WHERE id=?""",
       (outcome, exit_price, pnl_pips, datetime.now(timezone.utc).isoformat(), trade_id))


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


def fetch_quote(pair):
    """Returns (bid, ask)."""
    data = api_get("/api/v3/cfd/market/tickers", {"symbol": pair + SUFFIX})
    d = data[0] if isinstance(data, list) else data
    return float(d["bid1"]), float(d["ask1"])


def parse_rows(data):
    return [[int(r[0])] + [float(x) for x in r[1:5]] for r in data]   # [ts,o,h,l,c]


def raw_1m(pair, side, minutes):
    """1m candles for the last `minutes` minutes. Bitget returns at most 100 per
    request (the newest 100 in the range), so this pages backwards with endTime."""
    now_ms = int(time.time() * 1000)
    start = now_ms - minutes * 60000
    rows = {}

    def grab(s, e):
        data = api_get("/api/v3/cfd/market/history-candlestick", {
            "symbol": pair + SUFFIX, "interval": "1m", "side": side,
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


def latest_1m(pair, side, want):
    """The newest `want` 1m candles from the last 6 days (works when the market is closed).
    Returns {ts: [o,h,l,c]}. Only used to work out which candle side holds bid prices."""
    now_ms = int(time.time() * 1000)
    data = api_get("/api/v3/cfd/market/history-candlestick", {
        "symbol": pair + SUFFIX, "interval": "1m", "side": side,
        "startTime": str(now_ms - 6 * 86400000), "endTime": str(now_ms + 60000),
        "limit": str(want)})
    return {r[0]: r[1:] for r in parse_rows(data)} if data else {}


def detect_bid_side(pair):
    """Bid prices are lower than ask prices, so the candle side with the lower close is bid-based."""
    try:
        a = latest_1m(pair, "sell", 5)
        b = latest_1m(pair, "buy", 5)
        common = sorted(set(a) & set(b))
        if common:
            t = common[-1]
            if b[t][3] < a[t][3]:
                return "buy"
            if a[t][3] < b[t][3]:
                return "sell"
    except Exception as e:
        log(f"[SOURCE] {pair}: bid-side check failed ({str(e)[:100]})")
    return DEFAULT_BID_SIDE


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


def get_candles(pair, tf, now_ms):
    rows = raw_1m(pair, BID_SIDE[pair], (RUNUP_WINDOW + 4) * tf)
    return build_from_1m(rows, tf, now_ms)


# ---------------- STRATEGY (sell-only) ----------------
def evaluate_sell(pair, tf, candles, min_run_pips):
    """candles = closed bid-based candles, oldest first. Returns (fired, reason)."""
    pip, tf_ms = pip_of(pair), tf * 60000
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
    gap = abs(close1 - open2) / pip

    if close1 <= open1:
        return False, f"c1 not bullish (open {fmt(pair, open1)} close {fmt(pair, close1)})"
    level = max(closes[c1 - TOP_LOOKBACK:c1])
    move = (close1 - min(closes[c1 - RUNUP_WINDOW:c1])) / pip
    if close1 <= level:
        return False, f"c1 {fmt(pair, close1)} not above prior {TOP_LOOKBACK} high {fmt(pair, level)}"
    if move < min_run_pips:
        return False, f"run-up {move:.1f} pips < {min_run_pips}"
    if gap > GAP_PIPS:
        return False, f"gap {gap:.2f} pips > {GAP_PIPS}"
    low12 = min(closes[c2 - RUNUP_WINDOW:c2])
    if close2 < low12:
        return False, f"c2 close {fmt(pair, close2)} is a new {RUNUP_WINDOW}-candle low"
    back = (close1 - close2) / pip
    if back < MIN_BACK_PIPS:
        return False, f"c2 closed only {back:.1f} pips back below c1, need {MIN_BACK_PIPS}"
    return True, f"MATCH run-up {move:.1f} pips gap {gap:.2f} back {back:.1f}"


def scan(pair, tf, boundary_ms):
    """True = scan finished for this candle. False = data not ready, retry."""
    if open_trades(pair, tf):
        log_once(f"open-{pair}-{tf}", f"{pair} {tf}M scan skipped: a trade is open")
        return True
    _last_logged.pop(f"open-{pair}-{tf}", None)

    expected = boundary_ms - tf * 60000                 # open time of candle 2
    key = f"{pair}-{tf}M-{expected}"
    if signaled(key):
        return True

    try:
        candles = get_candles(pair, tf, int(time.time() * 1000))
    except Exception as e:
        log_once(f"fetch-{pair}-{tf}", f"{pair} {tf}M candle fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return False
    if not candles or candles[-1][0] != expected:
        log_once(f"late-{pair}-{tf}", f"{pair} {tf}M latest candle {expected} not available yet, retrying")
        return False

    min_run, sl_pips, max_spread = PAIR_SETTINGS[pair][tf]
    when = datetime.fromtimestamp(expected / 1000, timezone.utc).strftime("%m-%d %H:%M")
    ok, why = evaluate_sell(pair, tf, candles, min_run)
    log(f"{pair} {tf}M [{when}] sell: {why}")
    if not ok:
        return True

    try:
        bid, ask = fetch_quote(pair)
    except Exception as e:
        log(f"{pair} {tf}M pattern matched but quote fetch failed: {e}")
        return False
    pip = pip_of(pair)
    spread_pips = (ask - bid) / pip
    if spread_pips > max_spread:
        log(f"{pair} {tf}M [{when}] sell skipped: spread {spread_pips:.1f} pips > {max_spread}")
        log_signal(key)
        return True

    entry = bid                                         # a sell fills at the bid
    sl, tp = entry + sl_pips * pip, entry - sl_pips * pip
    tid = insert_trade(pair, tf, entry, sl, tp, spread_pips, expected)
    log_signal(key)
    send_telegram(
        f"🔴 *FX {tf}M SELL {pair}* (#{tid}) [PAPER]\n"
        f"Entry: `{fmt(pair, entry)}`\nSL: `{fmt(pair, sl)}` (+{sl_pips} pips)\n"
        f"TP: `{fmt(pair, tp)}` (-{sl_pips} pips)\nSpread: `{spread_pips:.1f}` pips")
    log(f"{pair} {tf}M [{when}] SELL SIGNAL @ {fmt(pair, entry)} (spread {spread_pips:.1f} pips)")
    return True


# ---------------- MONITOR ----------------
def monitor():
    trades = open_trades()
    if not trades:
        return
    quotes = {}
    for pair in {t["pair"] for t in trades}:
        try:
            quotes[pair] = fetch_quote(pair)
        except Exception as e:
            log_once(f"mon-{pair}", f"{pair} monitor quote failed: {type(e).__name__}: {str(e)[:150]}")

    for t in trades:
        if t["pair"] not in quotes:
            continue
        bid, ask = quotes[t["pair"]]
        pip = pip_of(t["pair"])
        px = ask                                        # a sell closes at the ask
        hit_sl, hit_tp = px >= t["sl"], px <= t["tp"]
        if not (hit_sl or hit_tp):
            continue
        pnl = (t["entry"] - px) / pip
        outcome = "SL" if hit_sl else "TP"
        close_trade(t["id"], outcome, px, pnl)          # recorded at the price the check saw
        icon = "✅" if outcome == "TP" else "❌"
        send_telegram(
            f"{icon} *FX {t['tf']}M SELL {t['pair']} #{t['id']} closed: {outcome}* [PAPER]\n"
            f"Entry `{fmt(t['pair'], t['entry'])}` -> exit `{fmt(t['pair'], px)}`\n"
            f"Result: `{pnl:+.1f}` pips")
        log(f"trade #{t['id']} {t['pair']} {t['tf']}M {outcome} pnl {pnl:+.1f} pips")


# ---------------- SUMMARY ----------------
def summary():
    now = datetime.now(timezone.utc)
    last = get_meta("last_summary_sent")
    if last is None:
        set_meta("last_summary_sent", now.isoformat())
        return
    if (now - datetime.fromisoformat(last)).total_seconds() < SUMMARY_INTERVAL_SECONDS:
        return
    rows = db("""SELECT pair, tf, outcome, pnl_pips FROM trades
                 WHERE status='closed' AND closed_at>=?""", (last,), fetch=True)
    lines = []
    for pair in PAIRS:
        for tf in TIMEFRAMES:
            sub = [r for r in rows if r["pair"] == pair and r["tf"] == tf]
            if not sub:
                continue
            tp = sum(1 for r in sub if r["outcome"] == "TP")
            net = sum(r["pnl_pips"] or 0 for r in sub)
            lines.append(f"{pair} {tf}M: {len(sub)} closed | TP {tp} | SL {len(sub) - tp} | Net `{net:+.1f}` pips")
    total = sum(r["pnl_pips"] or 0 for r in rows)
    body = "\n".join(lines) if lines else "No closed trades."
    send_telegram(f"📊 *Daily Summary FX (PAPER)*\n{body}\nTotal: {len(rows)} closed | Net `{total:+.1f}` pips")
    set_meta("last_summary_sent", now.isoformat())


# ---------------- MAIN ----------------
def main():
    global TOKEN, CHAT, DB_PATH, SUFFIX, API_KEY, API_SECRET, API_PASS, PAIRS
    TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT = os.environ["TELEGRAM_CHAT_ID"]
    API_KEY = os.environ["BITGET_API_KEY"]
    API_SECRET = os.environ["BITGET_API_SECRET"]
    API_PASS = os.environ["BITGET_API_PASSPHRASE"]
    SUFFIX = os.environ.get("SYMBOL_SUFFIX", "")
    DB_PATH = os.environ.get("DB_PATH", "forex_paper.db")
    wanted = [p.strip().upper() for p in os.environ.get("FX_PAIRS", "").split(",") if p.strip()]
    PAIRS = [p for p in PAIR_SETTINGS if not wanted or p in wanted]
    mode = os.environ.get("MODE", "PAPER").upper()
    if mode != "PAPER" or not PAIRS:
        log(f"MODE={mode} / pairs={PAIRS} not supported. Only PAPER with known pairs. Exiting.")
        sys.exit(1)

    init_db()
    for pair in PAIRS:
        while True:
            try:
                bid, ask = fetch_quote(pair)
                BID_SIDE[pair] = detect_bid_side(pair)
                log(f"[SOURCE] {pair}{SUFFIX}: bid {fmt(pair, bid)} ask {fmt(pair, ask)} "
                    f"spread {(ask - bid) / pip_of(pair):.1f} pips | bid candles = '{BID_SIDE[pair]}'")
                break
            except Exception as e:
                log(f"[SOURCE] {pair} failed: {type(e).__name__}: {str(e)[:150]}, retrying in 60s")
                time.sleep(60)

    send_telegram("✅ Forex 3M + 5M bot started [PAPER] (sell only)\n" + "\n".join(
        f"{p}: 3M run-up {PAIR_SETTINGS[p][3][0]} / SL-TP {PAIR_SETTINGS[p][3][1]} | "
        f"5M run-up {PAIR_SETTINGS[p][5][0]} / SL-TP {PAIR_SETTINGS[p][5][1]} pips" for p in PAIRS))
    log("Forex bot running in PAPER mode: " + ", ".join(PAIRS))

    last_scan = {}
    last_summary = 0
    while True:
        try:
            now = time.time()
            for tf in TIMEFRAMES:
                into = now % (tf * 60)
                boundary_ms = int((now - into) * 1000)
                if into >= SCAN_WINDOW_SECONDS[tf]:
                    continue
                for pair in PAIRS:
                    if last_scan.get((pair, tf)) != boundary_ms and scan(pair, tf, boundary_ms):
                        last_scan[(pair, tf)] = boundary_ms
            monitor()
            if time.time() - last_summary >= 3600:
                summary()
                last_summary = time.time()
        except Exception as e:
            log(f"Loop error: {type(e).__name__}: {e}")
        time.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
