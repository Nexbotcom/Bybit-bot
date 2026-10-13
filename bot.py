import os, time, hmac, hashlib, base64, json, requests
from urllib.parse import urlencode

K, S, P = (os.environ[x] for x in
           ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"))
PAIRS = ["EURUSD", "USDJPY", "USDCAD", "GBPUSD", "USDCHF", "AUDUSD"]
WANT = 1500                       # about one trading day of 1m candles


def api(path, params):
    q = f"{path}?{urlencode(params)}"
    ts = str(int(time.time() * 1000))
    sign = base64.b64encode(hmac.new(S.encode(), (ts + "GET" + q).encode(),
                            hashlib.sha256).digest()).decode()
    h = {"ACCESS-KEY": K, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts,
         "ACCESS-PASSPHRASE": P, "Content-Type": "application/json"}
    j = requests.get("https://api.bitget.com" + q, headers=h, timeout=15).json()
    if j.get("code") != "00000":
        raise RuntimeError(f"{j.get('code')}: {j.get('msg')}")
    return j["data"]


def pip_size(sym):
    return 0.01 if "JPY" in sym else 0.0001


def candles_1m(sym, side, want):
    """Newest `want` 1m candles from the last 6 days, paged backwards. Returns {ts: [o,h,l,c]}."""
    now = int(time.time() * 1000)
    start, end = now - 6 * 86400000, now + 60000
    rows = {}
    for _ in range(40):
        data = api("/api/v3/cfd/market/history-candlestick", {
            "symbol": sym, "interval": "1m", "side": side,
            "startTime": str(start), "endTime": str(end), "limit": "100"})
        if not data:
            break
        got = {int(r[0]): [float(x) for x in r[1:5]] for r in data}
        rows.update(got)
        oldest = min(got)
        if len(rows) >= want or len(got) < 100 or oldest >= end:
            break
        end = oldest
        time.sleep(0.12)
    keep = sorted(rows)[-want:]
    return {t: rows[t] for t in keep}


def build(rows, tf):
    ms = tf * 60000
    buckets = {}
    for t, (o, h, l, c) in rows.items():
        buckets.setdefault(t - t % ms, []).append((t, o, h, l, c))
    out = []
    for st in sorted(buckets):
        g = sorted(buckets[st])
        if [x[0] for x in g] != [st + k * 60000 for k in range(tf)]:
            continue
        out.append((st, g[0][1], max(x[2] for x in g), min(x[3] for x in g), g[-1][4]))
    return out


def pct(vals, p):
    v = sorted(vals)
    return v[min(len(v) - 1, int(len(v) * p / 100))] if v else float("nan")


def report(sym, sell, buy):
    pip = pip_size(sym)
    sp = [(buy[t][3] - sell[t][3]) / pip for t in sell if t in buy]
    sp_med = pct(sp, 50)
    print(f"{sym}: {len(sell)} one-minute candles | spread pips: median {sp_med:.2f}, "
          f"p90 {pct(sp, 90):.2f}")
    for tf in (3, 5):
        cs = build(sell, tf)
        rng = [(c[2] - c[3]) / pip for c in cs]
        ups, downs = [], []
        for i in range(12, len(cs)):
            if cs[i][0] - cs[i - 12][0] != 12 * tf * 60000:
                continue
            closes = [c[4] for c in cs[i - 12:i]]
            ups.append((cs[i][4] - min(closes)) / pip)
            downs.append((max(closes) - cs[i][4]) / pip)
        if not rng or not ups:
            print(f"  {tf}M: not enough data")
            continue
        avg = sum(rng) / len(rng)
        print(f"  {tf}M: {len(cs)} candles | avg range {avg:.2f} pips, median {pct(rng, 50):.2f} | "
              f"spread/avg range {100 * sp_med / avg:.0f}% | "
              f"12-candle RUN-UP pips p80 {pct(ups, 80):.1f} p90 {pct(ups, 90):.1f} p95 {pct(ups, 95):.1f}")


print("=== INSTRUMENT DETAILS ===")
try:
    data = api("/api/v3/cfd/account/instruments", {})
    items = data if isinstance(data, list) else next((v for v in data.values() if isinstance(v, list)), [])
    for i in items:
        if isinstance(i, dict) and str(i.get("symbol")) in PAIRS:
            print(json.dumps(i))
except Exception as e:
    print("instruments failed:", e)

print("=== VOLATILITY AND SPREAD SURVEY (latest available session) ===")
for sym in PAIRS:
    try:
        sell = candles_1m(sym, "sell", WANT)
        buy = candles_1m(sym, "buy", WANT)
        report(sym, sell, buy)
    except Exception as e:
        print(sym, "failed:", e)
