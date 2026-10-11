import os, time, hmac, hashlib, base64, json, requests
from urllib.parse import urlencode

K, S, P = (os.environ[x] for x in
           ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"))

def get(path, params):
    q = f"{path}?{urlencode(params)}" if params else path
    ts = str(int(time.time() * 1000))
    sign = base64.b64encode(hmac.new(S.encode(), (ts + "GET" + q).encode(),
                            hashlib.sha256).digest()).decode()
    h = {"ACCESS-KEY": K, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts,
         "ACCESS-PASSPHRASE": P, "Content-Type": "application/json"}
    return requests.get("https://api.bitget.com" + q, headers=h, timeout=15).json()

def find_items(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                return v
    return []

res = get("/api/v3/cfd/account/instruments", {})
items = find_items(res.get("data"))
print("code:", res.get("code"), res.get("msg"), "| instruments:", len(items))
if items:
    print("fields:", list(items[0].keys()) if isinstance(items[0], dict) else items[0])
names = [str(i.get("symbol")) for i in items if isinstance(i, dict)]
print("ALL SYMBOLS:", ", ".join(names))

wanted = ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "EURGBP", "AUDNZD", "EURJPY")
for i in items:
    if isinstance(i, dict) and any(str(i.get("symbol")).startswith(w) for w in wanted):
        print("INSTRUMENT:", json.dumps(i))
for w in wanted:
    for sym in (w, w + ".s", w + ".pro"):
        r = get("/api/v3/cfd/market/tickers", {"symbol": sym})
        d = r.get("data")
        d = d[0] if isinstance(d, list) and d else d
        if r.get("code") == "00000" and isinstance(d, dict):
            try:
                print(f"TICKER {sym}: bid {d['bid1']} ask {d['ask1']} spread {float(d['ask1']) - float(d['bid1']):.5f}")
            except Exception:
                print("TICKER", sym, json.dumps(d)[:200])
