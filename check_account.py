import os, time, hmac, hashlib, base64, json, requests
from urllib.parse import urlencode

K, S, P = (os.environ[x] for x in
           ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"))
SYMBOL = os.environ.get("CFD_SYMBOL", "XAUUSD")


def get(path, params=None):
    q = path + ("?" + urlencode(params) if params else "")
    ts = str(int(time.time() * 1000))
    sign = base64.b64encode(hmac.new(S.encode(), (ts + "GET" + q).encode(),
                            hashlib.sha256).digest()).decode()
    h = {"ACCESS-KEY": K, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts,
         "ACCESS-PASSPHRASE": P, "Content-Type": "application/json", "locale": "en-US"}
    try:
        return requests.get("https://api.bitget.com" + q, headers=h, timeout=15).json()
    except Exception as e:
        return {"error": str(e)}


def items_of(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


print("=== INSTRUMENT", SYMBOL, "===")
r = get("/api/v3/cfd/account/instruments")
print("code:", r.get("code"), r.get("msg"), r.get("error", ""))
found = [i for i in items_of(r.get("data")) if isinstance(i, dict) and i.get("symbol") == SYMBOL]
print(json.dumps(found[0], indent=1) if found else "not found; first 300 chars: " + json.dumps(r)[:300])

print("=== FUND DETAIL ===")
r = get("/api/v3/cfd/account/fund-detail")
print(json.dumps(r)[:700])

print("=== CURRENT POSITIONS ===")
r = get("/api/v3/cfd/trade/current-positions", {"symbol": SYMBOL})
print(json.dumps(r)[:400])
