# NEW file: order_test.py  (safe order test: places ONE limit sell far above the market, then cancels it)
import os, time, hmac, hashlib, base64, json, requests
from urllib.parse import urlencode

K, S, P = (os.environ[x] for x in
           ("BITGET_LIVE_KEY", "BITGET_LIVE_SECRET", "BITGET_LIVE_PASSPHRASE"))
SYMBOL = os.environ.get("CFD_SYMBOL", "XAUUSD")
DISTANCE = float(os.environ.get("LIVE_TEST_DISTANCE", "30"))       # dollars above the ask
USE_TPSL = os.environ.get("LIVE_TEST_TPSL", "YES").upper() == "YES"
BASE = "https://api.bitget.com"


def call(method, path, params=None, body=None):
    q = path + ("?" + urlencode(params) if params else "")
    body_s = json.dumps(body, separators=(",", ":")) if body is not None else ""
    ts = str(int(time.time() * 1000))
    sign = base64.b64encode(hmac.new(S.encode(), (ts + method + q + body_s).encode(),
                                     hashlib.sha256).digest()).decode()
    headers = {"ACCESS-KEY": K, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts,
               "ACCESS-PASSPHRASE": P, "Content-Type": "application/json", "locale": "en-US"}
    try:
        r = requests.request(method, BASE + q, headers=headers,
                             data=body_s if body_s else None, timeout=15)
        return r.json()
    except Exception as e:
        return {"code": "ERR", "msg": f"{type(e).__name__}: {str(e)[:150]}"}


def rows(res):
    d = res.get("data")
    if isinstance(d, dict):
        d = d.get("list")
    return d if isinstance(d, list) else []


def show(label, res):
    print(f"{label}: code {res.get('code')} | {res.get('msg')} | data {json.dumps(res.get('data'))[:300]}")


def run():
    if os.environ.get("LIVE_TEST_CONFIRM", "").upper() != "YES":
        print("LIVE_TEST_CONFIRM is not YES, so nothing was sent.")
        return

    print("=== 1. quote ===")
    t = call("GET", "/api/v3/cfd/market/tickers", {"symbol": SYMBOL})
    d = t.get("data")
    d = d[0] if isinstance(d, list) and d else d
    if not isinstance(d, dict) or "ask1" not in d:
        show("ticker failed", t)
        return
    ask, bid = float(d["ask1"]), float(d["bid1"])
    age = time.time() * 1000 - float(d.get("quoteTime", 0))
    print(f"bid {bid} ask {ask} quote age {age / 1000:.0f}s")
    if age > 60000:
        print("The quote is old, so the market looks closed. Test stopped. Run it when gold is open.")
        return

    print("=== 2. nothing else may be open ===")
    pos = rows(call("GET", "/api/v3/cfd/trade/current-positions", {"symbol": SYMBOL}))
    old = rows(call("GET", "/api/v3/cfd/trade/unfilled-order", {"symbol": SYMBOL}))
    print(f"open positions: {len(pos)} | pending orders: {len(old)}")
    if pos or old:
        print("You already have a position or order on this symbol. Test stopped so nothing else is touched.")
        return

    print("=== 3. place ONE limit sell far above the market ===")
    price = round(ask + DISTANCE, 2)
    body = {"symbol": SYMBOL, "orderType": "limit", "side": "sell", "qty": "0.01",
            "price": f"{price:.2f}"}
    if USE_TPSL:
        body["takeProfit"] = f"{price - 3:.2f}"
        body["stopLoss"] = f"{price + 3:.2f}"
    print("sending:", json.dumps(body))
    res = call("POST", "/api/v3/cfd/trade/place-order", body=body)
    show("place-order", res)
    trx = (res.get("data") or {}).get("trxId") if isinstance(res.get("data"), dict) else None
    if res.get("code") != "00000" or not trx:
        print("The order was NOT placed. Read the message above and send it to me.")
        return

    time.sleep(1.5)
    print("=== 4. find the order ===")
    pending = rows(call("GET", "/api/v3/cfd/trade/unfilled-order", {"symbol": SYMBOL}))
    mine = [o for o in pending if o.get("trxId") == trx] or pending[:1]
    print("pending:", json.dumps(pending)[:400])
    order_id = mine[0].get("orderId") if mine else None

    print("=== 5. cancel it ===")
    if order_id:
        show("cancel-order", call("POST", "/api/v3/cfd/trade/cancel-order", body={"orderId": order_id}))
    else:
        print("Order not in the pending list. Checking positions next.")
    time.sleep(1.5)

    print("=== 6. verify ===")
    pending = rows(call("GET", "/api/v3/cfd/trade/unfilled-order", {"symbol": SYMBOL}))
    pos = rows(call("GET", "/api/v3/cfd/trade/current-positions", {"symbol": SYMBOL}))
    if pending:
        print("Still pending, trying cancel-all for this symbol.")
        show("cancel-all", call("POST", "/api/v3/cfd/trade/cancel-all", body={"symbol": SYMBOL}))
        time.sleep(1.5)
        pending = rows(call("GET", "/api/v3/cfd/trade/unfilled-order", {"symbol": SYMBOL}))
    for p in pos:
        print("A POSITION EXISTS (the order filled). Closing it at market:", json.dumps(p)[:300])
        show("close-positions", call("POST", "/api/v3/cfd/trade/close-positions",
                                     body={"positionId": p.get("positionId"), "qty": p.get("qty")}))
    hist = rows(call("GET", "/api/v3/cfd/trade/history-order", {"symbol": SYMBOL, "limit": "3"}))
    print("recent orders:", json.dumps(hist)[:500])
    if not pending and not pos:
        print("RESULT: PASS. The order was placed and cancelled, and nothing is left open.")
    else:
        print("RESULT: CHECK THE BITGET APP NOW. Something may still be open: cancel any pending order "
              "and close any position by hand.")


if __name__ == "__main__":
    run()
    print("Done. Delete this Railway service now so it cannot run again.")
    while True:                       # stay idle so Railway does not restart the script
        time.sleep(3600)
