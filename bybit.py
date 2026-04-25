import asyncio
import json
import time
from datetime import datetime
from collections import defaultdict, deque
from pybit.unified_trading import HTTP
import aiohttp
import websockets
from dotenv import load_dotenv
import os

load_dotenv()

# =========================
# CONFIG
# =========================

session = HTTP(
    testnet=False,
    api_key=os.getenv("API_KEY"),
    api_secret=os.getenv("API_SECRET"),
    recv_window=15000,
)

initial = session.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]["totalEquity"]
print(initial)

log_file = open("logs_bybit.txt", "a", encoding="utf-8")

BYBIT_WS = "wss://stream.bybit.com/v5/public/spot"

FEE = {
    "bybit": 0.001
}

FEE_TOTAL = 0.003
SLIPPAGE_BUFFER = 0.001

PROFIT_THRESHOLD = 0.005  # 0.5%

LATENCY = {
    "bybit": 0.20
}

# =========================
# STATE (ORDERBOOK DEPTH)
# =========================

orderbooks = {
    "bybit": defaultdict(lambda: {
        "bids": deque(maxlen=20),
        "asks": deque(maxlen=20),
        "ts": 0
    })
}

graph = {
    "bybit": defaultdict(dict)
}

# =========================
# GRAPH REGISTER
# =========================

def register(exchange, base, quote, symbol):
    graph[exchange][base][quote] = (symbol, base, quote)
    graph[exchange][quote][base] = (symbol, base, quote)

# =========================
# ORDERBOOK UPDATE
# =========================

def update(exchange, symbol, bid, ask):
    ob = orderbooks[exchange][symbol]

    bid = float(bid)
    ask = float(ask)

    spread = max(ask - bid, bid * 0.0001)

    for i in range(5):
        ob["bids"].append((bid - i * spread * 0.2, 1.0))
        ob["asks"].append((ask + i * spread * 0.2, 1.0))

    ob["ts"] = time.time()

# =========================
# LATENCY CHECK
# =========================

def is_stale(exchange, symbol):
    return time.time() - orderbooks[exchange][symbol]["ts"] > LATENCY[exchange]

# =========================
# DEPTH EXECUTION ENGINE
# =========================

def execute_sell(ob, amount):
    total = 0
    remaining = amount

    for price, size in ob["bids"]:
        if price <= 0 or size <= 0:
            continue

        take = min(size, remaining)
        total += take * price
        remaining -= take

        if remaining <= 0:
            break

    if remaining > 0 or total <= 0:
        return None

    return total


def execute_buy(ob, amount):
    cost = 0
    remaining = amount

    for price, size in ob["asks"]:
        if price <= 0 or size <= 0:
            continue

        take = min(size, remaining)
        cost += take * price
        remaining -= take

        if remaining <= 0:
            break

    if remaining > 0 or cost <= 0:
        return None

    return cost

# =========================
# CONVERSION ENGINE
# =========================

def convert(exchange, from_asset, to_asset, amount):
    if amount is None or amount <= 0:
        return None

    data = graph[exchange][from_asset].get(to_asset)
    if not data:
        return None

    symbol, base, quote = data
    ob = orderbooks[exchange].get(symbol)

    if not ob or is_stale(exchange, symbol):
        return None

    fee = FEE[exchange]

    if from_asset == quote:
        cost = execute_buy(ob, amount)

        if cost is None or cost <= 0:
            return None

        avg_price = cost / amount

        if avg_price <= 0:
            return None

        base_amount = amount / avg_price

        return base_amount * (1 - fee)

    elif from_asset == base:
        proceeds = execute_sell(ob, amount)
        if proceeds is None:
            return None

        return proceeds * (1 - fee)

    return None

# =========================
# TRIANGLE FINDER
# =========================

def find_triangles():
    tris = []

    g = graph["bybit"]

    if "USDT" not in g:
        return tris

    for a in g["USDT"]:
        for b in g[a]:
            if "USDT" in g[b]:
                tris.append(("USDT", a, b, "USDT"))

    return tris

# =========================
# SIMULATE
# =========================

def simulate(exchange, tri):
    usdt, a, b, _ = tri

    v = 1.0

    v = convert(exchange, usdt, a, v)
    if v is None:
        return None

    v = convert(exchange, a, b, v)
    if v is None:
        return None

    v = convert(exchange, b, usdt, v)
    if v is None:
        return None

    return v

# =========================
# BYBIT SYMBOL FETCH
# =========================

async def fetch_bybit():
    url = "https://api.bybit.com/v5/market/instruments-info?category=spot"

    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            data = await r.json()

    return [
        (x["baseCoin"], x["quoteCoin"], x["symbol"])
        for x in data["result"]["list"]
        if x["status"] == "Trading"
    ]

# =========================
# BYBIT WS
# =========================

async def bybit_ws(symbols):
    while True:
        try:
            async with websockets.connect(BYBIT_WS, ping_interval=20, ping_timeout=20) as ws:
                for i in range(0, len(symbols), 10):
                    chunk = symbols[i:i+10]
                    args = [f"orderbook.1.{s[2]}" for s in chunk]

                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.1)

                async for msg in ws:
                    data = json.loads(msg)

                    if "topic" not in data:
                        continue

                    d = data.get("data")
                    if not d:
                        continue

                    symbol = data["topic"].split(".")[-1]
                    bids = d.get("b")
                    asks = d.get("a")

                    if not bids or not asks:
                        continue

                    update("bybit", symbol, bids[0][0], asks[0][0])

                    await asyncio.sleep(0)

        except:
            await asyncio.sleep(2)



# =========================
# SCANNER
# =========================

def safe_float(x):
    try:
        return float(x)
    except:
        return 0.0


async def execute_triangle_bybit(tri):
    try:
        balance_data = session.get_wallet_balance(accountType="UNIFIED")
        coins = balance_data["result"]["list"][0]["coin"]

        def get_balance(coins, asset):
            for c in coins:
                if c["coin"] == asset:
                    return float(
                        c.get("availableToTrade") or
                        c.get("walletBalance") or
                        c.get("equity") or 0
                    )
            return 0.0

        usdt_balance = get_balance(coins, "USDT")

        if usdt_balance <= 0:
            print("No USDT balance")
            return

        start_amount = min(5.0, usdt_balance * 0.95)  # don't use full balance
        if start_amount < 1:
            print("Balance too small")
            return

        amount = start_amount
        print(f"🚀 EXECUTING TRIANGLE with {amount:.4f} USDT")

        path = [(tri[0], tri[1]), (tri[1], tri[2]), (tri[2], tri[3])]

        for step, (from_asset, to_asset) in enumerate(path):
            data = graph["bybit"][from_asset].get(to_asset)
            if not data:
                print("Pair not found")
                return

            symbol, base, quote = data
            ob = orderbooks["bybit"][symbol]

            if not ob["bids"] or not ob["asks"]:
                print("No orderbook")
                return

            # ===== SAFETY BUFFER =====
            amount *= 0.995  # fees + slippage protection

            if from_asset == quote:
                # BUY
                side = "Buy"
                price = ob["asks"][0][0]

                if price <= 0:
                    return

                qty = amount / price

            else:
                # SELL
                side = "Sell"
                qty = amount

            # ===== ROUND DOWN (CRITICAL) =====
            qty = safe_float(f"{qty:.6f}")  # truncate, not round up

            if qty <= 0:
                print("Qty too small")
                return

            print(f"{side} {symbol} qty={qty}")

            try:
                order = session.place_order(
                    category="spot",
                    symbol=symbol,
                    side=side,
                    orderType="Market",
                    qty=str(qty),
                )
            except Exception as e:
                print("❌ ORDER EXCEPTION:", e)
                return

            if not order or order.get("retCode") != 0:
                print("❌ ORDER FAILED:", order)
                return

            print("ORDER:", order)

            # ===== WAIT FOR BALANCE UPDATE =====
            await asyncio.sleep(0.5)

            # refresh balances
            balance_data = session.get_wallet_balance(accountType="UNIFIED")
            coins = balance_data["result"]["list"][0]["coin"]

            new_amount = get_balance(to_asset)

            print(f"➡️ Received {new_amount:.6f} {to_asset}")

            if new_amount <= 0:
                print("Execution failed mid-path")
                return

            amount = new_amount

        print("✅ TRIANGLE COMPLETED")

    except Exception as e:
        print("EXECUTION ERROR:", e)

async def scanner():
    await asyncio.sleep(5)

    while True:
        await asyncio.sleep(0.2)

        tris = find_triangles()

        for tri in tris:
            result = simulate("bybit", tri)

            if not result:
                continue

            net_profit = result - 1 - SLIPPAGE_BUFFER
            if net_profit > PROFIT_THRESHOLD:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                print(f"🔥 BYBIT {tri} => {result:.6f} | NET {(net_profit*100):.3f}%")

                log_file.write(f"🔥 {now}: BYBIT {tri} => {result:.6f} | NET {(net_profit*100):.3f}%\n")
                log_file.flush()

                res = await execute_triangle_bybit(tri)
                print(initial, session.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]["totalEquity"])

                if res != "not supported":
                    quit()
                else:
                    print(res)

# =========================
# MAIN
# =========================

async def main():
    bybit = await fetch_bybit()

    for b, q, s in bybit:
        register("bybit", b, q, s)

    print("Bybit:", len(bybit))

    CHUNK_SIZE = 60
    chunks = [bybit[i:i+CHUNK_SIZE] for i in range(0, len(bybit), CHUNK_SIZE)]

    print(f"Starting {len(chunks)} WS connections...")

    await asyncio.gather(
        *[bybit_ws(chunk) for chunk in chunks],
        scanner()
    )

if __name__ == "__main__":
    asyncio.run(main())