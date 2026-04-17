import asyncio
import json
import time
from datetime import datetime
from collections import defaultdict, deque

import aiohttp
import websockets

# =========================
# CONFIG
# =========================

log_file = open("logs.txt", "a", encoding="utf-8")

BINANCE_WS = "wss://stream.binance.com:9443/stream"
BYBIT_WS = "wss://stream.bybit.com/v5/public/spot"

FEE = {
    "binance": 0.001,
    "bybit": 0.001
}

PROFIT_THRESHOLD = 0.001  # 0.5%

LATENCY = {
    "binance": 0.15,
    "bybit": 0.20
}

# =========================
# STATE (ORDERBOOK DEPTH)
# =========================

orderbooks = {
    "binance": defaultdict(lambda: {
        "bids": deque(maxlen=20),
        "asks": deque(maxlen=20),
        "ts": 0
    }),
    "bybit": defaultdict(lambda: {
        "bids": deque(maxlen=20),
        "asks": deque(maxlen=20),
        "ts": 0
    })
}

graph = {
    "binance": defaultdict(dict),
    "bybit": defaultdict(dict)
}

# =========================
# GRAPH REGISTER
# =========================

def register(exchange, base, quote, symbol):
    graph[exchange][base][quote] = (symbol, base, quote)
    graph[exchange][quote][base] = (symbol, base, quote)

# =========================
# ORDERBOOK UPDATE (DEPTH SIMULATION)
# =========================

def update(exchange, symbol, bid, ask):
    ob = orderbooks[exchange][symbol]

    bid = float(bid)
    ask = float(ask)

    spread = max(ask - bid, bid * 0.0001)

    # fake depth ladder (simple but effective)
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
        take = min(size, remaining)
        total += take * price
        remaining -= take
        if remaining <= 0:
            break

    if remaining > 0:
        return None

    return total


def execute_buy(ob, amount):
    cost = 0
    remaining = amount

    for price, size in ob["asks"]:
        take = min(size, remaining)
        cost += take * price
        remaining -= take
        if remaining <= 0:
            break

    if remaining > 0:
        return None

    return cost

# =========================
# CONVERSION ENGINE (UNCHANGED LOGIC FLOW)
# =========================

def convert(exchange, from_asset, to_asset, amount):
    data = graph[exchange][from_asset].get(to_asset)
    if not data:
        return None

    symbol, base, quote = data
    ob = orderbooks[exchange].get(symbol)

    if not ob or is_stale(exchange, symbol):
        return None

    # BUY base using quote
    if from_asset == quote:
        cost = execute_buy(ob, amount)
        if cost is None:
            return None
        return (amount / (cost / amount)) * (1 - FEE[exchange])

    # SELL base for quote
    elif from_asset == base:
        proceeds = execute_sell(ob, amount)
        if proceeds is None:
            return None
        return proceeds * (1 - FEE[exchange])

    return None

# =========================
# TRIANGLE FINDER
# =========================

def find_triangles():
    tris = []

    for ex in ["binance", "bybit"]:
        g = graph[ex]

        if "USDT" not in g:
            continue

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
# SYMBOL FETCHERS
# =========================

async def fetch_binance():
    url = "https://api.binance.com/api/v3/exchangeInfo"

    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            data = await r.json()

    return [
        (x["baseAsset"], x["quoteAsset"], x["symbol"])
        for x in data["symbols"]
        if x["status"] == "TRADING"
    ]


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
# BINANCE WS
# =========================

async def binance_ws(symbols):
    chunk_size = 50

    async def run(chunk):
        while True:
            try:
                streams = "/".join([s[2].lower() + "@bookTicker" for s in chunk])
                url = f"{BINANCE_WS}?streams={streams}"

                async with websockets.connect(url, ping_interval=20) as ws:
                    print(f"[BINANCE] connected {len(chunk)}")

                    async for msg in ws:
                        data = json.loads(msg).get("data")
                        if not data:
                            continue

                        update("binance", data["s"], data["b"], data["a"])

            except Exception as e:
                print("[BINANCE] reconnect", e)
                await asyncio.sleep(2)

    await asyncio.gather(*[run(symbols[i:i+chunk_size]) for i in range(0, len(symbols), chunk_size)])

# =========================
# BYBIT WS
# =========================

async def bybit_ws(symbols):
    while True:
        try:
            async with websockets.connect(BYBIT_WS, ping_interval=20) as ws:
                print("[BYBIT] connected")

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

        except Exception as e:
            print("[BYBIT] reconnect", e)
            await asyncio.sleep(2)

# =========================
# SCANNER
# =========================

async def scanner():
    await asyncio.sleep(5)

    while True:
        await asyncio.sleep(0.2)

        tris = find_triangles()

        for ex in ["binance", "bybit"]:
            for tri in tris:
                result = simulate(ex, tri)

                if not result:
                    continue

                profit = result - 1

                if profit > PROFIT_THRESHOLD:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    log_file.write(f"{now} 🔥 {ex.upper()} {tri} => {result:.6f} | {profit*100:.3f}%\n")
                    log_file.flush()

                    print(f"🔥 {ex.upper()} {tri} => {result:.6f} | {profit*100:.3f}%")

# =========================
# MAIN
# =========================

async def main():
    binance = await fetch_binance()
    bybit = await fetch_bybit()

    for b, q, s in binance:
        register("binance", b, q, s)

    for b, q, s in bybit:
        register("bybit", b, q, s)

    print("Binance:", len(binance))
    print("Bybit:", len(bybit))

    await asyncio.gather(
        binance_ws(binance),
        bybit_ws(bybit),
        scanner()
    )

if __name__ == "__main__":
    asyncio.run(main())