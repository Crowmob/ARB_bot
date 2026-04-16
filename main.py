import asyncio
import json
from datetime import datetime

import aiohttp
import websockets
from collections import defaultdict

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

MIN_PROFIT = 1.001  # 0.1%

# =========================
# STATE
# =========================

orderbooks = {
    "binance": {},
    "bybit": {}
}

graph = {
    "binance": defaultdict(dict),
    "bybit": defaultdict(dict)
}

queue = asyncio.Queue()

# =========================
# REGISTER GRAPH (DIRECTED)
# =========================

def register(exchange, base, quote, symbol):
    # base -> quote
    graph[exchange][base][quote] = symbol
    # quote -> base
    graph[exchange][quote][base] = symbol

# =========================
# UPDATE ORDERBOOK
# =========================

def update(exchange, symbol, bid, ask):
    orderbooks[exchange][symbol] = {
        "bid": float(bid),
        "ask": float(ask)
    }

# =========================
# TRIANGLE FINDER
# =========================

def find_triangles():
    tris = []

    for ex in ["binance", "bybit"]:
        g = graph[ex]

        # ONLY start from USDT
        if "USDT" not in g:
            continue

        for a in g["USDT"]:          # USDT → A
            for b in g[a]:            # A → B
                if "USDT" in g[b]:   # B → USDT
                    tris.append(("USDT", a, b, "USDT"))

    return tris

# =========================
# REAL CONVERSION ENGINE
# =========================

def convert(exchange, from_asset, to_asset, amount):
    symbol = graph[exchange][from_asset].get(to_asset)
    if not symbol:
        return None

    book = orderbooks[exchange].get(symbol)
    if not book:
        return None

    # BUY
    if from_asset == symbol[:len(from_asset)]:
        price = book["ask"]
        return (amount / price) * (1 - FEE[exchange])

    # SELL
    else:
        price = book["bid"]
        return amount * price * (1 - FEE[exchange])

# =========================
# SIMULATE TRIANGLE
# =========================

def simulate(exchange, tri):
    usdt, a, b, _ = tri

    v = 1.0  # start USDT

    # USDT → A
    v = convert(exchange, usdt, a, v)
    if v is None:
        return None

    # A → B
    v = convert(exchange, a, b, v)
    if v is None:
        return None

    # B → USDT
    v = convert(exchange, b, usdt, v)
    if v is None:
        return None

    return v

# =========================
# BINANCE SYMBOLS
# =========================

async def fetch_binance():
    url = "https://api.binance.com/api/v3/exchangeInfo"

    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            data = await r.json()

    out = []
    for x in data["symbols"]:
        if x["status"] != "TRADING":
            continue
        out.append((x["baseAsset"], x["quoteAsset"], x["symbol"]))
    return out

# =========================
# BYBIT SYMBOLS
# =========================

async def fetch_bybit():
    url = "https://api.bybit.com/v5/market/instruments-info?category=spot"

    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            data = await r.json()

    out = []
    for x in data["result"]["list"]:
        if x["status"] != "Trading":
            continue
        out.append((x["baseCoin"], x["quoteCoin"], x["symbol"]))
    return out

# =========================
# BINANCE WS (SAFE CHUNKED)
# =========================

async def binance_ws(symbols):
    chunk_size = 50

    async def run(chunk):
        while True:
            try:
                streams = "/".join([s[2].lower() + "@bookTicker" for s in chunk])
                url = f"{BINANCE_WS}?streams={streams}"

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20
                ) as ws:
                    print(f"[BINANCE] connected {len(chunk)}")

                    async for msg in ws:
                        data = json.loads(msg).get("data")
                        if not data:
                            continue

                        update("binance", data["s"], data["b"], data["a"])

            except Exception as e:
                print(f"[BINANCE] reconnecting... {e}")
                await asyncio.sleep(2)

    await asyncio.gather(*[
        run(symbols[i:i+chunk_size])
        for i in range(0, len(symbols), chunk_size)
    ])

# =========================
# BYBIT WS (10 PER SUB)
# =========================

async def bybit_ws(symbols):
    while True:
        try:
            async with websockets.connect(
                BYBIT_WS,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                print("[BYBIT] connected")

                for i in range(0, len(symbols), 10):
                    chunk = symbols[i:i+10]
                    args = [f"orderbook.1.{s[2]}" for s in chunk]

                    await ws.send(json.dumps({
                        "op": "subscribe",
                        "args": args
                    }))

                    await asyncio.sleep(0.1)  # 🔥 avoid burst spam

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

                    bid = bids[0][0]
                    ask = asks[0][0]

                    update("bybit", symbol, bid, ask)
                    await queue.put(True)

        except Exception as e:
            print(f"[BYBIT] reconnecting... {e}")
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

                profit = (result - 1) * 100

                if result > MIN_PROFIT:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    log_file.write(f"{now} 🔥 {ex.upper()} {tri} => {result:.6f} | {profit:.3f}%\n")
                    log_file.flush()
                    print(f"🔥 {ex.upper()} {tri} => {result:.6f} | {profit:.3f}%")

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