import asyncio
import json
import time
from collections import defaultdict, deque
from datetime import datetime

import aiohttp
import websockets
from dotenv import load_dotenv
import os

load_dotenv()

# =========================
# CONFIG
# =========================

BINANCE_WS = "wss://stream.binance.com:9443/stream"

FEE = 0.001  # Binance taker fee assumption
SLIPPAGE_BUFFER = 0.001
PROFIT_THRESHOLD = 0.005  # 0.5%

LATENCY = 0.15

log_file = open("logs_binance.txt", "a", encoding="utf-8")

# =========================
# ORDERBOOK STATE
# =========================

orderbooks = defaultdict(lambda: {
    "bids": deque(maxlen=20),
    "asks": deque(maxlen=20),
    "ts": 0
})

graph = defaultdict(dict)

# =========================
# GRAPH REGISTER
# =========================

def register(base, quote, symbol):
    graph[base][quote] = (symbol, base, quote)
    graph[quote][base] = (symbol, base, quote)

# =========================
# ORDERBOOK UPDATE (SIMPLIFIED DEPTH MODEL)
# =========================

def update(symbol, bid, ask):
    ob = orderbooks[symbol]

    bid = float(bid)
    ask = float(ask)

    spread = max(ask - bid, bid * 0.0001)

    for i in range(5):
        ob["bids"].append((bid - i * spread * 0.2, 1.0))
        ob["asks"].append((ask + i * spread * 0.2, 1.0))

    ob["ts"] = time.time()


def is_stale(symbol):
    return time.time() - orderbooks[symbol]["ts"] > LATENCY

# =========================
# DEPTH EXECUTION SIM (NO REAL TRADING)
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
# CONVERSION ENGINE
# =========================

def convert(from_asset, to_asset, amount):
    if amount <= 0:
        return None

    data = graph[from_asset].get(to_asset)
    if not data:
        return None

    symbol, base, quote = data
    ob = orderbooks[symbol]

    if is_stale(symbol):
        return None

    fee = FEE

    # BUY
    if from_asset == quote:
        cost = execute_buy(ob, amount)
        if not cost:
            return None

        avg_price = cost / amount
        base_amount = amount / avg_price
        return base_amount * (1 - fee)

    # SELL
    elif from_asset == base:
        proceeds = execute_sell(ob, amount)
        if not proceeds:
            return None

        return proceeds * (1 - fee)

    return None

# =========================
# TRIANGLE FINDER
# =========================

def find_triangles():
    tris = []

    if "USDT" not in graph:
        return tris

    for a in graph["USDT"]:
        for b in graph[a]:
            if "USDT" in graph[b]:
                tris.append(("USDT", a, b, "USDT"))

    return tris

# =========================
# SIMULATION
# =========================

def simulate(tri):
    usdt, a, b, _ = tri

    v = 1.0

    v = convert(usdt, a, v)
    if v is None:
        return None

    v = convert(a, b, v)
    if v is None:
        return None

    v = convert(b, usdt, v)
    if v is None:
        return None

    return v

# =========================
# SYMBOL FETCH
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

# =========================
# WS
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

                        update(data["s"], data["b"], data["a"])

            except Exception as e:
                print("[BINANCE WS ERROR]", e)
                await asyncio.sleep(2)

    await asyncio.gather(*[run(symbols[i:i+chunk_size]) for i in range(0, len(symbols), chunk_size)])

# =========================
# SCANNER
# =========================

async def scanner():
    await asyncio.sleep(5)

    while True:
        await asyncio.sleep(0.2)

        tris = find_triangles()

        for tri in tris:
            result = simulate(tri)

            if not result:
                continue

            net = result - 1 - SLIPPAGE_BUFFER

            if net > PROFIT_THRESHOLD:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                msg = f"🔥 BINANCE {tri} => {result:.6f} | NET {(net*100):.3f}%"
                print(msg)

                log_file.write(f"{now} {msg}\n")
                log_file.flush()

# =========================
# MAIN
# =========================

async def main():
    symbols = await fetch_binance()

    for b, q, s in symbols:
        register(b, q, s)

    print("Binance symbols:", len(symbols))

    await asyncio.gather(
        binance_ws(symbols),
        scanner()
    )

if __name__ == "__main__":
    asyncio.run(main())
