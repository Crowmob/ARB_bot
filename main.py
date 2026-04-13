import asyncio
import json
import requests
import websockets
import random
import threading
import os
from collections import defaultdict
from fastapi import FastAPI

# =========================
# CONFIG
# =========================
FEE = 0.001
BASE_SLIPPAGE = 0.001
VOLATILITY = 0.0015
MONTE_CARLO_RUNS = 3
START_CAPITAL = 1000
SCAN_INTERVAL = 0.5
MIN_PROFIT = 0.3

prices = {}
adj = defaultdict(set)
cycles = []
edge_to_cycles = defaultdict(list)
dirty_edges = set()

app = FastAPI()

# =========================
# SIMPLE STATS STORE
# =========================
stats = {
    "opportunities": 0,
    "last_scan": None
}

# =========================
# SYMBOLS
# =========================
def get_symbols():
    url = "https://api.bybit.com/v5/market/instruments-info"
    data = requests.get(url, params={"category": "spot"}).json()
    return data["result"]["list"]

# =========================
# GRAPH
# =========================
def build_graph(symbols):
    for s in symbols:
        base = s["baseCoin"]
        quote = s["quoteCoin"]

        adj[base].add(quote)
        adj[quote].add(base)

# =========================
# PRICE UPDATE
# =========================
def update_price(symbol, base, quote, bid, ask):
    prices[symbol] = {
        "base": base,
        "quote": quote,
        "bid": bid,
        "ask": ask
    }
    dirty_edges.add(frozenset((base, quote)))

# =========================
# RATE
# =========================
def get_rate(x, y):
    for sym, data in prices.items():
        if data["base"] == x and data["quote"] == y:
            return data["bid"]
        if data["base"] == y and data["quote"] == x:
            return 1 / data["ask"]
    return None

# =========================
# SIMULATION
# =========================
def slippage():
    return BASE_SLIPPAGE * random.uniform(0.5, 1.5)

def shock(v):
    return v * (1 + random.uniform(-VOLATILITY, VOLATILITY))

def simulate_once(a, b, c):
    cap = START_CAPITAL

    for x, y in [(a, b), (b, c), (c, a)]:
        rate = get_rate(x, y)
        if not rate:
            return None

        rate = shock(rate)

        cap *= rate
        cap *= (1 - FEE)
        cap *= (1 - slippage())

    return cap

def simulate_mc(a, b, c):
    res = []

    for _ in range(MONTE_CARLO_RUNS):
        r = simulate_once(a, b, c)
        if r:
            res.append(r)

    if not res:
        return None

    return sum(res) / len(res)

# =========================
# SCANNER
# =========================
def scan():
    found = 0

    for edge in list(dirty_edges):
        for sym, data in prices.items():
            pass

    for sym in list(prices.keys()):
        a = prices[sym]["base"]
        b = prices[sym]["quote"]

        for c in adj[b]:
            if c == a:
                continue

            result = simulate_mc(a, b, c)
            if not result:
                continue

            profit = ((result / START_CAPITAL) - 1) * 100

            if profit > MIN_PROFIT:
                print(f"🔥 ARB {a}->{b}->{c}->{a} | {profit:.3f}%")
                found += 1

    stats["opportunities"] = found

# =========================
# WEBSOCKET
# =========================
async def ws(symbols):
    url = "wss://stream.bybit.com/v5/public/spot"

    args = [f"orderbook.1.{s['symbol']}" for s in symbols]

    async with websockets.connect(url) as ws:
        print("WebSocket connected")

        for i in range(0, len(args), 10):
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": args[i:i+10]
            }))

        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            if "topic" not in data:
                continue

            symbol = data["topic"].split(".")[-1]
            ob = data["data"]

            try:
                update_price(
                    symbol,
                    symbol.split("USDT")[0] if "USDT" in symbol else symbol[:3],
                    "USDT",
                    float(ob["b"][0][0]),
                    float(ob["a"][0][0])
                )
            except:
                continue

# =========================
# LOOP
# =========================
def start_bot():
    symbols = get_symbols()
    build_graph(symbols)

    def run_ws():
        asyncio.run(ws(symbols))

    def run_scanner():
        while True:
            scan()
            stats["last_scan"] = str(asyncio.get_event_loop().time())
            asyncio.sleep(SCAN_INTERVAL)

    threading.Thread(target=run_ws, daemon=True).start()
    threading.Thread(target=run_scanner, daemon=True).start()

# =========================
# FASTAPI ROUTES (RENDER NEEDS THIS)
# =========================
@app.get("/")
def home():
    return {"status": "running"}

@app.get("/stats")
def get_stats():
    return stats

# =========================
# START BOT ON SERVER BOOT
# =========================
@app.on_event("startup")
def startup():
    print("Starting bot...")
    threading.Thread(target=start_bot, daemon=True).start()