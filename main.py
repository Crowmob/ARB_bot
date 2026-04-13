import asyncio
import json
import requests
import websockets
import random
import threading
import time
from collections import defaultdict
from fastapi import FastAPI
from threading import Lock

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

# =========================
# STATE
# =========================
prices = {}
prices_lock = Lock()

adj = defaultdict(set)
cycles = []
stats = {"opportunities": 0, "last_scan": None}

app = FastAPI()


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
# PRICE UPDATE (THREAD SAFE)
# =========================
def update_price(symbol, base, quote, bid, ask):
    with prices_lock:
        prices[symbol] = {
            "base": base,
            "quote": quote,
            "bid": bid,
            "ask": ask
        }


# =========================
# RATE
# =========================
def get_rate(x, y, snapshot):
    for sym, data in snapshot.items():
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


def simulate_once(a, b, c, snapshot):
    cap = START_CAPITAL

    for x, y in [(a, b), (b, c), (c, a)]:
        rate = get_rate(x, y, snapshot)
        if not rate:
            return None

        rate = shock(rate)

        cap *= rate
        cap *= (1 - FEE)
        cap *= (1 - slippage())

    return cap


def simulate_mc(a, b, c, snapshot):
    results = []

    for _ in range(MONTE_CARLO_RUNS):
        r = simulate_once(a, b, c, snapshot)
        if r:
            results.append(r)

    if not results:
        return None

    return sum(results) / len(results)


# =========================
# SCANNER (SAFE SNAPSHOT)
# =========================
def scan():
    with prices_lock:
        snapshot = dict(prices)

    found = 0

    nodes = list(adj.keys())

    for a in nodes:
        for b in adj[a]:
            for c in adj[b]:
                if c == a:
                    continue

                result = simulate_mc(a, b, c, snapshot)
                if not result:
                    continue

                profit = ((result / START_CAPITAL) - 1) * 100

                if profit > MIN_PROFIT:
                    print(f"🔥 ARB {a}->{b}->{c}->{a} | {profit:.3f}%")
                    found += 1

    stats["opportunities"] = found
    stats["last_scan"] = time.strftime("%H:%M:%S")


# =========================
# WEBSOCKET LOOP
# =========================
async def ws(symbols):
    url = "wss://stream.bybit.com/v5/public/spot"

    args = [f"orderbook.1.{s['symbol']}" for s in symbols]

    symbol_map = {s["symbol"]: s for s in symbols}

    while True:
        try:
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
                    if symbol not in symbol_map:
                        continue

                    ob = data["data"]

                    try:
                        update_price(
                            symbol,
                            symbol_map[symbol]["baseCoin"],
                            symbol_map[symbol]["quoteCoin"],
                            float(ob["b"][0][0]),
                            float(ob["a"][0][0])
                        )
                    except:
                        continue

        except Exception as e:
            print("WS reconnect:", e)
            await asyncio.sleep(5)


# =========================
# THREAD WORKERS
# =========================
def run_ws(symbols):
    asyncio.run(ws(symbols))


def run_scanner():
    while True:
        scan()
        time.sleep(SCAN_INTERVAL)


def start_bot():
    symbols = get_symbols()
    build_graph(symbols)

    threading.Thread(target=run_ws, args=(symbols,), daemon=True).start()
    threading.Thread(target=run_scanner, daemon=True).start()


# =========================
# FASTAPI
# =========================
@app.get("/")
def home():
    return {"status": "running"}


@app.get("/stats")
def get_stats():
    return stats


# =========================
# STARTUP
# =========================
@app.on_event("startup")
def startup():
    print("Starting bot...")
    start_bot()