import asyncio
import json
import requests
import websockets
import random
from collections import defaultdict

# =========================
# CONFIG
# =========================
FEE = 0.001
BASE_SLIPPAGE = 0.001
VOLATILITY = 0.0015
MONTE_CARLO_RUNS = 3
START_CAPITAL = 1000
SCAN_INTERVAL = 0.2
MIN_PROFIT = 0.3  # lower realistic threshold

# symbol -> {bid, ask, base, quote}
prices = {}

# graph: asset -> neighbors
adj = defaultdict(set)

cycles = []
edge_to_cycles = defaultdict(list)
dirty_edges = set()


# =========================
# SYMBOLS (REAL STRUCTURE)
# =========================
def get_symbols():
    url = "https://api.bybit.com/v5/market/instruments-info"
    r = requests.get(url, params={"category": "spot"}).json()

    symbols = []

    for item in r["result"]["list"]:
        symbols.append({
            "symbol": item["symbol"],
            "base": item["baseCoin"],
            "quote": item["quoteCoin"]
        })

    return symbols


# =========================
# GRAPH BUILD (CORRECT)
# =========================
def build_graph(symbols):
    for s in symbols:
        base = s["base"]
        quote = s["quote"]

        adj[base].add(quote)
        adj[quote].add(base)


# =========================
# CYCLE BUILD
# =========================
def build_cycles():
    global cycles
    nodes = list(adj.keys())

    for a in nodes:
        for b in adj[a]:
            for c in adj[b]:
                if c != a and a in adj[c]:
                    cycles.append((a, b, c))

    # index cycles by edges
    for i, (a, b, c) in enumerate(cycles):
        edge_to_cycles[frozenset((a, b))].append(i)
        edge_to_cycles[frozenset((b, c))].append(i)
        edge_to_cycles[frozenset((c, a))].append(i)


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
# RATE CALCULATION (CORRECT DIRECTION)
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
    results = []

    for _ in range(MONTE_CARLO_RUNS):
        r = simulate_once(a, b, c)
        if r:
            results.append(r)

    if not results:
        return None

    return sum(results) / len(results)


# =========================
# SCAN ENGINE
# =========================
def scan():
    checked = set()
    found = 0

    for edge in list(dirty_edges):
        idxs = edge_to_cycles.get(edge, [])

        for idx in idxs:
            if idx in checked:
                continue

            checked.add(idx)
            a, b, c = cycles[idx]

            result = simulate_mc(a, b, c)
            if not result:
                continue

            profit = ((result / START_CAPITAL) - 1) * 100

            if profit > MIN_PROFIT:
                print(f"🔥 ARB {a}->{b}->{c}->{a} | {profit:.3f}%")
                found += 1

    dirty_edges.clear()

    if found:
        print(f"--- {found} opportunities ---")


# =========================
# WEBSOCKET
# =========================
def chunk(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


async def ws(symbols):
    url = "wss://stream.bybit.com/v5/public/spot"

    args = [f"orderbook.1.{s['symbol']}" for s in symbols]

    symbol_map = {s["symbol"]: s for s in symbols}

    async with websockets.connect(url) as ws:
        print("WebSocket connected")

        for batch in chunk(args, 10):
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": batch
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
                    symbol_map[symbol]["base"],
                    symbol_map[symbol]["quote"],
                    float(ob["b"][0][0]),
                    float(ob["a"][0][0])
                )
            except:
                continue


# =========================
# SCANNER LOOP
# =========================
async def scanner():
    while True:
        scan()
        await asyncio.sleep(SCAN_INTERVAL)


# =========================
# MAIN
# =========================
async def main():
    print("Loading symbols...")
    symbols = get_symbols()

    print("Building graph...")
    build_graph(symbols)

    print("Building cycles...")
    build_cycles()

    print("Cycles found:", len(cycles))

    await asyncio.gather(
        ws(symbols),
        scanner()
    )


asyncio.run(main())