"""
Topological Alpha Engine — Flask + yfinance + WebSocket
Run: python server.py
Then open: http://localhost:5000
"""

import eventlet
eventlet.monkey_patch()

import json
import math
import os
import threading
import time
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit

# ── Config ─────────────────────────────────────────────────────────────────────
TICKERS = [
        # Mega-cap tech
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA",
        # Semiconductors
        "AMD","INTC","QCOM","AVGO","MU","AMAT","LRCX","KLAC","ARM",
        # Software / Cloud
        "ORCL","ADBE","CRM","NOW","INTU","PANW","CRWD","SNOW","DDOG","NET","PLTR",
        # Consumer Internet
        "NFLX","UBER","ABNB","SHOP","COIN","RBLX","SNAP","PINS","SPOT",
        # Financials
        "JPM","GS","BAC","MS","WFC","C","V","MA","PYPL","AXP","BLK","SCHW",
        # Energy
        "XOM","CVX","COP","SLB","EOG","MPC",
        # Healthcare / Pharma / Biotech
        "JNJ","UNH","PFE","MRK","ABBV","LLY","AMGN","GILD","MRNA","REGN",
        # Consumer
        "WMT","COST","TGT","HD","NKE","SBUX","MCD","CMG",
        # Industrials / Defence
        "GE","BA","CAT","HON","UPS","LMT","RTX",
]
REFRESH_SECONDS = 60 # how often to rerun the pipeline live
PORTFOLIO_FILE = "portfolio.json"
LONG_ALLOCATION = 0.80 # 80% of capital deployed in longs; 20% cash buffer
PORT = int(os.environ.get("PORT", 5000))

app = Flask(__name__)
app.config["SECRET_KEY"] = "topo-alpha-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ── Portfolio Engine ────────────────────────────────────────────────────────────

_port_lock = threading.Lock()

def _load_portfolio():
        if os.path.exists(PORTFOLIO_FILE):
                    try:
                                    with open(PORTFOLIO_FILE, encoding="utf-8") as f:
                                                        return json.load(f)
                    except Exception:
                                    pass
                            return {
                    "cash": 100_000.0,
                    "positions": {},
                    "trades": [],
                    "initial_value": 100_000.0,
                    "created": datetime.now().isoformat(),
                    }

    _portfolio = _load_portfolio()

def _save_portfolio():
        with open(PORTFOLIO_FILE, "w", encoding="utf-8") as f:
                    json.dump(_portfolio, f, indent=2)

    def _get_price(quotes, ticker, fallback):
            return (quotes.get(ticker) or {}).get("price") or fallback

def _portfolio_snapshot(quotes):
        pos_val = 0.0
        positions = []
        for t, p in _portfolio["positions"].items():
                    price = _get_price(quotes, t, p["entry_price"])
                    val = p["shares"] * price
                    cost = p["shares"] * p["entry_price"]
                    pnl = val - cost
                    pos_val += val
                    positions.append({
                        "ticker": t,
                        "shares": round(p["shares"], 4),
                        "entry_price": round(p["entry_price"], 2),
                        "current_price": round(price, 2),
                        "value": round(val, 2),
                        "pnl": round(pnl, 2),
                        "pnl_pct": round(pnl / cost * 100 if cost else 0, 2),
                    })
                positions.sort(key=lambda x: -x["value"])
    total = _portfolio["cash"] + pos_val
    total_pnl = total - _portfolio["initial_value"]
    return {
                "cash": round(_portfolio["cash"], 2),
                "positions_value": round(pos_val, 2),
                "total_value": round(total, 2),
                "total_pnl": round(total_pnl, 2),
                "total_pnl_pct": round(total_pnl / _portfolio["initial_value"] * 100, 2),
                "num_positions": len(positions),
                "positions": positions,
                "trades": list(reversed(_portfolio["trades"][-100:])),
                "updated": datetime.now().strftime("%H:%M:%S"),
    }

def execute_trades(signals, quotes):
        with _port_lock:
                    target = {s["ticker"]: s["weight"] for s in signals["long"]}

        # 1. Exit positions no longer in signal set
        for t in list(_portfolio["positions"]):
                        if t not in target:
                                            price = _get_price(quotes, t, _portfolio["positions"][t]["entry_price"])
                                            shares = _portfolio["positions"][t]["shares"]
                                            proceeds = shares * price
                                            _portfolio["cash"] += proceeds
                                            _portfolio["trades"].append({
                                                "time": datetime.now().strftime("%H:%M:%S"),
                                                "date": datetime.now().strftime("%Y-%m-%d"),
                                                "ticker": t, "action": "EXIT",
                                                "shares": round(shares, 4), "price": round(price, 2),
                                                "value": round(proceeds, 2), "reason": "Signal removed",
                                            })
                                            del _portfolio["positions"][t]

                    # 2. Recalculate total capital
                    pos_val = sum(
                                    _portfolio["positions"][t]["shares"] * _get_price(quotes, t, _portfolio["positions"][t]["entry_price"])
                                    for t in _portfolio["positions"]
                    )
        total_capital = _portfolio["cash"] + pos_val
        long_budget = total_capital * LONG_ALLOCATION

        # 3. Rebalance / enter positions
        for ticker, weight in target.items():
                        price = _get_price(quotes, ticker, 0)
                        if price <= 0:
                                            continue
                                        target_shares = (long_budget * weight) / price
            current_shares = _portfolio["positions"].get(ticker, {}).get("shares", 0)
            delta = target_shares - current_shares

            if abs(delta * price) < 5:
                                continue

            if delta > 0:
                                cost = delta * price
                                if cost > _portfolio["cash"]:
                                                        delta = _portfolio["cash"] / price * 0.999
                                                        cost = delta * price
                                                    if delta < 0.0001:
                                                                            continue
                                                                        _portfolio["cash"] -= cost
                if ticker in _portfolio["positions"]:
                                        old_cost = _portfolio["positions"][ticker]["shares"] * _portfolio["positions"][ticker]["entry_price"]
                                        new_shrs = _portfolio["positions"][ticker]["shares"] + delta
                                        _portfolio["positions"][ticker]["entry_price"] = (old_cost + cost) / new_shrs
                                        _portfolio["positions"][ticker]["shares"] = new_shrs
else:
                    _portfolio["positions"][ticker] = {
                                                "shares": delta, "entry_price": price,
                                                "entry_time": datetime.now().isoformat(),
                    }
                _portfolio["trades"].append({
                                        "time": datetime.now().strftime("%H:%M:%S"),
                                        "date": datetime.now().strftime("%Y-%m-%d"),
                                        "ticker": ticker, "action": "BUY",
                                        "shares": round(delta, 4), "price": round(price, 2),
                                        "value": round(cost, 2), "reason": f"wt {weight*100:.1f}%",
                })
else:
                sell = abs(delta)
                proc = sell * price
                _portfolio["cash"] += proc
                _portfolio["positions"][ticker]["shares"] -= sell
                if _portfolio["positions"][ticker]["shares"] < 0.0001:
                                        del _portfolio["positions"][ticker]
else:
                    _portfolio["trades"].append({
                                                "time": datetime.now().strftime("%H:%M:%S"),
                                                "date": datetime.now().strftime("%Y-%m-%d"),
                                                "ticker": ticker, "action": "TRIM",
                                                "shares": round(sell, 4), "price": round(price, 2),
                                                "value": round(proc, 2), "reason": "Rebalance",
                    })

        _portfolio["trades"] = _portfolio["trades"][-500:]
        _save_portfolio()
        return _portfolio_snapshot(quotes)

# ── Math Pipeline ──────────────────────────────────────────────────────────────

def fetch_returns(tickers, period="3mo"):
        """Download adjusted closes via yfinance, return dict of daily return arrays.
            Handles both old (flat columns) and new (MultiIndex columns) yfinance APIs.
                """
    raw = yf.download(tickers, period=period, auto_adjust=True, progress=False)

    # Normalise to a flat ticker-keyed DataFrame of closes regardless of yfinance version
    if isinstance(raw.columns, pd.MultiIndex):
                # yfinance >= 0.2.x returns MultiIndex (Price, Ticker)
                if "Close" in raw.columns.get_level_values(0):
                                closes = raw["Close"]
else:
            # Fallback: try first price level
                closes = raw.iloc[:, raw.columns.get_level_values(0) == raw.columns.get_level_values(0)[0]]
else:
        # Older yfinance with flat columns
            closes = raw["Close"] if "Close" in raw.columns else raw

    # If only one ticker was downloaded yfinance may return a Series
    if isinstance(closes, pd.Series):
                closes = closes.to_frame(name=tickers[0] if len(tickers) == 1 else "unknown")

    # Drop rows where ALL tickers are NaN (avoids killing entire dataset for one bad ticker)
    closes = closes.dropna(how="all")

    returns = closes.pct_change().dropna(how="all")

    result = {}
    for t in tickers:
                if t not in returns.columns:
                                continue
                            series = returns[t].dropna()
        if len(series) > 5:  # need at least a few data points
            result[t] = series.tolist()

    return result, closes

def corr_matrix(returns_dict, tickers):
        """Pearson correlation matrix as 2D list."""
    n = len(tickers)
    data = [returns_dict[t] for t in tickers]
    min_len = min(len(d) for d in data)
    data = [d[-min_len:] for d in data]
    arr = np.array(data) # (n, T)
    C = np.corrcoef(arr)
    C = np.clip(C, -1, 1)
    np.fill_diagonal(C, 1.0)
    return C.tolist(), min_len

def laplacian_diffusion(C, returns_dict, tickers, alpha=0.5, window=20):
        """Graph Laplacian diffusion on recent returns → residuals (mispricings)."""
    n = len(tickers)
    W = np.array(C)
    np.fill_diagonal(W, 0)
    W = np.maximum(W, 0) # only positive weights

    deg = W.sum(axis=1)
    D_inv_sqrt = np.diag(1.0 / np.sqrt(np.maximum(deg, 1e-9)))
    L_norm = np.eye(n) - D_inv_sqrt @ W @ D_inv_sqrt # normalised Laplacian

    recent = np.array([
                np.mean(returns_dict[t][-window:]) for t in tickers
    ])

    diffused = (np.eye(n) - alpha * L_norm) @ recent
    residuals = recent - diffused

    return {
                "recent": recent.tolist(),
                "diffused": diffused.tolist(),
                "residuals": residuals.tolist(),
    }

def persistent_homology(C):
        """
            Simplified Vietoris-Rips persistent homology on the correlation distance matrix.
                Returns H0 (components) and H1 (loops) bars as {birth, death, dim}.
                    """
    C_arr = np.array(C)
    n = C_arr.shape[0]
    dist = 1 - np.abs(C_arr)
    np.fill_diagonal(dist, 0)

    # Build sorted edge list
    edges = []
    for i in range(n):
                for j in range(i + 1, n):
                                edges.append((dist[i, j], i, j))
                        edges.sort()

    # Union-Find for H0
    parent = list(range(n))
    def find(x):
                while parent[x] != x:
                                parent[x] = parent[parent[x]]
                                x = parent[x]
                            return x
    def union(a, b):
                parent[find(a)] = find(b)

    bars = []
    for d, i, j in edges:
                ri, rj = find(i), find(j)
        if ri != rj:
                        bars.append({"birth": 0.0, "death": round(d, 4), "dim": 0})
            union(i, j)

    # Surviving H0 bar
    bars.append({"birth": 0.0, "death": None, "dim": 0}) # None = infinity

    # H1: triangle-based loop detection
    seen_h1 = set()
    for k, (d, i, j) in enumerate(edges[:15]):
                for m in range(n):
                                if m == i or m == j:
                                                    continue
                                                d1 = dist[i, m]
            d2 = dist[j, m]
            if d1 < 0.65 and d2 < 0.65:
                                birth = round(min(d, d1, d2), 4)
                death = round(max(d, d1, d2), 4)
                key = (birth, death)
                if death - birth > 0.04 and key not in seen_h1:
                                        seen_h1.add(key)
                                        bars.append({"birth": birth, "death": death, "dim": 1})
                                    if len([b for b in bars if b["dim"] == 1]) >= 8:
                                                            break
else:
            continue
        break

    return bars

def get_neighbors(C, tickers, n=3):
        """Top-n most correlated tickers for each ticker."""
    C_arr = np.array(C)
    result = {}
    for i, t in enumerate(tickers):
                row = [(j, C_arr[i, j]) for j in range(len(tickers)) if j != i]
        row.sort(key=lambda x: -abs(x[1]))
        result[t] = [{"ticker": tickers[j], "rho": round(v, 3)} for j, v in row[:n]]
    return result

def market_neutral_signals(residuals, tickers):
        """Long undervalued (negative residual), short overvalued (positive residual)."""
    scored = sorted(enumerate(residuals), key=lambda x: x[1])
    n = len(tickers)
    k = max(1, n // 4)

    long_idx = scored[:k] # lowest residuals → most underpriced
    short_idx = scored[-k:] # highest residuals → most overpriced

    long_sum = sum(abs(s) for _, s in long_idx) or 1
    short_sum = sum(abs(s) for _, s in short_idx) or 1

    longs = [
                {"ticker": tickers[i], "weight": round(abs(s) / long_sum, 4),
                          "residual": round(s, 6), "action": "BUY"}
                for i, s in long_idx
    ]
    shorts = [
                {"ticker": tickers[i], "weight": round(abs(s) / short_sum, 4),
                          "residual": round(s, 6), "action": "SELL"}
                for i, s in short_idx
    ]
    return {"long": longs, "short": shorts}

def fetch_live_quotes(tickers):
    """Grab latest price + day change % via yfinance fast_info."""
    quotes = {}
    for t in tickers:
                try:
                                info = yf.Ticker(t).fast_info
            price = round(float(info.last_price or 0), 2)
            prev = float(info.previous_close or price)
            change = round(((price - prev) / prev) * 100, 2) if prev else 0
            quotes[t] = {"price": price, "change": change}
except Exception:
            quotes[t] = {"price": 0, "change": 0}
    return quotes

def run_full_pipeline():
        """Run the complete topo algo pipeline and return serialisable result."""
    print(f"[{datetime.now():%H:%M:%S}] Running pipeline...")

    returns_dict, closes = fetch_returns(TICKERS)
    valid = [t for t in TICKERS if t in returns_dict]

    if len(valid) < 2:
                raise RuntimeError(f"Insufficient ticker data: only {len(valid)} tickers returned data")

    C, T = corr_matrix(returns_dict, valid)
    diff = laplacian_diffusion(C, returns_dict, valid)
    bars = persistent_homology(C)
    sigs = market_neutral_signals(diff["residuals"], valid)
    quotes = fetch_live_quotes(valid)
    neighbors = get_neighbors(C, valid)

    loop_count = sum(1 for b in bars if b["dim"] == 1 and b["death"] is not None)
    regime = "HIGH CONNECTIVITY" if loop_count > 3 else "NORMAL"

    portfolio = execute_trades(sigs, quotes)

    result = {
                "tickers": valid,
                "corr": C,
                "residuals": diff["residuals"],
                "recent": diff["recent"],
                "diffused": diff["diffused"],
                "bars": bars,
                "signals": sigs,
                "quotes": quotes,
                "neighbors": neighbors,
                "loop_count": loop_count,
                "regime": regime,
                "updated": datetime.now().strftime("%H:%M:%S"),
                "T": T,
                "portfolio": portfolio,
    }
    print(f"[{datetime.now():%H:%M:%S}] Pipeline complete. Regime: {regime} | Portfolio ${portfolio['total_value']:,.0f}")
    return result

# ── Background refresh loop ────────────────────────────────────────────────────

def background_pipeline():
        while True:
                    try:
                                    data = run_full_pipeline()
                                    socketio.emit("pipeline_update", data)
                                    socketio.emit("portfolio_update", data["portfolio"])
                    except Exception as e:
            socketio.emit("pipeline_error", {"message": str(e)})
            print(f"Pipeline error: {e}")
        time.sleep(REFRESH_SECONDS)

# ── Routes ─────────────────────────────────────────────────────────────────────

HTML = open("index.html", encoding="utf-8").read()
PORTFOLIO_HTML = open("portfolio.html", encoding="utf-8").read()

@app.route("/")
def index():
        return render_template_string(HTML)

@app.route("/portfolio")
def portfolio_page():
        return render_template_string(PORTFOLIO_HTML)

@socketio.on("connect")
def on_connect():
        print("Client connected — running pipeline...")
    def run_and_emit():
        try:
                data = run_full_pipeline()
                        socketio.emit("pipeline_update", data)
            socketio.emit("portfolio_update", data["portfolio"])
except Exception as e:
            socketio.emit("pipeline_error", {"message": str(e)})
    threading.Thread(target=run_and_emit, daemon=True).start()

@socketio.on("refresh")
def on_refresh():
        def run_and_emit():
                    try:
                                    data = run_full_pipeline()
                                    socketio.emit("pipeline_update", data)
            socketio.emit("portfolio_update", data["portfolio"])
except Exception as e:
            socketio.emit("pipeline_error", {"message": str(e)})
    threading.Thread(target=run_and_emit, daemon=True).start()

# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
        # Start background refresh thread
        bg = threading.Thread(target=background_pipeline, daemon=True)
    bg.start()
    print("=" * 55)
    print(" Topological Alpha Engine")
    print(" Open: http://localhost:5000")
    print(f" Tickers: {len(TICKERS)} stocks")
    print(f" Auto-refresh: every {REFRESH_SECONDS}s")
    print("=" * 55)
    socketio.run(app, host="0.0.0.0", port=PORT, debug=False)
