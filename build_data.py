#!/usr/bin/env python3
"""
build_data.py — Generate sp500_data.json with ~60 S&P 500 tickers + SPY.
Downloads 5 years of monthly adjusted-close data via yfinance,
computes monthly returns (%), and saves as compact JSON.

Usage:
    pip install yfinance pandas
    python build_data.py
"""

import json
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

# ~60 tickers spanning all 11 GICS sectors + SPY benchmark
TICKERS = [
    # Tech
    "AAPL", "MSFT", "GOOGL", "NVDA", "META", "AVGO", "CRM", "ADBE", "INTC", "CSCO",
    # Healthcare
    "JNJ", "UNH", "PFE", "ABBV", "MRK", "TMO", "ABT",
    # Financials
    "JPM", "BAC", "GS", "BRK-B", "MS", "BLK", "AXP",
    # Consumer Discretionary
    "AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "TJX",
    # Consumer Staples
    "PG", "KO", "PEP", "WMT", "COST", "CL",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG",
    # Industrials
    "CAT", "UNP", "HON", "GE", "RTX", "DE",
    # Utilities
    "NEE", "DUK", "SO", "D",
    # Real Estate
    "AMT", "PLD", "SPG",
    # Materials
    "LIN", "APD", "FCX",
    # Communication Services
    "NFLX", "DIS", "CMCSA", "T", "VZ",
]

SECTORS = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology",
    "NVDA": "Technology", "META": "Technology", "AVGO": "Technology",
    "CRM": "Technology", "ADBE": "Technology", "INTC": "Technology", "CSCO": "Technology",
    "JNJ": "Healthcare", "UNH": "Healthcare", "PFE": "Healthcare",
    "ABBV": "Healthcare", "MRK": "Healthcare", "TMO": "Healthcare", "ABT": "Healthcare",
    "JPM": "Financials", "BAC": "Financials", "GS": "Financials",
    "BRK-B": "Financials", "MS": "Financials", "BLK": "Financials", "AXP": "Financials",
    "AMZN": "Consumer Disc.", "TSLA": "Consumer Disc.", "HD": "Consumer Disc.",
    "MCD": "Consumer Disc.", "NKE": "Consumer Disc.", "SBUX": "Consumer Disc.", "TJX": "Consumer Disc.",
    "PG": "Consumer Staples", "KO": "Consumer Staples", "PEP": "Consumer Staples",
    "WMT": "Consumer Staples", "COST": "Consumer Staples", "CL": "Consumer Staples",
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy", "SLB": "Energy", "EOG": "Energy",
    "CAT": "Industrials", "UNP": "Industrials", "HON": "Industrials",
    "GE": "Industrials", "RTX": "Industrials", "DE": "Industrials",
    "NEE": "Utilities", "DUK": "Utilities", "SO": "Utilities", "D": "Utilities",
    "AMT": "Real Estate", "PLD": "Real Estate", "SPG": "Real Estate",
    "LIN": "Materials", "APD": "Materials", "FCX": "Materials",
    "NFLX": "Communication", "DIS": "Communication", "CMCSA": "Communication",
    "T": "Communication", "VZ": "Communication",
}


def main():
    end = datetime.now()
    start = end - timedelta(days=5 * 365 + 90)

    all_tickers = ["SPY"] + TICKERS
    print(f"Downloading {len(all_tickers)} tickers, {start:%Y-%m-%d} to {end:%Y-%m-%d} ...")

    df = yf.download(
        all_tickers,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval="1mo",
        auto_adjust=False,
        progress=True,
    )

    adj = df["Adj Close"]

    # SPY
    spy_adj = adj["SPY"].dropna()
    spy_ret = spy_adj.pct_change().dropna() * 100

    data = {
        "generated": datetime.now().isoformat()[:10],
        "months": len(spy_ret),
        "spy": {
            "dates": [d.strftime("%Y-%m") for d in spy_ret.index],
            "returns": [round(float(r), 4) for r in spy_ret.values],
        },
        "tickers": {},
    }

    for ticker in TICKERS:
        try:
            series = adj[ticker].dropna()
            ret = series.pct_change().dropna() * 100
            if len(ret) < 24:
                print(f"  SKIP {ticker}: only {len(ret)} months")
                continue
            data["tickers"][ticker] = {
                "sector": SECTORS.get(ticker, ""),
                "dates": [d.strftime("%Y-%m") for d in ret.index],
                "returns": [round(float(r), 4) for r in ret.values],
            }
            print(f"  OK {ticker}: {len(ret)} months")
        except Exception as e:
            print(f"  ERROR {ticker}: {e}")

    out = "sp500_data.json"
    with open(out, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    size_kb = len(json.dumps(data, separators=(",", ":"))) / 1024
    print(f"\nWrote {out}: {len(data['tickers'])} tickers, {size_kb:.0f} KB")


if __name__ == "__main__":
    main()
