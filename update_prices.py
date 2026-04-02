#!/usr/bin/env python3
"""
Fetches live prices from Yahoo Finance and writes prices.json.
Runs automatically 2x per day via GitHub Actions.
"""
import json
import urllib.request
import urllib.error
import time
from datetime import datetime, timezone

# All tickers in the portfolio (XAU = Gold futures)
TICKER_MAP = {
    'XAU':  'GC=F',   # Gold futures
    'VIST': 'VIST',
    'NVDA': 'NVDA',
    'AXP':  'AXP',
    'VALE': 'VALE',
    'AMD':  'AMD',
    'PLTR': 'PLTR',
    'CEG':  'CEG',
    'BMA':  'BMA',
    'PAM':  'PAM',
    'GGAL': 'GGAL',
    'MSFT': 'MSFT',
    'IBIT': 'IBIT',
    'MOO':  'MOO',
    'LMND': 'LMND',
    'GPRK': 'GPRK',
    'NBIS': 'NBIS',
    'BABA': 'BABA',
    'MSTR': 'MSTR',
    'PCLA': 'PCLA',
    'OSCR': 'OSCR',
    'TSLA': 'TSLA',
    'NNE':  'NNE',
    'UNH':  'UNH',
    'GEMI': 'GEMI',
}

# Fallback prices (last known from Excel) — used if Yahoo fails
FALLBACK = {
    'XAU':  4489.69,
    'VIST': 74.21,
    'NVDA': 167.52,
    'AXP':  292.97,
    'VALE': 15.03,
    'AMD':  201.99,
    'PLTR': 143.06,
    'CEG':  301.49,
    'BMA':  69.26,
    'PAM':  83.92,
    'GGAL': 42.70,
    'MSFT': 356.77,
    'IBIT': 37.40,
    'MOO':  82.65,
    'LMND': 60.70,
    'GPRK': 9.62,
    'NBIS': 100.82,
    'BABA': 122.69,
    'MSTR': 126.03,
    'PCLA': 2.00,
    'OSCR': 11.14,
    'TSLA': 361.83,
    'NNE':  20.29,
    'UNH':  259.02,
    'GEMI': 4.11,
}

def fetch_price(yahoo_symbol):
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range=1d'
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; portfolio-updater/1.0)',
        'Accept': 'application/json',
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            price = data['chart']['result'][0]['meta']['regularMarketPrice']
            if price and price > 0:
                return round(price, 4)
    except Exception as e:
        print(f"  Error fetching {yahoo_symbol}: {e}")
    return None

def main():
    prices = {}
    hits = 0
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    print(f"Updating prices at {now}")
    print("-" * 40)

    for ticker, yahoo_sym in TICKER_MAP.items():
        price = fetch_price(yahoo_sym)
        if price:
            prices[ticker] = price
            hits += 1
            print(f"  {ticker:6} = ${price}")
        else:
            prices[ticker] = FALLBACK.get(ticker)
            print(f"  {ticker:6} = ${FALLBACK.get(ticker)} (fallback)")
        time.sleep(0.3)  # be polite to Yahoo

    output = {
        'updated_at': now,
        'hits': hits,
        'total': len(TICKER_MAP),
        'prices': prices,
    }

    with open('prices.json', 'w') as f:
        json.dump(output, f, indent=2)

    print("-" * 40)
    print(f"Done: {hits}/{len(TICKER_MAP)} prices fetched live")
    print(f"Saved to prices.json")

if __name__ == '__main__':
    main()
