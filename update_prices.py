#!/usr/bin/env python3
"""
Fetches live prices from Yahoo Finance and:
1. Writes prices.json (fallback)
2. Upserts into Supabase price_history table (one row per ticker per day)
Runs automatically 2x per day via GitHub Actions.
"""
import json
import os
import urllib.request
import urllib.error
import time
from datetime import datetime, timezone

SUPABASE_URL         = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

TICKER_MAP = {
    'XAU':  'GC=F',
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

FALLBACK = {
    'XAU':  4489.69, 'VIST': 74.21,  'NVDA': 167.52, 'AXP':  292.97,
    'VALE': 15.03,   'AMD':  201.99,  'PLTR': 143.06,  'CEG':  301.49,
    'BMA':  69.26,   'PAM':  83.92,   'GGAL': 42.70,   'MSFT': 356.77,
    'IBIT': 37.40,   'MOO':  82.65,   'LMND': 60.70,   'GPRK': 9.62,
    'NBIS': 100.82,  'BABA': 122.69,  'MSTR': 126.03,  'PCLA': 2.00,
    'OSCR': 11.14,   'TSLA': 361.83,  'NNE':  20.29,   'UNH':  259.02,
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

def save_to_supabase(prices, today):
    """Upsert today's prices into price_history table."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("  ⚠ SUPABASE_URL / SUPABASE_SERVICE_KEY not set — skipping DB write")
        return
    rows = [
        {'ticker': ticker, 'date': today, 'price': price}
        for ticker, price in prices.items()
        if price is not None
    ]
    url = f'{SUPABASE_URL}/rest/v1/price_history'
    data = json.dumps(rows).encode('utf-8')
    headers = {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates',   # upsert on (ticker, date) conflict
    }
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"  ✓ Supabase: {len(rows)} precios guardados para {today}")
    except Exception as e:
        print(f"  ✗ Error guardando en Supabase: {e}")

def main():
    prices = {}
    hits   = 0
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime('%Y-%m-%d %H:%M UTC')
    today   = now_utc.strftime('%Y-%m-%d')

    print(f"Actualizando precios — {now_str}")
    print("-" * 42)

    for ticker, yahoo_sym in TICKER_MAP.items():
        price = fetch_price(yahoo_sym)
        if price:
            prices[ticker] = price
            hits += 1
            print(f"  {ticker:6} = ${price}")
        else:
            prices[ticker] = FALLBACK.get(ticker)
            print(f"  {ticker:6} = ${FALLBACK.get(ticker)} (fallback)")
        time.sleep(0.3)

    print("-" * 42)
    print(f"Yahoo: {hits}/{len(TICKER_MAP)} precios obtenidos en vivo")

    # 1. Write prices.json (legacy fallback for the HTML)
    output = {
        'updated_at': now_str,
        'hits': hits,
        'total': len(TICKER_MAP),
        'prices': prices,
    }
    with open('prices.json', 'w') as f:
        json.dump(output, f, indent=2)
    print("  ✓ prices.json actualizado")

    # 2. Upsert into Supabase price_history
    save_to_supabase(prices, today)

if __name__ == '__main__':
    main()
