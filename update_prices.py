#!/usr/bin/env python3
"""
Fetches live prices from Yahoo Finance and:
1. Writes prices.json (fallback)
2. Upserts into Supabase price_history table (one row per ticker per day)
3. Fetches extended data (P/E, market cap, 52-week) for watchlist tickers
4. Checks active alerts and sends email via Resend if any triggered
Runs automatically 2x per day via GitHub Actions (every day of the week).
On weekends, only 24/7 assets (gold, crypto) are updated.
"""
import json
import os
import urllib.request
import urllib.error
import time
from datetime import datetime, timezone, timedelta

SUPABASE_URL         = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')
RESEND_API_KEY       = os.environ.get('RESEND_API_KEY', '')
ALERT_EMAILS         = [e.strip() for e in os.environ.get('ALERT_EMAILS', '').split(',') if e.strip()]

# Override Yahoo Finance symbol for tickers where the symbol differs
SYMBOL_OVERRIDE = {
    'XAU': 'GC=F',   # Gold futures
    'BTC': 'BTC-USD', # Bitcoin
    'ETH': 'ETH-USD', # Ethereum
}

# Categories that trade 24/7 — always updated even on weekends
ALWAYS_ON_CATEGORIES = {'gold', 'crypto'}

# Fallback category per ticker (used when Supabase is unavailable)
TICKER_CATEGORY_FALLBACK = {
    'XAU': 'gold',   'VIST': 'stock', 'NVDA': 'stock', 'AXP':  'stock',
    'VALE': 'stock',  'AMD':  'stock', 'PLTR': 'stock', 'CEG':  'stock',
    'BMA':  'stock',  'PAM':  'stock', 'GGAL': 'stock', 'MSFT': 'stock',
    'IBIT': 'etf',    'MOO':  'etf',   'LMND': 'stock', 'GPRK': 'stock',
    'NBIS': 'stock',  'BABA': 'stock', 'MSTR': 'stock', 'PCLA': 'stock',
    'OSCR': 'stock',  'TSLA': 'stock', 'NNE':  'stock', 'UNH':  'stock',
    'GEMI': 'stock',  'BTC':  'crypto','ETH':  'crypto',
}

# Fallback ticker list used when Supabase is unavailable
TICKER_MAP_FALLBACK = {
    'XAU':  'GC=F',  'VIST': 'VIST',  'NVDA': 'NVDA',  'AXP':  'AXP',
    'VALE': 'VALE',   'AMD':  'AMD',   'PLTR': 'PLTR',  'CEG':  'CEG',
    'BMA':  'BMA',    'PAM':  'PAM',   'GGAL': 'GGAL',  'MSFT': 'MSFT',
    'IBIT': 'IBIT',   'MOO':  'MOO',   'LMND': 'LMND',  'GPRK': 'GPRK',
    'NBIS': 'NBIS',   'BABA': 'BABA',  'MSTR': 'MSTR',  'PCLA': 'PCLA',
    'OSCR': 'OSCR',   'TSLA': 'TSLA',  'NNE':  'NNE',   'UNH':  'UNH',
    'GEMI': 'GEMI',   'MELI': 'MELI',  'GLOB': 'GLOB',  'TSM':  'TSM',
}
WATCHLIST_TICKERS_FALLBACK = ['MELI', 'GLOB', 'TSM', 'BABA']

PRICE_FALLBACK = {
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

def supabase_get(path):
    """GET from Supabase REST API."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return []
    url = f'{SUPABASE_URL}/rest/v1/{path}'
    headers = {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
        'Accept': 'application/json',
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  ✗ Error leyendo {path}: {e}")
        return []

def supabase_patch(path, data):
    """PATCH rows in Supabase."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return
    url = f'{SUPABASE_URL}/rest/v1/{path}'
    headers = {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    }
    req = urllib.request.Request(url, data=json.dumps(data).encode(), headers=headers, method='PATCH')
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            pass
    except Exception as e:
        print(f"  ✗ Error en PATCH {path}: {e}")

def is_weekend():
    """Returns True if today is Saturday or Sunday (Argentina time, UTC-3)."""
    now_arg = datetime.now(timezone.utc) - timedelta(hours=3)
    return now_arg.weekday() >= 5  # 5=Saturday, 6=Sunday

def fetch_portfolio_tickers():
    """Fetch distinct tickers and their categories from Supabase positions table.
    Returns dict {ticker: category}."""
    rows = supabase_get('positions?select=ticker,category')
    if not rows:
        return {}
    seen = {}
    for r in rows:
        t = r.get('ticker')
        c = r.get('category', 'stock')
        if t and t not in seen:
            seen[t] = c
    return seen  # {ticker: category}

def fetch_watchlist_tickers():
    """Fetch distinct tickers from Supabase watchlist table."""
    rows = supabase_get('watchlist?select=ticker')
    if not rows:
        return []
    return list(dict.fromkeys(r['ticker'] for r in rows if r.get('ticker')))

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
        'Prefer': 'resolution=merge-duplicates',
    }
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"  ✓ Supabase: {len(rows)} precios guardados para {today}")
    except Exception as e:
        print(f"  ✗ Error guardando en Supabase: {e}")

def get_last_saved_prices(tickers):
    """Get the most recently saved price for each ticker from price_history."""
    if not tickers or not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return {}
    ticker_csv = ','.join(tickers)
    rows = supabase_get(f'price_history?ticker=in.({ticker_csv})&order=date.desc&limit=500')
    latest = {}
    for r in rows:
        t = r.get('ticker')
        if t and t not in latest:
            latest[t] = float(r['price'])
    return latest

def fetch_tickers_with_history():
    """Return set of tickers that already have at least one row in price_history."""
    rows = supabase_get('price_history?select=ticker')
    if not rows:
        return set()
    return set(r['ticker'] for r in rows if r.get('ticker'))

def backfill_ticker(ticker, yahoo_symbol):
    """Fetch last 10 days of closing prices from Yahoo and upsert into price_history."""
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range=15d'
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; portfolio-updater/1.0)',
        'Accept': 'application/json',
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            result = data['chart']['result'][0]
            timestamps = result.get('timestamp', [])
            closes = result['indicators']['quote'][0].get('close', [])
            rows = []
            for ts, close in zip(timestamps, closes):
                if close is None:
                    continue
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
                rows.append({'ticker': ticker, 'date': date_str, 'price': round(close, 4)})
            if not rows:
                print(f'  ✗ Backfill {ticker}: sin datos')
                return
            if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
                return
            ph_url = f'{SUPABASE_URL}/rest/v1/price_history'
            ph_data = json.dumps(rows).encode('utf-8')
            ph_headers = {
                'apikey': SUPABASE_SERVICE_KEY,
                'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                'Content-Type': 'application/json',
                'Prefer': 'resolution=merge-duplicates',
            }
            ph_req = urllib.request.Request(ph_url, data=ph_data, headers=ph_headers, method='POST')
            with urllib.request.urlopen(ph_req, timeout=15) as r:
                print(f'  ✓ Backfill {ticker}: {len(rows)} días cargados')
    except Exception as e:
        print(f'  ✗ Backfill {ticker}: {e}')

def check_alerts(prices):
    """
    Compare active alerts against current prices.
    Returns list of alert dicts that just triggered.
    """
    alerts = supabase_get('alerts?triggered=eq.false&order=created_at.asc')
    if not alerts:
        print("  ℹ Sin alertas activas para revisar")
        return []

    positions = supabase_get('positions?select=ticker,buy_price')
    buy_prices = {p['ticker']: float(p['buy_price']) for p in positions}

    triggered = []
    for a in alerts:
        ticker    = a['ticker']
        cond      = a['condition_type']
        threshold = float(a['value'])
        cur_price = prices.get(ticker)

        if cur_price is None:
            continue

        fired = False
        if cond == 'price_above':
            fired = cur_price >= threshold
        elif cond == 'price_below':
            fired = cur_price <= threshold
        elif cond in ('pct_above', 'pct_below'):
            buy = buy_prices.get(ticker)
            if buy and buy > 0:
                pct_change = (cur_price - buy) / buy * 100
                if cond == 'pct_above':
                    fired = pct_change >= threshold
                else:  # pct_below
                    fired = pct_change <= -threshold

        if fired:
            a['current_price'] = cur_price
            triggered.append(a)
            print(f"  🔔 ALERTA: {ticker} — {a.get('label', cond)} (precio actual: ${cur_price})")

    return triggered

def mark_triggered(alerts):
    """Mark alerts as triggered in Supabase."""
    for a in alerts:
        supabase_patch(f"alerts?id=eq.{a['id']}", {'triggered': True})
    if alerts:
        print(f"  ✓ {len(alerts)} alerta(s) marcadas como activadas en Supabase")

def send_email(triggered_alerts, now_str):
    """Send email notification via Resend."""
    if not RESEND_API_KEY or not ALERT_EMAILS:
        print("  ⚠ RESEND_API_KEY / ALERT_EMAILS no configurados — no se envía email")
        return
    if not triggered_alerts:
        return

    # Build email body
    rows_html = ''
    for a in triggered_alerts:
        ticker    = a['ticker']
        label     = a.get('label') or a['condition_type']
        note      = a.get('note') or ''
        cur_price = a.get('current_price', '—')
        color     = '#e74c3c' if 'baj' in label.lower() or 'pérdida' in label.lower() or 'below' in a['condition_type'] else '#27ae60'
        rows_html += f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #f0f0f0;font-weight:600;">{ticker}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #f0f0f0;">{label}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #f0f0f0;color:{color};font-weight:600;">${cur_price:.2f}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #f0f0f0;color:#888;font-size:13px;">{note}</td>
        </tr>"""

    count = len(triggered_alerts)
    subject = f"🔔 {count} alerta{'s' if count > 1 else ''} activada{'s' if count > 1 else ''} — Portfolio Familiar"

    html_body = f"""
    <div style="font-family:system-ui,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
      <h2 style="margin:0 0 4px;color:#1a1a2e;">🔔 Portfolio Familiar</h2>
      <p style="color:#666;margin:0 0 24px;font-size:14px;">Actualización de precios — {now_str}</p>

      <div style="background:#fff3cd;border-left:4px solid #f39c12;padding:12px 16px;border-radius:4px;margin-bottom:20px;">
        <strong>{count} alerta{'s' if count > 1 else ''} activada{'s' if count > 1 else ''}</strong> al revisar los precios de mercado.
      </div>

      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="background:#f8f9fa;">
            <th style="padding:10px 8px;text-align:left;font-size:12px;color:#888;text-transform:uppercase;">Ticker</th>
            <th style="padding:10px 8px;text-align:left;font-size:12px;color:#888;text-transform:uppercase;">Condición</th>
            <th style="padding:10px 8px;text-align:left;font-size:12px;color:#888;text-transform:uppercase;">Precio actual</th>
            <th style="padding:10px 8px;text-align:left;font-size:12px;color:#888;text-transform:uppercase;">Nota</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>

      <p style="margin-top:24px;font-size:13px;color:#aaa;">
        Las alertas fueron marcadas como activadas en el sistema.<br>
        Entrá al portfolio para más detalles o para crear nuevas alertas.
      </p>
    </div>"""

    payload = json.dumps({
        'from': 'Portfolio Familiar <onboarding@resend.dev>',
        'to': ALERT_EMAILS,
        'subject': subject,
        'html': html_body,
    }).encode()

    req = urllib.request.Request(
        'https://api.resend.com/emails',
        data=payload,
        headers={
            'Authorization': f'Bearer {RESEND_API_KEY}',
            'Content-Type': 'application/json',
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"  ✓ Email enviado a {', '.join(ALERT_EMAILS)}")
    except urllib.error.HTTPError as e:
        print(f"  ✗ Error enviando email: {e.code} — {e.read().decode()}")
    except Exception as e:
        print(f"  ✗ Error enviando email: {e}")

def fetch_watchlist_extended(ticker):
    """Fetch price history + P/E + market cap for a watchlist ticker."""
    yf_headers = {'User-Agent': 'Mozilla/5.0 (compatible; portfolio-updater/1.0)', 'Accept': 'application/json'}
    result = {'ticker': ticker, 'price': None, 'prev_close': None, 'week_ago_price': None,
              'hi52': None, 'lo52': None, 'pe_ratio': None, 'market_cap': None}
    try:
        # 1-year daily history for price, hi52, lo52, weekAgo
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1y'
        req = urllib.request.Request(url, headers=yf_headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            res  = data['chart']['result'][0]
            meta = res['meta']
            closes = res['indicators']['quote'][0].get('close', [])
            valid  = [c for c in closes if c is not None]
            if not valid:
                return None
            result['price']          = round(meta.get('regularMarketPrice') or valid[-1], 4)
            result['prev_close']     = round(valid[-2], 4) if len(valid) >= 2 else result['price']
            result['week_ago_price'] = round(valid[-6], 4) if len(valid) >= 6 else None
            result['hi52']           = round(max(valid), 4)
            result['lo52']           = round(min(valid), 4)
    except Exception as e:
        print(f'  ✗ Watchlist chart error {ticker}: {e}')
        return None
    try:
        # P/E ratio and market cap from quoteSummary
        url2 = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=summaryDetail'
        req2 = urllib.request.Request(url2, headers=yf_headers)
        with urllib.request.urlopen(req2, timeout=10) as resp:
            d2  = json.loads(resp.read().decode())
            sd  = d2['quoteSummary']['result'][0]['summaryDetail']
            pe  = sd.get('trailingPE',  {}).get('raw')
            mc  = sd.get('marketCap',   {}).get('raw')
            if pe: result['pe_ratio']   = round(float(pe), 2)
            if mc: result['market_cap'] = int(mc)
    except Exception as e:
        print(f'  ⚠ Watchlist PE/MC unavailable {ticker}: {e}')
    return result

def save_watchlist_meta(rows):
    """Upsert extended watchlist data into watchlist_meta table."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY or not rows:
        return
    import datetime as _dt
    for r in rows:
        r['updated_at'] = _dt.datetime.now(_dt.timezone.utc).isoformat()
    url = f'{SUPABASE_URL}/rest/v1/watchlist_meta'
    data = json.dumps(rows).encode('utf-8')
    headers = {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates',
    }
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f'  ✓ watchlist_meta: {len(rows)} tickers actualizados')
    except Exception as e:
        print(f'  ✗ Error guardando watchlist_meta: {e}')

def main():
    prices = {}
    hits   = 0

    # Use Argentina time (UTC-3) for the date — avoids off-by-one at midnight
    now_utc = datetime.now(timezone.utc)
    now_arg = now_utc - timedelta(hours=3)
    now_str = now_utc.strftime('%Y-%m-%d %H:%M UTC')
    today   = now_arg.strftime('%Y-%m-%d')   # Argentina local date
    weekend = is_weekend()

    print(f"Actualizando precios — {now_str}  (fecha AR: {today})")
    if weekend:
        print("  📅 Fin de semana — solo se actualizan activos 24/7 (gold, crypto)")
    print("-" * 42)

    # Build ticker map dynamically from Supabase
    # portfolio_ticker_cats = {ticker: category}
    portfolio_ticker_cats = fetch_portfolio_tickers()
    portfolio_tickers     = list(portfolio_ticker_cats.keys())
    watchlist_tickers     = fetch_watchlist_tickers()

    if portfolio_tickers:
        all_tickers        = list(dict.fromkeys(portfolio_tickers + watchlist_tickers))
        ticker_map         = {t: SYMBOL_OVERRIDE.get(t, t) for t in all_tickers}
        ticker_cats        = portfolio_ticker_cats   # {ticker: category}
        watchlist_extended = watchlist_tickers if watchlist_tickers else WATCHLIST_TICKERS_FALLBACK
        print(f"  ✓ Tickers dinámicos: {len(portfolio_tickers)} portfolio + {len(watchlist_tickers)} watchlist")
    else:
        print("  ⚠ No se encontraron tickers en Supabase — usando lista de respaldo")
        ticker_map         = dict(TICKER_MAP_FALLBACK)
        ticker_cats        = dict(TICKER_CATEGORY_FALLBACK)
        watchlist_extended = list(WATCHLIST_TICKERS_FALLBACK)

    # On weekends, only update 24/7 assets (gold, crypto)
    if weekend:
        active_ticker_map = {
            t: ys for t, ys in ticker_map.items()
            if ticker_cats.get(t, 'stock') in ALWAYS_ON_CATEGORIES
        }
        if not active_ticker_map:
            print("  ℹ No hay activos 24/7 en el portfolio — nada que actualizar hoy")
            return
        print(f"  → {len(active_ticker_map)} activos 24/7 a actualizar: {', '.join(active_ticker_map)}")
    else:
        active_ticker_map = ticker_map
        print(f"  → {len(active_ticker_map)} tickers a actualizar")
    print("-" * 42)

    for ticker, yahoo_sym in active_ticker_map.items():
        price = fetch_price(yahoo_sym)
        if price:
            prices[ticker] = price
            hits += 1
            print(f"  {ticker:6} = ${price}")
        else:
            prices[ticker] = PRICE_FALLBACK.get(ticker)
            print(f"  {ticker:6} = ${PRICE_FALLBACK.get(ticker)} (fallback)")
        time.sleep(0.3)

    print("-" * 42)
    print(f"Yahoo: {hits}/{len(active_ticker_map)} precios obtenidos en vivo")

    # 1. Write prices.json (legacy fallback)
    output = {
        'updated_at': now_str,
        'hits': hits,
        'total': len(active_ticker_map),
        'prices': prices,
    }
    with open('prices.json', 'w') as f:
        json.dump(output, f, indent=2)
    print("  ✓ prices.json actualizado")

    # 2. Upsert into Supabase price_history
    # On weekends: only save if the price actually changed (skip if market is closed)
    if weekend:
        last_saved = get_last_saved_prices(list(prices.keys()))
        prices_to_save = {t: p for t, p in prices.items()
                          if p is not None and abs(p - last_saved.get(t, 0)) > 0.001}
        if prices_to_save:
            save_to_supabase(prices_to_save, today)
            print(f"  ✓ {len(prices_to_save)} precio(s) cambiaron — historial actualizado")
        else:
            print("  ℹ Precios sin cambios (mercado cerrado) — no se guarda en historial")
    else:
        save_to_supabase(prices, today)

    # 2b. Backfill historical data for new portfolio AND watchlist tickers (weekdays only)
    if not weekend:
        print("-" * 42)
        print("Verificando tickers nuevos que necesiten backfill...")
        existing_tickers = fetch_tickers_with_history()
        all_tracked = list(dict.fromkeys(portfolio_tickers + watchlist_tickers))
        new_tickers = [t for t in all_tracked if t not in existing_tickers]
        if new_tickers:
            print(f"  Tickers nuevos detectados: {', '.join(new_tickers)}")
            for t in new_tickers:
                ys = ticker_map.get(t, t)
                time.sleep(0.5)
                backfill_ticker(t, ys)
        else:
            print("  ✓ Sin tickers nuevos, backfill no necesario")

    # 3. Fetch extended data for watchlist tickers (weekdays only)
    if not weekend:
        print("-" * 42)
        print(f"Actualizando watchlist_meta ({len(watchlist_extended)} tickers)...")
        wl_rows = []
        for ticker in watchlist_extended:
            time.sleep(0.4)
            row = fetch_watchlist_extended(ticker)
            if row:
                wl_rows.append(row)
                print(f'  {ticker:6} = ${row["price"]} | P/E: {row["pe_ratio"] or "N/A"} | Cap: {int(row["market_cap"]/1e9) if row["market_cap"] else "N/A"}B')
            else:
                print(f'  {ticker:6} = error')
        save_watchlist_meta(wl_rows)

    # 4. Check alerts and notify by email
    print("-" * 42)
    print("Revisando alertas...")
    triggered = check_alerts(prices)
    if triggered:
        mark_triggered(triggered)
        send_email(triggered, now_str)
    else:
        print("  ✓ Ninguna alerta alcanzó su límite")

if __name__ == '__main__':
    main()
