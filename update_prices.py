#!/usr/bin/env python3
"""
Fetches live prices from Yahoo Finance and:
1. Writes prices.json (fallback)
2. Upserts into Supabase price_history table (one row per ticker per day)
3. Checks active alerts and sends email via Resend if any triggered
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
RESEND_API_KEY       = os.environ.get('RESEND_API_KEY', '')
ALERT_EMAIL          = os.environ.get('ALERT_EMAIL', '')

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
    if not RESEND_API_KEY or not ALERT_EMAIL:
        print("  ⚠ RESEND_API_KEY / ALERT_EMAIL no configurados — no se envía email")
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
        'to': [ALERT_EMAIL],
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
            print(f"  ✓ Email enviado a {ALERT_EMAIL}")
    except urllib.error.HTTPError as e:
        print(f"  ✗ Error enviando email: {e.code} — {e.read().decode()}")
    except Exception as e:
        print(f"  ✗ Error enviando email: {e}")

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

    # 1. Write prices.json (legacy fallback)
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

    # 3. Check alerts and notify by email
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
