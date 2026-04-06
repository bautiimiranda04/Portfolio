#!/usr/bin/env python3
"""
Super Analyst — analyze_portfolio.py
Uses yfinance (handles Yahoo Finance auth properly, no 429 issues).
"""

import os, json, datetime, time, sys
import urllib.request, urllib.error

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')
GEMINI_KEY   = os.environ.get('GEMINI_API_KEY', '')
YAHOO_OVERRIDE = {'XAU': 'XAUT-USD', 'BTC': 'BTC-USD', 'ETH': 'ETH-USD'}

def sb_get(path, params=''):
    url = f'{SUPABASE_URL}/rest/v1/{path}?{params}'
    hdrs = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Accept': 'application/json'}
    req = urllib.request.Request(url, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f'  sb_get error: {e}')
        return None

def sb_upsert_batch(path, data_list, on_conflict):
    url = f'{SUPABASE_URL}/rest/v1/{path}?on_conflict={on_conflict}'
    body = json.dumps(data_list).encode()
    req = urllib.request.Request(url, data=body, method='POST')
    req.add_header('apikey', SUPABASE_KEY)
    req.add_header('Authorization', f'Bearer {SUPABASE_KEY}')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Prefer', 'resolution=merge-duplicates,return=minimal')
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
            return True
    except urllib.error.HTTPError as e:
        print(f'  sb_upsert_batch error {e.code}: {e.read().decode()[:200]}')
        return False

def sb_upsert(path, data, on_conflict):
    url = f'{SUPABASE_URL}/rest/v1/{path}?on_conflict={on_conflict}'
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method='POST')
    req.add_header('apikey', SUPABASE_KEY)
    req.add_header('Authorization', f'Bearer {SUPABASE_KEY}')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Prefer', 'resolution=merge-duplicates,return=minimal')
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            r.read()
            return True
    except urllib.error.HTTPError as e:
        print(f'  sb_upsert error {e.code}: {e.read().decode()[:200]}')
        return False

def get_ticker_data(ticker):
    import yfinance as yf
    sym = YAHOO_OVERRIDE.get(ticker, ticker)
    try:
        t = yf.Ticker(sym)
        info = t.info or {}
        price = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        analyst = {
            'target_mean': info.get('targetMeanPrice'),
            'target_high': info.get('targetHighPrice'),
            'target_low': info.get('targetLowPrice'),
            'recommendation_key': info.get('recommendationKey', ''),
            'num_analyst_opinions': info.get('numberOfAnalystOpinions', 0),
            'revenue_growth': info.get('revenueGrowth'),
            'gross_margins': info.get('grossMargins'),
            'forward_pe': info.get('forwardPE'),
            'beta': info.get('beta'),
            'sector': info.get('sector', ''),
            'industry': info.get('industry', ''),
            'upgrades': [],
        }
        try:
            uph = t.upgrades_downgrades
            if uph is not None and len(uph) > 0:
                for _, row in uph.head(3).iterrows():
                    analyst['upgrades'].append({
                        'firm': str(row.get('Firm', '')),
                        'action': str(row.get('Action', '')),
                        'from': str(row.get('FromGrade', '')),
                        'to': str(row.get('ToGrade', ''))
                    })
        except Exception:
            pass
        try:
            news_raw = t.news or []
            recent_news = [
                {'title': n.get('content', {}).get('title', n.get('title', '')),
                 'publisher': n.get('content', {}).get('provider', {}).get('displayName', '')}
                for n in news_raw[:2]
            ]
        except Exception:
            recent_news = []
        return {'price': price, 'analyst': analyst, 'recent_news': recent_news}
    except Exception as e:
        print(f'  yfinance error {ticker}: {e}')
        return {'price': None, 'analyst': {}, 'recent_news': []}

def build_portfolio_context(positions, watchlist):
    import yfinance as yf

    unique_tickers = {}
    for p in positions:
        t = p['ticker']
        if t not in unique_tickers:
            unique_tickers[t] = {'name': p['name'], 'category': p['category']}

    watchlist_tickers = {w['ticker']: {'name': w.get('name', w['ticker']), 'note': w.get('note', ''), 'signal': w.get('signal', '')} for w in watchlist}

    print(f'Portfolio: {len(unique_tickers)} tickers | Watchlist: {len(watchlist_tickers)} tickers')

    # Batch price download
    print('  Batch download de precios...')
    all_syms = list({YAHOO_OVERRIDE.get(t, t) for t in unique_tickers})
    latest_prices = {}
    try:
        if len(all_syms) == 1:
            sym = all_syms[0]
            batch_df = yf.download(sym, period='5d', interval='1d', auto_adjust=True, progress=False)
            if not batch_df.empty:
                price_val = float(batch_df['Close'].dropna().iloc[-1])
                orig = next((t for t in unique_tickers if YAHOO_OVERRIDE.get(t, t) == sym), sym)
                latest_prices[orig] = price_val
        else:
            batch_df = yf.download(all_syms, period='5d', interval='1d', auto_adjust=True, progress=False, threads=True)
            if not batch_df.empty:
                close = batch_df['Close']
                for sym in all_syms:
                    try:
                        col = close[sym] if sym in close.columns else close
                        val = float(col.dropna().iloc[-1])
                        orig = next((t for t in unique_tickers if YAHOO_OVERRIDE.get(t, t) == sym), sym)
                        latest_prices[orig] = val
                    except Exception:
                        pass
        print(f'  {len(latest_prices)} precios obtenidos')
    except Exception as e:
        print(f'  Batch download error: {e}')

    portfolio_data = {}
    for ticker, meta in unique_tickers.items():
        print(f'  {ticker}...', end=' ', flush=True)
        data = get_ticker_data(ticker)
        time.sleep(0.3)
        current_price = latest_prices.get(ticker) or data['price']
        pos_list = [p for p in positions if p['ticker'] == ticker]
        total_qty = sum(p['qty'] for p in pos_list)
        total_invested = sum(p['qty'] * p['buy_price'] for p in pos_list)
        total_dividends = sum(float(p.get('dividends', 0) or 0) for p in pos_list)
        current_value = total_qty * current_price if current_price else 0
        gain = current_value - total_invested + total_dividends if current_value else 0
        gain_pct = gain / total_invested if total_invested > 0 else 0
        analyst = data['analyst']
        portfolio_data[ticker] = {
            'name': meta['name'], 'category': meta['category'],
            'qty': total_qty, 'invested': round(total_invested, 2),
            'current_price': round(current_price, 2) if current_price else None,
            'current_value': round(current_value, 2),
            'gain_usd': round(gain, 2), 'gain_pct': round(gain_pct * 100, 2),
            'analyst': analyst, 'recent_news': data['recent_news'],
            'sector': analyst.get('sector', ''),
        }
        reco = analyst.get('recommendation_key', '-')
        target = analyst.get('target_mean')
        px = f"${current_price:.2f}" if current_price else "N/D"
        print(f"OK {px} | {reco} | target={'$'+f'{target:.2f}' if target else '-'}")

    watchlist_data = {}
    for ticker, meta in list(watchlist_tickers.items())[:10]:
        if ticker in portfolio_data:
            continue
        print(f'  watchlist {ticker}...', end=' ', flush=True)
        data = get_ticker_data(ticker)
        time.sleep(0.3)
        watchlist_data[ticker] = {
            'name': meta['name'], 'note': meta['note'], 'signal': meta['signal'],
            'current_price': data['price'], 'analyst': data['analyst'],
        }
        print('ok')

    total_invested = sum(d['invested'] for d in portfolio_data.values())
    total_value = sum(d['current_value'] for d in portfolio_data.values())
    total_gain = sum(d['gain_usd'] for d in portfolio_data.values())
    total_gain_pct = (total_gain / total_invested * 100) if total_invested > 0 else 0

    return {
        'summary': {
            'total_invested': round(total_invested, 2), 'total_value': round(total_value, 2),
            'total_gain_usd': round(total_gain, 2), 'total_gain_pct': round(total_gain_pct, 2),
            'num_positions': len(unique_tickers), 'as_of': datetime.date.today().isoformat(),
        },
        'portfolio': portfolio_data, 'watchlist': watchlist_data,
    }

def call_gemini(prompt, retries=4):
    url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}'
    payload = {'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
               'generationConfig': {'temperature': 0.4, 'maxOutputTokens': 8192}}
    body = json.dumps(payload).encode()
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, method='POST')
        req.add_header('Content-Type', 'application/json')
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                resp = json.loads(r.read().decode())
                return resp['candidates'][0]['content']['parts'][0]['text']
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f'  Gemini 429 rate limit — esperando {wait}s (intento {attempt+1}/{retries})...')
                time.sleep(wait)
                continue
            print(f'  Gemini error HTTP {e.code}: {e.read().decode()[:200]}')
            return None
        except Exception as e:
            print(f'  Gemini error: {e}')
            if attempt < retries - 1:
                time.sleep(20)
                continue
            return None
    return None

def build_gemini_prompt(ctx):
    s = ctx['summary']
    pf = ctx['portfolio']
    wl = ctx['watchlist']

    pos_lines = []
    for ticker, d in sorted(pf.items(), key=lambda x: -abs(x[1]['gain_usd'])):
        a = d['analyst']
        reco = a.get('recommendation_key', '?').upper().replace('_', ' ')
        target = a.get('target_mean')
        target_str = f'Target: ${target:.2f}' if target else 'sin target'
        updown = f"+{d['gain_usd']:,.0f} ({d['gain_pct']:+.1f}%)" if d['gain_usd'] >= 0 else f"{d['gain_usd']:,.0f} ({d['gain_pct']:+.1f}%)"
        analysts_n = a.get('num_analyst_opinions') or 0
        upgrades_str = ''
        if a.get('upgrades'):
            u = a['upgrades'][0]
            upgrades_str = f" | Rec: {u['firm']} {u['action']} ({u['from']}→{u['to']})"
        news_str = ''
        if d.get('recent_news'):
            titles = [n['title'][:60] for n in d['recent_news'][:2] if n.get('title')]
            if titles:
                news_str = ' | Noticias: ' + ' / '.join(titles)
        px_str = f"${d['current_price']:.2f}" if d['current_price'] is not None else 'N/D'
        pos_lines.append(f"• {ticker} ({d['name']}) — {px_str} | G/P: {updown} | Wall St: {reco} ({analysts_n} analistas), {target_str}{upgrades_str}{news_str}")

    wl_lines = []
    for ticker, d in wl.items():
        a = d['analyst']
        reco = a.get('recommendation_key', '?').upper().replace('_', ' ')
        target = a.get('target_mean')
        target_str = f'Target: ${target:.2f}' if target else 'sin target'
        price = d['current_price']
        upside = ((target / price - 1) * 100) if target and price else None
        upside_str = f' ({upside:+.1f}% upside)' if upside else ''
        price_str = f'${price:.2f}' if price is not None else 'N/D'
        wl_lines.append(f"• {ticker} ({d['name']}) — {price_str} | Wall St: {reco}, {target_str}{upside_str} | Nota: {d['note']}")

    return f"""Sos un analista financiero senior con acceso a datos reales de Wall Street.
Analizá este portfolio de inversiones y generá un informe ejecutivo en español, profesional y directo.

═══ RESUMEN DEL PORTFOLIO ═══
Fecha: {s['as_of']}
Capital invertido: ${s['total_invested']:,.2f}
Valor actual: ${s['total_value']:,.2f}
Ganancia/pérdida total: ${s['total_gain_usd']:+,.2f} ({s['total_gain_pct']:+.2f}%)
Posiciones activas: {s['num_positions']} tickers únicos

═══ POSICIONES ACTIVAS (datos Yahoo Finance + Wall Street) ═══
{chr(10).join(pos_lines)}

═══ WATCHLIST / POTENCIALES INVERSIONES ═══
{chr(10).join(wl_lines) if wl_lines else '(Watchlist vacía)'}

Generá un informe JSON con esta estructura EXACTA (sin texto fuera del JSON):

{{
  "resumen_ejecutivo": "2-3 párrafos con el estado general del portfolio: qué funcionó, qué no, tendencias macro. Sé específico con números.",
  "semaforo": [{{"ticker": "XYZ", "emoji": "🟢/🟡/🔴", "estado": "Mantener/Comprar/Vender/Vigilar", "razon": "1 línea concisa"}}],
  "destacados_positivos": [{{"ticker": "XYZ", "titulo": "título corto", "detalle": "análisis con datos de Wall St"}}],
  "alertas": [{{"ticker": "XYZ", "nivel": "🔴/🟡", "titulo": "título corto", "detalle": "riesgo específico"}}],
  "oportunidades_watchlist": [{{"ticker": "XYZ", "titulo": "título", "detalle": "por qué ahora, upside potencial"}}],
  "accion_semanal": "1 acción concreta que el inversor debería considerar esta semana.",
  "contexto_macro": "2-3 oraciones sobre contexto de mercado global relevante para este portfolio."
}}

Reglas: Semáforo incluí TODAS las posiciones. Sé ESPECÍFICO con precios y targets. Tono profesional y directo."""

def main():
    missing = []
    if not SUPABASE_URL: missing.append('SUPABASE_URL')
    if not SUPABASE_KEY: missing.append('SUPABASE_SERVICE_KEY')
    if not GEMINI_KEY: missing.append('GEMINI_API_KEY (https://aistudio.google.com)')
    if missing:
        print('Faltan secrets:', ', '.join(missing))
        sys.exit(1)

    today = datetime.date.today().isoformat()
    print(f'\nSuper Analyst — {today}')
    print('=' * 50)

    print('\nCargando datos de Supabase...')
    positions = sb_get('positions', 'select=*')
    watchlist = sb_get('watchlist', 'select=*') or []
    if not positions:
        print('No se pudieron cargar posiciones')
        sys.exit(1)

    for p in positions:
        p['qty'] = float(p.get('qty', 0) or 0)
        p['buy_price'] = float(p.get('buy_price', 0) or 0)
        p['dividends'] = float(p.get('dividends', 0) or 0)

    print(f'{len(positions)} posiciones | {len(watchlist)} watchlist')

    print('\nObteniendo datos de Yahoo Finance...')
    ctx = build_portfolio_context(positions, watchlist)

    print('\nLlamando a Gemini...')
    prompt = build_gemini_prompt(ctx)
    ai_text = call_gemini(prompt)
    if not ai_text:
        print('Gemini no respondio')
        sys.exit(1)

    try:
        text = ai_text.strip()
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        report_json = json.loads(text.strip())
        print('Analisis generado OK')
    except json.JSONDecodeError as e:
        print(f'JSON parse error: {e}')
        report_json = {'resumen_ejecutivo': ai_text, 'error': 'parse_failed'}

    full_report = {
        'report_date': today,
        'portfolio_summary': ctx['summary'],
        'portfolio_data': ctx['portfolio'],
        'watchlist_data': ctx['watchlist'],
        'ai_analysis': report_json,
        'generated_at': datetime.datetime.utcnow().isoformat() + 'Z',
    }

    print('\nGuardando en Supabase...')
    ok = sb_upsert('analyst_reports', {'report_date': today, 'report_json': full_report}, on_conflict='report_date')
    if ok:
        print(f'Reporte del {today} guardado OK')
    else:
        print('Error al guardar - verificar que la tabla analyst_reports existe en Supabase')
        sys.exit(1)

    # Save daily prices to price_history table (enables daily/weekly change tracking in portfolio)
    print('\nGuardando precios en price_history...')
    price_rows = []
    for ticker, d in ctx['portfolio'].items():
        if d['current_price'] is not None:
            price_rows.append({'ticker': ticker, 'date': today, 'price': d['current_price']})
    for ticker, d in ctx['watchlist'].items():
        if d['current_price'] is not None:
            price_rows.append({'ticker': ticker, 'date': today, 'price': d['current_price']})
    if price_rows:
        ok2 = sb_upsert_batch('price_history', price_rows, on_conflict='ticker,date')
        print(f'  {len(price_rows)} precios guardados en price_history')

    print('\nSuper Analyst completado!')

if __name__ == '__main__':
    main()
