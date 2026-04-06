#!/usr/bin/env python3
"""
Super Analyst — analyze_portfolio.py
Runs daily via GitHub Actions.
Fetches portfolio from Supabase, gets Yahoo Finance analyst data,
synthesizes with Gemini, saves report back to Supabase.
"""

import os, json, datetime, time, sys
import urllib.request, urllib.error

# ── CONFIG (from GitHub Secrets / env vars) ──────────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')   # https://xxxx.supabase.co
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']   # anon key
GEMINI_KEY   = os.environ.get('GEMINI_API_KEY', '')  # from aistudio.google.com

YAHOO_OVERRIDE = {'XAU':'XAUT-USD','BTC':'BTC-USD','ETH':'ETH-USD'}
HEADERS_YF = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36', 'Accept':'application/json'}

# ── HELPERS ──────────────────────────────────────────────────────────
def fetch_json(url, headers=None, timeout=12):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f'  ⚠ fetch_json error: {url[:80]} → {e}')
        return None

def sb_get(path, params=''):
    url = f'{SUPABASE_URL}/rest/v1/{path}?{params}'
    hdrs = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Accept': 'application/json'}
    return fetch_json(url, hdrs)

def sb_post(path, data, prefer='return=minimal'):
    url = f'{SUPABASE_URL}/rest/v1/{path}'
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method='POST')
    req.add_header('apikey', SUPABASE_KEY)
    req.add_header('Authorization', f'Bearer {SUPABASE_KEY}')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Prefer', prefer)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = r.read().decode()
            return json.loads(resp) if resp.strip() else {}
    except urllib.error.HTTPError as e:
        print(f'  ⚠ sb_post error {e.code}: {e.read().decode()[:200]}')
        return None

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
        print(f'  ⚠ sb_upsert error {e.code}: {e.read().decode()[:200]}')
        return False

# ── YAHOO FINANCE ─────────────────────────────────────────────────────
def ysym(ticker):
    return YAHOO_OVERRIDE.get(ticker, ticker)

def get_yahoo_summary(ticker):
    """Get analyst data: recommendationTrend, financialData, upgradeDowngradeHistory, assetProfile"""
    sym = ysym(ticker)
    modules = 'recommendationTrend,financialData,upgradeDowngradeHistory,assetProfile,defaultKeyStatistics'
    url = f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules={modules}'
    data = fetch_json(url, HEADERS_YF)
    if not data:
        url2 = f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules={modules}'
        data = fetch_json(url2, HEADERS_YF)
    if not data:
        return None
    result = data.get('quoteSummary', {}).get('result', [])
    return result[0] if result else None

def get_yahoo_price(ticker):
    sym = ysym(ticker)
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d'
    data = fetch_json(url, HEADERS_YF)
    if not data:
        url2 = f'https://query2.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d'
        data = fetch_json(url2, HEADERS_YF)
    if not data:
        return None
    meta = data.get('chart', {}).get('result', [{}])[0].get('meta', {})
    return {
        'price': meta.get('regularMarketPrice'),
        'prev_close': meta.get('previousClose') or meta.get('chartPreviousClose'),
        'currency': meta.get('currency', 'USD'),
        'name': meta.get('shortName') or meta.get('longName', ticker),
    }

def get_yahoo_news(ticker, n=3):
    sym = ysym(ticker)
    url = f'https://query1.finance.yahoo.com/v1/finance/search?q={sym}&newsCount={n}&quotesCount=0'
    data = fetch_json(url, HEADERS_YF)
    if not data:
        return []
    news = data.get('news', [])
    return [{'title': a.get('title', ''), 'publisher': a.get('publisher', ''), 'published': a.get('providerPublishTime', 0)} for a in news[:n]]

def parse_analyst(summary):
    """Extract key analyst metrics from quoteSummary result"""
    if not summary:
        return {}
    out = {}

    # Recommendation trend (last quarter)
    rt = summary.get('recommendationTrend', {}).get('trend', [])
    if rt:
        latest = rt[0]  # most recent period
        out['strong_buy'] = latest.get('strongBuy', 0)
        out['buy'] = latest.get('buy', 0)
        out['hold'] = latest.get('hold', 0)
        out['sell'] = latest.get('sell', 0)
        out['strong_sell'] = latest.get('strongSell', 0)
        total = sum([out['strong_buy'], out['buy'], out['hold'], out['sell'], out['strong_sell']])
        out['total_analysts'] = total

    # Financial data
    fd = summary.get('financialData', {})
    out['target_mean'] = fd.get('targetMeanPrice', {}).get('raw') if isinstance(fd.get('targetMeanPrice'), dict) else fd.get('targetMeanPrice')
    out['target_high'] = fd.get('targetHighPrice', {}).get('raw') if isinstance(fd.get('targetHighPrice'), dict) else fd.get('targetHighPrice')
    out['target_low'] = fd.get('targetLowPrice', {}).get('raw') if isinstance(fd.get('targetLowPrice'), dict) else fd.get('targetLowPrice')
    out['recommendation_key'] = fd.get('recommendationKey', '')  # 'buy','hold','sell','strong_buy'
    out['num_analyst_opinions'] = fd.get('numberOfAnalystOpinions', {}).get('raw') if isinstance(fd.get('numberOfAnalystOpinions'), dict) else fd.get('numberOfAnalystOpinions')

    # Revenue growth, gross margins
    out['revenue_growth'] = fd.get('revenueGrowth', {}).get('raw') if isinstance(fd.get('revenueGrowth'), dict) else None
    out['gross_margins'] = fd.get('grossMargins', {}).get('raw') if isinstance(fd.get('grossMargins'), dict) else None

    # Key stats
    ks = summary.get('defaultKeyStatistics', {})
    def raw(d, key):
        v = d.get(key, {})
        return v.get('raw') if isinstance(v, dict) else v
    out['pe_forward'] = raw(ks, 'forwardPE')
    out['beta'] = raw(ks, 'beta')
    out['short_ratio'] = raw(ks, 'shortRatio')

    # Recent upgrades/downgrades (last 5)
    uph = summary.get('upgradeDowngradeHistory', {}).get('history', [])[:5]
    out['upgrades'] = [
        {'firm': u.get('firm', ''), 'action': u.get('action', ''), 'from': u.get('fromGrade', ''), 'to': u.get('toGrade', '')}
        for u in uph
    ]

    # Asset profile
    ap = summary.get('assetProfile', {})
    out['sector'] = ap.get('sector', '')
    out['industry'] = ap.get('industry', '')
    out['country'] = ap.get('country', '')

    return out

# ── BUILD PORTFOLIO CONTEXT ───────────────────────────────────────────
def build_portfolio_context(positions, watchlist):
    """
    For each unique ticker, fetch price + analyst data.
    Returns a structured dict with portfolio summary and per-ticker data.
    """
    # Unique tickers from active positions
    unique_tickers = {}
    for p in positions:
        t = p['ticker']
        if t not in unique_tickers:
            unique_tickers[t] = {'name': p['name'], 'category': p['category']}

    # Unique tickers from watchlist
    watchlist_tickers = {}
    for w in watchlist:
        watchlist_tickers[w['ticker']] = {'name': w.get('name', w['ticker']), 'note': w.get('note', ''), 'signal': w.get('signal', '')}

    print(f'📊 Portfolio: {len(unique_tickers)} tickers | 👁 Watchlist: {len(watchlist_tickers)} tickers')

    # Fetch data for portfolio tickers
    portfolio_data = {}
    for ticker, meta in unique_tickers.items():
        print(f'  → {ticker}...', end=' ', flush=True)
        px = get_yahoo_price(ticker)
        time.sleep(0.4)  # rate limit
        summary = get_yahoo_summary(ticker)
        time.sleep(0.4)
        analyst = parse_analyst(summary)
        news = get_yahoo_news(ticker, 2)
        time.sleep(0.3)

        # Calculate portfolio stats for this ticker
        pos_list = [p for p in positions if p['ticker'] == ticker]
        total_qty = sum(p['qty'] for p in pos_list)
        total_invested = sum(p['qty'] * p['buy_price'] for p in pos_list)
        current_price = px['price'] if px else (pos_list[0]['last_price'] or pos_list[0]['buy_price'])
        current_value = total_qty * current_price if current_price else 0
        total_dividends = sum(p.get('dividends', 0) or 0 for p in pos_list)
        gain = (current_value - total_invested + total_dividends) if current_value else 0
        gain_pct = gain / total_invested if total_invested > 0 else 0

        portfolio_data[ticker] = {
            'name': meta['name'],
            'category': meta['category'],
            'qty': total_qty,
            'invested': round(total_invested, 2),
            'current_price': current_price,
            'current_value': round(current_value, 2),
            'gain_usd': round(gain, 2),
            'gain_pct': round(gain_pct * 100, 2),
            'analyst': analyst,
            'recent_news': news,
            'sector': analyst.get('sector') or '',
            'industry': analyst.get('industry') or '',
        }
        rating = analyst.get('recommendation_key', '—')
        target = analyst.get('target_mean')
        print(f'✓ ${current_price:.2f} | rating={rating} | target={f"${target:.2f}" if target else "—"}')

    # Fetch watchlist data (lighter — just price + analyst rating)
    watchlist_data = {}
    for ticker, meta in list(watchlist_tickers.items())[:10]:  # limit to 10
        if ticker in portfolio_data:
            continue  # already have it
        print(f'  👁 watchlist {ticker}...', end=' ', flush=True)
        px = get_yahoo_price(ticker)
        time.sleep(0.4)
        summary = get_yahoo_summary(ticker)
        time.sleep(0.4)
        analyst = parse_analyst(summary)
        watchlist_data[ticker] = {
            'name': meta['name'],
            'note': meta['note'],
            'signal': meta['signal'],
            'current_price': px['price'] if px else None,
            'analyst': analyst,
        }
        print(f'✓')

    # Portfolio summary
    total_invested = sum(d['invested'] for d in portfolio_data.values())
    total_value = sum(d['current_value'] for d in portfolio_data.values())
    total_gain = sum(d['gain_usd'] for d in portfolio_data.values())
    total_gain_pct = (total_gain / total_invested * 100) if total_invested > 0 else 0

    return {
        'summary': {
            'total_invested': round(total_invested, 2),
            'total_value': round(total_value, 2),
            'total_gain_usd': round(total_gain, 2),
            'total_gain_pct': round(total_gain_pct, 2),
            'num_positions': len(unique_tickers),
            'as_of': datetime.date.today().isoformat(),
        },
        'portfolio': portfolio_data,
        'watchlist': watchlist_data,
    }

# ── GEMINI API ────────────────────────────────────────────────────────
def call_gemini(prompt, system=''):
    url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}'
    payload = {
        'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
        'systemInstruction': {'parts': [{'text': system}]} if system else None,
        'generationConfig': {'temperature': 0.4, 'maxOutputTokens': 8192}
    }
    if not payload['systemInstruction']:
        del payload['systemInstruction']
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, method='POST')
    req.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            resp = json.loads(r.read().decode())
            return resp['candidates'][0]['content']['parts'][0]['text']
    except Exception as e:
        print(f'  ⚠ Gemini error: {e}')
        return None

def build_gemini_prompt(ctx):
    s = ctx['summary']
    pf = ctx['portfolio']
    wl = ctx['watchlist']

    # Format portfolio positions
    pos_lines = []
    for ticker, d in sorted(pf.items(), key=lambda x: -abs(x[1]['gain_usd'])):
        a = d['analyst']
        reco = a.get('recommendation_key', '?').upper()
        target = a.get('target_mean')
        target_str = f'Target: ${target:.2f}' if target else 'sin target'
        updown = f"+{d['gain_usd']:,.0f} ({d['gain_pct']:+.1f}%)" if d['gain_usd'] >= 0 else f"{d['gain_usd']:,.0f} ({d['gain_pct']:+.1f}%)"
        analysts_n = a.get('total_analysts') or a.get('num_analyst_opinions') or 0
        upgrades_str = ''
        if a.get('upgrades'):
            u = a['upgrades'][0]
            upgrades_str = f" | Último: {u['firm']} {u['action']} ({u['from']}→{u['to']})"
        news_str = ''
        if d.get('recent_news'):
            news_str = ' | Noticias: ' + ' | '.join(n['title'][:60] for n in d['recent_news'][:2])
        pos_lines.append(
            f"• {ticker} ({d['name']}) — ${d['current_price']:.2f} | G/P: {updown} | "
            f"Wall St: {reco} ({analysts_n} analistas), {target_str}{upgrades_str}{news_str}"
        )

    # Format watchlist
    wl_lines = []
    for ticker, d in wl.items():
        a = d['analyst']
        reco = a.get('recommendation_key', '?').upper()
        target = a.get('target_mean')
        target_str = f'Target: ${target:.2f}' if target else 'sin target'
        price = d['current_price']
        upside = ((target / price - 1) * 100) if target and price else None
        upside_str = f' ({upside:+.1f}% upside)' if upside else ''
        wl_lines.append(f"• {ticker} ({d['name']}) — ${price:.2f} | Wall St: {reco}, {target_str}{upside_str} | Nota: {d['note']}")

    prompt = f"""Sos un analista financiero senior con acceso a datos reales de Wall Street.
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

═══ INSTRUCCIONES PARA EL INFORME ═══

Generá un informe JSON con esta estructura EXACTA (sin texto fuera del JSON):

{{
  "resumen_ejecutivo": "2-3 párrafos con el estado general del portfolio: qué funcionó, qué no, tendencias macro relevantes. Sé específico con números.",

  "semaforo": [
    {{"ticker": "XYZ", "emoji": "🟢", "estado": "Mantener/Comprar/Vender/Vigilar", "razon": "1 línea concisa basada en datos"}}
  ],

  "destacados_positivos": [
    {{"ticker": "XYZ", "titulo": "título corto", "detalle": "análisis con datos concretos de Wall St, por qué seguir o tomar ganancias"}}
  ],

  "alertas": [
    {{"ticker": "XYZ", "nivel": "🔴/🟡", "titulo": "título corto", "detalle": "riesgo específico con datos"}}
  ],

  "oportunidades_watchlist": [
    {{"ticker": "XYZ", "titulo": "título corto", "detalle": "por qué ahora puede ser buen momento de entrada, upside potencial"}}
  ],

  "accion_semanal": "1 acción concreta y prioritaria que el inversor debería considerar esta semana. Sin vaguedades.",

  "contexto_macro": "2-3 oraciones sobre el contexto de mercado global relevante para este portfolio específico."
}}

Reglas:
- Semáforo: incluí TODAS las posiciones del portfolio
- Sé ESPECÍFICO: citá precios, targets, porcentajes de los datos provistos
- Si Wall St dice BUY con target 20% upside, decilo
- Si hay upgrades/downgrades recientes, mencionalos
- Tono: profesional, directo, sin relleno
- Si algo es incierto, decilo sin inventar datos"""

    return prompt

# ── MAIN ──────────────────────────────────────────────────────────────
def main():
    # Validate required secrets up front
    missing = []
    if not os.environ.get('SUPABASE_URL'): missing.append('SUPABASE_URL')
    if not os.environ.get('SUPABASE_SERVICE_KEY'): missing.append('SUPABASE_SERVICE_KEY')
    if not os.environ.get('GEMINI_API_KEY'):
        missing.append('GEMINI_API_KEY — obtenerla en: https://aistudio.google.com → Get API Key → Create API Key')
    if missing:
        print('❌ Faltan los siguientes secrets de GitHub Actions:')
        for m in missing:
            print(f'   • {m}')
        print('\n👉 Ir a: github.com/bautiimiranda04/Portfolio → Settings → Secrets and variables → Actions')
        sys.exit(1)

    today = datetime.date.today().isoformat()
    print(f'\n🧠 Super Analyst — {today}')
    print('=' * 50)

    # 1. Load positions from Supabase
    print('\n📥 Cargando datos de Supabase...')
    positions = sb_get('positions', 'select=*')
    watchlist = sb_get('watchlist', 'select=*') or []

    if not positions:
        print('❌ No se pudieron cargar las posiciones')
        sys.exit(1)

    print(f'  ✓ {len(positions)} posiciones | {len(watchlist)} watchlist')

    # 2. Build portfolio context with Yahoo Finance data
    print('\n📡 Obteniendo datos de Yahoo Finance...')
    ctx = build_portfolio_context(positions, watchlist)

    # 3. Call Gemini
    print('\n🤖 Llamando a Gemini...')
    prompt = build_gemini_prompt(ctx)
    ai_text = call_gemini(prompt)

    if not ai_text:
        print('❌ Gemini no respondió')
        sys.exit(1)

    # Parse JSON from Gemini response
    try:
        # Sometimes Gemini wraps in ```json ... ```
        text = ai_text.strip()
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        report_json = json.loads(text.strip())
    except json.JSONDecodeError as e:
        print(f'⚠ Gemini JSON parse error: {e}')
        print('Raw response (first 500 chars):', ai_text[:500])
        # Save raw text as fallback
        report_json = {'resumen_ejecutivo': ai_text, 'error': 'parse_failed'}

    print('  ✓ Análisis generado')

    # 4. Build full report to save
    full_report = {
        'report_date': today,
        'portfolio_summary': ctx['summary'],
        'portfolio_data': ctx['portfolio'],
        'watchlist_data': ctx['watchlist'],
        'ai_analysis': report_json,
        'generated_at': datetime.datetime.utcnow().isoformat() + 'Z',
    }

    # 5. Save to Supabase analyst_reports table
    print('\n💾 Guardando en Supabase...')
    # Try upsert — if table doesn't exist, we get a 404 with clear message
    ok = sb_upsert('analyst_reports', {
        'report_date': today,
        'report_json': full_report,
    }, on_conflict='report_date')

    if ok:
        print(f'  ✓ Reporte del {today} guardado exitosamente')
    else:
        print(f'  ❌ Error al guardar reporte — probablemente la tabla analyst_reports no existe.')
        print('  👉 Crear la tabla en Supabase SQL Editor:')
        print('     https://supabase.com/dashboard/project/wnymdtditjzvqftuhzlf/sql/new')
        print("""     SQL a ejecutar:
     CREATE TABLE IF NOT EXISTS analyst_reports (
       id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
       report_date DATE NOT NULL UNIQUE,
       report_json JSONB NOT NULL,
       created_at TIMESTAMPTZ DEFAULT NOW()
     );
     ALTER TABLE analyst_reports ENABLE ROW LEVEL SECURITY;
     CREATE POLICY \"read_all\" ON analyst_reports FOR SELECT USING (true);
     """)
        sys.exit(1)

    print('\n✅ Super Analyst completado')

if __name__ == '__main__':
    main()
