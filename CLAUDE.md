# Portfolio Dashboard — Guía para Claude Code

## Estado actual (6 Oct 2026)
- Todos los workflows activos y corriendo exitosamente
- `Actualizar precios` (#266): OK hoy 18:03 UTC
- `Super Analyst` (#109): OK hoy 18:32 UTC
- `Monitor` (#310): OK hoy 18:14 UTC
- Supabase activo (fue reactivado manualmente en septiembre por inactividad de 60+ días)

---

## Repositorio
- **URL:** https://github.com/bautiimiranda04/Portfolio
- **GitHub Pages:** https://bautiimiranda04.github.io/Portfolio/
- **Rama principal:** `main`

---

## Archivos clave

### `index.html` — Dashboard del portfolio (CIFRADO)
El archivo está cifrado con **StaticCrypt v3**. Para editarlo:

1. **Descifrar con Node.js** (password: `Racing2014`, salt: `8b3de301ecaec787ec6852f67fd0f461`):
```js
const crypto = require('crypto');
// 3 rondas PBKDF2:
// SHA-1 / 1000 iteraciones / 32 bytes
// SHA-256 / 14000 iteraciones / 32 bytes  
// SHA-256 / 585000 iteraciones / 32 bytes
// AES-256-CBC, IV = primeros 32 hex chars del ciphertext
// El resultado puede tener ~30 bytes basura antes de <!DOCTYPE — buscar indexOf('<!DOCTYPE')
```

2. Editar el HTML descifrado
3. **Re-cifrar:**
```bash
npm install staticrypt
./node_modules/.bin/staticrypt archivo.html --password Racing2014 --short -o index.html
```
4. Hacer commit y push — GitHub Pages actualiza automáticamente

**Fixes ya aplicados en el HTML actual:**
- Botón "Vender" (antes decía "Vendida" erróneamente en todas las filas)
- Timestamp honesto: muestra "Yahoo live · [hora]" solo si al menos 1 fetch de Yahoo fue exitoso; si no, muestra "Supabase · [fecha más reciente]"

### `update_prices.py` — Actualización de precios
**Funciones principales:**
- `fetch_price(yahoo_symbol)` → `(price, history_dict)` — usa `range=15d` para obtener hasta 15 días de historial
- `fetch_watchlist_extended(ticker)` — datos extendidos (P/E, market cap, 52w) para watchlist
- `save_to_supabase(prices, today, history_map=None)` — guarda precios hoy + ayer + 7d + 14d
- `backfill_ticker(ticker, yahoo_sym)` — rellena historial de un ticker específico

**IMPORTANTE — SYMBOL_OVERRIDE:**
```python
SYMBOL_OVERRIDE = {
    'XAU': 'XAUT-USD',  # Tether Gold (proxy de oro, cotiza 24/7)
    'BTC': 'BTC-USD',
    'ETH': 'ETH-USD',
}
```
Este override DEBE aplicarse en todas las funciones que llaman a Yahoo Finance, incluyendo `fetch_watchlist_extended`.

**URL de Supabase para upsert (CRÍTICO):**
```python
url = f'{SUPABASE_URL}/rest/v1/price_history?on_conflict=ticker,date'
# Headers necesarios:
# 'Prefer': 'resolution=merge-duplicates,return=minimal'
```
Sin `?on_conflict=ticker,date` en la URL, las inserciones dan 409 Conflict.

**Lógica de fines de semana:**
- Solo actualiza tickers con categoría `gold` o `crypto` (ALWAYS_ON_CATEGORIES)
- No guarda historial (history_map=None) los fines de semana

### `analyze_portfolio.py` — Super Analyst (IA)
- Usa **yfinance** (instalar con pip) + **Gemini API**
- Modelos: `gemini-2.5-flash` (principal), `gemini-2.5-flash-lite` (fallback)
- Obtiene posiciones de Supabase → analiza con Gemini → guarda en tabla `analyst_reports`
- Se ejecuta 12:30 UTC de lunes a viernes

---

## GitHub Actions Workflows

| Workflow | Archivo | Horario | ID |
|----------|---------|---------|-----|
| Actualizar precios | `update-prices.yml` | 12:00 y 21:00 UTC, todos los días | 255236953 |
| Super Analyst | `analyze.yml` | 12:30 UTC, lunes-viernes | 256631126 |
| Monitor | `monitor.yml` | Cada hora 12-22 UTC, lunes-viernes | 264155795 |

**⚠️ GitHub deshabilita workflows automáticamente si no hay commits en 60 días.**
El último commit fue el 28 ago 2026. Para oct 2026 = 39 días (todavía OK).
Si pasan 60 días sin commits, hay que ir a GitHub → Actions → cada workflow → "Enable workflow".

### Keepalive pendiente
Hay un workflow `keepalive.yml` **pendiente de agregar** que hace un commit cada 20 días para evitar la desactivación. No se pudo subir porque el PAT no tiene scope `workflow`.

**Para agregarlo:** ir a github.com/bautiimiranda04/Portfolio → `.github/workflows/` → "Add file" → crear `keepalive.yml` con este contenido:
```yaml
name: Keep-alive
on:
  schedule:
    - cron: '0 10 1,20 * *'  # Día 1 y 20 de cada mes
  workflow_dispatch:
jobs:
  keepalive:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: |
          echo "Last keepalive: $(date -u '+%Y-%m-%d %H:%M UTC')" > .github/keepalive.txt
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add .github/keepalive.txt
          git commit -m "chore: keepalive $(date -u '+%Y-%m-%d')" || echo "Nothing to commit"
          git push
```

---

## Supabase

**Tablas:**
- `positions` — posiciones del portfolio (ticker, cantidad, precio compra, etc.)
- `price_history` — historial de precios. **Unique constraint: `(ticker, date)`**
- `analyst_reports` — reportes del Super Analyst
- `watchlist` — tickers del watchlist con datos extendidos
- `watchlist_meta` — metadatos del watchlist
- `alerts` — alertas de precio configuradas

**⚠️ Supabase free tier pausa proyectos tras 7 días sin actividad.**
Si los workflows fallan con "Name or service not known" o DNS error → Supabase está pausado.
Solución: ir a supabase.com → dashboard → reactivar el proyecto manualmente.
Para evitarlo: agregar un ping a Supabase en `monitor.yml` o upgradear a plan pago.

**Credenciales:** en GitHub Secrets del repo:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`

---

## Credenciales / Secrets de GitHub

| Secret | Descripción |
|--------|-------------|
| `SUPABASE_URL` | URL del proyecto Supabase |
| `SUPABASE_SERVICE_KEY` | Service role key de Supabase |
| `RESEND_API_KEY` | Para envío de alertas por email (Resend.com) |
| `ALERT_EMAILS` | Emails separados por coma para recibir alertas |
| `GEMINI_API_KEY` | Para Super Analyst (Google AI) |
| `EMAILJS_TOKEN` | `O2EV97MeFZpQgEIblUvDa` — para monitor.yml (EmailJS) |

**GitHub PAT disponible:** guardado en el password manager del usuario (NO commitear en el repo)
- Scope: solo `repo` (NO tiene `workflow` — por eso no se puede subir keepalive.yml vía API)
- Para obtenerlo: github.com → Settings → Developer settings → Personal access tokens

---

## Yahoo Finance API

**Endpoint principal:**
```
https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=15d
```
- `result[0]['meta']['regularMarketPrice']` → precio actual
- `result[0]['timestamp']` + `result[0]['indicators']['quote'][0]['close']` → historial

**Endpoint extended (watchlist):**
```
https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=summaryDetail
```
⚠️ Este endpoint devuelve 401 para todos los tickers del watchlist. P/E y market cap no funcionan actualmente.

---

## Problemas conocidos / Pendientes

1. **`keepalive.yml` sin subir** — ver sección anterior. Necesita hacerse manualmente o con PAT con scope `workflow`.

2. **Supabase puede volver a pausarse** — el monitor no hace ping a Supabase fuera de horario de mercado. Considerar agregar un ping diario en el workflow.

3. **Yahoo quoteSummary 401** — Los P/E ratios y market caps del watchlist dan N/A. Necesita otra fuente de datos o autenticación.

4. **Monitor puede correr antes que precios** — Monitor corre a las 12:00 UTC igual que `Actualizar precios`. A veces el monitor detecta "sin datos" y re-dispara el workflow innecesariamente. Considerar retrasar el monitor a las 12:30.

5. **PCLA precio** — Históricamente tuvo precio desactualizado (2.35 en lugar de 2.20). Debería estar bien ahora con el workflow corriendo correctamente.

---

## Dashboard (index.html) — Funciones principales

El HTML descifrado contiene JavaScript vanilla que:
- Carga posiciones desde Supabase en tiempo real
- Obtiene precios actuales de Yahoo Finance directamente desde el navegador (CORS permitido)
- Calcula cambio diario y semanal comparando con `price_history` en Supabase (ayer y hace 7 días)
- Muestra "Yahoo live · [hora]" si al menos 1 precio vino de Yahoo; si no, "Supabase · [fecha]"
- `openSell(id)` — abre modal para registrar venta de posición
- Botón "Vender" (NO "Vendida") en cada fila de posición

---

## Flujo de desarrollo típico

```bash
# 1. Clonar repo
git clone https://github.com/bautiimiranda04/Portfolio.git
cd Portfolio

# 2. Para editar index.html: descifrar, editar, re-cifrar
npm install staticrypt
node decrypt.js  # o script manual con crypto
# ... editar el HTML ...
./node_modules/.bin/staticrypt decrypted.html --password Racing2014 --short -o index.html

# 3. Commitear
git add -A
git commit -m "descripción del cambio"
git push
# GitHub Pages actualiza en ~1-2 minutos
```

---

## Arquitectura general

```
GitHub Actions (3 workflows)
    │
    ├── update-prices.yml (2x/día)
    │     └── update_prices.py
    │           ├── Yahoo Finance API → precios actuales
    │           ├── Supabase price_history → guarda hoy + historial
    │           ├── Supabase watchlist → actualiza datos extendidos  
    │           └── Resend API → emails si alertas activadas
    │
    ├── analyze.yml (weekdays 12:30 UTC)
    │     └── analyze_portfolio.py
    │           ├── Supabase positions → obtiene posiciones
    │           ├── Yahoo Finance (via yfinance) → datos del mercado
    │           ├── Gemini API → análisis IA
    │           └── Supabase analyst_reports → guarda reporte
    │
    └── monitor.yml (hourly 12-22 UTC weekdays)
          └── Python inline en el YAML
                ├── Supabase price_history → verifica si hay datos de hoy
                ├── Si no hay datos → re-dispara update-prices.yml + analyze.yml
                └── EmailJS → notifica si hubo problema

GitHub Pages (index.html cifrado con StaticCrypt)
    ├── Usuario ingresa password "Racing2014"
    ├── JS descifra el HTML en el navegador
    └── Dashboard cargado:
          ├── Supabase → posiciones + historial de precios
          └── Yahoo Finance → precios en tiempo real
```
