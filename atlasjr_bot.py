"""
Atlas Jr — COT Report Bot
Invia il COT Report settimanale ogni lunedì alle 7:30 (ora italiana)
al canale Telegram privato degli abbonati.

Variabili d'ambiente richieste:
  TELEGRAM_BOT_TOKEN   → token del bot da BotFather
  TELEGRAM_CHANNEL_ID  → ID del canale privato (es. -1003791690745)
  ANTHROPIC_API_KEY    → chiave API Anthropic
"""

import os
import asyncio
import requests
import anthropic
import pytz
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError

# ── Env vars ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "-1003791690745")
ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY")

ITALY_TZ = pytz.timezone("Europe/Rome")
client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ── Asset mapping ─────────────────────────────────────────────────────────────
# Legacy report (Socrata 6dca-aqww) — commodity classiche
COT_MARKETS_LEGACY = {
    "Gold":      "GOLD",
    "Silver":    "SILVER",
    "Crude Oil": "CRUDE OIL",
}

# Financial Futures report (Socrata gpe5-46if) — indici, valute, BTC
COT_MARKETS_FINANCIAL = {
    "S&P 500":       "S&P 500",
    "Euro":          "EURO FX",
    "British Pound": "BRITISH POUND",
    "Japanese Yen":  "JAPANESE YEN",
    "Bitcoin":       "BITCOIN",
}


# ── Fetch CFTC data ───────────────────────────────────────────────────────────
def fetch_cot_data():
    """Scarica Legacy COT report (commodities: Gold, Silver, Crude Oil)."""
    try:
        r = requests.get(
            "https://publicreporting.cftc.gov/resource/6dca-aqww.json",
            params={"$order": "report_date_as_yyyy_mm_dd DESC", "$limit": 5000},
            timeout=20,
        )
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def fetch_cot_financial_data():
    """Scarica Financial Futures COT report (indici, valute, BTC)."""
    try:
        r = requests.get(
            "https://publicreporting.cftc.gov/resource/gpe5-46if.json",
            params={"$order": "report_date_as_yyyy_mm_dd DESC", "$limit": 5000},
            timeout=20,
        )
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


# ── Parse COT rows ────────────────────────────────────────────────────────────
def get_cot_for_market(rows, market_substring):
    """Trova current e previous row per un dato mercato.
    Quando ci sono più match (es. GOLD vs GOLD MICRO) sceglie quello
    con Open Interest più alto = future principale liquido."""
    matches = []
    for row in rows:
        name = (row.get("market_and_exchange_names") or "").upper()
        if market_substring.upper() in name:
            if "MICRO" in name and "MICRO" not in market_substring.upper():
                continue
            matches.append(row)

    if not matches:
        return None, None

    by_date = {}
    for m in matches:
        date = m.get("report_date_as_yyyy_mm_dd", "")
        by_date.setdefault(date, []).append(m)

    selected = []
    for date in sorted(by_date.keys(), reverse=True):
        candidates = by_date[date]
        try:
            best = max(candidates, key=lambda x: float(x.get("open_interest_all", 0) or 0))
        except Exception:
            best = candidates[0]
        selected.append(best)

    current  = selected[0] if len(selected) > 0 else None
    previous = selected[1] if len(selected) > 1 else None
    return current, previous


def format_cot_position(current, previous):
    """Net positions dal Legacy report (commodity classiche)."""
    if not current:
        return None
    try:
        nc_long   = int(current.get("noncomm_positions_long_all",  0) or 0)
        nc_short  = int(current.get("noncomm_positions_short_all", 0) or 0)
        c_long    = int(current.get("comm_positions_long_all",     0) or 0)
        c_short   = int(current.get("comm_positions_short_all",    0) or 0)
        nr_long   = int(current.get("nonrept_positions_long_all",  0) or 0)
        nr_short  = int(current.get("nonrept_positions_short_all", 0) or 0)

        nc_net     = nc_long  - nc_short
        comm_net   = c_long   - c_short
        retail_net = nr_long  - nr_short

        result = {
            "report_date": current.get("report_date_as_yyyy_mm_dd", "N/A")[:10],
            "nc_net": nc_net,
            "comm_net": comm_net,
            "retail_net": retail_net,
        }

        if previous:
            pnc  = int(previous.get("noncomm_positions_long_all",  0) or 0) - int(previous.get("noncomm_positions_short_all", 0) or 0)
            pc   = int(previous.get("comm_positions_long_all",     0) or 0) - int(previous.get("comm_positions_short_all",    0) or 0)
            pnr  = int(previous.get("nonrept_positions_long_all",  0) or 0) - int(previous.get("nonrept_positions_short_all", 0) or 0)
            result["nc_change"]     = nc_net     - pnc
            result["comm_change"]   = comm_net   - pc
            result["retail_change"] = retail_net - pnr

        return result
    except Exception:
        return None


def format_cot_position_financial(current, previous):
    """Net positions dal Financial Futures report.
    Large Specs = Asset Managers + Leveraged Money
    Commercials = Dealers
    Retail      = Non-reportables"""
    if not current:
        return None
    try:
        am_long  = int(current.get("asset_mgr_positions_long",  0) or 0)
        am_short = int(current.get("asset_mgr_positions_short", 0) or 0)
        lm_long  = int(current.get("lev_money_positions_long",  0) or 0)
        lm_short = int(current.get("lev_money_positions_short", 0) or 0)
        nc_net   = (am_long + lm_long) - (am_short + lm_short)

        d_long   = int(current.get("dealer_positions_long",  0) or 0)
        d_short  = int(current.get("dealer_positions_short", 0) or 0)
        comm_net = d_long - d_short

        nr_long    = int(current.get("nonrept_positions_long_all",  0) or 0)
        nr_short   = int(current.get("nonrept_positions_short_all", 0) or 0)
        retail_net = nr_long - nr_short

        result = {
            "report_date": current.get("report_date_as_yyyy_mm_dd", "N/A")[:10],
            "nc_net": nc_net,
            "comm_net": comm_net,
            "retail_net": retail_net,
        }

        if previous:
            p_am_long  = int(previous.get("asset_mgr_positions_long",  0) or 0)
            p_am_short = int(previous.get("asset_mgr_positions_short", 0) or 0)
            p_lm_long  = int(previous.get("lev_money_positions_long",  0) or 0)
            p_lm_short = int(previous.get("lev_money_positions_short", 0) or 0)
            pnc        = (p_am_long + p_lm_long) - (p_am_short + p_lm_short)

            p_d_long  = int(previous.get("dealer_positions_long",  0) or 0)
            p_d_short = int(previous.get("dealer_positions_short", 0) or 0)
            pc        = p_d_long - p_d_short

            p_nr_long  = int(previous.get("nonrept_positions_long_all",  0) or 0)
            p_nr_short = int(previous.get("nonrept_positions_short_all", 0) or 0)
            pnr        = p_nr_long - p_nr_short

            result["nc_change"]     = nc_net     - pnc
            result["comm_change"]   = comm_net   - pc
            result["retail_change"] = retail_net - pnr

        return result
    except Exception:
        return None


# ── Build raw data text ───────────────────────────────────────────────────────
def build_data_text(cot_data):
    """Costruisce il testo grezzo da passare al prompt AI."""
    text = ""
    for asset, d in cot_data.items():
        s = lambda v: ("+" if v >= 0 else "") + str(v)
        text += f"\n{asset.upper()} (data: {d['report_date']}):\n"
        text += f"  - Large Specs (NonComm): {s(d['nc_net'])} contratti net"
        if "nc_change" in d:
            text += f" (var: {s(d['nc_change'])})"
        text += "\n"
        text += f"  - Commercials: {s(d['comm_net'])} contratti net"
        if "comm_change" in d:
            text += f" (var: {s(d['comm_change'])})"
        text += "\n"
        text += f"  - Retail (NonRep): {s(d['retail_net'])} contratti net"
        if "retail_change" in d:
            text += f" (var: {s(d['retail_change'])})"
        text += "\n"
    return text


# ── Generate COT report text via Claude ──────────────────────────────────────
def generate_cot_report(data_text, today_str):
    """Chiama Claude per generare il report interpretato."""
    prompt = (
        f"Genera il COT REPORT settimanale per gli abbonati di Atlas Jr.\n\n"
        f"DATI GREZZI dal CFTC:\n{data_text}\n\n"
        f"Format esatto da rispettare:\n"
        f"ATLAS JR COT REPORT - Settimana del {today_str}\n\n"
        f"Per ogni asset:\n"
        f"[NOME ASSET]:\n"
        f"- Commercials: net [LONG/SHORT] X contratti (variazione: +/- Y)\n"
        f"- Large Specs: net [LONG/SHORT] X contratti (variazione: +/- Y)\n"
        f"- Retail: net [LONG/SHORT] X contratti (variazione: +/- Y)\n"
        f"INTERPRETAZIONE: [1-2 righe di analisi basata sui numeri]\n\n"
        f"Alla fine del report aggiungi:\n"
        f"SEGNALI CHIAVE:\n"
        f"- [setup estremi di posizionamento se presenti]\n"
        f"- [divergenze tra categorie se presenti]\n"
        f"- [implicazione operativa per la settimana]\n\n"
        f"REGOLE:\n"
        f"- Usa SOLO i dati forniti, non inventare numeri\n"
        f"- Non usare markdown, emoji o formattazioni speciali\n"
        f"- Tono professionale e diretto\n"
        f"- Se manca un asset dai dati, non includerlo"
    )

    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


# ── Send COT report to channel ────────────────────────────────────────────────
async def send_cot_report(bot: Bot):
    """Fetch dati CFTC → genera report AI → invia al canale Telegram."""
    print(f"[{datetime.now(ITALY_TZ).strftime('%H:%M:%S')}] Avvio generazione COT Report...")

    legacy_rows    = fetch_cot_data()
    financial_rows = fetch_cot_financial_data()

    cot_data = {}

    if legacy_rows:
        for asset_name, market_str in COT_MARKETS_LEGACY.items():
            current, previous = get_cot_for_market(legacy_rows, market_str)
            formatted = format_cot_position(current, previous)
            if formatted:
                cot_data[asset_name] = formatted

    if financial_rows:
        for asset_name, market_str in COT_MARKETS_FINANCIAL.items():
            current, previous = get_cot_for_market(financial_rows, market_str)
            formatted = format_cot_position_financial(current, previous)
            if formatted:
                cot_data[asset_name] = formatted

    if not cot_data:
        print("Nessun dato COT disponibile. Report non inviato.")
        return

    data_text = build_data_text(cot_data)
    today_str = datetime.now(ITALY_TZ).strftime("%d/%m/%Y")

    try:
        report_text = generate_cot_report(data_text, today_str)
    except Exception as e:
        print(f"Errore generazione AI: {e}")
        return

    # Telegram max 4096 chars per message — split se necessario
    max_len = 4096
    chunks = [report_text[i:i+max_len] for i in range(0, len(report_text), max_len)]

    for chunk in chunks:
        try:
            await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=chunk)
        except TelegramError as e:
            print(f"Errore invio Telegram: {e}")
            return

    print(f"COT Report inviato al canale ({len(report_text)} chars, {len(chunks)} messaggio/i).")


# ── Scheduler loop ────────────────────────────────────────────────────────────
async def scheduler_loop(bot: Bot):
    """Loop che controlla ogni minuto se è lunedì alle 7:30 per inviare il COT."""
    sent_cot_this_week = None
    print("Atlas Jr scheduler avviato. In attesa di lunedì 7:30...")

    while True:
        try:
            now        = datetime.now(ITALY_TZ)
            week_key   = now.strftime("%Y-W%U")
            is_monday  = now.weekday() == 0
            is_time    = now.hour == 7 and 30 <= now.minute < 35

            if is_monday and is_time and sent_cot_this_week != week_key:
                await send_cot_report(bot)
                sent_cot_this_week = week_key

        except Exception as e:
            print(f"Errore scheduler: {e}")

        await asyncio.sleep(60)


# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN non impostato")
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY non impostato")

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    info = await bot.get_me()
    print(f"Atlas Jr avviato — bot: @{info.username} — canale: {TELEGRAM_CHANNEL_ID}")

    await scheduler_loop(bot)


if __name__ == "__main__":
    asyncio.run(main())
