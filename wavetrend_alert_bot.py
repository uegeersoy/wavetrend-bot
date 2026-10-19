import json
import math
import os
import random
import string
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pandas as pd
import websocket

# ------------------------- AYARLAR -------------------------
# TradingView izleme listelerindeki birebir borsa eşleşmeleri
PAIR_EXCHANGES = {
    # Kırmızı liste (28 parite)
    "AUDCAD": "OANDA",
    "AUDCHF": "OANDA",
    "AUDGBP": "SAXO",
    "AUDNOK": "SAXO",
    "AUDNZD": "OANDA",
    "AUDPLN": "SAXO",
    "AUDSEK": "SAXO",
    "AUDSGD": "FOREXCOM",
    "AUDUSD": "FOREXCOM",
    "CADCHF": "FX",
    "CADPLN": "FOREXCOM",
    "CHFNOK": "FOREXCOM",
    "CHFPLN": "FOREXCOM",
    "CHFSEK": "SAXO",
    "CHFSGD": "SAXO",
    "EURAUD": "FX",
    "EURCAD": "FX",
    "EURGBP": "FX",
    "EURJPY": "FX",
    "EURNOK": "FX",
    "EURNZD": "FX",
    "EURPLN": "OANDA",
    "EURSEK": "FX",
    "EURSGD": "OANDA",
    "EURUSD": "FX",
    "GBPCAD": "FX",
    "EURCHF": "FX",
    "NZDCAD": "OANDA",
    # İzleme listesi - Forex (20 parite)
    "CHFNZD": "FX_IDC",
    "GBPAUD": "FX",
    "GBPCHF": "FX",
    "GBPNOK": "FOREXCOM",
    "GBPNZD": "FX",
    "GBPSEK": "FOREXCOM",
    "GBPSGD": "FOREXCOM",
    "USDNOK": "FX",
    "USDPLN": "FOREXCOM",
    "USDSEK": "FX",
    "USDSGD": "FOREXCOM",
    "USDNZD": "FX_IDC",
    "USDCHF": "FX",
    "USDGBP": "FX_IDC",
    "USDCAD": "FX",
    "NZDCHF": "FX",
    "NZDGBP": "FX_IDC",
    "NZDPLN": "FX_IDC",
    "NZDSGD": "FX_IDC",
    "NOKSEK": "FOREXCOM",
}

PAIRS = list(PAIR_EXCHANGES.keys())
UPPER = 65                  # bu değer ve üstü = üst bölge
LOWER = -65                 # bu değer ve altı = alt bölge
N1 = 10                     # Channel Length
N2 = 21                     # Average Length
CHECK_EVERY_SEC = int(os.environ.get("CHECK_EVERY_SEC", "1800"))   # varsayılan 30 dk
PAUSE_BETWEEN_PAIRS = 0.3   # pariteler arası bekleme
NOTIFY_RETREAT = False      # True: çizgi bölge içinde geri çekilirken de bildirim
DATA_SOURCE = os.environ.get("DATA_SOURCE", "tradingview").lower() # varsayılan: tradingview
FALLBACK_EXCHANGES = ["FX", "FX_IDC", "OANDA", "FOREXCOM", "SAXO"]
# -----------------------------------------------------------


class NoData(Exception):
    """Parite için veri alınamadı."""


def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def wavetrend(df):
    """Pine kodundaki wt1 (tci) hesabı."""
    ap = (df["High"] + df["Low"] + df["Close"]) / 3
    esa = ema(ap, N1)
    d = ema((ap - esa).abs(), N1)
    ci = (ap - esa) / (0.015 * d)
    return ema(ci, N2)


def fetch_yahoo(pair):
    import yfinance as yf
    df = yf.Ticker(f"{pair}=X").history(period="2y", interval="1d", auto_adjust=False)
    if df is None or df.empty:
        return None
    return df.dropna(subset=["High", "Low", "Close"])


def _fetch_tv_single(pair, exchange):
    ws = websocket.create_connection(
        "wss://data.tradingview.com/socket.io/websocket",
        headers={"Origin": "https://www.tradingview.com"},
        timeout=8,
    )
    cs = "cs_" + "".join(random.choices(string.ascii_lowercase, k=12))

    def send(m, p):
        msg = json.dumps({"m": m, "p": p})
        ws.send(f"~m~{len(msg)}~m~{msg}")

    send("set_auth_token", ["unauthorized_user_token"])
    send("chart_create_session", [cs, ""])
    send("resolve_symbol", [cs, "sds_sym_1", f'={{"symbol":"{exchange}:{pair}","adjustment":"splits"}}'])
    send("create_series", [cs, "sds_1", "s1", "sds_sym_1", "1D", 300, ""])

    bars = []
    try:
        for _ in range(40):
            raw = ws.recv()
            if "timescale_update" in raw:
                for part in raw.split("~m~"):
                    if part.startswith("{"):
                        d = json.loads(part)
                        if d.get("m") == "timescale_update":
                            s = d["p"][1].get("sds_1", {}).get("s", [])
                            for b in s:
                                v = b["v"]
                                dt = datetime.fromtimestamp(v[0] + 14400, tz=timezone.utc)
                                bars.append({
                                    "Date": pd.Timestamp(dt.date()),
                                    "Open": v[1],
                                    "High": v[2],
                                    "Low": v[3],
                                    "Close": v[4],
                                })
                break
            if "symbol_error" in raw or "critical_error" in raw:
                break
    finally:
        ws.close()

    if not bars:
        return None
    df = pd.DataFrame(bars).set_index("Date")
    df.attrs["exchange"] = exchange
    return df.dropna(subset=["High", "Low", "Close"])


def fetch_tradingview(pair):
    """Önce paritenin kendi borsasını, olmazsa yedek borsaları dener."""
    preferred = PAIR_EXCHANGES.get(pair)
    exchanges = ([preferred] if preferred else []) + [e for e in FALLBACK_EXCHANGES if e != preferred]
    for ex in exchanges:
        try:
            df = _fetch_tv_single(pair, ex)
            if df is not None and len(df) >= 100:
                return df
        except Exception:
            continue
    return None


def fetch_daily(pair):
    if DATA_SOURCE == "yahoo":
        return fetch_yahoo(pair)
    return fetch_tradingview(pair)


def level_of(v):
    if v >= UPPER:
        return math.floor(v)
    if v <= LOWER:
        return math.ceil(v)
    return None


def decide(prev, lv):
    if lv is None:
        return False, None
    if prev is None or (prev > 0) != (lv > 0):
        return True, lv
    if abs(lv) > abs(prev):
        return True, lv
    if abs(lv) < abs(prev) and NOTIFY_RETREAT:
        return True, lv
    return False, prev


def check_pair(pair, state, df=None):
    if df is None:
        try:
            df = fetch_daily(pair)
        except Exception as e:
            print(f"    ({pair}) veri hatası: {e}")
            df = None
    if df is None or len(df) < 100:
        raise NoData(pair)

    wt1 = wavetrend(df)
    v = float(wt1.iloc[-1])
    day = str(df.index[-1].date())
    ex_info = df.attrs.get("exchange", DATA_SOURCE)
    lv = level_of(v)
    print(f"{pair} [{ex_info}]: wt1 = {v:.2f}  (mum: {day})")

    notify, new = decide(state.get(pair), lv)
    if not notify:
        return None, new

    arrow, taraf = ("🔼", "üst") if lv > 0 else ("🔽", "alt")
    msg = f"{arrow} {pair} ({ex_info}): WaveTrend {v:.2f}\nSeviye: {lv} ({taraf} bölge) | Günlük mum: {day} (anlık)"
    return msg, new


def send_telegram(text):
    import requests
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_TOKEN ve TELEGRAM_CHAT_ID tanımlı değil.")
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={"chat_id": chat_id, "text": text},
        timeout=15,
    )
    r.raise_for_status()


def run_once(state):
    failed = []
    for pair in PAIRS:
        try:
            msg, new = check_pair(pair, state)
            if msg:
                send_telegram(msg)
                print(f"    -> bildirim gönderildi: {pair}")
            state[pair] = new
        except NoData:
            print(f"[!] {pair}: veri alınamadı")
            failed.append(pair)
        except Exception as e:
            print(f"[!] {pair}: hata: {e}")
        time.sleep(PAUSE_BETWEEN_PAIRS)
    return failed


def show_history(pairs):
    for pair in pairs:
        try:
            df = fetch_daily(pair)
        except Exception as e:
            print(f"\n{pair}: veri hatası: {e}")
            df = None
        if df is None or len(df) < 100:
            print(f"\n{pair}: veri alınamadı")
            continue
        wt1 = wavetrend(df)
        ex_info = df.attrs.get("exchange", DATA_SOURCE)
        print(f"\n{pair}   [{ex_info}]  (tarih | kapanış | wt1)")
        for i in range(-10, 0):
            print(f"  {df.index[i].date()}  {df['Close'].iloc[i]:.5f}  {wt1.iloc[i]:8.2f}")


class PingHandler(BaseHTTPRequestHandler):
    def _reply(self, with_body):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        if with_body:
            self.wfile.write(b"ok")

    def do_GET(self):
        self._reply(True)

    def do_HEAD(self):
        self._reply(False)

    def log_message(self, *args):
        pass


def start_web_server():
    port = os.environ.get("PORT")
    if not port:
        return
    server = HTTPServer(("0.0.0.0", int(port)), PingHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Ping sunucusu {port} portunda çalışıyor")


def main():
    if "--test" in sys.argv:
        send_telegram("✅ Bot çalışıyor.")
        print("Deneme mesajı gönderildi.")
        return

    if "--history" in sys.argv:
        args = sys.argv[sys.argv.index("--history") + 1:]
        show_history([a.upper() for a in args] or ["EURSEK"])
        return

    start_web_server()
    state = {}
    once = "--once" in sys.argv
    if not once:
        try:
            send_telegram(f"✅ Bot başladı, {len(PAIRS)} parite izleniyor (kaynak: {DATA_SOURCE}).")
        except Exception as e:
            print(f"[!] Başlangıç mesajı gönderilemedi: {e}")

    first = True
    while True:
        print(f"\n--- Kontrol: {datetime.now():%Y-%m-%d %H:%M:%S} ---")
        failed = run_once(state)
        if first and failed and not once:
            try:
                send_telegram("⚠️ Veri alınamayan pariteler (izlenmiyor):\n" + ", ".join(failed))
            except Exception as e:
                print(f"[!] Uyarı mesajı gönderilemedi: {e}")
        first = False
        if once:
            break
        time.sleep(CHECK_EVERY_SEC)


if __name__ == "__main__":
    main()
