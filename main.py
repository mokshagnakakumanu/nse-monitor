import time
import threading
import requests
import pyotp
import csv
import pytz
import sys
import os
from datetime import datetime
from io import StringIO
from fastapi import FastAPI
from contextlib import asynccontextmanager
from nxtradstream import NxtradStream

# ================= CONFIGURATION =================
# Passwords are loaded from Render/Koyeb Settings
API_KEY            = os.environ.get("TRADEJINI_API_KEY")
PASSWORD           = os.environ.get("TRADEJINI_PASSWORD")
TOTP_SECRET        = os.environ.get("TRADEJINI_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")
HOST               = "api.tradejini.com"

# ================= GLOBAL STORAGE =================
static_52_highs = {} 
token_map = {}
breakout_logs = [] 
triggered_tokens = set()

# ================= HELPERS =================
def get_ist_time():
    try:
        IST = pytz.timezone('Asia/Kolkata')
        return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def send_telegram_alert(message):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"⚠️ Telegram failed: {e}")

def get_access_token():
    if not API_KEY: return None
    url = f"https://{HOST}/v2/api-gw/oauth/individual-token-v2"
    try:
        totp = pyotp.TOTP(TOTP_SECRET)
        headers = {"Authorization": "Bearer " + API_KEY}
        data = {'password': PASSWORD, 'twoFa': totp.now(), 'twoFaTyp': 'totp'}
        res = requests.post(url, data=data, headers=headers)
        if res.status_code == 200: return res.json().get('access_token')
    except: pass
    return None

def load_master_data(token):
    print("📥 Downloading Master List...")
    url = f"https://{HOST}/api/mkt-data/scrips/symbol-store/Securities"
    headers = {"Authorization": f"Bearer {API_KEY}:{token}"}
    try:
        res = requests.get(url, headers=headers)
        if res.status_code != 200: return []
        csv_data = StringIO(res.text)
        reader = csv.DictReader(csv_data)
        stream_list = []
        count = 0
        for row in reader:
            if row.get('id', '').endswith('_EQ_NSE'):
                t = row['excToken']
                token_map[t] = row['dispName']
                stream_list.append(f"{t}_NSE")
                count += 1
        print(f"✅ Loaded {count} NSE Equity symbols.")
        return stream_list
    except Exception as e:
        print(f"❌ Error loading master: {e}")
        return []

# ================= TRADING LOGIC =================
def stream_callback(ws, data):
    try:
        if data.get('msgType') == 'L1':
            tk = str(data.get('token'))
            ltp = data.get('ltp')
            y_high = data.get('yHigh') 
            
            # Cache Highs
            if tk not in static_52_highs and y_high:
                static_52_highs[tk] = y_high
            
            # CHECK BREAKOUT
            if tk in static_52_highs and ltp and ltp > static_52_highs[tk]:
                if tk not in triggered_tokens:
                    symbol = token_map.get(tk, "Unknown")
                    prev_high = static_52_highs[tk]
                    timestamp = get_ist_time()
                    diff = round(ltp - prev_high, 2)
                    percent = 0.0
                    if prev_high > 0: percent = round((diff / prev_high) * 100, 2)
                    
                    log_entry = {
                        "time": timestamp, "symbol": symbol, "ltp": ltp,
                        "previous_52h": prev_high, "break_by": diff
                    }
                    breakout_logs.append(log_entry)
                    triggered_tokens.add(tk)
                    
                    print(f"🚀 BREAKOUT: {symbol} | LTP: {ltp}")
                    msg = (f"🚨 <b>52-WEEK HIGH BREACHED</b> 🚨\n\n"
                           f"📈 <b>Stock:</b> {symbol}\n💰 <b>LTP:</b> {ltp}\n"
                           f"🛑 <b>Old 52W High:</b> {prev_high}\n"
                           f"⚡ <b>Break by:</b> {diff} ({percent}%)\n"
                           f"🕒 <b>Time:</b> {timestamp}")
                    send_telegram_alert(msg)
    except Exception as e:
        print(f"Stream Error: {e}")

def connect_callback(ws, ev):
    print(f"📡 Stream Status: {ev}")

def start_background_worker():
    while True:
        try:
            token = get_access_token()
            if token:
                symbols = load_master_data(token)
                if not symbols:
                    time.sleep(60)
                    continue
                auth_token = f"{API_KEY}:{token}"
                nx = NxtradStream(HOST, stream_cb=stream_callback, connect_cb=connect_callback)
                nx.connect(auth_token)
                print("📡 Subscribing...")
                chunk_size = 100
                for i in range(0, len(symbols), chunk_size):
                    chunk = symbols[i:i+chunk_size]
                    nx.subscribeL1(chunk)
                    time.sleep(0.2)
                print("✅ Monitoring Live Market...")
                send_telegram_alert("🤖 <b>System Started</b>: Monitoring NSE Stocks.")
                while nx.isConnected:
                    time.sleep(5)
            print("⚠️ Restarting worker in 10s...")
            time.sleep(10)
        except Exception as e:
            print(f"❌ Worker Error: {e}")
            time.sleep(10)

# ================= FASTAPI APP =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=start_background_worker, daemon=True)
    t.start()
    yield

app = FastAPI(lifespan=lifespan, title="NSE Breakout Monitor")

@app.get("/")
def home():
    return {"status": "Running", "breakouts_today": len(breakout_logs), "time": get_ist_time()}

@app.get("/logs")
def get_logs():
    return sorted(breakout_logs, key=lambda x: x['time'], reverse=True)

@app.get("/check-telegram")
def test_alert():
    send_telegram_alert("✅ Test Alert from Tradejini Monitor")
    return {"status": "Test message sent"}

# --- THIS IS THE SIMULATION BUTTON ---
@app.get("/simulate-breakout")
def simulate_logic():
    """Tricks the bot into thinking a breakout occurred."""
    dummy_token = "999999"
    token_map[dummy_token] = "FAKE-STOCK-TEST"
    static_52_highs[dummy_token] = 100.00
    
    fake_packet = {
        'msgType': 'L1',
        'token': 999999,
        'ltp': 105.50, # Breakout!
        'yHigh': 100.0
    }
    
    if dummy_token in triggered_tokens:
        triggered_tokens.remove(dummy_token)

    print("🧪 Injecting Fake Packet...")
    stream_callback(None, fake_packet)
    return {"status": "Simulation Sent", "message": "Check Telegram for FAKE-STOCK-TEST"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)