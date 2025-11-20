import time
import threading
import requests
import pyotp
import csv
import pytz
import sys
import os
import json
from datetime import datetime
from io import StringIO
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from nxtradstream import NxtradStream

# ================= CONFIGURATION =================
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
        return datetime.now(IST).strftime("%H:%M:%S")
    except:
        return datetime.now().strftime("%H:%M:%S")

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
            
            if tk not in static_52_highs and y_high:
                static_52_highs[tk] = y_high
            
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
                        "previous_52h": prev_high, "break_by": diff, "percent": percent
                    }
                    # Insert at the top
                    breakout_logs.insert(0, log_entry)
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

# ================= WEB UI (HTML/CSS) =================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Breakout Monitor</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background-color: #0f172a; color: #e2e8f0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .navbar { background-color: #1e293b; border-bottom: 1px solid #334155; }
        .card { background-color: #1e293b; border: 1px solid #334155; margin-bottom: 15px; transition: transform 0.2s; }
        .card:hover { transform: translateY(-5px); border-color: #22c55e; }
        .breakout-val { color: #22c55e; font-weight: bold; font-size: 1.2rem; }
        .old-high { color: #94a3b8; font-size: 0.9rem; }
        .stats-box { background-color: #334155; padding: 15px; border-radius: 10px; text-align: center; }
        .symbol-name { color: #fff; font-weight: bold; font-size: 1.3rem; }
        .time-badge { background-color: #0f172a; color: #64748b; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; }
        #status-dot { height: 10px; width: 10px; background-color: #22c55e; border-radius: 50%; display: inline-block; margin-right: 5px; }
        .btn-test { background-color: #3b82f6; color: white; border: none; }
        .btn-test:hover { background-color: #2563eb; }
    </style>
</head>
<body>

<nav class="navbar navbar-dark">
    <div class="container-fluid">
        <span class="navbar-brand mb-0 h1">
            <span id="status-dot"></span> NSE Breakout Monitor
        </span>
        <button class="btn btn-sm btn-test" onclick="runSimulation()">🛠 Simulate Breakout</button>
    </div>
</nav>

<div class="container mt-4">
    <div class="row mb-4">
        <div class="col-4">
            <div class="stats-box">
                <h3 id="monitored-count">0</h3>
                <small>Stocks Monitored</small>
            </div>
        </div>
        <div class="col-4">
            <div class="stats-box">
                <h3 id="breakout-count" style="color: #22c55e">0</h3>
                <small>Breakouts Today</small>
            </div>
        </div>
        <div class="col-4">
            <div class="stats-box">
                <h3 id="server-time">--:--</h3>
                <small>Server Time (IST)</small>
            </div>
        </div>
    </div>

    <h5 class="mb-3" style="color: #94a3b8;">Latest Alerts</h5>
    <div id="logs-container" class="row">
        <!-- Cards will be injected here by JS -->
    </div>
</div>

<script>
    async function fetchStats() {
        try {
            let res = await fetch('/api/stats');
            let data = await res.json();
            
            document.getElementById('monitored-count').innerText = data.monitored;
            document.getElementById('breakout-count').innerText = data.breakouts_today;
            document.getElementById('server-time').innerText = data.time;
            
            let container = document.getElementById('logs-container');
            container.innerHTML = '';
            
            data.logs.forEach(log => {
                let card = `
                    <div class="col-md-4 col-sm-6">
                        <div class="card p-3">
                            <div class="d-flex justify-content-between align-items-center mb-2">
                                <div class="symbol-name">${log.symbol}</div>
                                <div class="time-badge">${log.time}</div>
                            </div>
                            <div class="d-flex justify-content-between align-items-end">
                                <div>
                                    <div class="old-high">Old High: ${log.previous_52h}</div>
                                    <div class="breakout-val">₹${log.ltp}</div>
                                </div>
                                <div class="text-end">
                                    <div style="color: #22c55e">+${log.break_by}</div>
                                    <div style="color: #22c55e; font-size:0.8rem">(+${log.percent}%)</div>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
                container.innerHTML += card;
            });
        } catch (e) { console.error("Fetch error", e); }
    }

    async function runSimulation() {
        if(confirm("Send a fake breakout alert for testing?")) {
            await fetch('/simulate-breakout');
            fetchStats();
        }
    }

    setInterval(fetchStats, 3000);
    fetchStats();
</script>

</body>
</html>
"""

# ================= FASTAPI APP =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=start_background_worker, daemon=True)
    t.start()
    yield

app = FastAPI(lifespan=lifespan, title="NSE Breakout Monitor")

@app.get("/", response_class=HTMLResponse)
def home():
    return HTML_TEMPLATE

@app.get("/api/stats")
def get_stats():
    return {
        "status": "Running",
        "monitored": len(static_52_highs),
        "breakouts_today": len(breakout_logs),
        "time": get_ist_time(),
        "logs": breakout_logs
    }

@app.get("/simulate-breakout")
def simulate_logic():
    dummy_token = "999999"
    token_map[dummy_token] = "FAKE-STOCK-TEST"
    static_52_highs[dummy_token] = 100.00
    
    fake_packet = {
        'msgType': 'L1', 'token': 999999, 'ltp': 105.50, 'yHigh': 100.0
    }
    
    if dummy_token in triggered_tokens:
        triggered_tokens.remove(dummy_token)

    stream_callback(None, fake_packet)
    return {"status": "Simulation Sent"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
