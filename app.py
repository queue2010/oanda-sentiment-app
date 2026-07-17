import os
import datetime
import time
import threading
import re
import requests
import concurrent.futures
from flask import Flask, jsonify, render_template_string
from pymongo import MongoClient

app = Flask(__name__)

# --- CONFIG ---
MONGO_URI = os.environ.get('MONGO_URI')
OANDA_API_KEY = os.environ.get('OANDA_API_KEY')
OANDA_URL = os.environ.get('OANDA_URL', 'https://api-fxpractice.oanda.com')

client = MongoClient(MONGO_URI if MONGO_URI else "mongodb://localhost:27017/")
db = client["oanda_sentiment_db"]
baseline_collection = db["session_baselines"]
daily_baseline_collection = db["daily_baselines"]
cache_collection = db["api_cache"]

OANDA_SYMBOL_MAP = {
    "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD", "AUDUSD": "AUD_USD", 
    "NZDUSD": "NZD_USD", "USDCHF": "USD_CHF", "USDCAD": "USD_CAD", 
    "USDJPY": "USD_JPY", "XAUUSD": "XAU_USD"
}

# --- WORKER-SAFE THREADING ---
background_engine_thread = None

@app.before_request
def ensure_background_engine_running():
    global background_engine_thread
    if background_engine_thread is None or not background_engine_thread.is_alive():
        print("🚀 [WORKER STARTUP] Spawning Oanda background engine...", flush=True)
        background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
        background_engine_thread.start()

# --- LOGIC ---
def get_ny_time():
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=-4)

def clean_symbol(symbol):
    return re.sub(r'[^a-zA-Z]', '', symbol).upper()

def fetch_oanda(pair_name, oanda_sym):
    try:
        url = f"{OANDA_URL}/v3/instruments/{oanda_sym}/positionBook"
        res = requests.get(url, headers={"Authorization": f"Bearer {OANDA_API_KEY}"}, timeout=5)
        if res.status_code == 200:
            data = res.json().get("positionBook", {})
            buckets = data.get("buckets", [])
            return pair_name, {
                "long": sum(float(b.get("longCountPercent", 0)) for b in buckets),
                "short": sum(float(b.get("shortCountPercent", 0)) for b in buckets),
                "price": float(data.get("price", 0))
            }
    except: return pair_name, None
    return pair_name, None

def run_background_state_scheduler():
    while True:
        try:
            symbols = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(fetch_oanda, p, s): p for p, s in OANDA_SYMBOL_MAP.items()}
                for f in concurrent.futures.as_completed(futures):
                    p, res = f.result()
                    if res: symbols[p] = res
            if symbols:
                cache_collection.replace_one({"_id": "state_doc"}, {
                    "_id": "state_doc",
                    "last_fetch_time": get_ny_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "live_pairs": symbols
                }, upsert=True)
        except Exception as e: print(f"Loop Error: {e}", flush=True)
        time.sleep(60)

def process_sentiment_matrix():
    data = cache_collection.find_one({"_id": "state_doc"}) or {}
    live_pairs = data.get("live_pairs", {})
    
    shifts = []
    bias_output = []
    
    # Calculate simple deltas and bias
    for pair, vals in live_pairs.items():
        base = pair[:3]
        score = vals['long'] - vals['short']
        shifts.append({"currency": base, "value": round(score, 2), "status": "UP" if score >= 0 else "DOWN"})
        
        # Total Inventory bias
        total = vals['long'] + vals['short']
        long_pct = round((vals['long'] / total) * 100, 1) if total > 0 else 50.0
        bias_output.append({"currency": base, "long_pct": long_pct, "bias_label": "BULLISH" if long_pct >= 50.0 else "BEARISH"})

    # Sorting
    ups = sorted([x for x in shifts if x['status'] == "UP"], key=lambda x: abs(x['value']), reverse=True)
    downs = sorted([x for x in shifts if x['status'] == "DOWN"], key=lambda x: abs(x['value']), reverse=True)
    
    return {
        "top_4_up": ups,
        "bottom_4_down": downs,
        "daily_top_4_up": ups, # Placeholder for daily if no separate baseline
        "daily_bottom_4_down": downs,
        "absolute_bias": bias_output,
        "api_sync_time": data.get("last_fetch_time", "N/A"),
        "ny_time": get_ny_time().strftime("%I:%M:%S %p"),
        "active_session": "ASIA", 
        "baseline_set_at": "Live Oanda Stream"
    }

# --- DASHBOARD HTML (MYFXBOOK STYLE) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Oanda Sentiment Matrix Terminal</title>
    <style>
        body { background-color: #0b0e14; color: #e2e8f0; font-family: sans-serif; padding: 25px; }
        .container { max-width: 1300px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; border-bottom: 1px solid #1e293b; padding-bottom: 15px; margin-bottom: 25px; }
        .session-tracker-bar { display: flex; gap: 10px; margin-bottom: 25px; }
        .session-card { flex: 1; padding: 12px; border-radius: 8px; text-align: center; background-color: #111827; border: 1px solid #1f2937; color: #475569; }
        .active-session-live { background-color: #1e1b4b; border: 2px solid #6366f1; color: #818cf8; }
        .section-split { display: flex; gap: 25px; }
        .panel { background-color: #111827; border: 1px solid #1f2937; border-radius: 10px; padding: 20px; flex: 1; }
        .grid-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 10px; }
        .grid-box { background-color: #1f2937; border-radius: 8px; padding: 15px; text-align: center; }
        .up-color { color: #10b981; } .down-color { color: #ef4444; }
        .bias-list { display: flex; flex-direction: column; gap: 10px; margin-top: 10px; }
        .data-row { display: flex; align-items: center; gap: 12px; border-bottom: 1px solid #1f2937; padding: 5px 0; }
        .bar-container { flex-grow: 1; background: #334155; height: 8px; border-radius: 4px; }
        .bar-fill { height: 100%; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Oanda Sentiment Matrix Terminal</h1>
            <div>Last Sync: {{ data.api_sync_time }}</div>
        </div>
        
        <div class="session-tracker-bar">
            <div class="session-card active-session-live">ASIA SESSION</div>
            <div class="session-card">LONDON SESSION</div>
            <div class="session-card">NEW YORK SESSION</div>
        </div>

        <div class="section-split">
            <div class="panel">
                <h2>Cumulative 24H Sentiment</h2>
                <div class="grid-row">
                    {% for item in data.daily_top_4_up %}<div class="grid-box"><span class="up-color">{{ item.currency }}<br>{{ item.value }}</span></div>{% endfor %}
                </div>
            </div>
            <div class="panel">
                <h2>Active Session Shifts</h2>
                <div class="grid-row">
                    {% for item in data.top_4_up %}<div class="grid-box"><span class="up-color">{{ item.currency }}<br>{{ item.value }}</span></div>{% endfor %}
                </div>
            </div>
        </div>
        
        <div class="panel" style="margin-top: 25px;">
            <h2>Absolute Retail Bias</h2>
            <div class="bias-list">
                {% for item in data.absolute_bias %}
                <div class="data-row">
                    <span style="width: 50px;">{{ item.currency }}</span>
                    <div class="bar-container"><div class="bar-fill" style="width: {{ item.long_pct }}%; background: #38bdf8;"></div></div>
                    <span>{{ item.long_pct }}%</span>
                </div>
                {% endfor %}
            </div>
        </div>
    </div>
    <script>setInterval(() => location.reload(), 60000);</script>
</body>
</html>
"""

@app.route('/')
def index():
    try:
        return render_template_string(DASHBOARD_HTML, data=process_sentiment_matrix())
    except Exception as e:
        return str(e)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
