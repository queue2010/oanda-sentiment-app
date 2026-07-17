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
        print("🚀 [WORKER STARTUP] Spawning background Oanda engine...", flush=True)
        background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
        background_engine_thread.start()

# --- LOGIC ---
def get_ny_time():
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=-4)

def clean_symbol(symbol):
    return re.sub(r'[^a-zA-Z]', '', symbol).upper()

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

def fetch_oanda(pair_name, oanda_sym):
    try:
        url = f"{OANDA_URL}/v3/instruments/{oanda_sym}/positionBook"
        res = requests.get(url, headers={"Authorization": f"Bearer {OANDA_API_KEY}"}, timeout=5)
        if res.status_code == 200:
            data = res.json().get("positionBook", {})
            buckets = data.get("buckets", [])
            return pair_name, {
                "long": sum(float(b.get("longCountPercent", 0)) for b in buckets),
                "short": sum(float(b.get("shortCountPercent", 0)) for b in buckets)
            }
    except: return pair_name, None
    return pair_name, None

def process_sentiment_matrix():
    data = cache_collection.find_one({"_id": "state_doc"}) or {}
    live_pairs = data.get("live_pairs", {})
    
    # Calculate simple delta
    shifts = []
    for pair, vals in live_pairs.items():
        base = pair[:3]
        score = vals['long'] - vals['short']
        shifts.append({"currency": base, "value": round(score, 2), "status": "UP" if score >= 0 else "DOWN"})
    
    # Sort
    ups = sorted([x for x in shifts if x['status'] == "UP"], key=lambda x: x['value'], reverse=True)
    downs = sorted([x for x in shifts if x['status'] == "DOWN"], key=lambda x: abs(x['value']), reverse=True)
    
    return {
        "top": ups, "bottom": downs,
        "api_sync_time": data.get("last_fetch_time", "N/A"),
        "ny_time": get_ny_time().strftime("%I:%M:%S %p")
    }

# --- HTML ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head><title>Oanda Sentiment Matrix</title>
<style>
    body { background-color: #0b0e14; color: #e2e8f0; font-family: sans-serif; padding: 25px; }
    .container { max-width: 1300px; margin: 0 auto; }
    .header { display: flex; justify-content: space-between; border-bottom: 1px solid #1e293b; padding-bottom: 15px; margin-bottom: 25px; }
    .panel { background-color: #111827; border: 1px solid #1f2937; border-radius: 10px; padding: 20px; }
    .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 15px; }
    .box { background-color: #1f2937; border-radius: 8px; padding: 15px; text-align: center; }
    .up { color: #10b981; } .down { color: #ef4444; }
</style></head>
<body>
<div class="container">
    <div class="header"><h1>Oanda Sentiment Matrix Terminal</h1><div>Last Sync: {{ data.api_sync_time }}</div></div>
    <div class="panel">
        <h2>Active Session Value Shifts</h2>
        <div class="grid">
            {% for item in data.top %}<div class="box"><span class="up">{{ item.currency }}<br>{{ item.value }}</span></div>{% endfor %}
            {% for item in data.bottom %}<div class="box"><span class="down">{{ item.currency }}<br>{{ item.value }}</span></div>{% endfor %}
        </div>
    </div>
</div>
<script>setInterval(() => location.reload(), 60000);</script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML, data=process_sentiment_matrix())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
