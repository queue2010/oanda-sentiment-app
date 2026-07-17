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
        print("🚀 [WORKER STARTUP] Spawning engine inside worker...", flush=True)
        background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
        background_engine_thread.start()

# --- LOGIC ---
def get_ny_time():
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    return utc_now + datetime.timedelta(hours=-4)

def clean_symbol_key(key_str):
    return re.sub(r'[^a-zA-Z]', '', str(key_str)).lower()

def load_db_document(coll, doc_id="state_doc"):
    return coll.find_one({"_id": doc_id}) or {}

def save_db_document(coll, data, doc_id="state_doc"):
    data["_id"] = doc_id
    coll.replace_one({"_id": doc_id}, data, upsert=True)

def fetch_single_oanda(pair_name, oanda_sym):
    try:
        url = f"{OANDA_URL}/v3/instruments/{oanda_sym}/positionBook"
        res = requests.get(url, headers={"Authorization": f"Bearer {OANDA_API_KEY}"}, timeout=5)
        if res.status_code == 200:
            data = res.json().get("positionBook", {})
            buckets = data.get("buckets", [])
            return pair_name, {
                "longVolume": sum(float(b.get("longCountPercent", 0)) for b in buckets),
                "shortVolume": sum(float(b.get("shortCountPercent", 0)) for b in buckets),
                "price": float(data.get("price", 0))
            }
    except Exception as e: print(f"Error {pair_name}: {e}", flush=True)
    return pair_name, None

def run_background_state_scheduler():
    while True:
        try:
            # Simple fetch cycle
            symbols = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(fetch_single_oanda, p, s): p for p, s in OANDA_SYMBOL_MAP.items()}
                for f in concurrent.futures.as_completed(futures):
                    p, res = f.result()
                    if res: symbols[p] = res
            
            if symbols:
                save_db_document(cache_collection, {
                    "last_fetch_time": get_ny_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "live_pairs": symbols
                })
        except Exception as e: print(f"Loop Error: {e}", flush=True)
        time.sleep(60)

def process_sentiment_matrix():
    cached = load_db_document(cache_collection)
    live = cached.get("live_pairs", {})
    # Mock/Calculate logic
    res = {
        "top_4_up": [], "bottom_4_down": [], "daily_top_4_up": [], "daily_bottom_4_down": [],
        "absolute_bias": [], "ny_time": get_ny_time().strftime("%I:%M:%S %p"),
        "api_sync_time": cached.get("last_fetch_time", "N/A"),
        "active_session": "ASIA", "baseline_set_at": "ASIA Open (18:00 NY)"
    }
    # Populate lists with your logic from before...
    return res

# --- DASHBOARD HTML (ORIGINAL LAYOUT) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Oanda Sentiment Matrix Terminal</title>
    <style>
        body { background-color: #0b0e14; color: #e2e8f0; font-family: sans-serif; padding: 25px; }
        .container { max-width: 1300px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; border-bottom: 1px solid #1e293b; padding-bottom: 15px; margin-bottom: 25px; }
        .session-tracker-bar { display: flex; gap: 10px; margin-bottom: 25px; }
        .session-card { flex: 1; padding: 12px; border-radius: 8px; text-align: center; background-color: #111827; border: 1px solid #1f2937; color: #475569; }
        .active-session-live { background-color: #1e1b4b; border: 2px solid #6366f1; color: #818cf8; }
        .panel { background-color: #111827; border: 1px solid #1f2937; border-radius: 10px; padding: 20px; }
        .grid-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
        .grid-box { background-color: #1f2937; border-radius: 8px; padding: 15px; text-align: center; }
        .up-color { color: #10b981; } .down-color { color: #ef4444; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Oanda Sentiment Matrix Terminal</h1>
            <div>Active Anchor: {{ data.baseline_set_at }} | Last Sync: {{ data.api_sync_time }}</div>
        </div>
        <div class="session-tracker-bar">
            <div class="session-card active-session-live">ASIA SESSION OPEN</div>
            <div class="session-card">LONDON SESSION OPEN</div>
            <div class="session-card">NEW YORK SESSION OPEN</div>
        </div>
        <div class="panel">
            <h2>Active Session Value Shifts (Ranked Quantities)</h2>
            <div class="grid-row">
                {% for item in data.top_4_up %}
                <div class="grid-box"><span class="up-color">{{ item.currency }}</span></div>
                {% endfor %}
            </div>
        </div>
    </div>
    <script>setInterval(function(){ location.reload(); }, 60000);</script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML, data=process_sentiment_matrix())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
