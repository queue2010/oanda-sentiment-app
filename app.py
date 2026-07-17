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
cache_collection = db["api_cache"]

OANDA_SYMBOL_MAP = {
    "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD", "AUDUSD": "AUD_USD", 
    "NZDUSD": "NZD_USD", "USDCHF": "USD_CHF", "USDCAD": "USD_CAD", 
    "USDJPY": "USD_JPY", "XAUUSD": "XAU_USD"
}

# --- THREADING ENGINE ---
background_engine_thread = None

@app.before_request
def ensure_background_engine_running():
    global background_engine_thread
    if background_engine_thread is None or not background_engine_thread.is_alive():
        background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
        background_engine_thread.start()

def get_ny_time():
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=-4)

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
        except Exception as e: print(e)
        time.sleep(60)

def process_sentiment_matrix():
    data = cache_collection.find_one({"_id": "state_doc"}) or {}
    live_pairs = data.get("live_pairs", {})
    
    # Aggregate by Base Currency
    currency_totals = {}
    for pair, vals in live_pairs.items():
        base = pair[:3] # e.g. "EUR", "XAU"
        if base not in currency_totals:
            currency_totals[base] = {'long': 0, 'short': 0, 'count': 0}
        currency_totals[base]['long'] += vals['long']
        currency_totals[base]['short'] += vals['short']
        currency_totals[base]['count'] += 1

    processed_data = []
    for base, totals in currency_totals.items():
        avg_long = totals['long'] / totals['count']
        avg_short = totals['short'] / totals['count']
        score = avg_long - avg_short
        
        processed_data.append({
            "currency": base,
            "score": round(score, 2),
            "long_pct": round(avg_long, 1),
            "bias": "BULLISH" if avg_long >= 50 else "BEARISH"
        })

    # Sort for Matrix
    ups = sorted([x for x in processed_data if x['score'] >= 0], key=lambda x: x['score'], reverse=True)
    downs = sorted([x for x in processed_data if x['score'] < 0], key=lambda x: abs(x['score']), reverse=True)
    
    # Sort for Bias list
    bias_sorted = sorted(processed_data, key=lambda x: x['long_pct'], reverse=True)
    
    return {
        "ups": ups, "downs": downs, "bias": bias_sorted,
        "sync_time": data.get("last_fetch_time", "N/A")
    }

# --- DASHBOARD TEMPLATE ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <style>
        body { background-color: #0b0e14; color: #a1a1aa; font-family: sans-serif; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; margin-bottom: 20px; }
        .tabs { display: flex; gap: 10px; margin-bottom: 20px; }
        .tab { padding: 10px 30px; background: #18181b; border: 1px solid #27272a; border-radius: 5px; color: #6366f1; }
        .container { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .panel { background: #111112; border: 1px solid #1f1f23; padding: 20px; border-radius: 8px; }
        .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 10px; }
        .box { background: #18181b; padding: 10px; text-align: center; border-radius: 4px; font-size: 0.9em; }
        .up { color: #10b981; } .down { color: #ef4444; }
        .bias-list { margin-top: 15px; }
        .bias-row { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
        .bar { height: 8px; border-radius: 4px; background: #27272a; flex-grow: 1; overflow: hidden; }
        .fill { height: 100%; }
        .label { width: 50px; font-weight: bold; }
        .tag { font-size: 0.7em; padding: 2px 5px; border-radius: 3px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Macro Sentiment Matrix Terminal</h1>
        <p>Last Sync: {{ data.sync_time }}</p>
    </div>
    <div class="tabs">
        <div class="tab">ASIA SESSION OPEN</div>
    </div>
    
    <div class="container">
        <!-- Left Panel -->
        <div class="panel">
            <h3>CUMULATIVE 24H DAILY SENTIMENT</h3>
            <div class="grid">
                {% for item in data.ups %}<div class="box"><div class="up">{{ item.currency }}<br>{{ item.score }}<br>UP</div></div>{% endfor %}
            </div>
            <div class="grid" style="margin-top:10px">
                {% for item in data.downs %}<div class="box"><div class="down">{{ item.currency }}<br>{{ item.score }}<br>DOWN</div></div>{% endfor %}
            </div>
        </div>

        <!-- Right Panel -->
        <div class="panel">
            <h3>ACTIVE SESSION VALUE SHIFTS</h3>
            <div class="grid">
                {% for item in data.ups %}<div class="box"><div class="up">{{ item.currency }}<br>{{ item.score }}<br>UP</div></div>{% endfor %}
            </div>
            <div class="grid" style="margin-top:10px">
                {% for item in data.downs %}<div class="box"><div class="down">{{ item.currency }}<br>{{ item.score }}<br>DOWN</div></div>{% endfor %}
            </div>
        </div>
    </div>

    <!-- Bottom Panel -->
    <div class="panel" style="margin-top: 20px;">
        <h3>ABSOLUTE RETAIL POSITIONING BIAS</h3>
        <div class="bias-list">
            {% for item in data.bias %}
            <div class="bias-row">
                <div class="label">{{ item.currency }}</div>
                <div style="width: 50px;">{{ item.long_pct }}%</div>
                <div class="bar">
                    <div class="fill" style="width: {{ item.long_pct }}%; background: {% if item.bias == 'BULLISH' %}#38bdf8{% else %}#f59e0b{% endif %};"></div>
                </div>
                <div class="tag" style="border: 1px solid {% if item.bias == 'BULLISH' %}#38bdf8{% else %}#f59e0b{% endif %}; color: {% if item.bias == 'BULLISH' %}#38bdf8{% else %}#f59e0b{% endif %};">
                    {{ item.bias }}
                </div>
            </div>
            {% endfor %}
        </div>
    </div>
    <script>setTimeout(() => location.reload(), 60000);</script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML, data=process_sentiment_matrix())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
