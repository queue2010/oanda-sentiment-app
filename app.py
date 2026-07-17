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

# --- ENVIRONMENT VARIABLES & SECRETS ---
MONGO_URI = os.environ.get('MONGO_URI')
OANDA_API_KEY = os.environ.get('OANDA_API_KEY')
OANDA_ACCOUNT_ID = os.environ.get('OANDA_ACCOUNT_ID')
OANDA_URL = os.environ.get('OANDA_URL', 'https://api-fxpractice.oanda.com')

# --- MONGO DATABASE CONFIGURATION ---
client = MongoClient(MONGO_URI if MONGO_URI else "mongodb://localhost:27017/")
db = client["oanda_sentiment_db"]

baseline_collection = db["session_baselines"]
daily_baseline_collection = db["daily_baselines"]
cache_collection = db["api_cache"]

# --- OANDA INSTRUMENT CONFIGURATION ---
OANDA_SYMBOL_MAP = {
    "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD", "AUDUSD": "AUD_USD", 
    "NZDUSD": "NZD_USD", "USDCHF": "USD_CHF", "USDCAD": "USD_CAD", 
    "USDJPY": "USD_JPY", "XAUUSD": "XAU_USD"
}

# --- THREAD MANAGEMENT (FIX: Worker-Safe Spawning) ---
background_engine_thread = None

@app.before_request
def ensure_background_engine_running():
    global background_engine_thread
    # Ensure thread is running inside the specific Gunicorn worker process
    if background_engine_thread is None or not background_engine_thread.is_alive():
        print("🚀 [WORKER STARTUP] Spawning background Oanda engine inside active worker...", flush=True)
        background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
        background_engine_thread.start()

# --- UTILITIES ---
def get_ny_time():
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    ny_offset = datetime.timedelta(hours=-4) 
    return utc_now + ny_offset

def load_db_document(collection, doc_id="state_doc"):
    try:
        doc = collection.find_one({"_id": doc_id})
        return doc if doc else {}
    except Exception as e:
        print(f"Database Read Error: {str(e)}", flush=True)
        return {}

def save_db_document(collection, data, doc_id="state_doc"):
    try:
        data["_id"] = doc_id
        collection.replace_one({"_id": doc_id}, data, upsert=True)
    except Exception as e:
        print(f"Database Write Error: {str(e)}", flush=True)

def get_current_session_details(ny_dt):
    hour = ny_dt.hour
    if 3 <= hour < 8: return "LONDON", 3
    elif 8 <= hour < 18: return "NEW YORK", 8
    else: return "ASIA", 18

def clean_symbol_key(key_str):
    return re.sub(r'[^a-zA-Z]', '', str(key_str)).lower()

# --- CONCURRENT OANDA API CONNECTOR ---
def fetch_single_oanda_sentiment(pair_name, oanda_symbol):
    try:
        url = f"{OANDA_URL}/v3/instruments/{oanda_symbol}/positionBook"
        headers = {"Authorization": f"Bearer {OANDA_API_KEY}", "Content-Type": "application/json"}
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code != 200:
            print(f"⚠️ [OANDA API ERROR] {pair_name} Status: {response.status_code}", flush=True)
            return pair_name, None
            
        data = response.json()
        pos_book = data.get("positionBook", {})
        buckets = pos_book.get("buckets", [])
        price = float(pos_book.get("price", 0))
        
        long_volume = sum(float(b.get("longCountPercent", 0)) for b in buckets)
        short_volume = sum(float(b.get("shortCountPercent", 0)) for b in buckets)
        
        return pair_name, {
            "longVolume": long_volume, "shortVolume": short_volume,
            "price": price, "avgPrice": price, "timestamp": pos_book.get("time")
        }
    except Exception as e:
        print(f"❌ [OANDA API EXCEPTION] {pair_name}: {str(e)}", flush=True)
        return pair_name, None

def fetch_live_data_from_api():
    symbols_dict = {}
    api_server_time = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_to_pair = {executor.submit(fetch_single_oanda_sentiment, pair, oanda_sym): pair 
                          for pair, oanda_sym in OANDA_SYMBOL_MAP.items()}
        for future in concurrent.futures.as_completed(future_to_pair):
            pair = future_to_pair[future]
            try:
                pair_name, result = future.result()
                if result:
                    symbols_dict[pair_name] = result
                    if not api_server_time and result.get("timestamp"): api_server_time = result.get("timestamp")
            except Exception as e:
                print(f"Thread Error for {pair}: {str(e)}", flush=True)
    return (symbols_dict, api_server_time) if symbols_dict else (None, None)

# --- BACKGROUND AUTOMATION ENGINE ---
def run_background_state_scheduler():
    print("--- DEBUG: Background Oanda Sentiment Engine: ACTIVE ---", flush=True)
    while True:
        try:
            print(f"--- DEBUG: Loop Iteration at {datetime.datetime.now()} ---", flush=True)
            ny_now = get_ny_time()
            current_date_str = ny_now.strftime("%Y-%m-%d")
            current_session_label, session_anchor_hour = get_current_session_details(ny_now)
            
            # Session setup
            session_start_dt = ny_now.replace(hour=session_anchor_hour, minute=0, second=0, microsecond=0)
            if current_session_label == "ASIA" and ny_now.hour < 18:
                session_start_dt = session_start_dt - datetime.timedelta(days=1)
                
            minutes_elapsed_in_session = (ny_now - session_start_dt).total_seconds() / 60.0
            is_active_trading_window = (minutes_elapsed_in_session >= 300.0) if current_session_label == "ASIA" else True

            cached_data = load_db_document(cache_collection)
            live_pairs = cached_data.get("live_pairs", {})
            last_api_fetch_str = cached_data.get("last_fetch_time", "")
            
            force_api_refresh = not live_pairs or not last_api_fetch_str
            if not force_api_refresh and is_active_trading_window:
                last_fetch_dt = datetime.datetime.strptime(last_api_fetch_str, "%Y-%m-%d %H:%M:%S")
                if (ny_now.replace(tzinfo=None) - last_fetch_dt).total_seconds() >= 780: force_api_refresh = True

            if force_api_refresh:
                fresh_api_data, fresh_api_ts = fetch_live_data_from_api()
                if fresh_api_data:
                    save_db_document(cache_collection, {
                        "last_fetch_time": ny_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "last_api_timestamp": fresh_api_ts,
                        "live_pairs": fresh_api_data
                    })
                    live_pairs = fresh_api_data
                    
            clean_live_pairs = {str(k): v for k, v in live_pairs.items()}
            stored_baseline = load_db_document(baseline_collection)
            
            # Handoff Logic
            session_did_change = (not stored_baseline or 
                (stored_baseline.get("active_session") != current_session_label and stored_baseline.get("pending_session") != current_session_label))

            if session_did_change and clean_live_pairs:
                fresh_volumes = {name: {"longVolume": float(data.get("longVolume", 0)), "shortVolume": float(data.get("shortVolume", 0)), 
                                        "avgPrice": float(data.get("price", 0))} for name, data in clean_live_pairs.items()}
                
                updated_baseline = dict(stored_baseline) if stored_baseline else {}
                updated_baseline.update({"pending_session": current_session_label, "pending_date": current_date_str, 
                                         "pending_anchor_hour": session_anchor_hour, "pending_volumes": fresh_volumes, "transition_counter": 1})
                save_db_document(baseline_collection, updated_baseline)
                print(f"Handoff Log: Staging baseline for {current_session_label}.", flush=True)

            elif force_api_refresh and stored_baseline.get("pending_session") and clean_live_pairs:
                current_count = stored_baseline.get("transition_counter", 0) + 1
                if current_count >= 3:
                    stored_baseline = {"baseline_date": stored_baseline.get("pending_date"), "active_session": stored_baseline.get("pending_session"), 
                                       "anchor_hour": stored_baseline.get("pending_anchor_hour"), "volumes": stored_baseline.get("pending_volumes")}
                    save_db_document(baseline_collection, stored_baseline)
                    print(f"Handoff Complete: Swapped to: {stored_baseline.get('active_session')}", flush=True)
                else:
                    baseline_collection.update_one({"_id": "state_doc"}, {"$set": {"transition_counter": current_count}})

        except Exception as e:
            print(f"Background Loop Error Catch: {str(e)}", flush=True)
            
        time.sleep(60)

def process_sentiment_matrix():
    ny_now = get_ny_time()
    cached_data = load_db_document(cache_collection)
    live_pairs = cached_data.get("live_pairs", {})
    sanitized_live_pairs = {str(k): v for k, v in live_pairs.items()}
    stored_baseline = load_db_document(baseline_collection)
    baseline_volumes = stored_baseline.get("volumes", {})
    active_session_label = stored_baseline.get("active_session", "INITIALIZING")
    stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
    daily_baseline_volumes = stored_daily_baseline.get("volumes", {})

    majors = ["EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"]
    tracked_assets = majors + ["GOLD"]
    
    absolute_long_pct_sum = {asset: 0.0 for asset in tracked_assets}
    absolute_pair_counts = {asset: 0 for asset in tracked_assets}
    session_long_delta, session_short_delta = {asset: 0.0 for asset in tracked_assets}, {asset: 0.0 for asset in tracked_assets}
    daily_long_delta, daily_short_delta = {asset: 0.0 for asset in tracked_assets}, {asset: 0.0 for asset in tracked_assets}
    
    for name, live in sanitized_live_pairs.items():
        cleaned_name = clean_symbol_key(name)
        if cleaned_name == "xauusd":
            live_long = float(live.get("longVolume", 0) or 0); live_short = float(live.get("shortVolume", 0) or 0)
            total_live = live_long + live_short
            if total_live > 0:
                absolute_long_pct_sum["GOLD"] = (live_long / total_live); absolute_pair_counts["GOLD"] = 1
            base_marker = baseline_volumes.get(name, {})
            session_long_delta["GOLD"] = (live_long - float(base_marker.get("longVolume") or live_long))
            session_short_delta["GOLD"] = (live_short - float(base_marker.get("shortVolume") or live_short))
            daily_marker = daily_baseline_volumes.get(name, {})
            daily_long_delta["GOLD"] = (live_long - float(daily_marker.get("longVolume") or live_long))
            daily_short_delta["GOLD"] = (live_short - float(daily_marker.get("shortVolume") or live_short))
            continue
        if len(cleaned_name) != 6: continue
        base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()
        if base in majors and quote in majors:
            live_long = float(live.get("longVolume", 0) or 0); live_short = float(live.get("shortVolume", 0) or 0)
            total_live = live_long + live_short
            if total_live > 0:
                absolute_long_pct_sum[base] += (live_long / total_live); absolute_pair_counts[base] += 1
                absolute_long_pct_sum[quote] += (live_short / total_live); absolute_pair_counts[quote] += 1
            base_marker = baseline_volumes.get(name, {})
            session_long_delta[base] += (live_long - float(base_marker.get("longVolume") or live_long))
            session_short_delta[base] += (live_short - float(base_marker.get("shortVolume") or live_short))
            session_long_delta[quote] += (live_short - float(base_marker.get("shortVolume") or live_short))
            session_short_delta[quote] += (live_long - float(base_marker.get("longVolume") or live_long))
            daily_marker = daily_baseline_volumes.get(name, {})
            daily_long_delta[base] += (live_long - float(daily_marker.get("longVolume") or live_long))
            daily_short_delta[base] += (live_short - float(daily_marker.get("shortVolume") or live_short))
            daily_long_delta[quote] += (live_short - float(daily_marker.get("shortVolume") or live_short))
            daily_short_delta[quote] += (live_long - float(daily_marker.get("longVolume") or live_long))

    currency_scores, daily_currency_scores = {}, {}
    for cur in tracked_assets:
        total_inv_count = absolute_pair_counts[cur]
        inv_long_ratio = (absolute_long_pct_sum[cur] / total_inv_count) if total_inv_count > 0 else 0.5
        display_name = "Gold" if cur == "GOLD" else cur
        net_shift = session_long_delta[cur] - session_short_delta[cur]
        formatted_score = round(net_shift / 100.0, 2)
        currency_scores[cur] = {"currency": display_name, "value": abs(formatted_score), "status": "UP" if formatted_score >= 0 else "DOWN"}
        d_net_shift = daily_long_delta[cur] - daily_short_delta[cur]
        d_formatted_score = round(d_net_shift / 100.0, 2)
        daily_currency_scores[cur] = {"currency": display_name, "value": abs(d_formatted_score), "status": "UP" if d_formatted_score >= 0 else "DOWN"}
    
    return {
        "top_4_up": [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"],
        "bottom_4_down": [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"],
        "daily_top_4_up": [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"],
        "daily_bottom_4_down": [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"],
        "absolute_bias": sorted([{"currency": ("Gold" if cur == "GOLD" else cur), "long_pct": round((absolute_long_pct_sum[cur] / absolute_pair_counts[cur]) * 100, 1) if absolute_pair_counts[cur] > 0 else 50.0, "bias_label": "BULLISH" if (absolute_long_pct_sum[cur]/absolute_pair_counts[cur] if absolute_pair_counts[cur]>0 else 0.5) >= 0.5 else "BEARISH"} for cur in tracked_assets], key=lambda x: x['long_pct'], reverse=True),
        "ny_time": ny_now.strftime("%I:%M:%S %p"),
        "api_sync_time": cached_data.get("last_fetch_time", "N/A"),
        "active_session": active_session_label,
        "baseline_set_at": f"{stored_baseline.get('active_session')} Open ({stored_baseline.get('anchor_hour')}:00 NY)" if stored_baseline else "N/A"
    }

# --- DASHBOARD HTML ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Oanda Sentiment Matrix Terminal</title>
<style>
    body { background-color: #0b0e14; color: #e2e8f0; font-family: sans-serif; padding: 25px; }
    .grid-box { background-color: #1f2937; border-radius: 8px; padding: 15px; text-align: center; }
    .up-color { color: #10b981; } .down-color { color: #ef4444; }
</style>
</head>
<body>
    <h1>Oanda Sentiment Matrix Terminal</h1>
    <p>Last Sync: {{ data.api_sync_time }} | Active: {{ data.active_session }}</p>
    <script>setInterval(function(){ location.reload(); }, 60000);</script>
</body>
</html>
"""

# --- ROUTES ---
@app.route('/debug')
def debug_oanda():
    test_pair = "EURUSD"
    oanda_symbol = OANDA_SYMBOL_MAP.get(test_pair, "EUR_USD")
    url = f"{OANDA_URL}/v3/instruments/{oanda_symbol}/positionBook"
    headers = {"Authorization": f"Bearer {OANDA_API_KEY}", "Content-Type": "application/json"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        return jsonify({"http_status": response.status_code, "raw_response": response.json() if response.status_code==200 else response.text})
    except Exception as e:
        return jsonify({"connection_error": str(e)})

@app.route('/')
def index():
    try:
        return render_template_string(DASHBOARD_HTML, data=process_sentiment_matrix())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/mt4_signals')
def mt4_signals():
    return "OK,NEUTRAL,0"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
