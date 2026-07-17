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
OANDA_URL = os.environ.get('OANDA_URL', 'https://api-fxpractice.oanda.com')

# --- MONGO DATABASE CONFIGURATION ---
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

# --- THREADING ENGINE ---
background_engine_thread = None

@app.before_request
def ensure_background_engine_running():
    global background_engine_thread
    if background_engine_thread is None or not background_engine_thread.is_alive():
        background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
        background_engine_thread.start()

def get_ny_time():
    """Calculates current New York Time (EST/EDT) from UTC"""
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    ny_offset = datetime.timedelta(hours=-4) 
    return utc_now + ny_offset

def load_db_document(collection, doc_id="state_doc"):
    try:
        doc = collection.find_one({"_id": doc_id})
        return doc if doc else {}
    except Exception as e:
        print(f"Database Read Error: {str(e)}")
        return {}

def save_db_document(collection, data, doc_id="state_doc"):
    try:
        data["_id"] = doc_id
        collection.replace_one({"_id": doc_id}, data, upsert=True)
    except Exception as e:
        print(f"Database Write Error: {str(e)}")

def get_current_session_details(ny_dt):
    hour = ny_dt.hour
    if 3 <= hour < 8:
        return "LONDON", 3
    elif 8 <= hour < 18:
        return "NEW YORK", 8
    else:
        return "ASIA", 18

def clean_symbol_key(key_str):
    return re.sub(r'[^a-zA-Z]', '', str(key_str)).lower()

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
    except Exception as e:
        print(f"Oanda Fetch Error for {pair_name}: {str(e)}")
        return pair_name, None
    return pair_name, None

# --- BACKGROUND AUTOMATION ENGINE ---
def run_background_state_scheduler():
    print("Background Sentiment Automation Engine: Oanda Real-time calibration active.")
    while True:
        try:
            ny_now = get_ny_time()
            current_date_str = ny_now.strftime("%Y-%m-%d")
            current_session_label, session_anchor_hour = get_current_session_details(ny_now)
            
            # Fetching Oanda Live Data
            symbols = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(fetch_oanda, p, s): p for p, s in OANDA_SYMBOL_MAP.items()}
                for f in concurrent.futures.as_completed(futures):
                    p, res = f.result()
                    if res: 
                        symbols[p] = res
            
            if symbols:
                # Update Cache Document
                cache_collection.replace_one({"_id": "state_doc"}, {
                    "_id": "state_doc",
                    "last_fetch_time": ny_now.strftime("%Y-%m-%d %H:%M:%S"),
                    "live_pairs": symbols
                }, upsert=True)
            
            # Retrieve stabilized cache for baseline calibration
            cached_data = load_db_document(cache_collection)
            live_pairs = cached_data.get("live_pairs", {})
            clean_live_pairs = {str(k): v for k, v in live_pairs.items()}

            if clean_live_pairs:
                # --- DUAL-TRACKING CALIBRATION LOGIC ---
                stored_baseline = load_db_document(baseline_collection)
                
                session_did_change = (
                    not stored_baseline or 
                    (stored_baseline.get("active_session") != current_session_label and stored_baseline.get("pending_session") != current_session_label) or
                    (stored_baseline.get("baseline_date") != current_date_str and current_session_label == "ASIA" and stored_baseline.get("pending_session") != current_session_label)
                )

                if session_did_change:
                    fresh_volumes = {}
                    for name, data in clean_live_pairs.items():
                        fresh_volumes[name] = {
                            "long": float(data.get("long", 0)),
                            "short": float(data.get("short", 0))
                        }
                    
                    updated_baseline = dict(stored_baseline) if stored_baseline else {}
                    updated_baseline.update({
                        "pending_session": current_session_label,
                        "pending_date": current_date_str,
                        "pending_anchor_hour": session_anchor_hour,
                        "pending_volumes": fresh_volumes,
                        "transition_counter": 1
                    })
                    save_db_document(baseline_collection, updated_baseline)
                    stored_baseline = updated_baseline
                    print(f"Handoff Log: Staging baseline sequence initiated for {current_session_label}. Reading 1/3 secured.")

                elif stored_baseline.get("pending_session"):
                    current_count = stored_baseline.get("transition_counter", 0) + 1
                    print(f"Handoff Log: Incremented update block {current_count}/3 for {stored_baseline.get('pending_session')}")
                    
                    if current_count >= 3:
                        stored_baseline = {
                            "baseline_date": stored_baseline.get("pending_date"),
                            "active_session": stored_baseline.get("pending_session"),
                            "anchor_hour": stored_baseline.get("pending_anchor_hour"),
                            "volumes": stored_baseline.get("pending_volumes")
                        }
                        save_db_document(baseline_collection, stored_baseline)
                        print(f"Handoff Complete: Safely hot-swapped session view over to: {stored_baseline.get('active_session')}")
                    else:
                        baseline_collection.update_one(
                            {"_id": "state_doc"}, 
                            {"$set": {"transition_counter": current_count}}
                        )

                # --- OVERALL DAILY SENTIMENT ANCHOR CALIBRATION (5:00 PM NY RESET) ---
                stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
                if ny_now.hour >= 17:
                    current_daily_anchor_date = ny_now.strftime("%Y-%m-%d")
                else:
                    current_daily_anchor_date = (ny_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

                daily_did_change = (
                    not stored_daily_baseline or
                    stored_daily_baseline.get("daily_anchor_date") != current_daily_anchor_date
                )

                if daily_did_change:
                    fresh_daily_volumes = {}
                    for name, data in clean_live_pairs.items():
                        fresh_daily_volumes[name] = {
                            "long": float(data.get("long", 0)),
                            "short": float(data.get("short", 0))
                        }
                    new_daily_baseline = {
                        "daily_anchor_date": current_daily_anchor_date,
                        "volumes": fresh_daily_volumes,
                        "captured_at": ny_now.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    save_db_document(daily_baseline_collection, new_daily_baseline, "daily_state_doc")
                    print(f"Daily Baseline Log: Successfully locked 24H reference anchor at NY Close for {current_daily_anchor_date}")

        except Exception as e:
            print(f"Background Loop Error Catch: {str(e)}")
            
        time.sleep(60)

def process_sentiment_matrix():
    ny_now = get_ny_time()
    
    cached_data = load_db_document(cache_collection)
    live_pairs = cached_data.get("live_pairs", {})
    last_api_fetch_str = cached_data.get("last_fetch_time", "")
    sanitized_live_pairs = {str(k): v for k, v in live_pairs.items()}

    stored_baseline = load_db_document(baseline_collection)
    baseline_volumes = stored_baseline.get("volumes", {})
    active_session_label = stored_baseline.get("active_session", "INITIALIZING")

    stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
    daily_baseline_volumes = stored_daily_baseline.get("volumes", {})

    # --- LIVE CONTINUOUS SENTIMENT CALCULATION ---
    majors = ["EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"]
    tracked_assets = majors + ["GOLD"]
    
    absolute_long_pct_sum = {asset: 0.0 for asset in tracked_assets}
    absolute_pair_counts = {asset: 0 for asset in tracked_assets}
    
    session_long_delta = {asset: 0.0 for asset in tracked_assets}
    session_short_delta = {asset: 0.0 for asset in tracked_assets}

    daily_long_delta = {asset: 0.0 for asset in tracked_assets}
    daily_short_delta = {asset: 0.0 for asset in tracked_assets}
    
    for name, live in sanitized_live_pairs.items():
        cleaned_name = clean_symbol_key(name)
        
        live_long = float(live.get("long", 0) or 0)
        live_short = float(live.get("short", 0) or 0)
        total_live = live_long + live_short
        
        if cleaned_name == "xauusd":
            if total_live > 0:
                absolute_long_pct_sum["GOLD"] = (live_long / total_live)
                absolute_pair_counts["GOLD"] = 1
                
            base_marker = baseline_volumes.get(name, {})
            b_long = float(base_marker.get("long") or live_long)
            b_short = float(base_marker.get("short") or live_short)
            
            session_long_delta["GOLD"] = (live_long - b_long)
            session_short_delta["GOLD"] = (live_short - b_short)
            
            daily_marker = daily_baseline_volumes.get(name, {})
            d_long = float(daily_marker.get("long") or live_long)
            d_short = float(daily_marker.get("short") or live_short)
            
            daily_long_delta["GOLD"] = (live_long - d_long)
            daily_short_delta["GOLD"] = (live_short - d_short)
            continue
            
        if len(cleaned_name) != 6: 
            continue
        base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()
        
        if base in majors and quote in majors:
            if total_live > 0:
                absolute_long_pct_sum[base] += (live_long / total_live)
                absolute_pair_counts[base] += 1
                absolute_long_pct_sum[quote] += (live_short / total_live)
                absolute_pair_counts[quote] += 1
            
            # 1. Session Relative Deltas
            base_marker = baseline_volumes.get(name, {})
            b_long = float(base_marker.get("long") or live_long)
            b_short = float(base_marker.get("short") or live_short)
            
            session_long_delta[base] += (live_long - b_long)
            session_short_delta[base] += (live_short - b_short)
            session_long_delta[quote] += (live_short - b_short)
            session_short_delta[quote] += (live_long - b_long)

            # 2. Cumulative 24H Daily Deltas (NY Close reference)
            daily_marker = daily_baseline_volumes.get(name, {})
            d_long = float(daily_marker.get("long") or live_long)
            d_short = float(daily_marker.get("short") or live_short)

            daily_long_delta[base] += (live_long - d_long)
            daily_short_delta[base] += (live_short - d_short)
            daily_long_delta[quote] += (live_short - d_short)
            daily_short_delta[quote] += (live_long - d_long)

    currency_scores = {}
    daily_currency_scores = {}
    
    for cur in tracked_assets:
        total_inv_count = absolute_pair_counts[cur]
        inv_long_ratio = (absolute_long_pct_sum[cur] / total_inv_count) if total_inv_count > 0 else 0.5
        display_name = "Gold" if cur == "GOLD" else cur

        # Process Session Metrics (formatted score directly maps to shift percentages)
        net_shift = session_long_delta[cur] - session_short_delta[cur]
        formatted_score = round(net_shift, 2)
        if formatted_score > 0: 
            status_str = "UP"
        elif formatted_score < 0: 
            status_str = "DOWN"
        else: 
            status_str = "UP" if inv_long_ratio >= 0.5 else "DOWN"
        currency_scores[cur] = {"currency": display_name, "value": abs(formatted_score), "status": status_str}

        # Process Cumulative 24H Daily Metrics
        d_net_shift = daily_long_delta[cur] - daily_short_delta[cur]
        d_formatted_score = round(d_net_shift, 2)
        if d_formatted_score > 0: 
            d_status_str = "UP"
        elif d_formatted_score < 0: 
            d_status_str = "DOWN"
        else: 
            d_status_str = "UP" if inv_long_ratio >= 0.5 else "DOWN"
        daily_currency_scores[cur] = {"currency": display_name, "value": abs(d_formatted_score), "status": d_status_str}
    
    # --- DIRECTIONAL SORTING REDIRECT ENGINE ---
    top_4_up = [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"]
    bottom_4_down = [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"]

    daily_top_4_up = [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"]
    daily_bottom_4_down = [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"]

    bias_output = []
    for cur in tracked_assets:
        count = absolute_pair_counts[cur]
        display_name = "Gold" if cur == "GOLD" else cur
        long_pct = round((absolute_long_pct_sum[cur] / count) * 100, 1) if count > 0 else 50.0
        bias_output.append({"currency": display_name, "long_pct": long_pct, "bias_label": "BULLISH" if long_pct >= 50.0 else "BEARISH"})
    bias_output = sorted(bias_output, key=lambda x: x['long_pct'], reverse=True)

    display_sync = last_api_fetch_str if last_api_fetch_str else ny_now.strftime("%Y-%m-%d %H:%M:%S")
    
    pending_label = stored_baseline.get("pending_session") if stored_baseline else None
    buffer_status = None
    if pending_label and pending_label != active_session_label:
        buffer_status = f"Caching new session baseline for {pending_label} (Gathered blocks: {stored_baseline.get('transition_counter', 0)}/3). Current matrix below remains fully live!"

    baseline_set_at = "INITIALIZING"
    if stored_baseline and stored_baseline.get('active_session'):
        baseline_set_at = f"{stored_baseline.get('active_session')} Open ({stored_baseline.get('anchor_hour')}:00 NY)"

    return {
        "top_4_up": top_4_up,
        "bottom_4_down": bottom_4_down,
        "daily_top_4_up": daily_top_4_up,
        "daily_bottom_4_down": daily_bottom_4_down,
        "absolute_bias": bias_output,
        "ny_time": ny_now.strftime("%I:%M:%S %p"),
        "api_sync_time": display_sync,
        "active_session": active_session_label,
        "baseline_set_at": baseline_set_at,
        "buffer_status": buffer_status
    }

# --- DASHBOARD HTML CONTAINER ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Macro Sentiment Matrix Terminal</title>
    <style>
        body { background-color: #0b0e14; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: 25px; }
        .container { max-width: 1300px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; padding-bottom: 15px; margin-bottom: 25px; }
        h1 { margin: 0; font-size: 22px; color: #38bdf8; font-weight: 700; }
        h2 { font-size: 13px; color: #94a3b8; margin-top: 0; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 0.5px;}
        .session-tracker-bar { display: flex; gap: 10px; margin-bottom: 25px; }
        .session-card { flex: 1; padding: 12px; border-radius: 8px; text-align: center; font-size: 12px; font-weight: 700; background-color: #111827; border: 1px solid #1f2937; color: #475569; text-transform: uppercase; letter-spacing: 1px; }
        .session-card.active-session-live { background-color: #1e1b4b; border: 2px solid #6366f1; color: #818cf8; box-shadow: 0 0 12px rgba(99, 102, 241, 0.3); }
        
        .section-split { display: flex; flex-direction: row; gap: 25px; margin-bottom: 25px; width: 100%; align-items: flex-start; }
        .panel { background-color: #111827; border: 1px solid #1f1f23; border-radius: 10px; padding: 20px; box-sizing: border-box; }
        .section-split > .panel { flex: 1; }
        .right-column-stack { flex: 1; display: flex; flex-direction: column; gap: 25px; }
        
        .velocity-row-container { display: flex; flex-direction: column; gap: 15px; margin-top: 10px; }
        .velocity-sub-heading { font-size: 11px; color: #64748b; text-transform: uppercase; font-weight: 700; margin-bottom: -5px; letter-spacing: 0.5px; }
        .grid-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
        .grid-box { background-color: #1f2937; border: 1px solid #374151; border-radius: 8px; padding: 15px; text-align: center; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; }
        .border-up { border-top: 3px solid #10b981; }
        .border-down { border-top: 3px solid #ef4444; }
        .currency-txt { font-size: 16px; font-weight: 700; color: #f1f5f9; }
        .value-box { font-size: 18px; font-weight: 800; }
        .up-color { color: #10b981; }
        .down-color { color: #ef4444; }
        
        .bias-list { display: flex; flex-direction: column; }
        .data-row { display: flex; align-items: center; padding: 11px 10px; border-bottom: 1px solid #1f2937; gap: 12px; }
        .bar-container { width: 110px; background-color: #334155; height: 8px; border-radius: 4px; overflow: hidden; }
        .bar-fill { height: 100%; }
        .badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; width: 65px; text-align: center; }
        
        .badge-up { background-color: rgba(16, 185, 129, 0.15); color: #10b981; }
        .badge-down { background-color: rgba(239, 68, 68, 0.15); color: #ef4444; }
        .badge-bull { background-color: rgba(56, 189, 248, 0.15); color: #38bdf8; }
        .badge-bear { background-color: rgba(245, 158, 11, 0.15); color: #f59e0b; }
        .footer-note { background-color: #1e293b; padding: 12px 20px; border-radius: 8px; font-size: 12px; color: #94a3b8; border-left: 4px solid #38bdf8; margin-top: 25px; }
        .staging-alert { background-color: #0f172a; border: 1px dashed #38bdf8; border-radius: 6px; padding: 10px; margin-bottom: 20px; text-align: center; font-size: 12px; font-weight: 600; color: #38bdf8; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <div style="display: flex; align-items: center;">
                    <h1>Macro Sentiment Matrix Terminal</h1>
                </div>
                <div style="color: #64748b; font-size: 12px; margin-top: 5px;">Active Anchor: <span style="color:#a5b4fc; font-weight:600;">{{ data.baseline_set_at }}</span></div>
            </div>
            <div class="timestamp" style="font-size: 12px; color: #64748b; text-align: right;">
                <div>Last API Fetch Sync: <span style="color: #38bdf8; font-weight:600;">{{ data.api_sync_time }}</span></div>
                <div style="color: #64748b; margin-top: 3px;">Local UI Heartbeat: {{ data.ny_time }} NY</div>
            </div>
        </div>

        {% if data.buffer_status %}
        <div class="staging-alert">
             ⚡ <strong>Continuous Data Stream Active:</strong> {{ data.buffer_status }}
        </div>
        {% endif %}

        <div class="session-tracker-bar">
            <div class="session-card {% if data.active_session == 'ASIA' %}active-session-live{% endif %}">Asia Session Open</div>
            <div class="session-card {% if data.active_session == 'LONDON' %}active-session-live{% endif %}">London Session Open</div>
            <div class="session-card {% if data.active_session == 'NEW YORK' %}active-session-live{% endif %}">New York Session Open</div>
        </div>

        <div class="section-split">
            <!-- Left Column: Cumulative 24H -->
            <div class="panel">
                <h2>Cumulative 24H Daily Sentiment Matrix (5:00 PM Anchor)</h2>
                <div class="velocity-row-container">
                    <div class="velocity-sub-heading">Daily Sentiment Up (Highest Value First)</div>
                    <div class="grid-row">
                        {% for item in data.daily_top_4_up %}
                        <div class="grid-box border-up">
                            <span class="currency-txt">{{ item.currency }}</span>
                            <span class="value-box up-color">{{ item.value }}</span>
                            <span class="badge badge-up">{{ item.status }}</span>
                        </div>
                        {% endfor %}
                    </div>
                    
                    <div class="velocity-sub-heading" style="margin-top: 10px;">Daily Sentiment Down (Lowest Value First)</div>
                    <div class="grid-row">
                        {% for item in data.daily_bottom_4_down %}
                        <div class="grid-box border-down">
                            <span class="currency-txt">{{ item.currency }}</span>
                            <span class="value-box down-color">{{ item.value }}</span>
                            <span class="badge badge-down">{{ item.status }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
            </div>

            <!-- Right Column Stack: Active Shifts + Total Inventory -->
            <div class="right-column-stack">
                <div class="panel">
                    <h2>Active Session Value Shifts (Ranked Quantities)</h2>
                    <div class="velocity-row-container">
                        <div class="velocity-sub-heading">Sentiment Up (Highest Value First)</div>
                        <div class="grid-row">
                            {% for item in data.top_4_up %}
                            <div class="grid-box border-up">
                                <span class="currency-txt">{{ item.currency }}</span>
                                <span class="value-box up-color">{{ item.value }}</span>
                                <span class="badge badge-up">{{ item.status }}</span>
                            </div>
                            {% endfor %}
                        </div>
                        
                        <div class="velocity-sub-heading" style="margin-top: 10px;">Sentiment Down (Lowest Value First)</div>
                        <div class="grid-row">
                            {% for item in data.bottom_4_down %}
                            <div class="grid-box border-down">
                                <span class="currency-txt">{{ item.currency }}</span>
                                <span class="value-box down-color">{{ item.value }}</span>
                                <span class="badge badge-down">{{ item.status }}</span>
                            </div>
                            {% endfor %}
                        </div>
                    </div>
                </div>

                <div class="panel">
                    <h2>Absolute Retail Positioning Bias (Total Inventory)</h2>
                    <div class="bias-list">
                        {% for item in data.absolute_bias %}
                        <div class="data-row">
                            <span class="currency-txt" style="min-width: 50px;">{{ item.currency }}</span>
                            <span class="value-box" style="min-width: 50px; font-size: 14px; text-align: right; color: #f1f5f9; margin-right: 5px;">{{ item.long_pct }}%</span>
                            <div class="bar-container">
                                <div class="bar-fill" style="width: {{ item.long_pct }}%; background-color: {% if item.long_pct >= 50.0 %}#38bdf8{% else %}#f59e0b{% endif %};"></div>
                            </div>
                            <span class="badge {% if item.long_pct >= 50.0 %}badge-bull{% else %}badge-bear{% endif %}">{{ item.bias_label }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
            </div>
        </div>

        <div class="footer-note">
            <strong>System Synchronization:</strong> Compares retail baseline positions tracked inside the background database to current raw books. Matrix offsets are normalized at each active session change and a hard reference reset executes daily at 5:00 PM NY Close.
        </div>
    </div>

    <script>
        setInterval(function(){ location.reload(); }, 60000); 
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    try:
        calculated_matrix = process_sentiment_matrix()
        return render_template_string(DASHBOARD_HTML, data=calculated_matrix)
    except Exception as e:
        return jsonify({"error": True, "message": f"Processing Runtime Failure: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
