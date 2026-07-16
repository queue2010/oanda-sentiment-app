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

# --- MONGO DATABASE CONFIGURATION (ISOLATED) ---
client = MongoClient(MONGO_URI if MONGO_URI else "mongodb://localhost:27017/")
db = client["oanda_sentiment_db"]

baseline_collection = db["session_baselines"]
daily_baseline_collection = db["daily_baselines"]
cache_collection = db["api_cache"]

# --- OANDA INSTRUMENT CONFIGURATION & MAPPING (ALL 28 PAIRS) ---
OANDA_SYMBOL_MAP = {
    "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD", "AUDUSD": "AUD_USD", "NZDUSD": "NZD_USD",
    "USDCHF": "USD_CHF", "USDCAD": "USD_CAD", "USDJPY": "USD_JPY",
    "EURGBP": "EUR_GBP", "EURAUD": "EUR_AUD", "EURNZD": "EUR_NZD", "EURCAD": "EUR_CAD", "EURCHF": "EUR_CHF", "EURJPY": "EUR_JPY",
    "GBPAUD": "GBP_AUD", "GBPNZD": "GBP_NZD", "GBPCAD": "GBP_CAD", "GBPCHF": "GBP_CHF", "GBPJPY": "GBP_JPY",
    "AUDNZD": "AUD_NZD", "AUDCAD": "AUD_CAD", "AUDCHF": "AUD_CHF", "AUDJPY": "AUD_JPY",
    "NZDCAD": "NZD_CAD", "NZDCHF": "NZD_CHF", "NZDJPY": "NZD_JPY",
    "CADCHF": "CAD_CHF", "CADJPY": "CAD_JPY",
    "CHFJPY": "CHF_JPY",
    "XAUUSD": "XAU_USD"
}

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

# --- CONCURRENT OANDA API CONNECTOR WITH VERBOSE LOGGING ---
def fetch_single_oanda_sentiment(pair_name, oanda_symbol):
    """Fetches and processes the position book data for a single instrument from Oanda"""
    try:
        url = f"{OANDA_URL}/v3/instruments/{oanda_symbol}/positionBook"
        headers = {
            "Authorization": f"Bearer {OANDA_API_KEY}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=5)
        
        # Verbose Logging: Capture exactly what Oanda is returning if it's not successful
        if response.status_code != 200:
            print(f"⚠️ [OANDA API ERROR] {pair_name} ({oanda_symbol}) failed. Status: {response.status_code}. Details: {response.text}")
            return pair_name, None
            
        data = response.json()
        pos_book = data.get("positionBook", {})
        buckets = pos_book.get("buckets", [])
        price = float(pos_book.get("price", 0))
        
        long_volume = sum(float(b.get("longCountPercent", 0)) for b in buckets)
        short_volume = sum(float(b.get("shortCountPercent", 0)) for b in buckets)
        
        return pair_name, {
            "longVolume": long_volume,
            "shortVolume": short_volume,
            "price": price,
            "avgPrice": price,
            "timestamp": pos_book.get("time")
        }
    except Exception as e:
        print(f"❌ [OANDA API EXCEPTION] {pair_name}: {str(e)}")
        return pair_name, None

def fetch_live_data_from_api():
    """Concurrently fetches the latest metrics for all 28 pairs plus Gold"""
    symbols_dict = {}
    api_server_time = None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_to_pair = {
            executor.submit(fetch_single_oanda_sentiment, pair, oanda_sym): pair 
            for pair, oanda_sym in OANDA_SYMBOL_MAP.items()
        }
        for future in concurrent.futures.as_completed(future_to_pair):
            pair = future_to_pair[future]
            try:
                pair_name, result = future.result()
                if result:
                    symbols_dict[pair_name] = result
                    if not api_server_time and result.get("timestamp"):
                        api_server_time = result.get("timestamp")
            except Exception as e:
                print(f"Thread Error resolving data for {pair}: {str(e)}")
                
    if not symbols_dict:
        return None, None
        
    return symbols_dict, api_server_time

# --- BACKGROUND AUTOMATION ENGINE ---
def run_background_state_scheduler():
    print("Background Oanda Sentiment Automation Engine: Active.")
    while True:
        try:
            ny_now = get_ny_time()
            current_date_str = ny_now.strftime("%Y-%m-%d")
            current_session_label, session_anchor_hour = get_current_session_details(ny_now)
            
            session_start_dt = ny_now.replace(hour=session_anchor_hour, minute=0, second=0, microsecond=0)
            if current_session_label == "ASIA" and ny_now.hour < 18:
                session_start_dt = session_start_dt - datetime.timedelta(days=1)
                
            minutes_elapsed_in_session = (ny_now - session_start_dt).total_seconds() / 60.0

            if current_session_label == "ASIA":
                is_active_trading_window = (minutes_elapsed_in_session >= 300.0)
            else:
                is_active_trading_window = True

            cached_data = load_db_document(cache_collection)
            live_pairs = cached_data.get("live_pairs", {})
            last_api_fetch_str = cached_data.get("last_fetch_time", "")
            last_api_timestamp = cached_data.get("last_api_timestamp", "") 
            
            force_api_refresh = False
            if not live_pairs or not last_api_fetch_str:
                force_api_refresh = True
            elif is_active_trading_window:
                last_fetch_dt = datetime.datetime.strptime(last_api_fetch_str, "%Y-%m-%d %H:%M:%S")
                if (ny_now.replace(tzinfo=None) - last_fetch_dt).total_seconds() >= 780: 
                    force_api_refresh = True

            if force_api_refresh:
                fresh_api_data, fresh_api_ts = fetch_live_data_from_api()
                if fresh_api_data:
                    live_pairs = fresh_api_data
                    last_api_timestamp = fresh_api_ts if fresh_api_ts else last_api_timestamp
                    save_db_document(cache_collection, {
                        "last_fetch_time": ny_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "last_api_timestamp": last_api_timestamp,
                        "live_pairs": live_pairs
                    })
                    
            clean_live_pairs = {str(k): v for k, v in live_pairs.items()}

            # --- DUAL-TRACKING CALIBRATION LOGIC ---
            stored_baseline = load_db_document(baseline_collection)
            
            session_did_change = (
                not stored_baseline or 
                (stored_baseline.get("active_session") != current_session_label and stored_baseline.get("pending_session") != current_session_label) or
                (stored_baseline.get("baseline_date") != current_date_str and current_session_label == "ASIA" and stored_baseline.get("pending_session") != current_session_label)
            )

            if session_did_change and clean_live_pairs:
                fresh_volumes = {}
                for name, data in clean_live_pairs.items():
                    session_start_spot = float(data.get("price") or data.get("avgPrice") or 0)
                    fresh_volumes[name] = {
                        "longVolume": float(data.get("longVolume", 0) or 0),
                        "shortVolume": float(data.get("shortVolume", 0) or 0),
                        "avgPrice": session_start_spot,
                        "session_open_price": session_start_spot
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

            elif force_api_refresh and stored_baseline.get("pending_session") and clean_live_pairs:
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

            # --- OVERALL DAILY SENTIMENT ANCHOR CALIBRATION ---
            stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
            if ny_now.hour >= 17:
                current_daily_anchor_date = ny_now.strftime("%Y-%m-%d")
            else:
                current_daily_anchor_date = (ny_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

            daily_did_change = (
                not stored_daily_baseline or
                stored_daily_baseline.get("daily_anchor_date") != current_daily_anchor_date
            )

            if daily_did_change and clean_live_pairs:
                fresh_daily_volumes = {}
                for name, data in clean_live_pairs.items():
                    fresh_daily_volumes[name] = {
                        "longVolume": float(data.get("longVolume", 0) or 0),
                        "shortVolume": float(data.get("shortVolume", 0) or 0)
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
        
        if cleaned_name == "xauusd":
            live_long = float(live.get("longVolume", 0) or 0)
            live_short = float(live.get("shortVolume", 0) or 0)
            total_live = live_long + live_short
            
            if total_live > 0:
                absolute_long_pct_sum["GOLD"] = (live_long / total_live)
                absolute_pair_counts["GOLD"] = 1
                
            base_marker = baseline_volumes.get(name, {})
            b_long = float(base_marker.get("longVolume") or live_long)
            b_short = float(base_marker.get("shortVolume") or live_short)
            
            session_long_delta["GOLD"] = (live_long - b_long)
            session_short_delta["GOLD"] = (live_short - b_short)
            
            daily_marker = daily_baseline_volumes.get(name, {})
            d_long = float(daily_marker.get("longVolume") or live_long)
            d_short = float(daily_marker.get("shortVolume") or live_short)
            
            daily_long_delta["GOLD"] = (live_long - d_long)
            daily_short_delta["GOLD"] = (live_short - d_short)
            continue
            
        if len(cleaned_name) != 6: continue
        base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()
        
        if base in majors and quote in majors:
            live_long = float(live.get("longVolume", 0) or 0)
            live_short = float(live.get("shortVolume", 0) or 0)
            total_live = live_long + live_short
            
            if total_live > 0:
                absolute_long_pct_sum[base] += (live_long / total_live)
                absolute_pair_counts[base] += 1
                absolute_long_pct_sum[quote] += (live_short / total_live)
                absolute_pair_counts[quote] += 1
            
            base_marker = baseline_volumes.get(name, {})
            b_long = float(base_marker.get("longVolume") or live_long)
            b_short = float(base_marker.get("shortVolume") or live_short)
            
            session_long_delta[base] += (live_long - b_long)
            session_short_delta[base] += (live_short - b_short)
            session_long_delta[quote] += (live_short - b_short)
            session_short_delta[quote] += (live_long - b_long)

            daily_marker = daily_baseline_volumes.get(name, {})
            d_long = float(daily_marker.get("longVolume") or live_long)
            d_short = float(daily_marker.get("shortVolume") or live_short)

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

        # Session Metrics
        net_shift = session_long_delta[cur] - session_short_delta[cur]
        formatted_score = round(net_shift / 100.0, 2)
        if formatted_score > 0: status_str = "UP"
        elif formatted_score < 0: status_str = "DOWN"
        else: status_str = "UP" if inv_long_ratio >= 0.5 else "DOWN"
        currency_scores[cur] = {"currency": display_name, "raw_score": formatted_score, "value": abs(formatted_score), "status": status_str}

        # Cumulative 24H Daily Metrics
        d_net_shift = daily_long_delta[cur] - daily_short_delta[cur]
        d_formatted_score = round(d_net_shift / 100.0, 2)
        if d_formatted_score > 0: d_status_str = "UP"
        elif d_formatted_score < 0: d_status_str = "DOWN"
        else: d_status_str = "UP" if inv_long_ratio >= 0.5 else "DOWN"
        daily_currency_scores[cur] = {"currency": display_name, "value": abs(d_formatted_score), "status": d_status_str}
    
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
    
    pending_label = stored_baseline.get("pending_session")
    buffer_status = None
    if pending_label and pending_label != active_session_label:
        buffer_status = f"Caching new session baseline for {pending_label} (Gathered blocks: {stored_baseline.get('transition_counter', 0)}/3)."

    return {
        "top_4_up": top_4_up,
        "bottom_4_down": bottom_4_down,
        "daily_top_4_up": daily_top_4_up,
        "daily_bottom_4_down": daily_bottom_4_down,
        "absolute_bias": bias_output,
        "ny_time": ny_now.strftime("%I:%M:%S %p"),
        "api_sync_time": display_sync,
        "active_session": active_session_label,
        "baseline_set_at": f"{stored_baseline.get('active_session')} Open ({stored_baseline.get('anchor_hour')}:00 NY)",
        "buffer_status": buffer_status
    }

# --- DASHBOARD HTML CONTAINER ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Oanda Sentiment Matrix Terminal</title>
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
        .panel { background-color: #111827; border: 1px solid #1f2937; border-radius: 10px; padding: 20px; box-sizing: border-box; }
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
                <h1>Oanda Sentiment Matrix Terminal</h1>
                <div style="color: #64748b; font-size: 12px; margin-top: 5px;">Active Anchor: <span style="color:#a5b4fc; font-weight:600;">{{ data.baseline_set_at }}</span></div>
            </div>
            <div class="timestamp" style="font-size: 12px; color: #64748b; text-align: right;">
                <div>Last Oanda Sync: <span style="color: #38bdf8; font-weight:600;">{{ data.api_sync_time }}</span></div>
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
            <div class="panel">
                <h2>Cumulative 24H Daily Sentiment Matrix (5:00 PM Anchor)</h2>
                <div class="velocity-row-container">
                    <div class="velocity-sub-heading">Daily Sentiment Up</div>
                    <div class="grid-row">
                        {% for item in data.daily_top_4_up %}
                        <div class="grid-box border-up">
                            <span class="currency-txt">{{ item.currency }}</span>
                            <span class="value-box up-color">{{ item.value }}</span>
                            <span class="badge badge-up">{{ item.status }}</span>
                        </div>
                        {% endfor %}
                    </div>
                    
                    <div class="velocity-sub-heading" style="margin-top: 10px;">Daily Sentiment Down</div>
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

            <div class="right-column-stack">
                <div class="panel">
                    <h2>Active Session Value Shifts (Ranked Quantities)</h2>
                    <div class="velocity-row-container">
                        <div class="velocity-sub-heading">Sentiment Up</div>
                        <div class="grid-row">
                            {% for item in data.top_4_up %}
                            <div class="grid-box border-up">
                                <span class="currency-txt">{{ item.currency }}</span>
                                <span class="value-box up-color">{{ item.value }}</span>
                                <span class="badge badge-up">{{ item.status }}</span>
                            </div>
                            {% endfor %}
                        </div>
                        
                        <div class="velocity-sub-heading" style="margin-top: 10px;">Sentiment Down</div>
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
            <strong>System Synchronization:</strong> Compares background Oanda position book retail volumes across continuous trading sessions.
        </div>
    </div>

    <script>
        setInterval(function(){ location.reload(); }, 60000); 
    </script>
</body>
</html>
"""

# --- NEW DIAGNOSTIC DEBUG ROUTE ---
@app.route('/debug')
def debug_oanda():
    """Instantly checks connection to Oanda API to output key authentication details."""
    test_pair = "EURUSD"
    oanda_symbol = OANDA_SYMBOL_MAP.get(test_pair, "EUR_USD")
    url = f"{OANDA_URL}/v3/instruments/{oanda_symbol}/positionBook"
    
    headers = {
        "Authorization": f"Bearer {OANDA_API_KEY}",
        "Content-Type": "application/json"
    }
    
    masked_key = "NOT_SET"
    if OANDA_API_KEY:
        masked_key = OANDA_API_KEY[:4] + "..." + OANDA_API_KEY[-4:] if len(OANDA_API_KEY) > 8 else "PRESENT_BUT_SHORT"
        
    debug_info = {
        "configured_oanda_url": OANDA_URL,
        "api_key_status": masked_key,
        "account_id_status": OANDA_ACCOUNT_ID if OANDA_ACCOUNT_ID else "NOT_SET",
        "testing_endpoint": url,
        "timestamp_local": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        debug_info["http_status"] = response.status_code
        try:
            debug_info["raw_response"] = response.json()
        except Exception:
            debug_info["raw_response"] = response.text
    except Exception as e:
        debug_info["connection_error"] = str(e)
        
    return jsonify(debug_info)

bg_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
bg_thread.start()

@app.route('/')
def index():
    try:
        calculated_matrix = process_sentiment_matrix()
        return render_template_string(DASHBOARD_HTML, data=calculated_matrix)
    except Exception as e:
        return jsonify({"error": True, "message": f"Processing Runtime Failure: {str(e)}"}), 500

@app.route('/api/mt4_signals')
def mt4_signals():
    try:
        matrix = process_sentiment_matrix()
        ny_now = get_ny_time()
        daily_status = "N/A"
            
        bias_list = matrix.get('absolute_bias', [])
        bias_lookup = {item['currency']: item['long_pct'] for item in bias_list}
        
        inv_on_matches = 0
        inv_off_matches = 0
        
        eur_pct = bias_lookup.get("EUR", 50.0)
        gbp_pct = bias_lookup.get("GBP", 50.0)
        aud_pct = bias_lookup.get("AUD", 50.0)
        usd_pct = bias_lookup.get("USD", 50.0)
        nzd_pct = bias_lookup.get("NZD", 50.0)
        chf_pct = bias_lookup.get("CHF", 50.0)
        cad_pct = bias_lookup.get("CAD", 50.0)
        jpy_pct = bias_lookup.get("JPY", 50.0)
        
        if eur_pct > gbp_pct: inv_on_matches += 1
        else:                  inv_off_matches += 1
            
        if aud_pct > usd_pct: inv_off_matches += 1
        else:                  inv_on_matches += 1
            
        if nzd_pct > chf_pct: inv_off_matches += 1
        else:                  inv_on_matches += 1
            
        if cad_pct > jpy_pct: inv_off_matches += 1
        else:                  inv_on_matches += 1
            
        if inv_on_matches >= 3:
            inv_status = "RISK ON"
        elif inv_off_matches >= 3:
            inv_status = "RISK OFF"
        else:
            inv_status = "NEUTRAL"
            
        return f"{daily_status},{inv_status},{ny_now.hour}"
    except Exception as e:
        print(f"MT4 Bridge Engine Processing Error: {str(e)}")
        return "ERROR,ERROR,0"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
