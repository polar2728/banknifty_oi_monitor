import os
import json
import time
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
WATCH_OI_PCT = 70 # default early-month
EXEC_OI_PCT = 200 # default early-month
SPOT_MOVE_PCT = 0.3
VOL_MULTIPLIER = 1.5
MIN_BASE_OI = 2000
STRIKE_RANGE = 200
CHECK_MARKET_HOURS = False # set to True in production
BASELINE_FILE = "bn_baseline_oi.json"

# ─── New thresholds for video-aligned quality filters ───
OI_BOTH_SIDES_AVOID = 180 # % if both CE & PE >= this → skip conflicted/range-bound
PREMIUM_MAX_RISE = 2 # max allowed premium % rise during buildup (confirms short)
MIN_DECLINE_PCT = -1.5 # minimum decline % for opposite side (noise filter)

DEBUG_MODE = str(os.environ.get("DEBUG_MODE", "false")).lower() == "true"

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))

# ================= SECRETS =================
CLIENT_ID = os.environ.get("CLIENT_ID")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise RuntimeError("❌ Missing FYERS credentials")

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    log_path=""
)

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def is_market_open():
    t = now_ist().time()
    return datetime.strptime("09:15", "%H:%M").time() <= t <= datetime.strptime("15:30", "%H:%M").time()

def after_1015():
    return now_ist().time() >= datetime.strptime("10:15", "%H:%M").time()

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        print("Telegram error:", e)

def safe_api_call(fn, payload, retries=3, delay=1):
    for _ in range(retries):
        try:
            resp = fn(payload)
            if resp and ("d" in resp or "data" in resp):
                return resp
        except Exception:
            time.sleep(delay)
    return None

# ================= SAFE SPOT FETCH =================
def get_banknifty_spot():
    resp = safe_api_call(fyers.quotes, {"symbols": "NSE:NIFTYBANK-INDEX"})
    if not resp or "d" not in resp or not resp["d"]:
        return None

    try:
        v = resp["d"][0].get("v", {})
        lp = v.get("lp") or v.get("ltp") or v.get("prev_close_price")
        if lp is None or lp == 0:
            return None
        return float(lp)
    except Exception:
        if DEBUG_MODE:
            print("❌ Spot parse error:", resp)
        return None

# ================= BASELINE =================
def load_baseline():
    if os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE, "r") as f:
            return json.load(f)
    return {
        "date": None,
        "started": False,
        "day_open": None,
        "data": {}
    }

def save_baseline(b):
    with open(BASELINE_FILE, "w") as f:
        json.dump(b, f, indent=2)

def reset_day(b):
    today = now_ist().date().isoformat()
    if b.get("date") != today:
        b["date"] = today
        b["started"] = False
        b["day_open"] = None
        b["data"] = {}
        save_baseline(b)
    return b

# ================= EXPIRY =================
def expiry_to_symbol_format(date_str):
    d = datetime.strptime(date_str, "%d-%m-%Y")
    year_short = d.strftime("%y")
    month_short = d.strftime("%b").upper()
    return year_short + month_short

def get_monthly_expiry(expiry_info):
    today = now_ist().date()
    valid_expiries = []

    print("DEBUG - All expiry_info from API:", expiry_info)

    for e in expiry_info:
        try:
            exp = datetime.fromtimestamp(int(e["expiry"]), tz=IST).date()
            days = (exp - today).days
            print(f"Expiry {e['date']}: {exp} → {days} days left")
            if days >= 0:
                valid_expiries.append((days, e["date"]))
        except Exception as ex:
            print("Expiry parse error:", ex)
            continue

    if not valid_expiries:
        print("No valid expiry found")
        return None

    nearest_days, nearest_date = min(valid_expiries, key=lambda x: x[0])
    print(f"SELECTED EXPIRY: {nearest_date} ({nearest_days} days left)")
    return nearest_date

# ================= STRIKE SELECTION =================
def select_trade_strike(atm, buildup_type):
    if buildup_type == "CE":
        return atm - 100, "PE"
    else:
        return atm + 100, "CE"

# ================= SCAN =================
def scan():
    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        return

    baseline = reset_day(load_baseline())

    spot = get_banknifty_spot()
    if spot is None:
        print("⚠ BANKNIFTY spot unavailable — skipping scan")
        return

    if baseline["day_open"] is None:
        baseline["day_open"] = spot

    atm = int(round(spot / 100) * 100)

    chain_resp = safe_api_call(fyers.optionchain, {
        "symbol": "NSE:NIFTYBANK-INDEX",
        "strikecount": 40,
        "timestamp": ""
    })
    if not chain_resp:
        print("Option chain API call returned None")
        return

    print("Optionchain response status:", chain_resp.get("s"))
    print("Full chain_resp keys:", list(chain_resp.keys()))

    if chain_resp.get("s") != "ok":
        print(f"Optionchain failed: {chain_resp.get('message', 'Unknown')}")
        return

    raw = chain_resp["data"]["optionsChain"]
    expiry_info = chain_resp["data"]["expiryData"]

    if not raw:
        print("No optionsChain data returned")
        return

    expiry_date = get_monthly_expiry(expiry_info)
    if not expiry_date:
        print("No suitable monthly expiry found")
        return

    expiry = expiry_to_symbol_format(expiry_date)

    df = pd.DataFrame(raw)
    
    # Apply expiry filter FIRST
    df = df[df["symbol"].str.contains(expiry, regex=False)]
    
    # Then strike range
    df = df[
        (df["strike_price"].between(atm - STRIKE_RANGE, atm + STRIKE_RANGE)) &
        (df["strike_price"] % 100 == 0)
    ]

    print(f"Selected monthly expiry date: {expiry_date}")
    print(f"Expiry filter string: {expiry}")
    print(f"Total raw options: {len(raw)}")
    print(f"After expiry filter: {len(df[df['symbol'].str.contains(expiry)])}")
    print(f"After strike range filter: {len(df)}")
    print(f"Number of valid CE/PE rows: {len(df[df['option_type'].isin(['CE', 'PE'])])}")

    # Days to expiry for dynamic thresholds
    expiry_dt = datetime.strptime(expiry_date, "%d-%m-%Y").date()
    today_dt = now_ist().date()
    days_to_expiry = (expiry_dt - today_dt).days

    if days_to_expiry > 14:
        WATCH_OI_PCT = 70
        EXEC_OI_PCT  = 100
        MIN_DECLINE_PCT = -1.5
        print(f"Days to expiry: {days_to_expiry} → low thresholds: {WATCH_OI_PCT}% / {EXEC_OI_PCT}%")
    elif 8 <= days_to_expiry <= 14:
        WATCH_OI_PCT = 120
        EXEC_OI_PCT  = 200
        MIN_DECLINE_PCT = -1.2
        print(f"Days to expiry: {days_to_expiry} → medium thresholds: {WATCH_OI_PCT}% / {EXEC_OI_PCT}%")
    else:
        WATCH_OI_PCT = 250
        EXEC_OI_PCT  = 400
        MIN_DECLINE_PCT = -0.8  # less strict near expiry
        print(f"Days to expiry: {days_to_expiry} → high thresholds: {WATCH_OI_PCT}% / {EXEC_OI_PCT}%")

    OI_BOTH_SIDES_AVOID = max(120, EXEC_OI_PCT * 0.6)

    # === Pre-compute OI % for opposite-side & conflict checks ===
    strike_oi_changes = {}   # strike -> {"CE": pct, "PE": pct}
    current_oi_map = {}      # (strike, opt) -> current_oi

    for _, r in df.iterrows():
        strike = int(r.get("strike_price", 0))
        opt    = r.get("option_type", "")
        oi     = int(r.get("oi", 0))

        if strike == 0 or opt not in ("CE", "PE"):
            continue

        # Store current OI for later lookup
        current_oi_map[(strike, opt)] = oi

        key = f"{opt}_{strike}"
        entry = baseline["data"].get(key)
        if entry is None or entry.get("base_oi", 0) < MIN_BASE_OI:
            continue

        base_oi = entry["base_oi"]
        oi_pct  = ((oi - base_oi) / base_oi) * 100 if base_oi > 0 else 0

        if strike not in strike_oi_changes:
            strike_oi_changes[strike] = {}
        strike_oi_changes[strike][opt] = oi_pct

    updated = False

    for _, r in df.iterrows():
        strike = int(r.get("strike_price", 0))
        opt    = r.get("option_type", "")
        oi     = int(r.get("oi", 0))
        ltp    = float(r.get("ltp", 0))  # NEW: Capture LTP for premium check
        vol    = int(r.get("volume", 0))

        if strike == 0 or opt not in ("CE", "PE"):
            continue

        key = f"{opt}_{strike}"
        entry = baseline["data"].setdefault(key, {
            "base_oi": oi,
            "base_ltp": ltp,  # NEW: Add base_ltp to baseline
            "base_vol": vol,
            "prev_oi": oi,    # Initialize prev_oi
            "state": "NONE"
        })

        if entry["base_oi"] < MIN_BASE_OI:
            continue

        oi_pct = ((oi - entry["base_oi"]) / entry["base_oi"]) * 100
        ltp_change_pct = ((ltp - entry["base_ltp"]) / entry["base_ltp"] * 100) if entry["base_ltp"] > 0 else 0  # NEW: Compute premium %
        vol_ok = vol > entry["base_vol"] * VOL_MULTIPLIER

        # ================= WATCH (original) =================
        if oi_pct >= WATCH_OI_PCT and entry["state"] == "NONE":
            send_telegram(
                f"👀 *BN OI WATCH*\n"
                f"{strike} {opt}\n"
                f"OI +{oi_pct:.0f}%\n"
                f"Spot: {spot:.0f}  ATM: {atm}"
            )
            entry["state"] = "WATCH"
            updated = True

        # ================= EXECUTION (enhanced with video filters) =================
        is_short_buildup = (oi_pct >= EXEC_OI_PCT) and (ltp_change_pct <= PREMIUM_MAX_RISE)

        if is_short_buildup and entry["state"] == "WATCH":
            if not after_1015():
                continue

            spot_move = abs(spot - baseline["day_open"]) / baseline["day_open"] * 100
            if spot_move < SPOT_MOVE_PCT or not vol_ok:
                continue

            # ─── NEW: Conflict check ───
            ce_pct_here = strike_oi_changes.get(strike, {}).get("CE", 0)
            pe_pct_here = strike_oi_changes.get(strike, {}).get("PE", 0)
            conflicted = (ce_pct_here >= OI_BOTH_SIDES_AVOID and pe_pct_here >= OI_BOTH_SIDES_AVOID)

            if conflicted:
                print(f"⛔ Skipping conflicted BN buildup at {strike}: both sides +{ce_pct_here:.0f}% / +{pe_pct_here:.0f}%")
                continue

            # ─── NEW: Opposite side covering ───
            opp_opt = "PE" if opt == "CE" else "CE"
            opp_key = f"{opp_opt}_{strike}"
            opp_entry = baseline["data"].get(opp_key)

            if opp_entry:
                opp_current_oi = current_oi_map.get((strike, opp_opt), 0)
                
                if opp_current_oi == 0:
                    print(f"⚠️ No current data for opposite {opp_opt} at {strike}")
                    continue
                
                opp_prev_oi = opp_entry.get("prev_oi", opp_entry.get("base_oi", 0))
                
                opp_decline_pct = ((opp_current_oi - opp_prev_oi) / opp_prev_oi * 100) if opp_prev_oi > 0 else 0
                
                is_covering = (opp_current_oi < opp_prev_oi) and (opp_decline_pct <= MIN_DECLINE_PCT)
                
                if not is_covering:
                    opp_pct = strike_oi_changes.get(strike, {}).get(opp_opt, 0)
                    print(f"⚠️ BN Near miss at {strike} {opt}: OI +{oi_pct:.0f}%, opposite {opp_pct:+.1f}% (decline {opp_decline_pct:+.1f}%, needs <= {MIN_DECLINE_PCT}%)")
                    continue
                
                # NEW: Debug print when covering detected
                print(f"✓ BN Covering detected at {strike} {opt}: {opp_opt} {opp_decline_pct:.1f}% ({opp_prev_oi} → {opp_current_oi})")
                
                # Valid signal
                trade_strike, trade_opt = select_trade_strike(atm, opt)

                send_telegram(
                    f"🚀 *BANK NIFTY EXECUTION - {opt} BUILDUP*\n"
                    f"Buy {trade_strike} {trade_opt}\n\n"
                    f"Qualifying {opt} @ {strike}: +{oi_pct:.0f}% (opp {opp_opt} {opp_decline_pct:+.1f}%)\n"
                    f"Spot Move: {spot_move:.2f}%   Vol ↑"
                )
                entry["state"] = "EXECUTED"
                updated = True
            else:
                print(f"⚠️ BN No opposite side entry for {strike} {opt}")

    if not baseline["started"]:
        send_telegram(
            f"*BANK NIFTY OI MONITOR STARTED*\n"
            f"Spot: {spot:.0f}   ATM: {atm}\n"
            f"Monthly expiry: {expiry_date}"
        )
        baseline["started"] = True
        updated = True
        
    if updated or baseline["data"]:
        if not baseline["data"]:
            print("WARNING: Processed rows but no baseline entries added (check MIN_BASE_OI or expiry)")
        save_baseline(baseline)
        print("Baseline saved — entries count:", len(baseline["data"]))
    else:
        print("No changes/alerts — baseline not saved this run")

# ================= ENTRY =================
if __name__ == "__main__":
    scan()