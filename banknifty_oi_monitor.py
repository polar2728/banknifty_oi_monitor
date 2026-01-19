import os
import json
import time
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
WATCH_OI_PCT        = 70
EXEC_OI_PCT         = 140
SPOT_MOVE_PCT       = 0.3
VOL_MULTIPLIER      = 1.5
MIN_BASE_OI         = 2000
STRIKE_RANGE        = 200
CHECK_MARKET_HOURS  = False
BASELINE_FILE       = "bn_baseline_oi.json"

DEBUG_MODE = str(os.environ.get("DEBUG_MODE", "false")).lower() == "true"

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))

# ================= SECRETS =================
CLIENT_ID        = os.environ.get("CLIENT_ID")
ACCESS_TOKEN     = os.environ.get("ACCESS_TOKEN")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
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
    year_short = d.strftime("%y")                # "26"
    month_short = d.strftime("%b").upper()       # "JAN", "FEB", "MAR", ..., "DEC"
    return year_short + month_short              # "26JAN"

def get_monthly_expiry(expiry_info):
    today = now_ist().date()
    expiries = []
    for e in expiry_info:
        try:
            exp = datetime.fromtimestamp(int(e["expiry"])).date()
            days = (exp - today).days
            if days >= 7:
                expiries.append((days, e["date"]))
        except:
            continue
    return sorted(expiries, key=lambda x: x[0])[0][1] if expiries else None

# ================= STRIKE SELECTION =================

def select_trade_strike(strike, buildup_type):
    # Same-strike contrarian: buy opposite option at the same strike
    if buildup_type == "CE":   # short buildup on CE → buy PE at same strike
        return strike, "PE"
    else:                      # short buildup on PE → buy CE at same strike
        return strike, "CE"

# ================= SCAN =================
def scan():
    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        return

    baseline = reset_day(load_baseline())

    # ---- Spot (SAFE) ----
    spot = get_banknifty_spot()
    if spot is None:
        print("⚠ BANKNIFTY spot unavailable — skipping scan")
        return

    if baseline["day_open"] is None:
        baseline["day_open"] = spot

    atm = int(round(spot / 100) * 100)

    # ---- Option Chain ----
    chain_resp = safe_api_call(fyers.optionchain, {
        "symbol": "NSE:NIFTYBANK-INDEX",
        "strikecount": 40,
        "timestamp": ""
    })
    if not chain_resp:
        print("Option chain API call returned None — likely network/auth issue")
        return

    print("Optionchain response status:", chain_resp.get("s"))          # Debug: 'ok' or 'error'
    print("Full chain_resp keys:", list(chain_resp.keys()))            # Debug: see what's actually there

    if chain_resp.get("s") != "ok":
        msg = f"Optionchain failed: {chain_resp.get('message', 'Unknown error')} (code: {chain_resp.get('code')})"
        print(msg)
        # Optional: send_telegram(msg) if you want alert
        return

    # Now safely access
    data = chain_resp.get("data", {})
    if not data:
        print("Response 'ok' but no 'data' key — possibly after-hours empty response")
        return
    
    raw = chain_resp["data"]["optionsChain"]
    expiry_info = chain_resp["data"]["expiryData"]

    if not raw:
        print("No optionsChain data returned (empty list) — likely market closed or no contracts loaded")
        return

    expiry_date = get_monthly_expiry(expiry_info)
    if not expiry_date:
        print("No suitable monthly expiry found")
        return

    expiry = expiry_to_symbol_format(expiry_date)

    df = pd.DataFrame(raw)
    df = df[df["symbol"].str.contains(expiry, regex=False)]
    
    df = df[
        (df["strike_price"].between(atm - STRIKE_RANGE, atm + STRIKE_RANGE)) &
        (df["strike_price"] % 100 == 0)
    ]

    # Debug prints
    print(f"Selected monthly expiry date: {expiry_date}")
    print(f"Expiry filter string: {expiry}")
    print(f"Total raw options: {len(raw)}")
    print(f"After expiry filter: {len(df[df['symbol'].str.contains(expiry)])}")
    print(f"After strike range filter: {len(df)}")
    print(f"Number of valid CE/PE rows: {len(df[df['option_type'].isin(['CE', 'PE'])])}")

    # FIXED: Calculate days to expiry correctly
    expiry_dt = datetime.strptime(expiry_date, "%d-%m-%Y").date()  # ← add .date()
    today_dt = now_ist().date()
    days_to_expiry = (expiry_dt - today_dt).days
    
    if days_to_expiry > 14:
        WATCH_OI_PCT = 70
        EXEC_OI_PCT  = 200
        print(f"Days to expiry: {days_to_expiry} → using low thresholds: {WATCH_OI_PCT}% / {EXEC_OI_PCT}% (early month)")
    elif 8 <= days_to_expiry <= 14:
        WATCH_OI_PCT = 150
        EXEC_OI_PCT  = 300
        print(f"Days to expiry: {days_to_expiry} → using medium thresholds: {WATCH_OI_PCT}% / {EXEC_OI_PCT}% (mid-cycle)")
    else:  # <= 7 days
        WATCH_OI_PCT = 300
        EXEC_OI_PCT  = 500
        print(f"Days to expiry: {days_to_expiry} → using high thresholds: {WATCH_OI_PCT}% / {EXEC_OI_PCT}% (expiry week)")
    

    expiry = expiry_to_symbol_format(expiry_date)

    df = pd.DataFrame(raw)
    # First filter: expiry code in symbol
    df = df[df["symbol"].str.contains(expiry, regex=False)]
    
    # Second filter: strike range around ATM + valid strikes
    df = df[
        (df["strike_price"].between(atm - STRIKE_RANGE, atm + STRIKE_RANGE)) &
        (df["strike_price"] % 100 == 0)
    ]

    # Now safe to print debug info
    print(f"Selected monthly expiry date: {expiry_date}")
    print(f"Expiry filter string: {expiry}")
    print(f"Total raw options: {len(raw)}")
    print(f"After expiry filter: {len(df[df['symbol'].str.contains(expiry)])}")  # redundant now, but ok
    print(f"After strike range filter: {len(df)}")
    print(f"Number of valid CE/PE rows: {len(df[df['option_type'].isin(['CE', 'PE'])])}")

    updated = False
    # Collect qualifying strikes per side (to group alerts)
    ce_buildups = []
    pe_buildups = []

    for _, r in df.iterrows():
        strike = int(r.get("strike_price", 0))
        opt    = r.get("option_type", "")
        oi     = int(r.get("oi", 0))
        vol    = int(r.get("volume", 0))

        if strike == 0 or opt not in ("CE", "PE"):
            continue

        key = f"{opt}_{strike}"
        entry = baseline["data"].setdefault(key, {
            "base_oi": oi,
            "base_vol": vol,
            "state": "NONE"
        })

        if entry["base_oi"] < MIN_BASE_OI:
            continue

        oi_pct = ((oi - entry["base_oi"]) / entry["base_oi"]) * 100
        vol_ok = vol > entry["base_vol"] * VOL_MULTIPLIER

        # ---- WATCH ----
        if oi_pct >= WATCH_OI_PCT and entry["state"] == "NONE":
            send_telegram(
                f"👀 *BN OI WATCH*\n"
                f"{strike} {opt}\n"
                f"OI +{oi_pct:.0f}%\n"
                f"Spot: {spot:.0f}  ATM: {atm}"
            )
            entry["state"] = "WATCH"
            updated = True

        # ---- EXECUTE ----
        if oi_pct >= EXEC_OI_PCT:
            if not after_1015():
                continue

            spot_move = abs(spot - baseline["day_open"]) / baseline["day_open"] * 100

            buildup_info = {
                "strike": strike,
                "oi_pct": oi_pct,
                "vol_ok": vol_ok
            }
            # Collect instead of immediate send
            if opt == "CE":
                ce_buildups.append(buildup_info)
            else:
                pe_buildups.append(buildup_info)

            entry["state"] = "EXECUTED"
            updated = True

    # Grouped alerts after loop (one per side)
    if ce_buildups:
        # Pick first qualifying strike for the trade recommendation
        first = ce_buildups[0]
        trade_strike = first["strike"]
        trade_opt = "PE"  # same-strike contrarian

        details = "\n".join(
            f"{b['strike']} CE: +{b['oi_pct']:.0f}%"
            for b in ce_buildups
        )

        msg = (
            f"🚀 *EXECUTION SIGNAL - CE BUILDUP*\n"
            f"Buy {trade_strike} {trade_opt}\n\n"
            f"Qualifying CE strikes:\n{details}\n\n"
            f"Spot: {spot}"
        )
        send_telegram_alert(msg)

    if pe_buildups:
        first = pe_buildups[0]
        trade_strike = first["strike"]
        trade_opt = "CE"

        details = "\n".join(
            f"{b['strike']} PE: +{b['oi_pct']:.0f}%"
            for b in pe_buildups
        )

        msg = (
            f"🚀 *EXECUTION SIGNAL - PE BUILDUP*\n"
            f"Buy {trade_strike} {trade_opt}\n\n"
            f"Qualifying PE strikes:\n{details}\n\n"
            f"Spot: {spot}"
        )
        send_telegram_alert(msg)

    if not baseline["started"]:
        send_telegram(
            f"*BANK NIFTY OI MONITOR STARTED*\n"
            f"Spot: {spot:.0f}   ATM: {atm}\n"
            f"Monthly expiry: {expiry_date}"
        )
        baseline["started"] = True
        updated = True

    # NEW: Save if we added any entries (even without alerts)
    if baseline["data"] or updated:  # or len(baseline["data"]) > 0
        if not baseline["data"]:
            print("WARNING: Processed rows but no baseline entries added (all OI < MIN_BASE_OI?)")
        save_baseline(baseline)
        print("Baseline saved — entries count:", len(baseline["data"]))
    else:
        print("No changes/alerts — baseline not saved this run")

# ================= ENTRY =================
if __name__ == "__main__":
    scan()