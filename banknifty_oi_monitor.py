import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel
import requests

# ================= CONFIG =================
OI_WATCH_THRESHOLD    = 300    # %
OI_EXEC_THRESHOLD     = 500    # %
MIN_BASE_OI           = 1000
STRIKE_RANGE_POINTS   = 300
CHECK_MARKET_HOURS    = True
BASELINE_FILE         = "bn_baseline_oi.json"

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

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

# ================= BASELINE =================
def load_baseline():
    if os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE, "r") as f:
            return json.load(f)
    return {"date": None, "data": {}, "first_alert_sent": False}

def save_baseline(b):
    with open(BASELINE_FILE, "w") as f:
        json.dump(b, f, indent=2)

def reset_on_new_day(b):
    today = now_ist().date().isoformat()
    if b.get("date") != today:
        print("🔄 New trading day → baseline reset")
        b["date"] = today
        b["data"] = {}
        b["first_alert_sent"] = False
        save_baseline(b)
    return b

# ================= API =================
def get_banknifty_spot():
    q = fyers.quotes({"symbols": "NSE:NIFTYBANK-INDEX"})

    d = q.get("d", [])
    if not d or "v" not in d[0]:
        raise RuntimeError(f"❌ Invalid FYERS response: {q}")

    v = d[0]["v"]

    if v.get("s") == "error":
        raise RuntimeError(f"❌ FYERS error: {v}")

    if "lp" in v:
        return float(v["lp"])
    if "ltp" in v:
        return float(v["ltp"])
    if "last_price" in v:
        return float(v["last_price"])

    raise RuntimeError(f"❌ Spot price not found in FYERS response: {v}")



def fetch_option_chain():
    r = fyers.optionchain({
        "symbol": "NSE:BANKNIFTY",
        "strikecount": 40,
        "timestamp": ""
    })

    if r.get("s") != "ok":
        print("⚠️ Option chain unavailable:", r.get("message"))
        return []

    data = r.get("data", {})
    return data.get("optionsChain", [])



# ================= STRIKE SELECTION =================
def select_trade_strike(atm, buildup_type):
    if buildup_type == "CE":   # short buildup → buy PE
        return atm - 100, "PE"
    else:                      # long buildup → buy CE
        return atm + 100, "CE"

# ================= SCAN =================
def scan():
    print("▶ BankNifty scan started")

    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        return

    baseline = reset_on_new_day(load_baseline())

    spot = get_banknifty_spot()
    atm = int(round(spot / 100) * 100)

    raw = fetch_option_chain()
    if not raw:
        print("⏱ Skipping scan – option chain not available")
        return
    
    df = pd.DataFrame(raw)

    # 🔑 IMPORTANT: FYERS BankNifty symbols look like:
    # BANKNIFTY 26Jan27 59500 CE
    df = df[df["symbol"].str.contains("BANKNIFTY", regex=False)]

    print("📊 Rows after BANKNIFTY filter:", len(df))

    for _, r in df.iterrows():
        try:
            parts = r.symbol.split()
            strike = int(parts[2])   # 59500
            opt    = parts[3]        # CE / PE
        except Exception:
            continue

        if not (atm - STRIKE_RANGE_POINTS <= strike <= atm + STRIKE_RANGE_POINTS):
            continue

        oi  = int(r.oi)
        ltp = float(r.ltp)
        vol = int(r.volume)

        key = f"{opt}_{strike}"
        entry = baseline["data"].get(key)

        # ================= BASELINE INIT =================
        if entry is None:
            baseline["data"][key] = {
                "baseline_oi": oi,
                "baseline_ltp": ltp,
                "baseline_vol": vol,
                "state": "NONE"
            }
            continue

        base_oi  = entry["baseline_oi"]
        base_ltp = entry["baseline_ltp"]
        base_vol = entry["baseline_vol"]
        state    = entry["state"]

        if base_oi < MIN_BASE_OI:
            continue

        oi_pct = ((oi - base_oi) / base_oi) * 100
        ltp_ok = ltp > base_ltp * 1.05
        vol_ok = vol > base_vol * 1.3

        # ================= WATCH =================
        if oi_pct >= OI_WATCH_THRESHOLD and state == "NONE":
            send_telegram_alert(
                f"👀 *BANKNIFTY OI WATCH*\n"
                f"{strike} {opt}\n"
                f"OI +{oi_pct:.0f}%\n"
                f"Spot: {spot:.0f}"
            )
            entry["state"] = "WATCH"

        # ================= EXECUTION =================
        if oi_pct >= OI_EXEC_THRESHOLD and state == "WATCH":
            if ltp_ok and vol_ok:
                trade_strike, trade_opt = select_trade_strike(atm, opt)
                send_telegram_alert(
                    f"🚀 *BANKNIFTY EXECUTION*\n"
                    f"{opt} buildup confirmed\n"
                    f"Buy {trade_strike} {trade_opt}\n\n"
                    f"OI +{oi_pct:.0f}%\n"
                    f"LTP ↑ | Volume ↑\n"
                    f"Spot: {spot:.0f}"
                )
                entry["state"] = "EXECUTED"

    if not baseline["first_alert_sent"]:
        send_telegram_alert(
            f"*BANKNIFTY OI MONITOR STARTED*\n"
            f"Spot: {spot:.0f}\nATM: {atm}"
        )
        baseline["first_alert_sent"] = True

    save_baseline(baseline)

# ================= ENTRY =================
if __name__ == "__main__":
    scan()
