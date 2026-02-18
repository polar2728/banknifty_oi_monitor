import os
import json
import time
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone, time as dt_time
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
MIN_BASE_OI = 2000
STRIKE_RANGE = 200
CHECK_MARKET_HOURS = False  # set to True in production
BASELINE_FILE = "bn_baseline_oi.json"

# ─── Core thresholds (will be adjusted by expiry) ───
DEFAULT_WATCH_OI = 70
DEFAULT_EXEC_OI = 200
SPOT_MOVE_PCT = 0.3
VOL_MULTIPLIER = 1.5

# ─── Quality filters ───
OI_BOTH_SIDES_AVOID = 180
PREMIUM_MAX_RISE = 2
MIN_DECLINE_PCT = -1.5  # Default, adjusted by expiry

# ─── Conviction scoring ───
MIN_CONVICTION_SCORE = 90  # Balanced mode
TIME_FILTER_START = dt_time(9, 45)
TIME_FILTER_END = dt_time(15, 0)
BN_ENTRY_TIME = dt_time(10, 15)  # Bank Nifty specific

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

def is_trading_window():
    """Check if current time is within valid trading window"""
    t = now_ist().time()
    return TIME_FILTER_START <= t <= TIME_FILTER_END

def after_bn_entry_time():
    """Bank Nifty specific: wait until 10:15 AM"""
    return now_ist().time() >= BN_ENTRY_TIME

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
    try:
        resp = safe_api_call(fyers.quotes, {"symbols": "NSE:NIFTYBANK-INDEX"})
        if not resp or "d" not in resp or not resp["d"]:
            return None

        v = resp["d"][0].get("v", {})
        lp = v.get("lp") or v.get("ltp") or v.get("prev_close_price")
        if lp is None or lp == 0:
            return None
        return float(lp)
    except Exception as e:
        error_msg = f"❌ *BN API ERROR - Spot Fetch Failed*\n{str(e)}"
        send_telegram(error_msg)
        if DEBUG_MODE:
            print("❌ Spot parse error:", e)
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

def migrate_baseline_if_needed(baseline):
    """Auto-migrate old baseline format"""
    migrated = False
    
    if "day_open" not in baseline:
        baseline["day_open"] = None
        migrated = True
        print("🔄 BN Migrated: Added day_open")
    
    for key, entry in baseline.get("data", {}).items():
        if "state" not in entry:
            entry["state"] = "NONE"
            migrated = True
        if "first_exec_time" not in entry:
            entry["first_exec_time"] = None
            migrated = True
        if "scan_count" not in entry:
            entry["scan_count"] = 0
            migrated = True
        if "prev_oi" not in entry:
            entry["prev_oi"] = entry.get("base_oi", 0)
            migrated = True
    
    if migrated:
        save_baseline(baseline)
        print("✅ BN Baseline auto-migrated")
    
    return baseline

def reset_day(b):
    today = now_ist().date().isoformat()
    if b.get("date") != today:
        print("🔄 BN New trading day → baseline reset")
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

    for e in expiry_info:
        try:
            exp = datetime.fromtimestamp(int(e["expiry"]), tz=IST).date()
            days = (exp - today).days
            if days >= 0:
                valid_expiries.append((days, e["date"]))
        except Exception as ex:
            print("Expiry parse error:", ex)
            continue

    if not valid_expiries:
        print("No valid expiry found")
        return None, None

    nearest_days, nearest_date = min(valid_expiries, key=lambda x: x[0])
    print(f"SELECTED EXPIRY: {nearest_date} ({nearest_days} days left)")
    return nearest_date, nearest_days

# ================= CONVICTION SCORING =================
def calculate_conviction_score(buildup_info, atm, day_open, spot, strike_oi_changes):
    """Calculate conviction score for Bank Nifty signals"""
    score = 0
    details = []
    
    strike = buildup_info['strike']
    opt = buildup_info['opt_type']
    oi_pct = buildup_info['oi_pct']
    opp_decline_pct = buildup_info['opp_decline_pct']
    vol_multiplier = buildup_info['vol_multiplier']
    buildup_time_mins = buildup_info['buildup_time_mins']
    scan_count = buildup_info['scan_count']
    spot_move_pct = buildup_info['spot_move_pct']
    
    # A. Strike Quality (0-30 points) - Bank Nifty uses 100 point strikes
    strike_distance = abs(strike - atm)
    if strike_distance <= 50:
        score += 30
        details.append("✓ ATM strike (+30)")
    elif strike_distance <= 100:
        score += 20
        details.append("✓ Near ATM (+20)")
    elif strike_distance <= 150:
        score += 10
        details.append("✓ Mid-range (+10)")
    else:
        details.append("○ Far OTM (+0)")
    
    # B. Volume Confirmation (0-20 points)
    if vol_multiplier >= 3:
        score += 20
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+20)")
    elif vol_multiplier >= 2:
        score += 10
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+10)")
    elif vol_multiplier >= 1.5:
        score += 5
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+5)")
    else:
        details.append(f"○ Low volume {vol_multiplier:.1f}x (+0)")
    
    # C. Buildup Velocity (0-25 points)
    if buildup_time_mins <= 30:
        score += 25
        details.append(f"✓ Fast buildup {buildup_time_mins:.0f}m (+25)")
    elif buildup_time_mins <= 60:
        score += 15
        details.append(f"✓ Moderate speed {buildup_time_mins:.0f}m (+15)")
    elif buildup_time_mins <= 120:
        score += 5
        details.append(f"○ Gradual {buildup_time_mins:.0f}m (+5)")
    else:
        details.append(f"○ Slow buildup (+0)")
    
    # D. Opposite Decline Magnitude (0-25 points)
    opp_decline_abs = abs(opp_decline_pct)
    if opp_decline_abs >= 10:
        score += 25
        details.append(f"✓ Heavy covering -{opp_decline_abs:.1f}% (+25)")
    elif opp_decline_abs >= 5:
        score += 15
        details.append(f"✓ Moderate covering -{opp_decline_abs:.1f}% (+15)")
    elif opp_decline_abs >= 1.5:
        score += 5
        details.append(f"○ Weak covering -{opp_decline_abs:.1f}% (+5)")
    
    # E. Spot Momentum Alignment (0-30 points) - BANK NIFTY WEIGHTED HIGHER
    # Bank Nifty already requires 0.3% move, so this is important
    if opt == "CE":  # CE buildup = bearish
        if spot_move_pct <= -0.5:
            score += 30
            details.append(f"✓ Strong move {spot_move_pct:.2f}% (+30)")
        elif spot_move_pct <= -0.3:
            score += 20
            details.append(f"✓ Aligned {spot_move_pct:.2f}% (+20)")
        elif spot_move_pct < 0:
            score += 10
            details.append(f"✓ Weak align {spot_move_pct:.2f}% (+10)")
        else:
            score -= 20
            details.append(f"✗ MISALIGNED +{spot_move_pct:.2f}% (-20)")
    else:  # PE buildup = bullish
        if spot_move_pct >= 0.5:
            score += 30
            details.append(f"✓ Strong move +{spot_move_pct:.2f}% (+30)")
        elif spot_move_pct >= 0.3:
            score += 20
            details.append(f"✓ Aligned +{spot_move_pct:.2f}% (+20)")
        elif spot_move_pct > 0:
            score += 10
            details.append(f"✓ Weak align +{spot_move_pct:.2f}% (+10)")
        else:
            score -= 20
            details.append(f"✗ MISALIGNED {spot_move_pct:.2f}% (-20)")
    
    # F. Sustainability Check (0-15 points)
    if scan_count >= 3:
        score += 15
        details.append(f"✓ Sustained {scan_count} scans (+15)")
    elif scan_count >= 2:
        score += 10
        details.append(f"✓ Confirmed 2 scans (+10)")
    else:
        details.append("○ Single scan (+0)")
    
    # G. Adjacent Strike Confirmation (0-15 points)
    adjacent_building = 0
    for offset in [-100, 100]:  # Bank Nifty 100-point strikes
        adj_strike = strike + offset
        ce_pct = strike_oi_changes.get(adj_strike, {}).get("CE", 0)
        pe_pct = strike_oi_changes.get(adj_strike, {}).get("PE", 0)
        
        if opt == "CE" and ce_pct >= buildup_info.get('exec_threshold', 200):
            adjacent_building += 1
        elif opt == "PE" and pe_pct >= buildup_info.get('exec_threshold', 200):
            adjacent_building += 1
    
    if adjacent_building >= 2:
        score += 15
        details.append(f"✓ {adjacent_building} adjacent strikes (+15)")
    elif adjacent_building >= 1:
        score += 10
        details.append(f"✓ 1 adjacent strike (+10)")
    else:
        details.append("○ Isolated strike (+0)")
    
    # Determine tier
    if score >= 120:
        tier = "🔥 PREMIUM"
        emoji = "🔥"
    elif score >= 90:
        tier = "✅ HIGH"
        emoji = "✅"
    elif score >= 60:
        tier = "⚠️ MEDIUM"
        emoji = "⚠️"
    else:
        tier = "❌ LOW"
        emoji = "❌"
    
    return score, tier, emoji, details

# ================= SCAN =================
def scan():
    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        return

    baseline = reset_day(load_baseline())
    baseline = migrate_baseline_if_needed(baseline)

    # Send startup ping
    if not baseline["started"]:
        try:
            spot = get_banknifty_spot()
            if spot is None:
                print("⚠ BANKNIFTY spot unavailable for startup")
                return
            
            atm = int(round(spot / 100) * 100)
            send_telegram(
                f"✅ *BANK NIFTY OI MONITOR STARTED*\n"
                f"Spot: {spot:.0f}   ATM: {atm}\n"
                f"Mode: Balanced (90+ score)"
            )
            baseline["started"] = True
            save_baseline(baseline)
        except Exception as e:
            print(f"Failed to send startup ping: {e}")
            return

    # Time-of-day filter
    if not is_trading_window():
        current_time = now_ist().strftime('%H:%M')
        print(f"⏸ Outside trading window ({current_time})")
        return

    spot = get_banknifty_spot()
    if spot is None:
        print("⚠ BANKNIFTY spot unavailable — skipping scan")
        return

    if baseline["day_open"] is None:
        baseline["day_open"] = spot
        save_baseline(baseline)
        print(f"📊 BN Day open captured: {spot:.0f}")

    atm = int(round(spot / 100) * 100)

    try:
        chain_resp = safe_api_call(fyers.optionchain, {
            "symbol": "NSE:NIFTYBANK-INDEX",
            "strikecount": 40,
            "timestamp": ""
        })
        if not chain_resp or chain_resp.get("s") != "ok":
            raise Exception("Option chain fetch failed")
    except Exception as e:
        error_msg = f"❌ *BN API ERROR - Option Chain Failed*\n{str(e)}"
        send_telegram(error_msg)
        return

    raw = chain_resp["data"]["optionsChain"]
    expiry_info = chain_resp["data"]["expiryData"]

    if not raw:
        print("No optionsChain data returned")
        return

    expiry_date, days_to_expiry = get_monthly_expiry(expiry_info)
    if not expiry_date:
        print("No suitable monthly expiry found")
        return

    # Dynamic thresholds based on days to expiry
    if days_to_expiry > 14:
        WATCH_OI_PCT = 70
        EXEC_OI_PCT = 100
        MIN_DECLINE_PCT = -1.5
        print(f"Days to expiry: {days_to_expiry} → low thresholds")
    elif 8 <= days_to_expiry <= 14:
        WATCH_OI_PCT = 120
        EXEC_OI_PCT = 200
        MIN_DECLINE_PCT = -1.2
        print(f"Days to expiry: {days_to_expiry} → medium thresholds")
    else:
        WATCH_OI_PCT = 250
        EXEC_OI_PCT = 400
        MIN_DECLINE_PCT = -0.8
        print(f"Days to expiry: {days_to_expiry} → high thresholds")

    OI_BOTH_SIDES_AVOID = max(120, EXEC_OI_PCT * 0.6)

    expiry = expiry_to_symbol_format(expiry_date)
    df = pd.DataFrame(raw)
    df = df[df["symbol"].str.contains(expiry, regex=False)]
    df = df[
        (df["strike_price"].between(atm - STRIKE_RANGE, atm + STRIKE_RANGE)) &
        (df["strike_price"] % 100 == 0)
    ]

    print(f"[{now_ist().strftime('%H:%M:%S')}] Spot: {spot:.0f} | ATM: {atm}")
    
    current_time = now_ist()
    
    # Pre-compute OI changes and current OI map
    strike_oi_changes = {}
    current_oi_map = {}
    
    for _, r in df.iterrows():
        strike = int(r.get("strike_price", 0))
        opt = r.get("option_type", "")
        oi = int(r.get("oi", 0))

        if strike == 0 or opt not in ("CE", "PE"):
            continue

        current_oi_map[(strike, opt)] = oi

        key = f"{opt}_{strike}"
        entry = baseline["data"].get(key)
        if entry is None or entry.get("base_oi", 0) < MIN_BASE_OI:
            continue

        base_oi = entry["base_oi"]
        oi_pct = ((oi - base_oi) / base_oi) * 100 if base_oi > 0 else 0

        if strike not in strike_oi_changes:
            strike_oi_changes[strike] = {}
        strike_oi_changes[strike][opt] = oi_pct

    # Main processing loop
    updated = False
    ce_buildups = []
    pe_buildups = []

    for _, r in df.iterrows():
        strike = int(r.get("strike_price", 0))
        opt = r.get("option_type", "")
        oi = int(r.get("oi", 0))
        ltp = float(r.get("ltp", 0))
        vol = int(r.get("volume", 0))

        if strike == 0 or opt not in ("CE", "PE"):
            continue

        key = f"{opt}_{strike}"
        entry = baseline["data"].setdefault(key, {
            "base_oi": oi,
            "base_ltp": ltp,
            "base_vol": vol,
            "prev_oi": oi,
            "state": "NONE",
            "first_exec_time": None,
            "scan_count": 0
        })

        if entry["base_oi"] < MIN_BASE_OI:
            continue

        oi_pct = ((oi - entry["base_oi"]) / entry["base_oi"]) * 100
        ltp_change_pct = ((ltp - entry["base_ltp"]) / entry["base_ltp"] * 100) if entry["base_ltp"] > 0 else 0
        vol_multiplier = vol / entry["base_vol"] if entry["base_vol"] > 0 else 1

        # WATCH alerts
        if oi_pct >= WATCH_OI_PCT and entry["state"] == "NONE":
            send_telegram(
                f"👀 *BN OI WATCH*\n"
                f"{strike} {opt}\n"
                f"OI +{oi_pct:.0f}%\n"
                f"Spot: {spot:.0f}  ATM: {atm}"
            )
            entry["state"] = "WATCH"
            updated = True

        # Track buildup time
        if oi_pct >= EXEC_OI_PCT:
            if entry.get("first_exec_time") is None:
                entry["first_exec_time"] = current_time.isoformat()
                entry["scan_count"] = 1
                updated = True
            else:
                entry["scan_count"] = entry.get("scan_count", 0) + 1
                updated = True
        else:
            if entry.get("first_exec_time"):
                entry["first_exec_time"] = None
                entry["scan_count"] = 0
                updated = True

        # EXECUTION checks
        if entry["state"] == "EXECUTED":
            continue

        is_short_buildup = (oi_pct >= EXEC_OI_PCT) and (ltp_change_pct <= PREMIUM_MAX_RISE)

        if is_short_buildup and entry["state"] == "WATCH":
            # Bank Nifty specific: wait until 10:15 AM
            if not after_bn_entry_time():
                continue

            # Bank Nifty specific: spot move requirement
            spot_move_pct = ((spot - baseline["day_open"]) / baseline["day_open"]) * 100
            if abs(spot_move_pct) < SPOT_MOVE_PCT or vol_multiplier < VOL_MULTIPLIER:
                continue

            # Conflict check
            ce_pct_here = strike_oi_changes.get(strike, {}).get("CE", 0)
            pe_pct_here = strike_oi_changes.get(strike, {}).get("PE", 0)
            if ce_pct_here >= OI_BOTH_SIDES_AVOID and pe_pct_here >= OI_BOTH_SIDES_AVOID:
                print(f"⛔ BN Skipping conflicted: CE +{ce_pct_here:.0f}%, PE +{pe_pct_here:.0f}%")
                continue

            # Covering check
            opp_opt = "PE" if opt == "CE" else "CE"
            opp_key = f"{opp_opt}_{strike}"
            opp_entry = baseline["data"].get(opp_key)

            if not opp_entry:
                print(f"⚠️ BN No opposite entry for {strike} {opt}")
                continue

            opp_current_oi = current_oi_map.get((strike, opp_opt), 0)
            if opp_current_oi == 0:
                print(f"⚠️ BN No opposite data for {opp_opt} at {strike}")
                continue

            opp_prev_oi = opp_entry.get("prev_oi", opp_entry.get("base_oi", 0))
            opp_decline_pct = ((opp_current_oi - opp_prev_oi) / opp_prev_oi * 100) if opp_prev_oi > 0 else 0
            is_covering = (opp_current_oi < opp_prev_oi) and (opp_decline_pct <= MIN_DECLINE_PCT)

            if not is_covering:
                print(f"⚠️ BN Rejected: opposite not declining enough ({opp_decline_pct:+.1f}%)")
                continue

            # Calculate buildup time
            first_exec_time = entry.get("first_exec_time")
            buildup_time_mins = (current_time - datetime.fromisoformat(first_exec_time)).total_seconds() / 60 if first_exec_time else 0

            print(f"✓ BN Covering detected: {opp_opt} {opp_decline_pct:.1f}% ({opp_prev_oi} → {opp_current_oi})")

            # Prepare for conviction scoring
            buildup_info = {
                "strike": strike,
                "opt_type": opt,
                "oi_pct": oi_pct,
                "ltp_change_pct": ltp_change_pct,
                "vol_multiplier": vol_multiplier,
                "opp_decline_pct": opp_decline_pct,
                "buildup_time_mins": buildup_time_mins,
                "scan_count": entry.get("scan_count", 1),
                "spot_move_pct": spot_move_pct,
                "exec_threshold": EXEC_OI_PCT
            }

            # CONVICTION SCORING
            conviction_score, tier, emoji, score_details = calculate_conviction_score(
                buildup_info, atm, baseline.get("day_open"), spot, strike_oi_changes
            )

            print(f"📊 BN Conviction: {conviction_score} - {tier}")
            for detail in score_details:
                print(f"   {detail}")

            if conviction_score < MIN_CONVICTION_SCORE:
                print(f"⚠️ BN Score {conviction_score} below threshold {MIN_CONVICTION_SCORE}")
                continue

            # Add to buildups
            buildup_info["conviction_score"] = conviction_score
            buildup_info["tier"] = tier
            buildup_info["emoji"] = emoji
            buildup_info["score_details"] = score_details

            if opt == "CE":
                ce_buildups.append(buildup_info)
            else:
                pe_buildups.append(buildup_info)

            entry["state"] = "EXECUTED"
            updated = True

    # Update prev_oi for all entries
    for _, r in df.iterrows():
        strike = int(r.get("strike_price", 0))
        opt = r.get("option_type", "")
        oi = int(r.get("oi", 0))

        if strike == 0 or opt not in ("CE", "PE"):
            continue

        key = f"{opt}_{strike}"
        if key in baseline["data"]:
            baseline["data"][key]["prev_oi"] = oi
            updated = True

    # Send grouped alerts
    if ce_buildups:
        ce_buildups_sorted = sorted(ce_buildups, key=lambda x: x["conviction_score"], reverse=True)
        best = ce_buildups_sorted[0]
        
        score_breakdown = "\n".join(f"  {d}" for d in best["score_details"])
        
        details = "\n".join(
            f"{b['strike']} CE: +{b['oi_pct']:.0f}% | Score: {b['conviction_score']}"
            for b in ce_buildups_sorted[:3]
        )

        # Calculate trade strike
        trade_strike = atm - 100
        trade_opt = "PE"

        msg = (
            f"{best['emoji']} *BN EXECUTION - CE BUILDUP*\n"
            f"*Tier: {best['tier']} | Score: {best['conviction_score']}/150*\n\n"
            f"*Action: Buy {trade_strike} {trade_opt}*\n\n"
            f"📊 *Score Breakdown:*\n{score_breakdown}\n\n"
            f"*Top Signals:*\n{details}\n\n"
            f"Spot: {spot:.0f} | Move: {best['spot_move_pct']:.2f}%"
        )
        send_telegram(msg)

    if pe_buildups:
        pe_buildups_sorted = sorted(pe_buildups, key=lambda x: x["conviction_score"], reverse=True)
        best = pe_buildups_sorted[0]
        
        score_breakdown = "\n".join(f"  {d}" for d in best["score_details"])
        
        details = "\n".join(
            f"{b['strike']} PE: +{b['oi_pct']:.0f}% | Score: {b['conviction_score']}"
            for b in pe_buildups_sorted[:3]
        )

        # Calculate trade strike
        trade_strike = atm + 100
        trade_opt = "CE"

        msg = (
            f"{best['emoji']} *BN EXECUTION - PE BUILDUP*\n"
            f"*Tier: {best['tier']} | Score: {best['conviction_score']}/150*\n\n"
            f"*Action: Buy {trade_strike} {trade_opt}*\n\n"
            f"📊 *Score Breakdown:*\n{score_breakdown}\n\n"
            f"*Top Signals:*\n{details}\n\n"
            f"Spot: {spot:.0f} | Move: {best['spot_move_pct']:.2f}%"
        )
        send_telegram(msg)

    if updated:
        save_baseline(baseline)
        print(f"✓ BN Baseline saved — {len(baseline['data'])} entries")
    else:
        print("No changes this scan")

# ================= ENTRY =================
if __name__ == "__main__":
    try:
        scan()
    except Exception as e:
        print(f"Fatal error: {e}")
        send_telegram(f"❌ *BN SCANNER CRASHED*\n{str(e)}")