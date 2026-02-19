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

# ─── Static OI thresholds (conviction-gated approach) ───
WATCH_OI_PCT = 120      # Static watch threshold
EXEC_OI_PCT = 200       # Static execution threshold
SPOT_MOVE_PCT = 0.3     # Bank Nifty specific
VOL_MULTIPLIER = 1.5

# ─── Quality filters ───
OI_BOTH_SIDES_AVOID = 180
PREMIUM_MAX_RISE = 8    # Dynamic tolerance for premium movement
MIN_DECLINE_PCT = -1.5

# ─── Conviction scoring ───
CONVICTION_MODE = os.environ.get("CONVICTION_MODE", "BALANCED").upper()
CONVICTION_THRESHOLDS = {
    "STRICT": 110,
    "BALANCED": 90,
    "AGGRESSIVE": 70
}
MIN_CONVICTION_SCORE_BASE = CONVICTION_THRESHOLDS.get(CONVICTION_MODE, 90)

# ─── Time filters ───
TIME_FILTER_START = dt_time(9, 45)
TIME_FILTER_END = dt_time(15, 0)
BN_ENTRY_TIME = dt_time(10, 15)

# ─── Daily limits ───
MAX_SIGNALS_PER_DAY = 3
MAX_WATCH_PER_DAY = 3  # Separate cap for WATCH alerts
SCORE_IMPROVEMENT_THRESHOLD = 10

# ─── Logging ───
SCORE_LOG_FILE = "bn_conviction_scores.jsonl"
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
    """Bank Nifty specific: wait until 10:15 AM for EXECUTION"""
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

def log_conviction_score(signal_data):
    """Log all conviction scores for calibration analysis"""
    try:
        with open(SCORE_LOG_FILE, "a") as f:
            log_entry = {
                "timestamp": now_ist().isoformat(),
                "date": now_ist().date().isoformat(),
                "strike": signal_data["strike"],
                "opt_type": signal_data["opt_type"],
                "score": signal_data["conviction_score"],
                "tier": signal_data["tier"],
                "oi_pct": signal_data["oi_pct"],
                "opp_decline_pct": signal_data.get("opp_decline_pct", 0),
                "days_to_expiry": signal_data.get("days_to_expiry", 0),
                "signal_type": signal_data.get("signal_type", "EXECUTION"),
                "components": {
                    "strike_quality": signal_data.get("strike_quality_pts", 0),
                    "volume": signal_data.get("volume_pts", 0),
                    "velocity": signal_data.get("velocity_pts", 0),
                    "decline_magnitude": signal_data.get("decline_pts", 0),
                    "decline_streak": signal_data.get("decline_streak_pts", 0),
                    "spot_alignment": signal_data.get("spot_pts", 0),
                    "sustainability": signal_data.get("sustainability_pts", 0),
                    "cluster": signal_data.get("cluster_pts", 0),
                    "premium_behavior": signal_data.get("premium_behavior_pts", 0)
                }
            }
            f.write(json.dumps(log_entry) + "\n")
    except Exception as e:
        print(f"Score logging error: {e}")

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
        try:
            with open(BASELINE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠ Baseline file corrupted → resetting")
            return create_empty_baseline()
    return create_empty_baseline()

def create_empty_baseline():
    return {
        "date": None,
        "started": False,
        "day_open": None,
        "data": {},
        "signals_today": 0,
        "watch_today": 0,
        "daily_signals": []
    }

def save_baseline(b):
    with open(BASELINE_FILE, "w") as f:
        json.dump(b, f, indent=2)

def migrate_baseline_if_needed(baseline):
    """Auto-migrate old baseline format"""
    migrated = False
    
    # Root-level fields
    if "day_open" not in baseline:
        baseline["day_open"] = None
        migrated = True
    if "signals_today" not in baseline:
        baseline["signals_today"] = 0
        migrated = True
    if "watch_today" not in baseline:
        baseline["watch_today"] = 0
        migrated = True
    if "daily_signals" not in baseline:
        baseline["daily_signals"] = []
        migrated = True
    
    # Entry-level fields
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
        if "decline_streak" not in entry:
            entry["decline_streak"] = 0
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
        b["signals_today"] = 0
        b["watch_today"] = 0
        b["daily_signals"] = []
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

# ================= CONVICTION SCORING HELPERS =================
def calculate_buildup_time(entry, current_time):
    """Calculate minutes since OI first crossed threshold"""
    first_exec_time = entry.get("first_exec_time")
    if first_exec_time:
        try:
            first_time = datetime.fromisoformat(first_exec_time)
            minutes = (current_time - first_time).total_seconds() / 60
            return max(0, minutes)
        except:
            return 0
    return 0

def check_adjacent_cluster(strike, opt, strike_oi_changes, exec_threshold):
    """
    Check for institutional cluster patterns.
    Bank Nifty uses 100-point strikes.
    """
    adjacent_same_side = 0
    adjacent_opp_declining = 0
    
    opp_opt = "PE" if opt == "CE" else "CE"
    
    # Check 4 adjacent strikes (±100, ±200 points for Bank Nifty)
    for offset in [-200, -100, 100, 200]:
        adj_strike = strike + offset
        
        # Same-side buildup (70% of threshold counts)
        if opt == "CE":
            ce_pct = strike_oi_changes.get(adj_strike, {}).get("CE", 0)
            if ce_pct >= exec_threshold * 0.7:
                adjacent_same_side += 1
        else:
            pe_pct = strike_oi_changes.get(adj_strike, {}).get("PE", 0)
            if pe_pct >= exec_threshold * 0.7:
                adjacent_same_side += 1
        
        # Opposite-side decline (at least 5% drop)
        opp_pct = strike_oi_changes.get(adj_strike, {}).get(opp_opt, 0)
        if opp_pct < -5:
            adjacent_opp_declining += 1
    
    return adjacent_same_side, adjacent_opp_declining

def calculate_conviction_score(buildup_data, atm, day_open, spot, strike_oi_changes):
    """
    Calculate conviction score for Bank Nifty signals.
    Max: 190 points (with premium behavior component)
    """
    score = 0
    details = []
    components = {}
    
    strike = buildup_data['strike']
    opt = buildup_data['opt_type']
    oi_pct = buildup_data['oi_pct']
    opp_decline_pct = buildup_data.get('opp_decline_pct', 0)
    vol_multiplier = buildup_data['vol_multiplier']
    buildup_time_mins = buildup_data.get('buildup_time_mins', 0)
    scan_count = buildup_data.get('scan_count', 1)
    decline_streak = buildup_data.get('decline_streak', 0)
    spot_move_pct = buildup_data.get('spot_move_pct', 0)
    exec_threshold = buildup_data.get('exec_threshold', 200)
    ltp_change_pct = buildup_data.get('ltp_change_pct', 0)
    
    # A. Strike Quality (0-30 points) - Bank Nifty 100-point strikes
    strike_distance = abs(strike - atm)
    if strike_distance <= 50:
        pts = 30
        details.append("✓ ATM strike (+30)")
    elif strike_distance <= 100:
        pts = 20
        details.append("✓ Near ATM (+20)")
    elif strike_distance <= 150:
        pts = 10
        details.append("✓ Mid-range (+10)")
    else:
        pts = 0
        details.append("○ Far OTM (+0)")
    score += pts
    components['strike_quality_pts'] = pts
    
    # B. Volume Confirmation (0-20 points)
    if vol_multiplier >= 3:
        pts = 20
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+20)")
    elif vol_multiplier >= 2:
        pts = 10
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+10)")
    elif vol_multiplier >= 1.5:
        pts = 5
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+5)")
    else:
        pts = 0
        details.append(f"○ Low volume {vol_multiplier:.1f}x (+0)")
    score += pts
    components['volume_pts'] = pts
    
    # C. Buildup Velocity (0-25 points)
    if buildup_time_mins <= 30:
        pts = 25
        details.append(f"✓ Fast buildup {buildup_time_mins:.0f}m (+25)")
    elif buildup_time_mins <= 60:
        pts = 15
        details.append(f"✓ Moderate speed {buildup_time_mins:.0f}m (+15)")
    elif buildup_time_mins <= 120:
        pts = 5
        details.append(f"○ Gradual {buildup_time_mins:.0f}m (+5)")
    else:
        pts = 0
        details.append(f"○ Slow buildup (+0)")
    score += pts
    components['velocity_pts'] = pts
    
    # D. Opposite Decline Magnitude (0-25 points)
    opp_decline_abs = abs(opp_decline_pct)
    if opp_decline_abs >= 10:
        pts = 25
        details.append(f"✓ Heavy covering -{opp_decline_abs:.1f}% (+25)")
    elif opp_decline_abs >= 5:
        pts = 15
        details.append(f"✓ Moderate covering -{opp_decline_abs:.1f}% (+15)")
    elif opp_decline_abs >= 1.5:
        pts = 5
        details.append(f"○ Weak covering -{opp_decline_abs:.1f}% (+5)")
    else:
        pts = 0
    score += pts
    components['decline_pts'] = pts
    
    # D2. Sustained Decline Bonus (0-20 points)
    if decline_streak >= 3:
        pts = 20
        details.append(f"✓ Sustained decline {decline_streak} scans (+20)")
    elif decline_streak >= 2:
        pts = 10
        details.append(f"✓ Confirmed decline 2 scans (+10)")
    else:
        pts = 0
    score += pts
    components['decline_streak_pts'] = pts
    
    # E. Spot Momentum Alignment (0-30 points) - Bank Nifty weighted higher
    if opt == "CE":  # CE buildup = bearish
        if spot_move_pct <= -0.5:
            pts = 30
            details.append(f"✓ Strong move {spot_move_pct:.2f}% (+30)")
        elif spot_move_pct <= -0.3:
            pts = 20
            details.append(f"✓ Aligned {spot_move_pct:.2f}% (+20)")
        elif spot_move_pct < 0:
            pts = 10
            details.append(f"✓ Weak align {spot_move_pct:.2f}% (+10)")
        else:
            pts = -20
            details.append(f"✗ MISALIGNED +{spot_move_pct:.2f}% (-20)")
    else:  # PE buildup = bullish
        if spot_move_pct >= 0.5:
            pts = 30
            details.append(f"✓ Strong move +{spot_move_pct:.2f}% (+30)")
        elif spot_move_pct >= 0.3:
            pts = 20
            details.append(f"✓ Aligned +{spot_move_pct:.2f}% (+20)")
        elif spot_move_pct > 0:
            pts = 10
            details.append(f"✓ Weak align +{spot_move_pct:.2f}% (+10)")
        else:
            pts = -20
            details.append(f"✗ MISALIGNED {spot_move_pct:.2f}% (-20)")
    score += pts
    components['spot_pts'] = pts
    
    # F. Sustainability Check (0-15 points)
    if scan_count >= 3:
        pts = 15
        details.append(f"✓ Sustained {scan_count} scans (+15)")
    elif scan_count >= 2:
        pts = 10
        details.append(f"✓ Confirmed 2 scans (+10)")
    else:
        pts = 0
        details.append("○ Single scan (+0)")
    score += pts
    components['sustainability_pts'] = pts
    
    # G. Adjacent Strike Cluster (0-20 points)
    adj_same, adj_opp = check_adjacent_cluster(strike, opt, strike_oi_changes, exec_threshold)
    
    if adj_same >= 3 or adj_opp >= 2:
        pts = 20
        details.append(f"✓ Strong cluster (same:{adj_same}, opp:{adj_opp}) (+20)")
    elif adj_same >= 2 or adj_opp >= 1:
        pts = 10
        details.append(f"✓ Moderate cluster (+10)")
    else:
        pts = 0
        details.append("○ Isolated strike (+0)")
    score += pts
    components['cluster_pts'] = pts

    # H. Premium Behavior (0-15 points)
    # Validates true short buildup vs delta/gamma effects
    # Short buildup = premium should be flat or falling despite OI spike
    if ltp_change_pct <= -5:
        pts = 15
        details.append(f"✓ Premium falling {ltp_change_pct:.1f}% (+15)")
    elif ltp_change_pct <= 0:
        pts = 10
        details.append(f"✓ Premium flat {ltp_change_pct:.1f}% (+10)")
    elif ltp_change_pct <= 5:
        pts = 5
        details.append(f"○ Slight rise {ltp_change_pct:.1f}% (+5)")
    elif ltp_change_pct <= PREMIUM_MAX_RISE:
        pts = 0
        details.append(f"○ Rising {ltp_change_pct:.1f}% (+0)")
    else:
        pts = -10
        details.append(f"✗ High rise {ltp_change_pct:.1f}% (-10)")
    score += pts
    components['premium_behavior_pts'] = pts
    
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
    
    return score, tier, emoji, details, components

# ================= DAILY SIGNAL MANAGEMENT =================
def should_send_signal(baseline, new_signal_score):
    """Priority-based filtering for EXECUTION signals"""
    signals_today = baseline.get("signals_today", 0)
    
    if signals_today < MAX_SIGNALS_PER_DAY:
        return True, "UNDER_LIMIT"
    
    daily_signals = baseline.get("daily_signals", [])
    if not daily_signals:
        return True, "FIRST_SIGNAL"
    
    min_score = min(s["score"] for s in daily_signals)
    
    if new_signal_score > min_score + SCORE_IMPROVEMENT_THRESHOLD:
        return True, f"REPLACES_LOWER (beat {min_score} by {new_signal_score - min_score})"
    
    return False, f"REJECTED (score {new_signal_score} not better than {min_score})"

def record_signal(baseline, signal_data):
    """Track EXECUTION signals sent today"""
    baseline["signals_today"] = baseline.get("signals_today", 0) + 1
    
    signal_record = {
        "time": now_ist().strftime("%H:%M"),
        "strike": signal_data["strike"],
        "opt_type": signal_data["opt_type"],
        "score": signal_data["conviction_score"],
        "tier": signal_data["tier"]
    }
    
    daily_signals = baseline.get("daily_signals", [])
    daily_signals.append(signal_record)
    daily_signals = sorted(daily_signals, key=lambda x: x["score"], reverse=True)[:MAX_SIGNALS_PER_DAY]
    baseline["daily_signals"] = daily_signals

# ================= MAIN SCAN =================
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
                f"Mode: {CONVICTION_MODE} ({MIN_CONVICTION_SCORE_BASE}+ base score)\n"
                f"Limits: {MAX_SIGNALS_PER_DAY} signals, {MAX_WATCH_PER_DAY} watch/day"
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

    # Dynamic conviction requirements based on expiry phase
    if days_to_expiry > 20:
        MIN_CONVICTION_SCORE = 110
        print(f"Days to expiry: {days_to_expiry} → Early month (require 110+ score)")
    elif days_to_expiry > 10:
        MIN_CONVICTION_SCORE = MIN_CONVICTION_SCORE_BASE
        print(f"Days to expiry: {days_to_expiry} → Mid-month (require {MIN_CONVICTION_SCORE_BASE}+ score)")
    elif days_to_expiry > 4:
        MIN_CONVICTION_SCORE = 100
        print(f"Days to expiry: {days_to_expiry} → Late month (require 100+ score)")
    else:
        MIN_CONVICTION_SCORE = 120
        print(f"Days to expiry: {days_to_expiry} → Expiry week (require 120+ PREMIUM only)")

    # Calculate dynamic premium tolerance based on spot move
    spot_move_pct = 0
    if baseline.get("day_open"):
        spot_move_pct = ((spot - baseline["day_open"]) / baseline["day_open"]) * 100
    
    abs_spot_move = abs(spot_move_pct)
    if abs_spot_move >= 0.5:
        PREMIUM_TOLERANCE = 15
        print(f"Spot move {abs_spot_move:.2f}% → Premium tolerance: 15%")
    elif abs_spot_move >= 0.3:
        PREMIUM_TOLERANCE = 10
        print(f"Spot move {abs_spot_move:.2f}% → Premium tolerance: 10%")
    else:
        PREMIUM_TOLERANCE = PREMIUM_MAX_RISE
        print(f"Spot move {abs_spot_move:.2f}% → Premium tolerance: {PREMIUM_MAX_RISE}%")

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
            "scan_count": 0,
            "decline_streak": 0
        })

        if entry["base_oi"] < MIN_BASE_OI:
            continue

        oi_pct = ((oi - entry["base_oi"]) / entry["base_oi"]) * 100
        ltp_change_pct = ((ltp - entry["base_ltp"]) / entry["base_ltp"] * 100) if entry["base_ltp"] > 0 else 0
        vol_multiplier = vol / entry["base_vol"] if entry["base_vol"] > 0 else 1

        # ================= WATCH (TWO-TIER WITH FILTERS) =================
        if oi_pct >= WATCH_OI_PCT and entry["state"] == "NONE":
            # Basic quality filters for WATCH
            ce_pct = strike_oi_changes.get(strike, {}).get("CE", 0)
            pe_pct = strike_oi_changes.get(strike, {}).get("PE", 0)
            conflicted = (ce_pct >= OI_BOTH_SIDES_AVOID and pe_pct >= OI_BOTH_SIDES_AVOID)
            
            # Strike proximity filter
            too_far_otm = abs(strike - atm) > 150
            
            # Daily cap for WATCH
            watch_limit_hit = baseline.get("watch_today", 0) >= MAX_WATCH_PER_DAY
            
            if not conflicted and not too_far_otm and not watch_limit_hit:
                send_telegram(
                    f"👁 *BN WATCH ALERT*\n"
                    f"{strike} {opt}\n"
                    f"OI +{oi_pct:.0f}%\n"
                    f"Not actionable yet - monitoring\n"
                    f"Spot: {spot:.0f}  ATM: {atm}"
                )
                baseline["watch_today"] = baseline.get("watch_today", 0) + 1
                updated = True
            elif watch_limit_hit and DEBUG_MODE:
                print(f"⏸ WATCH suppressed: daily limit ({MAX_WATCH_PER_DAY})")
            elif conflicted and DEBUG_MODE:
                print(f"⏸ WATCH suppressed: conflicted at {strike}")
            
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

        # ================= EXECUTION =================
        if entry["state"] == "EXECUTED":
            continue

        # Use dynamic premium tolerance
        is_aggressive_writing = (oi_pct >= EXEC_OI_PCT) and (ltp_change_pct <= PREMIUM_TOLERANCE)

        if is_aggressive_writing and entry["state"] == "WATCH":
            # Bank Nifty specific: wait until 10:15 AM
            if not after_bn_entry_time():
                continue

            # Bank Nifty specific: spot move requirement
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
                opp_entry["decline_streak"] = 0
                updated = True
                print(f"⚠️ BN Rejected: opposite not declining ({opp_decline_pct:+.1f}%)")
                continue

            # Update decline streak
            opp_entry["decline_streak"] = opp_entry.get("decline_streak", 0) + 1
            updated = True

            print(f"✓ BN Covering detected: {opp_opt} {opp_decline_pct:.1f}% ({opp_prev_oi} → {opp_current_oi})")
            print(f"  Decline streak: {opp_entry['decline_streak']} scans")

            # Prepare for conviction scoring
            buildup_data = {
                "strike": strike,
                "opt_type": opt,
                "oi_pct": oi_pct,
                "ltp_change_pct": ltp_change_pct,
                "vol_multiplier": vol_multiplier,
                "opp_decline_pct": opp_decline_pct,
                "decline_streak": opp_entry["decline_streak"],
                "buildup_time_mins": calculate_buildup_time(entry, current_time),
                "scan_count": entry.get("scan_count", 1),
                "spot_move_pct": spot_move_pct,
                "exec_threshold": EXEC_OI_PCT,
                "days_to_expiry": days_to_expiry
            }

            # Calculate conviction score
            score, tier, emoji, score_details, components = calculate_conviction_score(
                buildup_data, atm, baseline.get("day_open"), spot, strike_oi_changes
            )

            print(f"📊 BN Conviction: {score} - {tier}")
            for detail in score_details:
                print(f"   {detail}")

            # Check minimum score threshold (dynamic by expiry)
            if score < MIN_CONVICTION_SCORE:
                print(f"⚠️ BN Score {score} below threshold {MIN_CONVICTION_SCORE}")
                continue

            # Add score metadata
            buildup_data.update({
                "conviction_score": score,
                "tier": tier,
                "emoji": emoji,
                "score_details": score_details,
                **components
            })

            # Collect buildups
            if opt == "CE":
                ce_buildups.append(buildup_data)
            else:
                pe_buildups.append(buildup_data)

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

    # Process and send alerts with priority filtering
    all_buildups = []
    
    if ce_buildups:
        for buildup in ce_buildups:
            buildup["trade_strike"] = atm - 100
            buildup["trade_opt"] = "PE"
            all_buildups.append(buildup)
    
    if pe_buildups:
        for buildup in pe_buildups:
            buildup["trade_strike"] = atm + 100
            buildup["trade_opt"] = "CE"
            all_buildups.append(buildup)

    # Sort by score
    all_buildups_sorted = sorted(all_buildups, key=lambda x: x["conviction_score"], reverse=True)

    # Send alerts with daily limit
    for buildup in all_buildups_sorted:
        should_send, reason = should_send_signal(baseline, buildup["conviction_score"])
        
        if should_send:
            score_breakdown = "\n".join(f"  {d}" for d in buildup["score_details"])
            is_replacement = "REPLACES" in reason

            msg = (
                f"{buildup['emoji']} *BN EXECUTION - {buildup['opt_type']} BUILDUP*\n"
                f"*Tier: {buildup['tier']} | Score: {buildup['conviction_score']}/190*\n"
            )
            
            if is_replacement:
                msg += f"*(Replaced lower signal)*\n"
            
            msg += (
                f"\n*Action: Buy {buildup['trade_strike']} {buildup['trade_opt']}*\n\n"
                f"📊 *Score Breakdown:*\n{score_breakdown}\n\n"
                f"OI: +{buildup['oi_pct']:.0f}% | Opp: {buildup['opp_decline_pct']:.1f}%\n"
                f"Premium: {buildup['ltp_change_pct']:+.1f}%\n"
                f"Spot: {spot:.0f} | Move: {buildup['spot_move_pct']:.2f}%\n"
                f"Signals today: {baseline.get('signals_today', 0) + 1}/{MAX_SIGNALS_PER_DAY}"
            )
            
            send_telegram(msg)
            record_signal(baseline, buildup)
            
            # Log for calibration
            buildup["signal_type"] = "EXECUTION"
            log_conviction_score(buildup)
            
            updated = True
            print(f"✅ BN Signal sent: {buildup['strike']} {buildup['opt_type']} (Score: {buildup['conviction_score']})")
        else:
            print(f"⏸ BN Signal skipped: {buildup['strike']} {buildup['opt_type']} - {reason}")
            
            # Still log skipped signals
            buildup_copy = buildup.copy()
            buildup_copy["skipped"] = True
            buildup_copy["skip_reason"] = reason
            buildup_copy["signal_type"] = "EXECUTION_SKIPPED"
            log_conviction_score(buildup_copy)

    # Save baseline
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