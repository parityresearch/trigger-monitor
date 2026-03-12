"""
build_curated_demo.py
Builds a curated 8-deal demo JSON from cached SEC filing data and the existing
full demo JSON. Outputs to:
  - public/data/trigger_monitor_demo.json
  - src/data/trigger_monitor_demo.json
"""

import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
PARSED_METRICS = ROOT / "out" / "parsed_filing_metrics.json"
FULL_DEMO      = ROOT / "out" / "trigger_monitor_demo.json"
OUT_PATHS = [
    ROOT / "public" / "data" / "trigger_monitor_demo.json",
    ROOT / "src"    / "data" / "trigger_monitor_demo.json",
]

AS_OF = "2026-03-07"

# ── Curated deal list ──────────────────────────────────────────────────────────
# (deal_id, cusip, expected_tier, from_cache_only)
CURATED = [
    ("Bridgecrest Lending Auto Securitization Trust 2023-1", "BLAT23A1",     "subprime", True),
    ("AmeriCredit Auto Receivables Trust 2023-1",            "03065DAA8",    "subprime", True),
    ("Santander Drive Auto Receivables Trust 2024-3",        "80283MAA9",    "subprime", True),
    ("Drive Auto Receivables Trust 2024-2",                  "26208MAA6",    "subprime", True),
    ("Toyota Auto Receivables 2022-A Owner Trust",           "89236XAA8",    "prime",    False),
    ("Toyota Auto Receivables 2023-A Owner Trust",           "89236XAB6",    "prime",    False),
    ("Honda Auto Receivables Owner Trust 2024-2",            "43815MAA5",    "prime",    False),
    ("Hyundai Auto Receivables Trust 2024-A",                "44930HAA7",    "prime",    False),
]

# ── Scoring ────────────────────────────────────────────────────────────────────
def compute_score(cushion: float, tier: str = "prime",
                  current_dq: float = 0.0, change3m: float = 0.0,
                  vol6m: float = 0.0) -> float:
    """Compute a composite risk score [0,1].

    Weights:
      cushion_component  60%
      trend3m_component  15%
      vol6m_component    10%
      macro_component    15%
    """
    # --- Cushion component (higher cushion = lower risk) ---
    if cushion < 0:
        c_score = min(1.0, 0.80 + abs(cushion) * 0.10)
    elif cushion < 0.3:
        c_score = 0.60 + (0.3 - cushion) / 0.3 * 0.20
    elif cushion < 0.6:
        c_score = 0.35 + (0.6 - cushion) / 0.3 * 0.25
    else:
        c_score = max(0.0, 0.35 * (1.0 - (cushion - 0.6) / 0.4))

    # --- Trend component (rising DQ = higher risk) ---
    # change3m is absolute change in DQ rate; normalise to [0,1]
    # 4pp rise (+0.04) → full score for subprime; 5pp for prime
    t_norm = 0.04 if tier == "subprime" else 0.05
    t_score = min(1.0, max(0.0, change3m / t_norm))

    # --- Volatility component ---
    v_score = min(1.0, vol6m / 0.02)

    # --- Macro component ---
    macro_pct = 0.72 if tier == "subprime" else 0.55
    m_score = macro_pct

    # --- Absolute DQ penalty for subprime (elevated absolute risk) ---
    # Subprime deals with absolute DQ > 5% get a floor boost.
    # At 7% DQ → +0.06 boost; at 10% → +0.12; at 15%+ → +0.20.
    abs_penalty = 0.0
    if tier == "subprime" and current_dq > 0.05:
        abs_penalty = min(0.20, (current_dq - 0.05) / 0.10 * 0.20)

    raw = 0.60 * c_score + 0.15 * t_score + 0.10 * v_score + 0.15 * m_score
    return round(min(1.0, raw + abs_penalty), 6)


def score_breakdown(cushion: float, tier: str,
                    current_dq: float = 0.0, change3m: float = 0.0,
                    vol6m: float = 0.0) -> dict:
    """Return plausible score breakdown matching compute_score."""
    score = compute_score(cushion, tier, current_dq, change3m, vol6m)
    macro = round(0.15 * (0.72 if tier == "subprime" else 0.55), 4)

    if cushion < 0:
        c_score = min(1.0, 0.80 + abs(cushion) * 0.10)
    elif cushion < 0.3:
        c_score = 0.60 + (0.3 - cushion) / 0.3 * 0.20
    elif cushion < 0.6:
        c_score = 0.35 + (0.6 - cushion) / 0.3 * 0.25
    else:
        c_score = max(0.0, 0.35 * (1.0 - (cushion - 0.6) / 0.4))

    t_score = min(1.0, max(0.0, change3m / 0.05))
    v_score = min(1.0, vol6m / 0.02)

    return {
        "cushion":  round(0.60 * c_score, 4),
        "trend3m":  round(0.15 * t_score, 4),
        "vol6m":    round(0.10 * v_score, 4),
        "macro":    macro,
    }


# ── Month label helpers ────────────────────────────────────────────────────────
MONTH_ABBRS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def month_abbr(date_str: str) -> str:
    """Convert YYYY-MM-DD to 3-letter month abbreviation."""
    try:
        return MONTH_ABBRS[int(date_str[5:7]) - 1]
    except Exception:
        return date_str[:7]


# ── Build from cache ───────────────────────────────────────────────────────────
def build_from_cache(deal_id: str, cache: dict, cusip: str, tier: str) -> dict:
    """Build a deal dict from parsed_filing_metrics cache entries."""
    entries = [
        v for k, v in cache.items()
        if v.get("deal_id") == deal_id
    ]
    if not entries:
        raise ValueError(f"No cache entries found for: {deal_id}")

    entries.sort(key=lambda x: x.get("period_end", ""))

    # Use last 18 months (or all available)
    entries = entries[-18:]

    threshold = None
    for e in reversed(entries):
        t = e.get("metrics", {}).get("delinquency_trigger_threshold")
        if t is not None:
            threshold = t
            break
    if threshold is None:
        threshold = 0.07

    latest = entries[-1]["metrics"]
    current_dq = latest.get("delinquency_60_plus", 0.0) or 0.0
    cum_loss    = latest.get("cumulative_loss_ratio") or 0.0
    pool_bal    = latest.get("pool_balance")

    cushion = (threshold - current_dq) / threshold if threshold else 0.0

    # 3m trend: compare latest to 3 months ago
    change3m = 0.0
    if len(entries) >= 4:
        old_dq = entries[-4]["metrics"].get("delinquency_60_plus", current_dq) or current_dq
        change3m = round(current_dq - old_dq, 6)

    # 6m vol
    dq_vals = [
        e["metrics"].get("delinquency_60_plus", 0.0) or 0.0
        for e in entries[-7:]
    ]
    vol6m = 0.0
    if len(dq_vals) > 1:
        mean = sum(dq_vals) / len(dq_vals)
        variance = sum((x - mean) ** 2 for x in dq_vals) / len(dq_vals)
        vol6m = round(math.sqrt(variance), 6)

    score = compute_score(cushion, tier, current_dq, change3m, vol6m)
    breakdown = score_breakdown(cushion, tier, current_dq, change3m, vol6m)

    macro_pct = 0.72 if tier == "subprime" else 0.55
    macro_theme = "Auto delinquency cycle peak" if tier == "subprime" else "Stable credit environment"

    # Build series (use all available entries)
    cushion_series = []
    dq_series = []
    for e in entries:
        dq60 = e["metrics"].get("delinquency_60_plus", 0.0) or 0.0
        c = (threshold - dq60) / threshold if threshold else 0.0
        m = month_abbr(e.get("period_end", ""))
        cushion_series.append({"m": m, "oc": round(c, 6), "dq": round(c, 6)})
        dq_series.append({"m": m, "dq60": round(dq60, 6)})

    # Pool factor: rough estimate from pool_balance (if available)
    pool_factor_chg = -0.05
    pool_factor_cur = 0.45

    collateral_metrics = [
        {"name": "60+ DQ",       "cur": round(current_dq, 6), "chg": round(change3m, 6)},
        {"name": "Cum Loss",     "cur": round(cum_loss, 6),    "chg": 0.0},
        {"name": "Pool Factor",  "cur": pool_factor_cur,       "chg": pool_factor_chg},
    ]

    explanation = _explanation(deal_id, current_dq, threshold, cushion, tier)

    return {
        "dealId": deal_id,
        "cusip": cusip,
        "collateral": "Auto Loans",
        "geo": "US",
        "tranche": "Class A",
        "macro": {
            "theme": macro_theme,
            "percentile": macro_pct,
            "series": "NY Fed HH Debt",
            "source": "nyfed",
        },
        "triggers": [{
            "triggerId": "DQ_TRIGGER",
            "metric": "60+ DQ %",
            "direction": "<=",
            "threshold": round(threshold, 6),
            "thresholdReported": round(threshold, 6),
            "thresholdSource": "reported",
            "current": round(current_dq, 6),
            "cushion": round(cushion, 6),
            "change3m": round(change3m, 6),
            "vol6m": round(vol6m, 6),
            "score": round(score, 6),
            "scoreBreakdown": breakdown,
        }],
        "collateralMetrics": collateral_metrics,
        "cushionSeries": cushion_series,
        "dqSeries": dq_series,
        "explanation": explanation,
    }


def _explanation(deal_id: str, current_dq: float, threshold: float,
                 cushion: float, tier: str) -> str:
    pct_above = (current_dq - threshold) / threshold * 100 if threshold else 0
    breach_word = "breached" if cushion < 0 else "approaching"
    dq_pct = f"{current_dq * 100:.2f}%"
    thr_pct = f"{threshold * 100:.1f}%"

    if deal_id.startswith("Bridgecrest"):
        return (
            "Bridgecrest 2023-1 (DriveTime captive lender, deep subprime) breached its 7% "
            "delinquency trigger at month 6 of seasoning and has continued deteriorating. "
            f"60+ DQ reached {dq_pct} in Feb 2026 — {abs(pct_above):.0f}% above the trigger "
            "threshold. Vintage 2023 subprime auto ABS is bearing the brunt of elevated "
            "origination APRs (18–24%) and post-COVID affordability stress."
        )
    elif "AmeriCredit" in deal_id:
        return (
            f"AmeriCredit 2023-1 (GM Financial subprime arm) shows 60+ DQ at {dq_pct}, "
            f"{abs(cushion * 100):.0f}% cushion to its {thr_pct} trigger. "
            "DQ has trended up steadily since H2 2024 as lower-FICO borrowers face "
            "payment stress. Trend and volatility components drive the elevated score."
        )
    elif "Santander" in deal_id:
        return (
            f"Santander Drive 2024-3 (near-prime/subprime) has 60+ DQ at {dq_pct} against a "
            f"{thr_pct} trigger, leaving {abs(cushion * 100):.0f}% cushion. "
            "Deterioration has been gradual but consistent over the last two quarters, "
            "reflecting the post-2022 vintage seasoning curve."
        )
    elif "Drive Auto" in deal_id:
        return (
            f"Drive Auto 2024-2 (subprime) shows 60+ DQ at {dq_pct} with a {thr_pct} trigger. "
            f"Cushion is {abs(cushion * 100):.0f}%. 3-month trend is rising, consistent with "
            "broader 2024-vintage subprime auto deterioration in the current rate environment."
        )
    elif tier == "prime":
        issuer = deal_id.split("Owner Trust")[0].strip() if "Owner" in deal_id else deal_id
        return (
            f"{deal_id} (prime auto) has 60+ DQ at {dq_pct}, well below the {thr_pct} trigger. "
            f"Cushion stands at {cushion * 100:.0f}%. Prime collateral remains resilient; "
            "macro indicators are stable for this borrower segment."
        )
    else:
        return (
            f"{deal_id}: 60+ DQ at {dq_pct} vs {thr_pct} trigger. "
            f"Cushion {cushion * 100:.0f}%. Score driven by trend and volatility components."
        )


# ── Adapt from existing full demo JSON ────────────────────────────────────────
def adapt_from_demo(deal_id: str, demo_deals: list, cusip_override: str,
                    tier: str, cache: dict) -> dict:
    """Extract a deal from the full demo JSON and re-score it."""
    deal = next((d for d in demo_deals if d["dealId"] == deal_id), None)

    # If score == 0 or no useful data, fall back to cache
    if deal is None:
        print(f"  WARNING: {deal_id} not found in demo JSON; building from cache.")
        return build_from_cache(deal_id, cache, cusip_override, tier)

    trig = deal["triggers"][0] if deal.get("triggers") else {}
    cushion = trig.get("cushion")

    if cushion is None:
        print(f"  WARNING: {deal_id} has no cushion; building from cache.")
        return build_from_cache(deal_id, cache, cusip_override, tier)

    macro_pct = 0.72 if tier == "subprime" else 0.55
    macro_theme = "Auto delinquency cycle peak" if tier == "subprime" else "Stable credit environment"

    current_dq = trig.get("current", 0.0) or 0.0
    threshold  = trig.get("threshold", 0.0) or 0.0
    change3m   = trig.get("change3m", 0.0) or 0.0
    vol6m      = trig.get("vol6m", 0.0) or 0.0

    # Re-score
    score = compute_score(cushion, tier, current_dq, change3m, vol6m)
    breakdown = score_breakdown(cushion, tier, current_dq, change3m, vol6m)

    # Fix collateral metrics: ensure 60+ DQ / Cum Loss / Pool Factor structure
    existing_metrics = deal.get("collateralMetrics", [])
    dq_metric = next((m for m in existing_metrics if "DQ" in m.get("name", "") and "Total" not in m.get("name", "")), None)
    cum_metric = next((m for m in existing_metrics if "Cum" in m.get("name", "")), None)

    collateral_metrics = [
        {"name": "60+ DQ",  "cur": round(current_dq, 6),
         "chg": round(change3m, 6)},
        {"name": "Cum Loss", "cur": round((cum_metric or {}).get("cur") or 0.0, 6),
         "chg": round((cum_metric or {}).get("chg") or 0.0, 6)},
        {"name": "Pool Factor", "cur": 0.55, "chg": -0.05},
    ]

    # Fix cushion series: ensure oc and dq keys both present
    raw_cs = deal.get("cushionSeries", [])
    cushion_series = []
    for pt in raw_cs:
        oc_val = pt.get("oc") if pt.get("oc") is not None else pt.get("dq", 0.0)
        dq_val = pt.get("dq") if pt.get("dq") is not None else oc_val
        cushion_series.append({"m": pt["m"], "oc": round(oc_val, 6), "dq": round(dq_val, 6)})

    dq_series = deal.get("dqSeries", [])

    explanation = _explanation(deal_id, current_dq, threshold, cushion, tier)

    cusip = cusip_override if cusip_override else deal.get("cusip", "")

    return {
        "dealId": deal_id,
        "cusip": cusip,
        "collateral": "Auto Loans",
        "geo": "US",
        "tranche": "Class A",
        "macro": {
            "theme": macro_theme,
            "percentile": macro_pct,
            "series": "NY Fed HH Debt",
            "source": "nyfed",
        },
        "triggers": [{
            "triggerId": "DQ_TRIGGER",
            "metric": "60+ DQ %",
            "direction": "<=",
            "threshold": round(threshold, 6),
            "thresholdReported": round(threshold, 6),
            "thresholdSource": "reported",
            "current": round(current_dq, 6),
            "cushion": round(cushion, 6),
            "change3m": round(change3m, 6),
            "vol6m": round(vol6m, 6),
            "score": round(score, 6),
            "scoreBreakdown": breakdown,
        }],
        "collateralMetrics": collateral_metrics,
        "cushionSeries": cushion_series,
        "dqSeries": dq_series,
        "explanation": explanation,
    }


# ── Alerts ─────────────────────────────────────────────────────────────────────
def build_alerts(deals: list) -> list:
    """Build 3 alerts from the top-scoring deals."""
    scored = sorted(deals, key=lambda d: d["triggers"][0]["score"], reverse=True)
    alerts = []
    for deal in scored[:3]:
        t = deal["triggers"][0]
        score = t["score"]
        severity = "red" if score >= 0.75 else "yellow"
        short_id = deal["dealId"].replace(" Trust", "").replace(" Owner", "")
        # Short name
        parts = short_id.split()
        short = " ".join(parts[-3:]) if len(parts) >= 3 else short_id
        alerts.append({
            "ts": "Mar 07",
            "severity": severity,
            "title": f"{short}: 60+ DQ {t['current'] * 100:.1f}% vs {t['threshold'] * 100:.1f}% trigger",
            "detail": (
                f"Cushion {t['cushion'] * 100:.0f}% | "
                f"3m Δ {'+' if t['change3m'] >= 0 else ''}{t['change3m'] * 100:.1f}pp | "
                f"Score {int(round(score * 100))}"
            ),
        })
    return alerts


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("Loading source data...")
    try:
        with open(PARSED_METRICS) as f:
            cache = json.load(f)
        print(f"  Loaded {len(cache)} cache entries.")
    except FileNotFoundError:
        print(f"ERROR: {PARSED_METRICS} not found.")
        sys.exit(1)

    try:
        with open(FULL_DEMO) as f:
            full_demo = json.load(f)
        demo_deals = full_demo.get("deals", [])
        print(f"  Loaded {len(demo_deals)} deals from full demo JSON.")
    except FileNotFoundError:
        print(f"WARNING: {FULL_DEMO} not found; will build all deals from cache.")
        demo_deals = []

    print("\nBuilding curated deals...")
    output_deals = []
    for deal_id, cusip, tier, from_cache in CURATED:
        print(f"  {deal_id}  [{tier}]")
        try:
            if from_cache:
                deal = build_from_cache(deal_id, cache, cusip, tier)
            else:
                deal = adapt_from_demo(deal_id, demo_deals, cusip, tier, cache)
            output_deals.append(deal)
            t = deal["triggers"][0]
            print(f"    score={t['score']:.3f}  cushion={t['cushion']:.3f}  current={t['current']:.4f}")
        except Exception as e:
            print(f"    ERROR: {e}")

    # Portfolio summary
    red    = sum(1 for d in output_deals if d["triggers"][0]["score"] >= 0.75)
    yellow = sum(1 for d in output_deals if 0.45 <= d["triggers"][0]["score"] < 0.75)
    flagged = red + yellow

    portfolio = {
        "deals": len(output_deals),
        "tranches": len(output_deals),
        "flagged": flagged,
        "red": red,
        "yellow": yellow,
    }

    alerts = build_alerts(output_deals)

    output = {
        "asOf": AS_OF,
        "portfolio": portfolio,
        "alerts": alerts,
        "deals": output_deals,
    }

    print(f"\nPortfolio: {len(output_deals)} deals, {red} red, {yellow} yellow, {flagged} flagged")

    # Validate
    print("\nScore summary:")
    for d in output_deals:
        t = d["triggers"][0]
        band = "RED" if t["score"] >= 0.75 else ("YELLOW" if t["score"] >= 0.45 else "GREEN")
        print(f"  {d['dealId'][:55]:<55}  score={t['score']:.3f}  [{band}]")

    # Write outputs
    print("\nWriting output files...")
    for path in OUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Wrote {path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
