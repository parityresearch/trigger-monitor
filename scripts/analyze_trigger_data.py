#!/usr/bin/env python3
"""
ABS Trigger Breach Analysis
============================
Logistic regression + vintage cohort analysis on SEC 10-D filing data.
Intended output: findings for an open-source structured finance article.

Usage:
  python3 scripts/analyze_trigger_data.py \
      --csv out/trigger_training_data.csv \
      --out out/analysis/
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.pipeline import Pipeline

# --------------------------------------------------------------------------- #
# Classifier helpers
# --------------------------------------------------------------------------- #

SUBPRIME_TOKENS = ["drive auto", "santander drive", "americredit", "bridgecrest"]
PRIME_TOKENS    = ["mercedes-benz", "toyota", "honda", "hyundai", "nissan", "bmw",
                   "ally", "capital one", "world omni", "ford credit", "gm financial", "carmax"]

def credit_quality(deal_id: str) -> str:
    d = deal_id.lower()
    if any(t in d for t in SUBPRIME_TOKENS):
        return "Subprime"
    if any(t in d for t in PRIME_TOKENS):
        return "Prime"
    return "Other"

def vintage(deal_id: str) -> str | None:
    m = re.search(r"\b(20\d{2})-[A-Z0-9]", deal_id)
    return m.group(1) if m else None

def issuer(deal_id: str) -> str:
    for tok in ["Santander Drive", "Drive Auto", "AmeriCredit", "Bridgecrest",
                "Mercedes-Benz", "Toyota", "Honda", "Hyundai", "Nissan",
                "BMW", "Ally", "Capital One", "World Omni", "Ford Credit",
                "GM Financial", "CarMax"]:
        if tok.lower() in deal_id.lower():
            return tok
    return "Other"


# --------------------------------------------------------------------------- #
# Load + clean
# --------------------------------------------------------------------------- #

def load(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["credit_quality"] = df["deal_id"].apply(credit_quality)
    df["vintage"]        = df["deal_id"].apply(vintage)
    df["issuer"]         = df["deal_id"].apply(issuer)
    df["target_breach"]  = df["target_breach"].astype(float).round().astype(int)
    df["period_end"]     = pd.to_datetime(df["period_end"], errors="coerce")

    # Drop rows with missing features
    features = ["cushion", "trend3m", "vol6m", "macro"]
    df = df.dropna(subset=features + ["target_breach"])
    df = df[df["credit_quality"].isin(["Prime", "Subprime"])]

    # Exclude issuers whose Ex-99.1 report format is incompatible with the table parser,
    # producing implausible cushion values (e.g. dq60 parsed as section-header number):
    #   - Capital One: colspan artifacts → cushion ≈ −1233%  (2 deals, ~109 rows)
    #   - Mercedes-Benz: similar table format → cushion ≈ −40% to −55% for prime trusts
    #     that empirically never breach (Honda/Hyundai/Toyota/Nissan show +96–98% cushion).
    EXCLUDE_ISSUERS = ["Capital One", "Mercedes-Benz"]
    for iss in EXCLUDE_ISSUERS:
        df = df[~df["deal_id"].str.contains(iss, case=False, na=False)]

    # Sanity filter: drop rows with |cushion| > 2 (200% of threshold is physically impossible)
    df = df[df["cushion"].abs() <= 2.0]

    return df


# --------------------------------------------------------------------------- #
# Model
# --------------------------------------------------------------------------- #

FEATURES = ["cushion", "trend3m", "vol6m", "macro"]
FEATURE_LABELS = {
    "cushion":  "OC/Trigger Cushion",
    "trend3m":  "3-Month DQ Trend",
    "vol6m":    "6-Month DQ Volatility",
    "macro":    "Macro Regime Percentile",
}

def train_model(df: pd.DataFrame) -> dict:
    X = df[FEATURES].values
    y = df["target_breach"].values

    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LogisticRegression(C=1.0, max_iter=1000, random_state=42)),
    ])

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    aucs = cross_val_score(pipe, X, y, cv=cv, scoring="roc_auc")

    pipe.fit(X, y)
    coefs = pipe.named_steps["lr"].coef_[0]
    scaler = pipe.named_steps["scaler"]

    # Standardised coefficients (effect of 1-SD change)
    feature_importance = {
        FEATURES[i]: float(coefs[i] * scaler.scale_[i])
        for i in range(len(FEATURES))
    }

    y_prob = pipe.predict_proba(X)[:, 1]
    overall_auc = roc_auc_score(y, y_prob)

    return {
        "pipeline": pipe,
        "cv_auc_mean": float(aucs.mean()),
        "cv_auc_std":  float(aucs.std()),
        "in_sample_auc": overall_auc,
        "feature_importance": feature_importance,
        "coefs_raw": {FEATURES[i]: float(coefs[i]) for i in range(len(FEATURES))},
    }


def export_logit_config(pipe: Pipeline, path: str) -> None:
    scaler = pipe.named_steps["scaler"]
    lr = pipe.named_steps["lr"]
    cfg = {
        "weights": {FEATURES[i]: float(lr.coef_[0][i]) for i in range(len(FEATURES))},
        "intercept": float(lr.intercept_[0]),
        "scaler": {
            "means": {FEATURES[i]: float(scaler.mean_[i]) for i in range(len(FEATURES))},
            "stds": {FEATURES[i]: float(scaler.scale_[i]) for i in range(len(FEATURES))},
        },
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def subgroup_aucs(df: pd.DataFrame, pipe) -> dict:
    out = {}
    for grp, sub in df.groupby("credit_quality"):
        if sub["target_breach"].nunique() < 2:
            continue
        X = sub[FEATURES].values
        y = sub["target_breach"].values
        y_prob = pipe.predict_proba(X)[:, 1]
        out[grp] = roc_auc_score(y, y_prob)
    return out


# --------------------------------------------------------------------------- #
# Vintage cohort
# --------------------------------------------------------------------------- #

def vintage_cohort(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (vin, cq), grp in df.groupby(["vintage", "credit_quality"]):
        if vin is None or pd.isna(vin):
            continue
        n = len(grp)
        breach_rate = grp["target_breach"].mean()
        avg_cushion = grp["cushion"].mean()
        avg_trend   = grp["trend3m"].mean()
        rows.append({
            "vintage":        vin,
            "credit_quality": cq,
            "n_obs":          n,
            "breach_rate":    breach_rate,
            "avg_cushion":    avg_cushion,
            "avg_trend3m":    avg_trend,
        })
    return pd.DataFrame(rows).sort_values(["vintage", "credit_quality"])


# --------------------------------------------------------------------------- #
# Issuer-level summary
# --------------------------------------------------------------------------- #

def issuer_summary(df: pd.DataFrame, pipe) -> pd.DataFrame:
    rows = []
    for iss, grp in df.groupby("issuer"):
        cq = grp["credit_quality"].iloc[0]
        n = len(grp)
        breach_rate = grp["target_breach"].mean()
        avg_cushion = grp["cushion"].mean()
        avg_trend   = grp["trend3m"].mean()
        avg_vol     = grp["vol6m"].mean()
        # Predicted breach probability (mean)
        pred_prob = pipe.predict_proba(grp[FEATURES].values)[:, 1].mean()
        rows.append({
            "issuer":         iss,
            "credit_quality": cq,
            "n_obs":          n,
            "breach_rate":    round(breach_rate, 4),
            "avg_cushion":    round(avg_cushion, 4),
            "avg_trend3m":    round(avg_trend, 6),
            "avg_vol6m":      round(avg_vol, 6),
            "pred_breach_prob": round(pred_prob, 4),
        })
    return pd.DataFrame(rows).sort_values("pred_breach_prob", ascending=False)


# --------------------------------------------------------------------------- #
# Key findings narrative
# --------------------------------------------------------------------------- #

def narrative(model_result: dict, cohort: pd.DataFrame, issuer_df: pd.DataFrame,
              subgroup: dict, df: pd.DataFrame) -> str:
    fi = model_result["feature_importance"]
    ranked = sorted(fi.items(), key=lambda x: abs(x[1]), reverse=True)

    prime_breach  = df[df.credit_quality=="Prime"]["target_breach"].mean()
    sub_breach    = df[df.credit_quality=="Subprime"]["target_breach"].mean()
    prime_cushion = df[df.credit_quality=="Prime"]["cushion"].mean()
    sub_cushion   = df[df.credit_quality=="Subprime"]["cushion"].mean()

    # Vintage story: 2021/2022 vs 2023/2024 for subprime
    vc_sub = cohort[cohort.credit_quality=="Subprime"].set_index("vintage")
    early_vins = [v for v in ["2021","2022"] if v in vc_sub.index]
    late_vins  = [v for v in ["2023","2024"] if v in vc_sub.index]
    early_br = vc_sub.loc[early_vins, "breach_rate"].mean() if early_vins else None
    late_br  = vc_sub.loc[late_vins,  "breach_rate"].mean() if late_vins  else None

    lines = [
        "=" * 70,
        "  ABS Trigger Breach Analysis — Key Findings",
        "=" * 70,
        "",
        f"Dataset: {len(df):,} observations | "
        f"{df.deal_id.nunique()} deals | "
        f"{df.credit_quality.value_counts().to_dict()}",
        "",
        "── MODEL PERFORMANCE ──────────────────────────────────────────────",
        f"  Cross-val AUC:   {model_result['cv_auc_mean']:.3f} ± {model_result['cv_auc_std']:.3f}",
        f"  In-sample AUC:   {model_result['in_sample_auc']:.3f}",
        f"  AUC by segment:  Prime {subgroup.get('Prime',0):.3f}  |  Subprime {subgroup.get('Subprime',0):.3f}",
        "",
        "── FEATURE IMPORTANCE (standardised logit coefficients) ───────────",
    ]
    for feat, coef in ranked:
        direction = "↑ risk" if coef > 0 else "↓ risk"
        lines.append(f"  {FEATURE_LABELS[feat]:<35}  {coef:+.3f}  {direction}")

    lines += [
        "",
        "── PRIME vs SUBPRIME ───────────────────────────────────────────────",
        f"  Observed breach rate:  Prime {prime_breach:.1%}  |  Subprime {sub_breach:.1%}",
        f"  Avg OC cushion:        Prime {prime_cushion:.1%}  |  Subprime {sub_cushion:.1%}",
        "",
        "── VINTAGE COHORT (Subprime) ────────────────────────────────────────",
    ]
    for _, row in vc_sub.iterrows():
        vin = row.name
        lines.append(
            f"  {vin}  breach_rate={row['breach_rate']:.1%}  "
            f"avg_cushion={row['avg_cushion']:.1%}  n={int(row['n_obs'])}"
        )
    if early_br is not None and late_br is not None:
        lines.append(f"\n  2021-22 vs 2023-24 avg breach rate: {early_br:.1%} vs {late_br:.1%}")

    lines += [
        "",
        "── TOP 5 RISKIEST ISSUERS (by predicted breach prob) ───────────────",
    ]
    for _, row in issuer_df.head(5).iterrows():
        lines.append(
            f"  {row['issuer']:<22} [{row['credit_quality']:<8}]  "
            f"pred={row['pred_breach_prob']:.1%}  actual={row['breach_rate']:.1%}  "
            f"cushion={row['avg_cushion']:.1%}"
        )

    lines += ["", "=" * 70]
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="out/trigger_training_data.csv")
    ap.add_argument("--out", default="out/analysis")
    ap.add_argument("--logit-out", default=None, help="Path to write builder-ready logistic config JSON")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print("Loading data...")
    df = load(args.csv)
    print(f"  {len(df):,} rows | {df.deal_id.nunique()} deals | "
          f"Prime={( df.credit_quality=='Prime').sum()} Subprime={(df.credit_quality=='Subprime').sum()}")
    print(f"  Overall breach rate: {df.target_breach.mean():.1%}")

    print("\nTraining logistic regression...")
    result = train_model(df)
    print(f"  CV AUC: {result['cv_auc_mean']:.3f} ± {result['cv_auc_std']:.3f}")

    subgroup = subgroup_aucs(df, result["pipeline"])
    cohort   = vintage_cohort(df)
    iss_df   = issuer_summary(df, result["pipeline"])

    # ── Save outputs ──────────────────────────────────────────────────────── #
    cohort.to_csv(f"{args.out}/vintage_cohort.csv", index=False)
    iss_df.to_csv(f"{args.out}/issuer_summary.csv", index=False)

    model_json = {
        "cv_auc_mean":      result["cv_auc_mean"],
        "cv_auc_std":       result["cv_auc_std"],
        "in_sample_auc":    result["in_sample_auc"],
        "feature_importance": result["feature_importance"],
        "subgroup_auc":     subgroup,
    }
    with open(f"{args.out}/model_results.json", "w") as f:
        json.dump(model_json, f, indent=2)

    logit_out = args.logit_out or f"{args.out}/logit_config.json"
    export_logit_config(result["pipeline"], logit_out)

    text = narrative(result, cohort, iss_df, subgroup, df)
    print("\n" + text)
    with open(f"{args.out}/findings.txt", "w") as f:
        f.write(text)

    # ── Per-deal predicted risk (for article table) ───────────────────────── #
    df["pred_prob"] = result["pipeline"].predict_proba(df[FEATURES].values)[:, 1]
    deal_risk = (
        df.groupby(["deal_id", "credit_quality", "vintage", "issuer"])
        .agg(
            n_obs        =("target_breach", "count"),
            breach_rate  =("target_breach", "mean"),
            pred_prob    =("pred_prob",      "mean"),
            avg_cushion  =("cushion",        "mean"),
            avg_trend3m  =("trend3m",        "mean"),
        )
        .reset_index()
        .sort_values("pred_prob", ascending=False)
    )
    deal_risk.to_csv(f"{args.out}/deal_risk_table.csv", index=False)

    print(f"\nOutputs written to {args.out}/")
    print("  vintage_cohort.csv  — breach rate by vintage x credit quality")
    print("  issuer_summary.csv  — risk profile per issuer")
    print("  deal_risk_table.csv — per-deal predicted breach probability")
    print("  model_results.json  — model coefficients and AUC")
    print("  logit_config.json   — builder-ready logistic score config")
    print("  findings.txt        — narrative summary")


if __name__ == "__main__":
    main()
