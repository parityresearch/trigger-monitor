# Trigger Monitor - Personal Guide

This is a plain-English guide to how the demo tool works, how scores are computed, what the labels mean, and what other indicators/approaches exist in the market. It is written for internal use.

---

## 1) What the tool is (in one paragraph)

Trigger Monitor reads SEC 10-D Exhibit 99.1 tables, extracts performance metrics (delinquencies, cumulative loss, pool balance), computes trigger cushions and short-term trends, and assigns an explainable risk score for each trigger. It ranks the most at-risk triggers and provides a deal report with context, trends, and a short explanation.

---

## 2) Data inputs and pipeline

**Primary source**
- SEC 10-D filings, specifically Exhibit 99.1 tables

**How it finds the data**
- The script pulls recent 10-D filings for each deal CIK.
- It finds Exhibit 99.1 in the filing index and reads the tables.
- It searches tables for known metric patterns (e.g., total delinquency, 60+ day delinquency, cumulative loss ratio, pool balance).

**Metrics currently extracted**
- `total_delinquency`
- `delinquency_60_plus`
- `cumulative_loss_ratio`
- `pool_balance`

**Note on parsing**
The parser uses regex patterns and heuristics to decide whether a value is a ratio/percent vs. a raw number. If a value looks like a ratio but lacks a `%`, it is converted to a percentage.

---

## 3) Core definitions

**Delinquency**
A loan is delinquent when payments are past due. Trustee reports typically bucket by days past due (30/60/90+). The tool focuses on **60+ day delinquency** because it is a strong early-warning indicator of future losses.

**60+ day delinquency rate**
Typically defined as:

```
60+ DQ % = (balance of loans 60+ days past due) / (total pool balance)
```

**Cumulative loss ratio**
Total net losses to date as a fraction of original or current pool balance. It is a backward-looking credit loss measure.

**Pool balance**
Outstanding principal balance of the securitized pool.

**Trigger**
A structural covenant (e.g., 60+ DQ must be <= 10%). If breached, it can redirect cash flows or change waterfall behavior.

**Cushion**
Distance from the trigger threshold, expressed as a percent of the threshold. Example:

```
threshold = 10%, current = 0.5%
Cushion = (0.10 - 0.005) / 0.10 = 95%
```

---

## 4) Risk score (exact formula used in this tool)

The score is **rule-based** and explainable. It runs on each trigger. Higher = worse risk of trigger breach.

### Step 1: If a trigger is already breached
If `cushion <= 0`, score = **1.00** (100%).

### Step 2: Otherwise compute sub-scores
Cushion risk:

```
If cushion >= 20%, cushion_risk = 0
Else cushion_risk = 1 - (cushion / 0.20)
```

Trend risk (3m deterioration):

```
adverse_change = change3m   (for <= triggers)
trend_risk = clamp(adverse_change / 0.01)
```

Volatility risk:

```
vol_risk = clamp(vol6m / 0.005)
```

Macro risk:

```
macro_risk = clamp((macro_percentile - 0.50) / 0.50)
```

### Step 3: Weighted sum

```
score = 0.60*cushion_risk
      + 0.20*trend_risk
      + 0.10*vol_risk
      + 0.10*macro_risk
```

### Interpretation
- **0.00 to 0.44**: Low (Normal)
- **0.45 to 0.74**: Medium (Yellow)
- **0.75 to 1.00**: High (Red)

These thresholds are set directly in the code (not from a rating agency) and are tunable.

---

## 5) Macro regime

The demo can use **NY Fed Household Debt and Credit** data as a macro benchmark (auto-loan serious delinquency). The macro value is ranked vs. its own historical time series to produce a percentile.

Macro labels:
- **Severe**: percentile >= 0.85
- **Moderate**: percentile >= 0.70
- **Normal**: < 0.70

If the macro source is missing, the UI defaults to **Normal** to avoid false alarms.

---

## 6) Where the red/yellow cutoffs come from

They are **internal** operational thresholds in the demo (not mandated by any regulator). The idea is:

- **Red**: high risk of a trigger trip, requires immediate attention
- **Yellow**: trending worse but not yet urgent
- **Green/Normal**: stable

These are adjustable. In production you would calibrate them to your historical deal behavior and the risk tolerance of the PM/IC.

---

## 7) Why a deal can look "Severe" but have low risk

If the macro percentile is high but the trigger cushion is still wide, the **macro label** may show "Severe" while the **risk score** remains low. In this tool:

- Macro regime is a **context signal**.
- Risk score is a **trigger breach risk** measure.

Both can be true at the same time.

---

## 8) Other indicators commonly used in ABS surveillance

These are not all implemented in the demo, but are standard in the market:

**Delinquency buckets**
- 30, 60, 90+ day delinquency rates
- Roll rates (e.g., 30 -> 60 transitions)

**Loss measures**
- Cumulative net loss (CNL)
- Loss severity = (net loss / defaulted balance)
- Recovery rate

**Prepayment and amortization**
- CPR (Conditional Prepayment Rate)
- SMM (Single Monthly Mortality)
- WAL (Weighted Average Life)

**Credit enhancement and structure**
- Overcollateralization (OC) ratio
- Interest coverage (IC) ratio
- Excess spread
- Trigger step-down tests

**Portfolio quality**
- Weighted average FICO
- Weighted average coupon
- LTV or original term
- Vintage curves

**Macro / external context**
- Auto loan delinquency rates (macro)
- Unemployment rates
- Used car price indices

---

## 9) Other risk formulas used in the industry

Different shops use different approaches, often blending:

**A) Deterministic trigger risk**
- Threshold distance + trend + volatility (like this tool)

**B) Expected loss style**
- Expected loss = PD * LGD * EAD
  - PD = probability of default
  - LGD = loss given default
  - EAD = exposure at default

**C) Transition matrix / roll-rate models**
- Estimate how loans migrate between delinquency buckets
- Often used for near-term default forecasting

**D) Hazard or survival models**
- Estimate time-to-default and prepayment timing

**E) Stress testing**
- Apply macro shocks and recompute trigger cushions
- Typical in rating agency or risk committee reviews

**F) Cash flow waterfall modeling**
- Full deal-level modeling of OC/IC, excess spread, triggers, and tranche cash flows
- Used by rating agencies and buy-side analytics

---

## 10) How this maps to Excel workflows

A typical Excel surveillance workflow looks like:

1) Import trustee/SEC data by period
2) Normalize % and dollar metrics
3) Build time series per metric
4) Compute:
   - Cushion vs. trigger
   - 3m change (or 1m, 6m)
   - Volatility (stdev of monthly changes)
5) Assign a risk score rule or model
6) Rank triggers
7) Produce charts and a one-page report

This tool automates those steps and keeps it consistent.

---

## 11) Sources and learning resources

### Macro data used here
- NY Fed Household Debt and Credit Report (auto loan delinquency)
  - https://www.newyorkfed.org/microeconomics/hhdc

### Definitions / context
- 60+ delinquency rate definition (contract-style)
  - https://www.lawinsider.com/dictionary/60-day-delinquency-rate

### Videos (conceptual)
- "What is a securitization?" (Bionic Turtle)
  - https://www.youtube.com/watch?v=Z8kCQkfblzY
- "The Process of Securitization" (overview)
  - https://www.youtube.com/watch?v=eeaq1wT0hSA

### Excel / modeling resources (structured finance)
- *Modeling Structured Finance Cash Flows with Microsoft Excel* (Keith A. Allman)
  - https://www.wiley.com/en-us/Modeling+Structured+Finance+Cash+Flows+with+Microsoft+Excel-p-9780470042908
- Structured finance modeling notes with reference to a step-by-step video
  - https://edbodmer.com/wp-content/uploads/2022/07/StructuredFinanceModellingandtheFinancialCrisisof2008.pdf

---

## 12) Practical interpretation cheat sheet

- **Wide cushion + low trend** = low risk
- **Shrinking cushion + rising delinquencies** = escalating risk
- **High macro percentile** = environment is worsening even if triggers are not near breach

---

## 13) How to tune this for production

If you want to calibrate risk scores:

1) Gather historical trustee data
2) Label periods where triggers tripped
3) Backtest thresholds and weights
4) Adjust weights and cutoff levels to minimize false positives/negatives

---

## Appendix: where the tool gets its labels

**Risk labels**
- Red: score >= 0.75
- Yellow: score >= 0.45
- Green: otherwise

**Macro labels**
- Severe: percentile >= 0.85
- Moderate: percentile >= 0.70
- Normal: otherwise

These are defined in the UI logic and can be changed if desired.

