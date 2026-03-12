# Trigger Monitor UI

This project pulls SEC 10-D filings and shows trigger risk for auto ABS deals.

## What this app is doing

- Reads deal config from `scripts/sec_demo_deals.json`
- Pulls filing data and parses Exhibit 99.1 tables
- Builds UI data JSON
- Builds QA and validation reports so you can see what failed and why

## Key files (plain English)

- `scripts/sec_demo_deals.json`: your deal list (deal id, CIK, trigger setup)
- `scripts/sec_trigger_demo_builder.py`: main builder/parser
- `out/trigger_training_data.csv`: row-level model/training dataset
- `out/qa_summary.csv`: condensed issues by deal/variable
- `out/validation_report.json`: run stats and parse diagnostics
- `public/data/trigger_monitor_demo.json`: data used by the dashboard in Vite
- `public/data/trigger_explorer.json`: data used by the Explore section

## One command for a full refresh

This updates the main UI dataset, training CSV, QA report, and validation report.
It also prints progress while running.

```bash
python3 scripts/sec_trigger_demo_builder.py \
  --config scripts/sec_demo_deals.json \
  --out out/trigger_monitor_demo.json \
  --public-copy public/data/trigger_monitor_demo.json \
  --export-training-csv out/trigger_training_data.csv \
  --validation-report out/validation_report.json \
  --qa-report out/qa_summary.json \
  --cache-dir out/sec_cache \
  --parsed-cache out/parsed_filing_metrics.json
```

Notes:
- `qa_summary.csv` is auto-written from `qa_summary.json`.
- `skip-existing-training` is already on by default for faster reruns.

## Start or restart Vite

```bash
npm run dev -- --force
```

If Vite is already running and acting stale:

```bash
# stop current Vite first with Ctrl+C, then:
lsof -ti tcp:5173 | xargs kill -9
npm run dev -- --force
```

Then hard refresh in browser: `Cmd+Shift+R`.

## How to read cushion correctly

- Cushion formula: `(threshold - current_dq) / threshold`
- `100%` means very safe buffer
- `0%` means right at the trigger line
- Negative means already above threshold (trigger breached)

Example:
- threshold `7%`, current DQ `14%`
- cushion = `(0.07 - 0.14)/0.07 = -1.00` which is `-100%`

So values like `-80%` or `-100%` are possible in stressed subprime deals.

## Why QA shows common statuses

- `no_ex99`: filing found, but parser did not resolve Exhibit 99 link
- `no_metrics`: Exhibit found, but required metrics were not extracted
- `insufficient_history`: not enough months for rolling features like `trend3m`
- `missing`: specific variable is empty for that row/date

## Step-down threshold config (phase 1)

You can now set date/index-based trigger thresholds per deal without replacing parser output.

```json
{
  "deal_id": "Example Deal 2024-1",
  "trigger_threshold_schedule_mode": "fallback",
  "trigger_threshold_schedule": [
    { "start_date": "2024-01-01", "end_date": "2024-12-31", "threshold": 0.07 },
    { "start_date": "2025-01-01", "threshold": 0.075 }
  ]
}
```

- `trigger_threshold_schedule_mode: "fallback"`: use schedule only when filing threshold is missing.
- `trigger_threshold_schedule_mode: "override"`: always use schedule when a rule matches.
- Existing `trigger_threshold_override` and `force_threshold_override` still work.

## UI data sources

The app reads from these paths (in order):

- `/data/trigger_monitor_demo.json`
- `./data/trigger_monitor_demo.json`
- `/out/trigger_monitor_demo.json`
- `./out/trigger_monitor_demo.json`

Explore section reads:

- `/data/trigger_explorer.json`
- `./data/trigger_explorer.json`

If Dashboard and Explore look inconsistent, one of those JSON files is stale.

## Build for production

```bash
npm run build
```

Output is in `dist/`.
