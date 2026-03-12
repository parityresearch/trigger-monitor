#!/usr/bin/env python3
"""
Build Trigger Monitor demo data from SEC 10-D filings (Exhibit 99.1).

Outputs a JSON payload shaped like the MOCK data in TriggerMonitorWebsiteDemo.jsx:
{
  "asOf": "YYYY-MM-DD",
  "portfolio": {...},
  "deals": [...],
  "alerts": [...]
}

Usage:
  python3 scripts/sec_trigger_demo_builder.py \
    --config scripts/sec_demo_deals.json \
    --months 18 \
    --out out/trigger_monitor_demo.json \
    --user-agent "Your Name your@email.com"
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
import re
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests
from io import StringIO, BytesIO

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover
    raise SystemExit("pandas is required. Install with: pip install pandas lxml") from exc


SEC_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_nodash}/"
SEC_INDEX_JSON = SEC_ARCHIVES_BASE + "index.json"
SEC_DOC_URL = SEC_ARCHIVES_BASE + "{filename}"
SEC_INDEX_HTML = SEC_ARCHIVES_BASE + "index.htm"
SEC_INDEX_HTML_ALT = SEC_ARCHIVES_BASE + "index.html"
NYFED_HHDC_BACKGROUND = "https://www.newyorkfed.org/microeconomics/hhdc/background.html"
NYFED_HHDC_XLS_BASE = "https://www.newyorkfed.org/medialibrary/interactives/householdcredit/data/xls/"

MONTH_FMT = "%Y-%m-%d"
REQUIRED_METRICS = ("pool_balance", "total_delinquency", "delinquency_60_plus", "cumulative_loss_ratio")
CACHE_DIR: Optional[str] = None

STOP_DEAL_TOKENS = {
    "trust",
    "auto",
    "receivables",
    "owner",
    "lease",
    "credit",
    "card",
    "equipment",
    "notes",
    "asset",
    "backed",
    "abs",
    "deal",
    "series",
    "class",
    "fund",
    "issuer",
}

TEN_D_RE = re.compile(r"10\s*[-_]?d|10.?d", re.I)

EX99_FILENAME_RE = re.compile(
    r"(ex-?99\.?0?1[a-z]?|ex99[_-]?0?1[a-z]?|exhibit[_-]?99\.?0?1[a-z]?|exhibit991[a-z]?|exh[_-]?99[_-]?0?1[a-z]?|99.?1)",
    re.I,
)
EX99_LABEL_RE = re.compile(
    r"(ex-?99\.?0?1[a-z]?|exhibit\s*99\.?0?1[a-z]?|99\.?0?1[a-z]?|exh[_-]?\s*99[_-]?\s*0?1[a-z]?|99.?1)",
    re.I,
)
EX99_ANY_RE = re.compile(
    r"(ex-?99(?:[.\-_]?\d+)?[a-z]?|ex99[.\-_]?\d+[a-z]?|exhibit[_-]?\s*99(?:[.\-_]?\d+)?[a-z]?|exh[_-]?\s*99[.\-_]?\d+[a-z]?|99[.\-_]?\d+[a-z]?|exhibit\s+99\b|exh\s+99\b|ex-?99\b|99\b)",
    re.I,
)
DIST_REPORT_RE = re.compile(
    r"(distribution\s+report|investor\s*report|investor\s*rep|monthly\s+distribution|monthly\s+investor\s*report|"
    r"servicer\s*report|servicer\s*rep|monthly\s+servicer(?:'s)?\s+certificate|servicer(?:'s)?\s+certificate|"
    r"pool\s+performance|performance\s+report|monthly\s+servicer(?:'s)?\s+certificate)",
    re.I,
)
EX102_FILENAME_RE = re.compile(
    r"(ex-?102|exhibit[-_]?102|ex102[-_]?|absee[-_]?|abs-ee[-_]?)",
    re.I,
)

METRIC_DEFS = [
    {
        "key": "pool_balance",
        "patterns": [
            r"receivables?\s+pool\s+balance",
            r"pool\s+balance",
            r"outstanding\s+balance",
        ],
        "prefer_percent": False,
    },
    {
        "key": "total_delinquency",
        "patterns": [
            r"total\s+delinquenc(?:y|ies)\s+(?:rate|ratio|percentage|%)",
            r"total\s+delinquenc(?:y|ies)",
            r"total\s+delinquent",
            r"delinquency\s+rate",
            r"delinquency\s+profile",
            r"total\s+30\+\s+days?\s+past\s+due",
        ],
        "prefer_percent": True,
    },
    {
        "key": "delinquency_60_plus",
        "patterns": [
            r"60\s*[-+]\s*day.*(?:delinq|percent)",  # "60-Day Delinquency" or "60+ Day"
            r"61\s*\+\s*day",
            r"60\+",
            r"60\s+days?\s+(?:or\s+)?(?:more|greater)",
            r"total\s+60\+\s+days?\s+past\s+due",
        ],
        "prefer_percent": True,
    },
    {
        "key": "cumulative_loss_ratio",
        "patterns": [
            r"cumulative\s+net\s+loss\s+ratio",
            r"cumulative\s+loss\s+ratio",
            r"cumulative\s+net\s+loss",
            r"cumulative\s+losses",
            r"cumulative\s+principal\s+net\s+loss",
            r"net\s+loss\s*/\s*\(gain\).*cutoff\s+date\s+pool\s+balance",
        ],
        "prefer_percent": True,
    },
    {
        "key": "monthly_payment_rate",
        "patterns": [
            r"collections\s+of\s+principal\s+receivables\s+as\s+a\s+percentage\s+of\s+prior\s+month\s+principal\s+receivables",
            r"principal\s+payment\s+rate",
            r"monthly\s+payment\s+rate",
            r"\bmpr\b",
            r"payment\s+rate",
        ],
        "prefer_percent": True,
    },
    {
        "key": "total_collections_rate",
        "patterns": [
            r"collections\s+as\s+a\s+percentage\s+of\s+prior\s+month\s+principal\s+receivables\s+and\s+finance\s+charge\s+receivables",
            r"total\s+collections\s+rate",
            r"collections\s+rate",
        ],
        "prefer_percent": True,
    },
]


@dataclass
class FilingDoc:
    period_end: str
    accession_no: str
    ex99_url: str
    primary_doc_url: Optional[str] = None
    index_url: Optional[str] = None
    ex102_url: Optional[str] = None  # ABS-EE Exhibit 102 XML (loan-level tape)


def sec_headers(user_agent: str, host: str) -> Dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": host,
    }


def _safe_float(value: object) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_date(value: object) -> Optional[dt.date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        return None


def normalize_threshold_schedule_mode(value: object) -> str:
    mode = str(value or "fallback").strip().lower()
    if mode in {"override", "force", "replace"}:
        return "override"
    return "fallback"


def parse_threshold_schedule(raw: object) -> List[dict]:
    if not isinstance(raw, list):
        return []
    out: List[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        threshold = (
            _safe_float(item.get("threshold"))
            or _safe_float(item.get("value"))
            or _safe_float(item.get("trigger_threshold"))
        )
        if threshold is None or threshold <= 0:
            continue
        start_date = (
            _safe_date(item.get("start_date"))
            or _safe_date(item.get("start"))
            or _safe_date(item.get("from_date"))
            or _safe_date(item.get("from"))
        )
        end_date = (
            _safe_date(item.get("end_date"))
            or _safe_date(item.get("end"))
            or _safe_date(item.get("to_date"))
            or _safe_date(item.get("to"))
        )
        filing_from = (
            _safe_int(item.get("filing_from"))
            or _safe_int(item.get("start_filing"))
            or _safe_int(item.get("from_filing"))
        )
        filing_to = (
            _safe_int(item.get("filing_to"))
            or _safe_int(item.get("end_filing"))
            or _safe_int(item.get("to_filing"))
        )
        out.append(
            {
                "threshold": threshold,
                "start_date": start_date,
                "end_date": end_date,
                "filing_from": filing_from,
                "filing_to": filing_to,
            }
        )
    return out


def resolve_schedule_threshold(
    schedule: List[dict],
    period_end: str,
    filing_idx: int,
) -> Optional[float]:
    if not schedule:
        return None
    period_date = _safe_date(period_end)
    chosen: Optional[float] = None
    for rule in schedule:
        start_date = rule.get("start_date")
        end_date = rule.get("end_date")
        filing_from = rule.get("filing_from")
        filing_to = rule.get("filing_to")
        if start_date is not None:
            if period_date is None or period_date < start_date:
                continue
        if end_date is not None:
            if period_date is None or period_date > end_date:
                continue
        if isinstance(filing_from, int) and filing_idx < filing_from:
            continue
        if isinstance(filing_to, int) and filing_idx > filing_to:
            continue
        chosen = _safe_float(rule.get("threshold"))
    return chosen


def apply_configured_threshold(
    metrics: Dict[str, object],
    threshold_override: object,
    force_override: object,
    schedule_threshold: Optional[float],
    schedule_mode: str,
) -> Dict[str, bool]:
    flags = {"schedule_applied": False, "schedule_override_applied": False}
    if force_override is not None:
        try:
            metrics["delinquency_trigger_threshold"] = float(force_override)
        except (TypeError, ValueError):
            pass
        return flags
    if schedule_threshold is not None and schedule_mode == "override":
        metrics["delinquency_trigger_threshold"] = schedule_threshold
        flags["schedule_applied"] = True
        flags["schedule_override_applied"] = True
        return flags
    if metrics.get("delinquency_trigger_threshold") is None:
        if schedule_threshold is not None:
            metrics["delinquency_trigger_threshold"] = schedule_threshold
            flags["schedule_applied"] = True
        elif threshold_override is not None:
            try:
                metrics["delinquency_trigger_threshold"] = float(threshold_override)
            except (TypeError, ValueError):
                pass
    return flags


def resolve_threshold_source(
    reported_threshold: Optional[float],
    final_threshold: Optional[float],
    threshold_override: object,
    force_override: object,
    schedule_threshold: Optional[float] = None,
    schedule_mode: str = "fallback",
    schedule_applied: bool = False,
    schedule_override_applied: bool = False,
) -> str:
    if force_override is not None and final_threshold is not None:
        return "force_override"
    if schedule_override_applied and final_threshold is not None:
        return "schedule_force"
    if reported_threshold is not None:
        return "reported"
    if schedule_applied and final_threshold is not None:
        return "schedule"
    if threshold_override is not None and final_threshold is not None:
        return "override"
    if (
        schedule_threshold is not None
        and final_threshold is not None
        and abs(final_threshold - schedule_threshold) < 1e-9
    ):
        return "schedule_force" if schedule_mode == "override" else "schedule"
    if final_threshold is not None:
        return "reported"
    return "missing"


def sanitize_incomplete_dq_zero_spikes(
    values_by_period: List[Optional[float]],
    complete_by_period: List[bool],
) -> List[Optional[float]]:
    """
    Suppress likely parser artifacts where an incomplete filing row reports 0.00% DQ
    after a sustained run of elevated DQ values.

    This avoids showing misleading 100% cushion on the latest point while preserving
    genuine 0% prints from complete rows. For suspicious rows, carry forward the most
    recent prior value instead of dropping the point.
    """
    if not values_by_period:
        return values_by_period
    out = list(values_by_period)
    for idx, cur in enumerate(out):
        if not isinstance(cur, (int, float)) or cur != 0.0:
            continue
        is_complete = complete_by_period[idx] if idx < len(complete_by_period) else False
        if is_complete:
            continue
        prev_vals = [v for v in out[max(0, idx - 6) : idx] if isinstance(v, (int, float))]
        if len(prev_vals) < 4:
            continue
        recent = prev_vals[-4:]
        prior_nonzero = next((v for v in reversed(recent) if v > 0.0), recent[-1])
        if min(recent) >= 0.02 and (sum(recent) / len(recent)) >= 0.03:
            out[idx] = prior_nonzero
            continue
        # Prime deals can run at very low DQ levels; still treat a sudden zero in an
        # incomplete row as suspect if the prior points were consistently non-zero.
        nontrivial_count = sum(1 for v in recent if v > 0.001)
        if nontrivial_count >= 3 and max(recent) >= 0.0014:
            out[idx] = prior_nonzero
    return out


RETRY_STATUS = {429, 500, 502, 503, 504}


def _cache_path(url: str, suffix: str) -> Optional[str]:
    if not CACHE_DIR:
        return None
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception:
        return None
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{digest}{suffix}")


def _cache_read_text(url: str) -> Optional[str]:
    path = _cache_path(url, ".txt")
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _cache_write_text(url: str, text: str) -> None:
    path = _cache_path(url, ".txt")
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        pass


def _cache_read_json(url: str) -> Optional[dict]:
    path = _cache_path(url, ".json")
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _cache_write_json(url: str, data: dict) -> None:
    path = _cache_path(url, ".json")
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass


def _fetch_with_retry(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    sleep: float,
    retries: int,
) -> "requests.Response":
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            time.sleep(sleep)
            return resp
        except requests.HTTPError as exc:
            last_exc = exc
            status = exc.response.status_code if exc.response is not None else None
            if status in RETRY_STATUS and attempt < retries - 1:
                time.sleep(1.0 * (2**attempt))
                continue
            raise
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(1.0 * (2**attempt))
                continue
            raise
    raise last_exc  # pragma: no cover


def fetch_json(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    sleep: float = 0.2,
    retries: int = 3,
    use_cache: bool = True,
) -> dict:
    if use_cache:
        cached = _cache_read_json(url)
        if cached is not None:
            return cached
    data = _fetch_with_retry(session, url, headers, sleep, retries).json()
    _cache_write_json(url, data)
    return data


def fetch_text(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    sleep: float = 0.2,
    retries: int = 3,
) -> str:
    cached = _cache_read_text(url)
    if cached is not None:
        return cached
    text = _fetch_with_retry(session, url, headers, sleep, retries).text
    _cache_write_text(url, text)
    return text


def yyyymmdd_from_period(period: str) -> str:
    return f"{period[0:4]}-{period[4:6]}-{period[6:8]}"


def normalize_period(period: Optional[str], filing_date: Optional[str]) -> Optional[str]:
    if period:
        period = period.strip()
        if re.fullmatch(r"\d{8}", period):
            return yyyymmdd_from_period(period)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", period):
            return period
    if filing_date:
        filing_date = filing_date.strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", filing_date):
            return filing_date
    return None


def _parse_iso_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s[:10]
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        return None


def load_submission_rows(
    session: requests.Session,
    cik: str,
    user_agent: str,
    cutoff: Optional[dt.date] = None,
    refresh_submissions: bool = False,
    debug: bool = False,
) -> List[dict]:
    base_url = SEC_SUBMISSIONS.format(cik=cik)
    submissions = fetch_json(
        session,
        base_url,
        headers=sec_headers(user_agent, "data.sec.gov"),
        use_cache=not refresh_submissions,
    )

    def _rows_from_submission(payload: dict) -> List[dict]:
        recent = payload.get("filings", {}).get("recent", {})
        forms = recent.get("form", []) or []
        accessions = recent.get("accessionNumber", []) or []
        periods = recent.get("periodOfReport", []) or []
        filing_dates = recent.get("filingDate", []) or []
        count = min(len(forms), len(accessions))
        rows_local: List[dict] = []
        for i in range(count):
            rows_local.append({
                "form": forms[i],
                "accession": accessions[i],
                "period": periods[i] if i < len(periods) else None,
                "filing_date": filing_dates[i] if i < len(filing_dates) else None,
            })
        return rows_local

    rows = _rows_from_submission(submissions)
    seen_accessions = {str(r.get("accession", "")).strip() for r in rows if r.get("accession")}

    # Include older SEC submission shards listed under filings.files.
    files = submissions.get("filings", {}).get("files", []) or []
    for file_entry in files:
        name = str(file_entry.get("name", "")).strip()
        if not name.endswith(".json"):
            continue
        if cutoff is not None:
            filing_to = _parse_iso_date(file_entry.get("filingTo"))
            if filing_to and filing_to < cutoff:
                continue
        shard_url = f"https://data.sec.gov/submissions/{name}"
        try:
            shard = fetch_json(
                session,
                shard_url,
                headers=sec_headers(user_agent, "data.sec.gov"),
                use_cache=not refresh_submissions,
            )
        except requests.RequestException:
            if debug:
                print(f"  - shard fetch failed: {name}")
            continue
        for r in _rows_from_submission(shard):
            acc = str(r.get("accession", "")).strip()
            if acc and acc in seen_accessions:
                continue
            rows.append(r)
            if acc:
                seen_accessions.add(acc)

    return rows


def list_recent_10d_ex99(
    session: requests.Session,
    cik: str,
    months: int,
    user_agent: str,
    deal_name: Optional[str] = None,
    debug: bool = False,
    dump_dir: Optional[str] = None,
    refresh_submissions: bool = False,
) -> List[FilingDoc]:
    if debug and not dump_dir:
        dump_dir = "out/sec_debug"
    cutoff = (dt.date.today().replace(day=1) - dt.timedelta(days=months * 31))
    try:
        submission_rows = load_submission_rows(
            session,
            cik=cik,
            user_agent=user_agent,
            cutoff=cutoff,
            refresh_submissions=refresh_submissions,
            debug=debug,
        )
    except requests.RequestException as exc:
        if debug:
            print(f"  - submissions fetch failed ({exc})")
        return []
    docs: List[FilingDoc] = []

    for row in submission_rows:
        form = row.get("form")
        acc = row.get("accession")
        per = row.get("period")
        filing_date = row.get("filing_date")
        if form not in {"10-D", "10-D/A"}:
            continue
        if not acc:
            continue
        per_iso = normalize_period(per, filing_date)
        if not per_iso:
            continue
        per_date = dt.date.fromisoformat(per_iso)
        if per_date < cutoff:
            continue

        acc_nodash = acc.replace("-", "")
        cik_int = int(cik)
        try:
            idx = fetch_json(
                session,
                SEC_INDEX_JSON.format(cik_int=cik_int, acc_no_nodash=acc_nodash),
                headers=sec_headers(user_agent, "www.sec.gov"),
            )
        except requests.RequestException as exc:
            continue
        items = idx.get("directory", {}).get("item", [])
        files = [item.get("name", "") for item in items]
        if dump_dir:
            try:
                os.makedirs(dump_dir, exist_ok=True)
                with open(os.path.join(dump_dir, f"index_{acc_nodash}.json"), "w", encoding="utf-8") as f:
                    json.dump(idx, f, indent=2)
            except Exception:
                pass
        ex99_candidates = [f for f in files if EX99_FILENAME_RE.search(f)]
        if not ex99_candidates:
            ex99_candidates = [
                item.get("name", "")
                for item in items
                if EX99_ANY_RE.search(str(item.get("type", "")))
            ]
            ex99_candidates = [f for f in ex99_candidates if f]
        if not ex99_candidates:
            ex99_candidates = [f for f in files if EX99_ANY_RE.search(f)]
        if not ex99_candidates:
            ex99_candidates = [
                item.get("name", "")
                for item in items
                if EX99_LABEL_RE.search(str(item.get("type", "")))
            ]
            ex99_candidates = [f for f in ex99_candidates if f]
        if not ex99_candidates:
            ex99_candidates = [
                item.get("name", "")
                for item in items
                if DIST_REPORT_RE.search(str(item.get("name", "")))
            ]
            ex99_candidates = [f for f in ex99_candidates if f]
        if not ex99_candidates:
            # If the filing lists a 10-D doc and another non-index HTML, assume the other HTML is Exhibit 99.1.
            ten_d_names = [f for f in files if TEN_D_RE.search(f)]
            non_index_html_items = [
                item for item in items
                if str(item.get("name", "")).lower().endswith((".htm", ".html"))
                and "index" not in str(item.get("name", "")).lower()
                and not TEN_D_RE.search(str(item.get("name", "")))
            ]
            if ten_d_names and non_index_html_items:
                non_index_html_items = sorted(
                    non_index_html_items,
                    key=lambda x: int(str(x.get("size") or 0)),
                    reverse=True,
                )
                candidate = non_index_html_items[0].get("name", "")
                if candidate:
                    ex99_candidates = [candidate]
        ex99_source = "filename"
        primary_doc = None
        if not primary_doc:
            ten_d_files = [f for f in files if TEN_D_RE.search(f)]
            if ten_d_files:
                primary_doc = ten_d_files[0]
        if not ex99_candidates:
            ex99_candidates, primary_doc = find_docs_from_index_html(
                session,
                cik_int=cik_int,
                acc_no_nodash=acc_nodash,
                user_agent=user_agent,
                debug=debug,
                dump_dir=dump_dir,
            )
            ex99_source = "index_html"
        # If we found a 10-D primary doc, prioritize extracting EX-99.1 from inside it.
        if primary_doc:
            ex99_from_primary = find_ex99_from_primary_doc(
                session,
                cik_int=cik_int,
                acc_no_nodash=acc_nodash,
                primary_doc=primary_doc,
                user_agent=user_agent,
                debug=debug,
                dump_dir=dump_dir,
            )
            if ex99_from_primary:
                ex99_candidates = ex99_from_primary
                ex99_source = "primary_doc"
        if not ex99_candidates and primary_doc:
            ex99_candidates = find_ex99_from_primary_doc(
                session,
                cik_int=cik_int,
                acc_no_nodash=acc_nodash,
                primary_doc=primary_doc,
                user_agent=user_agent,
                debug=debug,
                dump_dir=dump_dir,
            )
            ex99_source = "primary_doc"
        # Ensure chosen file actually exists in this accession directory.
        if ex99_candidates:
            files_set = {f.lower() for f in files if f}
            filtered = []
            for c in ex99_candidates:
                if not c:
                    continue
                c_lower = c.lower()
                if c_lower.startswith(("http://", "https://")):
                    filtered.append(c)
                    continue
                c_name = c.rstrip("/").split("/")[-1].lower()
                if c_name in files_set:
                    filtered.append(c)
            if filtered:
                ex99_candidates = filtered
            else:
                ex99_candidates = []
        if ex99_candidates and deal_name:
            ex99_candidates = filter_candidates_by_deal_name(ex99_candidates, deal_name)
        if not ex99_candidates:
            # Last-resort fallback: any EX-99.* filename or distribution report-like name.
            fallback = [
                item.get("name", "")
                for item in items
                if EX99_ANY_RE.search(str(item.get("name", "")))
                or DIST_REPORT_RE.search(str(item.get("name", "")))
            ]
            fallback = [f for f in fallback if f]
            if not fallback:
                # If still nothing, pick the largest non-index HTML file (often contains exhibits).
                html_items = [
                    item for item in items
                    if str(item.get("name", "")).lower().endswith((".htm", ".html"))
                    and "index" not in str(item.get("name", "")).lower()
                    and not TEN_D_RE.search(str(item.get("name", "")))
                ]
                html_items = sorted(html_items, key=lambda x: int(str(x.get("size") or 0)), reverse=True)
                if html_items:
                    fallback = [html_items[0].get("name", "")]
            if fallback:
                ex99_candidates = fallback
                ex99_source = "fallback"
        if not ex99_candidates:
            continue

        ex99 = pick_ex99_candidate(ex99_candidates)
        if deal_name:
            matched = pick_ex99_candidate_by_deal_content(
                session,
                cik_int=cik_int,
                acc_no_nodash=acc_nodash,
                candidates=ex99_candidates,
                deal_name=deal_name,
                user_agent=user_agent,
            )
            if matched:
                ex99 = matched
            else:
                deal_matches = find_docs_by_deal_name(
                    session,
                    cik_int=cik_int,
                    acc_no_nodash=acc_nodash,
                    items=items,
                    deal_name=deal_name,
                    user_agent=user_agent,
                )
                if deal_matches:
                    ex99_candidates = deal_matches
                    ex99_source = "deal_match"
                    ex99 = pick_ex99_candidate(ex99_candidates)
        if ex99.lower().startswith(("http://", "https://")):
            ex99_url = re.sub(r"^http://", "https://", ex99, flags=re.I)
        else:
            ex99_url = SEC_DOC_URL.format(cik_int=cik_int, acc_no_nodash=acc_nodash, filename=ex99)
        index_url = SEC_INDEX_HTML.format(cik_int=cik_int, acc_no_nodash=acc_nodash)
        primary_doc_url = (
            SEC_DOC_URL.format(cik_int=cik_int, acc_no_nodash=acc_nodash, filename=primary_doc)
            if primary_doc
            else None
        )
        # Look for Exhibit 102 (ABS-EE XML loan-level tape) in the same filing.
        ex102_url_val: Optional[str] = None
        ex102_files = [
            f for f in files
            if EX102_FILENAME_RE.search(f) and f.lower().endswith(".xml")
        ]
        if ex102_files:
            ex102_url_val = SEC_DOC_URL.format(
                cik_int=cik_int, acc_no_nodash=acc_nodash, filename=ex102_files[0]
            )
        docs.append(
            FilingDoc(
                period_end=per_iso,
                accession_no=acc,
                ex99_url=ex99_url,
                primary_doc_url=primary_doc_url,
                index_url=index_url,
                ex102_url=ex102_url_val,
            )
        )

    uniq = {}
    for d in docs:
        uniq[d.period_end] = d
    docs_sorted = [uniq[k] for k in sorted(uniq.keys())]
    return docs_sorted


def list_recent_10d_index_urls(
    session: requests.Session,
    cik: str,
    months: int,
    user_agent: str,
    debug: bool = False,
    refresh_submissions: bool = False,
) -> List[dict]:
    cutoff = (dt.date.today().replace(day=1) - dt.timedelta(days=months * 31))
    try:
        submission_rows = load_submission_rows(
            session,
            cik=cik,
            user_agent=user_agent,
            cutoff=cutoff,
            refresh_submissions=refresh_submissions,
            debug=debug,
        )
    except requests.RequestException as exc:
        if debug:
            print(f"  - submissions fetch failed ({exc})")
        return []
    entries: List[dict] = []
    for row in submission_rows:
        form = row.get("form")
        acc = row.get("accession")
        per = row.get("period")
        filing_date = row.get("filing_date")
        if form not in {"10-D", "10-D/A"}:
            continue
        if not acc:
            continue
        per_iso = normalize_period(per, filing_date)
        if not per_iso:
            continue
        per_date = dt.date.fromisoformat(per_iso)
        if per_date < cutoff:
            continue
        acc_nodash = acc.replace("-", "")
        cik_int = int(cik)
        index_url = SEC_INDEX_HTML.format(cik_int=cik_int, acc_no_nodash=acc_nodash)
        entries.append({
            "period_end": per_iso,
            "accession_no": acc,
            "index_url": index_url,
        })
    uniq = {}
    for e in entries:
        uniq[e["period_end"]] = e
    return [uniq[k] for k in sorted(uniq.keys())]


def filings_from_overrides(
    session: requests.Session,
    overrides: List[dict],
    user_agent: str,
    debug: bool = False,
) -> List[FilingDoc]:
    docs: List[FilingDoc] = []
    for entry in overrides:
        period_end = (entry.get("period_end") or "").strip()
        ex99_url = (entry.get("ex99_url") or "").strip()
        primary_url = (entry.get("primary_doc_url") or "").strip()
        if not period_end:
            if debug:
                print("  - override missing period_end; skipping entry")
            continue
        if not ex99_url and primary_url:
            try:
                # Extract cik/accession/filename from the primary URL.
                parts = primary_url.split("/edgar/data/", 1)[-1].split("/")
                cik_int = int(parts[0])
                acc_no_nodash = parts[1]
                primary_doc = parts[-1]
                candidates = find_ex99_from_primary_doc(
                    session,
                    cik_int=cik_int,
                    acc_no_nodash=acc_no_nodash,
                    primary_doc=primary_doc,
                    user_agent=user_agent,
                    debug=debug,
                )
                if candidates:
                    picked = pick_ex99_candidate(candidates)
                    if picked.lower().startswith(("http://", "https://")):
                        ex99_url = re.sub(r"^http://", "https://", picked, flags=re.I)
                    else:
                        ex99_url = SEC_DOC_URL.format(
                            cik_int=cik_int,
                            acc_no_nodash=acc_no_nodash,
                            filename=picked,
                        )
            except Exception:
                if debug:
                    print("  - override primary_doc_url parse failed")
        if not ex99_url:
            # If we still don't have an EX-99 URL, skip (will be recorded as no_ex99 elsewhere).
            continue
        docs.append(
            FilingDoc(
                period_end=period_end,
                accession_no="override",
                ex99_url=ex99_url,
                primary_doc_url=primary_url or None,
                index_url=entry.get("index_url"),
            )
        )
    # Ensure newest per period_end
    uniq = {}
    for d in docs:
        uniq[d.period_end] = d
    return [uniq[k] for k in sorted(uniq.keys())]


def find_docs_from_index_html(
    session: requests.Session,
    cik_int: int,
    acc_no_nodash: str,
    user_agent: str,
    debug: bool = False,
    dump_dir: Optional[str] = None,
) -> tuple[List[str], Optional[str]]:
    urls = [
        SEC_INDEX_HTML.format(cik_int=cik_int, acc_no_nodash=acc_no_nodash),
        SEC_INDEX_HTML_ALT.format(cik_int=cik_int, acc_no_nodash=acc_no_nodash),
    ]
    for url in urls:
        try:
            html = fetch_text(session, url, headers=sec_headers(user_agent, "www.sec.gov"), sleep=0.1)
        except Exception:
            continue
        if dump_dir:
            try:
                os.makedirs(dump_dir, exist_ok=True)
                with open(os.path.join(dump_dir, f"index_{acc_no_nodash}.html"), "w", encoding="utf-8") as f:
                    f.write(html)
            except Exception:
                pass

        try:
            tables = pd.read_html(StringIO(html))
        except ValueError:
            tables = []

        ex99_candidates: List[str] = []
        dist_candidates: List[str] = []
        primary_doc: Optional[str] = None
        for df in tables:
            if df.empty:
                continue
            cols = [str(c).strip().lower() for c in df.columns]
            type_col = df.columns[cols.index("type")] if "type" in cols else None
            doc_col = df.columns[cols.index("document")] if "document" in cols else None
            desc_col = df.columns[cols.index("description")] if "description" in cols else None
            for _, row in df.iterrows():
                type_val = str(row.get(type_col, "")).upper() if type_col is not None else ""
                doc_val = str(row.get(doc_col, "")).strip() if doc_col is not None else ""
                desc_val = str(row.get(desc_col, "")).strip() if desc_col is not None else ""

                if type_val and EX99_LABEL_RE.search(type_val):
                    match = re.search(r"[\w.\-]+?\.(?:htm|html|pdf|xml|txt)", doc_val, re.I)
                    if match:
                        ex99_candidates.append(match.group(0))
                if type_val and EX99_ANY_RE.search(type_val):
                    match = re.search(r"[\w.\-]+?\.(?:htm|html|pdf|xml|txt)", doc_val, re.I)
                    if match:
                        ex99_candidates.append(match.group(0))
                if doc_val and EX99_FILENAME_RE.search(doc_val):
                    match = re.search(r"[\w.\-]+?\.(?:htm|html|pdf|xml|txt)", doc_val, re.I)
                    if match:
                        ex99_candidates.append(match.group(0))
                if doc_val and EX99_ANY_RE.search(doc_val):
                    match = re.search(r"[\w.\-]+?\.(?:htm|html|pdf|xml|txt)", doc_val, re.I)
                    if match:
                        ex99_candidates.append(match.group(0))
                if doc_val and desc_val and DIST_REPORT_RE.search(desc_val):
                    match = re.search(r"[\w.\-]+?\.(?:htm|html|pdf|xml|txt)", doc_val, re.I)
                    if match:
                        dist_candidates.append(match.group(0))
                if type_val in {"10-D", "10-D/A"} and not primary_doc and doc_val:
                    match = re.search(r"[\w.\-]+?\.(?:htm|html|txt)", doc_val, re.I)
                    if match:
                        primary_doc = match.group(0)

                if not doc_val and doc_col is None:
                    for cell in row.tolist():
                        cell_val = str(cell).strip()
                        if EX99_FILENAME_RE.search(cell_val):
                            match = re.search(r"[\w.\-]+?\.(?:htm|html|pdf|xml|txt)", cell_val, re.I)
                            if match:
                                ex99_candidates.append(match.group(0))
                                break

        if not ex99_candidates:
            ex99_candidates = find_ex99_links_in_html(html)

        if not ex99_candidates and dist_candidates:
            ex99_candidates = dist_candidates

        if ex99_candidates or primary_doc:
            return ex99_candidates, primary_doc

    return [], None


def find_ex99_links_in_html(html: str) -> List[str]:
    candidates: List[str] = []
    anchor_re = re.compile(r"<a[^>]+href=[\"']?([^\"'>\s]+)[^>]*>(.*?)</a>", re.I | re.S)
    for href, text in anchor_re.findall(html):
        if (
            not EX99_LABEL_RE.search(text)
            and not EX99_FILENAME_RE.search(href)
            and not EX99_ANY_RE.search(text)
            and not EX99_ANY_RE.search(href)
            and not DIST_REPORT_RE.search(text)
        ):
            continue
        href = href.split("#", 1)[0].split("?", 1)[0]
        filename = href.rstrip("/").split("/")[-1]
        if re.search(r"\.(?:htm|html|pdf|xml|txt)$", filename, re.I):
            candidates.append(filename)
    return list(dict.fromkeys(candidates))


def find_ex99_from_primary_doc(
    session: requests.Session,
    cik_int: int,
    acc_no_nodash: str,
    primary_doc: str,
    user_agent: str,
    debug: bool = False,
    dump_dir: Optional[str] = None,
) -> List[str]:
    url = SEC_DOC_URL.format(cik_int=cik_int, acc_no_nodash=acc_no_nodash, filename=primary_doc)
    try:
        html = fetch_text(session, url, headers=sec_headers(user_agent, "www.sec.gov"), sleep=0.1)
    except Exception:
        return []
    if dump_dir:
        try:
            os.makedirs(dump_dir, exist_ok=True)
            with open(os.path.join(dump_dir, f"primary_{acc_no_nodash}.html"), "w", encoding="utf-8") as f:
                f.write(html)
            safe_name = primary_doc.replace("/", "_")
            with open(os.path.join(dump_dir, f"primary_{acc_no_nodash}_{safe_name}"), "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

    candidates: List[str] = []
    anchor_re = re.compile(r"<a[^>]+href=[\"']?([^\"'>\s]+)[^>]*>(.*?)</a>", re.I | re.S)
    tr_re = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
    href_re = re.compile(r"href=[\"']?([^\"'>\s]+)", re.I)

    def normalize_href_candidate(raw_href: str) -> str:
        href_clean = raw_href.split("#", 1)[0].split("?", 1)[0]
        if href_clean.lower().startswith(("http://", "https://")):
            if "sec.gov/archives/" in href_clean.lower():
                return re.sub(r"^http://", "https://", href_clean, flags=re.I)
            return href_clean.rstrip("/").split("/")[-1]
        return href_clean.rstrip("/").split("/")[-1]

    for href, text in anchor_re.findall(html):
        if (
            not EX99_LABEL_RE.search(text)
            and not EX99_FILENAME_RE.search(href)
            and not EX99_ANY_RE.search(text)
            and not EX99_ANY_RE.search(href)
            and not DIST_REPORT_RE.search(text)
        ):
            continue
        candidate = normalize_href_candidate(href)
        if re.search(r"\.(?:htm|html|pdf|xml|txt)$", candidate, re.I):
            candidates.append(candidate)
    if not candidates:
        # Some filings include a plain-text exhibit list with a link-like filename.
        file_re = re.compile(r"([\w.\-]*99[\w.\-]*\d+[\w.\-]*\.(?:htm|html|pdf|xml|txt))", re.I)
        candidates.extend(file_re.findall(html))
    if not candidates:
        # As a last check, capture any href that contains 99.* even if label is missing.
        for href, _ in anchor_re.findall(html):
            if not EX99_ANY_RE.search(href):
                continue
            candidate = normalize_href_candidate(href)
            if re.search(r"\.(?:htm|html|pdf|xml|txt)$", candidate, re.I):
                candidates.append(candidate)
    if not candidates:
        # Handle rows where "99.x" or distribution report text is in a separate <td> from the <a href>.
        for row in tr_re.findall(html):
            if not EX99_LABEL_RE.search(row) and not EX99_ANY_RE.search(row) and not DIST_REPORT_RE.search(row):
                continue
            match = href_re.search(row)
            if not match:
                continue
            candidate = normalize_href_candidate(match.group(1))
            if re.search(r"\.(?:htm|html|pdf|xml|txt)$", candidate, re.I):
                candidates.append(candidate)
    if not candidates:
        # Look for exhibit index section and capture any exhibit links nearby.
        lower = html.lower()
        marker = None
        for pat in ("exhibit index", "item 10. exhibits", "item 10 exhibits", "item 10. exhibit", "item 10 exhibit"):
            idx = lower.find(pat)
            if idx != -1:
                marker = idx
                break
        if marker is not None:
            snippet = html[marker : marker + 60000]
            local_candidates = []
            for href, text in anchor_re.findall(snippet):
                candidate = normalize_href_candidate(href)
                if not re.search(r"\.(?:htm|html|pdf|xml|txt)$", candidate, re.I):
                    continue
                local_candidates.append((candidate, text))
            if local_candidates:
                scored = []
                for filename, text in local_candidates:
                    score = 0
                    if EX99_ANY_RE.search(filename):
                        score += 3
                    if EX99_LABEL_RE.search(text) or EX99_ANY_RE.search(text):
                        score += 2
                    if DIST_REPORT_RE.search(text):
                        score += 1
                    scored.append((score, filename))
                scored.sort(key=lambda x: (x[0], -len(x[1])), reverse=True)
                best_score = scored[0][0]
                if best_score > 0:
                    candidates.append(scored[0][1])
                elif len(scored) == 1:
                    candidates.append(scored[0][1])
    if not candidates:
        # Exhibit Index rows sometimes list "99.1" without a recognizable filename or href.
        # Only fall back to primary doc if it appears to contain actual report metrics inline.
        plain = re.sub(r"<[^>]+>", " ", html)
        plain = re.sub(r"\s+", " ", plain).strip()
        has_exhibit_ref = (
            re.search(r"Exhibit\s+Index", plain, re.I)
            or re.search(r"Item\s*10\.\s*Exhibits", plain, re.I)
        ) and re.search(r"\b99\.?\s*1\b", plain)
        has_inline_metrics = re.search(
            r"Receivables\s+with\s+Scheduled\s+Payment\s+Delinquent|Delinquency\s+Activity|Pool\s+Balance",
            plain,
            re.I,
        )
        if has_exhibit_ref and has_inline_metrics:
            candidates.append(primary_doc)
    if candidates:
        # Prefer HTML over XML/PDF, and de-duplicate.
        seen = set()
        def _key(x: str) -> str:
            return x.lower()
        html = [c for c in candidates if c.lower().endswith((".htm", ".html"))]
        ordered = html + [c for c in candidates if c not in html]
        uniq = []
        for c in ordered:
            key = _key(c)
            if key in seen:
                continue
            seen.add(key)
            uniq.append(c)
        candidates = uniq
    return candidates


def deal_tokens(name: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z0-9]+", name.lower())
    filtered = []
    for t in tokens:
        if len(t) < 3:
            continue
        if t in STOP_DEAL_TOKENS:
            continue
        if t.isdigit():
            continue
        if not re.search(r"[a-z]", t):
            continue
        filtered.append(t)
    return filtered


def filter_candidates_by_deal_name(candidates: List[str], deal_name: str) -> List[str]:
    tokens = deal_tokens(deal_name)
    if not tokens:
        return candidates
    filtered = [c for c in candidates if any(tok in c.lower() for tok in tokens)]
    return filtered or candidates


def deal_match_in_text(text: str, deal_name: str, tokens: List[str]) -> bool:
    if not text:
        return False
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = re.sub(r"[^A-Za-z0-9]+", " ", clean).lower()
    clean = re.sub(r"\s+", " ", clean).strip()
    if not clean:
        return False
    if deal_name:
        deal_norm = re.sub(r"[^A-Za-z0-9]+", " ", deal_name).lower().strip()
        if deal_norm and deal_norm in clean:
            return True
    return any(tok in clean for tok in tokens)


def rank_ex99_candidate(name: str) -> tuple[int, int, int]:
    lower = name.lower()
    is_991 = bool(EX99_FILENAME_RE.search(lower))
    is_99_any = bool(EX99_ANY_RE.search(lower))
    is_dist = bool(DIST_REPORT_RE.search(lower))
    if is_991:
        bucket = 0
    elif is_99_any:
        bucket = 1
    elif is_dist:
        bucket = 2
    else:
        bucket = 3
    html_bucket = 0 if lower.endswith((".htm", ".html")) else 1
    return (bucket, html_bucket, len(name))


def pick_ex99_candidate_by_deal_content(
    session: requests.Session,
    cik_int: int,
    acc_no_nodash: str,
    candidates: List[str],
    deal_name: str,
    user_agent: str,
) -> Optional[str]:
    tokens = deal_tokens(deal_name)
    if not tokens:
        return None
    ordered = sorted(candidates, key=rank_ex99_candidate)
    for candidate in ordered:
        if not candidate.lower().endswith((".htm", ".html", ".txt")):
            continue
        if candidate.lower().startswith(("http://", "https://")):
            url = re.sub(r"^http://", "https://", candidate, flags=re.I)
        else:
            url = SEC_DOC_URL.format(cik_int=cik_int, acc_no_nodash=acc_no_nodash, filename=candidate)
        try:
            html = fetch_text(session, url, headers=sec_headers(user_agent, "www.sec.gov"), sleep=0.1)
        except Exception:
            continue
        if deal_match_in_text(html, deal_name, tokens):
            return candidate
    return None


def find_docs_by_deal_name(
    session: requests.Session,
    cik_int: int,
    acc_no_nodash: str,
    items: List[dict],
    deal_name: str,
    user_agent: str,
    limit: int = 10,
) -> List[str]:
    tokens = deal_tokens(deal_name)
    if not tokens:
        return []
    html_items = []
    for item in items:
        name = str(item.get("name", ""))
        if not name:
            continue
        lower = name.lower()
        if "index" in lower:
            continue
        if not lower.endswith((".htm", ".html", ".txt")):
            continue
        size_raw = str(item.get("size") or "0")
        try:
            size = int(size_raw)
        except ValueError:
            size = 0
        html_items.append((name, size))
    if not html_items:
        return []
    html_items.sort(
        key=lambda t: (rank_ex99_candidate(t[0])[0], rank_ex99_candidate(t[0])[1], -t[1], len(t[0]))
    )
    matched: List[str] = []
    checked = 0
    for name, _ in html_items:
        if checked >= limit:
            break
        checked += 1
        url = SEC_DOC_URL.format(cik_int=cik_int, acc_no_nodash=acc_no_nodash, filename=name)
        try:
            html = fetch_text(session, url, headers=sec_headers(user_agent, "www.sec.gov"), sleep=0.1)
        except Exception:
            continue
        if deal_match_in_text(html, deal_name, tokens):
            matched.append(name)
    return matched


def pick_ex99_candidate(candidates: List[str]) -> str:
    return sorted(candidates, key=rank_ex99_candidate)[0]


def normalize_number(token: str, prefer_percent: bool) -> Optional[float]:
    s = token.strip()
    if not s:
        return None
    neg = "(" in s and ")" in s
    s2 = re.sub(r"[^0-9.\-%]", "", s)
    if not s2:
        return None
    is_pct = s2.endswith("%")
    if is_pct:
        s2 = s2[:-1]
    try:
        val = float(s2)
    except ValueError:
        return None
    
    # Sanity check: if prefer_percent is True and value is implausibly large (>100 without %), reject it
    if prefer_percent and not is_pct and val > 100:
        return None
    
    if is_pct:
        val = val / 100.0
    elif prefer_percent:
        # Heuristic: many reports omit the % sign for ratios (e.g., "0.30" meaning 0.30%).
        # Treat values >= 0.1 as percent-style unless they're implausibly large.
        if 0 <= val < 0.1:
            pass
        elif 0.1 <= val <= 100:
            val = val / 100.0
        else:
            return None
    return -val if neg else val


def choose_metric_value(raw_tokens: List[str], prefer_percent: bool) -> Optional[float]:
    if not raw_tokens:
        return None
    tokens_with_pct = [t for t in raw_tokens if "%" in t]
    if prefer_percent:
        if tokens_with_pct:
            return normalize_number(tokens_with_pct[-1], prefer_percent=True)
        # prefer ratio-like tokens if present
        for t in reversed(raw_tokens):
            val = normalize_number(t, prefer_percent=True)
            if val is not None:
                return val
        return None
    # non-percent metrics (pool balance): prefer non-percent tokens
    tokens_no_pct = [t for t in raw_tokens if "%" not in t]
    for t in reversed(tokens_no_pct or raw_tokens):
        val = normalize_number(t, prefer_percent=False)
        if val is not None:
            return val
    return None


def extract_metrics_from_tables(tables: List[pd.DataFrame], debug: bool = False) -> Dict[str, float]:
    found: Dict[str, float] = {}
    for df in tables:
        if df.empty:
            continue
        df2 = df.copy()
        df2 = df2.astype(str)
        
        # Try to identify label and value columns
        # Usually first column is labels, subsequent columns are values
        for i in range(len(df2)):
            row_data = df2.iloc[i]
            
            # Check if this is a header row (contains lots of text, few numbers)
            row_text = " ".join(str(v) for v in row_data.tolist()).lower()
            num_count = len(re.findall(r'\d+', row_text))
            text_len = len(re.sub(r'[\d\s\.\,\%\$\(\)\-]', '', row_text))
            if text_len > 50 and num_count < 3:  # Likely a header
                continue
            
            # Get label (usually first column)
            label = str(row_data.iloc[0]).lower() if len(row_data) > 0 else ""
            
            # Get numeric values from remaining columns
            value_cells = row_data.iloc[1:] if len(row_data) > 1 else row_data
            label_raw = str(row_data.iloc[0]).strip() if len(row_data) > 0 else ""
            tokens = []
            for cell in value_cells:
                cell_str = str(cell)
                # Skip colspan artifacts: cells that are identical to the label (pandas repeats
                # the content of a merged/colspan cell across multiple columns)
                if cell_str.strip() == label_raw:
                    continue
                # Extract numbers with optional % and ()
                cell_tokens = re.findall(r"[\(\-]?\$?\s*[\d,]+(?:\.\d+)?\s*%?\s*[\)]?", cell_str)
                tokens.extend(cell_tokens)
            
            if not tokens:
                continue
                
            # Match against metric patterns
            for metric in METRIC_DEFS:
                key = metric["key"]
                if key in found:
                    continue
                if any(re.search(p, label, re.I) for p in metric["patterns"]):
                    val = choose_metric_value(tokens, metric["prefer_percent"])
                    if val is not None:
                        found[key] = val
    return found


def _build_text_candidates(html: str) -> List[str]:
    # Basic tag-stripped text
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    candidates = [text] if text else []

    # Reconstruct text from absolutely-positioned divs (common in SEC converted PDFs)
    if "position:absolute" in html:
        div_re = re.compile(
            r'<div[^>]*style="[^"]*left:([0-9.]+)pt;[^"]*top:([0-9.]+)pt;[^"]*"[^>]*>(.*?)</div>',
            re.I | re.S,
        )
        positioned = []
        for left, top, inner in div_re.findall(html):
            try:
                x = float(left)
                y = float(top)
            except ValueError:
                continue
            # Strip nested tags from inner text
            inner_text = re.sub(r"<[^>]+>", " ", inner)
            inner_text = re.sub(r"\s+", " ", inner_text).strip()
            if inner_text:
                positioned.append((y, x, inner_text))
        if positioned:
            positioned.sort(key=lambda t: (t[0], t[1]))
            ordered_text = " ".join(p[2] for p in positioned)
            ordered_text = re.sub(r"\s+", " ", ordered_text).strip()
            if ordered_text:
                candidates.append(ordered_text)
    return candidates


def _extract_from_positioned(html: str, debug: bool = False) -> Dict[str, float]:
    metrics: Dict[str, float] = {}
    if "position:absolute" not in html:
        return metrics
    div_re = re.compile(
        r'<div[^>]*style="[^"]*left:([0-9.]+)pt;[^"]*top:([0-9.]+)pt;[^"]*"[^>]*>(.*?)</div>',
        re.I | re.S,
    )
    positioned = []
    for left, top, inner in div_re.findall(html):
        try:
            x = float(left)
            y = float(top)
        except ValueError:
            continue
        inner_text = re.sub(r"<[^>]+>", " ", inner)
        inner_text = re.sub(r"\s+", " ", inner_text).strip()
        if inner_text:
            positioned.append((y, x, inner_text))
    if not positioned:
        return metrics

    # Group positioned divs into lines by y coordinate, then build line text.
    positioned.sort(key=lambda t: (t[0], t[1]))
    lines = []
    cur = {"y": None, "items": []}
    for y, x, t in positioned:
        if cur["y"] is None or abs(y - cur["y"]) <= 2.0:
            if cur["y"] is None:
                cur["y"] = y
            cur["items"].append((x, t))
        else:
            lines.append(cur)
            cur = {"y": y, "items": [(x, t)]}
    if cur["items"]:
        lines.append(cur)
    for line in lines:
        line["items"].sort(key=lambda it: it[0])
        line["text"] = " ".join(t for _, t in line["items"])

    def _percents_in_line(line: dict) -> List[float]:
        percents: List[float] = []
        for _, t in line["items"]:
            for p in re.findall(r"(\d+(?:\.\d+)?)%", t):
                try:
                    percents.append(float(p))
                except ValueError:
                    continue
        return percents

    # Find delinquency trigger row: look for "Delinquency Trigger" and pull percents on same/next line.
    trigger_line = None
    for line in lines:
        if re.search(r"Delinquency\s*Trigger", line["text"], re.I):
            trigger_line = line
            break
    if trigger_line:
        percents = _percents_in_line(trigger_line)
        # If not on same line, check next line (60+ Delinquency row).
        if len(percents) < 2:
            idx = lines.index(trigger_line)
            if idx + 1 < len(lines):
                percents.extend(_percents_in_line(lines[idx + 1]))
        if len(percents) >= 2:
            metrics["delinquency_trigger_threshold"] = percents[0] / 100.0
            metrics["delinquency_60_plus"] = percents[1] / 100.0
    # Hard fallback: grab percents in the delinquency trigger band (y ~ 180-205, x high).
    if "delinquency_60_plus" not in metrics:
        band = []
        for y, x, t in positioned:
            if 180.0 <= y <= 205.0 and x >= 500 and "%" in t:
                for p in re.findall(r"(\d+(?:\.\d+)?)%", t):
                    band.append((y, float(p)))
        if len(band) >= 2:
            band.sort(key=lambda v: v[0])
            percents = [band[0][1], band[1][1]]
            metrics["delinquency_trigger_threshold"] = percents[0] / 100.0
            metrics["delinquency_60_plus"] = percents[1] / 100.0

    # Delinquency trigger occurred (Yes/No) line.
    if "delinquency_trigger_occurred" not in metrics:
        occurred_line = None
        for line in lines:
            if re.search(r"Delinquency\s*Trigger\s*occurred", line["text"], re.I):
                occurred_line = line
                break
        if occurred_line:
            text = occurred_line["text"]
            if re.search(r"\bYes\b", text, re.I):
                metrics["delinquency_trigger_occurred"] = 1.0
            elif re.search(r"\bNo\b", text, re.I):
                metrics["delinquency_trigger_occurred"] = 0.0

    # Derive total delinquency from Percentage row if present.
    perc_line = None
    dp_idx = None
    for i, line in enumerate(lines):
        if re.search(r"Delinquency\\s*Profile", line["text"], re.I):
            dp_idx = i
            break
    search_start = dp_idx if dp_idx is not None else 0
    perc_idx = None
    for i in range(search_start, min(len(lines), search_start + 40)):
        if re.search(r"\\bPercent(?:age)?\\b", lines[i]["text"], re.I):
            perc_line = lines[i]
            perc_idx = i
            break
    if perc_line:
        percents = _percents_in_line(perc_line)
        if not percents and perc_idx is not None:
            for j in range(perc_idx + 1, min(len(lines), perc_idx + 7)):
                percents.extend(_percents_in_line(lines[j]))
        if not percents and perc_idx is not None:
            # Fallback: grab percents in a vertical band to the right of the Percentage label.
            band = []
            y0 = perc_line["y"] if "y" in perc_line else lines[perc_idx]["y"]
            for y, x, t in positioned:
                if y0 <= y <= y0 + 80 and x >= 500 and "%" in t:
                    for p in re.findall(r"(\d+(?:\.\d+)?)%", t):
                        band.append(float(p))
            if band:
                percents = band
        if len(percents) >= 1:
            current_pct = percents[0] / 100.0
            metrics.setdefault("total_delinquency", max(0.0, 1.0 - current_pct))
            if "delinquency_60_plus" not in metrics and len(percents) >= 4:
                metrics["delinquency_60_plus"] = (percents[2] + percents[3]) / 100.0

    # Fallback: compute total delinquency from Amount row (current vs total).
    if "total_delinquency" not in metrics:
        amount_line = None
        start_idx = dp_idx if dp_idx is not None else 0
        for i in range(start_idx, min(len(lines), start_idx + 30)):
            line = lines[i]
            if re.search(r"\\bAmount\\b", line["text"], re.I) and re.search(r"\\d{1,3}(?:,\\d{3})+", line["text"]):
                amount_line = line
                break
        if amount_line:
            nums = re.findall(r"\\d{1,3}(?:,\\d{3})+(?:\\.\\d{2})", amount_line["text"])
            if len(nums) < 2:
                idx = lines.index(amount_line)
                for j in range(idx + 1, min(len(lines), idx + 7)):
                    nums.extend(re.findall(r"\\d{1,3}(?:,\\d{3})+(?:\\.\\d{2})", lines[j]["text"]))
            if len(nums) >= 2:
                try:
                    current_amt = float(nums[0].replace(",", ""))
                    total_amt = float(nums[-1].replace(",", ""))
                    if total_amt > 0:
                        metrics["total_delinquency"] = max(0.0, (total_amt - current_amt) / total_amt)
                except ValueError:
                    pass

    # Final fallback: use percentage column by position within the Delinquency Profile block.
    if "total_delinquency" not in metrics:
        dp_idx = None
        for i, line in enumerate(lines):
            if re.search(r"Delinquency\\s*Profile", line["text"], re.I):
                dp_idx = i
                break
        block = lines[dp_idx : min(len(lines), dp_idx + 60)] if dp_idx is not None else lines
        # Locate the Percentage column x-position inside the block.
        x_pct = None
        y_pct = None
        for line in block:
            for x, t in line["items"]:
                if re.fullmatch(r"Percent(?:age)?", t, re.I):
                    x_pct = x
                    y_pct = line["y"]
                    break
            if x_pct is not None:
                break
        # Locate y positions for delinquency rows using labels within the block.
        row_labels = {
            "current": r"^Current\\b",
            "31_60": r"31\\s*[-–]\\s*60",
            "61_90": r"61\\s*[-–]\\s*90",
            "91_120": r"91\\s*[-–]\\s*120",
            "total": r"^Total\\b",
        }
        row_ys = {}
        for line in block:
            for x, t in line["items"]:
                for key, pat in row_labels.items():
                    if re.search(pat, t, re.I):
                        row_ys[key] = line["y"]
        if x_pct is not None and row_ys:
            perc_by_row = {}
            for key, y0 in row_ys.items():
                candidates = []
                for y, x, t in positioned:
                    if "%" not in t:
                        continue
                    if abs(y - y0) > 3.5:
                        continue
                    if x < x_pct - 30 or x > x_pct + 120:
                        continue
                    for p in re.findall(r"(\d+(?:\.\d+)?)%", t):
                        try:
                            candidates.append(float(p))
                        except ValueError:
                            continue
                if candidates:
                    perc_by_row[key] = candidates[0]
            if perc_by_row:
                if "current" in perc_by_row:
                    current_pct = perc_by_row["current"] / 100.0
                    metrics["total_delinquency"] = max(0.0, 1.0 - current_pct)
                elif all(k in perc_by_row for k in ("31_60", "61_90", "91_120")):
                    metrics["total_delinquency"] = (
                        perc_by_row["31_60"] + perc_by_row["61_90"] + perc_by_row["91_120"]
                    ) / 100.0
                if "delinquency_60_plus" not in metrics and "61_90" in perc_by_row and "91_120" in perc_by_row:
                    metrics["delinquency_60_plus"] = (perc_by_row["61_90"] + perc_by_row["91_120"]) / 100.0

        # Last-resort: find percent on the same line as "Current" label.
        if "total_delinquency" not in metrics:
            current_line = None
            for line in block:
                if any(re.fullmatch(r"Current", t, re.I) for _, t in line["items"]):
                    current_line = line
                    break
            if current_line:
                percents = _percents_in_line(current_line)
                if percents:
                    current_pct = percents[-1] / 100.0
                    metrics["total_delinquency"] = max(0.0, 1.0 - current_pct)

        # If still missing, pick the first percent below the Percentage header in the column.
        if "total_delinquency" not in metrics and x_pct is not None and y_pct is not None:
            band = []
            for y, x, t in positioned:
                if y_pct + 5 <= y <= y_pct + 25 and abs(x - x_pct) <= 30 and "%" in t:
                    for p in re.findall(r"(\\d+(?:\\.\\d+)?)%", t):
                        try:
                            band.append(float(p))
                        except ValueError:
                            continue
            if band:
                band_sorted = [b for b in band if b < 100.0] or band
                current_pct = band_sorted[0] / 100.0
                metrics["total_delinquency"] = max(0.0, 1.0 - current_pct)

    # Cumulative loss ratio from cutoff date pool balance line (varied wording).
    if "cumulative_loss_ratio" not in metrics:
        cum_patterns = [
            r"Cumulative\s+Principal\s+Net\s+Loss.*Cutoff\s+Date\s+Pool\s+Balance",
            r"Cumulative\s+Net\s+Loss.*Cutoff\s+Date\s+Pool\s+Balance",
            r"Cumulative\s+Principal\s+Net\s+Loss.*Cutoff\s+Date\s+Pool",
            r"Cumulative\s+Net\s+Loss.*Cutoff\s+Date\s+Pool",
        ]
        cum_idx = None
        for i, line in enumerate(lines):
            if any(re.search(p, line["text"], re.I) for p in cum_patterns):
                cum_idx = i
                break
        if cum_idx is None:
            # Try a sliding window where the label is split across lines.
            for i in range(len(lines) - 2):
                window_text = " ".join(l["text"] for l in lines[i : i + 3])
                if any(re.search(p, window_text, re.I) for p in cum_patterns):
                    cum_idx = i
                    break
        if cum_idx is not None:
            percents = []
            for j in range(cum_idx, min(len(lines), cum_idx + 7)):
                percents.extend(_percents_in_line(lines[j]))
            if percents:
                metrics["cumulative_loss_ratio"] = percents[-1] / 100.0
    return metrics


def extract_metrics_from_text(html: str, debug: bool = False) -> Dict[str, float]:
    metrics: Dict[str, float] = {}
    texts = _build_text_candidates(html)
    positioned_metrics = _extract_from_positioned(html, debug=debug)
    metrics.update(positioned_metrics)

    def _strip_html(text: str) -> str:
        text = text.replace("&nbsp;", " ").replace("&#160;", " ")
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\{\d+\}", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _row_percents_from_cells(cells: List[str]) -> List[float]:
        vals = []
        percent_marker = False
        for cell in cells:
            if re.search(r"\d+(?:\.\d+)?\s*%", cell):
                for p in re.findall(r"(\d+(?:\.\d+)?)\s*%", cell):
                    try:
                        vals.append(float(p))
                    except ValueError:
                        continue
            if cell.strip() in {"%", "%\xa0"} or cell.strip().endswith("%"):
                percent_marker = True
        if not vals and percent_marker:
            nums = []
            for cell in cells:
                for n in re.findall(r"\d+(?:\.\d+)?", cell):
                    try:
                        nums.append(float(n))
                    except ValueError:
                        continue
            if nums:
                vals.append(nums[-1])
        return vals

    def _row_amounts(row_text: str) -> List[float]:
        vals = []
        for n in re.findall(r"\d{1,3}(?:,\d{3})+(?:\.\d{2})", row_text):
            try:
                vals.append(float(n.replace(",", "")))
            except ValueError:
                continue
        return vals

    def _extract_percent_after_patterns(text: str, patterns: List[str], window: int = 220) -> Optional[float]:
        for pat in patterns:
            match = re.search(pat, text, re.I)
            if not match:
                continue
            snippet = text[match.end() : match.end() + window]
            pcts = re.findall(r"(\d+(?:\.\d+)?)\s*%", snippet)
            if pcts:
                try:
                    return float(pcts[-1]) / 100.0
                except ValueError:
                    continue
        return None

    # Parse HTML rows directly (table-style filings).
    if "<tr" in html.lower():
        delinquency_block = 0
        over60_block = 0
        bucket_percents: Dict[str, float] = {}
        for row_html in re.findall(r"<tr[^>]*>.*?</tr>", html, re.I | re.S):
            cells = []
            for cell_html in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.I | re.S):
                cells.append(_strip_html(cell_html))
            row_text = " ".join(cells).strip()
            if not row_text:
                continue
            if re.search(r"Receivables\s+with\s+Scheduled\s+Payment\s+Delinquent", row_text, re.I):
                delinquency_block = 12
            elif re.search(r"Delinquent\s+Balances", row_text, re.I):
                delinquency_block = 12
            elif delinquency_block > 0:
                delinquency_block -= 1

            if re.search(r"Accounts\s+over\s+60", row_text, re.I) and re.search(r"Percent\s+Delinquent", row_text, re.I):
                over60_block = 6
            elif over60_block > 0:
                over60_block -= 1
                if "delinquency_60_plus" not in metrics:
                    m = re.search(r"Percent\s+Delinquent.*?(\d+(?:\.\d+)?)\s*%", row_text, re.I)
                    if m:
                        try:
                            metrics["delinquency_60_plus"] = float(m.group(1)) / 100.0
                        except ValueError:
                            pass
                    else:
                        pcts = _row_percents_from_cells(cells)
                        if pcts:
                            metrics["delinquency_60_plus"] = pcts[-1] / 100.0

            if "delinquency_trigger_threshold" not in metrics and re.search(r"\bDelinquency\s+Trigger\b", row_text, re.I):
                pcts = _row_percents_from_cells(cells)
                if pcts:
                    metrics["delinquency_trigger_threshold"] = pcts[-1] / 100.0
            if "delinquency_60_plus" not in metrics and re.search(
                r"Delinquency\s+Percentage\s+as\s+of\s+the\s+End\s+of\s+the\s+Collection\s+Period",
                row_text,
                re.I,
            ):
                pcts = _row_percents_from_cells(cells)
                if pcts:
                    metrics["delinquency_60_plus"] = pcts[-1] / 100.0
            if "delinquency_60_plus" not in metrics and re.search(
                r"60\s*\+[-–]?\s*Day\s+Delinquency\s+Rate",
                row_text,
                re.I,
            ):
                pcts = _row_percents_from_cells(cells)
                if pcts:
                    metrics["delinquency_60_plus"] = pcts[-1] / 100.0
            if "delinquency_trigger_threshold" not in metrics and re.search(
                r"Delinquency\s+Trigger\s+Rate",
                row_text,
                re.I,
            ):
                pcts = _row_percents_from_cells(cells)
                if pcts:
                    metrics["delinquency_trigger_threshold"] = pcts[-1] / 100.0
            if "total_delinquency" not in metrics and delinquency_block > 0 and re.search(r"\bTotal\b", row_text, re.I):
                pcts = _row_percents_from_cells(cells)
                if pcts:
                    metrics["total_delinquency"] = pcts[-1] / 100.0
            if "total_delinquency" not in metrics and re.search(
                r"Total\s+31\+\s+Delinquent\s+as\s+%.*(Aggregate\s+Ending\s+Principal\s+Balance|Ending\s+Pool\s+Balance)",
                row_text,
                re.I,
            ):
                pcts = _row_percents_from_cells(cells)
                if pcts:
                    metrics["total_delinquency"] = pcts[-1] / 100.0
            if "delinquency_60_plus" not in metrics and re.search(
                r"Total\s+61\+\s+Delinquent\s+as\s+%.*(Aggregate\s+Ending\s+Principal\s+Balance|Ending\s+Pool\s+Balance)",
                row_text,
                re.I,
            ):
                pcts = _row_percents_from_cells(cells)
                if pcts:
                    metrics["delinquency_60_plus"] = pcts[-1] / 100.0
            if "delinquency_trigger_occurred" not in metrics and re.search(
                r"Delinquency\s+Trigger\s+Occurred",
                row_text,
                re.I,
            ):
                if re.search(r"\bYES\b", row_text, re.I):
                    metrics["delinquency_trigger_occurred"] = 1.0
                elif re.search(r"\bNO\b", row_text, re.I):
                    metrics["delinquency_trigger_occurred"] = 0.0
            if delinquency_block > 0:
                if re.search(r"61\s*[-–]\s*90\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["61_90"] = pcts[-1]
                if re.search(r"91\s*[-–]\s*120\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["91_120"] = pcts[-1]
                if re.search(r"121\s*\+\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["121_plus"] = pcts[-1]
                if re.search(r"60\s*[-–]\s*89\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["60_89"] = pcts[-1]
                if re.search(r"90\s*[-–]\s*119\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["90_119"] = pcts[-1]
                if re.search(r"120\s*[-–]\s*149\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["120_149"] = pcts[-1]
                if re.search(r"150\s*[-–]\s*179\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["150_179"] = pcts[-1]
                if re.search(r"180\s*[-–]\s*or\s*more\s*days", row_text, re.I):
                    pcts = _row_percents_from_cells(cells)
                    if pcts:
                        bucket_percents["180_plus"] = pcts[-1]
            if "delinquency_60_plus" not in metrics and re.search(
                r"Aggregate\s+Principal\s+Balance\s+of\s+60\s+Day\s+Delinquent\s+Receivables",
                row_text,
                re.I,
            ):
                nums = _row_amounts(row_text)
                if nums and "pool_balance" in metrics and metrics["pool_balance"]:
                    metrics["delinquency_60_plus"] = max(0.0, nums[-1] / metrics["pool_balance"])
        if "delinquency_60_plus" not in metrics and bucket_percents:
            total_60 = (
                bucket_percents.get("61_90", 0.0)
                + bucket_percents.get("91_120", 0.0)
                + bucket_percents.get("121_plus", 0.0)
                + bucket_percents.get("60_89", 0.0)
                + bucket_percents.get("90_119", 0.0)
                + bucket_percents.get("120_149", 0.0)
                + bucket_percents.get("150_179", 0.0)
                + bucket_percents.get("180_plus", 0.0)
            )
            if total_60 > 0:
                metrics["delinquency_60_plus"] = total_60 / 100.0

    def pct_after(label_pat: str, window: int = 240) -> Optional[float]:
        for text in texts:
            m = re.search(label_pat, text, re.I)
            if not m:
                continue
            snippet = text[m.end(): m.end() + window]
            pcts = re.findall(r"(\d+(?:\.\d+)?)%", snippet)
            if not pcts:
                continue
            try:
                return float(pcts[-1]) / 100.0
            except ValueError:
                continue
        return None

    def pct_after_first(label_pat: str, window: int = 180) -> Optional[float]:
        for text in texts:
            m = re.search(label_pat, text, re.I)
            if not m:
                continue
            snippet = text[m.end() : m.end() + window]
            pcts = re.findall(r"(\d+(?:\.\d+)?)\s*%", snippet)
            if not pcts:
                continue
            try:
                return float(pcts[0]) / 100.0
            except ValueError:
                continue
        return None

    def amount_after(label_pat: str, window: int = 240) -> Optional[float]:
        for text in texts:
            m = re.search(label_pat, text, re.I)
            if not m:
                continue
            snippet = text[m.end(): m.end() + window]
            nums = re.findall(r"\d{1,3}(?:,\d{3})+(?:\.\d{2})", snippet)
            if not nums:
                continue
            try:
                return float(nums[0].replace(",", ""))
            except ValueError:
                continue
        return None

    def pct_line(pattern: str, window: int = 120) -> Optional[float]:
        for text in texts:
            m = re.search(pattern, text, re.I)
            if not m:
                continue
            snippet = text[m.start(): m.start() + window]
            pcts = re.findall(r"(\d+(?:\.\d+)?)\s*%", snippet)
            if pcts:
                try:
                    return float(pcts[-1]) / 100.0
                except ValueError:
                    continue
        return None

    def large_number_after(label_pat: str, window: int = 280) -> Optional[float]:
        for text in texts:
            m = re.search(label_pat, text, re.I)
            if not m:
                continue
            snippet = text[m.end(): m.end() + window]
            nums = re.findall(r"\d{1,3}(?:,\d{3})+(?:\.\d{2})", snippet)
            if not nums:
                continue
            vals = []
            for n in nums:
                try:
                    vals.append(float(n.replace(",", "")))
                except ValueError:
                    continue
            if vals:
                return max(vals)
        return None

    # Delinquency trigger row often includes threshold + current.
    for text in texts:
        m = re.search(
            r"Delinquency Trigger\s*60\+\s*Delinquency.*?(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%",
            text,
            re.I | re.S,
        )
        if m:
            try:
                metrics["delinquency_60_plus"] = float(m.group(2)) / 100.0
                break
            except ValueError:
                pass
        # Looser match: grab the last two percents after "Delinquency Trigger 60+ Delinquency".
        m2 = re.search(r"Delinquency Trigger\s*60\+\s*Delinquency(.*?)(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%", text, re.I | re.S)
        if m2:
            try:
                metrics["delinquency_60_plus"] = float(m2.group(3)) / 100.0
                break
            except ValueError:
                pass
    if "delinquency_trigger_threshold" not in metrics:
        th = pct_after(r"Delinquency\s+Trigger\b", window=160)
        if th is not None:
            metrics["delinquency_trigger_threshold"] = th
    if "delinquency_60_plus" not in metrics:
        # Common label for overall delinquency % at period end.
        dq_pct = pct_after(r"Delinquency\s+Percentage\s+as\s+of\s+the\s+End\s+of\s+the\s+Collection\s+Period", window=160)
        if dq_pct is not None:
            metrics["delinquency_60_plus"] = dq_pct

    # CNH-style trigger language with statistical contract value.
    if "delinquency_60_plus" not in metrics or "delinquency_trigger_threshold" not in metrics:
        for text in texts:
            m = re.search(
                r"End\s+of\s+Collection\s+Period\s+61\+\s+days\s+delinquent\s+Receivables\s+as\s+a\s+percentage\s+of\s+the\s+aggregate\s+Statistical\s+Contract\s+Value",
                text,
                re.I,
            )
            if not m:
                continue
            snippet = text[m.end() : m.end() + 300]
            pcts = re.findall(r"(\d+(?:\.\d+)?)%", snippet)
            if pcts:
                vals = []
                for p in pcts:
                    try:
                        vals.append(float(p))
                    except ValueError:
                        continue
                if vals:
                    if "delinquency_60_plus" not in metrics:
                        metrics["delinquency_60_plus"] = vals[0] / 100.0
                    if "delinquency_trigger_threshold" not in metrics and len(vals) >= 2:
                        metrics["delinquency_trigger_threshold"] = vals[-1] / 100.0
            break

    # Trigger occurred question format.
    if "delinquency_trigger_occurred" not in metrics:
        for text in texts:
            m = re.search(r"did\s+Delinquency\s+Trigger\s+occur", text, re.I)
            if not m:
                continue
            snippet = text[m.start() : m.start() + 200]
            if re.search(r"\bYES\b", snippet, re.I):
                metrics["delinquency_trigger_occurred"] = 1.0
            elif re.search(r"\bNO\b", snippet, re.I):
                metrics["delinquency_trigger_occurred"] = 0.0
            break
    if "delinquency_trigger_occurred" not in metrics:
        for text in texts:
            m = re.search(
                r"(?:\b\d+\.?\s*)?Has\s+a\s+Delinquency\s+Trigger\s+Event\s+occurred\??\s*(Yes|No)",
                text,
                re.I,
            )
            if m:
                metrics["delinquency_trigger_occurred"] = 1.0 if m.group(1).strip().lower() == "yes" else 0.0
                break

    # Delinquency Activity table (loan-level delinquency analysis).
    for text in texts:
        if "Delinquency Activity" not in text:
            continue
        if "total_delinquency" not in metrics:
            m = re.search(
                r"Delinquent\s+Loans\s+as\s+a\s+percentage\s+of\s+end\s+of\s+(?:the\s+)?period\s+Pool\s+Balance.*?(\d+(?:\.\d+)?)\s*%",
                text,
                re.I | re.S,
            )
            if m:
                try:
                    metrics["total_delinquency"] = float(m.group(1)) / 100.0
                except ValueError:
                    pass
        if "delinquency_trigger_occurred" not in metrics:
            m = re.search(r"Has\s+a\s+Delinquency\s+Trigger\s+Event\s+occurred\??\s*(Yes|No)", text, re.I)
            if m:
                metrics["delinquency_trigger_occurred"] = 1.0 if m.group(1).strip().lower() == "yes" else 0.0
        if "delinquency_60_plus" not in metrics:
            amounts = []
            for pat in [
                r"61\s*to\s*90\s*days\s*past\s*due",
                r"91\s*to\s*120\s*days\s*past\s*due",
                r"121\s*(?:or\s*more|\+)?\s*days\s*past\s*due",
            ]:
                m = re.search(pat + r".*?\$\s*([0-9,]+(?:\.\d{2})?)", text, re.I | re.S)
                if m:
                    try:
                        amounts.append(float(m.group(1).replace(",", "")))
                    except ValueError:
                        pass
            if amounts:
                pool_bal = metrics.get("pool_balance")
                if pool_bal is None:
                    m = re.search(r"end\s+of\s+(?:the\s+)?period\s+Pool\s+Balance.*?\$\s*([0-9,]+(?:\.\d{2})?)", text, re.I | re.S)
                    if m:
                        try:
                            pool_bal = float(m.group(1).replace(",", ""))
                            metrics["pool_balance"] = pool_bal
                        except ValueError:
                            pool_bal = None
                if pool_bal:
                    metrics["delinquency_60_plus"] = max(0.0, sum(amounts) / pool_bal)
        break

    # Nissan-style performance tests / asset representations triggers.
    # Example: "Asset Representations Review Delinquency Trigger PASS"
    if "delinquency_trigger_occurred" not in metrics:
        for text in texts:
            if re.search(r"Asset\s+Representations\s+Review\s+Delinquency\s+Trigger\s+PASS", text, re.I):
                metrics["delinquency_trigger_occurred"] = 0.0
                break
            if re.search(r"Asset\s+Representations\s+Review\s+Delinquency\s+Trigger\s+FAIL", text, re.I):
                metrics["delinquency_trigger_occurred"] = 1.0
                break

    if "delinquency_trigger_threshold" not in metrics or "delinquency_60_plus" not in metrics:
        for text in texts:
            m = re.search(
                r"Asset\s+Representations\s+Delinquency\s+Triggers(.*?)(?:Supplemental\s+Disclosures|\n\s*10\.|$)",
                text,
                re.I | re.S,
            )
            if not m:
                continue
            block = m.group(1)

            if "delinquency_trigger_threshold" not in metrics:
                m_th = re.search(r"Trigger\s+Level(.*?)(?:61\+\s*Delinquencies|Period\s*\d+|$)", block, re.I | re.S)
                if m_th:
                    pcts = re.findall(r"(\d+(?:\.\d+)?)\s*%", m_th.group(1))
                    if pcts:
                        try:
                            metrics["delinquency_trigger_threshold"] = float(pcts[-1]) / 100.0
                        except ValueError:
                            pass

            if "delinquency_60_plus" not in metrics:
                m_dq = re.search(
                    r"61\+\s*Delinquencies(.*?)(?:Period\s*\d+|Trigger\s+Level|Supplemental\s+Disclosures|$)",
                    block,
                    re.I | re.S,
                )
                if m_dq:
                    pcts = re.findall(r"(\d+(?:\.\d+)?)\s*%", m_dq.group(1))
                    if pcts:
                        try:
                            metrics["delinquency_60_plus"] = float(pcts[-1]) / 100.0
                        except ValueError:
                            pass
            break

    # Pool balance (end of collection period).
    pb = large_number_after(r"Pool Balance end of Collection Period")
    if pb is None:
        pb = amount_after(r"Pool Balance on the close of the last day of the related Collection Period")
    if pb is None:
        pb = amount_after(r"Pool Balance on the close of the last day of the preceding Collection Period")
    if pb is None:
        pb = amount_after(r"Pool Balance on the close of the last day of the Collection Period")
    if pb is not None:
        metrics["pool_balance"] = pb

    # Cumulative net loss as % of cutoff date pool balance.
    cl = pct_after(r"Cumulative Principal Net Loss / \(Gain\) as % of Cutoff Date Pool Balance")
    if cl is None:
        cl = pct_after(r"Cumulative Net Loss.*?% of Cutoff Date Pool Balance")
    if cl is not None:
        metrics["cumulative_loss_ratio"] = cl

    # Derive total delinquency and 60+ delinquency from the percentage row if present.
    for text in texts:
        m = re.search(
            r"Delinquency Profile.*?Percentage\s+(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%.*?100\.00%",
            text,
            re.I | re.S,
        )
        if m:
            try:
                current_pct = float(m.group(1)) / 100.0
                total_delinq = max(0.0, 1.0 - current_pct)
                metrics.setdefault("total_delinquency", total_delinq)
                if "delinquency_60_plus" not in metrics:
                    metrics["delinquency_60_plus"] = (float(m.group(3)) + float(m.group(4))) / 100.0
                break
            except ValueError:
                pass
        if "total_delinquency" not in metrics:
            # Fallback: first Percentage row anywhere after "Delinquency Profile".
            m2 = re.search(r"Delinquency Profile.*?Percentage\s+(\d+(?:\.\d+)?)%", text, re.I | re.S)
            if not m2:
                m2 = re.search(r"Percentage\s+(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%\s+(\d+(?:\.\d+)?)%", text, re.I | re.S)
            if m2:
                try:
                    current_pct = float(m2.group(1)) / 100.0
                    metrics["total_delinquency"] = max(0.0, 1.0 - current_pct)
                    if "delinquency_60_plus" not in metrics and m2.lastindex and m2.lastindex >= 4:
                        metrics["delinquency_60_plus"] = (float(m2.group(3)) + float(m2.group(4))) / 100.0
                    break
                except ValueError:
                    pass

    # Delinquency table layout (31-60 / 61-90 / 91-120 / 121+ / Total).
    dq_pct_direct = pct_after_first(
        r"Delinquency\s+Percentage\s+as\s+of\s+the\s+End\s+of\s+the\s+Collection\s+Period",
        window=220,
    )
    if dq_pct_direct is not None:
        # In Drive-style reports this row is the 60+ delinquency percentage.
        metrics["delinquency_60_plus"] = dq_pct_direct

    if "total_delinquency" not in metrics:
        total_pct = None
        for text in texts:
            m_total = re.search(r"(?:\{\s*76\s*\}\s*)?Total.*?(\d+(?:\.\d+)?)\s*%", text, re.I | re.S)
            if m_total:
                try:
                    total_pct = float(m_total.group(1)) / 100.0
                    break
                except ValueError:
                    pass
        if total_pct is None:
            total_pct = pct_line(r"Receivables\s+with\s+Scheduled\s+Payment\s+Delinquent.*?\bTotal\b", window=260)
        if total_pct is not None:
            metrics["total_delinquency"] = total_pct
    if "delinquency_60_plus" not in metrics:
        p61_90 = pct_after_first(r"61\s*[-–]\s*90\s*days", window=140)
        p91_120 = pct_after_first(r"91\s*[-–]\s*120\s*days", window=140)
        p121 = pct_after_first(r"121\s*(?:\+|plus)\s*days", window=140)
        if p61_90 is not None and p91_120 is not None:
            total_60 = p61_90 + p91_120 + (p121 or 0.0)
            metrics["delinquency_60_plus"] = total_60
    if "delinquency_trigger_threshold" not in metrics:
        th_pct = pct_after_first(r"\bDelinquency\s+Trigger\b", window=200)
        if th_pct is not None:
            metrics["delinquency_trigger_threshold"] = th_pct
    if "delinquency_trigger_occurred" not in metrics:
        for text in texts:
            m = re.search(r"Delinquency\s+Trigger\s+Occurred.*?\b(Yes|No)\b", text, re.I | re.S)
            if m:
                metrics["delinquency_trigger_occurred"] = 1.0 if m.group(1).strip().lower() == "yes" else 0.0
                break

    # AmeriCredit-style disclosure:
    # "Compliance (Trigger Violation is a Delinquency Rate Greater Than X.XX%) ... Yes/No"
    # where "Compliance Yes" means the trigger did NOT occur.
    for text in texts:
        m_th = re.search(
            r"Trigger\s+Violation\s+is\s+a\s+Delinquency\s+Rate\s+Greater\s+Than\s*(\d+(?:\.\d+)?)\s*%",
            text,
            re.I,
        )
        if not m_th:
            continue
        if "delinquency_trigger_threshold" not in metrics:
            try:
                metrics["delinquency_trigger_threshold"] = float(m_th.group(1)) / 100.0
            except ValueError:
                pass
        if "delinquency_trigger_occurred" not in metrics:
            tail = text[m_th.end() : m_th.end() + 260]
            m_comp = re.search(r"\b(Yes|No)\b", tail, re.I)
            if m_comp:
                # "Compliance Yes" => no trigger violation.
                metrics["delinquency_trigger_occurred"] = 0.0 if m_comp.group(1).strip().lower() == "yes" else 1.0
        break

    # Capital One-style layout:
    # "60+ Delinquencies as % of EOP Net Pool Balance" with
    # Third/Second/Preceding/Current Collection Period rows.
    for text in texts:
        m_anchor = re.search(
            r"60\+\s+Delinquenc(?:y|ies)\s+as\s+%\s+of\s+EOP\s+Net\s+Pool\s+Balance",
            text,
            re.I,
        )
        if not m_anchor:
            continue
        block = text[m_anchor.end() : m_anchor.end() + 1800]
        m_cur = re.search(r"Current\s+Collection\s+Period.*?(\d+(?:\.\d+)?)\s*%", block, re.I | re.S)
        if m_cur:
            try:
                metrics["delinquency_60_plus"] = float(m_cur.group(1)) / 100.0
            except ValueError:
                pass
        if "delinquency_trigger_threshold" not in metrics:
            m_th = re.search(r"Delinquency\s+Trigger.*?(\d+(?:\.\d+)?)\s*%", block, re.I | re.S)
            if m_th:
                try:
                    metrics["delinquency_trigger_threshold"] = float(m_th.group(1)) / 100.0
                except ValueError:
                    pass
        if "delinquency_trigger_occurred" not in metrics:
            m_occ = re.search(
                r"(?:Current\s+)?Delinquency\s+Percentage\s+Exceeds\s+Delinquency\s+Trigger.*?\b(Yes|No)\b",
                block,
                re.I | re.S,
            )
            if m_occ:
                metrics["delinquency_trigger_occurred"] = 1.0 if m_occ.group(1).strip().lower() == "yes" else 0.0
        break

    # Bridgecrest / DriveTime layout: "Receivables greater than 60 days delinquent at end of Collection Period"
    if "delinquency_60_plus" not in metrics:
        for text in texts:
            m = re.search(
                r"Receivables\s+greater\s+than\s+60\s+days\s+delinquent\s+at\s+end\s+of\s+(?:the\s+)?Collection\s+Period",
                text, re.I
            )
            if not m:
                continue
            snippet = text[m.end() : m.end() + 200]
            pct = re.search(r"(\d+(?:\.\d+)?)\s*%", snippet)
            if pct:
                try:
                    metrics["delinquency_60_plus"] = float(pct.group(1)) / 100.0
                    break
                except ValueError:
                    continue

    # Bridgecrest/DriveTime row label:
    # "Delinquency Trigger Rate (based on Current Collection Period)"
    if "delinquency_trigger_threshold" not in metrics:
        th_bridge = pct_after_first(
            r"Delinquency\s+Trigger\s+Rate\s*\(\s*based\s+on\s+Current\s+Collection\s+Period\s*\)",
            window=260,
        )
        if th_bridge is not None:
            metrics["delinquency_trigger_threshold"] = th_bridge

    # "Accounts over 60" + "Percent Delinquent" layout.
    if "delinquency_60_plus" not in metrics:
        for text in texts:
            m = re.search(r"Accounts\s+over\s+60.*?Percent\s+Delinquent", text, re.I | re.S)
            if not m:
                continue
            snippet = text[m.end() : m.end() + 120]
            pct = re.search(r"(\d+(?:\.\d+)?)\s*%", snippet)
            if pct:
                try:
                    metrics["delinquency_60_plus"] = float(pct.group(1)) / 100.0
                    break
                except ValueError:
                    continue

    # Aggregate principal balance of 60+ day delinquent receivables.
    if "delinquency_60_plus" not in metrics and "pool_balance" in metrics:
        amt = amount_after(r"Aggregate\s+Principal\s+Balance\s+of\s+60\s+Day\s+Delinquent\s+Receivables", window=200)
        if amt is not None and metrics["pool_balance"]:
            metrics["delinquency_60_plus"] = max(0.0, amt / metrics["pool_balance"])

    # Fallback: use delinquency percentage as total if still missing.
    if "total_delinquency" not in metrics:
        dq_pct = pct_after(r"Delinquency\s+Percentage\s+as\s+of\s+the\s+End\s+of\s+the\s+Collection\s+Period", window=160)
        if dq_pct is not None:
            metrics["total_delinquency"] = dq_pct

    # Monthly Payment Rate (MPR) for credit card trusts and similar.
    if "monthly_payment_rate" not in metrics:
        mpr_patterns = [
            r"Collections\s+of\s+Principal\s+Receivables\s+as\s+a\s+percentage\s+of\s+prior\s+month\s+Principal\s+Receivables",
            r"Principal\s+Payment\s+Rate",
            r"Monthly\s+Payment\s+Rate",
            r"\bMPR\b",
            r"Payment\s+Rate",
        ]
        for text in texts:
            cleaned = _strip_html(text)
            val = _extract_percent_after_patterns(cleaned, mpr_patterns)
            if val is not None:
                metrics["monthly_payment_rate"] = val
                break

    # Total collections rate (principal + finance charges).
    if "total_collections_rate" not in metrics:
        total_patterns = [
            r"Collections\s+as\s+a\s+percentage\s+of\s+prior\s+month\s+Principal\s+Receivables\s+and\s+Finance\s+Charge\s+Receivables",
            r"Total\s+Collections\s+Rate",
            r"Collections\s+Rate",
        ]
        for text in texts:
            cleaned = _strip_html(text)
            val = _extract_percent_after_patterns(cleaned, total_patterns)
            if val is not None:
                metrics["total_collections_rate"] = val
                break

    # If the Yes/No checkbox parsing is ambiguous, reconcile against the numeric test.
    dq_val = metrics.get("delinquency_60_plus")
    trig_val = metrics.get("delinquency_trigger_threshold")
    occ_val = metrics.get("delinquency_trigger_occurred")
    if isinstance(dq_val, (int, float)) and isinstance(trig_val, (int, float)) and trig_val > 0:
        implied = 1.0 if dq_val >= trig_val else 0.0
        if not isinstance(occ_val, (int, float)) or occ_val not in (0.0, 1.0) or occ_val != implied:
            metrics["delinquency_trigger_occurred"] = implied

    return metrics


def parse_ex102_xml(xml_text: str) -> Dict[str, float]:
    """
    Parse ABS-EE Exhibit 102 XML (loan-level tape) into pool-level metrics.

    Aggregates individual asset records to compute:
      pool_balance       - sum of ending loan balances
      delinquency_60_plus - (balance of 60+ DPD loans) / pool_balance

    Uses iterparse for memory-efficiency on large files (50k+ records).
    Handles various field naming conventions across issuers.
    """
    import xml.etree.ElementTree as ET

    def _tag(full: str) -> str:
        return full.split("}")[-1].lower() if "}" in full else full.lower()

    # Ending balance fields (various naming conventions)
    ENDING_BALANCE_FIELDS = frozenset([
        "reportingperiodendingloanbalance",
        "reportingperiodendingloanlbalance",  # seen in some schemas
        "reportingperiodendingbalance",
        "assetactualendofperiodbalance",
        "endingprincipalbalance",
        "currentprincipalbalance",
        "scheduledprincipalbalance",
    ])
    # Delinquency status fields (integer days or bucket code)
    DQ_STATUS_FIELDS = frozenset([
        "reportingperioddelinquencystatus",
        "currentdelinquencystatus",
        "delinquencystatus",
        "dayspastdue",
        "daysdelinquent",
        "scheduledpaymentsdelinquent",
    ])
    # Tags that wrap each individual asset record (repeating element)
    ASSET_RECORD_TAGS = frozenset([
        "autoasset", "asset", "assetdata", "loanleveldata",
        "autoloan", "receivable", "assetrecord", "autoloanasset",
        "loan", "loandata", "autoloanlevel",
    ])

    pool_balance = 0.0
    dq_60_balance = 0.0
    parsed_any = False

    cur_balance: Optional[float] = None
    cur_dq: Optional[float] = None
    record_depth: Optional[int] = None
    depth = 0

    try:
        xml_bytes = xml_text.encode("utf-8", errors="replace")
        context = ET.iterparse(BytesIO(xml_bytes), events=("start", "end"))
        for event, elem in context:
            tag = _tag(elem.tag)
            if event == "start":
                depth += 1
                # Record the depth of the first asset record container seen
                if tag in ASSET_RECORD_TAGS and record_depth is None:
                    record_depth = depth
            else:
                text = (elem.text or "").strip()
                if tag in ENDING_BALANCE_FIELDS and text:
                    try:
                        val = float(text.replace(",", "").replace("$", ""))
                        if val >= 0:
                            cur_balance = val
                    except ValueError:
                        pass
                elif tag in DQ_STATUS_FIELDS and text:
                    try:
                        cur_dq = float(text)
                    except ValueError:
                        # Handle string codes like "60-89", "90+", "3" (bucket number)
                        m = re.match(r"(\d+)", text)
                        if m:
                            raw = float(m.group(1))
                            # Bucket-encoded: 0=current,1=1-29,2=30-59,3=60-89,4=90-119,5=120+
                            # Map small bucket numbers to actual days
                            if raw <= 7 and "60" not in text and "90" not in text:
                                cur_dq = raw * 30.0
                            else:
                                cur_dq = raw
                elif tag in ASSET_RECORD_TAGS and record_depth is not None and depth == record_depth:
                    # End of an asset record at the expected depth
                    if cur_balance is not None and cur_balance > 0:
                        pool_balance += cur_balance
                        if cur_dq is not None and cur_dq >= 60:
                            dq_60_balance += cur_balance
                        parsed_any = True
                    cur_balance = None
                    cur_dq = None
                    elem.clear()
                depth -= 1
    except ET.ParseError:
        pass

    metrics: Dict[str, float] = {}
    if parsed_any and pool_balance > 0:
        metrics["pool_balance"] = pool_balance
        metrics["delinquency_60_plus"] = min(1.0, dq_60_balance / pool_balance)
    return metrics


def tables_to_text(tables: List[pd.DataFrame]) -> str:
    parts = []
    for df in tables:
        if df.empty:
            continue
        df2 = df.astype(str)
        for i in range(len(df2)):
            row = " | ".join(df2.iloc[i].tolist())
            if row.strip():
                parts.append(row)
    return "\n".join(parts)


def extract_metrics_via_llm(
    text: str,
    api_key: str,
    model: str,
    timeout: int = 60,
) -> Dict[str, float]:
    prompt = (
        "You are extracting metrics from a securitization trustee report.\n"
        "Return ONLY valid JSON with keys:\n"
        "pool_balance, total_delinquency, delinquency_60_plus, cumulative_loss_ratio.\n"
        "Values must be decimals (0.005 = 0.5%). Use null if missing.\n\n"
        f"TEXT:\n{text}"
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "input": prompt,
    }
    resp = requests.post("https://api.openai.com/v1/responses", headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    raw = data.get("output_text")
    if not raw:
        output = data.get("output", [])
        for item in output:
            for content in item.get("content", []):
                if content.get("type") == "output_text" and content.get("text"):
                    raw = content["text"]
                    break
            if raw:
                break
    if not raw:
        return {}
    match = re.search(r"\{.*\}", raw, re.S)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    out: Dict[str, float] = {}
    for key in ("pool_balance", "total_delinquency", "delinquency_60_plus", "cumulative_loss_ratio"):
        val = parsed.get(key)
        if val is None:
            continue
        try:
            out[key] = float(val)
        except (TypeError, ValueError):
            continue
    return out


def stddev(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


SCORE_WEIGHTS = {
    "cushion": 0.6,
    "trend3m": 0.2,
    "vol6m": 0.1,
    "macro": 0.1,
}
SCORE_SCALES = {
    "cushion_full": 0.20,
    "trend3m_full": 0.01,
    "vol6m_full": 0.005,
}


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def rule_based_score(
    cushion: float,
    change3m: float,
    vol6m: float,
    direction: str,
    macro_percentile: float,
) -> tuple[float, Dict[str, float]]:
    if cushion <= 0:
        return 1.0, {
            "cushion": 1.0,
            "trend3m": 0.0,
            "vol6m": 0.0,
            "macro": clamp01((macro_percentile - 0.5) / 0.5),
        }

    cushion_risk = 1.0 - min(cushion / SCORE_SCALES["cushion_full"], 1.0)
    adverse_change = change3m if direction == "<=" else -change3m
    trend_risk = clamp01(adverse_change / SCORE_SCALES["trend3m_full"])
    vol_risk = clamp01(vol6m / SCORE_SCALES["vol6m_full"])
    macro_risk = clamp01((macro_percentile - 0.5) / 0.5)

    score = (
        SCORE_WEIGHTS["cushion"] * cushion_risk
        + SCORE_WEIGHTS["trend3m"] * trend_risk
        + SCORE_WEIGHTS["vol6m"] * vol_risk
        + SCORE_WEIGHTS["macro"] * macro_risk
    )
    return clamp01(score), {
        "cushion": clamp01(cushion_risk),
        "trend3m": trend_risk,
        "vol6m": vol_risk,
        "macro": macro_risk,
    }


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def apply_scaler(features: Dict[str, float], scaler: Optional[dict]) -> Dict[str, float]:
    if not scaler:
        return features
    means = scaler.get("means", {})
    stds = scaler.get("stds", {})
    out = {}
    for key, val in features.items():
        mean = float(means.get(key, 0.0))
        std = float(stds.get(key, 1.0))
        out[key] = (val - mean) / std if std else 0.0
    return out


def logit_score(features: Dict[str, float], weights: Dict[str, float], intercept: float = 0.0, scaler: Optional[dict] = None) -> tuple[float, Dict[str, float]]:
    scaled = apply_scaler(features, scaler)
    linear_terms = {k: float(weights.get(k, 0.0)) * scaled.get(k, 0.0) for k in scaled.keys()}
    linear = intercept + sum(linear_terms.values())
    score = clamp01(sigmoid(linear))
    total = sum(abs(v) for v in linear_terms.values()) or 1.0
    breakdown = {k: min(1.0, abs(v) / total) for k, v in linear_terms.items()}
    return score, breakdown


def breach_probability(
    current: float,
    threshold: float,
    direction: str,
    mean_change: float,
    vol_change: float,
    months: int,
) -> float:
    if threshold == 0 or vol_change == 0:
        if direction == "<=":
            return 1.0 if current > threshold else 0.0
        return 1.0 if current < threshold else 0.0

    mu = current + mean_change * months
    sigma = vol_change * math.sqrt(months)
    if sigma == 0:
        return 0.0
    if direction == "<=":
        z = (threshold - mu) / sigma
        return max(0.0, min(1.0, 1.0 - normal_cdf(z)))
    z = (threshold - mu) / sigma
    return max(0.0, min(1.0, normal_cdf(z)))


def percent_rank(values: List[float], current: float) -> float:
    if not values:
        return 0.5
    if min(values) == max(values):
        return 0.5
    sorted_vals = sorted(values)
    rank = sum(1 for v in sorted_vals if v <= current)
    return max(0.0, min(1.0, rank / len(sorted_vals)))


def parse_quarter_token(token: str) -> Optional[dt.date]:
    if not token:
        return None
    s = str(token).strip().upper()
    m = re.search(r"(\d{4})\s*Q([1-4])", s)
    if not m:
        m = re.search(r"Q([1-4])\s*(\d{4})", s)
        if m:
            year = int(m.group(2))
            q = int(m.group(1))
        else:
            return None
    else:
        year = int(m.group(1))
        q = int(m.group(2))
    month = q * 3
    day = (dt.date(year, month, 1) + dt.timedelta(days=32)).replace(day=1) - dt.timedelta(days=1)
    return day


def parse_date_series(values: List[object]) -> List[Optional[dt.date]]:
    parsed: List[Optional[dt.date]] = []
    for v in values:
        if isinstance(v, dt.date):
            parsed.append(v)
            continue
        if isinstance(v, dt.datetime):
            parsed.append(v.date())
            continue
        s = str(v).strip()
        q = parse_quarter_token(s)
        if q:
            parsed.append(q)
            continue
        try:
            dt_val = dt.date.fromisoformat(s[0:10])
            parsed.append(dt_val)
            continue
        except Exception:
            pass
        try:
            dt_val = dt.datetime.fromisoformat(s).date()
            parsed.append(dt_val)
            continue
        except Exception:
            parsed.append(None)
    return parsed


def infer_date_col(df: pd.DataFrame) -> Optional[str]:
    for col in df.columns:
        parsed = parse_date_series(df[col].tolist())
        valid = sum(1 for v in parsed if v is not None)
        if valid >= max(5, int(len(parsed) * 0.5)):
            return col
    return None


def infer_date_columns_from_headers(columns: List[object]) -> Dict[str, dt.date]:
    out: Dict[str, dt.date] = {}
    for col in columns:
        d = parse_quarter_token(str(col))
        if not d:
            try:
                d = dt.date.fromisoformat(str(col)[0:10])
            except Exception:
                d = None
        if d:
            out[str(col)] = d
    return out


def find_latest_nyfed_hhdc_url(session: requests.Session, user_agent: str) -> Optional[str]:
    try:
        html = fetch_text(session, NYFED_HHDC_BACKGROUND, headers=sec_headers(user_agent, "www.newyorkfed.org"), sleep=0.1)
    except Exception:
        return None
    matches = re.findall(
        r"https://www\\.newyorkfed\\.org/medialibrary/[^\"']*hhd_c_report_\\d{4}q[1-4]\\.xlsx\\?sc_lang=en",
        html,
        re.I,
    )
    if not matches:
        files = re.findall(r"hhd_c_report_(\\d{4})q([1-4])\\.xlsx", html, re.I)
        if not files:
            return None
        year, q = max(((int(y), int(q)) for y, q in files), default=(0, 0))
        if year == 0:
            return None
        return f"{NYFED_HHDC_XLS_BASE}hhd_c_report_{year}q{q}.xlsx?sc_lang=en"
    def key(u: str) -> tuple[int, int]:
        m = re.search(r"hhd_c_report_(\\d{4})q([1-4])", u, re.I)
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    return sorted(matches, key=key)[-1]


def find_latest_local_hhdc(path: str = "src/data") -> Optional[str]:
    root = Path(path)
    if not root.exists():
        return None
    candidates = list(root.glob("HHD_C_Report_*.xlsx"))
    if not candidates:
        candidates = list(root.glob("hhd_c_report_*.xlsx"))
    if not candidates:
        return None

    def key(p: Path) -> tuple[int, int]:
        m = re.search(r"hhd[_-]?c[_-]?report[_-]?(\d{4})q([1-4])", p.name, re.I)
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

    return str(sorted(candidates, key=key)[-1])


def extract_macro_series_from_df(
    df: pd.DataFrame,
    value_pattern: str,
    date_col: Optional[str] = None,
    value_col: Optional[str] = None,
) -> Optional[List[tuple[dt.date, float]]]:
    df2 = df.copy()
    df2.columns = [str(c).strip() for c in df2.columns]
    df2 = df2.dropna(how="all")

    date_col = date_col or infer_date_col(df2)
    if date_col:
        if value_col is None:
            for col in df2.columns:
                if col == date_col:
                    continue
                if re.search(value_pattern, col, re.I):
                    value_col = col
                    break
        if value_col is None:
            numeric_cols = [c for c in df2.columns if c != date_col]
            for col in numeric_cols:
                if pd.to_numeric(df2[col], errors="coerce").notna().sum() >= max(5, int(len(df2) * 0.5)):
                    value_col = col
                    break
        if value_col:
            dates = parse_date_series(df2[date_col].tolist())
            values = pd.to_numeric(df2[value_col], errors="coerce").tolist()
            series = [(d, float(v)) for d, v in zip(dates, values) if d and v == v]
            return sorted(series, key=lambda x: x[0]) if series else None

    header_dates = infer_date_columns_from_headers(df2.columns.tolist())
    if header_dates:
        category_col = df2.columns[0]
        matches = df2[category_col].astype(str).str.contains(value_pattern, case=False, na=False)
        if matches.any():
            row = df2[matches].iloc[0]
            series = []
            for col, d in header_dates.items():
                v = row.get(col)
                try:
                    val = float(v)
                except Exception:
                    continue
                series.append((d, val))
            return sorted(series, key=lambda x: x[0]) if series else None

    return None


def load_macro_series(
    session: requests.Session,
    source: Optional[str],
    user_agent: str,
    sheet: Optional[str],
    date_col: Optional[str],
    value_col: Optional[str],
    value_pattern: str,
    window_months: Optional[int],
) -> Optional[List[tuple[dt.date, float]]]:
    if not source:
        return None
    source_resolved = source
    if source.lower() == "nyfed":
        local = find_latest_local_hhdc()
        if local:
            source_resolved = local
        else:
            latest = find_latest_nyfed_hhdc_url(session, user_agent)
            if not latest:
                raise SystemExit("Failed to resolve NY Fed HHDC download URL.")
            source_resolved = latest

    if re.match(r"^https?://", source_resolved, re.I):
        resp = session.get(source_resolved, headers=sec_headers(user_agent, "www.newyorkfed.org"), timeout=60)
        resp.raise_for_status()
        content = resp.content
        if source_resolved.lower().endswith((".xls", ".xlsx")):
            try:
                xls = pd.ExcelFile(BytesIO(content))
            except Exception as exc:
                raise SystemExit("Reading Excel macro source requires openpyxl. Install: pip install openpyxl") from exc
            sheets = [sheet] if sheet else xls.sheet_names
            series = None
            for sh in sheets:
                df = xls.parse(sh)
                series = extract_macro_series_from_df(df, value_pattern, date_col=date_col, value_col=value_col)
                if series:
                    break
        else:
            df = pd.read_csv(StringIO(resp.text))
            series = extract_macro_series_from_df(df, value_pattern, date_col=date_col, value_col=value_col)
    else:
        if source_resolved.lower().endswith((".xls", ".xlsx")):
            try:
                xls = pd.ExcelFile(source_resolved)
            except Exception as exc:
                raise SystemExit("Reading Excel macro source requires openpyxl. Install: pip install openpyxl") from exc
            sheets = [sheet] if sheet else xls.sheet_names
            series = None
            for sh in sheets:
                df = xls.parse(sh)
                series = extract_macro_series_from_df(df, value_pattern, date_col=date_col, value_col=value_col)
                if series:
                    break
        else:
            df = pd.read_csv(source_resolved)
            series = extract_macro_series_from_df(df, value_pattern, date_col=date_col, value_col=value_col)

    if not series:
        return None

    if window_months:
        cutoff = dt.date.today() - dt.timedelta(days=window_months * 31)
        series = [(d, v) for d, v in series if d >= cutoff]
    return series


def macro_value_for_date(series: List[tuple[dt.date, float]], target: dt.date) -> Optional[tuple[dt.date, float]]:
    if not series:
        return None
    eligible = [(d, v) for d, v in series if d <= target]
    if not eligible:
        return series[0]
    return max(eligible, key=lambda x: x[0])


def load_logit_config(spec: Optional[str]) -> Optional[dict]:
    if not spec:
        return None
    try:
        if spec.strip().startswith("{"):
            cfg = json.loads(spec)
        else:
            if not os.path.exists(spec):
                return None
            with open(spec, "r", encoding="utf-8") as f:
                cfg = json.load(f)
    except Exception as exc:
        raise SystemExit(f"Failed to load logit config: {exc}") from exc
    weights = cfg.get("weights") or cfg.get("coef") or cfg.get("coefficients") or {}
    return {
        "weights": {k: float(v) for k, v in weights.items()},
        "intercept": float(cfg.get("intercept", 0.0)),
        "scaler": cfg.get("scaler"),
    }


def export_training_rows(
    periods: List[str],
    features: List[Optional[Dict[str, float]]],
    outcomes: List[bool],
    horizon: int,
    meta: Dict[str, str],
    occurred: Optional[List[Optional[float]]] = None,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    count = min(len(features), len(outcomes), len(periods))
    for i in range(count - horizon):
        feat = features[i]
        if not feat:
            continue
        label = 1 if any(outcomes[i + 1 : i + 1 + horizon]) else 0
        occ_val = None
        if occurred is not None and i < len(occurred):
            occ_val = occurred[i]
        row = {
            "deal_id": meta.get("deal_id", ""),
            "cusip": meta.get("cusip", ""),
            "trigger_id": meta.get("trigger_id", ""),
            "metric": meta.get("metric", ""),
            "period_end": periods[i],
            "cushion": feat.get("cushion", 0.0),
            "trend3m": feat.get("trend3m", 0.0),
            "vol6m": feat.get("vol6m", 0.0),
            "macro": feat.get("macro", 0.0),
            "monthly_payment_rate": feat.get("monthly_payment_rate"),
            "total_collections_rate": feat.get("total_collections_rate"),
            "target_breach": label,
            "trigger_occurred": occ_val,
        }
        rows.append(row)
    return rows


def training_row_key(cusip: str, deal_id: str, trigger_id: str, period_end: str) -> tuple[str, str, str]:
    key_cusip = (cusip or "").strip()
    if not key_cusip or key_cusip.upper() in {"N/A", "NA", "—"}:
        key_cusip = f"DEAL:{(deal_id or '').strip()}"
    return (key_cusip, (period_end or "").strip(), (trigger_id or "").strip())


def filing_cache_key(deal_id: str, filing: FilingDoc) -> str:
    return "|".join(
        [
            (deal_id or "").strip(),
            (filing.period_end or "").strip(),
            (filing.accession_no or "").strip(),
            (filing.ex99_url or "").strip(),
        ]
    )


def month_label(date_str: str) -> str:
    try:
        d = dt.date.fromisoformat(date_str)
        return d.strftime("%b")
    except ValueError:
        return date_str


def load_config(path: str) -> List[dict]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if isinstance(cfg, dict):
        return cfg.get("deals", [])
    return cfg


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Deals config JSON")
    ap.add_argument(
        "--deal-id",
        action="append",
        help="Only process matching deal_id values (repeatable; commas allowed)",
    )
    ap.add_argument("--months", type=int, default=36)
    ap.add_argument("--out", default="out/trigger_monitor_demo.json")
    ap.add_argument(
        "--user-agent",
        default="Gurman Kaur gurmankdhaliwal2@gmail.com",
        help="SEC requires a real User-Agent",
    )
    ap.add_argument("--history", type=int, default=6, help="Months of history to chart")
    ap.add_argument("--debug", action="store_true", help="Print parsing diagnostics")
    ap.add_argument("--dump-dir", type=str, default=None, help="Write index/primary HTML/JSON for debugging")
    ap.add_argument("--cache-dir", default="out/sec_cache", help="Cache SEC responses (set to '' to disable)")
    ap.add_argument(
        "--refresh-submissions",
        action="store_true",
        help="Bypass cache for SEC submissions feeds (CIK*.json and shard files)",
    )
    ap.add_argument(
        "--public-copy",
        default="public/data/trigger_monitor_demo.json",
        help="Optional path to also write a public JSON copy for the web UI (set to '' to disable)",
    )
    ap.add_argument("--score-model", choices=["rule", "logit"], default="logit", help="Risk score model to use")
    ap.add_argument(
        "--logit-config",
        default="out/analysis/logit_config.json",
        help="Path to JSON or JSON string with logit weights/intercept/scaler",
    )
    ap.add_argument("--export-training-csv", help="Path to export training rows (CSV)")
    ap.add_argument(
        "--skip-existing-training",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip re-parsing filings already represented in training CSV (uses parsed cache when available)",
    )
    ap.add_argument(
        "--parsed-cache",
        default="out/parsed_filing_metrics.json",
        help="Cache parsed filing metrics to avoid re-parsing (set to '' to disable)",
    )
    ap.add_argument("--label-horizon", type=int, default=6, help="Label horizon in months/filings for training export")
    ap.add_argument("--csv-only", action="store_true", help="Skip JSON output and only write training CSV")
    ap.add_argument("--validation-report", default="out/validation_report.json", help="Write validation report JSON (set to '' to disable)")
    ap.add_argument("--qa-report", default="out/qa_summary.json", help="Write QA summary for skipped rows (set to '' to disable)")
    ap.add_argument("--llm-fallback", action="store_true", help="Use OpenAI API to extract metrics if regex fails")
    ap.add_argument("--llm-model", help="OpenAI model for fallback extraction (e.g. gpt-4.1-mini)")
    ap.add_argument("--llm-max-chars", type=int, default=6000, help="Max chars of table text sent to LLM")
    ap.add_argument("--llm-timeout", type=int, default=60, help="Timeout for LLM extraction (seconds)")
    ap.add_argument("--macro-source", help="Path/URL to macro series file, or 'nyfed' to auto-fetch HHDC data")
    ap.add_argument("--macro-sheet", help="Excel sheet name for macro series (optional)")
    ap.add_argument("--macro-date-col", help="Date/Quarter column name for macro series (optional)")
    ap.add_argument("--macro-value-col", help="Value column name for macro series (optional)")
    ap.add_argument(
        "--macro-value-pattern",
        default=r"auto.*(serious|90|90\\+|delinquen)",
        help="Regex to locate macro series in the file",
    )
    ap.add_argument("--macro-window-months", type=int, default=None, help="Limit macro series to last N months")
    ap.add_argument("--no-progress", action="store_true", help="Disable periodic progress logs")
    args = ap.parse_args()

    # If running a subset config or a non-default output, avoid overwriting the main public/QA outputs
    # unless the user explicitly provided paths.
    default_out = "out/trigger_monitor_demo.json"
    default_public = "public/data/trigger_monitor_demo.json"
    default_qa = "out/qa_summary.json"
    if args.out != default_out and args.public_copy == default_public:
        args.public_copy = str(Path("public/data") / Path(args.out).name)
    config_name = Path(args.config).name if args.config else ""
    if config_name and config_name != "sec_demo_deals.json" and args.qa_report == default_qa:
        stem = Path(args.config).stem
        args.qa_report = f"out/qa_summary_{stem}.json"

    global CACHE_DIR
    CACHE_DIR = args.cache_dir if args.cache_dir else None

    progress_enabled = not args.no_progress

    def progress(msg: str) -> None:
        if progress_enabled:
            print(msg, flush=True)

    deals_cfg = load_config(args.config)
    if args.deal_id:
        wanted: set[str] = set()
        for raw in args.deal_id:
            if not raw:
                continue
            for part in str(raw).split(","):
                p = part.strip()
                if p:
                    wanted.add(p)
        if wanted:
            deals_cfg = [
                d for d in deals_cfg
                if (d.get("deal_id") or d.get("deal") or "").strip() in wanted
            ]
            if not deals_cfg:
                raise SystemExit(
                    f"No matching deal_id values found for --deal-id: {', '.join(sorted(wanted))}"
                )
    progress(f"Starting build: {len(deals_cfg)} deals from {args.config}")
    session = requests.Session()
    macro_series = load_macro_series(
        session,
        source=args.macro_source,
        user_agent=args.user_agent,
        sheet=args.macro_sheet,
        date_col=args.macro_date_col,
        value_col=args.macro_value_col,
        value_pattern=args.macro_value_pattern,
        window_months=args.macro_window_months,
    )
    logit_cfg = load_logit_config(args.logit_config)
    if args.score_model == "logit" and not logit_cfg:
        print("Warning: --score-model logit set but no --logit-config provided; falling back to rule-based scoring.")
    if args.llm_fallback and not args.llm_model:
        print("Warning: --llm-fallback set but no --llm-model provided; LLM extraction will be skipped.")

    deals_out: List[dict] = []
    alerts: List[dict] = []
    all_as_of: List[str] = []
    training_rows: List[Dict[str, float]] = []
    llm_warned = False
    validation_rows: List[dict] = []
    qa_missing_rows: List[dict] = []
    qa_full_map: Dict[tuple, dict] = {}
    debug_missing_rows: List[dict] = []

    existing_training_keys: set[tuple[str, str, str]] = set()
    training_reference_csv = args.export_training_csv or "out/trigger_training_data.csv"
    if args.skip_existing_training and training_reference_csv and os.path.exists(training_reference_csv):
        try:
            with open(training_reference_csv, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = training_row_key(
                        row.get("cusip", ""),
                        row.get("deal_id", ""),
                        row.get("trigger_id", ""),
                        row.get("period_end", ""),
                    )
                    existing_training_keys.add(key)
        except Exception:
            existing_training_keys = set()

    parsed_cache: Dict[str, dict] = {}
    parsed_cache_dirty = False
    if args.parsed_cache and str(args.parsed_cache).strip() and os.path.exists(args.parsed_cache):
        try:
            with open(args.parsed_cache, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                parsed_cache = loaded
        except Exception:
            parsed_cache = {}

    def make_qa_row(
        deal_id: str,
        trigger_id: str,
        period_end: str,
        source_url: str,
        missing_metrics: List[str],
        reason: str,
        primary_doc_url: Optional[str] = None,
        ex99_url: Optional[str] = None,
    ) -> dict:
        def status_from_reason(row_reason: str) -> str:
            if row_reason == "parsed":
                return "parsed"
            if row_reason == "skipped_existing":
                return "skipped_existing"
            return "missing"

        if primary_doc_url:
            source_url = primary_doc_url
        elif not source_url and ex99_url:
            source_url = ex99_url
        filename = source_url.rstrip("/").split("/")[-1] if source_url else ""
        return {
            "deal_id": deal_id,
            "trigger_id": trigger_id,
            "period_end": period_end,
            "source_url": source_url,
            "primary_doc_url": primary_doc_url or "",
            "ex99_url": ex99_url or "",
            "ex99_filename": filename,
            "missing_metrics": sorted(set(missing_metrics)),
            "status": status_from_reason(reason),
            "reason": reason,
        }

    def merge_reason(existing: str, new_reason: str) -> str:
        priority = {
            "no_ex99": 6,
            "no_metrics": 5,
            "missing_metric": 4,
            "insufficient_history": 3,
            "macro_missing": 3,
            "parsed": 1,
            "skipped_existing": 0,
        }
        if not existing:
            return new_reason
        if priority.get(new_reason, 2) > priority.get(existing, 2):
            return new_reason
        return existing

    def record_full_row(
        deal_id: str,
        trigger_id: str,
        period_end: str,
        source_url: str,
        missing_metrics: List[str],
        reason: str,
        primary_doc_url: Optional[str] = None,
        ex99_url: Optional[str] = None,
    ) -> None:
        key = (deal_id, trigger_id, period_end)
        row = qa_full_map.get(key)
        new_row = make_qa_row(
            deal_id=deal_id,
            trigger_id=trigger_id,
            period_end=period_end,
            source_url=source_url,
            missing_metrics=missing_metrics,
            reason=reason,
            primary_doc_url=primary_doc_url,
            ex99_url=ex99_url,
        )
        if not row:
            qa_full_map[key] = new_row
            return
        # Merge into existing row.
        row["missing_metrics"] = sorted(set(row.get("missing_metrics", [])) | set(new_row.get("missing_metrics", [])))
        row["reason"] = merge_reason(row.get("reason", ""), reason)
        row["status"] = new_row.get("status", row.get("status", "missing"))
        if not row.get("source_url") and new_row.get("source_url"):
            row["source_url"] = new_row["source_url"]
        if not row.get("primary_doc_url") and new_row.get("primary_doc_url"):
            row["primary_doc_url"] = new_row["primary_doc_url"]
        if not row.get("ex99_url") and new_row.get("ex99_url"):
            row["ex99_url"] = new_row["ex99_url"]
        if not row.get("ex99_filename") and new_row.get("ex99_filename"):
            row["ex99_filename"] = new_row["ex99_filename"]

    def record_missing_row(
        deal_id: str,
        trigger_id: str,
        period_end: str,
        source_url: str,
        missing_metrics: List[str],
        reason: str,
        primary_doc_url: Optional[str] = None,
        ex99_url: Optional[str] = None,
    ) -> None:
        if not args.qa_report:
            return
        row = make_qa_row(
            deal_id=deal_id,
            trigger_id=trigger_id,
            period_end=period_end,
            source_url=source_url,
            missing_metrics=missing_metrics,
            reason=reason,
            primary_doc_url=primary_doc_url,
            ex99_url=ex99_url,
        )
        qa_missing_rows.append(row)
        record_full_row(
            deal_id=deal_id,
            trigger_id=trigger_id,
            period_end=period_end,
            source_url=source_url,
            missing_metrics=missing_metrics,
            reason=reason,
            primary_doc_url=primary_doc_url,
            ex99_url=ex99_url,
        )
        debug_missing_rows.append(row)

    def filing_source_url(filing: FilingDoc) -> str:
        return filing.primary_doc_url or filing.ex99_url or filing.index_url or ""

    total_deals = len(deals_cfg)
    for deal_idx, deal in enumerate(deals_cfg, start=1):
        deal_id = deal.get("deal_id") or deal.get("deal") or "Unknown Deal"
        cik = str(deal.get("cik", "")).zfill(10)
        triggers_cfg = deal.get("triggers") or []
        threshold_override = deal.get("trigger_threshold_override")
        force_override = deal.get("force_threshold_override")
        threshold_schedule = parse_threshold_schedule(deal.get("trigger_threshold_schedule"))
        threshold_schedule_mode = normalize_threshold_schedule_mode(
            deal.get("trigger_threshold_schedule_mode")
        )
        progress(f"[{deal_idx}/{total_deals}] {deal_id} (CIK {cik})")

        overrides = deal.get("filings_override") or []
        if overrides:
            filings = filings_from_overrides(
                session,
                overrides=overrides,
                user_agent=args.user_agent,
                debug=args.debug,
            )
        else:
            try:
                filings = list_recent_10d_ex99(
                    session,
                    cik=cik,
                    months=args.months,
                    user_agent=args.user_agent,
                    deal_name=deal_id,
                    debug=args.debug,
                    dump_dir=args.dump_dir,
                    refresh_submissions=args.refresh_submissions,
                )
            except requests.RequestException as exc:
                filings = []
        progress(f"  filings: {len(filings)}")
        series: List[dict] = []
        if args.qa_report and not filings:
            index_rows = list_recent_10d_index_urls(
                session,
                cik=cik,
                months=args.months,
                user_agent=args.user_agent,
                debug=args.debug,
                refresh_submissions=args.refresh_submissions,
            )
            if index_rows:
                for idx in index_rows:
                    for trig in triggers_cfg:
                        record_missing_row(
                            deal_id=deal_id,
                            trigger_id=trig.get("trigger_id", "TRIGGER"),
                            period_end=idx.get("period_end", ""),
                            source_url=idx.get("index_url", ""),
                            missing_metrics=["ex99"],
                            reason="no_ex99",
                        )
            else:
                for trig in triggers_cfg:
                    record_missing_row(
                        deal_id=deal_id,
                        trigger_id=trig.get("trigger_id", "TRIGGER"),
                        period_end="",
                        source_url="",
                        missing_metrics=["ex99"],
                        reason="no_ex99",
                    )

        total_filings = len(filings)
        for filing_idx, filing in enumerate(filings, start=1):
            if filing_idx == 1 or filing_idx == total_filings or filing_idx % 10 == 0:
                progress(f"  parsing filing {filing_idx}/{total_filings} ({filing.period_end})")
            cache_key = filing_cache_key(deal_id, filing)
            cached = parsed_cache.get(cache_key) if parsed_cache else None
            if cached and isinstance(cached, dict) and cached.get("metrics"):
                metrics = cached.get("metrics", {})
                complete = bool(cached.get("complete", False))
                source_url_cached = cached.get("source_url") or filing_source_url(filing)
                reported_threshold_cached = _safe_float(cached.get("threshold_reported"))
                schedule_threshold_cached = resolve_schedule_threshold(
                    threshold_schedule,
                    filing.period_end,
                    filing_idx,
                )

                # Repair stale cached parser artifacts (e.g., 60+ DQ parsed as ~0.6 instead of 0.006/0.001)
                # by re-reading EX-99 text from cache and re-running text extraction when values are implausible.
                dq_cached = _safe_float(metrics.get("delinquency_60_plus"))
                th_cached = _safe_float(metrics.get("delinquency_trigger_threshold"))
                needs_dq_repair = (
                    dq_cached is not None
                    and dq_cached > 0.25
                    and (th_cached is None or th_cached <= 0.10)
                    and bool(filing.ex99_url)
                )
                needs_threshold_repair = (
                    bool(filing.ex99_url)
                    and th_cached is not None
                    and 0.0 < th_cached <= 0.05
                    and dq_cached is not None
                    and dq_cached == 0.0
                )
                if (needs_dq_repair or needs_threshold_repair) and filing.ex99_url:
                    html_cached = _cache_read_text(filing.ex99_url)
                    if not html_cached:
                        try:
                            html_cached = fetch_text(
                                session,
                                filing.ex99_url,
                                headers=sec_headers(args.user_agent, "www.sec.gov"),
                                sleep=0.0,
                            )
                        except Exception:
                            html_cached = None
                    if html_cached:
                        text_metrics_cached = extract_metrics_from_text(html_cached, debug=False)
                        dq_text = _safe_float(text_metrics_cached.get("delinquency_60_plus"))
                        if dq_text is not None and 0.0 <= dq_text <= 0.25 and (
                            dq_cached is None
                            or (dq_cached > 0.25 and dq_text <= 0.25)
                            or (dq_cached == 0.0 and dq_text > 0.0)
                        ):
                            metrics["delinquency_60_plus"] = dq_text
                            if args.parsed_cache and str(args.parsed_cache).strip():
                                parsed_cache_dirty = True
                        th_text = _safe_float(text_metrics_cached.get("delinquency_trigger_threshold"))
                        th_cur = _safe_float(metrics.get("delinquency_trigger_threshold"))
                        if th_text is not None:
                            should_replace_threshold = (
                                th_cur is None
                                or (th_cur > 1.0 and 0.0 < th_text <= 0.30)
                                or (
                                    0.0 < th_cur <= 0.10
                                    and 0.0 < th_text <= 0.30
                                    and abs(th_text - th_cur) >= 0.01
                                    and th_text >= (th_cur * 1.5)
                                )
                            )
                            if should_replace_threshold:
                                metrics["delinquency_trigger_threshold"] = th_text
                                reported_threshold_cached = th_text
                                if args.parsed_cache and str(args.parsed_cache).strip():
                                    parsed_cache_dirty = True
                        if metrics.get("delinquency_trigger_occurred") is None and text_metrics_cached.get("delinquency_trigger_occurred") is not None:
                            metrics["delinquency_trigger_occurred"] = float(text_metrics_cached.get("delinquency_trigger_occurred"))
                            if args.parsed_cache and str(args.parsed_cache).strip():
                                parsed_cache_dirty = True

                threshold_flags_cached = apply_configured_threshold(
                    metrics=metrics,
                    threshold_override=threshold_override,
                    force_override=force_override,
                    schedule_threshold=schedule_threshold_cached,
                    schedule_mode=threshold_schedule_mode,
                )

                # Keep trigger occurrence internally consistent on cached rows.
                dq_cached = _safe_float(metrics.get("delinquency_60_plus"))
                final_threshold_cached = _safe_float(metrics.get("delinquency_trigger_threshold"))
                dq_occ_cached = _safe_float(metrics.get("delinquency_trigger_occurred"))
                if dq_cached is not None and final_threshold_cached is not None and final_threshold_cached > 0:
                    implied_occ = 1.0 if dq_cached >= final_threshold_cached else 0.0
                    if dq_occ_cached not in (0.0, 1.0) or dq_occ_cached != implied_occ:
                        metrics["delinquency_trigger_occurred"] = implied_occ
                        if args.parsed_cache and str(args.parsed_cache).strip():
                            parsed_cache_dirty = True

                # Backfill threshold-reported metadata for old cache entries.
                if (
                    reported_threshold_cached is None
                    and not threshold_flags_cached.get("schedule_applied", False)
                    and threshold_override is None
                    and force_override is None
                ):
                    reported_threshold_cached = final_threshold_cached
                elif (
                    final_threshold_cached is not None
                    and threshold_override is None
                    and force_override is None
                    and not threshold_flags_cached.get("schedule_applied", False)
                    and abs(reported_threshold_cached - final_threshold_cached) >= 0.01
                ):
                    # Keep reported threshold aligned with repaired parser output
                    # when no explicit config override is in play.
                    reported_threshold_cached = final_threshold_cached
                threshold_source_cached = resolve_threshold_source(
                    reported_threshold=reported_threshold_cached,
                    final_threshold=final_threshold_cached,
                    threshold_override=threshold_override,
                    force_override=force_override,
                    schedule_threshold=schedule_threshold_cached,
                    schedule_mode=threshold_schedule_mode,
                    schedule_applied=threshold_flags_cached.get("schedule_applied", False),
                    schedule_override_applied=threshold_flags_cached.get("schedule_override_applied", False),
                )
                if args.parsed_cache and str(args.parsed_cache).strip() and parsed_cache is not None and cache_key in parsed_cache:
                    parsed_cache[cache_key]["metrics"] = metrics
                    parsed_cache[cache_key]["threshold_source"] = threshold_source_cached
                    parsed_cache[cache_key]["threshold_reported"] = reported_threshold_cached
                if args.validation_report:
                    missing = [k for k in REQUIRED_METRICS if k not in metrics or metrics[k] is None]
                    if metrics.get("delinquency_trigger_threshold") is None:
                        missing.append("delinquency_trigger_threshold")
                    validation_rows.append({
                        "deal_id": deal_id,
                        "period_end": filing.period_end,
                        "accession_no": filing.accession_no,
                        "source_url": source_url_cached,
                        "status": "cached",
                        "missing_metrics": missing,
                        "extracted_metrics": sorted(metrics.keys()),
                    })
                if args.qa_report:
                    for trig in triggers_cfg:
                        record_full_row(
                            deal_id=deal_id,
                            trigger_id=trig.get("trigger_id", "TRIGGER"),
                            period_end=filing.period_end,
                            source_url=source_url_cached,
                            missing_metrics=[],
                            reason="parsed",
                            primary_doc_url=filing.primary_doc_url,
                            ex99_url=filing.ex99_url,
                        )
                series.append({
                    "period_end": filing.period_end,
                    "metrics": metrics,
                    "source_url": source_url_cached,
                    "primary_doc_url": filing.primary_doc_url or "",
                    "ex99_url": filing.ex99_url or "",
                    "accession_no": filing.accession_no,
                    "complete": complete,
                    "threshold_source": threshold_source_cached,
                    "threshold_reported": reported_threshold_cached,
                })
                continue

            should_skip_existing = False
            if args.skip_existing_training and existing_training_keys and triggers_cfg:
                row_keys = [
                    training_row_key(
                        deal.get("cusip", ""),
                        deal_id,
                        trig.get("trigger_id", "TRIGGER"),
                        filing.period_end,
                    )
                    for trig in triggers_cfg
                ]
                should_skip_existing = bool(row_keys and all(k in existing_training_keys for k in row_keys))
                if should_skip_existing:
                    # If this filing is already represented in training data but has no parsed cache row,
                    # parse once so UI/QA outputs remain complete and future runs can reuse cache.
                    progress(f"  training-covered filing {filing.period_end}; parsing once to populate cache")
            try:
                html = fetch_text(session, filing.ex99_url, headers=sec_headers(args.user_agent, "www.sec.gov"), sleep=0.25)
            except requests.HTTPError as exc:
                progress(f"  skipped filing {filing.period_end}: failed to fetch EX-99")
                continue
            if args.dump_dir:
                try:
                    os.makedirs(args.dump_dir, exist_ok=True)
                    filename = filing.ex99_url.rstrip("/").split("/")[-1]
                    with open(os.path.join(args.dump_dir, f"ex99_{filing.period_end}_{filename}"), "w", encoding="utf-8") as f:
                        f.write(html)
                except Exception:
                    pass
            try:
                tables = pd.read_html(StringIO(html))
            except ValueError:
                tables = []
            metrics = extract_metrics_from_tables(tables, debug=args.debug)
            if html:
                text_metrics = extract_metrics_from_text(html, debug=args.debug)

                def should_prefer_text_metric(key: str, current_val: object, text_val: object) -> bool:
                    if not isinstance(current_val, (int, float)) or not isinstance(text_val, (int, float)):
                        return False
                    # Guard against table-parser scale errors (e.g., 0.60 interpreted as 60%)
                    # when text parsing has a plausible percentage for delinquency fields.
                    if key in ("delinquency_60_plus", "total_delinquency"):
                        if text_val <= 0.10 and current_val >= (text_val + 0.10) and current_val >= (text_val * 5.0):
                            return True
                        if current_val > 0.35 and text_val <= 0.20:
                            return True
                        if current_val > 1.0 and text_val <= 1.0:
                            return True
                    if key == "delinquency_trigger_threshold":
                        if current_val > 1.0 and 0.0 < text_val <= 0.30:
                            return True
                        if (
                            0.0 < current_val <= 0.10
                            and 0.0 < text_val <= 0.30
                            and abs(text_val - current_val) >= 0.01
                            and text_val >= (current_val * 1.5)
                        ):
                            return True
                    return False

                for key, val in text_metrics.items():
                    if val is None:
                        continue
                    if key not in metrics or should_prefer_text_metric(key, metrics.get(key), val):
                        metrics[key] = val
            missing_keys = [k for k in ("pool_balance", "total_delinquency", "delinquency_60_plus", "cumulative_loss_ratio") if k not in metrics]
            if (not metrics or missing_keys) and args.llm_fallback and args.llm_model:
                api_key = os.environ.get("OPENAI_API_KEY", "")
                if not api_key:
                    if not llm_warned:
                        print("Warning: --llm-fallback set but OPENAI_API_KEY is not defined.")
                        llm_warned = True
                else:
                    text = tables_to_text(tables)
                    if not text:
                        text = re.sub(r"<[^>]+>", " ", html or "")
                        text = re.sub(r"\s+", " ", text)
                    if text:
                        text = text[: args.llm_max_chars]
                        try:
                            llm_metrics = extract_metrics_via_llm(
                                text=text,
                                api_key=api_key,
                                model=args.llm_model,
                                timeout=args.llm_timeout,
                            )
                            if llm_metrics:
                                for key, val in llm_metrics.items():
                                    if key not in metrics and val is not None:
                                        metrics[key] = val
                        except Exception as exc:
                            pass

            # --- Exhibit 102 XML fallback (ABS-EE loan-level tape) ---
            # Try Ex102 when pool_balance or delinquency_60_plus is still missing.
            if ("pool_balance" not in metrics or "delinquency_60_plus" not in metrics) and filing.ex102_url:
                try:
                    ex102_text = fetch_text(
                        session,
                        filing.ex102_url,
                        headers=sec_headers(args.user_agent, "www.sec.gov"),
                        sleep=0.25,
                    )
                    if ex102_text:
                        ex102_metrics = parse_ex102_xml(ex102_text)
                        for key, val in ex102_metrics.items():
                            if key not in metrics:
                                metrics[key] = val
                        if args.debug and ex102_metrics:
                            print(f"  Ex102 added: {list(ex102_metrics.keys())}")
                except Exception:
                    pass

            # --- Deal-level trigger threshold override ---
            # trigger_threshold_override: inject when Ex99.1 doesn't report the threshold.
            # force_threshold_override: always use config value, even if Ex99.1 reported something
            #   (useful when the parser picks up the wrong metric as the trigger threshold).
            reported_threshold = _safe_float(metrics.get("delinquency_trigger_threshold"))
            schedule_threshold = resolve_schedule_threshold(
                threshold_schedule,
                filing.period_end,
                filing_idx,
            )
            threshold_flags = apply_configured_threshold(
                metrics=metrics,
                threshold_override=threshold_override,
                force_override=force_override,
                schedule_threshold=schedule_threshold,
                schedule_mode=threshold_schedule_mode,
            )
            final_threshold = _safe_float(metrics.get("delinquency_trigger_threshold"))
            threshold_source = resolve_threshold_source(
                reported_threshold=reported_threshold,
                final_threshold=final_threshold,
                threshold_override=threshold_override,
                force_override=force_override,
                schedule_threshold=schedule_threshold,
                schedule_mode=threshold_schedule_mode,
                schedule_applied=threshold_flags.get("schedule_applied", False),
                schedule_override_applied=threshold_flags.get("schedule_override_applied", False),
            )

            # If filing doesn't explicitly state Yes/No (or conflicts with parsed threshold/current),
            # derive trigger occurrence deterministically from current vs threshold.
            dq_cur = metrics.get("delinquency_60_plus")
            dq_th = metrics.get("delinquency_trigger_threshold")
            dq_occ = metrics.get("delinquency_trigger_occurred")
            if isinstance(dq_cur, (int, float)) and isinstance(dq_th, (int, float)) and dq_th > 0:
                implied_occ = 1.0 if dq_cur >= dq_th else 0.0
                if not isinstance(dq_occ, (int, float)) or dq_occ not in (0.0, 1.0) or dq_occ != implied_occ:
                    metrics["delinquency_trigger_occurred"] = implied_occ

            if args.debug:
                pass
            if metrics:
                complete = all((k in metrics and metrics[k] is not None) for k in REQUIRED_METRICS)
                if args.debug:
                    missing = [k for k in REQUIRED_METRICS if k not in metrics or metrics[k] is None]
                    if metrics.get("delinquency_trigger_threshold") is None and metrics.get("delinquency_trigger_occurred") is None:
                        missing.append("delinquency_trigger_threshold/occurred")
                    if missing:
                        debug_missing_rows.append({
                            "deal_id": deal_id,
                            "trigger_id": "TRIGGER",
                            "period_end": filing.period_end,
                            "source_url": filing_source_url(filing),
                            "primary_doc_url": filing.primary_doc_url or "",
                            "ex99_url": filing.ex99_url or "",
                            "ex99_filename": filing.ex99_url.rstrip("/").split("/")[-1] if filing.ex99_url else "",
                            "missing_metrics": missing,
                            "reason": "missing_required",
                        })
                if args.validation_report:
                    missing = [k for k in REQUIRED_METRICS if k not in metrics or metrics[k] is None]
                    if metrics.get("delinquency_trigger_threshold") is None:
                        missing.append("delinquency_trigger_threshold")
                    validation_rows.append({
                        "deal_id": deal_id,
                        "period_end": filing.period_end,
                        "accession_no": filing.accession_no,
                        "source_url": filing_source_url(filing),
                        "status": "parsed",
                        "missing_metrics": missing,
                        "extracted_metrics": sorted(metrics.keys()),
                    })
                if args.parsed_cache and str(args.parsed_cache).strip():
                    parsed_cache[cache_key] = {
                        "deal_id": deal_id,
                        "period_end": filing.period_end,
                        "accession_no": filing.accession_no,
                        "source_url": filing_source_url(filing),
                        "primary_doc_url": filing.primary_doc_url or "",
                        "ex99_url": filing.ex99_url or "",
                        "metrics": metrics,
                        "complete": complete,
                        "threshold_source": threshold_source,
                        "threshold_reported": reported_threshold,
                        "cached_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    }
                    parsed_cache_dirty = True
                if args.qa_report:
                    for trig in triggers_cfg:
                        record_full_row(
                            deal_id=deal_id,
                            trigger_id=trig.get("trigger_id", "TRIGGER"),
                            period_end=filing.period_end,
                            source_url=filing_source_url(filing),
                            missing_metrics=[],
                            reason="parsed",
                            primary_doc_url=filing.primary_doc_url,
                            ex99_url=filing.ex99_url,
                        )
                series.append({
                    "period_end": filing.period_end,
                    "metrics": metrics,
                    "source_url": filing_source_url(filing),
                    "primary_doc_url": filing.primary_doc_url or "",
                    "ex99_url": filing.ex99_url or "",
                    "accession_no": filing.accession_no,
                    "complete": complete,
                    "threshold_source": threshold_source,
                    "threshold_reported": reported_threshold,
                })
            elif args.validation_report:
                validation_rows.append({
                    "deal_id": deal_id,
                    "period_end": filing.period_end,
                    "accession_no": filing.accession_no,
                    "source_url": filing_source_url(filing),
                    "status": "no_metrics",
                    "missing_metrics": list(REQUIRED_METRICS) + ["delinquency_trigger_threshold"],
                    "extracted_metrics": [],
                })
                if args.debug:
                    missing = list(REQUIRED_METRICS) + ["delinquency_trigger_threshold/occurred"]
                    debug_missing_rows.append({
                        "deal_id": deal_id,
                        "trigger_id": "TRIGGER",
                        "period_end": filing.period_end,
                        "source_url": filing_source_url(filing),
                        "primary_doc_url": filing.primary_doc_url or "",
                        "ex99_url": filing.ex99_url or "",
                        "ex99_filename": filing.ex99_url.rstrip("/").split("/")[-1] if filing.ex99_url else "",
                        "missing_metrics": missing,
                        "reason": "no_metrics",
                    })
                for trig in triggers_cfg:
                    record_missing_row(
                        deal_id=deal_id,
                        trigger_id=trig.get("trigger_id", "TRIGGER"),
                        period_end=filing.period_end,
                        source_url=filing_source_url(filing),
                        primary_doc_url=filing.primary_doc_url,
                        ex99_url=filing.ex99_url,
                        missing_metrics=list(REQUIRED_METRICS) + ["delinquency_trigger_threshold"],
                        reason="no_metrics",
                    )

        if not series:
            progress("  no parsable series for this deal")
            deal_triggers = []
            for trig in triggers_cfg:
                deal_triggers.append({
                    "triggerId": trig.get("trigger_id", "TRIGGER"),
                    "metric": trig.get("metric_label", "60+ DQ %"),
                    "direction": trig.get("direction", "<="),
                    "threshold": None,
                    "current": None,
                    "cushion": None,
                    "change3m": None,
                    "vol6m": None,
                    "score": 0.0,
                    "scoreBreakdown": {"cushion": 0.0, "trend3m": 0.0, "vol6m": 0.0, "macro": 0.0},
                })
            macro_payload = {
                "theme": deal.get("macro_theme", "Collateral stress"),
                "percentile": 0.5,
                "series": deal.get("macro_series", "60+ delinquency"),
            }
            deals_out.append({
                "dealId": deal_id,
                "cusip": deal.get("cusip", "—"),
                "collateral": deal.get("collateral", "Auto ABS"),
                "geo": deal.get("geo", "US"),
                "tranche": deal.get("tranche", "Class A"),
                "macro": macro_payload,
                "triggers": deal_triggers,
                "collateralMetrics": [],
                "cushionSeries": [],
                "dqSeries": [],
                "explanation": "No Exhibit 99.1 metrics matched for this deal in the selected window.",
                "dataStatus": "missing",
            })
            continue

        series.sort(key=lambda r: r["period_end"])
        all_as_of.append(series[-1]["period_end"])
        progress(f"  parsed periods: {len(series)} (latest {series[-1]['period_end']})")

        # Build metric arrays
        def series_values(key: str) -> List[float]:
            return [row["metrics"][key] for row in series if key in row["metrics"]]

        complete_by_period_all = [bool(row.get("complete", False)) for row in series]
        delinq_60_raw_by_period = [row["metrics"].get("delinquency_60_plus") for row in series]
        delinq_60_by_period = sanitize_incomplete_dq_zero_spikes(
            delinq_60_raw_by_period,
            complete_by_period_all,
        )
        total_dq_raw_by_period = [row["metrics"].get("total_delinquency") for row in series]
        total_dq_by_period = sanitize_incomplete_dq_zero_spikes(
            total_dq_raw_by_period,
            complete_by_period_all,
        )
        delinq_60 = [v for v in delinq_60_by_period if isinstance(v, (int, float))]
        total_dq = [v for v in total_dq_by_period if isinstance(v, (int, float))]
        cum_loss = series_values("cumulative_loss_ratio")
        pool_bal = series_values("pool_balance")
        mpr_values = series_values("monthly_payment_rate")
        total_collections_values = series_values("total_collections_rate")
        periods = [row["period_end"] for row in series]

        latest_metrics = series[-1]["metrics"]
        latest_60 = next((v for v in reversed(delinq_60_by_period) if isinstance(v, (int, float))), None)
        latest_total = next((v for v in reversed(total_dq_by_period) if isinstance(v, (int, float))), None)
        latest_loss = latest_metrics.get("cumulative_loss_ratio")
        latest_mpr = latest_metrics.get("monthly_payment_rate")
        latest_total_collections = latest_metrics.get("total_collections_rate")
        if delinq_60 and latest_60 is not None:
            macro_percentile = percent_rank(delinq_60, latest_60)
        else:
            macro_percentile = 0.5
        macro_value = None
        macro_as_of = None
        macro_pct_by_period: List[Optional[float]] = []
        if macro_series:
            macro_points = []
            for row in series:
                latest_date = dt.date.fromisoformat(row["period_end"])
                macro_point = macro_value_for_date(macro_series, latest_date)
                macro_points.append(macro_point)
            macro_base = [v for _, v in macro_series]
            macro_pct_by_period = [percent_rank(macro_base, p[1]) if p else None for p in macro_points]
            macro_percentile = macro_pct_by_period[-1] if macro_pct_by_period and macro_pct_by_period[-1] is not None else macro_percentile
            latest_point = macro_points[-1]
            if latest_point:
                macro_as_of, macro_value = latest_point
        else:
            for idx, v in enumerate(delinq_60_by_period):
                if v is None:
                    macro_pct_by_period.append(None)
                else:
                    hist = [x for x in delinq_60_by_period[: idx + 1] if x is not None]
                    macro_pct_by_period.append(percent_rank(hist, v))

        # Changes and volatility for trigger metrics
        def change_n(values: List[float], n: int) -> float:
            if len(values) <= n:
                return 0.0
            return values[-1] - values[-n - 1]

        def recent_changes(values: List[float], window: int) -> List[float]:
            if len(values) < 2:
                return []
            changes = [values[i] - values[i - 1] for i in range(1, len(values))]
            return changes[-window:]

        deal_triggers = []
        max_score = 0.0

        for trig in triggers_cfg:
            metric_key = trig.get("metric_key", "delinquency_60_plus")
            metric_label = trig.get("metric_label", "60+ DQ %")
            direction = trig.get("direction", "<=")
            reported_threshold = _safe_float(series[-1].get("threshold_reported"))
            threshold = _safe_float(latest_metrics.get("delinquency_trigger_threshold"))
            threshold_source = str(series[-1].get("threshold_source") or "")
            if not threshold_source:
                schedule_threshold_latest = resolve_schedule_threshold(
                    threshold_schedule,
                    str(series[-1].get("period_end") or ""),
                    len(series),
                )
                threshold_source = resolve_threshold_source(
                    reported_threshold=reported_threshold,
                    final_threshold=threshold,
                    threshold_override=threshold_override,
                    force_override=force_override,
                    schedule_threshold=schedule_threshold_latest,
                    schedule_mode=threshold_schedule_mode,
                )

            values_by_period = delinq_60_by_period if metric_key == "delinquency_60_plus" else [row["metrics"].get(metric_key) for row in series]
            values = [v for v in values_by_period if isinstance(v, (int, float))]
            complete_by_period = [row.get("complete", False) for row in series]
            threshold_by_period = [row["metrics"].get("delinquency_trigger_threshold") for row in series]
            occurred_by_period = [row["metrics"].get("delinquency_trigger_occurred") for row in series]
            current = values[-1] if values else None
            change3m = change_n(values, 3)
            changes = recent_changes(values, 6)
            mean_change = sum(changes) / len(changes) if changes else 0.0
            vol_change = stddev(changes) if changes else 0.0
            vol6m = vol_change
            if threshold is not None and threshold != 0 and current is not None:
                if direction == "<=":
                    cushion = (threshold - current) / threshold
                else:
                    cushion = (current - threshold) / threshold
            else:
                cushion = None
            if threshold is None or threshold <= 0 or current is None:
                score = 0.0
                score_breakdown = {"cushion": 0.0, "trend3m": 0.0, "vol6m": 0.0, "macro": 0.0}
            else:
                features = {
                    "cushion": cushion,
                    "trend3m": change3m,
                    "vol6m": vol6m,
                    "macro": macro_percentile,
                }
                if args.score_model == "logit" and logit_cfg:
                    score, score_breakdown = logit_score(
                        features,
                        weights=logit_cfg["weights"],
                        intercept=logit_cfg.get("intercept", 0.0),
                        scaler=logit_cfg.get("scaler"),
                    )
                else:
                    score, score_breakdown = rule_based_score(cushion, change3m, vol6m, direction, macro_percentile)
            max_score = max(max_score, score)

            deal_triggers.append({
                "triggerId": trig.get("trigger_id", "TRIGGER"),
                "metric": metric_label,
                "direction": direction,
                "threshold": threshold,
                "thresholdReported": float(reported_threshold) if reported_threshold is not None else None,
                "thresholdSource": threshold_source,
                "current": current,
                "cushion": cushion,
                "change3m": change3m,
                "vol6m": vol6m,
                "score": score,
                "scoreBreakdown": score_breakdown,
            })

            if args.export_training_csv:
                def change_n_at(values_list: List[Optional[float]], idx: int, n: int) -> Optional[float]:
                    if idx - n < 0:
                        return None
                    if values_list[idx] is None or values_list[idx - n] is None:
                        return None
                    return values_list[idx] - values_list[idx - n]

                def change_recent_at(values_list: List[Optional[float]], idx: int, max_n: int) -> Optional[float]:
                    if idx < 1:
                        return None
                    for n in range(max_n, 0, -1):
                        if idx - n < 0:
                            continue
                        if values_list[idx] is None or values_list[idx - n] is None:
                            continue
                        return values_list[idx] - values_list[idx - n]
                    return None

                def recent_changes_at(values_list: List[Optional[float]], idx: int, window: int) -> List[float]:
                    if idx < 1:
                        return []
                    changes_local = []
                    start = max(1, idx - window + 1)
                    for j in range(start, idx + 1):
                        prev = values_list[j - 1]
                        cur = values_list[j]
                        if prev is None or cur is None:
                            continue
                        changes_local.append(cur - prev)
                    return changes_local

                features_by_period: List[Optional[Dict[str, float]]] = []
                outcomes: List[bool] = []
                for idx, cur_val in enumerate(values_by_period):
                    th = threshold_by_period[idx] if idx < len(threshold_by_period) else None
                    occ = occurred_by_period[idx] if idx < len(occurred_by_period) else None
                    if cur_val is None:
                        src_row = series[idx] if idx < len(series) else {}
                        record_missing_row(
                            deal_id=deal_id,
                            trigger_id=trig.get("trigger_id", "TRIGGER"),
                            period_end=periods[idx] if idx < len(periods) else "",
                            source_url=src_row.get("source_url", ""),
                            primary_doc_url=src_row.get("primary_doc_url") or "",
                            ex99_url=src_row.get("ex99_url") or "",
                            missing_metrics=[metric_key],
                            reason="missing_metric",
                        )
                        features_by_period.append(None)
                        outcomes.append(False)
                        continue
                    if (th is None or th <= 0) and occ is None:
                        src_row = series[idx] if idx < len(series) else {}
                        record_missing_row(
                            deal_id=deal_id,
                            trigger_id=trig.get("trigger_id", "TRIGGER"),
                            period_end=periods[idx] if idx < len(periods) else "",
                            source_url=src_row.get("source_url", ""),
                            primary_doc_url=src_row.get("primary_doc_url") or "",
                            ex99_url=src_row.get("ex99_url") or "",
                            missing_metrics=["delinquency_trigger_threshold", "delinquency_trigger_occurred"],
                            reason="missing_metric",
                        )
                        features_by_period.append(None)
                        outcomes.append(False)
                        continue
                    if direction == "<=":
                        csh = (th - cur_val) / th if th else None
                    else:
                        csh = (cur_val - th) / th if th else None
                    chg3 = change_recent_at(values_by_period, idx, 3)
                    # First observation has no prior history by definition; treat trend as flat.
                    if chg3 is None and idx == 0:
                        chg3 = 0.0
                    changes_local = recent_changes_at(values_by_period, idx, 6)
                    vol_local = stddev(changes_local) if changes_local else 0.0
                    macro_pct = macro_pct_by_period[idx] if idx < len(macro_pct_by_period) else None
                    if chg3 is None or macro_pct is None:
                        reason = "insufficient_history" if chg3 is None else "macro_missing"
                        missing = []
                        if chg3 is None:
                            missing.append("trend3m")
                        if macro_pct is None:
                            missing.append("macro")
                        src_row = series[idx] if idx < len(series) else {}
                        record_missing_row(
                            deal_id=deal_id,
                            trigger_id=trig.get("trigger_id", "TRIGGER"),
                            period_end=periods[idx] if idx < len(periods) else "",
                            source_url=src_row.get("source_url", ""),
                            primary_doc_url=src_row.get("primary_doc_url") or "",
                            ex99_url=src_row.get("ex99_url") or "",
                            missing_metrics=missing,
                            reason=reason,
                        )
                        features_by_period.append(None)
                        outcomes.append(bool(occ) if occ is not None else (csh is not None and csh <= 0))
                        continue
                    features_by_period.append({
                        "cushion": csh,
                        "trend3m": chg3,
                        "vol6m": vol_local,
                        "macro": macro_pct,
                        "monthly_payment_rate": series[idx]["metrics"].get("monthly_payment_rate"),
                        "total_collections_rate": series[idx]["metrics"].get("total_collections_rate"),
                    })
                    outcomes.append(bool(occ) if occ is not None else (csh is not None and csh <= 0))

                training_rows.extend(
                    export_training_rows(
                        periods=periods,
                        features=features_by_period,
                        outcomes=outcomes,
                        horizon=args.label_horizon,
                        meta={
                            "deal_id": deal_id,
                            "trigger_id": trig.get("trigger_id", "TRIGGER"),
                            "metric": metric_label,
                            "cusip": deal.get("cusip", ""),
                        },
                        occurred=occurred_by_period,
                    )
                )

        # Build chart series
        history = series[-args.history:]
        cushion_series = []
        dq_series = []
        history_start = max(0, len(series) - len(history))
        for offset, row in enumerate(history):
            row_idx = history_start + offset
            period_end = row["period_end"]
            label = month_label(period_end)
            entry = {"m": label, "periodEnd": period_end}
            has_cushion_value = False
            for trig in triggers_cfg:
                metric_key = trig.get("metric_key", "delinquency_60_plus")
                direction = trig.get("direction", "<=")
                reported = row["metrics"].get("delinquency_trigger_threshold")
                threshold = float(reported) if reported is not None else None
                if metric_key == "delinquency_60_plus":
                    value = delinq_60_by_period[row_idx] if row_idx < len(delinq_60_by_period) else None
                else:
                    value = row["metrics"].get(metric_key)
                series_key = trig.get("series_key", "dq" if direction == "<=" else "oc")
                source_period = str(row.get("threshold_source") or "")
                if not source_period:
                    schedule_threshold_period = resolve_schedule_threshold(
                        threshold_schedule,
                        period_end,
                        row_idx + 1,
                    )
                    source_period = resolve_threshold_source(
                        reported_threshold=_safe_float(row.get("threshold_reported")),
                        final_threshold=_safe_float(threshold),
                        threshold_override=threshold_override,
                        force_override=force_override,
                        schedule_threshold=schedule_threshold_period,
                        schedule_mode=threshold_schedule_mode,
                    )
                entry[f"{series_key}ThresholdSource"] = source_period
                if threshold is not None:
                    entry[f"{series_key}Threshold"] = threshold
                if value is None or threshold is None or threshold == 0:
                    continue
                if direction == "<=":
                    entry[series_key] = (threshold - value) / threshold
                else:
                    entry[series_key] = (value - threshold) / threshold
                has_cushion_value = True
            if has_cushion_value:
                cushion_series.append(entry)

            dq_val = delinq_60_by_period[row_idx] if row_idx < len(delinq_60_by_period) else None
            if dq_val is not None:
                dq_series.append({"m": label, "dq60": dq_val})

        collateral_metrics = [
            {"name": "Total DQ", "cur": latest_total, "chg": change_n(total_dq, 3)},
            {"name": "61+ DQ", "cur": latest_60, "chg": change_n(delinq_60, 3)},
            {"name": "Cum Loss", "cur": latest_loss, "chg": change_n(cum_loss, 3)},
        ]
        if latest_mpr is not None:
            collateral_metrics.append({"name": "MPR", "cur": latest_mpr, "chg": change_n(mpr_values, 3)})
        if latest_total_collections is not None:
            collateral_metrics.append({
                "name": "Total Collections Rate",
                "cur": latest_total_collections,
                "chg": change_n(total_collections_values, 3),
            })

        if args.score_model == "logit":
            explanation = (
                "Derived from SEC 10-D Exhibit 99.1 tables. "
                "Scores use a logistic regression-style formula (sigmoid of weighted inputs)."
            )
        else:
            explanation = (
                "Derived from SEC 10-D Exhibit 99.1 tables. "
                "Scores are rule-based: 60% cushion distance, 20% 3m deterioration, "
                "10% volatility, 10% macro percentile. Breached triggers score 100."
            )

        macro_payload = {
            "theme": deal.get("macro_theme", "Collateral stress"),
            "percentile": macro_percentile,
            "series": deal.get("macro_series", "60+ delinquency"),
        }
        if macro_value is not None and macro_as_of is not None:
            macro_payload["value"] = macro_value
            macro_payload["asOf"] = macro_as_of.isoformat()
            macro_payload["source"] = "NY Fed HHDC" if (args.macro_source or "").lower() == "nyfed" else "Macro series"

        deals_out.append({
            "dealId": deal_id,
            "cusip": deal.get("cusip", "—"),
            "collateral": deal.get("collateral", "Auto ABS"),
            "geo": deal.get("geo", "US"),
            "tranche": deal.get("tranche", "Class A"),
            "macro": macro_payload,
            "triggers": deal_triggers,
            "collateralMetrics": collateral_metrics,
            "cushionSeries": cushion_series,
            "dqSeries": dq_series,
            "explanation": explanation,
        })

        if deal_triggers:
            top = max(deal_triggers, key=lambda t: t["score"])
            if top["score"] >= 0.45:
                severity = "red" if top["score"] >= 0.75 else "yellow"
                alerts.append({
                    "ts": f"{series[-1]['period_end']} 09:00",
                    "dealId": deal_id,
                    "severity": severity,
                    "title": f"{top['metric']} nearing trigger",
                    "detail": f"Cushion {top['cushion']:.2%}; risk score {top['score']:.0%}.",
                })

    if not deals_out and not args.csv_only:
        raise SystemExit("No deals built. Check CIKs, filings, or metric patterns.")

    if not args.csv_only:
        as_of = max(all_as_of) if all_as_of else dt.date.today().strftime(MONTH_FMT)
        tranches = sum(len(d.get("triggers", [])) for d in deals_out)
        flagged = sum(1 for d in deals_out if max(t["score"] for t in d.get("triggers", [])) >= 0.45)
        red = sum(1 for d in deals_out if max(t["score"] for t in d.get("triggers", [])) >= 0.75)
        yellow = max(0, flagged - red)

        payload = {
            "asOf": as_of,
            "portfolio": {
                "deals": len(deals_out),
                "tranches": tranches,
                "flagged": flagged,
                "red": red,
                "yellow": yellow,
            },
            "deals": deals_out,
            "alerts": alerts,
        }

        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print(f"Wrote {args.out}")
        if args.public_copy:
            try:
                dest = Path(args.public_copy)
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                print(f"Wrote {dest}")
            except Exception as exc:  # pragma: no cover
                print(f"Warning: failed to write public copy to {args.public_copy}: {exc}")
    progress("Build complete.")

    if args.export_training_csv:
        try:
            fieldnames = [
                "deal_id",
                "cusip",
                "trigger_id",
                "metric",
                "period_end",
                "cushion",
                "trend3m",
                "vol6m",
                "macro",
                "monthly_payment_rate",
                "total_collections_rate",
                "target_breach",
                "trigger_occurred",
            ]
            existing_keys = set(existing_training_keys)
            file_exists = os.path.exists(args.export_training_csv)

            rows_to_write = []
            for row in training_rows:
                key = training_row_key(
                    row.get("cusip", ""),
                    row.get("deal_id", ""),
                    row.get("trigger_id", ""),
                    row.get("period_end", ""),
                )
                if key in existing_keys:
                    continue
                rows_to_write.append(row)

            if rows_to_write:
                mode = "a" if file_exists else "w"
                with open(args.export_training_csv, mode, newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(rows_to_write)
                for row in rows_to_write:
                    existing_keys.add(
                        training_row_key(
                            row.get("cusip", ""),
                            row.get("deal_id", ""),
                            row.get("trigger_id", ""),
                            row.get("period_end", ""),
                        )
                    )
                print(f"Wrote {len(rows_to_write)} new rows to {args.export_training_csv}")
            else:
                print(f"No new rows written (all CUSIP+period+trigger rows already present) to {args.export_training_csv}")
        except Exception as exc:  # pragma: no cover
            print(f"Warning: failed to write training CSV to {args.export_training_csv}: {exc}")

    if args.validation_report:
        try:
            if str(args.validation_report).strip():
                dest = Path(args.validation_report)
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "w", encoding="utf-8") as f:
                    json.dump(validation_rows, f, indent=2)
                print(f"Wrote validation report to {dest}")
        except Exception as exc:  # pragma: no cover
            print(f"Warning: failed to write validation report to {args.validation_report}: {exc}")

    if args.qa_report:
        try:
            if str(args.qa_report).strip():
                dest = Path(args.qa_report)
                dest.parent.mkdir(parents=True, exist_ok=True)
                qa_full_rows = list(qa_full_map.values())
                periods_by_deal_trigger: Dict[tuple, set] = {}
                for row in qa_full_rows:
                    period_end = row.get("period_end") or ""
                    if not period_end or ".." in period_end:
                        continue
                    key = (row.get("deal_id", ""), row.get("trigger_id", "TRIGGER"))
                    periods_by_deal_trigger.setdefault(key, set()).add(period_end)

                def condense_full_rows(rows: List[dict]) -> List[dict]:
                    condensed: List[dict] = []
                    grouped: Dict[tuple, List[dict]] = {}
                    for row in rows:
                        missing = row.get("missing_metrics", [])
                        if not missing or row.get("reason") in ("parsed", "skipped_existing"):
                            condensed.append(row)
                            continue
                        key = (
                            row.get("deal_id", ""),
                            row.get("trigger_id", "TRIGGER"),
                            tuple(missing),
                            row.get("reason", ""),
                        )
                        grouped.setdefault(key, []).append(row)
                    for key, group in grouped.items():
                        deal_id, trigger_id, _, _ = key
                        all_periods = periods_by_deal_trigger.get((deal_id, trigger_id), set())
                        group_periods = {r.get("period_end", "") for r in group if r.get("period_end")}
                        if all_periods and group_periods == all_periods and len(group_periods) > 1:
                            latest = max(group, key=lambda r: r.get("period_end", ""))
                            sorted_periods = sorted(group_periods)
                            span = f"{sorted_periods[0]}..{sorted_periods[-1]} ({len(sorted_periods)})"
                            row = dict(latest)
                            row["period_end"] = span
                            condensed.append(row)
                        else:
                            condensed.extend(group)
                    return condensed

                qa_full_rows = condense_full_rows(qa_full_rows)
                if dest.suffix.lower() == ".csv":
                    fieldnames = [
                        "deal_id",
                        "trigger_id",
                        "period_end",
                        "source_url",
                        "primary_doc_url",
                        "ex99_url",
                        "ex99_filename",
                        "missing_metrics",
                        "status",
                        "reason",
                    ]
                    with open(dest, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        for row in qa_missing_rows:
                            writer.writerow({
                                "deal_id": row.get("deal_id", ""),
                                "trigger_id": row.get("trigger_id", ""),
                                "period_end": row.get("period_end", ""),
                                "source_url": row.get("source_url", ""),
                                "primary_doc_url": row.get("primary_doc_url", ""),
                                "ex99_url": row.get("ex99_url", ""),
                                "ex99_filename": row.get("ex99_filename", ""),
                                "missing_metrics": ",".join(row.get("missing_metrics", [])),
                                "status": row.get("status", ""),
                                "reason": row.get("reason", ""),
                            })
                    print(f"Wrote QA summary CSV to {dest}")
                else:
                    by_metric: Dict[str, int] = {}
                    by_reason: Dict[str, int] = {}
                    by_deal: Dict[str, int] = {}
                    for row in qa_missing_rows:
                        by_deal[row["deal_id"]] = by_deal.get(row["deal_id"], 0) + 1
                        by_reason[row["reason"]] = by_reason.get(row["reason"], 0) + 1
                        for m in row.get("missing_metrics", []):
                            by_metric[m] = by_metric.get(m, 0) + 1
                    summary = {
                        "total_rows_missing": len(qa_missing_rows),
                        "by_metric": dict(sorted(by_metric.items(), key=lambda kv: kv[1], reverse=True)),
                        "by_reason": dict(sorted(by_reason.items(), key=lambda kv: kv[1], reverse=True)),
                        "by_deal": dict(sorted(by_deal.items(), key=lambda kv: kv[1], reverse=True)),
                        "rows": qa_missing_rows,
                    }
                    with open(dest, "w", encoding="utf-8") as f:
                        json.dump(summary, f, indent=2)
                    print(f"Wrote QA summary to {dest}")
                    # Also emit a CSV alongside the JSON for easy review.
                    csv_dest = dest.with_suffix(".csv")
                    fieldnames = [
                        "deal_id",
                        "trigger_id",
                        "period_end",
                        "source_url",
                        "primary_doc_url",
                        "ex99_url",
                        "ex99_filename",
                        "missing_metrics",
                        "status",
                        "reason",
                    ]
                    with open(csv_dest, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        for row in qa_missing_rows:
                            writer.writerow({
                                "deal_id": row.get("deal_id", ""),
                                "trigger_id": row.get("trigger_id", ""),
                                "period_end": row.get("period_end", ""),
                                "source_url": row.get("source_url", ""),
                                "primary_doc_url": row.get("primary_doc_url", ""),
                                "ex99_url": row.get("ex99_url", ""),
                                "ex99_filename": row.get("ex99_filename", ""),
                                "missing_metrics": "|".join(row.get("missing_metrics", [])),
                                "status": row.get("status", ""),
                                "reason": row.get("reason", ""),
                            })
                    print(f"Wrote QA summary CSV to {csv_dest}")
                # Full QA CSV (all rows, condensed).
                full_csv = dest.with_name("qa_full.csv")
                fieldnames = [
                    "deal_id",
                    "trigger_id",
                    "period_end",
                    "source_url",
                    "primary_doc_url",
                    "ex99_url",
                    "ex99_filename",
                    "missing_metrics",
                    "status",
                    "reason",
                ]
                with open(full_csv, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in qa_full_rows:
                        writer.writerow({
                            "deal_id": row.get("deal_id", ""),
                            "trigger_id": row.get("trigger_id", ""),
                            "period_end": row.get("period_end", ""),
                            "source_url": row.get("source_url", ""),
                            "primary_doc_url": row.get("primary_doc_url", ""),
                            "ex99_url": row.get("ex99_url", ""),
                            "ex99_filename": row.get("ex99_filename", ""),
                            "missing_metrics": ",".join(row.get("missing_metrics", [])),
                            "status": row.get("status", ""),
                            "reason": row.get("reason", ""),
                        })
                print(f"Wrote full QA CSV to {full_csv}")
        except Exception as exc:  # pragma: no cover
            print(f"Warning: failed to write QA summary to {args.qa_report}: {exc}")

    if args.parsed_cache and str(args.parsed_cache).strip() and parsed_cache_dirty:
        try:
            dest = Path(args.parsed_cache)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "w", encoding="utf-8") as f:
                json.dump(parsed_cache, f, indent=2)
            print(f"Wrote parsed cache to {dest}")
        except Exception as exc:  # pragma: no cover
            print(f"Warning: failed to write parsed cache to {args.parsed_cache}: {exc}")

    if args.debug and debug_missing_rows:
        # Condensed debug summary for missing data.
        debug_full = list(qa_full_map.values()) if qa_full_map else []
        periods_by_deal_trigger: Dict[tuple, set] = {}
        for row in debug_full:
            period_end = row.get("period_end") or ""
            if not period_end or ".." in period_end:
                continue
            key = (row.get("deal_id", ""), row.get("trigger_id", "TRIGGER"))
            periods_by_deal_trigger.setdefault(key, set()).add(period_end)
        condensed = []
        grouped: Dict[tuple, List[dict]] = {}
        for row in debug_missing_rows:
            key = (
                row.get("deal_id", ""),
                row.get("trigger_id", "TRIGGER"),
                tuple(row.get("missing_metrics", [])),
                row.get("reason", ""),
            )
            grouped.setdefault(key, []).append(row)
        for key, group in grouped.items():
            deal_id, trigger_id, _, _ = key
            all_periods = periods_by_deal_trigger.get((deal_id, trigger_id), set())
            group_periods = {r.get("period_end", "") for r in group if r.get("period_end")}
            if all_periods and group_periods == all_periods and len(group_periods) > 1:
                sorted_periods = sorted(group_periods)
                span = f"{sorted_periods[0]}..{sorted_periods[-1]} ({len(sorted_periods)})"
                row = dict(group[0])
                row["period_end"] = span
                condensed.append(row)
            else:
                condensed.extend(group)
        print("Missing data summary (condensed):")
        for row in condensed:
            missing = ",".join(row.get("missing_metrics", []))
            print(
                f"- {row.get('deal_id','')} {row.get('period_end','')}: "
                f"{missing} [{row.get('status','missing')}] ({row.get('reason','')})"
            )


if __name__ == "__main__":
    main()
