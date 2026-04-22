#!/usr/bin/env python3
"""
Discover per-table metadata (primary key, modified-at column) by probing the
HireTrack Sync API schema endpoint.

Heuristic-only — we can't ask NexusDB "which column is the PK?" cheaply via
ODBC for a V3 database, so we pattern-match column names using conventions
common in HireTrack/legacy ERPs. The output is written as YAML and is meant
to be reviewed and edited by hand: it's a source file, not a black box.

Output structure:

    tables:
      Jobs:
        pk: JobID
        mtime_col: ModifiedDate
        sync_strategy: incremental      # mtime-based change detection
      CURRENCY:
        pk: CurrencyCode
        mtime_col: null
        sync_strategy: full_refresh     # no mtime signal; re-dump each cycle
      ...

Usage:
    python tools/hiretrack-ops/discover_catalog.py \
        --api http://100.100.139.110:5003 \
        --api-user "$API_USERNAME" --api-password "$API_PASSWORD" \
        --out var/artifacts/catalog.yaml
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---- heuristics ---------------------------------------------------------

# Ordered by specificity: earlier patterns win. Case-insensitive exact match
# against column names (not substring — to avoid matching "CreatedByUserID"
# as a pk).
PK_PATTERNS = [
    r"^id$",
    r"^(?P<stem>.+?)id$",              # JobID, InvoiceID, etc.
    r"^(?P<stem>.+?)_id$",             # job_id
    r"^(?P<stem>.+?)code$",            # CurrencyCode, CountryCode
    r"^(?P<stem>.+?)key$",             # PrimaryKey
    r"^(?P<stem>.+?)no$",              # InvoiceNo
    r"^pk_",
]

# Match the FIRST column whose name matches any of these. Ordered strongest-
# signal first (ModifiedDate beats ChangeDate beats generic Timestamp).
MTIME_PATTERNS = [
    r"^modified(date|on|at)?$",
    r"^last[_ ]?modified(date|on|at)?$",
    r"^updated(date|on|at)?$",
    r"^last[_ ]?updated(date|on|at)?$",
    r"^changed(date|on|at)?$",
    r"^edit(ed)?(date|on|at)?$",
    r"^stamp(date|on)?$",
    r"^mtime$",
]

SOFT_DELETE_PATTERNS = [
    r"^deleted$",
    r"^is[_ ]?deleted$",
    r"^active$",
    r"^is[_ ]?active$",
    r"^status$",
    r"^recordstatus$",
]


def _compile(patterns: list[str]) -> list[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]


PK_RX = _compile(PK_PATTERNS)
MTIME_RX = _compile(MTIME_PATTERNS)
SOFT_DELETE_RX = _compile(SOFT_DELETE_PATTERNS)


def first_match(names: list[str], patterns: list[re.Pattern]) -> Optional[str]:
    # Respect the order of `patterns` (specificity), then the order of `names`
    # (column order in the table, which is usually meaningful).
    for rx in patterns:
        for n in names:
            if rx.match(n):
                return n
    return None


def classify_columns(table: str, columns: list[dict]) -> dict:
    names = [c["name"] for c in columns]
    types = {c["name"]: str(c.get("type", "")).lower() for c in columns}

    # PK heuristic: prefer columns that are a) int-typed and b) look like "<TableName>ID".
    pk = None
    table_root = re.sub(r"s$", "", table)  # crude singularization
    for rx in PK_RX:
        for n in names:
            m = rx.match(n)
            if not m:
                continue
            # Prefer a PK whose stem matches the table name (e.g. Jobs -> JobID).
            stem = m.groupdict().get("stem", "")
            if stem and stem.lower() == table_root.lower():
                pk = n
                break
            if pk is None:
                pk = n
        if pk:
            break

    mtime = first_match(names, MTIME_RX)
    # Sanity check: mtime column type should look temporal.
    if mtime:
        t = types.get(mtime, "")
        if not any(k in t for k in ("date", "time", "datetime")):
            mtime = None

    soft_delete = first_match(names, SOFT_DELETE_RX)

    return {
        "pk": pk,
        "mtime_col": mtime,
        "soft_delete_col": soft_delete,
        "column_count": len(names),
    }


# ---- HTTP ---------------------------------------------------------------

def make_session(auth: tuple[str, str] | None) -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.5,
                  status_forcelist=(500, 502, 503, 504),
                  allowed_methods=frozenset(["GET"]), raise_on_status=False)
    s = requests.Session()
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_maxsize=32))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=32))
    if auth:
        s.auth = auth
    return s


def fetch_schema(session: requests.Session, api: str, table: str) -> dict:
    r = session.get(f"{api}/api/table/{table}/schema", timeout=60)
    r.raise_for_status()
    return r.json()


# ---- main ---------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--api", default=os.environ.get("API_URL", "http://100.100.139.110:5003"))
    p.add_argument("--api-user", default=os.environ.get("API_USERNAME"))
    p.add_argument("--api-password", default=os.environ.get("API_PASSWORD"))
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--out", default="var/artifacts/catalog.yaml")
    args = p.parse_args()

    auth = (args.api_user, args.api_password) if args.api_user else None
    session = make_session(auth)
    api = args.api.rstrip("/")

    tables = session.get(f"{api}/api/tables", timeout=60).json()["tables"]
    print(f"Probing schema for {len(tables)} tables with {args.workers} workers...")

    catalog: dict[str, dict] = {}
    errors: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_schema, session, api, t): t for t in tables}
        done = 0
        for f in as_completed(futures):
            t = futures[f]
            done += 1
            try:
                schema = f.result()
            except Exception as e:
                errors.append((t, str(e)[:200]))
                print(f"  [{done:>3}/{len(tables)}] ERROR  {t}: {e}")
                continue
            meta = classify_columns(t, schema.get("schema", []))
            # Default strategy: mtime-based incremental if we found one,
            # else full_refresh. Review the output YAML before trusting.
            meta["sync_strategy"] = "incremental" if meta["mtime_col"] else "full_refresh"
            catalog[t] = meta
            print(f"  [{done:>3}/{len(tables)}] {t:30s} "
                  f"pk={meta['pk'] or '?':15s} mtime={meta['mtime_col'] or '-'}")

    # Summarize & decide defaults
    have_mtime = sum(1 for v in catalog.values() if v["mtime_col"])
    have_pk = sum(1 for v in catalog.values() if v["pk"])
    have_soft = sum(1 for v in catalog.values() if v["soft_delete_col"])

    # Sort by name for stable YAML diffs in git.
    out = {"tables": {k: catalog[k] for k in sorted(catalog)}}
    with open(args.out, "w") as fh:
        yaml.safe_dump(out, fh, sort_keys=False, default_flow_style=False)

    print("\n=== Catalog summary ===")
    print(f"Total tables       : {len(catalog)}")
    print(f"With inferred PK   : {have_pk}")
    print(f"With mtime column  : {have_mtime}   → candidates for incremental sync")
    print(f"With soft-delete   : {have_soft}")
    print(f"Errors             : {len(errors)}")
    if errors:
        for t, e in errors[:10]:
            print(f"  - {t}: {e}")
    print(f"\nWrote {args.out}. REVIEW IT — heuristics are wrong sometimes.")
    print("Tables without mtime will be full_refresh; edit the YAML to override.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
