#!/usr/bin/env python3
"""
Phase A: concurrent sizing probe for the HireTrack Sync API.

Hits GET /api/table/<name>/count for every table returned by /api/tables and
writes a CSV report sorted by row count descending. Use this BEFORE running
sync_to_mysql.py so you know which tables are risky (huge) vs. trivial.

Usage:
    python tools/hiretrack-ops/probe_sizes.py \
        --api http://100.100.139.110:5003 \
        --api-user "$API_USERNAME" --api-password "$API_PASSWORD" \
        --out var/artifacts/table_sizes.csv \
        --workers 16
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class TableStat:
    name: str
    rows: int | None
    seconds: float
    error: str | None = None


def make_session(auth: tuple[str, str] | None) -> requests.Session:
    # Retry on transient errors only. 4xx (e.g. 401) should fail fast.
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_maxsize=32))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=32))
    if auth:
        s.auth = auth
    return s


def list_tables(session: requests.Session, api: str) -> list[str]:
    r = session.get(f"{api}/api/tables", timeout=60)
    r.raise_for_status()
    return r.json()["tables"]


def count_one(session: requests.Session, api: str, table: str) -> TableStat:
    t0 = time.perf_counter()
    try:
        r = session.get(f"{api}/api/table/{table}/count", timeout=120)
        r.raise_for_status()
        body = r.json()
        return TableStat(table, int(body.get("count", 0)), time.perf_counter() - t0)
    except Exception as e:
        return TableStat(table, None, time.perf_counter() - t0, str(e)[:200])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--api", default=os.environ.get("API_URL", "http://100.100.139.110:5003"))
    p.add_argument("--api-user", default=os.environ.get("API_USERNAME"))
    p.add_argument("--api-password", default=os.environ.get("API_PASSWORD"))
    p.add_argument("--workers", type=int, default=16, help="concurrent COUNT requests")
    p.add_argument("--out", default="var/artifacts/table_sizes.csv", help="CSV output path")
    args = p.parse_args()

    auth = (args.api_user, args.api_password) if args.api_user else None
    session = make_session(auth)

    try:
        tables = list_tables(session, args.api.rstrip("/"))
    except Exception as e:
        print(f"[FATAL] /api/tables failed: {e}", file=sys.stderr)
        return 2
    print(f"Tables discovered: {len(tables)}")
    print(f"Probing COUNT(*) with {args.workers} workers...")

    results: list[TableStat] = []
    wall_t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(count_one, session, args.api, t): t for t in tables}
        done = 0
        for f in as_completed(futures):
            stat = f.result()
            results.append(stat)
            done += 1
            status = f"{stat.rows:>10,}" if stat.rows is not None else "    ERROR"
            print(f"  [{done:>3}/{len(tables)}] {status} rows  {stat.seconds:>6.2f}s  {stat.name}")

    results.sort(key=lambda s: (s.rows is None, -(s.rows or 0)))

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["table", "rows", "seconds", "error"])
        for s in results:
            w.writerow([s.name, s.rows if s.rows is not None else "", f"{s.seconds:.3f}", s.error or ""])

    total_rows = sum((s.rows or 0) for s in results)
    failed = [s for s in results if s.error]
    wall = time.perf_counter() - wall_t0

    print("\n=== Summary ===")
    print(f"Wall time        : {wall:.1f}s")
    print(f"Total tables     : {len(results)}")
    print(f"Total rows       : {total_rows:,}")
    print(f"Failed           : {len(failed)}")
    if failed:
        print("Failed tables:")
        for s in failed[:20]:
            print(f"  - {s.name}: {s.error}")

    print("\nTop 20 by row count:")
    for s in results[:20]:
        rows = f"{s.rows:,}" if s.rows is not None else "ERROR"
        print(f"  {rows:>14}  {s.name}")
    print(f"\nFull report: {args.out}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
