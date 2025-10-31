#!/usr/bin/env python3
"""
Facility QC Report Generator

Generates comprehensive quality control reports for facility data:
- Overall coverage statistics
- Per-country breakdowns
- Type confidence distribution
- Slug collision detection
- Data quality flags

Usage:
    python scripts/reporting/facility_qc_report.py

Output:
    - Console summary with overall statistics
    - CSV report: data/reports/facility_qc_YYYYMMDD_HHMM.csv
"""

from __future__ import annotations
import os, glob, json, csv
from collections import Counter, defaultdict
from datetime import datetime

def iter_facilities(root="facilities"):
    """Iterate through all facility JSON files."""
    for country_dir in glob.glob(os.path.join(root, "*")):
        if not os.path.isdir(country_dir):
            continue
        iso3 = os.path.basename(country_dir)
        for path in glob.glob(os.path.join(country_dir, "*.json")):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                    doc["_country"] = iso3
                    doc["_path"] = path
                    yield doc
            except Exception:
                continue

def bucket(x):
    """Bucket confidence scores for reporting."""
    if x is None:
        return "none"
    try:
        x = float(x)
    except Exception:
        return "none"
    if x < 0.5:
        return "<0.5"
    if x < 0.8:
        return "0.5–0.8"
    return "≥0.8"

def main():
    by_country = defaultdict(list)
    slug_collisions = defaultdict(list)  # (iso3, slug) -> [facility_id]
    totals = Counter()
    tc_buckets = Counter()

    # Collect statistics
    for fac in iter_facilities():
        iso3 = fac.get("_country", "???")
        fid = fac.get("facility_id", "?")
        slug = fac.get("canonical_slug")
        dq = fac.get("data_quality") or {}
        flags = dq.get("flags") or {}
        loc = fac.get("location") or {}

        totals["n"] += 1
        if fac.get("canonical_name"):
            totals["canonical_name"] += 1
        if slug:
            totals["canonical_slug"] += 1
        if loc.get("town"):
            totals["town"] += 1
        if loc.get("geohash"):
            totals["geohash"] += 1
        if fac.get("primary_type"):
            totals["primary_type"] += 1
        if fac.get("display_name"):
            totals["display_name"] += 1
        if fac.get("operator_display"):
            totals["operator_display"] += 1
        if flags.get("operator_unresolved"):
            totals["operator_unresolved"] += 1
        if flags.get("canonical_name_incomplete"):
            totals["canonical_incomplete"] += 1
        if flags.get("town_missing"):
            totals["town_missing"] += 1

        tc_buckets[bucket(fac.get("type_confidence"))] += 1

        if slug:
            slug_collisions[(iso3, slug)].append(fid)

        by_country[iso3].append(fac)

    # Print overall statistics
    n = totals["n"] or 1
    pct = lambda k: f"{100.0*totals[k]/n:5.1f}%"

    print("\n" + "="*70)
    print("FACILITY QC REPORT (OVERALL)")
    print("="*70)
    print(f"Total facilities:          {n:6d}")
    print()
    print("Field Coverage:")
    print(f"  canonical_name:          {totals['canonical_name']:6d} ({pct('canonical_name')})")
    print(f"  canonical_slug:          {totals['canonical_slug']:6d} ({pct('canonical_slug')})")
    print(f"  town:                    {totals['town']:6d} ({pct('town')})")
    print(f"  geohash:                 {totals['geohash']:6d} ({pct('geohash')})")
    print(f"  primary_type:            {totals['primary_type']:6d} ({pct('primary_type')})")
    print(f"  display_name:            {totals['display_name']:6d} ({pct('display_name')})")
    print(f"  operator_display:        {totals['operator_display']:6d} ({pct('operator_display')})")
    print()
    print("Data Quality Flags:")
    print(f"  town_missing:            {totals['town_missing']:6d} ({pct('town_missing')})")
    print(f"  operator_unresolved:     {totals['operator_unresolved']:6d} ({pct('operator_unresolved')})")
    print(f"  canonical_incomplete:    {totals['canonical_incomplete']:6d} ({pct('canonical_incomplete')})")
    print()
    print("Type Confidence Distribution:")
    for k in ["none", "<0.5", "0.5–0.8", "≥0.8"]:
        count = tc_buckets[k]
        pct_val = f"{100.0*count/n:5.1f}%" if n > 0 else "0.0%"
        print(f"  {k:8s}: {count:6d} ({pct_val})")

    # Slug collision detection
    bad = [(k, v) for k, v in slug_collisions.items() if len(v) > 1]
    bad.sort(key=lambda x: len(x[1]), reverse=True)

    if bad:
        print()
        print("="*70)
        print(f"SLUG COLLISIONS DETECTED: {len(bad)} collision groups")
        print("="*70)
        print("\nTop 10 collisions (country, slug) → count, examples:")
        for (iso3, slug), ids in bad[:10]:
            print(f"  ({iso3}, {slug:30s}) → {len(ids):3d}  e.g., {ids[:3]}")
    else:
        print()
        print("✅ No slug collisions detected")

    # Write per-country CSV
    os.makedirs("data/reports", exist_ok=True)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M')
    out = f"data/reports/facility_qc_{timestamp}.csv"

    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "country", "n", "canonical_name", "canonical_slug", "town", "geohash",
            "primary_type", "display_name", "operator_display",
            "town_missing", "operator_unresolved", "canonical_incomplete",
            "type_conf_none", "type_conf_<0.5", "type_conf_0.5–0.8", "type_conf_≥0.8"
        ])

        for iso3 in sorted(by_country.keys()):
            rows = by_country[iso3]
            cc = Counter()

            for fac in rows:
                dq = (fac.get("data_quality") or {})
                flags = dq.get("flags") or {}
                loc = fac.get("location") or {}

                cc["n"] += 1
                if fac.get("canonical_name"):
                    cc["canonical_name"] += 1
                if fac.get("canonical_slug"):
                    cc["canonical_slug"] += 1
                if loc.get("town"):
                    cc["town"] += 1
                if loc.get("geohash"):
                    cc["geohash"] += 1
                if fac.get("primary_type"):
                    cc["primary_type"] += 1
                if fac.get("display_name"):
                    cc["display_name"] += 1
                if fac.get("operator_display"):
                    cc["operator_display"] += 1
                if flags.get("town_missing"):
                    cc["town_missing"] += 1
                if flags.get("operator_unresolved"):
                    cc["operator_unresolved"] += 1
                if flags.get("canonical_name_incomplete"):
                    cc["canonical_incomplete"] += 1

                tc = bucket(fac.get("type_confidence"))
                cc[f"type_conf_{tc}"] += 1

            w.writerow([
                iso3, cc["n"],
                cc["canonical_name"], cc["canonical_slug"], cc["town"], cc["geohash"],
                cc["primary_type"], cc["display_name"], cc["operator_display"],
                cc["town_missing"], cc["operator_unresolved"], cc["canonical_incomplete"],
                cc["type_conf_none"], cc["type_conf_<0.5"],
                cc["type_conf_0.5–0.8"], cc["type_conf_≥0.8"]
            ])

    print()
    print("="*70)
    print(f"Per-country CSV report: {out}")
    print("="*70)
    print()

if __name__ == "__main__":
    main()
