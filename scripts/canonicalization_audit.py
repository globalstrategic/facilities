#!/usr/bin/env python3
"""Audit current state of canonical naming and generate reports."""

import json
import csv
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any
import sys

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.type_map import normalize_type
from scripts.utils.name_parts import nfc, to_ascii, slugify, equal_ignoring_accents
from scripts.utils.slug_registry import SlugRegistry


def load_all_facilities() -> List[Dict[str, Any]]:
    """Load all facility JSON files."""
    facilities = []
    facilities_dir = Path("facilities")

    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        for fac_file in sorted(country_dir.glob("*.json")):
            try:
                with open(fac_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)
                    facilities.append(facility)
            except Exception as e:
                print(f"Error loading {fac_file}: {e}", file=sys.stderr)

    return facilities


def analyze_facilities(facilities: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze facilities and generate metrics."""

    stats = {
        "total": len(facilities),
        "missing_canonical_name": 0,
        "missing_canonical_slug": 0,
        "missing_primary_type": 0,
        "low_type_confidence": 0,
        "type_is_facility": 0,
        "town_missing": 0,
        "operator_unresolved": 0,
        "location_precision_poor": 0,
        "has_todo_literal": 0,
        "has_coordinates": 0,
        "slug_collisions": 0,
        "unicode_in_name": 0,
    }

    # Lists for reports
    geocoding_needed = []
    collision_cases = []
    type_problems = []

    # Slug registry
    slug_registry = SlugRegistry()
    slug_map = {}

    for fac in facilities:
        fac_id = fac.get("facility_id", "unknown")

        # Check canonical fields
        if not fac.get("canonical_name"):
            stats["missing_canonical_name"] += 1

        if not fac.get("canonical_slug"):
            stats["missing_canonical_slug"] += 1
        else:
            # Check for collisions
            slug = fac["canonical_slug"]
            if slug in slug_map:
                stats["slug_collisions"] += 1
                collision_cases.append({
                    "slug": slug,
                    "facility1": slug_map[slug],
                    "facility2": fac_id
                })
            else:
                slug_map[slug] = fac_id

        # Check type quality
        primary_type = fac.get("primary_type")
        type_conf = fac.get("type_confidence", 0)

        if not primary_type:
            stats["missing_primary_type"] += 1
            # Try to infer from types[0]
            if fac.get("types"):
                raw_type = fac["types"][0] if fac["types"] else None
                inferred_type, conf = normalize_type(raw_type)
                type_problems.append({
                    "facility_id": fac_id,
                    "raw_type": raw_type,
                    "inferred": inferred_type,
                    "confidence": conf
                })
        elif primary_type == "facility":
            stats["type_is_facility"] += 1

        if type_conf < 0.6:
            stats["low_type_confidence"] += 1

        # Check location quality
        loc = fac.get("location", {})

        if loc.get("town") == "TODO" or str(loc.get("town", "")).upper() == "TODO":
            stats["has_todo_literal"] += 1

        if not loc.get("town"):
            stats["town_missing"] += 1

        precision = loc.get("precision", "unknown")
        if precision in ["unknown", "region"]:
            stats["location_precision_poor"] += 1

        if loc.get("lat") is not None and loc.get("lon") is not None:
            stats["has_coordinates"] += 1

        # Check operator resolution
        if not fac.get("operator_display") and not fac.get("operator_link"):
            if fac.get("company_mentions"):
                stats["operator_unresolved"] += 1

        # Check for Unicode
        name = fac.get("name", "")
        if name and name != to_ascii(name):
            stats["unicode_in_name"] += 1

        # Collect geocoding candidates
        needs_geocoding = False
        priority = 4

        if loc.get("lat") is None or loc.get("lon") is None:
            needs_geocoding = True
            priority = 1
        elif precision in ["unknown", "region"]:
            needs_geocoding = True
            priority = 1
        elif not loc.get("town"):
            needs_geocoding = True
            priority = 2
        elif not fac.get("operator_display"):
            priority = 3
        elif type_conf < 0.6:
            priority = 4

        if needs_geocoding or priority <= 3:
            geocoding_needed.append({
                "priority": priority,
                "facility_id": fac_id,
                "country_iso3": fac.get("country_iso3"),
                "raw_name": fac.get("name"),
                "canonical_name": fac.get("canonical_name"),
                "operator_display": fac.get("operator_display"),
                "primary_type": primary_type or fac.get("types", [""])[0] if fac.get("types") else "",
                "lat": loc.get("lat"),
                "lon": loc.get("lon"),
                "precision": precision,
                "town": loc.get("town"),
                "region": loc.get("region"),
                "aliases": "|".join(fac.get("aliases", [])),
                "commodities": "|".join([c.get("metal", "") for c in fac.get("commodities", [])]),
            })

    # Calculate percentages
    if stats["total"] > 0:
        stats["canonical_name_coverage"] = (stats["total"] - stats["missing_canonical_name"]) / stats["total"] * 100
        stats["canonical_slug_coverage"] = (stats["total"] - stats["missing_canonical_slug"]) / stats["total"] * 100
        stats["type_quality"] = (stats["total"] - stats["type_is_facility"] - stats["missing_primary_type"]) / stats["total"] * 100
        stats["town_coverage"] = (stats["total"] - stats["town_missing"]) / stats["total"] * 100
        stats["coordinate_coverage"] = stats["has_coordinates"] / stats["total"] * 100

    return {
        "stats": stats,
        "geocoding_needed": sorted(geocoding_needed, key=lambda x: (x["priority"], x["facility_id"]))[:250],
        "collisions": collision_cases,
        "type_problems": type_problems[:100],
    }


def main():
    """Run audit and generate reports."""

    print("Loading facilities...")
    facilities = load_all_facilities()
    print(f"Loaded {len(facilities)} facilities")

    print("Analyzing...")
    results = analyze_facilities(facilities)

    # Create reports directory
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)

    # Write JSON report
    report_file = reports_dir / "canonicalization_report.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(results["stats"], f, indent=2)
    print(f"Wrote {report_file}")

    # Write geocoding CSV
    csv_file = reports_dir / "geocoding_request.csv"
    if results["geocoding_needed"]:
        with open(csv_file, 'w', encoding='utf-8', newline='') as f:
            fieldnames = [
                "facility_id", "country_iso3", "raw_name", "canonical_name",
                "operator_display", "primary_type", "lat", "lon", "precision",
                "town", "region", "aliases", "commodities"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in results["geocoding_needed"]:
                # Remove priority field for CSV
                row.pop("priority", None)
                writer.writerow(row)
        print(f"Wrote {csv_file} with {len(results['geocoding_needed'])} facilities needing geocoding")

    # Write markdown helper
    md_file = reports_dir / "geocoding_request.md"
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write("# Facilities Needing Geocoding\n\n")
        f.write("Quick reference for open-source lookups:\n\n")

        for fac in results["geocoding_needed"][:250]:
            name = fac.get("raw_name", "Unknown")
            country = fac.get("country_iso3", "")
            region = fac.get("region", "")
            operator = fac.get("operator_display", "")
            commodities = fac.get("commodities", "")

            # Build clues string
            clues = []
            if region:
                clues.append(f"region: {region}")
            if operator:
                clues.append(f"operator: {operator}")
            if commodities:
                clues.append(f"commodities: {commodities}")

            clues_str = f" ({', '.join(clues)})" if clues else ""
            f.write(f"- **{name}** [{country}]{clues_str}\n")

    print(f"Wrote {md_file}")

    # Write collisions CSV if any
    if results["collisions"]:
        collision_file = reports_dir / "collisions.csv"
        with open(collision_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["slug", "facility1", "facility2"])
            writer.writeheader()
            writer.writerows(results["collisions"])
        print(f"WARNING: Found {len(results['collisions'])} slug collisions! See {collision_file}")

    # Print summary
    stats = results["stats"]
    print("\n=== Canonicalization Audit Summary ===")
    print(f"Total facilities: {stats['total']}")
    print(f"Canonical name coverage: {stats.get('canonical_name_coverage', 0):.1f}%")
    print(f"Canonical slug coverage: {stats.get('canonical_slug_coverage', 0):.1f}%")
    print(f"Type quality: {stats.get('type_quality', 0):.1f}%")
    print(f"Town coverage: {stats.get('town_coverage', 0):.1f}%")
    print(f"Coordinate coverage: {stats.get('coordinate_coverage', 0):.1f}%")
    print(f"Slug collisions: {stats['slug_collisions']}")
    print(f"TODO literals: {stats['has_todo_literal']}")
    print(f"Unicode names: {stats['unicode_in_name']}")


if __name__ == "__main__":
    main()