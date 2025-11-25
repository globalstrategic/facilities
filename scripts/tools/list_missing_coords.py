#!/usr/bin/env python3
"""
List facilities that are missing lat/lon coordinates.

Outputs a summary by country and optionally exports to CSV.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def check_facility_coords(facility_path: Path) -> dict:
    """Check if a facility has coordinates."""
    try:
        with open(facility_path) as f:
            data = json.load(f)

        loc = data.get("location", {})
        lat = loc.get("lat")
        lon = loc.get("lon")

        has_coords = lat is not None and lon is not None

        return {
            "facility_id": data.get("facility_id", facility_path.stem),
            "name": data.get("name", "Unknown"),
            "country_iso3": data.get("country_iso3", "???"),
            "has_coords": has_coords,
            "lat": lat,
            "lon": lon,
            "status": data.get("status", "unknown"),
            "commodities": data.get("commodities", [])
        }
    except Exception as e:
        print(f"Error reading {facility_path}: {e}", file=sys.stderr)
        return None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="List facilities missing coordinates")
    parser.add_argument("--country", help="Filter by ISO3 country code")
    parser.add_argument("--export", help="Export to CSV file")
    parser.add_argument("--status", help="Filter by status (operating, closed, project, etc)")
    parser.add_argument("--limit", type=int, help="Limit results per country")
    args = parser.parse_args()

    facilities_dir = Path("facilities")

    # Determine which countries to process
    if args.country:
        country_dirs = [facilities_dir / args.country.upper()]
        if not country_dirs[0].exists():
            print(f"ERROR: Country directory not found: {args.country}")
            sys.exit(1)
    else:
        country_dirs = sorted([d for d in facilities_dir.iterdir() if d.is_dir()])

    # Scan facilities
    missing_by_country = defaultdict(list)
    total_facilities = 0
    total_missing = 0

    for country_dir in country_dirs:
        country_iso3 = country_dir.name

        for facility_file in sorted(country_dir.glob("*.json")):
            total_facilities += 1
            result = check_facility_coords(facility_file)

            if result and not result["has_coords"]:
                # Apply status filter if specified
                if args.status and result["status"] != args.status:
                    continue

                missing_by_country[country_iso3].append(result)
                total_missing += 1

    # Print summary
    print(f"\n{'='*80}")
    print(f"FACILITIES MISSING COORDINATES")
    print(f"{'='*80}")
    print(f"Total facilities scanned: {total_facilities:,}")
    print(f"Missing coordinates: {total_missing:,} ({total_missing/total_facilities*100:.1f}%)")
    print(f"Countries affected: {len(missing_by_country)}")
    print(f"{'='*80}\n")

    # Print by country
    print(f"{'Country':<8} {'Missing':<10} {'Top Facilities'}")
    print(f"{'-'*8} {'-'*10} {'-'*60}")

    for country_iso3 in sorted(missing_by_country.keys(),
                               key=lambda c: len(missing_by_country[c]),
                               reverse=True):
        missing = missing_by_country[country_iso3]

        # Apply limit if specified
        display_count = len(missing)
        if args.limit:
            missing = missing[:args.limit]

        # Get top 3 facility names
        top_names = [f["name"][:40] for f in missing[:3]]

        print(f"{country_iso3:<8} {display_count:<10} {', '.join(top_names)}")

    # Detailed listing
    if args.country or total_missing < 100:
        print(f"\n{'='*80}")
        print("DETAILED LISTING")
        print(f"{'='*80}\n")

        for country_iso3 in sorted(missing_by_country.keys()):
            missing = missing_by_country[country_iso3]

            # Apply limit if specified
            if args.limit:
                missing = missing[:args.limit]

            print(f"\n{country_iso3} ({len(missing)} facilities)")
            print(f"{'-'*80}")

            for i, fac in enumerate(missing, 1):
                commodities = ', '.join(fac['commodities'][:3]) if fac['commodities'] else 'N/A'
                print(f"{i:3}. {fac['name'][:50]:<50} [{fac['status']:<10}] {commodities}")

    # Export to CSV if requested
    if args.export:
        import csv

        with open(args.export, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'facility_id', 'name', 'country_iso3', 'status', 'commodities'
            ])
            writer.writeheader()

            for country_iso3 in sorted(missing_by_country.keys()):
                for fac in missing_by_country[country_iso3]:
                    writer.writerow({
                        'facility_id': fac['facility_id'],
                        'name': fac['name'],
                        'country_iso3': fac['country_iso3'],
                        'status': fac['status'],
                        'commodities': ', '.join(fac['commodities'])
                    })

        print(f"\n✓ Exported to {args.export}")


if __name__ == "__main__":
    main()
