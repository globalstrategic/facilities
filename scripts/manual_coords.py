#!/usr/bin/env python3
"""
Manual coordinate entry for facilities.

Usage:
    # From a CSV file
    python scripts/manual_coords.py import coords.csv

    # Interactive mode (one at a time)
    python scripts/manual_coords.py add zaf-example-mine-fac

CSV Format:
    facility_id,lat,lon,precision,source,notes
    zaf-karee-mine-fac,-25.7234,27.2156,site,Google Maps,"Main shaft coordinates"
    ind-bailadila-mine-fac,18.6297,81.2644,mine,Company report,

Fields:
    - facility_id: Required (e.g., zaf-karee-mine-fac)
    - lat: Required (decimal degrees, -90 to 90)
    - lon: Required (decimal degrees, -180 to 180)
    - precision: Optional (site/mine/town/region/country, default: site)
    - source: Optional (where you got the coordinates from)
    - notes: Optional (any additional context)
"""

import sys
import csv
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.geocoding import is_valid_coord, in_country_bbox, is_sentinel_coord

ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"


def load_facility(facility_id: str) -> Optional[dict]:
    """Load facility JSON by ID."""
    # Extract country code from facility_id (first 3 chars)
    if len(facility_id) < 3:
        return None

    country = facility_id[:3].upper()
    json_file = FACILITIES_DIR / country / f"{facility_id}.json"

    if not json_file.exists():
        print(f"✗ Facility not found: {json_file}")
        return None

    with open(json_file, 'r') as f:
        return json.load(f)


def save_facility(facility: dict) -> None:
    """Save facility JSON."""
    facility_id = facility['facility_id']
    country = facility['country_iso3']
    json_file = FACILITIES_DIR / country / f"{facility_id}.json"

    with open(json_file, 'w') as f:
        json.dump(facility, f, indent=2, ensure_ascii=False)

    print(f"✓ Saved: {json_file}")


def add_coordinates(
    facility_id: str,
    lat: float,
    lon: float,
    precision: str = "site",
    source: str = "manual_entry",
    notes: str = None,
    dry_run: bool = False
) -> bool:
    """Add coordinates to a facility with validation."""

    # Load facility
    facility = load_facility(facility_id)
    if not facility:
        return False

    country_iso3 = facility['country_iso3']

    # Validation gates
    if is_sentinel_coord(lat, lon):
        print(f"✗ Rejected: Sentinel coordinates ({lat}, {lon})")
        return False

    if not is_valid_coord(lat, lon):
        print(f"✗ Rejected: Invalid coordinates ({lat}, {lon})")
        return False

    if not in_country_bbox(lat, lon, country_iso3):
        print(f"✗ Warning: Coordinates ({lat}, {lon}) outside {country_iso3} bbox")
        response = input("  Continue anyway? (y/N): ")
        if response.lower() != 'y':
            return False

    # Update facility
    old_lat = facility.get('location', {}).get('lat')
    old_lon = facility.get('location', {}).get('lon')

    facility['location'] = {
        'lat': lat,
        'lon': lon,
        'precision': precision
    }

    # Update verification
    if 'verification' not in facility:
        facility['verification'] = {}

    facility['verification']['last_checked'] = datetime.now().isoformat()

    note_parts = [f"Manual coordinate entry: {source}"]
    if notes:
        note_parts.append(notes)
    if old_lat is not None and old_lon is not None:
        note_parts.append(f"(replaced: {old_lat}, {old_lon})")

    facility['verification']['notes'] = " | ".join(note_parts)

    # Display change
    action = "Would update" if dry_run else "Updated"
    if old_lat is not None and old_lon is not None:
        print(f"{action}: {facility['name']}")
        print(f"  Old: {old_lat}, {old_lon}")
        print(f"  New: {lat}, {lon}")
    else:
        print(f"{action}: {facility['name']}")
        print(f"  Coordinates: {lat}, {lon}")

    # Save
    if not dry_run:
        save_facility(facility)

    return True


def import_from_csv(csv_path: str, dry_run: bool = False):
    """Import coordinates from CSV file."""
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"✗ CSV file not found: {csv_file}")
        return

    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)

        # Validate headers
        required = {'facility_id', 'lat', 'lon'}
        if not required.issubset(set(reader.fieldnames)):
            print(f"✗ CSV missing required columns: {required}")
            print(f"  Found: {reader.fieldnames}")
            return

        stats = {"success": 0, "failed": 0}

        for i, row in enumerate(reader, start=1):
            facility_id = row['facility_id'].strip()

            try:
                lat = float(row['lat'])
                lon = float(row['lon'])
            except (ValueError, KeyError) as e:
                print(f"✗ Row {i}: Invalid lat/lon - {e}")
                stats['failed'] += 1
                continue

            precision = row.get('precision', 'site').strip() or 'site'
            source = row.get('source', 'manual_entry').strip() or 'manual_entry'
            notes = row.get('notes', '').strip() or None

            print(f"\n[{i}] {facility_id}")
            success = add_coordinates(
                facility_id, lat, lon, precision, source, notes, dry_run
            )

            if success:
                stats['success'] += 1
            else:
                stats['failed'] += 1

        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        print(f"Successful: {stats['success']}")
        print(f"Failed: {stats['failed']}")


def find_missing_coordinates(country: str = None) -> list:
    """Find all facilities missing coordinates, optionally filtered by country."""
    missing = []

    if country:
        # Single country
        country_dir = FACILITIES_DIR / country.upper()
        if not country_dir.exists():
            print(f"✗ Country not found: {country}")
            return []

        for json_file in sorted(country_dir.glob("*.json")):
            with open(json_file, 'r') as f:
                facility = json.load(f)

            lat = facility.get('location', {}).get('lat')
            lon = facility.get('location', {}).get('lon')

            if lat is None or lon is None:
                missing.append(facility)
    else:
        # All countries
        for country_dir in sorted(FACILITIES_DIR.iterdir()):
            if not country_dir.is_dir():
                continue

            for json_file in sorted(country_dir.glob("*.json")):
                with open(json_file, 'r') as f:
                    facility = json.load(f)

                lat = facility.get('location', {}).get('lat')
                lon = facility.get('location', {}).get('lon')

                if lat is None or lon is None:
                    missing.append(facility)

    return missing


def interactive_add(country: str = None):
    """Interactive mode for adding coordinates - automatically queues missing facilities."""
    print("Manual Coordinate Entry (Interactive Mode)")
    print("="*60)

    # Find facilities missing coordinates
    missing = find_missing_coordinates(country)

    if not missing:
        print("✓ No facilities missing coordinates!")
        return

    print(f"Found {len(missing)} facilities missing coordinates")
    if country:
        print(f"Country: {country.upper()}")
    print()

    stats = {"added": 0, "skipped": 0}

    for i, facility in enumerate(missing, start=1):
        print("="*60)
        print(f"[{i}/{len(missing)}] {facility['facility_id']}")
        print(f"Name: {facility['name']}")
        print(f"Country: {facility['country_iso3']}")

        # Show commodities if available
        commodities = facility.get('commodities', [])
        if commodities:
            metals = [c.get('metal', '') for c in commodities if c.get('metal')]
            if metals:
                print(f"Commodities: {', '.join(metals)}")

        print()

        # Get user input
        try:
            lat_input = input("Latitude (or 's' to skip, 'q' to quit): ").strip()

            if lat_input.lower() == 'q':
                print("\nQuitting...")
                break
            elif lat_input.lower() == 's':
                print("Skipped")
                stats['skipped'] += 1
                continue

            lat = float(lat_input)
            lon = float(input("Longitude: ").strip())

        except ValueError:
            print("✗ Invalid lat/lon - skipping")
            stats['skipped'] += 1
            continue
        except (KeyboardInterrupt, EOFError):
            print("\n\nInterrupted - exiting")
            break

        precision = input("Precision (site/mine/town/region) [site]: ").strip() or "site"
        source = input("Source [manual_entry]: ").strip() or "manual_entry"
        notes = input("Notes (optional): ").strip() or None

        print()
        success = add_coordinates(
            facility['facility_id'], lat, lon, precision, source, notes
        )

        if success:
            stats['added'] += 1

        print()

    print("\n" + "="*60)
    print("SESSION SUMMARY")
    print("="*60)
    print(f"Coordinates added: {stats['added']}")
    print(f"Skipped: {stats['skipped']}")
    print(f"Remaining: {len(missing) - stats['added'] - stats['skipped']}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1

    command = sys.argv[1]

    if command == "list":
        # List facilities missing coordinates
        country = sys.argv[2] if len(sys.argv) > 2 else None
        missing = find_missing_coordinates(country)

        if not missing:
            print("✓ No facilities missing coordinates!")
            return 0

        print(f"Found {len(missing)} facilities missing coordinates")
        if country:
            print(f"Country: {country.upper()}\n")
        else:
            print()

        for facility in missing[:50]:  # Show first 50
            commodities = facility.get('commodities', [])
            metals = [c.get('metal', '') for c in commodities if c.get('metal')]
            metals_str = f" ({', '.join(metals[:3])})" if metals else ""

            print(f"{facility['facility_id']:<35} {facility['name'][:60]}{metals_str}")

        if len(missing) > 50:
            print(f"\n... and {len(missing) - 50} more")

        return 0

    elif command == "import":
        if len(sys.argv) < 3:
            print("Usage: python scripts/manual_coords.py import <csv_file>")
            return 1

        csv_path = sys.argv[2]
        dry_run = "--dry-run" in sys.argv
        import_from_csv(csv_path, dry_run)

    elif command == "add":
        if len(sys.argv) == 2:
            # Interactive mode - all countries
            interactive_add()
        elif len(sys.argv) == 3:
            # Interactive mode - specific country
            country = sys.argv[2]
            interactive_add(country)
        elif len(sys.argv) >= 5:
            # Command-line mode
            facility_id = sys.argv[2]
            lat = float(sys.argv[3])
            lon = float(sys.argv[4])
            precision = sys.argv[5] if len(sys.argv) > 5 else "site"
            source = sys.argv[6] if len(sys.argv) > 6 else "manual_entry"

            add_coordinates(facility_id, lat, lon, precision, source)
        else:
            print("Usage: python scripts/manual_coords.py add <facility_id> <lat> <lon> [precision] [source]")
            return 1

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
