#!/usr/bin/env python3
"""
Find all facilities with incomplete or missing lat/lon coordinates.

This script:
1. Loads all facilities from the JSON files
2. Exports all to CSV (Mines.csv)
3. Identifies facilities with missing/incomplete coordinates
4. Generates summary report by country
"""

import json
import csv
from pathlib import Path
from collections import defaultdict


def load_all_facilities(facilities_dir):
    """Load all facility JSON files."""
    facilities = []

    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir():
            continue

        for json_file in sorted(country_dir.glob("*.json")):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)
                    facility['_file_path'] = str(json_file)
                    facilities.append(facility)
            except Exception as e:
                print(f"Error loading {json_file}: {e}")

    return facilities


def has_complete_coords(facility):
    """Check if facility has complete coordinates."""
    # Coordinates are stored in location.lat and location.lon
    location = facility.get('location', {})

    if not isinstance(location, dict):
        return False

    lat = location.get('lat')
    lon = location.get('lon')

    # Check if both exist and are not None/empty
    if lat is None or lon is None:
        return False

    # Check if they are valid numbers (not empty strings or 0,0)
    try:
        lat_f = float(lat)
        lon_f = float(lon)

        # Exclude (0,0) as it's likely a placeholder
        if lat_f == 0.0 and lon_f == 0.0:
            return False

        return True
    except (ValueError, TypeError):
        return False


def export_to_csv(facilities, output_path):
    """Export all facilities to CSV."""
    if not facilities:
        print("No facilities to export!")
        return

    # Define CSV columns
    columns = [
        'facility_id',
        'name',
        'country_iso3',
        'latitude',
        'longitude',
        'location',
        'province',
        'status',
        'facility_type',
        'primary_metal',
        'metals',
        'operator',
        'operator_link',
        'owner',
        'owner_links',
        'confidence',
        'source',
        '_file_path'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()

        for facility in facilities:
            # Flatten complex fields
            row = facility.copy()

            # Extract lat/lon from location object
            location = facility.get('location', {})
            if isinstance(location, dict):
                row['latitude'] = location.get('lat')
                row['longitude'] = location.get('lon')
                # Keep location as string for reference
                row['location'] = str(location)
            else:
                row['latitude'] = None
                row['longitude'] = None
                row['location'] = str(location) if location else ''

            # Convert lists to comma-separated strings
            if 'metals' in row and isinstance(row['metals'], list):
                row['metals'] = ', '.join(row['metals'])
            if 'owner_links' in row and isinstance(row['owner_links'], list):
                row['owner_links'] = ', '.join(row['owner_links'])

            # Get primary metal from commodities
            commodities = facility.get('commodities', [])
            if commodities and isinstance(commodities, list):
                # Find primary commodity or just use first
                primary = next((c for c in commodities if c.get('primary')), commodities[0] if commodities else None)
                row['primary_metal'] = primary.get('metal', '') if primary else ''
            else:
                row['primary_metal'] = ''

            writer.writerow(row)

    print(f"Exported {len(facilities)} facilities to {output_path}")


def main():
    # Setup paths
    script_dir = Path(__file__).parent
    facilities_dir = script_dir.parent / "facilities"
    output_dir = script_dir.parent / "output"
    output_dir.mkdir(exist_ok=True)

    # Load all facilities
    print("Loading all facilities...")
    facilities = load_all_facilities(facilities_dir)
    print(f"Loaded {len(facilities)} facilities from {len(list(facilities_dir.iterdir()))} countries")

    # Export all to CSV
    csv_path = output_dir / "Mines.csv"
    print(f"\nExporting to {csv_path}...")
    export_to_csv(facilities, csv_path)

    # Find facilities with missing/incomplete coordinates
    print("\n" + "="*80)
    print("ANALYZING COORDINATE COMPLETENESS")
    print("="*80)

    missing_coords = []
    by_country = defaultdict(list)

    for facility in facilities:
        if not has_complete_coords(facility):
            missing_coords.append(facility)
            by_country[facility.get('country_iso3', 'UNKNOWN')].append(facility)

    # Export missing to separate CSV
    if missing_coords:
        missing_csv_path = output_dir / "Mines_Missing_Coords.csv"
        export_to_csv(missing_coords, missing_csv_path)
        print(f"\nExported {len(missing_coords)} facilities with missing coords to {missing_csv_path}")

    # Summary statistics
    print(f"\nSUMMARY:")
    print(f"  Total facilities: {len(facilities)}")
    print(f"  With complete coordinates: {len(facilities) - len(missing_coords)} ({100*(len(facilities)-len(missing_coords))/len(facilities):.1f}%)")
    print(f"  Missing/incomplete coordinates: {len(missing_coords)} ({100*len(missing_coords)/len(facilities):.1f}%)")

    # Breakdown by country
    if by_country:
        print(f"\nBREAKDOWN BY COUNTRY (sorted by count):")
        print(f"{'Country':<10} {'Missing':<10} {'Total':<10} {'%Missing':<10}")
        print("-" * 42)

        # Count totals per country
        country_totals = defaultdict(int)
        for facility in facilities:
            country_totals[facility.get('country_iso3', 'UNKNOWN')] += 1

        # Sort by missing count
        sorted_countries = sorted(by_country.items(), key=lambda x: len(x[1]), reverse=True)

        for country, missing_list in sorted_countries:
            total = country_totals[country]
            missing_count = len(missing_list)
            pct = 100 * missing_count / total if total > 0 else 0
            print(f"{country:<10} {missing_count:<10} {total:<10} {pct:<10.1f}")

    # Show some examples
    if missing_coords:
        print(f"\nFIRST 20 EXAMPLES OF MISSING COORDINATES:")
        print("-" * 80)
        for i, facility in enumerate(missing_coords[:20], 1):
            location = facility.get('location', {})
            print(f"{i}. {facility.get('name', 'N/A')} ({facility.get('country_iso3', 'N/A')})")
            print(f"   ID: {facility.get('facility_id', 'N/A')}")
            if isinstance(location, dict):
                print(f"   Location dict: lat={location.get('lat')}, lon={location.get('lon')}")
                if 'town' in location:
                    print(f"   Town: {location.get('town')}")
                if 'region' in location:
                    print(f"   Region: {location.get('region')}")
                if 'precision' in location:
                    print(f"   Precision: {location.get('precision')}")
            else:
                print(f"   Location: {location}")
            print()

    print(f"\nOutput files:")
    print(f"  All facilities: {csv_path}")
    if missing_coords:
        print(f"  Missing coords: {missing_csv_path}")


if __name__ == '__main__':
    main()
