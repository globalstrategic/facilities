#!/usr/bin/env python3
"""
Comprehensive Coordinate Validation and Scanning

Detects and reports problematic coordinates including:
- Null Island (0,0)
- Coordinates at sea/ocean
- Longitude or Latitude at exactly 0
- Out of country boundaries
- Invalid ranges

Usage:
    # Scan all facilities
    python scripts/validate_coordinates.py --scan-all

    # Scan specific country
    python scripts/validate_coordinates.py --country USA

    # Fix facilities interactively
    python scripts/validate_coordinates.py --fix --interactive

    # Export report
    python scripts/validate_coordinates.py --scan-all --export-csv report.csv
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import csv

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from utils.geocoding import is_valid_coord, in_country_bbox, is_sentinel_coord
    HAS_GEOCODING = True
except ImportError:
    HAS_GEOCODING = False
    print("Warning: geocoding utils not available")

try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut
    import time
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False


class CoordinateValidator:
    """Comprehensive coordinate validation."""

    def __init__(self, use_reverse_geocoding: bool = False):
        self.use_reverse_geocoding = use_reverse_geocoding
        self.geocoder = None

        if use_reverse_geocoding and HAS_GEOPY:
            self.geocoder = Nominatim(user_agent="facilities_validator")

        self.issues_found = {
            'null_island': [],
            'longitude_zero': [],
            'latitude_zero': [],
            'out_of_bounds': [],
            'out_of_country': [],
            'in_ocean': [],
            'sentinel_value': [],
            'missing': []
        }

    def is_null_island(self, lat: float, lon: float) -> bool:
        """Check if coordinates are at Null Island (0,0)."""
        return lat == 0.0 and lon == 0.0

    def has_zero_coordinate(self, lat: float, lon: float) -> Tuple[bool, str]:
        """Check if either coordinate is exactly 0."""
        if lat == 0.0 and lon != 0.0:
            return True, 'latitude_zero'
        if lon == 0.0 and lat != 0.0:
            return True, 'longitude_zero'
        return False, None

    def is_in_ocean(self, lat: float, lon: float) -> bool:
        """
        Check if coordinates are in ocean/water using reverse geocoding.
        Returns True if in ocean, False if on land or unknown.
        """
        if not self.use_reverse_geocoding or not self.geocoder:
            return False

        try:
            time.sleep(1)  # Rate limiting
            location = self.geocoder.reverse(f"{lat}, {lon}", timeout=10)

            if not location:
                # No result usually means ocean
                return True

            # Check if address contains water-related terms
            address = location.raw.get('address', {})
            display_name = location.raw.get('display_name', '').lower()

            water_keywords = ['ocean', 'sea', 'water', 'atlantic', 'pacific', 'indian']
            if any(kw in display_name for kw in water_keywords):
                return True

            return False

        except GeocoderTimedOut:
            return False
        except Exception as e:
            print(f"  Warning: Reverse geocoding failed: {e}")
            return False

    def validate_facility_coordinates(self, facility: Dict, facility_id: str, country: str) -> List[Dict]:
        """
        Validate facility coordinates and return list of issues found.

        Returns:
            List of issue dicts with keys: type, facility_id, country, lat, lon, message
        """
        issues = []

        # Check if coordinates exist
        location = facility.get('location')
        if not location:
            issues.append({
                'type': 'missing',
                'facility_id': facility_id,
                'country': country,
                'lat': None,
                'lon': None,
                'message': 'No location object'
            })
            return issues

        lat = location.get('lat')
        lon = location.get('lon')

        if lat is None or lon is None:
            issues.append({
                'type': 'missing',
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': 'Missing lat or lon'
            })
            return issues

        # Convert to float
        try:
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            issues.append({
                'type': 'invalid',
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': f'Invalid coordinate format: {lat}, {lon}'
            })
            return issues

        # Check for Null Island
        if self.is_null_island(lat, lon):
            issues.append({
                'type': 'null_island',
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': 'Coordinates at Null Island (0,0)'
            })
            self.issues_found['null_island'].append((facility_id, country, lat, lon))

        # Check for zero coordinates
        has_zero, zero_type = self.has_zero_coordinate(lat, lon)
        if has_zero:
            issues.append({
                'type': zero_type,
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': f'{zero_type.replace("_", " ").title()}'
            })
            self.issues_found[zero_type].append((facility_id, country, lat, lon))

        # Check if coordinates are valid (within Earth bounds)
        if HAS_GEOCODING and not is_valid_coord(lat, lon):
            issues.append({
                'type': 'out_of_bounds',
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': f'Coordinates out of valid range (lat: {lat}, lon: {lon})'
            })
            self.issues_found['out_of_bounds'].append((facility_id, country, lat, lon))

        # Check if sentinel value
        if HAS_GEOCODING and is_sentinel_coord(lat, lon):
            issues.append({
                'type': 'sentinel_value',
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': 'Known bad sentinel coordinate'
            })
            self.issues_found['sentinel_value'].append((facility_id, country, lat, lon))

        # Check if in country bounding box
        if HAS_GEOCODING and not in_country_bbox(lat, lon, country):
            issues.append({
                'type': 'out_of_country',
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': f'Coordinates outside {country} bounding box'
            })
            self.issues_found['out_of_country'].append((facility_id, country, lat, lon))

        # Check if in ocean (expensive, only if requested)
        if self.use_reverse_geocoding and self.is_in_ocean(lat, lon):
            issues.append({
                'type': 'in_ocean',
                'facility_id': facility_id,
                'country': country,
                'lat': lat,
                'lon': lon,
                'message': 'Coordinates appear to be in ocean/water'
            })
            self.issues_found['in_ocean'].append((facility_id, country, lat, lon))

        return issues

    def scan_country(self, country_iso3: str, verbose: bool = True) -> List[Dict]:
        """Scan all facilities in a country for coordinate issues."""
        facilities_dir = Path(__file__).parent.parent / 'facilities' / country_iso3

        if not facilities_dir.exists():
            print(f"Country directory not found: {country_iso3}")
            return []

        facility_files = sorted(facilities_dir.glob('*.json'))
        all_issues = []

        if verbose:
            print(f"\nScanning {len(facility_files)} facilities in {country_iso3}...")

        for facility_file in facility_files:
            try:
                with open(facility_file) as f:
                    facility = json.load(f)

                facility_id = facility.get('facility_id', facility_file.stem)
                issues = self.validate_facility_coordinates(facility, facility_id, country_iso3)

                if issues:
                    all_issues.extend(issues)
                    if verbose:
                        for issue in issues:
                            print(f"  ⚠ {facility_id}: {issue['message']}")

            except Exception as e:
                print(f"Error processing {facility_file.name}: {e}")

        return all_issues

    def scan_all_countries(self, verbose: bool = True) -> List[Dict]:
        """Scan all countries for coordinate issues."""
        facilities_dir = Path(__file__).parent.parent / 'facilities'

        if not facilities_dir.exists():
            print("Facilities directory not found")
            return []

        countries = sorted([d.name for d in facilities_dir.iterdir() if d.is_dir()])
        all_issues = []

        print(f"\nScanning {len(countries)} countries...")
        print("="*70)

        for country in countries:
            issues = self.scan_country(country, verbose=verbose)
            all_issues.extend(issues)

        return all_issues

    def print_summary(self):
        """Print summary of issues found."""
        print("\n" + "="*70)
        print("COORDINATE VALIDATION SUMMARY")
        print("="*70)

        total_issues = sum(len(v) for v in self.issues_found.values())

        print(f"\nTotal issues found: {total_issues}\n")

        for issue_type, facilities in self.issues_found.items():
            if facilities:
                print(f"{issue_type.replace('_', ' ').title()}: {len(facilities)}")
                for fac_id, country, lat, lon in facilities[:5]:
                    print(f"  - {fac_id} ({country}): {lat}, {lon}")
                if len(facilities) > 5:
                    print(f"  ... and {len(facilities) - 5} more")
                print()

        print("="*70)

    def export_csv(self, issues: List[Dict], output_file: str):
        """Export issues to CSV file."""
        if not issues:
            print("No issues to export")
            return

        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['type', 'facility_id', 'country', 'lat', 'lon', 'message'])
            writer.writeheader()
            writer.writerows(issues)

        print(f"\n✓ Exported {len(issues)} issues to {output_file}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Validate facility coordinates")
    parser.add_argument('--country', help='Scan specific country (ISO3 code)')
    parser.add_argument('--scan-all', action='store_true', help='Scan all countries')
    parser.add_argument('--check-ocean', action='store_true',
                       help='Use reverse geocoding to check for ocean coordinates (slow)')
    parser.add_argument('--export-csv', help='Export issues to CSV file')
    parser.add_argument('--verbose', action='store_true', default=True,
                       help='Print detailed output')

    args = parser.parse_args()

    validator = CoordinateValidator(use_reverse_geocoding=args.check_ocean)

    issues = []
    if args.scan_all:
        issues = validator.scan_all_countries(verbose=args.verbose)
    elif args.country:
        issues = validator.scan_country(args.country, verbose=args.verbose)
    else:
        print("Error: Must specify --scan-all or --country")
        return 1

    validator.print_summary()

    if args.export_csv:
        validator.export_csv(issues, args.export_csv)

    return 0


if __name__ == '__main__':
    sys.exit(main())
