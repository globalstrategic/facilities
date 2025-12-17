#!/usr/bin/env python3
"""
Validate and fix geocoding errors in facilities database.

Detects and optionally fixes:
- Coordinates outside country boundaries
- Coordinates in the ocean
- Invalid coordinates (lat/lon out of range)
- Potential lat/lon swaps
- Facilities with wrong country codes

Usage:
    # Query Snowflake directly (recommended)
    python validate_geocoding.py --snowflake --check
    python validate_geocoding.py --snowflake --country-id 148
    python validate_geocoding.py --snowflake --export errors.json

    # Query local JSON files
    python validate_geocoding.py --check                    # Check all facilities
    python validate_geocoding.py --country BRA              # Check specific country
    python validate_geocoding.py --fix --dry-run            # Preview fixes
    python validate_geocoding.py --fix                      # Apply fixes
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, asdict
import math

# Paths
ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"
sys.path.insert(0, str(ROOT / "scripts"))

from utils.geocoding import COUNTRY_BBOX

# Snowflake imports (optional - only needed for --snowflake mode)
try:
    import snowflake.connector
    import pandas as pd
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


# Extended country bounding boxes (expand the existing dict)
EXTENDED_COUNTRY_BBOX = {
    **COUNTRY_BBOX,
    # Add more countries as needed
    "ARG": {"lat_min": -55.0, "lat_max": -21.0, "lon_min": -74.0, "lon_max": -53.0},
    "BOL": {"lat_min": -23.0, "lat_max": -9.0, "lon_min": -70.0, "lon_max": -57.0},
    "ECU": {"lat_min": -5.0, "lat_max": 2.0, "lon_min": -92.0, "lon_max": -75.0},
    "VEN": {"lat_min": 0.0, "lat_max": 13.0, "lon_min": -74.0, "lon_max": -59.0},
    "PRY": {"lat_min": -28.0, "lat_max": -19.0, "lon_min": -63.0, "lon_max": -54.0},
    "URY": {"lat_min": -35.0, "lat_max": -30.0, "lon_min": -59.0, "lon_max": -53.0},
    "GUY": {"lat_min": 1.0, "lat_max": 9.0, "lon_min": -62.0, "lon_max": -56.0},
    "SUR": {"lat_min": 2.0, "lat_max": 6.0, "lon_min": -59.0, "lon_max": -54.0},
    "FRA": {"lat_min": -21.0, "lat_max": 51.0, "lon_min": -62.0, "lon_max": 10.0},  # Including territories
}


@dataclass
class GeocodingError:
    """Represents a geocoding error."""
    facility_id: str
    name: str
    country_iso3: str
    lat: Optional[float]
    lon: Optional[float]
    error_type: str
    severity: str  # 'critical', 'high', 'medium', 'low'
    details: str
    suggested_fix: Optional[Dict] = None
    file_path: str = ""


class GeocodingValidator:
    """Validate and fix geocoding errors in facilities."""

    def __init__(self, use_snowflake: bool = False):
        self.errors: List[GeocodingError] = []
        self.stats = defaultdict(int)
        self.country_bbox = EXTENDED_COUNTRY_BBOX
        self.use_snowflake = use_snowflake
        self.snowflake_conn = None
        self.country_id_map = {}  # Maps country_id to ISO3

        if use_snowflake:
            if not SNOWFLAKE_AVAILABLE:
                raise RuntimeError("Snowflake packages not available. Install: pip install snowflake-connector-python pandas cryptography")
            self._connect_snowflake()

    def _connect_snowflake(self):
        """Connect to Snowflake using private key authentication."""
        key_path = Path.home() / ".snowsql" / "rsa_key.p8"
        if not key_path.exists():
            raise FileNotFoundError(f"Snowflake private key not found: {key_path}")

        with open(key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        self.snowflake_conn = snowflake.connector.connect(
            account="HDHACZZ-AE73585",
            user="WILLIAM.BRODHEAD",
            private_key=pkb,
            warehouse="GSMC_WH_XS",
            database="MIKHAIL",
            schema="ENTITY",
            role="ACCOUNTADMIN"
        )

        # Load country ID to ISO3 mapping
        cur = self.snowflake_conn.cursor()
        cur.execute("SELECT ID, ISO3, NAME FROM ENTITY.COUNTRY")
        for row in cur.fetchall():
            country_id, iso3, name = row
            self.country_id_map[country_id] = {'iso3': iso3, 'name': name}
        cur.close()

        print(f"Connected to Snowflake. Loaded {len(self.country_id_map)} countries.")

    def _close_snowflake(self):
        """Close Snowflake connection."""
        if self.snowflake_conn:
            self.snowflake_conn.close()

    def validate_snowflake_facility(self, row: Dict) -> List[GeocodingError]:
        """Validate a facility from Snowflake."""
        facility_id = row.get('FACILITY_ID', 'unknown')
        name = row.get('NAME', '')
        country_id = row.get('COUNTRY_ID')
        lat = row.get('LATITUDE')
        lon = row.get('LONGITUDE')

        # Map country_id to ISO3
        country_iso3 = 'UNK'
        if country_id and country_id in self.country_id_map:
            country_iso3 = self.country_id_map[country_id]['iso3']

        # Create a facility-like dict for validation
        facility = {
            'facility_id': facility_id,
            'name': name,
            'country_iso3': country_iso3,
            'location': {
                'lat': lat,
                'lon': lon
            }
        }

        # Use existing validation logic
        errors = []

        # Error 1: Missing coordinates
        if lat is None or lon is None:
            errors.append(GeocodingError(
                facility_id=facility_id,
                name=name,
                country_iso3=country_iso3,
                lat=lat,
                lon=lon,
                error_type='missing_coordinates',
                severity='medium',
                details=f'Facility has no coordinates (country_id={country_id})',
                file_path=f'SNOWFLAKE:country_id={country_id}'
            ))
            return errors

        # Error 2: Invalid coordinate ranges
        if not self.is_valid_coordinate(lat, lon):
            errors.append(GeocodingError(
                facility_id=facility_id,
                name=name,
                country_iso3=country_iso3,
                lat=lat,
                lon=lon,
                error_type='invalid_range',
                severity='critical',
                details=f'Coordinates out of valid range: lat={lat}, lon={lon} (country_id={country_id})',
                file_path=f'SNOWFLAKE:country_id={country_id}'
            ))
            return errors

        # Error 3: Coordinates outside country bounds
        if country_iso3 in self.country_bbox:
            if not self.is_in_country_bounds(lat, lon, country_iso3):
                # Check if it's a lat/lon swap
                if self.check_latlon_swap(lat, lon, country_iso3):
                    errors.append(GeocodingError(
                        facility_id=facility_id,
                        name=name,
                        country_iso3=country_iso3,
                        lat=lat,
                        lon=lon,
                        error_type='latlon_swap',
                        severity='high',
                        details=f'Coordinates appear to be swapped (currently {lat}, {lon}) (country_id={country_id})',
                        suggested_fix={'lat': lon, 'lon': lat},
                        file_path=f'SNOWFLAKE:country_id={country_id}'
                    ))
                else:
                    # Check if in ocean
                    in_ocean = self.is_likely_ocean(lat, lon)
                    distance = self.get_distance_from_country(lat, lon, country_iso3)

                    severity = 'critical' if in_ocean or distance > 50 else 'high'
                    details = f'Coordinates outside {country_iso3} bounds: ({lat}, {lon}) (country_id={country_id})'
                    if in_ocean:
                        details += ' - LIKELY IN OCEAN'
                    details += f' - Distance from country: {distance:.1f}°'

                    errors.append(GeocodingError(
                        facility_id=facility_id,
                        name=name,
                        country_iso3=country_iso3,
                        lat=lat,
                        lon=lon,
                        error_type='wrong_country' if not in_ocean else 'in_ocean',
                        severity=severity,
                        details=details,
                        file_path=f'SNOWFLAKE:country_id={country_id}'
                    ))

        return errors

    def validate_snowflake(self, country_id: Optional[int] = None,
                          limit: Optional[int] = None) -> List[GeocodingError]:
        """Validate facilities directly from Snowflake."""
        if not self.use_snowflake or not self.snowflake_conn:
            raise RuntimeError("Not connected to Snowflake. Initialize with use_snowflake=True")

        cur = self.snowflake_conn.cursor()

        # Build query
        query = """
            SELECT
                FACILITY_ID,
                NAME,
                COUNTRY_ID,
                LATITUDE,
                LONGITUDE,
                FACILITY_TYPE,
                PRIMARY_METAL
            FROM ENTITY.FACILITY
        """

        if country_id is not None:
            query += f" WHERE COUNTRY_ID = {country_id}"

        if limit:
            query += f" LIMIT {limit}"

        print(f"Querying Snowflake: {query[:100]}...")
        cur.execute(query)

        all_errors = []
        row_count = 0

        for row in cur.fetchall():
            row_count += 1
            row_dict = {
                'FACILITY_ID': row[0],
                'NAME': row[1],
                'COUNTRY_ID': row[2],
                'LATITUDE': row[3],
                'LONGITUDE': row[4],
                'FACILITY_TYPE': row[5],
                'PRIMARY_METAL': row[6]
            }

            errors = self.validate_snowflake_facility(row_dict)
            all_errors.extend(errors)

            for error in errors:
                self.stats[error.error_type] += 1
                self.stats[f'{error.severity}_severity'] += 1

        cur.close()
        self.errors = all_errors

        print(f"Validated {row_count} facilities from Snowflake.")
        return all_errors

    def is_valid_coordinate(self, lat: Optional[float], lon: Optional[float]) -> bool:
        """Check if coordinates are within valid ranges."""
        if lat is None or lon is None:
            return False
        return -90 <= lat <= 90 and -180 <= lon <= 180

    def is_in_country_bounds(self, lat: float, lon: float, country_iso3: str) -> bool:
        """Check if coordinates are within country bounding box."""
        if country_iso3 not in self.country_bbox:
            # No bounding box defined, can't validate
            return True

        bbox = self.country_bbox[country_iso3]
        lat_ok = bbox["lat_min"] <= lat <= bbox["lat_max"]
        lon_ok = bbox["lon_min"] <= lon <= bbox["lon_max"]

        return lat_ok and lon_ok

    def check_latlon_swap(self, lat: float, lon: float, country_iso3: str) -> bool:
        """Check if swapping lat/lon would put coordinates in correct country."""
        if country_iso3 not in self.country_bbox:
            return False

        # Check if swapped coordinates are valid
        if not self.is_valid_coordinate(lon, lat):
            return False

        # Check if swapped coordinates fall within country bounds
        return self.is_in_country_bounds(lon, lat, country_iso3)

    def is_likely_ocean(self, lat: float, lon: float) -> bool:
        """
        Heuristic check if coordinates are likely in ocean.
        Uses simple distance-from-land heuristic.
        """
        # Major landmass boundaries (very rough)
        # This is a simplified check - for production use a proper land/ocean dataset

        # Deep ocean areas (middle of oceans, far from land)
        ocean_zones = [
            # Atlantic (mid-ocean)
            {"lat_min": -60, "lat_max": 60, "lon_min": -50, "lon_max": -10},
            # Pacific (mid-ocean)
            {"lat_min": -60, "lat_max": 60, "lon_min": 140, "lon_max": -90},
            # Indian Ocean (mid-ocean)
            {"lat_min": -50, "lat_max": 20, "lon_min": 50, "lon_max": 100},
            # Southern Ocean
            {"lat_min": -90, "lat_max": -60, "lon_min": -180, "lon_max": 180},
        ]

        for zone in ocean_zones:
            if (zone["lat_min"] <= lat <= zone["lat_max"] and
                zone["lon_min"] <= lon <= zone["lon_max"]):
                return True

        return False

    def get_distance_from_country(self, lat: float, lon: float, country_iso3: str) -> float:
        """
        Calculate approximate distance from coordinates to country bounding box center.
        Returns distance in degrees (rough approximation).
        """
        if country_iso3 not in self.country_bbox:
            return 0.0

        bbox = self.country_bbox[country_iso3]
        center_lat = (bbox["lat_min"] + bbox["lat_max"]) / 2
        center_lon = (bbox["lon_min"] + bbox["lon_max"]) / 2

        # Euclidean distance (rough approximation)
        return math.sqrt((lat - center_lat)**2 + (lon - center_lon)**2)

    def validate_facility(self, facility: Dict, file_path: Path) -> List[GeocodingError]:
        """Validate a single facility's geocoding."""
        errors = []

        facility_id = facility.get('facility_id', 'unknown')
        name = facility.get('name', '')
        country_iso3 = facility.get('country_iso3', '')
        location = facility.get('location', {})
        lat = location.get('lat')
        lon = location.get('lon')

        # Error 1: Missing coordinates
        if lat is None or lon is None:
            errors.append(GeocodingError(
                facility_id=facility_id,
                name=name,
                country_iso3=country_iso3,
                lat=lat,
                lon=lon,
                error_type='missing_coordinates',
                severity='medium',
                details='Facility has no coordinates',
                file_path=str(file_path.relative_to(ROOT))
            ))
            return errors  # Can't validate further

        # Error 2: Invalid coordinate ranges
        if not self.is_valid_coordinate(lat, lon):
            errors.append(GeocodingError(
                facility_id=facility_id,
                name=name,
                country_iso3=country_iso3,
                lat=lat,
                lon=lon,
                error_type='invalid_range',
                severity='critical',
                details=f'Coordinates out of valid range: lat={lat}, lon={lon}',
                file_path=str(file_path.relative_to(ROOT))
            ))
            return errors

        # Error 3: Coordinates outside country bounds
        if country_iso3 in self.country_bbox:
            if not self.is_in_country_bounds(lat, lon, country_iso3):
                # Check if it's a lat/lon swap
                if self.check_latlon_swap(lat, lon, country_iso3):
                    errors.append(GeocodingError(
                        facility_id=facility_id,
                        name=name,
                        country_iso3=country_iso3,
                        lat=lat,
                        lon=lon,
                        error_type='latlon_swap',
                        severity='high',
                        details=f'Coordinates appear to be swapped (currently {lat}, {lon})',
                        suggested_fix={'lat': lon, 'lon': lat},
                        file_path=str(file_path.relative_to(ROOT))
                    ))
                else:
                    # Check if in ocean
                    in_ocean = self.is_likely_ocean(lat, lon)
                    distance = self.get_distance_from_country(lat, lon, country_iso3)

                    severity = 'critical' if in_ocean or distance > 50 else 'high'
                    details = f'Coordinates outside {country_iso3} bounds: ({lat}, {lon})'
                    if in_ocean:
                        details += ' - LIKELY IN OCEAN'
                    details += f' - Distance from country: {distance:.1f}°'

                    errors.append(GeocodingError(
                        facility_id=facility_id,
                        name=name,
                        country_iso3=country_iso3,
                        lat=lat,
                        lon=lon,
                        error_type='wrong_country' if not in_ocean else 'in_ocean',
                        severity=severity,
                        details=details,
                        file_path=str(file_path.relative_to(ROOT))
                    ))

        # Error 4: Suspicious precision (e.g., exact round numbers)
        if lat is not None and lon is not None:
            if lat == round(lat) and lon == round(lon):
                errors.append(GeocodingError(
                    facility_id=facility_id,
                    name=name,
                    country_iso3=country_iso3,
                    lat=lat,
                    lon=lon,
                    error_type='suspicious_precision',
                    severity='low',
                    details=f'Coordinates are suspiciously round: ({lat}, {lon})',
                    file_path=str(file_path.relative_to(ROOT))
                ))

        return errors

    def validate_country(self, country_code: str) -> List[GeocodingError]:
        """Validate all facilities in a country."""
        country_dir = FACILITIES_DIR / country_code
        if not country_dir.exists():
            print(f"Country directory not found: {country_code}")
            return []

        country_errors = []

        for facility_file in country_dir.glob("*.json"):
            try:
                with open(facility_file, 'r') as f:
                    facility = json.load(f)

                errors = self.validate_facility(facility, facility_file)
                country_errors.extend(errors)

                for error in errors:
                    self.stats[error.error_type] += 1
                    self.stats[f'{error.severity}_severity'] += 1

            except Exception as e:
                print(f"Error reading {facility_file}: {e}")

        return country_errors

    def validate_all(self, countries: Optional[List[str]] = None) -> List[GeocodingError]:
        """Validate all facilities or specific countries."""
        if countries:
            country_dirs = [FACILITIES_DIR / c for c in countries if (FACILITIES_DIR / c).exists()]
        else:
            country_dirs = [d for d in FACILITIES_DIR.iterdir() if d.is_dir()]

        print(f"Validating geocoding for {len(country_dirs)} countries...")

        all_errors = []
        for country_dir in sorted(country_dirs):
            errors = self.validate_country(country_dir.name)
            all_errors.extend(errors)

        self.errors = all_errors
        return all_errors

    def fix_facility(self, error: GeocodingError, dry_run: bool = True) -> bool:
        """
        Fix a geocoding error in the facility file.
        Returns True if fix was applied (or would be applied in dry-run).
        """
        if error.error_type == 'latlon_swap' and error.suggested_fix:
            facility_path = ROOT / error.file_path

            if not facility_path.exists():
                print(f"  Error: File not found: {facility_path}")
                return False

            try:
                with open(facility_path, 'r') as f:
                    facility = json.load(f)

                old_lat = facility['location']['lat']
                old_lon = facility['location']['lon']
                new_lat = error.suggested_fix['lat']
                new_lon = error.suggested_fix['lon']

                print(f"  {error.facility_id}: ({old_lat}, {old_lon}) → ({new_lat}, {new_lon})")

                if not dry_run:
                    facility['location']['lat'] = new_lat
                    facility['location']['lon'] = new_lon

                    # Add note about fix
                    if 'notes' not in facility:
                        facility['notes'] = ''
                    facility['notes'] += f'\n[AUTO-FIX] Swapped lat/lon coordinates on {Path(__file__).name}'

                    with open(facility_path, 'w') as f:
                        json.dump(facility, f, indent=2, ensure_ascii=False)
                        f.write('\n')

                return True

            except Exception as e:
                print(f"  Error fixing {error.facility_id}: {e}")
                return False

        return False

    def apply_fixes(self, dry_run: bool = True) -> int:
        """Apply automatic fixes to facilities with suggested fixes."""
        fixable_errors = [e for e in self.errors if e.suggested_fix]

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Applying fixes to {len(fixable_errors)} facilities...")

        fixed_count = 0
        for error in fixable_errors:
            if self.fix_facility(error, dry_run=dry_run):
                fixed_count += 1

        print(f"\n{'Would fix' if dry_run else 'Fixed'} {fixed_count}/{len(fixable_errors)} facilities")
        return fixed_count

    def print_summary(self):
        """Print validation summary."""
        print("\n" + "="*80)
        print("GEOCODING VALIDATION SUMMARY")
        print("="*80)

        total_errors = len(self.errors)
        facilities_with_errors = len(set(e.facility_id for e in self.errors))

        print(f"\nTotal geocoding errors: {total_errors}")
        print(f"Facilities with errors: {facilities_with_errors}")

        print("\nErrors by type:")
        error_types = defaultdict(int)
        for error in self.errors:
            error_types[error.error_type] += 1

        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
            print(f"  • {error_type:30s} {count:5d}")

        print("\nErrors by severity:")
        for severity in ['critical', 'high', 'medium', 'low']:
            count = self.stats.get(f'{severity}_severity', 0)
            if count > 0:
                print(f"  • {severity.upper():30s} {count:5d}")

        # Show fixable errors
        fixable = len([e for e in self.errors if e.suggested_fix])
        if fixable > 0:
            print(f"\nAuto-fixable errors: {fixable} (use --fix to apply)")

        print("\n" + "="*80)

    def print_detailed_report(self, error_type: Optional[str] = None,
                            severity: Optional[str] = None,
                            limit: int = 20):
        """Print detailed error report."""
        errors_to_show = self.errors

        if error_type:
            errors_to_show = [e for e in errors_to_show if e.error_type == error_type]

        if severity:
            errors_to_show = [e for e in errors_to_show if e.severity == severity]

        if not errors_to_show:
            print(f"No errors found matching criteria")
            return

        print(f"\nShowing {min(len(errors_to_show), limit)} of {len(errors_to_show)} errors:")
        print("-" * 80)

        for error in errors_to_show[:limit]:
            print(f"\n{error.facility_id} ({error.country_iso3}) - {error.severity.upper()}")
            print(f"  Name: {error.name}")
            print(f"  Type: {error.error_type}")
            print(f"  Coords: ({error.lat}, {error.lon})")
            print(f"  Details: {error.details}")
            if error.suggested_fix:
                print(f"  Suggested fix: {error.suggested_fix}")
            print(f"  File: {error.file_path}")

        if len(errors_to_show) > limit:
            print(f"\n... and {len(errors_to_show) - limit} more")

    def export_errors(self, output_path: Path):
        """Export errors to JSON file."""
        output_data = {
            'validation_date': str(Path(__file__).name),
            'total_errors': len(self.errors),
            'facilities_affected': len(set(e.facility_id for e in self.errors)),
            'statistics': dict(self.stats),
            'errors': [asdict(e) for e in self.errors]
        }

        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)

        print(f"\nErrors exported to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Validate and fix geocoding errors in facilities database",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--snowflake',
        action='store_true',
        help='Query Snowflake directly instead of local JSON files'
    )
    parser.add_argument(
        '--country',
        help='[JSON mode] Validate specific country ISO3 code (e.g., BRA, USA)'
    )
    parser.add_argument(
        '--country-id',
        type=int,
        help='[Snowflake mode] Validate specific country by Snowflake country_id (e.g., 148)'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Run validation check (default action)'
    )
    parser.add_argument(
        '--fix',
        action='store_true',
        help='[JSON mode only] Apply automatic fixes to errors'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview fixes without applying them (use with --fix)'
    )
    parser.add_argument(
        '--error-type',
        choices=['missing_coordinates', 'invalid_range', 'latlon_swap',
                'wrong_country', 'in_ocean', 'suspicious_precision'],
        help='Show only specific error type'
    )
    parser.add_argument(
        '--severity',
        choices=['critical', 'high', 'medium', 'low'],
        help='Show only specific severity level'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=20,
        help='Limit number of errors shown (default: 20)'
    )
    parser.add_argument(
        '--query-limit',
        type=int,
        help='[Snowflake mode] Limit number of facilities to query'
    )
    parser.add_argument(
        '--export',
        type=Path,
        help='Export errors to JSON file'
    )

    args = parser.parse_args()

    # Create validator
    try:
        validator = GeocodingValidator(use_snowflake=args.snowflake)
    except Exception as e:
        print(f"Error initializing validator: {e}", file=sys.stderr)
        return 1

    try:
        # Run validation
        if args.snowflake:
            # Snowflake mode
            validator.validate_snowflake(
                country_id=args.country_id,
                limit=args.query_limit
            )
        else:
            # JSON file mode
            if args.country:
                validator.validate_country(args.country)
            else:
                validator.validate_all()

        # Print results
        validator.print_summary()

        if args.error_type or args.severity:
            validator.print_detailed_report(
                error_type=args.error_type,
                severity=args.severity,
                limit=args.limit
            )
        elif not args.fix:
            # Show detailed report for critical/high severity
            validator.print_detailed_report(severity='critical', limit=10)
            if args.severity != 'critical':
                validator.print_detailed_report(severity='high', limit=10)

        # Apply fixes if requested (JSON mode only)
        if args.fix:
            if args.snowflake:
                print("\nError: --fix is not supported in Snowflake mode. Fix errors in JSON files first, then re-export to Snowflake.")
                return 1
            validator.apply_fixes(dry_run=args.dry_run)

        # Export if requested
        if args.export:
            validator.export_errors(args.export)

    finally:
        # Clean up Snowflake connection
        if args.snowflake:
            validator._close_snowflake()

    return 0


if __name__ == "__main__":
    sys.exit(main())
