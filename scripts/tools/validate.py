#!/usr/bin/env python3
"""
Unified facility validation tool.

Consolidates:
- validate_country_polygons.py (Natural Earth polygon validation)
- validate_geocoding.py (bounding box, range, and Snowflake validation)

Subcommands:
    polygons    Validate coordinates against actual country polygons (requires geopandas)
    geocoding   Validate coordinates using bounding boxes (faster, no geopandas)

Usage:
    python validate.py polygons                     # All countries with 10m resolution
    python validate.py polygons --country USA       # Single country
    python validate.py polygons --resolution 110m   # Faster, less accurate

    python validate.py geocoding                    # All countries
    python validate.py geocoding --country BRA      # Single country
    python validate.py geocoding --snowflake        # Query Snowflake directly
    python validate.py geocoding --fix              # Apply automatic fixes
"""

import json
import argparse
import sys
import math
import urllib.request
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, asdict
import warnings

warnings.filterwarnings('ignore', category=FutureWarning)

ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"
CACHE_DIR = ROOT / ".cache"

sys.path.insert(0, str(ROOT / "scripts"))

# Try importing geopandas for polygon validation
try:
    import geopandas as gpd
    from shapely.geometry import Point
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False

# Try importing Snowflake for direct DB validation
try:
    import snowflake.connector
    import pandas as pd
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


# =============================================================================
# SHARED DATA
# =============================================================================

# Natural Earth URLs
NATURAL_EARTH_URLS = {
    "10m": "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip",
    "110m": "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip",
}

# Territory mappings
ISO3_FIXES = {
    "FRA": ["FRA", "GUF"], "NLD": ["NLD", "ABW", "CUW"],
    "DNK": ["DNK", "GRL"], "NOR": ["NOR", "SJM"],
    "USA": ["USA", "PRI", "VIR", "GUM", "ASM"],
    "GBR": ["GBR", "FLK", "GIB", "SGS"],
    "AUS": ["AUS", "NFK", "CCK", "CXR"],
}

TERRITORY_TO_SOVEREIGN = {
    "GUF": "FRA", "GLP": "FRA", "MTQ": "FRA", "REU": "FRA", "MYT": "FRA", "NCL": "FRA", "PYF": "FRA",
    "ABW": "NLD", "CUW": "NLD", "SXM": "NLD", "GRL": "DNK", "FRO": "DNK",
    "PRI": "USA", "VIR": "USA", "GUM": "USA", "ASM": "USA",
    "FLK": "GBR", "GIB": "GBR", "SGS": "GBR", "BMU": "GBR", "CYM": "GBR", "VGB": "GBR",
}

# Country bounding boxes
try:
    from utils.geocoding import COUNTRY_BBOX
except ImportError:
    COUNTRY_BBOX = {}

EXTENDED_COUNTRY_BBOX = {
    **COUNTRY_BBOX,
    "ARG": {"lat_min": -55.0, "lat_max": -21.0, "lon_min": -74.0, "lon_max": -53.0},
    "BOL": {"lat_min": -23.0, "lat_max": -9.0, "lon_min": -70.0, "lon_max": -57.0},
    "ECU": {"lat_min": -5.0, "lat_max": 2.0, "lon_min": -92.0, "lon_max": -75.0},
    "VEN": {"lat_min": 0.0, "lat_max": 13.0, "lon_min": -74.0, "lon_max": -59.0},
    "FRA": {"lat_min": -21.0, "lat_max": 51.0, "lon_min": -62.0, "lon_max": 10.0},
}


# =============================================================================
# VALIDATION RESULT CLASSES
# =============================================================================

@dataclass
class ValidationError:
    """A validation error for a facility."""
    facility_id: str
    name: str
    country_iso3: str
    lat: Optional[float]
    lon: Optional[float]
    error_type: str
    severity: str = "medium"
    details: str = ""
    suggested_fix: Optional[Dict] = None
    file_path: str = ""
    actual_country: Optional[str] = None
    distance_km: Optional[float] = None


# =============================================================================
# POLYGON VALIDATOR (Natural Earth)
# =============================================================================

def download_natural_earth(resolution: str = "10m") -> Path:
    """Download Natural Earth shapefile if not cached."""
    CACHE_DIR.mkdir(exist_ok=True)
    shapefile_path = CACHE_DIR / f"ne_{resolution}_admin_0_countries.shp"

    if shapefile_path.exists():
        return shapefile_path

    url = NATURAL_EARTH_URLS[resolution]
    size = "~5MB" if resolution == "10m" else "~800KB"
    print(f"Downloading Natural Earth {resolution} data (one-time, {size})...")

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "ne_countries.zip"
        urllib.request.urlretrieve(url, zip_path)
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(CACHE_DIR)

    return shapefile_path


class PolygonValidator:
    """Validate coordinates against Natural Earth country polygons."""

    def __init__(self, resolution: str = "10m", coastal_buffer_km: float = 50.0):
        if not GEOPANDAS_AVAILABLE:
            raise RuntimeError("geopandas required. Install: pip install geopandas shapely")

        self.errors: List[ValidationError] = []
        self.stats = defaultdict(int)
        self.coastal_buffer_km = coastal_buffer_km

        print(f"Loading country polygons ({resolution})...")
        shapefile_path = download_natural_earth(resolution)
        self.world = gpd.read_file(shapefile_path)

        # Build country polygons lookup
        self.country_polygons = {}
        for idx, row in self.world.iterrows():
            iso3 = None
            for col in ['ISO_A3', 'ADM0_A3', 'ISO_A3_EH']:
                if col in self.world.columns:
                    val = row.get(col, '')
                    if val and val not in ['-99', '-1', None, '']:
                        iso3 = val
                        break
            if iso3:
                if iso3 not in self.country_polygons:
                    self.country_polygons[iso3] = row.geometry
                else:
                    self.country_polygons[iso3] = self.country_polygons[iso3].union(row.geometry)

        self.world_sindex = self.world.sindex
        print(f"Loaded {len(self.country_polygons)} country polygons")

    def get_country_at_point(self, lat: float, lon: float) -> Optional[str]:
        """Get ISO3 country code at coordinates."""
        point = Point(lon, lat)
        possible_matches_idx = list(self.world_sindex.intersection(point.bounds))

        for idx in possible_matches_idx:
            if self.world.iloc[idx].geometry.contains(point):
                row = self.world.iloc[idx]
                for col in ['ISO_A3', 'ADM0_A3', 'ISO_A3_EH']:
                    if col in self.world.columns:
                        iso3 = row.get(col, '')
                        if iso3 and iso3 not in ['-99', '-1', None, '']:
                            return iso3
        return None

    def get_nearest_country(self, lat: float, lon: float) -> Tuple[Optional[str], float]:
        """Find nearest country and distance in km."""
        point = Point(lon, lat)
        min_distance = float('inf')
        nearest_country = None

        for iso3, geometry in self.country_polygons.items():
            try:
                dist = point.distance(geometry)
                if dist < min_distance:
                    min_distance = dist
                    nearest_country = iso3
            except Exception:
                continue

        distance_km = min_distance * 111
        return nearest_country, distance_km

    def countries_match(self, declared: str, actual: str) -> bool:
        """Check if countries match, accounting for territories."""
        if declared == actual:
            return True
        if declared in ISO3_FIXES and actual in ISO3_FIXES[declared]:
            return True
        if actual in TERRITORY_TO_SOVEREIGN and TERRITORY_TO_SOVEREIGN[actual] == declared:
            return True
        return False

    def validate_facility(self, facility: Dict, file_path: Path) -> Optional[ValidationError]:
        """Validate a facility's coordinates."""
        facility_id = facility.get('facility_id', 'unknown')
        name = facility.get('name', '')
        declared_country = facility.get('country_iso3', '')
        location = facility.get('location', {})
        lat, lon = location.get('lat'), location.get('lon')

        if lat is None or lon is None:
            return None
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return ValidationError(
                facility_id=facility_id, name=name, country_iso3=declared_country,
                lat=lat, lon=lon, error_type='invalid_coordinates', severity='critical',
                file_path=str(file_path.relative_to(ROOT)),
                details=f'Invalid coordinates: ({lat}, {lon})'
            )

        actual_country = self.get_country_at_point(lat, lon)

        if actual_country is None:
            nearest, distance_km = self.get_nearest_country(lat, lon)

            # Check coastal buffer
            if self.coastal_buffer_km > 0 and declared_country in self.country_polygons:
                point = Point(lon, lat)
                try:
                    dist_to_declared = point.distance(self.country_polygons[declared_country]) * 111
                    if dist_to_declared <= self.coastal_buffer_km:
                        self.stats['coastal_buffer_ok'] += 1
                        return None
                except Exception:
                    pass

            return ValidationError(
                facility_id=facility_id, name=name, country_iso3=declared_country,
                lat=lat, lon=lon, error_type='in_ocean', severity='high',
                file_path=str(file_path.relative_to(ROOT)), distance_km=distance_km,
                details=f'In ocean. Nearest: {nearest} ({distance_km:.1f}km)'
            )

        if not self.countries_match(declared_country, actual_country):
            return ValidationError(
                facility_id=facility_id, name=name, country_iso3=declared_country,
                lat=lat, lon=lon, error_type='wrong_country', severity='high',
                actual_country=actual_country,
                file_path=str(file_path.relative_to(ROOT)),
                details=f'Declared: {declared_country}, Actual: {actual_country}'
            )

        return None

    def validate_all(self, countries: Optional[List[str]] = None) -> List[ValidationError]:
        """Validate all facilities."""
        if countries:
            country_dirs = [FACILITIES_DIR / c for c in countries if (FACILITIES_DIR / c).exists()]
        else:
            country_dirs = sorted([d for d in FACILITIES_DIR.iterdir() if d.is_dir()])

        print(f"Validating {len(country_dirs)} countries...")
        total = 0

        for country_dir in country_dirs:
            for fac_file in sorted(country_dir.glob("*.json")):
                try:
                    with open(fac_file, 'r') as f:
                        facility = json.load(f)
                    total += 1
                    error = self.validate_facility(facility, fac_file)
                    if error:
                        self.errors.append(error)
                        self.stats[error.error_type] += 1
                except Exception as e:
                    print(f"Error reading {fac_file}: {e}")

        self.stats['total_facilities'] = total
        print(f"Validated {total} facilities")
        return self.errors


# =============================================================================
# GEOCODING VALIDATOR (Bounding Box)
# =============================================================================

class GeocodingValidator:
    """Validate geocoding using bounding boxes."""

    def __init__(self, use_snowflake: bool = False):
        self.errors: List[ValidationError] = []
        self.stats = defaultdict(int)
        self.country_bbox = EXTENDED_COUNTRY_BBOX
        self.use_snowflake = use_snowflake
        self.snowflake_conn = None
        self.country_id_map = {}

        if use_snowflake:
            if not SNOWFLAKE_AVAILABLE:
                raise RuntimeError("Snowflake packages required")
            self._connect_snowflake()

    def _connect_snowflake(self):
        """Connect to Snowflake."""
        key_path = Path.home() / ".snowsql" / "rsa_key.p8"
        with open(key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(), password=None, backend=default_backend()
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        self.snowflake_conn = snowflake.connector.connect(
            account="HDHACZZ-AE73585", user="WILLIAM.BRODHEAD",
            private_key=pkb, warehouse="GSMC_WH_XS",
            database="MIKHAIL", schema="ENTITY", role="ACCOUNTADMIN"
        )

        cur = self.snowflake_conn.cursor()
        cur.execute("SELECT ID, ISO3, NAME FROM ENTITY.COUNTRY")
        for row in cur.fetchall():
            self.country_id_map[row[0]] = {'iso3': row[1], 'name': row[2]}
        cur.close()
        print(f"Connected to Snowflake. Loaded {len(self.country_id_map)} countries.")

    def is_in_bounds(self, lat: float, lon: float, country: str) -> bool:
        """Check if coordinates are in country bounding box."""
        if country not in self.country_bbox:
            return True
        bbox = self.country_bbox[country]
        return bbox["lat_min"] <= lat <= bbox["lat_max"] and bbox["lon_min"] <= lon <= bbox["lon_max"]

    def check_latlon_swap(self, lat: float, lon: float, country: str) -> bool:
        """Check if swapping lat/lon would fix the issue."""
        if country not in self.country_bbox:
            return False
        if not (-90 <= lon <= 90 and -180 <= lat <= 180):
            return False
        return self.is_in_bounds(lon, lat, country)

    def validate_facility(self, facility: Dict, file_path: Path) -> List[ValidationError]:
        """Validate a facility."""
        errors = []
        facility_id = facility.get('facility_id', 'unknown')
        name = facility.get('name', '')
        country = facility.get('country_iso3', '')
        location = facility.get('location', {})
        lat, lon = location.get('lat'), location.get('lon')

        if lat is None or lon is None:
            errors.append(ValidationError(
                facility_id=facility_id, name=name, country_iso3=country,
                lat=lat, lon=lon, error_type='missing_coordinates', severity='medium',
                file_path=str(file_path.relative_to(ROOT)),
                details='No coordinates'
            ))
            return errors

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            errors.append(ValidationError(
                facility_id=facility_id, name=name, country_iso3=country,
                lat=lat, lon=lon, error_type='invalid_range', severity='critical',
                file_path=str(file_path.relative_to(ROOT)),
                details=f'Out of range: ({lat}, {lon})'
            ))
            return errors

        if country in self.country_bbox and not self.is_in_bounds(lat, lon, country):
            if self.check_latlon_swap(lat, lon, country):
                errors.append(ValidationError(
                    facility_id=facility_id, name=name, country_iso3=country,
                    lat=lat, lon=lon, error_type='latlon_swap', severity='high',
                    file_path=str(file_path.relative_to(ROOT)),
                    suggested_fix={'lat': lon, 'lon': lat},
                    details=f'Coords appear swapped: ({lat}, {lon})'
                ))
            else:
                errors.append(ValidationError(
                    facility_id=facility_id, name=name, country_iso3=country,
                    lat=lat, lon=lon, error_type='out_of_bounds', severity='high',
                    file_path=str(file_path.relative_to(ROOT)),
                    details=f'Outside {country} bounds: ({lat}, {lon})'
                ))

        if lat == round(lat) and lon == round(lon):
            errors.append(ValidationError(
                facility_id=facility_id, name=name, country_iso3=country,
                lat=lat, lon=lon, error_type='suspicious_precision', severity='low',
                file_path=str(file_path.relative_to(ROOT)),
                details=f'Suspiciously round: ({lat}, {lon})'
            ))

        return errors

    def validate_all(self, countries: Optional[List[str]] = None) -> List[ValidationError]:
        """Validate all facilities."""
        if countries:
            country_dirs = [FACILITIES_DIR / c for c in countries if (FACILITIES_DIR / c).exists()]
        else:
            country_dirs = [d for d in FACILITIES_DIR.iterdir() if d.is_dir()]

        print(f"Validating {len(country_dirs)} countries...")

        for country_dir in sorted(country_dirs):
            for fac_file in country_dir.glob("*.json"):
                try:
                    with open(fac_file, 'r') as f:
                        facility = json.load(f)
                    errors = self.validate_facility(facility, fac_file)
                    self.errors.extend(errors)
                    for error in errors:
                        self.stats[error.error_type] += 1
                except Exception as e:
                    print(f"Error reading {fac_file}: {e}")

        return self.errors

    def apply_fixes(self, dry_run: bool = True) -> int:
        """Apply automatic fixes."""
        fixable = [e for e in self.errors if e.suggested_fix]
        if not fixable:
            print("No auto-fixable errors.")
            return 0

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Applying {len(fixable)} fixes...")
        fixed = 0

        for error in fixable:
            path = ROOT / error.file_path
            if not path.exists():
                continue

            try:
                with open(path, 'r') as f:
                    facility = json.load(f)

                old_lat = facility['location']['lat']
                old_lon = facility['location']['lon']
                new_lat = error.suggested_fix['lat']
                new_lon = error.suggested_fix['lon']

                print(f"  {error.facility_id}: ({old_lat}, {old_lon}) -> ({new_lat}, {new_lon})")

                if not dry_run:
                    facility['location']['lat'] = new_lat
                    facility['location']['lon'] = new_lon
                    with open(path, 'w') as f:
                        json.dump(facility, f, indent=2, ensure_ascii=False)
                        f.write('\n')
                fixed += 1
            except Exception as e:
                print(f"  Error: {e}")

        print(f"\n{'Would fix' if dry_run else 'Fixed'}: {fixed}/{len(fixable)}")
        return fixed


# =============================================================================
# SHARED OUTPUT FUNCTIONS
# =============================================================================

def print_summary(errors: List[ValidationError], stats: Dict):
    """Print validation summary."""
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"\nTotal errors: {len(errors)}")

    if errors:
        print("\nBy type:")
        types = defaultdict(int)
        for e in errors:
            types[e.error_type] += 1
        for t, c in sorted(types.items(), key=lambda x: -x[1]):
            print(f"  {t:30s} {c:5d}")

    if stats.get('coastal_buffer_ok'):
        print(f"\nCoastal buffer accepted: {stats['coastal_buffer_ok']}")


def export_errors(errors: List[ValidationError], output_path: Path):
    """Export errors to JSON."""
    output = {
        'total_errors': len(errors),
        'errors': [asdict(e) for e in errors]
    }
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\nExported to: {output_path}")


# =============================================================================
# CLI
# =============================================================================

def cmd_polygons(args):
    """Run polygon validation."""
    if not GEOPANDAS_AVAILABLE:
        print("Error: geopandas required. Install: pip install geopandas shapely")
        return 1

    validator = PolygonValidator(resolution=args.resolution, coastal_buffer_km=args.coastal_buffer)

    if args.country:
        validator.validate_all([args.country])
    else:
        validator.validate_all()

    print_summary(validator.errors, validator.stats)

    if args.verbose:
        for e in validator.errors[:args.limit]:
            print(f"\n{e.facility_id} ({e.country_iso3})")
            print(f"  {e.details}")

    if args.export:
        export_errors(validator.errors, args.export)

    return 0 if not validator.errors else 1


def cmd_geocoding(args):
    """Run geocoding validation."""
    validator = GeocodingValidator(use_snowflake=args.snowflake)

    if args.country:
        validator.validate_all([args.country])
    else:
        validator.validate_all()

    print_summary(validator.errors, validator.stats)

    if args.fix:
        validator.apply_fixes(dry_run=args.dry_run)

    if args.export:
        export_errors(validator.errors, args.export)

    return 0 if not validator.errors else 1


def main():
    parser = argparse.ArgumentParser(description="Unified facility validation")
    subparsers = parser.add_subparsers(dest='command', help='Validation type')

    # Polygon subcommand
    poly = subparsers.add_parser('polygons', help='Validate against country polygons')
    poly.add_argument('--country', '-c', help='Validate single country')
    poly.add_argument('--resolution', '-r', choices=['10m', '110m'], default='10m')
    poly.add_argument('--coastal-buffer', '-b', type=float, default=50.0)
    poly.add_argument('--export', '-e', type=Path, help='Export errors to JSON')
    poly.add_argument('--verbose', '-v', action='store_true')
    poly.add_argument('--limit', type=int, default=50)

    # Geocoding subcommand
    geo = subparsers.add_parser('geocoding', help='Validate using bounding boxes')
    geo.add_argument('--country', '-c', help='Validate single country')
    geo.add_argument('--snowflake', action='store_true', help='Query Snowflake directly')
    geo.add_argument('--fix', action='store_true', help='Apply auto-fixes')
    geo.add_argument('--dry-run', action='store_true', help='Preview fixes only')
    geo.add_argument('--export', '-e', type=Path, help='Export errors to JSON')
    geo.add_argument('--verbose', '-v', action='store_true')
    geo.add_argument('--limit', type=int, default=50)

    args = parser.parse_args()

    if args.command == 'polygons':
        return cmd_polygons(args)
    elif args.command == 'geocoding':
        return cmd_geocoding(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
