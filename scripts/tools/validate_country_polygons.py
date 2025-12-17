#!/usr/bin/env python3
"""
Validate facility coordinates against actual country polygons.

Uses Natural Earth shapefiles via geopandas to check if facility coordinates
fall within the correct country boundaries (not just bounding boxes).

Usage:
    python scripts/tools/validate_country_polygons.py                          # Check all (10m)
    python scripts/tools/validate_country_polygons.py --country USA            # Check one country
    python scripts/tools/validate_country_polygons.py --resolution 110m        # Use faster 110m
    python scripts/tools/validate_country_polygons.py --export errors.json     # Export errors
    python scripts/tools/validate_country_polygons.py --verbose                # Show details

Requirements:
    pip install geopandas shapely
"""

import json
import argparse
import sys
import urllib.request
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass, asdict
import warnings

# Suppress geopandas warnings
warnings.filterwarnings('ignore', category=FutureWarning)

try:
    import geopandas as gpd
    from shapely.geometry import Point
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False

# Paths
ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"
CACHE_DIR = ROOT / ".cache"

# Natural Earth data URLs
NATURAL_EARTH_URLS = {
    "10m": "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip",  # ~5MB, high detail
    "110m": "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip",  # ~800KB, low detail
}

# ISO3 code mapping fixes (Natural Earth uses some non-standard codes)
ISO3_FIXES = {
    "FRA": ["FRA", "GUF"],  # France includes French Guiana
    "NLD": ["NLD", "ABW", "CUW"],  # Netherlands includes Caribbean
    "DNK": ["DNK", "GRL"],  # Denmark includes Greenland
    "NOR": ["NOR", "SJM"],  # Norway includes Svalbard
    "USA": ["USA", "PRI", "VIR", "GUM", "ASM"],  # US territories
    "GBR": ["GBR", "FLK", "GIB", "SGS"],  # UK territories
    "AUS": ["AUS", "NFK", "CCK", "CXR"],  # Australia territories
}

# Reverse mapping for territories
TERRITORY_TO_SOVEREIGN = {
    "GUF": "FRA", "GLP": "FRA", "MTQ": "FRA", "REU": "FRA", "MYT": "FRA", "NCL": "FRA", "PYF": "FRA",
    "ABW": "NLD", "CUW": "NLD", "SXM": "NLD",
    "GRL": "DNK", "FRO": "DNK",
    "PRI": "USA", "VIR": "USA", "GUM": "USA", "ASM": "USA",
    "FLK": "GBR", "GIB": "GBR", "SGS": "GBR", "BMU": "GBR", "CYM": "GBR", "VGB": "GBR",
}


@dataclass
class ValidationError:
    """Represents a coordinate validation error."""
    facility_id: str
    name: str
    declared_country: str
    actual_country: Optional[str]
    lat: float
    lon: float
    error_type: str  # 'wrong_country', 'in_ocean', 'ambiguous'
    distance_km: Optional[float] = None
    file_path: str = ""
    details: str = ""


def download_natural_earth(resolution: str = "10m") -> Path:
    """Download Natural Earth shapefile if not cached.

    Args:
        resolution: Either "10m" (default, ~5MB) or "110m" (~800KB)
    """
    CACHE_DIR.mkdir(exist_ok=True)
    shapefile_path = CACHE_DIR / f"ne_{resolution}_admin_0_countries.shp"

    if shapefile_path.exists():
        return shapefile_path

    if resolution not in NATURAL_EARTH_URLS:
        raise ValueError(f"Invalid resolution: {resolution}. Must be '10m' or '110m'")

    url = NATURAL_EARTH_URLS[resolution]
    size = "~5MB" if resolution == "10m" else "~800KB"

    print(f"Downloading Natural Earth {resolution} data (one-time, {size})...")

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "ne_countries.zip"

        # Download
        urllib.request.urlretrieve(url, zip_path)

        # Extract
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(CACHE_DIR)

    print(f"Cached to: {CACHE_DIR}")
    return shapefile_path


class CountryPolygonValidator:
    """Validate coordinates against actual country polygons."""

    def __init__(self, resolution: str = "10m"):
        """Initialize validator with Natural Earth data.

        Args:
            resolution: Either "10m" (default, more accurate) or "110m" (faster)
        """
        if not GEOPANDAS_AVAILABLE:
            raise RuntimeError(
                "geopandas not available. Install with: pip install geopandas shapely"
            )

        self.errors: List[ValidationError] = []
        self.stats = defaultdict(int)
        self.resolution = resolution

        # Load Natural Earth data
        print(f"Loading country polygons ({resolution} resolution)...")
        shapefile_path = download_natural_earth(resolution)
        self.world = gpd.read_file(shapefile_path)

        # Build ISO3 lookup - Natural Earth uses 'ISO_A3' or 'ADM0_A3'
        self.country_polygons = {}
        iso3_col = 'ISO_A3' if 'ISO_A3' in self.world.columns else 'ADM0_A3'

        for idx, row in self.world.iterrows():
            iso3 = row.get(iso3_col, '')
            if iso3 and iso3 not in ['-99', '-1', None]:
                if iso3 not in self.country_polygons:
                    self.country_polygons[iso3] = row.geometry
                else:
                    # Merge multi-part countries
                    self.country_polygons[iso3] = self.country_polygons[iso3].union(row.geometry)

        # Store the column name for later use
        self.iso3_col = iso3_col

        print(f"Loaded {len(self.country_polygons)} country polygons")

        # Create spatial index for fast point-in-polygon queries
        self.world_sindex = self.world.sindex

    def get_country_at_point(self, lat: float, lon: float) -> Optional[str]:
        """
        Get the ISO3 country code for a given coordinate.

        Returns None if point is in ocean/no country.
        """
        point = Point(lon, lat)  # Note: Point takes (x, y) = (lon, lat)

        # Use spatial index for fast lookup
        possible_matches_idx = list(self.world_sindex.intersection(point.bounds))

        for idx in possible_matches_idx:
            if self.world.iloc[idx].geometry.contains(point):
                iso3 = self.world.iloc[idx].get(self.iso3_col, '')
                if iso3 and iso3 not in ['-99', '-1']:
                    return iso3

        return None

    def get_nearest_country(self, lat: float, lon: float) -> Tuple[Optional[str], float]:
        """
        Find the nearest country to a point and return distance in km.

        Returns (iso3, distance_km).
        """
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

        # Convert degrees to approximate km (rough estimate)
        distance_km = min_distance * 111  # ~111 km per degree at equator

        return nearest_country, distance_km

    def is_valid_coordinate(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within valid ranges."""
        return -90 <= lat <= 90 and -180 <= lon <= 180

    def countries_match(self, declared: str, actual: str) -> bool:
        """
        Check if declared and actual countries match, accounting for territories.
        """
        if declared == actual:
            return True

        # Check if actual is a territory of declared
        if declared in ISO3_FIXES:
            if actual in ISO3_FIXES[declared]:
                return True

        # Check reverse mapping
        if actual in TERRITORY_TO_SOVEREIGN:
            if TERRITORY_TO_SOVEREIGN[actual] == declared:
                return True

        return False

    def validate_facility(self, facility: Dict, file_path: Path) -> Optional[ValidationError]:
        """Validate a single facility's coordinates."""
        facility_id = facility.get('facility_id', 'unknown')
        name = facility.get('name', '')
        declared_country = facility.get('country_iso3', '')
        location = facility.get('location', {})
        lat = location.get('lat')
        lon = location.get('lon')

        # Skip if no coordinates
        if lat is None or lon is None:
            return None

        # Skip invalid coordinates
        if not self.is_valid_coordinate(lat, lon):
            return ValidationError(
                facility_id=facility_id,
                name=name,
                declared_country=declared_country,
                actual_country=None,
                lat=lat,
                lon=lon,
                error_type='invalid_coordinates',
                file_path=str(file_path.relative_to(ROOT)),
                details=f'Invalid coordinates: ({lat}, {lon})'
            )

        # Get actual country at coordinates
        actual_country = self.get_country_at_point(lat, lon)

        if actual_country is None:
            # Point is in ocean or unmapped area
            nearest, distance_km = self.get_nearest_country(lat, lon)
            return ValidationError(
                facility_id=facility_id,
                name=name,
                declared_country=declared_country,
                actual_country=None,
                lat=lat,
                lon=lon,
                error_type='in_ocean',
                distance_km=distance_km,
                file_path=str(file_path.relative_to(ROOT)),
                details=f'Coordinates in ocean/unmapped. Nearest country: {nearest} ({distance_km:.1f} km away)'
            )

        # Check if countries match
        if not self.countries_match(declared_country, actual_country):
            return ValidationError(
                facility_id=facility_id,
                name=name,
                declared_country=declared_country,
                actual_country=actual_country,
                lat=lat,
                lon=lon,
                error_type='wrong_country',
                file_path=str(file_path.relative_to(ROOT)),
                details=f'Declared: {declared_country}, Actual: {actual_country}'
            )

        return None

    def validate_country_dir(self, country_code: str) -> List[ValidationError]:
        """Validate all facilities in a country directory."""
        country_dir = FACILITIES_DIR / country_code
        if not country_dir.exists():
            print(f"Country directory not found: {country_code}")
            return []

        errors = []
        facility_count = 0

        for facility_file in sorted(country_dir.glob("*.json")):
            try:
                with open(facility_file, 'r') as f:
                    facility = json.load(f)

                facility_count += 1
                error = self.validate_facility(facility, facility_file)

                if error:
                    errors.append(error)
                    self.stats[error.error_type] += 1

            except Exception as e:
                print(f"Error reading {facility_file}: {e}")

        self.stats[f'{country_code}_total'] = facility_count
        return errors

    def validate_all(self, countries: Optional[List[str]] = None,
                     progress: bool = True) -> List[ValidationError]:
        """Validate all facilities or specific countries."""
        if countries:
            country_dirs = [FACILITIES_DIR / c for c in countries if (FACILITIES_DIR / c).exists()]
        else:
            country_dirs = sorted([d for d in FACILITIES_DIR.iterdir() if d.is_dir()])

        print(f"Validating {len(country_dirs)} countries...")

        all_errors = []
        total_facilities = 0

        for i, country_dir in enumerate(country_dirs):
            if progress and i % 20 == 0:
                print(f"  Progress: {i}/{len(country_dirs)} countries...")

            errors = self.validate_country_dir(country_dir.name)
            all_errors.extend(errors)
            total_facilities += self.stats.get(f'{country_dir.name}_total', 0)

        self.errors = all_errors
        self.stats['total_facilities'] = total_facilities

        print(f"Validated {total_facilities} facilities")
        return all_errors

    def print_summary(self):
        """Print validation summary."""
        print("\n" + "=" * 80)
        print("COUNTRY POLYGON VALIDATION SUMMARY")
        print("=" * 80)

        total = self.stats.get('total_facilities', 0)
        wrong_country = self.stats.get('wrong_country', 0)
        in_ocean = self.stats.get('in_ocean', 0)
        invalid = self.stats.get('invalid_coordinates', 0)

        print(f"\nTotal facilities checked: {total}")
        print(f"Total errors: {len(self.errors)}")
        print(f"  - Wrong country: {wrong_country}")
        print(f"  - In ocean: {in_ocean}")
        print(f"  - Invalid coordinates: {invalid}")

        if total > 0:
            error_rate = len(self.errors) / total * 100
            print(f"\nError rate: {error_rate:.2f}%")

        # Group errors by declared country
        if self.errors:
            print("\nErrors by declared country:")
            by_country = defaultdict(list)
            for err in self.errors:
                by_country[err.declared_country].append(err)

            for country in sorted(by_country.keys(), key=lambda c: -len(by_country[c])):
                count = len(by_country[country])
                print(f"  {country}: {count} errors")

        print("=" * 80)

    def print_errors(self, limit: int = 50, error_type: Optional[str] = None):
        """Print detailed error list."""
        errors = self.errors
        if error_type:
            errors = [e for e in errors if e.error_type == error_type]

        if not errors:
            print("\nNo errors found.")
            return

        print(f"\nShowing {min(len(errors), limit)} of {len(errors)} errors:\n")

        for err in errors[:limit]:
            print(f"{err.facility_id}")
            print(f"  Name: {err.name}")
            print(f"  Coords: ({err.lat}, {err.lon})")
            print(f"  {err.details}")
            print(f"  File: {err.file_path}")
            print()

        if len(errors) > limit:
            print(f"... and {len(errors) - limit} more errors")

    def export_errors(self, output_path: Path):
        """Export errors to JSON."""
        output = {
            'total_facilities': self.stats.get('total_facilities', 0),
            'total_errors': len(self.errors),
            'error_counts': {
                'wrong_country': self.stats.get('wrong_country', 0),
                'in_ocean': self.stats.get('in_ocean', 0),
                'invalid_coordinates': self.stats.get('invalid_coordinates', 0),
            },
            'errors': [asdict(e) for e in self.errors]
        }

        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"\nExported to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Validate facility coordinates against country polygons"
    )
    parser.add_argument(
        '--country', '-c',
        help='Validate specific country ISO3 code (e.g., USA, BRA)'
    )
    parser.add_argument(
        '--export', '-e',
        type=Path,
        help='Export errors to JSON file'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed error list'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=50,
        help='Limit number of errors shown (default: 50)'
    )
    parser.add_argument(
        '--error-type',
        choices=['wrong_country', 'in_ocean', 'invalid_coordinates'],
        help='Filter by error type'
    )
    parser.add_argument(
        '--resolution', '-r',
        choices=['10m', '110m'],
        default='10m',
        help='Natural Earth resolution: 10m (default, more accurate) or 110m (faster)'
    )

    args = parser.parse_args()

    if not GEOPANDAS_AVAILABLE:
        print("Error: geopandas not installed. Run: pip install geopandas shapely")
        return 1

    # Create validator
    validator = CountryPolygonValidator(resolution=args.resolution)

    # Run validation
    if args.country:
        errors = validator.validate_country_dir(args.country)
        validator.errors = errors
        # Set total_facilities from country-specific stat
        validator.stats['total_facilities'] = validator.stats.get(f'{args.country}_total', 0)
    else:
        validator.validate_all()

    # Print results
    validator.print_summary()

    if args.verbose:
        validator.print_errors(limit=args.limit, error_type=args.error_type)

    # Export if requested
    if args.export:
        validator.export_errors(args.export)

    return 0 if not validator.errors else 1


if __name__ == "__main__":
    sys.exit(main())
