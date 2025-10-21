#!/usr/bin/env python3
"""
Geocode facilities with missing coordinates.

Backfills coordinates for existing facilities using multiple strategies:
- Industrial zone database lookup
- Nominatim (OpenStreetMap) API
- Interactive prompting

Usage:
    # Geocode all facilities in a country
    python scripts/geocode_facilities.py --country ARE

    # Interactive mode (prompts for failures)
    python scripts/geocode_facilities.py --country ARE --interactive

    # Dry run (preview without saving)
    python scripts/geocode_facilities.py --country ARE --dry-run

    # Geocode specific facilities
    python scripts/geocode_facilities.py --facility-id are-union-cement-company-fac
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from utils.geocoding import geocode_facility, GeocodingResult
    from utils.country_utils import normalize_country_to_iso3, iso3_to_country_name
except ImportError as e:
    logger.error(f"Failed to import utilities: {e}")
    sys.exit(1)

# Paths
ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"


def load_facilities_for_country(country_iso3: str) -> List[Dict]:
    """Load all facility JSONs for a country."""
    facilities = []
    country_dir = FACILITIES_DIR / country_iso3

    if not country_dir.exists():
        logger.error(f"No facilities directory found for {country_iso3}")
        return facilities

    for facility_file in country_dir.glob("*.json"):
        try:
            with open(facility_file, 'r') as f:
                facility = json.load(f)
                facility['_path'] = facility_file
                facilities.append(facility)
        except Exception as e:
            logger.warning(f"Could not load {facility_file}: {e}")

    return facilities


def load_facility_by_id(facility_id: str) -> Dict:
    """Load a single facility by ID."""
    # Extract country from facility ID
    country_iso3 = facility_id.split('-')[0].upper()
    facility_path = FACILITIES_DIR / country_iso3 / f"{facility_id}.json"

    if not facility_path.exists():
        logger.error(f"Facility not found: {facility_path}")
        return None

    with open(facility_path, 'r') as f:
        facility = json.load(f)
        facility['_path'] = facility_path
        return facility


def needs_geocoding(facility: Dict) -> bool:
    """Check if facility needs geocoding."""
    location = facility.get('location', {})
    return location.get('lat') is None or location.get('lon') is None


def update_facility_coordinates(
    facility: Dict,
    result: GeocodingResult,
    dry_run: bool = False
) -> None:
    """Update facility JSON with geocoded coordinates."""
    if result.lat is None or result.lon is None:
        logger.warning(f"No coordinates found for {facility['facility_id']}")
        return

    # Update location
    facility['location'] = {
        'lat': result.lat,
        'lon': result.lon,
        'precision': result.precision
    }

    # Update verification
    if 'verification' not in facility:
        facility['verification'] = {}

    facility['verification']['last_checked'] = datetime.now().isoformat()
    facility['verification']['notes'] = (
        f"Geocoded via {result.source} (confidence: {result.confidence:.2f})"
    )

    # Adjust confidence based on geocoding
    if result.confidence >= 0.9:
        # User input or high-confidence match
        pass  # Keep existing confidence
    elif result.confidence >= 0.7:
        # Moderate confidence - slight boost
        current_conf = facility['verification'].get('confidence', 0.5)
        facility['verification']['confidence'] = min(current_conf + 0.05, 1.0)
    else:
        # Low confidence - no change
        pass

    # Save to file
    if not dry_run:
        with open(facility['_path'], 'w') as f:
            # Remove internal _path before saving
            facility_copy = {k: v for k, v in facility.items() if k != '_path'}
            json.dump(facility_copy, f, indent=2, ensure_ascii=False)
            f.write('\n')  # Add trailing newline
        logger.info(f"✓ Updated {facility['facility_id']}: {result.lat}, {result.lon}")
    else:
        logger.info(f"DRY RUN: Would update {facility['facility_id']}: {result.lat}, {result.lon}")


def geocode_country(
    country_iso3: str,
    interactive: bool = False,
    dry_run: bool = False,
    use_nominatim: bool = True
) -> None:
    """Geocode all facilities in a country."""
    country_name = iso3_to_country_name(country_iso3)
    logger.info(f"Geocoding facilities for {country_name} ({country_iso3})")

    # Load facilities
    facilities = load_facilities_for_country(country_iso3)
    if not facilities:
        logger.error("No facilities found")
        return

    # Filter to those needing geocoding
    to_geocode = [f for f in facilities if needs_geocoding(f)]

    logger.info(f"Found {len(facilities)} facilities, {len(to_geocode)} need geocoding")

    if not to_geocode:
        logger.info("All facilities already have coordinates!")
        return

    # Geocode each facility
    success_count = 0
    fail_count = 0

    for i, facility in enumerate(to_geocode):
        logger.info(f"\n[{i+1}/{len(to_geocode)}] {facility['name']}")

        result = geocode_facility(
            facility_name=facility['name'],
            country_iso3=country_iso3,
            country_name=country_name,
            interactive=interactive,
            use_nominatim=use_nominatim
        )

        if result.lat is not None and result.lon is not None:
            update_facility_coordinates(facility, result, dry_run=dry_run)
            success_count += 1
        else:
            logger.warning(f"  ✗ Failed to geocode: {facility['name']}")
            fail_count += 1

    # Summary
    print(f"\n{'='*60}")
    print("GEOCODING SUMMARY")
    print(f"{'='*60}")
    print(f"Total facilities: {len(facilities)}")
    print(f"Needed geocoding: {len(to_geocode)}")
    print(f"Successfully geocoded: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Success rate: {success_count/len(to_geocode)*100:.1f}%")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description="Geocode facilities with missing coordinates"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--country',
        help='Country ISO3 code (e.g., ARE, USA, CHN)'
    )
    group.add_argument(
        '--facility-id',
        help='Specific facility ID to geocode'
    )

    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Enable interactive prompting for failed geocoding'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without saving'
    )
    parser.add_argument(
        '--no-nominatim',
        action='store_true',
        help='Disable Nominatim API (offline mode)'
    )

    args = parser.parse_args()

    if args.country:
        # Normalize country code
        iso3 = normalize_country_to_iso3(args.country)
        if not iso3:
            logger.error(f"Could not resolve country: {args.country}")
            return 1

        geocode_country(
            country_iso3=iso3,
            interactive=args.interactive,
            dry_run=args.dry_run,
            use_nominatim=not args.no_nominatim
        )

    elif args.facility_id:
        facility = load_facility_by_id(args.facility_id)
        if not facility:
            return 1

        if not needs_geocoding(facility):
            logger.info(f"Facility already has coordinates: {facility['location']}")
            return 0

        country_iso3 = facility['country_iso3']
        country_name = iso3_to_country_name(country_iso3)

        result = geocode_facility(
            facility_name=facility['name'],
            country_iso3=country_iso3,
            country_name=country_name,
            interactive=args.interactive,
            use_nominatim=not args.no_nominatim
        )

        if result.lat is not None and result.lon is not None:
            update_facility_coordinates(facility, result, dry_run=args.dry_run)
            print(f"\n✓ Geocoded: {result.lat}, {result.lon} (via {result.source})")
        else:
            print(f"\n✗ Failed to geocode {facility['name']}")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
