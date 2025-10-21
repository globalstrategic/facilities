#!/usr/bin/env python3
"""
Test script for advanced geocoding system.

Tests on Kazakhstan uranium JVs:
- JV Inkai (Cameco/Kazatomprom)
- KATCO (Orano/Kazatomprom)
- Zarechnoye JV (uranium)

Expected results:
- Inkai: ~45.333°N, 67.500°E (Sozak District)
- Zarechnoye: 42.52806°N, 67.58472°E (Mindat coords)
- KATCO (Tortkuduk): Should find via OSM/Wikidata

Usage:
    python scripts/test_geocoding_v2.py
    python scripts/test_geocoding_v2.py --verbose
    python scripts/test_geocoding_v2.py --source overpass
    python scripts/test_geocoding_v2.py --source wikidata
"""

import argparse
import logging
import sys
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.geocoding_v2 import AdvancedGeocoder, GeocodingResult

# Test cases (Kazakhstan uranium JVs)
TEST_CASES = [
    {
        'name': 'Inkai',
        'aliases': ['JV Inkai', 'South Inkai', 'Blocks 1-3'],
        'country': 'KAZ',
        'commodities': ['uranium'],
        'expected_lat': 45.333,
        'expected_lon': 67.500,
        'tolerance': 0.1  # degrees (~11km)
    },
    {
        'name': 'Zarechnoye',
        'aliases': ['Zarechnoye Uranium Mine'],
        'country': 'KAZ',
        'commodities': ['uranium'],
        'expected_lat': 42.52806,
        'expected_lon': 67.58472,
        'tolerance': 0.1
    },
    {
        'name': 'KATCO',
        'aliases': ['Tortkuduk', 'Muyunkum'],
        'country': 'KAZ',
        'commodities': ['uranium'],
        'expected_lat': None,  # Unknown exact coords
        'expected_lon': None,
        'tolerance': None
    }
]


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(levelname)s: %(message)s'
    )


def test_facility(
    geocoder: AdvancedGeocoder,
    test_case: dict,
    verbose: bool = False
) -> bool:
    """
    Test geocoding for a single facility.

    Args:
        geocoder: AdvancedGeocoder instance
        test_case: Test case dict
        verbose: Print verbose output

    Returns:
        True if test passed, False otherwise
    """
    print(f"\n{'='*60}")
    print(f"Testing: {test_case['name']}")
    print(f"{'='*60}")

    # Geocode
    result = geocoder.geocode_facility(
        facility_name=test_case['name'],
        country_iso3=test_case['country'],
        commodities=test_case['commodities'],
        aliases=test_case['aliases'],
        min_confidence=0.4  # Lower threshold for better recall
    )

    # Print results
    print(f"\nResults:")
    print(f"  Coordinates: {result.lat}, {result.lon}")
    print(f"  Precision: {result.precision}")
    print(f"  Source: {result.source}")
    print(f"  Confidence: {result.confidence:.3f}")
    print(f"  Matched name: {result.matched_name}")
    print(f"  Match score: {result.match_score:.3f}" if result.match_score else "")
    print(f"  Source ID: {result.source_id}")

    if verbose and result.evidence:
        print(f"\nEvidence:")
        for key, value in result.evidence.items():
            print(f"  {key}: {value}")

    # Validate
    success = True

    if result.lat is None or result.lon is None:
        print(f"\n❌ FAILED: No coordinates found")
        success = False
    elif test_case['expected_lat'] and test_case['expected_lon']:
        # Check distance from expected
        lat_diff = abs(result.lat - test_case['expected_lat'])
        lon_diff = abs(result.lon - test_case['expected_lon'])

        if lat_diff <= test_case['tolerance'] and lon_diff <= test_case['tolerance']:
            print(f"\n✅ PASSED: Coordinates within tolerance ({test_case['tolerance']}°)")
        else:
            print(f"\n⚠️  WARNING: Coordinates outside tolerance")
            print(f"  Expected: {test_case['expected_lat']}, {test_case['expected_lon']}")
            print(f"  Difference: {lat_diff:.4f}°, {lon_diff:.4f}°")
            success = False
    else:
        # No expected coords - just check if we got something
        print(f"\n✅ PASSED: Found coordinates (no reference to validate)")

    return success


def main():
    """Run geocoding tests."""
    parser = argparse.ArgumentParser(description='Test advanced geocoding system')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--source', choices=['overpass', 'wikidata', 'nominatim', 'all'],
                        default='all', help='Test specific source only')
    parser.add_argument('--no-cache', action='store_true', help='Disable result caching')
    args = parser.parse_args()

    setup_logging(args.verbose)

    # Initialize geocoder
    print("Initializing geocoder...")
    geocoder = AdvancedGeocoder(
        use_overpass=(args.source in ['overpass', 'all']),
        use_wikidata=(args.source in ['wikidata', 'all']),
        use_nominatim=(args.source in ['nominatim', 'all']),
        cache_results=(not args.no_cache)
    )

    # Run tests
    print(f"\n{'='*60}")
    print(f"Testing Advanced Geocoding System")
    print(f"Sources: {args.source}")
    print(f"{'='*60}")

    results = []
    for test_case in TEST_CASES:
        success = test_facility(geocoder, test_case, args.verbose)
        results.append((test_case['name'], success))

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {status}: {name}")

    print(f"\nTotal: {passed}/{total} passed")

    # Exit code
    sys.exit(0 if passed == total else 1)


if __name__ == '__main__':
    main()
