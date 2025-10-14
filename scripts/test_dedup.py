#!/usr/bin/env python3
"""
Quick test to verify duplicate detection is working.
"""

import json
import pathlib
import sys

# Add the import script to path
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from import_from_report import check_duplicate, slugify

def test_duplicate_detection():
    """Test duplicate detection logic."""
    print("Testing duplicate detection...")
    print("="*60)

    # Create mock existing facilities
    existing = {
        "dza-gara-djebilet-fac": {
            "facility_id": "dza-gara-djebilet-fac",
            "name": "Gara Djebilet Mine",
            "aliases": ["Gara Djebilet Iron Ore Project"],
            "location": {
                "lat": 26.766,
                "lon": -7.333
            }
        },
        "dza-ouenza-mine-fac": {
            "facility_id": "dza-ouenza-mine-fac",
            "name": "Ouenza Mine",
            "aliases": ["MFE Ouenza"],
            "location": {
                "lat": 35.756,
                "lon": 8.043
            }
        },
        "dza-test-no-coords-fac": {
            "facility_id": "dza-test-no-coords-fac",
            "name": "Test Facility Without Coords",
            "aliases": [],
            "location": {
                "lat": None,
                "lon": None
            }
        }
    }

    # Test cases
    tests = [
        # Test 1: Exact ID match
        {
            "name": "Gara Djebilet Mine",
            "id": "dza-gara-djebilet-fac",
            "lat": 26.766,
            "lon": -7.333,
            "should_detect": True,
            "reason": "Exact ID match"
        },
        # Test 2: Name match with close location
        {
            "name": "Gara Djebilet Mine",
            "id": "dza-gara-djebilet-mine-fac",  # Different ID
            "lat": 26.767,  # Slightly different coords
            "lon": -7.334,
            "should_detect": True,
            "reason": "Name match + location within 1km"
        },
        # Test 3: Name match without coordinates
        {
            "name": "Test Facility Without Coords",
            "id": "dza-test-facility-fac",
            "lat": None,
            "lon": None,
            "should_detect": True,
            "reason": "Name match without coords (assumes duplicate)"
        },
        # Test 4: Alias match
        {
            "name": "MFE Ouenza",
            "id": "dza-mfe-ouenza-fac",
            "lat": 35.756,
            "lon": 8.043,
            "should_detect": True,
            "reason": "Name matches existing alias"
        },
        # Test 5: Same name but far location (NOT a duplicate)
        {
            "name": "Ouenza Mine",
            "id": "dza-ouenza-mine-2-fac",
            "lat": 35.000,  # Far away
            "lon": 8.000,
            "should_detect": False,  # Should NOT detect (different location)
            "reason": "Same name but location >1km away"
        },
        # Test 6: New facility (NOT a duplicate)
        {
            "name": "Brand New Mine",
            "id": "dza-brand-new-mine-fac",
            "lat": 30.000,
            "lon": 5.000,
            "should_detect": False,
            "reason": "Completely new facility"
        }
    ]

    passed = 0
    failed = 0

    for i, test in enumerate(tests, 1):
        result = check_duplicate(
            test["id"],
            test["name"],
            test["lat"],
            test["lon"],
            existing
        )

        detected = result is not None

        if detected == test["should_detect"]:
            status = "✅ PASS"
            passed += 1
        else:
            status = "❌ FAIL"
            failed += 1

        print(f"\nTest {i}: {status}")
        print(f"  Facility: {test['name']}")
        print(f"  Expected: {'Duplicate' if test['should_detect'] else 'New'}")
        print(f"  Got: {'Duplicate' if detected else 'New'}")
        if detected:
            print(f"  Matched: {result}")
        print(f"  Reason: {test['reason']}")

    print("\n" + "="*60)
    print(f"Results: {passed} passed, {failed} failed")
    print("="*60)

    return failed == 0

if __name__ == "__main__":
    success = test_duplicate_detection()
    sys.exit(0 if success else 1)
