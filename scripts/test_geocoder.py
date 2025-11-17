#!/usr/bin/env python3
"""Test the AdvancedGeocoder to see what's working"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.geocoding import AdvancedGeocoder, GeocodingResult

# Test with a well-known mine
geocoder = AdvancedGeocoder(
    use_overpass=True,
    use_wikidata=False,  # Disable to test faster
    use_mindat=False,    # Requires API key
    use_web_search=False,  # Requires API keys
    cache_results=True
)

print("Testing geocoder with Escondida Mine (Chile)...")

result = geocoder.geocode_facility(
    facility_name="Escondida",
    country_iso3="CHL",
    commodities=["copper"],
    aliases=[]
)

if result and result.lat:
    print(f"✓ Success!")
    print(f"  Coordinates: {result.lat}, {result.lon}")
    print(f"  Precision: {result.precision}")
    print(f"  Source: {result.source}")
    print(f"  Confidence: {result.confidence}")
else:
    print("✗ Failed to geocode")
    print(f"  Result: {result}")

print("\n" + "="*50)
print("Testing with a UAE facility...")

result2 = geocoder.geocode_facility(
    facility_name="EMSTEEL Steel Division",
    country_iso3="ARE",
    commodities=["steel"],
    aliases=["Emirates Steel"]
)

if result2 and result2.lat:
    print(f"✓ Success!")
    print(f"  Coordinates: {result2.lat}, {result2.lon}")
    print(f"  Precision: {result2.precision}")
    print(f"  Source: {result2.source}")
else:
    print("✗ Failed to geocode")
    if result2:
        print(f"  Result: {result2}")