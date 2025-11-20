#!/usr/bin/env python3
"""
LLM-based geocoding for mining facilities.

Uses web search + LLM reasoning to find coordinates for mining projects
that don't exist in standard geocoding databases.

This is much more effective than Nominatim because:
1. It can search mining databases, technical reports, and company websites
2. It understands context like "22 km NE from Theunissen"
3. It can parse location information from various formats
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, Dict, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False
    print("WARNING: openai package not installed. pip install openai")

try:
    from geopy.geocoders import Nominatim
    from geopy.distance import geodesic
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False
    print("WARNING: geopy not installed. pip install geopy")


def load_facilities_needing_geocoding(country_iso3: str) -> list:
    """Load facilities that need coordinates."""
    facilities_dir = Path(f"facilities/{country_iso3}")
    if not facilities_dir.exists():
        return []

    missing = []
    for f in facilities_dir.glob("*.json"):
        with open(f) as fp:
            data = json.load(fp)
        loc = data.get("location", {})
        if loc.get("lat") is None or loc.get("lon") is None:
            missing.append(data)

    return missing


def search_facility_location(facility_name: str, country: str, client: OpenAI) -> str:
    """Use LLM to search for facility location information."""

    # Build a search-oriented prompt
    prompt = f"""I need to find the geographic coordinates (latitude/longitude) for this mining facility:

Facility Name: {facility_name}
Country: {country}

Please search for information about this facility's location. Look for:
1. Technical mining reports (NI 43-101, JORC reports)
2. Mining company websites
3. Geological survey databases
4. Mining news articles
5. Mining property databases (like miningdataonline.com)

I need:
- Specific latitude/longitude coordinates if available
- OR distance and direction from a known town/city (e.g., "22 km NE from Theunissen")
- OR nearby town/city names with province/state

Return your findings in this JSON format:
{{
    "found": true/false,
    "coordinates": {{"lat": -26.5, "lon": 27.3}} or null,
    "reference_point": "Town name if coords calculated from distance",
    "distance_km": 22,
    "direction": "NE",
    "province": "Province name",
    "source": "Where you found this info",
    "confidence": 0.0-1.0,
    "notes": "Any relevant context"
}}

If you can't find specific coordinates but have relative location info, I can calculate from there."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",  # Use a model that can do web searches
            messages=[
                {"role": "system", "content": "You are a mining geologist expert helping locate mining facilities. Be precise and cite your sources."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"  Error searching: {e}")
        return json.dumps({"found": False, "error": str(e)})


def calculate_coords_from_reference(
    reference_town: str,
    country: str,
    distance_km: float,
    direction: str
) -> Optional[Tuple[float, float]]:
    """Calculate coordinates from a reference point + distance/direction."""
    if not HAS_GEOPY:
        return None

    # First, geocode the reference town
    geolocator = Nominatim(user_agent="gsmc-facilities-geocoder")
    time.sleep(1)  # Rate limiting

    location = geolocator.geocode(f"{reference_town}, {country}")
    if not location:
        return None

    ref_lat, ref_lon = location.latitude, location.longitude

    # Calculate bearing from direction
    direction_bearings = {
        "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
        "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
        "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
        "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5
    }

    bearing = direction_bearings.get(direction.upper(), None)
    if bearing is None:
        return None

    # Calculate new point
    from geopy.distance import distance as geopy_distance
    origin = (ref_lat, ref_lon)
    destination = geopy_distance(kilometers=distance_km).destination(origin, bearing)

    return (destination.latitude, destination.longitude)


def geocode_facility_with_llm(
    facility: Dict,
    country_name: str,
    client: OpenAI
) -> Optional[Dict]:
    """
    Use LLM to find coordinates for a facility.

    Returns updated location dict or None if not found.
    """
    facility_name = facility.get("name", "")

    print(f"  Searching web for: {facility_name}")
    result_json = search_facility_location(facility_name, country_name, client)

    try:
        result = json.loads(result_json)
    except json.JSONDecodeError:
        print(f"  Failed to parse LLM response")
        return None

    if not result.get("found", False):
        print(f"  No location info found")
        return None

    # Case 1: Direct coordinates
    if result.get("coordinates"):
        coords = result["coordinates"]
        if coords.get("lat") and coords.get("lon"):
            print(f"  Found direct coordinates: {coords['lat']}, {coords['lon']}")
            return {
                "lat": coords["lat"],
                "lon": coords["lon"],
                "source": result.get("source", "LLM web search"),
                "confidence": result.get("confidence", 0.7),
                "province": result.get("province"),
                "notes": result.get("notes", "")
            }

    # Case 2: Calculate from reference point
    if result.get("reference_point") and result.get("distance_km") and result.get("direction"):
        print(f"  Found relative location: {result['distance_km']}km {result['direction']} from {result['reference_point']}")

        coords = calculate_coords_from_reference(
            result["reference_point"],
            country_name,
            result["distance_km"],
            result["direction"]
        )

        if coords:
            print(f"  Calculated coordinates: {coords[0]:.6f}, {coords[1]:.6f}")
            return {
                "lat": coords[0],
                "lon": coords[1],
                "source": f"Calculated from {result['reference_point']} + {result['distance_km']}km {result['direction']}",
                "confidence": result.get("confidence", 0.6) * 0.9,  # Reduce confidence for calculated
                "province": result.get("province"),
                "town": result.get("reference_point"),
                "notes": result.get("notes", "")
            }
        else:
            print(f"  Could not geocode reference point: {result['reference_point']}")

    # Case 3: Only have province/town info
    if result.get("province") or result.get("town"):
        print(f"  Found general area: {result.get('town', '')} {result.get('province', '')}")
        # Could attempt to geocode just the town/province as fallback
        return {
            "province": result.get("province"),
            "town": result.get("town"),
            "source": result.get("source"),
            "notes": result.get("notes", ""),
            "needs_manual_coords": True
        }

    return None


def update_facility_file(facility: Dict, location_update: Dict) -> bool:
    """Update a facility JSON file with new location data."""
    facility_path = Path(f"facilities/{facility['country_iso3']}/{facility['facility_id']}.json")

    if not facility_path.exists():
        return False

    with open(facility_path) as f:
        data = json.load(f)

    # Update location fields
    if "lat" in location_update:
        data["location"]["lat"] = location_update["lat"]
    if "lon" in location_update:
        data["location"]["lon"] = location_update["lon"]

    # Update metadata
    if location_update.get("province"):
        data["province"] = location_update["province"]
    if location_update.get("town"):
        data["town"] = location_update["town"]

    # Update verification
    data["verification"]["status"] = "llm_geocoded"
    data["verification"]["confidence"] = location_update.get("confidence", 0.7)
    data["verification"]["last_checked"] = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    data["verification"]["checked_by"] = "llm_web_search"

    notes = data["verification"].get("notes", "")
    new_note = f"LLM geocoded from: {location_update.get('source', 'web search')}. {location_update.get('notes', '')}"
    data["verification"]["notes"] = f"{notes}. {new_note}".strip(". ")

    # Write back
    with open(facility_path, "w") as f:
        json.dump(data, f, indent=2)

    return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="LLM-based geocoding for mining facilities")
    parser.add_argument("--country", required=True, help="ISO3 country code (e.g., ZAF)")
    parser.add_argument("--limit", type=int, default=10, help="Max facilities to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't update files")
    args = parser.parse_args()

    if not HAS_OPENAI:
        print("ERROR: OpenAI package required. pip install openai")
        sys.exit(1)

    # Initialize OpenAI client
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable not set")
        sys.exit(1)

    client = OpenAI(api_key=api_key)

    # Country name mapping (simplified)
    country_names = {
        "ZAF": "South Africa",
        "USA": "United States",
        "AUS": "Australia",
        "IND": "India",
        "CHN": "China",
        "BRA": "Brazil",
        # Add more as needed
    }
    country_name = country_names.get(args.country, args.country)

    print(f"\n{'='*60}")
    print(f"LLM-BASED GEOCODING: {country_name} ({args.country})")
    print(f"{'='*60}")

    facilities = load_facilities_needing_geocoding(args.country)
    print(f"Found {len(facilities)} facilities needing coordinates")

    if not facilities:
        print("No facilities to process")
        return

    # Limit processing
    to_process = facilities[:args.limit]
    print(f"Processing {len(to_process)} facilities (limit: {args.limit})\n")

    success_count = 0
    partial_count = 0
    failed_count = 0

    for i, facility in enumerate(to_process, 1):
        print(f"[{i}/{len(to_process)}] {facility['name']}")

        # Add delay to avoid rate limits
        if i > 1:
            time.sleep(2)

        result = geocode_facility_with_llm(facility, country_name, client)

        if result:
            if result.get("needs_manual_coords"):
                print(f"  → Partial info found (no coordinates)")
                partial_count += 1
            else:
                if not args.dry_run:
                    if update_facility_file(facility, result):
                        print(f"  ✓ Updated facility file")
                        success_count += 1
                    else:
                        print(f"  ✗ Failed to update file")
                        failed_count += 1
                else:
                    print(f"  [DRY RUN] Would update with: {result['lat']:.6f}, {result['lon']:.6f}")
                    success_count += 1
        else:
            print(f"  ✗ Could not locate facility")
            failed_count += 1

        print()

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Successfully geocoded: {success_count}")
    print(f"Partial info (no coords): {partial_count}")
    print(f"Failed to locate: {failed_count}")
    print(f"Total processed: {len(to_process)}")


if __name__ == "__main__":
    main()
