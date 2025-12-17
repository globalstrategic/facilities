#!/usr/bin/env python3
"""
Geocode facilities with null coordinates (appearing at 0,0 on maps).

Uses web search (Tavily) + LLM extraction to find real coordinates,
or marks facilities as invalid/unlocatable if no coords can be found.

Usage:
    # Scan and report null-coordinate facilities
    python geocode_null_island.py --scan

    # Geocode a specific country
    python geocode_null_island.py --country KOR --limit 10

    # Geocode all null-island facilities (batch mode)
    python geocode_null_island.py --all --limit 50

    # Dry run (don't write changes)
    python geocode_null_island.py --country PAN --dry-run

Requirements:
    export TAVILY_API_KEY="your-key"
    export OPENAI_API_KEY="your-key"
"""

import json
import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"

# Country bounding boxes for validation
COUNTRY_BBOX = {
    "AUS": {"lat_min": -44, "lat_max": -10, "lon_min": 113, "lon_max": 154},
    "BRA": {"lat_min": -34, "lat_max": 6, "lon_min": -74, "lon_max": -34},
    "CAN": {"lat_min": 41, "lat_max": 84, "lon_min": -141, "lon_max": -52},
    "CHN": {"lat_min": 18, "lat_max": 54, "lon_min": 73, "lon_max": 135},
    "COL": {"lat_min": -5, "lat_max": 13, "lon_min": -82, "lon_max": -66},
    "CUB": {"lat_min": 19, "lat_max": 24, "lon_min": -85, "lon_max": -74},
    "FRA": {"lat_min": 41, "lat_max": 51, "lon_min": -5, "lon_max": 10},
    "GEO": {"lat_min": 41, "lat_max": 44, "lon_min": 40, "lon_max": 47},
    "IND": {"lat_min": 6, "lat_max": 36, "lon_min": 68, "lon_max": 98},
    "IDN": {"lat_min": -11, "lat_max": 6, "lon_min": 95, "lon_max": 141},
    "IRN": {"lat_min": 25, "lat_max": 40, "lon_min": 44, "lon_max": 64},
    "KAZ": {"lat_min": 40, "lat_max": 56, "lon_min": 46, "lon_max": 88},
    "KOR": {"lat_min": 33, "lat_max": 39, "lon_min": 124, "lon_max": 132},
    "MEX": {"lat_min": 14, "lat_max": 33, "lon_min": -118, "lon_max": -86},
    "MMR": {"lat_min": 9, "lat_max": 29, "lon_min": 92, "lon_max": 102},
    "MNG": {"lat_min": 41, "lat_max": 52, "lon_min": 87, "lon_max": 120},
    "MOZ": {"lat_min": -27, "lat_max": -10, "lon_min": 30, "lon_max": 41},
    "NGA": {"lat_min": 4, "lat_max": 14, "lon_min": 2, "lon_max": 15},
    "PAK": {"lat_min": 23, "lat_max": 37, "lon_min": 60, "lon_max": 77},
    "PAN": {"lat_min": 7, "lat_max": 10, "lon_min": -83, "lon_max": -77},
    "PER": {"lat_min": -19, "lat_max": 0, "lon_min": -82, "lon_max": -68},
    "PHL": {"lat_min": 4, "lat_max": 21, "lon_min": 116, "lon_max": 127},
    "PRK": {"lat_min": 37, "lat_max": 43, "lon_min": 124, "lon_max": 131},
    "RUS": {"lat_min": 41, "lat_max": 82, "lon_min": 19, "lon_max": 180},
    "USA": {"lat_min": 24, "lat_max": 72, "lon_min": -180, "lon_max": -66},
    "VEN": {"lat_min": 0, "lat_max": 13, "lon_min": -74, "lon_max": -59},
    "VNM": {"lat_min": 8, "lat_max": 24, "lon_min": 102, "lon_max": 110},
    "ZAF": {"lat_min": -35, "lat_max": -22, "lon_min": 16, "lon_max": 33},
}


def find_null_island_facilities() -> List[Dict]:
    """Find all facilities with null coordinates."""
    facilities = []

    for f in FACILITIES_DIR.glob("*/*.json"):
        try:
            data = json.load(open(f))
            lat = data.get("location", {}).get("lat")
            lon = data.get("location", {}).get("lon")

            if lat is None or lon is None:
                facilities.append({
                    "file": f,
                    "facility_id": data.get("facility_id"),
                    "name": data.get("name", ""),
                    "country_iso3": data.get("country_iso3"),
                    "commodities": [c.get("metal", "") for c in data.get("commodities", [])],
                    "data": data,
                })
        except Exception as e:
            logger.warning(f"Error reading {f}: {e}")

    return facilities


def tavily_search(query: str, api_key: str, retries: int = 3) -> List[Dict]:
    """Search using Tavily API."""
    import requests

    url = "https://api.tavily.com/search"

    for attempt in range(retries):
        try:
            response = requests.post(
                url,
                json={
                    "api_key": api_key,
                    "query": query,
                    "search_depth": "advanced",
                    "max_results": 8,
                    "include_answer": False,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("results", [])
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logger.warning(f"Tavily search failed: {e}")
                return []
    return []


def extract_coordinates_with_llm(
    facility_name: str,
    country: str,
    commodities: List[str],
    search_results: List[Dict],
    client
) -> Optional[Dict]:
    """Use LLM to extract coordinates from search results."""

    # Format search results for LLM
    results_text = ""
    for i, result in enumerate(search_results[:8], 1):
        results_text += f"\n--- Result {i} ---\n"
        results_text += f"Title: {result.get('title', 'N/A')}\n"
        results_text += f"URL: {result.get('url', 'N/A')}\n"
        results_text += f"Content: {result.get('content', 'N/A')[:1500]}\n"

    commodity_str = ", ".join(commodities[:3]) if commodities else "unknown"

    prompt = f"""Extract the geographic coordinates for this mining facility from the search results.

FACILITY: {facility_name}
COUNTRY: {country}
COMMODITIES: {commodity_str}

SEARCH RESULTS:
{results_text}

Return JSON with:
{{
    "found": true/false,
    "lat": latitude (decimal degrees, NEGATIVE for South) or null,
    "lon": longitude (decimal degrees, NEGATIVE for West) or null,
    "reference_town": "nearest town if exact coords not found" or null,
    "distance_km": distance from reference town (number) or null,
    "direction": "N/NE/E/SE/S/SW/W/NW" from reference town or null,
    "province": "province/state name" or null,
    "source_url": "URL where coordinates found",
    "confidence": 0.0-1.0,
    "notes": "brief explanation of how coordinates were determined",
    "is_real_facility": true/false (false if this seems to be a placeholder or category, not a real location)
}}

COORDINATE RULES:
- South latitudes are NEGATIVE (e.g., South Africa: -26.0)
- West longitudes are NEGATIVE (e.g., Brazil: -47.0, USA: -110.0)
- East longitudes are POSITIVE (e.g., China: 116.0, Australia: 145.0)
- Parse DMS format: 26°30'S = -26.5, 47°15'W = -47.25
- If only relative location given (e.g., "50km NE of Lagos"), provide reference_town + distance + direction
- Confidence: direct coords = 0.9, calculated from reference = 0.7, general area = 0.5

VALIDITY CHECK:
- Set is_real_facility=false if this appears to be a category (e.g., "Various Mines", "All Facilities")
- Set is_real_facility=false if no specific location can be identified
- Set found=false if you cannot determine any coordinates"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert at extracting geographic coordinates from mining reports. Be precise with coordinate signs."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        logger.error(f"LLM extraction error: {e}")
        return None


def calculate_from_reference(
    reference_town: str,
    country: str,
    distance_km: float,
    direction: str
) -> Optional[Tuple[float, float]]:
    """Calculate coordinates from a reference point."""
    try:
        from geopy.geocoders import Nominatim
        from geopy.distance import distance as geopy_distance
    except ImportError:
        logger.warning("geopy not installed - can't calculate from reference")
        return None

    geolocator = Nominatim(user_agent="gsmc-null-island-geocoder")
    time.sleep(1.1)  # Rate limit

    try:
        location = geolocator.geocode(f"{reference_town}, {country}", timeout=10)
        if not location:
            return None

        ref_lat, ref_lon = location.latitude, location.longitude

        bearings = {
            "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
            "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
            "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
            "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5
        }

        bearing = bearings.get(direction.upper())
        if bearing is None:
            return None

        origin = (ref_lat, ref_lon)
        destination = geopy_distance(kilometers=distance_km).destination(origin, bearing)

        return (destination.latitude, destination.longitude)
    except Exception as e:
        logger.warning(f"Error calculating from reference: {e}")
        return None


def validate_coordinates(lat: float, lon: float, country_iso3: str) -> bool:
    """Validate coordinates are within country bounds."""
    if lat is None or lon is None:
        return False

    # Basic Earth bounds
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False

    # Country-specific validation
    bbox = COUNTRY_BBOX.get(country_iso3)
    if bbox:
        in_bounds = (
            bbox["lat_min"] <= lat <= bbox["lat_max"] and
            bbox["lon_min"] <= lon <= bbox["lon_max"]
        )
        if not in_bounds:
            logger.warning(f"Coordinates ({lat}, {lon}) outside {country_iso3} bounds")
            return False

    return True


def geocode_facility(
    facility: Dict,
    tavily_key: str,
    openai_client,
    dry_run: bool = True
) -> Optional[Dict]:
    """Geocode a single facility using web search + LLM."""

    name = facility["name"]
    country = facility["country_iso3"]
    commodities = facility["commodities"]

    # Clean up name (remove markdown artifacts)
    clean_name = name.replace("**", "").replace("*", "").strip()

    # Build search queries
    commodity_str = commodities[0] if commodities else "mine"
    queries = [
        f'"{clean_name}" {country} coordinates location',
        f'"{clean_name}" mine {country} latitude longitude',
        f"{clean_name} {commodity_str} {country} site location",
    ]

    # Try each query
    all_results = []
    for query in queries:
        logger.info(f"  Searching: {query[:60]}...")
        results = tavily_search(query, tavily_key)
        if results:
            all_results.extend(results)
            if len(all_results) >= 8:
                break
        time.sleep(0.5)

    if not all_results:
        logger.info(f"  No search results found")
        return {"status": "no_results"}

    logger.info(f"  Found {len(all_results)} results, extracting coordinates...")

    # Extract coordinates using LLM
    extraction = extract_coordinates_with_llm(
        clean_name, country, commodities, all_results, openai_client
    )

    if not extraction:
        return {"status": "llm_error"}

    if not extraction.get("is_real_facility", True):
        return {
            "status": "not_real_facility",
            "notes": extraction.get("notes", "Appears to be a category or placeholder")
        }

    if not extraction.get("found"):
        return {
            "status": "not_found",
            "notes": extraction.get("notes", "No coordinates found in search results")
        }

    # Get coordinates (direct or calculated)
    lat = extraction.get("lat")
    lon = extraction.get("lon")

    if lat is None or lon is None:
        # Try calculating from reference
        if extraction.get("reference_town") and extraction.get("distance_km") and extraction.get("direction"):
            logger.info(f"  Calculating from {extraction['reference_town']}...")
            coords = calculate_from_reference(
                extraction["reference_town"],
                country,
                extraction["distance_km"],
                extraction["direction"]
            )
            if coords:
                lat, lon = coords
                extraction["lat"] = lat
                extraction["lon"] = lon
                extraction["confidence"] = extraction.get("confidence", 0.7) * 0.9

    if lat is None or lon is None:
        return {
            "status": "no_coordinates",
            "notes": extraction.get("notes", "Could not determine coordinates")
        }

    # Validate coordinates
    if not validate_coordinates(lat, lon, country):
        return {
            "status": "invalid_coordinates",
            "lat": lat,
            "lon": lon,
            "notes": f"Coordinates outside {country} bounds"
        }

    # Success!
    return {
        "status": "success",
        "lat": lat,
        "lon": lon,
        "confidence": extraction.get("confidence", 0.7),
        "source": extraction.get("source_url", "web_search"),
        "province": extraction.get("province"),
        "notes": extraction.get("notes", ""),
    }


def apply_geocoding_result(
    facility: Dict,
    result: Dict,
    dry_run: bool = True
) -> bool:
    """Apply geocoding result to facility file."""

    if result["status"] != "success":
        return False

    file_path = facility["file"]
    data = facility["data"]

    # Update location
    data["location"]["lat"] = result["lat"]
    data["location"]["lon"] = result["lon"]
    data["location"]["precision"] = "site" if result["confidence"] >= 0.8 else "region"

    # Update province if found
    if result.get("province"):
        data["province"] = result["province"]

    # Update verification
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if "verification" not in data:
        data["verification"] = {}

    data["verification"]["status"] = "web_search_geocoded"
    data["verification"]["last_checked"] = timestamp
    data["verification"]["checked_by"] = "geocode_null_island"

    note = f"[NULL-ISLAND-FIX {timestamp}] Geocoded via web search. "
    note += f"Coords: ({result['lat']}, {result['lon']}). "
    note += f"Confidence: {result['confidence']:.2f}. "
    if result.get("notes"):
        note += f"Notes: {result['notes']}"

    existing_notes = data["verification"].get("notes", "")
    data["verification"]["notes"] = (existing_notes + " | " + note).strip(" |")

    if dry_run:
        logger.info(f"  [DRY RUN] Would update {file_path.name}")
        return True

    # Write file
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        return True
    except Exception as e:
        logger.error(f"Error writing {file_path}: {e}")
        return False


def mark_as_invalid(
    facility: Dict,
    reason: str,
    dry_run: bool = True
) -> bool:
    """Mark a facility as invalid/unlocatable."""

    file_path = facility["file"]
    data = facility["data"]

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if "verification" not in data:
        data["verification"] = {}

    data["verification"]["status"] = "unlocatable"
    data["verification"]["last_checked"] = timestamp
    data["verification"]["checked_by"] = "geocode_null_island"

    note = f"[NULL-ISLAND-CHECK {timestamp}] {reason}"
    existing_notes = data["verification"].get("notes", "")
    data["verification"]["notes"] = (existing_notes + " | " + note).strip(" |")

    if dry_run:
        logger.info(f"  [DRY RUN] Would mark as invalid: {file_path.name}")
        return True

    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        return True
    except Exception as e:
        logger.error(f"Error writing {file_path}: {e}")
        return False


def scan_null_island():
    """Scan and report null-island facilities."""
    facilities = find_null_island_facilities()

    print(f"\n{'='*70}")
    print(f"NULL ISLAND FACILITIES REPORT")
    print(f"{'='*70}")
    print(f"\nTotal facilities with null coordinates: {len(facilities)}")

    # Group by country
    by_country = {}
    for f in facilities:
        country = f["country_iso3"]
        by_country.setdefault(country, []).append(f)

    print(f"\nBy country (top 20):")
    for country, facs in sorted(by_country.items(), key=lambda x: -len(x[1]))[:20]:
        print(f"  {country}: {len(facs)}")

    print(f"\nSample facilities:")
    for f in facilities[:10]:
        name = f["name"][:50]
        print(f"  {f['country_iso3']}/{f['facility_id']}: {name}")

    return facilities


def main():
    parser = argparse.ArgumentParser(description="Geocode null-island facilities")
    parser.add_argument("--scan", action="store_true", help="Scan and report null facilities")
    parser.add_argument("--country", type=str, help="Process specific country (ISO3)")
    parser.add_argument("--all", action="store_true", help="Process all countries")
    parser.add_argument("--limit", type=int, default=10, help="Max facilities to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument("--mark-invalid", action="store_true",
                        help="Mark unfound facilities as invalid")

    args = parser.parse_args()

    if args.scan:
        scan_null_island()
        return

    if not args.country and not args.all:
        print("Use --scan to see null facilities, or --country/--all to geocode them")
        return

    # Check API keys
    tavily_key = os.environ.get("TAVILY_API_KEY")
    openai_key = os.environ.get("OPENAI_API_KEY")

    if not tavily_key:
        print("ERROR: TAVILY_API_KEY not set")
        print("Get one at https://tavily.com/ (free tier available)")
        sys.exit(1)

    if not openai_key:
        print("ERROR: OPENAI_API_KEY not set")
        sys.exit(1)

    # Import OpenAI
    try:
        from openai import OpenAI
        openai_client = OpenAI(api_key=openai_key)
    except ImportError:
        print("ERROR: openai not installed. pip install openai")
        sys.exit(1)

    # Find facilities to process
    all_facilities = find_null_island_facilities()

    if args.country:
        facilities = [f for f in all_facilities if f["country_iso3"] == args.country]
        print(f"\nFound {len(facilities)} null-coordinate facilities in {args.country}")
    else:
        facilities = all_facilities
        print(f"\nFound {len(facilities)} null-coordinate facilities total")

    if not facilities:
        print("No facilities to process")
        return

    # Limit
    facilities = facilities[:args.limit]
    print(f"Processing {len(facilities)} facilities...")

    # Process each facility
    stats = {"success": 0, "not_found": 0, "invalid": 0, "error": 0}

    for i, facility in enumerate(facilities, 1):
        name = facility["name"].replace("**", "")[:40]
        print(f"\n[{i}/{len(facilities)}] {facility['country_iso3']}/{name}")

        result = geocode_facility(
            facility, tavily_key, openai_client, dry_run=args.dry_run
        )

        if result["status"] == "success":
            print(f"  ✓ Found: ({result['lat']:.4f}, {result['lon']:.4f})")
            apply_geocoding_result(facility, result, dry_run=args.dry_run)
            stats["success"] += 1

        elif result["status"] == "not_real_facility":
            print(f"  ⚠ Not a real facility: {result.get('notes', '')[:60]}")
            if args.mark_invalid:
                mark_as_invalid(facility, result.get("notes", "Not a real facility"),
                               dry_run=args.dry_run)
            stats["invalid"] += 1

        elif result["status"] in ["not_found", "no_results", "no_coordinates"]:
            print(f"  ✗ Not found: {result.get('notes', result['status'])[:60]}")
            stats["not_found"] += 1

        elif result["status"] == "invalid_coordinates":
            print(f"  ✗ Invalid coords: ({result.get('lat')}, {result.get('lon')})")
            stats["error"] += 1

        else:
            print(f"  ✗ Error: {result['status']}")
            stats["error"] += 1

        # Rate limiting
        time.sleep(1)

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"  Geocoded successfully: {stats['success']}")
    print(f"  Not found: {stats['not_found']}")
    print(f"  Invalid/placeholder: {stats['invalid']}")
    print(f"  Errors: {stats['error']}")

    if args.dry_run:
        print(f"\n  [DRY RUN - no files were modified]")


if __name__ == "__main__":
    main()
