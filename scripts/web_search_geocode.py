#!/usr/bin/env python3
"""
Web search-based geocoding for mining facilities.

Uses real web searches (Tavily, Brave, or Google) + LLM to interpret results.
This actually searches the web, unlike pure LLM approaches.

Usage:
    # With Tavily (recommended - best for mining data)
    export TAVILY_API_KEY="your-key"
    python scripts/web_search_geocode.py --country ZAF --limit 10

    # With Brave Search
    export BRAVE_API_KEY="your-key"
    python scripts/web_search_geocode.py --country ZAF --search-engine brave

Environment:
    TAVILY_API_KEY: For Tavily search API (https://tavily.com/)
    BRAVE_API_KEY: For Brave search API
    OPENAI_API_KEY: For LLM processing of search results
"""

import json
import os
import sys
import time
import re
from pathlib import Path
from typing import Optional, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

try:
    from geopy.geocoders import Nominatim
    from geopy.distance import distance as geopy_distance
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False


def tavily_search(query: str, api_key: str) -> List[Dict]:
    """Search using Tavily API - great for technical content."""
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": api_key,
        "query": query,
        "search_depth": "advanced",
        "include_answer": True,
        "include_raw_content": False,
        "max_results": 10,
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except Exception as e:
        print(f"  Tavily search error: {e}")
        return []


def brave_search(query: str, api_key: str) -> List[Dict]:
    """Search using Brave Search API."""
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key
    }
    params = {
        "q": query,
        "count": 10
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        results = []
        for item in data.get("web", {}).get("results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "content": item.get("description", "")
            })
        return results
    except Exception as e:
        print(f"  Brave search error: {e}")
        return []


def extract_coordinates_with_llm(
    facility_name: str,
    country: str,
    search_results: List[Dict],
    client: OpenAI
) -> Optional[Dict]:
    """Use LLM to extract coordinates from search results."""

    # Format search results for LLM
    results_text = ""
    for i, result in enumerate(search_results[:8], 1):
        results_text += f"\n--- Result {i} ---\n"
        results_text += f"Title: {result.get('title', 'N/A')}\n"
        results_text += f"URL: {result.get('url', 'N/A')}\n"
        results_text += f"Content: {result.get('content', 'N/A')[:1000]}\n"

    prompt = f"""Extract location information for this mining facility from the search results:

FACILITY: {facility_name}
COUNTRY: {country}

SEARCH RESULTS:
{results_text}

Extract and return JSON with:
{{
    "found": true/false,
    "lat": latitude (decimal degrees, negative for South) or null,
    "lon": longitude (decimal degrees, negative for West) or null,
    "reference_town": "nearest town if coords not directly available",
    "distance_km": distance from reference town (number) or null,
    "direction": "N/NE/E/SE/S/SW/W/NW" from reference town or null,
    "province": "province/state name",
    "source_url": "URL where you found the info",
    "confidence": 0.0-1.0,
    "notes": "relevant context about the location"
}}

IMPORTANT:
- Extract EXACT coordinates if mentioned (e.g., "-26.5°S, 27.3°E" = lat:-26.5, lon:27.3)
- If only relative location (e.g., "22km NE of Welkom"), provide reference_town + distance + direction
- South latitudes are NEGATIVE, West longitudes are NEGATIVE
- Parse various formats: decimal degrees, DMS, or relative locations
- Confidence should reflect how certain you are (direct coords = 0.9, relative = 0.7, general area = 0.5)"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Fast and cheap for extraction
            messages=[
                {"role": "system", "content": "You are an expert at extracting geographic coordinates from mining reports and technical documents. Be precise with coordinate signs (negative for South/West)."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"  LLM extraction error: {e}")
        return None


def calculate_from_reference(
    reference_town: str,
    country: str,
    distance_km: float,
    direction: str
) -> Optional[Tuple[float, float]]:
    """Calculate coordinates from a reference point."""
    if not HAS_GEOPY:
        print("  geopy not installed - can't calculate from reference")
        return None

    geolocator = Nominatim(user_agent="gsmc-mining-geocoder")
    time.sleep(1.1)  # Rate limit

    try:
        location = geolocator.geocode(f"{reference_town}, {country}", timeout=10)
        if not location:
            print(f"  Could not geocode reference town: {reference_town}")
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
            print(f"  Unknown direction: {direction}")
            return None

        origin = (ref_lat, ref_lon)
        destination = geopy_distance(kilometers=distance_km).destination(origin, bearing)

        return (destination.latitude, destination.longitude)
    except Exception as e:
        print(f"  Error calculating from reference: {e}")
        return None


def geocode_single_facility(
    facility: Dict,
    country_name: str,
    search_func,
    openai_client: OpenAI
) -> Optional[Dict]:
    """Geocode a single facility using web search + LLM."""

    name = facility.get("name", "")
    country_iso3 = facility.get("country_iso3", "")

    # Build search queries (multiple attempts)
    queries = [
        f"{name} {country_name} coordinates location",
        f'"{name}" site:miningdataonline.com OR site:infomine.com',
        f"{name} {country_name} NI 43-101 OR JORC report",
    ]

    # Try each query until we get results
    all_results = []
    for query in queries:
        print(f"  Searching: {query[:60]}...")
        results = search_func(query)
        if results:
            all_results.extend(results)
            if len(all_results) >= 10:
                break
        time.sleep(0.5)  # Rate limiting between searches

    if not all_results:
        print(f"  No search results found")
        return None

    print(f"  Found {len(all_results)} search results, extracting location...")

    # Extract coordinates using LLM
    extraction = extract_coordinates_with_llm(name, country_name, all_results, openai_client)

    if not extraction or not extraction.get("found"):
        return None

    result = {
        "source": extraction.get("source_url", "web search"),
        "confidence": extraction.get("confidence", 0.6),
        "province": extraction.get("province"),
        "notes": extraction.get("notes", "")
    }

    # Case 1: Direct coordinates
    if extraction.get("lat") and extraction.get("lon"):
        result["lat"] = extraction["lat"]
        result["lon"] = extraction["lon"]
        print(f"  ✓ Found coordinates: {result['lat']:.6f}, {result['lon']:.6f}")
        return result

    # Case 2: Calculate from reference
    if extraction.get("reference_town") and extraction.get("distance_km") and extraction.get("direction"):
        print(f"  Calculating from {extraction['reference_town']} + {extraction['distance_km']}km {extraction['direction']}")
        coords = calculate_from_reference(
            extraction["reference_town"],
            country_name,
            extraction["distance_km"],
            extraction["direction"]
        )
        if coords:
            result["lat"] = coords[0]
            result["lon"] = coords[1]
            result["confidence"] *= 0.9  # Slightly lower confidence for calculated
            result["town"] = extraction["reference_town"]
            result["source"] = f"Calculated from {extraction['reference_town']} ({extraction['source_url']})"
            print(f"  ✓ Calculated coordinates: {result['lat']:.6f}, {result['lon']:.6f}")
            return result

    return None


def update_facility_json(facility: Dict, location: Dict) -> bool:
    """Update facility JSON file with new coordinates."""
    path = Path(f"facilities/{facility['country_iso3']}/{facility['facility_id']}.json")

    if not path.exists():
        return False

    with open(path) as f:
        data = json.load(f)

    # Update coordinates
    data["location"]["lat"] = location["lat"]
    data["location"]["lon"] = location["lon"]

    # Update metadata
    if location.get("province"):
        data["province"] = location["province"]
    if location.get("town"):
        data["town"] = location["town"]

    # Update verification
    data["verification"]["status"] = "web_search_geocoded"
    data["verification"]["confidence"] = location.get("confidence", 0.7)
    data["verification"]["last_checked"] = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    data["verification"]["checked_by"] = "web_search_llm"

    old_notes = data["verification"].get("notes", "")
    new_notes = f"Web search geocoded. Source: {location.get('source', 'web')}. {location.get('notes', '')}"
    data["verification"]["notes"] = f"{old_notes}. {new_notes}".strip(". ")

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Web search-based geocoding")
    parser.add_argument("--country", required=True, help="ISO3 country code")
    parser.add_argument("--limit", type=int, default=10, help="Max facilities")
    parser.add_argument("--search-engine", default="tavily", choices=["tavily", "brave"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Check dependencies
    if not HAS_REQUESTS:
        print("ERROR: requests not installed. pip install requests")
        sys.exit(1)
    if not HAS_OPENAI:
        print("ERROR: openai not installed. pip install openai")
        sys.exit(1)

    # Check API keys
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not openai_key:
        print("ERROR: OPENAI_API_KEY not set")
        sys.exit(1)

    # Set up search function
    if args.search_engine == "tavily":
        tavily_key = os.environ.get("TAVILY_API_KEY")
        if not tavily_key:
            print("ERROR: TAVILY_API_KEY not set")
            print("Get one at https://tavily.com/ (free tier available)")
            sys.exit(1)
        search_func = lambda q: tavily_search(q, tavily_key)
    else:
        brave_key = os.environ.get("BRAVE_API_KEY")
        if not brave_key:
            print("ERROR: BRAVE_API_KEY not set")
            sys.exit(1)
        search_func = lambda q: brave_search(q, brave_key)

    openai_client = OpenAI(api_key=openai_key)

    # Country names
    country_names = {
        "ZAF": "South Africa", "USA": "United States", "AUS": "Australia",
        "IND": "India", "CHN": "China", "BRA": "Brazil", "RUS": "Russia",
        "CAN": "Canada", "IDN": "Indonesia", "KAZ": "Kazakhstan",
        "MEX": "Mexico", "PER": "Peru", "CHL": "Chile", "COL": "Colombia",
        "ARE": "United Arab Emirates", "BEL": "Belgium", "MAR": "Morocco",
    }
    country_name = country_names.get(args.country, args.country)

    print(f"\n{'='*60}")
    print(f"WEB SEARCH GEOCODING: {country_name} ({args.country})")
    print(f"Search Engine: {args.search_engine}")
    print(f"{'='*60}")

    # Load facilities needing geocoding
    facilities_dir = Path(f"facilities/{args.country}")
    if not facilities_dir.exists():
        print(f"No facilities directory for {args.country}")
        return

    missing = []
    for f in facilities_dir.glob("*.json"):
        with open(f) as fp:
            data = json.load(fp)
        loc = data.get("location", {})
        if loc.get("lat") is None or loc.get("lon") is None:
            missing.append(data)

    print(f"Found {len(missing)} facilities needing coordinates")

    if not missing:
        return

    to_process = missing[:args.limit]
    print(f"Processing {len(to_process)} facilities\n")

    success = 0
    failed = 0

    for i, facility in enumerate(to_process, 1):
        print(f"[{i}/{len(to_process)}] {facility['name']}")

        result = geocode_single_facility(facility, country_name, search_func, openai_client)

        if result and result.get("lat") and result.get("lon"):
            if not args.dry_run:
                if update_facility_json(facility, result):
                    print(f"  → Updated facility file")
                    success += 1
                else:
                    failed += 1
            else:
                print(f"  [DRY RUN] Would update")
                success += 1
        else:
            print(f"  ✗ Could not geocode")
            failed += 1

        print()
        time.sleep(1)  # Rate limiting

    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"Successfully geocoded: {success}")
    print(f"Failed: {failed}")
    print(f"Remaining: {len(missing) - args.limit}")


if __name__ == "__main__":
    main()
