#!/usr/bin/env python3
"""
Unified geocoding and enrichment for mining facilities.

Combines web search, manual entry, CSV import, and intelligent data smoothing.

Features:
1. Web search geocoding (Tavily, Brave) + LLM extraction
2. Coordinates (lat/lon) extraction
3. Company mentions (operators/owners) extraction
4. Facility status extraction (operating/closed/unknown)
5. Location metadata (province, town)
6. Name quality assessment
7. Interactive validation for uncertain cases

Usage:
    # Web search enrichment (default mode)
    export TAVILY_API_KEY="your-key"
    export OPENAI_API_KEY="your-key"
    python scripts/enrich_facilities.py --limit 10

    # Process specific country
    python scripts/enrich_facilities.py --country MAR --limit 20

    # Process ALL facilities (regardless of missing data)
    python scripts/enrich_facilities.py --all --limit 3

    # Focus on low-quality facility names
    python scripts/enrich_facilities.py --low-quality-names --limit 10

    # Interactive enrichment: web search + ask user to confirm/fill gaps
    python scripts/enrich_facilities.py --interactive-enrich --country MDG --limit 5

    # Assess and report without making changes
    python scripts/enrich_facilities.py --assess-only --country USA

    # With Brave Search instead
    export BRAVE_API_KEY="your-key"
    python scripts/enrich_facilities.py --search-engine brave --limit 5

    # Manual/CSV modes (no API keys needed)
    python scripts/enrich_facilities.py --list-missing              # List facilities needing coords
    python scripts/enrich_facilities.py --list-missing --country ZAF
    python scripts/enrich_facilities.py --import-csv coords.csv     # Import from CSV
    python scripts/enrich_facilities.py --interactive               # Manual entry mode
    python scripts/enrich_facilities.py --interactive --country ZAF

CSV Format:
    facility_id,lat,lon,precision,source,notes
    zaf-karee-mine-fac,-25.7234,27.2156,site,Google Maps,"Main shaft coordinates"

Environment:
    TAVILY_API_KEY: For Tavily search API (https://tavily.com/)
    BRAVE_API_KEY: For Brave search API
    OPENAI_API_KEY: For LLM processing of search results

Flags:
    --all: Process ALL facilities (not just missing coords)
    --missing-companies: Process facilities with empty company_mentions
    --low-quality-names: Only process facilities with low name quality scores
    --interactive-enrich: Web search + ask user to confirm/fill gaps for each facility
    --assess-only: Dry-run that shows what WOULD be processed
    --dry-run: Don't update files, just show what would be done
    --list-missing: List facilities missing coordinates
    --import-csv: Import coordinates from CSV file
    --interactive: Interactive coordinate entry mode (manual, no web search)
"""

import json
import os
import sys
import time
import re
import csv
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import name quality assessment
try:
    from scripts.utils.name_quality import NameQualityAssessor
    HAS_NAME_QUALITY = True
except ImportError:
    HAS_NAME_QUALITY = False

# Import validation functions from geocoding utils
try:
    from scripts.utils.geocoding import is_valid_coord, in_country_bbox, is_sentinel_coord
    HAS_VALIDATION = True
except ImportError:
    HAS_VALIDATION = False
    # Fallback implementations
    def is_valid_coord(lat, lon):
        try:
            return -90 <= float(lat) <= 90 and -180 <= float(lon) <= 180
        except (TypeError, ValueError):
            return False
    def in_country_bbox(lat, lon, country_iso3):
        return True  # Permissive fallback
    def is_sentinel_coord(lat, lon):
        return (round(float(lat), 4), round(float(lon), 4)) == (0.0, 0.0)

try:
    import requests
    from requests.exceptions import HTTPError, RequestException, Timeout
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    HTTPError = RequestException = Timeout = Exception

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


# =============================================================================
# INTERACTIVE VALIDATION
# =============================================================================

class InteractiveValidator:
    """Handle interactive validation with human input."""

    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self.decisions_cache = {}

    def ask_user(self, question: str, options: List[str], default: Optional[str] = None) -> str:
        """
        Ask user a question with multiple choice options.

        Args:
            question: The question to ask
            options: List of valid options
            default: Default option if user presses Enter

        Returns:
            Selected option
        """
        if not self.enabled:
            return default or options[0]

        print(f"\n{'='*70}")
        print(f"QUESTION: {question}")
        print('='*70)
        for i, opt in enumerate(options, 1):
            marker = " (default)" if opt == default else ""
            print(f"  [{i}] {opt}{marker}")

        while True:
            try:
                response = input(f"\nYour choice [1-{len(options)}]: ").strip()
                if not response and default:
                    print(f"Using default: {default}")
                    return default
                idx = int(response) - 1
                if 0 <= idx < len(options):
                    return options[idx]
                print(f"Please enter a number between 1 and {len(options)}")
            except (ValueError, KeyboardInterrupt):
                if default:
                    print(f"\nUsing default: {default}")
                    return default
                print("Invalid input. Please try again.")

    def confirm(self, message: str, default: bool = True) -> bool:
        """Ask for yes/no confirmation."""
        if not self.enabled:
            return default

        default_str = "Y/n" if default else "y/N"
        while True:
            response = input(f"\n{message} [{default_str}]: ").strip().lower()
            if not response:
                return default
            if response in ['y', 'yes']:
                return True
            if response in ['n', 'no']:
                return False
            print("Please answer 'y' or 'n'")

    def get_text_input(self, prompt: str, default: Optional[str] = None) -> Optional[str]:
        """Get free-form text input from user."""
        if not self.enabled:
            return default

        default_str = f" [{default}]" if default else ""
        response = input(f"\n{prompt}{default_str}: ").strip()
        return response if response else default


# =============================================================================
# WEB SEARCH AND LLM EXTRACTION
# =============================================================================

def _should_retry(status_code: Optional[int]) -> bool:
    """Return True if status indicates a transient issue."""
    if status_code is None:
        return False
    return status_code in {408, 420, 429, 430, 431, 432, 499, 500, 502, 503, 504}


def tavily_search(query: str, api_key: str, retries: int = 3) -> List[Dict]:
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

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("results", [])
        except HTTPError as e:
            status = e.response.status_code if e.response else None
            if _should_retry(status):
                wait = min(60, attempt * 5)
                print(f"  Tavily rate limit ({status}). Retrying in {wait}s...")
                time.sleep(wait)
                continue
            print(f"  Tavily search error ({status}): {e}")
            break
        except (Timeout, RequestException) as e:
            wait = min(60, attempt * 5)
            print(f"  Tavily network issue: {e}. Retrying in {wait}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"  Tavily search error: {e}")
            break
    return []


def brave_search(query: str, api_key: str, retries: int = 3) -> List[Dict]:
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

    for attempt in range(1, retries + 1):
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
        except HTTPError as e:
            status = e.response.status_code if e.response else None
            if _should_retry(status):
                wait = min(60, attempt * 5)
                print(f"  Brave rate limit ({status}). Retrying in {wait}s...")
                time.sleep(wait)
                continue
            print(f"  Brave search error ({status}): {e}")
            break
        except (Timeout, RequestException) as e:
            wait = min(60, attempt * 5)
            print(f"  Brave network issue: {e}. Retrying in {wait}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"  Brave search error: {e}")
            break
    return []


def extract_coordinates_with_llm(
    facility_name: str,
    country: str,
    search_results: List[Dict],
    client: OpenAI
) -> Optional[Dict]:
    """Use LLM to extract coordinates AND company information from search results."""

    # Format search results for LLM
    results_text = ""
    for i, result in enumerate(search_results[:8], 1):
        results_text += f"\n--- Result {i} ---\n"
        results_text += f"Title: {result.get('title', 'N/A')}\n"
        results_text += f"URL: {result.get('url', 'N/A')}\n"
        results_text += f"Content: {result.get('content', 'N/A')[:1000]}\n"

    prompt = f"""Extract location, company information, and status for this mining facility from the search results:

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
    "notes": "relevant context about the location",
    "status": "operating|closed|care_and_maintenance|development|unknown",
    "companies": {{
        "operators": ["Company Name 1", "Company Name 2"],
        "owners": ["Owner Company 1", "Owner Company 2"],
        "notes": "Any context about ownership/operation"
    }}
}}

IMPORTANT FOR COORDINATES:
- Extract EXACT coordinates if mentioned (e.g., "-26.5°S, 27.3°E" = lat:-26.5, lon:27.3)
- If only relative location (e.g., "22km NE of Welkom"), provide reference_town + distance + direction
- South latitudes are NEGATIVE, West longitudes are NEGATIVE
- Parse various formats: decimal degrees, DMS, or relative locations
- Confidence should reflect how certain you are (direct coords = 0.9, relative = 0.7, general area = 0.5)

IMPORTANT FOR COMPANIES:
- Extract ALL company names mentioned as operators, owners, or developers
- Include full legal names (e.g., "BHP Billiton Ltd" not just "BHP")
- Include joint venture partners separately
- Include parent companies if mentioned
- Use "operators" for companies actively running the facility
- Use "owners" for companies that own equity/assets

IMPORTANT FOR STATUS:
- "operating" = currently producing/active
- "closed" = permanently shut down or abandoned
- "care_and_maintenance" = temporarily suspended but maintained
- "development" = under construction or planned
- "unknown" = no clear status information in search results"""

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
    """Geocode a single facility AND extract company information using web search + LLM."""

    name = facility.get("name", "")
    country_iso3 = facility.get("country_iso3", "")

    # Build search queries (multiple attempts)
    queries = [
        f"{name} {country_name} mine operator owner",
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

    print(f"  Found {len(all_results)} search results, extracting info...")

    # Extract coordinates AND company info using LLM
    extraction = extract_coordinates_with_llm(name, country_name, all_results, openai_client)

    if not extraction or not extraction.get("found"):
        return None

    result = {
        "source": extraction.get("source_url", "web search"),
        "confidence": extraction.get("confidence", 0.6),
        "province": extraction.get("province"),
        "notes": extraction.get("notes", "")
    }

    # Extract company information
    companies = extraction.get("companies", {})
    if companies:
        result["companies"] = companies
        operators = companies.get("operators", [])
        owners = companies.get("owners", [])
        if operators or owners:
            print(f"  ✓ Found companies: {', '.join(operators[:2] + owners[:2])}")

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

    # Return company info even if no coordinates found
    if result.get("companies"):
        return result

    return None


def interactive_review_extraction(
    facility: Dict,
    extraction: Optional[Dict],
    validator: InteractiveValidator
) -> Optional[Dict]:
    """
    Interactive review of LLM extraction results.
    Allows user to confirm, modify, or fill in missing data.

    Returns the final result dict (possibly modified) or None to skip.
    """
    if not validator.enabled:
        return extraction

    name = facility.get("name", "")
    facility_id = facility.get("facility_id", "")

    print(f"\n{'='*70}")
    print(f"REVIEW: {name}")
    print(f"ID: {facility_id}")
    print('='*70)

    # Initialize result from extraction or empty
    result = extraction.copy() if extraction else {
        "found": False,
        "confidence": 0.0,
        "companies": {"operators": [], "owners": []},
    }

    # Show what was found
    print("\n📋 EXTRACTED DATA:")

    lat = result.get("lat")
    lon = result.get("lon")
    if lat and lon:
        print(f"  Coordinates: {lat:.6f}, {lon:.6f}")
    else:
        print(f"  Coordinates: NOT FOUND")

    companies = result.get("companies", {})
    operators = companies.get("operators", [])
    owners = companies.get("owners", [])
    if operators or owners:
        print(f"  Operators: {', '.join(operators) if operators else 'None'}")
        print(f"  Owners: {', '.join(owners) if owners else 'None'}")
    else:
        print(f"  Companies: NOT FOUND")

    status = result.get("status", "unknown")
    print(f"  Status: {status}")

    confidence = result.get("confidence", 0.0)
    print(f"  Confidence: {confidence:.0%}")

    if result.get("notes"):
        print(f"  Notes: {result['notes']}")

    # Ask if user wants to process this facility
    print()
    action = validator.ask_user(
        "What would you like to do?",
        ["Accept as-is", "Edit/Add data", "Skip this facility"],
        default="Accept as-is" if confidence >= 0.7 else "Edit/Add data"
    )

    if action == "Skip this facility":
        print("  → Skipped")
        return None

    if action == "Edit/Add data":
        print("\n📝 MANUAL INPUT (press Enter to keep current value)")
        print("-"*50)

        # Coordinates
        if lat and lon:
            print(f"  Current coords: {lat:.6f}, {lon:.6f}")
            if validator.confirm("Change coordinates?", default=False):
                new_lat = validator.get_text_input("Latitude", default=str(lat))
                new_lon = validator.get_text_input("Longitude", default=str(lon))
                try:
                    result["lat"] = float(new_lat)
                    result["lon"] = float(new_lon)
                    print(f"  → Updated to: {result['lat']:.6f}, {result['lon']:.6f}")
                except (ValueError, TypeError):
                    print("  → Invalid format, keeping original")
        else:
            print("  No coordinates found.")
            if validator.confirm("Enter coordinates manually?", default=True):
                new_lat = validator.get_text_input("Latitude")
                new_lon = validator.get_text_input("Longitude")
                if new_lat and new_lon:
                    try:
                        result["lat"] = float(new_lat)
                        result["lon"] = float(new_lon)
                        result["confidence"] = 0.9  # High confidence for manual entry
                        print(f"  → Set to: {result['lat']:.6f}, {result['lon']:.6f}")
                    except (ValueError, TypeError):
                        print("  → Invalid format, skipping coordinates")

        # Companies
        print()
        current_ops = ', '.join(operators) if operators else 'None'
        print(f"  Current operators: {current_ops}")
        new_ops = validator.get_text_input("Operators (comma-separated)", default=current_ops if operators else None)
        if new_ops and new_ops != 'None':
            result.setdefault("companies", {})["operators"] = [op.strip() for op in new_ops.split(",") if op.strip()]

        current_owners = ', '.join(owners) if owners else 'None'
        print(f"  Current owners: {current_owners}")
        new_owners = validator.get_text_input("Owners (comma-separated)", default=current_owners if owners else None)
        if new_owners and new_owners != 'None':
            result.setdefault("companies", {})["owners"] = [ow.strip() for ow in new_owners.split(",") if ow.strip()]

        # Status
        print()
        new_status = validator.ask_user(
            f"Facility status? (current: {status})",
            ["operating", "closed", "care_and_maintenance", "development", "unknown"],
            default=status if status != "unknown" else "operating"
        )
        result["status"] = new_status

    # Final confirmation
    print("\n" + "="*70)
    print("FINAL DATA TO SAVE:")
    print("="*70)
    if result.get("lat") and result.get("lon"):
        print(f"  Coordinates: {result['lat']:.6f}, {result['lon']:.6f}")
    companies = result.get("companies", {})
    if companies.get("operators") or companies.get("owners"):
        print(f"  Operators: {', '.join(companies.get('operators', []))}")
        print(f"  Owners: {', '.join(companies.get('owners', []))}")
    print(f"  Status: {result.get('status', 'unknown')}")

    if validator.confirm("Save these changes?", default=True):
        result["found"] = True
        return result
    else:
        print("  → Cancelled")
        return None


def update_facility_json(facility: Dict, location: Dict) -> bool:
    """Update facility JSON file with new coordinates, company mentions, and status."""
    path = Path(f"facilities/{facility['country_iso3']}/{facility['facility_id']}.json")

    if not path.exists():
        return False

    with open(path) as f:
        data = json.load(f)

    # Update coordinates (if provided)
    if location.get("lat") is not None and location.get("lon") is not None:
        data["location"]["lat"] = location["lat"]
        data["location"]["lon"] = location["lon"]

        # Update verification for coordinates
        data["verification"]["status"] = "web_search_geocoded"
        data["verification"]["confidence"] = location.get("confidence", 0.7)
        data["verification"]["last_checked"] = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        data["verification"]["checked_by"] = "web_search_llm"

        old_notes = data["verification"].get("notes", "")
        new_notes = f"Web search geocoded. Source: {location.get('source', 'web')}. {location.get('notes', '')}"
        data["verification"]["notes"] = f"{old_notes}. {new_notes}".strip(". ")

    # Update metadata
    if location.get("province"):
        data["province"] = location["province"]
    if location.get("town"):
        data["town"] = location["town"]

    # Update facility status
    if location.get("status") and location["status"] != "unknown":
        data["status"] = location["status"]

    # Update company mentions
    companies = location.get("companies", {})
    if companies:
        operators = companies.get("operators", [])
        owners = companies.get("owners", [])

        # Merge with existing company_mentions (deduplicate)
        # Handle both string and dict formats in company_mentions
        raw_mentions = data.get("company_mentions", [])
        existing_mentions = set()
        for mention in raw_mentions:
            if isinstance(mention, dict):
                # Extract name from structured format
                if "name" in mention:
                    existing_mentions.add(mention["name"])
            elif isinstance(mention, str):
                existing_mentions.add(mention)

        new_mentions = set(operators + owners)
        all_mentions = sorted(list(existing_mentions | new_mentions))

        if all_mentions:
            data["company_mentions"] = all_mentions

            # Add note about web search enrichment
            if "enrichment_notes" not in data:
                data["enrichment_notes"] = {}
            data["enrichment_notes"]["web_search_companies"] = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
                "operators": operators,
                "owners": owners,
                "notes": companies.get("notes", "")
            }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return True


# =============================================================================
# MANUAL ENTRY / CSV IMPORT FUNCTIONS
# =============================================================================

ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"


def load_facility(facility_id: str) -> Optional[Dict]:
    """Load facility JSON by ID."""
    if len(facility_id) < 3:
        return None

    country = facility_id[:3].upper()
    json_file = FACILITIES_DIR / country / f"{facility_id}.json"

    if not json_file.exists():
        print(f"  Facility not found: {json_file}")
        return None

    with open(json_file, 'r') as f:
        return json.load(f)


def save_facility(facility: Dict) -> None:
    """Save facility JSON."""
    facility_id = facility['facility_id']
    country = facility['country_iso3']
    json_file = FACILITIES_DIR / country / f"{facility_id}.json"

    with open(json_file, 'w') as f:
        json.dump(facility, f, indent=2, ensure_ascii=False)


def validate_coordinates(lat: float, lon: float, country_iso3: str, interactive: bool = False) -> bool:
    """Validate coordinates with multiple checks."""
    if is_sentinel_coord(lat, lon):
        print(f"  Rejected: Sentinel coordinates ({lat}, {lon})")
        return False

    if not is_valid_coord(lat, lon):
        print(f"  Rejected: Invalid coordinates ({lat}, {lon})")
        return False

    if not in_country_bbox(lat, lon, country_iso3):
        print(f"  Warning: Coordinates ({lat}, {lon}) outside {country_iso3} bbox")
        if interactive:
            response = input("  Continue anyway? (y/N): ")
            if response.lower() != 'y':
                return False
        else:
            return False

    return True


def add_manual_coordinates(
    facility_id: str,
    lat: float,
    lon: float,
    precision: str = "site",
    source: str = "manual_entry",
    notes: str = None,
    dry_run: bool = False,
    interactive: bool = False
) -> bool:
    """Add coordinates to a facility with validation."""
    facility = load_facility(facility_id)
    if not facility:
        return False

    country_iso3 = facility['country_iso3']

    # Validation gates
    if not validate_coordinates(lat, lon, country_iso3, interactive):
        return False

    # Update facility
    old_lat = facility.get('location', {}).get('lat')
    old_lon = facility.get('location', {}).get('lon')

    facility['location'] = {
        'lat': lat,
        'lon': lon,
        'precision': precision
    }

    # Update verification
    if 'verification' not in facility:
        facility['verification'] = {}

    facility['verification']['last_checked'] = datetime.now().isoformat()

    note_parts = [f"Manual coordinate entry: {source}"]
    if notes:
        note_parts.append(notes)
    if old_lat is not None and old_lon is not None:
        note_parts.append(f"(replaced: {old_lat}, {old_lon})")

    facility['verification']['notes'] = " | ".join(note_parts)

    # Display change
    action = "Would update" if dry_run else "Updated"
    if old_lat is not None and old_lon is not None:
        print(f"  {action}: {facility['name']}")
        print(f"    Old: {old_lat}, {old_lon}")
        print(f"    New: {lat}, {lon}")
    else:
        print(f"  {action}: {facility['name']}")
        print(f"    Coordinates: {lat}, {lon}")

    # Save
    if not dry_run:
        save_facility(facility)

    return True


def import_from_csv(csv_path: str, dry_run: bool = False) -> None:
    """Import coordinates from CSV file."""
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"CSV file not found: {csv_file}")
        return

    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)

        # Validate headers
        required = {'facility_id', 'lat', 'lon'}
        if not required.issubset(set(reader.fieldnames or [])):
            print(f"CSV missing required columns: {required}")
            print(f"  Found: {reader.fieldnames}")
            return

        stats = {"success": 0, "failed": 0}

        for i, row in enumerate(reader, start=1):
            facility_id = row['facility_id'].strip()

            try:
                lat = float(row['lat'])
                lon = float(row['lon'])
            except (ValueError, KeyError) as e:
                print(f"Row {i}: Invalid lat/lon - {e}")
                stats['failed'] += 1
                continue

            precision = row.get('precision', 'site').strip() or 'site'
            source = row.get('source', 'csv_import').strip() or 'csv_import'
            notes = row.get('notes', '').strip() or None

            print(f"\n[{i}] {facility_id}")
            success = add_manual_coordinates(
                facility_id, lat, lon, precision, source, notes, dry_run
            )

            if success:
                stats['success'] += 1
            else:
                stats['failed'] += 1

        print("\n" + "=" * 60)
        print("CSV IMPORT SUMMARY")
        print("=" * 60)
        print(f"Successful: {stats['success']}")
        print(f"Failed: {stats['failed']}")


def find_missing_coordinates(country: str = None) -> List[Dict]:
    """Find all facilities missing coordinates, optionally filtered by country."""
    missing = []

    if country:
        country_dir = FACILITIES_DIR / country.upper()
        if not country_dir.exists():
            print(f"Country not found: {country}")
            return []

        for json_file in sorted(country_dir.glob("*.json")):
            with open(json_file, 'r') as f:
                facility = json.load(f)

            lat = facility.get('location', {}).get('lat')
            lon = facility.get('location', {}).get('lon')

            if lat is None or lon is None:
                missing.append(facility)
    else:
        for country_dir in sorted(FACILITIES_DIR.iterdir()):
            if not country_dir.is_dir():
                continue

            for json_file in sorted(country_dir.glob("*.json")):
                with open(json_file, 'r') as f:
                    facility = json.load(f)

                lat = facility.get('location', {}).get('lat')
                lon = facility.get('location', {}).get('lon')

                if lat is None or lon is None:
                    missing.append(facility)

    return missing


def list_missing_coords(country: str = None) -> None:
    """List facilities missing coordinates."""
    missing = find_missing_coordinates(country)

    if not missing:
        print("No facilities missing coordinates!")
        return

    print(f"Found {len(missing)} facilities missing coordinates")
    if country:
        print(f"Country: {country.upper()}\n")
    else:
        print()

    for facility in missing[:100]:  # Show first 100
        commodities = facility.get('commodities', [])
        metals = [c.get('metal', '') for c in commodities if c.get('metal')]
        metals_str = f" ({', '.join(metals[:3])})" if metals else ""

        print(f"{facility['facility_id']:<40} {facility['name'][:50]}{metals_str}")

    if len(missing) > 100:
        print(f"\n... and {len(missing) - 100} more")


def interactive_add(country: str = None, dry_run: bool = False) -> None:
    """Interactive mode for adding coordinates."""
    print("Manual Coordinate Entry (Interactive Mode)")
    print("=" * 60)

    missing = find_missing_coordinates(country)

    if not missing:
        print("No facilities missing coordinates!")
        return

    print(f"Found {len(missing)} facilities missing coordinates")
    if country:
        print(f"Country: {country.upper()}")
    print()

    stats = {"added": 0, "skipped": 0}

    for i, facility in enumerate(missing, start=1):
        print("=" * 60)
        print(f"[{i}/{len(missing)}] {facility['facility_id']}")
        print(f"Name: {facility['name']}")
        print(f"Country: {facility['country_iso3']}")

        commodities = facility.get('commodities', [])
        if commodities:
            metals = [c.get('metal', '') for c in commodities if c.get('metal')]
            if metals:
                print(f"Commodities: {', '.join(metals)}")

        print()

        try:
            lat_input = input("Latitude (or 's' to skip, 'q' to quit): ").strip()

            if lat_input.lower() == 'q':
                print("\nQuitting...")
                break
            elif lat_input.lower() == 's':
                print("Skipped")
                stats['skipped'] += 1
                continue

            lat = float(lat_input)
            lon = float(input("Longitude: ").strip())

        except ValueError:
            print("Invalid lat/lon - skipping")
            stats['skipped'] += 1
            continue
        except (KeyboardInterrupt, EOFError):
            print("\n\nInterrupted - exiting")
            break

        precision = input("Precision (site/mine/town/region) [site]: ").strip() or "site"
        source = input("Source [manual_entry]: ").strip() or "manual_entry"
        notes = input("Notes (optional): ").strip() or None

        print()
        success = add_manual_coordinates(
            facility['facility_id'], lat, lon, precision, source, notes, dry_run, interactive=True
        )

        if success:
            stats['added'] += 1

        print()

    print("\n" + "=" * 60)
    print("SESSION SUMMARY")
    print("=" * 60)
    print(f"Coordinates added: {stats['added']}")
    print(f"Skipped: {stats['skipped']}")
    print(f"Remaining: {len(missing) - stats['added'] - stats['skipped']}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Unified geocoding: web search, manual entry, and CSV import",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Mode selection (mutually exclusive)
    mode_group = parser.add_argument_group("Mode selection (pick one)")
    mode_group.add_argument("--list-missing", action="store_true",
                           help="List facilities missing coordinates (no API keys needed)")
    mode_group.add_argument("--import-csv", metavar="FILE",
                           help="Import coordinates from CSV file (no API keys needed)")
    mode_group.add_argument("--interactive", action="store_true",
                           help="Interactive coordinate entry mode (no API keys needed)")

    # Common options
    parser.add_argument("--country", help="ISO3 country code (omit to process ALL countries)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without saving")

    # Web search options
    search_group = parser.add_argument_group("Web search options")
    search_group.add_argument("--start-from", help="ISO3 country code to start from")
    search_group.add_argument("--limit", type=int, default=10, help="Max facilities per country")
    search_group.add_argument("--limit-total", type=int, help="Max total facilities across all countries")
    search_group.add_argument("--search-engine", default="tavily", choices=["tavily", "brave"])
    search_group.add_argument("--all", action="store_true", help="Process ALL facilities (not just missing)")
    search_group.add_argument("--missing-companies", action="store_true",
                             help="Process facilities missing company_mentions")
    search_group.add_argument("--low-quality-names", action="store_true",
                             help="Only process facilities with low name quality scores")
    search_group.add_argument("--assess-only", action="store_true",
                             help="Dry-run that shows what WOULD be processed (no changes)")
    search_group.add_argument("--reverse", action="store_true", help="Process countries Z to A")
    search_group.add_argument("--interactive-enrich", action="store_true",
                             help="Interactive mode: web search + ask user to confirm/fill gaps")

    args = parser.parse_args()

    # ==========================================================================
    # Handle non-API modes first (no API keys needed)
    # ==========================================================================

    if args.list_missing:
        list_missing_coords(args.country)
        return

    if args.import_csv:
        import_from_csv(args.import_csv, args.dry_run)
        return

    if args.interactive:
        interactive_add(args.country, args.dry_run)
        return

    # ==========================================================================
    # Web search mode - requires API keys
    # ==========================================================================

    # Validate arguments
    if args.country and args.start_from:
        parser.error("Cannot use both --country and --start-from together")

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

    # Initialize name quality assessor if needed
    name_assessor = None
    if args.low_quality_names:
        if not HAS_NAME_QUALITY:
            print("ERROR: NameQualityAssessor not available. Install required dependencies.")
            sys.exit(1)
        name_assessor = NameQualityAssessor()
        print("Using name quality assessment to filter facilities")

    # Initialize interactive validator if needed
    interactive = InteractiveValidator(enabled=args.interactive_enrich)
    if args.interactive_enrich:
        print("Interactive mode: will ask for confirmation/input on each facility")

    # Country names mapping
    country_names = {
        "ZAF": "South Africa", "USA": "United States", "AUS": "Australia",
        "IND": "India", "CHN": "China", "BRA": "Brazil", "RUS": "Russia",
        "CAN": "Canada", "IDN": "Indonesia", "KAZ": "Kazakhstan",
        "MEX": "Mexico", "PER": "Peru", "CHL": "Chile", "COL": "Colombia",
        "ARE": "United Arab Emirates", "BEL": "Belgium", "MAR": "Morocco",
        "KOR": "South Korea", "PRK": "North Korea", "MNG": "Mongolia",
        "ZWE": "Zimbabwe", "FRA": "France", "AUT": "Austria", "CIV": "Ivory Coast",
        "MDG": "Madagascar", "NER": "Niger", "PAK": "Pakistan", "AZE": "Azerbaijan",
        "NCL": "New Caledonia", "NPL": "Nepal", "PRT": "Portugal", "SDN": "Sudan",
    }

    # Determine which countries to process
    if args.country:
        countries_to_process = [args.country.upper()]
    else:
        # Process all countries
        facilities_base = Path("facilities")
        all_countries = sorted([d.name for d in facilities_base.iterdir() if d.is_dir()])

        # If start-from specified, skip countries until we reach it
        if args.start_from:
            start_country = args.start_from.upper()
            if start_country in all_countries:
                start_idx = all_countries.index(start_country)
                countries_to_process = all_countries[start_idx:]
                print(f"Starting from {start_country}: {len(countries_to_process)} countries to process\n")
            else:
                print(f"WARNING: Start country {start_country} not found. Processing all countries.")
                countries_to_process = all_countries
        else:
            countries_to_process = all_countries
            print(f"Processing ALL countries: {len(countries_to_process)} found\n")

    # Apply reverse order if requested
    if args.reverse:
        countries_to_process = list(reversed(countries_to_process))
        print(f"Processing in REVERSE order (Z→A)\n")

    print(f"\n{'='*60}")
    print(f"WEB SEARCH GEOCODING + COMPANY ENRICHMENT")
    print(f"Search Engine: {args.search_engine}")
    print(f"{'='*60}\n")

    total_success = 0
    total_failed = 0
    total_processed = 0
    countries_processed = 0

    for country_iso3 in countries_to_process:
        country_name = country_names.get(country_iso3, country_iso3)

        # Check total limit
        if args.limit_total and total_processed >= args.limit_total:
            print(f"\n→ Reached total limit of {args.limit_total} facilities")
            break

        print(f"\n{'─'*60}")
        print(f"COUNTRY: {country_name} ({country_iso3})")
        print(f"{'─'*60}")

        # Load facilities to process
        facilities_dir = Path(f"facilities/{country_iso3}")
        if not facilities_dir.exists():
            print(f"⚠ No facilities directory found, skipping")
            continue

        to_process = []

        for f in facilities_dir.glob("*.json"):
            try:
                with open(f) as fp:
                    data = json.load(fp)
            except Exception as e:
                print(f"⚠ Error reading {f.name}: {e}")
                continue

            # Determine if we should process this facility
            should_process = False

            if args.low_quality_names:
                # Process ONLY if name quality is low
                if name_assessor:
                    assessment = name_assessor.assess_name(data.get("name", ""))
                    if assessment['quality_score'] < 0.5 or assessment['is_generic']:
                        should_process = True
            elif args.all:
                # Process everything
                should_process = True
            elif args.missing_companies:
                # Process ONLY if company_mentions is empty or missing
                if not data.get("company_mentions"):
                    should_process = True
            else:
                # Default: process if missing coordinates OR company_mentions
                loc = data.get("location", {})
                missing_coords = loc.get("lat") is None or loc.get("lon") is None
                missing_companies = not data.get("company_mentions")

                if missing_coords or missing_companies:
                    should_process = True

            if should_process:
                to_process.append(data)

        # Determine mode description
        if args.low_quality_names:
            mode = "low-quality names"
        elif args.all:
            mode = "ALL facilities"
        elif args.missing_companies:
            mode = "missing companies"
        else:
            mode = "missing coords/companies"

        if not to_process:
            print(f"✓ No facilities need processing ({mode})")
            continue

        # Apply limits
        original_count = len(to_process)

        # Per-country limit
        if args.limit:
            to_process = to_process[:args.limit]

        # Total limit
        if args.limit_total:
            remaining_budget = args.limit_total - total_processed
            to_process = to_process[:remaining_budget]

        print(f"Found {original_count} {mode}, processing {len(to_process)}")

        # If assess-only mode, just list and continue
        if args.assess_only:
            print("\n[ASSESS-ONLY MODE] Would process:")
            for i, facility in enumerate(to_process, 1):
                name = facility.get('name', 'Unknown')
                fac_id = facility.get('facility_id', 'Unknown')
                print(f"  [{i}] {fac_id}: {name}")

                # Show name quality if available
                if name_assessor:
                    assessment = name_assessor.assess_name(name)
                    print(f"      Quality: {assessment['quality_score']:.2f} | Generic: {assessment['is_generic']}")
                    if assessment['issues']:
                        print(f"      Issues: {', '.join(assessment['issues'][:3])}")
            continue

        country_success = 0
        country_failed = 0

        for i, facility in enumerate(to_process, 1):
            print(f"\n[{i}/{len(to_process)}] {facility['name']}")

            result = geocode_single_facility(facility, country_name, search_func, openai_client)

            # Interactive mode: let user review and modify extraction results
            if args.interactive_enrich:
                result = interactive_review_extraction(facility, result, interactive)
                if result is None:
                    # User chose to skip
                    country_failed += 1
                    continue

            if result:
                has_coords = result.get("lat") is not None and result.get("lon") is not None
                has_companies = bool(result.get("companies", {}).get("operators") or result.get("companies", {}).get("owners"))
                has_status = result.get("status") and result.get("status") != "unknown"

                if has_coords or has_companies or has_status:
                    if not args.dry_run:
                        if update_facility_json(facility, result):
                            update_msg = []
                            if has_coords:
                                update_msg.append(f"coords: {result['lat']:.6f}, {result['lon']:.6f}")
                            if has_companies:
                                update_msg.append(f"companies: {len(result['companies'].get('operators', []) + result['companies'].get('owners', []))}")
                            if has_status:
                                update_msg.append(f"status: {result['status']}")
                            print(f"  → Updated ({', '.join(update_msg)})")
                            country_success += 1
                        else:
                            country_failed += 1
                    else:
                        print(f"  [DRY RUN] Would update")
                        country_success += 1
                else:
                    print(f"  ✗ No info found")
                    country_failed += 1
            else:
                print(f"  ✗ No info found")
                country_failed += 1

            time.sleep(1)  # Rate limiting

        total_success += country_success
        total_failed += country_failed
        total_processed += len(to_process)
        countries_processed += 1

        print(f"\n{country_iso3} Summary: {country_success} success, {country_failed} failed")

    # Final summary
    print(f"\n{'='*60}")
    print("FINAL RESULTS")
    print(f"{'='*60}")
    print(f"Countries processed: {countries_processed}")
    print(f"Total facilities processed: {total_processed}")
    print(f"Successfully enriched: {total_success}")
    print(f"Failed: {total_failed}")
    if args.limit_total:
        print(f"Total limit: {args.limit_total}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
