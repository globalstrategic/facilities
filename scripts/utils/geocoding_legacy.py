#!/usr/bin/env python3
"""
Geocoding utilities for facilities with missing coordinates.

Provides multiple fallback strategies:
1. Nominatim (OpenStreetMap) - Free, rate-limited
2. City/region extraction from facility names
3. Industrial zone database lookup
4. Interactive prompting as last resort

Usage:
    from scripts.utils.geocoding import geocode_facility

    coords = geocode_facility(
        facility_name="Union Cement Company",
        country_iso3="ARE",
        city="Abu Dhabi",
        interactive=True
    )
"""

import re
import time
import logging
from typing import Optional, Dict, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Rate limiting for Nominatim (max 1 req/sec)
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 1.0  # seconds


@dataclass
class GeocodingResult:
    """Result from geocoding operation."""
    lat: Optional[float]
    lon: Optional[float]
    precision: str  # 'site', 'city', 'region', 'country', 'unknown'
    source: str     # 'nominatim', 'industrial_zone', 'user_input', etc.
    confidence: float  # 0.0-1.0


# Known industrial zones and their approximate coordinates
INDUSTRIAL_ZONES = {
    # UAE
    "icad": {"lat": 24.338, "lon": 54.524, "city": "Abu Dhabi", "country": "ARE"},
    "icad i": {"lat": 24.338, "lon": 54.524, "city": "Abu Dhabi", "country": "ARE"},
    "icad ii": {"lat": 24.315, "lon": 54.495, "city": "Abu Dhabi", "country": "ARE"},
    "icad iii": {"lat": 24.303, "lon": 54.462, "city": "Abu Dhabi", "country": "ARE"},
    "musaffah": {"lat": 24.353, "lon": 54.504, "city": "Abu Dhabi", "country": "ARE"},
    "jebel ali": {"lat": 24.986, "lon": 55.048, "city": "Dubai", "country": "ARE"},
    "foiz": {"lat": 25.111, "lon": 56.342, "city": "Fujairah", "country": "ARE"},
    "fujairah oil industry zone": {"lat": 25.111, "lon": 56.342, "city": "Fujairah", "country": "ARE"},
    "hamriyah": {"lat": 25.434, "lon": 55.528, "city": "Sharjah", "country": "ARE"},

    # Add more as needed
}


def extract_location_hints(facility_name: str, country_iso3: str) -> Dict[str, str]:
    """
    Extract city/region hints from facility name.

    Examples:
        "Union Cement Company Abu Dhabi" → {"city": "Abu Dhabi"}
        "Hamriyah Steel" → {"industrial_zone": "hamriyah"}
        "Fujairah Cement Industries" → {"city": "Fujairah"}

    Args:
        facility_name: Name of facility
        country_iso3: Country code

    Returns:
        Dict with location hints (city, region, industrial_zone, etc.)
    """
    hints = {}
    name_lower = facility_name.lower()

    # Check for industrial zones
    for zone_name, zone_data in INDUSTRIAL_ZONES.items():
        if zone_data['country'] == country_iso3 and zone_name in name_lower:
            hints['industrial_zone'] = zone_name
            hints['city'] = zone_data['city']
            return hints

    # UAE cities
    if country_iso3 == "ARE":
        uae_cities = [
            "Abu Dhabi", "Dubai", "Sharjah", "Ajman", "Fujairah",
            "Ras Al Khaimah", "Umm Al Quwain"
        ]
        for city in uae_cities:
            if city.lower() in name_lower:
                hints['city'] = city
                return hints

    # Extract parenthetical location hints
    paren_match = re.search(r'\(([^)]+)\)', facility_name)
    if paren_match:
        location = paren_match.group(1)
        # Check if it's a city/location (not a company abbreviation)
        if len(location.split()) <= 3 and not location.isupper():
            hints['city'] = location

    return hints


def geocode_via_nominatim(
    query: str,
    country_iso3: str,
    timeout: int = 10
) -> Optional[GeocodingResult]:
    """
    Geocode using Nominatim (OpenStreetMap) API.

    Rate-limited to 1 request per second per Nominatim usage policy.

    Args:
        query: Search query (facility name, city, etc.)
        country_iso3: Country code to constrain search
        timeout: Request timeout in seconds

    Returns:
        GeocodingResult or None if geocoding failed
    """
    global LAST_REQUEST_TIME

    try:
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    except ImportError:
        logger.warning("geopy not installed. Install with: pip install geopy")
        return None

    # Rate limiting
    elapsed = time.time() - LAST_REQUEST_TIME
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)

    try:
        geolocator = Nominatim(user_agent="facilities_geocoding/1.0")

        # Try with country constraint
        location = geolocator.geocode(
            query,
            country_codes=country_iso3.lower(),
            timeout=timeout,
            exactly_one=True
        )

        LAST_REQUEST_TIME = time.time()

        if location:
            # Determine precision based on location type
            precision = 'city'  # Default
            if hasattr(location, 'raw'):
                location_type = location.raw.get('type', '')
                if location_type in ['industrial', 'factory', 'commercial']:
                    precision = 'site'
                elif location_type in ['city', 'town', 'village']:
                    precision = 'city'
                elif location_type in ['state', 'region']:
                    precision = 'region'
                elif location_type == 'country':
                    precision = 'country'

            return GeocodingResult(
                lat=location.latitude,
                lon=location.longitude,
                precision=precision,
                source='nominatim',
                confidence=0.7  # Moderate confidence for automated geocoding
            )

    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logger.warning(f"Nominatim geocoding failed: {e}")
        LAST_REQUEST_TIME = time.time()

    return None


def geocode_industrial_zone(zone_name: str, country_iso3: str) -> Optional[GeocodingResult]:
    """
    Look up coordinates from industrial zone database.

    Args:
        zone_name: Industrial zone identifier
        country_iso3: Country code

    Returns:
        GeocodingResult or None
    """
    zone_data = INDUSTRIAL_ZONES.get(zone_name.lower())

    if zone_data and zone_data['country'] == country_iso3:
        return GeocodingResult(
            lat=zone_data['lat'],
            lon=zone_data['lon'],
            precision='region',  # Industrial zone is region-level precision
            source='industrial_zone_db',
            confidence=0.8
        )

    return None


def prompt_for_coordinates(
    facility_name: str,
    country_iso3: str,
    country_name: str
) -> Optional[GeocodingResult]:
    """
    Interactively prompt user for coordinates.

    Args:
        facility_name: Name of facility
        country_iso3: Country code
        country_name: Country name

    Returns:
        GeocodingResult from user input or None if skipped
    """
    print(f"\n{'='*60}")
    print(f"GEOCODING REQUIRED: {facility_name}")
    print(f"Country: {country_name} ({country_iso3})")
    print(f"{'='*60}")
    print("\nOptions:")
    print("  1. Enter coordinates (lat, lon)")
    print("  2. Enter city/location (will geocode)")
    print("  3. Skip (leave coordinates empty)")

    choice = input("\nChoice [1/2/3]: ").strip()

    if choice == "1":
        try:
            lat_str = input("Latitude: ").strip()
            lon_str = input("Longitude: ").strip()
            lat = float(lat_str)
            lon = float(lon_str)

            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                print("Invalid coordinates. Skipping.")
                return None

            precision = input("Precision [site/city/region/country]: ").strip() or "site"

            return GeocodingResult(
                lat=lat,
                lon=lon,
                precision=precision,
                source='user_input',
                confidence=1.0  # User-provided is highest confidence
            )
        except ValueError:
            print("Invalid input. Skipping.")
            return None

    elif choice == "2":
        location = input("City/Location: ").strip()
        if location:
            result = geocode_via_nominatim(location, country_iso3)
            if result:
                print(f"Found: {result.lat}, {result.lon} (precision: {result.precision})")
                confirm = input("Use these coordinates? [y/n]: ").strip().lower()
                if confirm == 'y':
                    return result
            else:
                print("Geocoding failed.")
        return None

    else:
        print("Skipping geocoding.")
        return None


def geocode_facility(
    facility_name: str,
    country_iso3: str,
    country_name: Optional[str] = None,
    city: Optional[str] = None,
    interactive: bool = False,
    use_nominatim: bool = True
) -> GeocodingResult:
    """
    Geocode a facility using multiple fallback strategies.

    Tries in order:
    1. Industrial zone lookup (if zone detected in name)
    2. Nominatim with city (if city provided)
    3. Nominatim with facility name + country
    4. Interactive prompting (if enabled)
    5. Returns unknown location

    Args:
        facility_name: Name of facility
        country_iso3: Country code
        country_name: Country name (for display)
        city: City name (optional, speeds up geocoding)
        interactive: Enable interactive prompting for failures
        use_nominatim: Enable Nominatim API (disable for offline use)

    Returns:
        GeocodingResult (may have null coordinates if all methods fail)
    """
    # Extract location hints from facility name
    hints = extract_location_hints(facility_name, country_iso3)

    # Strategy 1: Industrial zone lookup
    if 'industrial_zone' in hints:
        result = geocode_industrial_zone(hints['industrial_zone'], country_iso3)
        if result:
            logger.info(f"Geocoded via industrial zone: {facility_name} → {result.lat}, {result.lon}")
            return result

    # Strategy 2: Nominatim with city
    if use_nominatim and (city or hints.get('city')):
        search_city = city or hints['city']
        query = f"{search_city}, {country_iso3}"
        result = geocode_via_nominatim(query, country_iso3)
        if result:
            logger.info(f"Geocoded via city: {facility_name} → {result.lat}, {result.lon}")
            return result

    # Strategy 3: Nominatim with facility name
    if use_nominatim:
        result = geocode_via_nominatim(facility_name, country_iso3)
        if result:
            logger.info(f"Geocoded via facility name: {facility_name} → {result.lat}, {result.lon}")
            return result

    # Strategy 4: Interactive prompting
    if interactive:
        result = prompt_for_coordinates(facility_name, country_iso3, country_name or country_iso3)
        if result:
            return result

    # Strategy 5: Return unknown location
    logger.warning(f"Could not geocode: {facility_name} ({country_iso3})")
    return GeocodingResult(
        lat=None,
        lon=None,
        precision='unknown',
        source='none',
        confidence=0.0
    )


def batch_geocode_facilities(
    facilities: list,
    country_iso3: str,
    interactive: bool = False,
    use_nominatim: bool = True,
    delay: float = 1.0
) -> Dict[str, GeocodingResult]:
    """
    Batch geocode multiple facilities.

    Args:
        facilities: List of facility dicts with 'name' and optionally 'city'
        country_iso3: Country code
        interactive: Enable interactive prompting
        use_nominatim: Enable Nominatim API
        delay: Delay between requests (seconds)

    Returns:
        Dict mapping facility name → GeocodingResult
    """
    results = {}

    for i, facility in enumerate(facilities):
        name = facility['name']
        city = facility.get('city')

        logger.info(f"Geocoding {i+1}/{len(facilities)}: {name}")

        result = geocode_facility(
            facility_name=name,
            country_iso3=country_iso3,
            city=city,
            interactive=interactive,
            use_nominatim=use_nominatim
        )

        results[name] = result

        # Rate limiting
        if i < len(facilities) - 1 and use_nominatim:
            time.sleep(delay)

    return results
