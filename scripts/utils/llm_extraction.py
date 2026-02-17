"""
LLM-based coordinate and facility data extraction.

Consolidates extraction logic from:
- scripts/tools/geocode_null_island.py
- scripts/enrich_facilities.py

Usage:
    from utils.llm_extraction import extract_coordinates, calculate_from_reference

    result = extract_coordinates(
        facility_name="Karee Mine",
        country="South Africa",
        search_results=search_results,
        client=openai_client
    )
"""

import json
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Result from LLM coordinate extraction."""
    found: bool
    lat: Optional[float] = None
    lon: Optional[float] = None
    reference_town: Optional[str] = None
    distance_km: Optional[float] = None
    direction: Optional[str] = None
    province: Optional[str] = None
    source_url: Optional[str] = None
    confidence: float = 0.0
    notes: Optional[str] = None
    is_real_facility: bool = True
    status: Optional[str] = None
    operators: List[str] = None
    owners: List[str] = None
    company_notes: Optional[str] = None

    def __post_init__(self):
        if self.operators is None:
            self.operators = []
        if self.owners is None:
            self.owners = []

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExtractionResult':
        """Create from LLM response dict."""
        companies = data.get('companies', {})
        return cls(
            found=data.get('found', False),
            lat=data.get('lat'),
            lon=data.get('lon'),
            reference_town=data.get('reference_town'),
            distance_km=data.get('distance_km'),
            direction=data.get('direction'),
            province=data.get('province'),
            source_url=data.get('source_url'),
            confidence=data.get('confidence', 0.0),
            notes=data.get('notes'),
            is_real_facility=data.get('is_real_facility', True),
            status=data.get('status'),
            operators=companies.get('operators', []),
            owners=companies.get('owners', []),
            company_notes=companies.get('notes'),
        )


def extract_coordinates(
    facility_name: str,
    country: str,
    search_results: List[Dict],
    client,  # OpenAI client
    commodities: Optional[List[str]] = None,
    extract_companies: bool = True,
    extract_status: bool = True,
    model: str = "gpt-4o-mini"
) -> Optional[ExtractionResult]:
    """
    Use LLM to extract coordinates and facility data from search results.

    Args:
        facility_name: Name of the facility
        country: Country where facility is located
        search_results: List of web search results with 'title', 'url', 'content'
        client: OpenAI client instance
        commodities: Optional list of commodities produced
        extract_companies: Whether to extract company information
        extract_status: Whether to extract operational status
        model: LLM model to use (default: gpt-4o-mini for speed/cost)

    Returns:
        ExtractionResult with coordinates and optional company/status info,
        or None if extraction fails
    """
    if not search_results:
        return None

    # Format search results for LLM
    results_text = ""
    for i, result in enumerate(search_results[:8], 1):
        results_text += f"\n--- Result {i} ---\n"
        results_text += f"Title: {result.get('title', 'N/A')}\n"
        results_text += f"URL: {result.get('url', 'N/A')}\n"
        content = result.get('content', 'N/A')
        # Truncate long content
        if len(content) > 1200:
            content = content[:1200] + "..."
        results_text += f"Content: {content}\n"

    commodity_str = ", ".join(commodities[:3]) if commodities else "unknown"

    # Build the extraction schema
    extraction_schema = """
{
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
    "is_real_facility": true/false (false if this is a placeholder or category, not a real location)"""

    if extract_status:
        extraction_schema += """,
    "status": "operating|closed|care_and_maintenance|development|unknown\""""

    if extract_companies:
        extraction_schema += """,
    "companies": {
        "operators": ["Company Name 1", "Company Name 2"],
        "owners": ["Owner Company 1", "Owner Company 2"],
        "notes": "Any context about ownership/operation"
    }"""

    extraction_schema += "\n}"

    # Build instructions
    instructions = """
COORDINATE RULES:
- South latitudes are NEGATIVE (e.g., South Africa: -26.0)
- West longitudes are NEGATIVE (e.g., Brazil: -47.0, USA: -110.0)
- East longitudes are POSITIVE (e.g., China: 116.0, Australia: 145.0)
- Parse DMS format: 26d30'S = -26.5, 47d15'W = -47.25
- If only relative location (e.g., "50km NE of Lagos"), provide reference_town + distance + direction
- Confidence: direct coords = 0.9, calculated from reference = 0.7, general area = 0.5

VALIDITY CHECK:
- Set is_real_facility=false if this appears to be a category (e.g., "Various Mines", "All Facilities")
- Set is_real_facility=false if no specific location can be identified
- Set found=false if you cannot determine any coordinates"""

    if extract_companies:
        instructions += """

COMPANY RULES:
- Extract ALL company names mentioned as operators, owners, or developers
- Include full legal names (e.g., "BHP Billiton Ltd" not just "BHP")
- Include joint venture partners separately
- Include parent companies if mentioned
- Use "operators" for companies actively running the facility
- Use "owners" for companies that own equity/assets"""

    if extract_status:
        instructions += """

STATUS RULES:
- "operating" = currently producing/active
- "closed" = permanently shut down or abandoned
- "care_and_maintenance" = temporarily suspended but maintained
- "development" = under construction or planned
- "unknown" = no clear status information"""

    prompt = f"""Extract location information for this mining facility from the search results.

FACILITY: {facility_name}
COUNTRY: {country}
COMMODITIES: {commodity_str}

SEARCH RESULTS:
{results_text}

Return JSON with:
{extraction_schema}
{instructions}"""

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at extracting geographic coordinates from mining reports and technical documents. Be precise with coordinate signs (negative for South/West)."
                },
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        data = json.loads(response.choices[0].message.content)
        return ExtractionResult.from_dict(data)
    except Exception as e:
        logger.error(f"LLM extraction error: {e}")
        return None


def calculate_from_reference(
    reference_town: str,
    country: str,
    distance_km: float,
    direction: str
) -> Optional[Tuple[float, float]]:
    """
    Calculate coordinates from a reference point using bearing and distance.

    Args:
        reference_town: Name of the reference town/city
        country: Country name for geocoding context
        distance_km: Distance from reference point in kilometers
        direction: Cardinal/intercardinal direction (N, NE, E, SE, S, SW, W, NW, etc.)

    Returns:
        Tuple of (latitude, longitude) or None if calculation fails
    """
    try:
        from geopy.geocoders import Nominatim
        from geopy.distance import distance as geopy_distance
    except ImportError:
        logger.warning("geopy not installed - can't calculate from reference")
        return None

    geolocator = Nominatim(user_agent="gsmc-facility-geocoder")
    time.sleep(1.1)  # Nominatim rate limit

    try:
        location = geolocator.geocode(f"{reference_town}, {country}", timeout=10)
        if not location:
            logger.warning(f"Could not geocode reference town: {reference_town}")
            return None

        ref_lat, ref_lon = location.latitude, location.longitude

        # Direction to bearing mapping
        bearings = {
            "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
            "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
            "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
            "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5
        }

        bearing = bearings.get(direction.upper())
        if bearing is None:
            logger.warning(f"Unknown direction: {direction}")
            return None

        origin = (ref_lat, ref_lon)
        destination = geopy_distance(kilometers=distance_km).destination(origin, bearing)

        return (destination.latitude, destination.longitude)
    except Exception as e:
        logger.warning(f"Error calculating from reference: {e}")
        return None


def resolve_extraction_coordinates(
    result: ExtractionResult,
    country: str
) -> Optional[Tuple[float, float]]:
    """
    Resolve final coordinates from an extraction result.

    If direct coordinates are available, return those.
    If only reference location is available, calculate from reference.

    Args:
        result: ExtractionResult from extract_coordinates()
        country: Country name for reference calculation

    Returns:
        Tuple of (latitude, longitude) or None
    """
    if not result or not result.found:
        return None

    # Direct coordinates available
    if result.lat is not None and result.lon is not None:
        return (result.lat, result.lon)

    # Calculate from reference
    if result.reference_town and result.distance_km and result.direction:
        return calculate_from_reference(
            result.reference_town,
            country,
            result.distance_km,
            result.direction
        )

    return None
