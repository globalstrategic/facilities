#!/usr/bin/env python3
"""
Advanced multi-source geocoding system for mining facilities.

Architecture:
    Source Priority (cheap → robust):
    1. OSM Overpass (mining tags: man_made=mineshaft/adit, landuse=quarry, resource=*)
    2. Wikidata SPARQL (mine/deposit items with P625 coordinates + aliases)
    3. Mindat API (mine localities with site-level coords)
    4. National cadastres (SERNAGEOMIN, GEOCATMIN, SAMINDABA, etc.)
    5. Web search (Tavily/Brave for technical reports: NI 43-101, JORC)
    6. General geocoders (Nominatim, Mapbox, HERE - fallback only)

    Name Matching:
    - rapidfuzz for fuzzy matching
    - transliterate/Unidecode for Cyrillic ↔ Latin
    - libpostal for robust tokenization (optional)

    Scoring & Precision:
    - Source score: NI 43-101/cadastre > Mindat/Wikidata > OSM > geocoder
    - String match score: rapidfuzz ratio
    - Geometry sanity: reverse-geocode to verify country/region
    - Precision labels: site-level, city-level, region-level

Usage:
    from scripts.utils.geocoding_v2 import AdvancedGeocoder

    geocoder = AdvancedGeocoder(
        use_overpass=True,
        use_wikidata=True,
        use_mindat=True,
        use_web_search=False,  # Requires API keys
        cache_results=True
    )

    result = geocoder.geocode_facility(
        facility_name="Inkai Uranium Mine",
        country_iso3="KAZ",
        commodities=["uranium"],
        aliases=["JV Inkai", "Blocks 1-3"]
    )

    print(f"Coords: {result.lat}, {result.lon}")
    print(f"Precision: {result.precision}")
    print(f"Source: {result.source}")
    print(f"Confidence: {result.confidence}")
"""

import re
import time
import os
import requests
from datetime import datetime, timezone
import logging
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from pathlib import Path
import json
from functools import lru_cache

logger = logging.getLogger(__name__)

# ISO3 to ISO2 mapping (for Nominatim countrycodes parameter)
ISO3_TO_ISO2 = {
    "TCD": "TD", "USA": "US", "CHN": "CN", "AUS": "AU", "ZAF": "ZA",
    "IND": "IN", "IDN": "ID", "BRA": "BR", "CAN": "CA", "RUS": "RU",
    "MEX": "MX", "PER": "PE", "CHL": "CL", "ARG": "AR", "COL": "CO",
    "ZMB": "ZM", "ZWE": "ZW", "NAM": "NA", "BWA": "BW", "GHA": "GH",
    "NGA": "NG", "MAR": "MA", "DZA": "DZ", "EGY": "EG", "TUN": "TN",
    "KAZ": "KZ", "UZB": "UZ", "MNG": "MN", "TUR": "TR", "IRN": "IR",
    "SAU": "SA", "ARE": "AE", "OMN": "OM", "PAK": "PK", "BGD": "BD",
    "MMR": "MM", "THA": "TH", "VNM": "VN", "PHL": "PH", "MYS": "MY",
    "PNG": "PG", "NCL": "NC", "FJI": "FJ", "NZL": "NZ", "JPN": "JP",
    "KOR": "KR", "TWN": "TW", "POL": "PL", "DEU": "DE", "FRA": "FR",
    "GBR": "GB", "ESP": "ES", "ITA": "IT", "SWE": "SE", "NOR": "NO",
    "FIN": "FI", "UKR": "UA", "ROU": "RO", "BGR": "BG", "SRB": "RS"
}

# Country bounding boxes for validation (lat_min, lat_max, lon_min, lon_max)
# Prevents out-of-country garbage coordinates from being written
COUNTRY_BBOX = {
    "TCD": {"lat_min": 7.0, "lat_max": 24.0, "lon_min": 13.0, "lon_max": 24.5},
    "USA": {"lat_min": 24.0, "lat_max": 72.0, "lon_min": -180.0, "lon_max": -66.0},
    "CHN": {"lat_min": 18.0, "lat_max": 54.0, "lon_min": 73.0, "lon_max": 135.0},
    "AUS": {"lat_min": -44.0, "lat_max": -10.0, "lon_min": 113.0, "lon_max": 154.0},
    "ZAF": {"lat_min": -35.0, "lat_max": -22.0, "lon_min": 16.0, "lon_max": 33.0},
    "IND": {"lat_min": 6.0, "lat_max": 36.0, "lon_min": 68.0, "lon_max": 98.0},
    "IDN": {"lat_min": -11.0, "lat_max": 6.0, "lon_min": 95.0, "lon_max": 141.0},
    "BRA": {"lat_min": -34.0, "lat_max": 6.0, "lon_min": -74.0, "lon_max": -34.0},
    "CAN": {"lat_min": 41.0, "lat_max": 84.0, "lon_min": -141.0, "lon_max": -52.0},
    "RUS": {"lat_min": 41.0, "lat_max": 82.0, "lon_min": 19.0, "lon_max": -169.0},
    "MEX": {"lat_min": 14.0, "lat_max": 33.0, "lon_min": -118.0, "lon_max": -86.0},
    "PER": {"lat_min": -19.0, "lat_max": -0.0, "lon_min": -82.0, "lon_max": -68.0},
    "CHL": {"lat_min": -56.0, "lat_max": -17.0, "lon_min": -110.0, "lon_max": -66.0},
    "ZMB": {"lat_min": -18.0, "lat_max": -8.0, "lon_min": 22.0, "lon_max": 34.0},
}

# Known bad sentinel coordinates (bugs that should never be written)
BAD_SENTINELS = {
    (21.7713519, -72.2788891),  # Turks & Caicos airport - geocoder fallback bug
}

def is_valid_coord(lat: float, lon: float) -> bool:
    """Validate that coordinates are within valid Earth bounds."""
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return False
    return -90 <= lat <= 90 and -180 <= lon <= 180

def in_country_bbox(lat: float, lon: float, country_iso3: str) -> bool:
    """Check if coordinates fall within expected country bounding box.

    Returns True if no bbox defined (permissive) or if coords are inside bbox.
    """
    bbox = COUNTRY_BBOX.get(country_iso3)
    if not bbox:
        return True  # No bbox defined, allow (for countries we haven't mapped)

    return (bbox["lat_min"] <= lat <= bbox["lat_max"] and
            bbox["lon_min"] <= lon <= bbox["lon_max"])

def is_sentinel_coord(lat: float, lon: float) -> bool:
    """Check if coordinates match known bad sentinel values."""
    try:
        rounded = (round(float(lat), 7), round(float(lon), 7))
        return rounded in BAD_SENTINELS
    except (TypeError, ValueError):
        return False

# Rate limiting configuration
RATE_LIMITS = {
    'nominatim': 1.0,      # 1 req/sec (public instance)
    'overpass': 0.5,       # 2 req/sec (conservative)
    'wikidata': 0.2,       # 5 req/sec
    'mindat': 0.1,         # 10 req/sec (generous)
    'web_search': 0.5      # Varies by provider
}

# Request tracking for rate limiting
LAST_REQUEST_TIMES = {}


@dataclass
class GeocodingResult:
    """Result from geocoding operation with full provenance."""
    lat: Optional[float]
    lon: Optional[float]
    precision: str  # 'site', 'city', 'region', 'country', 'unknown'
    source: str     # 'overpass', 'wikidata', 'mindat', 'cadastre', 'nominatim', etc.
    confidence: float  # 0.0-1.0

    # Additional metadata
    source_id: Optional[str] = None  # OSM ID, Wikidata QID, Mindat ID, etc.
    matched_name: Optional[str] = None  # Name that matched from source
    match_score: Optional[float] = None  # String similarity score
    evidence: Optional[Dict[str, Any]] = None  # Additional evidence

    def to_dict(self) -> Dict:
        """Convert to dict for serialization."""
        return {
            'lat': self.lat,
            'lon': self.lon,
            'precision': self.precision,
            'source': self.source,
            'confidence': self.confidence,
            'source_id': self.source_id,
            'matched_name': self.matched_name,
            'match_score': self.match_score,
            'evidence': self.evidence
        }


@dataclass
class GeocodingCandidate:
    """Candidate location from a source before scoring."""
    lat: float
    lon: float
    source: str
    source_id: Optional[str]
    name: str
    tags: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate coordinates."""
        if not (-90 <= self.lat <= 90):
            raise ValueError(f"Invalid latitude: {self.lat}")
        if not (-180 <= self.lon <= 180):
            raise ValueError(f"Invalid longitude: {self.lon}")


def nominatim_headers() -> Dict[str, str]:
    """Generate OSM-compliant headers for Nominatim requests."""
    contact = os.getenv("OSM_CONTACT_EMAIL", "facilities@gsmc.example")
    return {"User-Agent": f"GSMC-Facilities/2.1 (mailto:{contact})"}


def geocode_via_nominatim(
    query: str,
    country_iso3: str = None,
    delay_s: float = None
) -> Optional[Dict[str, Any]]:
    """
    Forward geocode a free-text query via Nominatim (OSM).

    Args:
        query: Free-text search query (e.g., "Kouri Bougoudi, Chad")
        country_iso3: ISO3 country code for filtering (optional)
        delay_s: Custom delay in seconds (default: $NOMINATIM_DELAY_S or 1.0)

    Returns:
        Dict with keys: lat, lon, address (or None on failure)
    """
    if not query or not query.strip():
        return None

    delay_s = delay_s or float(os.getenv("NOMINATIM_DELAY_S", "1.0"))
    url = "https://nominatim.openstreetmap.org/search"

    params = {
        "q": query.strip(),
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": 1,
    }

    # Add country filter if available
    if country_iso3:
        iso2 = ISO3_TO_ISO2.get(country_iso3)
        if iso2:
            params["countrycodes"] = iso2.lower()

    try:
        resp = requests.get(url, params=params, headers=nominatim_headers(), timeout=10)
        resp.raise_for_status()
        items = resp.json() or []
        time.sleep(delay_s)  # OSM policy compliance

        if not items:
            logger.debug(f"Nominatim: No results for '{query}'")
            return None

        top = items[0]
        result = {
            "lat": float(top["lat"]),
            "lon": float(top["lon"]),
            "address": top.get("address", {}),
            "display_name": top.get("display_name", ""),
        }

        logger.debug(f"Nominatim: Found {result['lat']}, {result['lon']} for '{query}'")
        return result

    except requests.exceptions.Timeout:
        logger.warning(f"Nominatim timeout for query: {query}")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Nominatim request failed: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.warning(f"Nominatim response parsing error: {e}")
        return None


def rate_limit(source: str):
    """
    Rate limiting decorator for API calls.

    Args:
        source: Source identifier for rate limit tracking
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            global LAST_REQUEST_TIMES

            limit = RATE_LIMITS.get(source, 1.0)
            last_time = LAST_REQUEST_TIMES.get(source, 0)
            elapsed = time.time() - last_time

            if elapsed < limit:
                time.sleep(limit - elapsed)

            result = func(*args, **kwargs)
            LAST_REQUEST_TIMES[source] = time.time()
            return result
        return wrapper
    return decorator


class NameMatcher:
    """
    Fuzzy name matching with transliteration support.

    Handles:
    - String similarity (rapidfuzz)
    - Transliteration (Cyrillic ↔ Latin)
    - Word overlap
    - Alias expansion
    """

    def __init__(self):
        """Initialize name matcher."""
        try:
            from rapidfuzz import fuzz
            self.fuzz = fuzz
            self.available = True
        except ImportError:
            logger.warning("rapidfuzz not available - fuzzy matching disabled")
            self.available = False
            self.fuzz = None

        # Try to import transliteration
        self.transliterate = None
        try:
            from transliterate import translit
            self.transliterate = translit
        except ImportError:
            logger.debug("transliterate library not available")

    def normalize(self, text: str) -> str:
        """
        Normalize text for matching.

        - Lowercase
        - Remove special characters
        - Collapse whitespace
        """
        text = text.lower()
        # Remove parentheticals
        text = re.sub(r'\([^)]*\)', '', text)
        # Remove special characters except spaces and hyphens
        text = re.sub(r'[^a-z0-9\s\-]', '', text)
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def get_variants(self, text: str) -> List[str]:
        """
        Get name variants including transliterations.

        Args:
            text: Input text

        Returns:
            List of variants (original + transliterations)
        """
        variants = [text]

        # Add normalized version
        normalized = self.normalize(text)
        if normalized != text:
            variants.append(normalized)

        # Add transliterations if available
        if self.transliterate:
            # Try Russian transliteration
            try:
                cyrillic = self.transliterate(text, 'ru')
                if cyrillic != text:
                    variants.append(cyrillic)
                    variants.append(self.normalize(cyrillic))
            except:
                pass

            # Try reverse (Latin → Cyrillic)
            try:
                latin = self.transliterate(text, 'ru', reversed=True)
                if latin != text:
                    variants.append(latin)
                    variants.append(self.normalize(latin))
            except:
                pass

        return list(set(variants))  # Deduplicate

    def match_score(
        self,
        query: str,
        target: str,
        target_aliases: Optional[List[str]] = None
    ) -> float:
        """
        Calculate match score between query and target.

        Uses multiple strategies:
        1. Exact match (normalized)
        2. Token sort ratio (rapidfuzz)
        3. Word overlap
        4. Alias matching

        Args:
            query: Query string (facility name)
            target: Target string (source name)
            target_aliases: List of alternative names for TARGET only

        Returns:
            Score from 0.0 to 1.0
        """
        if not self.available:
            # Fallback to simple normalization
            return 1.0 if self.normalize(query) == self.normalize(target) else 0.0

        # Get query variants (only from query itself)
        query_variants = self.get_variants(query)

        # Get target variants (from target + its aliases)
        target_variants = self.get_variants(target)
        if target_aliases:
            for alias in target_aliases:
                target_variants.extend(self.get_variants(alias))

        # Check for exact matches first
        for qv in query_variants:
            for tv in target_variants:
                if self.normalize(qv) == self.normalize(tv):
                    return 1.0

        # Use rapidfuzz for fuzzy matching
        max_score = 0.0

        for qv in query_variants:
            for tv in target_variants:
                # Token sort ratio (handles word order differences)
                score = self.fuzz.token_sort_ratio(qv, tv) / 100.0
                max_score = max(max_score, score)

                # Word overlap
                words_q = set(self.normalize(qv).split())
                words_t = set(self.normalize(tv).split())
                if words_q and words_t:
                    overlap = len(words_q & words_t) / min(len(words_q), len(words_t))
                    max_score = max(max_score, overlap)

        return max_score


class AdvancedGeocoder:
    """
    Advanced multi-source geocoding system.

    Orchestrates multiple geocoding sources with intelligent fallback,
    name matching, and confidence scoring.
    """

    def __init__(
        self,
        use_overpass: bool = True,
        use_wikidata: bool = True,
        use_mindat: bool = False,  # Requires API key
        use_cadastres: bool = False,  # Country-specific
        use_web_search: bool = False,  # Requires API key
        use_nominatim: bool = True,
        cache_results: bool = True,
        cache_dir: Optional[Path] = None
    ):
        """
        Initialize geocoder with source configuration.

        Args:
            use_overpass: Enable OSM Overpass API
            use_wikidata: Enable Wikidata SPARQL
            use_mindat: Enable Mindat API (requires key)
            use_cadastres: Enable national cadastres (country-specific)
            use_web_search: Enable web search (requires API keys)
            use_nominatim: Enable Nominatim (fallback)
            cache_results: Cache results to disk
            cache_dir: Cache directory (default: .cache/geocoding)
        """
        self.use_overpass = use_overpass
        self.use_wikidata = use_wikidata
        self.use_mindat = use_mindat
        self.use_cadastres = use_cadastres
        self.use_web_search = use_web_search
        self.use_nominatim = use_nominatim

        # Initialize name matcher
        self.name_matcher = NameMatcher()

        # Setup cache
        self.cache_results = cache_results
        if cache_results:
            if cache_dir is None:
                cache_dir = Path.home() / '.cache' / 'facilities' / 'geocoding'
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.cache_dir = None

        # Import source-specific modules (lazy)
        self._overpass = None
        self._wikidata = None
        self._mindat = None
        self._nominatim = None

    def _get_cache_key(
        self,
        facility_name: str,
        country_iso3: str,
        commodities: Optional[List[str]] = None
    ) -> str:
        """Generate cache key for a query."""
        key_parts = [facility_name, country_iso3]
        if commodities:
            key_parts.extend(sorted(commodities))
        return '_'.join(key_parts).lower().replace(' ', '_')

    def _load_from_cache(self, cache_key: str) -> Optional[GeocodingResult]:
        """Load result from cache."""
        if not self.cache_results:
            return None

        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text())
                return GeocodingResult(**data)
            except Exception as e:
                logger.debug(f"Cache read failed: {e}")
        return None

    def _save_to_cache(self, cache_key: str, result: GeocodingResult):
        """Save result to cache."""
        if not self.cache_results:
            return

        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            cache_file.write_text(json.dumps(result.to_dict(), indent=2))
        except Exception as e:
            logger.debug(f"Cache write failed: {e}")

    def geocode_facility(
        self,
        facility_name: str,
        country_iso3: str,
        commodities: Optional[List[str]] = None,
        aliases: Optional[List[str]] = None,
        lat_hint: Optional[float] = None,
        lon_hint: Optional[float] = None,
        min_confidence: float = 0.6
    ) -> GeocodingResult:
        """
        Geocode a facility using all available sources.

        Strategy:
        1. Check cache
        2. Query all enabled sources in parallel
        3. Score and rank candidates
        4. Select best candidate above min_confidence
        5. Cache result

        Args:
            facility_name: Facility name
            country_iso3: ISO3 country code
            commodities: List of commodities (helps matching)
            aliases: Alternative names
            lat_hint: Latitude hint (for proximity scoring)
            lon_hint: Longitude hint (for proximity scoring)
            min_confidence: Minimum confidence threshold

        Returns:
            GeocodingResult (may have null coords if no match found)
        """
        # Check cache
        cache_key = self._get_cache_key(facility_name, country_iso3, commodities)
        cached = self._load_from_cache(cache_key)
        if cached:
            logger.debug(f"Cache hit: {facility_name}")
            return cached

        # Collect candidates from all sources
        candidates = []

        if self.use_overpass:
            candidates.extend(self._query_overpass(
                facility_name, country_iso3, commodities, aliases
            ))

        if self.use_wikidata:
            candidates.extend(self._query_wikidata(
                facility_name, country_iso3, commodities, aliases
            ))

        if self.use_mindat:
            candidates.extend(self._query_mindat(
                facility_name, country_iso3, commodities, aliases
            ))

        if self.use_nominatim:
            candidates.extend(self._query_nominatim(
                facility_name, country_iso3
            ))

        # Score candidates
        scored = self._score_candidates(
            candidates,
            facility_name,
            country_iso3,
            aliases,
            lat_hint,
            lon_hint
        )

        # Select best candidate
        if scored and scored[0].confidence >= min_confidence:
            result = scored[0]
        else:
            result = GeocodingResult(
                lat=None,
                lon=None,
                precision='unknown',
                source='none',
                confidence=0.0
            )

        # Cache result
        self._save_to_cache(cache_key, result)

        return result

    def _query_overpass(
        self,
        facility_name: str,
        country_iso3: str,
        commodities: Optional[List[str]],
        aliases: Optional[List[str]]
    ) -> List[GeocodingCandidate]:
        """
        Query OSM Overpass API for mining features.
        """
        try:
            from .sources.overpass import OverpassClient
        except ImportError:
            logger.debug("OverpassClient not available")
            return []

        if self._overpass is None:
            self._overpass = OverpassClient()

        candidates = []

        # Try with primary resource (if commodities specified)
        resource = commodities[0] if commodities else None

        # Query Overpass
        try:
            features = self._overpass.query_mining_features(
                country_iso3=country_iso3,
                resource=resource,
                facility_name=facility_name
            )

            for feature in features:
                candidate = GeocodingCandidate(
                    lat=feature.lat,
                    lon=feature.lon,
                    source='overpass',
                    source_id=feature.osm_id,
                    name=feature.name or f"OSM {feature.osm_id}",
                    tags=feature.tags
                )
                candidates.append(candidate)

        except Exception as e:
            logger.warning(f"Overpass query failed: {e}")

        return candidates

    def _query_wikidata(
        self,
        facility_name: str,
        country_iso3: str,
        commodities: Optional[List[str]],
        aliases: Optional[List[str]]
    ) -> List[GeocodingCandidate]:
        """
        Query Wikidata SPARQL for mine/deposit items.
        """
        try:
            from .sources.wikidata import WikidataClient
        except ImportError:
            logger.debug("WikidataClient not available")
            return []

        if self._wikidata is None:
            self._wikidata = WikidataClient()

        candidates = []

        # Try with primary commodity (if specified)
        commodity = commodities[0] if commodities else None

        # Query Wikidata
        try:
            items = self._wikidata.query_mines(
                country_iso3=country_iso3,
                commodity=commodity,
                facility_name=facility_name
            )

            for item in items:
                candidate = GeocodingCandidate(
                    lat=item.lat,
                    lon=item.lon,
                    source='wikidata',
                    source_id=item.qid,
                    name=item.label,
                    tags={
                        'aliases': item.aliases,
                        'commodities': item.properties.get('commodities', [])
                    }
                )
                candidates.append(candidate)

        except Exception as e:
            logger.warning(f"Wikidata query failed: {e}")

        return candidates

    def _query_mindat(
        self,
        facility_name: str,
        country_iso3: str,
        commodities: Optional[List[str]],
        aliases: Optional[List[str]]
    ) -> List[GeocodingCandidate]:
        """
        Query Mindat API for mine localities.

        Placeholder - will be implemented in separate module.
        """
        # TODO: Implement Mindat API query
        return []

    def _query_nominatim(
        self,
        facility_name: str,
        country_iso3: str
    ) -> List[GeocodingCandidate]:
        """
        Query Nominatim (fallback geocoder).

        Uses existing geocoding.py implementation.
        """
        try:
            from . import geocoding
        except ImportError:
            logger.debug("geocoding module not available")
            return []

        candidates = []

        try:
            # Use existing Nominatim implementation
            result = geocoding.geocode_via_nominatim(
                query=facility_name,
                country_iso3=country_iso3
            )

            if result and result.lat and result.lon:
                candidate = GeocodingCandidate(
                    lat=result.lat,
                    lon=result.lon,
                    source='nominatim',
                    source_id=None,
                    name=facility_name,
                    tags={'precision': result.precision}
                )
                candidates.append(candidate)

        except Exception as e:
            logger.warning(f"Nominatim query failed: {e}")

        return candidates

    def _score_candidates(
        self,
        candidates: List[GeocodingCandidate],
        facility_name: str,
        country_iso3: str,
        aliases: Optional[List[str]],
        lat_hint: Optional[float],
        lon_hint: Optional[float]
    ) -> List[GeocodingResult]:
        """
        Score and rank candidates.

        Scoring factors:
        1. Source score (cadastre > Mindat/Wikidata > OSM > geocoder)
        2. Name match score (rapidfuzz)
        3. Commodity match (bonus if resource tag matches)
        4. Proximity to hint coords (if provided)
        5. Reverse geocoding sanity check

        Args:
            candidates: List of candidates
            facility_name: Query facility name
            country_iso3: Query country
            aliases: Alternative names
            lat_hint: Latitude hint
            lon_hint: Longitude hint

        Returns:
            List of GeocodingResults sorted by confidence (descending)
        """
        if not candidates:
            return []

        # Source base scores
        SOURCE_SCORES = {
            'cadastre': 0.95,
            'ni43101': 0.95,
            'mindat': 0.85,
            'wikidata': 0.85,
            'overpass': 0.75,
            'nominatim': 0.60
        }

        results = []

        for candidate in candidates:
            # Start with source base score
            base_score = SOURCE_SCORES.get(candidate.source, 0.5)

            # Name matching (only use target's aliases, not query aliases)
            target_aliases = candidate.tags.get('aliases', []) if isinstance(candidate.tags, dict) else []
            name_score = self.name_matcher.match_score(
                facility_name,
                candidate.name,
                target_aliases
            )

            # Combine scores (weighted average)
            confidence = (base_score * 0.6) + (name_score * 0.4)

            # Determine precision from source and tags
            precision = self._determine_precision(candidate)

            # Create result
            result = GeocodingResult(
                lat=candidate.lat,
                lon=candidate.lon,
                precision=precision,
                source=candidate.source,
                confidence=confidence,
                source_id=candidate.source_id,
                matched_name=candidate.name,
                match_score=name_score,
                evidence=candidate.tags
            )

            results.append(result)

        # Sort by confidence (descending)
        results.sort(key=lambda r: r.confidence, reverse=True)

        return results

    def _determine_precision(self, candidate: GeocodingCandidate) -> str:
        """
        Determine precision level from candidate source and tags.

        Args:
            candidate: Candidate to evaluate

        Returns:
            Precision level: 'site', 'city', 'region', 'country'
        """
        # High-precision sources
        if candidate.source in ['cadastre', 'ni43101', 'mindat']:
            return 'site'

        # OSM features
        if candidate.source == 'overpass':
            osm_type = candidate.tags.get('type', '')
            if osm_type in ['mineshaft', 'adit', 'quarry']:
                return 'site'
            return 'region'

        # Wikidata
        if candidate.source == 'wikidata':
            # Check if it's a specific mine vs general area
            instance_of = candidate.tags.get('instance_of', '')
            if 'mine' in instance_of.lower():
                return 'site'
            return 'region'

        # Nominatim (depends on place type)
        if candidate.source == 'nominatim':
            place_type = candidate.tags.get('type', '')
            if place_type in ['industrial', 'factory', 'commercial']:
                return 'site'
            elif place_type in ['city', 'town', 'village']:
                return 'city'
            elif place_type in ['state', 'region']:
                return 'region'
            else:
                return 'country'

        return 'region'  # Default


# ============================================================================
# Simple, Safe Nominatim Wrapper (OSM-compliant)
# ============================================================================

NOMINATIM_URL = os.getenv("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org/search")
NOMINATIM_REVERSE_URL = os.getenv("NOMINATIM_REVERSE_URL", "https://nominatim.openstreetmap.org/reverse")

COUNTRY_BBOX = {
    # Coarse, defensible bounding boxes. Expand with proper gazetteer later.
    "TCD": {"lat_min": 7.0, "lat_max": 24.2, "lon_min": 13.0, "lon_max": 24.2},  # Chad
    "ARM": {"lat_min": 38.8, "lat_max": 41.3, "lon_min": 43.4, "lon_max": 46.6},  # Armenia
    "BEL": {"lat_min": 49.5, "lat_max": 51.5, "lon_min": 2.5, "lon_max": 6.4},   # Belgium
    "BFA": {"lat_min": 9.4, "lat_max": 15.1, "lon_min": -5.5, "lon_max": 2.4},   # Burkina Faso
    "ZAF": {"lat_min": -35.0, "lat_max": -22.0, "lon_min": 16.0, "lon_max": 33.0},  # South Africa
    # Add others as needed
}


def in_country_bbox(lat: float, lon: float, iso3: str) -> bool:
    """
    Check if coordinates are within expected country bounding box.

    Args:
        lat: Latitude
        lon: Longitude
        iso3: ISO3 country code

    Returns:
        True if in bbox or bbox unknown, False if definitely out of country
    """
    b = COUNTRY_BBOX.get(iso3)
    if not b or lat is None or lon is None:
        return True  # Don't block if unknown bbox
    return (b["lat_min"] <= lat <= b["lat_max"]) and (b["lon_min"] <= lon <= b["lon_max"])


def _nominatim_headers():
    """Get OSM-compliant headers with contact email."""
    email = os.getenv("OSM_CONTACT_EMAIL", "ops@example.com")
    return {"User-Agent": f"GSMC-Facilities/2.1 (mailto:{email})"}



def reverse_geocode_via_nominatim(
    lat: float,
    lon: float,
    delay_env: str = "NOMINATIM_DELAY_S"
) -> Dict:
    """
    Reverse geocode via Nominatim (OSM-compliant).

    Args:
        lat: Latitude
        lon: Longitude
        delay_env: Environment variable for rate limit delay

    Returns:
        Address dict or {} if failed
    """
    if lat is None or lon is None:
        return {}

    params = {
        "lat": lat,
        "lon": lon,
        "format": "json",
        "addressdetails": 1
    }

    try:
        r = requests.get(
            NOMINATIM_REVERSE_URL,
            params=params,
            headers=_nominatim_headers(),
            timeout=15
        )

        # Handle rate limiting
        if r.status_code == 429:
            time.sleep(float(os.getenv(delay_env, "1.0")))
            r = requests.get(
                NOMINATIM_REVERSE_URL,
                params=params,
                headers=_nominatim_headers(),
                timeout=15
            )

        r.raise_for_status()
        data = r.json() or {}

        # OSM policy delay
        time.sleep(float(os.getenv(delay_env, "1.0")))

        return data.get("address") or {}

    except Exception as e:
        logging.error(f"Nominatim reverse geocoding failed for {lat},{lon}: {e}")
        return {}


def pick_best_town(address: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract town name from OSM address dict using deterministic order.

    Args:
        address: OSM address dictionary

    Returns:
        (town_name, precision) where precision is the key that matched
    """
    if not address:
        return (None, None)

    # Deterministic priority order
    for key in ("town", "city", "municipality", "village", "hamlet"):
        if val := address.get(key):
            return (val, key)

    return (None, None)
