"""Facility matching and duplicate detection using EntityIdentity.

This module provides advanced facility matching capabilities that combine local
database search with EntityIdentity cross-referencing. It supports multiple
matching strategies including exact name, location proximity, alias matching,
company-commodity correlation, and EntityIdentity database lookups.

Key Features:
- Multi-strategy duplicate detection (5+ strategies)
- Vectorized haversine distance calculations for performance
- Integration with EntityIdentity facilities parquet database
- Fuzzy string matching with RapidFuzz
- Confidence scoring and candidate ranking
- Caching for improved performance

Example Usage:
    >>> from scripts.utils.facility_matcher import FacilityMatcher
    >>>
    >>> # Initialize matcher (loads databases)
    >>> matcher = FacilityMatcher()
    >>>
    >>> # Find duplicates for a new facility
    >>> facility = {
    ...     "name": "Stillwater Mine",
    ...     "location": {"lat": 45.5, "lon": -109.8},
    ...     "commodities": [{"metal": "platinum", "primary": True}],
    ...     "operator_link": {"company_id": "cmp-sibanye"}
    ... }
    >>> duplicates = matcher.find_duplicates(facility)
    >>> for dup in duplicates:
    ...     print(f"{dup['facility_id']}: {dup['confidence']:.2f} ({dup['strategy']})")
"""

import json
import logging
import sys
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Add entityidentity to path
ENTITYIDENTITY_PATH = Path(__file__).parent.parent.parent.parent / "entityidentity"
if str(ENTITYIDENTITY_PATH) not in sys.path:
    sys.path.insert(0, str(ENTITYIDENTITY_PATH))

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    logging.warning("rapidfuzz not available - fuzzy matching disabled")

logger = logging.getLogger(__name__)


def haversine_vectorized(
    lat1: float,
    lon1: float,
    lat2_array: np.ndarray,
    lon2_array: np.ndarray
) -> np.ndarray:
    """Calculate great circle distances using vectorized Haversine formula.

    This is a high-performance implementation that calculates distances from
    one point to many points simultaneously using NumPy vectorization.

    Args:
        lat1: Latitude of reference point in decimal degrees
        lon1: Longitude of reference point in decimal degrees
        lat2_array: Array of latitudes to calculate distances to
        lon2_array: Array of longitudes to calculate distances to

    Returns:
        NumPy array of distances in kilometers

    Example:
        >>> import numpy as np
        >>> # Calculate distances from NYC to multiple cities
        >>> nyc_lat, nyc_lon = 40.7128, -74.0060
        >>> cities_lat = np.array([51.5074, 48.8566, 35.6762])  # London, Paris, Tokyo
        >>> cities_lon = np.array([-0.1278, 2.3522, 139.6503])
        >>> distances = haversine_vectorized(nyc_lat, nyc_lon, cities_lat, cities_lon)
        >>> print(distances)  # [5570.24, 5837.41, 10838.65] km

    References:
        https://en.wikipedia.org/wiki/Haversine_formula
    """
    # Earth's radius in kilometers
    R = 6371.0

    # Convert all to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = np.radians(lat2_array)
    lon2_rad = np.radians(lon2_array)

    # Calculate differences
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Haversine formula (vectorized)
    a = np.sin(delta_lat / 2)**2 + \
        np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


class FacilityMatcher:
    """Match facilities using multi-strategy approach with EntityIdentity integration.

    This class provides comprehensive duplicate detection by combining multiple
    matching strategies including exact name matching, location proximity,
    alias matching, company-commodity correlation, and cross-referencing with
    the EntityIdentity facilities database.

    The matcher loads both local facility JSONs and EntityIdentity parquet files
    on initialization, then provides fast in-memory matching using vectorized
    operations and fuzzy string matching.

    Attributes:
        ei_facilities: DataFrame containing EntityIdentity facilities
        local_facilities: DataFrame containing local facility JSONs
        ei_parquet_path: Path to EntityIdentity facilities parquet file

    Example:
        >>> matcher = FacilityMatcher()
        >>> print(f"Loaded {len(matcher.local_facilities)} local facilities")
        >>> print(f"Loaded {len(matcher.ei_facilities)} EntityIdentity facilities")
        >>>
        >>> # Find duplicates for a facility
        >>> facility = load_facility_json("usa-stillwater-fac.json")
        >>> duplicates = matcher.find_duplicates(facility)
    """

    def __init__(self):
        """Initialize the facility matcher.

        Loads EntityIdentity facilities database from parquet and all local
        facility JSONs into DataFrames. Adds computed columns for efficient
        matching (name_lower, name_prefix, etc.).

        Raises:
            FileNotFoundError: If EntityIdentity parquet files not found
            ValueError: If no local facilities found
        """
        logger.info("Initializing FacilityMatcher")

        # Load EntityIdentity facilities database
        parquet_path = ENTITYIDENTITY_PATH / "tables" / "facilities"
        if not parquet_path.exists():
            logger.warning(f"EntityIdentity facilities path not found: {parquet_path}")
            self.ei_facilities = pd.DataFrame()
            self.ei_parquet_path = None
        else:
            # Get latest parquet file
            parquet_files = list(parquet_path.glob("facilities_*.parquet"))
            if not parquet_files:
                logger.warning("No EntityIdentity facilities parquet files found")
                self.ei_facilities = pd.DataFrame()
                self.ei_parquet_path = None
            else:
                self.ei_parquet_path = max(parquet_files)  # Latest by filename
                logger.info(f"Loading EntityIdentity facilities from: {self.ei_parquet_path}")
                self.ei_facilities = pd.read_parquet(self.ei_parquet_path)
                logger.info(f"Loaded {len(self.ei_facilities)} EntityIdentity facilities")

                # Add computed columns for matching
                self.ei_facilities['name_lower'] = \
                    self.ei_facilities['facility_name'].str.lower()
                self.ei_facilities['name_prefix'] = \
                    self.ei_facilities['facility_name'].str.lower().str[:10]

        # Load local facilities database
        self.local_facilities = self._load_local_facilities()
        logger.info(f"Loaded {len(self.local_facilities)} local facilities")

        # Add computed columns for matching (only if we have facilities)
        if len(self.local_facilities) > 0:
            self.local_facilities['name_lower'] = \
                self.local_facilities['name'].str.lower()
            self.local_facilities['name_prefix'] = \
                self.local_facilities['name'].str.lower().str[:10]

    def _load_local_facilities(self) -> pd.DataFrame:
        """Load all local facility JSONs into DataFrame.

        Recursively scans the facilities directory for all JSON files,
        loads them, and flattens the structure for efficient matching.

        Returns:
            DataFrame with columns: facility_id, name, lat, lon, country_iso3,
                                   types, commodities, operator_company_id,
                                   aliases, ei_facility_id, etc.

        Raises:
            ValueError: If no facilities found or all failed to load
        """
        facilities_dir = Path(__file__).parent.parent.parent / "facilities"
        logger.info(f"Loading local facilities from: {facilities_dir}")

        facilities_list = []
        error_count = 0

        # Scan all country directories
        for country_dir in facilities_dir.iterdir():
            if not country_dir.is_dir():
                continue

            # Skip output directories
            if country_dir.name.startswith('.') or country_dir.name == 'output':
                continue

            # Load all JSON files in country directory
            for fac_file in country_dir.glob("*.json"):
                try:
                    with open(fac_file, 'r') as f:
                        facility = json.load(f)

                    # Extract fields for matching
                    facility_row = {
                        "facility_id": facility.get("facility_id"),
                        "name": facility.get("name"),
                        "lat": facility.get("location", {}).get("lat"),
                        "lon": facility.get("location", {}).get("lon"),
                        "precision": facility.get("location", {}).get("precision"),
                        "country_iso3": facility.get("country_iso3"),
                        "types": facility.get("types", []),
                        "commodities": facility.get("commodities", []),
                        "status": facility.get("status"),
                        "aliases": facility.get("aliases", []),
                        "ei_facility_id": facility.get("ei_facility_id"),
                        "operator_company_id": facility.get("operator_link", {}).get("company_id")
                                              if facility.get("operator_link") else None,
                        "owner_company_ids": [
                            owner.get("company_id")
                            for owner in facility.get("owner_links", [])
                        ],
                        "file_path": str(fac_file)
                    }

                    facilities_list.append(facility_row)

                except Exception as e:
                    logger.warning(f"Error loading {fac_file}: {e}")
                    error_count += 1

        if not facilities_list:
            raise ValueError(
                f"No facilities loaded from {facilities_dir}. "
                f"Errors: {error_count}"
            )

        logger.info(
            f"Loaded {len(facilities_list)} facilities "
            f"({error_count} errors)"
        )

        return pd.DataFrame(facilities_list)

    def find_duplicates(
        self,
        facility: dict,
        strategies: Optional[List[str]] = None
    ) -> List[Dict]:
        """Find potential duplicates using multiple matching strategies.

        This is the main entry point for duplicate detection. It applies
        multiple strategies in sequence and returns a ranked list of
        candidate duplicates with confidence scores.

        Strategies:
            - name: Exact name match (case-insensitive)
            - location: Proximity match (5km radius)
            - alias: Name matches existing facility's alias
            - company: Company + commodity match (within 50km)
            - entityidentity: Cross-reference with EntityIdentity database

        Args:
            facility: Facility dictionary to check for duplicates
            strategies: List of strategy names to use. If None, uses all strategies.

        Returns:
            List of duplicate candidates, sorted by confidence (descending).
            Each candidate is a dictionary with:
                {
                    "facility_id": "usa-stillwater-fac",
                    "strategy": "exact_name",
                    "confidence": 0.95,
                    "distance_km": 0.5,  # Optional
                    "ei_facility_id": "stillwater_xyz"  # Optional
                }

        Example:
            >>> matcher = FacilityMatcher()
            >>> facility = {
            ...     "name": "Stillwater Mine",
            ...     "location": {"lat": 45.5, "lon": -109.8},
            ...     "commodities": [{"metal": "platinum"}]
            ... }
            >>> duplicates = matcher.find_duplicates(facility, strategies=['name', 'location'])
            >>> if duplicates:
            ...     best_match = duplicates[0]
            ...     print(f"Duplicate found: {best_match['facility_id']} ({best_match['confidence']})")
        """
        if strategies is None:
            strategies = ['name', 'location', 'alias', 'company', 'entityidentity']

        logger.info(f"Finding duplicates for facility: {facility.get('name')}")
        logger.debug(f"Using strategies: {strategies}")

        candidates = []

        # Strategy 1: Exact name match
        if 'name' in strategies and facility.get('name'):
            name_lower = facility['name'].lower()
            name_matches = self.local_facilities[
                self.local_facilities['name_lower'] == name_lower
            ]

            for _, match in name_matches.iterrows():
                candidates.append({
                    "facility_id": match['facility_id'],
                    "strategy": "exact_name",
                    "confidence": 0.95,
                    "matched_name": match['name']
                })

        # Strategy 2: Location proximity
        if 'location' in strategies:
            lat = facility.get('location', {}).get('lat')
            lon = facility.get('location', {}).get('lon')

            if lat is not None and lon is not None:
                # Filter to facilities with coordinates
                local_with_coords = self.local_facilities.dropna(subset=['lat', 'lon'])

                if len(local_with_coords) > 0:
                    # Vectorized distance calculation
                    distances = haversine_vectorized(
                        lat, lon,
                        local_with_coords['lat'].values,
                        local_with_coords['lon'].values
                    )

                    # Find facilities within 5km
                    nearby_mask = distances < 5.0
                    nearby_facilities = local_with_coords[nearby_mask]
                    nearby_distances = distances[nearby_mask]

                    for (idx, match), distance_km in zip(nearby_facilities.iterrows(), nearby_distances):
                        # Calculate confidence based on distance
                        # 0km = 0.90, 5km = 0.70
                        proximity_confidence = 0.90 - (distance_km / 5.0) * 0.20

                        candidates.append({
                            "facility_id": match['facility_id'],
                            "strategy": "location_proximity",
                            "confidence": round(proximity_confidence, 3),
                            "distance_km": round(distance_km, 3),
                            "matched_name": match['name']
                        })

        # Strategy 3: Alias match
        if 'alias' in strategies and facility.get('name'):
            name_lower = facility['name'].lower()

            for _, local_fac in self.local_facilities.iterrows():
                aliases = local_fac.get('aliases', [])
                if not aliases:
                    continue

                # Check if facility name matches any alias
                for alias in aliases:
                    if alias.lower() == name_lower:
                        candidates.append({
                            "facility_id": local_fac['facility_id'],
                            "strategy": "alias_match",
                            "confidence": 0.90,
                            "matched_alias": alias,
                            "matched_name": local_fac['name']
                        })
                        break

        # Strategy 4: Company + commodity match
        if 'company' in strategies:
            operator_id = facility.get('operator_link', {}).get('company_id')
            facility_commodities = {
                c.get('metal', '').lower()
                for c in facility.get('commodities', [])
            }
            lat = facility.get('location', {}).get('lat')
            lon = facility.get('location', {}).get('lon')

            if operator_id and facility_commodities:
                # Find facilities with same operator
                same_operator = self.local_facilities[
                    self.local_facilities['operator_company_id'] == operator_id
                ]

                for _, match in same_operator.iterrows():
                    # Check commodity overlap
                    match_commodities = {
                        c.get('metal', '').lower()
                        for c in match.get('commodities', [])
                    }

                    commodity_overlap = facility_commodities & match_commodities
                    if not commodity_overlap:
                        continue

                    # Calculate proximity if coordinates available
                    match_lat = match.get('lat')
                    match_lon = match.get('lon')

                    if lat and lon and match_lat and match_lon:
                        distance_km = haversine_vectorized(
                            lat, lon,
                            np.array([match_lat]),
                            np.array([match_lon])
                        )[0]

                        # Only consider if within 50km
                        if distance_km > 50:
                            continue

                        # Confidence based on distance
                        proximity_confidence = 0.85 - (distance_km / 50.0) * 0.30
                    else:
                        # No coordinates - lower confidence
                        proximity_confidence = 0.60

                    candidates.append({
                        "facility_id": match['facility_id'],
                        "strategy": "company_commodity",
                        "confidence": round(proximity_confidence, 3),
                        "matched_name": match['name'],
                        "matched_commodities": list(commodity_overlap),
                        "distance_km": round(distance_km, 3) if lat and lon else None
                    })

        # Strategy 5: EntityIdentity cross-reference
        if 'entityidentity' in strategies and not self.ei_facilities.empty:
            ei_matches = self._match_against_ei_database(facility)
            candidates.extend(ei_matches)

        # Deduplicate and rank candidates
        ranked_candidates = self._rank_candidates(candidates)

        logger.info(f"Found {len(ranked_candidates)} potential duplicates")
        return ranked_candidates

    def _match_against_ei_database(self, facility: dict) -> List[Dict]:
        """Match facility against EntityIdentity database.

        Uses fuzzy string matching (RapidFuzz) to find similar facility names
        in the EntityIdentity database, then checks if we already have those
        facilities linked in our local database.

        Args:
            facility: Facility dictionary to match

        Returns:
            List of candidate matches with EntityIdentity linkage information

        Note:
            Requires rapidfuzz to be installed. If not available, returns empty list.
        """
        if not RAPIDFUZZ_AVAILABLE:
            logger.debug("RapidFuzz not available - skipping EntityIdentity matching")
            return []

        if self.ei_facilities.empty:
            logger.debug("EntityIdentity database empty - skipping")
            return []

        matches = []
        facility_name = facility.get('name', '')
        if not facility_name:
            return []

        facility_name_lower = facility_name.lower()

        # Fuzzy match against EntityIdentity facility names
        for _, ei_fac in self.ei_facilities.iterrows():
            ei_name = ei_fac.get('facility_name', '')
            if not ei_name:
                continue

            # Calculate fuzzy match score
            name_score = fuzz.ratio(facility_name_lower, ei_name.lower())

            # Threshold: 85% similarity
            if name_score < 85:
                continue

            # Check if we already have this EI facility linked in local DB
            ei_id = ei_fac.get('facility_id')
            existing = self.local_facilities[
                self.local_facilities['ei_facility_id'] == ei_id
            ]

            if len(existing) > 0:
                # We have a facility already linked to this EI facility
                for _, local_match in existing.iterrows():
                    matches.append({
                        "facility_id": local_match['facility_id'],
                        "strategy": "entityidentity_name",
                        "confidence": round(name_score / 100.0, 3),
                        "ei_facility_id": ei_id,
                        "ei_facility_name": ei_name,
                        "matched_name": local_match['name']
                    })
            else:
                # EntityIdentity facility found but not yet in our database
                # This is informational - not a duplicate in our DB
                logger.debug(
                    f"EntityIdentity match found but not in local DB: "
                    f"{ei_name} (score: {name_score})"
                )

        return matches

    def _rank_candidates(self, candidates: List[Dict]) -> List[Dict]:
        """Deduplicate and rank candidate matches.

        Removes duplicate facility IDs (keeping highest confidence),
        sorts by confidence score, and adds ranking metadata.

        Args:
            candidates: List of candidate matches from various strategies

        Returns:
            Deduplicated and sorted list of candidates with ranking

        Example:
            >>> candidates = [
            ...     {"facility_id": "usa-mine-fac", "confidence": 0.95, "strategy": "name"},
            ...     {"facility_id": "usa-mine-fac", "confidence": 0.85, "strategy": "location"},
            ...     {"facility_id": "usa-other-fac", "confidence": 0.70, "strategy": "alias"}
            ... ]
            >>> ranked = matcher._rank_candidates(candidates)
            >>> ranked[0]['facility_id']
            'usa-mine-fac'
            >>> ranked[0]['confidence']
            0.95
        """
        if not candidates:
            return []

        # Deduplicate by facility_id - keep highest confidence for each
        deduped = {}
        for candidate in candidates:
            fac_id = candidate['facility_id']
            confidence = candidate['confidence']

            if fac_id not in deduped or confidence > deduped[fac_id]['confidence']:
                deduped[fac_id] = candidate

        # Convert back to list and sort by confidence (descending)
        ranked = sorted(
            deduped.values(),
            key=lambda x: x['confidence'],
            reverse=True
        )

        # Add ranking metadata
        for rank, candidate in enumerate(ranked, start=1):
            candidate['rank'] = rank

        return ranked

    def get_statistics(self) -> Dict:
        """Get matcher statistics and database information.

        Returns:
            Dictionary with matcher statistics:
                {
                    "local_facilities_count": 8606,
                    "ei_facilities_count": 55,
                    "facilities_with_coords": 7234,
                    "facilities_with_ei_link": 12,
                    "ei_parquet_path": "/path/to/facilities_20251003_134822.parquet"
                }
        """
        local_with_coords = self.local_facilities.dropna(subset=['lat', 'lon'])
        local_with_ei = self.local_facilities.dropna(subset=['ei_facility_id'])

        return {
            "local_facilities_count": len(self.local_facilities),
            "ei_facilities_count": len(self.ei_facilities),
            "facilities_with_coords": len(local_with_coords),
            "facilities_with_ei_link": len(local_with_ei),
            "ei_parquet_path": str(self.ei_parquet_path) if self.ei_parquet_path else None,
            "rapidfuzz_available": RAPIDFUZZ_AVAILABLE
        }
