"""Company resolution for facilities using EntityIdentity.

This module provides company name resolution and ownership parsing for the
facilities database. It integrates with the EntityIdentity library to match
operator and owner names to canonical company IDs.

Example Usage:
    >>> from scripts.utils.company_resolver import FacilityCompanyResolver
    >>>
    >>> # Initialize resolver (loads company database on first call)
    >>> resolver = FacilityCompanyResolver()
    >>>
    >>> # Resolve single operator
    >>> result = resolver.resolve_operator("BHP Billiton", country_hint="AUS")
    >>> print(result)
    {'company_id': 'cmp-LEI_...',
     'confidence': 0.95,
     'match_explanation': 'Exact name match'}
    >>>
    >>> # Parse ownership structure
    >>> owners = resolver.resolve_owners(
    ...     "BHP (60%), Rio Tinto (40%)",
    ...     country_hint="AUS"
    ... )
    >>> print(owners)
    [{'company_id': 'cmp-LEI_...', 'role': 'owner', 'percentage': 60.0, ...},
     {'company_id': 'cmp-LEI_...', 'role': 'minority_owner', 'percentage': 40.0, ...}]
"""

import logging
import re
import sys
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add entityidentity to path
ENTITYIDENTITY_PATH = Path(__file__).parent.parent.parent.parent / "entityidentity"
if str(ENTITYIDENTITY_PATH) not in sys.path:
    sys.path.insert(0, str(ENTITYIDENTITY_PATH))

from entityidentity.companies import EnhancedCompanyMatcher

logger = logging.getLogger(__name__)


def haversine_distance(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """Calculate the great circle distance between two points on Earth.

    Uses the Haversine formula to compute distance in kilometers between
    two geographic coordinates.

    Args:
        coord1: First coordinate as (latitude, longitude) tuple
        coord2: Second coordinate as (latitude, longitude) tuple

    Returns:
        Distance in kilometers as a float

    Example:
        >>> # Distance between New York and London
        >>> nyc = (40.7128, -74.0060)
        >>> london = (51.5074, -0.1278)
        >>> distance = haversine_distance(nyc, london)
        >>> print(f"{distance:.2f} km")
        5570.24 km

    References:
        https://en.wikipedia.org/wiki/Haversine_formula
    """
    # Earth's radius in kilometers
    R = 6371.0

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert to radians
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)

    # Haversine formula
    a = sin(delta_lat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2)**2
    c = 2 * asin(sqrt(a))

    return R * c


class FacilityCompanyResolver:
    """Resolve company names to canonical IDs for facilities.

    This class provides methods to resolve operator and owner company names
    to canonical company IDs using the EntityIdentity library. It includes
    caching to avoid redundant lookups and proximity-based confidence boosting.

    Attributes:
        matcher: EnhancedCompanyMatcher instance for company resolution
        _cache: Dictionary cache for resolved companies

    Example:
        >>> resolver = FacilityCompanyResolver()
        >>>
        >>> # Resolve operator
        >>> operator = resolver.resolve_operator(
        ...     "Sibanye-Stillwater",
        ...     country_hint="ZAF",
        ...     facility_coords=(25.5, 27.5)
        ... )
        >>>
        >>> # Parse ownership
        >>> owners = resolver.resolve_owners("Anglo American 50%, Impala 50%")
    """

    # Default quality gate configuration
    DEFAULT_GATES = {
        "auto_accept_threshold": 0.90,
        "review_min_threshold": 0.75,
        "prefer_registry_boost": 0.05,
        "dual_source_boost": 0.03,
        "parent_match_boost": 0.02,
        "profiles": {
            "strict": {
                "auto_accept_threshold": 0.90,
                "review_min_threshold": 0.80
            },
            "moderate": {
                "auto_accept_threshold": 0.85,
                "review_min_threshold": 0.70
            },
            "permissive": {
                "auto_accept_threshold": 0.80,
                "review_min_threshold": 0.60
            }
        }
    }

    def __init__(self, config: Optional[Dict] = None):
        """Initialize the company resolver.

        Loads the EnhancedCompanyMatcher with enriched LEI dataset (~50MB).
        The company database is loaded lazily on first use.

        Args:
            config: Optional configuration dict with gate thresholds.
                   If not provided, uses DEFAULT_GATES.
        """
        self.config = config or self.DEFAULT_GATES.copy()
        logger.info("Initializing FacilityCompanyResolver")
        self.matcher = EnhancedCompanyMatcher()
        self._cache: Dict[Tuple, Optional[Dict]] = {}
        logger.info("Company resolver ready (database will load on first use)")

    @classmethod
    def from_config(cls, config_path: str = None, profile: str = "strict"):
        """Create resolver from config file or use defaults with profile.

        Args:
            config_path: Optional path to gate_config.json (if None, uses DEFAULT_GATES)
            profile: Profile name (strict, moderate, permissive) - default: strict

        Returns:
            Initialized FacilityCompanyResolver instance
        """
        import json
        from pathlib import Path

        # Start with default config
        config = cls.DEFAULT_GATES.copy()

        # Override with file config if provided
        if config_path:
            config_file = Path(config_path)
            if config_file.exists():
                with open(config_file) as f:
                    file_config = json.load(f)
                    config.update(file_config)
            else:
                logger.warning(f"Config file not found: {config_path}, using defaults")

        # Apply profile overrides if specified
        if profile and 'profiles' in config and profile in config['profiles']:
            logger.info(f"Applying profile '{profile}' overrides")
            overrides = config['profiles'][profile]
            for key, value in overrides.items():
                logger.info(f"  {key}: {value}")
                config[key] = value

        return cls(config=config)

    def resolve_operator(
        self,
        operator_name: str,
        country_hint: Optional[str] = None,
        facility_coords: Optional[Tuple[float, float]] = None
    ) -> Optional[Dict]:
        """Resolve operator name to canonical company ID.

        Performs fuzzy matching against the company database and returns the
        best match above the minimum score threshold. Applies proximity boost
        if facility coordinates are provided.

        Args:
            operator_name: Raw operator name from import data
            country_hint: Optional ISO2 or ISO3 country code for filtering
            facility_coords: Optional (lat, lon) tuple for proximity matching

        Returns:
            Dictionary with resolved company information:
                {
                    "company_id": "cmp-LEI_...",
                    "company_name": "Resolved Company Name",
                    "confidence": 0.92,
                    "match_explanation": "Exact name match with alias"
                }
            Returns None if no match found above minimum threshold.

        Example:
            >>> resolver = FacilityCompanyResolver()
            >>> result = resolver.resolve_operator("BHP", country_hint="AUS")
            >>> print(result['company_id'])
            cmp-LEI_X1QHVB8X1JQHVB8X
        """
        # Check cache first
        cache_key = (operator_name.lower().strip(), country_hint)
        if cache_key in self._cache:
            logger.debug(f"Cache hit for operator: {operator_name}")
            return self._cache[cache_key]

        # Skip empty names
        if not operator_name or not operator_name.strip():
            self._cache[cache_key] = None
            return None

        logger.info(f"Resolving operator: {operator_name}")

        try:
            # Match with EntityIdentity (min_score=70 returns scores 0-100)
            results = self.matcher.match_best(
                operator_name,
                limit=3,
                min_score=70
            )

            if not results or len(results) == 0:
                logger.warning(f"No match found for operator: {operator_name}")
                self._cache[cache_key] = None
                return None

            # Get best match (results is a list of dicts)
            best_match = results[0]

            # Convert score from 0-100 to 0-1
            confidence = best_match.get('score', 70) / 100.0

            # Apply proximity boost if coordinates available
            proximity_boost = 0.0
            if facility_coords and best_match.get('Entity.HeadquartersAddress.latitude'):
                try:
                    hq_coords = (
                        best_match['Entity.HeadquartersAddress.latitude'],
                        best_match['Entity.HeadquartersAddress.longitude']
                    )
                    distance_km = haversine_distance(facility_coords, hq_coords)

                    # Proximity boost: +0.05 if within 100km, +0.10 if within 10km
                    if distance_km < 10:
                        proximity_boost = 0.10
                    elif distance_km < 100:
                        proximity_boost = 0.05

                    if proximity_boost > 0:
                        logger.info(
                            f"Proximity boost +{proximity_boost:.2f} "
                            f"(distance: {distance_km:.1f}km)"
                        )
                except (KeyError, TypeError, ValueError) as e:
                    logger.debug(f"Could not calculate proximity: {e}")

            # Apply boost but cap confidence at 1.0
            confidence = min(1.0, confidence + proximity_boost)

            # Build match explanation
            explanation_parts = []
            if best_match.get('score', 0) >= 95:
                explanation_parts.append("Exact name match")
            elif best_match.get('score', 0) >= 85:
                explanation_parts.append("Strong name match")
            else:
                explanation_parts.append("Fuzzy name match")

            if proximity_boost > 0:
                explanation_parts.append(f"proximity boost +{proximity_boost:.2f}")

            match_explanation = "; ".join(explanation_parts)

            # Convert company_id to facility schema format
            # goodgleif returns 'lei' field, not 'company_id'
            lei = best_match.get('lei', '')
            if not lei:
                # Fallback to other possible fields
                lei = best_match.get('company_id', best_match.get('identifier', ''))

            if lei.startswith('cmp-'):
                company_id = lei
            else:
                company_id = f"cmp-{lei}" if lei else "cmp-unknown"

            # Build result
            # goodgleif returns 'original_name', not 'name'
            company_name = best_match.get('original_name') or best_match.get('name', operator_name)

            result = {
                "company_id": company_id,
                "company_name": company_name,
                "confidence": round(confidence, 3),
                "match_explanation": match_explanation
            }

            logger.info(
                f"Matched '{operator_name}' to '{result['company_name']}' "
                f"(confidence: {result['confidence']:.3f})"
            )

            # Cache result
            self._cache[cache_key] = result
            return result

        except Exception as e:
            logger.error(f"Error resolving operator '{operator_name}': {e}", exc_info=True)
            self._cache[cache_key] = None
            return None

    def resolve_owners(
        self,
        owner_text: str,
        country_hint: Optional[str] = None
    ) -> List[Dict]:
        """Parse and resolve ownership structure from text.

        Handles various ownership text formats and returns a list of owner links
        matching the facility schema format. Each owner is resolved to a
        canonical company ID with ownership percentage and confidence score.

        Supported formats:
            - "BHP (60%), Rio Tinto (40%)"
            - "Sibanye-Stillwater"
            - "Joint venture: Anglo American Platinum 50%, Impala Platinum 50%"
            - "BHP Billiton 60%, Rio Tinto Ltd 40%"

        Args:
            owner_text: Raw ownership text from import data
            country_hint: Optional ISO2 or ISO3 country code for filtering

        Returns:
            List of owner_links matching facility schema format:
                [
                    {
                        "company_id": "cmp-LEI_...",
                        "role": "owner",  # or "minority_owner"
                        "percentage": 60.0,
                        "confidence": 0.92
                    },
                    ...
                ]

        Example:
            >>> resolver = FacilityCompanyResolver()
            >>> owners = resolver.resolve_owners(
            ...     "Anglo American (50%), Impala Platinum (50%)",
            ...     country_hint="ZAF"
            ... )
            >>> len(owners)
            2
        """
        if not owner_text or not owner_text.strip():
            return []

        logger.info(f"Parsing ownership: {owner_text}")

        owner_links = []

        # Pattern 1: "Company Name (XX%)" - most common
        pattern1 = r'([^,\(\)]+?)\s*\((\d+(?:\.\d+)?)\s*%\)'
        matches = re.findall(pattern1, owner_text)

        # Pattern 2: "Company Name XX%" - alternative format
        if not matches:
            pattern2 = r'([^,\d]+?)\s+(\d+(?:\.\d+)?)\s*%'
            matches = re.findall(pattern2, owner_text)

        if matches:
            # Parse companies with percentages
            for company_name, percentage in matches:
                company_name = company_name.strip()
                # Remove common prefixes like "Joint venture:", "JV:"
                company_name = re.sub(r'^(joint\s+venture|jv)\s*:\s*', '',
                                     company_name, flags=re.IGNORECASE)

                resolved = self.resolve_operator(company_name, country_hint)

                if resolved:
                    percentage_float = float(percentage)

                    owner_links.append({
                        "company_id": resolved['company_id'],
                        "role": "owner" if percentage_float > 50 else "minority_owner",
                        "percentage": percentage_float,
                        "confidence": resolved['confidence']
                    })
                else:
                    logger.warning(f"Could not resolve owner: {company_name}")
        else:
            # No percentages found - treat as single owner
            owner_text_clean = owner_text.strip()
            # Remove common prefixes
            owner_text_clean = re.sub(r'^(joint\s+venture|jv)\s*:\s*', '',
                                     owner_text_clean, flags=re.IGNORECASE)

            resolved = self.resolve_operator(owner_text_clean, country_hint)

            if resolved:
                owner_links.append({
                    "company_id": resolved['company_id'],
                    "role": "owner",
                    "percentage": None,  # Unknown percentage
                    "confidence": resolved['confidence']
                })
            else:
                logger.warning(f"Could not resolve owner: {owner_text_clean}")

        logger.info(f"Resolved {len(owner_links)} owners from text")
        return owner_links

    def resolve_mentions(self, mentions: List[Dict], facility: Optional[Dict] = None) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Batch resolve company mentions with quality gates.

        Args:
            mentions: List of mention dicts with 'name', 'role', etc.
            facility: Optional facility dict for context

        Returns:
            Tuple of (accepted, review, pending) lists
        """
        import uuid

        accepted = []
        review = []
        pending = []

        # Get thresholds from config
        auto_accept_threshold = self.config.get('auto_accept_threshold', 0.90)
        review_min_threshold = self.config.get('review_min_threshold', 0.75)

        for mention in mentions:
            name = mention.get('name', '').strip()
            if not name:
                continue

            # Resolve using resolve_operator
            country_hint = mention.get('country_hint') or (facility.get('country_iso3') if facility else None)
            facility_coords = None
            if facility and facility.get('location'):
                loc = facility['location']
                if loc.get('lat') and loc.get('lon'):
                    facility_coords = (loc['lat'], loc['lon'])

            resolution = self.resolve_operator(name, country_hint, facility_coords)

            if not resolution:
                # No match - add to pending
                pending.append(mention)
                continue

            # Apply quality gates
            confidence = resolution['confidence']
            if confidence >= auto_accept_threshold:
                gate = 'auto_accept'
                accepted.append({
                    **mention,
                    'resolution': {
                        **resolution,
                        'gate': gate,
                        'base_confidence': confidence,
                        'penalties_applied': []
                    },
                    'relationship_id': str(uuid.uuid4())
                })
            elif confidence >= review_min_threshold:
                gate = 'review'
                review.append({
                    **mention,
                    'resolution': {
                        **resolution,
                        'gate': gate,
                        'base_confidence': confidence,
                        'penalties_applied': []
                    },
                    'relationship_id': str(uuid.uuid4())
                })
            else:
                # Below review threshold - add to pending
                pending.append(mention)

        logger.info(f"Resolved {len(mentions)} mentions: {len(accepted)} accepted, {len(review)} review, {len(pending)} pending")
        return accepted, review, pending

    def clear_cache(self):
        """Clear the resolution cache.

        Useful for testing or when you want to force re-resolution of
        previously cached companies.
        """
        self._cache.clear()
        logger.info("Cleared company resolution cache")

    def get_cache_stats(self) -> Dict:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics:
                {
                    "cache_size": 123,
                    "database_size": 50000  # or None if not loaded yet
                }
        """
        return {
            "cache_size": len(self._cache),
            "database_size": len(self.matcher.df) if self.matcher.df is not None else None
        }


# Alias for backward compatibility
CompanyResolver = FacilityCompanyResolver
