#!/usr/bin/env python3
"""
OSM Overpass API integration for mining facility geocoding.

Queries OpenStreetMap for mining-related features:
- man_made=mineshaft, man_made=adit
- landuse=quarry
- resource=* (uranium, copper, iron_ore, etc.)

Example Overpass QL query:
    [out:json][timeout:120];
    area["ISO3166-1"="KZ"]->.a;
    (
      node(area.a)["man_made"~"mineshaft|adit"];
      way(area.a)["landuse"="quarry"];
      nwr(area.a)["resource"="uranium"];
    );
    out center tags;

Usage:
    from scripts.utils.sources.overpass import OverpassClient

    client = OverpassClient()
    features = client.query_mining_features(
        country_iso3="KAZ",
        resource="uranium"
    )
"""

import time
import logging
import requests
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Overpass API endpoints (community instances)
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter"
]

# Rate limiting (conservative for community instances)
RATE_LIMIT = 0.5  # seconds between requests
LAST_REQUEST_TIME = 0


@dataclass
class OSMFeature:
    """OSM feature from Overpass API."""
    osm_id: str  # e.g., "node/123456" or "way/789012"
    osm_type: str  # 'node', 'way', 'relation'
    lat: float
    lon: float
    name: Optional[str]
    tags: Dict[str, Any]


class OverpassClient:
    """
    Client for OSM Overpass API.

    Handles:
    - Query construction for mining features
    - Rate limiting
    - Error handling and retries
    - Result parsing
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        timeout: int = 120,
        rate_limit: float = RATE_LIMIT
    ):
        """
        Initialize Overpass client.

        Args:
            endpoint: Overpass API endpoint (uses default if None)
            timeout: Query timeout in seconds
            rate_limit: Minimum seconds between requests
        """
        self.endpoint = endpoint or OVERPASS_ENDPOINTS[0]
        self.timeout = timeout
        self.rate_limit = rate_limit

    def _rate_limit_wait(self):
        """Wait to respect rate limits."""
        global LAST_REQUEST_TIME
        elapsed = time.time() - LAST_REQUEST_TIME
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        LAST_REQUEST_TIME = time.time()

    def _build_mining_query(
        self,
        country_iso3: str,
        resource: Optional[str] = None,
        facility_name: Optional[str] = None,
        bbox: Optional[tuple] = None
    ) -> str:
        """
        Build Overpass QL query for mining features.

        Args:
            country_iso3: ISO3 country code (e.g., "KAZ")
            resource: Specific resource to filter (e.g., "uranium", "copper")
            facility_name: Facility name to search (regex matching)
            bbox: Bounding box (south, west, north, east) instead of country

        Returns:
            Overpass QL query string
        """
        # Convert ISO3 to ISO2 for Overpass (uses ISO3166-1:alpha2)
        iso2_map = {
            'KAZ': 'KZ', 'USA': 'US', 'ZAF': 'ZA', 'AUS': 'AU',
            'CHL': 'CL', 'PER': 'PE', 'CHN': 'CN', 'IND': 'IN',
            # Add more mappings as needed
        }
        iso2 = iso2_map.get(country_iso3, country_iso3[:2])

        query_parts = [
            f'[out:json][timeout:{self.timeout}];'
        ]

        # Define area (country or bbox)
        if bbox:
            south, west, north, east = bbox
            area_filter = f'({south},{west},{north},{east})'
        else:
            query_parts.append(f'area["ISO3166-1"="{iso2}"]->.searchArea;')
            area_filter = '(area.searchArea)'

        # Build feature queries
        features = []

        # Mining infrastructure
        features.append(f'node{area_filter}["man_made"~"mineshaft|adit"];')
        features.append(f'way{area_filter}["man_made"~"mineshaft|adit"];')

        # Quarries
        features.append(f'way{area_filter}["landuse"="quarry"];')

        # Resource-specific (if specified)
        if resource:
            resource_tag = self._normalize_resource_tag(resource)
            features.append(f'nwr{area_filter}["resource"="{resource_tag}"];')
        else:
            # All resources
            features.append(f'nwr{area_filter}["resource"];')

        # Name search (if specified)
        if facility_name:
            # Create regex-safe search term
            search_term = self._create_name_regex(facility_name)
            features.append(f'nwr{area_filter}["name"~"{search_term}",i];')

        # Combine features
        query_parts.append('(')
        query_parts.extend(features)
        query_parts.append(');')

        # Output with center coordinates for ways/relations
        query_parts.append('out center tags;')

        return '\n'.join(query_parts)

    def _normalize_resource_tag(self, resource: str) -> str:
        """
        Normalize resource name to OSM resource tag.

        Args:
            resource: Resource name (e.g., "copper", "uranium", "iron ore")

        Returns:
            OSM resource tag value
        """
        # Map common resource names to OSM tags
        resource_map = {
            'copper': 'copper',
            'cu': 'copper',
            'gold': 'gold',
            'au': 'gold',
            'uranium': 'uranium',
            'u': 'uranium',
            'iron': 'iron',
            'iron ore': 'iron_ore',
            'fe': 'iron',
            'coal': 'coal',
            'platinum': 'platinum',
            'pt': 'platinum',
            'lithium': 'lithium',
            'li': 'lithium',
            'nickel': 'nickel',
            'ni': 'nickel',
            'zinc': 'zinc',
            'zn': 'zinc',
            'lead': 'lead',
            'pb': 'lead',
            'silver': 'silver',
            'ag': 'silver'
        }

        normalized = resource.lower().strip()
        return resource_map.get(normalized, normalized.replace(' ', '_'))

    def _create_name_regex(self, facility_name: str) -> str:
        """
        Create regex for name search (case-insensitive, flexible).

        Args:
            facility_name: Facility name

        Returns:
            Regex pattern for Overpass
        """
        # Remove special characters, split into words
        words = facility_name.lower().split()
        # Escape special regex characters
        words = [re.escape(w) for w in words]
        # Join with flexible spacing
        return '.*'.join(words)

    def query(self, overpass_ql: str) -> List[OSMFeature]:
        """
        Execute Overpass QL query.

        Args:
            overpass_ql: Overpass QL query string

        Returns:
            List of OSMFeature objects
        """
        self._rate_limit_wait()

        try:
            response = requests.post(
                self.endpoint,
                data={'data': overpass_ql},
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            elements = data.get('elements', [])

            return self._parse_elements(elements)

        except requests.exceptions.Timeout:
            logger.warning(f"Overpass query timeout ({self.timeout}s)")
            return []
        except requests.exceptions.RequestException as e:
            logger.warning(f"Overpass query failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in Overpass query: {e}")
            return []

    def _parse_elements(self, elements: List[Dict]) -> List[OSMFeature]:
        """
        Parse Overpass API response elements.

        Args:
            elements: List of elements from Overpass response

        Returns:
            List of OSMFeature objects
        """
        features = []

        for elem in elements:
            osm_type = elem.get('type')
            osm_id = elem.get('id')

            if not osm_type or not osm_id:
                continue

            # Get coordinates
            lat = elem.get('lat')
            lon = elem.get('lon')

            # For ways/relations, use center coordinates
            if lat is None or lon is None:
                center = elem.get('center', {})
                lat = center.get('lat')
                lon = center.get('lon')

            if lat is None or lon is None:
                continue

            # Get tags
            tags = elem.get('tags', {})
            name = tags.get('name')

            # Create feature
            feature = OSMFeature(
                osm_id=f"{osm_type}/{osm_id}",
                osm_type=osm_type,
                lat=lat,
                lon=lon,
                name=name,
                tags=tags
            )

            features.append(feature)

        return features

    def query_mining_features(
        self,
        country_iso3: str,
        resource: Optional[str] = None,
        facility_name: Optional[str] = None,
        bbox: Optional[tuple] = None
    ) -> List[OSMFeature]:
        """
        Query mining features for a country/region.

        Args:
            country_iso3: ISO3 country code
            resource: Optional resource filter
            facility_name: Optional name search
            bbox: Optional bounding box (south, west, north, east)

        Returns:
            List of OSMFeature objects
        """
        query = self._build_mining_query(
            country_iso3=country_iso3,
            resource=resource,
            facility_name=facility_name,
            bbox=bbox
        )

        logger.debug(f"Overpass query:\n{query}")

        return self.query(query)


# Import regex for name search
import re
