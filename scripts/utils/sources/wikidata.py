#!/usr/bin/env python3
"""
Wikidata SPARQL integration for mining facility geocoding.

Queries Wikidata for mine/deposit items with:
- P625 (coordinate location)
- P31 (instance of) → Q386190 (mine), Q820477 (mineral deposit)
- P17 (country)
- Aliases and multilingual labels

Example SPARQL query:
    SELECT ?item ?itemLabel ?coord ?alias WHERE {
      ?item wdt:P31/wdt:P279* wd:Q386190;  # mine / mining site
            wdt:P17 wd:Q232;                # Kazakhstan
            wdt:P625 ?coord.                # coordinates
      OPTIONAL { ?item skos:altLabel ?alias. }
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,ru".
      }
    }

Usage:
    from scripts.utils.sources.wikidata import WikidataClient

    client = WikidataClient()
    items = client.query_mines(
        country_iso3="KAZ",
        commodity="uranium"
    )
"""

import time
import logging
import requests
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Wikidata SPARQL endpoint
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Rate limiting
RATE_LIMIT = 0.2  # seconds (5 req/sec)
LAST_REQUEST_TIME = 0

# Wikidata entity mappings
WIKIDATA_COUNTRIES = {
    # Major mining countries
    'KAZ': 'Q232',    # Kazakhstan
    'USA': 'Q30',     # United States
    'ZAF': 'Q258',    # South Africa
    'AUS': 'Q408',    # Australia
    'CHL': 'Q298',    # Chile
    'PER': 'Q419',    # Peru
    'CHN': 'Q148',    # China
    'IND': 'Q668',    # India
    'BRA': 'Q155',    # Brazil
    'CAN': 'Q16',     # Canada
    'RUS': 'Q159',    # Russia
    'MEX': 'Q96',     # Mexico
    'IDN': 'Q252',    # Indonesia
    # Add more as needed
}

WIKIDATA_COMMODITIES = {
    'uranium': 'Q1098',
    'copper': 'Q753',
    'gold': 'Q897',
    'iron': 'Q677',
    'coal': 'Q24489',
    'platinum': 'Q880',
    'lithium': 'Q568',
    'nickel': 'Q744',
    'zinc': 'Q758',
    'lead': 'Q708',
    'silver': 'Q1090',
    'diamond': 'Q5283',
    # Add more as needed
}


@dataclass
class WikidataItem:
    """Wikidata item representing a mine or deposit."""
    qid: str  # Wikidata QID (e.g., "Q12345")
    label: str
    lat: float
    lon: float
    aliases: List[str]
    properties: Dict[str, Any]


class WikidataClient:
    """
    Client for Wikidata SPARQL queries.

    Handles:
    - Query construction for mines/deposits
    - Rate limiting
    - Result parsing
    - Coordinate extraction
    """

    def __init__(
        self,
        endpoint: str = SPARQL_ENDPOINT,
        timeout: int = 30,
        rate_limit: float = RATE_LIMIT
    ):
        """
        Initialize Wikidata client.

        Args:
            endpoint: SPARQL endpoint URL
            timeout: Query timeout in seconds
            rate_limit: Minimum seconds between requests
        """
        self.endpoint = endpoint
        self.timeout = timeout
        self.rate_limit = rate_limit

    def _rate_limit_wait(self):
        """Wait to respect rate limits."""
        global LAST_REQUEST_TIME
        elapsed = time.time() - LAST_REQUEST_TIME
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        LAST_REQUEST_TIME = time.time()

    def _build_mine_query(
        self,
        country_iso3: Optional[str] = None,
        commodity: Optional[str] = None,
        facility_name: Optional[str] = None
    ) -> str:
        """
        Build SPARQL query for mines/deposits.

        Args:
            country_iso3: ISO3 country code
            commodity: Commodity name
            facility_name: Facility name (for label search)

        Returns:
            SPARQL query string
        """
        # Start with base query
        query_parts = [
            'SELECT DISTINCT ?item ?itemLabel ?coord ?alias ?commodityLabel WHERE {'
        ]

        # Mine/deposit classes
        # Q386190 = mine
        # Q820477 = mineral deposit
        # Q188076 = quarry
        query_parts.append('  ?item wdt:P31/wdt:P279* ?mineClass.')
        query_parts.append('  VALUES ?mineClass { wd:Q386190 wd:Q820477 wd:Q188076 }')

        # Coordinates (required)
        query_parts.append('  ?item wdt:P625 ?coord.')

        # Country filter
        if country_iso3 and country_iso3 in WIKIDATA_COUNTRIES:
            country_qid = WIKIDATA_COUNTRIES[country_iso3]
            query_parts.append(f'  ?item wdt:P17 wd:{country_qid}.')

        # Commodity filter - mines can have commodity via multiple properties
        # P279 = subclass of (for deposits)
        # P186 = material / made from material
        # For now, don't filter by commodity in SPARQL - do it in post-processing
        # This is more reliable since Wikidata's commodity tagging is inconsistent
        if False:  # Disabled commodity filtering in SPARQL
            commodity_qid = WIKIDATA_COMMODITIES.get(commodity.lower())
            if commodity_qid:
                query_parts.append(f'  {{')
                query_parts.append(f'    ?item wdt:P279 wd:{commodity_qid}.')  # subclass
                query_parts.append(f'  }} UNION {{')
                query_parts.append(f'    ?item wdt:P186 wd:{commodity_qid}.')  # material
                query_parts.append(f'  }}')

        # Name search (if specified) - make it optional to get all mines in country
        # We'll filter by name in post-processing with fuzzy matching
        # This is more reliable than SPARQL string matching
        pass  # Don't filter by name in SPARQL - do fuzzy matching later

        # Get aliases
        query_parts.append('  OPTIONAL {')
        query_parts.append('    ?item skos:altLabel ?alias.')
        query_parts.append('    FILTER(LANG(?alias) IN ("en", "ru", "")).')
        query_parts.append('  }')

        # Get commodity labels
        query_parts.append('  OPTIONAL {')
        query_parts.append('    ?item wdt:P279 ?commodityItem.')
        query_parts.append('  }')

        # Labels service
        query_parts.append('  SERVICE wikibase:label {')
        query_parts.append('    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,ru,es,fr,de,zh".')
        query_parts.append('  }')

        query_parts.append('}')

        # Limit results to avoid timeouts
        query_parts.append('LIMIT 1000')

        return '\n'.join(query_parts)

    def query(self, sparql: str) -> List[Dict]:
        """
        Execute SPARQL query.

        Args:
            sparql: SPARQL query string

        Returns:
            List of result bindings
        """
        self._rate_limit_wait()

        try:
            response = requests.get(
                self.endpoint,
                params={
                    'query': sparql,
                    'format': 'json'
                },
                headers={
                    'User-Agent': 'FacilitiesGeocodingBot/1.0'
                },
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            results = data.get('results', {}).get('bindings', [])

            return results

        except requests.exceptions.Timeout:
            logger.warning(f"Wikidata query timeout ({self.timeout}s)")
            return []
        except requests.exceptions.RequestException as e:
            logger.warning(f"Wikidata query failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in Wikidata query: {e}")
            return []

    def _parse_results(self, results: List[Dict]) -> List[WikidataItem]:
        """
        Parse SPARQL results into WikidataItem objects.

        Args:
            results: List of SPARQL result bindings

        Returns:
            List of WikidataItem objects
        """
        # Group results by QID (multiple rows for aliases)
        items_by_qid = {}

        for binding in results:
            # Extract QID
            item_uri = binding.get('item', {}).get('value', '')
            qid = item_uri.split('/')[-1] if item_uri else None

            if not qid or not qid.startswith('Q'):
                continue

            # Get label
            label = binding.get('itemLabel', {}).get('value', qid)

            # Parse coordinates (format: "Point(lon lat)")
            coord_str = binding.get('coord', {}).get('value', '')
            lat, lon = self._parse_coordinate(coord_str)

            if lat is None or lon is None:
                continue

            # Get alias
            alias = binding.get('alias', {}).get('value')

            # Get commodity
            commodity = binding.get('commodityLabel', {}).get('value')

            # Add or update item
            if qid not in items_by_qid:
                items_by_qid[qid] = {
                    'qid': qid,
                    'label': label,
                    'lat': lat,
                    'lon': lon,
                    'aliases': [],
                    'commodities': []
                }

            # Add alias if present
            if alias and alias not in items_by_qid[qid]['aliases']:
                items_by_qid[qid]['aliases'].append(alias)

            # Add commodity if present
            if commodity and commodity not in items_by_qid[qid]['commodities']:
                items_by_qid[qid]['commodities'].append(commodity)

        # Convert to WikidataItem objects
        items = []
        for qid, data in items_by_qid.items():
            item = WikidataItem(
                qid=data['qid'],
                label=data['label'],
                lat=data['lat'],
                lon=data['lon'],
                aliases=data['aliases'],
                properties={
                    'commodities': data['commodities']
                }
            )
            items.append(item)

        return items

    def _parse_coordinate(self, coord_str: str) -> tuple:
        """
        Parse Wikidata coordinate string.

        Format: "Point(lon lat)" (WKT format)

        Args:
            coord_str: Coordinate string

        Returns:
            Tuple of (lat, lon) or (None, None) if parsing fails
        """
        if not coord_str:
            return None, None

        try:
            # Extract coordinates from "Point(lon lat)"
            import re
            match = re.search(r'Point\(([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\)', coord_str)
            if match:
                lon = float(match.group(1))
                lat = float(match.group(2))
                return lat, lon
        except Exception as e:
            logger.debug(f"Failed to parse coordinate: {coord_str} - {e}")

        return None, None

    def query_mines(
        self,
        country_iso3: Optional[str] = None,
        commodity: Optional[str] = None,
        facility_name: Optional[str] = None
    ) -> List[WikidataItem]:
        """
        Query mines/deposits from Wikidata.

        Args:
            country_iso3: ISO3 country code
            commodity: Commodity name
            facility_name: Facility name

        Returns:
            List of WikidataItem objects
        """
        query = self._build_mine_query(
            country_iso3=country_iso3,
            commodity=commodity,
            facility_name=facility_name
        )

        logger.debug(f"Wikidata SPARQL query:\n{query}")

        results = self.query(query)
        items = self._parse_results(results)

        logger.info(f"Wikidata found {len(items)} items")

        return items
