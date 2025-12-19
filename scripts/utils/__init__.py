"""Utilities for the facilities database.

This package provides utility functions for:
- Web search (Tavily, Brave)
- LLM-based coordinate extraction
- Geocoding (OSM, Wikidata)
- Company resolution
- Deduplication
- Facility synchronization
- Name canonicalization

For entity resolution (countries, metals, companies), use entityidentity directly:
    from entityidentity import country_identifier, metal_identifier
    from entityidentity.companies import EnhancedCompanyMatcher
"""

__all__ = []

# Web search client
try:
    from .web_search import WebSearchClient, tavily_search, brave_search
    __all__.extend(['WebSearchClient', 'tavily_search', 'brave_search'])
except ImportError:
    pass

# LLM extraction
try:
    from .llm_extraction import (
        extract_coordinates,
        calculate_from_reference,
        resolve_extraction_coordinates,
        ExtractionResult
    )
    __all__.extend([
        'extract_coordinates',
        'calculate_from_reference',
        'resolve_extraction_coordinates',
        'ExtractionResult'
    ])
except ImportError:
    pass

# Deduplication
try:
    from .deduplication import (
        is_duplicate_facility,
        merge_facilities,
        find_duplicate_groups,
        score_facility_completeness,
        select_best_facility
    )
    __all__.extend([
        'is_duplicate_facility',
        'merge_facilities',
        'find_duplicate_groups',
        'score_facility_completeness',
        'select_best_facility'
    ])
except ImportError:
    pass

# Geocoding (consolidated: geocoding + geo + geocode_cache)
try:
    from .geocoding import (
        AdvancedGeocoder,
        GeocodingResult,
        GeocodeCache,
        encode_geohash,
        geocode_via_nominatim,
        reverse_geocode_via_nominatim,
        pick_best_town
    )
    __all__.extend([
        'AdvancedGeocoder',
        'GeocodingResult',
        'GeocodeCache',
        'encode_geohash',
        'geocode_via_nominatim',
        'reverse_geocode_via_nominatim',
        'pick_best_town'
    ])
except ImportError:
    pass

# Company resolver
try:
    from .company_resolver import CompanyResolver
    __all__.extend(['CompanyResolver'])
except ImportError:
    pass

# Country utilities
try:
    from .country_utils import normalize_country_to_iso3, iso3_to_country_name
    __all__.extend(['normalize_country_to_iso3', 'iso3_to_country_name'])
except ImportError:
    pass

# Name canonicalization (v2)
try:
    from .name_canonicalizer_v2 import FacilityNameCanonicalizer, choose_town_from_address
    __all__.extend(['FacilityNameCanonicalizer', 'choose_town_from_address'])
except ImportError:
    pass

# Facility sync (optional - requires pandas)
try:
    from .facility_sync import FacilitySyncManager, iso2_to_iso3, iso3_to_iso2
    __all__.extend(['FacilitySyncManager', 'iso2_to_iso3', 'iso3_to_iso2'])
except ImportError:
    pass
