"""
Utility modules for facility data processing.

This package provides entity resolution utilities for:
- Country detection and normalization
- Metal/commodity normalization
- Company resolution
- Facility matching (planned)
- Facility synchronization with entityidentity
"""

# Import facility_sync first (no entityidentity dependency)
from .facility_sync import (
    FacilitySyncManager,
    iso2_to_iso3,
    iso3_to_iso2,
)

__all__ = [
    # Facility synchronization (always available)
    "FacilitySyncManager",
    "iso2_to_iso3",
    "iso3_to_iso2",
]

# Try to import entityidentity-dependent modules
try:
    from .country_detection import (
        detect_country_from_facility,
        validate_country_code,
    )

    __all__.extend([
        "detect_country_from_facility",
        "validate_country_code",
    ])
except ImportError:
    pass

# Try to import metal normalizer
try:
    from .metal_normalizer import (
        normalize_commodity,
        normalize_commodities,
        get_metal_info,
        is_valid_metal,
    )

    __all__.extend([
        "normalize_commodity",
        "normalize_commodities",
        "get_metal_info",
        "is_valid_metal",
    ])
except ImportError:
    pass

# Try to import company resolver
try:
    from .company_resolver import (
        FacilityCompanyResolver,
        haversine_distance,
    )

    __all__.extend([
        "FacilityCompanyResolver",
        "haversine_distance",
    ])
except ImportError:
    pass

# Try to import facility matcher
try:
    from .facility_matcher import (
        FacilityMatcher,
        haversine_vectorized,
    )

    __all__.extend([
        "FacilityMatcher",
        "haversine_vectorized",
    ])
except ImportError:
    pass
