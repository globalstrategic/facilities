"""Utilities for the facilities database.

This package provides utility functions for:
- Ownership parsing for joint ventures
- Facility synchronization with entityidentity parquet format
- Schema migration tools

For entity resolution (countries, metals, companies), use entityidentity directly:
    from entityidentity import country_identifier, metal_identifier
    from entityidentity.companies import EnhancedCompanyMatcher
"""

# Ownership parsing
try:
    from .ownership_parser import parse_ownership
    __all__ = ['parse_ownership']
except ImportError:
    __all__ = []

# Facility sync (optional - requires pandas)
try:
    from .facility_sync import FacilitySyncManager, iso2_to_iso3, iso3_to_iso2
    __all__.extend(['FacilitySyncManager', 'iso2_to_iso3', 'iso3_to_iso2'])
except ImportError:
    pass

# Schema migration (optional)
try:
    from .migrate_schema import migrate_facility, migrate_all_facilities
    __all__.extend(['migrate_facility', 'migrate_all_facilities'])
except ImportError:
    pass
