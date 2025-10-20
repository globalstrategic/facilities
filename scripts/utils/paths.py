"""Path utilities for facilities project.

Provides canonical paths for output files.
"""

from pathlib import Path


def relationships_path() -> Path:
    """Get the canonical path for facility-company relationships file.

    Returns:
        Path to relationships parquet file
    """
    # Assume we're in scripts/utils, go up to project root
    root = Path(__file__).parent.parent.parent

    # Relationships stored in tables/facilities/
    relationships_file = root / "tables" / "facilities" / "facility_company_relationships.parquet"

    return relationships_file
