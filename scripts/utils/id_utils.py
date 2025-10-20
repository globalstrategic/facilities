"""ID utilities for canonical company ID mapping.

Provides functions to normalize company IDs and load alias mappings.
"""

import json
from pathlib import Path
from typing import Dict, Optional


def to_canonical(company_id: str, alias_map: Dict[str, str]) -> str:
    """Convert company ID to canonical form using alias map.

    Args:
        company_id: Company ID to normalize
        alias_map: Dictionary mapping aliases to canonical IDs

    Returns:
        Canonical company ID (or original if no alias found)
    """
    if not company_id:
        return company_id

    # Check if this ID is an alias
    canonical = alias_map.get(company_id, company_id)

    return canonical


def load_alias_map(alias_file_path: str) -> Dict[str, str]:
    """Load company alias map from JSON file.

    Args:
        alias_file_path: Path to JSON file with alias mappings

    Returns:
        Dictionary mapping alias IDs to canonical IDs

    Example file format:
        {
            "cmp-old-id-1": "cmp-canonical-id-1",
            "cmp-old-id-2": "cmp-canonical-id-1"
        }
    """
    path = Path(alias_file_path)

    if not path.exists():
        return {}

    try:
        with open(path, 'r') as f:
            alias_map = json.load(f)
        return alias_map
    except Exception as e:
        # Log warning but don't fail
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Could not load alias map from {alias_file_path}: {e}")
        return {}
