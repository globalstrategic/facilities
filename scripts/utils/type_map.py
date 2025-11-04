"""Facility type normalization and mapping."""

from typing import Tuple

# Type mapping table: messy strings → validated enum values
MAPPING = {
    # Hydromet variations
    "sx-ew": "hydromet_plant",
    "sxew": "hydromet_plant",
    "sx ew": "hydromet_plant",
    "solvent extraction": "hydromet_plant",
    "sx/ew": "hydromet_plant",
    "heap leach": "heap_leach",
    "heap-leach": "heap_leach",

    # Rolling mill variations
    "rod mill": "rolling_mill",
    "wire mill": "rolling_mill",
    "wire rod mill": "rolling_mill",

    # Steel plant variations
    "steelworks": "steel_plant",
    "steel works": "steel_plant",
    "steel mill": "steel_plant",
    "steel plant": "steel_plant",

    # Battery recycling variations
    "battery recycle": "battery_recycling",
    "battery recycling": "battery_recycling",
    "recycling plant": "battery_recycling",
    "battery plant": "battery_recycling",

    # Processing plant variations
    "processing plant": "processing_plant",
    "processing": "processing_plant",
    "process plant": "processing_plant",

    # Core types (pass-through)
    "mine": "mine",
    "smelter": "smelter",
    "refinery": "refinery",
    "concentrator": "concentrator",
    "plant": "plant",
    "mill": "mill",
    "tailings": "tailings",
    "exploration": "exploration",
    "development": "development",
}

# Valid enum values (from schema)
VALID_TYPES = {
    "mine", "smelter", "refinery", "concentrator", "plant", "mill",
    "heap_leach", "tailings", "exploration", "development",
    "hydromet_plant", "rolling_mill", "steel_plant",
    "battery_recycling", "processing_plant"
}


def normalize_type(raw: str) -> Tuple[str, float]:
    """
    Normalize facility type string to validated enum value.

    Args:
        raw: Raw type string

    Returns:
        (normalized_type, confidence) tuple
    """
    if not raw:
        return ("facility", 0.2)

    # Clean input
    r = str(raw).strip().lower()

    # Remove numeric garbage (e.g., "16.797")
    if r.replace(".", "").replace("-", "").isdigit():
        return ("facility", 0.1)

    # Exact match in mapping
    if r in MAPPING:
        return (MAPPING[r], 0.95)

    # Try with normalization
    r_norm = r.replace("_", " ").replace("-", " ")
    if r_norm in MAPPING:
        return (MAPPING[r_norm], 0.9)

    # Partial match in mapping keys
    for key, value in MAPPING.items():
        if key in r:
            return (value, 0.85)

    # Direct match with valid enum
    if r in VALID_TYPES:
        return (r, 0.8)

    # Check if contains valid type
    for valid_type in VALID_TYPES:
        if valid_type in r:
            return (valid_type, 0.7)

    # Special case: if contains "facility" but nothing else
    if "facility" in r and not any(t in r for t in VALID_TYPES):
        return ("facility", 0.3)

    # Default fallback
    return ("facility", 0.3)