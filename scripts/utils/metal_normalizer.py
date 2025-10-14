#!/usr/bin/env python3
"""
Metal normalization utilities for facility data.

This module provides functions to normalize commodity/metal names using the
entityidentity library's metal resolution capabilities.
"""

import logging
from typing import Optional, Dict, Any

# Import from entityidentity
try:
    from entityidentity import metal_identifier, match_metal
except ImportError:
    raise ImportError(
        "entityidentity library not found. Please install it:\n"
        "pip install git+https://github.com/microprediction/entityidentity.git"
    )

logger = logging.getLogger(__name__)


def normalize_commodity(commodity_string: str) -> Dict[str, Any]:
    """
    Normalize commodity name to canonical form.

    Args:
        commodity_string: Raw metal/commodity name (e.g., "Cu", "Platinum", "lithium carbonate")

    Returns:
        Dictionary with normalized metal information:
        {
            "metal": "copper",  # Canonical lowercase name
            "chemical_formula": "Cu",  # Chemical symbol/formula
            "category": "base_metal"  # Metal category
        }

    Notes:
        - Returns the input string in lowercase with None values if normalization fails
        - Logs warnings for commodities that cannot be normalized
    """
    if not commodity_string or not isinstance(commodity_string, str):
        logger.warning(f"Invalid commodity string: {commodity_string}")
        return {
            "metal": str(commodity_string).lower() if commodity_string else "unknown",
            "chemical_formula": None,
            "category": "unknown"
        }

    # Clean up the input string
    commodity_clean = commodity_string.strip()

    # Try exact match first using metal_identifier
    try:
        result = metal_identifier(commodity_clean)
        if result:
            logger.debug(f"Exact match for '{commodity_string}': {result}")
            return {
                "metal": result.get('name', commodity_clean).lower(),
                "chemical_formula": result.get('symbol'),
                "category": result.get('category', 'unknown')
            }
    except Exception as e:
        logger.debug(f"Exact match failed for '{commodity_string}': {e}")

    # Try fuzzy matching if exact match fails
    try:
        matches = match_metal(commodity_clean, k=3)  # Get top 3 matches
        if matches:
            # Use the best match if score is high enough
            best_match = matches[0]
            score = best_match.get('score', 0)

            if score >= 85:  # High confidence threshold
                logger.info(
                    f"Fuzzy matched '{commodity_string}' -> '{best_match.get('name')}' "
                    f"(score: {score})"
                )
                return {
                    "metal": best_match.get('name', commodity_clean).lower(),
                    "chemical_formula": best_match.get('symbol'),
                    "category": best_match.get('category', 'unknown')
                }
            elif score >= 70:  # Medium confidence - log but still use
                logger.warning(
                    f"Low confidence match for '{commodity_string}' -> "
                    f"'{best_match.get('name')}' (score: {score})"
                )
                return {
                    "metal": best_match.get('name', commodity_clean).lower(),
                    "chemical_formula": best_match.get('symbol'),
                    "category": best_match.get('category', 'unknown')
                }
            else:
                logger.warning(
                    f"Very low confidence match for '{commodity_string}' "
                    f"(best score: {score}), using raw string"
                )
    except Exception as e:
        logger.debug(f"Fuzzy matching failed for '{commodity_string}': {e}")

    # Fallback: return as-is with warning
    logger.warning(f"Could not normalize commodity: '{commodity_string}' - using raw string")
    return {
        "metal": commodity_clean.lower(),
        "chemical_formula": None,
        "category": "unknown"
    }


def normalize_commodities(commodities: list) -> list:
    """
    Normalize a list of commodities.

    Args:
        commodities: List of commodity dictionaries with 'metal' field
                    Example: [{"metal": "copper", "primary": True}, ...]

    Returns:
        List of normalized commodity dictionaries with added fields:
        [
            {
                "metal": "copper",
                "chemical_formula": "Cu",
                "category": "base_metal",
                "primary": True
            },
            ...
        ]
    """
    if not commodities or not isinstance(commodities, list):
        logger.warning(f"Invalid commodities list: {commodities}")
        return []

    normalized = []
    for commodity in commodities:
        if not isinstance(commodity, dict):
            logger.warning(f"Skipping invalid commodity item: {commodity}")
            continue

        # Get the metal name
        metal_name = commodity.get('metal')
        if not metal_name:
            logger.warning(f"Commodity missing 'metal' field: {commodity}")
            continue

        # Normalize the metal
        normalized_metal = normalize_commodity(metal_name)

        # Merge with original commodity data (preserving other fields like 'primary')
        normalized_commodity = {**commodity, **normalized_metal}
        normalized.append(normalized_commodity)

    return normalized


def get_metal_info(metal_name: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed information about a metal.

    Args:
        metal_name: Name or symbol of the metal

    Returns:
        Dictionary with metal information, or None if not found
    """
    try:
        result = metal_identifier(metal_name)
        if result:
            return result
    except Exception as e:
        logger.debug(f"Could not get metal info for '{metal_name}': {e}")

    return None


def is_valid_metal(metal_name: str, min_confidence: float = 0.70) -> bool:
    """
    Check if a string represents a valid metal/commodity.

    Args:
        metal_name: Name or symbol to check
        min_confidence: Minimum match score to consider valid (0-1)

    Returns:
        True if the metal name can be resolved with sufficient confidence
    """
    if not metal_name or not isinstance(metal_name, str):
        return False

    # Try exact match first
    try:
        result = metal_identifier(metal_name.strip())
        if result:
            return True
    except Exception:
        pass

    # Try fuzzy match
    try:
        matches = match_metal(metal_name.strip(), k=1)
        if matches:
            score = matches[0].get('score', 0)
            return score >= (min_confidence * 100)  # Convert to percentage
    except Exception:
        pass

    return False
