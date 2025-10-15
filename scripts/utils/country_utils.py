"""Country code normalization utilities.

Provides a single function to normalize any country input (name, ISO2, ISO3)
to ISO3 using entityidentity and pycountry.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import pycountry

logger = logging.getLogger(__name__)

# Add entityidentity to path if needed
ENTITYIDENTITY_PATH = Path(__file__).parent.parent.parent.parent / "entityidentity"
if str(ENTITYIDENTITY_PATH) not in sys.path:
    sys.path.insert(0, str(ENTITYIDENTITY_PATH))


def normalize_country_to_iso3(country_input: str) -> str:
    """Normalize any country input to ISO3 code.

    Accepts:
        - Country names: "Algeria", "United States"
        - ISO2 codes: "DZ", "US"
        - ISO3 codes: "DZA", "USA"

    Returns ISO3 code (3 uppercase letters).

    Args:
        country_input: Country name, ISO2, or ISO3 code

    Returns:
        ISO3 country code (e.g., "DZA", "USA", "ZAF")

    Raises:
        ValueError: If country cannot be resolved

    Example:
        >>> normalize_country_to_iso3("Algeria")
        'DZA'
        >>> normalize_country_to_iso3("DZ")
        'DZA'
        >>> normalize_country_to_iso3("USA")
        'USA'
    """
    if not country_input or not country_input.strip():
        raise ValueError("Country input cannot be empty")

    country_input = country_input.strip()

    # Try entityidentity first (handles fuzzy matching, abbreviations, etc.)
    try:
        from entityidentity import country_identifier

        iso2 = country_identifier(country_input)

        if iso2:
            # country_identifier returns ISO2, convert to ISO3
            try:
                country = pycountry.countries.get(alpha_2=iso2.upper())
                if country:
                    logger.debug(f"Resolved '{country_input}' → {iso2} → {country.alpha_3}")
                    return country.alpha_3
            except (AttributeError, LookupError):
                pass

    except ImportError:
        logger.debug("entityidentity not available, falling back to pycountry")
    except Exception as e:
        logger.debug(f"entityidentity resolution failed: {e}")

    # Fallback: Try pycountry directly
    try:
        # Check if already ISO3
        if len(country_input) == 3 and country_input.isupper():
            country = pycountry.countries.get(alpha_3=country_input)
            if country:
                logger.debug(f"'{country_input}' is already ISO3")
                return country.alpha_3

        # Check if ISO2
        if len(country_input) == 2 and country_input.isupper():
            country = pycountry.countries.get(alpha_2=country_input)
            if country:
                logger.debug(f"Converted ISO2 '{country_input}' → {country.alpha_3}")
                return country.alpha_3

        # Try as country name
        country = pycountry.countries.get(name=country_input)
        if country:
            logger.debug(f"Resolved name '{country_input}' → {country.alpha_3}")
            return country.alpha_3

        # Try fuzzy search
        matches = pycountry.countries.search_fuzzy(country_input)
        if matches:
            country = matches[0]
            logger.debug(f"Fuzzy matched '{country_input}' → {country.alpha_3}")
            return country.alpha_3

    except (AttributeError, LookupError) as e:
        logger.debug(f"pycountry lookup failed: {e}")

    # Could not resolve
    raise ValueError(
        f"Could not resolve country '{country_input}' to ISO3 code. "
        f"Please provide a valid country name, ISO2, or ISO3 code."
    )


def iso3_to_country_name(iso3: str) -> Optional[str]:
    """Get official country name from ISO3 code.

    Args:
        iso3: 3-character ISO country code

    Returns:
        Official country name or None if not found

    Example:
        >>> iso3_to_country_name("DZA")
        'Algeria'
    """
    try:
        country = pycountry.countries.get(alpha_3=iso3.upper())
        return country.name if country else None
    except (AttributeError, LookupError):
        return None


def validate_iso3(iso3: str) -> bool:
    """Check if a string is a valid ISO3 country code.

    Args:
        iso3: String to validate

    Returns:
        True if valid ISO3 code

    Example:
        >>> validate_iso3("DZA")
        True
        >>> validate_iso3("XYZ")
        False
    """
    if not iso3 or len(iso3) != 3:
        return False

    try:
        country = pycountry.countries.get(alpha_3=iso3.upper())
        return country is not None
    except (AttributeError, LookupError):
        return False
