#!/usr/bin/env python3
"""
Country detection utilities for facility data.

This module provides functions to auto-detect country codes from facility data
using the entityidentity library for country resolution.
"""

import logging
import re
from typing import Optional

import pycountry

# Import from entityidentity
try:
    from entityidentity import country_identifier
except ImportError:
    raise ImportError(
        "entityidentity library not found. Please install it:\n"
        "pip install git+https://github.com/microprediction/entityidentity.git"
    )

logger = logging.getLogger(__name__)


def iso2_to_iso3(iso2: str) -> str:
    """
    Convert ISO2 country code to ISO3.

    Args:
        iso2: Two-letter ISO country code (e.g., "DZ", "US")

    Returns:
        Three-letter ISO3 country code (e.g., "DZA", "USA")

    Raises:
        ValueError: If the ISO2 code is invalid
    """
    try:
        country = pycountry.countries.get(alpha_2=iso2.upper())
        if country:
            return country.alpha_3
        else:
            raise ValueError(f"Invalid ISO2 country code: {iso2}")
    except Exception as e:
        logger.error(f"Error converting ISO2 to ISO3 for '{iso2}': {e}")
        raise ValueError(f"Invalid ISO2 country code: {iso2}") from e


def iso3_to_iso2(iso3: str) -> str:
    """
    Convert ISO3 country code to ISO2.

    Args:
        iso3: Three-letter ISO country code (e.g., "DZA", "USA")

    Returns:
        Two-letter ISO2 country code (e.g., "DZ", "US")

    Raises:
        ValueError: If the ISO3 code is invalid
    """
    try:
        country = pycountry.countries.get(alpha_3=iso3.upper())
        if country:
            return country.alpha_2
        else:
            raise ValueError(f"Invalid ISO3 country code: {iso3}")
    except Exception as e:
        logger.error(f"Error converting ISO3 to ISO2 for '{iso3}': {e}")
        raise ValueError(f"Invalid ISO3 country code: {iso3}") from e


def detect_country_from_facility(facility_data: dict) -> str:
    """
    Auto-detect country ISO3 code from facility data.

    Tries multiple strategies:
    1. Direct 'country' field in data (name or ISO code)
    2. Parse from 'location' field if it contains country info
    3. Extract from facility name if it contains country

    Args:
        facility_data: Dictionary containing facility information

    Returns:
        ISO3 country code (e.g., "DZA", "USA")

    Raises:
        ValueError: If country cannot be detected from the provided data
    """
    # Strategy 1: Check for explicit country field
    if "country" in facility_data and facility_data["country"]:
        country_value = facility_data["country"]
        logger.debug(f"Found explicit country field: {country_value}")

        # Check if it's already an ISO code (2 or 3 letters)
        if isinstance(country_value, str):
            country_value_upper = country_value.upper().strip()

            # Check if it's ISO2
            if len(country_value_upper) == 2:
                try:
                    return iso2_to_iso3(country_value_upper)
                except ValueError:
                    pass  # Not a valid ISO2, try other strategies

            # Check if it's ISO3
            elif len(country_value_upper) == 3:
                try:
                    # Verify it's valid
                    country = pycountry.countries.get(alpha_3=country_value_upper)
                    if country:
                        return country_value_upper
                except Exception:
                    pass  # Not a valid ISO3, try country name resolution

            # Try resolving as country name using entityidentity
            try:
                iso2 = country_identifier(country_value)
                if iso2:
                    logger.info(f"Resolved country name '{country_value}' to {iso2}")
                    return iso2_to_iso3(iso2)
            except Exception as e:
                logger.warning(f"Could not resolve country name '{country_value}': {e}")

    # Strategy 2: Check for country_iso3 field (already in target format)
    if "country_iso3" in facility_data and facility_data["country_iso3"]:
        iso3 = facility_data["country_iso3"].upper().strip()
        try:
            # Verify it's valid
            country = pycountry.countries.get(alpha_3=iso3)
            if country:
                logger.debug(f"Using existing country_iso3: {iso3}")
                return iso3
        except Exception:
            pass

    # Strategy 3: Check location field for country information
    if "location" in facility_data:
        location = facility_data["location"]
        if isinstance(location, dict):
            # Check for country in location dict
            if "country" in location and location["country"]:
                try:
                    iso2 = country_identifier(location["country"])
                    if iso2:
                        logger.info(f"Resolved country from location: {location['country']} -> {iso2}")
                        return iso2_to_iso3(iso2)
                except Exception as e:
                    logger.warning(f"Could not resolve country from location: {e}")

        elif isinstance(location, str):
            # Try to extract country from location string
            # Look for patterns like "City, Country" or "Country"
            parts = location.split(",")
            if parts:
                # Try the last part (usually country in "City, State, Country" format)
                country_part = parts[-1].strip()
                try:
                    iso2 = country_identifier(country_part)
                    if iso2:
                        logger.info(f"Extracted country from location string: {country_part} -> {iso2}")
                        return iso2_to_iso3(iso2)
                except Exception:
                    pass

    # Strategy 4: Try to extract from facility name (last resort)
    # This is a weak strategy but can help in some cases
    if "name" in facility_data and facility_data["name"]:
        name = facility_data["name"]
        # Look for country names in parentheses like "Mine Name (Algeria)"
        match = re.search(r'\(([^)]+)\)$', name)
        if match:
            potential_country = match.group(1).strip()
            try:
                iso2 = country_identifier(potential_country)
                if iso2:
                    logger.info(f"Extracted country from facility name: {potential_country} -> {iso2}")
                    return iso2_to_iso3(iso2)
            except Exception:
                pass

    # If all strategies fail, raise an error
    logger.error(f"Cannot auto-detect country from facility data: {facility_data.get('name', 'Unknown')}")
    raise ValueError(
        "Cannot auto-detect country from facility data. "
        "Please provide country explicitly using --country flag."
    )


def validate_country_code(country_code: str) -> str:
    """
    Validate and normalize a country code (ISO2 or ISO3).

    Args:
        country_code: Country code to validate (2 or 3 letters)

    Returns:
        Normalized ISO3 country code

    Raises:
        ValueError: If the country code is invalid
    """
    if not country_code or not isinstance(country_code, str):
        raise ValueError("Country code must be a non-empty string")

    country_code = country_code.upper().strip()

    if len(country_code) == 2:
        return iso2_to_iso3(country_code)
    elif len(country_code) == 3:
        # Verify it's valid
        try:
            country = pycountry.countries.get(alpha_3=country_code)
            if country:
                return country_code
            else:
                raise ValueError(f"Invalid ISO3 country code: {country_code}")
        except Exception as e:
            raise ValueError(f"Invalid ISO3 country code: {country_code}") from e
    else:
        # Try resolving as country name
        try:
            iso2 = country_identifier(country_code)
            if iso2:
                return iso2_to_iso3(iso2)
        except Exception:
            pass

        raise ValueError(
            f"Invalid country code: '{country_code}'. "
            "Must be ISO2 (2 letters), ISO3 (3 letters), or country name."
        )
