"""
Centralized facility loading utilities.

This module provides consistent, reusable functions for loading and saving
facility JSON files. Use these functions instead of implementing custom
loaders in each script.

Usage:
    from scripts.utils.facility_loader import (
        load_facility,
        load_facilities_from_country,
        load_all_facilities,
        save_facility,
        iter_country_dirs,
        get_facilities_dir,
    )

    # Load all facilities from a country
    facilities = load_facilities_from_country("ZAF")

    # Load all facilities globally
    for facility in load_all_facilities():
        process(facility)

    # Iterate over country directories
    for country_dir in iter_country_dirs():
        print(f"Processing {country_dir.name}")
"""

import json
import logging
from pathlib import Path
from typing import Dict, Generator, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)


def get_facilities_dir() -> Path:
    """Get the root facilities directory path.

    Returns:
        Path to the facilities/ directory
    """
    return Path(__file__).parent.parent.parent / "facilities"


def iter_country_dirs(facilities_dir: Optional[Path] = None) -> Iterator[Path]:
    """Iterate over country directories in the facilities folder.

    Args:
        facilities_dir: Override facilities directory path

    Yields:
        Path to each country directory (e.g., facilities/ZAF/)
    """
    base_dir = facilities_dir or get_facilities_dir()

    for country_dir in sorted(base_dir.iterdir()):
        if country_dir.is_dir() and not country_dir.name.startswith('.'):
            yield country_dir


def load_facility(facility_path: Path) -> Optional[Dict]:
    """Load a single facility JSON file.

    Args:
        facility_path: Path to the facility JSON file

    Returns:
        Facility dictionary with '_path' metadata, or None if load fails
    """
    try:
        with open(facility_path, 'r', encoding='utf-8') as f:
            facility = json.load(f)
            facility['_path'] = facility_path
            return facility
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in {facility_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading {facility_path}: {e}")
        return None


def load_facilities_from_country(
    country_iso3: str,
    facilities_dir: Optional[Path] = None,
    include_path: bool = True
) -> List[Dict]:
    """Load all facility JSONs for a country.

    Args:
        country_iso3: ISO3 country code (e.g., "ZAF", "USA")
        facilities_dir: Override facilities directory path
        include_path: Whether to include '_path' metadata (default: True)

    Returns:
        List of facility dictionaries
    """
    base_dir = facilities_dir or get_facilities_dir()
    country_dir = base_dir / country_iso3

    if not country_dir.exists():
        logger.warning(f"No facilities directory found for {country_iso3}")
        return []

    facilities = []
    for facility_file in sorted(country_dir.glob("*.json")):
        facility = load_facility(facility_file)
        if facility:
            if not include_path:
                facility.pop('_path', None)
            facilities.append(facility)

    return facilities


def load_all_facilities(
    facilities_dir: Optional[Path] = None,
    include_path: bool = True,
    countries: Optional[List[str]] = None
) -> Generator[Dict, None, Tuple[int, int]]:
    """Generate all facility dictionaries from the database.

    This is a generator to handle large datasets efficiently.
    For a list, use: list(load_all_facilities())

    Args:
        facilities_dir: Override facilities directory path
        include_path: Whether to include '_path' metadata (default: True)
        countries: Optional list of country codes to filter

    Yields:
        Facility dictionaries

    Returns:
        Tuple of (total_loaded, error_count) after iteration completes
    """
    base_dir = facilities_dir or get_facilities_dir()
    loaded = 0
    errors = 0

    for country_dir in iter_country_dirs(base_dir):
        # Skip if not in countries filter
        if countries and country_dir.name not in countries:
            continue

        for facility_file in sorted(country_dir.glob("*.json")):
            facility = load_facility(facility_file)
            if facility:
                if not include_path:
                    facility.pop('_path', None)
                loaded += 1
                yield facility
            else:
                errors += 1

    return loaded, errors


def load_all_facilities_list(
    facilities_dir: Optional[Path] = None,
    include_path: bool = True,
    countries: Optional[List[str]] = None
) -> Tuple[List[Dict], int]:
    """Load all facilities as a list with error count.

    Convenience wrapper around load_all_facilities() generator.

    Args:
        facilities_dir: Override facilities directory path
        include_path: Whether to include '_path' metadata
        countries: Optional list of country codes to filter

    Returns:
        Tuple of (facilities list, error count)
    """
    facilities = []
    errors = 0

    base_dir = facilities_dir or get_facilities_dir()

    for country_dir in iter_country_dirs(base_dir):
        if countries and country_dir.name not in countries:
            continue

        for facility_file in sorted(country_dir.glob("*.json")):
            facility = load_facility(facility_file)
            if facility:
                if not include_path:
                    facility.pop('_path', None)
                facilities.append(facility)
            else:
                errors += 1

    return facilities, errors


def save_facility(
    facility: Dict,
    dry_run: bool = False,
    indent: int = 2
) -> bool:
    """Save a facility dictionary to its JSON file.

    Requires '_path' metadata in the facility dict. Use facility['_path']
    to specify the output path.

    Args:
        facility: Facility dictionary with '_path' metadata
        dry_run: If True, don't actually write (default: False)
        indent: JSON indentation (default: 2)

    Returns:
        True if saved successfully, False otherwise
    """
    facility_path = facility.get('_path')
    if not facility_path:
        logger.error("Cannot save facility: no '_path' metadata")
        return False

    if dry_run:
        logger.debug(f"Dry run: would save {facility_path}")
        return True

    try:
        # Remove internal metadata before saving
        save_data = {k: v for k, v in facility.items() if not k.startswith('_')}

        with open(facility_path, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=indent, ensure_ascii=False)

        return True
    except Exception as e:
        logger.error(f"Error saving {facility_path}: {e}")
        return False


def get_country_facility_count(facilities_dir: Optional[Path] = None) -> Dict[str, int]:
    """Get count of facilities per country.

    Args:
        facilities_dir: Override facilities directory path

    Returns:
        Dict mapping country code to facility count
    """
    base_dir = facilities_dir or get_facilities_dir()
    counts = {}

    for country_dir in iter_country_dirs(base_dir):
        count = len(list(country_dir.glob("*.json")))
        if count > 0:
            counts[country_dir.name] = count

    return counts
