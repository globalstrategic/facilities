#!/usr/bin/env python3
"""
Export facilities from a country to Mines.csv format.

This script converts facility JSON files to the same CSV format as Mines.csv,
with columns:
- Confidence Factor
- Mine Name
- Companies (semicolon-separated list)
- Latitude
- Longitude
- Asset Type
- Country or Region
- Primary Commodity
- Secondary Commodity
- Other Commodities
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import List, Dict, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.country_utils import normalize_country_to_iso3, iso3_to_country_name


def get_confidence_label(confidence: float) -> str:
    """Convert numeric confidence to label."""
    if confidence >= 0.85:
        return "Very High"
    elif confidence >= 0.75:
        return "High"
    elif confidence >= 0.60:
        return "Moderate"
    elif confidence >= 0.40:
        return "Low"
    else:
        return "Very Low"


def get_companies(facility: Dict) -> List[str]:
    """
    Extract company names from facility.

    Checks (in order):
    1. operator_link.name
    2. owner_links[].name
    3. company_mentions[].name
    4. operator field (legacy)
    5. owner field (legacy)
    """
    companies = []

    # Check operator_link
    if facility.get("operator_link") and facility["operator_link"].get("name"):
        companies.append(facility["operator_link"]["name"])

    # Check owner_links
    if facility.get("owner_links"):
        for owner in facility["owner_links"]:
            if owner.get("name") and owner["name"] not in companies:
                companies.append(owner["name"])

    # Check company_mentions
    if facility.get("company_mentions"):
        for mention in facility["company_mentions"]:
            if mention.get("name") and mention["name"] not in companies:
                companies.append(mention["name"])

    # Legacy fields
    if facility.get("operator") and facility["operator"] not in companies:
        companies.append(facility["operator"])

    if facility.get("owner") and facility["owner"] not in companies:
        companies.append(facility["owner"])

    return companies


def get_asset_types(facility: Dict) -> str:
    """Get asset type from facility types field."""
    types = facility.get("types", [])
    if not types:
        return "Mine"

    # Capitalize and join with semicolon
    type_map = {
        "mine": "Mine",
        "plant": "Plant",
        "smelter": "Smelter",
        "refinery": "Refinery",
        "concentrator": "Concentrator",
        "processing_plant": "Plant",
    }

    formatted = [type_map.get(t.lower(), t.title()) for t in types]
    return ";".join(formatted)


def get_commodities(facility: Dict) -> tuple[Optional[str], Optional[str], List[str]]:
    """
    Extract primary, secondary, and other commodities.

    Returns:
        (primary, secondary, other_list)
    """
    commodities = facility.get("commodities", [])
    if not commodities:
        return None, None, []

    primary = None
    secondary = None
    others = []

    # Find primary
    for comm in commodities:
        if comm.get("primary"):
            primary = comm.get("metal")
            break

    # Get remaining commodities
    remaining = [c.get("metal") for c in commodities if c.get("metal") and c.get("metal") != primary]

    if remaining:
        secondary = remaining[0]
        others = remaining[1:]

    return primary, secondary, others


def facility_to_csv_row(facility: Dict, country_name: str) -> Dict[str, str]:
    """Convert facility JSON to Mines.csv row format."""

    # Get location
    location = facility.get("location", {})
    lat = location.get("lat") or ""
    lon = location.get("lon") or ""

    # Get confidence
    verification = facility.get("verification", {})
    confidence = verification.get("confidence", 0.5)
    confidence_label = get_confidence_label(confidence)

    # Get companies
    companies = get_companies(facility)
    company_str = ";".join(companies) if companies else ""

    # Get commodities
    primary, secondary, others = get_commodities(facility)
    other_str = ",".join(others) if others else ""

    # Get aliases for Mine Name (include main name and aliases)
    name = facility.get("name", "")
    aliases = facility.get("aliases", [])
    all_names = [name] + [a for a in aliases if a and a != name]
    mine_name = ";".join(all_names) if all_names else name

    return {
        "Confidence Factor": confidence_label,
        "Mine Name": mine_name,
        "Companies": company_str,
        "Latitude": str(lat) if lat else "",
        "Longitude": str(lon) if lon else "",
        "Asset Type": get_asset_types(facility),
        "Country or Region": country_name,
        "Primary Commodity": primary or "",
        "Secondary Commodity": secondary or "",
        "Other Commodities": other_str,
    }


def export_country_to_csv(country: str, output_file: Optional[str] = None) -> int:
    """
    Export all facilities from a country to Mines.csv format.

    Args:
        country: Country name or ISO3 code
        output_file: Output CSV path (defaults to {iso3}_mines.csv)

    Returns:
        Number of facilities exported
    """
    # Normalize country
    iso3 = normalize_country_to_iso3(country)
    if not iso3:
        print(f"Error: Could not resolve country '{country}'")
        return 0

    country_name = iso3_to_country_name(iso3)

    # Find country directory (could be ISO2 or ISO3)
    base_dir = Path(__file__).parent.parent / "facilities"
    country_dir = None

    # Try ISO3 first
    if (base_dir / iso3).exists():
        country_dir = base_dir / iso3
    else:
        # Try ISO2
        iso2 = iso3[:2]  # Rough approximation
        if (base_dir / iso2).exists():
            country_dir = base_dir / iso2

    if not country_dir or not country_dir.exists():
        print(f"Error: No facilities directory found for {iso3}")
        return 0

    # Load all facilities
    facilities = []
    for json_file in country_dir.glob("*.json"):
        try:
            with open(json_file) as f:
                facility = json.load(f)
                facilities.append(facility)
        except Exception as e:
            print(f"Warning: Error loading {json_file}: {e}", file=sys.stderr)

    if not facilities:
        print(f"No facilities found in {country_dir}")
        return 0

    # Set output file
    if not output_file:
        output_file = f"{iso3}_mines.csv"

    # Write CSV
    fieldnames = [
        "Confidence Factor",
        "Mine Name",
        "Companies",
        "Latitude",
        "Longitude",
        "Asset Type",
        "Country or Region",
        "Primary Commodity",
        "Secondary Commodity",
        "Other Commodities",
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for facility in facilities:
            row = facility_to_csv_row(facility, country_name)
            writer.writerow(row)

    print(f"✓ Exported {len(facilities)} facilities to {output_file}")
    return len(facilities)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export facilities from a country to Mines.csv format"
    )
    parser.add_argument(
        "country",
        help="Country name or ISO3 code (e.g., 'Algeria' or 'DZA')"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output CSV file (default: {ISO3}_mines.csv)"
    )

    args = parser.parse_args()

    count = export_country_to_csv(args.country, args.output)

    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
