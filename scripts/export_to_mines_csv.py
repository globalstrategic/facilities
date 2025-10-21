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

Can filter by:
- Country (single or all)
- Metal/commodity (using EntityIdentity normalization)
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.country_utils import normalize_country_to_iso3, iso3_to_country_name

# Try to import metal_identifier from entityidentity
try:
    from entityidentity import metal_identifier
    METAL_IDENTIFIER_AVAILABLE = True
except ImportError:
    METAL_IDENTIFIER_AVAILABLE = False
    print("Warning: metal_identifier not available (entityidentity library)", file=sys.stderr)


def normalize_metal(metal_name: str) -> Optional[str]:
    """
    Normalize metal name using EntityIdentity.

    Returns canonical metal name or None if not available.
    """
    if not METAL_IDENTIFIER_AVAILABLE:
        return metal_name.lower()

    try:
        result = metal_identifier(metal_name)
        if result and result.get('valid'):
            # Return the normalized name or formula
            return result.get('name', metal_name).lower()
    except Exception:
        pass

    return metal_name.lower()


def facility_has_metal(facility: Dict, target_metal: str) -> bool:
    """
    Check if facility produces the target metal.

    Uses metal_identifier to match different forms (e.g., "copper", "Cu", "Copper ore").
    """
    commodities = facility.get("commodities", [])
    if not commodities:
        return False

    # Normalize target metal
    normalized_target = normalize_metal(target_metal)
    if not normalized_target:
        return False

    # Check each commodity
    for comm in commodities:
        metal = comm.get("metal")
        if not metal:
            continue

        # Normalize facility metal
        normalized_facility_metal = normalize_metal(metal)

        # Match
        if normalized_facility_metal and normalized_target in normalized_facility_metal:
            return True
        if normalized_facility_metal and normalized_facility_metal in normalized_target:
            return True

    return False


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


def export_country_to_csv(country: str, output_file: Optional[str] = None, metal: Optional[str] = None) -> int:
    """
    Export all facilities from a country to Mines.csv format.

    Args:
        country: Country name or ISO3 code
        output_file: Output CSV path (defaults to {iso3}_mines.csv or {iso3}_{metal}_mines.csv)
        metal: Optional metal filter (e.g., "copper", "Cu", "lithium")

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

                # Apply metal filter if specified
                if metal and not facility_has_metal(facility, metal):
                    continue

                facilities.append(facility)
        except Exception as e:
            print(f"Warning: Error loading {json_file}: {e}", file=sys.stderr)

    if not facilities:
        metal_msg = f" producing {metal}" if metal else ""
        print(f"No facilities{metal_msg} found in {country_dir}")
        return 0

    # Set output file
    if not output_file:
        if metal:
            metal_slug = normalize_metal(metal) or metal
            output_file = f"{iso3}_{metal_slug}_mines.csv"
        else:
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


def export_all_to_csv(output_file: Optional[str] = None, metal: Optional[str] = None) -> int:
    """
    Export all facilities from all countries to a single Mines.csv file.

    Args:
        output_file: Output CSV path (defaults to gt/Mines_{timestamp}.csv or gt/{metal}_Mines_{timestamp}.csv)
        metal: Optional metal filter (e.g., "copper", "Cu", "lithium")

    Returns:
        Number of facilities exported
    """
    base_dir = Path(__file__).parent.parent / "facilities"

    if not base_dir.exists():
        print(f"Error: Facilities directory not found: {base_dir}")
        return 0

    # Set output file with timestamp
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gt_dir = Path(__file__).parent.parent / "gt"
        gt_dir.mkdir(exist_ok=True)

        if metal:
            metal_slug = normalize_metal(metal) or metal
            output_file = gt_dir / f"{metal_slug}_Mines_{timestamp}.csv"
        else:
            output_file = gt_dir / f"Mines_{timestamp}.csv"

    output_path = Path(output_file)

    # Collect all facilities from all country directories
    all_facilities = []
    country_counts = {}

    print("Scanning all country directories...")

    for country_dir in sorted(base_dir.iterdir()):
        if not country_dir.is_dir():
            continue

        # Try to resolve country name
        dir_name = country_dir.name
        iso3 = normalize_country_to_iso3(dir_name)

        if not iso3:
            print(f"Warning: Could not resolve country for directory '{dir_name}'", file=sys.stderr)
            country_name = dir_name  # Use directory name as fallback
        else:
            country_name = iso3_to_country_name(iso3)

        # Load facilities from this country
        facility_count = 0
        for json_file in country_dir.glob("*.json"):
            try:
                with open(json_file) as f:
                    facility = json.load(f)

                    # Apply metal filter if specified
                    if metal and not facility_has_metal(facility, metal):
                        continue

                    # Store country name with facility for later use
                    facility['_export_country_name'] = country_name
                    all_facilities.append(facility)
                    facility_count += 1
            except Exception as e:
                print(f"Warning: Error loading {json_file}: {e}", file=sys.stderr)

        if facility_count > 0:
            country_counts[country_name] = facility_count
            print(f"  {country_name}: {facility_count} facilities")

    if not all_facilities:
        print("No facilities found in any country directory")
        return 0

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

    print(f"\nWriting to {output_path}...")

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for facility in all_facilities:
            country_name = facility.pop('_export_country_name', 'Unknown')
            row = facility_to_csv_row(facility, country_name)
            writer.writerow(row)

    print(f"\n✓ Exported {len(all_facilities)} facilities from {len(country_counts)} countries to {output_path}")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")

    return len(all_facilities)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export facilities to Mines.csv format",
        epilog="Examples:\n"
               "  %(prog)s Chile\n"
               "  %(prog)s --all\n"
               "  %(prog)s Chile --metal copper\n"
               "  %(prog)s --all --metal lithium -o lithium_mines.csv",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "country",
        nargs="?",
        help="Country name or ISO3 code (e.g., 'Chile' or 'CHL'). Omit with --all to export all countries."
    )
    parser.add_argument(
        "-o", "--output",
        help="Output CSV file (default: {ISO3}_mines.csv for country, gt/Mines_{timestamp}.csv for --all)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Export all facilities from all countries to a timestamped Mines.csv in gt/"
    )
    parser.add_argument(
        "--metal",
        help="Filter by metal/commodity (e.g., 'copper', 'Cu', 'lithium', 'REE'). Uses EntityIdentity normalization."
    )

    args = parser.parse_args()

    # Check if --all flag is used
    if args.all:
        count = export_all_to_csv(args.output, metal=args.metal)
    elif args.country:
        count = export_country_to_csv(args.country, args.output, metal=args.metal)
    else:
        parser.error("Either provide a country name or use --all flag")

    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
