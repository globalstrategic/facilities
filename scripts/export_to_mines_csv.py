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
    import pandas as pd
    METAL_IDENTIFIER_AVAILABLE = True

    # Load metals database to support basket queries
    entity_path = Path(__file__).parent.parent.parent / "entityidentity"
    metals_parquet = entity_path / "entityidentity" / "metals" / "data" / "metals.parquet"
    if metals_parquet.exists():
        METALS_DB = pd.read_parquet(metals_parquet)
    else:
        METALS_DB = None
        print("Warning: metals.parquet not found, basket queries may be limited", file=sys.stderr)
except ImportError:
    METAL_IDENTIFIER_AVAILABLE = False
    METALS_DB = None
    print("Warning: metal_identifier not available (entityidentity library)", file=sys.stderr)


def normalize_metal(metal_name: str) -> Optional[Dict]:
    """
    Normalize metal name using EntityIdentity.

    Returns dict with normalized name and category info, or None if not available.
    """
    if not METAL_IDENTIFIER_AVAILABLE:
        return {
            'name': metal_name.lower(),
            'category': None,
            'category_bucket': None
        }

    try:
        result = metal_identifier(metal_name)
        # metal_identifier returns dict if match found, None if no match
        # Check if result has category_bucket (key indicator of success)
        if result and result.get('category_bucket'):
            return {
                'name': result.get('name', metal_name).lower(),
                'category': result.get('category'),
                'category_bucket': result.get('category_bucket'),
                'symbol': result.get('symbol')
            }
    except Exception:
        pass

    # Fallback: return lowercase name without category info
    return {
        'name': metal_name.lower(),
        'category': None,
        'category_bucket': None
    }


# Basket term mappings to category_bucket values
BASKET_MAPPINGS = {
    # Rare earth elements
    'ree': 'ree',
    'rare earth': 'ree',
    'rare earths': 'ree',
    'rare-earth': 'ree',
    'rare-earths': 'ree',
    'rareearth': 'ree',
    'rareearths': 'ree',

    # Platinum group metals
    'pgm': 'pgm',
    'pgms': 'pgm',
    'platinum group': 'pgm',
    'platinum group metal': 'pgm',
    'platinum group metals': 'pgm',

    # Base metals
    'base': 'base',
    'base metal': 'base',
    'base metals': 'base',

    # Battery metals
    'battery': 'battery',
    'battery metal': 'battery',
    'battery metals': 'battery',

    # Precious metals
    'precious': 'precious',
    'precious metal': 'precious',
    'precious metals': 'precious',

    # Ferroalloys
    'ferroalloy': 'ferroalloy',
    'ferroalloys': 'ferroalloy',
    'ferro': 'ferroalloy',

    # Specialty
    'specialty': 'specialty',
    'specialty metal': 'specialty',
    'specialty metals': 'specialty',

    # Industrial
    'industrial': 'industrial',

    # Nuclear
    'nuclear': 'nuclear',
}


def resolve_basket_to_metals(basket_term: str) -> Optional[List[str]]:
    """
    Resolve basket term to list of individual metal names.

    Args:
        basket_term: Basket name (e.g., "REE", "rare earths", "PGM", "battery metals")

    Returns:
        List of metal names in the basket, or None if not a basket or no data available
    """
    if not METAL_IDENTIFIER_AVAILABLE or METALS_DB is None:
        return None

    basket_lower = basket_term.lower().strip()
    category_bucket = BASKET_MAPPINGS.get(basket_lower)

    if not category_bucket:
        return None

    # Get all metals in this category_bucket
    metals_in_basket = METALS_DB[METALS_DB['category_bucket'] == category_bucket]
    if metals_in_basket.empty:
        return None

    # Return normalized names
    return metals_in_basket['name_norm'].tolist()


def is_basket_search(metal_name: str) -> bool:
    """Check if search term is a basket/group query."""
    metal_lower = metal_name.lower().strip()
    return metal_lower in BASKET_MAPPINGS


def facility_has_metal(facility: Dict, target_metal: str) -> bool:
    """
    Check if facility produces the target metal or any metal in a basket.

    Supports:
    - Individual metals: "copper", "Cu", "lithium"
    - Basket queries: "REE", "rare earths", "PGM", "battery metals", etc.

    Uses EntityIdentity to match different forms and expand baskets.
    """
    commodities = facility.get("commodities", [])
    if not commodities:
        return False

    # Normalize facility metals once (cache for efficiency)
    facility_metals_normalized = []
    for comm in commodities:
        metal = comm.get("metal")
        if metal:
            metal_info = normalize_metal(metal)
            if metal_info:
                facility_metals_normalized.append(metal_info)

    if not facility_metals_normalized:
        return False

    # Check if this is a basket query
    if is_basket_search(target_metal):
        # Use category_bucket for efficient matching
        basket_lower = target_metal.lower().strip()
        target_bucket = BASKET_MAPPINGS.get(basket_lower)
        if target_bucket:
            # Check if any facility metal is in this category_bucket
            for metal_info in facility_metals_normalized:
                if metal_info.get('category_bucket') == target_bucket:
                    return True
        return False

    # Individual metal search - normalize target once
    target_info = normalize_metal(target_metal)
    if not target_info:
        return False

    normalized_target = target_info['name']
    target_symbol = target_info.get('symbol')

    # Check each commodity against target
    for metal_info in facility_metals_normalized:
        facility_metal_name = metal_info['name']

        # Name matching (substring in both directions)
        if normalized_target in facility_metal_name:
            return True
        if facility_metal_name in normalized_target:
            return True

        # Symbol matching if available
        if target_symbol:
            facility_symbol = metal_info.get('symbol')
            if facility_symbol and target_symbol == facility_symbol:
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


def facility_has_company(facility: Dict, target_company: str) -> bool:
    """
    Check if facility is operated by or owned by the target company.

    Uses fuzzy matching to handle variations like:
    - "BHP" vs "BHP Billiton" vs "BHP Group"
    - "Rio Tinto" vs "Rio Tinto Plc"
    - "MP Materials" vs "MP Materials Corp"
    """
    companies = get_companies(facility)
    if not companies:
        return False

    target_lower = target_company.lower().strip()

    # Direct exact match (case insensitive)
    for company in companies:
        if company.lower() == target_lower:
            return True

    # Substring matching (both directions)
    for company in companies:
        company_lower = company.lower()
        # Check if target is in company name or company name is in target
        if target_lower in company_lower or company_lower in target_lower:
            return True

    # Word-based matching for multi-word companies
    # e.g., "MP Materials" matches "MP Materials Corp"
    target_words = set(target_lower.split())
    if len(target_words) > 0:
        for company in companies:
            company_words = set(company.lower().split())
            # If all target words are in company name, it's a match
            if target_words.issubset(company_words):
                return True
            # Or if all company words are in target (reversed check)
            if company_words.issubset(target_words):
                return True

    return False


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


def export_country_to_csv(country: str, output_file: Optional[str] = None, metal: Optional[str] = None, company: Optional[str] = None) -> int:
    """
    Export all facilities from a country to Mines.csv format.

    Args:
        country: Country name or ISO3 code
        output_file: Output CSV path (defaults to {iso3}_mines.csv or {iso3}_{metal}_mines.csv)
        metal: Optional metal filter (e.g., "copper", "Cu", "lithium", "REE", "battery metals")
        company: Optional company filter (e.g., "BHP", "Rio Tinto", "MP Materials")

    Returns:
        Number of facilities exported
    """
    # Check if metal filter is a basket query
    if metal and is_basket_search(metal):
        basket_metals = resolve_basket_to_metals(metal)
        if basket_metals:
            print(f"[Basket Query] Expanding '{metal}' to {len(basket_metals)} metals:")
            print(f"  {', '.join(basket_metals[:10])}")
            if len(basket_metals) > 10:
                print(f"  ... and {len(basket_metals) - 10} more")

    # Check if company filter is provided
    if company:
        print(f"[Company Filter] Filtering for facilities operated/owned by '{company}'")

    # Normalize country
    iso3 = normalize_country_to_iso3(country)
    if not iso3:
        print(f"Error: Could not resolve country '{country}'")
        return 0

    country_name = iso3_to_country_name(iso3)

    # Find country directory (should be ISO3, but legacy directories may use ISO2)
    base_dir = Path(__file__).parent.parent / "facilities"
    country_dir = None

    # Try ISO3 first (preferred)
    if (base_dir / iso3).exists():
        country_dir = base_dir / iso3
    else:
        # Try ISO2 for legacy directories
        # Use pycountry for proper conversion
        try:
            import pycountry
            country_obj = pycountry.countries.get(alpha_3=iso3)
            if country_obj:
                iso2 = country_obj.alpha_2
                if (base_dir / iso2).exists():
                    country_dir = base_dir / iso2
        except:
            pass

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

                # Apply company filter if specified
                if company and not facility_has_company(facility, company):
                    continue

                facilities.append(facility)
        except Exception as e:
            print(f"Warning: Error loading {json_file}: {e}", file=sys.stderr)

    if not facilities:
        filters = []
        if metal:
            filters.append(f"metal: {metal}")
        if company:
            filters.append(f"company: {company}")
        filter_msg = f" with filters ({', '.join(filters)})" if filters else ""
        print(f"No facilities{filter_msg} found in {country_dir}")
        return 0

    # Set output file
    if not output_file:
        parts = [iso3]
        if company:
            company_slug = company.lower().replace(' ', '_').replace(',', '')
            parts.append(company_slug)
        if metal:
            metal_info = normalize_metal(metal)
            metal_slug = metal_info['name'] if metal_info else metal
            metal_slug = metal_slug.replace(' ', '_')
            parts.append(metal_slug)
        parts.append("mines.csv")
        output_file = "_".join(parts)

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

    print(f"[OK] Exported {len(facilities)} facilities to {output_file}")
    return len(facilities)


def export_all_to_csv(output_file: Optional[str] = None, metal: Optional[str] = None, company: Optional[str] = None) -> int:
    """
    Export all facilities from all countries to a single Mines.csv file.

    Args:
        output_file: Output CSV path (defaults to gt/Mines_{timestamp}.csv or gt/{metal}_Mines_{timestamp}.csv)
        metal: Optional metal filter (e.g., "copper", "Cu", "lithium", "REE", "battery metals")
        company: Optional company filter (e.g., "BHP", "Rio Tinto", "MP Materials")

    Returns:
        Number of facilities exported
    """
    # Check if metal filter is a basket query
    if metal and is_basket_search(metal):
        basket_metals = resolve_basket_to_metals(metal)
        if basket_metals:
            print(f"[Basket Query] Expanding '{metal}' to {len(basket_metals)} metals:")
            print(f"  {', '.join(basket_metals[:10])}")
            if len(basket_metals) > 10:
                print(f"  ... and {len(basket_metals) - 10} more")

    # Check if company filter is provided
    if company:
        print(f"[Company Filter] Filtering for facilities operated/owned by '{company}'")

    base_dir = Path(__file__).parent.parent / "facilities"

    if not base_dir.exists():
        print(f"Error: Facilities directory not found: {base_dir}")
        return 0

    # Set output file with timestamp
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gt_dir = Path(__file__).parent.parent / "gt"
        gt_dir.mkdir(exist_ok=True)

        parts = []
        if company:
            company_slug = company.lower().replace(' ', '_').replace(',', '')
            parts.append(company_slug)
        if metal:
            metal_info = normalize_metal(metal)
            metal_slug = metal_info['name'] if metal_info else metal
            metal_slug = metal_slug.replace(' ', '_')
            parts.append(metal_slug)

        if parts:
            output_file = gt_dir / f"{'_'.join(parts)}_Mines_{timestamp}.csv"
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

                    # Apply company filter if specified
                    if company and not facility_has_company(facility, company):
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

    print(f"\n[OK] Exported {len(all_facilities)} facilities from {len(country_counts)} countries to {output_path}")
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
               "  %(prog)s --all --metal lithium -o lithium_mines.csv\n"
               "  %(prog)s --all --company \"BHP\"\n"
               "  %(prog)s --all --company \"Rio Tinto\" --metal copper",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "country",
        nargs="?",
        help="Country name, ISO3 code, or company name. Use --company flag for company filtering."
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
        help="Filter by metal/commodity or basket. "
             "Examples: 'copper', 'Cu', 'lithium' (individual) | "
             "'REE', 'rare earths', 'PGM', 'battery metals', 'base metals' (baskets). "
             "Uses EntityIdentity for normalization and basket expansion."
    )
    parser.add_argument(
        "--company",
        help="Filter by company (operator or owner). "
             "Examples: 'BHP', 'Rio Tinto', 'MP Materials'. "
             "Uses fuzzy matching to handle variations."
    )

    args = parser.parse_args()

    # Handle different invocation patterns
    # Pattern 1: --all with optional filters
    if args.all:
        count = export_all_to_csv(args.output, metal=args.metal, company=args.company)
    # Pattern 2: Country with optional filters
    elif args.country and not args.company:
        count = export_country_to_csv(args.country, args.output, metal=args.metal, company=None)
    # Pattern 3: --company flag (implies --all if no country)
    elif args.company and not args.country:
        count = export_all_to_csv(args.output, metal=args.metal, company=args.company)
    # Pattern 4: Country + --company flag
    elif args.country and args.company:
        count = export_country_to_csv(args.country, args.output, metal=args.metal, company=args.company)
    else:
        parser.error("Either provide a country name or use --all flag")

    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
