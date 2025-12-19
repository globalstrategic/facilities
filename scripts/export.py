#!/usr/bin/env python3
"""
Unified export for facilities database.

Supports multiple output formats:
- parquet: Efficient columnar storage with relationship tables
- csv: Mines.csv format for compatibility with external tools

Examples:
    # Export all facilities to parquet (default)
    python scripts/export.py

    # Export to CSV (Mines.csv format)
    python scripts/export.py --format csv

    # Export single country to CSV
    python scripts/export.py --format csv --country Chile

    # Filter by metal or company
    python scripts/export.py --format csv --metal lithium
    python scripts/export.py --format csv --company "BHP"
    python scripts/export.py --format csv --all --metal REE
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.country_utils import normalize_country_to_iso3, iso3_to_country_name
from scripts.utils.facility_loader import (
    load_all_facilities_list,
    load_facilities_from_country,
    get_facilities_dir,
)

# Try to import metal_identifier from entityidentity
try:
    from entityidentity import metal_identifier
    METAL_IDENTIFIER_AVAILABLE = True

    # Load metals database to support basket queries
    entity_path = Path(__file__).parent.parent.parent / "entityidentity"
    metals_parquet = entity_path / "entityidentity" / "metals" / "data" / "metals.parquet"
    if metals_parquet.exists():
        METALS_DB = pd.read_parquet(metals_parquet)
    else:
        METALS_DB = None
except ImportError:
    METAL_IDENTIFIER_AVAILABLE = False
    METALS_DB = None


# =============================================================================
# Shared Utilities
# =============================================================================

def load_all_facilities() -> Tuple[List[Dict], int]:
    """Load all facility JSON files from the facilities directory.

    Returns:
        Tuple of (facilities list, error count)
    """
    facilities_dir = Path(__file__).parent.parent / "facilities"
    facilities = []
    errors = 0

    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith('.'):
            continue

        for fac_file in sorted(country_dir.glob('*-fac.json')):
            try:
                with open(fac_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)
                    facilities.append(facility)
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"Warning: Skipping {fac_file.name}: {str(e)[:60]}", file=sys.stderr)

    return facilities, errors


def load_country_facilities(country: str, metal: Optional[str] = None,
                           company: Optional[str] = None) -> Tuple[List[Dict], str]:
    """Load facilities for a specific country with optional filters.

    Returns:
        Tuple of (facilities list, country_name)
    """
    iso3 = normalize_country_to_iso3(country)
    if not iso3:
        print(f"Error: Could not resolve country '{country}'")
        return [], ""

    country_name = iso3_to_country_name(iso3)
    base_dir = Path(__file__).parent.parent / "facilities"
    country_dir = None

    # Try ISO3 first (preferred)
    if (base_dir / iso3).exists():
        country_dir = base_dir / iso3
    else:
        # Try ISO2 for legacy directories
        try:
            import pycountry
            country_obj = pycountry.countries.get(alpha_3=iso3)
            if country_obj:
                iso2 = country_obj.alpha_2
                if (base_dir / iso2).exists():
                    country_dir = base_dir / iso2
        except ImportError:
            pass

    if not country_dir or not country_dir.exists():
        print(f"Error: No facilities directory found for {iso3}")
        return [], country_name

    facilities = []
    for json_file in country_dir.glob("*.json"):
        try:
            with open(json_file) as f:
                facility = json.load(f)

                # Apply filters
                if metal and not facility_has_metal(facility, metal):
                    continue
                if company and not facility_has_company(facility, company):
                    continue

                facilities.append(facility)
        except Exception as e:
            print(f"Warning: Error loading {json_file}: {e}", file=sys.stderr)

    return facilities, country_name


# =============================================================================
# Metal Filtering (for CSV export)
# =============================================================================

BASKET_MAPPINGS = {
    'ree': 'ree', 'rare earth': 'ree', 'rare earths': 'ree',
    'rare-earth': 'ree', 'rare-earths': 'ree',
    'pgm': 'pgm', 'pgms': 'pgm', 'platinum group': 'pgm',
    'platinum group metal': 'pgm', 'platinum group metals': 'pgm',
    'base': 'base', 'base metal': 'base', 'base metals': 'base',
    'battery': 'battery', 'battery metal': 'battery', 'battery metals': 'battery',
    'precious': 'precious', 'precious metal': 'precious', 'precious metals': 'precious',
    'ferroalloy': 'ferroalloy', 'ferroalloys': 'ferroalloy', 'ferro': 'ferroalloy',
    'specialty': 'specialty', 'specialty metal': 'specialty', 'specialty metals': 'specialty',
    'industrial': 'industrial',
    'nuclear': 'nuclear',
}


def normalize_metal(metal_name: str) -> Optional[Dict]:
    """Normalize metal name using EntityIdentity."""
    if not METAL_IDENTIFIER_AVAILABLE:
        return {'name': metal_name.lower(), 'category': None, 'category_bucket': None}

    try:
        result = metal_identifier(metal_name)
        if result and result.get('category_bucket'):
            return {
                'name': result.get('name', metal_name).lower(),
                'category': result.get('category'),
                'category_bucket': result.get('category_bucket'),
                'symbol': result.get('symbol')
            }
    except Exception:
        pass

    return {'name': metal_name.lower(), 'category': None, 'category_bucket': None}


def is_basket_search(metal_name: str) -> bool:
    """Check if search term is a basket/group query."""
    return metal_name.lower().strip() in BASKET_MAPPINGS


def resolve_basket_to_metals(basket_term: str) -> Optional[List[str]]:
    """Resolve basket term to list of individual metal names."""
    if not METAL_IDENTIFIER_AVAILABLE or METALS_DB is None:
        return None

    category_bucket = BASKET_MAPPINGS.get(basket_term.lower().strip())
    if not category_bucket:
        return None

    metals_in_basket = METALS_DB[METALS_DB['category_bucket'] == category_bucket]
    if metals_in_basket.empty:
        return None

    return metals_in_basket['name_norm'].tolist()


def facility_has_metal(facility: Dict, target_metal: str) -> bool:
    """Check if facility produces the target metal or any metal in a basket."""
    commodities = facility.get("commodities", [])
    if not commodities:
        return False

    # Normalize facility metals
    facility_metals = []
    for comm in commodities:
        metal = comm.get("metal")
        if metal:
            metal_info = normalize_metal(metal)
            if metal_info:
                facility_metals.append(metal_info)

    if not facility_metals:
        return False

    # Basket query
    if is_basket_search(target_metal):
        target_bucket = BASKET_MAPPINGS.get(target_metal.lower().strip())
        if target_bucket:
            for metal_info in facility_metals:
                if metal_info.get('category_bucket') == target_bucket:
                    return True
        return False

    # Individual metal search
    target_info = normalize_metal(target_metal)
    if not target_info:
        return False

    normalized_target = target_info['name']
    target_symbol = target_info.get('symbol')

    for metal_info in facility_metals:
        facility_metal_name = metal_info['name']
        if normalized_target in facility_metal_name or facility_metal_name in normalized_target:
            return True
        if target_symbol:
            if metal_info.get('symbol') == target_symbol:
                return True

    return False


# =============================================================================
# Company Filtering (for CSV export)
# =============================================================================

def get_companies(facility: Dict) -> List[str]:
    """Extract company names from facility (all sources)."""
    companies = []

    # Operator link
    if facility.get("operator_link") and facility["operator_link"].get("name"):
        companies.append(facility["operator_link"]["name"])

    # Owner links
    if facility.get("owner_links"):
        for owner in facility["owner_links"]:
            if owner.get("name") and owner["name"] not in companies:
                companies.append(owner["name"])

    # Company mentions
    if facility.get("company_mentions"):
        for mention in facility["company_mentions"]:
            if isinstance(mention, str) and mention and mention not in companies:
                companies.append(mention)
            elif isinstance(mention, dict):
                if mention.get("name") and mention["name"] not in companies:
                    companies.append(mention["name"])

    # Legacy fields
    if facility.get("operator") and facility["operator"] not in companies:
        companies.append(facility["operator"])
    if facility.get("owner") and facility["owner"] not in companies:
        companies.append(facility["owner"])

    return companies


def facility_has_company(facility: Dict, target_company: str) -> bool:
    """Check if facility is operated by or owned by the target company (fuzzy matching)."""
    companies = get_companies(facility)
    if not companies:
        return False

    target_lower = target_company.lower().strip()

    # Exact match
    for company in companies:
        if company.lower() == target_lower:
            return True

    # Substring match
    for company in companies:
        company_lower = company.lower()
        if target_lower in company_lower or company_lower in target_lower:
            return True

    # Word-based match
    target_words = set(target_lower.split())
    if target_words:
        for company in companies:
            company_words = set(company.lower().split())
            if target_words.issubset(company_words) or company_words.issubset(target_words):
                return True

    return False


# =============================================================================
# Parquet Export
# =============================================================================

def flatten_facility_for_parquet(facility: Dict) -> Dict:
    """Flatten a facility JSON into a single-row dictionary for DataFrame."""
    row = {
        'facility_id': facility.get('facility_id', ''),
        'name': facility.get('name', ''),
        'country_iso3': facility.get('country_iso3', ''),
        'status': facility.get('status', ''),
    }

    # Location
    location = facility.get('location', {})
    row['latitude'] = location.get('lat')
    row['longitude'] = location.get('lon')
    row['location_precision'] = location.get('precision', '')
    row['location_address'] = location.get('address', '')
    row['location_region'] = location.get('region', '')

    # Types
    types = facility.get('types', [])
    row['types'] = ','.join(types) if types else ''

    # Commodities
    commodities = facility.get('commodities', [])
    primary_commodities = [c.get('metal', '') for c in commodities if c.get('primary', False)]
    secondary_commodities = [c.get('metal', '') for c in commodities if not c.get('primary', False)]
    row['primary_commodity'] = primary_commodities[0] if primary_commodities else ''
    row['secondary_commodities'] = ','.join(secondary_commodities) if secondary_commodities else ''
    row['all_commodities'] = ','.join([c.get('metal', '') for c in commodities])

    # Products
    products = facility.get('products', [])
    row['products'] = ','.join(products) if products else ''

    # Companies
    company_mentions = facility.get('company_mentions', [])
    if company_mentions:
        mentions_list = []
        for mention in company_mentions:
            if isinstance(mention, str):
                mentions_list.append(mention)
            elif isinstance(mention, dict) and mention.get('name'):
                mentions_list.append(mention['name'])
        row['company_mentions'] = ','.join(mentions_list)
    else:
        row['company_mentions'] = ''

    operator_link = facility.get('operator_link')
    row['operator_name'] = operator_link.get('name', '') if operator_link else ''
    row['operator_lei'] = operator_link.get('lei', '') if operator_link else ''

    owner_links = facility.get('owner_links', [])
    row['owner_names'] = ','.join([o.get('name', '') for o in owner_links if o.get('name')])
    row['owner_leis'] = ','.join([o.get('lei', '') for o in owner_links if o.get('lei')])

    # Aliases
    aliases = facility.get('aliases', [])
    row['aliases'] = ','.join(aliases) if aliases else ''

    # Verification
    verification = facility.get('verification', {})
    row['confidence'] = verification.get('confidence')
    row['verified_date'] = verification.get('date', '')
    row['verification_method'] = verification.get('method', '')

    # Capacity
    capacity = facility.get('capacity', {})
    row['capacity_value'] = capacity.get('value')
    row['capacity_unit'] = capacity.get('unit', '')
    row['capacity_commodity'] = capacity.get('commodity', '')

    # Dates
    row['commissioned_date'] = facility.get('commissioned_date', '')
    row['closure_date'] = facility.get('closure_date', '')

    # Sources
    sources = facility.get('sources', [])
    row['source_count'] = len(sources)
    source_types = [s.get('type', '') for s in sources if s.get('type')]
    row['source_types'] = ','.join(set(source_types)) if source_types else ''

    # Notes
    notes = facility.get('notes', '')
    row['notes'] = notes[:500] if notes else ''

    return row


def parse_facility_materials(df: pd.DataFrame) -> pd.DataFrame:
    """Parse commodity data into normalized relationship table."""
    relationships = []

    for _, row in df.iterrows():
        facility_id = row['facility_id']
        all_commodities = row.get('all_commodities', '')
        primary_commodity = row.get('primary_commodity', '')

        if pd.notna(all_commodities) and all_commodities:
            commodities = [c.strip() for c in str(all_commodities).split(',') if c.strip()]
            for commodity in commodities:
                relationships.append({
                    'facility_id': facility_id,
                    'material_name': commodity,
                    'is_primary': (commodity == primary_commodity)
                })

    return pd.DataFrame(relationships)


def parse_facility_companies(df: pd.DataFrame) -> pd.DataFrame:
    """Parse company data into normalized relationship table."""
    relationships = []

    for _, row in df.iterrows():
        facility_id = row['facility_id']

        # Operator
        operator_name = row.get('operator_name', '')
        operator_lei = row.get('operator_lei', '')
        if pd.notna(operator_name) and operator_name:
            relationships.append({
                'facility_id': facility_id,
                'company_name': operator_name,
                'relationship_type': 'operator',
                'lei': operator_lei if pd.notna(operator_lei) else ''
            })

        # Owners
        owner_names = row.get('owner_names', '')
        owner_leis = row.get('owner_leis', '')
        if pd.notna(owner_names) and owner_names:
            names = [n.strip() for n in str(owner_names).split(',') if n.strip()]
            leis = [l.strip() for l in str(owner_leis).split(',') if l.strip()] if pd.notna(owner_leis) and owner_leis else []
            for i, name in enumerate(names):
                relationships.append({
                    'facility_id': facility_id,
                    'company_name': name,
                    'relationship_type': 'owner',
                    'lei': leis[i] if i < len(leis) else ''
                })

        # Mentions
        company_mentions = row.get('company_mentions', '')
        if pd.notna(company_mentions) and company_mentions:
            mentions = [m.strip() for m in str(company_mentions).split(',') if m.strip()]
            for mention in mentions:
                if mention != operator_name and (not pd.notna(owner_names) or mention not in str(owner_names)):
                    relationships.append({
                        'facility_id': facility_id,
                        'company_name': mention,
                        'relationship_type': 'mention',
                        'lei': ''
                    })

    return pd.DataFrame(relationships)


def export_parquet(output_dir: str = '.', preview: bool = False):
    """Export all facilities to parquet files."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print("Loading facilities...")
    facilities, errors = load_all_facilities()
    print(f"Loaded {len(facilities):,} facilities", end='')
    if errors > 0:
        print(f" (skipped {errors} corrupted files)")
    else:
        print()

    print("\nFlattening to tabular format...")
    rows = [flatten_facility_for_parquet(fac) for fac in facilities]

    print("Creating DataFrame...")
    df = pd.DataFrame(rows)

    # Column order
    column_order = [
        'facility_id', 'name', 'country_iso3', 'status',
        'latitude', 'longitude', 'location_precision', 'location_address', 'location_region',
        'types', 'primary_commodity', 'secondary_commodities', 'all_commodities', 'products',
        'company_mentions', 'operator_name', 'operator_lei', 'owner_names', 'owner_leis',
        'aliases', 'confidence', 'verified_date', 'verification_method',
        'capacity_value', 'capacity_unit', 'capacity_commodity',
        'commissioned_date', 'closure_date',
        'source_count', 'source_types', 'notes'
    ]
    df = df[column_order]

    # Convert dtypes
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')
    df['capacity_value'] = pd.to_numeric(df['capacity_value'], errors='coerce')
    df['source_count'] = df['source_count'].astype('int32')

    # Save facilities
    facilities_file = output_path / 'facilities.parquet'
    print(f"\n[1/3] Writing {facilities_file}...")
    df.to_parquet(facilities_file, index=False, engine='pyarrow', compression='snappy')
    fac_size = facilities_file.stat().st_size / (1024 * 1024)
    print(f"      ✓ {len(df):,} facilities ({fac_size:.2f} MB)")

    # Save materials
    print(f"\n[2/3] Parsing facility-material relationships...")
    materials_df = parse_facility_materials(df)
    materials_file = output_path / 'facility_materials.parquet'
    materials_df.to_parquet(materials_file, index=False, engine='pyarrow', compression='snappy')
    mat_size = materials_file.stat().st_size / (1024 * 1024)
    print(f"      ✓ {len(materials_df):,} relationships ({mat_size:.2f} MB)")

    # Save companies
    print(f"\n[3/3] Parsing facility-company relationships...")
    companies_df = parse_facility_companies(df)
    companies_file = output_path / 'facility_companies.parquet'
    companies_df.to_parquet(companies_file, index=False, engine='pyarrow', compression='snappy')
    comp_size = companies_file.stat().st_size / (1024 * 1024)
    print(f"      ✓ {len(companies_df):,} relationships ({comp_size:.2f} MB)")

    # Summary
    total_size = fac_size + mat_size + comp_size
    print(f"\n{'='*60}")
    print(f"EXPORT COMPLETE")
    print(f"{'='*60}")
    print(f"Output directory: {output_path.absolute()}")
    print(f"\nFiles created:")
    print(f"  • facilities.parquet          {len(df):>6,} rows  ({fac_size:>5.2f} MB)")
    print(f"  • facility_materials.parquet  {len(materials_df):>6,} rows  ({mat_size:>5.2f} MB)")
    print(f"  • facility_companies.parquet  {len(companies_df):>6,} rows  ({comp_size:>5.2f} MB)")
    print(f"\nTotal: {total_size:.2f} MB")
    print(f"\nFacilities: {len(df):,}")
    print(f"  • With coordinates: {df['latitude'].notna().sum():,} ({df['latitude'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  • Countries: {df['country_iso3'].nunique()}")
    print(f"\nMaterials: {materials_df['material_name'].nunique():,} unique")
    print(f"  • Primary: {materials_df['is_primary'].sum():,}")
    print(f"  • Secondary: {(~materials_df['is_primary']).sum():,}")
    print(f"\nCompanies: {companies_df['company_name'].nunique():,} unique")
    print(f"  • Operators: {(companies_df['relationship_type'] == 'operator').sum():,}")
    print(f"  • Owners: {(companies_df['relationship_type'] == 'owner').sum():,}")
    print(f"  • Mentions: {(companies_df['relationship_type'] == 'mention').sum():,}")

    if preview:
        print("\n" + "="*60)
        print("FACILITIES PREVIEW")
        print("="*60)
        print(df[['facility_id', 'name', 'country_iso3', 'primary_commodity', 'latitude', 'longitude']].head().to_string())

        print("\n" + "="*60)
        print("MATERIALS PREVIEW")
        print("="*60)
        print(materials_df.head(10).to_string())

        print("\n" + "="*60)
        print("COMPANIES PREVIEW")
        print("="*60)
        print(companies_df.head(10).to_string())

    return df, materials_df, companies_df


# =============================================================================
# CSV Export (Mines.csv format)
# =============================================================================

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


def get_asset_types(facility: Dict) -> str:
    """Get asset type from facility types field."""
    types = facility.get("types", [])
    if not types:
        return "Mine"

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


def get_commodities(facility: Dict) -> Tuple[Optional[str], Optional[str], List[str]]:
    """Extract primary, secondary, and other commodities."""
    commodities = facility.get("commodities", [])
    if not commodities:
        return None, None, []

    primary = None
    secondary = None
    others = []

    for comm in commodities:
        if comm.get("primary"):
            primary = comm.get("metal")
            break

    remaining = [c.get("metal") for c in commodities if c.get("metal") and c.get("metal") != primary]
    if remaining:
        secondary = remaining[0]
        others = remaining[1:]

    return primary, secondary, others


def facility_to_csv_row(facility: Dict, country_name: str) -> Dict[str, str]:
    """Convert facility JSON to Mines.csv row format."""
    location = facility.get("location", {})
    lat = location.get("lat") or ""
    lon = location.get("lon") or ""

    verification = facility.get("verification", {})
    confidence = verification.get("confidence", 0.5)
    confidence_label = get_confidence_label(confidence)

    companies = get_companies(facility)
    company_str = ";".join(companies) if companies else ""

    primary, secondary, others = get_commodities(facility)
    other_str = ",".join(others) if others else ""

    name = facility.get("name", "")
    aliases = facility.get("aliases", [])
    all_names = [name] + [a for a in aliases if a and a != name]
    mine_name = ";".join(all_names) if all_names else name

    return {
        "Confidence Factor": confidence_label,
        "Mine Name": mine_name,
        "Company Name(s)": company_str,
        "Latitude": str(lat) if lat else "",
        "Longitude": str(lon) if lon else "",
        "Asset Type": get_asset_types(facility),
        "Country or Region": country_name,
        "Primary Commodity": primary or "",
        "Secondary Commodity": secondary or "",
        "Other Commodities": other_str,
    }


def export_csv(output_file: Optional[str] = None, country: Optional[str] = None,
               metal: Optional[str] = None, company: Optional[str] = None,
               export_all: bool = False) -> int:
    """Export facilities to Mines.csv format."""

    # Print filter info
    if metal and is_basket_search(metal):
        basket_metals = resolve_basket_to_metals(metal)
        if basket_metals:
            print(f"[Basket Query] Expanding '{metal}' to {len(basket_metals)} metals:")
            print(f"  {', '.join(basket_metals[:10])}")
            if len(basket_metals) > 10:
                print(f"  ... and {len(basket_metals) - 10} more")

    if company:
        print(f"[Company Filter] Filtering for facilities operated/owned by '{company}'")

    CSV_FIELDS = [
        "Confidence Factor", "Mine Name", "Company Name(s)",
        "Latitude", "Longitude", "Asset Type", "Country or Region",
        "Primary Commodity", "Secondary Commodity", "Other Commodities",
    ]

    # Single country export
    if country and not export_all:
        facilities, country_name = load_country_facilities(country, metal, company)
        if not facilities:
            filters = []
            if metal:
                filters.append(f"metal: {metal}")
            if company:
                filters.append(f"company: {company}")
            filter_msg = f" with filters ({', '.join(filters)})" if filters else ""
            print(f"No facilities{filter_msg} found for {country}")
            return 0

        # Generate output filename
        if not output_file:
            iso3 = normalize_country_to_iso3(country)
            parts = [iso3]
            if company:
                parts.append(company.lower().replace(' ', '_').replace(',', ''))
            if metal:
                metal_info = normalize_metal(metal)
                parts.append((metal_info['name'] if metal_info else metal).replace(' ', '_'))
            parts.append("mines.csv")
            output_file = "_".join(parts)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for facility in facilities:
                row = facility_to_csv_row(facility, country_name)
                writer.writerow(row)

        print(f"[OK] Exported {len(facilities)} facilities to {output_file}")
        return len(facilities)

    # All countries export
    base_dir = Path(__file__).parent.parent / "facilities"
    if not base_dir.exists():
        print(f"Error: Facilities directory not found: {base_dir}")
        return 0

    # Generate output filename
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gt_dir = Path(__file__).parent.parent / "gt"
        gt_dir.mkdir(exist_ok=True)

        parts = []
        if company:
            parts.append(company.lower().replace(' ', '_').replace(',', ''))
        if metal:
            metal_info = normalize_metal(metal)
            parts.append((metal_info['name'] if metal_info else metal).replace(' ', '_'))

        if parts:
            output_file = gt_dir / f"{'_'.join(parts)}_Mines_{timestamp}.csv"
        else:
            output_file = gt_dir / f"Mines_{timestamp}.csv"

    output_path = Path(output_file)

    # Collect all facilities
    all_facilities = []
    country_counts = {}

    print("Scanning all country directories...")

    for country_dir in sorted(base_dir.iterdir()):
        if not country_dir.is_dir():
            continue

        dir_name = country_dir.name
        iso3 = normalize_country_to_iso3(dir_name)

        if not iso3:
            print(f"Warning: Could not resolve country for directory '{dir_name}'", file=sys.stderr)
            country_name = dir_name
        else:
            country_name = iso3_to_country_name(iso3)

        facility_count = 0
        for json_file in country_dir.glob("*.json"):
            try:
                with open(json_file) as f:
                    facility = json.load(f)

                    if metal and not facility_has_metal(facility, metal):
                        continue
                    if company and not facility_has_company(facility, company):
                        continue

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

    print(f"\nWriting to {output_path}...")

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()

        for facility in all_facilities:
            country_name = facility.pop('_export_country_name', 'Unknown')
            row = facility_to_csv_row(facility, country_name)
            writer.writerow(row)

    print(f"\n[OK] Exported {len(all_facilities)} facilities from {len(country_counts)} countries to {output_path}")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")

    return len(all_facilities)


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export facilities database to various formats",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export to parquet (default)
  python scripts/export.py
  python scripts/export.py --format parquet --output output/

  # Export to CSV (Mines.csv format)
  python scripts/export.py --format csv --all
  python scripts/export.py --format csv --country Chile
  python scripts/export.py --format csv --all --metal lithium
  python scripts/export.py --format csv --all --company "BHP"
  python scripts/export.py --format csv --all --metal REE --company "MP Materials"
        """
    )

    parser.add_argument(
        '--format', '-f',
        choices=['parquet', 'csv'],
        default='parquet',
        help='Output format: parquet (default) or csv (Mines.csv format)'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file or directory'
    )

    parser.add_argument(
        '--preview', '-p',
        action='store_true',
        help='Show preview of data after export (parquet only)'
    )

    # CSV-specific options
    parser.add_argument(
        '--country', '-c',
        help='Country to export (CSV only). Use country name or ISO3 code.'
    )

    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Export all countries (CSV only)'
    )

    parser.add_argument(
        '--metal', '-m',
        help='Filter by metal/commodity or basket (CSV only). '
             'Examples: copper, lithium, REE, "battery metals"'
    )

    parser.add_argument(
        '--company',
        help='Filter by company (CSV only). Uses fuzzy matching. '
             'Examples: BHP, "Rio Tinto", "MP Materials"'
    )

    args = parser.parse_args()

    if args.format == 'parquet':
        output_dir = args.output or '.'
        export_parquet(output_dir, preview=args.preview)
        return 0

    elif args.format == 'csv':
        # Need either --country or --all for CSV
        if not args.country and not args.all and not args.company:
            parser.error("CSV export requires --country, --all, or --company")

        # If only --company specified, imply --all
        export_all = args.all or (args.company and not args.country)

        count = export_csv(
            output_file=args.output,
            country=args.country,
            metal=args.metal,
            company=args.company,
            export_all=export_all
        )
        return 0 if count > 0 else 1


if __name__ == '__main__':
    sys.exit(main())
