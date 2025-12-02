#!/usr/bin/env python3
"""
Export facilities database to parquet format.

Creates a flat parquet file with all facility data for efficient querying and analysis.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd


def load_all_facilities() -> tuple[List[Dict], int]:
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
                # Only print first few errors
                if errors <= 5:
                    print(f"Warning: Skipping {fac_file.name}: {str(e)[:60]}", file=sys.stderr)

    return facilities, errors


def flatten_facility(facility: Dict) -> Dict:
    """Flatten a facility JSON into a single-row dictionary for DataFrame."""

    # Basic fields
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

    # Types (concatenate as string)
    types = facility.get('types', [])
    row['types'] = ','.join(types) if types else ''

    # Primary commodity
    commodities = facility.get('commodities', [])
    primary_commodities = [c.get('metal', '') for c in commodities if c.get('primary', False)]
    secondary_commodities = [c.get('metal', '') for c in commodities if not c.get('primary', False)]

    row['primary_commodity'] = primary_commodities[0] if primary_commodities else ''
    row['secondary_commodities'] = ','.join(secondary_commodities) if secondary_commodities else ''
    row['all_commodities'] = ','.join([c.get('metal', '') for c in commodities])

    # Products
    products = facility.get('products', [])
    row['products'] = ','.join(products) if products else ''

    # Companies (handle both string and dict formats)
    company_mentions = facility.get('company_mentions', [])
    if company_mentions:
        mentions_list = []
        for mention in company_mentions:
            if isinstance(mention, str):
                mentions_list.append(mention)
            elif isinstance(mention, dict):
                name = mention.get('name', '')
                if name:
                    mentions_list.append(name)
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

    # Sources (count and types)
    sources = facility.get('sources', [])
    row['source_count'] = len(sources)
    source_types = [s.get('type', '') for s in sources if s.get('type')]
    row['source_types'] = ','.join(set(source_types)) if source_types else ''

    # Notes
    notes = facility.get('notes', '')
    row['notes'] = notes[:500] if notes else ''  # Truncate long notes

    return row


def export_to_parquet(output_dir: str = '.'):
    """
    Export all facilities to parquet files.

    Creates three parquet files:
    - facilities.parquet: Main facility data
    - facility_materials.parquet: Facility-material relationships
    - facility_companies.parquet: Facility-company relationships

    Args:
        output_dir: Output directory for parquet files (default: current directory)
    """
    from pathlib import Path
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
    rows = [flatten_facility(fac) for fac in facilities]

    print("Creating DataFrame...")
    df = pd.DataFrame(rows)

    # Ensure consistent column order
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

    # Convert to appropriate dtypes
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')
    df['capacity_value'] = pd.to_numeric(df['capacity_value'], errors='coerce')
    df['source_count'] = df['source_count'].astype('int32')

    # Save facilities parquet
    facilities_file = output_path / 'facilities.parquet'
    print(f"\n[1/3] Writing {facilities_file}...")
    df.to_parquet(facilities_file, index=False, engine='pyarrow', compression='snappy')
    fac_size = facilities_file.stat().st_size / (1024 * 1024)
    print(f"      ✓ {len(df):,} facilities ({fac_size:.2f} MB)")

    # Parse and save materials relationships
    print(f"\n[2/3] Parsing facility-material relationships...")
    from export_relationships_parquet import parse_facility_materials
    materials_df = parse_facility_materials(df)
    materials_file = output_path / 'facility_materials.parquet'
    materials_df.to_parquet(materials_file, index=False, engine='pyarrow', compression='snappy')
    mat_size = materials_file.stat().st_size / (1024 * 1024)
    print(f"      ✓ {len(materials_df):,} relationships ({mat_size:.2f} MB)")

    # Parse and save company relationships
    print(f"\n[3/3] Parsing facility-company relationships...")
    from export_relationships_parquet import parse_facility_companies
    companies_df = parse_facility_companies(df)
    companies_file = output_path / 'facility_companies.parquet'
    companies_df.to_parquet(companies_file, index=False, engine='pyarrow', compression='snappy')
    comp_size = companies_file.stat().st_size / (1024 * 1024)
    print(f"      ✓ {len(companies_df):,} relationships ({comp_size:.2f} MB)")

    # Print summary
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

    return df, materials_df, companies_df


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Export facilities database to parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export to current directory (creates facilities.parquet, etc.)
  python scripts/export_to_parquet.py

  # Export to specific directory
  python scripts/export_to_parquet.py --output output/parquet

  # Export and show preview
  python scripts/export_to_parquet.py --preview
        """
    )

    parser.add_argument(
        '--output', '-o',
        default='.',
        help='Output directory for parquet files (default: current directory)'
    )

    parser.add_argument(
        '--preview', '-p',
        action='store_true',
        help='Show preview of the data after export'
    )

    args = parser.parse_args()

    # Export
    df, materials_df, companies_df = export_to_parquet(args.output)

    # Show preview if requested
    if args.preview:
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

    return 0


if __name__ == '__main__':
    sys.exit(main())
