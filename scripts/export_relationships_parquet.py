#!/usr/bin/env python3
"""
Export facility relationships (materials and companies) to separate parquet files.

This creates normalized relationship tables from the facilities.parquet file:
- facility_materials.parquet: facility_id -> material mappings
- facility_companies.parquet: facility_id -> company mappings
"""

import sys
from pathlib import Path
import pandas as pd


def parse_facility_materials(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse commodity data from facilities into normalized relationship table.

    Returns DataFrame with columns: facility_id, material_name, is_primary
    """
    relationships = []

    for _, row in df.iterrows():
        facility_id = row['facility_id']

        # Get all commodities
        all_commodities = row.get('all_commodities', '')
        primary_commodity = row.get('primary_commodity', '')

        if pd.notna(all_commodities) and all_commodities:
            # Split on comma and clean
            commodities = [c.strip() for c in str(all_commodities).split(',') if c.strip()]

            for commodity in commodities:
                relationships.append({
                    'facility_id': facility_id,
                    'material_name': commodity,
                    'is_primary': (commodity == primary_commodity)
                })

    return pd.DataFrame(relationships)


def parse_facility_companies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse company data from facilities into normalized relationship table.

    Returns DataFrame with columns: facility_id, company_name, relationship_type, lei
    """
    relationships = []

    for _, row in df.iterrows():
        facility_id = row['facility_id']

        # Operator (single company)
        operator_name = row.get('operator_name', '')
        operator_lei = row.get('operator_lei', '')
        if pd.notna(operator_name) and operator_name:
            relationships.append({
                'facility_id': facility_id,
                'company_name': operator_name,
                'relationship_type': 'operator',
                'lei': operator_lei if pd.notna(operator_lei) else ''
            })

        # Owners (multiple companies, comma-separated)
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

        # Company mentions (unresolved mentions)
        company_mentions = row.get('company_mentions', '')
        if pd.notna(company_mentions) and company_mentions:
            mentions = [m.strip() for m in str(company_mentions).split(',') if m.strip()]

            for mention in mentions:
                # Only add if not already in as operator or owner
                if mention != operator_name and (not pd.notna(owner_names) or mention not in str(owner_names)):
                    relationships.append({
                        'facility_id': facility_id,
                        'company_name': mention,
                        'relationship_type': 'mention',
                        'lei': ''
                    })

    return pd.DataFrame(relationships)


def export_relationships(
    facilities_file: str = 'facilities.parquet',
    materials_output: str = 'facility_materials.parquet',
    companies_output: str = 'facility_companies.parquet'
):
    """Export facility relationships to separate parquet files."""

    print(f"Loading {facilities_file}...")
    df = pd.read_parquet(facilities_file)
    print(f"Loaded {len(df):,} facilities")

    # Parse materials
    print("\nParsing facility-material relationships...")
    materials_df = parse_facility_materials(df)
    print(f"Found {len(materials_df):,} facility-material relationships")
    print(f"  Primary: {materials_df['is_primary'].sum():,}")
    print(f"  Secondary: {(~materials_df['is_primary']).sum():,}")
    print(f"  Unique materials: {materials_df['material_name'].nunique():,}")

    # Save materials
    materials_df.to_parquet(materials_output, index=False)
    print(f"✓ Saved to {materials_output}")

    # Parse companies
    print("\nParsing facility-company relationships...")
    companies_df = parse_facility_companies(df)
    print(f"Found {len(companies_df):,} facility-company relationships")
    print(f"  Operators: {(companies_df['relationship_type'] == 'operator').sum():,}")
    print(f"  Owners: {(companies_df['relationship_type'] == 'owner').sum():,}")
    print(f"  Mentions: {(companies_df['relationship_type'] == 'mention').sum():,}")
    print(f"  Unique companies: {companies_df['company_name'].nunique():,}")
    print(f"  With LEI: {(companies_df['lei'] != '').sum():,}")

    # Save companies
    companies_df.to_parquet(companies_output, index=False)
    print(f"✓ Saved to {companies_output}")

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Facilities: {len(df):,}")
    print(f"Facility-Material relationships: {len(materials_df):,}")
    print(f"Facility-Company relationships: {len(companies_df):,}")
    print(f"\nFiles created:")
    print(f"  - {materials_output}")
    print(f"  - {companies_output}")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Export facility relationships to parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export using default files
  python scripts/export_relationships_parquet.py

  # Specify input file
  python scripts/export_relationships_parquet.py --input facilities.parquet

  # Specify output files
  python scripts/export_relationships_parquet.py \\
    --materials output/materials.parquet \\
    --companies output/companies.parquet
        """
    )

    parser.add_argument(
        '--input', '-i',
        default='facilities.parquet',
        help='Input facilities parquet file (default: facilities.parquet)'
    )

    parser.add_argument(
        '--materials', '-m',
        default='facility_materials.parquet',
        help='Output materials parquet file (default: facility_materials.parquet)'
    )

    parser.add_argument(
        '--companies', '-c',
        default='facility_companies.parquet',
        help='Output companies parquet file (default: facility_companies.parquet)'
    )

    args = parser.parse_args()

    export_relationships(
        facilities_file=args.input,
        materials_output=args.materials,
        companies_output=args.companies
    )

    return 0


if __name__ == '__main__':
    sys.exit(main())
