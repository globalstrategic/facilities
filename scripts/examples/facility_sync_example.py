#!/usr/bin/env python3
"""
Example usage of FacilitySyncManager for bidirectional sync with entityidentity.

This script demonstrates:
1. Exporting facilities to entityidentity parquet format
2. Importing facilities from entityidentity parquet files
3. Verifying schema compatibility

Usage:
    python -m scripts.examples.facility_sync_example
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.utils import FacilitySyncManager


def export_example():
    """Export facilities to entityidentity parquet format."""
    print("=" * 70)
    print("EXPORT EXAMPLE: Facilities DB → EntityIdentity Parquet")
    print("=" * 70)

    # Initialize manager
    manager = FacilitySyncManager()

    # Export to parquet
    output_path = Path("output/entityidentity_export")
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"\nExporting facilities to {output_path}...")
    parquet_file = manager.export_to_entityidentity_format(output_path)

    print(f"✓ Export complete!")
    print(f"  File: {parquet_file}")
    print(f"  Size: {parquet_file.stat().st_size / 1024 / 1024:.2f} MB")

    # Load and show stats
    import pandas as pd
    df = pd.read_parquet(parquet_file)

    print(f"\n📊 Export Statistics:")
    print(f"  Total facilities: {len(df)}")
    print(f"  Countries: {df['country_iso2'].nunique()}")
    print(f"  With coordinates: {df['lat'].notna().sum()} ({df['lat'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  With commodities: {df['commodities'].notna().sum()} ({df['commodities'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  Mean confidence: {df['confidence'].mean():.3f}")

    print(f"\n  Top 5 countries by facility count:")
    for country, count in df['country_iso2'].value_counts().head().items():
        print(f"    {country}: {count}")

    return parquet_file


def import_example(parquet_file: Path):
    """Import facilities from entityidentity parquet."""
    print("\n" + "=" * 70)
    print("IMPORT EXAMPLE: EntityIdentity Parquet → Facilities DB")
    print("=" * 70)

    # Initialize manager with a test directory
    test_dir = Path("output/test_import")
    test_dir.mkdir(parents=True, exist_ok=True)

    manager = FacilitySyncManager(facilities_dir=test_dir)

    print(f"\nImporting from {parquet_file}...")
    print("Mode: Skip existing facilities (overwrite=False)")

    # Import without overwriting
    stats = manager.import_from_entityidentity(parquet_file, overwrite=False)

    print(f"\n✓ Import complete!")
    print(f"  Imported: {stats['imported']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Failed: {stats['failed']}")

    # Show created directories
    print(f"\n📁 Created directories:")
    for country_dir in sorted(test_dir.iterdir()):
        if country_dir.is_dir():
            count = len(list(country_dir.glob("*.json")))
            print(f"  {country_dir.name}: {count} facilities")

    return stats


def verify_schema():
    """Verify exported parquet matches entityidentity schema."""
    print("\n" + "=" * 70)
    print("SCHEMA VERIFICATION")
    print("=" * 70)

    import pandas as pd

    # Export a small sample
    manager = FacilitySyncManager()
    output_path = Path("output/schema_test")
    output_path.mkdir(parents=True, exist_ok=True)
    exported_file = manager.export_to_entityidentity_format(output_path)

    # Load reference schema
    reference_file = Path("/Users/willb/Github/GSMC/entityidentity/tables/facilities/facilities_20251003_134822.parquet")

    if not reference_file.exists():
        print("\n⚠ Reference parquet not found, skipping schema verification")
        return

    exported = pd.read_parquet(exported_file)
    reference = pd.read_parquet(reference_file)

    # Compare columns
    exported_cols = set(exported.columns)
    reference_cols = set(reference.columns)

    print(f"\nColumn comparison:")
    print(f"  Exported: {len(exported_cols)} columns")
    print(f"  Reference: {len(reference_cols)} columns")

    if exported_cols == reference_cols:
        print(f"\n✓ Schema match: All {len(exported_cols)} columns present")
    else:
        missing = reference_cols - exported_cols
        extra = exported_cols - reference_cols
        if missing:
            print(f"\n⚠ Missing: {missing}")
        if extra:
            print(f"\n⚠ Extra: {extra}")

    # Compare data types
    type_mismatches = []
    for col in sorted(exported_cols & reference_cols):
        exp_type = str(exported[col].dtype)
        ref_type = str(reference[col].dtype)
        if exp_type != ref_type:
            type_mismatches.append((col, exp_type, ref_type))

    if type_mismatches:
        print(f"\n⚠ Data type differences ({len(type_mismatches)}):")
        for col, exp_type, ref_type in type_mismatches:
            print(f"  {col}: {exp_type} vs {ref_type}")
    else:
        print(f"\n✓ All data types match")


def main():
    """Run all examples."""
    print("\n🔄 FACILITY SYNC MANAGER EXAMPLES\n")

    # Export facilities
    parquet_file = export_example()

    # Import facilities
    import_example(parquet_file)

    # Verify schema
    verify_schema()

    print("\n" + "=" * 70)
    print("✓ All examples completed successfully!")
    print("=" * 70)
    print("\nTo use FacilitySyncManager in your own code:")
    print("  from scripts.utils import FacilitySyncManager")
    print("  manager = FacilitySyncManager()")
    print("  parquet_file = manager.export_to_entityidentity_format(output_path)")
    print("  stats = manager.import_from_entityidentity(parquet_file)")


if __name__ == "__main__":
    main()
