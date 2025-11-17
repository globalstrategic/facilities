#!/usr/bin/env python3
"""
Delete facilities identified as invalid.
Reads the CSV of invalid facilities and removes them from the repository.
"""

import os
import sys
import csv
import json
from pathlib import Path
import shutil
from datetime import datetime
import pandas as pd

def main():
    # Get the most recent invalid facilities file
    invalid_dir = Path('output/facilities_to_delete')
    if not invalid_dir.exists():
        print("No facilities_to_delete directory found")
        return

    csv_files = list(invalid_dir.glob('invalid_facilities_*.csv'))
    if not csv_files:
        print("No invalid facility CSV files found")
        return

    # Use the most recent file
    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
    print(f"Using: {latest_file}")

    # Read the CSV
    df = pd.read_csv(latest_file)
    print(f"Found {len(df)} facilities marked for deletion\n")

    # Filter to only the REALLY invalid ones (high confidence + obvious errors)
    obvious_invalid = df[
        (df['confidence'] >= 0.90) &
        (
            (df['name'] == '\\-') |
            (df['name'] == '** **') |
            (df['name'].str.upper() == df['name']) & (df['name'].str.len() <= 15) & (df['reason'] == 'too_generic') |
            (df['name'].str.contains('^\W+$', regex=True, na=False))
        )
    ]

    print("DEFINITELY INVALID (will delete):")
    print("-" * 50)
    for _, row in obvious_invalid.iterrows():
        print(f"  {row['facility_id']}: {row['name']}")

    # Also check for facilities that are just region names
    region_names = ['CARINTHIA', 'LOWER AUSTRIA', 'SALZBURG', 'STYRIA', 'TYROL', 'UPPER AUSTRIA']
    region_facilities = df[df['name'].isin(region_names)]

    if len(region_facilities) > 0:
        print("\nREGION NAMES (will delete):")
        print("-" * 50)
        for _, row in region_facilities.iterrows():
            print(f"  {row['facility_id']}: {row['name']}")

    # Combine the deletion lists
    to_delete = pd.concat([obvious_invalid, region_facilities]).drop_duplicates()

    if len(to_delete) == 0:
        print("\nNo obviously invalid facilities found to delete.")
        print("Manual review recommended for the remaining entries.")
        return

    print(f"\n{len(to_delete)} facilities will be deleted")

    response = input("\nProceed with deletion? (y/n): ")
    if response.lower() != 'y':
        print("Deletion cancelled")
        return

    # Create backup directory
    backup_dir = Path(f'output/deleted_facilities_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    backup_dir.mkdir(exist_ok=True, parents=True)

    deleted_count = 0
    errors = []

    for _, row in to_delete.iterrows():
        facility_id = row['facility_id']
        country = row['country_iso3']
        file_path = Path(f'facilities/{country}/{facility_id}.json')

        if file_path.exists():
            try:
                # Backup the file first
                backup_path = backup_dir / f'{facility_id}.json'
                shutil.copy2(file_path, backup_path)

                # Delete the file
                file_path.unlink()
                deleted_count += 1
                print(f"  ✓ Deleted: {facility_id}")

            except Exception as e:
                errors.append((facility_id, str(e)))
                print(f"  ✗ Error deleting {facility_id}: {e}")
        else:
            print(f"  ⚠ Not found: {file_path}")

    # Save deletion log
    log_file = backup_dir / 'deletion_log.json'
    with open(log_file, 'w') as f:
        json.dump({
            'deleted_count': deleted_count,
            'total_attempted': len(to_delete),
            'timestamp': datetime.now().isoformat(),
            'deleted_facilities': to_delete.to_dict('records'),
            'errors': errors
        }, f, indent=2)

    print("\n" + "=" * 60)
    print(f"DELETION COMPLETE")
    print(f"Deleted: {deleted_count} facilities")
    print(f"Backed up to: {backup_dir}")
    if errors:
        print(f"Errors: {len(errors)}")
    print("=" * 60)

    # Now show facilities that might need manual review
    maybe_invalid = df[~df['facility_id'].isin(to_delete['facility_id'])]
    if len(maybe_invalid) > 0:
        print("\nFACILITIES NEEDING MANUAL REVIEW:")
        print("(These have parentheses or might be real facilities)")
        print("-" * 60)

        # Show a sample
        for _, row in maybe_invalid.head(20).iterrows():
            print(f"  {row['facility_id']}: {row['name']}")
            print(f"    Reason: {row['reason']} (conf: {row['confidence']:.2f})")

        print(f"\n... and {len(maybe_invalid) - 20} more")
        print("\nThese facilities have alternate names in parentheses")
        print("Many are likely REAL facilities and should NOT be deleted")
        print("Review manually before deletion!")

if __name__ == "__main__":
    main()