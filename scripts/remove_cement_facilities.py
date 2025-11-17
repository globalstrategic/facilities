#!/usr/bin/env python3
"""
Remove cement facilities from the database.
Cement is a construction material, not a metal.
"""

import json
import os
from pathlib import Path
import shutil
from datetime import datetime

def is_cement_facility(file_path):
    """Check if a facility is primarily cement-related"""

    with open(file_path, 'r') as f:
        data = json.load(f)

    name = data.get('name', '').lower()

    # Check if cement is in the name
    if 'cement' in name:
        # But exclude if it also mentions metals
        metal_keywords = ['copper', 'steel', 'iron', 'aluminum', 'zinc', 'lead', 'gold', 'silver']
        if any(metal in name for metal in metal_keywords):
            return False
        return True

    # Check commodities
    commodities = data.get('commodities', [])
    primary_commodities = [c.get('metal', '').lower() for c in commodities if c.get('primary', False)]

    # If primary commodity is cement
    if 'cement' in primary_commodities:
        return True

    # Check facility types
    types = data.get('types', [])
    type_str = ' '.join(types).lower()
    if 'cement' in type_str:
        return True

    return False

def main():
    # Find all facility files
    facility_files = list(Path('facilities').glob('*/*.json'))
    print(f"Scanning {len(facility_files)} facilities for cement plants...")

    cement_facilities = []

    for file_path in facility_files:
        if is_cement_facility(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)

            cement_facilities.append({
                'facility_id': data.get('facility_id'),
                'name': data.get('name'),
                'country': data.get('country_iso3'),
                'file_path': str(file_path)
            })

    if not cement_facilities:
        print("No cement facilities found!")
        return

    # Show all cement facilities
    print("\n" + "=" * 60)
    print(f"CEMENT FACILITIES FOUND: {len(cement_facilities)}")
    print("=" * 60)

    # Group by country
    by_country = {}
    for facility in cement_facilities:
        country = facility['country']
        if country not in by_country:
            by_country[country] = []
        by_country[country].append(facility)

    for country in sorted(by_country.keys()):
        facilities = by_country[country]
        print(f"\n{country}: {len(facilities)} facilities")
        for facility in facilities:
            print(f"  - {facility['name']}")

    # Ask for confirmation
    print("\n" + "=" * 60)
    print("Cement is a construction material, not a metal.")
    response = input(f"\nDelete all {len(cement_facilities)} cement facilities? (y/n): ")

    if response.lower() != 'y':
        print("Deletion cancelled")
        return

    # Create backup
    backup_dir = Path(f'output/deleted_cement_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    backup_dir.mkdir(exist_ok=True, parents=True)

    # Delete facilities
    deleted_count = 0
    errors = []

    for facility in cement_facilities:
        file_path = Path(facility['file_path'])

        try:
            # Backup first
            backup_path = backup_dir / file_path.name
            shutil.copy2(file_path, backup_path)

            # Delete
            file_path.unlink()
            deleted_count += 1
            print(f"✓ Deleted: {facility['facility_id']}")

        except Exception as e:
            errors.append((facility['facility_id'], str(e)))
            print(f"✗ Error: {facility['facility_id']} - {e}")

    # Save summary
    with open(backup_dir / 'deletion_summary.json', 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'deleted_count': deleted_count,
            'facilities': [f['facility_id'] for f in cement_facilities],
            'errors': errors
        }, f, indent=2)

    print("\n" + "=" * 60)
    print(f"DELETION COMPLETE")
    print(f"Deleted: {deleted_count} cement facilities")
    print(f"Backed up to: {backup_dir}")
    if errors:
        print(f"Errors: {len(errors)}")
    print("=" * 60)

if __name__ == "__main__":
    main()