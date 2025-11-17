#!/usr/bin/env python3
"""
Remove non-metal facilities from the database.
This includes cement plants, limestone quarries, gypsum mines, clay pits, etc.
These are construction materials, not metals.
"""

import json
import os
from pathlib import Path
import shutil
from datetime import datetime
from collections import defaultdict

# Non-metal commodities that shouldn't be in a metals database
NON_METAL_KEYWORDS = [
    'cement', 'limestone', 'gypsum', 'clay', 'sand', 'gravel',
    'aggregate', 'concrete', 'marble', 'granite', 'stone',
    'dolomite', 'chalk', 'kaolin', 'bentonite', 'perlite',
    'diatomite', 'pumice', 'slate', 'shale', 'basalt',
    'sandstone', 'quartzite', 'feldspar', 'silica sand'
]

# Some exceptions - these might contain metal operations
EXCEPTIONS = [
    'iron ore', 'bauxite', 'copper ore', 'gold ore',
    'chromite', 'manganese', 'nickel', 'zinc', 'lead',
    'uranium', 'thorium', 'rare earth', 'lithium',
    'cobalt', 'titanium', 'vanadium', 'tungsten'
]

def is_non_metal_facility(file_path):
    """Check if a facility is non-metal based on commodities and type"""

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Check commodities
    commodities = data.get('commodities', [])
    primary_metals = []

    for commodity in commodities:
        metal = commodity.get('metal', '').lower()
        if commodity.get('primary', False):
            primary_metals.append(metal)

    # Check if ANY primary commodity is a metal
    has_metal = False
    for metal in primary_metals:
        # Check if it's explicitly a metal
        for exception in EXCEPTIONS:
            if exception in metal:
                has_metal = True
                break

    # Check if ALL primary commodities are non-metals
    all_non_metal = False
    if primary_metals:
        all_non_metal = all(
            any(keyword in metal for keyword in NON_METAL_KEYWORDS)
            for metal in primary_metals
        )

    # Also check facility types
    types = data.get('types', [])
    type_str = ' '.join(types).lower()

    is_cement = 'cement' in type_str or 'cement' in data.get('name', '').lower()
    is_quarry = 'quarry' in type_str and not has_metal

    # Decision logic
    if is_cement:
        return True, 'cement_facility'
    elif all_non_metal and not has_metal:
        return True, 'non_metal_commodities'
    elif is_quarry and not has_metal:
        return True, 'non_metal_quarry'

    # Check if the name is obviously non-metal
    name = data.get('name', '').lower()
    for keyword in ['cement', 'gypsum', 'limestone', 'clay', 'sand', 'gravel']:
        if keyword in name and not has_metal:
            return True, f'{keyword}_in_name'

    return False, None

def main():
    # Find all facility files
    facility_files = list(Path('facilities').glob('*/*.json'))
    print(f"Scanning {len(facility_files)} facilities...")

    non_metal_facilities = defaultdict(list)

    for file_path in facility_files:
        is_non_metal, reason = is_non_metal_facility(file_path)

        if is_non_metal:
            with open(file_path, 'r') as f:
                data = json.load(f)

            non_metal_facilities[reason].append({
                'facility_id': data.get('facility_id'),
                'name': data.get('name'),
                'country': data.get('country_iso3'),
                'commodities': [c.get('metal') for c in data.get('commodities', [])],
                'file_path': str(file_path)
            })

    # Summary
    total_non_metal = sum(len(facilities) for facilities in non_metal_facilities.values())

    print("\n" + "=" * 60)
    print("NON-METAL FACILITIES FOUND")
    print("=" * 60)
    print(f"Total non-metal facilities: {total_non_metal}")
    print("\nBreakdown by category:")

    for reason, facilities in sorted(non_metal_facilities.items()):
        print(f"\n{reason}: {len(facilities)} facilities")
        # Show first 5 examples
        for facility in facilities[:5]:
            commodities = ', '.join(c for c in facility['commodities'] if c)
            print(f"  - {facility['name']} ({facility['country']})")
            if commodities:
                print(f"    Commodities: {commodities}")

        if len(facilities) > 5:
            print(f"  ... and {len(facilities) - 5} more")

    if total_non_metal == 0:
        print("No non-metal facilities found!")
        return

    # Ask for confirmation
    print("\n" + "=" * 60)
    print("These facilities produce construction materials, not metals.")
    response = input(f"\nDelete all {total_non_metal} non-metal facilities? (y/n): ")

    if response.lower() != 'y':
        print("Deletion cancelled")
        return

    # Create backup
    backup_dir = Path(f'output/deleted_non_metal_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    backup_dir.mkdir(exist_ok=True, parents=True)

    # Delete facilities
    deleted_count = 0
    errors = []

    for reason, facilities in non_metal_facilities.items():
        reason_dir = backup_dir / reason
        reason_dir.mkdir(exist_ok=True)

        for facility in facilities:
            file_path = Path(facility['file_path'])

            try:
                # Backup first
                backup_path = reason_dir / file_path.name
                shutil.copy2(file_path, backup_path)

                # Delete
                file_path.unlink()
                deleted_count += 1

            except Exception as e:
                errors.append((facility['facility_id'], str(e)))

    # Save summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'deleted_count': deleted_count,
        'total_found': total_non_metal,
        'categories': {
            reason: [f['facility_id'] for f in facilities]
            for reason, facilities in non_metal_facilities.items()
        },
        'errors': errors
    }

    with open(backup_dir / 'deletion_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)

    print("\n" + "=" * 60)
    print(f"DELETION COMPLETE")
    print(f"Deleted: {deleted_count} non-metal facilities")
    print(f"Backed up to: {backup_dir}")
    if errors:
        print(f"Errors: {len(errors)}")
    print("=" * 60)

if __name__ == "__main__":
    main()