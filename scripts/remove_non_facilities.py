#!/usr/bin/env python3
"""
Identify and remove non-facility entries from the database.

Non-facilities include:
- Mining districts (regional groupings, not specific facilities)
- Generic placeholders ("no steel mill exists", "potential for REEs")
- Exploration areas without specific projects
- Regional headers
- Conceptual/research entries

These should be removed as they cannot be geocoded to a specific location.
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Tuple


# Patterns that indicate non-facilities
NON_FACILITY_PATTERNS = {
    'name': [
        r'.*\bdistrict\b.*',
        r'.*\bprovince\b.*',
        r'.*\bregion\b.*',
        r'.*\barea\b.*',  # "Exploration Area", "Processing Area"
        r'.*\bzone\b.*',
        r'.*\bbelt\b.*',
        r'.*\bfields?\b.*',  # "Goldfields", "Coalfields"
        r'^\*\*.*\*\*$',  # Markdown bold (headers)
    ],
    'content': [
        r'no\s+steel.*exist',
        r'no\s+processing.*exist',
        r'not\s+identified',
        r'traces?\s+identified',
        r'generic\s+potential',
        r'potential\s+for\s+ree',
        r'potential\s+for\s+rare',
        r'potential\s+noted',
        r'no\s+other\s+name',
        r'is\s+a\s+net\s+importer',
        r'no\s+.*\s+industry\s+exists?',
        r'uneconomic\s+traces',
        r'not\s+specified',
        r'generic\b',
    ]
}


def is_mining_district(facility: Dict) -> bool:
    """Check if facility is a mining district (not a specific facility)."""
    name = facility.get('name', '').lower()

    # Explicit district/province markers - these are definitely not specific facilities
    if 'mining district' in name:
        return True
    if name.endswith(' district'):
        return True
    if name.endswith(' province'):
        return True
    if name.endswith(' belt'):
        return True
    if name.endswith(' region'):
        return True
    if name.endswith(' fields'):
        return True
    if name.endswith(' area') and 'exploration area' in name:
        return True

    # "Zone" and "Area" are tricky - many are actual facilities
    # Only remove if it's clearly generic
    if name.startswith('zone ') or name.startswith('area '):
        # "Zone 2 North", "Area C" - probably not specific facilities
        return True

    # "Integrated Regional Processing Area" is borderline but probably real
    if 'processing area' in name or 'industrial area' in name:
        return False

    return False


def is_generic_placeholder(facility: Dict) -> bool:
    """Check if facility is a generic placeholder."""
    name = facility.get('name', '').lower()
    notes = facility.get('verification', {}).get('notes', '').lower()

    # Check name and notes for placeholder patterns
    text_to_check = f"{name} {notes}"

    for pattern in NON_FACILITY_PATTERNS['content']:
        if re.search(pattern, text_to_check, re.IGNORECASE):
            return True

    return False


def is_header_entry(facility: Dict) -> bool:
    """Check if facility is a markdown header (bold text)."""
    name = facility.get('name', '')

    # Check for markdown bold
    if name.startswith('**') and name.endswith('**'):
        # But keep it if it's clearly a real facility (has mine/project/plant/smelter in name)
        cleaned_name = name.strip('*').lower()
        facility_keywords = ['mine', 'project', 'plant', 'smelter', 'refinery', 'mill', 'quarry', 'deposit']
        for keyword in facility_keywords:
            if keyword in cleaned_name:
                return False  # Keep it - it's a real facility

        # If it has commodities or operators, it's probably real
        if facility.get('commodities'):
            return False
        if facility.get('operator'):
            return False
        if facility.get('company_mentions'):
            return False

        # Otherwise, it's probably a header
        return True

    return False


def is_exploration_concept(facility: Dict) -> bool:
    """Check if facility is just an exploration concept/potential."""
    name = facility.get('name', '').lower()
    notes = facility.get('verification', {}).get('notes', '').lower()

    # Check for exploration/concept indicators
    exploration_keywords = [
        'exploration area',
        'prospective',
        'potential for',
        'research/concept',
        'early-stage exploration',
        'conceptual',
    ]

    text_to_check = f"{name} {notes}"

    for keyword in exploration_keywords:
        if keyword in text_to_check:
            # Only flag if it's ONLY exploration, not an actual project
            # If it has coordinates, operator, or operating status, it's probably real
            if facility.get('status') == 'operating':
                return False
            if facility.get('operator'):
                return False

            location = facility.get('location', {})
            if location.get('lat') and location.get('lon'):
                return False

            # Otherwise, likely just a concept
            return True

    return False


def should_remove(facility: Dict) -> Tuple[bool, str]:
    """
    Determine if facility should be removed.

    Returns:
        (should_remove, reason)
    """
    if is_header_entry(facility):
        return True, "Markdown header (bold text)"

    if is_mining_district(facility):
        return True, "Mining district/region (not specific facility)"

    if is_generic_placeholder(facility):
        return True, "Generic placeholder (no actual facility)"

    if is_exploration_concept(facility):
        return True, "Exploration concept only (no actual facility)"

    return False, ""


def analyze_facilities(facilities_dir: Path) -> Dict:
    """Analyze all facilities and identify non-facilities."""
    results = {
        'total': 0,
        'to_remove': [],
        'to_keep': [],
        'by_reason': {},
        'by_country': {}
    }

    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir():
            continue

        country_iso3 = country_dir.name

        for json_file in sorted(country_dir.glob("*.json")):
            results['total'] += 1

            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)

                should_rem, reason = should_remove(facility)

                if should_rem:
                    results['to_remove'].append({
                        'file': json_file,
                        'facility': facility,
                        'reason': reason
                    })

                    # Track by reason
                    if reason not in results['by_reason']:
                        results['by_reason'][reason] = []
                    results['by_reason'][reason].append(facility.get('name'))

                    # Track by country
                    if country_iso3 not in results['by_country']:
                        results['by_country'][country_iso3] = 0
                    results['by_country'][country_iso3] += 1
                else:
                    results['to_keep'].append(json_file)

            except Exception as e:
                print(f"Error processing {json_file}: {e}")

    return results


def main():
    import sys

    script_dir = Path(__file__).parent
    facilities_dir = script_dir.parent / "facilities"

    # Check for --yes flag
    auto_confirm = '--yes' in sys.argv or '-y' in sys.argv

    print("Analyzing facilities for non-facility entries...")
    print("=" * 80)

    results = analyze_facilities(facilities_dir)

    print(f"\nTOTAL FACILITIES: {results['total']}")
    print(f"TO REMOVE: {len(results['to_remove'])}")
    print(f"TO KEEP: {len(results['to_keep'])}")
    print(f"Removal Rate: {100*len(results['to_remove'])/results['total']:.1f}%")

    # Breakdown by reason
    print(f"\n{'='*80}")
    print("BREAKDOWN BY REASON")
    print(f"{'='*80}")
    for reason, names in sorted(results['by_reason'].items(), key=lambda x: len(x[1]), reverse=True):
        print(f"\n{reason}: {len(names)} facilities")
        for name in names[:10]:  # Show first 10
            print(f"  - {name}")
        if len(names) > 10:
            print(f"  ... and {len(names)-10} more")

    # Breakdown by country
    print(f"\n{'='*80}")
    print("BREAKDOWN BY COUNTRY")
    print(f"{'='*80}")
    sorted_countries = sorted(results['by_country'].items(), key=lambda x: x[1], reverse=True)
    for country, count in sorted_countries[:20]:
        print(f"{country}: {count} facilities to remove")

    # Show examples
    print(f"\n{'='*80}")
    print("EXAMPLES TO BE REMOVED")
    print(f"{'='*80}")
    for item in results['to_remove'][:20]:
        print(f"\n{item['facility'].get('name')} ({item['facility'].get('country_iso3')})")
        print(f"  File: {item['file'].name}")
        print(f"  Reason: {item['reason']}")
        print(f"  Status: {item['facility'].get('status', 'unknown')}")
        location = item['facility'].get('location', {})
        if isinstance(location, dict):
            print(f"  Location: lat={location.get('lat')}, lon={location.get('lon')}")

    # Ask for confirmation
    print(f"\n{'='*80}")
    print("CONFIRMATION")
    print(f"{'='*80}")
    print(f"This will DELETE {len(results['to_remove'])} facility files.")

    if auto_confirm:
        print("\nAuto-confirmed with --yes flag")
        response = 'yes'
    else:
        try:
            response = input("\nProceed with deletion? (yes/no): ").strip().lower()
        except EOFError:
            response = 'no'

    if response == 'yes':
        print("\nDeleting non-facility entries...")
        deleted = 0
        for item in results['to_remove']:
            try:
                item['file'].unlink()
                deleted += 1
            except Exception as e:
                print(f"Error deleting {item['file']}: {e}")

        print(f"\nDeleted {deleted}/{len(results['to_remove'])} files")
        print("\nRun the export script again to regenerate CSV files:")
        print("  python scripts/find_missing_coords.py")
        print("  python scripts/export_missing_coords_for_geocoding.py")
    else:
        print("\nCancelled - no files deleted")
        print("\nTo review individual files, see the examples above.")


if __name__ == '__main__':
    main()
