#!/usr/bin/env python3
"""
Clean up USA facilities with generic numeric names.

1. Rename facilities that have useful aliases
2. Delete facilities with no distinguishing information
"""

import json
import os
import sys
import glob
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional


def slugify(text: str) -> str:
    """Create a slug from text."""
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheticals
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')


def is_generic_name(name: str) -> bool:
    """Check if a name is too generic."""
    # Starts with # and has numbers
    if name.startswith('#') and any(c.isdigit() for c in name):
        # But "Abundance No 1" is not generic because it has more context
        if len(name.split()) <= 3:
            return True

    # Just numbers
    if name.split()[0].isdigit() if name.split() else False:
        if len(name.split()) <= 3:
            return True

    return False


def find_best_alias(facility: Dict) -> Optional[str]:
    """Find the best non-generic alias to use as the primary name."""
    name = facility.get('name', '')
    aliases = facility.get('aliases', [])

    # Find aliases that are different from the name and not generic
    candidates = []
    for alias in aliases:
        if alias == name:
            continue
        if is_generic_name(alias):
            continue
        # Skip very short aliases
        if len(alias) < 5:
            continue
        # Prefer aliases without numbers at the start
        score = 10 if not alias[0].isdigit() else 5
        # Longer names are better
        score += len(alias) / 100.0
        candidates.append((score, alias))

    if candidates:
        # Return the highest scoring candidate
        return max(candidates, key=lambda x: x[0])[1]

    return None


def rename_facility(filepath: str, new_name: str, dry_run: bool = True) -> Tuple[bool, str]:
    """
    Rename a facility to use a better name.
    Returns (success, new_filepath)
    """
    try:
        # Load facility
        with open(filepath) as f:
            facility = json.load(f)

        old_name = facility['name']
        old_id = facility['facility_id']

        # Update name and IDs
        facility['name'] = new_name
        new_slug = slugify(new_name)
        facility['facility_id'] = f"usa-{new_slug}-fac"
        facility['canonical_name'] = new_name
        facility['canonical_slug'] = new_slug

        # Add to canonical name history
        if 'canonical_name_history' not in facility:
            facility['canonical_name_history'] = []
        facility['canonical_name_history'].append({
            'name': old_name,
            'from': facility.get('sources', [{}])[0].get('date', ''),
            'to': '2025-12-01T00:00:00Z',
            'reason': 'renamed_from_generic'
        })

        # New filepath
        new_filepath = f"usa-{new_slug}-fac.json"

        if dry_run:
            print(f"  [DRY RUN] Would rename to: {new_name} ({new_filepath})")
            return True, new_filepath
        else:
            # Write updated JSON
            with open(new_filepath, 'w') as f:
                json.dump(facility, f, indent=2)

            # Delete old file if different name
            if filepath != new_filepath:
                os.remove(filepath)

            print(f"  ✓ Renamed to: {new_name} ({new_filepath})")
            return True, new_filepath

    except Exception as e:
        print(f"  ✗ Error renaming: {e}")
        return False, filepath


def delete_facility(filepath: str, dry_run: bool = True) -> bool:
    """Delete a facility file."""
    try:
        if dry_run:
            print(f"  [DRY RUN] Would delete")
            return True
        else:
            os.remove(filepath)
            print(f"  ✓ Deleted")
            return True
    except Exception as e:
        print(f"  ✗ Error deleting: {e}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Clean up numeric USA facilities")
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Preview changes without modifying files (default)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually modify/delete files')

    args = parser.parse_args()
    dry_run = not args.execute

    # Change to USA facilities directory
    script_dir = Path(__file__).parent.parent
    usa_dir = script_dir / 'facilities' / 'USA'

    if not usa_dir.exists():
        print(f"Error: Directory not found: {usa_dir}")
        return 1

    os.chdir(usa_dir)

    if dry_run:
        print("=" * 70)
        print("DRY RUN MODE - No files will be modified")
        print("Use --execute to actually make changes")
        print("=" * 70 + "\n")

    # Find all numeric-named facilities
    files = sorted(glob.glob("usa-[0-9]*.json"))

    print(f"Found {len(files)} numeric-named facilities\n")
    print("=" * 70)

    stats = {
        'renamed': 0,
        'deleted': 0,
        'kept': 0,
        'errors': 0
    }

    for filepath in files:
        try:
            with open(filepath) as f:
                facility = json.load(f)

            name = facility.get('name', 'N/A')
            facility_id = facility.get('facility_id', 'N/A')

            print(f"\n{filepath}")
            print(f"  Current name: {name}")

            # Check if it has a better alias
            best_alias = find_best_alias(facility)

            if best_alias:
                print(f"  → Found better name: {best_alias}")
                success, new_path = rename_facility(filepath, best_alias, dry_run)
                if success:
                    stats['renamed'] += 1
                else:
                    stats['errors'] += 1
            else:
                # No useful alias - check if we should delete
                aliases = facility.get('aliases', [])
                companies = facility.get('company_mentions', [])

                if not aliases and not companies:
                    print(f"  → No useful info, marking for deletion")
                    if delete_facility(filepath, dry_run):
                        stats['deleted'] += 1
                    else:
                        stats['errors'] += 1
                else:
                    print(f"  → Keeping (has some info)")
                    stats['kept'] += 1

        except Exception as e:
            print(f"  ✗ Error processing: {e}")
            stats['errors'] += 1

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Renamed: {stats['renamed']}")
    print(f"Deleted: {stats['deleted']}")
    print(f"Kept unchanged: {stats['kept']}")
    print(f"Errors: {stats['errors']}")
    print("=" * 70)

    return 0 if stats['errors'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
