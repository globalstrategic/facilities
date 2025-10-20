#!/usr/bin/env python3
"""
Legacy Field Migration Script
Removes operator_link and owner_links from all facility JSONs.
Preserves company_mentions and ensures data integrity.

Usage:
    python scripts/migrate_legacy_fields.py --countries ALL --dry-run
    python scripts/migrate_legacy_fields.py --countries BRA IND RUS
    python scripts/migrate_legacy_fields.py --check-only
"""

import argparse
import json
import os
from pathlib import Path
from datetime import datetime
import shutil
from typing import List, Dict, Tuple


def get_all_country_dirs():
    """Get all country directories in facilities/."""
    facilities_dir = Path("facilities")
    return [d.name for d in facilities_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]


def check_legacy_fields(facility_path: Path) -> Tuple[bool, bool, Dict]:
    """Check if facility has legacy fields and their values."""
    with open(facility_path) as f:
        data = json.load(f)

    has_operator_link = 'operator_link' in data
    has_owner_links = 'owner_links' in data

    # Track if they have non-empty values
    info = {
        'has_operator_link': has_operator_link,
        'has_owner_links': has_owner_links,
        'operator_link_value': data.get('operator_link') if has_operator_link else None,
        'owner_links_count': len(data.get('owner_links', [])) if has_owner_links else 0,
        'has_company_mentions': 'company_mentions' in data,
        'mentions_count': len(data.get('company_mentions', []))
    }

    return has_operator_link, has_owner_links, info


def migrate_facility(facility_path: Path, dry_run: bool = False) -> Dict:
    """Remove legacy fields from a single facility."""
    with open(facility_path) as f:
        data = json.load(f)

    original_keys = set(data.keys())

    # Track what we're removing
    removed = {
        'operator_link': data.pop('operator_link', None),
        'owner_links': data.pop('owner_links', [])
    }

    # Check if anything was actually removed
    modified = ('operator_link' in original_keys or 'owner_links' in original_keys)

    if modified and not dry_run:
        # Backup original
        backup_path = facility_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        shutil.copy2(facility_path, backup_path)

        # Write cleaned version
        with open(facility_path, 'w') as f:
            json.dump(data, f, indent=2)

    return {
        'modified': modified,
        'removed': removed,
        'has_mentions': 'company_mentions' in data,
        'mentions_count': len(data.get('company_mentions', []))
    }


def main():
    parser = argparse.ArgumentParser(description="Migrate facility JSONs to remove legacy fields")
    parser.add_argument("--countries", nargs='+',
                        help="Country codes to migrate (or 'ALL' for all countries)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be changed without modifying files")
    parser.add_argument("--check-only", action="store_true",
                        help="Only check for legacy fields, don't migrate")
    parser.add_argument("--verbose", action="store_true",
                        help="Show details for each file")

    args = parser.parse_args()

    # Determine which countries to process
    if args.countries and args.countries[0].upper() == 'ALL':
        countries = get_all_country_dirs()
        print(f"Processing ALL countries: {', '.join(sorted(countries))}")
    elif args.countries:
        countries = args.countries
    else:
        # Default to checking all for --check-only
        countries = get_all_country_dirs() if args.check_only else []
        if not countries:
            print("❌ Specify --countries or use --check-only")
            return 1

    # Statistics
    total_files = 0
    files_with_legacy = 0
    files_with_operator_link = 0
    files_with_owner_links = 0
    files_with_non_empty_values = 0
    files_with_mentions = 0
    country_stats = {}

    print(f"""
╔══════════════════════════════════════════════╗
║   LEGACY FIELD MIGRATION                    ║
╠══════════════════════════════════════════════╣
║   Mode: {('CHECK-ONLY' if args.check_only else 'DRY-RUN' if args.dry_run else 'LIVE MIGRATION'):<37}║
║   Countries: {len(countries):<32}║
╚══════════════════════════════════════════════╝
    """)

    # Process each country
    for country in sorted(countries):
        country_dir = Path(f"facilities/{country}")
        if not country_dir.exists():
            print(f"⚠️  Country directory not found: {country}")
            continue

        json_files = list(country_dir.glob("*.json"))
        # Exclude backup files
        json_files = [f for f in json_files if not '.backup_' in f.name]

        country_legacy_count = 0
        country_modified = 0

        for facility_path in json_files:
            total_files += 1

            if args.check_only:
                # Just check for legacy fields
                has_op, has_own, info = check_legacy_fields(facility_path)

                if has_op or has_own:
                    files_with_legacy += 1
                    country_legacy_count += 1

                    if has_op:
                        files_with_operator_link += 1
                    if has_own:
                        files_with_owner_links += 1

                    # Check for non-empty values
                    if info['operator_link_value'] or info['owner_links_count'] > 0:
                        files_with_non_empty_values += 1
                        if args.verbose:
                            print(f"   ⚠️  {facility_path.name}: operator={info['operator_link_value']}, owners={info['owner_links_count']}")

                if info['has_company_mentions']:
                    files_with_mentions += 1

            else:
                # Migrate the file
                result = migrate_facility(facility_path, dry_run=args.dry_run)
                if result['modified']:
                    country_modified += 1
                    files_with_legacy += 1
                    if args.verbose:
                        print(f"   {'[DRY-RUN] ' if args.dry_run else ''}✏️  {facility_path.name}")

                if result['has_mentions']:
                    files_with_mentions += 1

        # Country summary
        if args.check_only:
            if country_legacy_count > 0:
                print(f"  {country}: {country_legacy_count}/{len(json_files)} files with legacy fields")
            country_stats[country] = {'total': len(json_files), 'legacy': country_legacy_count}
        else:
            if country_modified > 0:
                print(f"  {country}: {'Would modify' if args.dry_run else 'Modified'} {country_modified}/{len(json_files)} files")
            country_stats[country] = {'total': len(json_files), 'modified': country_modified}

    # Final report
    print(f"""
╔══════════════════════════════════════════════╗
║   MIGRATION SUMMARY                         ║
╚══════════════════════════════════════════════╝

📊 Statistics:
   Total files scanned:        {total_files:,}
   Files with legacy fields:   {files_with_legacy:,} ({files_with_legacy/total_files*100:.1f}%)
   - With operator_link:       {files_with_operator_link:,}
   - With owner_links:         {files_with_owner_links:,}
   - With non-empty values:    {files_with_non_empty_values:,}
   Files with company_mentions: {files_with_mentions:,} ({files_with_mentions/total_files*100:.1f}%)
""")

    if args.check_only:
        # Show which countries need migration
        needs_migration = [c for c, stats in country_stats.items() if stats['legacy'] > 0]
        if needs_migration:
            print(f"""
⚠️  Countries needing migration:
   {', '.join(sorted(needs_migration))}

To migrate all:
   python scripts/migrate_legacy_fields.py --countries ALL

To migrate specific countries:
   python scripts/migrate_legacy_fields.py --countries {' '.join(needs_migration[:3])}
""")
        else:
            print("✅ No legacy fields found! Migration complete.")

    elif args.dry_run:
        print("""
This was a DRY-RUN. To apply changes:
   python scripts/migrate_legacy_fields.py --countries ALL
""")

    else:
        if files_with_legacy > 0:
            print(f"""
✅ Migration complete:
   - Modified {files_with_legacy} files
   - Created {files_with_legacy} backup files
   - All legacy fields removed

Next: Update pre-commit hook to prevent re-introduction
""")
        else:
            print("✅ No files needed migration.")

    return 0


if __name__ == "__main__":
    exit(main())