#!/usr/bin/env python3
"""
Company Mentions Backfill Script

Populates company_mentions from Mines.csv "Group Names" field for facilities
that were migrated but lost this data during operator_link/owner_links removal.

Background:
- Migration removed operator_link/owner_links fields (always empty)
- Original Mines.csv has "Group Names" with semicolon-separated company names
- 4,196 facilities have Group Names data available
- Current state: 100% of facilities have empty company_mentions

Strategy:
1. Load Mines.csv and index by row number
2. For each facility with mines_csv source:
   - Extract row number from sources
   - Look up Group Names in CSV
   - Parse semicolon-separated names
   - Create company_mentions entries with:
     - name: company name
     - role: 'unknown' (will be resolved during enrichment)
     - source: 'mines_csv'
     - confidence: 0.5 (moderate - needs enrichment)
     - first_seen: CSV import timestamp
3. Save updated facilities with backups

Usage:
    # Dry run - show what would change
    python scripts/backfill_mentions.py --dry-run

    # Backfill specific country
    python scripts/backfill_mentions.py --country BRA --dry-run
    python scripts/backfill_mentions.py --country BRA

    # Backfill all countries
    python scripts/backfill_mentions.py

    # Only facilities with 0 mentions (default)
    python scripts/backfill_mentions.py --empty-only

    # Force overwrite facilities that already have mentions
    python scripts/backfill_mentions.py --force

Output:
    - Before/after statistics
    - Detailed log of changes
    - Use 'git revert' if you need to undo changes
"""

import argparse
import csv
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict


# Paths
ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"
CSV_PATH = ROOT / "gt" / "Mines.csv"


def load_mines_csv() -> Dict[int, Dict]:
    """Load Mines.csv indexed by row number."""
    csv_data = {}

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Row numbers start at 2 (after header)
        for idx, row in enumerate(reader, start=2):
            csv_data[idx] = row

    print(f"✓ Loaded {len(csv_data)} rows from Mines.csv")
    return csv_data


def parse_group_names(group_names: str) -> List[str]:
    """
    Parse semicolon-separated company names from Group Names field.

    Returns list of unique, cleaned company names.
    """
    if not group_names or not group_names.strip():
        return []

    # Split on semicolon
    names = [n.strip() for n in group_names.split(';') if n.strip()]

    # Remove duplicates while preserving order
    seen = set()
    unique_names = []
    for name in names:
        # Normalize for deduplication (case-insensitive)
        name_lower = name.lower()
        if name_lower not in seen:
            seen.add(name_lower)
            unique_names.append(name)

    return unique_names


def get_csv_row_from_facility(facility: Dict) -> Optional[int]:
    """Extract CSV row number from facility sources."""
    for source in facility.get('sources', []):
        if source.get('type') == 'mines_csv':
            return source.get('row')
    return None


def create_company_mention(name: str, csv_row: int, import_timestamp: str) -> Dict:
    """
    Create a company_mentions entry from CSV Group Name.

    Structure follows the schema expected by enrich_companies.py:
    - name: company name
    - role: 'unknown' (will be mapped to operator during enrichment)
    - source: provenance information
    - confidence: 0.5 (moderate - needs resolution)
    - first_seen: when it was originally imported
    """
    return {
        "name": name,
        "role": "unknown",  # Will be converted to 'operator' by enrich_companies.py
        "source": f"mines_csv_row_{csv_row}",
        "confidence": 0.5,
        "first_seen": import_timestamp,
        "evidence": "Extracted from Mines.csv 'Group Names' field during backfill"
    }


def backfill_facility(
    facility: Dict,
    facility_path: Path,
    csv_data: Dict[int, Dict],
    dry_run: bool = False,
    force: bool = False,
    empty_only: bool = True
) -> Tuple[bool, int, str]:
    """
    Backfill company_mentions for a single facility.

    Returns:
        (modified, mentions_added, status_message)
    """
    facility_id = facility.get('facility_id', '???')

    # Check if facility already has mentions
    existing_mentions = facility.get('company_mentions', [])

    if existing_mentions and empty_only and not force:
        return False, 0, f"Already has {len(existing_mentions)} mentions (skipped)"

    # Get CSV row
    csv_row = get_csv_row_from_facility(facility)
    if not csv_row:
        return False, 0, "No mines_csv source found"

    # Look up row in CSV
    if csv_row not in csv_data:
        return False, 0, f"CSV row {csv_row} not found"

    # Extract Group Names
    group_names_raw = csv_data[csv_row].get('Group Names', '').strip()
    if not group_names_raw:
        return False, 0, f"No Group Names in CSV row {csv_row}"

    # Parse company names
    company_names = parse_group_names(group_names_raw)
    if not company_names:
        return False, 0, "No valid company names parsed"

    # Get original import timestamp from verification
    import_timestamp = facility.get('verification', {}).get('last_checked', datetime.now().isoformat())

    # Create company_mentions entries
    new_mentions = [
        create_company_mention(name, csv_row, import_timestamp)
        for name in company_names
    ]

    # Merge with existing mentions if force mode
    if force and existing_mentions:
        # Deduplicate by name (case-insensitive)
        existing_names = {m['name'].lower() for m in existing_mentions}
        new_mentions = [m for m in new_mentions if m['name'].lower() not in existing_names]

        if not new_mentions:
            return False, 0, "All mentions already exist"

        final_mentions = existing_mentions + new_mentions
    else:
        final_mentions = new_mentions

    # Apply changes
    if not dry_run:
        # Update facility (no backup needed - use git revert if needed)
        facility['company_mentions'] = final_mentions

        # Write back
        with open(facility_path, 'w') as f:
            json.dump(facility, f, indent=2)

    action = "Would add" if dry_run else "Added"
    return True, len(new_mentions), f"{action} {len(new_mentions)} mentions from CSV"


def backfill_country(
    country_code: str,
    csv_data: Dict[int, Dict],
    dry_run: bool = False,
    force: bool = False,
    empty_only: bool = True,
    verbose: bool = False
) -> Dict:
    """Backfill all facilities in a country."""
    country_dir = FACILITIES_DIR / country_code

    if not country_dir.exists():
        print(f"❌ Country directory not found: {country_code}")
        return {
            'total': 0,
            'modified': 0,
            'mentions_added': 0,
            'skipped': 0,
            'errors': 0
        }

    stats = {
        'total': 0,
        'modified': 0,
        'mentions_added': 0,
        'skipped': 0,
        'errors': 0,
        'reasons': defaultdict(int)
    }

    print(f"\n{'='*80}")
    print(f"Processing {country_code}")
    print(f"{'='*80}")

    for facility_path in sorted(country_dir.glob("*.json")):
        if '.backup_' in facility_path.name:
            continue

        stats['total'] += 1

        try:
            with open(facility_path, 'r') as f:
                facility = json.load(f)

            modified, mentions_added, status = backfill_facility(
                facility,
                facility_path,
                csv_data,
                dry_run=dry_run,
                force=force,
                empty_only=empty_only
            )

            if modified:
                stats['modified'] += 1
                stats['mentions_added'] += mentions_added

                if verbose or mentions_added > 0:
                    print(f"  ✓ {facility['facility_id']}: {status}")
            else:
                stats['skipped'] += 1
                stats['reasons'][status] += 1

                if verbose:
                    print(f"  - {facility['facility_id']}: {status}")

        except Exception as e:
            stats['errors'] += 1
            print(f"  ❌ {facility_path.name}: ERROR - {e}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Backfill company_mentions from Mines.csv Group Names",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--country',
        help='Backfill specific country (e.g., BRA, USA, ZAF)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would change without modifying files'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Add mentions even if facility already has some (merge mode)'
    )
    parser.add_argument(
        '--empty-only',
        action='store_true',
        default=True,
        help='Only backfill facilities with 0 mentions (default: True)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Override --empty-only and process all facilities'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show details for every facility (not just modified ones)'
    )

    args = parser.parse_args()

    # Load CSV data
    print(f"\nLoading Mines.csv from {CSV_PATH}...")
    csv_data = load_mines_csv()

    # Determine countries to process
    if args.country:
        countries = [args.country]
    else:
        countries = sorted([d.name for d in FACILITIES_DIR.iterdir() if d.is_dir()])

    print(f"\n{'='*80}")
    print(f"COMPANY MENTIONS BACKFILL")
    print(f"{'='*80}")
    print(f"Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print(f"Countries: {len(countries)} ({', '.join(countries[:5])}{', ...' if len(countries) > 5 else ''})")
    print(f"Strategy: {'All facilities' if args.all else 'Empty mentions only'}")
    print(f"Force merge: {args.force}")
    print(f"{'='*80}")

    # Process countries
    empty_only = not args.all
    global_stats = {
        'total': 0,
        'modified': 0,
        'mentions_added': 0,
        'skipped': 0,
        'errors': 0,
        'reasons': defaultdict(int)
    }

    country_results = {}

    for country in countries:
        stats = backfill_country(
            country,
            csv_data,
            dry_run=args.dry_run,
            force=args.force,
            empty_only=empty_only,
            verbose=args.verbose
        )

        country_results[country] = stats

        # Accumulate global stats
        for key in ['total', 'modified', 'mentions_added', 'skipped', 'errors']:
            global_stats[key] += stats[key]

        for reason, count in stats.get('reasons', {}).items():
            global_stats['reasons'][reason] += count

    # Final summary
    print(f"\n{'='*80}")
    print(f"BACKFILL SUMMARY")
    print(f"{'='*80}")
    print(f"\nGlobal Statistics:")
    print(f"  Total facilities:       {global_stats['total']:,}")
    print(f"  Modified:               {global_stats['modified']:,} ({global_stats['modified']/global_stats['total']*100:.1f}%)")
    print(f"  Mentions added:         {global_stats['mentions_added']:,}")
    print(f"  Skipped:                {global_stats['skipped']:,}")
    print(f"  Errors:                 {global_stats['errors']}")

    if global_stats['modified'] > 0:
        print(f"\n  Average mentions/facility: {global_stats['mentions_added']/global_stats['modified']:.1f}")

    # Top skip reasons
    if global_stats['reasons']:
        print(f"\nTop skip reasons:")
        sorted_reasons = sorted(
            global_stats['reasons'].items(),
            key=lambda x: x[1],
            reverse=True
        )
        for reason, count in sorted_reasons[:5]:
            print(f"  - {reason}: {count:,}")

    # Country breakdown (top 10 by modifications)
    print(f"\nTop 10 Countries by Modifications:")
    sorted_countries = sorted(
        country_results.items(),
        key=lambda x: x[1]['modified'],
        reverse=True
    )

    print(f"\n{'Country':<10} {'Total':>8} {'Modified':>10} {'Mentions':>10} {'% Modified':>12}")
    print("-" * 60)
    for country, stats in sorted_countries[:10]:
        pct = (stats['modified'] / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"{country:<10} {stats['total']:>8} {stats['modified']:>10} {stats['mentions_added']:>10} {pct:>11.1f}%")

    if args.dry_run:
        print(f"\n{'='*80}")
        print(f"⚠️  DRY RUN - No changes were saved")
        print(f"{'='*80}")
        print(f"\nTo apply changes:")
        if args.country:
            print(f"  python scripts/backfill_mentions.py --country {args.country}")
        else:
            print(f"  python scripts/backfill_mentions.py")
    else:
        print(f"\n{'='*80}")
        print(f"✓ Backfill complete!")
        print(f"{'='*80}")
        print(f"\nModified {global_stats['modified']} facilities")
        print(f"Use 'git diff' to review changes")
        print(f"Use 'git revert' if you need to undo")
        print(f"\nNext steps:")
        print(f"1. Validate a sample of facilities")
        print(f"2. Run enrichment to resolve mentions:")
        print(f"   export PYTHONPATH=\"../entityidentity:$PYTHONPATH\"")
        print(f"   python scripts/enrich_companies.py --country BRA")

    return 0 if global_stats['errors'] == 0 else 1


if __name__ == "__main__":
    exit(main())
