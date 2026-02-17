#!/usr/bin/env python3
"""
Batch deduplication utility for existing facilities.

This script finds and merges duplicate facilities using shared deduplication logic
from scripts.utils.deduplication. Use this for batch cleanup of existing data.

For automatic deduplication during import, see scripts/import_from_report.py which
uses the same underlying logic.

Usage:
    # Dry run (no changes) - always do this first
    python scripts/tools/deduplicate.py --country ZAF --dry-run

    # Actually deduplicate
    python scripts/tools/deduplicate.py --country ZAF

    # All countries
    python scripts/tools/deduplicate.py --all
"""

import json
import argparse
from pathlib import Path
from typing import List, Dict
import logging
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.utils.deduplication import (
    find_duplicate_groups,
    select_best_facility,
    merge_facilities,
    score_facility_completeness,
)
from scripts.utils.facility_loader import load_facilities_from_country

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def deduplicate_country(country_iso3: str, dry_run: bool = True) -> Dict:
    """Deduplicate all facilities in a country directory."""
    logger.info(f"Processing {country_iso3}...")

    facilities = load_facilities_from_country(country_iso3)
    logger.info(f"  Loaded {len(facilities)} facilities")

    # Use shared utility function
    duplicate_groups = find_duplicate_groups(facilities)
    logger.info(f"  Found {len(duplicate_groups)} duplicate groups")

    if not duplicate_groups:
        return {
            'country': country_iso3,
            'total': len(facilities),
            'duplicate_groups': 0,
            'duplicates_removed': 0,
            'facilities_kept': 0
        }

    # Show duplicate groups
    duplicates_removed = 0
    facilities_kept = 0

    for i, group in enumerate(duplicate_groups, 1):
        logger.info(f"\n  Group {i} ({len(group)} facilities):")
        for fac in group:
            score = score_facility_completeness(fac)
            logger.info(f"    [{score:.1f}] {fac['facility_id']}: {fac['name']}")

        # Use shared utility functions
        best, duplicates = select_best_facility(group)
        merged = merge_facilities(best, duplicates)
        logger.info(f"    → Keeping: {merged['facility_id']}")

        if not dry_run:
            # Save merged facility
            with open(merged['_path'], 'w') as f:
                # Remove internal metadata keys
                save_data = {k: v for k, v in merged.items() if not k.startswith('_')}
                json.dump(save_data, f, indent=2)

            # Delete duplicates
            for fac in duplicates:
                fac['_path'].unlink()
                logger.info(f"    → Deleted: {fac['facility_id']}")
                duplicates_removed += 1

        facilities_kept += 1

    return {
        'country': country_iso3,
        'total': len(facilities),
        'duplicate_groups': len(duplicate_groups),
        'duplicates_removed': duplicates_removed,
        'facilities_kept': facilities_kept
    }


def main():
    parser = argparse.ArgumentParser(description="Deduplicate facilities")
    parser.add_argument('--country', help='Country code (e.g., ZAF)')
    parser.add_argument('--all', action='store_true', help='Process all countries')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no changes')
    args = parser.parse_args()

    from scripts.utils.facility_loader import iter_country_dirs, get_facilities_dir

    if args.country:
        countries = [args.country]
    elif args.all:
        countries = [d.name for d in iter_country_dirs()]
    else:
        logger.error("Must specify --country or --all")
        return

    mode = "DRY RUN" if args.dry_run else "LIVE"
    logger.info(f"=== Deduplication {mode} ===\n")

    facilities_dir = get_facilities_dir()
    results = []
    for country in countries:
        country_dir = facilities_dir / country
        if not country_dir.exists():
            logger.warning(f"Country directory not found: {country}")
            continue

        result = deduplicate_country(country, dry_run=args.dry_run)
        results.append(result)

    # Summary
    logger.info("\n=== SUMMARY ===")
    total_groups = sum(r['duplicate_groups'] for r in results)
    total_removed = sum(r['duplicates_removed'] for r in results)

    for result in results:
        if result['duplicate_groups'] > 0:
            logger.info(f"{result['country']}: {result['duplicate_groups']} groups, "
                       f"{result['duplicates_removed']} removed, {result['facilities_kept']} kept")

    logger.info(f"\nTotal: {total_groups} duplicate groups, {total_removed} facilities removed")

    if args.dry_run:
        logger.info("\n*** DRY RUN - No changes made ***")
        logger.info("Run without --dry-run to actually deduplicate")


if __name__ == '__main__':
    main()
