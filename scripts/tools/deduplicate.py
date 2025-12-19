#!/usr/bin/env python3
"""
Batch deduplication utility for existing facilities.

This script finds and merges duplicate facilities using shared deduplication logic
from scripts.utils.deduplication. Use this for batch cleanup of existing data.

For automatic deduplication during import, see scripts/import_from_report.py which
uses the same underlying logic.

Usage:
    # Dry run (no changes) - always do this first
    python scripts/deduplicate_facilities.py --country ZAF --dry-run

    # Actually deduplicate
    python scripts/deduplicate_facilities.py --country ZAF

    # All countries
    python scripts/deduplicate_facilities.py --all
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
    merge_facilities
)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def load_facilities(country_dir: Path) -> List[Dict]:
    """Load all facilities from a country directory."""
    facilities = []
    for file in country_dir.glob("*.json"):
        try:
            with open(file) as f:
                fac = json.load(f)
                fac['_file'] = file
                facilities.append(fac)
        except Exception as e:
            logger.error(f"Error loading {file}: {e}")
    return facilities


def find_duplicates(facilities: List[Dict]) -> List[List[Dict]]:
    """Find duplicate groups based on coordinates and name similarity."""
    # Build coordinate index
    coord_index = defaultdict(list)

    for fac in facilities:
        lat = fac.get('location', {}).get('lat')
        lon = fac.get('location', {}).get('lon')
        if lat is not None and lon is not None:
            # Round to 1 decimal place for initial grouping
            coord_key = (round(lat, 1), round(lon, 1))
            coord_index[coord_key].append(fac)

    # Find duplicate groups
    duplicate_groups = []
    processed = set()

    for coord_key, candidates in coord_index.items():
        if len(candidates) < 2:
            continue

        # Check each pair in this coordinate bucket
        for i, fac1 in enumerate(candidates):
            if fac1['facility_id'] in processed:
                continue

            group = [fac1]
            processed.add(fac1['facility_id'])

            for fac2 in candidates[i+1:]:
                if fac2['facility_id'] in processed:
                    continue

                # Check if truly duplicates
                if is_duplicate(fac1, fac2):
                    group.append(fac2)
                    processed.add(fac2['facility_id'])

            if len(group) > 1:
                duplicate_groups.append(group)

    return duplicate_groups


def is_duplicate(fac1: Dict, fac2: Dict) -> bool:
    """Check if two facilities are duplicates using same logic as import.

    Two-tier matching:
    - Tier 1: Very close coords (0.01 deg ~1km) + moderate name match (>0.6 OR contains)
    - Tier 2: Close coords (0.1 deg ~11km) + high name match (>0.85 OR contains)
    """
    lat1 = fac1.get('location', {}).get('lat')
    lon1 = fac1.get('location', {}).get('lon')
    lat2 = fac2.get('location', {}).get('lat')
    lon2 = fac2.get('location', {}).get('lon')

    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return False

    lat_diff = abs(lat1 - lat2)
    lon_diff = abs(lon1 - lon2)

    # Check name similarity
    name1 = fac1['name'].lower()
    name2 = fac2['name'].lower()
    name_similarity = SequenceMatcher(None, name1, name2).ratio()

    # Check containment
    shorter = name1 if len(name1) < len(name2) else name2
    longer = name2 if len(name1) < len(name2) else name1
    contains_match = shorter in longer

    # Two-tier matching
    tier1_match = (lat_diff < 0.01 and lon_diff < 0.01) and (name_similarity > 0.6 or contains_match)
    tier2_match = (lat_diff < 0.1 and lon_diff < 0.1) and (name_similarity > 0.85 or contains_match)

    return tier1_match or tier2_match


def score_facility(fac: Dict) -> float:
    """Score facility completeness to determine which to keep."""
    score = 0.0

    # Prefer facilities with coordinates
    if fac.get('location', {}).get('lat') is not None:
        score += 10

    # More commodities is better
    score += len(fac.get('commodities', [])) * 2

    # Company mentions
    score += len(fac.get('company_mentions', [])) * 3

    # Products
    score += len(fac.get('products', [])) * 2

    # Aliases
    score += len(fac.get('aliases', []))

    # Known status is better than unknown
    if fac.get('status') != 'unknown':
        score += 5

    # Higher confidence
    score += fac.get('verification', {}).get('confidence', 0) * 10

    # Prefer csv_imported or human_verified over llm_suggested
    status = fac.get('verification', {}).get('status', '')
    if status == 'human_verified':
        score += 20
    elif status == 'csv_imported':
        score += 10
    elif status == 'llm_verified':
        score += 5

    return score


def merge_facilities(group: List[Dict]) -> Dict:
    """Merge a group of duplicate facilities into one best facility."""
    # Select best facility to keep
    best = max(group, key=score_facility)

    # Merge aliases from all facilities
    all_aliases = set(best.get('aliases', []))
    for fac in group:
        all_aliases.update(fac.get('aliases', []))
        # Add the facility name as an alias if different from best
        if fac['name'] != best['name']:
            all_aliases.add(fac['name'])

    # Remove the best facility's own name from aliases
    all_aliases.discard(best['name'])

    # Merge sources
    all_sources = list(best.get('sources', []))
    seen_sources = {(s['type'], s['id']) for s in all_sources}

    for fac in group:
        for source in fac.get('sources', []):
            source_key = (source['type'], source['id'])
            if source_key not in seen_sources:
                all_sources.append(source)
                seen_sources.add(source_key)

    # Merge commodities (prefer ones with formulas)
    all_commodities = {}
    for fac in group:
        for comm in fac.get('commodities', []):
            metal = comm['metal']
            if metal not in all_commodities or comm.get('chemical_formula'):
                all_commodities[metal] = comm

    # Merge company mentions (deduplicate by name)
    all_mentions = {}
    for fac in group:
        for mention in fac.get('company_mentions', []):
            name = mention['name']
            # Keep highest confidence mention for each company
            if name not in all_mentions or mention.get('confidence', 0) > all_mentions[name].get('confidence', 0):
                all_mentions[name] = mention

    # Update best facility
    best['aliases'] = sorted(list(all_aliases))
    best['sources'] = all_sources
    best['commodities'] = list(all_commodities.values())
    best['company_mentions'] = list(all_mentions.values())

    # Add note about merge
    notes = best.get('verification', {}).get('notes', '')
    merge_ids = [f['facility_id'] for f in group if f['facility_id'] != best['facility_id']]
    merge_note = f"Merged from: {', '.join(merge_ids)}"
    if notes:
        best['verification']['notes'] = f"{notes}; {merge_note}"
    else:
        best['verification']['notes'] = merge_note

    return best


def deduplicate_country(country_dir: Path, dry_run: bool = True) -> Dict:
    """Deduplicate all facilities in a country directory."""
    logger.info(f"Processing {country_dir.name}...")

    facilities = load_facilities(country_dir)
    logger.info(f"  Loaded {len(facilities)} facilities")

    duplicate_groups = find_duplicates(facilities)
    logger.info(f"  Found {len(duplicate_groups)} duplicate groups")

    if not duplicate_groups:
        return {
            'country': country_dir.name,
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
            score = score_facility(fac)
            logger.info(f"    [{score:.1f}] {fac['facility_id']}: {fac['name']}")

        # Merge and save
        merged = merge_facilities(group)
        logger.info(f"    → Keeping: {merged['facility_id']}")

        if not dry_run:
            # Save merged facility
            with open(merged['_file'], 'w') as f:
                # Remove internal _file key
                save_data = {k: v for k, v in merged.items() if k != '_file'}
                json.dump(save_data, f, indent=2)

            # Delete duplicates
            for fac in group:
                if fac['facility_id'] != merged['facility_id']:
                    fac['_file'].unlink()
                    logger.info(f"    → Deleted: {fac['facility_id']}")
                    duplicates_removed += 1

        facilities_kept += 1

    return {
        'country': country_dir.name,
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

    facilities_dir = Path(__file__).parent.parent / 'facilities'

    if args.country:
        countries = [args.country]
    elif args.all:
        countries = [d.name for d in facilities_dir.iterdir() if d.is_dir()]
    else:
        logger.error("Must specify --country or --all")
        return

    mode = "DRY RUN" if args.dry_run else "LIVE"
    logger.info(f"=== Deduplication {mode} ===\n")

    results = []
    for country in countries:
        country_dir = facilities_dir / country
        if not country_dir.exists():
            logger.warning(f"Country directory not found: {country}")
            continue

        result = deduplicate_country(country_dir, dry_run=args.dry_run)
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
