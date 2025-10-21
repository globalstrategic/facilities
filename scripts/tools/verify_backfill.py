#!/usr/bin/env python3
"""
Verify Backfill Results

Quick verification script to check company_mentions coverage before/after backfill.
Useful for validating that backfill worked correctly.

Usage:
    python scripts/verify_backfill.py
    python scripts/verify_backfill.py --country BRA
    python scripts/verify_backfill.py --sample 10
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict


ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"


def analyze_facility(facility_path: Path) -> dict:
    """Analyze a single facility's company_mentions."""
    with open(facility_path, 'r') as f:
        data = json.load(f)

    mentions = data.get('company_mentions', [])

    return {
        'has_mentions': len(mentions) > 0,
        'mention_count': len(mentions),
        'names': [m.get('name') for m in mentions],
        'sources': [m.get('source') for m in mentions],
        'roles': [m.get('role') for m in mentions]
    }


def verify_country(country_code: str, sample_size: int = 0) -> dict:
    """Verify backfill for a country."""
    country_dir = FACILITIES_DIR / country_code

    if not country_dir.exists():
        return None

    stats = {
        'total': 0,
        'with_mentions': 0,
        'without_mentions': 0,
        'total_mentions': 0,
        'backfilled': 0,  # From mines_csv_row sources
        'samples': []
    }

    for facility_path in sorted(country_dir.glob("*.json")):
        if '.backup_' in facility_path.name:
            continue

        stats['total'] += 1
        result = analyze_facility(facility_path)

        if result['has_mentions']:
            stats['with_mentions'] += 1
            stats['total_mentions'] += result['mention_count']

            # Check if backfilled (source starts with mines_csv_row)
            if any(s and s.startswith('mines_csv_row') for s in result['sources']):
                stats['backfilled'] += 1
        else:
            stats['without_mentions'] += 1

        # Sample facilities
        if sample_size > 0 and len(stats['samples']) < sample_size:
            stats['samples'].append({
                'facility_id': json.loads(facility_path.read_text())['facility_id'],
                'has_mentions': result['has_mentions'],
                'mention_count': result['mention_count'],
                'names': result['names'][:3]  # First 3 names
            })

    return stats


def main():
    parser = argparse.ArgumentParser(description="Verify backfill results")
    parser.add_argument('--country', help='Check specific country')
    parser.add_argument('--sample', type=int, default=0,
                       help='Show N sample facilities per country')
    args = parser.parse_args()

    print("=" * 80)
    print("COMPANY MENTIONS VERIFICATION")
    print("=" * 80)

    if args.country:
        countries = [args.country]
    else:
        countries = sorted([d.name for d in FACILITIES_DIR.iterdir() if d.is_dir()])

    global_stats = {
        'total': 0,
        'with_mentions': 0,
        'without_mentions': 0,
        'total_mentions': 0,
        'backfilled': 0
    }

    country_results = {}

    for country in countries:
        stats = verify_country(country, sample_size=args.sample)
        if not stats:
            continue

        country_results[country] = stats

        for key in ['total', 'with_mentions', 'without_mentions', 'total_mentions', 'backfilled']:
            global_stats[key] += stats[key]

    # Print summary
    print(f"\nGlobal Summary:")
    print(f"  Total facilities:       {global_stats['total']:,}")
    print(f"  With mentions:          {global_stats['with_mentions']:,} ({global_stats['with_mentions']/global_stats['total']*100:.1f}%)")
    print(f"  Without mentions:       {global_stats['without_mentions']:,} ({global_stats['without_mentions']/global_stats['total']*100:.1f}%)")
    print(f"  Total mentions:         {global_stats['total_mentions']:,}")
    print(f"  Backfilled facilities:  {global_stats['backfilled']:,}")

    if global_stats['with_mentions'] > 0:
        print(f"  Avg mentions/facility:  {global_stats['total_mentions']/global_stats['with_mentions']:.1f}")

    # Top countries by coverage
    print(f"\n{'=' * 80}")
    print("Top 20 Countries by Coverage")
    print(f"{'=' * 80}")

    sorted_countries = sorted(
        country_results.items(),
        key=lambda x: x[1]['with_mentions'] / x[1]['total'] if x[1]['total'] > 0 else 0,
        reverse=True
    )

    print(f"\n{'Country':<10} {'Total':>8} {'With':>8} {'Without':>8} {'Coverage':>10} {'Backfilled':>12}")
    print("-" * 80)

    for country, stats in sorted_countries[:20]:
        coverage = stats['with_mentions'] / stats['total'] * 100 if stats['total'] > 0 else 0
        print(f"{country:<10} {stats['total']:>8} {stats['with_mentions']:>8} "
              f"{stats['without_mentions']:>8} {coverage:>9.1f}% {stats['backfilled']:>12}")

    # Show samples if requested
    if args.sample > 0:
        print(f"\n{'=' * 80}")
        print(f"Sample Facilities (first {args.sample} per country)")
        print(f"{'=' * 80}")

        for country, stats in list(sorted_countries)[:5]:
            if not stats['samples']:
                continue

            print(f"\n{country}:")
            for sample in stats['samples']:
                status = "✓" if sample['has_mentions'] else "✗"
                names_str = ", ".join(sample['names']) if sample['names'] else "None"
                print(f"  {status} {sample['facility_id']}: "
                      f"{sample['mention_count']} mentions ({names_str}...)")

    print(f"\n{'=' * 80}")

    # Interpretation
    if global_stats['with_mentions'] == 0:
        print("⚠️  WARNING: No facilities have company_mentions!")
        print("   Run: python scripts/backfill.py mentions --all")
    elif global_stats['with_mentions'] < global_stats['total'] * 0.4:
        print(f"⚠️  Coverage is low ({global_stats['with_mentions']/global_stats['total']*100:.1f}%)")
        print("   Expected: ~48.8% after backfill")
        print("   Action: Verify backfill completed successfully")
    else:
        print(f"✓ Coverage looks good ({global_stats['with_mentions']/global_stats['total']*100:.1f}%)")
        print(f"✓ {global_stats['backfilled']:,} facilities backfilled from CSV")
        print("  Next: Run enrichment to resolve company mentions")


if __name__ == "__main__":
    main()
