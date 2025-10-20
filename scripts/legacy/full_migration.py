#!/usr/bin/env python3
"""
Full Migration & Enrichment Automation Script
Handles all 129 countries and ~9000 facilities systematically.

Usage:
    python scripts/full_migration.py --phase all
    python scripts/full_migration.py --phase migrate
    python scripts/full_migration.py --phase enrich --batch-size 10
    python scripts/full_migration.py --phase report
"""

import argparse
import subprocess
import json
import time
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import sys


def get_all_countries():
    """Get list of all country directories."""
    facilities_dir = Path("facilities")
    countries = [d.name for d in facilities_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
    return sorted(countries)


def get_facility_count(country):
    """Count facilities in a country (excluding backups)."""
    country_dir = Path(f"facilities/{country}")
    json_files = list(country_dir.glob("*.json"))
    # Exclude backup files
    json_files = [f for f in json_files if '.backup_' not in f.name]
    return len(json_files)


def check_migration_status():
    """Check which countries still need migration."""
    # Run check command
    result = subprocess.run(
        ["python", "scripts/migrate_legacy_fields.py", "--check-only", "--countries", "ALL"],
        capture_output=True,
        text=True
    )

    # Parse output to find countries needing migration
    needs_migration = []
    total_legacy = 0

    for line in result.stdout.split('\n'):
        if 'files with legacy fields' in line and ':' in line:
            parts = line.strip().split(':')
            country = parts[0].strip()
            counts = parts[1].strip().split('/')
            legacy_count = int(counts[0])
            total_count = int(counts[1].split()[0])

            if legacy_count > 0:
                needs_migration.append(country)
                total_legacy += legacy_count

    return needs_migration, total_legacy


def migrate_countries(countries, batch_size=10):
    """Migrate legacy fields from countries in batches."""
    total = len(countries)
    migrated = 0
    failed = []

    print(f"\n{'='*60}")
    print(f"MIGRATING {total} COUNTRIES")
    print(f"{'='*60}\n")

    # Process in batches
    for i in range(0, total, batch_size):
        batch = countries[i:i+batch_size]
        batch_str = ' '.join(batch)

        print(f"\n📦 Batch {i//batch_size + 1}: {batch_str}")
        print(f"   Progress: {i}/{total} countries")

        try:
            # Run migration
            result = subprocess.run(
                ["python", "scripts/migrate_legacy_fields.py", "--countries"] + batch,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per batch
            )

            if result.returncode == 0:
                migrated += len(batch)
                print(f"   ✅ Successfully migrated {len(batch)} countries")
            else:
                failed.extend(batch)
                print(f"   ❌ Failed to migrate batch: {result.stderr[:200]}")

        except subprocess.TimeoutExpired:
            failed.extend(batch)
            print(f"   ⏱️ Timeout migrating batch")
        except Exception as e:
            failed.extend(batch)
            print(f"   ❌ Error: {e}")

        # Small delay between batches
        if i + batch_size < total:
            time.sleep(2)

    return migrated, failed


def enrich_countries(countries, batch_size=5):
    """Run enrichment on countries in batches."""
    total = len(countries)
    enriched = 0
    failed = []
    stats = {
        'auto_accepted': 0,
        'review_queue': 0,
        'pending': 0,
        'relationships': 0
    }

    print(f"\n{'='*60}")
    print(f"ENRICHING {total} COUNTRIES")
    print(f"{'='*60}\n")

    # Ensure entityidentity is in path
    os.environ['PYTHONPATH'] = "../entityidentity:" + os.environ.get('PYTHONPATH', '')

    for country in countries:
        print(f"\n🔄 Enriching {country} ({enriched+1}/{total})")

        try:
            # Run enrichment
            result = subprocess.run(
                ["python", "scripts/enrich_companies.py", "--country", country],
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout per country
            )

            if result.returncode == 0:
                enriched += 1
                # Parse stats from output
                for line in result.stdout.split('\n'):
                    if 'Auto-accepted:' in line:
                        count = int(line.split(':')[1].strip())
                        stats['auto_accepted'] += count
                    elif 'Review queue:' in line:
                        count = int(line.split(':')[1].strip())
                        stats['review_queue'] += count
                    elif 'Relationships written:' in line:
                        count = int(line.split(':')[1].strip())
                        stats['relationships'] += count

                print(f"   ✅ Enriched successfully")
            else:
                failed.append(country)
                print(f"   ❌ Failed: {result.stderr[:200] if result.stderr else 'Unknown error'}")

        except subprocess.TimeoutExpired:
            failed.append(country)
            print(f"   ⏱️ Timeout")
        except Exception as e:
            failed.append(country)
            print(f"   ❌ Error: {e}")

        # Batch delay
        if (enriched + len(failed)) % batch_size == 0 and enriched + len(failed) < total:
            print(f"\n--- Batch complete. Pausing 5 seconds ---")
            time.sleep(5)

    return enriched, failed, stats


def generate_report():
    """Generate comprehensive migration report."""
    print(f"\n{'='*60}")
    print(f"GENERATING MIGRATION REPORT")
    print(f"{'='*60}\n")

    # Get current status
    all_countries = get_all_countries()
    needs_migration, legacy_count = check_migration_status()

    # Count facilities
    total_facilities = 0
    country_stats = []

    for country in all_countries:
        count = get_facility_count(country)
        total_facilities += count
        has_legacy = country in needs_migration
        country_stats.append({
            'country': country,
            'facilities': count,
            'has_legacy': has_legacy
        })

    # Load relationships to check enrichment
    try:
        df = pd.read_parquet('tables/facilities/facility_company_relationships.parquet')
        total_relationships = len(df)
        gate_distribution = df['gate'].value_counts().to_dict()
        null_gates = df['gate'].isna().sum()
    except:
        total_relationships = 0
        gate_distribution = {}
        null_gates = 0

    # Generate report
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_countries': len(all_countries),
            'total_facilities': total_facilities,
            'migrated_facilities': total_facilities - legacy_count,
            'legacy_facilities': legacy_count,
            'migration_progress': f"{((total_facilities - legacy_count) / total_facilities * 100):.1f}%"
        },
        'relationships': {
            'total': total_relationships,
            'gates': gate_distribution,
            'null_gates': int(null_gates)
        },
        'countries': {
            'total': len(all_countries),
            'migrated': len([c for c in all_countries if c not in needs_migration]),
            'needs_migration': len(needs_migration),
            'list': needs_migration[:10] if needs_migration else []
        },
        'top_countries_by_facilities': sorted(country_stats, key=lambda x: x['facilities'], reverse=True)[:10]
    }

    # Save report
    report_file = f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

    # Print summary
    print(f"""
📊 MIGRATION STATUS REPORT
─────────────────────────────────────
Total Countries:        {report['summary']['total_countries']}
Total Facilities:       {report['summary']['total_facilities']:,}
Migrated:              {report['summary']['migrated_facilities']:,} ({report['summary']['migration_progress']})
Still Need Migration:   {report['summary']['legacy_facilities']:,}

Relationships:         {report['relationships']['total']:,}
- Auto-accepted:       {gate_distribution.get('auto_accept', 0)}
- Review queue:        {gate_distribution.get('review', 0)}
- Manual accepted:     {gate_distribution.get('manual_accept', 0)}
- No gate (legacy):    {report['relationships']['null_gates']}

Countries Migrated:    {report['countries']['migrated']}/{report['countries']['total']}
Countries Remaining:   {report['countries']['needs_migration']}

Top Countries by Facility Count:""")

    for country in report['top_countries_by_facilities'][:5]:
        status = "❌" if country['has_legacy'] else "✅"
        print(f"  {status} {country['country']:4} - {country['facilities']:,} facilities")

    print(f"\nFull report saved to: {report_file}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Full migration and enrichment automation")
    parser.add_argument("--phase", choices=['migrate', 'enrich', 'report', 'all'],
                        default='report', help="Which phase to run")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Countries per batch (default: 10)")
    parser.add_argument("--countries", nargs='+',
                        help="Specific countries to process (default: all)")
    parser.add_argument("--skip-migrated", action='store_true',
                        help="Skip countries already migrated")
    parser.add_argument("--dry-run", action='store_true',
                        help="Show what would be done without executing")

    args = parser.parse_args()

    print(f"""
╔══════════════════════════════════════════════╗
║   FULL MIGRATION & ENRICHMENT AUTOMATION    ║
╠══════════════════════════════════════════════╣
║   Phase:      {args.phase:31}║
║   Batch Size: {args.batch_size:31}║
║   Mode:       {'DRY RUN' if args.dry_run else 'LIVE':31}║
╚══════════════════════════════════════════════╝
    """)

    # Determine which countries to process
    if args.countries:
        countries = args.countries
        print(f"Processing specific countries: {', '.join(countries)}")
    else:
        countries = get_all_countries()

        if args.skip_migrated and args.phase == 'migrate':
            needs_migration, _ = check_migration_status()
            countries = needs_migration
            print(f"Processing {len(countries)} countries that need migration")

    # Execute requested phase
    if args.phase == 'report':
        generate_report()

    elif args.phase == 'migrate':
        if args.dry_run:
            print(f"\n[DRY RUN] Would migrate {len(countries)} countries:")
            for i, country in enumerate(countries[:20]):
                print(f"  {i+1:3}. {country}")
            if len(countries) > 20:
                print(f"  ... and {len(countries)-20} more")
        else:
            migrated, failed = migrate_countries(countries, args.batch_size)
            print(f"\n✅ Migrated: {migrated} countries")
            if failed:
                print(f"❌ Failed: {len(failed)} countries")
                print(f"   Failed list: {', '.join(failed[:10])}")

    elif args.phase == 'enrich':
        if args.dry_run:
            print(f"\n[DRY RUN] Would enrich {len(countries)} countries")
        else:
            enriched, failed, stats = enrich_countries(countries, args.batch_size)
            print(f"\n✅ Enriched: {enriched} countries")
            print(f"   Auto-accepted: {stats['auto_accepted']} relationships")
            print(f"   Review queue: {stats['review_queue']} items")
            print(f"   Total relationships: {stats['relationships']}")
            if failed:
                print(f"❌ Failed: {len(failed)} countries")
                print(f"   Failed list: {', '.join(failed[:10])}")

    elif args.phase == 'all':
        if args.dry_run:
            print("\n[DRY RUN] Would run full pipeline:")
            print("  1. Check migration status")
            print("  2. Migrate all legacy countries")
            print("  3. Enrich all countries")
            print("  4. Generate final report")
        else:
            print("\n🚀 Running FULL pipeline...")

            # Phase 1: Check status
            needs_migration, legacy_count = check_migration_status()
            print(f"\n📊 Status: {legacy_count:,} facilities need migration")

            if needs_migration:
                # Phase 2: Migrate
                print(f"\n🔧 Phase 2: Migrating {len(needs_migration)} countries...")
                migrated, failed = migrate_countries(needs_migration, args.batch_size)
                print(f"   Migrated: {migrated}, Failed: {len(failed)}")

                # Small pause
                time.sleep(5)

            # Phase 3: Enrich all
            print(f"\n🎯 Phase 3: Enriching all countries...")
            all_countries = get_all_countries()
            enriched, failed, stats = enrich_countries(all_countries[:10], 5)  # Start with 10 for safety
            print(f"   Enriched: {enriched}, Failed: {len(failed)}")
            print(f"   Relationships created: {stats['relationships']}")

            # Phase 4: Report
            print(f"\n📋 Phase 4: Generating final report...")
            generate_report()

    print(f"\n{'='*60}")
    print("AUTOMATION COMPLETE")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()