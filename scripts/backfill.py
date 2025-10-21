#!/usr/bin/env python3
"""
Unified backfill system for enriching existing facilities.

Backfills missing or incomplete data:
- Geocoding: Add coordinates to facilities
- Companies: Resolve company_mentions to canonical IDs
- Metals: Add chemical formulas and categories to commodities

Usage:
    # Backfill geocoding
    python scripts/backfill.py geocode --country ARE
    python scripts/backfill.py geocode --country ARE --interactive

    # Backfill company resolution
    python scripts/backfill.py companies --country IND
    python scripts/backfill.py companies --country IND --profile strict

    # Backfill metal normalization
    python scripts/backfill.py metals --country CHN
    python scripts/backfill.py metals --all

    # Backfill everything
    python scripts/backfill.py all --country ARE --interactive

    # Batch mode (multiple countries)
    python scripts/backfill.py geocode --countries ARE,IND,CHN

    # Dry run (preview changes)
    python scripts/backfill.py all --country ARE --dry-run
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from utils.geocoding import geocode_facility, GeocodingResult
    from utils.country_utils import normalize_country_to_iso3, iso3_to_country_name
except ImportError as e:
    logger.error(f"Failed to import utilities: {e}")
    sys.exit(1)

# Try to import optional dependencies
try:
    from entityidentity import metal_identifier
    METAL_IDENTIFIER_AVAILABLE = True
except ImportError:
    METAL_IDENTIFIER_AVAILABLE = False
    logger.warning("metal_identifier not available (entityidentity library)")

try:
    from utils.company_resolver import CompanyResolver
    COMPANY_RESOLVER_AVAILABLE = True
except ImportError:
    COMPANY_RESOLVER_AVAILABLE = False
    logger.warning("CompanyResolver not available")

# Paths
ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"


class BackfillStats:
    """Track backfill statistics."""
    def __init__(self):
        self.total = 0
        self.processed = 0
        self.updated = 0
        self.skipped = 0
        self.failed = 0
        self.details = []

    def add_result(self, facility_id: str, status: str, details: str = ""):
        """Add a processing result."""
        self.processed += 1
        if status == "updated":
            self.updated += 1
        elif status == "skipped":
            self.skipped += 1
        elif status == "failed":
            self.failed += 1

        self.details.append({
            'facility_id': facility_id,
            'status': status,
            'details': details
        })

    def print_summary(self, backfill_type: str):
        """Print summary statistics."""
        print(f"\n{'='*60}")
        print(f"BACKFILL SUMMARY: {backfill_type}")
        print(f"{'='*60}")
        print(f"Total facilities: {self.total}")
        print(f"Processed: {self.processed}")
        print(f"Updated: {self.updated}")
        print(f"Skipped: {self.skipped}")
        print(f"Failed: {self.failed}")
        if self.processed > 0:
            print(f"Success rate: {self.updated/self.processed*100:.1f}%")
        print(f"{'='*60}")


def load_facilities_for_country(country_iso3: str) -> List[Dict]:
    """Load all facility JSONs for a country."""
    facilities = []
    country_dir = FACILITIES_DIR / country_iso3

    if not country_dir.exists():
        logger.error(f"No facilities directory found for {country_iso3}")
        return facilities

    for facility_file in country_dir.glob("*.json"):
        try:
            with open(facility_file, 'r') as f:
                facility = json.load(f)
                facility['_path'] = facility_file
                facilities.append(facility)
        except Exception as e:
            logger.warning(f"Could not load {facility_file}: {e}")

    return facilities


def save_facility(facility: Dict, dry_run: bool = False) -> None:
    """Save facility JSON to disk."""
    if dry_run:
        return

    facility_path = facility.get('_path')
    if not facility_path:
        logger.error(f"No path for facility {facility.get('facility_id')}")
        return

    # Remove internal fields
    facility_copy = {k: v for k, v in facility.items() if not k.startswith('_')}

    with open(facility_path, 'w') as f:
        json.dump(facility_copy, f, indent=2, ensure_ascii=False)
        f.write('\n')


def backfill_geocoding(
    facilities: List[Dict],
    country_iso3: str,
    interactive: bool = False,
    dry_run: bool = False
) -> BackfillStats:
    """Backfill missing coordinates."""
    stats = BackfillStats()
    stats.total = len(facilities)

    country_name = iso3_to_country_name(country_iso3)
    logger.info(f"Backfilling geocoding for {country_name} ({country_iso3})")

    # Filter to facilities needing geocoding
    to_geocode = []
    for facility in facilities:
        location = facility.get('location', {})
        if location.get('lat') is None or location.get('lon') is None:
            to_geocode.append(facility)

    logger.info(f"Found {len(to_geocode)}/{len(facilities)} facilities needing geocoding")

    if not to_geocode:
        return stats

    # Geocode each facility
    for i, facility in enumerate(to_geocode):
        facility_id = facility['facility_id']
        logger.info(f"[{i+1}/{len(to_geocode)}] {facility['name']}")

        result = geocode_facility(
            facility_name=facility['name'],
            country_iso3=country_iso3,
            country_name=country_name,
            interactive=interactive,
            use_nominatim=True
        )

        if result.lat is not None and result.lon is not None:
            # Update facility
            facility['location'] = {
                'lat': result.lat,
                'lon': result.lon,
                'precision': result.precision
            }

            # Update verification
            if 'verification' not in facility:
                facility['verification'] = {}

            facility['verification']['last_checked'] = datetime.now().isoformat()
            notes = f"Geocoded via {result.source} (confidence: {result.confidence:.2f})"
            facility['verification']['notes'] = notes

            # Save
            save_facility(facility, dry_run=dry_run)

            action = "Would update" if dry_run else "Updated"
            logger.info(f"  ✓ {action}: {result.lat}, {result.lon}")
            stats.add_result(facility_id, "updated", f"{result.lat}, {result.lon}")
        else:
            logger.warning(f"  ✗ Failed to geocode")
            stats.add_result(facility_id, "failed", "No coordinates found")

    return stats


def backfill_companies(
    facilities: List[Dict],
    country_iso3: str,
    profile: str = "moderate",
    dry_run: bool = False
) -> BackfillStats:
    """Backfill company resolution (Phase 2)."""
    stats = BackfillStats()
    stats.total = len(facilities)

    if not COMPANY_RESOLVER_AVAILABLE:
        logger.error("CompanyResolver not available - cannot backfill companies")
        return stats

    logger.info(f"Backfilling company resolution for {country_iso3}")

    # Initialize CompanyResolver
    config_path = ROOT / "config" / "gate_config.json"
    if config_path.exists():
        resolver = CompanyResolver.from_config(str(config_path), profile=profile)
    else:
        resolver = CompanyResolver()

    # Filter to facilities with company_mentions
    to_resolve = []
    for facility in facilities:
        mentions = facility.get('company_mentions', [])
        if mentions:
            to_resolve.append(facility)

    logger.info(f"Found {len(to_resolve)}/{len(facilities)} facilities with company mentions")

    if not to_resolve:
        return stats

    # Resolve each facility's company mentions
    for i, facility in enumerate(to_resolve):
        facility_id = facility['facility_id']
        mentions = facility.get('company_mentions', [])

        logger.info(f"[{i+1}/{len(to_resolve)}] {facility['name']} ({len(mentions)} mentions)")

        try:
            accepted, review, pending = resolver.resolve_mentions(
                mentions,
                facility=facility,
                country_hint=country_iso3
            )

            updated = False

            # Add operator link if we have high-confidence operator
            for rel in accepted:
                if rel.get('role') == 'operator' and not facility.get('operator_link'):
                    facility['operator_link'] = {
                        'company_id': rel['company_id'],
                        'confidence': rel['confidence']
                    }
                    updated = True

            # Add owner links if we have high-confidence owners
            if accepted:
                if 'owner_links' not in facility:
                    facility['owner_links'] = []

                for rel in accepted:
                    if rel.get('role') in ['owner', 'majority_owner', 'minority_owner']:
                        owner_link = {
                            'company_id': rel['company_id'],
                            'role': rel['role'],
                            'confidence': rel['confidence']
                        }
                        if 'percentage' in rel:
                            owner_link['percentage'] = rel['percentage']

                        facility['owner_links'].append(owner_link)
                        updated = True

            if updated:
                # Update verification
                if 'verification' not in facility:
                    facility['verification'] = {}
                facility['verification']['last_checked'] = datetime.now().isoformat()

                save_facility(facility, dry_run=dry_run)

                action = "Would resolve" if dry_run else "Resolved"
                logger.info(f"  ✓ {action}: {len(accepted)} companies")
                stats.add_result(facility_id, "updated", f"{len(accepted)} resolved")
            else:
                logger.info(f"  → No high-confidence matches")
                stats.add_result(facility_id, "skipped", "No high-confidence matches")

        except Exception as e:
            logger.error(f"  ✗ Error resolving companies: {e}")
            stats.add_result(facility_id, "failed", str(e))

    return stats


def backfill_metals(
    facilities: List[Dict],
    dry_run: bool = False
) -> BackfillStats:
    """Backfill metal chemical formulas and categories."""
    stats = BackfillStats()
    stats.total = len(facilities)

    if not METAL_IDENTIFIER_AVAILABLE:
        logger.error("metal_identifier not available - cannot backfill metals")
        return stats

    logger.info("Backfilling metal normalization")

    # Filter to facilities with commodities missing formulas
    to_enrich = []
    for facility in facilities:
        commodities = facility.get('commodities', [])
        for commodity in commodities:
            if not commodity.get('chemical_formula') or not commodity.get('category'):
                to_enrich.append(facility)
                break

    logger.info(f"Found {len(to_enrich)}/{len(facilities)} facilities needing metal enrichment")

    if not to_enrich:
        return stats

    # Enrich each facility's commodities
    for i, facility in enumerate(to_enrich):
        facility_id = facility['facility_id']
        commodities = facility.get('commodities', [])

        logger.info(f"[{i+1}/{len(to_enrich)}] {facility['name']} ({len(commodities)} commodities)")

        updated = False

        for commodity in commodities:
            metal_name = commodity.get('metal')
            if not metal_name:
                continue

            # Skip if already has formula and category
            if commodity.get('chemical_formula') and commodity.get('category'):
                continue

            try:
                # Use metal_identifier from entityidentity
                result = metal_identifier(metal_name)

                if result and result.get('valid'):
                    if not commodity.get('chemical_formula') and result.get('formula'):
                        commodity['chemical_formula'] = result['formula']
                        updated = True

                    if not commodity.get('category') and result.get('category'):
                        commodity['category'] = result['category']
                        updated = True

                    logger.info(f"  ✓ {metal_name} → {result.get('formula')} ({result.get('category')})")

            except Exception as e:
                logger.debug(f"  Could not normalize {metal_name}: {e}")

        if updated:
            # Update verification
            if 'verification' not in facility:
                facility['verification'] = {}
            facility['verification']['last_checked'] = datetime.now().isoformat()

            save_facility(facility, dry_run=dry_run)

            action = "Would enrich" if dry_run else "Enriched"
            stats.add_result(facility_id, "updated", "Metals enriched")
        else:
            stats.add_result(facility_id, "skipped", "No updates needed")

    return stats


def backfill_all(
    facilities: List[Dict],
    country_iso3: str,
    interactive: bool = False,
    company_profile: str = "moderate",
    dry_run: bool = False
) -> Dict[str, BackfillStats]:
    """Run all backfill operations."""
    results = {}

    logger.info("Running all backfill operations")

    # 1. Geocoding
    logger.info("\n=== STEP 1: GEOCODING ===")
    results['geocoding'] = backfill_geocoding(
        facilities,
        country_iso3,
        interactive=interactive,
        dry_run=dry_run
    )

    # 2. Metal normalization
    logger.info("\n=== STEP 2: METAL NORMALIZATION ===")
    results['metals'] = backfill_metals(
        facilities,
        dry_run=dry_run
    )

    # 3. Company resolution
    logger.info("\n=== STEP 3: COMPANY RESOLUTION ===")
    results['companies'] = backfill_companies(
        facilities,
        country_iso3,
        profile=company_profile,
        dry_run=dry_run
    )

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Unified backfill system for enriching facilities"
    )

    subparsers = parser.add_subparsers(dest='command', required=True)

    # Geocode subcommand
    geocode_parser = subparsers.add_parser('geocode', help='Backfill coordinates')
    geocode_parser.add_argument('--country', help='Country ISO3 code')
    geocode_parser.add_argument('--countries', help='Comma-separated country codes')
    geocode_parser.add_argument('--interactive', action='store_true', help='Interactive prompting')
    geocode_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    # Companies subcommand
    companies_parser = subparsers.add_parser('companies', help='Backfill company resolution')
    companies_parser.add_argument('--country', help='Country ISO3 code')
    companies_parser.add_argument('--countries', help='Comma-separated country codes')
    companies_parser.add_argument('--profile', default='moderate', choices=['strict', 'moderate', 'permissive'])
    companies_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    # Metals subcommand
    metals_parser = subparsers.add_parser('metals', help='Backfill metal normalization')
    metals_parser.add_argument('--country', help='Country ISO3 code')
    metals_parser.add_argument('--countries', help='Comma-separated country codes')
    metals_parser.add_argument('--all', action='store_true', help='Process all countries')
    metals_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    # All subcommand
    all_parser = subparsers.add_parser('all', help='Run all backfill operations')
    all_parser.add_argument('--country', help='Country ISO3 code')
    all_parser.add_argument('--countries', help='Comma-separated country codes')
    all_parser.add_argument('--interactive', action='store_true', help='Interactive prompting')
    all_parser.add_argument('--profile', default='moderate', choices=['strict', 'moderate', 'permissive'])
    all_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    args = parser.parse_args()

    # Determine countries to process
    countries = []
    if hasattr(args, 'all') and args.all:
        # Get all country directories
        countries = [d.name for d in FACILITIES_DIR.iterdir() if d.is_dir()]
    elif hasattr(args, 'countries') and args.countries:
        countries = [c.strip() for c in args.countries.split(',')]
    elif hasattr(args, 'country') and args.country:
        countries = [args.country]
    else:
        logger.error("Must specify --country, --countries, or --all")
        return 1

    # Normalize country codes
    normalized_countries = []
    for country in countries:
        iso3 = normalize_country_to_iso3(country)
        if iso3:
            normalized_countries.append(iso3)
        else:
            logger.warning(f"Could not resolve country: {country}")

    if not normalized_countries:
        logger.error("No valid countries to process")
        return 1

    # Process each country
    all_stats = {}

    for country_iso3 in normalized_countries:
        country_name = iso3_to_country_name(country_iso3)
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {country_name} ({country_iso3})")
        logger.info(f"{'='*60}")

        # Load facilities
        facilities = load_facilities_for_country(country_iso3)
        if not facilities:
            logger.warning(f"No facilities found for {country_iso3}")
            continue

        # Run backfill based on command
        if args.command == 'geocode':
            stats = backfill_geocoding(
                facilities,
                country_iso3,
                interactive=args.interactive,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = {'geocoding': stats}

        elif args.command == 'companies':
            stats = backfill_companies(
                facilities,
                country_iso3,
                profile=args.profile,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = {'companies': stats}

        elif args.command == 'metals':
            stats = backfill_metals(
                facilities,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = {'metals': stats}

        elif args.command == 'all':
            stats = backfill_all(
                facilities,
                country_iso3,
                interactive=args.interactive,
                company_profile=args.profile,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = stats

    # Print final summary
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")

    for country_iso3, stats_dict in all_stats.items():
        country_name = iso3_to_country_name(country_iso3)
        print(f"\n{country_name} ({country_iso3}):")
        for backfill_type, stats in stats_dict.items():
            stats.print_summary(backfill_type)

    return 0


if __name__ == "__main__":
    sys.exit(main())
