#!/usr/bin/env python3
"""
Enrich existing facilities with company links using CompanyResolver.

Phase 2: Uses unified CompanyResolver for batch resolution with quality gates.
Reads company_mentions from facilities and writes relationships to parquet.

Usage:
    python enrich_companies.py                      # Enrich all facilities
    python enrich_companies.py --country AFG        # Enrich specific country
    python enrich_companies.py --dry-run            # Preview without saving
    python enrich_companies.py --min-confidence 0.75  # Set confidence threshold
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging
import pandas as pd
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Paths
ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"
OUTPUT_DIR = ROOT / "output"

# Add scripts utils to path
sys.path.insert(0, str(ROOT / "scripts"))

# Phase 2: Import unified utilities
try:
    from utils.company_resolver import CompanyResolver
    RESOLVER_AVAILABLE = True
except ImportError as e:
    logger.error(f"Could not import CompanyResolver utilities: {e}")
    RESOLVER_AVAILABLE = False


def to_canonical(company_id: str, alias_map: Dict[str, str]) -> str:
    """Convert company ID to canonical form using alias map."""
    if not company_id:
        return company_id
    return alias_map.get(company_id, company_id)


def load_alias_map(alias_file_path: str) -> Dict[str, str]:
    """Load company alias map from JSON file."""
    path = Path(alias_file_path)
    if not path.exists():
        return {}
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load alias map from {alias_file_path}: {e}")
        return {}


# Canonical relationships path
RELATIONSHIPS_FILE = ROOT / "tables" / "facilities" / "facility_company_relationships.parquet"

# Add entityidentity to path (for PendingCompanyTracker)
sys.path.insert(0, str(ROOT.parent / 'entityidentity'))

try:
    from entityidentity.companies.pending_tracker import PendingCompanyTracker
    PENDING_TRACKER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Could not import PendingCompanyTracker: {e}")
    PENDING_TRACKER_AVAILABLE = False


class CompanyEnricher:
    """Enrich facilities with company links using CompanyResolver (Phase 2)."""

    def __init__(self, min_confidence: float = 0.75, dry_run: bool = False):
        """
        Initialize enricher with Phase 2 utilities.

        Args:
            min_confidence: Minimum confidence for auto-accept (0.0-1.0)
            dry_run: If True, don't save changes
        """
        self.min_confidence = min_confidence
        self.dry_run = dry_run
        self.stats = {
            'total_facilities': 0,
            'facilities_with_mentions': 0,
            'mentions_processed': 0,
            'auto_accepted': 0,
            'review_queue': 0,
            'pending': 0,
            'relationships_written': 0,
            'failed': 0
        }
        self.relationships = []  # Accumulated relationships for parquet

        if not RESOLVER_AVAILABLE:
            raise RuntimeError("CompanyResolver not available - check imports")

        # Phase 2: Initialize CompanyResolver with strict profile (uses hardcoded defaults)
        self.resolver = CompanyResolver.from_config(profile="strict")
        logger.info("CompanyResolver initialized (profile=strict, using default gates)")

        # Alias map for canonical IDs (currently not used, placeholder for future)
        self.alias_map = {}
        logger.info(f"Loaded {len(self.alias_map)} company aliases")

        # Initialize PendingCompanyTracker (optional)
        self.pending_tracker = None
        if PENDING_TRACKER_AVAILABLE:
            try:
                self.pending_tracker = PendingCompanyTracker()
                logger.info("PendingCompanyTracker initialized")
            except Exception as e:
                logger.warning(f"Could not initialize PendingCompanyTracker: {e}")

    def extract_mentions(self, facility: Dict) -> List[Dict]:
        """
        Extract company mentions from facility (Phase 2).

        Returns list of mention dicts suitable for resolver.resolve_mentions().
        Handles operator, owner, and unknown roles (defaults unknown→operator).
        """
        mentions = []

        # Extract from company_mentions array
        for mention in facility.get('company_mentions', []):
            role = mention.get('role', '')

            # Handle unknown role by defaulting to operator (common for CSV imports)
            if role == 'unknown':
                role = 'operator'

            # Only handle operator and owner roles (skip other types like contractor, customer, etc.)
            if role not in ['operator', 'owner', 'majority_owner', 'minority_owner']:
                continue

            # Skip mentions without names
            name = mention.get('name', '').strip()
            if not name:
                continue

            # Build mention dict for resolver
            mentions.append({
                'name': name,
                'role': role,
                'lei': mention.get('lei'),
                'percentage': mention.get('percentage'),
                'confidence': mention.get('confidence'),
                'first_seen': mention.get('first_seen'),
                'source': mention.get('source', 'unknown'),
                'evidence': mention.get('evidence'),
                'country_hint': mention.get('country_hint'),
                'registry': mention.get('registry'),  # Pass through for registry-first resolution
                'company_id': mention.get('company_id')  # Pass through for pre-resolved mentions
            })

        return mentions

    def match_company(self, company_name: str, facility_id: Optional[str] = None,
                     country_hint: Optional[str] = None, role: Optional[str] = None) -> Optional[Dict]:
        """
        [DEPRECATED] Phase 2: Use resolver.resolve_name() or batch resolve_mentions().

        Kept for backward compatibility. Delegates to CompanyResolver.
        """
        logger.warning(f"[DEPRECATED] match_company() called for '{company_name}'. "
                      f"Use resolver.resolve_mentions() for batch resolution")

        try:
            result = self.resolver.resolve_name(
                company_name,
                role=role,
                country_hint=country_hint,
                facility=None,
                lei=None
            )

            if result and result.get('company_id'):
                # Canonicalize ID
                canonical_id = to_canonical(result['company_id'], self.alias_map)

                return {
                    'company_id': canonical_id,
                    'company_name': result.get('company_name', company_name),
                    'confidence': round(result['confidence'], 3),
                    'matched_from': company_name
                }
            else:
                # No match - track in pending
                if self.pending_tracker:
                    self.pending_tracker.add_pending_company(
                        company_name=company_name,
                        facility_id=facility_id,
                        country_hint=country_hint,
                        role=role,
                        notes="First seen during company enrichment"
                    )

        except Exception as e:
            logger.warning(f"Error matching company '{company_name}': {e}")

        return None

    def enrich_facility(self, facility: Dict, file_path: Path) -> bool:
        """
        Enrich a single facility using Phase 2 batch resolution.

        Extracts company_mentions, resolves in batch, writes relationships to parquet.
        Does NOT modify facility JSON (no operator_link/owner_links writes).

        Returns True if relationships were created.
        """
        facility_id = facility.get('facility_id')

        # Extract mentions from facility
        mentions = self.extract_mentions(facility)

        if not mentions:
            # No mentions to process
            return False

        self.stats['facilities_with_mentions'] += 1
        self.stats['mentions_processed'] += len(mentions)

        logger.info(f"\n{facility_id}: Processing {len(mentions)} mentions")

        # Batch resolve mentions using CompanyResolver (strict profile)
        try:
            accepted, review, pending = self.resolver.resolve_mentions(
                mentions,
                facility=facility
            )
        except Exception as e:
            logger.error(f"Error resolving mentions for {facility_id}: {e}")
            self.stats['failed'] += 1
            return False

        # Log results
        logger.info(f"  Accepted: {len(accepted)}, Review: {len(review)}, Pending: {len(pending)}")

        # Process accepted relationships
        for item in accepted:
            resolution = item['resolution']
            mention = item

            # Canonicalize company_id
            company_id = to_canonical(resolution['company_id'], self.alias_map)

            # Create relationship record
            relationship = {
                'relationship_id': item.get('relationship_id'),
                'facility_id': facility_id,
                'company_id': company_id,
                'company_name': resolution['company_name'],
                'role': mention['role'],
                'confidence': resolution['confidence'],
                'base_confidence': resolution.get('base_confidence'),
                'match_method': 'resolver',
                'provenance': mention.get('source', 'unknown'),
                'evidence': mention.get('evidence'),
                'percentage': mention.get('percentage'),
                'gate': resolution['gate'],  # Top-level gate field for filtering
                'gates_applied': {
                    'gate': resolution['gate'],
                    'penalties': resolution.get('penalties_applied', [])
                },
                'created_at': datetime.now()  # Keep as datetime for parquet compatibility
            }

            self.relationships.append(relationship)
            self.stats['auto_accepted'] += 1
            self.stats['relationships_written'] += 1

            logger.info(f"  ✓ Accepted: {mention['name']} → {resolution['company_name']} "
                       f"(conf={resolution['confidence']:.3f}, gate={resolution['gate']})")

        # Process review queue
        for item in review:
            resolution = item['resolution']
            mention = item

            # Log for review pack (could write to review queue file)
            logger.info(f"  ⚠️  Review: {mention['name']} → {resolution['company_name']} "
                       f"(conf={resolution['confidence']:.3f}, gate={resolution['gate']})")

            self.stats['review_queue'] += 1

            # Could write to review pack here:
            # review_pack = {
            #     'facility_id': facility_id,
            #     'mention': mention,
            #     'resolution': resolution,
            #     'candidates': resolution.get('candidates', [])
            # }

        # Process pending (track unresolved companies)
        for item in pending:
            mention = item
            name = mention.get('name', 'unknown')

            if self.pending_tracker:
                self.pending_tracker.add_pending_company(
                    company_name=name,
                    facility_id=facility_id,
                    country_hint=mention.get('country_hint') or facility.get('country_iso3'),
                    role=mention.get('role'),
                    notes="No match found during enrichment"
                )

            logger.info(f"  ⊘ Pending: {name} (no match)")
            self.stats['pending'] += 1

        return len(accepted) > 0

    def enrich_country(self, country_code: str):
        """Enrich all facilities in a country."""
        country_dir = FACILITIES_DIR / country_code
        if not country_dir.exists():
            logger.error(f"Country directory not found: {country_code}")
            return

        logger.info(f"\nProcessing {country_code}...")

        for facility_file in sorted(country_dir.glob("*.json")):
            if 'backup' in facility_file.name:
                continue

            try:
                with open(facility_file, 'r') as f:
                    facility = json.load(f)

                self.stats['total_facilities'] += 1
                self.enrich_facility(facility, facility_file)

            except Exception as e:
                logger.error(f"Error processing {facility_file}: {e}")
                self.stats['failed'] += 1

    def enrich_all(self, countries: Optional[List[str]] = None):
        """Enrich all facilities or specific countries."""
        if countries:
            country_dirs = [FACILITIES_DIR / c for c in countries if (FACILITIES_DIR / c).exists()]
        else:
            country_dirs = sorted([d for d in FACILITIES_DIR.iterdir() if d.is_dir()])

        logger.info(f"Enriching {len(country_dirs)} countries...")

        for country_dir in country_dirs:
            self.enrich_country(country_dir.name)

    def save_relationships(self):
        """Save accumulated relationships to parquet file."""
        if not self.relationships:
            logger.info("No relationships to save")
            return

        if self.dry_run:
            logger.info(f"[DRY RUN] Would save {len(self.relationships)} relationships to {RELATIONSHIPS_FILE}")
            return

        # Ensure parent directory exists
        RELATIONSHIPS_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Convert to DataFrame
        df = pd.DataFrame(self.relationships)

        # Load existing relationships if file exists
        if RELATIONSHIPS_FILE.exists():
            try:
                existing_df = pd.read_parquet(RELATIONSHIPS_FILE)
                # Append new relationships
                df = pd.concat([existing_df, df], ignore_index=True)
                logger.info(f"Appended {len(self.relationships)} relationships to existing {len(existing_df)} records")
            except Exception as e:
                logger.warning(f"Could not load existing relationships: {e}")

        # Save to parquet
        df.to_parquet(RELATIONSHIPS_FILE, index=False)
        logger.info(f"✓ Saved {len(df)} total relationships to {RELATIONSHIPS_FILE}")

        # Also save as CSV for easy inspection
        csv_file = RELATIONSHIPS_FILE.with_suffix('.csv')
        df.to_csv(csv_file, index=False)
        logger.info(f"✓ Saved CSV to {csv_file}")

    def print_summary(self):
        """Print enrichment summary."""
        print("\n" + "="*60)
        print("COMPANY ENRICHMENT SUMMARY (Phase 2)")
        print("="*60)
        print(f"Total facilities:           {self.stats['total_facilities']}")
        print(f"Facilities with mentions:   {self.stats['facilities_with_mentions']}")
        print(f"Mentions processed:         {self.stats['mentions_processed']}")
        print()
        print(f"Auto-accepted:              {self.stats['auto_accepted']}")
        print(f"Review queue:               {self.stats['review_queue']}")
        print(f"Pending:                    {self.stats['pending']}")
        print()
        print(f"Relationships written:      {self.stats['relationships_written']}")
        print(f"Failed:                     {self.stats['failed']}")
        print("="*60)

        # Add pending companies summary
        if self.pending_tracker:
            try:
                summary = self.pending_tracker.get_summary_stats()
                print("\nPENDING COMPANIES")
                print("-"*60)
                print(f"Total pending:              {summary['total_pending']}")
                print(f"By status:                  {summary['by_status']}")
                print(f"Avg frequency:              {summary['avg_frequency']}")
                if summary['most_mentioned_company']:
                    print(f"Most mentioned:             {summary['most_mentioned_company']} ({summary['max_frequency']}x)")
            except Exception as e:
                logger.debug(f"Could not get pending companies summary: {e}")

        if self.dry_run:
            print("\n[DRY RUN] No changes were saved")
        else:
            print(f"\nRelationships saved to: {RELATIONSHIPS_FILE}")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich facilities with company links using CompanyResolver (Phase 2)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--country',
        help='Enrich specific country (e.g., ZAF, AUS, IDN)'
    )
    parser.add_argument(
        '--min-confidence',
        type=float,
        default=0.75,
        help='Minimum confidence for relationships (0.0-1.0, default: 0.75)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without saving'
    )

    args = parser.parse_args()

    if not RESOLVER_AVAILABLE:
        print("ERROR: CompanyResolver not available")
        print("\nMake sure Phase 2 utilities are set up:")
        print("  - scripts/utils/company_resolver.py")
        print("  - scripts/utils/id_utils.py")
        print("  - EntityIdentity library (pip install entityidentity)")
        return 1

    # Run enrichment
    enricher = CompanyEnricher(min_confidence=args.min_confidence, dry_run=args.dry_run)

    try:
        if args.country:
            enricher.enrich_country(args.country)
        else:
            enricher.enrich_all()

        # Save relationships to parquet
        enricher.save_relationships()

        # Print summary
        enricher.print_summary()
        return 0

    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
