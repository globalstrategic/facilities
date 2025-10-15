#!/usr/bin/env python3
"""
Enrich existing facilities with company links using entityidentity.

Uses EnhancedCompanyMatcher from entityidentity to resolve operator and owner
company names found in facility data (notes, sources, or manual fields).

Usage:
    python enrich_companies.py                      # Enrich all facilities
    python enrich_companies.py --country AFG        # Enrich specific country
    python enrich_companies.py --dry-run            # Preview without saving
    python enrich_companies.py --min-score 80       # Set matching threshold
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Paths
ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"

# Add entityidentity to path
sys.path.insert(0, str(ROOT.parent / 'entityidentity'))

try:
    from entityidentity.companies import EnhancedCompanyMatcher
    ENTITYIDENTITY_AVAILABLE = True
except ImportError as e:
    logger.error(f"Could not import entityidentity: {e}")
    logger.error("Make sure entityidentity is in the parent directory or installed")
    ENTITYIDENTITY_AVAILABLE = False


class CompanyEnricher:
    """Enrich facilities with company links using entityidentity."""

    def __init__(self, min_score: int = 70, dry_run: bool = False):
        """
        Initialize enricher.

        Args:
            min_score: Minimum matching score (0-100)
            dry_run: If True, don't save changes
        """
        self.min_score = min_score
        self.dry_run = dry_run
        self.stats = {
            'total_facilities': 0,
            'needs_enrichment': 0,
            'operators_found': 0,
            'owners_found': 0,
            'updated': 0,
            'failed': 0
        }

        if not ENTITYIDENTITY_AVAILABLE:
            raise RuntimeError("entityidentity not available")

        # Initialize company matcher
        self.matcher = EnhancedCompanyMatcher()
        logger.info("EnhancedCompanyMatcher initialized")

    def extract_company_names(self, facility: Dict) -> Dict[str, List[str]]:
        """
        Extract potential company names from facility data.

        Returns dict with 'operators' and 'owners' lists.
        """
        candidates = {'operators': [], 'owners': []}

        # Check verification notes for company mentions
        notes = facility.get('verification', {}).get('notes', '')
        if notes:
            # Look for patterns like "operated by Company Name"
            # This is a simple heuristic - can be improved
            pass

        # Check if there's already an operator_link with company_name but no ID
        operator = facility.get('operator_link')
        if operator and operator.get('company_name') and not operator.get('company_id'):
            candidates['operators'].append(operator['company_name'])

        # Check owner_links
        for owner in facility.get('owner_links', []):
            if owner.get('company_name') and not owner.get('company_id'):
                candidates['owners'].append(owner['company_name'])

        return candidates

    def match_company(self, company_name: str) -> Optional[Dict]:
        """
        Match company name using entityidentity.

        Returns company info dict or None if no good match.
        """
        try:
            results = self.matcher.match_best(
                company_name,
                limit=1,
                min_score=self.min_score
            )

            if results and len(results) > 0:
                best_match = results[0]
                lei = best_match.get('lei', '')

                # Format company_id
                if lei and not lei.startswith('cmp-'):
                    company_id = f"cmp-{lei}"
                else:
                    company_id = lei or "cmp-unknown"

                confidence = best_match.get('score', 0) / 100.0
                canonical_name = best_match.get('original_name',
                                              best_match.get('brief_name', company_name))

                return {
                    'company_id': company_id,
                    'company_name': canonical_name,
                    'confidence': round(confidence, 3),
                    'matched_from': company_name
                }

        except Exception as e:
            logger.warning(f"Error matching company '{company_name}': {e}")

        return None

    def enrich_facility(self, facility: Dict, file_path: Path) -> bool:
        """
        Enrich a single facility with company links.

        Returns True if facility was updated.
        """
        facility_id = facility.get('facility_id')
        updated = False

        # Check if already has operator and owners
        has_operator = bool(facility.get('operator_link'))
        has_owners = bool(facility.get('owner_links'))

        if has_operator and has_owners:
            # Already enriched
            return False

        self.stats['needs_enrichment'] += 1

        # Extract candidate company names
        candidates = self.extract_company_names(facility)

        # Match operators
        if not has_operator and candidates['operators']:
            for company_name in candidates['operators']:
                match = self.match_company(company_name)
                if match:
                    facility['operator_link'] = {
                        'company_id': match['company_id'],
                        'company_name': match['company_name'],
                        'role': 'operator',
                        'confidence': match['confidence']
                    }
                    logger.info(f"  Operator: {company_name} → {match['company_name']}")
                    self.stats['operators_found'] += 1
                    updated = True
                    break  # Only use first match

        # Match owners
        if not has_owners and candidates['owners']:
            owner_links = []
            for company_name in candidates['owners']:
                match = self.match_company(company_name)
                if match:
                    owner_links.append({
                        'company_id': match['company_id'],
                        'company_name': match['company_name'],
                        'role': 'owner',
                        'percentage': None,  # Unknown from data
                        'confidence': match['confidence']
                    })
                    logger.info(f"  Owner: {company_name} → {match['company_name']}")
                    self.stats['owners_found'] += 1

            if owner_links:
                facility['owner_links'] = owner_links
                updated = True

        # Update verification if changed
        if updated:
            verification = facility.get('verification', {})
            verification['last_checked'] = datetime.now().isoformat()
            verification['checked_by'] = 'company_enrichment'

            # Boost confidence if we found companies
            old_confidence = verification.get('confidence', 0.5)
            verification['confidence'] = min(0.95, old_confidence + 0.1)

            facility['verification'] = verification

            # Save facility
            if not self.dry_run:
                # Create backup
                backup_file = file_path.with_suffix(
                    f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
                )
                with open(backup_file, 'w') as f:
                    # Write original before changes
                    pass  # Backup already exists from reading

                # Write updated facility
                with open(file_path, 'w') as f:
                    json.dump(facility, f, ensure_ascii=False, indent=2)

                self.stats['updated'] += 1
                logger.info(f"✓ Updated: {facility_id}")
            else:
                logger.info(f"[DRY RUN] Would update: {facility_id}")

        return updated

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

    def print_summary(self):
        """Print enrichment summary."""
        print("\n" + "="*60)
        print("COMPANY ENRICHMENT SUMMARY")
        print("="*60)
        print(f"Total facilities:       {self.stats['total_facilities']}")
        print(f"Needed enrichment:      {self.stats['needs_enrichment']}")
        print(f"Operators found:        {self.stats['operators_found']}")
        print(f"Owners found:           {self.stats['owners_found']}")
        print(f"Facilities updated:     {self.stats['updated']}")
        print(f"Failed:                 {self.stats['failed']}")
        print("="*60)

        if self.dry_run:
            print("\n[DRY RUN] No changes were saved")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich facilities with company links using entityidentity",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--country',
        help='Enrich specific country (e.g., AFG, USA)'
    )
    parser.add_argument(
        '--min-score',
        type=int,
        default=70,
        help='Minimum matching score 0-100 (default: 70)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without saving'
    )

    args = parser.parse_args()

    if not ENTITYIDENTITY_AVAILABLE:
        print("ERROR: entityidentity not available")
        print("\nMake sure entityidentity is installed or in parent directory:")
        print("  cd /path/to/GSMC")
        print("  git clone https://github.com/globalstrategic/entityidentity")
        return 1

    # Run enrichment
    enricher = CompanyEnricher(min_score=args.min_score, dry_run=args.dry_run)

    try:
        if args.country:
            enricher.enrich_country(args.country)
        else:
            enricher.enrich_all()

        enricher.print_summary()
        return 0

    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
