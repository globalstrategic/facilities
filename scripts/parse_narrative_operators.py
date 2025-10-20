#!/usr/bin/env python3
"""
Parse operator/owner mentions from narrative research text using regex patterns.

This script extracts company mentions from prose descriptions like:
  "Operated by Myanmar Wanbao Mining Copper Ltd., a joint venture between
   Wanbao Mining Ltd. (30%), UMEHL (19%), and No. 1 Mining Enterprise (51%)"

Then uses the existing ownership_parser.py to resolve companies via EntityIdentity.

Usage:
    python scripts/parse_narrative_operators.py data/import_reports/Myanmar.txt --country MMR
    python scripts/parse_narrative_operators.py report.txt --country DZA --dry-run
"""

import json
import argparse
import pathlib
import sys
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Add utils to path
sys.path.insert(0, str(pathlib.Path(__file__).parent))

# Try to import ownership parser and CompanyResolver
try:
    from utils.ownership_parser import parse_ownership
    from utils.company_resolver import CompanyResolver
    ENTITY_RESOLUTION_AVAILABLE = True
    logger.info("Entity resolution modules loaded")
except ImportError as e:
    logger.warning(f"Could not load entity resolution: {e}")
    ENTITY_RESOLUTION_AVAILABLE = False

# Paths
ROOT = pathlib.Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"


class NarrativeParser:
    """Parse operator/owner information from narrative text."""

    def __init__(self, company_resolver=None):
        self.company_resolver = company_resolver
        self.stats = defaultdict(int)

    def extract_facility_paragraphs(self, text: str) -> Dict[str, str]:
        """
        Extract paragraphs describing each facility.

        Returns: Dict mapping facility_name -> description_text
        """
        # Pattern: "Facility Name: Description text that continues..."
        # Stops at next facility or section header

        paragraphs = {}
        lines = text.split('\n')

        current_facility = None
        current_text = []

        for line in lines:
            # Check if line is a facility header (ends with colon, capitalized)
            if ':' in line and not line.strip().startswith('Section'):
                parts = line.split(':', 1)
                if len(parts) == 2 and parts[0].strip() and parts[0][0].isupper():
                    # Save previous facility
                    if current_facility and current_text:
                        paragraphs[current_facility] = ' '.join(current_text).strip()

                    # Start new facility
                    facility_name = parts[0].strip()
                    # Clean up common prefixes
                    facility_name = re.sub(r'^\d+\.\d+\s+', '', facility_name)  # "2.1 Copper" -> "Copper"

                    current_facility = facility_name
                    current_text = [parts[1].strip()]
                    continue

            # Continuation of current facility description
            if current_facility:
                # Stop at section headers or empty lines followed by headers
                if line.strip().startswith('Section') or line.strip().startswith('Table'):
                    if current_facility and current_text:
                        paragraphs[current_facility] = ' '.join(current_text).strip()
                    current_facility = None
                    current_text = []
                elif line.strip():
                    current_text.append(line.strip())

        # Save last facility
        if current_facility and current_text:
            paragraphs[current_facility] = ' '.join(current_text).strip()

        logger.info(f"Extracted {len(paragraphs)} facility descriptions")
        return paragraphs

    def parse_operator(self, text: str) -> Optional[str]:
        """Extract operator from text using regex patterns."""
        patterns = [
            r'operated by ([^,\.]+?)(?:,|\.|$)',
            r'operator of [^,]+ is ([^,\.]+?)(?:,|\.|$)',
            r'([^,\.]+?) operates',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                operator = match.group(1).strip()
                # Clean up common suffixes
                operator = re.sub(r'\s+\([^)]*\)$', '', operator)  # Remove trailing (details)
                return operator

        return None

    def parse_ownership(self, text: str) -> Optional[str]:
        """Extract ownership string for parsing with ownership_parser."""
        # Pattern 1: "joint venture between ... (X%)" - capture ALL companies until sentence end
        # Use GREEDY matching to get all ownership percentages
        jv_pattern = r'joint venture\s+between\s+(.+\(\d+(?:\.\d+)?%\))'
        match = re.search(jv_pattern, text, re.IGNORECASE)
        if match:
            ownership = match.group(1).strip()
            # Stop at sentence-ending punctuation (not mid-sentence abbreviations like "No.")
            ownership = re.split(r'\.\s+[A-Z]', ownership)[0]  # Stop at ". [Capital]" (new sentence)
            return ownership.strip()

        # Pattern 2: "partnership between ... (X%)"
        partner_pattern = r'partnership between\s+(.+\(\d+(?:\.\d+)?%\))'
        match = re.search(partner_pattern, text, re.IGNORECASE)
        if match:
            ownership = match.group(1).strip()
            ownership = re.split(r'\.\s+[A-Z]', ownership)[0]
            return ownership.strip()

        # Pattern 3: "XX% controlling stake by Company, with partners Company (YY%) and Company (ZZ%)"
        stake_pattern = r'stake\s+by\s+([^,]+),\s+with\s+(?:local\s+)?partners?\s+(.+\(\d+(?:\.\d+)?%\))'
        match = re.search(stake_pattern, text, re.IGNORECASE)
        if match:
            ownership = f"{match.group(1)}, {match.group(2)}".strip()
            ownership = re.split(r'\.\s+[A-Z]', ownership)[0]
            return ownership.strip()

        # Pattern 4: "owned by Company" (no percentage)
        owned_pattern = r'owned by\s+([^\.]+?)(?:\s*\.|,|$)'
        match = re.search(owned_pattern, text, re.IGNORECASE)
        if match:
            ownership = match.group(1).strip()
            # Don't include trailing sentence fragments
            ownership = re.sub(r'\s+(which|that|who|under|in).*$', '', ownership)
            return ownership

        return None

    def extract_company_mentions(self, facility_name: str, text: str) -> Dict:
        """
        Extract operator and ownership from facility description.

        Returns dict with 'operator' and 'owners' keys
        """
        result = {'operator': None, 'owners': []}

        # Extract operator
        operator_str = self.parse_operator(text)
        if operator_str:
            result['operator'] = operator_str
            self.stats['operators_found'] += 1

        # Extract ownership
        ownership_str = self.parse_ownership(text)
        if ownership_str:
            result['ownership_raw'] = ownership_str
            self.stats['ownership_strings_found'] += 1

        return result

    def resolve_company_mentions(self, mentions: Dict, country_hint: str) -> Dict:
        """Resolve company names to canonical IDs using CompanyResolver."""
        if not ENTITY_RESOLUTION_AVAILABLE or not self.company_resolver:
            logger.warning("Entity resolution not available")
            return mentions

        # Import ownership parser
        from utils.ownership_parser import parse_ownership

        resolved = {'operator_link': None, 'owner_links': []}

        # Resolve operator using CompanyResolver
        if mentions.get('operator'):
            try:
                result = self.company_resolver.resolve_name(
                    mentions['operator'],
                    country_hint=country_hint
                )
                if result:
                    resolved['operator_link'] = {
                        "company_id": result['company_id'],
                        "confidence": result['confidence']
                    }
                    self.stats['operators_resolved'] += 1
            except Exception as e:
                logger.debug(f"Error resolving operator '{mentions['operator']}': {e}")

        # Resolve ownership using ownership_parser
        # Note: ownership_parser will use the company_resolver's matcher internally
        if mentions.get('ownership_raw'):
            try:
                # Get the underlying matcher from company_resolver for ownership_parser
                company_matcher = self.company_resolver.company_matcher if hasattr(self.company_resolver, 'company_matcher') else None
                if company_matcher:
                    owner_links = parse_ownership(
                        mentions['ownership_raw'],
                        company_matcher,
                        country_hint=country_hint
                    )
                    if owner_links:
                        resolved['owner_links'] = owner_links
                        self.stats['owners_resolved'] += len(owner_links)
            except Exception as e:
                logger.debug(f"Error parsing ownership '{mentions['ownership_raw']}': {e}")

        return resolved


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

    logger.info(f"Loaded {len(facilities)} facilities for {country_iso3}")
    return facilities


def match_facility_to_description(facility_name: str, descriptions: Dict[str, str]) -> Optional[Tuple[str, str]]:
    """
    Match a facility name to a description from the narrative.

    Returns: (matched_key, description_text) or None
    """
    # Try exact match first
    if facility_name in descriptions:
        return (facility_name, descriptions[facility_name])

    # Try fuzzy match (check if facility name is in description key)
    facility_slug = facility_name.lower().replace(' ', '').replace('-', '')
    for desc_key, desc_text in descriptions.items():
        desc_slug = desc_key.lower().replace(' ', '').replace('-', '')
        if facility_slug in desc_slug or desc_slug in facility_slug:
            return (desc_key, desc_text)

    # Try checking if facility name appears in description text
    for desc_key, desc_text in descriptions.items():
        if facility_name.lower() in desc_text.lower():
            return (desc_key, desc_text)

    return None


def update_facility_json(facility_path: pathlib.Path, mentions: Dict, resolved: Dict,
                        dry_run: bool = False, backup: bool = True) -> None:
    """Update facility JSON with extracted company information."""
    # Load existing facility
    with open(facility_path, 'r') as f:
        facility = json.load(f)

    # Backup if requested
    if backup and not dry_run:
        backup_path = facility_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(backup_path, 'w') as f:
            json.dump(facility, f, indent=2)

    # Update operator_link if resolved
    if resolved.get('operator_link'):
        facility['operator_link'] = resolved['operator_link']
    elif mentions.get('operator'):
        # Add to company_mentions as unresolved
        mention = {
            "name": mentions['operator'],
            "role": "operator",
            "confidence": 0.6,
            "source": "narrative_extraction"
        }
        if mention not in facility.get('company_mentions', []):
            if 'company_mentions' not in facility:
                facility['company_mentions'] = []
            facility['company_mentions'].append(mention)

    # Update owner_links if resolved
    if resolved.get('owner_links'):
        facility['owner_links'] = resolved['owner_links']
    elif mentions.get('ownership_raw'):
        # Add to company_mentions as unresolved
        # Try to extract individual companies from ownership string
        ownership_str = mentions['ownership_raw']
        # Simple regex to find company names before percentages
        companies = re.findall(r'([^,\(]+?)\s*\((\d+(?:\.\d+)?)\s*%\)', ownership_str)
        for company_name, percentage in companies:
            mention = {
                "name": company_name.strip(),
                "role": "owner",
                "percentage": float(percentage),
                "confidence": 0.6,
                "source": "narrative_extraction"
            }
            if 'company_mentions' not in facility:
                facility['company_mentions'] = []
            # Avoid duplicates
            if not any(m.get('name') == mention['name'] and m.get('role') == mention['role']
                      for m in facility['company_mentions']):
                facility['company_mentions'].append(mention)

    # Update verification
    facility['verification']['last_checked'] = datetime.now().isoformat()
    facility['verification']['notes'] = "Company mentions extracted from narrative research"

    # Save updated facility
    if not dry_run:
        with open(facility_path, 'w') as f:
            json.dump(facility, f, indent=2, ensure_ascii=False)
        logger.info(f"Updated {facility_path.name}")
    else:
        logger.info(f"DRY RUN: Would update {facility_path.name}")


def main():
    parser = argparse.ArgumentParser(
        description='Parse operator/owner from narrative research text'
    )
    parser.add_argument('report', type=str, help='Path to narrative report')
    parser.add_argument('--country', type=str, required=True, help='Country ISO3 code')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    parser.add_argument('--no-backup', action='store_true', help='Skip backup files')
    parser.add_argument('--no-resolve', action='store_true',
                       help='Skip entity resolution (just extract raw mentions)')

    args = parser.parse_args()

    # Load narrative
    report_path = pathlib.Path(args.report)
    if not report_path.exists():
        logger.error(f"Report not found: {report_path}")
        return 1

    with open(report_path, 'r') as f:
        narrative = f.read()

    logger.info(f"Loaded narrative: {len(narrative)} characters")

    # Initialize parser with CompanyResolver
    company_resolver = None
    if not args.no_resolve and ENTITY_RESOLUTION_AVAILABLE:
        try:
            # Use default config file if it exists
            config_path = ROOT / "config" / "gate_config.json"
            if config_path.exists():
                company_resolver = CompanyResolver.from_config(str(config_path), profile="moderate")
            else:
                # Fallback to default initialization
                company_resolver = CompanyResolver()
            logger.info("Entity resolution enabled")
        except Exception as e:
            logger.warning(f"Could not initialize CompanyResolver: {e}")
            logger.info("Entity resolution disabled - will only extract raw mentions")
    else:
        logger.info("Entity resolution disabled - will only extract raw mentions")

    parser_obj = NarrativeParser(company_resolver=company_resolver)

    # Extract facility descriptions
    descriptions = parser_obj.extract_facility_paragraphs(narrative)

    # Load existing facilities
    facilities = load_facilities_for_country(args.country.upper())
    if not facilities:
        logger.error("No facilities found")
        return 1

    # Match facilities to descriptions and extract mentions
    updated = 0
    for facility in facilities:
        match_result = match_facility_to_description(facility['name'], descriptions)
        if not match_result:
            logger.debug(f"No description found for {facility['name']}")
            continue

        desc_key, desc_text = match_result
        logger.info(f"Processing: {facility['name']} (matched to '{desc_key}')")

        # Extract mentions
        mentions = parser_obj.extract_company_mentions(facility['name'], desc_text)
        if not mentions.get('operator') and not mentions.get('ownership_raw'):
            logger.debug(f"  No company info found")
            continue

        logger.info(f"  Operator: {mentions.get('operator', 'N/A')}")
        logger.info(f"  Ownership: {mentions.get('ownership_raw', 'N/A')}")

        # Resolve if enabled
        resolved = {}
        if company_resolver and not args.no_resolve:
            resolved = parser_obj.resolve_company_mentions(mentions, args.country.upper())

        # Update facility JSON
        update_facility_json(
            facility['_path'],
            mentions,
            resolved,
            dry_run=args.dry_run,
            backup=not args.no_backup
        )
        updated += 1

    # Print summary
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Facilities processed: {len(facilities)}")
    print(f"Descriptions found: {len(descriptions)}")
    print(f"Facilities updated: {updated}")
    print(f"Operators found: {parser_obj.stats['operators_found']}")
    print(f"Ownership strings found: {parser_obj.stats['ownership_strings_found']}")
    if company_resolver:
        print(f"Operators resolved: {parser_obj.stats['operators_resolved']}")
        print(f"Owners resolved: {parser_obj.stats['owners_resolved']}")
    print("=" * 60)

    if not args.dry_run:
        print(f"\nNext step: Run company resolution with:")
        print(f"  python scripts/enrich_companies.py --country {args.country.upper()}")


if __name__ == "__main__":
    sys.exit(main())
