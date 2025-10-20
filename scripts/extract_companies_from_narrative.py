#!/usr/bin/env python3
"""
Extract company mentions from narrative research reports.

This script:
1. Reads a narrative research report (like Myanmar.txt)
2. Uses Claude API to extract company/operator mentions for each facility
3. Updates facility JSONs with company_mentions[] array (Phase 1)
4. Prepares facilities for Phase 2 resolution via enrich_companies.py

Usage:
    export ANTHROPIC_API_KEY="your-key"
    python scripts/extract_companies_from_narrative.py data/import_reports/Myanmar.txt --country MMR
    python scripts/extract_companies_from_narrative.py report.txt --country DZA --dry-run
"""

import json
import argparse
import pathlib
import sys
import os
import re
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import Anthropic
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    logger.warning("anthropic package not installed. Install with: pip install anthropic")
    ANTHROPIC_AVAILABLE = False

# Paths
ROOT = pathlib.Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"
OUTPUT_DIR = ROOT / "output" / "company_extraction"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class CompanyExtractor:
    """Extracts company mentions from narrative text using Claude."""

    def __init__(self, api_key: Optional[str] = None, dry_run: bool = False):
        self.dry_run = dry_run
        self.stats = defaultdict(int)
        self.client = None

        if ANTHROPIC_AVAILABLE and not dry_run:
            api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
            if not api_key:
                raise ValueError("ANTHROPIC_API_KEY environment variable not set")
            self.client = anthropic.Anthropic(api_key=api_key)
            logger.info("Initialized Claude API client")

    def extract_companies_for_facilities(self, narrative_text: str,
                                        facilities: List[Dict],
                                        country_iso3: str) -> Dict[str, List[Dict]]:
        """
        Extract company mentions for a list of facilities from narrative text.

        Returns: Dict mapping facility_id -> list of company_mention objects
        """
        facility_names = [f['name'] for f in facilities]
        facility_ids = {f['name']: f['facility_id'] for f in facilities}

        prompt = self._build_extraction_prompt(narrative_text, facility_names, country_iso3)

        if self.dry_run:
            logger.info(f"DRY RUN: Would extract companies for {len(facilities)} facilities")
            return {}

        # Call Claude API
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=16000,
                temperature=0,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )

            # Parse JSON response
            response_text = response.content[0].text

            # Extract JSON from response (might be wrapped in ```json blocks)
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
            if json_match:
                response_text = json_match.group(1)

            extracted_data = json.loads(response_text)

            # Convert facility names to IDs
            result = {}
            for facility_name, mentions in extracted_data.items():
                if facility_name in facility_ids:
                    facility_id = facility_ids[facility_name]
                    result[facility_id] = mentions
                    self.stats['facilities_processed'] += 1
                    self.stats['mentions_extracted'] += len(mentions)

            logger.info(f"Extracted {self.stats['mentions_extracted']} company mentions "
                       f"for {self.stats['facilities_processed']} facilities")

            return result

        except Exception as e:
            logger.error(f"Error calling Claude API: {e}")
            raise

    def _build_extraction_prompt(self, narrative: str, facility_names: List[str],
                                 country_iso3: str) -> str:
        """Build the prompt for Claude to extract company mentions."""
        return f"""You are analyzing a research report about mining facilities in {country_iso3}.

TASK: Extract company mentions (operators, owners, partners) for each facility listed below.

FACILITIES TO ANALYZE:
{json.dumps(facility_names, indent=2)}

RESEARCH REPORT:
{narrative}

INSTRUCTIONS:
1. For each facility listed, find ALL company mentions in the report
2. Extract: company name, role (operator/owner/partner), ownership percentage (if mentioned)
3. Include confidence score (0.0-1.0) based on how explicit the mention is
4. Add source context (quote or paraphrase from the report)

ROLES:
- "operator": Company operating/running the facility
- "owner": Company owning shares (specify percentage if mentioned)
- "majority_owner": Owner with >50%
- "minority_owner": Owner with <50%
- "partner": Joint venture partner
- "contractor": Service provider/contractor

OUTPUT FORMAT (strict JSON):
{{
  "Facility Name 1": [
    {{
      "name": "Company Name",
      "role": "operator|owner|majority_owner|minority_owner|partner",
      "percentage": 51.0,  // optional, only if mentioned
      "confidence": 0.85,
      "source": "gemini_research",
      "evidence": "Quote or paraphrase from the report showing this relationship"
    }}
  ],
  "Facility Name 2": [...]
}}

IMPORTANT:
- Only include facilities from the provided list
- Only include companies explicitly mentioned in the report
- If no companies found for a facility, use empty array: []
- Use exact facility names from the list
- Be conservative with confidence scores
- Include percentage only when explicitly stated

Return ONLY the JSON object, no other text."""

    def update_facility_json(self, facility_path: pathlib.Path,
                            mentions: List[Dict], backup: bool = True) -> None:
        """Update a facility JSON file with extracted company mentions."""
        # Load existing facility
        with open(facility_path, 'r') as f:
            facility = json.load(f)

        # Backup if requested
        if backup and not self.dry_run:
            backup_path = facility_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
            with open(backup_path, 'w') as f:
                json.dump(facility, f, indent=2)

        # Update company_mentions
        existing_mentions = facility.get('company_mentions', [])

        # Merge new mentions with existing (avoid duplicates)
        existing_keys = {(m.get('name'), m.get('role')) for m in existing_mentions}
        for mention in mentions:
            key = (mention.get('name'), mention.get('role'))
            if key not in existing_keys:
                existing_mentions.append(mention)
                existing_keys.add(key)

        facility['company_mentions'] = existing_mentions

        # Update verification
        if mentions:
            facility['verification']['last_checked'] = datetime.now().isoformat()
            facility['verification']['notes'] = f"Company mentions extracted from narrative research"

        # Save updated facility
        if not self.dry_run:
            with open(facility_path, 'w') as f:
                json.dump(facility, f, indent=2, ensure_ascii=False)
            logger.info(f"Updated {facility_path.name} with {len(mentions)} company mentions")
        else:
            logger.info(f"DRY RUN: Would update {facility_path.name} with {len(mentions)} mentions")

        self.stats['facilities_updated'] += 1


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
                facility['_path'] = facility_file  # Store path for later update
                facilities.append(facility)
        except Exception as e:
            logger.warning(f"Could not load {facility_file}: {e}")

    logger.info(f"Loaded {len(facilities)} facilities for {country_iso3}")
    return facilities


def main():
    parser = argparse.ArgumentParser(
        description='Extract company mentions from narrative research reports'
    )
    parser.add_argument('report', type=str, help='Path to narrative research report')
    parser.add_argument('--country', type=str, required=True,
                       help='Country ISO3 code (e.g., MMR, DZA)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without making changes')
    parser.add_argument('--no-backup', action='store_true',
                       help='Skip backup of original files')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of facilities to process (for testing)')

    args = parser.parse_args()

    # Validate inputs
    report_path = pathlib.Path(args.report)
    if not report_path.exists():
        logger.error(f"Report file not found: {report_path}")
        sys.exit(1)

    country_iso3 = args.country.upper()

    # Load narrative text
    with open(report_path, 'r', encoding='utf-8') as f:
        narrative_text = f.read()

    logger.info(f"Loaded narrative report: {len(narrative_text)} characters")

    # Load existing facilities
    facilities = load_facilities_for_country(country_iso3)
    if not facilities:
        logger.error("No facilities found to process")
        sys.exit(1)

    # Limit if requested
    if args.limit:
        facilities = facilities[:args.limit]
        logger.info(f"Limited to {args.limit} facilities for testing")

    # Initialize extractor
    extractor = CompanyExtractor(dry_run=args.dry_run)

    # Extract company mentions
    try:
        extractions = extractor.extract_companies_for_facilities(
            narrative_text, facilities, country_iso3
        )
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)

    # Update facility JSONs
    for facility in facilities:
        facility_id = facility['facility_id']
        if facility_id in extractions:
            mentions = extractions[facility_id]
            if mentions:  # Only update if we found mentions
                extractor.update_facility_json(
                    facility['_path'],
                    mentions,
                    backup=not args.no_backup
                )

    # Print statistics
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Facilities processed: {extractor.stats['facilities_processed']}")
    print(f"Facilities updated: {extractor.stats['facilities_updated']}")
    print(f"Total mentions extracted: {extractor.stats['mentions_extracted']}")
    print("=" * 60)

    if not args.dry_run:
        print(f"\nNext step: Run company resolution with:")
        print(f"  python scripts/enrich_companies.py --country {country_iso3}")


if __name__ == "__main__":
    main()
