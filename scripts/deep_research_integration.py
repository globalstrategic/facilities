#!/usr/bin/env python3
"""
Deep Research Integration Pipeline for Facility Enrichment

This script handles the integration of Gemini Deep Research data into the facility JSON files.
It supports both batch processing and incremental updates, maintaining data lineage and verification status.

Uses entityidentity library directly for company resolution via EnhancedCompanyMatcher.

Usage:
    # Process research data for a specific country/metal
    python deep_research_integration.py --country USA --metal platinum --file research_output.json

    # Process batch research data
    python deep_research_integration.py --batch research_batch.jsonl

    # Generate prompts for Deep Research
    python deep_research_integration.py --generate-prompt --country ZAF --metal gold

Requirements:
    - entityidentity library (https://github.com/globalstrategic/entityidentity)
    - Clone to parent directory or install as package
"""

import json
import argparse
import pathlib
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import logging
import re

# Add entityidentity to path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / 'entityidentity'))

try:
    from entityidentity.companies import EnhancedCompanyMatcher
    ENTITYIDENTITY_AVAILABLE = True
except ImportError:
    print("Warning: entityidentity not available. Company resolution will be limited.")
    ENTITYIDENTITY_AVAILABLE = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deep_research_integration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Paths
ROOT = pathlib.Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"
SUPPLY_DIR = ROOT / "config" / "supply"
RESEARCH_RAW_DIR = ROOT / "output" / "research_raw"
RESEARCH_EVIDENCE_DIR = ROOT / "output" / "research_evidence"
PROMPTS_DIR = ROOT / "output" / "research_prompts"

# Create directories
for dir_path in [RESEARCH_RAW_DIR, RESEARCH_EVIDENCE_DIR, PROMPTS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)


class DeepResearchIntegrator:
    """Handles integration of Deep Research data into facility files.

    Uses entityidentity's EnhancedCompanyMatcher for company resolution.
    """

    def __init__(self):
        self.stats = defaultdict(int)
        self.company_cache = {}
        self.updates_log = []

        # Initialize company matcher if entityidentity available
        self.company_matcher = None
        if ENTITYIDENTITY_AVAILABLE:
            try:
                self.company_matcher = EnhancedCompanyMatcher()
                logger.info("EnhancedCompanyMatcher initialized")
            except Exception as e:
                logger.warning(f"Could not initialize EnhancedCompanyMatcher: {e}")

    def slugify(self, text: str) -> str:
        """Convert text to URL-safe slug."""
        if not text:
            return ""
        text = text.lower().strip()
        text = re.sub(r'[^a-z0-9]+', '-', text)
        return text.strip('-')

    def load_facility(self, facility_id: str, country_iso3: str) -> Optional[Dict]:
        """Load a facility JSON file."""
        facility_path = FACILITIES_DIR / country_iso3 / f"{facility_id}.json"
        if not facility_path.exists():
            logger.warning(f"Facility not found: {facility_path}")
            return None

        with open(facility_path, 'r') as f:
            return json.load(f)

    def save_facility(self, facility: Dict, backup: bool = True):
        """Save updated facility JSON file."""
        country_iso3 = facility['country_iso3']
        facility_id = facility['facility_id']
        facility_path = FACILITIES_DIR / country_iso3 / f"{facility_id}.json"

        # Backup original if requested
        if backup and facility_path.exists():
            backup_path = facility_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
            with open(facility_path, 'r') as f:
                original = json.load(f)
            with open(backup_path, 'w') as f:
                json.dump(original, f, indent=2)
            logger.debug(f"Backed up original to {backup_path}")

        # Save updated facility
        with open(facility_path, 'w') as f:
            json.dump(facility, f, indent=2, ensure_ascii=False)

        self.stats['facilities_updated'] += 1
        logger.info(f"Updated facility: {facility_id}")

    def resolve_company(self, company_name: str, country: Optional[str] = None) -> Dict:
        """Resolve company name to canonical ID using EnhancedCompanyMatcher."""
        cache_key = (company_name, country)
        if cache_key in self.company_cache:
            return self.company_cache[cache_key]

        result = {
            'company_id': f"cmp-{self.slugify(company_name)}",
            'name': company_name,
            'confidence': 0.5
        }

        if self.company_matcher:
            try:
                results = self.company_matcher.match_best(
                    company_name,
                    limit=1,
                    min_score=70
                )
                if results and len(results) > 0:
                    best = results[0]
                    lei = best.get('lei', '')
                    company_id = f"cmp-{lei}" if lei and not lei.startswith('cmp-') else (lei or f"cmp-{self.slugify(company_name)}")
                    confidence = best.get('score', 70) / 100.0
                    resolved_name = best.get('original_name', best.get('brief_name', company_name))

                    result = {
                        'company_id': company_id,
                        'name': resolved_name,
                        'country': best.get('country'),
                        'lei': lei if lei else None,
                        'confidence': round(confidence, 3)
                    }
                    logger.debug(f"Resolved '{company_name}' to '{result['company_id']}' (score: {best.get('score')})")
            except Exception as e:
                logger.debug(f"Could not resolve company '{company_name}': {e}")

        self.company_cache[cache_key] = result
        return result

    def merge_research_data(self, facility: Dict, research: Dict) -> Dict:
        """Merge Deep Research data into facility JSON."""
        updated = False

        # Update status if provided
        if 'status' in research and research['status'] != 'unknown':
            facility['status'] = research['status']
            updated = True
            self.stats['status_updates'] += 1

        # Update owner links
        if 'owners' in research and research['owners']:
            new_owner_links = []
            for owner in research['owners']:
                company_info = self.resolve_company(
                    owner.get('name', ''),
                    owner.get('country')
                )

                owner_link = {
                    'company_id': company_info['company_id'],
                    'role': owner.get('role', 'owner'),
                    'confidence': min(
                        company_info['confidence'],
                        owner.get('confidence', 0.7)
                    )
                }

                # Add percentage if available
                if 'percentage' in owner:
                    owner_link['percentage'] = owner['percentage']

                # Add additional metadata if available
                if 'lei' in company_info:
                    owner_link['lei'] = company_info['lei']

                new_owner_links.append(owner_link)

            if new_owner_links:
                facility['owner_links'] = new_owner_links
                updated = True
                self.stats['ownership_updates'] += 1

        # Update operator
        if 'operator' in research and research['operator']:
            operator = research['operator']
            company_info = self.resolve_company(
                operator.get('name', ''),
                operator.get('country')
            )

            facility['operator_link'] = {
                'company_id': company_info['company_id'],
                'confidence': min(
                    company_info['confidence'],
                    operator.get('confidence', 0.7)
                )
            }

            if 'lei' in company_info:
                facility['operator_link']['lei'] = company_info['lei']

            updated = True
            self.stats['operator_updates'] += 1

        # Update products/capacity
        if 'products' in research and research['products']:
            new_products = []
            for product in research['products']:
                new_products.append({
                    'stream': product.get('stream', 'unknown'),
                    'capacity': product.get('capacity'),
                    'unit': product.get('unit'),
                    'year': product.get('year')
                })

            if new_products:
                facility['products'] = new_products
                updated = True
                self.stats['product_updates'] += 1

        # Add new sources
        if 'sources' in research:
            existing_sources = {(s.get('type'), s.get('id', s.get('url')))
                              for s in facility.get('sources', [])}

            for source in research['sources']:
                source_key = (source.get('type'), source.get('id', source.get('url')))
                if source_key not in existing_sources:
                    facility['sources'].append({
                        'type': source.get('type', 'gemini_research'),
                        'id': source.get('id'),
                        'url': source.get('url'),
                        'date': datetime.now().isoformat()
                    })
                    self.stats['sources_added'] += 1

        # Update verification status
        if updated:
            facility['verification'] = {
                'status': 'llm_suggested',
                'confidence': research.get('confidence', 0.7),
                'last_checked': datetime.now().isoformat(),
                'checked_by': 'gemini_deep_research'
            }

            # Add research notes if provided
            if 'notes' in research:
                facility['verification']['notes'] = research['notes']

        # Log the update
        self.updates_log.append({
            'facility_id': facility['facility_id'],
            'timestamp': datetime.now().isoformat(),
            'fields_updated': [k for k in ['status', 'owners', 'operator', 'products']
                              if k in research],
            'sources_added': len(research.get('sources', []))
        })

        return facility

    def process_research_file(self, research_file: pathlib.Path,
                            country: Optional[str] = None,
                            metal: Optional[str] = None) -> Dict:
        """Process a Deep Research output file."""
        logger.info(f"Processing research file: {research_file}")

        with open(research_file, 'r') as f:
            if research_file.suffix == '.jsonl':
                # Process JSONL format (multiple facilities)
                for line_num, line in enumerate(f, 1):
                    try:
                        research_data = json.loads(line)
                        self._process_single_research(research_data, country, metal)
                    except json.JSONDecodeError as e:
                        logger.error(f"Line {line_num}: Invalid JSON - {e}")
                    except Exception as e:
                        logger.error(f"Line {line_num}: Processing error - {e}")
            else:
                # Process single JSON file
                research_data = json.load(f)
                if isinstance(research_data, list):
                    # List of facilities
                    for item in research_data:
                        self._process_single_research(item, country, metal)
                else:
                    # Single facility
                    self._process_single_research(research_data, country, metal)

        # Save raw research for audit
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_copy = RESEARCH_RAW_DIR / f"{metal or 'unknown'}_{country or 'unknown'}_{timestamp}.json"
        with open(research_file, 'r') as src:
            with open(raw_copy, 'w') as dst:
                dst.write(src.read())
        logger.info(f"Saved raw research to {raw_copy}")

        return dict(self.stats)

    def _process_single_research(self, research_data: Dict,
                                country: Optional[str],
                                metal: Optional[str]):
        """Process research data for a single facility."""
        # Get facility identifier
        facility_id = research_data.get('facility_id')
        facility_name = research_data.get('name')

        if not facility_id and not facility_name:
            logger.warning("Research data missing facility_id and name")
            return

        # Try to find the facility
        if facility_id:
            # Direct ID match
            country_iso3 = facility_id.split('-')[0].upper()
            facility = self.load_facility(facility_id, country_iso3)
        else:
            # Search by name
            facility = self._find_facility_by_name(facility_name, country, metal)

        if not facility:
            logger.warning(f"Could not find facility: {facility_id or facility_name}")
            self.stats['facilities_not_found'] += 1
            return

        # Merge research data
        updated_facility = self.merge_research_data(facility, research_data)

        # Save updated facility
        self.save_facility(updated_facility)

    def _find_facility_by_name(self, name: str, country: Optional[str],
                              metal: Optional[str]) -> Optional[Dict]:
        """Find facility by name, optionally filtered by country/metal."""
        name_slug = self.slugify(name)

        # If country provided, search in that country's directory
        if country:
            country_dir = FACILITIES_DIR / country.upper()
            if country_dir.exists():
                for facility_file in country_dir.glob("*.json"):
                    with open(facility_file, 'r') as f:
                        facility = json.load(f)
                    if self.slugify(facility['name']) == name_slug:
                        return facility

        # Search all facilities if no country or not found
        for facility_file in FACILITIES_DIR.glob("**/*.json"):
            with open(facility_file, 'r') as f:
                facility = json.load(f)

            # Check name match
            if self.slugify(facility['name']) == name_slug:
                # If metal specified, verify it's in commodities
                if metal:
                    commodities = [c['metal'] for c in facility.get('commodities', [])]
                    if metal.lower() in commodities:
                        return facility
                else:
                    return facility

        return None

    def generate_research_prompt(self, country: str, metal: str,
                                limit: int = 50) -> str:
        """Generate a prompt for Deep Research based on existing facilities."""
        logger.info(f"Generating research prompt for {metal} in {country}")

        # Load metal index
        metal_index_path = SUPPLY_DIR / self.slugify(metal) / "facilities.index.json"
        if not metal_index_path.exists():
            logger.error(f"No facilities index for metal: {metal}")
            return ""

        with open(metal_index_path, 'r') as f:
            index = json.load(f)

        # Filter facilities by country
        facilities_data = []
        for facility_id in index['facilities']:
            if facility_id.startswith(f"{country.lower()}-"):
                country_iso3 = facility_id.split('-')[0].upper()
                facility = self.load_facility(facility_id, country_iso3)
                if facility:
                    facilities_data.append({
                        'facility_id': facility['facility_id'],
                        'name': facility['name'],
                        'aliases': facility.get('aliases', []),
                        'types': facility.get('types', []),
                        'location': facility.get('location', {}),
                        'primary_commodity': next(
                            (c['metal'] for c in facility.get('commodities', [])
                             if c.get('primary')), metal
                        )
                    })

                    if len(facilities_data) >= limit:
                        break

        if not facilities_data:
            logger.warning(f"No facilities found for {metal} in {country}")
            return ""

        # Generate prompt
        prompt = f"""You are analyzing {metal} mining and processing facilities in {country}.

I have identified {len(facilities_data)} facilities that need enrichment with current operational data.

## Existing Facility Data

```json
{json.dumps(facilities_data, indent=2)}
```

## Required Information

For each facility listed above, please research and provide:

1. **Operational Status**: Current status (operating, care_and_maintenance, closed, suspended, planned, construction)
2. **Ownership**: Company names with ownership percentages if available
3. **Operator**: The company currently operating the facility
4. **Production Capacity**: Annual production capacity with units and year
5. **Recent Updates**: Any significant changes or developments in the last 2 years

## Output Format

Please provide the enriched data in the following JSON format:

```json
[
  {{
    "facility_id": "existing_facility_id",
    "name": "Facility Name",
    "status": "operating|closed|care_and_maintenance|suspended|planned|construction",
    "owners": [
      {{
        "name": "Company Name",
        "percentage": 75.0,
        "role": "owner",
        "confidence": 0.9
      }}
    ],
    "operator": {{
      "name": "Operating Company",
      "confidence": 0.85
    }},
    "products": [
      {{
        "stream": "Product type (e.g., concentrate, refined metal)",
        "capacity": 100000,
        "unit": "tonnes",
        "year": 2024
      }}
    ],
    "sources": [
      {{
        "type": "web",
        "url": "https://example.com/article",
        "date": "2024-10-12"
      }}
    ],
    "confidence": 0.8,
    "notes": "Additional context or important information"
  }}
]
```

## Important Instructions

1. Only include facilities from the list provided above
2. Use the exact facility_id from the existing data
3. Provide sources (URLs) for all information
4. Indicate confidence levels (0.0-1.0) for uncertain data
5. If information is not available, omit the field rather than guessing
6. Focus on accuracy over completeness

Country: {country}
Metal: {metal}
Number of facilities: {len(facilities_data)}

Please research and provide the enriched facility data."""

        # Save prompt
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompt_file = PROMPTS_DIR / f"prompt_{metal}_{country}_{timestamp}.txt"
        with open(prompt_file, 'w') as f:
            f.write(prompt)

        logger.info(f"Prompt saved to {prompt_file}")
        return prompt

    def generate_report(self) -> str:
        """Generate a summary report of the integration process."""
        report = f"""
Deep Research Integration Report
================================
Generated: {datetime.now().isoformat()}

Statistics:
-----------
Facilities Updated: {self.stats['facilities_updated']}
Facilities Not Found: {self.stats['facilities_not_found']}
Status Updates: {self.stats['status_updates']}
Ownership Updates: {self.stats['ownership_updates']}
Operator Updates: {self.stats['operator_updates']}
Product Updates: {self.stats['product_updates']}
Sources Added: {self.stats['sources_added']}

Recent Updates:
--------------
"""
        for update in self.updates_log[-10:]:  # Last 10 updates
            report += f"\n- {update['facility_id']}: {', '.join(update['fields_updated'])}"

        return report


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Deep Research Integration Pipeline')

    # Action arguments
    parser.add_argument('--process', type=str, help='Process a research file')
    parser.add_argument('--batch', type=str, help='Process batch JSONL file')
    parser.add_argument('--generate-prompt', action='store_true',
                       help='Generate a research prompt')

    # Filter arguments
    parser.add_argument('--country', type=str, help='Country ISO3 code')
    parser.add_argument('--metal', type=str, help='Metal name')

    # Options
    parser.add_argument('--limit', type=int, default=50,
                       help='Limit facilities in prompt')
    parser.add_argument('--no-backup', action='store_true',
                       help='Skip backup of original files')

    args = parser.parse_args()

    integrator = DeepResearchIntegrator()

    if args.generate_prompt:
        if not args.country or not args.metal:
            print("Error: --country and --metal required for prompt generation")
            sys.exit(1)

        prompt = integrator.generate_research_prompt(
            args.country, args.metal, args.limit
        )
        print(prompt)

    elif args.process:
        research_file = pathlib.Path(args.process)
        if not research_file.exists():
            print(f"Error: File not found: {research_file}")
            sys.exit(1)

        stats = integrator.process_research_file(
            research_file, args.country, args.metal
        )
        print(integrator.generate_report())

    elif args.batch:
        batch_file = pathlib.Path(args.batch)
        if not batch_file.exists():
            print(f"Error: File not found: {batch_file}")
            sys.exit(1)

        stats = integrator.process_research_file(
            batch_file, args.country, args.metal
        )
        print(integrator.generate_report())

    else:
        parser.print_help()


if __name__ == "__main__":
    main()