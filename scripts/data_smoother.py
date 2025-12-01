#!/usr/bin/env python3
"""
Comprehensive Data Smoother for Facility Database

Enhances facility data by:
1. Assessing name quality and identifying nonsensical names
2. Filling in missing coordinates via web search
3. Finding company/operator information
4. Determining facility status (operating/closed/etc.)
5. Interactive mode for human validation when confused

Usage:
    # Process with interactive mode
    python scripts/data_smoother.py --country USA --interactive --limit 10

    # Batch mode (no interaction)
    python scripts/data_smoother.py --country CHN --limit 20

    # Focus on low-quality names
    python scripts/data_smoother.py --low-quality-names-only --interactive
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.name_quality import NameQualityAssessor

try:
    import requests
    from openai import OpenAI
    HAS_DEPS = True
except ImportError:
    print("Error: Missing dependencies. Install with: pip install openai requests")
    sys.exit(1)


class InteractiveValidator:
    """Handle interactive validation with human input."""

    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self.decisions_cache = {}

    def ask_user(self, question: str, options: List[str], default: Optional[str] = None) -> str:
        """
        Ask user a question with multiple choice options.

        Args:
            question: The question to ask
            options: List of valid options
            default: Default option if user presses Enter

        Returns:
            Selected option
        """
        if not self.enabled:
            return default or options[0]

        print(f"\n{'='*70}")
        print(f"QUESTION: {question}")
        print('='*70)
        for i, opt in enumerate(options, 1):
            marker = " (default)" if opt == default else ""
            print(f"  [{i}] {opt}{marker}")

        while True:
            try:
                response = input(f"\nYour choice [1-{len(options)}]: ").strip()
                if not response and default:
                    print(f"Using default: {default}")
                    return default
                idx = int(response) - 1
                if 0 <= idx < len(options):
                    return options[idx]
                print(f"Please enter a number between 1 and {len(options)}")
            except (ValueError, KeyboardInterrupt):
                if default:
                    print(f"\nUsing default: {default}")
                    return default
                print("Invalid input. Please try again.")

    def confirm(self, message: str, default: bool = True) -> bool:
        """Ask for yes/no confirmation."""
        if not self.enabled:
            return default

        default_str = "Y/n" if default else "y/N"
        while True:
            response = input(f"\n{message} [{default_str}]: ").strip().lower()
            if not response:
                return default
            if response in ['y', 'yes']:
                return True
            if response in ['n', 'no']:
                return False
            print("Please answer 'y' or 'n'")

    def get_text_input(self, prompt: str, default: Optional[str] = None) -> Optional[str]:
        """Get free-form text input from user."""
        if not self.enabled:
            return default

        default_str = f" [{default}]" if default else ""
        response = input(f"\n{prompt}{default_str}: ").strip()
        return response if response else default


class DataSmoother:
    """Comprehensive facility data enrichment."""

    def __init__(self, interactive: bool = False, dry_run: bool = False):
        self.interactive = InteractiveValidator(enabled=interactive)
        self.dry_run = dry_run
        self.name_assessor = NameQualityAssessor()

        # Initialize clients
        self.openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
        self.tavily_key = os.environ.get('TAVILY_API_KEY')

        self.stats = {
            'processed': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0,
            'user_interventions': 0
        }

    def search_web(self, query: str) -> List[Dict]:
        """Search the web using Tavily."""
        if not self.tavily_key:
            return []

        try:
            response = requests.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": self.tavily_key,
                    "query": query,
                    "max_results": 10,
                    "include_raw_content": False
                },
                timeout=30
            )
            response.raise_for_status()
            return response.json().get("results", [])
        except Exception as e:
            print(f"  Search error: {e}")
            return []

    def extract_info_from_search(self, facility: Dict, search_results: List[Dict]) -> Dict:
        """
        Use LLM to extract structured information from search results.

        Returns enriched data including:
        - real_name: Better facility name
        - companies: List of operator/owner companies
        - coordinates: (lat, lon) tuple
        - status: operating/closed/unknown
        - location_details: town, province, etc.
        """
        if not search_results:
            return {}

        name = facility.get('name', '')
        lat = facility.get('location', {}).get('lat')
        lon = facility.get('location', {}).get('lon')

        # Build context from search results
        context = "\n\n".join([
            f"Title: {r.get('title', '')}\n{r.get('content', '')}"
            for r in search_results[:5]
        ])

        prompt = f"""Given this facility and web search results, extract structured information:

Facility Name: {name}
Coordinates: {lat}, {lon} (if available)

Search Results:
{context}

Extract and return JSON with:
{{
    "real_name": "actual facility name (null if current name is correct)",
    "companies": ["operator", "owner", ...],
    "coordinates": {{"lat": float, "lon": float}} (null if not found),
    "status": "operating|closed|unknown",
    "location_details": {{"town": "...", "province": "...", "country": "..."}},
    "confidence": "high|medium|low",
    "notes": "any relevant information"
}}

Be conservative - only fill in what you're confident about from the search results."""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"  LLM extraction error: {e}")
            return {}

    def smooth_facility(self, facility: Dict, facility_path: Path) -> bool:
        """
        Smooth/enrich a single facility.

        Returns True if facility was updated.
        """
        facility_id = facility.get('facility_id', 'unknown')
        name = facility.get('name', '')

        print(f"\n{'='*70}")
        print(f"Processing: {facility_id}")
        print(f"Name: {name}")
        print('='*70)

        # Assess name quality
        name_assessment = self.name_assessor.assess_name(name)
        print(f"Name Quality: {name_assessment['quality_score']:.2f} (Generic: {name_assessment['is_generic']})")

        if name_assessment['issues']:
            print(f"Issues: {', '.join(name_assessment['issues'])}")

        # Check what needs enrichment
        needs_enrichment, reasons = self.name_assessor.needs_enrichment(facility)

        if not needs_enrichment:
            print("✓ Facility looks good, skipping")
            self.stats['skipped'] += 1
            return False

        print(f"Needs enrichment: {', '.join(reasons)}")

        # Interactive: Ask if user wants to process this facility
        if self.interactive.enabled:
            if not self.interactive.confirm(f"Process this facility?", default=True):
                print("Skipped by user")
                self.stats['skipped'] += 1
                return False
            self.stats['user_interventions'] += 1

        # Build search query
        country = facility.get('country_iso3', '')
        query = f"{name} {country} mine facility operator owner"
        print(f"Searching: {query}")

        # Search the web
        search_results = self.search_web(query)
        print(f"Found {len(search_results)} search results")

        if not search_results:
            if self.interactive.enabled:
                custom_query = self.interactive.get_text_input(
                    "No results. Enter custom search query (or press Enter to skip)"
                )
                if custom_query:
                    search_results = self.search_web(custom_query)

        if not search_results:
            print("✗ No search results, cannot enrich")
            self.stats['errors'] += 1
            return False

        # Extract structured info from search results
        enriched = self.extract_info_from_search(facility, search_results)

        print("\nExtracted Information:")
        print(f"  Real name: {enriched.get('real_name') or 'None'}")
        print(f"  Companies: {enriched.get('companies', [])}")
        print(f"  Status: {enriched.get('status', 'unknown')}")
        print(f"  Confidence: {enriched.get('confidence', 'unknown')}")

        # Interactive: Allow manual input for missing fields
        if self.interactive.enabled:
            print("\n" + "="*70)
            print("MANUAL INPUT (press Enter to skip)")
            print("="*70)

            # Real name - only prompt if name quality is poor AND no better name found
            if not enriched.get('real_name') and name_assessment['is_generic']:
                print("\n⚠ Current name appears generic/low-quality")
                manual_name = self.interactive.get_text_input(
                    "Better facility name? (or press Enter to keep current)",
                    default=None
                )
                if manual_name:
                    enriched['real_name'] = manual_name
                    print(f"  → Set real name to: {manual_name}")
                    self.stats['user_interventions'] += 1

            # Companies
            if not enriched.get('companies') or len(enriched.get('companies', [])) == 0:
                manual_company = self.interactive.get_text_input(
                    "Company/operator name (or press Enter to skip)",
                    default=None
                )
                if manual_company:
                    enriched['companies'] = [manual_company]
                    print(f"  → Added company: {manual_company}")
                    self.stats['user_interventions'] += 1

            # Additional companies
            if enriched.get('companies'):
                add_more = self.interactive.get_text_input(
                    "Additional company? (or press Enter to skip)",
                    default=None
                )
                if add_more:
                    enriched['companies'].append(add_more)
                    print(f"  → Added company: {add_more}")
                    self.stats['user_interventions'] += 1

            # Status
            if enriched.get('status') == 'unknown':
                status_choice = self.interactive.ask_user(
                    "Facility status?",
                    ["operating", "closed", "unknown"],
                    default="unknown"
                )
                enriched['status'] = status_choice
                print(f"  → Set status to: {status_choice}")
                self.stats['user_interventions'] += 1

            # Coordinates
            if not enriched.get('coordinates') and not facility.get('location'):
                if self.interactive.confirm("Enter coordinates manually?", default=False):
                    lat = self.interactive.get_text_input("Latitude", default=None)
                    lon = self.interactive.get_text_input("Longitude", default=None)
                    if lat and lon:
                        try:
                            enriched['coordinates'] = {
                                'lat': float(lat),
                                'lon': float(lon)
                            }
                            print(f"  → Added coordinates: {lat}, {lon}")
                            self.stats['user_interventions'] += 1
                        except ValueError:
                            print("  ✗ Invalid coordinates format")

            # Final confirmation
            print("\n" + "="*70)
            print("FINAL REVIEW")
            print("="*70)
            print(f"  Real name: {enriched.get('real_name') or 'No change'}")
            print(f"  Companies: {enriched.get('companies', [])}")
            print(f"  Status: {enriched.get('status', 'unknown')}")
            if enriched.get('coordinates'):
                print(f"  Coordinates: {enriched['coordinates']}")

            if not self.interactive.confirm("Apply these changes?", default=True):
                print("Changes rejected by user")
                self.stats['skipped'] += 1
                return False

        # Apply enrichments
        updated = False

        # Update name if better one found
        if enriched.get('real_name') and enriched['real_name'] != name:
            facility['name'] = enriched['real_name']
            updated = True
            print(f"  → Updated name to: {enriched['real_name']}")

        # Add companies
        if enriched.get('companies'):
            existing_companies = set(facility.get('company_mentions', []))
            new_companies = [c for c in enriched['companies'] if c not in existing_companies]
            if new_companies:
                facility['company_mentions'] = list(existing_companies | set(new_companies))
                updated = True
                print(f"  → Added companies: {', '.join(new_companies)}")

        # Update coordinates
        if enriched.get('coordinates') and not facility.get('location'):
            facility['location'] = {
                'lat': enriched['coordinates']['lat'],
                'lon': enriched['coordinates']['lon'],
                'precision': 'approximate'
            }
            updated = True
            print(f"  → Added coordinates")

        # Update status
        if enriched.get('status') and enriched['status'] != 'unknown':
            facility['status'] = enriched['status']
            updated = True
            print(f"  → Updated status to: {enriched['status']}")

        # Update verification
        if updated:
            facility.setdefault('sources', []).append({
                'type': 'web_research',
                'id': 'data_smoother',
                'date': time.strftime('%Y-%m-%dT%H:%M:%SZ')
            })

            verification = facility.setdefault('verification', {})
            verification['last_checked'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
            verification['checked_by'] = 'data_smoother'
            if enriched.get('confidence') == 'high':
                verification['confidence'] = min(1.0, verification.get('confidence', 0.5) + 0.2)

        # Save if not dry run
        if updated and not self.dry_run:
            with open(facility_path, 'w') as f:
                json.dump(facility, f, indent=2)
            print("✓ Saved changes")
            self.stats['updated'] += 1
        elif updated:
            print("[DRY RUN] Would save changes")
            self.stats['updated'] += 1

        self.stats['processed'] += 1
        return updated

    def process_country(self, country_iso3: str, limit: Optional[int] = None):
        """Process all facilities in a country."""
        facilities_dir = Path(__file__).parent.parent / 'facilities' / country_iso3

        if not facilities_dir.exists():
            print(f"Country directory not found: {country_iso3}")
            return

        facility_files = sorted(facilities_dir.glob('*.json'))

        if limit:
            facility_files = facility_files[:limit]

        print(f"\nProcessing {len(facility_files)} facilities in {country_iso3}")
        print("="*70)

        for facility_path in facility_files:
            try:
                with open(facility_path) as f:
                    facility = json.load(f)

                self.smooth_facility(facility, facility_path)

            except Exception as e:
                print(f"Error processing {facility_path.name}: {e}")
                self.stats['errors'] += 1

    def print_summary(self):
        """Print processing summary."""
        print("\n" + "="*70)
        print("DATA SMOOTHING SUMMARY")
        print("="*70)
        print(f"Processed: {self.stats['processed']}")
        print(f"Updated: {self.stats['updated']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"Errors: {self.stats['errors']}")
        if self.interactive.enabled:
            print(f"User Interventions: {self.stats['user_interventions']}")
        print("="*70)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Comprehensive facility data smoother")
    parser.add_argument('--country', help='ISO3 country code')
    parser.add_argument('--limit', type=int, help='Max facilities to process')
    parser.add_argument('--interactive', action='store_true',
                       help='Interactive mode with human validation')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without saving')
    parser.add_argument('--low-quality-names-only', action='store_true',
                       help='Only process facilities with low-quality names')

    args = parser.parse_args()

    smoother = DataSmoother(
        interactive=args.interactive,
        dry_run=args.dry_run
    )

    if args.country:
        smoother.process_country(args.country, args.limit)
    else:
        print("Error: --country required for now")
        return 1

    smoother.print_summary()
    return 0


if __name__ == '__main__':
    sys.exit(main())
