#!/usr/bin/env python3
"""
Clean up USA facilities with generic numeric names.

Uses web search to validate facilities and either enrich them
or mark them for deletion if they can't be validated.
"""

import json
import os
import sys
import glob
from pathlib import Path
import requests
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils.geocoding import GeocodingService


class NumericFacilityValidator:
    """Validates and cleans up numeric-named facilities."""

    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.geocoding = GeocodingService()
        self.results = {
            'validated': [],
            'to_delete': [],
            'errors': []
        }

    def load_facility(self, filepath: str) -> Dict:
        """Load facility JSON."""
        with open(filepath) as f:
            return json.load(f)

    def is_generic_name(self, name: str) -> bool:
        """Check if facility name is too generic."""
        generic_patterns = [
            lambda n: n.startswith('#') and any(c.isdigit() for c in n),
            lambda n: n.split()[0].isdigit() if n.split() else False,
            lambda n: len(n.split()) <= 2 and any(c.isdigit() for c in n),
        ]
        return any(pattern(name) for pattern in generic_patterns)

    def search_facility_info(self, facility: Dict) -> Optional[Dict]:
        """
        Search web for facility information using coordinates and name.
        Returns enriched data or None if not found.
        """
        name = facility.get('name', '')
        lat = facility.get('location', {}).get('lat')
        lon = facility.get('location', {}).get('lon')

        if not (lat and lon):
            return None

        # Try reverse geocoding to get location name
        try:
            location_info = self.geocoding.reverse_geocode(lat, lon)
            if location_info:
                town = location_info.get('town') or location_info.get('city')
                state = location_info.get('state')

                # Search for "{name} {town} {state} mine operator owner"
                search_query = f"{name}"
                if town:
                    search_query += f" {town}"
                if state:
                    search_query += f" {state}"
                search_query += " United States mine operator owner"

                print(f"  Searching: {search_query}")

                # TODO: Integrate with web_search_geocode.py or use Tavily API
                # For now, we'll just do basic validation

                return {
                    'town': town,
                    'state': state,
                    'search_query': search_query
                }
        except Exception as e:
            print(f"  Error reverse geocoding: {e}")
            return None

    def has_useful_info(self, facility: Dict) -> bool:
        """Check if facility has any useful distinguishing information."""
        # Has operator/owner info
        if facility.get('company_mentions') and len(facility['company_mentions']) > 0:
            return True

        # Has non-generic aliases
        aliases = facility.get('aliases', [])
        if aliases:
            for alias in aliases:
                if not self.is_generic_name(alias) and len(alias) > 10:
                    return True

        # Has high confidence
        if facility.get('verification', {}).get('confidence', 0) >= 0.85:
            return True

        return False

    def validate_facility(self, filepath: str) -> str:
        """
        Validate a facility and return 'keep', 'delete', or 'error'.
        """
        try:
            facility = self.load_facility(filepath)
            name = facility.get('name', 'N/A')
            facility_id = facility.get('facility_id', 'N/A')

            print(f"\n[{facility_id}] {name}")

            # Check if name is too generic
            if not self.is_generic_name(name):
                print("  ✓ Name is specific enough, keeping")
                self.results['validated'].append({
                    'file': filepath,
                    'reason': 'specific_name'
                })
                return 'keep'

            # Check if it has useful distinguishing info
            if self.has_useful_info(facility):
                print("  ✓ Has useful info (aliases/companies/high confidence), keeping")
                self.results['validated'].append({
                    'file': filepath,
                    'reason': 'has_useful_info'
                })
                return 'keep'

            # Try to enrich with web search
            print("  Searching for validation...")
            enriched = self.search_facility_info(facility)

            if enriched:
                print(f"  → Found location: {enriched.get('town')}, {enriched.get('state')}")
                print(f"  → Query: {enriched.get('search_query')}")
                print("  ⚠ Manual validation needed")
                self.results['validated'].append({
                    'file': filepath,
                    'reason': 'needs_manual_validation',
                    'enriched': enriched
                })
                return 'keep'
            else:
                print("  ✗ No validation possible, marking for deletion")
                self.results['to_delete'].append({
                    'file': filepath,
                    'facility': facility,
                    'reason': 'no_validation'
                })
                return 'delete'

        except Exception as e:
            print(f"  ERROR: {e}")
            self.results['errors'].append({
                'file': filepath,
                'error': str(e)
            })
            return 'error'

    def delete_facility(self, filepath: str):
        """Delete a facility file."""
        if self.dry_run:
            print(f"  [DRY RUN] Would delete: {filepath}")
        else:
            os.remove(filepath)
            print(f"  ✓ Deleted: {filepath}")

    def run(self, pattern: str = "usa-[0-9]*.json"):
        """Run validation on all matching facilities."""
        files = sorted(glob.glob(pattern))

        print(f"Found {len(files)} numeric-named USA facilities\n")
        print("=" * 70)

        for filepath in files:
            result = self.validate_facility(filepath)

        # Summary
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Validated (keeping): {len(self.results['validated'])}")
        print(f"To delete: {len(self.results['to_delete'])}")
        print(f"Errors: {len(self.results['errors'])}")

        # Show deletion candidates
        if self.results['to_delete']:
            print("\n" + "=" * 70)
            print("FACILITIES MARKED FOR DELETION:")
            print("=" * 70)
            for item in self.results['to_delete']:
                facility = item['facility']
                print(f"\n{item['file']}")
                print(f"  Name: {facility.get('name')}")
                print(f"  ID: {facility.get('facility_id')}")
                print(f"  Reason: {item['reason']}")

        # Perform deletions if not dry run
        if not self.dry_run and self.results['to_delete']:
            print("\n" + "=" * 70)
            print("DELETING FILES...")
            print("=" * 70)
            for item in self.results['to_delete']:
                self.delete_facility(item['file'])

        return self.results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clean up numeric USA facilities")
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Preview changes without deleting (default)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually delete files')
    parser.add_argument('--pattern', default='usa-[0-9]*.json',
                       help='File pattern to match (default: usa-[0-9]*.json)')

    args = parser.parse_args()

    # Change to USA facilities directory
    facilities_dir = Path(__file__).parent.parent / 'facilities' / 'USA'
    if facilities_dir.exists():
        os.chdir(facilities_dir)
    else:
        print(f"Error: Directory not found: {facilities_dir}")
        sys.exit(1)

    dry_run = not args.execute

    if dry_run:
        print("=" * 70)
        print("DRY RUN MODE - No files will be deleted")
        print("Use --execute to actually delete files")
        print("=" * 70 + "\n")

    validator = NumericFacilityValidator(dry_run=dry_run)
    results = validator.run(args.pattern)

    sys.exit(0 if not results['errors'] else 1)
