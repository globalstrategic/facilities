#!/usr/bin/env python3
"""
Audit facilities database to find incomplete or problematic entries.

Identifies facilities that need more information:
- Numeric-only names
- Missing coordinates
- No commodities
- No operator/owner information
- Low confidence scores
- Missing critical fields

Usage:
    python audit_facilities.py                    # Audit all countries
    python audit_facilities.py --country AFG      # Audit specific country
    python audit_facilities.py --issue no-coords  # Find specific issue
    python audit_facilities.py --output report.json
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict
import re

# Paths
ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"


class FacilityAuditor:
    """Audit facilities for completeness and quality issues."""

    def __init__(self):
        self.issues = defaultdict(list)
        self.stats = defaultdict(int)

    def is_numeric_name(self, name: str) -> bool:
        """Check if facility name is just numbers."""
        return bool(re.match(r'^\d+$', name.strip()))

    def is_generic_name(self, name: str) -> bool:
        """Check if facility name is generic/placeholder."""
        generic_patterns = [
            r'^mine \d+$',
            r'^facility \d+$',
            r'^project \d+$',
            r'^unknown',
            r'^unnamed',
            r'^tbd$',
        ]
        name_lower = name.lower().strip()
        return any(re.match(pattern, name_lower) for pattern in generic_patterns)

    def audit_facility(self, facility: Dict, file_path: Path) -> List[str]:
        """Audit a single facility and return list of issues."""
        issues = []
        fac_id = facility.get('facility_id', 'unknown')
        name = facility.get('name', '')

        # Check for numeric-only names
        if self.is_numeric_name(name):
            issues.append('numeric_name')

        # Check for generic names
        if self.is_generic_name(name):
            issues.append('generic_name')

        # Check for missing or invalid coordinates
        location = facility.get('location', {})
        if not location.get('lat') or not location.get('lon'):
            issues.append('no_coordinates')
        elif location.get('precision') == 'unknown':
            issues.append('imprecise_location')

        # Check for missing commodities
        commodities = facility.get('commodities', [])
        if not commodities:
            issues.append('no_commodities')

        # Check for no primary commodity
        if commodities and not any(c.get('primary') for c in commodities):
            issues.append('no_primary_commodity')

        # Check for missing operator/owner
        operator = facility.get('operator_link')
        owners = facility.get('owner_links', [])
        if not operator and not owners:
            issues.append('no_operator_owner')

        # Check status
        status = facility.get('status', 'unknown')
        if status == 'unknown':
            issues.append('unknown_status')

        # Check verification confidence
        verification = facility.get('verification', {})
        confidence = verification.get('confidence', 0)
        if confidence < 0.5:
            issues.append('low_confidence')

        # Check for missing sources
        sources = facility.get('sources', [])
        if not sources:
            issues.append('no_sources')

        # Check facility types
        types = facility.get('types', [])
        if not types or types == ['mine']:  # Default type
            issues.append('generic_type')

        return issues

    def audit_country(self, country_code: str) -> Dict:
        """Audit all facilities in a country."""
        country_dir = FACILITIES_DIR / country_code
        if not country_dir.exists():
            print(f"Country directory not found: {country_code}")
            return {}

        country_issues = defaultdict(list)
        facility_count = 0

        for facility_file in country_dir.glob("*.json"):
            try:
                with open(facility_file, 'r') as f:
                    facility = json.load(f)

                facility_count += 1
                issues = self.audit_facility(facility, facility_file)

                if issues:
                    facility_info = {
                        'facility_id': facility.get('facility_id'),
                        'name': facility.get('name'),
                        'file': str(facility_file.relative_to(ROOT)),
                        'issues': issues
                    }

                    for issue in issues:
                        country_issues[issue].append(facility_info)
                        self.issues[issue].append(facility_info)

            except Exception as e:
                print(f"Error reading {facility_file}: {e}")

        self.stats[country_code] = facility_count
        return country_issues

    def audit_all(self, countries: Optional[List[str]] = None) -> Dict:
        """Audit all facilities or specific countries."""
        if countries:
            country_dirs = [FACILITIES_DIR / c for c in countries if (FACILITIES_DIR / c).exists()]
        else:
            country_dirs = [d for d in FACILITIES_DIR.iterdir() if d.is_dir()]

        print(f"Auditing {len(country_dirs)} countries...")

        for country_dir in sorted(country_dirs):
            self.audit_country(country_dir.name)

        return dict(self.issues)

    def print_summary(self):
        """Print audit summary."""
        print("\n" + "="*70)
        print("FACILITY AUDIT SUMMARY")
        print("="*70)

        total_facilities = sum(self.stats.values())
        if total_facilities == 0:
           print(f"\nNo facilities found to audit.")
           return 
        facilities_with_issues = len(set(
            fac['facility_id'] for issue_list in self.issues.values()
            for fac in issue_list
        ))

        print(f"\nTotal facilities audited: {total_facilities}")
        print(f"Facilities with issues: {facilities_with_issues} ({facilities_with_issues/total_facilities*100:.1f}%)")
        print(f"\nIssues found:")

        issue_descriptions = {
            'numeric_name': 'Numeric-only names',
            'generic_name': 'Generic/placeholder names',
            'no_coordinates': 'Missing coordinates',
            'imprecise_location': 'Imprecise location',
            'no_commodities': 'No commodities listed',
            'no_primary_commodity': 'No primary commodity',
            'no_operator_owner': 'No operator or owner',
            'unknown_status': 'Unknown operational status',
            'low_confidence': 'Low confidence score (<0.5)',
            'no_sources': 'No data sources',
            'generic_type': 'Generic facility type'
        }

        for issue_type, facilities in sorted(self.issues.items(), key=lambda x: len(x[1]), reverse=True):
            description = issue_descriptions.get(issue_type, issue_type)
            count = len(facilities)
            print(f"  • {description:35s} {count:5d} facilities")

        print("\n" + "="*70)

    def print_detailed_report(self, issue_type: Optional[str] = None, limit: int = 10):
        """Print detailed report for specific issue or all issues."""
        if issue_type:
            if issue_type not in self.issues:
                print(f"No facilities found with issue: {issue_type}")
                return

            facilities = self.issues[issue_type]
            print(f"\n{len(facilities)} facilities with issue: {issue_type}")
            print("-" * 70)

            for fac in facilities[:limit]:
                print(f"\n{fac['facility_id']}")
                print(f"  Name: {fac['name']}")
                print(f"  File: {fac['file']}")
                print(f"  Issues: {', '.join(fac['issues'])}")

            if len(facilities) > limit:
                print(f"\n... and {len(facilities) - limit} more")
        else:
            # Print top issues
            print("\nTop Issues (showing up to 5 examples each):")
            print("="*70)

            for issue_type, facilities in sorted(self.issues.items(), key=lambda x: len(x[1]), reverse=True):
                print(f"\n{issue_type.upper()} ({len(facilities)} facilities):")
                print("-" * 70)

                for fac in facilities[:5]:
                    print(f"  • {fac['facility_id']:40s} {fac['name']}")

                if len(facilities) > 5:
                    print(f"  ... and {len(facilities) - 5} more")


def main():
    parser = argparse.ArgumentParser(
        description="Audit facilities database for incomplete or problematic entries",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--country',
        help='Audit specific country (e.g., AFG, USA)'
    )
    parser.add_argument(
        '--issue',
        choices=['numeric_name', 'generic_name', 'no_coordinates', 'imprecise_location',
                'no_commodities', 'no_primary_commodity', 'no_operator_owner',
                'unknown_status', 'low_confidence', 'no_sources', 'generic_type'],
        help='Show detailed report for specific issue type'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Limit number of examples shown (default: 10)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Write detailed report to JSON file'
    )

    args = parser.parse_args()

    # Run audit
    auditor = FacilityAuditor()

    if args.country:
        auditor.audit_country(args.country)
    else:
        auditor.audit_all()

    # Print results
    auditor.print_summary()

    if args.issue:
        auditor.print_detailed_report(issue_type=args.issue, limit=args.limit)
    else:
        auditor.print_detailed_report(limit=5)

    # Write to file if requested
    if args.output:
        output_data = {
            'total_facilities': sum(auditor.stats.values()),
            'countries_audited': len(auditor.stats),
            'issues': {
                issue_type: [
                    {
                        'facility_id': fac['facility_id'],
                        'name': fac['name'],
                        'file': fac['file'],
                        'issues': fac['issues']
                    }
                    for fac in facilities
                ]
                for issue_type, facilities in auditor.issues.items()
            }
        }

        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)

        print(f"\nDetailed report written to: {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
