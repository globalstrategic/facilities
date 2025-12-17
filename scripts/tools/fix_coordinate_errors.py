#!/usr/bin/env python3
"""
Fix obvious coordinate errors in facilities database.

Fixes these patterns:
1. Wrong hemisphere: Negative latitude for northern hemisphere countries
2. Truncated longitude (lon ≈ 1): Flags for manual research

Usage:
    # Preview fixes (dry-run mode)
    python fix_coordinate_errors.py --dry-run

    # Apply fixes
    python fix_coordinate_errors.py

    # Fix specific facility
    python fix_coordinate_errors.py --facility mmr-zawtika-project-fac

    # Show all detected errors
    python fix_coordinate_errors.py --list
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import shutil

# Paths
ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"

# Northern hemisphere countries (all should have positive latitudes)
# Only includes countries where the ENTIRE landmass is in the northern hemisphere
# Note: Equatorial/cross-equator countries are NOT included here
NORTHERN_HEMISPHERE_COUNTRIES = {
    # Middle East / Gulf
    'SAU',  # Saudi Arabia (16°N - 32°N)
    'ARE',  # UAE (22°N - 26°N)
    'OMN',  # Oman (17°N - 26°N)
    'YEM',  # Yemen (12°N - 19°N)
    'QAT',  # Qatar (24°N - 26°N)
    'KWT',  # Kuwait (29°N - 30°N)
    'BHR',  # Bahrain (26°N)
    'IRQ',  # Iraq (29°N - 37°N)
    'IRN',  # Iran (25°N - 40°N)
    'JOR',  # Jordan (29°N - 33°N)
    'LBN',  # Lebanon (33°N - 34°N)
    'SYR',  # Syria (33°N - 37°N)
    'ISR',  # Israel (30°N - 33°N)
    'PSE',  # Palestine (31°N - 32°N)

    # South/Southeast Asia
    'AFG',  # Afghanistan (29°N - 38°N)
    'PAK',  # Pakistan (24°N - 37°N)
    'NPL',  # Nepal (26°N - 30°N)
    'BTN',  # Bhutan (27°N - 28°N)
    'BGD',  # Bangladesh (21°N - 26°N)
    'MMR',  # Myanmar (10°N - 28°N)
    'THA',  # Thailand (6°N - 21°N)
    'LAO',  # Laos (14°N - 23°N)
    'VNM',  # Vietnam (8°N - 23°N)
    'KHM',  # Cambodia (10°N - 15°N)
    'PHL',  # Philippines (5°N - 21°N)

    # East Asia
    'CHN',  # China (18°N - 53°N)
    'MNG',  # Mongolia (42°N - 52°N)
    'KOR',  # South Korea (33°N - 39°N)
    'PRK',  # North Korea (38°N - 43°N)
    'JPN',  # Japan (24°N - 46°N)
    'TWN',  # Taiwan (22°N - 25°N)

    # North America
    'USA',  # United States (mostly, 25°N+)
    'CAN',  # Canada (42°N - 83°N)

    # Central America & Caribbean
    'MEX',  # Mexico (14°N - 32°N)
    'GTM',  # Guatemala (14°N - 18°N)
    'BLZ',  # Belize (16°N - 18°N)
    'SLV',  # El Salvador (13°N - 14°N)
    'HND',  # Honduras (13°N - 16°N)
    'NIC',  # Nicaragua (11°N - 15°N)
    'CRI',  # Costa Rica (8°N - 11°N)
    'PAN',  # Panama (7°N - 10°N)
    'CUB',  # Cuba (20°N - 23°N)
    'HTI',  # Haiti (18°N - 20°N)
    'DOM',  # Dominican Republic (18°N - 20°N)
    'JAM',  # Jamaica (17°N - 18°N)
    'TTO',  # Trinidad and Tobago (10°N - 11°N)

    # North Africa
    'MAR',  # Morocco (28°N - 36°N)
    'DZA',  # Algeria (19°N - 37°N)
    'TUN',  # Tunisia (30°N - 37°N)
    'LBY',  # Libya (20°N - 33°N)
    'EGY',  # Egypt (22°N - 32°N)

    # Sahel / West Africa (northern)
    'MRT',  # Mauritania (15°N - 27°N)
    'MLI',  # Mali (10°N - 25°N)
    'NER',  # Niger (12°N - 24°N)
    'TCD',  # Chad (8°N - 24°N)
    'SDN',  # Sudan (9°N - 22°N)
    'SSD',  # South Sudan (3°N - 12°N)
    'ERI',  # Eritrea (13°N - 18°N)
    'DJI',  # Djibouti (11°N - 12°N)
    'ETH',  # Ethiopia (3°N - 15°N)

    # West Africa
    'SEN',  # Senegal (12°N - 17°N)
    'GMB',  # Gambia (13°N - 14°N)
    'GNB',  # Guinea-Bissau (11°N - 12°N)
    'GIN',  # Guinea (7°N - 13°N)
    'SLE',  # Sierra Leone (7°N - 10°N)
    'LBR',  # Liberia (4°N - 8°N)
    'CIV',  # Côte d'Ivoire (4°N - 11°N)
    'GHA',  # Ghana (5°N - 11°N)
    'TGO',  # Togo (6°N - 11°N)
    'BEN',  # Benin (6°N - 13°N)
    'BFA',  # Burkina Faso (10°N - 15°N)
    'NGA',  # Nigeria (4°N - 14°N)
    'CMR',  # Cameroon (2°N - 13°N)
    'CAF',  # Central African Republic (2°N - 11°N)

    # Europe
    'ESP',  # Spain (36°N - 44°N)
    'PRT',  # Portugal (37°N - 42°N)
    'FRA',  # France (42°N - 51°N)
    'BEL',  # Belgium (50°N - 51°N)
    'NLD',  # Netherlands (51°N - 53°N)
    'DEU',  # Germany (47°N - 55°N)
    'POL',  # Poland (49°N - 55°N)
    'CZE',  # Czech Republic (49°N - 51°N)
    'SVK',  # Slovakia (48°N - 49°N)
    'AUT',  # Austria (47°N - 49°N)
    'CHE',  # Switzerland (46°N - 48°N)
    'ITA',  # Italy (37°N - 47°N)
    'GRC',  # Greece (35°N - 41°N)
    'TUR',  # Turkey (36°N - 42°N)
    'ROU',  # Romania (44°N - 48°N)
    'BGR',  # Bulgaria (41°N - 44°N)
    'HUN',  # Hungary (46°N - 48°N)
    'HRV',  # Croatia (42°N - 46°N)
    'SVN',  # Slovenia (46°N - 47°N)
    'BIH',  # Bosnia and Herzegovina (43°N - 45°N)
    'SRB',  # Serbia (42°N - 46°N)
    'MNE',  # Montenegro (42°N - 43°N)
    'MKD',  # North Macedonia (41°N - 42°N)
    'ALB',  # Albania (40°N - 42°N)
    'KOS',  # Kosovo (42°N - 43°N)
    'GBR',  # United Kingdom (50°N - 59°N)
    'IRL',  # Ireland (52°N - 55°N)
    'ISL',  # Iceland (63°N - 67°N)
    'NOR',  # Norway (58°N - 71°N)
    'SWE',  # Sweden (55°N - 69°N)
    'FIN',  # Finland (60°N - 70°N)
    'DNK',  # Denmark (55°N - 58°N)
    'EST',  # Estonia (58°N - 60°N)
    'LVA',  # Latvia (56°N - 58°N)
    'LTU',  # Lithuania (54°N - 56°N)
    'BLR',  # Belarus (51°N - 56°N)
    'UKR',  # Ukraine (45°N - 52°N)
    'MDA',  # Moldova (46°N - 48°N)

    # Eurasia
    'RUS',  # Russia (42°N - 82°N)
    'GEO',  # Georgia (41°N - 43°N)
    'ARM',  # Armenia (39°N - 41°N)
    'AZE',  # Azerbaijan (39°N - 42°N)
    'KAZ',  # Kazakhstan (41°N - 55°N)
    'UZB',  # Uzbekistan (37°N - 46°N)
    'TKM',  # Turkmenistan (36°N - 43°N)
    'TJK',  # Tajikistan (37°N - 41°N)
    'KGZ',  # Kyrgyzstan (40°N - 43°N)
}


class CoordinateErrorFixer:
    """Fix obvious coordinate errors in facilities."""

    def __init__(self):
        self.errors_found: List[Dict] = []
        self.fixes_applied: List[Dict] = []

    def detect_errors(self, facility: Dict, file_path: Path) -> List[Dict]:
        """Detect coordinate errors in a facility."""
        errors = []
        facility_id = facility.get('facility_id', 'unknown')
        name = facility.get('name', '')
        country_iso3 = facility.get('country_iso3', '')
        location = facility.get('location', {})
        lat = location.get('lat')
        lon = location.get('lon')

        if lat is None or lon is None:
            return errors

        # Error 1: Wrong hemisphere (negative lat for northern countries)
        if country_iso3 in NORTHERN_HEMISPHERE_COUNTRIES and lat < 0:
            errors.append({
                'type': 'wrong_hemisphere',
                'facility_id': facility_id,
                'name': name,
                'country_iso3': country_iso3,
                'file_path': file_path,
                'current_lat': lat,
                'current_lon': lon,
                'suggested_lat': abs(lat),
                'suggested_lon': lon,
                'description': f'Negative latitude for northern hemisphere country {country_iso3}',
                'auto_fixable': True
            })

        # Error 2: Truncated longitude (lon ≈ 1)
        # Check if lon is EXACTLY 1 or very close (0.95-1.05) for countries where this is unlikely
        # France (FRA), Algeria (DZA), Burkina Faso (BFA), Niger (NER) legitimately have lon ~1°
        # So we only flag exact 1.0 or countries where lon=1 is impossible
        legitimate_lon_1_countries = {'FRA', 'DZA', 'BFA', 'NER', 'BEN', 'TGO', 'GHA'}

        if country_iso3 not in legitimate_lon_1_countries:
            # For other countries, flag lon very close to 1.0 (0.95-1.05) as suspicious
            if 0.95 <= lon <= 1.05:
                errors.append({
                    'type': 'truncated_longitude',
                    'facility_id': facility_id,
                    'name': name,
                    'country_iso3': country_iso3,
                    'file_path': file_path,
                    'current_lat': lat,
                    'current_lon': lon,
                    'suggested_lat': None,
                    'suggested_lon': None,
                    'description': f'Longitude ≈ 1 (likely truncated) for {country_iso3}',
                    'auto_fixable': False,
                    'needs_research': True
                })

        # Error 3: Completely wrong coordinates (e.g., BOL facility in PNG waters)
        # Santa Barbara in BOL has coords (-5.5, 152.5) which is PNG
        if country_iso3 == 'BOL' and lon > 100:
            errors.append({
                'type': 'completely_wrong',
                'facility_id': facility_id,
                'name': name,
                'country_iso3': country_iso3,
                'file_path': file_path,
                'current_lat': lat,
                'current_lon': lon,
                'suggested_lat': None,
                'suggested_lon': None,
                'description': f'BOL facility has longitude {lon} (should be ~-65)',
                'auto_fixable': False,
                'needs_research': True
            })

        return errors

    def scan_all_facilities(self) -> List[Dict]:
        """Scan all facilities for coordinate errors."""
        print("Scanning all facilities for coordinate errors...")

        for country_dir in sorted(FACILITIES_DIR.iterdir()):
            if not country_dir.is_dir():
                continue

            for facility_file in country_dir.glob("*.json"):
                try:
                    with open(facility_file, 'r') as f:
                        facility = json.load(f)

                    errors = self.detect_errors(facility, facility_file)
                    self.errors_found.extend(errors)

                except Exception as e:
                    print(f"Error reading {facility_file}: {e}")

        return self.errors_found

    def scan_specific_facility(self, facility_id: str) -> List[Dict]:
        """Scan a specific facility for errors."""
        # Find the facility file
        for country_dir in FACILITIES_DIR.iterdir():
            if not country_dir.is_dir():
                continue

            facility_file = country_dir / f"{facility_id}.json"
            if facility_file.exists():
                try:
                    with open(facility_file, 'r') as f:
                        facility = json.load(f)

                    errors = self.detect_errors(facility, facility_file)
                    self.errors_found.extend(errors)
                    return errors

                except Exception as e:
                    print(f"Error reading {facility_file}: {e}")
                    return []

        print(f"Facility not found: {facility_id}")
        return []

    def fix_error(self, error: Dict, dry_run: bool = True) -> bool:
        """
        Fix a coordinate error.
        Returns True if fix was applied (or would be applied in dry-run).
        """
        if not error['auto_fixable']:
            return False

        file_path = error['file_path']

        if not file_path.exists():
            print(f"  Error: File not found: {file_path}")
            return False

        try:
            # Read facility
            with open(file_path, 'r') as f:
                facility = json.load(f)

            old_lat = facility['location']['lat']
            old_lon = facility['location']['lon']
            new_lat = error['suggested_lat']
            new_lon = error['suggested_lon']

            print(f"  {error['facility_id']}: ({old_lat}, {old_lon}) → ({new_lat}, {new_lon})")
            print(f"    Reason: {error['description']}")

            if not dry_run:
                # Create backup
                backup_path = file_path.with_suffix('.json.bak')
                shutil.copy2(file_path, backup_path)
                print(f"    Backup created: {backup_path.name}")

                # Apply fix
                facility['location']['lat'] = new_lat
                facility['location']['lon'] = new_lon

                # Add note about fix
                timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                if 'notes' not in facility:
                    facility['notes'] = ''

                fix_note = f"\n[AUTO-FIX {timestamp}] Fixed {error['type']}: Changed lat from {old_lat} to {new_lat}"
                if facility['notes']:
                    facility['notes'] += fix_note
                else:
                    facility['notes'] = fix_note.strip()

                # Write back
                with open(file_path, 'w') as f:
                    json.dump(facility, f, indent=2, ensure_ascii=False)
                    f.write('\n')

                self.fixes_applied.append(error)

            return True

        except Exception as e:
            print(f"  Error fixing {error['facility_id']}: {e}")
            return False

    def apply_fixes(self, dry_run: bool = True) -> int:
        """Apply automatic fixes to auto-fixable errors."""
        auto_fixable = [e for e in self.errors_found if e['auto_fixable']]
        manual_review = [e for e in self.errors_found if not e['auto_fixable']]

        if auto_fixable:
            print(f"\n{'[DRY RUN] ' if dry_run else ''}Applying fixes to {len(auto_fixable)} facilities...")
            print("=" * 80)

            fixed_count = 0
            for error in auto_fixable:
                if self.fix_error(error, dry_run=dry_run):
                    fixed_count += 1

            print(f"\n{'Would fix' if dry_run else 'Fixed'} {fixed_count}/{len(auto_fixable)} facilities")

        if manual_review:
            print(f"\n{len(manual_review)} errors require manual review:")
            print("=" * 80)
            for error in manual_review:
                print(f"\n{error['facility_id']} ({error['country_iso3']})")
                print(f"  Name: {error['name']}")
                print(f"  Type: {error['type']}")
                print(f"  Current: ({error['current_lat']}, {error['current_lon']})")
                print(f"  Issue: {error['description']}")
                print(f"  File: {error['file_path'].relative_to(ROOT)}")

        return len(auto_fixable)

    def print_summary(self):
        """Print summary of detected errors."""
        print("\n" + "=" * 80)
        print("COORDINATE ERROR DETECTION SUMMARY")
        print("=" * 80)

        total_errors = len(self.errors_found)
        auto_fixable = len([e for e in self.errors_found if e['auto_fixable']])
        manual_review = len([e for e in self.errors_found if not e['auto_fixable']])

        print(f"\nTotal errors found: {total_errors}")
        print(f"  • Auto-fixable: {auto_fixable}")
        print(f"  • Require manual review: {manual_review}")

        if total_errors > 0:
            print("\nErrors by type:")
            error_types = {}
            for error in self.errors_found:
                error_type = error['type']
                error_types[error_type] = error_types.get(error_type, 0) + 1

            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                print(f"  • {error_type:30s} {count:5d}")

        print("\n" + "=" * 80)

    def list_all_errors(self):
        """List all detected errors with details."""
        if not self.errors_found:
            print("No errors found.")
            return

        print("\n" + "=" * 80)
        print("ALL DETECTED ERRORS")
        print("=" * 80)

        for i, error in enumerate(self.errors_found, 1):
            print(f"\n{i}. {error['facility_id']} ({error['country_iso3']}) - {error['type'].upper()}")
            print(f"   Name: {error['name']}")
            print(f"   Current: ({error['current_lat']}, {error['current_lon']})")
            if error['auto_fixable']:
                print(f"   Suggested: ({error['suggested_lat']}, {error['suggested_lon']}) [AUTO-FIXABLE]")
            else:
                print(f"   Status: Requires manual research")
            print(f"   Issue: {error['description']}")
            print(f"   File: {error['file_path'].relative_to(ROOT)}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix obvious coordinate errors in facilities database",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview fixes without applying them (default mode)'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually apply the fixes (default is dry-run)'
    )
    parser.add_argument(
        '--facility',
        help='Fix specific facility by ID (e.g., mmr-zawtika-project-fac)'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all detected errors with details'
    )

    args = parser.parse_args()

    # Create fixer
    fixer = CoordinateErrorFixer()

    # Scan for errors
    if args.facility:
        print(f"Scanning facility: {args.facility}")
        fixer.scan_specific_facility(args.facility)
    else:
        fixer.scan_all_facilities()

    # Print summary
    fixer.print_summary()

    # List all errors if requested
    if args.list:
        fixer.list_all_errors()
        return 0

    # Apply fixes
    if fixer.errors_found:
        # Dry-run unless --execute is passed
        dry_run = not args.execute or args.dry_run
        fixer.apply_fixes(dry_run=dry_run)

        if dry_run:
            print("\nThis was a dry-run. No files were modified.")
            print("To apply fixes, run with --execute flag.")
    else:
        print("\nNo errors found to fix.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
