#!/usr/bin/env python3
"""
Unified coordinate fixing tool for facilities database.

Consolidates functionality from:
- fix_coordinate_errors.py (hemisphere detection, auto-fix)
- fix_coordinate_issues.py (known fixes, boundary validation)

Issue types detected:
1. Wrong hemisphere (negative lat for northern countries)
2. Truncated longitude (lon near 1)
3. Wrong sign (negative lon for eastern countries)
4. Swapped lat/lon
5. Out of bounds (coords don't match country)
6. Null island (0, 0)
7. Completely wrong (e.g., BOL facility with PNG coords)

Usage:
    # Scan and report all issues
    python fix_coordinates.py --scan

    # Apply automatic fixes (hemisphere)
    python fix_coordinates.py --auto-fix

    # Apply known manual fixes
    python fix_coordinates.py --apply-known

    # Apply all fixes
    python fix_coordinates.py --execute

    # Check specific facility
    python fix_coordinates.py --facility mmr-zawtika-project-fac
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, field
import shutil

ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"

# =============================================================================
# NORTHERN HEMISPHERE COUNTRIES (all should have positive latitudes)
# =============================================================================
NORTHERN_HEMISPHERE_COUNTRIES = {
    # Middle East / Gulf
    'SAU', 'ARE', 'OMN', 'YEM', 'QAT', 'KWT', 'BHR', 'IRQ', 'IRN',
    'JOR', 'LBN', 'SYR', 'ISR', 'PSE',

    # South/Southeast Asia
    'AFG', 'PAK', 'NPL', 'BTN', 'BGD', 'MMR', 'THA', 'LAO', 'VNM',
    'KHM', 'PHL',

    # East Asia
    'CHN', 'MNG', 'KOR', 'PRK', 'JPN', 'TWN',

    # North America
    'USA', 'CAN',

    # Central America & Caribbean
    'MEX', 'GTM', 'BLZ', 'SLV', 'HND', 'NIC', 'CRI', 'PAN',
    'CUB', 'HTI', 'DOM', 'JAM', 'TTO',

    # North Africa
    'MAR', 'DZA', 'TUN', 'LBY', 'EGY',

    # Sahel / West Africa (northern)
    'MRT', 'MLI', 'NER', 'TCD', 'SDN', 'SSD', 'ERI', 'DJI', 'ETH',

    # West Africa
    'SEN', 'GMB', 'GNB', 'GIN', 'SLE', 'LBR', 'CIV', 'GHA', 'TGO',
    'BEN', 'BFA', 'NGA', 'CMR', 'CAF',

    # Europe
    'ESP', 'PRT', 'FRA', 'BEL', 'NLD', 'DEU', 'POL', 'CZE', 'SVK',
    'AUT', 'CHE', 'ITA', 'GRC', 'TUR', 'ROU', 'BGR', 'HUN', 'HRV',
    'SVN', 'BIH', 'SRB', 'MNE', 'MKD', 'ALB', 'KOS', 'GBR', 'IRL',
    'ISL', 'NOR', 'SWE', 'FIN', 'DNK', 'EST', 'LVA', 'LTU', 'BLR',
    'UKR', 'MDA',

    # Eurasia
    'RUS', 'GEO', 'ARM', 'AZE', 'KAZ', 'UZB', 'TKM', 'TJK', 'KGZ',
}

# Countries where lon=1 is legitimate
LEGITIMATE_LON_1_COUNTRIES = {'FRA', 'DZA', 'BFA', 'NER', 'BEN', 'TGO', 'GHA', 'GBR', 'AND', 'ESP'}

# =============================================================================
# KNOWN FIXES (manually researched coordinates)
# =============================================================================
KNOWN_FIXES = {
    # Truncated longitude fixes
    'aus-mount-newman-fac': {'lat': -23.361111, 'lon': 119.7325, 'source': 'Mount Newman mine, Pilbara WA'},
    'aus-yandicoogina-fac': {'lat': -22.776192, 'lon': 119.2, 'source': 'Yandicoogina mine, Pilbara WA'},
    'idn-earthstone-nalo-baru-mine-fac': {'lat': -2.01394, 'lon': 101.5, 'source': 'Sumatra, Indonesia'},
    'idn-abe-coal-mine-fac': {'lat': 1.02, 'lon': 117.0, 'source': 'East Kalimantan, Indonesia'},
    'bra-bemisa-baratinha-mine-fac': {'lat': -19.564705, 'lon': -44.0, 'source': 'Minas Gerais, Brazil'},
    'zaf-elders-coal-project-fac': {'lat': -26.239016, 'lon': 29.0, 'source': 'Mpumalanga Province, South Africa'},

    # Wrong sign fixes
    'sau-jabal-sayid-fac': {'lat': 24.276714683519764, 'lon': 45.51155643412581, 'source': 'Jabal Sayid copper mine, Saudi Arabia'},
    'col-san-juaquin-coal-mine-fac': {'lat': 6.0282939, 'lon': -75.6922416, 'source': 'Colombia'},

    # Wrong hemisphere fixes
    'bol-bolivar-mine-fac': {'lat': -19.5, 'lon': -66.1, 'source': 'Antequera, Potosi Department, Bolivia'},

    # Wrong location fixes
    'ncl-snowy-river-project-fac': {'lat': -22.3, 'lon': 166.5, 'source': 'Moved from NZ coords to New Caledonia'},
    'aus-esk-river-fac': {'lat': -41.5, 'lon': 146.5, 'source': 'Tasmania, Australia'},
    'gin-lola-graphite-project-fac': {'lat': 7.8, 'lon': -8.5, 'source': 'Lola, Forest Region, Guinea'},
    'kor-sangdong-mine-fac': {'lat': 37.15, 'lon': 128.7, 'source': 'Sangdong mine, Gangwon Province, South Korea'},
    'ncl-sln-ti-baghi-mine-fac': {'lat': -20.44472, 'lon': 164.21722, 'source': 'Tiebaghi mine, northern New Caledonia'},
    'idn-gosowong-fac': {'lat': 1.0, 'lon': 127.9, 'source': 'Gosowong mine, Halmahera, North Maluku, Indonesia'},

    # Completely wrong locations
    'bol-laramcota-mine-fac': {'lat': -18.5, 'lon': -68.0, 'source': 'La Paz Department, Bolivia'},
    'bol-santa-barbara-fac': {'lat': -17.8, 'lon': -63.2, 'source': 'Santa Cruz Department, Bolivia'},
    'usa-berwind-coal-mine-fac': {'lat': 37.28, 'lon': -81.65, 'source': 'Berwind, McDowell County, WV'},
    'usa-tonkin-springs-gold-mine-fac': {'lat': 39.77, 'lon': -116.23, 'source': 'Tonkin Springs, Eureka County, Nevada'},
    'ven-los-pijiguaos-mine-fac': {'lat': 6.58, 'lon': -66.75, 'source': 'Los Pijiguaos bauxite mine, Bolivar State, Venezuela'},
    'zaf-namakwa-sands-fac': {'lat': -31.2, 'lon': 17.9, 'source': 'Namakwa Sands, West Coast, South Africa'},
    'zaf-namakwa-sands-smelter-fac': {'lat': -32.9, 'lon': 18.0, 'source': 'Saldanha Smelter, Western Cape, South Africa'},
    'chl-salar-del-carmen-fac': {'lat': -23.62, 'lon': -70.35, 'source': 'Salar del Carmen, Antofagasta Region, Chile'},
    'gab-et-k-gold-project-fac': {'lat': -1.15, 'lon': 10.85, 'source': 'Eteke Gold Project, Ogooue-Lolo Province, Gabon'},
    'idn-agm-coal-mine-fac': {'lat': 2.5, 'lon': 99.5, 'source': 'North Sumatra, Indonesia'},
    'mdg-manampotsy-pgm-occurrence-fac': {'lat': -22.3, 'lon': 47.5, 'source': 'Manampotsy area, Madagascar'},
    'prk-nampo-smelting-complex-fac': {'lat': 38.73, 'lon': 125.38, 'source': 'Nampo city, South Pyongan Province, North Korea'},
    'sdn-chikay-mine-fac': {'lat': 20.5, 'lon': 36.5, 'source': 'Red Sea State, Sudan'},

    # Duplicates to delete
    'tkm-jv-inkai-fac': {'delete': True, 'reason': 'JV Inkai is in Kazakhstan, duplicate of kaz-jv-inkai-fac'},
}

# =============================================================================
# COUNTRY BOUNDING BOXES (approximate)
# =============================================================================
COUNTRY_BOUNDS = {
    'AUS': ((-44, -10), (112, 154)),
    'BRA': ((-34, 5), (-74, -34)),
    'IDN': ((-11, 6), (95, 141)),
    'SAU': ((16, 32), (34, 56)),
    'GIN': ((7, 13), (-15, -7)),
    'NCL': ((-23, -19), (163, 169)),
    'KOR': ((33, 39), (124, 132)),
    'TKM': ((35, 43), (52, 66)),
    'ZAF': ((-35, -22), (16, 33)),
    'CHL': ((-56, -17), (-76, -66)),
    'BOL': ((-23, -9), (-69, -57)),
    'USA': ((24, 72), (-180, -66)),
    'CAN': ((41, 84), (-141, -52)),
    'RUS': ((41, 82), (19, 180)),
}


@dataclass
class CoordinateIssue:
    """A detected coordinate issue."""
    issue_type: str
    facility_id: str
    name: str
    country_iso3: str
    file_path: Path
    current_lat: float
    current_lon: float
    suggested_lat: Optional[float] = None
    suggested_lon: Optional[float] = None
    description: str = ""
    auto_fixable: bool = False
    source: str = ""


class CoordinateFixer:
    """Unified coordinate issue detector and fixer."""

    def __init__(self):
        self.issues: List[CoordinateIssue] = []
        self.fixes_applied: List[str] = []
        self.files_deleted: List[str] = []

    def detect_issues(self, facility: Dict, file_path: Path) -> List[CoordinateIssue]:
        """Detect all coordinate issues in a facility."""
        issues = []
        facility_id = facility.get('facility_id', '')
        name = facility.get('name', '')
        country = facility.get('country_iso3', '')
        location = facility.get('location', {})
        lat = location.get('lat')
        lon = location.get('lon')

        if lat is None or lon is None:
            return issues

        # Check known fixes first
        if facility_id in KNOWN_FIXES:
            fix = KNOWN_FIXES[facility_id]
            if fix.get('delete'):
                issues.append(CoordinateIssue(
                    issue_type='duplicate_to_delete',
                    facility_id=facility_id,
                    name=name,
                    country_iso3=country,
                    file_path=file_path,
                    current_lat=lat,
                    current_lon=lon,
                    description=fix.get('reason', 'Duplicate'),
                    auto_fixable=True,
                    source='KNOWN_FIXES'
                ))
            else:
                issues.append(CoordinateIssue(
                    issue_type='known_fix',
                    facility_id=facility_id,
                    name=name,
                    country_iso3=country,
                    file_path=file_path,
                    current_lat=lat,
                    current_lon=lon,
                    suggested_lat=fix['lat'],
                    suggested_lon=fix['lon'],
                    description=fix.get('source', 'Manual fix'),
                    auto_fixable=True,
                    source='KNOWN_FIXES'
                ))
            return issues

        # Null island
        if lat == 0 and lon == 0:
            issues.append(CoordinateIssue(
                issue_type='null_island',
                facility_id=facility_id,
                name=name,
                country_iso3=country,
                file_path=file_path,
                current_lat=lat,
                current_lon=lon,
                description='Null island (0,0) - needs research',
                auto_fixable=False
            ))
            return issues

        # Wrong hemisphere (northern country with negative lat)
        if country in NORTHERN_HEMISPHERE_COUNTRIES and lat < 0:
            issues.append(CoordinateIssue(
                issue_type='wrong_hemisphere',
                facility_id=facility_id,
                name=name,
                country_iso3=country,
                file_path=file_path,
                current_lat=lat,
                current_lon=lon,
                suggested_lat=abs(lat),
                suggested_lon=lon,
                description=f'Negative latitude for northern hemisphere country {country}',
                auto_fixable=True
            ))

        # Truncated longitude (lon near 1)
        if country not in LEGITIMATE_LON_1_COUNTRIES and 0.5 <= lon <= 1.5:
            issues.append(CoordinateIssue(
                issue_type='truncated_longitude',
                facility_id=facility_id,
                name=name,
                country_iso3=country,
                file_path=file_path,
                current_lat=lat,
                current_lon=lon,
                description=f'Longitude near 1 (likely truncated) for {country}',
                auto_fixable=False
            ))

        # Completely wrong (e.g., BOL with Pacific coords)
        if country == 'BOL' and lon > 100:
            issues.append(CoordinateIssue(
                issue_type='completely_wrong',
                facility_id=facility_id,
                name=name,
                country_iso3=country,
                file_path=file_path,
                current_lat=lat,
                current_lon=lon,
                description=f'BOL facility has longitude {lon} (should be ~-65)',
                auto_fixable=False
            ))

        # Bounds checking
        if country in COUNTRY_BOUNDS:
            (lat_min, lat_max), (lon_min, lon_max) = COUNTRY_BOUNDS[country]
            in_bounds = lat_min <= lat <= lat_max and lon_min <= lon <= lon_max

            if not in_bounds:
                # Check if it's a sign error
                if lat_min <= lat <= lat_max and lon_min <= abs(lon) <= lon_max:
                    issues.append(CoordinateIssue(
                        issue_type='wrong_sign',
                        facility_id=facility_id,
                        name=name,
                        country_iso3=country,
                        file_path=file_path,
                        current_lat=lat,
                        current_lon=lon,
                        suggested_lat=lat,
                        suggested_lon=abs(lon),
                        description=f'Longitude sign likely wrong for {country}',
                        auto_fixable=False  # Needs verification
                    ))
                # Check if swapped
                elif lat_min <= lon <= lat_max and lon_min <= lat <= lon_max:
                    issues.append(CoordinateIssue(
                        issue_type='swapped',
                        facility_id=facility_id,
                        name=name,
                        country_iso3=country,
                        file_path=file_path,
                        current_lat=lat,
                        current_lon=lon,
                        suggested_lat=lon,
                        suggested_lon=lat,
                        description=f'Lat/lon appear swapped for {country}',
                        auto_fixable=False  # Needs verification
                    ))
                else:
                    issues.append(CoordinateIssue(
                        issue_type='out_of_bounds',
                        facility_id=facility_id,
                        name=name,
                        country_iso3=country,
                        file_path=file_path,
                        current_lat=lat,
                        current_lon=lon,
                        description=f'Coordinates outside expected bounds for {country}',
                        auto_fixable=False
                    ))

        return issues

    def scan_all(self) -> List[CoordinateIssue]:
        """Scan all facilities for coordinate issues."""
        print("Scanning all facilities for coordinate issues...")

        for country_dir in sorted(FACILITIES_DIR.iterdir()):
            if not country_dir.is_dir():
                continue

            for facility_file in country_dir.glob("*.json"):
                try:
                    with open(facility_file, 'r') as f:
                        facility = json.load(f)
                    issues = self.detect_issues(facility, facility_file)
                    self.issues.extend(issues)
                except Exception as e:
                    print(f"Error reading {facility_file}: {e}")

        return self.issues

    def scan_facility(self, facility_id: str) -> List[CoordinateIssue]:
        """Scan a specific facility."""
        for country_dir in FACILITIES_DIR.iterdir():
            if not country_dir.is_dir():
                continue

            facility_file = country_dir / f"{facility_id}.json"
            if facility_file.exists():
                with open(facility_file, 'r') as f:
                    facility = json.load(f)
                issues = self.detect_issues(facility, facility_file)
                self.issues.extend(issues)
                return issues

        print(f"Facility not found: {facility_id}")
        return []

    def apply_fix(self, issue: CoordinateIssue, dry_run: bool = True) -> bool:
        """Apply a single fix."""
        if not issue.auto_fixable:
            return False

        file_path = issue.file_path

        if issue.issue_type == 'duplicate_to_delete':
            print(f"  [DELETE] {issue.facility_id}: {issue.description}")
            if not dry_run:
                file_path.unlink()
                self.files_deleted.append(issue.facility_id)
            return True

        if not file_path.exists():
            print(f"  [ERROR] File not found: {file_path}")
            return False

        try:
            with open(file_path, 'r') as f:
                facility = json.load(f)

            old_lat = facility['location']['lat']
            old_lon = facility['location']['lon']
            new_lat = issue.suggested_lat
            new_lon = issue.suggested_lon

            print(f"  [FIX] {issue.facility_id}: ({old_lat}, {old_lon}) -> ({new_lat}, {new_lon})")
            print(f"        Type: {issue.issue_type}")
            print(f"        Reason: {issue.description}")

            if not dry_run:
                # Backup
                backup_path = file_path.with_suffix('.json.bak')
                shutil.copy2(file_path, backup_path)

                # Apply fix
                facility['location']['lat'] = new_lat
                facility['location']['lon'] = new_lon

                # Add note
                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                note = f"[COORD-FIX {timestamp}] {issue.issue_type}: ({old_lat}, {old_lon}) -> ({new_lat}, {new_lon})"

                if 'verification' not in facility:
                    facility['verification'] = {}
                if 'notes' in facility['verification'] and facility['verification']['notes']:
                    facility['verification']['notes'] += f" | {note}"
                else:
                    facility['verification']['notes'] = note

                with open(file_path, 'w') as f:
                    json.dump(facility, f, indent=2, ensure_ascii=False)
                    f.write('\n')

                self.fixes_applied.append(issue.facility_id)

            return True

        except Exception as e:
            print(f"  [ERROR] {issue.facility_id}: {e}")
            return False

    def apply_auto_fixes(self, dry_run: bool = True) -> int:
        """Apply all auto-fixable issues."""
        auto_fixable = [i for i in self.issues if i.auto_fixable]

        if not auto_fixable:
            print("No auto-fixable issues found.")
            return 0

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Applying {len(auto_fixable)} automatic fixes...")
        print("=" * 80)

        fixed = 0
        for issue in auto_fixable:
            if self.apply_fix(issue, dry_run=dry_run):
                fixed += 1

        print(f"\n{'Would fix' if dry_run else 'Fixed'}: {fixed}/{len(auto_fixable)}")
        return fixed

    def print_summary(self):
        """Print summary of detected issues."""
        print("\n" + "=" * 80)
        print("COORDINATE ISSUES SUMMARY")
        print("=" * 80)

        total = len(self.issues)
        auto_fixable = len([i for i in self.issues if i.auto_fixable])
        manual = total - auto_fixable

        print(f"\nTotal issues: {total}")
        print(f"  Auto-fixable: {auto_fixable}")
        print(f"  Require research: {manual}")

        if total > 0:
            print("\nBy type:")
            types = {}
            for issue in self.issues:
                types[issue.issue_type] = types.get(issue.issue_type, 0) + 1
            for t, count in sorted(types.items(), key=lambda x: -x[1]):
                fixable = len([i for i in self.issues if i.issue_type == t and i.auto_fixable])
                print(f"  {t:25s} {count:4d}  (auto-fixable: {fixable})")

    def print_detailed(self):
        """Print detailed issue list."""
        if not self.issues:
            print("No issues found.")
            return

        print("\n" + "=" * 80)
        print("DETAILED ISSUES")
        print("=" * 80)

        for i, issue in enumerate(self.issues, 1):
            status = "[AUTO]" if issue.auto_fixable else "[MANUAL]"
            print(f"\n{i}. {status} {issue.facility_id} ({issue.country_iso3})")
            print(f"   Name: {issue.name}")
            print(f"   Type: {issue.issue_type}")
            print(f"   Current: ({issue.current_lat}, {issue.current_lon})")
            if issue.suggested_lat is not None:
                print(f"   Suggested: ({issue.suggested_lat}, {issue.suggested_lon})")
            print(f"   {issue.description}")


def main():
    parser = argparse.ArgumentParser(
        description="Unified coordinate fixing tool",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--scan', action='store_true', help='Scan and report all issues')
    parser.add_argument('--auto-fix', action='store_true', help='Apply automatic fixes only')
    parser.add_argument('--apply-known', action='store_true', help='Apply known manual fixes')
    parser.add_argument('--execute', action='store_true', help='Apply all fixes (requires --confirm)')
    parser.add_argument('--confirm', action='store_true', help='Confirm execution (not dry-run)')
    parser.add_argument('--facility', help='Check specific facility')
    parser.add_argument('--list', action='store_true', help='List detailed issues')

    args = parser.parse_args()

    fixer = CoordinateFixer()

    # Scan
    if args.facility:
        print(f"Scanning facility: {args.facility}")
        fixer.scan_facility(args.facility)
    else:
        fixer.scan_all()

    # Print summary
    fixer.print_summary()

    if args.list:
        fixer.print_detailed()

    # Apply fixes
    if args.auto_fix or args.execute:
        dry_run = not args.confirm
        fixer.apply_auto_fixes(dry_run=dry_run)

        if dry_run:
            print("\nThis was a dry run. Use --confirm to apply fixes.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
