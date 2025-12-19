#!/usr/bin/env python3
"""
Unified facility fixing tool.

Consolidates:
- fix_coordinates.py (hemisphere, truncated longitude, known fixes)
- fix_wrong_country.py (move facilities to correct country folders)

Subcommands:
    coordinates   Fix coordinate issues (hemisphere, swapped, known fixes)
    country       Move facilities to correct country folders

Usage:
    # Scan for coordinate issues
    python fix.py coordinates --scan
    python fix.py coordinates --auto-fix --confirm

    # Move facilities to correct country
    python fix.py country --dry-run
    python fix.py country --execute
"""

import json
import argparse
import re
import subprocess
import sys
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass

ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"


# =============================================================================
# COORDINATE DATA
# =============================================================================

NORTHERN_HEMISPHERE_COUNTRIES = {
    'SAU', 'ARE', 'OMN', 'YEM', 'QAT', 'KWT', 'BHR', 'IRQ', 'IRN',
    'JOR', 'LBN', 'SYR', 'ISR', 'PSE', 'AFG', 'PAK', 'NPL', 'BTN',
    'BGD', 'MMR', 'THA', 'LAO', 'VNM', 'KHM', 'PHL', 'CHN', 'MNG',
    'KOR', 'PRK', 'JPN', 'TWN', 'USA', 'CAN', 'MEX', 'GTM', 'BLZ',
    'SLV', 'HND', 'NIC', 'CRI', 'PAN', 'CUB', 'HTI', 'DOM', 'JAM',
    'TTO', 'MAR', 'DZA', 'TUN', 'LBY', 'EGY', 'MRT', 'MLI', 'NER',
    'TCD', 'SDN', 'SSD', 'ERI', 'DJI', 'ETH', 'SEN', 'GMB', 'GNB',
    'GIN', 'SLE', 'LBR', 'CIV', 'GHA', 'TGO', 'BEN', 'BFA', 'NGA',
    'CMR', 'CAF', 'ESP', 'PRT', 'FRA', 'BEL', 'NLD', 'DEU', 'POL',
    'CZE', 'SVK', 'AUT', 'CHE', 'ITA', 'GRC', 'TUR', 'ROU', 'BGR',
    'HUN', 'HRV', 'SVN', 'BIH', 'SRB', 'MNE', 'MKD', 'ALB', 'KOS',
    'GBR', 'IRL', 'ISL', 'NOR', 'SWE', 'FIN', 'DNK', 'EST', 'LVA',
    'LTU', 'BLR', 'UKR', 'MDA', 'RUS', 'GEO', 'ARM', 'AZE', 'KAZ',
    'UZB', 'TKM', 'TJK', 'KGZ',
}

LEGITIMATE_LON_1_COUNTRIES = {'FRA', 'DZA', 'BFA', 'NER', 'BEN', 'TGO', 'GHA', 'GBR', 'AND', 'ESP'}

KNOWN_FIXES = {
    'aus-mount-newman-fac': {'lat': -23.361111, 'lon': 119.7325, 'source': 'Mount Newman mine, Pilbara WA'},
    'aus-yandicoogina-fac': {'lat': -22.776192, 'lon': 119.2, 'source': 'Yandicoogina mine, Pilbara WA'},
    'idn-earthstone-nalo-baru-mine-fac': {'lat': -2.01394, 'lon': 101.5, 'source': 'Sumatra, Indonesia'},
    'sau-jabal-sayid-fac': {'lat': 24.276714683519764, 'lon': 45.51155643412581, 'source': 'Jabal Sayid copper mine'},
    'bol-bolivar-mine-fac': {'lat': -19.5, 'lon': -66.1, 'source': 'Antequera, Potosi, Bolivia'},
    'ncl-snowy-river-project-fac': {'lat': -22.3, 'lon': 166.5, 'source': 'New Caledonia'},
    'kor-sangdong-mine-fac': {'lat': 37.15, 'lon': 128.7, 'source': 'Sangdong, Gangwon, South Korea'},
    'tkm-jv-inkai-fac': {'delete': True, 'reason': 'Duplicate of kaz-jv-inkai-fac'},
}

COUNTRY_BOUNDS = {
    'AUS': ((-44, -10), (112, 154)),
    'BRA': ((-34, 5), (-74, -34)),
    'IDN': ((-11, 6), (95, 141)),
    'SAU': ((16, 32), (34, 56)),
    'ZAF': ((-35, -22), (16, 33)),
    'CHL': ((-56, -17), (-76, -66)),
    'BOL': ((-23, -9), (-69, -57)),
    'USA': ((24, 72), (-180, -66)),
}


# =============================================================================
# COORDINATE ISSUE CLASS
# =============================================================================

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


# =============================================================================
# COORDINATE FIXER
# =============================================================================

class CoordinateFixer:
    """Detect and fix coordinate issues."""

    def __init__(self):
        self.issues: List[CoordinateIssue] = []
        self.fixes_applied: List[str] = []

    def detect_issues(self, facility: Dict, file_path: Path) -> List[CoordinateIssue]:
        """Detect coordinate issues in a facility."""
        issues = []
        facility_id = facility.get('facility_id', '')
        name = facility.get('name', '')
        country = facility.get('country_iso3', '')
        location = facility.get('location', {})
        lat, lon = location.get('lat'), location.get('lon')

        if lat is None or lon is None:
            return issues

        # Known fixes
        if facility_id in KNOWN_FIXES:
            fix = KNOWN_FIXES[facility_id]
            if fix.get('delete'):
                issues.append(CoordinateIssue(
                    issue_type='duplicate_to_delete', facility_id=facility_id, name=name,
                    country_iso3=country, file_path=file_path, current_lat=lat, current_lon=lon,
                    description=fix.get('reason', 'Duplicate'), auto_fixable=True
                ))
            else:
                issues.append(CoordinateIssue(
                    issue_type='known_fix', facility_id=facility_id, name=name,
                    country_iso3=country, file_path=file_path, current_lat=lat, current_lon=lon,
                    suggested_lat=fix['lat'], suggested_lon=fix['lon'],
                    description=fix.get('source', 'Manual fix'), auto_fixable=True
                ))
            return issues

        # Null island
        if lat == 0 and lon == 0:
            issues.append(CoordinateIssue(
                issue_type='null_island', facility_id=facility_id, name=name,
                country_iso3=country, file_path=file_path, current_lat=lat, current_lon=lon,
                description='Null island (0,0)', auto_fixable=False
            ))
            return issues

        # Wrong hemisphere
        if country in NORTHERN_HEMISPHERE_COUNTRIES and lat < 0:
            issues.append(CoordinateIssue(
                issue_type='wrong_hemisphere', facility_id=facility_id, name=name,
                country_iso3=country, file_path=file_path, current_lat=lat, current_lon=lon,
                suggested_lat=abs(lat), suggested_lon=lon,
                description=f'Negative lat for northern country {country}', auto_fixable=True
            ))

        # Truncated longitude
        if country not in LEGITIMATE_LON_1_COUNTRIES and 0.5 <= lon <= 1.5:
            issues.append(CoordinateIssue(
                issue_type='truncated_longitude', facility_id=facility_id, name=name,
                country_iso3=country, file_path=file_path, current_lat=lat, current_lon=lon,
                description=f'Longitude near 1 (likely truncated)', auto_fixable=False
            ))

        # Out of bounds
        if country in COUNTRY_BOUNDS:
            (lat_min, lat_max), (lon_min, lon_max) = COUNTRY_BOUNDS[country]
            if not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
                # Check if swapped
                if lat_min <= lon <= lat_max and lon_min <= lat <= lon_max:
                    issues.append(CoordinateIssue(
                        issue_type='swapped', facility_id=facility_id, name=name,
                        country_iso3=country, file_path=file_path, current_lat=lat, current_lon=lon,
                        suggested_lat=lon, suggested_lon=lat,
                        description=f'Lat/lon appear swapped', auto_fixable=False
                    ))
                else:
                    issues.append(CoordinateIssue(
                        issue_type='out_of_bounds', facility_id=facility_id, name=name,
                        country_iso3=country, file_path=file_path, current_lat=lat, current_lon=lon,
                        description=f'Outside {country} bounds', auto_fixable=False
                    ))

        return issues

    def scan_all(self) -> List[CoordinateIssue]:
        """Scan all facilities."""
        print("Scanning all facilities for coordinate issues...")

        for country_dir in sorted(FACILITIES_DIR.iterdir()):
            if not country_dir.is_dir():
                continue
            for fac_file in country_dir.glob("*.json"):
                try:
                    with open(fac_file, 'r') as f:
                        facility = json.load(f)
                    issues = self.detect_issues(facility, fac_file)
                    self.issues.extend(issues)
                except Exception as e:
                    print(f"Error reading {fac_file}: {e}")

        return self.issues

    def apply_fix(self, issue: CoordinateIssue, dry_run: bool = True) -> bool:
        """Apply a single fix."""
        if not issue.auto_fixable:
            return False

        if issue.issue_type == 'duplicate_to_delete':
            print(f"  [DELETE] {issue.facility_id}: {issue.description}")
            if not dry_run:
                issue.file_path.unlink()
            return True

        if not issue.file_path.exists():
            return False

        try:
            with open(issue.file_path, 'r') as f:
                facility = json.load(f)

            old_lat = facility['location']['lat']
            old_lon = facility['location']['lon']
            new_lat = issue.suggested_lat
            new_lon = issue.suggested_lon

            print(f"  [FIX] {issue.facility_id}: ({old_lat}, {old_lon}) -> ({new_lat}, {new_lon})")

            if not dry_run:
                shutil.copy2(issue.file_path, issue.file_path.with_suffix('.json.bak'))
                facility['location']['lat'] = new_lat
                facility['location']['lon'] = new_lon

                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                note = f"[COORD-FIX {timestamp}] {issue.issue_type}"
                if 'verification' not in facility:
                    facility['verification'] = {}
                facility['verification']['notes'] = facility['verification'].get('notes', '') + f" | {note}"

                with open(issue.file_path, 'w') as f:
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
            print("No auto-fixable issues.")
            return 0

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Applying {len(auto_fixable)} fixes...")
        fixed = sum(1 for i in auto_fixable if self.apply_fix(i, dry_run))
        print(f"\n{'Would fix' if dry_run else 'Fixed'}: {fixed}/{len(auto_fixable)}")
        return fixed

    def print_summary(self):
        """Print summary."""
        print("\n" + "=" * 80)
        print("COORDINATE ISSUES SUMMARY")
        print("=" * 80)

        total = len(self.issues)
        auto_fixable = len([i for i in self.issues if i.auto_fixable])

        print(f"\nTotal issues: {total}")
        print(f"  Auto-fixable: {auto_fixable}")
        print(f"  Manual: {total - auto_fixable}")

        if total > 0:
            print("\nBy type:")
            types = {}
            for i in self.issues:
                types[i.issue_type] = types.get(i.issue_type, 0) + 1
            for t, c in sorted(types.items(), key=lambda x: -x[1]):
                print(f"  {t:25s} {c:4d}")


# =============================================================================
# WRONG COUNTRY FIXER
# =============================================================================

def slugify(text: str) -> str:
    """Convert text to slug."""
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')


class WrongCountryFixer:
    """Move facilities to correct country folders."""

    def __init__(self, validation_file: Path):
        self.errors: List[Dict] = []
        self.load_errors(validation_file)

    def load_errors(self, validation_file: Path):
        """Load wrong_country errors from validation file."""
        if not validation_file.exists():
            print(f"Validation file not found: {validation_file}")
            print("Run 'python validate.py polygons --export errors.json' first")
            return

        with open(validation_file, 'r') as f:
            data = json.load(f)

        self.errors = [e for e in data['errors'] if e['error_type'] == 'wrong_country']
        print(f"Found {len(self.errors)} wrong_country errors")

    def generate_paths(self, error: Dict) -> Tuple[Path, Path, str, str]:
        """Generate new file paths and IDs."""
        current_path = ROOT / error['file_path']
        old_country = error['country_iso3']
        new_country = error['actual_country']
        old_id = error['facility_id']

        old_slug = old_id[4:-4]  # Remove country prefix and -fac suffix
        new_id = f"{new_country.lower()}-{old_slug}-fac"
        new_path = FACILITIES_DIR / new_country / f"{new_id}.json"

        return current_path, new_path, old_id, new_id

    def update_json(self, file_path: Path, new_country: str, new_id: str):
        """Update facility JSON."""
        with open(file_path, 'r') as f:
            facility = json.load(f)

        facility['country_iso3'] = new_country
        facility['facility_id'] = new_id

        with open(file_path, 'w') as f:
            json.dump(facility, f, indent=2, ensure_ascii=False)
            f.write('\n')

    def git_mv(self, src: Path, dest: Path, dry_run: bool = True) -> bool:
        """Move file using git mv."""
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dry_run:
            print(f"  [DRY RUN] git mv {src.name} -> {dest.parent.name}/{dest.name}")
            return True

        try:
            subprocess.run(['git', 'mv', str(src), str(dest)],
                          capture_output=True, text=True, check=True)
            return True
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] git mv failed: {e.stderr}")
            return False

    def process_all(self, dry_run: bool = True) -> Tuple[int, int]:
        """Process all wrong country facilities."""
        if not self.errors:
            print("No wrong_country errors to process.")
            return 0, 0

        successful = 0
        failed = 0

        for error in self.errors:
            current_path, new_path, old_id, new_id = self.generate_paths(error)

            if not current_path.exists():
                print(f"[ERROR] Not found: {current_path}")
                failed += 1
                continue

            if new_path.exists():
                print(f"[WARNING] Already exists: {new_path}")
                failed += 1
                continue

            print(f"\n{error['name']}")
            print(f"  {error['country_iso3']} -> {error['actual_country']}")
            print(f"  {old_id} -> {new_id}")

            if self.git_mv(current_path, new_path, dry_run):
                if not dry_run:
                    try:
                        self.update_json(new_path, error['actual_country'], new_id)
                        print(f"  [SUCCESS]")
                    except Exception as e:
                        print(f"  [ERROR] {e}")
                        failed += 1
                        continue
                successful += 1
            else:
                failed += 1

        return successful, failed

    def print_summary(self, successful: int, failed: int, dry_run: bool):
        """Print summary."""
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total: {len(self.errors)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")

        if dry_run:
            print("\nThis was a DRY RUN. Use --execute to apply.")


# =============================================================================
# CLI
# =============================================================================

def cmd_coordinates(args):
    """Handle coordinates subcommand."""
    fixer = CoordinateFixer()

    if args.facility:
        for country_dir in FACILITIES_DIR.iterdir():
            if not country_dir.is_dir():
                continue
            fac_file = country_dir / f"{args.facility}.json"
            if fac_file.exists():
                with open(fac_file, 'r') as f:
                    facility = json.load(f)
                issues = fixer.detect_issues(facility, fac_file)
                fixer.issues.extend(issues)
                break
    else:
        fixer.scan_all()

    fixer.print_summary()

    if args.list:
        print("\n" + "=" * 80)
        for i, issue in enumerate(fixer.issues, 1):
            status = "[AUTO]" if issue.auto_fixable else "[MANUAL]"
            print(f"\n{i}. {status} {issue.facility_id}")
            print(f"   Type: {issue.issue_type}")
            print(f"   Current: ({issue.current_lat}, {issue.current_lon})")
            if issue.suggested_lat:
                print(f"   Suggested: ({issue.suggested_lat}, {issue.suggested_lon})")
            print(f"   {issue.description}")

    if args.auto_fix:
        fixer.apply_auto_fixes(dry_run=not args.confirm)
        if not args.confirm:
            print("\nUse --confirm to apply fixes.")

    return 0


def cmd_country(args):
    """Handle country subcommand."""
    validation_file = args.validation_file or (ROOT / 'output' / 'geocoding_validation_errors.json')

    fixer = WrongCountryFixer(validation_file)

    if not fixer.errors:
        return 1

    dry_run = not args.execute
    successful, failed = fixer.process_all(dry_run=dry_run)
    fixer.print_summary(successful, failed, dry_run)

    return 0 if failed == 0 else 1


def main():
    parser = argparse.ArgumentParser(description="Unified facility fixing tool")
    subparsers = parser.add_subparsers(dest='command', help='Fix type')

    # Coordinates subcommand
    coords = subparsers.add_parser('coordinates', help='Fix coordinate issues')
    coords.add_argument('--scan', action='store_true', help='Scan for issues (default)')
    coords.add_argument('--auto-fix', action='store_true', help='Apply auto-fixes')
    coords.add_argument('--confirm', action='store_true', help='Confirm (not dry-run)')
    coords.add_argument('--facility', help='Check specific facility')
    coords.add_argument('--list', action='store_true', help='List all issues')

    # Country subcommand
    country = subparsers.add_parser('country', help='Move facilities to correct country')
    country.add_argument('--execute', action='store_true', help='Actually move files')
    country.add_argument('--dry-run', action='store_true', help='Preview only (default)')
    country.add_argument('--validation-file', type=Path, help='Validation errors JSON')

    args = parser.parse_args()

    if args.command == 'coordinates':
        return cmd_coordinates(args)
    elif args.command == 'country':
        return cmd_country(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
