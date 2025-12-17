#!/usr/bin/env python3
"""
Fix various coordinate issues in facilities database.

Issues detected:
1. Truncated longitude (lon ≈ 1)
2. Wrong sign (negative lon for eastern hemisphere countries)
3. Swapped lat/lon
4. Completely wrong locations
5. Null island (0, 0)

Usage:
    python fix_coordinate_issues.py --scan          # Scan and report issues
    python fix_coordinate_issues.py --execute      # Apply fixes
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import shutil

ROOT = Path(__file__).parent.parent.parent
FACILITIES_DIR = ROOT / "facilities"

# Known fixes - manually researched correct coordinates
KNOWN_FIXES = {
    # Truncated longitude fixes (lon ≈ 1)
    'aus-mount-newman-fac': {'lat': -23.361111, 'lon': 119.7325, 'source': 'Mount Newman mine, Pilbara WA'},
    'aus-yandicoogina-fac': {'lat': -22.776192, 'lon': 119.2, 'source': 'Yandicoogina mine, Pilbara WA'},
    'idn-earthstone-nalo-baru-mine-fac': {'lat': -2.01394, 'lon': 101.5, 'source': 'Sumatra, Indonesia (estimated)'},
    'idn-abe-coal-mine-fac': {'lat': 1.02, 'lon': 117.0, 'source': 'East Kalimantan, Indonesia (estimated)'},

    # Wrong sign fixes
    'sau-jabal-sayid-fac': {'lat': 24.276714683519764, 'lon': 45.51155643412581, 'source': 'Jabal Sayid copper mine, Saudi Arabia - fixed lon sign'},

    # Wrong location fixes - facilities in wrong country
    'ncl-snowy-river-project-fac': {'lat': -22.3, 'lon': 166.5, 'source': 'Moved from NZ coords to New Caledonia', 'note': 'Or delete if this is actually a NZ project'},
    'aus-esk-river-fac': {'lat': -41.5, 'lon': 146.5, 'source': 'Tasmania, Australia (estimated from name)'},

    # Brazil truncated longitude
    'bra-bemisa-baratinha-mine-fac': {'lat': -19.564705, 'lon': -44.0, 'source': 'Minas Gerais, Brazil (estimated)'},

    # Guinea - Lola Graphite is near N'Zérékoré
    'gin-lola-graphite-project-fac': {'lat': 7.8, 'lon': -8.5, 'source': 'Lola, Forest Region, Guinea'},

    # TKM JV Inkai should be in Kazakhstan, not Turkmenistan
    'tkm-jv-inkai-fac': {'delete': True, 'reason': 'JV Inkai is in Kazakhstan, duplicate of kaz-jv-inkai-fac'},

    # Korea Sangdong has wrong coords
    'kor-sangdong-mine-fac': {'lat': 37.15, 'lon': 128.7, 'source': 'Sangdong mine, Gangwon Province, South Korea'},

    # NCL Tiebaghi - lat is slightly off for New Caledonia
    'ncl-sln-ti-baghi-mine-fac': {'lat': -20.44472, 'lon': 164.21722, 'source': 'Tiébaghi mine, northern New Caledonia'},

    # Indonesia Gosowong - coords are in Philippines, should be North Maluku
    'idn-gosowong-fac': {'lat': 1.0, 'lon': 127.9, 'source': 'Gosowong mine, Halmahera, North Maluku, Indonesia'},

    # South Africa Elders Coal - lon=2 is truncated
    'zaf-elders-coal-project-fac': {'lat': -26.239016, 'lon': 29.0, 'source': 'Mpumalanga Province, South Africa (estimated)'},

    # === Round 2 fixes - remaining validation errors ===

    # Bolivia - wrong hemisphere (lat should be negative)
    'bol-bolivar-mine-fac': {'lat': -19.5, 'lon': -66.1, 'source': 'Antequera, Potosí Department, Bolivia - fixed hemisphere'},

    # Colombia - wrong sign on longitude (should be western hemisphere)
    'col-san-juaquin-coal-mine-fac': {'lat': 6.0282939, 'lon': -75.6922416, 'source': 'Colombia - fixed longitude sign'},

    # Bolivia Laramcota - coords point to Caribbean, need proper Bolivia coords
    'bol-laramcota-mine-fac': {'lat': -18.5, 'lon': -68.0, 'source': 'La Paz Department, Bolivia (estimated)'},

    # Bolivia Santa Barbara - has PNG coords (!), needs proper Bolivia coords
    'bol-santa-barbara-fac': {'lat': -17.8, 'lon': -63.2, 'source': 'Santa Cruz Department, Bolivia (estimated)'},

    # USA Berwind Coal - offshore SC coords, should be in WV/VA coal country
    'usa-berwind-coal-mine-fac': {'lat': 37.28, 'lon': -81.65, 'source': 'Berwind, McDowell County, WV'},

    # USA Tonkin Springs - offshore CA, should be Nevada
    'usa-tonkin-springs-gold-mine-fac': {'lat': 39.77, 'lon': -116.23, 'source': 'Tonkin Springs, Eureka County, Nevada'},

    # Venezuela Los Pijiguaos - offshore Caribbean, should be inland Bolivar
    'ven-los-pijiguaos-mine-fac': {'lat': 6.58, 'lon': -66.75, 'source': 'Los Pijiguaos bauxite mine, Bolivar State, Venezuela'},

    # South Africa Namakwa Sands - slightly offshore, should be onshore
    'zaf-namakwa-sands-fac': {'lat': -31.2, 'lon': 17.9, 'source': 'Namakwa Sands, West Coast, South Africa'},
    'zaf-namakwa-sands-smelter-fac': {'lat': -32.9, 'lon': 18.0, 'source': 'Saldanha Smelter, Western Cape, South Africa'},

    # Chile Salar del Carmen - slightly offshore, should be Atacama inland
    'chl-salar-del-carmen-fac': {'lat': -23.62, 'lon': -70.35, 'source': 'Salar del Carmen, Antofagasta Region, Chile'},

    # Gabon Eteke Gold - slightly offshore
    'gab-et-k-gold-project-fac': {'lat': -1.15, 'lon': 10.85, 'source': 'Etéké Gold Project, Ogooué-Lolo Province, Gabon'},

    # Indonesia AGM Coal - in strait, needs proper Sumatra coords
    'idn-agm-coal-mine-fac': {'lat': 2.5, 'lon': 99.5, 'source': 'North Sumatra, Indonesia (estimated)'},

    # Madagascar Manampotsy - south of island
    'mdg-manampotsy-pgm-occurrence-fac': {'lat': -22.3, 'lon': 47.5, 'source': 'Manampotsy area, Madagascar (estimated)'},

    # North Korea Nampo - offshore, should be on coast
    'prk-nampo-smelting-complex-fac': {'lat': 38.73, 'lon': 125.38, 'source': 'Nampo city, South Pyongan Province, North Korea'},

    # Sudan Chikay - in Red Sea, needs proper Sudan coords
    'sdn-chikay-mine-fac': {'lat': 20.5, 'lon': 36.5, 'source': 'Red Sea State, Sudan (estimated inland)'},
}


def scan_all_issues() -> Dict[str, List]:
    """Scan for all coordinate issues."""
    issues = {
        'null_island': [],
        'truncated_lon': [],
        'wrong_sign': [],
        'swapped': [],
        'out_of_bounds': [],
        'known_fixes': [],
    }

    # Country bounding boxes (approximate)
    BOUNDS = {
        'AUS': ((-44, -10), (112, 154)),
        'BRA': ((-34, 5), (-74, -34)),
        'IDN': ((-11, 6), (95, 141)),
        'SAU': ((16, 32), (34, 56)),
        'GIN': ((7, 13), (-15, -7)),
        'NCL': ((-23, -19), (163, 169)),
        'KOR': ((33, 39), (124, 132)),
        'TKM': ((35, 43), (52, 66)),
    }

    for f in FACILITIES_DIR.glob('*/*.json'):
        try:
            data = json.load(open(f))
            fid = data.get('facility_id', '')
            lat = data.get('location', {}).get('lat')
            lon = data.get('location', {}).get('lon')
            country = data.get('country_iso3', '')
            name = data.get('name', '')

            if lat is None or lon is None:
                continue

            # Check for known fixes first
            if fid in KNOWN_FIXES:
                fix = KNOWN_FIXES[fid]
                issues['known_fixes'].append({
                    'file': f,
                    'facility_id': fid,
                    'name': name,
                    'country': country,
                    'current_lat': lat,
                    'current_lon': lon,
                    'fix': fix,
                })
                continue

            # Null island
            if lat == 0 and lon == 0:
                issues['null_island'].append({
                    'file': f, 'facility_id': fid, 'name': name, 'country': country
                })
                continue

            # Truncated longitude (lon ≈ 1)
            if 0.5 <= lon <= 1.5 and country not in ['FRA', 'DZA', 'BFA', 'NER', 'BEN', 'TGO', 'GHA', 'GBR', 'AND', 'ESP']:
                issues['truncated_lon'].append({
                    'file': f, 'facility_id': fid, 'name': name, 'country': country,
                    'lat': lat, 'lon': lon
                })

            # Check bounds
            if country in BOUNDS:
                (lat_min, lat_max), (lon_min, lon_max) = BOUNDS[country]
                in_bounds = lat_min <= lat <= lat_max and lon_min <= lon <= lon_max

                if not in_bounds:
                    # Check if it's a sign error
                    if lat_min <= lat <= lat_max and lon_min <= abs(lon) <= lon_max:
                        issues['wrong_sign'].append({
                            'file': f, 'facility_id': fid, 'name': name, 'country': country,
                            'lat': lat, 'lon': lon, 'suggested_lon': abs(lon)
                        })
                    # Check if swapped
                    elif lat_min <= lon <= lat_max and lon_min <= lat <= lon_max:
                        issues['swapped'].append({
                            'file': f, 'facility_id': fid, 'name': name, 'country': country,
                            'lat': lat, 'lon': lon
                        })
                    else:
                        issues['out_of_bounds'].append({
                            'file': f, 'facility_id': fid, 'name': name, 'country': country,
                            'lat': lat, 'lon': lon
                        })

        except Exception as e:
            pass

    return issues


def apply_known_fixes(dry_run: bool = True) -> Tuple[int, int]:
    """Apply known coordinate fixes."""
    fixed = 0
    deleted = 0

    for fid, fix in KNOWN_FIXES.items():
        # Find the facility file
        file_path = None
        for f in FACILITIES_DIR.glob(f'*/{fid}.json'):
            file_path = f
            break

        if not file_path or not file_path.exists():
            print(f"  [SKIP] {fid}: File not found")
            continue

        if fix.get('delete'):
            print(f"  [DELETE] {fid}: {fix.get('reason', 'No reason given')}")
            if not dry_run:
                file_path.unlink()
                deleted += 1
            continue

        try:
            data = json.load(open(file_path))
            old_lat = data['location']['lat']
            old_lon = data['location']['lon']
            new_lat = fix['lat']
            new_lon = fix['lon']

            print(f"  [FIX] {fid}")
            print(f"        ({old_lat}, {old_lon}) -> ({new_lat}, {new_lon})")
            print(f"        Source: {fix.get('source', 'Unknown')}")

            if not dry_run:
                # Backup
                backup = file_path.with_suffix('.json.coord_bak')
                shutil.copy2(file_path, backup)

                # Apply fix
                data['location']['lat'] = new_lat
                data['location']['lon'] = new_lon

                # Add note
                timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                note = f"[COORD-FIX {timestamp}] Changed from ({old_lat}, {old_lon}) to ({new_lat}, {new_lon}). Source: {fix.get('source', 'manual')}"

                if 'verification' in data and 'notes' in data['verification']:
                    data['verification']['notes'] += f" | {note}"
                else:
                    if 'verification' not in data:
                        data['verification'] = {}
                    data['verification']['notes'] = note

                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                    f.write('\n')

                fixed += 1

        except Exception as e:
            print(f"  [ERROR] {fid}: {e}")

    return fixed, deleted


def print_report(issues: Dict[str, List]):
    """Print issues report."""
    print("\n" + "=" * 80)
    print("COORDINATE ISSUES REPORT")
    print("=" * 80)

    print(f"\n=== KNOWN FIXES ({len(issues['known_fixes'])}) ===")
    for item in issues['known_fixes']:
        fix = item['fix']
        if fix.get('delete'):
            print(f"  [DELETE] {item['country']}/{item['facility_id']}: {fix.get('reason')}")
        else:
            print(f"  {item['country']}/{item['facility_id']}: ({item['current_lat']}, {item['current_lon']}) -> ({fix['lat']}, {fix['lon']})")

    print(f"\n=== NULL ISLAND ({len(issues['null_island'])}) ===")
    for item in issues['null_island'][:10]:
        print(f"  {item['country']}/{item['facility_id']}: {item['name']}")
    if len(issues['null_island']) > 10:
        print(f"  ... and {len(issues['null_island']) - 10} more")

    print(f"\n=== TRUNCATED LONGITUDE ({len(issues['truncated_lon'])}) ===")
    for item in issues['truncated_lon']:
        print(f"  {item['country']}/{item['facility_id']}: lat={item['lat']}, lon={item['lon']} - {item['name']}")

    print(f"\n=== WRONG SIGN ({len(issues['wrong_sign'])}) ===")
    for item in issues['wrong_sign']:
        print(f"  {item['country']}/{item['facility_id']}: lon={item['lon']} -> {item['suggested_lon']} - {item['name']}")

    print(f"\n=== SWAPPED ({len(issues['swapped'])}) ===")
    for item in issues['swapped']:
        print(f"  {item['country']}/{item['facility_id']}: lat={item['lat']}, lon={item['lon']} - {item['name']}")

    print(f"\n=== OUT OF BOUNDS ({len(issues['out_of_bounds'])}) ===")
    for item in issues['out_of_bounds'][:15]:
        print(f"  {item['country']}/{item['facility_id']}: ({item['lat']}, {item['lon']}) - {item['name']}")
    if len(issues['out_of_bounds']) > 15:
        print(f"  ... and {len(issues['out_of_bounds']) - 15} more")

    total = sum(len(v) for v in issues.values())
    print(f"\n{'=' * 80}")
    print(f"TOTAL ISSUES: {total}")
    print(f"  Known fixes ready: {len(issues['known_fixes'])}")
    print(f"  Need research: {total - len(issues['known_fixes'])}")


def main():
    parser = argparse.ArgumentParser(description="Fix coordinate issues in facilities")
    parser.add_argument('--scan', action='store_true', help='Scan and report issues')
    parser.add_argument('--execute', action='store_true', help='Apply known fixes')
    parser.add_argument('--dry-run', action='store_true', help='Preview fixes without applying')

    args = parser.parse_args()

    if args.scan or (not args.execute):
        print("Scanning for coordinate issues...")
        issues = scan_all_issues()
        print_report(issues)

    if args.execute:
        dry_run = args.dry_run
        print(f"\n{'[DRY RUN] ' if dry_run else ''}Applying known fixes...")
        print("=" * 80)
        fixed, deleted = apply_known_fixes(dry_run=dry_run)
        print(f"\n{'Would fix' if dry_run else 'Fixed'}: {fixed}")
        print(f"{'Would delete' if dry_run else 'Deleted'}: {deleted}")

        if dry_run:
            print("\nThis was a dry run. Run with --execute without --dry-run to apply.")


if __name__ == '__main__':
    main()
