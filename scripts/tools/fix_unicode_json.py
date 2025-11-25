#!/usr/bin/env python3
"""
Fix Unicode escape sequences in facility JSON files.

Converts: "Shkod\u00ebr" → "Shkodër"
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def fix_json_file(path: Path) -> bool:
    """Fix Unicode escapes in a JSON file."""
    try:
        # Read with escaped unicode
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Write with actual unicode characters
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return True
    except Exception as e:
        print(f"Error fixing {path}: {e}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fix Unicode escapes in JSON files")
    parser.add_argument("--country", help="ISO3 country code (omit for all)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    facilities_dir = Path("facilities")

    # Determine which countries
    if args.country:
        country_dirs = [facilities_dir / args.country.upper()]
    else:
        country_dirs = sorted([d for d in facilities_dir.iterdir() if d.is_dir()])

    print(f"Fixing Unicode escapes in JSON files...")
    print(f"Countries to process: {len(country_dirs)}\n")

    total_fixed = 0
    total_files = 0

    for country_dir in country_dirs:
        if not country_dir.exists():
            continue

        country_fixed = 0
        for json_file in country_dir.glob("*.json"):
            total_files += 1

            if args.dry_run:
                # Just check if file contains escape sequences
                with open(json_file, 'r') as f:
                    content = f.read()
                    if '\\u' in content:
                        print(f"Would fix: {json_file.name}")
                        country_fixed += 1
            else:
                if fix_json_file(json_file):
                    country_fixed += 1

        if country_fixed > 0:
            total_fixed += country_fixed
            action = "Would fix" if args.dry_run else "Fixed"
            print(f"{country_dir.name}: {action} {country_fixed} files")

    print(f"\n{'='*60}")
    print(f"Total files scanned: {total_files}")
    action = "Would fix" if args.dry_run else "Fixed"
    print(f"{action}: {total_fixed}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
