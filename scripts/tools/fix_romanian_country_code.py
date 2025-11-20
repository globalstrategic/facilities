#!/usr/bin/env python3
"""
Fix ROMANIAN country code to ROU in facility JSON files.

This script finds all facilities with country_iso3="ROMANIAN" and updates them to "ROU".
"""

import json
import sys
from pathlib import Path


def fix_romanian_files():
    """Fix all facility files with ROMANIAN country code."""

    # Get project root and facilities directory
    project_root = Path(__file__).parent.parent.parent
    facilities_dir = project_root / "facilities"
    rou_dir = facilities_dir / "ROU"

    if not rou_dir.exists():
        print(f"❌ Error: ROU directory not found at {rou_dir}")
        return

    json_files = list(rou_dir.glob("*.json"))
    print(f"Found {len(json_files)} files in ROU directory")

    fixed_count = 0
    already_correct = 0

    for json_file in json_files:
        # Read the file
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Check if it has ROMANIAN
        if data.get('country_iso3') == 'ROMANIAN':
            print(f"  Fixing: {json_file.name}")

            # Fix country_iso3
            data['country_iso3'] = 'ROU'

            # Write back
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write('\n')  # Add trailing newline

            fixed_count += 1
        elif data.get('country_iso3') == 'ROU':
            already_correct += 1

    print(f"\n✅ Complete!")
    print(f"  Fixed: {fixed_count} files")
    print(f"  Already correct: {already_correct} files")
    print(f"  Total processed: {len(json_files)} files")


def main():
    print("Fixing ROMANIAN → ROU in facility JSON files...")
    print("=" * 60)
    fix_romanian_files()


if __name__ == '__main__':
    main()
