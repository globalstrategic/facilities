#!/usr/bin/env python3
"""
Fix facilities that are in the wrong country folder.

This script reads geocoding validation errors and moves facilities to the correct
country folder, updating their IDs and metadata accordingly.

Usage:
    python fix_wrong_country.py --dry-run    # Preview changes (default)
    python fix_wrong_country.py --execute    # Actually move files
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheticals
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')


def load_wrong_country_errors(validation_file: Path) -> List[Dict]:
    """Load and filter wrong_country errors from validation file."""
    print(f"Loading validation errors from {validation_file}")

    with open(validation_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    wrong_country = [e for e in data['errors'] if e['error_type'] == 'wrong_country']
    print(f"Found {len(wrong_country)} facilities in wrong country folders")

    return wrong_country


def generate_new_paths(error: Dict, root_dir: Path) -> Tuple[Path, Path, str, str]:
    """
    Generate new file paths and IDs for a facility.

    Returns:
        Tuple of (current_path, new_path, old_facility_id, new_facility_id)
    """
    current_path = root_dir / error['file_path']

    old_country = error['declared_country']
    new_country = error['actual_country']
    old_facility_id = error['facility_id']

    # Extract the name slug from old ID (everything after the country prefix)
    # e.g., "arg-el-teniente-fac" -> "el-teniente"
    old_slug = old_facility_id[4:-4]  # Remove "xxx-" prefix and "-fac" suffix

    # Generate new facility ID with correct country
    new_facility_id = f"{new_country.lower()}-{old_slug}-fac"

    # Generate new file path
    new_filename = f"{new_facility_id}.json"
    new_path = root_dir / "facilities" / new_country / new_filename

    return current_path, new_path, old_facility_id, new_facility_id


def update_facility_json(file_path: Path, new_country: str, new_facility_id: str) -> None:
    """Update country_iso3 and facility_id in the facility JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        facility = json.load(f)

    facility['country_iso3'] = new_country
    facility['facility_id'] = new_facility_id

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(facility, f, indent=2, ensure_ascii=False)
        f.write('\n')


def git_mv(src: Path, dest: Path, dry_run: bool = True) -> bool:
    """
    Move a file using git mv.

    Args:
        src: Source file path
        dest: Destination file path
        dry_run: If True, only print what would happen

    Returns:
        True if successful, False otherwise
    """
    # Ensure destination directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dry_run:
        print(f"  [DRY RUN] Would execute: git mv {src} {dest}")
        return True

    try:
        result = subprocess.run(
            ['git', 'mv', str(src), str(dest)],
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Failed to git mv: {e.stderr}")
        return False


def process_facilities(
    errors: List[Dict],
    root_dir: Path,
    dry_run: bool = True
) -> Tuple[int, int]:
    """
    Process all facilities, moving them to correct folders.

    Returns:
        Tuple of (successful_count, failed_count)
    """
    successful = 0
    failed = 0

    for error in errors:
        current_path, new_path, old_id, new_id = generate_new_paths(error, root_dir)

        # Check if source file exists
        if not current_path.exists():
            print(f"\n[ERROR] Source file not found: {current_path}")
            failed += 1
            continue

        # Check if destination already exists
        if new_path.exists() and current_path != new_path:
            print(f"\n[WARNING] Destination already exists: {new_path}")
            print(f"  Skipping {error['name']}")
            failed += 1
            continue

        print(f"\n{error['name']}")
        print(f"  Current:  {current_path.relative_to(root_dir)}")
        print(f"  New:      {new_path.relative_to(root_dir)}")
        print(f"  Country:  {error['declared_country']} -> {error['actual_country']}")
        print(f"  ID:       {old_id} -> {new_id}")

        if not dry_run:
            # First move the file
            if git_mv(current_path, new_path, dry_run=False):
                # Then update the JSON content
                try:
                    update_facility_json(new_path, error['actual_country'], new_id)
                    print(f"  [SUCCESS] Moved and updated")
                    successful += 1
                except Exception as e:
                    print(f"  [ERROR] Failed to update JSON: {e}")
                    failed += 1
            else:
                failed += 1
        else:
            successful += 1

    return successful, failed


def generate_markdown_report(errors: List[Dict], output_file: Path, root_dir: Path) -> None:
    """Generate a markdown report of facilities to fix."""
    print(f"\nGenerating report at {output_file}")

    # Group by declared -> actual country
    by_country_pair = {}
    for error in errors:
        key = f"{error['declared_country']} -> {error['actual_country']}"
        if key not in by_country_pair:
            by_country_pair[key] = []
        by_country_pair[key].append(error)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Wrong Country Facilities Report\n\n")
        f.write(f"**Total Facilities:** {len(errors)}\n\n")
        f.write(f"**Generated:** {Path(output_file).stat().st_mtime}\n\n")

        f.write("## Summary by Country Pair\n\n")
        f.write("| From → To | Count |\n")
        f.write("|-----------|-------|\n")
        for pair in sorted(by_country_pair.keys()):
            count = len(by_country_pair[pair])
            f.write(f"| {pair} | {count} |\n")

        f.write("\n## Detailed Facility List\n\n")

        for pair in sorted(by_country_pair.keys()):
            facilities = by_country_pair[pair]
            f.write(f"### {pair} ({len(facilities)} facilities)\n\n")

            for error in sorted(facilities, key=lambda x: x['name']):
                current_path, new_path, old_id, new_id = generate_new_paths(error, root_dir)

                f.write(f"#### {error['name']}\n\n")
                f.write(f"- **Current Path:** `{current_path.relative_to(root_dir)}`\n")
                f.write(f"- **New Path:** `{new_path.relative_to(root_dir)}`\n")
                f.write(f"- **Coordinates:** {error['lat']:.6f}, {error['lon']:.6f}\n")
                f.write(f"- **Declared Country:** {error['declared_country']}\n")
                f.write(f"- **Actual Country:** {error['actual_country']}\n")
                f.write(f"- **Current ID:** `{old_id}`\n")
                f.write(f"- **New ID:** `{new_id}`\n")
                f.write(f"- **Action:** Move to `facilities/{error['actual_country']}/` and update metadata\n\n")

        f.write("## How to Fix\n\n")
        f.write("```bash\n")
        f.write("# Dry run (preview changes)\n")
        f.write("python scripts/tools/fix_wrong_country.py --dry-run\n\n")
        f.write("# Execute the fixes\n")
        f.write("python scripts/tools/fix_wrong_country.py --execute\n")
        f.write("```\n\n")

        f.write("## Notes\n\n")
        f.write("- Files will be moved using `git mv` to preserve history\n")
        f.write("- Facility IDs will be updated to match new country code\n")
        f.write("- `country_iso3` field will be updated in JSON\n")
        f.write("- Directory structure will be created if needed\n")

    print(f"Report written to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix facilities in wrong country folders",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview what would be fixed (default)
  python fix_wrong_country.py --dry-run

  # Actually move the files
  python fix_wrong_country.py --execute
        """
    )

    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually move files (default is dry-run mode)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without moving files (default)'
    )

    parser.add_argument(
        '--validation-file',
        type=Path,
        default=Path(__file__).parent.parent.parent / 'output' / 'geocoding_validation_errors.json',
        help='Path to geocoding validation errors file'
    )

    parser.add_argument(
        '--report',
        type=Path,
        default=Path(__file__).parent.parent.parent / 'output' / 'wrong_country_report.md',
        help='Path to output markdown report'
    )

    args = parser.parse_args()

    # Determine if we're in dry-run mode
    dry_run = not args.execute or args.dry_run

    # Get root directory (3 levels up from this script)
    root_dir = Path(__file__).parent.parent.parent

    print("=" * 80)
    print("WRONG COUNTRY FACILITY FIXER")
    print("=" * 80)
    print(f"Mode: {'DRY RUN (preview only)' if dry_run else 'EXECUTE (will move files)'}")
    print(f"Root directory: {root_dir}")
    print()

    # Load errors
    errors = load_wrong_country_errors(args.validation_file)

    if not errors:
        print("No wrong_country errors found. Nothing to do.")
        return 0

    # Generate markdown report
    generate_markdown_report(errors, args.report, root_dir)

    print("\n" + "=" * 80)
    print("PROCESSING FACILITIES")
    print("=" * 80)

    # Process facilities
    successful, failed = process_facilities(errors, root_dir, dry_run=dry_run)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total facilities:  {len(errors)}")
    print(f"Successful:        {successful}")
    print(f"Failed:            {failed}")

    if dry_run:
        print("\nThis was a DRY RUN. No files were moved.")
        print("To actually move files, run with --execute flag:")
        print(f"  python {Path(__file__).name} --execute")
    else:
        print("\nFiles have been moved and updated.")
        print("Don't forget to commit the changes:")
        print("  git status")
        print("  git add .")
        print("  git commit -m 'Fix facilities in wrong country folders'")

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
