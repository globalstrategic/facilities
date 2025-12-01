#!/usr/bin/env python3
"""
Fix schema violations in facility JSON files.

Fixes:
- Invalid status values: historic → closed, development_project → construction, etc.
- Invalid type values: district → mine, project → development, facility → plant
- Invalid precision values: district → approximate
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

# Mapping of invalid to valid values
STATUS_MAPPING = {
    "historic": "closed",
    "development_project": "construction",
    "inactive": "care_and_maintenance",
    "abandoned": "closed",
}

TYPE_MAPPING = {
    "district": "mine",
    "project": "development",
    "complex": "plant",
    "facility": "plant",
}

PRECISION_MAPPING = {
    "district": "approximate",
}

# Valid schema values
VALID_STATUSES = ["operating", "planned", "construction", "care_and_maintenance", "closed", "suspended", "unknown"]
VALID_TYPES = ["mine", "smelter", "refinery", "concentrator", "plant", "mill", "heap_leach", "tailings",
               "exploration", "development", "hydromet_plant", "rolling_mill", "steel_plant",
               "battery_recycling", "processing_plant"]
VALID_PRECISIONS = ["exact", "site", "approximate", "region", "unknown"]


def fix_facility(filepath: Path) -> Tuple[bool, List[str], str]:
    """Fix schema violations in a single facility file.

    Returns:
        (changed, changes, error): Whether file was changed, list of change descriptions, error message if any
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return False, [], f"JSON decode error: {e}"
    except Exception as e:
        return False, [], f"Error reading file: {e}"

    changes = []

    # Fix status
    if "status" in data:
        old_status = data["status"]
        if old_status not in VALID_STATUSES:
            new_status = STATUS_MAPPING.get(old_status, "unknown")
            data["status"] = new_status
            changes.append(f"status: {old_status} → {new_status}")

    # Fix primary_type
    if "primary_type" in data:
        old_type = data["primary_type"]
        if old_type and old_type not in VALID_TYPES:
            new_type = TYPE_MAPPING.get(old_type, "plant")
            data["primary_type"] = new_type
            changes.append(f"primary_type: {old_type} → {new_type}")

    # Fix types array
    if "types" in data:
        old_types = data["types"]
        new_types = []
        for t in old_types:
            if t not in VALID_TYPES:
                new_t = TYPE_MAPPING.get(t, "plant")
                if f"types[]: {t} → {new_t}" not in [c for c in changes]:
                    changes.append(f"types[]: {t} → {new_t}")
                new_types.append(new_t)
            else:
                new_types.append(t)
        if new_types != old_types:
            data["types"] = new_types

    # Fix location precision
    if "location" in data and "precision" in data["location"]:
        old_precision = data["location"]["precision"]
        if old_precision not in VALID_PRECISIONS:
            new_precision = PRECISION_MAPPING.get(old_precision, "unknown")
            data["location"]["precision"] = new_precision
            changes.append(f"precision: {old_precision} → {new_precision}")

    if changes:
        # Write back to file
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write('\n')
            return True, changes, None
        except Exception as e:
            return False, [], f"Error writing file: {e}"

    return False, [], None


def main():
    """Scan all facility files and fix schema violations."""
    facilities_dir = Path(__file__).parent.parent / "facilities"

    if not facilities_dir.exists():
        print(f"Error: facilities directory not found at {facilities_dir}")
        return

    total_files = 0
    fixed_files = 0
    error_files = {}
    all_changes = {}

    for json_file in facilities_dir.rglob("*.json"):
        total_files += 1
        changed, changes, error = fix_facility(json_file)

        if error:
            rel_path = json_file.relative_to(facilities_dir)
            error_files[str(rel_path)] = error
        elif changed:
            fixed_files += 1
            rel_path = json_file.relative_to(facilities_dir)
            all_changes[str(rel_path)] = changes

    # Print summary
    print(f"\n{'='*70}")
    print(f"Schema Violation Fix Summary")
    print(f"{'='*70}")
    print(f"Total files scanned: {total_files}")
    print(f"Files fixed: {fixed_files}")
    print(f"Files with errors: {len(error_files)}")
    print(f"{'='*70}\n")

    if error_files:
        print("Files with errors:\n")
        for filepath, error in sorted(error_files.items()):
            print(f"{filepath}: {error}")
        print()

    if all_changes:
        print("Changes made:\n")
        for filepath, changes in sorted(all_changes.items()):
            print(f"{filepath}:")
            for change in changes:
                print(f"  - {change}")
            print()
    else:
        print("No schema violations found!")


if __name__ == "__main__":
    main()
