#!/usr/bin/env python3
"""Backfill canonical names using the new canonicalizer."""

import json
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.name_canonicalizer_v2 import FacilityNameCanonicalizer, SLUGS

def main(dry_run=False):
    """Run canonical name backfill."""

    # Load all facilities first to build global slug registry
    print("Loading all facilities...")
    facilities_dir = Path("facilities")
    all_facilities = []

    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        for fac_file in sorted(country_dir.glob("*.json")):
            try:
                with open(fac_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)
                    facility['_file_path'] = fac_file
                    all_facilities.append(facility)
            except Exception as e:
                print(f"Error loading {fac_file}: {e}")

    print(f"Loaded {len(all_facilities)} facilities")

    # Pre-load existing slugs to avoid collisions
    existing_slugs = [f.get("canonical_slug") for f in all_facilities if f.get("canonical_slug")]
    if existing_slugs:
        print(f"Pre-loading {len(existing_slugs)} existing slugs")
        SLUGS.load_existing(existing_slugs)

    # Process each facility
    canonicalizer = FacilityNameCanonicalizer()
    updated = 0
    skipped = 0

    for facility in all_facilities:
        # Skip if already has canonical name (unless forcing)
        if facility.get("canonical_name") and not dry_run:
            skipped += 1
            continue

        # Canonicalize
        result = canonicalizer.canonicalize_facility(facility)

        # Update facility
        facility["canonical_name"] = result["canonical_name"]
        facility["canonical_slug"] = result["canonical_slug"]
        facility["display_name"] = result["display_name"]
        facility["primary_type"] = result["primary_type"]
        facility["type_confidence"] = result["type_confidence"]

        # Update data quality
        if "data_quality" not in facility:
            facility["data_quality"] = {}
        facility["data_quality"]["canonicalization_confidence"] = result["canonicalization_confidence"]

        # Set flags
        flags = facility["data_quality"].setdefault("flags", {})
        comps = result["canonical_components"]

        if not comps.get("town"):
            flags["town_missing"] = True
        if not comps.get("operator_display"):
            flags["operator_unresolved"] = True
        if not result["canonical_name"] or result["canonicalization_confidence"] < 0.5:
            flags["canonical_name_incomplete"] = True

        if not dry_run:
            # Write back
            fac_file = facility.pop('_file_path')
            with open(fac_file, 'w', encoding='utf-8') as f:
                json.dump(facility, f, ensure_ascii=False, indent=2)
                f.write("\n")
            updated += 1
            if updated % 100 == 0:
                print(f"Updated {updated} facilities...")
        else:
            # Dry run - just count
            updated += 1

    print(f"\n{'[DRY RUN] Would update' if dry_run else 'Updated'} {updated} facilities")
    print(f"Skipped {skipped} facilities with existing canonical names")

    # Check for collisions
    slug_counts = {}
    for f in all_facilities:
        slug = f.get("canonical_slug")
        if slug:
            slug_counts[slug] = slug_counts.get(slug, 0) + 1

    collisions = {k: v for k, v in slug_counts.items() if v > 1}
    if collisions:
        print(f"\nWARNING: Found {len(collisions)} slug collisions:")
        for slug, count in list(collisions.items())[:10]:
            print(f"  {slug}: {count} facilities")
    else:
        print("\n✓ No slug collisions detected")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backfill canonical names")
    parser.add_argument("--dry-run", action="store_true", help="Dry run only")
    args = parser.parse_args()

    main(dry_run=args.dry_run)